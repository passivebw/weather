"""
intraday_monitor.py — Intraday position monitor

Runs every minute via cron. Reads watchlist.json for active positions,
checks live METAR obs against kill signals, and fires Discord alerts
when a position is at risk.

Cron entry (runs every minute):
  * * * * * cd /opt/kalshi-weather-bot && .venv/bin/python3 intraday_monitor.py >> /var/log/intraday_monitor.log 2>&1

watchlist.json format:
{
  "positions": [
    {
      "id": "unique_id",
      "city": "Minneapolis",
      "station": "KMSP",
      "city_tz": "America/Chicago",
      "legs": [
        {"label": "YES 61-62", "ticker": "KXHIGHTMIN-23APR08-T61", "side": "high", "lo": 61, "hi": 62},
        {"label": "YES 59-60", "ticker": "KXHIGHTMIN-23APR08-T59", "side": "high", "lo": 59, "hi": 60}
      ],
      "kill_signals": [
        {"type": "precip", "codes": ["RA", "SHRA", "TSRA"], "message": "Rain detected — afternoon temps may not recover", "affects_legs": ["YES 59-60"]},
        {"type": "temp_drop", "drop_f": 3.0, "before_hour_local": 14, "message": "Temp falling before 2PM — high may be set early", "affects_legs": ["YES 61-62", "YES 59-60"]},
        {"type": "temp_above", "threshold_f": 63, "message": "Temp above 63F — position out of range high side", "affects_legs": ["YES 61-62", "YES 59-60"]}
      ],
      "notes": "Post-Frontal Lock. Rain 11AM-5PM is key risk. NWS says falling to 57 in afternoon.",
      "entered_at": "2026-04-08T08:30:00",
      "active": true
    }
  ]
}
"""

import json
import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

WATCHLIST_PATH = os.path.join(os.path.dirname(__file__), "watchlist.json")
STATE_PATH = os.path.join(os.path.dirname(__file__), "monitor_state.json")

# Load env
env = {}
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()

DISCORD_WEBHOOK = env.get("DISCORD_WEBHOOK_URL", "")

TZ_OFFSETS = {
    "America/New_York":    -5,
    "America/Chicago":     -6,
    "America/Denver":      -7,
    "America/Los_Angeles": -8,
    "America/Phoenix":     -7,
}

# ── State management ──────────────────────────────────────────────────────────
# Tracks which alerts have already been sent to avoid spamming

def load_state() -> dict:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {"alerted": {}, "peak_temps": {}, "last_status": {}}

def save_state(state: dict):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)

# ── METAR fetching ────────────────────────────────────────────────────────────

def get_current_obs(station: str, city_tz: str) -> dict:
    """
    Fetch today's obs for station since local midnight.
    Returns dict with: current_f, peak_f, day_low_f, precip_codes, wind_dir, raw_messages
    """
    offset_h = TZ_OFFSETS.get(city_tz, -5)
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc + timedelta(hours=offset_h)
    local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    utc_midnight = local_midnight - timedelta(hours=offset_h)
    start_str = utc_midnight.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        r = requests.get(
            f"https://api.weather.gov/stations/{station}/observations",
            headers={"User-Agent": "kalshi-intraday-monitor/1.0"},
            params={"start": start_str, "limit": 500},
            timeout=15,
        )
        features = r.json().get("features", [])
    except Exception as e:
        print(f"  METAR fetch error for {station}: {e}")
        return {}

    temps = []
    current_f = None
    precip_codes = set()
    wind_dirs = []
    raw_messages = []

    for feat in features:
        props = feat["properties"]
        ts_str = props.get("timestamp", "")
        if not ts_str:
            continue
        ts_utc = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if ts_utc < utc_midnight:
            continue

        # Individual temp
        temp_c = props.get("temperature", {}).get("value")
        if temp_c is not None:
            temp_f = round(temp_c * 9/5 + 32, 1)
            temps.append(temp_f)
            if current_f is None:
                current_f = temp_f  # newest first

        # 6-hour METAR groups
        raw = props.get("rawMessage", "")
        if raw:
            raw_messages.append(raw)
            for m in re.finditer(r'\b([12])([01])(\d{3})\b', raw):
                gtype, sign, ttt = m.group(1), m.group(2), int(m.group(3))
                tc = ttt / 10.0 if sign == "0" else -ttt / 10.0
                tf = round(tc * 9/5 + 32, 1)
                temps.append(tf)

            # Present weather codes — look in raw METAR for precip
            for code in ["TSRA", "SHRA", "RA", "SN", "FZRA", "DZ", "RASN"]:
                if f" {code} " in f" {raw} " or f" {code}\n" in raw or raw.endswith(f" {code}"):
                    precip_codes.add(code)

        # Wind direction
        wind_d = props.get("windDirection", {}).get("value")
        if wind_d is not None:
            wind_dirs.append(int(wind_d))

    return {
        "current_f": current_f,
        "peak_f": max(temps) if temps else None,
        "day_low_f": min(temps) if temps else None,
        "precip_codes": precip_codes,
        "wind_dirs": wind_dirs,
        "local_hour": local_now.hour + local_now.minute / 60.0,
        "local_time_str": local_now.strftime("%I:%M %p"),
    }

# ── Kill signal evaluation ────────────────────────────────────────────────────

def check_kill_signals(position: dict, obs: dict, state: dict) -> list:
    """
    Returns list of triggered kill signals not yet alerted.
    Each item: {"signal": {...}, "legs": [...], "detail": "human readable"}
    """
    if not obs:
        return []

    triggered = []
    pos_id = position["id"]
    alerted = state["alerted"].get(pos_id, set())
    if isinstance(alerted, list):
        alerted = set(alerted)

    # Track peak temp for this station
    station = position["station"]
    if obs.get("peak_f") is not None:
        prev_peak = state["peak_temps"].get(station, 0)
        state["peak_temps"][station] = max(prev_peak, obs["peak_f"])

    for sig in position.get("kill_signals", []):
        sig_key = sig["type"] + "_" + sig.get("message", "")[:30]
        if sig_key in alerted:
            continue  # already sent this alert

        stype = sig["type"]
        detail = None

        if stype == "precip":
            codes = set(sig.get("codes", ["RA", "SHRA", "TSRA"]))
            found = obs["precip_codes"] & codes
            if found:
                detail = f"Precip detected: {', '.join(found)} at {obs['local_time_str']}"

        elif stype == "temp_drop":
            drop_f = sig.get("drop_f", 3.0)
            cutoff = sig.get("before_hour_local", 14)
            peak = state["peak_temps"].get(station)
            cur = obs.get("current_f")
            if peak and cur and obs["local_hour"] < cutoff:
                actual_drop = peak - cur
                if actual_drop >= drop_f:
                    detail = f"Temp dropped {round(actual_drop,1)}F from peak {peak}F → now {cur}F before {cutoff}:00 local"

        elif stype == "temp_above":
            threshold = sig.get("threshold_f")
            cur = obs.get("current_f")
            peak = obs.get("peak_f")
            check_val = peak if peak else cur
            if threshold and check_val and check_val >= threshold:
                detail = f"Temp {check_val}F exceeded threshold {threshold}F — position out of range"

        elif stype == "temp_below":
            threshold = sig.get("threshold_f")
            low = obs.get("day_low_f")
            if threshold and low and low <= threshold:
                detail = f"Day low {low}F at or below threshold {threshold}F"

        elif stype == "wind_shift":
            target_dirs = sig.get("directions", [])  # e.g. [0, 45] for N/NE
            tolerance = sig.get("tolerance_deg", 45)
            wind_dirs = obs.get("wind_dirs", [])
            if wind_dirs and target_dirs:
                latest_wind = wind_dirs[0]
                for td in target_dirs:
                    diff = abs(latest_wind - td)
                    if diff > 180:
                        diff = 360 - diff
                    if diff <= tolerance:
                        detail = f"Wind shifted to {latest_wind}° (watching for {td}°)"
                        break

        if detail:
            triggered.append({
                "signal": sig,
                "sig_key": sig_key,
                "legs": sig.get("affects_legs", [leg["label"] for leg in position["legs"]]),
                "detail": detail,
                "message": sig.get("message", ""),
            })

    return triggered

# ── Discord alert ─────────────────────────────────────────────────────────────

def send_discord_alert(position: dict, triggered: list, obs: dict):
    if not DISCORD_WEBHOOK:
        return
    lines = []
    lines.append(f"⚠️ **{position['city']} — Position Alert**")
    lines.append(f"*{obs.get('local_time_str', '')} local | {position.get('notes', '')}*")
    lines.append("─────────────────────────────────")

    for t in triggered:
        legs_str = ", ".join(t["legs"])
        lines.append(f"🚨 **{t['message']}**")
        lines.append(f"   Detail: {t['detail']}")
        lines.append(f"   Affected legs: {legs_str}")
        lines.append(f"   → Consider exiting: {legs_str}")

    lines.append("─────────────────────────────────")
    cur = obs.get("current_f")
    peak = obs.get("peak_f")
    low = obs.get("day_low_f")
    lines.append(f"📊 Current: {cur}F | Peak: {peak}F | Low: {low}F")
    for leg in position["legs"]:
        lines.append(f"   {leg['label']} — ticker: `{leg['ticker']}`")

    payload = {"content": "\n".join(lines)}
    try:
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        print(f"  Discord alert sent: {resp.status_code}")
    except Exception as e:
        print(f"  Discord alert failed: {e}")

def send_status_update(positions: list, obs_cache: dict, state: dict):
    """Send a clean status update every 30 minutes for active positions."""
    if not DISCORD_WEBHOOK or not positions:
        return
    lines = []
    now_et = datetime.now(timezone.utc) + timedelta(hours=-5)
    lines.append(f"📋 **Position Status — {now_et.strftime('%I:%M %p')} ET**")
    lines.append("─────────────────────────────────")
    for pos in positions:
        obs = obs_cache.get(pos["station"], {})
        cur = obs.get("current_f", "?")
        peak = obs.get("peak_f", "?")
        low = obs.get("day_low_f", "?")
        legs_str = " | ".join(l["label"] for l in pos["legs"])
        lines.append(f"✅ **{pos['city']}** — {legs_str}")
        lines.append(f"   Now: {cur}F | Peak: {peak}F | Low: {low}F | No alerts")
    payload = {"content": "\n".join(lines)}
    try:
        requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
    except Exception:
        pass

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(WATCHLIST_PATH):
        print("No watchlist.json found — nothing to monitor")
        return

    with open(WATCHLIST_PATH) as f:
        watchlist = json.load(f)

    positions = [p for p in watchlist.get("positions", []) if p.get("active", True)]
    if not positions:
        print("No active positions in watchlist")
        return

    state = load_state()
    obs_cache = {}
    any_alerted = False

    for pos in positions:
        station = pos["station"]
        city_tz = pos.get("city_tz", "America/New_York")
        print(f"Checking {pos['city']} ({station})...")

        obs = get_current_obs(station, city_tz)
        obs_cache[station] = obs

        triggered = check_kill_signals(pos, obs, state)

        if triggered:
            any_alerted = True
            send_discord_alert(pos, triggered, obs)
            # Mark as alerted so we don't spam
            pos_id = pos["id"]
            if pos_id not in state["alerted"]:
                state["alerted"][pos_id] = []
            alerted_set = set(state["alerted"][pos_id])
            for t in triggered:
                alerted_set.add(t["sig_key"])
            state["alerted"][pos_id] = list(alerted_set)
            print(f"  ALERT sent for {pos['city']}: {[t['message'] for t in triggered]}")
        else:
            cur = obs.get("current_f")
            peak = obs.get("peak_f")
            print(f"  {pos['city']}: cur={cur}F peak={peak}F — no kill signals triggered")

    # Send 30-minute status update if no alerts fired
    now_min = datetime.now(timezone.utc).minute
    if not any_alerted and now_min in (0, 30):
        send_status_update(positions, obs_cache, state)

    save_state(state)

if __name__ == "__main__":
    main()
