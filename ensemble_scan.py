import requests, time, base64, re
from collections import Counter
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

CITIES = {
    "Washington DC":  (38.8512, -77.0402, "KDCA",  "America/New_York"),
    "Atlanta":        (33.6407, -84.4277, "KATL",  "America/New_York"),
    "Austin":         (30.1945, -97.6699, "KAUS",  "America/Chicago"),
    "Boston":         (42.3656, -71.0096, "KBOS",  "America/New_York"),
    "Chicago":        (41.7868, -87.7522, "KMDW",  "America/Chicago"),
    "Denver":         (39.8561, -104.6737,"KDEN",  "America/Denver"),
    "Las Vegas":      (36.0840, -115.1537,"KLAS",  "America/Los_Angeles"),
    "Los Angeles":    (33.9416, -118.4085,"KLAX",  "America/Los_Angeles"),
    "Miami":          (25.7959, -80.2870, "KMIA",  "America/New_York"),
    "Philadelphia":   (39.8744, -75.2424, "KPHL",  "America/New_York"),
    "Seattle":        (47.4489, -122.3094,"KSEA",  "America/Los_Angeles"),
    "Oklahoma City":  (35.3931, -97.6007, "KOKC",  "America/Chicago"),
    "San Francisco":  (37.6190, -122.3748,"KSFO",  "America/Los_Angeles"),
    "Houston":        (29.6454, -95.2789, "KHOU",  "America/Chicago"),
    "Dallas":         (32.8998, -97.0403, "KDFW",  "America/Chicago"),
    "Phoenix":        (33.4352, -112.0101,"KPHX",  "America/Phoenix"),
    "New Orleans":    (29.9934, -90.2580, "KMSY",  "America/Chicago"),
    "Minneapolis":    (44.8848, -93.2223, "KMSP",  "America/Chicago"),
    "San Antonio":    (29.5337, -98.4698, "KSAT",  "America/Chicago"),
    "New York City":  (40.7789, -73.9692, "KNYC",  "America/New_York"),
}

SPRING_BIAS = {
    "Washington DC": (-0.55, -1.53), "Atlanta": (-0.8, -1.5), "Austin": (-0.4, -0.9),
    "Boston": (0.3, -0.2), "Chicago": (-1.0, -1.2), "Denver": (-1.2, -1.0),
    "Las Vegas": (-0.5, -0.5), "Los Angeles": (0.1, 3.24), "Miami": (0.0, -0.8),
    "Philadelphia": (-0.1, -0.5), "Seattle": (-1.2, -1.8), "Oklahoma City": (-0.2, -0.4),
    "San Francisco": (1.48, 0.9), "Houston": (-0.4, -0.6), "Dallas": (-0.2, -0.3),
    "Phoenix": (-1.0, -2.0), "New Orleans": (-0.6, -0.7), "Minneapolis": (-0.7, -0.8),
    "San Antonio": (-0.3, -0.8), "New York City": (0.2, -0.7),
}

SERIES = {
    "Washington DC":  ("KXHIGHTDC",   "KXLOWTDC"),
    "Atlanta":        ("KXHIGHTATL",  "KXLOWTATL"),
    "Austin":         ("KXHIGHAUS",   "KXLOWTAUS"),
    "Boston":         ("KXHIGHTBOS",  "KXLOWTBOS"),
    "Chicago":        ("KXHIGHTCHI",  "KXLOWTCHI"),
    "Denver":         ("KXHIGHTDEN",  "KXLOWTDEN"),
    "Las Vegas":      ("KXHIGHTLAS",  "KXLOWTLAS"),
    "Los Angeles":    ("KXHIGHLAX",   "KXLOWTLAX"),
    "Miami":          ("KXHIGHTMIA",  None),
    "Philadelphia":   ("KXHIGHTPHL",  "KXLOWTPHL"),
    "Seattle":        ("KXHIGHTSEA",  "KXLOWTSEA"),
    "Oklahoma City":  ("KXHIGHTOKC",  None),
    "San Francisco":  ("KXHIGHTSFO",  "KXLOWTSFO"),
    "Houston":        ("KXHIGHTHOU",  None),
    "Dallas":         ("KXHIGHTDFW",  None),
    "Phoenix":        ("KXHIGHTPHX",  None),
    "New Orleans":    ("KXHIGHTNOLA", None),
    "Minneapolis":    ("KXHIGHTMIN",  "KXLOWTMIN"),
    "San Antonio":    ("KXHIGHTSAT",  None),
    "New York City":  ("KXHIGHTNYC",  "KXLOWTNYC"),
}

env = {}
with open("/opt/kalshi-weather-bot/.env") as f:
    for line in f:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()

with open(env.get("KALSHI_PRIVATE_KEY_PATH",""), "rb") as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)
key_id = env.get("KALSHI_API_KEY_ID","")
BASE = "https://api.elections.kalshi.com/trade-api/v2"

def kalshi_get(path, params=None):
    ts_ms = str(int(time.time() * 1000))
    msg = f"{ts_ms}GET{path}"
    sig = private_key.sign(msg.encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    h = {"KALSHI-ACCESS-KEY": key_id, "KALSHI-ACCESS-TIMESTAMP": ts_ms, "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}
    return requests.get(BASE + path, headers=h, params=params, timeout=15).json()

def get_ensemble(lat, lon, tz, var):
    members = []
    for model in ["gfs_seamless", "icon_seamless"]:
        try:
            r = requests.get(
                "https://ensemble-api.open-meteo.com/v1/ensemble",
                params={"latitude": lat, "longitude": lon, "daily": var,
                        "temperature_unit": "fahrenheit", "timezone": tz,
                        "forecast_days": 1, "models": model},
                timeout=20
            )
            d = r.json().get("daily", {})
            for k, v in d.items():
                if var in k and v[0] is not None:
                    members.append((model, v[0]))
        except Exception:
            pass
    return members

# Kalshi settles on city LOCAL STANDARD TIME (fixed, DST-safe)
# Always use standard time offsets regardless of DST
TZ_OFFSETS = {
    "America/New_York":    -5,  # EST
    "America/Chicago":     -6,  # CST
    "America/Denver":      -7,  # MST
    "America/Los_Angeles": -8,  # PST
    "America/Phoenix":     -7,  # MST (no DST, same year-round)
}

def get_obs_day_range(station, city_tz):
    """Return (obs_low, obs_high, current_temp) recorded since local midnight, timestamps shown in ET."""
    from datetime import datetime, timezone, timedelta
    now_utc = datetime.now(timezone.utc)
    offset_h = TZ_OFFSETS.get(city_tz, -4)
    # Local midnight in UTC
    local_now = now_utc + timedelta(hours=offset_h)
    local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    utc_midnight = local_midnight - timedelta(hours=offset_h)

    try:
        start_str = utc_midnight.strftime("%Y-%m-%dT%H:%M:%SZ")
        r = requests.get(
            f"https://api.weather.gov/stations/{station}/observations",
            headers={"User-Agent": "kalshi-weather-bot/1.0"},
            params={"start": start_str, "limit": 500},
            timeout=15
        )
        features = r.json().get("features", [])
        temps = []
        latest = None
        for f in features:
            ts_str = f["properties"].get("timestamp","")
            if not ts_str:
                continue
            # Parse timestamp
            ts_utc = datetime.fromisoformat(ts_str.replace("Z","+00:00"))
            if ts_utc < utc_midnight:
                continue
            temp_c = f["properties"].get("temperature", {}).get("value")
            if temp_c is not None:
                temp_f = round(temp_c * 9/5 + 32, 1)
                temps.append(temp_f)
                if latest is None:
                    latest = temp_f  # features are newest-first
        if temps:
            return min(temps), max(temps), latest
    except Exception:
        pass
    return None, None, None


def get_kalshi_prices(series_ticker):
    try:
        mkts = kalshi_get("/markets", {"series_ticker": series_ticker, "limit": 100, "status": "open"}).get("markets", [])
        from datetime import datetime, timezone, timedelta
        et_today = datetime.now(timezone.utc) + timedelta(hours=-4)
        today_label = et_today.strftime("%b ") + str(et_today.day)  # e.g. "Apr 6"
        today_mkts = [m for m in mkts if today_label in m.get("title","")]
        prices = {}
        for m in today_mkts:
            ticker = m.get("ticker","")
            title = m.get("title","")
            ob = kalshi_get(f"/markets/{ticker}/orderbook").get("orderbook_fp", {})
            yes_bids = ob.get("yes_dollars", [])
            no_bids  = ob.get("no_dollars", [])
            yes_bid = round(max((float(p) for p, _ in yes_bids), default=0) * 100) if yes_bids else None
            yes_ask = round((1 - max((float(p) for p, _ in no_bids), default=1)) * 100) if no_bids else None
            no_ask = (100 - yes_bid) if yes_bid else None

            # Standard range bucket: "42-43"
            rng = re.search(r"(\d+)-(\d+)", title)
            if rng:
                prices[ticker] = {"title": title, "yes_bid": yes_bid, "ask": yes_ask, "no_ask": no_ask,
                                  "lo": int(rng.group(1)), "hi": int(rng.group(2)), "tail": None}
                continue
            # Lower tail: "<42" — YES wins if temp < threshold
            lt = re.search(r"<\s*(\d+)", title)
            if lt:
                thr = int(lt.group(1))  # threshold (exclusive upper bound)
                prices[ticker] = {"title": title, "yes_bid": yes_bid, "ask": yes_ask, "no_ask": no_ask,
                                  "lo": 0, "hi": thr - 1, "tail": "lt", "threshold": thr}
                continue
            # Upper tail: ">49" — YES wins if temp > threshold
            gt = re.search(r">\s*(\d+)", title)
            if gt:
                thr = int(gt.group(1))
                prices[ticker] = {"title": title, "yes_bid": yes_bid, "ask": yes_ask, "no_ask": no_ask,
                                  "lo": thr + 1, "hi": 999, "tail": "gt", "threshold": thr}
        return prices
    except Exception:
        return {}

results = []

# Cache obs per station
obs_cache = {}

for city, (lat, lon, station, tz) in CITIES.items():
    series_high, series_low = SERIES.get(city, (None, None))
    bias_gfs, bias_icon = SPRING_BIAS.get(city, (0, 0))

    if station not in obs_cache:
        obs_cache[station] = get_obs_day_range(station, tz)
    obs_low, obs_high, obs_current = obs_cache[station]

    for side, series, var in [("HIGH", series_high, "temperature_2m_max"), ("LOW", series_low, "temperature_2m_min")]:
        if series is None:
            continue
        try:
            members = get_ensemble(lat, lon, tz, var)
            if not members:
                continue

            corrected = []
            for model, temp in members:
                if "gfs" in model:
                    corrected.append(temp - bias_gfs)
                else:
                    corrected.append(temp - bias_icon)

            mean_c = round(sum(corrected) / len(corrected), 1)
            n = len(corrected)
            votes = Counter(int(t) for t in corrected)

            prices = get_kalshi_prices(series)
            if not prices:
                continue

            best_edge = None
            best_bucket = None
            best_ask = None
            best_prob = None
            best_dir = None

            for ticker, pdata in prices.items():
                yes_ask = pdata["ask"]
                no_ask = pdata["no_ask"]
                lo, hi = pdata["lo"], pdata["hi"]
                tail = pdata.get("tail")
                thr = pdata.get("threshold")
                if tail == "lt":   # <thr: count members below threshold
                    bucket_votes = sum(v for b, v in votes.items() if b < thr)
                elif tail == "gt": # >thr: count members above threshold
                    bucket_votes = sum(v for b, v in votes.items() if b > thr)
                else:
                    bucket_votes = sum(v for b, v in votes.items() if lo <= b <= hi)
                prob = round(bucket_votes / n * 100, 1)

                # YES edge: ensemble prob vs cost to buy YES
                yes_edge = (prob - yes_ask) if yes_ask and 1 < yes_ask < 99 else None
                # NO edge: ensemble prob of NOT landing here vs cost to buy NO
                no_edge = ((100 - prob) - no_ask) if no_ask and 1 < no_ask < 99 else None

                # Pick best direction
                edge = None
                direction = None
                trade_price = None
                if yes_edge is not None and (no_edge is None or yes_edge >= no_edge):
                    edge = yes_edge
                    direction = "YES"
                    trade_price = yes_ask
                elif no_edge is not None:
                    edge = no_edge
                    direction = "NO"
                    trade_price = no_ask

                # Suppress NO on LOW bucket if day low already sits inside it (contradiction)
                if direction == "NO" and side == "LOW" and obs_low is not None:
                    if tail == "lt" and obs_low < thr:
                        continue  # low already below threshold — NO is losing
                    elif tail is None and lo <= round(obs_low) <= hi:
                        continue  # low already in bucket — NO is losing
                # Suppress NO on HIGH bucket if day high already sits inside it
                if direction == "NO" and side == "HIGH" and obs_high is not None:
                    if tail == "gt" and obs_high > thr:
                        continue  # high already above threshold — NO is losing
                    elif tail is None and lo <= round(obs_high) <= hi:
                        continue  # high already in bucket — NO is losing

                if edge is not None and (best_edge is None or edge > best_edge):
                    best_edge = edge
                    best_bucket = f"{lo}-{hi}"
                    best_ask = trade_price
                    best_prob = prob
                    best_dir = direction

            if best_edge is not None and best_edge >= 10:
                # Override direction if obs already locked in the bucket
                blo, bhi = [int(x) for x in best_bucket.split("-")]
                best_buffer = None
                alt_options = []

                if side == "LOW" and obs_low is not None and blo <= round(obs_low) <= bhi:
                    # Low locked in. Find cheapest equivalent winning position:
                    # Option A: YES on current bucket (loses if temp drops below blo)
                    # Option B: NO on any bucket whose hi < blo (loses if temp drops further into that bucket)
                    # Option C: NO on <thr tail where thr <= blo (loses if temp drops below thr)
                    options = []
                    for ticker, pdata in prices.items():
                        lo2, hi2 = pdata["lo"], pdata["hi"]
                        tail2 = pdata.get("tail")
                        thr2 = pdata.get("threshold")
                        # YES on the locked bucket
                        if lo2 == blo and hi2 == bhi and pdata["ask"] and 1 < pdata["ask"] < 99:
                            buf = round(obs_low - blo, 1)  # drop needed to lose YES
                            options.append({"dir": "YES", "bucket": f"{lo2}-{hi2}",
                                            "cost": pdata["ask"], "buffer": buf, "ticker": ticker})
                        # NO on <thr tail where thr <= blo (NO loses when low < thr)
                        elif tail2 == "lt" and thr2 <= blo and pdata["no_ask"] and 1 < pdata["no_ask"] < 99:
                            buf = round(obs_low - thr2, 1)  # drop needed to go below thr and lose
                            label = f"<{thr2}"
                            options.append({"dir": "NO", "bucket": label,
                                            "cost": pdata["no_ask"], "buffer": buf, "ticker": ticker})
                        # NO on any standard bucket entirely below current bucket
                        elif tail2 is None and hi2 < blo and pdata["no_ask"] and 1 < pdata["no_ask"] < 99:
                            buf = round(obs_low - hi2, 1)
                            options.append({"dir": "NO", "bucket": f"{lo2}-{hi2}",
                                            "cost": pdata["no_ask"], "buffer": buf, "ticker": ticker})
                    if not options:
                        continue
                    # Pick: minimum cost, tiebreak by maximum buffer
                    best_opt = min(options, key=lambda x: (x["cost"], -x["buffer"]))
                    best_dir = best_opt["dir"]
                    best_ask = best_opt["cost"]
                    best_bucket = best_opt["bucket"]
                    best_buffer = best_opt["buffer"]
                    # Also keep runner-up for display
                    options.sort(key=lambda x: (x["cost"], -x["buffer"]))
                    alt_options = [o for o in options if o["bucket"] != best_opt["bucket"]][:2]

                elif side == "HIGH" and obs_high is not None and blo <= round(obs_high) <= bhi and obs_current is not None and obs_current < obs_high:
                    # High locked in. Find cheapest equivalent winning position:
                    # Option A: YES on current bucket (loses if temp rises above bhi)
                    # Option B: NO on any bucket whose lo > bhi (loses if temp rises into that bucket)
                    # Option C: NO on >thr tail where thr >= bhi (loses if temp rises above thr)
                    options = []
                    for ticker, pdata in prices.items():
                        lo2, hi2 = pdata["lo"], pdata["hi"]
                        tail2 = pdata.get("tail")
                        thr2 = pdata.get("threshold")
                        # YES on the locked bucket
                        if lo2 == blo and hi2 == bhi and pdata["ask"] and 1 < pdata["ask"] < 99:
                            buf = round(bhi - obs_high, 1)
                            options.append({"dir": "YES", "bucket": f"{lo2}-{hi2}",
                                            "cost": pdata["ask"], "buffer": buf, "ticker": ticker})
                        # NO on >thr tail where thr >= bhi (NO loses when high > thr)
                        elif tail2 == "gt" and thr2 >= bhi and pdata["no_ask"] and 1 < pdata["no_ask"] < 99:
                            buf = round(thr2 - obs_high, 1)
                            label = f">{thr2}"
                            options.append({"dir": "NO", "bucket": label,
                                            "cost": pdata["no_ask"], "buffer": buf, "ticker": ticker})
                        # NO on any standard bucket entirely above current high
                        elif tail2 is None and lo2 > bhi and pdata["no_ask"] and 1 < pdata["no_ask"] < 99:
                            buf = round(lo2 - obs_high, 1)
                            options.append({"dir": "NO", "bucket": f"{lo2}-{hi2}",
                                            "cost": pdata["no_ask"], "buffer": buf, "ticker": ticker})
                    if not options:
                        continue
                    best_opt = min(options, key=lambda x: (x["cost"], -x["buffer"]))
                    best_dir = best_opt["dir"]
                    best_ask = best_opt["cost"]
                    best_bucket = best_opt["bucket"]
                    best_buffer = best_opt["buffer"]
                    options.sort(key=lambda x: (x["cost"], -x["buffer"]))
                    alt_options = [o for o in options if o["bucket"] != best_opt["bucket"]][:2]
                else:
                    alt_options = []

                if best_ask is None:
                    continue

                results.append({
                    "city": city, "side": side,
                    "bucket": best_bucket, "ask": best_ask,
                    "ens_prob": best_prob, "edge": best_edge,
                    "direction": best_dir, "mean": mean_c, "n": n,
                    "obs_low": obs_low, "obs_high": obs_high, "obs_current": obs_current,
                    "buffer": best_buffer, "alt_options": alt_options,
                })
                print(f"  done: {city} {side} edge={best_edge:+.1f}% low={obs_low} high={obs_high} cur={obs_current}")
        except Exception as e:
            print(f"  error: {city} {side}: {e}")

results.sort(key=lambda x: abs(x["edge"]), reverse=True)

print("")
print("=" * 72)
from datetime import datetime, timezone, timedelta
et_now = datetime.now(timezone.utc) + timedelta(hours=-4)
print(f"BARO — {et_now.strftime('%B %d, %Y').replace(' 0',' ')}  (bias-corrected GFS+ICON, min edge 10%)")
print("=" * 72)
print("")
print(f"As of {et_now.strftime('%I:%M %p')} ET")
print("")
print("City             Side  Bucket    Price  Ens%    Edge  Sig  Buffer  DayLow DayHigh  Now    Status")
print("-" * 103)
for r in results:
    bucket_str = r["bucket"]
    # Parse lo/hi safely — tail labels like "<42" or ">49" skip numeric parse
    if "-" in bucket_str and not bucket_str.startswith("<") and not bucket_str.startswith(">"):
        parts = bucket_str.split("-")
        lo, hi = int(parts[0]), int(parts[1])
    else:
        lo, hi = None, None

    obs_low = r.get("obs_low")
    obs_high = r.get("obs_high")
    obs_cur = r.get("obs_current")
    buf = r.get("buffer")

    low_str  = str(obs_low)  + "F" if obs_low  is not None else "?"
    high_str = str(obs_high) + "F" if obs_high is not None else "?"
    cur_str  = str(obs_cur)  + "F" if obs_cur  is not None else "?"
    buf_str  = (str(buf) + "F").rjust(6) if buf is not None else "      "

    if r["side"] == "LOW":
        if obs_low is not None:
            if r["direction"] in ("YES", "NO") and buf is not None:
                status = "LOCKED"
            elif lo is not None and obs_low > lo:
                status = "not cold enough yet"
            else:
                status = "too cold already"
        else:
            status = ""
    else:
        if obs_high is not None:
            if r["direction"] in ("YES", "NO") and buf is not None:
                status = "LOCKED"
            elif lo is not None and obs_high < lo:
                status = "not warm enough yet"
            elif lo is not None:
                status = "too hot already"
            else:
                status = ""
        else:
            status = ""

    print(
        r["city"].ljust(16) + " " +
        r["side"].ljust(5) + " " +
        r["direction"].ljust(3) + " " +
        r["bucket"].ljust(9) + " " +
        (str(r["ask"]) + "c").rjust(5) + "  " +
        (str(r["ens_prob"]) + "%").rjust(5) + "  " +
        ("+" + str(round(r["edge"],1)) + "%").rjust(6) + " " +
        buf_str + "  " +
        low_str.ljust(7) + high_str.ljust(8) + cur_str.ljust(7) + status
    )
    # Show alternative options for locked positions
    for alt in r.get("alt_options", []):
        print(
            " " * 26 +
            "alt: " + alt["dir"].ljust(3) + " " + alt["bucket"].ljust(9) +
            (str(alt["cost"]) + "c").rjust(5) +
            "                  " +
            ("buf=" + str(alt["buffer"]) + "F")
        )
print("")
print(f"Total actionable signals: {len(results)}")

# ── Discord alert ──────────────────────────────────────────────────────────────
DISCORD_WEBHOOK = env.get("DISCORD_WEBHOOK_URL", "")
if DISCORD_WEBHOOK and results:
    lines = []
    date_str = et_now.strftime("%b ") + str(et_now.day) + ", " + str(et_now.year)
    time_str = et_now.strftime("%I:%M %p").lstrip("0")
    lines.append(f"\U0001f321\ufe0f **Baro \u2014 {date_str}** | {time_str} ET")
    lines.append("─────────────────────────────────")

    # Tier each result
    # Tier 1: LOCKED with buffer >= 2F and cost <= 60c (high confidence, act on these)
    # Tier 2: LOCKED tight (buf < 2F) or strong ensemble (edge >= 25%, cost <= 50c)
    # Tier 3: everything else (informational only)
    tier1, tier2, tier3 = [], [], []
    for r in results:
        buf = r.get("buffer")
        edge = r["edge"]
        cost = r["ask"]
        is_locked = buf is not None
        if is_locked and buf >= 2.0 and cost <= 60:
            tier1.append(r)
        elif (is_locked and buf < 2.0) or (not is_locked and edge >= 25 and cost <= 50):
            tier2.append(r)
        else:
            tier3.append(r)

    def fmt_line(r):
        buf = r.get("buffer")
        dir_prob = r["ens_prob"] if r["direction"] == "YES" else round(100 - r["ens_prob"], 1)
        if buf is not None:
            status = f"LOCKED buf={buf}F"
        else:
            status = f"{dir_prob}% prob {r['direction']}"
        alts = r.get("alt_options", [])
        alt_str = f"  *(alt: {alts[0]['dir']} {alts[0]['bucket']} @ {alts[0]['cost']}¢)*" if alts else ""
        return (f"**{r['city']} {r['side'].upper()} {r['direction']} {r['bucket']}** | "
                f"{r['ask']}¢ | +{round(r['edge'],1)}% | {status}{alt_str}")

    if tier1:
        lines.append("\U0001f7e2 **TAKE THESE** (locked, high confidence)")
        for r in tier1:
            lines.append("  \u2705 " + fmt_line(r))
    if tier2:
        lines.append("\U0001f7e1 **CONSIDER** (tight lock or strong ensemble)")
        for r in tier2:
            lines.append("  \U0001f4ca " + fmt_line(r))
    if tier3:
        lines.append("\u26aa **SKIP / FYI** (weak edge or risky)")
        for r in tier3:
            lines.append("  \u23e9 " + fmt_line(r))

    lines.append("─────────────────────────────────")
    lines.append(f"📄 *Baro paper mode — {len(tier1)} strong | {len(tier2)} consider | {len(tier3)} skip*")

    payload = {"content": "\n".join(lines)}
    try:
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        print(f"Discord posted: {resp.status_code}")
    except Exception as e:
        print(f"Discord post failed: {e}")
