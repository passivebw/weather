import os
import time
import math
import random
import threading
import re
import glob
import base64
import csv
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from dateutil import tz
from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:
    hashes = None
    serialization = None
    padding = None


# -----------------------
# Load .env via python-dotenv (installed)
# -----------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def normalize_time_in_force(tif: str, *, default: str = "fill_or_kill") -> str:
    raw = str(tif or "").strip().lower()
    if not raw:
        return default
    aliases = {
        "fok": "fill_or_kill",
        "fill_or_kill": "fill_or_kill",
        "ioc": "immediate_or_cancel",
        "immediate_or_cancel": "immediate_or_cancel",
        "gtc": "good_till_canceled",
        "good_til_cancelled": "good_till_canceled",
        "good_till_cancelled": "good_till_canceled",
        "good_til_canceled": "good_till_canceled",
        "good_till_canceled": "good_till_canceled",
        "gtd": "good_til_date",
        "good_til_date": "good_til_date",
        "good_till_date": "good_til_date",
    }
    return aliases.get(raw, default)

def sanitize_time_in_force_for_order(
    tif: str,
    *,
    default: str = "fill_or_kill",
    allow_resting: bool = False,
) -> str:
    """
    Weather order flow currently accepts FOK/IOC reliably; sanitize unsupported TIFs.
    """
    allowed = {"fill_or_kill", "immediate_or_cancel"}
    if allow_resting:
        allowed.update({"good_till_canceled", "good_til_date"})
    safe_default = normalize_time_in_force(default, default="fill_or_kill")
    if safe_default not in allowed:
        safe_default = "fill_or_kill"
    tif_norm = normalize_time_in_force(tif, default=safe_default)
    if tif_norm in allowed:
        return tif_norm
    return safe_default


def fmt_est(dt: datetime) -> str:
    est_tz = tz.tzoffset("EST", -5 * 3600)
    return dt.astimezone(est_tz).strftime("%Y-%m-%d %I:%M:%S %p EST")


def fmt_est_short(dt: datetime) -> str:
    est_tz = tz.tzoffset("EST", -5 * 3600)
    return dt.astimezone(est_tz).strftime("%a %I:%M %p EST")

# -----------------------
# Config
# -----------------------
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
DAILY_UPDATE_DISCORD_WEBHOOK_URL = os.getenv("DAILY_UPDATE_DISCORD_WEBHOOK_URL", "").strip()
DAILY_UPDATE_DISCORD_ENABLED = env_bool("DAILY_UPDATE_DISCORD_ENABLED", default=True)
DAILY_UPDATE_EST_HOUR = int(os.getenv("DAILY_UPDATE_EST_HOUR", "8"))
DAILY_UPDATE_EST_MINUTE = int(os.getenv("DAILY_UPDATE_EST_MINUTE", "0"))
DAILY_UPDATE_TOTAL_ROI_BASELINE_DOLLARS = float(os.getenv("DAILY_UPDATE_TOTAL_ROI_BASELINE_DOLLARS", "294"))
ACCOUNT_DEPOSITS_DOLLARS = float(
    os.getenv(
        "ACCOUNT_DEPOSITS_DOLLARS",
        os.getenv("DAILY_UPDATE_TOTAL_ROI_BASELINE_DOLLARS", "294"),
    )
)
NYC_FORECAST_BRIEF_ENABLED = env_bool("NYC_FORECAST_BRIEF_ENABLED", default=True)
NYC_FORECAST_BRIEF_CITY = os.getenv("NYC_FORECAST_BRIEF_CITY", "New York City").strip() or "New York City"
_nyc_forecast_brief_temp_side_raw = os.getenv("NYC_FORECAST_BRIEF_TEMP_SIDE", "high").strip().lower()
NYC_FORECAST_BRIEF_TEMP_SIDE = _nyc_forecast_brief_temp_side_raw if _nyc_forecast_brief_temp_side_raw in {"high", "low"} else "high"
NYC_FORECAST_BRIEF_EVENING_HOUR_ET = int(os.getenv("NYC_FORECAST_BRIEF_EVENING_HOUR_ET", "20"))
NYC_FORECAST_BRIEF_MORNING_HOUR_ET = int(os.getenv("NYC_FORECAST_BRIEF_MORNING_HOUR_ET", "7"))
NYC_FORECAST_BRIEF_MINUTE_ET = int(os.getenv("NYC_FORECAST_BRIEF_MINUTE_ET", "0"))

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "120"))
SCAN_ALIGN_TO_INTERVAL = env_bool("SCAN_ALIGN_TO_INTERVAL", default=True)
SCAN_USE_SCHEDULE = env_bool("SCAN_USE_SCHEDULE", default=False)
SCAN_SCHEDULE_ANCHOR_HOUR = int(os.getenv("SCAN_SCHEDULE_ANCHOR_HOUR", "6"))
SCAN_SCHEDULE_INTERVAL_HOURS = int(os.getenv("SCAN_SCHEDULE_INTERVAL_HOURS", "6"))
SCAN_SCHEDULE_MINUTE = int(os.getenv("SCAN_SCHEDULE_MINUTE", "12"))
FAST_SCAN_ON_EDGE_ENABLED = env_bool("FAST_SCAN_ON_EDGE_ENABLED", default=True)
FAST_SCAN_INTERVAL_SECONDS = int(os.getenv("FAST_SCAN_INTERVAL_SECONDS", "60"))
FAST_SCAN_WINDOW_MINUTES = int(os.getenv("FAST_SCAN_WINDOW_MINUTES", "20"))
FAST_SCAN_EDGE_THRESHOLD_PCT = float(os.getenv("FAST_SCAN_EDGE_THRESHOLD_PCT", "10.0"))
BOARD_CACHE_TTL_SECONDS = int(os.getenv("BOARD_CACHE_TTL_SECONDS", "180"))
SNAPSHOT_LOGGING_ENABLED = env_bool("SNAPSHOT_LOGGING_ENABLED", default=True)
SNAPSHOT_LOG_DIR = os.getenv("SNAPSHOT_LOG_DIR", "logs").strip() or "logs"
EDGE_TRACKING_ENABLED = env_bool("EDGE_TRACKING_ENABLED", default=True)
BOARD_MIN_TOP_SIZE = int(os.getenv("BOARD_MIN_TOP_SIZE", "10"))
BOARD_MAX_SPREAD_CENTS = int(os.getenv("BOARD_MAX_SPREAD_CENTS", "12"))
BOARD_MIN_BUCKET_COUNT = int(os.getenv("BOARD_MIN_BUCKET_COUNT", "4"))
BOARD_MIN_TOP_SIZE_LOW = int(os.getenv("BOARD_MIN_TOP_SIZE_LOW", str(BOARD_MIN_TOP_SIZE)))
BOARD_MAX_SPREAD_CENTS_LOW = int(os.getenv("BOARD_MAX_SPREAD_CENTS_LOW", str(BOARD_MAX_SPREAD_CENTS)))
BOARD_MIN_BUCKET_COUNT_LOW = int(os.getenv("BOARD_MIN_BUCKET_COUNT_LOW", str(BOARD_MIN_BUCKET_COUNT)))
NO_TRADE_IMPLIED_PROB_MIN = float(os.getenv("NO_TRADE_IMPLIED_PROB_MIN", "0.08"))
NO_TRADE_IMPLIED_PROB_MAX = float(os.getenv("NO_TRADE_IMPLIED_PROB_MAX", "0.92"))
LOW_SIGNALS_ENABLED = env_bool("LOW_SIGNALS_ENABLED", default=False)
CALIBRATION_ENABLED = env_bool("CALIBRATION_ENABLED", default=True)
CALIBRATION_MIN_SAMPLES = int(os.getenv("CALIBRATION_MIN_SAMPLES", "20"))
EV_SLIPPAGE_PCT = float(os.getenv("EV_SLIPPAGE_PCT", "1.0"))
MODEL_WIN_PROB_FLOOR = float(os.getenv("MODEL_WIN_PROB_FLOOR", "0.05"))
MODEL_WIN_PROB_CEIL = float(os.getenv("MODEL_WIN_PROB_CEIL", "0.95"))
POLICY_MIN_NET_EDGE_PCT = float(os.getenv("POLICY_MIN_NET_EDGE_PCT", "10.0"))
LIVE_LOCKED_OUTCOME_CAPTURE_ENABLED = env_bool("LIVE_LOCKED_OUTCOME_CAPTURE_ENABLED", default=True)
LIVE_LOCKED_OUTCOME_MIN_NET_EDGE_PCT = float(os.getenv("LIVE_LOCKED_OUTCOME_MIN_NET_EDGE_PCT", "8.0"))
LIVE_LOCKED_OUTCOME_MAX_SPREAD_CENTS = int(os.getenv("LIVE_LOCKED_OUTCOME_MAX_SPREAD_CENTS", "6"))
LIVE_LOCKED_OUTCOME_MIN_TOP_SIZE = int(os.getenv("LIVE_LOCKED_OUTCOME_MIN_TOP_SIZE", "20"))
LIVE_LOCKED_OUTCOME_MAX_OBS_AGE_MINUTES = float(os.getenv("LIVE_LOCKED_OUTCOME_MAX_OBS_AGE_MINUTES", "20.0"))
LIVE_LOCKED_OUTCOME_MAX_UNITS = float(os.getenv("LIVE_LOCKED_OUTCOME_MAX_UNITS", "1.0"))
UNIT_SIZE_DOLLARS = float(os.getenv("UNIT_SIZE_DOLLARS", "50.0"))
PAPER_TRADE_DISCORD_ENABLED = env_bool("PAPER_TRADE_DISCORD_ENABLED", default=True)
DISCORD_TRADE_ALERTS_ENABLED = env_bool("DISCORD_TRADE_ALERTS_ENABLED", default=False)
PAPER_TRADE_POST_TOP_N = int(os.getenv("PAPER_TRADE_POST_TOP_N", "3"))
PAPER_TRADE_MAX_ALERTS_PER_MARKET_PER_DAY = int(os.getenv("PAPER_TRADE_MAX_ALERTS_PER_MARKET_PER_DAY", "2"))
PAPER_TRADE_MAX_ALERTS_PER_CITY_SIDE_PER_DAY = int(os.getenv("PAPER_TRADE_MAX_ALERTS_PER_CITY_SIDE_PER_DAY", "2"))
PAPER_TRADE_MIN_EDGE_IMPROVEMENT_PCT = float(os.getenv("PAPER_TRADE_MIN_EDGE_IMPROVEMENT_PCT", "3.0"))
PAPER_TRADE_MIN_MINUTES_BETWEEN_RE_ALERTS = int(os.getenv("PAPER_TRADE_MIN_MINUTES_BETWEEN_RE_ALERTS", "90"))
DISCORD_LEADERBOARD_ENABLED = env_bool("DISCORD_LEADERBOARD_ENABLED", default=True)
DISCORD_DISCREPANCY_ENABLED = env_bool("DISCORD_DISCREPANCY_ENABLED", default=True)
LIVE_TRADING_ENABLED = env_bool("LIVE_TRADING_ENABLED", default=False)
LIVE_KILL_SWITCH = env_bool("LIVE_KILL_SWITCH", default=False)
MANUAL_MARKET_BLOCK_ENABLED = env_bool("MANUAL_MARKET_BLOCK_ENABLED", default=True)
MANUAL_AUTO_SYNC_ENABLED = env_bool("MANUAL_AUTO_SYNC_ENABLED", default=True)
MANUAL_AUTO_SYNC_INTERVAL_MINUTES = int(os.getenv("MANUAL_AUTO_SYNC_INTERVAL_MINUTES", "30"))
LIVE_MAX_ORDERS_PER_SCAN = int(os.getenv("LIVE_MAX_ORDERS_PER_SCAN", "3"))
LIVE_MAX_ORDERS_PER_DAY = int(os.getenv("LIVE_MAX_ORDERS_PER_DAY", "25"))
LIVE_MAX_ORDERS_PER_MARKET_PER_DAY = int(os.getenv("LIVE_MAX_ORDERS_PER_MARKET_PER_DAY", "1"))
LIVE_MAX_ORDERS_PER_CITY_SIDE_PER_DAY = int(os.getenv("LIVE_MAX_ORDERS_PER_CITY_SIDE_PER_DAY", "2"))
LIVE_ORDER_FILL_MODE = os.getenv("LIVE_ORDER_FILL_MODE", "one_cent_worse").strip().lower()
LIVE_ORDER_TIME_IN_FORCE = sanitize_time_in_force_for_order(
    os.getenv("LIVE_ORDER_TIME_IN_FORCE", "fill_or_kill"),
    default="fill_or_kill",
)
LIVE_ORDER_EXPIRATION_SECONDS = int(os.getenv("LIVE_ORDER_EXPIRATION_SECONDS", "30"))
LIVE_MAX_CONTRACTS_PER_ORDER = int(os.getenv("LIVE_MAX_CONTRACTS_PER_ORDER", "10"))
LIVE_MIN_STAKE_DOLLARS = float(os.getenv("LIVE_MIN_STAKE_DOLLARS", "0.5"))
LIVE_MAX_OPEN_BOT_EXPOSURE_DOLLARS = float(os.getenv("LIVE_MAX_OPEN_BOT_EXPOSURE_DOLLARS", "100.0"))
LIVE_EDGE_IMMEDIATE_AGGRESSIVE_PCT = float(os.getenv("LIVE_EDGE_IMMEDIATE_AGGRESSIVE_PCT", "30.0"))
LIVE_EDGE_PASSIVE_THEN_AGGR_PCT = float(os.getenv("LIVE_EDGE_PASSIVE_THEN_AGGR_PCT", "12.0"))
LIVE_AGGRESSIVE_OVERRIDE_EDGE_PCT = float(os.getenv("LIVE_AGGRESSIVE_OVERRIDE_EDGE_PCT", "50.0"))
LIVE_PASSIVE_WAIT_SECONDS_MID = int(os.getenv("LIVE_PASSIVE_WAIT_SECONDS_MID", "20"))
LIVE_PASSIVE_WAIT_SECONDS_LOW = int(os.getenv("LIVE_PASSIVE_WAIT_SECONDS_LOW", "45"))
LIVE_PASSIVE_ALLOW_RESTING_LIMITS = env_bool("LIVE_PASSIVE_ALLOW_RESTING_LIMITS", default=False)
LIVE_PASSIVE_RESCAN_MODE_ENABLED = env_bool("LIVE_PASSIVE_RESCAN_MODE_ENABLED", default=True)
LIVE_PASSIVE_RESCAN_SECONDS = int(os.getenv("LIVE_PASSIVE_RESCAN_SECONDS", "60"))
LIVE_PASSIVE_ONE_TICK_FROM_ASK = env_bool("LIVE_PASSIVE_ONE_TICK_FROM_ASK", default=True)
LIVE_PASSIVE_TIME_IN_FORCE = sanitize_time_in_force_for_order(
    os.getenv("LIVE_PASSIVE_TIME_IN_FORCE", "fill_or_kill"),
    default=("good_till_canceled" if LIVE_PASSIVE_ALLOW_RESTING_LIMITS else LIVE_ORDER_TIME_IN_FORCE),
    allow_resting=LIVE_PASSIVE_ALLOW_RESTING_LIMITS,
)
LIVE_PASSIVE_REPRICE_STEP_CENTS = int(os.getenv("LIVE_PASSIVE_REPRICE_STEP_CENTS", "1"))
LIVE_PASSIVE_REPRICE_STEPS_MID = int(os.getenv("LIVE_PASSIVE_REPRICE_STEPS_MID", "2"))
LIVE_PASSIVE_REPRICE_STEPS_LOW = int(os.getenv("LIVE_PASSIVE_REPRICE_STEPS_LOW", "2"))
LIVE_ALWAYS_PASSIVE_FIRST = env_bool("LIVE_ALWAYS_PASSIVE_FIRST", default=True)
LIVE_AGGRESSIVE_MAX_SPREAD_CENTS = int(os.getenv("LIVE_AGGRESSIVE_MAX_SPREAD_CENTS", "8"))
LIVE_REQUIRE_CANCEL_BEFORE_AGGRESSIVE = env_bool("LIVE_REQUIRE_CANCEL_BEFORE_AGGRESSIVE", default=True)
LIVE_MID_EDGE_MAKER_ONLY = env_bool("LIVE_MID_EDGE_MAKER_ONLY", default=True)
LIVE_STABILITY_GATE_ENABLED = env_bool("LIVE_STABILITY_GATE_ENABLED", default=True)
LIVE_STABILITY_GATE_EDGE_MIN_PCT = float(os.getenv("LIVE_STABILITY_GATE_EDGE_MIN_PCT", "12.0"))
LIVE_STABILITY_GATE_EDGE_MAX_PCT = float(os.getenv("LIVE_STABILITY_GATE_EDGE_MAX_PCT", "30.0"))
LIVE_STABILITY_GATE_MIN_SCANS_MID = int(os.getenv("LIVE_STABILITY_GATE_MIN_SCANS_MID", "2"))
LIVE_STABILITY_REQUIRE_CHANGE_MID = env_bool("LIVE_STABILITY_REQUIRE_CHANGE_MID", default=True)
LIVE_EARLY_SESSION_ENABLED = env_bool("LIVE_EARLY_SESSION_ENABLED", default=True)
LIVE_EARLY_SESSION_START_HOUR_ET = int(os.getenv("LIVE_EARLY_SESSION_START_HOUR_ET", "10"))
LIVE_EARLY_SESSION_END_HOUR_ET = int(os.getenv("LIVE_EARLY_SESSION_END_HOUR_ET", "16"))
LIVE_EARLY_SESSION_MIN_EDGE_PCT = float(os.getenv("LIVE_EARLY_SESSION_MIN_EDGE_PCT", "30.0"))
LIVE_EARLY_SESSION_MIN_SCANS = int(os.getenv("LIVE_EARLY_SESSION_MIN_SCANS", "3"))
LIVE_EARLY_SESSION_SIZE_MULT = float(os.getenv("LIVE_EARLY_SESSION_SIZE_MULT", "0.5"))
LIVE_EARLY_SESSION_APPLY_TO_HIGH_ONLY = env_bool("LIVE_EARLY_SESSION_APPLY_TO_HIGH_ONLY", default=True)
LIVE_EXIT_ENABLED = env_bool("LIVE_EXIT_ENABLED", default=True)
LIVE_EXIT_MIN_HOLD_MINUTES = int(os.getenv("LIVE_EXIT_MIN_HOLD_MINUTES", "45"))
LIVE_EXIT_EDGE_SOFT_PCT = float(os.getenv("LIVE_EXIT_EDGE_SOFT_PCT", "-4.0"))
LIVE_EXIT_EDGE_HARD_PCT = float(os.getenv("LIVE_EXIT_EDGE_HARD_PCT", "-12.0"))
LIVE_EXIT_EDGE_DROP_PCT = float(os.getenv("LIVE_EXIT_EDGE_DROP_PCT", "30.0"))
LIVE_EXIT_SOFT_MAX_ENTRY_EDGE_PCT = float(os.getenv("LIVE_EXIT_SOFT_MAX_ENTRY_EDGE_PCT", "30.0"))
LIVE_EXIT_CONSECUTIVE_SCANS = int(os.getenv("LIVE_EXIT_CONSECUTIVE_SCANS", "3"))
LIVE_EXIT_CONSECUTIVE_MINUTES = float(os.getenv("LIVE_EXIT_CONSECUTIVE_MINUTES", "15.0"))
LIVE_EXIT_HYSTERESIS_ENABLED = env_bool("LIVE_EXIT_HYSTERESIS_ENABLED", default=True)
LIVE_EXIT_HYSTERESIS_MIN_DROP_PCT_POINTS = float(os.getenv("LIVE_EXIT_HYSTERESIS_MIN_DROP_PCT_POINTS", "8.0"))
LIVE_EXIT_HOLD_TO_SETTLE_ENABLED = env_bool("LIVE_EXIT_HOLD_TO_SETTLE_ENABLED", default=True)
LIVE_EXIT_HOLD_TO_SETTLE_HOURS_BEFORE_CLOSE = float(os.getenv("LIVE_EXIT_HOLD_TO_SETTLE_HOURS_BEFORE_CLOSE", "6.0"))
LIVE_EXIT_HOLD_TO_SETTLE_MODEL_YES_INVALIDATION_PCT = float(os.getenv("LIVE_EXIT_HOLD_TO_SETTLE_MODEL_YES_INVALIDATION_PCT", "97.0"))
LIVE_EXIT_HOLD_TO_SETTLE_EDGE_INVALIDATION_PCT = float(os.getenv("LIVE_EXIT_HOLD_TO_SETTLE_EDGE_INVALIDATION_PCT", "-35.0"))
LIVE_EXIT_MAX_ORDERS_PER_SCAN = int(os.getenv("LIVE_EXIT_MAX_ORDERS_PER_SCAN", "4"))
LIVE_EXIT_PASSIVE_WAIT_SECONDS = int(os.getenv("LIVE_EXIT_PASSIVE_WAIT_SECONDS", "90"))
LIVE_EXIT_PASSIVE_REPRICE_STEP_CENTS = int(os.getenv("LIVE_EXIT_PASSIVE_REPRICE_STEP_CENTS", "1"))
LIVE_EXIT_PASSIVE_REPRICE_STEPS = int(os.getenv("LIVE_EXIT_PASSIVE_REPRICE_STEPS", "2"))
LIVE_EXIT_PASSIVE_TIME_IN_FORCE = sanitize_time_in_force_for_order(
    os.getenv("LIVE_EXIT_PASSIVE_TIME_IN_FORCE", "fill_or_kill"),
    default="fill_or_kill",
)
LIVE_EXIT_REQUIRE_CANCEL_BEFORE_AGGRESSIVE = env_bool("LIVE_EXIT_REQUIRE_CANCEL_BEFORE_AGGRESSIVE", default=True)
LIVE_EXIT_AGGRESSIVE_FALLBACK_ENABLED = env_bool("LIVE_EXIT_AGGRESSIVE_FALLBACK_ENABLED", default=True)
LIVE_EXIT_AGGRESSIVE_TIME_IN_FORCE = sanitize_time_in_force_for_order(
    os.getenv("LIVE_EXIT_AGGRESSIVE_TIME_IN_FORCE", "fill_or_kill"),
    default="fill_or_kill",
)
LIVE_EXIT_MAX_SPREAD_CENTS = int(os.getenv("LIVE_EXIT_MAX_SPREAD_CENTS", "8"))
LIVE_EXIT_ONLY_WHEN_LOSING = env_bool("LIVE_EXIT_ONLY_WHEN_LOSING", default=True)
LIVE_EDGE_DROP_EXIT_ENABLED = env_bool("LIVE_EDGE_DROP_EXIT_ENABLED", default=True)
LIVE_EDGE_DROP_TRIGGER_PCT_POINTS = float(os.getenv("LIVE_EDGE_DROP_TRIGGER_PCT_POINTS", "25.0"))
LIVE_EDGE_DROP_SMALL_GREEN_MAX_PCT_OF_STAKE = float(os.getenv("LIVE_EDGE_DROP_SMALL_GREEN_MAX_PCT_OF_STAKE", "10.0"))
LIVE_EDGE_DROP_PARTIAL_SELL_FRACTION = float(os.getenv("LIVE_EDGE_DROP_PARTIAL_SELL_FRACTION", "0.5"))
LIVE_EDGE_DROP_AGGRESSIVE_WORSEN_PCT_POINTS = float(os.getenv("LIVE_EDGE_DROP_AGGRESSIVE_WORSEN_PCT_POINTS", "5.0"))
KELLY_SIZING_ENABLED = env_bool("KELLY_SIZING_ENABLED", default=True)
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.5"))
KELLY_BANKROLL_DOLLARS = float(os.getenv("KELLY_BANKROLL_DOLLARS", "1500"))
KELLY_MAX_BET_FRACTION_OF_BANKROLL = float(os.getenv("KELLY_MAX_BET_FRACTION_OF_BANKROLL", "0.02"))
KELLY_MIN_BET_FRACTION_OF_BANKROLL = float(os.getenv("KELLY_MIN_BET_FRACTION_OF_BANKROLL", "0.0005"))
KELLY_PRICE_BUFFER_PCT = float(os.getenv("KELLY_PRICE_BUFFER_PCT", "0.01"))
EDGE_LADDER_SIZING_ENABLED = env_bool("EDGE_LADDER_SIZING_ENABLED", default=True)
LADDER_UNIT_FRACTION_OF_BANKROLL = float(os.getenv("LADDER_UNIT_FRACTION_OF_BANKROLL", "0.02"))
LADDER_MAX_UNITS = int(os.getenv("LADDER_MAX_UNITS", "4"))
MIN_SCORE_FOR_1_UNIT = float(os.getenv("MIN_SCORE_FOR_1_UNIT", "0.05"))
MIN_SECONDS_BETWEEN_POSTS = int(os.getenv("MIN_SECONDS_BETWEEN_POSTS", "600"))
MARKET_CACHE_TTL_SECONDS = int(os.getenv("MARKET_CACHE_TTL_SECONDS", "3600"))
DISCREPANCY_ALERT_THRESHOLD = float(os.getenv("DISCREPANCY_ALERT_THRESHOLD", "0.18"))
DISCREPANCY_MEAN_TEMP_THRESHOLD_F = float(os.getenv("DISCREPANCY_MEAN_TEMP_THRESHOLD_F", "2.0"))
MIN_SECONDS_BETWEEN_DISCREPANCY_POSTS = int(os.getenv("MIN_SECONDS_BETWEEN_DISCREPANCY_POSTS", "900"))
CONSENSUS_BASE_SIGMA_F = float(os.getenv("CONSENSUS_BASE_SIGMA_F", "2.2"))
NWS_HIST_MAE_F = float(os.getenv("NWS_HIST_MAE_F", "2.0"))
NWS_LOW_HIST_MAE_F = float(os.getenv("NWS_LOW_HIST_MAE_F", str(NWS_HIST_MAE_F)))
NWS_OBS_STALE_MINUTES = int(os.getenv("NWS_OBS_STALE_MINUTES", "130"))
NWS_OBS_UPDATE_MINUTE = int(os.getenv("NWS_OBS_UPDATE_MINUTE", "51"))
NWS_OBS_HISTORY_LIMIT = int(os.getenv("NWS_OBS_HISTORY_LIMIT", "500"))
HIGH_LOCK_MARGIN_F = float(os.getenv("HIGH_LOCK_MARGIN_F", "0.0"))
LOW_LOCK_MARGIN_F = float(os.getenv("LOW_LOCK_MARGIN_F", "0.0"))
# Observation-to-settlement boundary uncertainty (F) used to soften near-threshold locks.
OBS_BOUNDARY_SIGMA_F = float(os.getenv("OBS_BOUNDARY_SIGMA_F", "0.35"))
# Keep deterministic impossible-locks only for clearly separated observations.
HIGH_HARD_LOCK_EXTRA_MARGIN_F = float(os.getenv("HIGH_HARD_LOCK_EXTRA_MARGIN_F", "1.5"))
LOW_HARD_LOCK_EXTRA_MARGIN_F = float(os.getenv("LOW_HARD_LOCK_EXTRA_MARGIN_F", "1.5"))
HIGH_EARLY_EDGE_DAMPING_MULTIPLIER = float(os.getenv("HIGH_EARLY_EDGE_DAMPING_MULTIPLIER", "0.8"))
HIGH_EARLY_DAMPING_HOUR_LST = int(os.getenv("HIGH_EARLY_DAMPING_HOUR_LST", "12"))
OPEN_METEO_HIST_MAE_F = float(os.getenv("OPEN_METEO_HIST_MAE_F", "2.4"))
OPEN_METEO_ECMWF_HIST_MAE_F = float(os.getenv("OPEN_METEO_ECMWF_HIST_MAE_F", str(OPEN_METEO_HIST_MAE_F)))
OPEN_METEO_GFS_HIST_MAE_F = float(os.getenv("OPEN_METEO_GFS_HIST_MAE_F", str(OPEN_METEO_HIST_MAE_F)))
METNO_HIST_MAE_F = float(os.getenv("METNO_HIST_MAE_F", "2.7"))
WEATHERCOM_API_KEY = os.getenv("WEATHERCOM_API_KEY", "").strip()
WEATHERCOM_HIST_MAE_F = float(os.getenv("WEATHERCOM_HIST_MAE_F", "2.1"))
ACCUWEATHER_API_KEY = os.getenv("ACCUWEATHER_API_KEY", "").strip()
ACCUWEATHER_HIST_MAE_F = float(os.getenv("ACCUWEATHER_HIST_MAE_F", "2.0"))
ENABLE_NWS_SOURCE = env_bool("ENABLE_NWS_SOURCE", default=False)
ENABLE_METNO_SOURCE = env_bool("ENABLE_METNO_SOURCE", default=True)
ENABLE_ACCUWEATHER_SOURCE = env_bool("ENABLE_ACCUWEATHER_SOURCE", default=True)
ACCUWEATHER_LOCATION_CACHE_TTL_SECONDS = int(os.getenv("ACCUWEATHER_LOCATION_CACHE_TTL_SECONDS", "2592000"))
ACCUWEATHER_FORECAST_CACHE_TTL_SECONDS = int(os.getenv("ACCUWEATHER_FORECAST_CACHE_TTL_SECONDS", "3600"))
ACCUWEATHER_STALE_FALLBACK_MAX_AGE_SECONDS = int(os.getenv("ACCUWEATHER_STALE_FALLBACK_MAX_AGE_SECONDS", "172800"))
ACCUWEATHER_LOCATION_LOOKUP_MIN_SECONDS = int(os.getenv("ACCUWEATHER_LOCATION_LOOKUP_MIN_SECONDS", "10"))
ACCUWEATHER_LOCATION_ERROR_BACKOFF_SECONDS = int(os.getenv("ACCUWEATHER_LOCATION_ERROR_BACKOFF_SECONDS", "1800"))
LIVE_PRETRADE_ACCUWEATHER_REFRESH_ENABLED = env_bool("LIVE_PRETRADE_ACCUWEATHER_REFRESH_ENABLED", default=True)
LIVE_PRETRADE_ACCUWEATHER_MAX_AGE_SECONDS = int(os.getenv("LIVE_PRETRADE_ACCUWEATHER_MAX_AGE_SECONDS", "1800"))
MANUAL_WEATHERCOM_HIGHS = os.getenv("MANUAL_WEATHERCOM_HIGHS", "").strip()
MANUAL_ACCUWEATHER_HIGHS = os.getenv("MANUAL_ACCUWEATHER_HIGHS", "").strip()

NWS_USER_AGENT = os.getenv("NWS_USER_AGENT", "kalshi-ev-bot").strip()

KALSHI_BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()
KALSHI_PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM", "").strip()
LOCAL_TZ = tz.gettz("America/New_York")

# Kalshi weather settlement uses local standard time (LST).
# We model each city on a fixed standard-time offset so DST transitions do not
# shift the contract day boundary.
CITY_STANDARD_UTC_OFFSETS = {
    "Atlanta": -5,
    "Austin": -6,
    "Boston": -5,
    "Chicago": -6,
    "Denver": -7,
    "Las Vegas": -8,
    "Los Angeles": -8,
    "Miami": -5,
    "Philadelphia": -5,
    "Seattle": -8,
    "Washington DC": -5,
    "Oklahoma City": -6,
    "San Francisco": -8,
    "Houston": -6,
    "Dallas": -6,
    "Phoenix": -7,
    "New Orleans": -6,
    "Minneapolis": -6,
    "San Antonio": -6,
    "New York City": -5,
}

# City config: real-time obs station + confidence, plus CLI code for correctness
CITY_CONFIG = {
    "Atlanta":        {"cli": "CLIATL", "station": "KATL", "confidence": 0.90, "lat": 33.6407, "lon": -84.4277},
    "Austin":         {"cli": "CLIAUS", "station": "KAUS", "confidence": 0.90, "lat": 30.1945, "lon": -97.6699},
    "Boston":         {"cli": "CLIBOS", "station": "KBOS", "confidence": 0.78, "lat": 42.3656, "lon": -71.0096},
    "Chicago":        {"cli": "CLICHI", "station": "KMDW", "confidence": 0.88, "lat": 41.7868, "lon": -87.7522},
    "Denver":         {"cli": "CLIDEN", "station": "KDEN", "confidence": 0.90, "lat": 39.8561, "lon": -104.6737},
    "Las Vegas":      {"cli": "CLILAS", "station": "KLAS", "confidence": 1.00, "lat": 36.0840, "lon": -115.1537},
    "Los Angeles":    {"cli": "CLILAX", "station": "KLAX", "confidence": 0.88, "lat": 33.9416, "lon": -118.4085},
    "Miami":          {"cli": "CLIMIA", "station": "KMIA", "confidence": 0.90, "lat": 25.7959, "lon": -80.2870},
    "Philadelphia":   {"cli": "CLIPHIL", "station": "KPHL", "confidence": 0.88, "lat": 39.8744, "lon": -75.2424},
    "Seattle":        {"cli": "CLISEA", "station": "KSEA", "confidence": 0.80, "lat": 47.4489, "lon": -122.3094},
    "Washington DC":  {"cli": "CLIDCA", "station": "KDCA", "confidence": 0.92, "lat": 38.8512, "lon": -77.0402},
    "Oklahoma City":  {"cli": "CLIOKC", "station": "KOKC", "confidence": 0.95, "lat": 35.3931, "lon": -97.6007},
    "San Francisco":  {"cli": "CLISFO", "station": "KSFO", "confidence": 0.80, "lat": 37.6190, "lon": -122.3748},
    "Houston":        {"cli": "CLIHOU", "station": "KHOU", "confidence": 0.90, "lat": 29.6454, "lon": -95.2789},
    "Dallas":         {"cli": "CLIDFW", "station": "KDFW", "confidence": 0.95, "lat": 32.8998, "lon": -97.0403},
    "Phoenix":        {"cli": "CLIPHX", "station": "KPHX", "confidence": 1.00, "lat": 33.4352, "lon": -112.0101},
    "New Orleans":    {"cli": "CLIMSY", "station": "KMSY", "confidence": 0.90, "lat": 29.9934, "lon": -90.2580},
    "Minneapolis":    {"cli": "CLIMSP", "station": "KMSP", "confidence": 0.85, "lat": 44.8848, "lon": -93.2223},
    "San Antonio":    {"cli": "CLISAT", "station": "KSAT", "confidence": 0.92, "lat": 29.5337, "lon": -98.4698},
    "New York City":  {"cli": "CLINYC", "station": "KNYC", "confidence": 0.88, "lat": 40.7789, "lon": -73.9692},
}

_last_post_ts = 0.0
_last_top_signature = ""
_last_discrepancy_post_ts = 0.0
_last_discrepancy_signature = ""
_last_daily_update_date = ""
_nyc_forecast_brief_state: Dict[str, str] = {}
_paper_alert_state_date = ""
_paper_alert_state: Dict[str, dict] = {}
_live_trade_state_date = ""
_live_trade_state: Dict[str, dict] = {}
_live_exit_state_date = ""
_live_exit_state: Dict[str, dict] = {}
_live_kill_switch_state = LIVE_KILL_SWITCH
_market_cache_lock = threading.Lock()
_accuweather_cache_lock = threading.Lock()
_kalshi_key_cache = None
_kalshi_key_lock = threading.Lock()
_accuweather_location_cache: Dict[str, Dict[str, object]] = {}
_accuweather_forecast_cache: Dict[str, Dict[str, object]] = {}
_accuweather_cache_loaded = False
_accuweather_last_success_est = ""
_accuweather_last_error = ""
_accuweather_last_error_est = ""
_accuweather_location_lookup_last_ts = 0.0
_manual_auto_sync_last_ts = 0.0


# -----------------------
# Helpers
# -----------------------
def f_from_c(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def normal_cdf(x: float, mu: float, sigma: float) -> float:
    if sigma <= 0:
        return 1.0 if x >= mu else 0.0
    z = (x - mu) / (sigma * math.sqrt(2))
    return 0.5 * (1 + math.erf(z))

def prob_between_inclusive(mu: float, sigma: float, lo: float, hi: float) -> float:
    return clamp(normal_cdf(hi + 0.5, mu, sigma) - normal_cdf(lo - 0.5, mu, sigma), 0.0, 1.0)

def safe_inverse_mae_weight(mae_f: float) -> float:
    return 1.0 / max(mae_f, 0.25)

def weighted_mean(values: List[Tuple[float, float]]) -> Optional[float]:
    if not values:
        return None
    total_w = sum(w for _, w in values)
    if total_w <= 0:
        return None
    return sum(v * w for v, w in values) / total_w

def weighted_std(values: List[Tuple[float, float]], mu: float) -> float:
    if not values:
        return 0.0
    total_w = sum(w for _, w in values)
    if total_w <= 0:
        return 0.0
    var = sum(w * (v - mu) ** 2 for v, w in values) / total_w
    return math.sqrt(max(0.0, var))

def canonical_city_name(city: str) -> Optional[str]:
    key = city.strip().lower()
    if not key:
        return None
    aliases = {
        "washington, dc": "Washington DC",
        "washington dc": "Washington DC",
        "nyc": "New York City",
        "new york": "New York City",
        "new york, ny": "New York City",
        "new york city": "New York City",
    }
    if key in aliases:
        return aliases[key]
    for known_city in CITY_CONFIG.keys():
        if known_city.lower() == key:
            return known_city
    return None

def manual_high_for_city(raw: str, city: str) -> Optional[float]:
    if not raw:
        return None
    for chunk in re.split(r"[;\n]+", raw):
        entry = chunk.strip()
        if not entry or "=" not in entry:
            continue
        left, right = entry.split("=", 1)
        parsed_city = canonical_city_name(left)
        if parsed_city != city:
            continue
        m = re.search(r"-?\d+(?:\.\d+)?", right)
        if not m:
            continue
        try:
            return float(m.group(0))
        except Exception:
            continue
    return None


# -----------------------
# Discord
# -----------------------
def discord_send(content: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        return
    r = requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=20)
    r.raise_for_status()

def discord_send_daily(content: str) -> None:
    url = DAILY_UPDATE_DISCORD_WEBHOOK_URL or DISCORD_WEBHOOK_URL
    if not url:
        return
    r = requests.post(url, json={"content": content}, timeout=20)
    r.raise_for_status()


# -----------------------
# NWS Observations
# -----------------------
def nws_get_recent_observations(station_id: str, limit: int = 200) -> List[dict]:
    headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
    url = f"https://api.weather.gov/stations/{station_id}/observations"
    r = requests.get(url, params={"limit": limit}, headers=headers, timeout=20)
    r.raise_for_status()
    return r.json().get("features", [])

def nws_get_today_temp_stats_f(
    station_id: str,
    date_tz: Optional[timezone] = None,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[datetime]]:
    # Use a larger window so early-morning lows/highs are still included later in the day.
    # date_tz is the settlement timezone basis (LST for the city); all day filtering
    # must happen in this timezone to avoid DST window drift.
    feats = nws_get_recent_observations(station_id, limit=max(200, NWS_OBS_HISTORY_LIMIT))
    obs_tz = date_tz or LOCAL_TZ
    now_local = datetime.now(tz=obs_tz)
    today_date = now_local.date()

    temps_today: List[Tuple[datetime, float]] = []
    latest_temp = None
    latest_time = None

    for f in feats:
        props = f.get("properties", {})
        ts = props.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(obs_tz)
        except Exception:
            continue

        temp_c = props.get("temperature", {}).get("value")
        if temp_c is None:
            continue
        temp_f = f_from_c(float(temp_c))

        if latest_time is None or dt > latest_time:
            latest_time = dt
            latest_temp = temp_f

        if dt.date() == today_date:
            temps_today.append((dt, temp_f))

    if not temps_today:
        return latest_temp, None, None, latest_time

    max_so_far = max(t for _, t in temps_today)
    min_so_far = min(t for _, t in temps_today)
    return latest_temp, max_so_far, min_so_far, latest_time

def nws_get_forecast_high_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
    points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    r_points = requests.get(points_url, headers=headers, timeout=20)
    r_points.raise_for_status()
    forecast_url = r_points.json().get("properties", {}).get("forecast")
    if not forecast_url:
        return None

    r_fcst = requests.get(forecast_url, headers=headers, timeout=20)
    r_fcst.raise_for_status()
    periods = r_fcst.json().get("properties", {}).get("periods", []) or []
    today = now_local.date()

    candidates: List[dict] = []
    for p in periods:
        start = p.get("startTime")
        if not start:
            continue
        try:
            dt = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(LOCAL_TZ)
        except Exception:
            continue
        if bool(p.get("isDaytime")) and dt.date() == today:
            candidates.append(p)

    if not candidates:
        for p in periods:
            if bool(p.get("isDaytime")):
                candidates.append(p)
                break

    if not candidates:
        return None

    temp = candidates[0].get("temperature")
    if temp is None:
        return None
    unit = str(candidates[0].get("temperatureUnit", "F")).upper()
    temp_f = float(temp)
    if unit == "C":
        temp_f = f_from_c(temp_f)
    return temp_f

def nws_get_forecast_low_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
    points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    r_points = requests.get(points_url, headers=headers, timeout=20)
    r_points.raise_for_status()
    forecast_url = r_points.json().get("properties", {}).get("forecast")
    if not forecast_url:
        return None

    r_fcst = requests.get(forecast_url, headers=headers, timeout=20)
    r_fcst.raise_for_status()
    periods = r_fcst.json().get("properties", {}).get("periods", []) or []
    today = now_local.date()

    candidates: List[dict] = []
    for p in periods:
        start = p.get("startTime")
        if not start:
            continue
        try:
            dt = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(LOCAL_TZ)
        except Exception:
            continue
        if (not bool(p.get("isDaytime"))) and dt.date() in (today, today + timedelta(days=1)):
            candidates.append({"period": p, "dt": dt})

    if not candidates:
        for p in periods:
            if not bool(p.get("isDaytime")):
                candidates.append({"period": p, "dt": now_local})
                break
    if not candidates:
        return None

    # Prefer the next nighttime period from now for low-temperature forecasting.
    candidates.sort(key=lambda x: x["dt"])
    pick = None
    for c in candidates:
        if c["dt"] >= now_local:
            pick = c["period"]
            break
    if pick is None:
        pick = candidates[0]["period"]

    temp = pick.get("temperature")
    if temp is None:
        return None
    unit = str(pick.get("temperatureUnit", "F")).upper()
    temp_f = float(temp)
    if unit == "C":
        temp_f = f_from_c(temp_f)
    return temp_f

def open_meteo_get_forecast_high_f(
    lat: float,
    lon: float,
    now_local: datetime,
    model: Optional[str] = None,
) -> Optional[float]:
    return open_meteo_get_forecast_temp_f(lat, lon, now_local, model=model, temp_side="high")

def open_meteo_get_forecast_low_f(
    lat: float,
    lon: float,
    now_local: datetime,
    model: Optional[str] = None,
) -> Optional[float]:
    return open_meteo_get_forecast_temp_f(lat, lon, now_local, model=model, temp_side="low")

def open_meteo_get_forecast_temp_f(
    lat: float,
    lon: float,
    now_local: datetime,
    model: Optional[str] = None,
    temp_side: str = "high",
) -> Optional[float]:
    side = normalize_temp_side(temp_side)
    daily_field = "temperature_2m_max" if side == "high" else "temperature_2m_min"
    params = {
        "latitude": f"{lat:.4f}",
        "longitude": f"{lon:.4f}",
        "daily": daily_field,
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "forecast_days": 3,
    }
    if model:
        params["models"] = model
    r = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=20)
    r.raise_for_status()
    payload = r.json()
    daily = payload.get("daily", {})
    days = daily.get("time", []) or []
    values = daily.get(daily_field, []) or []
    if not days or not values or len(days) != len(values):
        return None

    today = now_local.date().isoformat()
    for i, day in enumerate(days):
        if day == today:
            return float(values[i])
    return float(values[0]) if values else None

def metno_get_forecast_high_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    v = metno_get_forecast_temp_f(lat, lon, now_local, temp_side="high")
    return v

def metno_get_forecast_low_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    v = metno_get_forecast_temp_f(lat, lon, now_local, temp_side="low")
    return v

def metno_get_forecast_temp_f(lat: float, lon: float, now_local: datetime, temp_side: str = "high") -> Optional[float]:
    side = normalize_temp_side(temp_side)
    headers = {"User-Agent": NWS_USER_AGENT}
    params = {"lat": f"{lat:.4f}", "lon": f"{lon:.4f}"}
    r = requests.get("https://api.met.no/weatherapi/locationforecast/2.0/compact", params=params, headers=headers, timeout=20)
    r.raise_for_status()
    payload = r.json()

    timeseries = payload.get("properties", {}).get("timeseries", []) or []
    today = now_local.date()
    temps_f: List[float] = []
    for row in timeseries:
        ts = row.get("time")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(LOCAL_TZ)
        except Exception:
            continue
        if dt.date() != today:
            continue
        temp_c = row.get("data", {}).get("instant", {}).get("details", {}).get("air_temperature")
        if temp_c is None:
            continue
        temps_f.append(f_from_c(float(temp_c)))

    if not temps_f:
        return None
    return max(temps_f) if side == "high" else min(temps_f)

def weathercom_get_forecast_high_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    return weathercom_get_forecast_temp_f(lat, lon, now_local, temp_side="high")

def weathercom_get_forecast_low_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    return weathercom_get_forecast_temp_f(lat, lon, now_local, temp_side="low")

def weathercom_get_forecast_temp_f(lat: float, lon: float, now_local: datetime, temp_side: str = "high") -> Optional[float]:
    side = normalize_temp_side(temp_side)
    if not WEATHERCOM_API_KEY:
        return None
    params = {
        "geocode": f"{lat:.4f},{lon:.4f}",
        "format": "json",
        "units": "e",
        "language": "en-US",
        "apiKey": WEATHERCOM_API_KEY,
    }
    r = requests.get("https://api.weather.com/v3/wx/forecast/daily/5day", params=params, timeout=20)
    r.raise_for_status()
    payload = r.json()

    valid = payload.get("validTimeLocal", []) or []
    values = payload.get("temperatureMax", []) or payload.get("calendarDayTemperatureMax", []) or []
    if side == "low":
        values = payload.get("temperatureMin", []) or payload.get("calendarDayTemperatureMin", []) or []
    if not values:
        return None

    today = now_local.date().isoformat()
    for i, ts in enumerate(valid):
        if i >= len(values):
            break
        v = values[i]
        if v is None:
            continue
        if isinstance(ts, str) and len(ts) >= 10 and ts[:10] == today:
            return float(v)

    for v in values:
        if v is not None:
            return float(v)
    return None

def accuweather_cache_state_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "accuweather_cache_state.json")

def _load_accuweather_cache_state() -> None:
    global _accuweather_cache_loaded, _accuweather_last_success_est, _accuweather_last_error, _accuweather_last_error_est
    if _accuweather_cache_loaded:
        return
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = accuweather_cache_state_path()
    with _accuweather_cache_lock:
        if _accuweather_cache_loaded:
            return
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                loc = payload.get("location_cache", {})
                fc = payload.get("forecast_cache", {})
                if isinstance(loc, dict):
                    _accuweather_location_cache.update({str(k): v for k, v in loc.items() if isinstance(v, dict)})
                if isinstance(fc, dict):
                    _accuweather_forecast_cache.update({str(k): v for k, v in fc.items() if isinstance(v, dict)})
                _accuweather_last_success_est = str(payload.get("last_success_est", "") or "")
                _accuweather_last_error = str(payload.get("last_error", "") or "")
                _accuweather_last_error_est = str(payload.get("last_error_est", "") or "")
            except Exception:
                pass
        _accuweather_cache_loaded = True

def _save_accuweather_cache_state() -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = accuweather_cache_state_path()
    tmp = path + ".tmp"
    with _accuweather_cache_lock:
        payload = {
            "saved_ts_est": fmt_est(datetime.now(tz=LOCAL_TZ)),
            "last_success_est": _accuweather_last_success_est,
            "last_error": _accuweather_last_error,
            "last_error_est": _accuweather_last_error_est,
            "location_cache": _accuweather_location_cache,
            "forecast_cache": _accuweather_forecast_cache,
        }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
    os.replace(tmp, path)

def _record_accuweather_error(err: Exception) -> None:
    global _accuweather_last_error, _accuweather_last_error_est
    _accuweather_last_error = f"{type(err).__name__}: {str(err)}"
    _accuweather_last_error_est = fmt_est(datetime.now(tz=LOCAL_TZ))
    try:
        _save_accuweather_cache_state()
    except Exception:
        pass

def accuweather_location_key_from_latlon(lat: float, lon: float) -> Optional[str]:
    global _accuweather_location_lookup_last_ts
    _load_accuweather_cache_state()
    if not ACCUWEATHER_API_KEY:
        return None
    coord_key = f"{lat:.4f},{lon:.4f}"
    now_ts = time.time()
    with _accuweather_cache_lock:
        cached = _accuweather_location_cache.get(coord_key)
        if isinstance(cached, dict):
            age = now_ts - float(cached.get("ts", 0.0) or 0.0)
            loc_key_cached = str(cached.get("location_key", "") or "").strip()
            if loc_key_cached and age < max(60, ACCUWEATHER_LOCATION_CACHE_TTL_SECONDS):
                return loc_key_cached
            no_retry_until = float(cached.get("no_retry_until_ts", 0.0) or 0.0)
            if no_retry_until > now_ts:
                return None
        min_gap = max(1, ACCUWEATHER_LOCATION_LOOKUP_MIN_SECONDS)
        if (now_ts - _accuweather_location_lookup_last_ts) < min_gap:
            return None
        _accuweather_location_lookup_last_ts = now_ts
    params = {
        "apikey": ACCUWEATHER_API_KEY,
        "q": coord_key,
    }
    try:
        r = requests.get("https://dataservice.accuweather.com/locations/v1/cities/geoposition/search", params=params, timeout=20)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        _record_accuweather_error(e)
        backoff_seconds = max(60, ACCUWEATHER_LOCATION_ERROR_BACKOFF_SECONDS)
        if isinstance(e, requests.HTTPError):
            try:
                status = int(getattr(e.response, "status_code", 0) or 0)
            except Exception:
                status = 0
            if status in (401, 403):
                backoff_seconds = max(backoff_seconds, 21600)
            elif status == 429:
                backoff_seconds = max(backoff_seconds, 3600)
        with _accuweather_cache_lock:
            cur = _accuweather_location_cache.get(coord_key, {})
            if not isinstance(cur, dict):
                cur = {}
            cur["no_retry_until_ts"] = time.time() + float(backoff_seconds)
            _accuweather_location_cache[coord_key] = cur
        try:
            _save_accuweather_cache_state()
        except Exception:
            pass
        return None
    key = payload.get("Key")
    loc_key = str(key) if key else None
    if loc_key:
        with _accuweather_cache_lock:
            _accuweather_location_cache[coord_key] = {
                "ts": now_ts,
                "location_key": loc_key,
                "no_retry_until_ts": 0.0,
            }
        try:
            _save_accuweather_cache_state()
        except Exception:
            pass
    return loc_key

def accuweather_get_forecast_high_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    return accuweather_get_forecast_temp_f(lat, lon, now_local, temp_side="high")

def accuweather_get_forecast_low_f(lat: float, lon: float, now_local: datetime) -> Optional[float]:
    return accuweather_get_forecast_temp_f(lat, lon, now_local, temp_side="low")

def accuweather_get_forecast_temp_f(
    lat: float,
    lon: float,
    now_local: datetime,
    temp_side: str = "high",
    force_refresh: bool = False,
) -> Optional[float]:
    global _accuweather_last_success_est
    _load_accuweather_cache_state()
    side = normalize_temp_side(temp_side)
    if not ACCUWEATHER_API_KEY:
        return None
    loc_key = accuweather_location_key_from_latlon(lat, lon)
    if not loc_key:
        return None
    cache_key = str(loc_key).strip()
    today_iso = now_local.date().isoformat()
    now_ts = time.time()
    if not force_refresh:
        with _accuweather_cache_lock:
            cached = _accuweather_forecast_cache.get(cache_key)
            if isinstance(cached, dict):
                age = now_ts - float(cached.get("ts", 0.0) or 0.0)
                date_cached = str(cached.get("date", "") or "").strip()
                if age < max(60, ACCUWEATHER_FORECAST_CACHE_TTL_SECONDS) and date_cached == today_iso:
                    v = cached.get("high_f") if side == "high" else cached.get("low_f")
                    if v is not None:
                        return float(v)
    params = {
        "apikey": ACCUWEATHER_API_KEY,
        "details": "false",
        "metric": "false",
    }
    url = f"https://dataservice.accuweather.com/forecasts/v1/daily/1day/{loc_key}"
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        _record_accuweather_error(e)
        # Fallback to most recent cached value if still reasonably fresh.
        with _accuweather_cache_lock:
            cached = _accuweather_forecast_cache.get(cache_key)
            if isinstance(cached, dict):
                age = now_ts - float(cached.get("ts", 0.0) or 0.0)
                if age <= max(300, ACCUWEATHER_STALE_FALLBACK_MAX_AGE_SECONDS):
                    v = cached.get("high_f") if side == "high" else cached.get("low_f")
                    if v is not None:
                        return float(v)
        return None
    forecasts = payload.get("DailyForecasts", []) or []
    if not forecasts:
        return None
    t_obj = forecasts[0].get("Temperature", {}) or {}
    v_high = t_obj.get("Maximum", {}).get("Value")
    v_low = t_obj.get("Minimum", {}).get("Value")
    high_f = (None if v_high is None else float(v_high))
    low_f = (None if v_low is None else float(v_low))
    with _accuweather_cache_lock:
        _accuweather_forecast_cache[cache_key] = {
            "ts": now_ts,
            "date": today_iso,
            "high_f": high_f,
            "low_f": low_f,
        }
    _accuweather_last_success_est = fmt_est(datetime.now(tz=LOCAL_TZ))
    try:
        _save_accuweather_cache_state()
    except Exception:
        pass
    value = high_f if side == "high" else low_f
    return None if value is None else float(value)


def accuweather_forecast_cache_age_seconds(lat: float, lon: float) -> Optional[float]:
    coord_key = f"{lat:.4f},{lon:.4f}"
    now_ts = time.time()
    with _accuweather_cache_lock:
        loc = _accuweather_location_cache.get(coord_key)
        if not isinstance(loc, dict):
            return None
        loc_key = str(loc.get("location_key", "") or "").strip()
        if not loc_key:
            return None
        fc = _accuweather_forecast_cache.get(loc_key)
        if not isinstance(fc, dict):
            return None
        ts = float(fc.get("ts", 0.0) or 0.0)
        if ts <= 0:
            return None
        return max(0.0, now_ts - ts)


# -----------------------
# Kalshi market data
# -----------------------
@dataclass
class Market:
    ticker: str
    title: str
    temp_side: str = "high"
    series_ticker: str = ""
    market_date_iso: str = ""

market_cache: Dict[str, object] = {
    "ts": 0.0,
    "by_city": {city: [] for city in CITY_CONFIG.keys()},
}
_weather_series_cache: Dict[str, object] = {"ts": 0.0, "by_city": {}}
WEATHER_SERIES_CACHE_TTL_SECONDS = int(os.getenv("WEATHER_SERIES_CACHE_TTL_SECONDS", "21600"))
_series_metadata_cache: Dict[str, object] = {"ts": 0.0, "by_ticker": {}}
SERIES_METADATA_CACHE_TTL_SECONDS = int(os.getenv("SERIES_METADATA_CACHE_TTL_SECONDS", "21600"))
_board_cache: Dict[str, object] = {"ts": 0.0, "market_day": "", "payload": None}

def kalshi_has_auth_config() -> bool:
    return bool(KALSHI_API_KEY_ID) and bool(KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PEM)

def _kalshi_private_key_obj():
    global _kalshi_key_cache
    if _kalshi_key_cache is not None:
        return _kalshi_key_cache
    if serialization is None:
        raise RuntimeError("cryptography package not installed; run: .\\.venv\\Scripts\\python.exe -m pip install -r requirements.txt")
    pem_text = KALSHI_PRIVATE_KEY_PEM
    if not pem_text and KALSHI_PRIVATE_KEY_PATH:
        with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
            pem_bytes = f.read()
    else:
        pem_bytes = pem_text.encode("utf-8")
    with _kalshi_key_lock:
        if _kalshi_key_cache is None:
            _kalshi_key_cache = serialization.load_pem_private_key(pem_bytes, password=None)
    return _kalshi_key_cache

def kalshi_auth_headers(method: str, url: str) -> Dict[str, str]:
    if not kalshi_has_auth_config():
        return {}
    parsed = urlparse(url)
    path = parsed.path or "/"
    ts_ms = str(int(time.time() * 1000))
    msg = f"{ts_ms}{method.upper()}{path}"
    signature = _kalshi_private_key_obj().sign(
        msg.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    sig_b64 = base64.b64encode(signature).decode("ascii")
    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
    }

def kalshi_get(path: str, params: Optional[dict] = None, timeout: int = 20, max_retries: int = 5) -> dict:
    url = path if path.startswith("http") else f"{KALSHI_BASE_URL}{path}"
    backoff = 1.0

    for attempt in range(max_retries):
        try:
            headers = kalshi_auth_headers("GET", url)
            r = requests.get(url, params=params, headers=headers or None, timeout=timeout)
        except requests.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff + random.uniform(0.0, 0.4))
            backoff = min(backoff * 2.0, 30.0)
            continue

        if r.status_code == 401 and kalshi_has_auth_config():
            raise RuntimeError("Kalshi auth failed (401). Check KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH/.PEM")

        if r.status_code == 429 or r.status_code >= 500:
            if attempt == max_retries - 1:
                r.raise_for_status()
            retry_after = r.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    wait_s = float(retry_after)
                except ValueError:
                    wait_s = backoff + random.uniform(0.0, 0.4)
            else:
                wait_s = backoff + random.uniform(0.0, 0.4)
            time.sleep(min(wait_s, 30.0))
            backoff = min(backoff * 2.0, 30.0)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError("kalshi_get exhausted retries")

def kalshi_post(path: str, payload: dict, timeout: int = 20, max_retries: int = 3) -> dict:
    url = path if path.startswith("http") else f"{KALSHI_BASE_URL}{path}"
    backoff = 1.0
    for attempt in range(max_retries):
        try:
            headers = kalshi_auth_headers("POST", url)
            headers["Content-Type"] = "application/json"
            r = requests.post(url, headers=headers or None, json=payload, timeout=timeout)
        except requests.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff + random.uniform(0.0, 0.4))
            backoff = min(backoff * 2.0, 20.0)
            continue

        if r.status_code == 401 and kalshi_has_auth_config():
            raise RuntimeError("Kalshi auth failed (401) on POST. Check write key + matching private key.")
        if r.status_code == 429 or r.status_code >= 500:
            if attempt == max_retries - 1:
                try:
                    return r.json()
                except Exception:
                    r.raise_for_status()
            retry_after = r.headers.get("Retry-After")
            try:
                wait_s = float(retry_after) if retry_after is not None else (backoff + random.uniform(0.0, 0.4))
            except Exception:
                wait_s = backoff + random.uniform(0.0, 0.4)
            time.sleep(min(wait_s, 20.0))
            backoff = min(backoff * 2.0, 20.0)
            continue
        try:
            return r.json()
        except Exception:
            r.raise_for_status()
    raise RuntimeError("kalshi_post exhausted retries")

def kalshi_delete(path: str, params: Optional[dict] = None, timeout: int = 20, max_retries: int = 3) -> dict:
    url = path if path.startswith("http") else f"{KALSHI_BASE_URL}{path}"
    backoff = 1.0
    for attempt in range(max_retries):
        try:
            headers = kalshi_auth_headers("DELETE", url)
            r = requests.delete(url, params=params, headers=headers or None, timeout=timeout)
        except requests.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff + random.uniform(0.0, 0.4))
            backoff = min(backoff * 2.0, 20.0)
            continue

        if r.status_code == 401 and kalshi_has_auth_config():
            raise RuntimeError("Kalshi auth failed (401) on DELETE. Check write key + matching private key.")
        if r.status_code == 429 or r.status_code >= 500:
            if attempt == max_retries - 1:
                try:
                    return r.json()
                except Exception:
                    r.raise_for_status()
            retry_after = r.headers.get("Retry-After")
            try:
                wait_s = float(retry_after) if retry_after is not None else (backoff + random.uniform(0.0, 0.4))
            except Exception:
                wait_s = backoff + random.uniform(0.0, 0.4)
            time.sleep(min(wait_s, 20.0))
            backoff = min(backoff * 2.0, 20.0)
            continue
        if r.status_code == 204:
            return {}
        try:
            return r.json()
        except Exception:
            r.raise_for_status()
    raise RuntimeError("kalshi_delete exhausted retries")

def kalshi_cancel_order(order_id: str, timeout: int = 20, max_retries: int = 2) -> Tuple[bool, str]:
    oid = str(order_id or "").strip()
    if not oid:
        return False, "missing order_id"
    delete_paths = [
        f"/portfolio/orders/{oid}",
    ]
    legacy_post_paths = [
        f"/portfolio/orders/{oid}/cancel",
        f"/portfolio/orders/{oid}/cancel_order",
    ]
    last_err = ""
    for p in delete_paths:
        try:
            resp = kalshi_delete(p, timeout=timeout, max_retries=max_retries)
            err = str(resp.get("error", "") or "")
            if not err:
                return True, ""
            last_err = err
        except Exception as e:
            last_err = str(e)
    for p in legacy_post_paths:
        try:
            resp = kalshi_post(p, {}, timeout=timeout, max_retries=max_retries)
            err = str(resp.get("error", "") or "")
            if not err:
                return True, ""
            last_err = err
        except Exception as e:
            last_err = str(e)
    return False, last_err or "cancel failed"

def normalize_temp_side(temp_side: str) -> str:
    t = (temp_side or "high").strip().lower()
    if t in ("high", "max", "maximum"):
        return "high"
    if t in ("low", "min", "minimum"):
        return "low"
    return "high"

def normalize_market_day(market_day: str) -> str:
    d = (market_day or "today").strip().lower()
    if d in ("today", "tod", "t"):
        return "today"
    if d in ("tomorrow", "tmr", "tmrw", "next"):
        return "tomorrow"
    if d in ("auto", "default"):
        return "auto"
    return "today"

def city_lst_tz(city: Optional[str]) -> timezone:
    offset_h = CITY_STANDARD_UTC_OFFSETS.get(str(city or "").strip(), -5)
    return timezone(timedelta(hours=int(offset_h)))

def city_lst_now(now_local: datetime, city: Optional[str]) -> datetime:
    # Convert from timezone-aware "now_local" to per-city fixed LST.
    base = now_local
    if base.tzinfo is None:
        base = base.replace(tzinfo=LOCAL_TZ)
    return base.astimezone(city_lst_tz(city))

def market_date_for_day(now_local: datetime, market_day: str, city: Optional[str] = None) -> str:
    day = normalize_market_day(market_day)
    base_date = city_lst_now(now_local, city).date()
    if day == "tomorrow":
        return (base_date + timedelta(days=1)).isoformat()
    return base_date.isoformat()

def city_name_aliases(city: str) -> List[str]:
    aliases = [city.lower()]
    if city == "New York City":
        aliases.extend(["new york", "nyc"])
    elif city == "Washington DC":
        aliases.extend(["washington dc", "washington, dc"])
    elif city == "Las Vegas":
        aliases.extend(["vegas"])
    elif city == "San Antonio":
        aliases.extend(["san antonio", "satx"])
    elif city == "New Orleans":
        aliases.extend(["new orleans", "nola"])
    return aliases

def parse_market_date_iso_from_ticker(ticker: str) -> Optional[str]:
    t = (ticker or "").strip().upper()
    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})-", t)
    if not m:
        return None
    yy = int(m.group(1))
    mon = m.group(2)
    dd = int(m.group(3))
    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
    mm = month_map.get(mon)
    if mm is None:
        return None
    year = 2000 + yy
    try:
        return datetime(year, mm, dd).date().isoformat()
    except Exception:
        return None

def lead_hours_to_market_close(now_local: datetime, market_date_iso: Optional[str]) -> Optional[float]:
    if not market_date_iso:
        return None
    try:
        d = datetime.fromisoformat(str(market_date_iso)).date()
    except Exception:
        return None
    close_local = datetime(d.year, d.month, d.day, 23, 59, 0, tzinfo=LOCAL_TZ)
    return (close_local - now_local).total_seconds() / 3600.0

def next_scheduled_scan_time(now_local: datetime) -> datetime:
    interval_h = max(1, min(24, int(SCAN_SCHEDULE_INTERVAL_HOURS)))
    minute = max(0, min(59, int(SCAN_SCHEDULE_MINUTE)))
    anchor = int(SCAN_SCHEDULE_ANCHOR_HOUR) % 24
    slots_per_day = max(1, 24 // interval_h)

    for day_offset in (0, 1, 2):
        d = (now_local + timedelta(days=day_offset)).date()
        for i in range(slots_per_day):
            hour = (anchor + i * interval_h) % 24
            candidate = datetime(d.year, d.month, d.day, hour, minute, tzinfo=LOCAL_TZ)
            if candidate > now_local:
                return candidate
    return now_local + timedelta(hours=interval_h)

_fast_scan_until_ts = 0.0

def _board_has_fast_scan_edge(board_payload: Optional[dict]) -> bool:
    if not board_payload:
        return False
    threshold = float(FAST_SCAN_EDGE_THRESHOLD_PCT)
    for row in board_payload.get("rows", []) or []:
        edge_pct = float(row.get("net_calibrated_edge_pct", row.get("edge_pct", 0.0)))
        if edge_pct >= threshold:
            return True
    return False

def _maybe_extend_fast_scan_window(board_payload: Optional[dict], now_ts: float) -> None:
    global _fast_scan_until_ts
    if not FAST_SCAN_ON_EDGE_ENABLED:
        return
    if not _board_has_fast_scan_edge(board_payload):
        return
    window_seconds = max(0, int(FAST_SCAN_WINDOW_MINUTES) * 60)
    if window_seconds <= 0:
        return
    _fast_scan_until_ts = max(_fast_scan_until_ts, now_ts + window_seconds)

def _active_scan_interval_seconds(now_ts: float) -> int:
    base_interval = max(1, int(SCAN_INTERVAL_SECONDS))
    if not FAST_SCAN_ON_EDGE_ENABLED:
        return base_interval
    if now_ts < _fast_scan_until_ts:
        return max(1, int(FAST_SCAN_INTERVAL_SECONDS))
    return base_interval

def compute_sleep_seconds(now_local: datetime) -> float:
    if not SCAN_USE_SCHEDULE:
        now_ts = time.time()
        interval = _active_scan_interval_seconds(now_ts)
        if SCAN_ALIGN_TO_INTERVAL and interval >= 60:
            next_tick = (math.floor(now_ts / interval) + 1) * interval
            return max(1.0, next_tick - now_ts)
        return max(1.0, float(interval))
    nxt = next_scheduled_scan_time(now_local)
    return max(1.0, (nxt - now_local).total_seconds())

def _load_weather_series_by_city(force: bool = False) -> Dict[str, Dict[str, List[str]]]:
    now_ts = time.time()
    with _market_cache_lock:
        cache_ts = float(_weather_series_cache.get("ts", 0.0))
        if not force and (now_ts - cache_ts) < WEATHER_SERIES_CACHE_TTL_SECONDS:
            cached = _weather_series_cache.get("by_city", {})
            if cached:
                return {k: {"high": list(v.get("high", [])), "low": list(v.get("low", []))} for k, v in cached.items()}

    data = kalshi_get("/series", params={"limit": 10000}, timeout=30, max_retries=3)
    entries = data.get("series", []) or []
    out: Dict[str, Dict[str, List[str]]] = {city: {"high": [], "low": []} for city in CITY_CONFIG.keys()}

    for s in entries:
        ticker = str(s.get("ticker", "")).strip()
        title = str(s.get("title", "")).strip().lower()
        category = str(s.get("category", "")).strip().lower()
        if not ticker or not title:
            continue
        if category != "climate and weather":
            continue

        side = None
        if ticker.startswith("KXHIGH"):
            side = "high"
        elif ticker.startswith("KXLOW"):
            side = "low"
        if side is None:
            continue

        for city in CITY_CONFIG.keys():
            aliases = city_name_aliases(city)
            if any(a in title for a in aliases):
                out[city][side].append(ticker)

    # Keep stable order and unique values.
    for city in out.keys():
        out[city]["high"] = sorted(list(dict.fromkeys(out[city]["high"])))
        out[city]["low"] = sorted(list(dict.fromkeys(out[city]["low"])))

    with _market_cache_lock:
        _weather_series_cache["ts"] = now_ts
        _weather_series_cache["by_city"] = out
    return out

def _load_series_metadata_map(force: bool = False) -> Dict[str, dict]:
    now_ts = time.time()
    with _market_cache_lock:
        cache_ts = float(_series_metadata_cache.get("ts", 0.0))
        if not force and (now_ts - cache_ts) < SERIES_METADATA_CACHE_TTL_SECONDS:
            cached = _series_metadata_cache.get("by_ticker", {})
            if cached:
                return dict(cached)

    data = kalshi_get("/series", params={"limit": 10000}, timeout=30, max_retries=3)
    entries = data.get("series", []) or []
    out: Dict[str, dict] = {}
    for s in entries:
        ticker = str(s.get("ticker", "")).strip()
        if not ticker:
            continue
        out[ticker] = {
            "title": s.get("title"),
            "category": s.get("category"),
            "contract_terms_url": s.get("contract_terms_url"),
            "settlement_sources": s.get("settlement_sources", []),
        }
    with _market_cache_lock:
        _series_metadata_cache["ts"] = now_ts
        _series_metadata_cache["by_ticker"] = out
    return dict(out)

def _is_temp_market_title(title: str) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return False
    if "temperature" not in t or "today" not in t:
        return False
    return ("highest temperature in" in t) or ("high temperature in" in t)

def _search_series_markets(series_ticker: str, temp_side: str, max_pages: int = 2, limit: int = 200) -> List[Market]:
    markets: List[Market] = []
    seen = set()
    cursor = None
    for _ in range(max_pages):
        params = {"status": "open", "limit": limit, "series_ticker": series_ticker}
        if cursor:
            params["cursor"] = cursor
        data = kalshi_get("/markets", params=params)
        rows = data.get("markets", []) or []
        for m in rows:
            ticker = str(m.get("ticker", "")).strip()
            title = str(m.get("title", "")).strip()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            markets.append(
                Market(
                    ticker=ticker,
                    title=title,
                    temp_side=temp_side,
                    series_ticker=series_ticker,
                    market_date_iso=parse_market_date_iso_from_ticker(ticker) or "",
                )
            )
        cursor = data.get("cursor")
        if not cursor:
            break
    return markets

def _search_temp_markets(query: str, max_pages: int = 3, limit: int = 200) -> List[Market]:
    markets: List[Market] = []
    seen = set()
    cursor = None

    for _ in range(max_pages):
        params = {"status": "open", "limit": limit, "search": query}
        if cursor:
            params["cursor"] = cursor
        data = kalshi_get("/markets", params=params)
        for m in data.get("markets", []):
            title = m.get("title", "")
            ticker = m.get("ticker", "")
            if not ticker or ticker in seen:
                continue
            if _is_temp_market_title(title):
                seen.add(ticker)
                markets.append(Market(ticker=ticker, title=title))
        cursor = data.get("cursor")
        if not cursor:
            break
    return markets

def refresh_markets_cache(force: bool = False) -> Dict[str, List[Market]]:
    now_ts = time.time()
    with _market_cache_lock:
        cache_ts = float(market_cache.get("ts", 0.0))
        if not force and (now_ts - cache_ts) < MARKET_CACHE_TTL_SECONDS:
            cached = market_cache.get("by_city", {})
            return {city: list(cached.get(city, [])) for city in CITY_CONFIG.keys()}

    try:
        grouped: Dict[str, List[Market]] = {c: [] for c in CITY_CONFIG.keys()}
        series_by_city = _load_weather_series_by_city(force=force)
        for city in CITY_CONFIG.keys():
            high_series = series_by_city.get(city, {}).get("high", [])
            low_series = series_by_city.get(city, {}).get("low", [])
            for st in high_series:
                grouped[city].extend(_search_series_markets(st, temp_side="high", max_pages=2, limit=200))
            for st in low_series:
                grouped[city].extend(_search_series_markets(st, temp_side="low", max_pages=2, limit=200))

        # Fallback legacy search for high-temperature markets only.
        missing = [city for city, markets in grouped.items() if not markets]
        for city in missing:
            city_discovered: List[Market] = []
            city_queries = [city, f"Highest temperature in {city}", f"{city} today temperature"]
            if city == "New York City":
                city_queries.extend(["NYC", "Highest temperature in NYC", "NYC today temperature"])
            if city == "Washington DC":
                city_queries.extend(["Washington, DC", "Highest temperature in Washington, DC"])
            for q in city_queries:
                city_discovered.extend(_search_temp_markets(q, max_pages=2, limit=200))
            for m in city_discovered:
                c = extract_city_from_title(m.title)
                if c == city:
                    grouped[city].append(m)

        for city, markets in grouped.items():
            dedup = {}
            for m in markets:
                dedup[m.ticker] = m
            grouped[city] = list(dedup.values())

        with _market_cache_lock:
            market_cache["ts"] = now_ts
            market_cache["by_city"] = grouped
        return {city: list(grouped.get(city, [])) for city in CITY_CONFIG.keys()}
    except Exception:
        with _market_cache_lock:
            cached = market_cache.get("by_city", {})
            has_any = any(cached.get(city, []) for city in CITY_CONFIG.keys())
            if has_any:
                return {city: list(cached.get(city, [])) for city in CITY_CONFIG.keys()}
        raise

def kalshi_get_orderbook(ticker: str) -> dict:
    return kalshi_get(f"/markets/{ticker}/orderbook")

def best_quotes_from_orderbook(ob: dict) -> Dict[str, Optional[int]]:
    book = ob.get("orderbook", ob)

    def _normalize_price_to_cents(value: object) -> int:
        try:
            if isinstance(value, str):
                v = value.strip()
                if "." in v:
                    return int(round(float(v) * 100.0))
                return int(v)
            if isinstance(value, float):
                if 0.0 <= value <= 1.0:
                    return int(round(value * 100.0))
                return int(round(value))
            return int(value)
        except Exception:
            return -1

    def _price_qty(level):
        if isinstance(level, dict):
            qty = level.get("quantity", level.get("qty", level.get("count", 0)))
            return _normalize_price_to_cents(level.get("price", -1)), int(float(qty or 0))
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return _normalize_price_to_cents(level[0]), int(float(level[1]))
        return -1, 0

    def _levels_from(obj: object) -> List[object]:
        if isinstance(obj, list):
            return obj
        if isinstance(obj, tuple):
            return list(obj)
        return []

    def _nested_levels(src: dict, parent_key: str, child_key: str) -> List[object]:
        parent = src.get(parent_key)
        if isinstance(parent, dict):
            return _levels_from(parent.get(child_key))
        return []

    def _fp_levels(src: dict, key: str) -> List[object]:
        fp = src.get("orderbook_fp")
        if isinstance(fp, dict):
            return _levels_from(fp.get(key))
        return []

    yes_bids = (
        _levels_from(book.get("yes"))
        or _levels_from(book.get("yes_bids"))
        or _nested_levels(book, "bids", "yes")
        or _fp_levels(book, "yes_dollars")
    )
    no_bids = (
        _levels_from(book.get("no"))
        or _levels_from(book.get("no_bids"))
        or _nested_levels(book, "bids", "no")
        or _fp_levels(book, "no_dollars")
    )
    yes_asks_direct = (
        _levels_from(book.get("yes_asks"))
        or _nested_levels(book, "asks", "yes")
    )
    no_asks_direct = (
        _levels_from(book.get("no_asks"))
        or _nested_levels(book, "asks", "no")
    )

    best_yes_bid = None
    best_yes_bid_size = None
    if yes_bids:
        y0 = max(yes_bids, key=lambda x: _price_qty(x)[0])
        best_yes_bid, best_yes_bid_size = _price_qty(y0)
        if best_yes_bid < 0:
            best_yes_bid = None
            best_yes_bid_size = None

    best_no_bid = None
    best_no_bid_size = None
    if no_bids:
        n0 = max(no_bids, key=lambda x: _price_qty(x)[0])
        best_no_bid, best_no_bid_size = _price_qty(n0)
        if best_no_bid < 0:
            best_no_bid = None
            best_no_bid_size = None

    best_yes_ask = None
    best_yes_ask_size = None
    if yes_asks_direct:
        y1 = min(yes_asks_direct, key=lambda x: _price_qty(x)[0])
        best_yes_ask, best_yes_ask_size = _price_qty(y1)
        if best_yes_ask < 0:
            best_yes_ask = None
            best_yes_ask_size = None
    elif best_no_bid is not None:
        best_yes_ask = 100 - best_no_bid

    best_no_ask = None
    best_no_ask_size = None
    if no_asks_direct:
        n1 = min(no_asks_direct, key=lambda x: _price_qty(x)[0])
        best_no_ask, best_no_ask_size = _price_qty(n1)
        if best_no_ask < 0:
            best_no_ask = None
            best_no_ask_size = None
    elif best_yes_bid is not None:
        best_no_ask = 100 - best_yes_bid

    top_size = None
    sizes = [s for s in [best_yes_bid_size, best_no_bid_size, best_yes_ask_size, best_no_ask_size] if s is not None]
    if sizes:
        top_size = min(sizes)

    return {
        "yes_bid": best_yes_bid,
        "yes_ask": best_yes_ask,
        "no_bid": best_no_bid,
        "no_ask": best_no_ask,
        "top_size": top_size,
    }

def best_bid_and_ask_from_orderbook(ob: dict) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    q = best_quotes_from_orderbook(ob)
    return q.get("yes_bid"), q.get("yes_ask"), q.get("top_size")


# -----------------------
# Parsing
# -----------------------
def extract_city_from_title(title: str) -> Optional[str]:
    if not title:
        return None
    m = re.search(r"(?:highest|high)\s+temperature\s+in\s+(.+?)\s+today\??\s*$", title.strip(), flags=re.IGNORECASE)
    if not m:
        return None
    city = m.group(1).strip()
    city = re.sub(r"\s+", " ", city)

    if city.lower() in ["washington, dc", "washington dc"]:
        city = "Washington DC"
    if city.lower() in ["nyc", "new york", "new york, ny", "new york city (nyc)"]:
        city = "New York City"
    if city in CITY_CONFIG:
        return city
    for known_city in CITY_CONFIG.keys():
        if known_city.lower() == city.lower():
            return known_city
    return city

def parse_bucket_from_title(title: str) -> Optional[Tuple[float, float]]:
    if not title:
        return None
    t = re.sub(r"\*\*", "", title.replace("Âº", "Â°")).strip()

    m = re.search(r">\s*(-?\d+(?:\.\d+)?)\s*[°º]", t)
    if m:
        x = float(m.group(1))
        # Strict ">" on whole-degree settlement maps to the next integer bucket floor.
        return (float(math.floor(x) + 1), 999.0)
    m = re.search(r"<\s*(-?\d+(?:\.\d+)?)\s*[°º]", t)
    if m:
        x = float(m.group(1))
        # Strict "<" on whole-degree settlement maps to the prior integer bucket ceiling.
        return (-999.0, float(math.ceil(x) - 1))
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*[°º]", t)
    if m:
        return (float(m.group(1)), float(m.group(2)))

    if "or below" in t.lower():
        nums = [float(n) for n in re.findall(r"(-?\d+(?:\.\d+)?)\s*[°º]", t)]
        if nums:
            return (-999.0, nums[0])
    if "or above" in t.lower():
        nums = [float(n) for n in re.findall(r"(-?\d+(?:\.\d+)?)\s*[°º]", t)]
        if nums:
            return (nums[0], 999.0)
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[°º]\s*to\s*(-?\d+(?:\.\d+)?)\s*[°º]", t, flags=re.IGNORECASE)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    return None

def parse_bucket_from_line(line: str) -> Optional[Tuple[float, float]]:
    if not line:
        return None
    t = str(line).strip()
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*F\s*to\s*(-?\d+(?:\.\d+)?)\s*F", t, flags=re.IGNORECASE)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*F\s*or\s*below", t, flags=re.IGNORECASE)
    if m:
        return (-999.0, float(m.group(1)))
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*F\s*or\s*above", t, flags=re.IGNORECASE)
    if m:
        return (float(m.group(1)), 999.0)
    return None

# -----------------------
# Model
# -----------------------
def estimate_daily_high_distribution(current_f: Optional[float], max_so_far_f: Optional[float], now_local: datetime) -> Tuple[float, float]:
    if max_so_far_f is None and current_f is None:
        return 0.0, 999.0

    floor = max_so_far_f if max_so_far_f is not None else current_f
    assert floor is not None

    peak = now_local.replace(hour=14, minute=30, second=0, microsecond=0)
    hrs_to_peak = (peak - now_local).total_seconds() / 3600.0

    if hrs_to_peak <= 0:
        expected_increment = 0.4
        sigma = 1.1
    else:
        expected_increment = min(1.2 * hrs_to_peak, 7.0)
        if now_local.hour < 11:
            sigma = 2.4
        elif now_local.hour < 14:
            sigma = 2.0
        else:
            sigma = 1.3

    mu = max(floor + 0.2, floor + expected_increment)
    return mu, sigma

def intraday_high_sigma_factor(now_local: datetime) -> float:
    h = now_local.hour + (now_local.minute / 60.0)
    if h >= 19:
        return 0.45
    if h >= 17:
        return 0.55
    if h >= 15:
        return 0.65
    if h >= 13:
        return 0.80
    if h >= 11:
        return 0.90
    return 1.00

def conditional_high_bucket_prob(mu: float, sigma: float, lo: float, hi: float, max_so_far_f: float) -> float:
    # Settlement is integer-F based; use rounded-integer floor implied by max-so-far.
    min_final_high_int = int(math.floor(max_so_far_f + 0.5))
    if hi < min_final_high_int:
        return 0.0
    lo_eff = max(lo, float(min_final_high_int))
    numer = prob_between_inclusive(mu, sigma, lo_eff, hi)
    denom = max(1e-9, 1.0 - normal_cdf(float(min_final_high_int) - 0.5, mu, sigma))
    return clamp(numer / denom, 0.0, 1.0)

def _obs_tail_prob_at_or_above(obs_f: float, threshold_f: float, sigma_f: float) -> float:
    s = max(1e-6, float(sigma_f))
    return clamp(1.0 - normal_cdf(float(threshold_f), float(obs_f), s), 0.0, 1.0)

def _obs_tail_prob_at_or_below(obs_f: float, threshold_f: float, sigma_f: float) -> float:
    s = max(1e-6, float(sigma_f))
    return clamp(normal_cdf(float(threshold_f), float(obs_f), s), 0.0, 1.0)

def next_nws_update_time(now_local: datetime) -> datetime:
    minute = max(0, min(59, int(NWS_OBS_UPDATE_MINUTE)))
    candidate = now_local.replace(minute=minute, second=0, microsecond=0)
    if candidate <= now_local:
        candidate = candidate + timedelta(hours=1)
    return candidate

def build_expert_consensus(
    city: str,
    now_local: datetime,
    temp_side: str = "high",
    force_accuweather_refresh: bool = False,
) -> Optional[dict]:
    side = normalize_temp_side(temp_side)
    cfg = CITY_CONFIG[city]
    lat = float(cfg["lat"])
    lon = float(cfg["lon"])

    source_values: List[Tuple[str, float, float]] = []

    try:
        om_ecmwf = open_meteo_get_forecast_temp_f(lat, lon, now_local, model="ecmwf_seamless", temp_side=side)
        if om_ecmwf is not None:
            source_values.append(("OpenMeteo-ECMWF", om_ecmwf, safe_inverse_mae_weight(OPEN_METEO_ECMWF_HIST_MAE_F)))
    except Exception:
        pass

    try:
        om_gfs = open_meteo_get_forecast_temp_f(lat, lon, now_local, model="gfs_seamless", temp_side=side)
        if om_gfs is not None:
            source_values.append(("OpenMeteo-GFS", om_gfs, safe_inverse_mae_weight(OPEN_METEO_GFS_HIST_MAE_F)))
    except Exception:
        pass

    if ENABLE_METNO_SOURCE:
        try:
            metno_v = metno_get_forecast_temp_f(lat, lon, now_local, temp_side=side)
            if metno_v is not None:
                source_values.append(("MET-Norway", metno_v, safe_inverse_mae_weight(METNO_HIST_MAE_F)))
        except Exception:
            pass

    if ENABLE_NWS_SOURCE:
        try:
            if side == "high":
                nws_high = nws_get_forecast_high_f(lat, lon, now_local)
                if nws_high is not None:
                    source_values.append(("NWS", nws_high, safe_inverse_mae_weight(NWS_HIST_MAE_F)))
            else:
                nws_low = nws_get_forecast_low_f(lat, lon, now_local)
                if nws_low is not None:
                    source_values.append(("NWS", nws_low, safe_inverse_mae_weight(NWS_LOW_HIST_MAE_F)))
        except Exception:
            pass

    if ENABLE_ACCUWEATHER_SOURCE:
        try:
            aw_temp = accuweather_get_forecast_temp_f(
                lat,
                lon,
                now_local,
                temp_side=side,
                force_refresh=force_accuweather_refresh,
            )
            if aw_temp is not None:
                source_values.append(("AccuWeather", aw_temp, safe_inverse_mae_weight(ACCUWEATHER_HIST_MAE_F)))
        except Exception:
            pass

    if not source_values:
        return None

    temp_weight_pairs = [(temp, weight) for _, temp, weight in source_values]
    mu = weighted_mean(temp_weight_pairs)
    if mu is None:
        return None
    disagreement_sigma = weighted_std(temp_weight_pairs, mu)
    sigma = max(1.2, CONSENSUS_BASE_SIGMA_F + 0.5 * disagreement_sigma)

    return {
        "mu": mu,
        "sigma": sigma,
        "sources": [{"name": name, "high_f": temp, "weight": w} for name, temp, w in source_values],
    }

def bucket_midpoint(lo: float, hi: float, fallback_width: float = 4.0) -> float:
    if lo <= -900 and hi >= 900:
        return 70.0
    if lo <= -900:
        return hi - (fallback_width / 2.0)
    if hi >= 900:
        return lo + (fallback_width / 2.0)
    return (lo + hi) / 2.0

def estimate_bucket_width(buckets: List[Tuple[float, float]]) -> float:
    widths = [hi - lo for lo, hi in buckets if lo > -900 and hi < 900 and hi > lo]
    if not widths:
        return 4.0
    widths.sort()
    return widths[len(widths) // 2]

def format_bucket_label(lo: float, hi: float) -> str:
    if lo <= -900:
        return f"{int(round(hi))}F or below"
    if hi >= 900:
        return f"{int(round(lo))}F or above"
    return f"{int(round(lo))}F to {int(round(hi))}F"

def resolve_city_name(city: str) -> Optional[str]:
    return canonical_city_name(city)

def build_city_bucket_comparison(
    city: str,
    markets: List[Market],
    now_local: datetime,
    temp_side: str = "high",
    consensus_override: Optional[dict] = None,
) -> Optional[dict]:
    side = normalize_temp_side(temp_side)
    consensus = consensus_override if consensus_override is not None else build_expert_consensus(city, now_local, temp_side=side)
    if consensus is None:
        return None
    side_markets = [m for m in markets if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
    if not side_markets:
        return None

    city_now_lst = city_lst_now(now_local, city)
    city_today_iso = city_now_lst.date().isoformat()
    target_date: Optional[str] = None
    date_counts: Dict[str, int] = {}
    for m in side_markets:
        d = getattr(m, "market_date_iso", "") or parse_market_date_iso_from_ticker(m.ticker) or ""
        if not d:
            continue
        date_counts[d] = date_counts.get(d, 0) + 1
    if date_counts:
        if city_today_iso in date_counts:
            target_date = city_today_iso
        else:
            future_keys = [k for k in date_counts.keys() if k >= city_today_iso]
            if future_keys:
                target_date = sorted(future_keys)[0]
            else:
                target_date = max(date_counts.keys())
        side_markets = [m for m in side_markets if ((getattr(m, "market_date_iso", "") or parse_market_date_iso_from_ticker(m.ticker) or "") == target_date)]

    obs_context = {
        "current_f": None,
        "max_so_far_f": None,
        "min_so_far_f": None,
        "obs_time_est": None,
        "obs_age_minutes": None,
        "obs_fresh": False,
        "nws_obs_update_minute": NWS_OBS_UPDATE_MINUTE,
        "next_expected_obs_est": fmt_est(next_nws_update_time(now_local)),
        "minutes_to_next_expected_obs": round((next_nws_update_time(now_local) - now_local).total_seconds() / 60.0, 1),
    }

    consensus_mu = float(consensus["mu"])
    consensus_sigma = float(consensus["sigma"])
    city_hour_lst = city_now_lst.hour
    forecast_ceiling_f: Optional[float] = None
    if side == "high":
        vals = [float(s.get("high_f")) for s in consensus.get("sources", []) if s.get("high_f") is not None]
        if vals:
            forecast_ceiling_f = max(vals)
    apply_intraday_obs_adjustments = (target_date is None) or (target_date == city_today_iso)
    if side in ("high", "low") and apply_intraday_obs_adjustments:
        try:
            station = CITY_CONFIG[city]["station"]
            current_f, max_so_far_f, min_so_far_f, obs_time = nws_get_today_temp_stats_f(
                station,
                date_tz=city_lst_tz(city),
            )
            obs_context["current_f"] = current_f
            obs_context["max_so_far_f"] = max_so_far_f
            obs_context["min_so_far_f"] = min_so_far_f
            if obs_time is not None:
                obs_context["obs_time_est"] = fmt_est(obs_time)
                obs_age_min = max(0.0, (now_local - obs_time).total_seconds() / 60.0)
                obs_context["obs_age_minutes"] = obs_age_min
                freshness_cap = max(float(NWS_OBS_STALE_MINUTES), 130.0)
                obs_context["obs_fresh"] = obs_age_min <= freshness_cap
            if side == "high" and obs_context["obs_fresh"] and max_so_far_f is not None:
                consensus_mu = max(consensus_mu, float(max_so_far_f) + 0.10)
                consensus_sigma = max(0.7, consensus_sigma * intraday_high_sigma_factor(now_local))
        except Exception:
            pass

    rows: List[dict] = []
    parsed_buckets: List[Tuple[float, float]] = []
    for m in side_markets:
        bucket = parse_bucket_from_title(m.title)
        if not bucket:
            continue
        ob = kalshi_get_orderbook(m.ticker)
        yes_bid, yes_ask, top_size = best_bid_and_ask_from_orderbook(ob)
        if yes_bid is None or yes_ask is None:
            continue
        if yes_ask < yes_bid:
            continue
        spread = yes_ask - yes_bid
        lo, hi = bucket
        parsed_buckets.append((lo, hi))
        rows.append({
            "ticker": m.ticker,
            "title": m.title,
            "bucket_label": format_bucket_label(lo, hi),
            "lo": lo,
            "hi": hi,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "spread_cents": spread,
            "top_size": top_size,
            # Midpoint-implied YES probability from live Kalshi orderbook prices.
            "kalshi_yes_mid_prob": clamp(((yes_bid + yes_ask) / 2.0) / 100.0, 0.001, 0.999),
        })

    if not rows:
        return None

    rows.sort(key=lambda r: (r["lo"], r["hi"]))
    total_mid = sum(r["kalshi_yes_mid_prob"] for r in rows)
    if total_mid <= 0:
        return None

    fallback_width = estimate_bucket_width(parsed_buckets)
    kalshi_mean = 0.0

    for r in rows:
        kalshi_yes_p = r["kalshi_yes_mid_prob"] / total_mid
        locked_outcome = False
        locked_reason = ""
        obs_impossible_prob = 0.0
        if side == "high" and obs_context["obs_fresh"] and obs_context["max_so_far_f"] is not None:
            max_so_far_f = float(obs_context["max_so_far_f"])
            high_impossible_boundary_f = float(r["hi"]) + 0.5 + HIGH_LOCK_MARGIN_F
            # Keep hard lock only when observation is clearly beyond boundary.
            if max_so_far_f >= (high_impossible_boundary_f + HIGH_HARD_LOCK_EXTRA_MARGIN_F):
                source_yes_p_raw = 0.0
                locked_outcome = True
                locked_reason = "high_obs_exceeded_bucket"
            else:
                base_yes_p = conditional_high_bucket_prob(
                    consensus_mu,
                    consensus_sigma,
                    r["lo"],
                    r["hi"],
                    max_so_far_f,
                )
                obs_impossible_prob = _obs_tail_prob_at_or_above(
                    max_so_far_f,
                    high_impossible_boundary_f,
                    OBS_BOUNDARY_SIGMA_F,
                )
                source_yes_p_raw = base_yes_p * (1.0 - obs_impossible_prob)
        elif side == "low" and obs_context["obs_fresh"] and obs_context["min_so_far_f"] is not None:
            min_so_far_f = float(obs_context["min_so_far_f"])
            low_impossible_boundary_f = float(r["lo"]) - 0.5 - LOW_LOCK_MARGIN_F
            # Keep hard lock only when observation is clearly beyond boundary.
            if min_so_far_f <= (low_impossible_boundary_f - LOW_HARD_LOCK_EXTRA_MARGIN_F):
                source_yes_p_raw = 0.0
                locked_outcome = True
                locked_reason = "low_obs_below_bucket"
            else:
                base_yes_p = prob_between_inclusive(consensus_mu, consensus_sigma, r["lo"], r["hi"])
                obs_impossible_prob = _obs_tail_prob_at_or_below(
                    min_so_far_f,
                    low_impossible_boundary_f,
                    OBS_BOUNDARY_SIGMA_F,
                )
                source_yes_p_raw = base_yes_p * (1.0 - obs_impossible_prob)
        else:
            source_yes_p_raw = prob_between_inclusive(consensus_mu, consensus_sigma, r["lo"], r["hi"])
        source_yes_p = (
            clamp(source_yes_p_raw, 0.0, 1.0)
            if locked_outcome
            else clamp(source_yes_p_raw, MODEL_WIN_PROB_FLOOR, MODEL_WIN_PROB_CEIL)
        )
        gap = source_yes_p - kalshi_yes_p

        yes_ask_p = r["yes_ask"] / 100.0
        yes_bid_p = r["yes_bid"] / 100.0
        edge_buy_yes = source_yes_p - yes_ask_p
        edge_buy_no = yes_bid_p - source_yes_p
        # Be conservative on high-temp intraday signals before local noon.
        if side == "high" and city_hour_lst < HIGH_EARLY_DAMPING_HOUR_LST and not locked_outcome:
            edge_buy_yes *= HIGH_EARLY_EDGE_DAMPING_MULTIPLIER
            edge_buy_no *= HIGH_EARLY_EDGE_DAMPING_MULTIPLIER
        if edge_buy_yes >= edge_buy_no:
            best_side = "BUY YES"
            best_edge = edge_buy_yes
        else:
            best_side = "BUY NO"
            best_edge = edge_buy_no

        mid_temp = bucket_midpoint(r["lo"], r["hi"], fallback_width=fallback_width)
        kalshi_mean += kalshi_yes_p * mid_temp

        r["kalshi_yes_prob"] = kalshi_yes_p
        r["source_yes_prob_raw"] = source_yes_p_raw
        r["source_yes_prob"] = source_yes_p
        r["locked_outcome"] = locked_outcome
        r["locked_reason"] = locked_reason
        r["obs_impossible_prob"] = obs_impossible_prob
        r["prob_gap"] = gap
        r["best_side"] = best_side
        r["best_edge"] = best_edge
        r["kalshi_no_bid"] = 100 - r["yes_ask"]
        r["kalshi_no_ask"] = 100 - r["yes_bid"]

    source_text = ", ".join(f"{s['name']}={s['high_f']:.1f}F" for s in consensus["sources"])
    source_map = {str(s.get("name", "")): s.get("high_f") for s in consensus["sources"]}
    weight_map = {str(s.get("name", "")): s.get("weight") for s in consensus["sources"]}
    return {
        "city": city,
        "temp_side": side,
        "as_of_est": fmt_est(now_local),
        "market_date_selected": target_date,
        "kalshi_mean_f": kalshi_mean,
        "consensus_mu_f": consensus_mu,
        "consensus_sigma_f": consensus_sigma,
        "city_hour_lst": city_hour_lst,
        "forecast_ceiling_f": forecast_ceiling_f,
        "source_text": source_text,
        "sources": consensus["sources"],
        "source_values_map": source_map,
        "source_weights_map": weight_map,
        "bucket_count": len(rows),
        "nws_obs_context": obs_context,
        "buckets": rows,
    }

def debug_city_bucket_comparison(
    city: str,
    now_local: datetime,
    temp_side: str = "high",
    market_day: str = "auto",
    force_refresh: bool = False,
) -> dict:
    resolved_city = resolve_city_name(city)
    if resolved_city is None:
        return {"ok": False, "error": f"unknown city: {city}"}

    side = normalize_temp_side(temp_side)
    grouped = refresh_markets_cache(force=force_refresh)
    city_markets = [m for m in grouped.get(resolved_city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
    selected_markets, selected_date, available_dates = select_markets_for_day(
        city_markets,
        now_local,
        market_day,
        city=resolved_city,
    )
    consensus = build_expert_consensus(resolved_city, now_local, temp_side=side)
    comparison = None
    if consensus is not None and selected_markets:
        comparison = build_city_bucket_comparison(
            resolved_city,
            selected_markets,
            now_local,
            temp_side=side,
            consensus_override=consensus,
        )

    market_debug: List[dict] = []
    parsed_bucket_count = 0
    usable_quote_count = 0
    for m in selected_markets[:50]:
        bucket = parse_bucket_from_title(m.title)
        bucket_ok = bucket is not None
        if bucket_ok:
            parsed_bucket_count += 1
        ob = kalshi_get_orderbook(m.ticker)
        yes_bid, yes_ask, top_size = best_bid_and_ask_from_orderbook(ob)
        quote_ok = (yes_bid is not None and yes_ask is not None and yes_ask >= yes_bid)
        if quote_ok:
            usable_quote_count += 1
        market_debug.append({
            "ticker": m.ticker,
            "title": m.title,
            "market_date_iso": getattr(m, "market_date_iso", "") or parse_market_date_iso_from_ticker(m.ticker) or "",
            "bucket_ok": bucket_ok,
            "bucket": None if bucket is None else {"lo": bucket[0], "hi": bucket[1]},
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "top_size": top_size,
            "quote_ok": quote_ok,
        })

    failure_reason = ""
    if consensus is None:
        failure_reason = "consensus_unavailable"
    elif not city_markets:
        failure_reason = "no_city_markets"
    elif not selected_markets:
        failure_reason = "no_markets_for_requested_day"
    elif parsed_bucket_count == 0:
        failure_reason = "no_parsed_buckets"
    elif usable_quote_count == 0:
        failure_reason = "no_usable_two_sided_quotes"
    elif comparison is None or not comparison.get("buckets"):
        failure_reason = "comparison_builder_returned_empty"

    return {
        "ok": True,
        "as_of_est": fmt_est(now_local),
        "city": resolved_city,
        "temp_side": side,
        "market_day_requested": normalize_market_day(market_day),
        "selected_market_date": selected_date,
        "available_market_dates": available_dates,
        "counts": {
            "city_markets": len(city_markets),
            "selected_markets": len(selected_markets),
            "parsed_bucket_count": parsed_bucket_count,
            "usable_quote_count": usable_quote_count,
            "comparison_bucket_count": int((comparison or {}).get("bucket_count", 0) or 0),
        },
        "failure_reason": failure_reason,
        "consensus": None if consensus is None else {
            "mu": consensus.get("mu"),
            "sigma": consensus.get("sigma"),
            "sources": consensus.get("sources", []),
        },
        "market_debug": market_debug,
    }

def build_city_odds_discrepancy(city: str, markets: List[Market], now_local: datetime, temp_side: str = "high") -> Optional[dict]:
    side = normalize_temp_side(temp_side)
    comparison = build_city_bucket_comparison(city, markets, now_local, temp_side=side)
    if comparison is None:
        return None

    rows = comparison["buckets"]
    max_gap = 0.0
    best_row = None
    best_edge = -1.0
    best_side = ""

    for r in rows:
        kalshi_p = r["kalshi_yes_prob"]
        model_p = r["source_yes_prob"]
        gap = r["prob_gap"]
        abs_gap = abs(gap)
        max_gap = max(max_gap, abs_gap)

        edge = r["best_edge"]
        side = r["best_side"]

        if edge > best_edge:
            best_edge = edge
            best_row = r
            best_side = side

    if best_row is None:
        return None

    mean_diff = comparison["consensus_mu_f"] - comparison["kalshi_mean_f"]
    return {
        "city": city,
        "temp_side": side,
        "kalshi_mean_f": comparison["kalshi_mean_f"],
        "consensus_mu_f": comparison["consensus_mu_f"],
        "consensus_sigma_f": comparison["consensus_sigma_f"],
        "mean_diff_f": mean_diff,
        "max_bucket_gap": max_gap,
        "best_edge": best_edge,
        "best_side": best_side,
        "best_ticker": best_row["ticker"],
        "best_title": best_row["title"],
        "source_text": comparison["source_text"],
    }

def build_discrepancy_alerts(grouped: Dict[str, List[Market]], now_local: datetime) -> List[dict]:
    alerts: List[dict] = []
    for city in CITY_CONFIG.keys():
        d = build_city_odds_discrepancy(city, grouped.get(city, []), now_local)
        if not d:
            continue
        if (
            abs(d["mean_diff_f"]) >= DISCREPANCY_MEAN_TEMP_THRESHOLD_F
            or d["max_bucket_gap"] >= DISCREPANCY_ALERT_THRESHOLD
        ):
            alerts.append(d)
    alerts.sort(key=lambda x: (x["max_bucket_gap"], abs(x["mean_diff_f"])), reverse=True)
    return alerts

def should_post_discrepancy(alerts: List[dict]) -> bool:
    global _last_discrepancy_post_ts, _last_discrepancy_signature
    if not alerts:
        return False

    top = alerts[0]
    sig = f"{top['city']}|{top['best_ticker']}|{top['best_side']}|{round(top['mean_diff_f'], 1)}|{round(top['max_bucket_gap'], 3)}"
    now = time.time()

    if sig != _last_discrepancy_signature:
        _last_discrepancy_signature = sig
        _last_discrepancy_post_ts = now
        return True

    if now - _last_discrepancy_post_ts >= MIN_SECONDS_BETWEEN_DISCREPANCY_POSTS:
        _last_discrepancy_post_ts = now
        return True

    return False

def discrepancy_text(alerts: List[dict], now_local: datetime, top_n: int = 5) -> str:
    ts = fmt_est_short(now_local)
    lines = [
        f"Weather Odds Discrepancy Alert ({ts})",
        "City | Kalshi mean | Consensus mean | Max bucket gap | Best action",
        "---",
    ]
    for a in alerts[:top_n]:
        lines.append(
            f"{a['city']} | {a['kalshi_mean_f']:.1f}F | {a['consensus_mu_f']:.1f}F "
            f"(d={a['mean_diff_f']:+.1f}F) | {a['max_bucket_gap']:.0%} | "
            f"{a['best_side']} edge={a['best_edge']:.1%} {a['best_ticker']} [{a['source_text']}]"
        )
    return "\n".join(lines)


# -----------------------
# Scoring
# -----------------------
def liquidity_factor(spread_cents: int, top_size: int) -> float:
    if spread_cents <= 4:
        sf = 1.0
    elif spread_cents <= 8:
        sf = 0.85
    elif spread_cents <= 12:
        sf = 0.65
    else:
        return 0.0

    if top_size >= 50:
        qf = 1.0
    elif top_size >= 20:
        qf = 0.85
    elif top_size >= 10:
        qf = 0.6
    else:
        return 0.0

    return sf * qf

def units_for_rank(rank: int) -> float:
    if rank == 1:
        return 3.0
    if rank in (2, 3):
        return 2.5
    if rank in (4, 5, 6):
        return 2.0
    if rank in (7, 8, 9):
        return 1.0
    if rank in (10, 11, 12):
        return 0.5
    return 0.0

def suggested_units_from_net_edge(edge_pct: float) -> float:
    e = float(edge_pct)
    if e >= 35.0:
        return 4.0
    if e >= 20.0:
        return 3.0
    if e >= 10.0:
        return 2.0
    if e >= 5.0:
        return 1.0
    return 0.0


def compute_city_best_play(city: str, markets: List[Market], now_local: datetime) -> dict:
    cfg = CITY_CONFIG[city]
    station = cfg["station"]
    conf = float(cfg["confidence"])

    current_f, max_so_far_f, _min_so_far_f, obs_time = nws_get_today_temp_stats_f(
        station,
        date_tz=city_lst_tz(city),
    )

    if obs_time is None:
        return {"city": city, "score": 0.0, "reason": "no obs"}

    if (now_local - obs_time).total_seconds() > 45 * 60:
        return {"city": city, "score": 0.0, "reason": "stale obs"}

    mu, sigma = estimate_daily_high_distribution(current_f, max_so_far_f, now_local)

    best = None  # (score, edge, side, ticker, title, yes_bid, yes_ask, spread, size, p_yes)
    for m in markets:
        if normalize_temp_side(getattr(m, "temp_side", "high")) != "high":
            continue
        bucket = parse_bucket_from_title(m.title)
        if not bucket:
            continue
        lo, hi = bucket
        p_yes = prob_between_inclusive(mu, sigma, lo, hi)

        ob = kalshi_get_orderbook(m.ticker)
        yes_bid, yes_ask, top_size = best_bid_and_ask_from_orderbook(ob)
        if yes_bid is None or yes_ask is None or top_size is None:
            continue

        spread = yes_ask - yes_bid
        lf = liquidity_factor(spread, top_size)
        if lf == 0.0:
            continue

        # BUY YES at ask
        edge_buy_yes = p_yes - (yes_ask / 100.0)
        # BUY NO (sell YES) at bid
        edge_buy_no = (yes_bid / 100.0) - p_yes

        if edge_buy_yes >= edge_buy_no:
            side = "BUY YES"
            edge = edge_buy_yes
        else:
            side = "BUY NO"
            edge = edge_buy_no

        score = max(0.0, edge) * lf * conf

        if best is None or score > best[0]:
            best = (score, edge, side, m.ticker, m.title, yes_bid, yes_ask, spread, top_size, p_yes)

    if best is None:
        return {"city": city, "score": 0.0, "reason": "no liquid markets"}

    score, edge, side, ticker, title, yes_bid, yes_ask, spread, size, p_yes = best
    return {
        "city": city,
        "score": score,
        "edge": edge,
        "side": side,
        "ticker": ticker,
        "title": title,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "spread": spread,
        "size": size,
        "p_yes": p_yes,
        "current_f": current_f,
        "max_so_far_f": max_so_far_f,
        "obs_time": fmt_est(obs_time),
    }


def build_ranked_results(grouped: Dict[str, List[Market]], now_local: datetime) -> List[dict]:
    results: List[dict] = []
    for city in CITY_CONFIG.keys():
        results.append(compute_city_best_play(city, grouped.get(city, []), now_local))

    results.sort(key=lambda r: r.get("score", 0.0), reverse=True)
    for i, r in enumerate(results, start=1):
        r["rank"] = i
        r["units"] = units_for_rank(i)
    return results


def leaderboard_text(results: List[dict], now_local: datetime) -> str:
    ts = fmt_est_short(now_local)
    lines = [f"ðŸ“Š Weather EV Leaderboard ({ts})", "Rank | City | Score | Units | Best play", "---"]
    for r in results:
        rank = r["rank"]
        city = r["city"]
        score = r.get("score", 0.0)
        units = r["units"]

        if score <= 0 or "ticker" not in r:
            reason = r.get("reason", "no data")
            lines.append(f"{rank:>2} | {city} | {score:.4f} | {units:.1f}u | {reason}")
            continue

        lines.append(
            f"{rank:>2} | {city} | {score:.4f} | {units:.1f}u | "
            f"{r['side']} edge={r['edge']:.1%}, p={r['p_yes']:.0%}, "
            f"bid/ask={r['yes_bid']}/{r['yes_ask']}Â¢ spr={r['spread']}Â¢ sz~{r['size']} "
            f"(obs {r['current_f']:.1f}F, max {r['max_so_far_f']:.1f}F @ {r['obs_time']}) "
            f"{r['ticker']}"
        )
    return "\n".join(lines)


def should_post(results: List[dict]) -> bool:
    global _last_post_ts, _last_top_signature
    if not results:
        return False

    best = results[0]
    if best.get("score", 0.0) < MIN_SCORE_FOR_1_UNIT:
        return False

    sig = ""
    if "ticker" in best:
        sig = f"{best['city']}|{best.get('side','')}|{best['ticker']}"

    now = time.time()

    # Post immediately if the top play changed
    if sig and sig != _last_top_signature:
        _last_top_signature = sig
        _last_post_ts = now
        return True

    # Otherwise respect cooldown
    if now - _last_post_ts >= MIN_SECONDS_BETWEEN_POSTS:
        _last_post_ts = now
        return True

    return False

def select_markets_for_day(
    markets: List[Market],
    now_local: datetime,
    market_day: str,
    city: Optional[str] = None,
) -> Tuple[List[Market], Optional[str], List[str]]:
    by_date: Dict[str, List[Market]] = {}
    for m in markets:
        d = getattr(m, "market_date_iso", "") or parse_market_date_iso_from_ticker(m.ticker) or ""
        if not d:
            continue
        by_date.setdefault(d, []).append(m)
    available_dates = sorted(by_date.keys())
    if not by_date:
        return list(markets), None, available_dates

    day_pref = normalize_market_day(market_day)
    selected_date = None
    if day_pref == "auto":
        today_iso = city_lst_now(now_local, city).date().isoformat()
        if today_iso in by_date:
            selected_date = today_iso
        else:
            future_dates = [d for d in available_dates if d >= today_iso]
            selected_date = future_dates[0] if future_dates else available_dates[-1]
    else:
        wanted = market_date_for_day(now_local, day_pref, city=city)
        if wanted in by_date:
            selected_date = wanted
        else:
            return [], wanted, available_dates
    return by_date.get(selected_date, []), selected_date, available_dates

def build_odds_board(now_local: datetime, market_day: str = "auto") -> dict:
    grouped = refresh_markets_cache()
    tables = build_calibration_tables()
    rows: List[dict] = []
    unavailable: List[dict] = []

    for city in sorted(CITY_CONFIG.keys()):
        for side in ("high", "low"):
            min_bucket_count = BOARD_MIN_BUCKET_COUNT_LOW if side == "low" else BOARD_MIN_BUCKET_COUNT
            min_top_size = BOARD_MIN_TOP_SIZE_LOW if side == "low" else BOARD_MIN_TOP_SIZE
            max_spread_cents = BOARD_MAX_SPREAD_CENTS_LOW if side == "low" else BOARD_MAX_SPREAD_CENTS
            if side == "low" and not LOW_SIGNALS_ENABLED:
                unavailable.append({"city": city, "temp_side": side, "reason": "low signals disabled"})
                continue
            city_markets = [m for m in grouped.get(city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
            if not city_markets:
                unavailable.append({"city": city, "temp_side": side, "reason": "no city markets"})
                continue
            selected_markets, selected_date, available_dates = select_markets_for_day(
                city_markets,
                now_local,
                market_day,
                city=city,
            )
            if not selected_markets:
                unavailable.append({
                    "city": city,
                    "temp_side": side,
                    "reason": "no markets for requested day",
                    "available_market_dates": available_dates,
                })
                continue
            detail = build_city_bucket_comparison(city, selected_markets, now_local, temp_side=side)
            if detail is None or not detail.get("buckets"):
                unavailable.append({
                    "city": city,
                    "temp_side": side,
                    "reason": "no odds comparison available",
                    "market_date_selected": selected_date,
                })
                continue
            if int(detail.get("bucket_count", 0)) < min_bucket_count:
                unavailable.append({
                    "city": city,
                    "temp_side": side,
                    "reason": "insufficient bucket coverage",
                    "market_date_selected": selected_date,
                    "bucket_count": detail.get("bucket_count"),
                })
                continue

            best = max(detail["buckets"], key=lambda r: r.get("best_edge", -1.0))
            best_spread = best.get("spread_cents")
            best_size = best.get("top_size")
            if best_spread is None or best_spread > max_spread_cents:
                unavailable.append({
                    "city": city,
                    "temp_side": side,
                    "reason": "spread too wide",
                    "market_date_selected": selected_date,
                    "spread_cents": best_spread,
                })
                continue
            if best_size is None or best_size < min_top_size:
                unavailable.append({
                    "city": city,
                    "temp_side": side,
                    "reason": "top size too small",
                    "market_date_selected": selected_date,
                    "top_size": best_size,
                })
                continue
            market_win_p = implied_market_win_prob(best.get("best_side", ""), best.get("yes_bid"), best.get("yes_ask"))
            if market_win_p is None:
                unavailable.append({
                    "city": city,
                    "temp_side": side,
                    "reason": "unable to compute implied win probability",
                    "market_date_selected": selected_date,
                })
                continue
            locked_outcome = bool(best.get("locked_outcome", False))
            locked_reason = str(best.get("locked_reason", "") or "")
            obs_ctx = (detail.get("nws_obs_context", {}) or {})
            obs_fresh = bool(obs_ctx.get("obs_fresh", False))
            obs_age_min = _to_float(obs_ctx.get("obs_age_minutes"))
            is_locked_capture_candidate = (
                LIVE_LOCKED_OUTCOME_CAPTURE_ENABLED
                and locked_outcome
                and str(best.get("best_side", "")).upper() == "BUY NO"
                and obs_fresh
                and (obs_age_min is not None and obs_age_min <= LIVE_LOCKED_OUTCOME_MAX_OBS_AGE_MINUTES)
                and (best_spread is not None and int(best_spread) <= LIVE_LOCKED_OUTCOME_MAX_SPREAD_CENTS)
                and (best_size is not None and int(best_size) >= LIVE_LOCKED_OUTCOME_MIN_TOP_SIZE)
            )
            if (market_win_p < NO_TRADE_IMPLIED_PROB_MIN or market_win_p > NO_TRADE_IMPLIED_PROB_MAX) and not is_locked_capture_candidate:
                unavailable.append({
                    "city": city,
                    "temp_side": side,
                    "reason": "tail implied probability no-trade filter",
                    "market_date_selected": selected_date,
                    "market_win_prob_pct": market_win_p * 100.0,
                })
                continue
            rows.append({
                "city": city,
                "temp_side": side,
                "market_date_selected": selected_date,
                "consensus_mu_f": detail["consensus_mu_f"],
                "kalshi_mean_f": detail["kalshi_mean_f"],
                "bucket_label": best.get("bucket_label"),
                "ticker": best.get("ticker"),
                "best_side": best.get("best_side"),
                "best_edge": best.get("best_edge", 0.0),
                "edge_pct": best.get("best_edge", 0.0) * 100.0,
                "model_yes_prob_pct": best.get("source_yes_prob", 0.0) * 100.0,
                "kalshi_yes_prob_pct": best.get("kalshi_yes_prob", 0.0) * 100.0,
                "market_win_prob_pct": market_win_p * 100.0,
                "lead_hours_to_close": lead_hours_to_market_close(now_local, selected_date),
                "yes_bid": best.get("yes_bid"),
                "yes_ask": best.get("yes_ask"),
                "spread_cents": best_spread,
                "top_size": best_size,
                "source_values_map": detail.get("source_values_map", {}),
                "nws_obs_time_est": ((detail.get("nws_obs_context", {}) or {}).get("obs_time_est")),
                "nws_obs_age_minutes": obs_age_min,
                "nws_obs_fresh": obs_fresh,
                "locked_outcome": locked_outcome,
                "locked_reason": locked_reason,
                "is_locked_capture_candidate": is_locked_capture_candidate,
            })
    for r in rows:
        raw_edge = float(r.get("best_edge", 0.0))
        cal_edge, meta = calibrate_edge(
            raw_edge,
            str(r.get("city", "")),
            normalize_temp_side(str(r.get("temp_side", "high"))),
            _to_float(r.get("lead_hours_to_close")),
            tables,
        )
        net_edge = cal_edge - (EV_SLIPPAGE_PCT / 100.0)
        r["raw_edge_pct"] = raw_edge * 100.0
        r["calibrated_edge_pct"] = cal_edge * 100.0
        r["net_calibrated_edge_pct"] = net_edge * 100.0
        r["calibration_meta"] = meta

    rows.sort(key=lambda r: r.get("net_calibrated_edge_pct", -1e9), reverse=True)
    return {
        "rows": rows,
        "unavailable": unavailable,
    }

def snapshot_log_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "forecast_snapshots.csv")

def final_settlements_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "final_settlements.csv")

def paper_trade_alert_state_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "paper_trade_alert_state.json")

def live_trade_state_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "live_trade_state.json")

def live_exit_state_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "live_exit_state.json")

def daily_update_state_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "daily_update_state.json")

def daily_update_history_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "daily_update_history.csv")

def nyc_forecast_brief_state_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "nyc_forecast_brief_state.json")

def _get_last_daily_update_posted_ts_est() -> Optional[datetime]:
    path = daily_update_history_path()
    if not os.path.exists(path):
        return None
    last_row = None
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                last_row = r
    except Exception:
        return None
    if not last_row:
        return None
    return parse_ts_est(str(last_row.get("posted_ts_est", "")).strip())

def _get_last_daily_update_current_balance() -> Optional[float]:
    path = daily_update_history_path()
    if not os.path.exists(path):
        return None
    last_row = None
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                last_row = r
    except Exception:
        return None
    if not last_row:
        return None
    return _to_float(last_row.get("current_portfolio_balance_dollars"))

def _fetch_portfolio_balance_dollars() -> Optional[float]:
    if not kalshi_has_auth_config():
        return None
    candidates = [
        "/portfolio/balance",
        "/portfolio",
        "/portfolio/summary",
    ]
    keys = [
        "portfolio_value",
        "total_portfolio_value",
        "equity",
        "balance",
        "cash_balance",
    ]
    for ep in candidates:
        try:
            resp = kalshi_get(ep, timeout=20, max_retries=1)
        except Exception:
            continue
        if not isinstance(resp, dict):
            continue
        if ep == "/portfolio/balance":
            bal_c = _to_float(resp.get("balance"))
            pv_c = _to_float(resp.get("portfolio_value"))
            if bal_c is not None or pv_c is not None:
                return ((float(bal_c or 0.0) + float(pv_c or 0.0)) / 100.0)
        # Flatten one level for nested payloads.
        pools = [resp]
        for k in ("portfolio", "summary", "data"):
            v = resp.get(k)
            if isinstance(v, dict):
                pools.append(v)
        for p in pools:
            for k in keys:
                v = _to_float(p.get(k))
                if v is not None:
                    return float(v)
    return None

def _fetch_portfolio_components_dollars() -> Dict[str, Optional[float]]:
    out = {"cash_dollars": None, "positions_dollars": None, "total_dollars": None}
    if not kalshi_has_auth_config():
        return out
    try:
        resp = kalshi_get("/portfolio/balance", timeout=20, max_retries=1)
    except Exception:
        return out
    if not isinstance(resp, dict):
        return out
    bal_c = _to_float(resp.get("balance"))
    pv_c = _to_float(resp.get("portfolio_value"))
    cash = (float(bal_c) / 100.0) if bal_c is not None else None
    pos = (float(pv_c) / 100.0) if pv_c is not None else None
    total = None
    if cash is not None or pos is not None:
        total = float(cash or 0.0) + float(pos or 0.0)
    out["cash_dollars"] = cash
    out["positions_dollars"] = pos
    out["total_dollars"] = total
    return out

def live_trade_log_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "live_trade_orders.csv")

def manual_positions_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "manual_positions.csv")

def manual_auto_weather_positions_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "manual_positions_auto_weather.csv")

def manual_btc_positions_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "manual_positions_btc.csv")

def _read_csv_rows_with_header(path: str) -> Tuple[List[str], List[dict]]:
    if not os.path.exists(path):
        return [], []
    rows: List[dict] = []
    header: List[str] = []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = [str(h) for h in (reader.fieldnames or []) if str(h).strip()]
            for r in reader:
                rows.append(dict(r))
    except Exception:
        return [], []
    return header, rows

def _rewrite_csv_with_header(path: str, fieldnames: List[str], rows: List[dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames))
        w.writeheader()
        for r in rows:
            rr = {k: r.get(k, "") for k in fieldnames}
            w.writerow(rr)

def _ensure_csv_header_contains(path: str, required_fields: List[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        _rewrite_csv_with_header(path, required_fields, [])
        return
    header, rows = _read_csv_rows_with_header(path)
    if not header:
        _rewrite_csv_with_header(path, required_fields, rows)
        return
    missing = [f for f in required_fields if f not in header]
    if not missing:
        return
    merged_header = list(header) + missing
    _rewrite_csv_with_header(path, merged_header, rows)

def ensure_manual_positions_header() -> None:
    path = manual_positions_path()
    _ensure_csv_header_contains(path, [
        "manual_trade_id", "position_origin",
        "market_type", "market_name",
        "opened_ts_est", "date", "city", "temp_side", "ticker", "bet",
        "line", "price_cents", "count",
        "outcome", "total_cost_dollars", "fees_dollars", "total_payout_dollars", "total_return_dollars",
        "source", "note",
    ])

def ensure_manual_btc_positions_header() -> None:
    path = manual_btc_positions_path()
    _ensure_csv_header_contains(path, [
        "manual_trade_id", "position_origin",
        "opened_ts_est", "date", "market_type", "market_name", "ticker", "bet", "outcome",
        "total_cost_dollars", "fees_dollars", "total_payout_dollars", "total_return_dollars",
        "source", "note",
    ])

def ensure_manual_auto_weather_positions_header() -> None:
    path = manual_auto_weather_positions_path()
    _ensure_csv_header_contains(path, [
        "manual_trade_id", "position_origin",
        "market_type", "market_name",
        "opened_ts_est", "date", "city", "temp_side", "ticker", "bet",
        "line", "price_cents", "count",
        "outcome", "total_cost_dollars", "fees_dollars", "total_payout_dollars", "total_return_dollars",
        "source", "note",
    ])

def _new_manual_trade_id() -> str:
    return f"m_{uuid.uuid4().hex}"

def _row_position_origin(r: dict, default_origin: str = "user_manual") -> str:
    raw = str((r or {}).get("position_origin", "")).strip().lower()
    if raw in {"user_manual", "auto_kalshi_settlement", "bot_live"}:
        return raw
    src = str((r or {}).get("source", "")).strip().lower()
    if src == "auto_kalshi_settlement":
        return "auto_kalshi_settlement"
    return default_origin

def _normalize_manual_row_metadata(rows: List[dict], default_origin: str = "user_manual") -> bool:
    changed = False
    seen_ids = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        tid = str(r.get("manual_trade_id", "")).strip()
        if not tid or tid in seen_ids:
            r["manual_trade_id"] = _new_manual_trade_id()
            tid = str(r.get("manual_trade_id", "")).strip()
            changed = True
        if tid:
            seen_ids.add(tid)
        origin = _row_position_origin(r, default_origin=default_origin)
        if str(r.get("position_origin", "")).strip().lower() != origin:
            r["position_origin"] = origin
            changed = True
    return changed

def load_manual_positions_rows() -> List[dict]:
    ensure_manual_positions_header()
    path = manual_positions_path()
    rows: List[dict] = []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(dict(r))
    except Exception:
        return []
    _normalize_manual_row_metadata(rows, default_origin="user_manual")
    return rows

def load_manual_btc_positions_rows() -> List[dict]:
    ensure_manual_btc_positions_header()
    path = manual_btc_positions_path()
    rows: List[dict] = []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(dict(r))
    except Exception:
        return []
    _normalize_manual_row_metadata(rows, default_origin="user_manual")
    return rows

def load_manual_auto_weather_positions_rows() -> List[dict]:
    ensure_manual_auto_weather_positions_header()
    path = manual_auto_weather_positions_path()
    rows: List[dict] = []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(dict(r))
    except Exception:
        return []
    _normalize_manual_row_metadata(rows, default_origin="auto_kalshi_settlement")
    return rows

def _manual_market_type(r: dict) -> str:
    raw = str((r or {}).get("market_type", "")).strip().lower()
    if raw:
        return raw
    ticker = str((r or {}).get("ticker", "")).strip().upper()
    market_name = str((r or {}).get("market_name", "")).strip().lower()
    city = str((r or {}).get("city", "")).strip()
    side = str((r or {}).get("temp_side", "")).strip().lower()
    if ticker.startswith("KXHIGH") or ticker.startswith("KXLOW") or city or side in {"high", "low"}:
        return "weather"
    if "btc" in ticker.lower() or "btc" in market_name:
        return "btc_up_down"
    return "other"

def _manual_is_weather_row(r: dict) -> bool:
    return _manual_market_type(r) == "weather"

def _manual_is_btc_row(r: dict) -> bool:
    mt = _manual_market_type(r)
    if mt in {"btc", "btc_up_down", "crypto"}:
        return True
    ticker = str((r or {}).get("ticker", "")).strip().lower()
    market_name = str((r or {}).get("market_name", "")).strip().lower()
    return ("btc" in ticker) or ("btc" in market_name)

def _manual_weather_city_by_code() -> Dict[str, str]:
    return {
        "ATL": "Atlanta",
        "AUS": "Austin",
        "BOS": "Boston",
        "CHI": "Chicago",
        "DEN": "Denver",
        "LAS": "Las Vegas",
        "LAX": "Los Angeles",
        "MIA": "Miami",
        "PHIL": "Philadelphia",
        "SEA": "Seattle",
        "DCA": "Washington DC",
        "OKC": "Oklahoma City",
        "SFO": "San Francisco",
        "HOU": "Houston",
        "DFW": "Dallas",
        "DAL": "Dallas",
        "PHX": "Phoenix",
        "MSY": "New Orleans",
        "MSP": "Minneapolis",
        "SAT": "San Antonio",
        "NY": "New York City",
        "NYC": "New York City",
        "DC": "Washington DC",
    }

def _decode_weather_ticker_fields(ticker: str) -> Optional[dict]:
    t = str(ticker or "").strip().upper()
    m = re.match(r"^KX(?P<side>HIGH|LOW)T?(?P<city>[A-Z]+)-(?P<date>\d{2}[A-Z]{3}\d{2})-(?P<bound>[BT])(?P<val>-?\d+(?:\.\d+)?)$", t)
    if not m:
        return None
    side = "high" if m.group("side") == "HIGH" else "low"
    city_code = str(m.group("city")).strip().upper()
    city = _manual_weather_city_by_code().get(city_code, "")
    val = float(m.group("val"))
    bound = str(m.group("bound")).strip().upper()
    line = ""
    if bound == "B":
        lo = int(math.floor(val))
        hi = lo + 1
        line = f"{lo}F to {hi}F"
    elif bound == "T":
        lo = int(math.floor(val)) + 1
        line = f"{lo}F or above"
    return {
        "city": city,
        "temp_side": side,
        "line": line,
        "market_date_iso": parse_market_date_iso_from_ticker(t) or "",
    }

def _bot_logged_weather_tickers() -> set:
    out = set()
    for r in load_live_trade_log_rows():
        st = str(r.get("status", "")).strip().lower()
        if st not in ("submitted", "partial", "partial_filled"):
            continue
        if str(r.get("order_action", "buy")).strip().lower() != "buy":
            continue
        t = str(r.get("ticker", "")).strip().upper()
        if t.startswith("KXHIGH") or t.startswith("KXLOW"):
            out.add(t)
    return out

def _to_money_2(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    return round(float(v), 2)

def sync_manual_positions_from_kalshi(max_pages: int = 30, per_page_limit: int = 200, force_update: bool = False, dry_run: bool = False) -> dict:
    ensure_manual_positions_header()
    ensure_manual_auto_weather_positions_header()
    ensure_manual_btc_positions_header()
    if not kalshi_has_auth_config():
        return {"ok": False, "error": "kalshi auth not configured"}

    user_weather_path = manual_positions_path()
    auto_weather_path = manual_auto_weather_positions_path()
    btc_path = manual_btc_positions_path()
    uw_header, uw_rows = _read_csv_rows_with_header(user_weather_path)
    aw_header, aw_rows = _read_csv_rows_with_header(auto_weather_path)
    b_header, b_rows = _read_csv_rows_with_header(btc_path)
    if not uw_header:
        ensure_manual_positions_header()
        uw_header, uw_rows = _read_csv_rows_with_header(user_weather_path)
    if not aw_header:
        ensure_manual_auto_weather_positions_header()
        aw_header, aw_rows = _read_csv_rows_with_header(auto_weather_path)
    if not b_header:
        ensure_manual_btc_positions_header()
        b_header, b_rows = _read_csv_rows_with_header(btc_path)

    _normalize_manual_row_metadata(uw_rows, default_origin="user_manual")
    _normalize_manual_row_metadata(aw_rows, default_origin="auto_kalshi_settlement")
    _normalize_manual_row_metadata(b_rows, default_origin="user_manual")

    idx_user_weather: Dict[Tuple[str, str], List[int]] = {}
    for i, r in enumerate(uw_rows):
        key = (str(r.get("ticker", "")).strip().upper(), str(r.get("date", "")).strip())
        idx_user_weather.setdefault(key, []).append(i)
    idx_auto_weather: Dict[Tuple[str, str], int] = {}
    for i, r in enumerate(aw_rows):
        idx_auto_weather[(str(r.get("ticker", "")).strip().upper(), str(r.get("date", "")).strip())] = i
    idx_b: Dict[Tuple[str, str], int] = {}
    for i, r in enumerate(b_rows):
        idx_b[(str(r.get("ticker", "")).strip().upper(), str(r.get("date", "")).strip())] = i

    bot_weather_tickers = _bot_logged_weather_tickers()
    settlements = _fetch_kalshi_settlements(max_pages=max(1, int(max_pages)), per_page_limit=max(1, int(per_page_limit)))
    inserted_weather = 0
    inserted_auto_weather = 0
    inserted_btc = 0
    updated_weather = 0
    updated_auto_weather = 0
    updated_user_weather = 0
    updated_btc = 0
    skipped_bot_weather = 0
    skipped_unclassified = 0
    skipped_missing_ticker = 0
    skipped_zero_economic = 0

    for s in settlements:
        ticker = str(s.get("ticker") or s.get("market_ticker") or "").strip().upper()
        if not ticker:
            skipped_missing_ticker += 1
            continue
        market_date = parse_market_date_iso_from_ticker(ticker) or ""
        settled_time_iso = str(s.get("settled_time", "")).strip()
        settled_time_est = ""
        if settled_time_iso:
            try:
                settled_time_est = fmt_est(datetime.fromisoformat(settled_time_iso.replace("Z", "+00:00")))
            except Exception:
                settled_time_est = ""
        date_iso = market_date
        if not date_iso and settled_time_iso:
            try:
                date_iso = datetime.fromisoformat(settled_time_iso.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                date_iso = ""

        yes_cost_c = float(_to_float(s.get("yes_total_cost")) or 0.0)
        no_cost_c = float(_to_float(s.get("no_total_cost")) or 0.0)
        revenue_c = float(_to_float(s.get("revenue")) or 0.0)
        fee_d = float(_to_float(s.get("fee_cost")) or 0.0)
        total_cost_d = (yes_cost_c + no_cost_c) / 100.0
        total_payout_d = revenue_c / 100.0
        total_return_d = (revenue_c - yes_cost_c - no_cost_c) / 100.0 - fee_d
        market_result = str(s.get("market_result", "")).strip().upper()
        market_title = str(s.get("market_title") or s.get("title") or "").strip()

        # Ignore no-economics settlements (no stake, no payout, no pnl) that add noisy rows.
        if total_cost_d <= 1e-9 and total_payout_d <= 1e-9 and abs(total_return_d) <= 1e-9:
            skipped_zero_economic += 1
            continue

        is_weather = ticker.startswith("KXHIGH") or ticker.startswith("KXLOW")
        is_btc = ("BTC" in ticker) or ("BTC" in market_title.upper())
        if not is_weather and not is_btc:
            skipped_unclassified += 1
            continue

        if yes_cost_c > 0 and no_cost_c <= 0:
            bet = "BUY YES"
            side_cost_d = yes_cost_c / 100.0
        elif no_cost_c > 0 and yes_cost_c <= 0:
            bet = "BUY NO"
            side_cost_d = no_cost_c / 100.0
        else:
            bet = "MIXED"
            side_cost_d = total_cost_d

        base = {
            "manual_trade_id": _new_manual_trade_id(),
            "position_origin": "auto_kalshi_settlement",
            "opened_ts_est": settled_time_est or fmt_est(datetime.now(tz=LOCAL_TZ)),
            "date": date_iso,
            "ticker": ticker,
            "bet": bet,
            "outcome": market_result,
            "total_cost_dollars": f"{_to_money_2(side_cost_d):.2f}",
            "fees_dollars": f"{_to_money_2(fee_d):.2f}",
            "total_payout_dollars": f"{_to_money_2(total_payout_d):.2f}",
            "total_return_dollars": f"{_to_money_2(total_return_d):.2f}",
            "source": "auto_kalshi_settlement",
            "note": f"auto_sync settled={settled_time_iso or 'unknown'}",
        }

        if is_weather:
            decoded = _decode_weather_ticker_fields(ticker) or {}
            row = dict(base)
            row.update({
                "market_type": "weather",
                "market_name": market_title or "Weather",
                "city": str(decoded.get("city", "")),
                "temp_side": str(decoded.get("temp_side", "")),
                "line": str(decoded.get("line", "")),
                "price_cents": "",
                "count": "",
            })
            key = (ticker, date_iso)
            # User-manual rows always win: apply settlement onto that row if present.
            if key in idx_user_weather and idx_user_weather[key]:
                rr = uw_rows[idx_user_weather[key][0]]
                changed = False
                cnt_user = float(_to_float(rr.get("count")) or 0.0)
                px_user = float(_to_float(rr.get("price_cents")) or 0.0)
                has_manual_fill = (cnt_user > 0.0 and px_user > 0.0)
                # User-manual rows keep their own economics; auto-sync should only close them.
                if force_update or not str(rr.get("outcome", "")).strip():
                    if str(rr.get("outcome", "")) != str(row.get("outcome", "")):
                        rr["outcome"] = row.get("outcome", "")
                        changed = True
                can_update_econ = (not has_manual_fill)
                if can_update_econ:
                    for k in ("total_cost_dollars", "fees_dollars", "total_payout_dollars", "total_return_dollars"):
                        v = row.get(k, "")
                        if (force_update and not has_manual_fill) or not str(rr.get(k, "")).strip():
                            if str(rr.get(k, "")) != str(v):
                                rr[k] = v
                                changed = True
                if not str(rr.get("market_type", "")).strip():
                    rr["market_type"] = "weather"
                    changed = True
                if str(rr.get("position_origin", "")).strip().lower() != "user_manual":
                    rr["position_origin"] = "user_manual"
                    changed = True
                if not str(rr.get("manual_trade_id", "")).strip():
                    rr["manual_trade_id"] = _new_manual_trade_id()
                    changed = True
                if changed:
                    updated_weather += 1
                    updated_user_weather += 1
            elif ticker in bot_weather_tickers:
                skipped_bot_weather += 1
            elif key in idx_auto_weather:
                rr = aw_rows[idx_auto_weather[key]]
                changed = False
                for k, v in row.items():
                    if force_update or not str(rr.get(k, "")).strip():
                        if str(rr.get(k, "")) != str(v):
                            rr[k] = v
                            changed = True
                if changed:
                    updated_weather += 1
                    updated_auto_weather += 1
            else:
                aw_rows.append(row)
                idx_auto_weather[key] = len(aw_rows) - 1
                inserted_weather += 1
                inserted_auto_weather += 1
            continue

        if is_btc:
            row = dict(base)
            row.update({
                "market_type": "btc_up_down",
                "market_name": market_title or "BTC Up or Down",
            })
            key = (ticker, date_iso)
            if key in idx_b:
                rr = b_rows[idx_b[key]]
                changed = False
                for k, v in row.items():
                    if force_update or not str(rr.get(k, "")).strip():
                        if str(rr.get(k, "")) != str(v):
                            rr[k] = v
                            changed = True
                if changed:
                    updated_btc += 1
            else:
                b_rows.append(row)
                idx_b[key] = len(b_rows) - 1
                inserted_btc += 1
            continue

    if not dry_run:
        _rewrite_csv_with_header(user_weather_path, uw_header, uw_rows)
        _rewrite_csv_with_header(auto_weather_path, aw_header, aw_rows)
        _rewrite_csv_with_header(btc_path, b_header, b_rows)

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "force_update": bool(force_update),
        "settlements_scanned": len(settlements),
        "inserted_weather": inserted_weather,
        "updated_weather": updated_weather,
        "inserted_user_weather": 0,
        "updated_user_weather": updated_user_weather,
        "inserted_auto_weather": inserted_auto_weather,
        "updated_auto_weather": updated_auto_weather,
        "inserted_btc": inserted_btc,
        "updated_btc": updated_btc,
        "skipped_bot_weather": skipped_bot_weather,
        "skipped_unclassified": skipped_unclassified,
        "skipped_missing_ticker": skipped_missing_ticker,
        "skipped_zero_economic": skipped_zero_economic,
        "weather_path": user_weather_path,
        "auto_weather_path": auto_weather_path,
        "btc_path": btc_path,
    }

def _manual_blocked_tickers() -> set:
    if not MANUAL_MARKET_BLOCK_ENABLED:
        return set()
    blocked = set()
    for r in load_manual_positions_rows():
        if not _manual_is_weather_row(r):
            continue
        if _row_position_origin(r, default_origin="user_manual") != "user_manual":
            continue
        t = str(r.get("ticker", "")).strip()
        if t:
            blocked.add(t)
    return blocked

def list_live_trade_log_paths() -> List[str]:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    pattern = os.path.join(SNAPSHOT_LOG_DIR, "live_trade_orders*.csv")
    files = [p for p in glob.glob(pattern) if os.path.isfile(p)]
    files.sort(key=lambda p: os.path.getmtime(p))
    return files

def load_live_trade_log_rows() -> List[dict]:
    rows: List[dict] = []
    for path in list_live_trade_log_paths():
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    rows.append(dict(r))
        except Exception:
            continue
    return rows

def live_trade_log_date_bounds() -> Tuple[Optional[str], Optional[str]]:
    rows = load_live_trade_log_rows()
    dates = sorted({
        str(r.get("date", "")).strip()
        for r in rows
        if re.match(r"^\d{4}-\d{2}-\d{2}$", str(r.get("date", "")).strip())
    })
    if not dates:
        return None, None
    return dates[0], dates[-1]

def rewrite_live_trade_log_rows(path: str, rows: List[dict]) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fieldnames: List[str] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k in r.keys():
            kk = str(k)
            if kk not in fieldnames:
                fieldnames.append(kk)
    if "fee_dollars" not in fieldnames:
        fieldnames.append("fee_dollars")
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            out = {k: r.get(k, "") for k in fieldnames}
            w.writerow(out)
    os.replace(tmp, path)

def edge_lifecycle_state_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "edge_lifecycle_state.json")

def edge_lifecycle_history_path() -> str:
    return os.path.join(SNAPSHOT_LOG_DIR, "edge_lifecycle_history.csv")

def _load_paper_trade_alert_state(today_key: str) -> Dict[str, dict]:
    global _paper_alert_state_date, _paper_alert_state
    if _paper_alert_state_date == today_key:
        return _paper_alert_state
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = paper_trade_alert_state_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if str(payload.get("date", "")) == today_key and isinstance(payload.get("entries"), dict):
                _paper_alert_state = payload["entries"]
            else:
                _paper_alert_state = {}
        except Exception:
            _paper_alert_state = {}
    else:
        _paper_alert_state = {}
    _paper_alert_state_date = today_key
    return _paper_alert_state

def _save_paper_trade_alert_state(today_key: str) -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = paper_trade_alert_state_path()
    tmp = path + ".tmp"
    payload = {"date": today_key, "entries": _paper_alert_state}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
    os.replace(tmp, path)

def _load_live_trade_state(today_key: str) -> Dict[str, dict]:
    global _live_trade_state_date, _live_trade_state
    if _live_trade_state_date == today_key:
        return _live_trade_state
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = live_trade_state_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if str(payload.get("date", "")) == today_key and isinstance(payload.get("entries"), dict):
                _live_trade_state = payload["entries"]
            else:
                _live_trade_state = {}
        except Exception:
            _live_trade_state = {}
    else:
        _live_trade_state = {}
    _live_trade_state_date = today_key
    return _live_trade_state

def _save_live_trade_state(today_key: str) -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = live_trade_state_path()
    tmp = path + ".tmp"
    payload = {"date": today_key, "entries": _live_trade_state}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
    os.replace(tmp, path)

def _load_live_exit_state(today_key: str) -> Dict[str, dict]:
    global _live_exit_state_date, _live_exit_state
    if _live_exit_state_date == today_key:
        return _live_exit_state
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = live_exit_state_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if str(payload.get("date", "")) == today_key and isinstance(payload.get("entries"), dict):
                _live_exit_state = payload["entries"]
            else:
                _live_exit_state = {}
        except Exception:
            _live_exit_state = {}
    else:
        _live_exit_state = {}
    _live_exit_state_date = today_key
    return _live_exit_state

def _save_live_exit_state(today_key: str) -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = live_exit_state_path()
    tmp = path + ".tmp"
    payload = {"date": today_key, "entries": _live_exit_state}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
    os.replace(tmp, path)

def _load_last_daily_update_date() -> str:
    global _last_daily_update_date
    if _last_daily_update_date:
        return _last_daily_update_date
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = daily_update_state_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            _last_daily_update_date = str(payload.get("date", "")).strip()
        except Exception:
            _last_daily_update_date = ""
    return _last_daily_update_date

def _save_last_daily_update_date(date_key: str) -> None:
    global _last_daily_update_date
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = daily_update_state_path()
    tmp = path + ".tmp"
    payload = {"date": str(date_key).strip()}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
    os.replace(tmp, path)
    _last_daily_update_date = payload["date"]

def _load_nyc_forecast_brief_state() -> Dict[str, str]:
    global _nyc_forecast_brief_state
    if _nyc_forecast_brief_state:
        return _nyc_forecast_brief_state
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = nyc_forecast_brief_state_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            entries = payload.get("entries", {})
            if isinstance(entries, dict):
                _nyc_forecast_brief_state = {str(k): str(v) for k, v in entries.items()}
            else:
                _nyc_forecast_brief_state = {}
        except Exception:
            _nyc_forecast_brief_state = {}
    return _nyc_forecast_brief_state

def _save_nyc_forecast_brief_state(entries: Dict[str, str]) -> None:
    global _nyc_forecast_brief_state
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = nyc_forecast_brief_state_path()
    tmp = path + ".tmp"
    payload = {
        "entries": {str(k): str(v) for k, v in (entries or {}).items()},
        "saved_ts_est": fmt_est(datetime.now(tz=LOCAL_TZ)),
    }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
    os.replace(tmp, path)
    _nyc_forecast_brief_state = payload["entries"]

def _append_live_trade_log(row: dict) -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = live_trade_log_path()
    header = [
        "ts_est", "date", "city", "temp_type", "ticker", "bet", "line",
        "edge_pct", "units", "stake_dollars", "side", "limit_price_cents",
        "count", "time_in_force", "order_action", "status", "error", "fee_dollars", "order_id", "client_order_id",
        "execution_mode", "attempt_count", "passive_attempted", "aggressive_attempted",
        "aggressive_used", "initial_limit_price_cents", "final_order_status_raw",
    ]
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as rf:
                first = rf.readline().strip()
            if first and first != ",".join(header):
                stamp = datetime.now(tz=LOCAL_TZ).strftime("%Y%m%d_%H%M%S")
                bak = os.path.join(SNAPSHOT_LOG_DIR, f"live_trade_orders_{stamp}.bak.csv")
                os.replace(path, bak)
        except Exception:
            pass
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        out = {k: row.get(k, "") for k in header}
        w.writerow(out)

def _load_edge_lifecycle_state(today_key: str) -> Dict[str, dict]:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = edge_lifecycle_state_path()
    if not os.path.exists(path):
        return {"date": today_key, "entries": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {"date": today_key, "entries": {}}
    if not isinstance(payload, dict):
        return {"date": today_key, "entries": {}}
    date_k = str(payload.get("date", "")).strip()
    entries = payload.get("entries", {})
    if not isinstance(entries, dict):
        entries = {}
    if date_k != today_key:
        return {"date": today_key, "entries": {}}
    return {"date": today_key, "entries": entries}

def _save_edge_lifecycle_state(today_key: str, entries: Dict[str, dict]) -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = edge_lifecycle_state_path()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"date": today_key, "entries": entries}, f, ensure_ascii=True)
    os.replace(tmp, path)

def _append_edge_lifecycle_history(row: dict) -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = edge_lifecycle_history_path()
    header = [
        "date", "sig", "city", "temp_type", "ticker", "bet", "line",
        "first_seen_est", "last_seen_est", "end_seen_est", "duration_seconds",
        "scan_count", "max_edge_pct", "last_edge_pct", "close_reason",
    ]
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        out = {k: row.get(k, "") for k in header}
        w.writerow(out)

def _edge_sig_from_bet(b: dict) -> str:
    return f"{b.get('date','')}|{b.get('ticker','')}|{b.get('bet','')}"

def track_edge_lifecycles(now_local: datetime, board_payload: dict) -> dict:
    today_key = now_local.date().isoformat()
    state = _load_edge_lifecycle_state(today_key)
    entries = dict(state.get("entries", {}))
    current_bets, _ = build_policy_bets_from_board_payload(
        board_payload,
        top_n=200,
        min_edge_pct=POLICY_MIN_NET_EDGE_PCT,
    )
    now_ts = now_local.timestamp()
    active_sigs: set = set()

    for b in current_bets:
        sig = _edge_sig_from_bet(b)
        if not sig:
            continue
        active_sigs.add(sig)
        edge = float(b.get("net_edge_pct", 0.0))
        market_key = f"{b.get('yes_bid','')}|{b.get('yes_ask','')}"
        weather_key = f"{b.get('nws_obs_time_est','')}|{b.get('source_values_key','')}"
        if sig not in entries:
            entries[sig] = {
                "date": b.get("date"),
                "city": b.get("city"),
                "temp_type": b.get("temp_type"),
                "ticker": b.get("ticker"),
                "bet": b.get("bet"),
                "line": b.get("line"),
                "first_seen_ts": now_ts,
                "last_seen_ts": now_ts,
                "first_seen_est": fmt_est(now_local),
                "last_seen_est": fmt_est(now_local),
                "scan_count": 1,
                "max_edge_pct": edge,
                "last_edge_pct": edge,
                "first_market_key": market_key,
                "last_market_key": market_key,
                "first_weather_key": weather_key,
                "last_weather_key": weather_key,
                "market_changed": False,
                "weather_changed": False,
                "fresh_trigger": False,
            }
        else:
            e = entries[sig]
            e["last_seen_ts"] = now_ts
            e["last_seen_est"] = fmt_est(now_local)
            e["scan_count"] = int(e.get("scan_count", 0)) + 1
            e["last_edge_pct"] = edge
            e["max_edge_pct"] = max(float(e.get("max_edge_pct", edge)), edge)
            first_market_key = str(e.get("first_market_key", "") or "")
            first_weather_key = str(e.get("first_weather_key", "") or "")
            market_changed = bool(first_market_key) and bool(market_key) and (market_key != first_market_key)
            weather_changed = bool(first_weather_key) and bool(weather_key) and (weather_key != first_weather_key)
            e["last_market_key"] = market_key
            e["last_weather_key"] = weather_key
            e["market_changed"] = bool(e.get("market_changed", False) or market_changed)
            e["weather_changed"] = bool(e.get("weather_changed", False) or weather_changed)
            e["fresh_trigger"] = bool(e.get("market_changed", False) or e.get("weather_changed", False))

    closed_count = 0
    to_close = [sig for sig in list(entries.keys()) if sig not in active_sigs]
    for sig in to_close:
        e = entries.pop(sig, None)
        if not e:
            continue
        first_ts = float(e.get("first_seen_ts", now_ts))
        last_ts = float(e.get("last_seen_ts", now_ts))
        duration = max(0.0, last_ts - first_ts)
        _append_edge_lifecycle_history({
            "date": e.get("date") or today_key,
            "sig": sig,
            "city": e.get("city"),
            "temp_type": e.get("temp_type"),
            "ticker": e.get("ticker"),
            "bet": e.get("bet"),
            "line": e.get("line"),
            "first_seen_est": e.get("first_seen_est"),
            "last_seen_est": e.get("last_seen_est"),
            "end_seen_est": fmt_est(now_local),
            "duration_seconds": int(round(duration)),
            "scan_count": int(e.get("scan_count", 0)),
            "max_edge_pct": round(float(e.get("max_edge_pct", 0.0)), 4),
            "last_edge_pct": round(float(e.get("last_edge_pct", 0.0)), 4),
            "close_reason": "edge_dropped_or_filtered",
        })
        closed_count += 1

    _save_edge_lifecycle_state(today_key, entries)
    return {
        "active_count": len(entries),
        "closed_count": closed_count,
    }

def _live_order_signature(bet: dict) -> str:
    return f"{bet.get('date','')}|{bet.get('ticker','')}|{bet.get('bet','')}"

def _bet_side_and_price_field(bet_side: str) -> Tuple[Optional[str], Optional[str]]:
    s = str(bet_side or "").strip().upper()
    if s == "BUY YES":
        return "yes", "yes_price"
    if s == "BUY NO":
        return "no", "no_price"
    return None, None

def _fill_penalty_cents(spread_cents: int, fill_mode: str) -> int:
    mode = (fill_mode or "touch").strip().lower()
    if mode == "touch":
        return 0
    if mode == "one_cent_worse":
        return 1
    if mode == "half_spread_worse":
        return max(1, int(math.ceil(max(0, spread_cents) / 2.0)))
    return 0

def _compute_limit_price_cents(quotes: Dict[str, Optional[int]], bet_side: str, fill_mode: str) -> Optional[int]:
    yes_bid = quotes.get("yes_bid")
    yes_ask = quotes.get("yes_ask")
    no_bid = quotes.get("no_bid")
    no_ask = quotes.get("no_ask")
    spread = None
    if yes_bid is not None and yes_ask is not None:
        spread = max(0, int(yes_ask) - int(yes_bid))
    pen = _fill_penalty_cents(int(spread or 0), fill_mode)
    if str(bet_side).upper() == "BUY YES":
        if yes_ask is None:
            return None
        return int(clamp(int(yes_ask) + pen, 1, 99))
    if str(bet_side).upper() == "BUY NO":
        if no_ask is None:
            return None
        return int(clamp(int(no_ask) + pen, 1, 99))
    return None

def _compute_passive_limit_price_cents(quotes: Dict[str, Optional[int]], bet_side: str) -> Optional[int]:
    yes_bid = quotes.get("yes_bid")
    no_bid = quotes.get("no_bid")
    if str(bet_side).upper() == "BUY YES":
        if yes_bid is None:
            return None
        return int(clamp(int(yes_bid), 1, 99))
    if str(bet_side).upper() == "BUY NO":
        if no_bid is None:
            return None
        return int(clamp(int(no_bid), 1, 99))
    return None

def _compute_repriced_passive_limit_price_cents(
    quotes: Dict[str, Optional[int]],
    bet_side: str,
    offset_cents: int,
    fill_mode: str,
) -> Optional[int]:
    base = _compute_passive_limit_price_cents(quotes, bet_side)
    if base is None:
        return None
    offset = max(0, int(offset_cents))
    if offset <= 0:
        return int(base)
    cap = _compute_limit_price_cents(quotes, bet_side, fill_mode)
    target = int(clamp(int(base) + offset, 1, 99))
    if cap is None:
        return target
    return min(target, int(cap))

def _compute_maker_one_tick_limit_price_cents(
    quotes: Dict[str, Optional[int]],
    bet_side: str,
    fill_mode: str,
) -> Optional[int]:
    yes_bid = quotes.get("yes_bid")
    yes_ask = quotes.get("yes_ask")
    no_bid = quotes.get("no_bid")
    no_ask = quotes.get("no_ask")
    side = str(bet_side).upper().strip()
    if side == "BUY YES":
        if yes_ask is None:
            return None
        target = int(yes_ask) - 1
        if yes_bid is not None:
            target = max(int(yes_bid), target)
        return int(clamp(target, 1, 99))
    if side == "BUY NO":
        if no_ask is None:
            return None
        target = int(no_ask) - 1
        if no_bid is not None:
            target = max(int(no_bid), target)
        return int(clamp(target, 1, 99))
    return _compute_repriced_passive_limit_price_cents(quotes, side, 0, fill_mode)

def _compute_sell_aggressive_price_cents(quotes: Dict[str, Optional[int]], order_side: str, fill_mode: str) -> Optional[int]:
    yes_bid = quotes.get("yes_bid")
    yes_ask = quotes.get("yes_ask")
    no_bid = quotes.get("no_bid")
    no_ask = quotes.get("no_ask")
    spread = None
    if yes_bid is not None and yes_ask is not None:
        spread = max(0, int(yes_ask) - int(yes_bid))
    pen = _fill_penalty_cents(int(spread or 0), fill_mode)
    if str(order_side).lower() == "yes":
        if yes_bid is None:
            return None
        return int(clamp(int(yes_bid) - pen, 1, 99))
    if str(order_side).lower() == "no":
        if no_bid is None:
            return None
        return int(clamp(int(no_bid) - pen, 1, 99))
    return None

def _compute_sell_passive_price_cents(quotes: Dict[str, Optional[int]], order_side: str) -> Optional[int]:
    if str(order_side).lower() == "yes":
        y = quotes.get("yes_ask")
        return None if y is None else int(clamp(int(y), 1, 99))
    if str(order_side).lower() == "no":
        n = quotes.get("no_ask")
        return None if n is None else int(clamp(int(n), 1, 99))
    return None

def _compute_repriced_passive_sell_price_cents(
    quotes: Dict[str, Optional[int]],
    order_side: str,
    offset_cents: int,
    fill_mode: str,
) -> Optional[int]:
    base = _compute_sell_passive_price_cents(quotes, order_side)
    if base is None:
        return None
    floor_price = _compute_sell_aggressive_price_cents(quotes, order_side, fill_mode)
    offset = max(0, int(offset_cents))
    target = int(clamp(int(base) - offset, 1, 99))
    if floor_price is None:
        return target
    return max(int(floor_price), int(target))

def _aggregate_open_live_positions(now_local: datetime) -> List[dict]:
    rows = load_live_trade_log_rows()
    positions: Dict[Tuple[str, str, str, str, str, str], dict] = {}
    now_date = now_local.date()
    for r in rows:
        st = str(r.get("status", "")).strip().lower()
        if st not in ("submitted", "partial", "partial_filled"):
            continue
        try:
            cnt = int(float(_to_float(r.get("count")) or 0))
        except Exception:
            cnt = 0
        if cnt <= 0:
            continue
        ts = parse_ts_est(str(r.get("ts_est", "")))
        if ts is None:
            continue
        d = str(r.get("date", "")).strip()
        city = str(r.get("city", "")).strip()
        side = normalize_temp_side(str(r.get("temp_type", "high")))
        ticker = str(r.get("ticker", "")).strip()
        bet = str(r.get("bet", "")).strip().upper()
        line = str(r.get("line", "")).strip()
        if not (d and city and ticker and bet):
            continue
        action = str(r.get("order_action", "buy") or "buy").strip().lower()
        sign = 1 if action == "buy" else (-1 if action == "sell" else 0)
        if sign == 0:
            continue
        key = (d, city, side, ticker, bet, line)
        px = float(_to_float(r.get("limit_price_cents")) or 0.0)
        e = positions.get(key)
        if e is None:
            e = {
                "date": d,
                "city": city,
                "temp_type": side,
                "ticker": ticker,
                "bet": bet,
                "line": line,
                "open_count": 0,
                "buy_count": 0,
                "sell_count": 0,
                "buy_notional_cents": 0.0,
                "buy_fee_dollars": 0.0,
                "first_entry_ts": ts,
                "last_ts": ts,
                "entry_edge_pct": float(_to_float(r.get("edge_pct")) or 0.0),
            }
            positions[key] = e
        e["last_ts"] = max(e["last_ts"], ts)
        if sign > 0:
            e["buy_count"] += cnt
            e["buy_notional_cents"] += (px * cnt)
            e["buy_fee_dollars"] += float(_to_float(r.get("fee_dollars")) or 0.0)
            e["entry_edge_pct"] = max(float(e.get("entry_edge_pct", 0.0)), float(_to_float(r.get("edge_pct")) or 0.0))
            e["first_entry_ts"] = min(e["first_entry_ts"], ts)
        else:
            e["sell_count"] += cnt
        e["open_count"] = max(0, int(e["buy_count"]) - int(e["sell_count"]))
    out: List[dict] = []
    for p in positions.values():
        if int(p.get("open_count", 0)) <= 0:
            continue
        try:
            market_dt = datetime.strptime(str(p.get("date", "")), "%Y-%m-%d").date()
            if market_dt < now_date:
                continue
        except Exception:
            pass
        bc = max(1, int(p.get("buy_count", 0)))
        p["avg_entry_price_cents"] = float(p.get("buy_notional_cents", 0.0)) / float(bc)
        out.append(p)
    return out

def _current_live_bot_exposure_dollars(now_local: datetime, state: Optional[Dict[str, dict]] = None) -> float:
    total = 0.0
    try:
        open_positions = _aggregate_open_live_positions(now_local)
    except Exception:
        open_positions = []
    for pos in open_positions:
        avg_entry_cents = float(_to_float(pos.get("avg_entry_price_cents")) or 0.0)
        open_count = max(0, int(pos.get("open_count", 0) or 0))
        if avg_entry_cents > 0.0 and open_count > 0:
            total += (avg_entry_cents * float(open_count)) / 100.0
    if isinstance(state, dict):
        for row in state.values():
            if not isinstance(row, dict):
                continue
            pending_order_id = str(row.get("pending_passive_order_id", "") or "").strip()
            if not pending_order_id:
                continue
            total += max(0.0, float(_to_float(row.get("pending_passive_stake_dollars")) or 0.0))
    return round(float(total), 6)

def _open_live_position_signatures(now_local: datetime) -> set:
    out = set()
    try:
        open_positions = _aggregate_open_live_positions(now_local)
    except Exception:
        open_positions = []
    for pos in open_positions:
        sig = f"{pos.get('date','')}|{pos.get('ticker','')}|{pos.get('bet','')}"
        if str(sig).strip():
            out.add(str(sig))
    try:
        exchange_positions = kalshi_get_market_positions(limit=500, max_pages=5)
    except Exception:
        exchange_positions = []
    for pos in exchange_positions:
        ticker = str(pos.get("ticker", "") or "").strip()
        if not ticker:
            continue
        qty = _kalshi_int_from_fp(
            pos.get("position")
            if pos.get("position") is not None else
            pos.get("position_fp")
        )
        if qty == 0:
            continue
        market_date = parse_market_date_iso_from_ticker(ticker) or ""
        bet = "BUY YES" if qty > 0 else "BUY NO"
        sig = f"{market_date}|{ticker}|{bet}"
        if str(sig).strip():
            out.add(str(sig))
    return out

def _is_open_position_currently_losing(pos: dict, quotes: Dict[str, Optional[int]]) -> Optional[bool]:
    bet_side = str(pos.get("bet", "")).strip().upper()
    entry_px = _to_float(pos.get("avg_entry_price_cents"))
    if entry_px is None:
        return None
    if bet_side == "BUY YES":
        current = _to_float(quotes.get("yes_bid"))
    elif bet_side == "BUY NO":
        current = _to_float(quotes.get("no_bid"))
    else:
        return None
    if current is None:
        return None
    return float(current) < float(entry_px)

def _estimate_unrealized_pnl_net_dollars(pos: dict, quotes: Dict[str, Optional[int]]) -> Optional[float]:
    bet_side = str(pos.get("bet", "")).strip().upper()
    entry_px = _to_float(pos.get("avg_entry_price_cents"))
    open_count = int(_to_float(pos.get("open_count")) or 0)
    if entry_px is None or open_count <= 0:
        return None
    if bet_side == "BUY YES":
        current = _to_float(quotes.get("yes_bid"))
    elif bet_side == "BUY NO":
        current = _to_float(quotes.get("no_bid"))
    else:
        return None
    if current is None:
        return None
    gross = (float(current) - float(entry_px)) * float(open_count) / 100.0
    buy_count = max(1, int(_to_float(pos.get("buy_count")) or 0))
    buy_fee_total = float(_to_float(pos.get("buy_fee_dollars")) or 0.0)
    fee_per_contract = buy_fee_total / float(buy_count)
    est_exit_fee = fee_per_contract * float(open_count)
    return float(gross - est_exit_fee)

def _compute_contract_count(stake_dollars: float, limit_price_cents: int) -> int:
    max_loss_per_contract = max(1, int(limit_price_cents))
    stake_cents = max(0, int(round(stake_dollars * 100.0)))
    if stake_cents < max_loss_per_contract:
        return 1
    return max(1, min(LIVE_MAX_CONTRACTS_PER_ORDER, stake_cents // max_loss_per_contract))

def _kelly_fraction_for_binary(p_win: float, price: float) -> float:
    # Binary contract with stake=price and win profit=(1-price): f* = (p-price)/(1-price)
    p = clamp(float(p_win), 0.001, 0.999)
    c = clamp(float(price), 0.001, 0.999)
    den = max(1e-9, 1.0 - c)
    return (p - c) / den

def _ladder_units_from_edge_pct(edge_pct: float) -> float:
    e = float(edge_pct)
    if e >= 40.0:
        return float(min(LADDER_MAX_UNITS, 2.0))
    if e >= 25.0:
        return float(min(LADDER_MAX_UNITS, 1.5))
    if e >= 20.0:
        return float(min(LADDER_MAX_UNITS, 1.0))
    if e >= 10.0:
        return float(min(LADDER_MAX_UNITS, 0.5))
    return 0.0

def _compute_stake_dollars_for_bet(b: dict) -> Tuple[float, float]:
    # Returns (stake_dollars, effective_units)
    trade_mode = str(b.get("trade_mode", "normal")).strip().lower()
    locked_cap_stake = max(
        LIVE_MIN_STAKE_DOLLARS,
        max(0.0, float(LIVE_LOCKED_OUTCOME_MAX_UNITS)) * max(0.01, UNIT_SIZE_DOLLARS),
    )
    units = float(b.get("suggested_units", 0.0))
    fallback_stake = max(LIVE_MIN_STAKE_DOLLARS, UNIT_SIZE_DOLLARS * max(0.0, units))
    if EDGE_LADDER_SIZING_ENABLED:
        edge_pct = float(b.get("net_edge_pct", 0.0))
        ladder_units = _ladder_units_from_edge_pct(edge_pct)
        unit_dollars = max(LIVE_MIN_STAKE_DOLLARS, KELLY_BANKROLL_DOLLARS * LADDER_UNIT_FRACTION_OF_BANKROLL)
        stake = max(LIVE_MIN_STAKE_DOLLARS, ladder_units * unit_dollars)
        if trade_mode == "locked_capture":
            stake = min(stake, locked_cap_stake)
        if ladder_units <= 0:
            fs = min(fallback_stake, locked_cap_stake) if trade_mode == "locked_capture" else fallback_stake
            return fs, (fs / max(0.01, UNIT_SIZE_DOLLARS))
        return float(stake), float(stake / max(0.01, UNIT_SIZE_DOLLARS))

    if not KELLY_SIZING_ENABLED:
        return fallback_stake, (fallback_stake / max(0.01, UNIT_SIZE_DOLLARS))

    market_p = float(b.get("market_implied_win_prob_pct", 0.0)) / 100.0
    cal_edge = float(b.get("calibrated_edge_pct", 0.0)) / 100.0
    # Calibrated win prob estimate and conservative effective price with execution buffer.
    p_hat = clamp(market_p + cal_edge, MODEL_WIN_PROB_FLOOR, MODEL_WIN_PROB_CEIL)
    c_eff = clamp(market_p + (KELLY_PRICE_BUFFER_PCT / 100.0), 0.001, 0.999)

    f_star = _kelly_fraction_for_binary(p_hat, c_eff)
    f_used = max(0.0, KELLY_FRACTION * f_star)
    f_used = min(f_used, max(0.0, KELLY_MAX_BET_FRACTION_OF_BANKROLL))
    if f_used > 0:
        f_used = max(f_used, max(0.0, KELLY_MIN_BET_FRACTION_OF_BANKROLL))

    stake = max(LIVE_MIN_STAKE_DOLLARS, KELLY_BANKROLL_DOLLARS * f_used)
    # Keep a floor fallback for very small or pathological Kelly outcomes.
    if f_star <= 0:
        stake = fallback_stake
    if trade_mode == "locked_capture":
        stake = min(stake, locked_cap_stake)
    return float(stake), float(stake / max(0.01, UNIT_SIZE_DOLLARS))

def _live_trade_text(now_local: datetime, results: List[dict]) -> str:
    ts = fmt_est_short(now_local)
    lines = [f"Live Execution ({ts})", "City | Type | Bet | Edge | Contract | Price | Count | Status | Order Type | Action", "---"]
    for r in results:
        date_part = str(r.get("date", "") or "").strip()
        line_part = str(r.get("line", "") or "").strip()
        contract = f"{date_part} {line_part}".strip()
        edge_pct = float(_to_float(r.get("edge_pct")) or 0.0)
        tif = str(r.get("time_in_force", "") or "").strip()
        exec_mode = str(r.get("execution_mode", "") or "").strip().lower()
        tif_norm = tif.lower()
        if exec_mode == "aggressive" or tif_norm in {"fill_or_kill", "immediate_or_cancel"}:
            order_type = "Market"
        else:
            order_type = "Limit"
        lines.append(
            f"{r.get('city')} | {r.get('temp_type')} | {r.get('bet')} | {edge_pct:.1f}% | {contract} | "
            f"{r.get('limit_price_cents')}c | {r.get('count')} | {r.get('status')} | {order_type} | {str(r.get('order_action', 'buy')).upper()}"
        )
    return "\n".join(lines)

def _response_order_meta(resp: dict) -> Tuple[str, int]:
    order = resp.get("order", {}) if isinstance(resp, dict) else {}
    status = str(order.get("status") or resp.get("status") or "").strip().lower()
    try:
        fill_count = int(order.get("fill_count", 0))
    except Exception:
        fill_count = 0
    return status, fill_count

def _extract_fee_dollars_from_order_response(resp: dict) -> float:
    if not isinstance(resp, dict):
        return 0.0
    order = resp.get("order", {}) if isinstance(resp.get("order", {}), dict) else {}
    total = 0.0
    def _to_fee_dollars(v: float) -> float:
        fv = float(v)
        # Kalshi order payloads commonly express fees as integer cents.
        if float(fv).is_integer() and abs(fv) >= 1.0:
            return fv / 100.0
        if abs(fv) > 1000.0:
            return fv / 100.0
        return fv

    def _num_from(keys: Tuple[str, ...]) -> Optional[float]:
        for k in keys:
            v = order.get(k, None)
            if v is None:
                v = resp.get(k, None)
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, dict):
                amt = v.get("amount", None)
                if isinstance(amt, (int, float)):
                    return float(amt)
        return None

    # Prefer explicit dollar fields first to avoid double counting with cent fields.
    td = _num_from(("taker_fees_dollars",))
    md = _num_from(("maker_fees_dollars",))
    if td is not None or md is not None:
        total = max(0.0, float(td or 0.0)) + max(0.0, float(md or 0.0))
        return max(0.0, total)

    fd = _num_from(("fee_dollars", "fees_dollars"))
    if fd is not None:
        return max(0.0, float(fd))

    tc = _num_from(("taker_fees", "taker_fee"))
    mc = _num_from(("maker_fees", "maker_fee"))
    if tc is not None or mc is not None:
        total = _to_fee_dollars(float(tc or 0.0)) + _to_fee_dollars(float(mc or 0.0))
        return max(0.0, total)

    fc = _num_from(("fee", "fees", "fee_paid", "fees_paid"))
    if fc is not None:
        return max(0.0, _to_fee_dollars(fc))

    return 0.0

def _extract_fee_dollars_from_any_payload(payload: object) -> float:
    total = 0.0
    if isinstance(payload, dict):
        total += _extract_fee_dollars_from_order_response(payload)
        for key in ("fills", "trades", "executions", "orders", "data", "items", "results"):
            arr = payload.get(key)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict):
                        total += _extract_fee_dollars_from_order_response(item)
        for k in ("fill", "trade", "execution", "result"):
            obj = payload.get(k)
            if isinstance(obj, dict):
                total += _extract_fee_dollars_from_order_response(obj)
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                total += _extract_fee_dollars_from_order_response(item)
    return max(0.0, float(total))

def kalshi_get_order_fee_dollars(order_id: str) -> float:
    oid = str(order_id or "").strip()
    if not oid:
        return 0.0
    try:
        resp = kalshi_get(f"/portfolio/orders/{oid}", timeout=20, max_retries=2)
        fee = _extract_fee_dollars_from_any_payload(resp)
        if fee > 0.0:
            return float(fee)
    except Exception:
        pass
    try:
        resp = kalshi_get(f"/portfolio/orders/{oid}/fills", timeout=20, max_retries=2)
        fee = _extract_fee_dollars_from_any_payload(resp)
        if fee > 0.0:
            return float(fee)
    except Exception:
        pass
    try:
        resp = kalshi_get("/portfolio/fills", params={"order_id": oid}, timeout=20, max_retries=2)
        fee = _extract_fee_dollars_from_any_payload(resp)
        if fee > 0.0:
            return float(fee)
    except Exception:
        pass
    return 0.0

def kalshi_get_order_snapshot(order_id: str) -> dict:
    oid = str(order_id or "").strip()
    if not oid:
        return {
            "ok": False,
            "order_id": "",
            "status": "",
            "fill_count": 0,
            "fee_dollars": 0.0,
            "raw": {},
            "error": "missing order_id",
        }
    try:
        resp = kalshi_get(f"/portfolio/orders/{oid}", timeout=20, max_retries=2)
        status, fill_count = _response_order_meta(resp)
        fee_dollars = _extract_fee_dollars_from_any_payload(resp)
        return {
            "ok": True,
            "order_id": oid,
            "status": status,
            "fill_count": int(max(0, fill_count)),
            "fee_dollars": float(max(0.0, fee_dollars)),
            "raw": resp,
            "error": "",
        }
    except Exception as e:
        return {
            "ok": False,
            "order_id": oid,
            "status": "",
            "fill_count": 0,
            "fee_dollars": 0.0,
            "raw": {},
            "error": str(e),
        }

def _kalshi_int_from_fp(v: object) -> int:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return int(round(float(v)))
    s = str(v or "").strip()
    if not s:
        return 0
    try:
        return int(round(float(s)))
    except Exception:
        return 0

def _kalshi_price_cents_from_order(order: dict, side: str) -> int:
    side_norm = str(side or "").strip().lower()
    keys = ("yes_price", "yes_price_cents", "yes_price_fp") if side_norm == "yes" else ("no_price", "no_price_cents", "no_price_fp")
    for k in keys:
        v = order.get(k, None)
        if isinstance(v, (int, float)):
            return int(round(float(v)))
        s = str(v or "").strip()
        if s:
            try:
                fv = float(s)
                if abs(fv) <= 1.0:
                    return int(round(fv * 100.0))
                return int(round(fv))
            except Exception:
                pass
    dollar_keys = ("yes_price_dollars",) if side_norm == "yes" else ("no_price_dollars",)
    for k in dollar_keys:
        s = str(order.get(k, "") or "").strip()
        if not s:
            continue
        try:
            return int(round(float(s) * 100.0))
        except Exception:
            continue
    return 0

def kalshi_get_orders(status: Optional[str] = None, ticker: Optional[str] = None, limit: int = 200, max_pages: int = 5) -> List[dict]:
    params = {"limit": max(1, min(200, int(limit)))}
    if status:
        params["status"] = str(status)
    if ticker:
        params["ticker"] = str(ticker)
    cursor = ""
    pages = 0
    out: List[dict] = []
    while pages < max(1, int(max_pages)):
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        resp = kalshi_get("/portfolio/orders", params=q, timeout=20, max_retries=2)
        orders = resp.get("orders", [])
        if isinstance(orders, list):
            out.extend([o for o in orders if isinstance(o, dict)])
        cursor = str(resp.get("cursor", "") or "").strip()
        pages += 1
        if not cursor:
            break
    return out

def kalshi_get_market_positions(limit: int = 500, max_pages: int = 5) -> List[dict]:
    params = {
        "limit": max(1, min(1000, int(limit))),
        "count_filter": "position",
    }
    cursor = ""
    pages = 0
    out: List[dict] = []
    while pages < max(1, int(max_pages)):
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        resp = kalshi_get("/portfolio/positions", params=q, timeout=20, max_retries=2)
        positions = resp.get("market_positions", [])
        if isinstance(positions, list):
            out.extend([p for p in positions if isinstance(p, dict)])
        cursor = str(resp.get("cursor", "") or "").strip()
        pages += 1
        if not cursor:
            break
    return out

def _is_order_closed_status(status: str) -> bool:
    s = str(status or "").strip().lower()
    return s in ("executed", "filled", "canceled", "cancelled", "rejected", "expired")

def _net_edge_now_pct_for_side(
    bet_side: str,
    model_yes_prob_pct: float,
    yes_bid: Optional[float],
    yes_ask: Optional[float],
) -> Optional[float]:
    market_win_p = implied_market_win_prob(bet_side, yes_bid, yes_ask)
    if market_win_p is None:
        return None
    model_yes_p = clamp(float(model_yes_prob_pct) / 100.0, 0.001, 0.999)
    model_win_p = model_yes_p if str(bet_side).upper() == "BUY YES" else (1.0 - model_yes_p)
    return (model_win_p - market_win_p) * 100.0 - EV_SLIPPAGE_PCT


def _refresh_trade_signal_with_fresh_accuweather(b: dict, now_local: datetime) -> dict:
    if not (ENABLE_ACCUWEATHER_SOURCE and LIVE_PRETRADE_ACCUWEATHER_REFRESH_ENABLED):
        return b
    city = str(b.get("city", "")).strip()
    if city not in CITY_CONFIG:
        return b
    side = normalize_temp_side(str(b.get("temp_type", "high")))
    cfg = CITY_CONFIG[city]
    lat = float(cfg["lat"])
    lon = float(cfg["lon"])
    max_age = max(60, int(LIVE_PRETRADE_ACCUWEATHER_MAX_AGE_SECONDS))
    age = accuweather_forecast_cache_age_seconds(lat, lon)
    if age is not None and age <= max_age:
        return b

    # Force-refresh AccuWeather before re-scoring this candidate.
    _ = accuweather_get_forecast_temp_f(lat, lon, now_local, temp_side=side, force_refresh=True)

    grouped = refresh_markets_cache()
    city_markets = [m for m in grouped.get(city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
    if not city_markets:
        return b

    target_date = str(b.get("date", "") or b.get("market_date_selected", "")).strip()
    by_date: Dict[str, List[Market]] = {}
    for m in city_markets:
        d = getattr(m, "market_date_iso", "") or parse_market_date_iso_from_ticker(m.ticker) or ""
        if d:
            by_date.setdefault(d, []).append(m)
    if not target_date:
        target_date = city_lst_now(now_local, city).date().isoformat()
    selected = by_date.get(target_date, [])
    if not selected:
        return b

    consensus = build_expert_consensus(city, now_local, temp_side=side, force_accuweather_refresh=True)
    if consensus is None:
        return b
    detail = build_city_bucket_comparison(city, selected, now_local, temp_side=side, consensus_override=consensus)
    if not detail:
        return b
    ticker = str(b.get("ticker", "")).strip()
    bucket = None
    for r in (detail.get("buckets", []) or []):
        if str(r.get("ticker", "")).strip() == ticker:
            bucket = r
            break
    if not bucket:
        return b

    out = dict(b)
    model_yes_prob_pct = float(_to_float(bucket.get("source_yes_prob")) or 0.0) * 100.0
    net_edge_pct = _net_edge_now_pct_for_side(
        str(out.get("bet", "")),
        model_yes_prob_pct,
        _to_float(bucket.get("yes_bid")),
        _to_float(bucket.get("yes_ask")),
    )
    out["model_yes_prob_pct"] = model_yes_prob_pct
    out["kalshi_yes_prob_pct"] = float(_to_float(bucket.get("kalshi_yes_prob")) or 0.0) * 100.0
    out["yes_bid"] = bucket.get("yes_bid")
    out["yes_ask"] = bucket.get("yes_ask")
    out["spread_cents"] = bucket.get("spread_cents")
    out["top_size"] = bucket.get("top_size")
    out["consensus_mu_f"] = detail.get("consensus_mu_f")
    out["source_values_map"] = detail.get("source_values_map", {})
    if net_edge_pct is not None:
        out["net_edge_pct"] = float(net_edge_pct)
    return out

def maybe_execute_live_trades(now_local: datetime, bets: List[dict]) -> int:
    if not LIVE_TRADING_ENABLED or _live_kill_switch_state:
        return 0
    if not kalshi_has_auth_config():
        return 0

    today_key = now_local.date().isoformat()
    state = _load_live_trade_state(today_key)
    edge_state = _load_edge_lifecycle_state(today_key)
    edge_entries = edge_state.get("entries", {}) if isinstance(edge_state, dict) else {}
    current_bot_exposure_dollars = _current_live_bot_exposure_dollars(now_local, state)
    open_position_sigs = _open_live_position_signatures(now_local)
    per_city_side: Dict[Tuple[str, str], int] = {}
    total_orders = 0
    for _, row in state.items():
        total_orders += int(row.get("count", 0))
        city_k = str(row.get("city", "")).strip()
        side_k = normalize_temp_side(str(row.get("temp_side", "high")))
        if city_k:
            per_city_side[(city_k, side_k)] = per_city_side.get((city_k, side_k), 0) + int(row.get("count", 0))

    done: List[dict] = []
    placed = 0
    blocked_tickers = _manual_blocked_tickers()
    now_et = now_local.astimezone(LOCAL_TZ)
    hour_et = int(now_et.hour)
    early_session = (
        LIVE_EARLY_SESSION_ENABLED
        and (hour_et >= LIVE_EARLY_SESSION_START_HOUR_ET)
        and (hour_et < LIVE_EARLY_SESSION_END_HOUR_ET)
    )
    active_sigs = {_live_order_signature(b) for b in bets if _live_order_signature(b)}
    active_order_keys: set = set()
    if LIVE_PASSIVE_ALLOW_RESTING_LIMITS and LIVE_PASSIVE_RESCAN_MODE_ENABLED:
        for b in bets:
            ticker_k = str(b.get("ticker", "")).strip()
            bet_side_k = str(b.get("bet", "")).strip().upper()
            order_side_k, _ = _bet_side_and_price_field(bet_side_k)
            if ticker_k and order_side_k:
                active_order_keys.add((ticker_k, str(order_side_k).lower(), "buy"))
    exchange_resting_by_key: Dict[Tuple[str, str, str], List[dict]] = {}
    if LIVE_PASSIVE_ALLOW_RESTING_LIMITS and LIVE_PASSIVE_RESCAN_MODE_ENABLED:
        try:
            for order in kalshi_get_orders(status="resting", limit=200, max_pages=5):
                ticker_k = str(order.get("ticker", "") or "").strip()
                side_k = str(order.get("side", "") or "").strip().lower()
                action_k = str(order.get("action", "buy") or "buy").strip().lower()
                client_oid = str(order.get("client_order_id", "") or "").strip()
                if (not ticker_k) or side_k not in {"yes", "no"} or action_k != "buy":
                    continue
                if not client_oid.startswith("bot-"):
                    continue
                exchange_resting_by_key.setdefault((ticker_k, side_k, action_k), []).append(order)
        except Exception:
            exchange_resting_by_key = {}

    def _clear_pending_passive(sig_key: str) -> None:
        if sig_key not in state:
            return
        row_local = state.get(sig_key, {}) or {}
        for k in (
            "pending_passive_order_id",
            "pending_passive_client_order_id",
            "pending_passive_price_cents",
            "pending_passive_requested_count",
            "pending_passive_reported_fill_count",
            "pending_passive_fee_dollars",
            "pending_passive_created_ts_epoch",
            "pending_passive_bet",
            "pending_passive_line",
            "pending_passive_ticker",
            "pending_passive_date",
            "pending_passive_city",
            "pending_passive_temp_side",
            "pending_passive_units",
            "pending_passive_stake_dollars",
            "pending_passive_order_action",
        ):
            row_local.pop(k, None)
        state[sig_key] = row_local

    def _record_pending_fill(sig_key: str, row_local: dict, snapshot: dict) -> int:
        reported = int(row_local.get("pending_passive_reported_fill_count", 0) or 0)
        fill_count = int(snapshot.get("fill_count", 0) or 0)
        delta = max(0, fill_count - reported)
        if delta <= 0:
            return 0
        fee_total = float(snapshot.get("fee_dollars", 0.0) or 0.0)
        prior_fee = float(row_local.get("pending_passive_fee_dollars", 0.0) or 0.0)
        fee_delta = max(0.0, fee_total - prior_fee)
        row_local["pending_passive_reported_fill_count"] = int(fill_count)
        row_local["pending_passive_fee_dollars"] = float(fee_total)
        count_before = int(row_local.get("count", 0) or 0)
        if fill_count > 0 and count_before < max(1, LIVE_MAX_ORDERS_PER_MARKET_PER_DAY):
            row_local["count"] = count_before + 1
        city_local = str(row_local.get("city", row_local.get("pending_passive_city", ""))).strip()
        side_local = normalize_temp_side(str(row_local.get("temp_side", row_local.get("pending_passive_temp_side", "high"))))
        if city_local:
            per_city_side[(city_local, side_local)] = per_city_side.get((city_local, side_local), 0) + 1
        done_item = {
            "ts_est": fmt_est(now_local),
            "date": row_local.get("pending_passive_date", ""),
            "city": row_local.get("pending_passive_city", city_local),
            "temp_type": row_local.get("pending_passive_temp_side", side_local),
            "ticker": row_local.get("pending_passive_ticker", ""),
            "bet": row_local.get("pending_passive_bet", ""),
            "line": row_local.get("pending_passive_line", ""),
            "edge_pct": "",
            "units": row_local.get("pending_passive_units", ""),
            "stake_dollars": row_local.get("pending_passive_stake_dollars", ""),
            "side": row_local.get("pending_passive_order_action", "buy"),
            "limit_price_cents": row_local.get("pending_passive_price_cents", ""),
            "count": int(delta),
            "time_in_force": LIVE_PASSIVE_TIME_IN_FORCE,
            "order_action": str(row_local.get("pending_passive_order_action", "buy")).lower(),
            "status": "submitted" if _is_order_closed_status(str(snapshot.get("status", ""))) else "partial_filled",
            "error": "",
            "fee_dollars": round(float(fee_delta), 6),
            "order_id": snapshot.get("order_id", ""),
            "client_order_id": row_local.get("pending_passive_client_order_id", ""),
            "execution_mode": "passive_resting_fill",
            "attempt_count": 1,
            "passive_attempted": True,
            "aggressive_attempted": False,
            "aggressive_used": False,
            "initial_limit_price_cents": row_local.get("pending_passive_price_cents", ""),
            "final_order_status_raw": snapshot.get("status", ""),
        }
        _append_live_trade_log(done_item)
        done.append(done_item)
        state[sig_key] = row_local
        return int(delta)

    def _cancel_pending_passive_if_possible(sig_key: str, order_id: str) -> bool:
        canceled, cancel_err = kalshi_cancel_order(order_id)
        if canceled:
            _clear_pending_passive(sig_key)
            return True
        row_local = state.get(sig_key, {}) or {}
        row_local["pending_passive_cancel_error"] = str(cancel_err or "cancel failed")
        state[sig_key] = row_local
        return False

    def _adopt_exchange_resting(sig_key: str, row_local: dict, order_obj: dict, city_local: str, side_local: str, bet_local: str, line_local: str, date_local: str, units_local: float, stake_local: float) -> None:
        order_id_local = str(order_obj.get("order_id", "") or "").strip()
        if not order_id_local:
            return
        order_side_local = str(order_obj.get("side", "") or "").strip().lower()
        fill_count_local = _kalshi_int_from_fp(order_obj.get("fill_count", order_obj.get("fill_count_fp", 0)))
        requested_count_local = _kalshi_int_from_fp(order_obj.get("initial_count", order_obj.get("initial_count_fp", order_obj.get("count", 0))))
        if requested_count_local <= 0:
            requested_count_local = max(fill_count_local, _kalshi_int_from_fp(order_obj.get("remaining_count", order_obj.get("remaining_count_fp", 0))))
        row_local["pending_passive_order_id"] = order_id_local
        row_local["pending_passive_client_order_id"] = str(order_obj.get("client_order_id", "") or "")
        row_local["pending_passive_price_cents"] = _kalshi_price_cents_from_order(order_obj, order_side_local)
        row_local["pending_passive_requested_count"] = int(max(0, requested_count_local))
        row_local["pending_passive_reported_fill_count"] = int(max(0, fill_count_local))
        row_local["pending_passive_fee_dollars"] = float(_extract_fee_dollars_from_order_response({"order": order_obj}) or 0.0)
        row_local["pending_passive_created_ts_epoch"] = float(now_local.timestamp())
        row_local["pending_passive_bet"] = bet_local
        row_local["pending_passive_line"] = line_local
        row_local["pending_passive_ticker"] = str(order_obj.get("ticker", "") or "")
        row_local["pending_passive_date"] = date_local
        row_local["pending_passive_city"] = city_local
        row_local["pending_passive_temp_side"] = side_local
        row_local["pending_passive_units"] = units_local
        row_local["pending_passive_stake_dollars"] = round(stake_local, 2)
        row_local["pending_passive_order_action"] = str(order_obj.get("action", "buy") or "buy").lower()
        row_local["city"] = city_local
        row_local["temp_side"] = side_local
        state[sig_key] = row_local

    for order_key, orders in list(exchange_resting_by_key.items()):
        if order_key in active_order_keys:
            continue
        keepers: List[dict] = []
        for order in orders:
            order_id = str(order.get("order_id", "") or "").strip()
            if not order_id:
                continue
            canceled, _ = kalshi_cancel_order(order_id)
            if not canceled:
                keepers.append(order)
        if keepers:
            exchange_resting_by_key[order_key] = keepers
        else:
            exchange_resting_by_key.pop(order_key, None)

    for sig_key, row_local in list(state.items()):
        pending_order_id = str((row_local or {}).get("pending_passive_order_id", "")).strip()
        if not pending_order_id:
            continue
        if sig_key in active_sigs:
            continue
        snapshot = kalshi_get_order_snapshot(pending_order_id)
        if bool(snapshot.get("ok")):
            _record_pending_fill(sig_key, row_local, snapshot)
            if _is_order_closed_status(str(snapshot.get("status", ""))):
                _clear_pending_passive(sig_key)
            else:
                _cancel_pending_passive_if_possible(sig_key, pending_order_id)

    for b in bets:
        if placed >= max(1, LIVE_MAX_ORDERS_PER_SCAN):
            break
        if total_orders >= max(1, LIVE_MAX_ORDERS_PER_DAY):
            break

        sig = _live_order_signature(b)
        if sig in open_position_sigs:
            continue
        row = state.get(sig, {})
        already = int(row.get("count", 0))
        city_k = str(b.get("city", "")).strip()
        side_k = normalize_temp_side(str(b.get("temp_type", "high")))
        if already >= max(1, LIVE_MAX_ORDERS_PER_MARKET_PER_DAY):
            continue
        if city_k and per_city_side.get((city_k, side_k), 0) >= max(1, LIVE_MAX_ORDERS_PER_CITY_SIDE_PER_DAY):
            continue

        bet_side = str(b.get("bet", "")).strip().upper()
        order_side, price_field = _bet_side_and_price_field(bet_side)
        if order_side is None or price_field is None:
            continue

        ticker = str(b.get("ticker", "")).strip()
        if not ticker:
            continue
        if ticker in blocked_tickers:
            continue
        try:
            b = _refresh_trade_signal_with_fresh_accuweather(b, now_local)
            units = float(b.get("suggested_units", 0.0))
            edge_pct = float(b.get("net_edge_pct", 0.0))
            sig_entry = (edge_entries.get(sig, {}) or {})
            sig_scans = int(sig_entry.get("scan_count", 1))
            if early_session and (
                (not LIVE_EARLY_SESSION_APPLY_TO_HIGH_ONLY) or (side_k == "high")
            ):
                if edge_pct < float(LIVE_EARLY_SESSION_MIN_EDGE_PCT):
                    continue
                if sig_scans < max(1, LIVE_EARLY_SESSION_MIN_SCANS):
                    continue
            if LIVE_STABILITY_GATE_ENABLED and (LIVE_STABILITY_GATE_EDGE_MIN_PCT <= edge_pct < LIVE_STABILITY_GATE_EDGE_MAX_PCT):
                if sig_scans < max(1, LIVE_STABILITY_GATE_MIN_SCANS_MID):
                    continue
                if LIVE_STABILITY_REQUIRE_CHANGE_MID and (not bool(sig_entry.get("fresh_trigger", False))):
                    continue
            stake_dollars, kelly_units = _compute_stake_dollars_for_bet(b)
            units = kelly_units if KELLY_SIZING_ENABLED else units
            if early_session and (
                (not LIVE_EARLY_SESSION_APPLY_TO_HIGH_ONLY) or (side_k == "high")
            ):
                size_mult = clamp(float(LIVE_EARLY_SESSION_SIZE_MULT), 0.05, 1.0)
                stake_dollars = max(0.0, stake_dollars * size_mult)
                units = max(0.0, units * size_mult)
            if (stake_dollars > 0.0) and (
                float(current_bot_exposure_dollars) + float(stake_dollars) > float(LIVE_MAX_OPEN_BOT_EXPOSURE_DOLLARS)
            ):
                continue

            def _submit_limit(limit_price: int, tif: str, mode: str, spread_cents: Optional[int], desired_count: Optional[int] = None) -> dict:
                count_local = int(desired_count) if desired_count is not None else _compute_contract_count(stake_dollars, int(limit_price))
                count_local = max(1, min(LIVE_MAX_CONTRACTS_PER_ORDER, int(count_local)))
                mode_norm = str(mode).strip().lower()
                tif_norm = sanitize_time_in_force_for_order(
                    tif,
                    default=("fill_or_kill" if mode_norm == "aggressive" else "good_till_canceled"),
                    allow_resting=(mode_norm == "passive" and LIVE_PASSIVE_ALLOW_RESTING_LIMITS),
                )
                payload = {
                    "ticker": ticker,
                    "client_order_id": f"bot-{today_key}-{uuid.uuid4().hex[:12]}",
                    "action": "buy",
                    "side": order_side,
                    "count": int(count_local),
                    price_field: int(limit_price),
                    "time_in_force": tif_norm,
                }
                # Only GTD should carry an explicit expiration timestamp.
                if tif_norm == "good_til_date":
                    payload["expiration_ts"] = int(time.time()) + max(5, LIVE_ORDER_EXPIRATION_SECONDS)
                resp_local = kalshi_post("/portfolio/orders", payload, timeout=20, max_retries=2)
                err_local = str(resp_local.get("error", "") or "")
                order_id_local = str(resp_local.get("order", {}).get("order_id") or resp_local.get("order_id") or "")
                order_status, fill_count = _response_order_meta(resp_local)
                fee_dollars = _extract_fee_dollars_from_order_response(resp_local)
                fill_count = max(0, min(int(count_local), int(fill_count)))
                filled_any = fill_count > 0
                filled_all = fill_count >= int(count_local)
                if err_local:
                    status_local = "rejected"
                elif filled_all:
                    status_local = "submitted"
                elif filled_any:
                    status_local = "partial"
                else:
                    status_local = "not_filled"
                return {
                    "status": status_local,
                    "error": err_local,
                    "order_id": order_id_local,
                    "client_order_id": payload["client_order_id"],
                    "order_action": str(payload.get("action", "buy")),
                    "count": int(count_local),
                    "limit_price_cents": int(limit_price),
                    "time_in_force": tif_norm,
                    "mode": mode,
                    "filled": filled_any,
                    "filled_all": filled_all,
                    "fill_count": fill_count,
                    "fee_dollars": float(fee_dollars),
                    "order_status_raw": order_status,
                    "spread_cents": spread_cents,
                }

            ob = kalshi_get_orderbook(ticker)
            quotes = best_quotes_from_orderbook(ob)
            yes_bid = quotes.get("yes_bid")
            yes_ask = quotes.get("yes_ask")
            spread_cents = None
            if yes_bid is not None and yes_ask is not None:
                spread_cents = max(0, int(yes_ask) - int(yes_bid))

            attempts: List[dict] = []
            seen_attempt_keys: set = set()

            def _append_attempt(kind: str, price: int, tif: str, wait_s: int, passive_offset_cents: int = 0) -> None:
                key = (str(kind), int(price), str(tif))
                if key in seen_attempt_keys:
                    return
                seen_attempt_keys.add(key)
                attempts.append({
                    "kind": str(kind),
                    "price": int(price),
                    "tif": str(tif),
                    "wait_s": int(max(0, wait_s)),
                    "passive_offset_cents": int(max(0, passive_offset_cents)),
                })

            is_high = edge_pct >= LIVE_EDGE_IMMEDIATE_AGGRESSIVE_PCT
            is_mid = edge_pct >= LIVE_EDGE_PASSIVE_THEN_AGGR_PCT
            is_aggressive_override = edge_pct >= LIVE_AGGRESSIVE_OVERRIDE_EDGE_PCT
            passive_wait_s = LIVE_PASSIVE_WAIT_SECONDS_MID if (is_high or is_mid) else LIVE_PASSIVE_WAIT_SECONDS_LOW
            passive_steps = LIVE_PASSIVE_REPRICE_STEPS_MID if (is_high or is_mid) else LIVE_PASSIVE_REPRICE_STEPS_LOW
            desired_passive_price = None
            desired_passive_count = 0
            if LIVE_PASSIVE_ALLOW_RESTING_LIMITS and LIVE_PASSIVE_RESCAN_MODE_ENABLED and (not is_aggressive_override):
                if LIVE_PASSIVE_ONE_TICK_FROM_ASK:
                    desired_passive_price = _compute_maker_one_tick_limit_price_cents(quotes, bet_side, LIVE_ORDER_FILL_MODE)
                else:
                    desired_passive_price = _compute_repriced_passive_limit_price_cents(quotes, bet_side, 0, LIVE_ORDER_FILL_MODE)
                if desired_passive_price is not None:
                    desired_passive_count = _compute_contract_count(stake_dollars, int(desired_passive_price))

            exchange_key = (ticker, str(order_side).lower(), "buy")
            exchange_resting = list(exchange_resting_by_key.get(exchange_key, []) or [])
            pending_order_id = str(row.get("pending_passive_order_id", "")).strip()
            if exchange_resting:
                def _resting_rank(order_obj: dict) -> Tuple[int, int, str]:
                    order_id_local = str(order_obj.get("order_id", "") or "").strip()
                    price_local = _kalshi_price_cents_from_order(order_obj, order_side)
                    exact_price = 1 if (desired_passive_price is not None and int(price_local) == int(desired_passive_price)) else 0
                    local_match = 1 if (pending_order_id and order_id_local == pending_order_id) else 0
                    created_local = str(order_obj.get("created_time", "") or order_obj.get("created_ts", "") or "")
                    return (local_match, exact_price, created_local)

                exchange_resting.sort(key=_resting_rank, reverse=True)
                survivor = exchange_resting[0]
                extras = exchange_resting[1:]
                cancel_failed = False
                for extra in extras:
                    extra_id = str(extra.get("order_id", "") or "").strip()
                    if not extra_id:
                        continue
                    canceled, cancel_err = kalshi_cancel_order(extra_id)
                    if not canceled:
                        row["pending_passive_cancel_error"] = str(cancel_err or "cancel duplicate failed")
                        state[sig] = row
                        cancel_failed = True
                        break
                if cancel_failed:
                    continue
                exchange_resting_by_key[exchange_key] = [survivor]
                _adopt_exchange_resting(
                    sig,
                    row,
                    survivor,
                    city_k,
                    side_k,
                    bet_side,
                    str(b.get("line", "") or ""),
                    str(b.get("date", "") or ""),
                    units,
                    stake_dollars,
                )
                row = state.get(sig, row) or {}
                pending_order_id = str(row.get("pending_passive_order_id", "")).strip()

            if pending_order_id:
                snapshot = kalshi_get_order_snapshot(pending_order_id)
                if bool(snapshot.get("ok")):
                    _record_pending_fill(sig, row, snapshot)
                    row = state.get(sig, row)
                    already = int(row.get("count", 0))
                    if already >= max(1, LIVE_MAX_ORDERS_PER_MARKET_PER_DAY):
                        _clear_pending_passive(sig)
                        continue
                    if _is_order_closed_status(str(snapshot.get("status", ""))):
                        _clear_pending_passive(sig)
                        row = state.get(sig, {})
                    else:
                        if edge_pct < POLICY_MIN_NET_EDGE_PCT:
                            _cancel_pending_passive_if_possible(sig, pending_order_id)
                            continue
                        # Conservative resting-order mode: keep exactly one live resting
                        # order on the book while the edge still qualifies. We do not
                        # cancel/reprice active resting orders intra-day, because losing
                        # track during cancel/replace can stack duplicates.
                        continue
                else:
                    snap_err = str(snapshot.get("error", "") or "snapshot failed")
                    if ("404" in snap_err) or ("not found" in snap_err.lower()):
                        _clear_pending_passive(sig)
                        row = state.get(sig, {}) or {}
                    else:
                        row = state.get(sig, row) or {}
                        row["pending_passive_snapshot_error"] = snap_err
                        state[sig] = row
                        continue

            if LIVE_PASSIVE_RESCAN_MODE_ENABLED and (not is_aggressive_override):
                p0 = desired_passive_price
                if p0 is None:
                    continue
                _append_attempt("passive", int(p0), LIVE_PASSIVE_TIME_IN_FORCE, 0, 0)
            elif LIVE_ALWAYS_PASSIVE_FIRST or is_mid or (not is_high):
                p0 = _compute_repriced_passive_limit_price_cents(quotes, bet_side, 0, LIVE_ORDER_FILL_MODE)
                if p0 is None:
                    continue
                _append_attempt("passive", int(p0), LIVE_PASSIVE_TIME_IN_FORCE, passive_wait_s, 0)
                if LIVE_PASSIVE_REPRICE_STEP_CENTS > 0 and passive_steps > 0:
                    for step_idx in range(1, int(passive_steps) + 1):
                        offset = int(step_idx) * int(LIVE_PASSIVE_REPRICE_STEP_CENTS)
                        p_next = _compute_repriced_passive_limit_price_cents(quotes, bet_side, offset, LIVE_ORDER_FILL_MODE)
                        if p_next is None:
                            continue
                        _append_attempt("passive", int(p_next), LIVE_PASSIVE_TIME_IN_FORCE, passive_wait_s, offset)

            # Aggressive fallback:
            # - high edge: always eligible (subject to edge check before submit)
            # - mid edge: only if spread is acceptable
            # - low edge: no aggressive fallback
            allow_aggressive = False
            if is_aggressive_override:
                allow_aggressive = True
            elif is_high:
                allow_aggressive = True
            elif is_mid and (spread_cents is None or spread_cents <= LIVE_AGGRESSIVE_MAX_SPREAD_CENTS):
                allow_aggressive = True

            if is_mid and (not is_high) and (not is_aggressive_override) and LIVE_MID_EDGE_MAKER_ONLY:
                allow_aggressive = False

            if allow_aggressive:
                aggressive_price = _compute_limit_price_cents(quotes, bet_side, LIVE_ORDER_FILL_MODE)
                if aggressive_price is not None:
                    _append_attempt("aggressive", int(aggressive_price), LIVE_ORDER_TIME_IN_FORCE, 0)

            if not attempts:
                continue
            target_count = _compute_contract_count(stake_dollars, int(attempts[0]["price"]))
            remaining_count = int(target_count)
            total_filled_count = 0
            total_fee_dollars = 0.0
            final_exec = None
            submitted_attempts: List[dict] = []
            for idx, a in enumerate(attempts):
                if remaining_count <= 0:
                    break
                if idx > 0:
                    ob = kalshi_get_orderbook(ticker)
                    quotes = best_quotes_from_orderbook(ob)
                    yes_bid = quotes.get("yes_bid")
                    yes_ask = quotes.get("yes_ask")
                    spread_cents = None
                    if yes_bid is not None and yes_ask is not None:
                        spread_cents = max(0, int(yes_ask) - int(yes_bid))
                    if a["kind"] == "aggressive":
                        p = _compute_limit_price_cents(quotes, bet_side, LIVE_ORDER_FILL_MODE)
                    else:
                        p = _compute_repriced_passive_limit_price_cents(
                            quotes,
                            bet_side,
                            int(a.get("passive_offset_cents", 0) or 0),
                            LIVE_ORDER_FILL_MODE,
                        )
                    if p is None:
                        continue
                    if a["kind"] == "aggressive":
                        edge_now = _net_edge_now_pct_for_side(
                            bet_side,
                            float(b.get("model_yes_prob_pct", 0.0)),
                            yes_bid,
                            yes_ask,
                        )
                        if edge_now is None or edge_now < POLICY_MIN_NET_EDGE_PCT:
                            final_exec = {
                                "status": "edge_gone",
                                "error": f"net edge dropped before aggressive fallback (edge_now={edge_now})",
                                "order_id": "",
                                "client_order_id": "",
                                "count": int(remaining_count),
                                "limit_price_cents": int(p),
                                "time_in_force": str(a.get("tif")),
                                "mode": str(a.get("kind")),
                                "filled": total_filled_count > 0,
                                "filled_all": False,
                                "fill_count": 0,
                                "order_status_raw": "skipped",
                                "spread_cents": spread_cents,
                            }
                            break
                    a = {**a, "price": int(p)}
                result = _submit_limit(int(a["price"]), str(a["tif"]), str(a["kind"]), spread_cents, desired_count=remaining_count)
                submitted_attempts.append(result)
                final_exec = result
                total_fee_dollars += float(result.get("fee_dollars", 0.0) or 0.0)
                got = int(result.get("fill_count", 0))
                total_filled_count += got
                remaining_count = max(0, int(target_count) - int(total_filled_count))
                if (
                    str(a.get("kind", "")).strip().lower() == "passive"
                    and LIVE_PASSIVE_ALLOW_RESTING_LIMITS
                    and LIVE_PASSIVE_RESCAN_MODE_ENABLED
                    and bool(result.get("order_id"))
                    and (not _is_order_closed_status(str(result.get("order_status_raw", ""))))
                    and remaining_count > 0
                ):
                    row = state.get(sig, {}) or {}
                    row["pending_passive_order_id"] = str(result.get("order_id", "") or "")
                    row["pending_passive_client_order_id"] = str(result.get("client_order_id", "") or "")
                    row["pending_passive_price_cents"] = int(result.get("limit_price_cents", 0) or 0)
                    row["pending_passive_requested_count"] = int(result.get("count", 0) or 0)
                    row["pending_passive_reported_fill_count"] = int(result.get("fill_count", 0) or 0)
                    row["pending_passive_fee_dollars"] = float(result.get("fee_dollars", 0.0) or 0.0)
                    row["pending_passive_created_ts_epoch"] = float(now_local.timestamp())
                    row["pending_passive_bet"] = bet_side
                    row["pending_passive_line"] = str(b.get("line", "") or "")
                    row["pending_passive_ticker"] = ticker
                    row["pending_passive_date"] = str(b.get("date", "") or "")
                    row["pending_passive_city"] = city_k
                    row["pending_passive_temp_side"] = side_k
                    row["pending_passive_units"] = units
                    row["pending_passive_stake_dollars"] = round(stake_dollars, 2)
                    row["pending_passive_order_action"] = str(result.get("order_action", "buy")).lower()
                    row["city"] = city_k
                    row["temp_side"] = side_k
                    state[sig] = row
                    current_bot_exposure_dollars = _current_live_bot_exposure_dollars(now_local, state)
                    final_exec = {
                        **result,
                        "status": "resting",
                        "filled": total_filled_count > 0,
                        "filled_all": False,
                    }
                    break
                if remaining_count <= 0:
                    break
                wait_s = int(a.get("wait_s", 0))
                if wait_s > 0 and idx < len(attempts) - 1:
                    time.sleep(wait_s)
                if idx < len(attempts) - 1 and LIVE_REQUIRE_CANCEL_BEFORE_AGGRESSIVE:
                    raw_status = str(result.get("order_status_raw", ""))
                    order_id_to_cancel = str(result.get("order_id", "") or "")
                    if order_id_to_cancel and not _is_order_closed_status(raw_status):
                        canceled, cancel_err = kalshi_cancel_order(order_id_to_cancel)
                        if not canceled:
                            final_exec = {
                                **result,
                                "status": "cancel_failed",
                                "error": f"cancel before aggressive failed: {cancel_err}",
                                "filled": total_filled_count > 0,
                                "filled_all": False,
                                "fill_count": 0,
                            }
                            break
            if final_exec is None:
                continue
            if total_filled_count > 0:
                final_exec["filled"] = True
                final_exec["filled_all"] = (remaining_count <= 0)
                final_exec["status"] = "submitted" if remaining_count <= 0 else "partial_filled"
            final_exec["fill_count_total"] = int(total_filled_count)
            final_exec["requested_count_total"] = int(target_count)
            final_exec["fee_dollars_total"] = float(total_fee_dollars)

            done_item = {
                "ts_est": fmt_est(now_local),
                "date": b.get("date"),
                "city": b.get("city"),
                "temp_type": b.get("temp_type"),
                "ticker": ticker,
                "bet": bet_side,
                "line": b.get("line"),
                "edge_pct": edge_pct,
                "units": units,
                "stake_dollars": round(stake_dollars, 2),
                "side": order_side,
                "limit_price_cents": final_exec.get("limit_price_cents"),
                "count": final_exec.get("fill_count_total", final_exec.get("count")),
                "time_in_force": final_exec.get("time_in_force"),
                "order_action": str(final_exec.get("order_action", "buy")).lower(),
                "status": final_exec.get("status"),
                "error": final_exec.get("error"),
                "fee_dollars": round(float(final_exec.get("fee_dollars_total", 0.0) or 0.0), 6),
                "order_id": final_exec.get("order_id"),
                "client_order_id": final_exec.get("client_order_id"),
                "execution_mode": final_exec.get("mode"),
                "attempt_count": len(submitted_attempts),
                "passive_attempted": any(str(x.get("mode", "")) == "passive" for x in submitted_attempts),
                "aggressive_attempted": any(str(x.get("mode", "")) == "aggressive" for x in submitted_attempts),
                "aggressive_used": str(final_exec.get("mode", "")) == "aggressive",
                "initial_limit_price_cents": (submitted_attempts[0].get("limit_price_cents") if submitted_attempts else final_exec.get("limit_price_cents")),
                "final_order_status_raw": final_exec.get("order_status_raw"),
            }
            _append_live_trade_log(done_item)
            done.append(done_item)
            if int(final_exec.get("fill_count_total", 0)) > 0:
                state[sig] = {
                    "count": already + 1,
                    "last_post_ts_epoch": now_local.timestamp(),
                    "city": city_k,
                    "temp_side": side_k,
                }
                current_bot_exposure_dollars = _current_live_bot_exposure_dollars(now_local, state)
                per_city_side[(city_k, side_k)] = per_city_side.get((city_k, side_k), 0) + 1
                total_orders += 1
                placed += 1
        except Exception as e:
            done_item = {
                "ts_est": fmt_est(now_local),
                "date": b.get("date"),
                "city": b.get("city"),
                "temp_type": b.get("temp_type"),
                "ticker": ticker,
                "bet": bet_side,
                "line": b.get("line"),
                "edge_pct": b.get("net_edge_pct"),
                "units": b.get("suggested_units"),
                "stake_dollars": "",
                "side": order_side,
                "limit_price_cents": "",
                "count": "",
                "time_in_force": LIVE_ORDER_TIME_IN_FORCE,
                "order_action": "buy",
                "status": "error",
                "error": str(e),
                "fee_dollars": "",
                "order_id": "",
                "client_order_id": "",
            }
            _append_live_trade_log(done_item)
            done.append(done_item)

    if done:
        executed_for_discord: List[dict] = []
        for r in done:
            st = str(r.get("status", "")).strip().lower()
            try:
                c = int(r.get("count", 0))
            except Exception:
                c = 0
            if st in ("submitted", "partial", "partial_filled") and c > 0:
                executed_for_discord.append(r)
        if executed_for_discord:
            chunk_size = 6
            max_rows = max(1, int(LIVE_MAX_ORDERS_PER_SCAN))
            rows = executed_for_discord[:max_rows]
            for i in range(0, len(rows), chunk_size):
                discord_send(_live_trade_text(now_local, rows[i:i + chunk_size]))
    _save_live_trade_state(today_key)
    return placed

def maybe_execute_live_exits(now_local: datetime) -> int:
    if not LIVE_TRADING_ENABLED or _live_kill_switch_state or not LIVE_EXIT_ENABLED:
        return 0
    if not kalshi_has_auth_config():
        return 0

    open_positions = _aggregate_open_live_positions(now_local)
    if not open_positions:
        return 0
    today_key = now_local.strftime("%Y-%m-%d")
    exit_state = _load_live_exit_state(today_key)

    grouped = refresh_markets_cache()
    placed = 0
    exit_logs: List[dict] = []
    detail_cache: Dict[Tuple[str, str, str], dict] = {}
    blocked_tickers = _manual_blocked_tickers()

    for pos in open_positions:
        if placed >= max(1, LIVE_EXIT_MAX_ORDERS_PER_SCAN):
            break
        first_ts = pos.get("first_entry_ts")
        if isinstance(first_ts, datetime):
            held_min = (now_local - first_ts).total_seconds() / 60.0
            if held_min < max(0, LIVE_EXIT_MIN_HOLD_MINUTES):
                continue

        city = str(pos.get("city", "")).strip()
        side = normalize_temp_side(str(pos.get("temp_type", "high")))
        market_date = str(pos.get("date", "")).strip()
        ticker = str(pos.get("ticker", "")).strip()
        if ticker in blocked_tickers:
            continue
        bet_side = str(pos.get("bet", "")).strip().upper()
        order_side, price_field = _bet_side_and_price_field(bet_side)
        if order_side is None or price_field is None:
            continue

        cache_key = (city, side, market_date)
        detail = detail_cache.get(cache_key)
        if detail is None:
            city_markets = [m for m in grouped.get(city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
            if not city_markets:
                continue
            by_date: Dict[str, List[Market]] = {}
            for m in city_markets:
                d = getattr(m, "market_date_iso", "") or parse_market_date_iso_from_ticker(m.ticker) or ""
                if d:
                    by_date.setdefault(d, []).append(m)
            if market_date not in by_date:
                continue
            consensus = build_expert_consensus(city, now_local, temp_side=side)
            if consensus is None:
                continue
            detail = build_city_bucket_comparison(city, by_date[market_date], now_local, temp_side=side, consensus_override=consensus)
            if detail is None:
                continue
            detail_cache[cache_key] = detail

        bucket_map = {str(b.get("ticker", "")).strip(): b for b in (detail.get("buckets", []) or [])}
        b_row = bucket_map.get(str(pos.get("ticker", "")).strip())
        if not b_row:
            continue
        yes_bid = _to_float(b_row.get("yes_bid"))
        yes_ask = _to_float(b_row.get("yes_ask"))
        if yes_bid is None or yes_ask is None:
            continue
        spread_cents = max(0, int(yes_ask) - int(yes_bid))
        if spread_cents > LIVE_EXIT_MAX_SPREAD_CENTS:
            continue
        model_yes_prob_pct = float(_to_float(b_row.get("source_yes_prob")) or 0.0) * 100.0
        edge_now = _net_edge_now_pct_for_side(bet_side, model_yes_prob_pct, yes_bid, yes_ask)
        if edge_now is None:
            continue
        lead_h_to_close = lead_hours_to_market_close(now_local, market_date)
        if (
            LIVE_EXIT_HOLD_TO_SETTLE_ENABLED and
            bet_side == "BUY NO" and
            lead_h_to_close is not None and
            0.0 <= float(lead_h_to_close) <= max(0.0, float(LIVE_EXIT_HOLD_TO_SETTLE_HOURS_BEFORE_CLOSE))
        ):
            hard_invalidation = (
                float(model_yes_prob_pct) >= float(LIVE_EXIT_HOLD_TO_SETTLE_MODEL_YES_INVALIDATION_PCT) or
                float(edge_now) <= float(LIVE_EXIT_HOLD_TO_SETTLE_EDGE_INVALIDATION_PCT)
            )
            if not hard_invalidation:
                continue
        entry_edge = float(pos.get("entry_edge_pct", 0.0))
        drop = entry_edge - float(edge_now)
        quotes = {
            "yes_bid": int(yes_bid),
            "yes_ask": int(yes_ask),
            "no_bid": int(100 - yes_ask),
            "no_ask": int(100 - yes_bid),
        }
        pnl_net = _estimate_unrealized_pnl_net_dollars(pos, quotes)
        open_count_total = max(1, int(pos.get("open_count", 0)))
        stake_open_dollars = (float(_to_float(pos.get("avg_entry_price_cents")) or 0.0) * float(open_count_total)) / 100.0

        pos_sig = "|".join([
            str(pos.get("date", "")).strip(),
            city,
            side,
            str(pos.get("ticker", "")).strip(),
            bet_side,
            str(pos.get("line", "")).strip(),
        ])
        prior = exit_state.get(pos_sig, {}) if isinstance(exit_state.get(pos_sig), dict) else {}
        partial_taken = bool(prior.get("partial_taken", False))
        exit_plan = ""  # "partial" | "full" | ""
        trigger_name = ""
        hysteresis_ok = True
        if LIVE_EXIT_HYSTERESIS_ENABLED:
            hysteresis_ok = float(drop) >= float(LIVE_EXIT_HYSTERESIS_MIN_DROP_PCT_POINTS)

        if LIVE_EDGE_DROP_EXIT_ENABLED and entry_edge > 0:
            edge_drop_points = float(entry_edge) - float(edge_now)
            edge_drop_triggered = edge_drop_points >= float(LIVE_EDGE_DROP_TRIGGER_PCT_POINTS)
            small_green_cap = max(
                0.0,
                float(stake_open_dollars) * max(0.0, float(LIVE_EDGE_DROP_SMALL_GREEN_MAX_PCT_OF_STAKE)) / 100.0,
            )
            if edge_drop_triggered and hysteresis_ok:
                if pnl_net is not None and float(pnl_net) < 0.0:
                    exit_plan = "full"
                    trigger_name = "edge_drop_red"
                elif (not partial_taken) and pnl_net is not None and float(pnl_net) <= small_green_cap:
                    exit_plan = "partial"
                    trigger_name = "edge_drop_small_green"
        else:
            hard_trigger = float(edge_now) <= LIVE_EXIT_EDGE_HARD_PCT
            soft_allowed = float(entry_edge) <= LIVE_EXIT_SOFT_MAX_ENTRY_EDGE_PCT
            soft_trigger = (
                soft_allowed and
                float(edge_now) <= LIVE_EXIT_EDGE_SOFT_PCT and
                drop >= LIVE_EXIT_EDGE_DROP_PCT
            )
            if not hysteresis_ok:
                soft_trigger = False
            if hard_trigger or soft_trigger:
                exit_plan = "full"
                trigger_name = ("hard" if hard_trigger else "soft")

        should_exit = bool(exit_plan)
        streak = int(prior.get("streak", 0))
        candidate_since = parse_ts_est(str(prior.get("candidate_since_ts_est", "")))
        if candidate_since is None:
            candidate_since = now_local
        dwell_minutes = max(0.0, (now_local - candidate_since).total_seconds() / 60.0)
        streak = streak + 1 if should_exit else 0
        if should_exit:
            exit_state[pos_sig] = {
                "streak": streak,
                "last_ts_est": fmt_est(now_local),
                "candidate_since_ts_est": fmt_est(candidate_since),
                "edge_now_pct": round(float(edge_now), 4),
                "entry_edge_pct": round(float(entry_edge), 4),
                "trigger": trigger_name,
                "partial_taken": partial_taken,
            }
        else:
            if partial_taken:
                exit_state[pos_sig] = {
                    "streak": 0,
                    "last_ts_est": fmt_est(now_local),
                    "edge_now_pct": round(float(edge_now), 4),
                    "entry_edge_pct": round(float(entry_edge), 4),
                    "trigger": "",
                    "candidate_since_ts_est": "",
                    "partial_taken": True,
                }
            elif pos_sig in exit_state:
                del exit_state[pos_sig]
        if not should_exit:
            continue
        if streak < max(1, LIVE_EXIT_CONSECUTIVE_SCANS):
            continue
        if dwell_minutes < max(0.0, float(LIVE_EXIT_CONSECUTIVE_MINUTES)):
            continue

        if exit_plan == "partial":
            frac = clamp(float(LIVE_EDGE_DROP_PARTIAL_SELL_FRACTION), 0.01, 0.99)
            target_count = max(1, int(math.ceil(float(open_count_total) * frac)))
            if open_count_total > 1:
                target_count = min(target_count, open_count_total - 1)
        else:
            target_count = open_count_total
        remaining = max(1, min(LIVE_MAX_CONTRACTS_PER_ORDER, int(target_count)))

        if LIVE_EXIT_ONLY_WHEN_LOSING and not LIVE_EDGE_DROP_EXIT_ENABLED:
            losing_now = _is_open_position_currently_losing(pos, quotes)
            if losing_now is not True:
                continue

        attempts: List[dict] = []
        p0 = _compute_sell_passive_price_cents(quotes, order_side)
        if p0 is not None:
            attempts.append({"kind": "passive", "price": int(p0), "tif": LIVE_EXIT_PASSIVE_TIME_IN_FORCE, "wait_s": LIVE_EXIT_PASSIVE_WAIT_SECONDS})
            for step_idx in range(1, max(0, LIVE_EXIT_PASSIVE_REPRICE_STEPS) + 1):
                p_next = _compute_repriced_passive_sell_price_cents(
                    quotes, order_side, step_idx * max(0, LIVE_EXIT_PASSIVE_REPRICE_STEP_CENTS), LIVE_ORDER_FILL_MODE
                )
                if p_next is not None:
                    attempts.append({"kind": "passive", "price": int(p_next), "tif": LIVE_EXIT_PASSIVE_TIME_IN_FORCE, "wait_s": LIVE_EXIT_PASSIVE_WAIT_SECONDS})
        aggressive_gate = True
        if LIVE_EDGE_DROP_EXIT_ENABLED and should_exit:
            worsened_need = max(0.0, float(LIVE_EDGE_DROP_AGGRESSIVE_WORSEN_PCT_POINTS))
            aggressive_gate = float(edge_now) <= (float(entry_edge) - float(LIVE_EDGE_DROP_TRIGGER_PCT_POINTS) - worsened_need)
        if LIVE_EXIT_AGGRESSIVE_FALLBACK_ENABLED:
            pa = _compute_sell_aggressive_price_cents(quotes, order_side, LIVE_ORDER_FILL_MODE)
            if pa is not None:
                attempts.append({"kind": "aggressive", "price": int(pa), "tif": LIVE_EXIT_AGGRESSIVE_TIME_IN_FORCE, "wait_s": 0})
        if not attempts:
            continue

        total_filled = 0
        total_fee = 0.0
        final_exec = None
        submitted_attempts: List[dict] = []

        for idx, a in enumerate(attempts):
            if remaining <= 0:
                break
            if str(a.get("kind", "")).strip().lower() == "aggressive" and LIVE_EDGE_DROP_EXIT_ENABLED and not aggressive_gate:
                # Re-check worsening before escalating to aggressive exit.
                ob_now = kalshi_get_orderbook(ticker)
                yb_now, ya_now, _sz_now = best_bid_and_ask_from_orderbook(ob_now)
                if yb_now is not None and ya_now is not None:
                    edge_now_latest = _net_edge_now_pct_for_side(
                        bet_side,
                        model_yes_prob_pct,
                        float(yb_now),
                        float(ya_now),
                    )
                    worsened_need = max(0.0, float(LIVE_EDGE_DROP_AGGRESSIVE_WORSEN_PCT_POINTS))
                    if edge_now_latest is not None:
                        aggressive_gate = float(edge_now_latest) <= (
                            float(entry_edge) - float(LIVE_EDGE_DROP_TRIGGER_PCT_POINTS) - worsened_need
                        )
                if not aggressive_gate:
                    continue
            tif_norm = sanitize_time_in_force_for_order(
                str(a["tif"]),
                default=("fill_or_kill" if str(a.get("kind", "")).strip().lower() == "aggressive" else "good_till_canceled"),
            )
            payload = {
                "ticker": ticker,
                "client_order_id": f"bot-exit-{market_date}-{uuid.uuid4().hex[:10]}",
                "action": "sell",
                "side": order_side,
                "count": int(remaining),
                price_field: int(a["price"]),
                "time_in_force": tif_norm,
            }
            if tif_norm == "good_til_date":
                payload["expiration_ts"] = int(time.time()) + max(5, LIVE_ORDER_EXPIRATION_SECONDS)
            resp = kalshi_post("/portfolio/orders", payload, timeout=20, max_retries=2)
            err = str(resp.get("error", "") or "")
            order_id = str(resp.get("order", {}).get("order_id") or resp.get("order_id") or "")
            order_status, fill_count = _response_order_meta(resp)
            fill_count = max(0, min(int(remaining), int(fill_count)))
            fee_d = _extract_fee_dollars_from_order_response(resp)
            total_fee += float(fee_d)
            total_filled += int(fill_count)
            remaining = max(0, int(target_count) - int(total_filled))
            status_local = "rejected" if err else ("submitted" if fill_count > 0 else "not_filled")
            final_exec = {
                "status": status_local,
                "error": err,
                "order_id": order_id,
                "client_order_id": payload["client_order_id"],
                "order_action": "sell",
                "count": int(fill_count),
                "limit_price_cents": int(a["price"]),
                "time_in_force": tif_norm,
                "mode": str(a["kind"]),
                "order_status_raw": order_status,
            }
            submitted_attempts.append(final_exec)
            if remaining <= 0:
                break
            wait_s = int(a.get("wait_s", 0))
            if wait_s > 0 and idx < len(attempts) - 1:
                time.sleep(wait_s)
            if idx < len(attempts) - 1 and LIVE_EXIT_REQUIRE_CANCEL_BEFORE_AGGRESSIVE:
                if order_id and not _is_order_closed_status(order_status):
                    canceled, cancel_err = kalshi_cancel_order(order_id)
                    if not canceled:
                        final_exec["status"] = "cancel_failed"
                        final_exec["error"] = f"cancel before aggressive failed: {cancel_err}"
                        break

        if final_exec is None:
            continue
        done_item = {
            "ts_est": fmt_est(now_local),
            "date": pos.get("date"),
            "city": city,
            "temp_type": side,
            "ticker": ticker,
            "bet": bet_side,
            "line": pos.get("line"),
            "edge_pct": round(float(edge_now), 2),
            "units": target_count,
            "stake_dollars": "",
            "side": order_side,
            "limit_price_cents": final_exec.get("limit_price_cents"),
            "count": int(total_filled),
            "time_in_force": final_exec.get("time_in_force"),
            "order_action": "sell",
            "status": ("submitted" if total_filled > 0 else final_exec.get("status")),
            "error": final_exec.get("error"),
            "fee_dollars": round(float(total_fee), 6),
            "order_id": final_exec.get("order_id"),
            "client_order_id": final_exec.get("client_order_id"),
            "execution_mode": final_exec.get("mode"),
            "attempt_count": len(submitted_attempts),
            "passive_attempted": any(str(x.get("mode", "")) == "passive" for x in submitted_attempts),
            "aggressive_attempted": any(str(x.get("mode", "")) == "aggressive" for x in submitted_attempts),
            "aggressive_used": str(final_exec.get("mode", "")) == "aggressive",
            "initial_limit_price_cents": (submitted_attempts[0].get("limit_price_cents") if submitted_attempts else final_exec.get("limit_price_cents")),
            "final_order_status_raw": final_exec.get("order_status_raw"),
        }
        _append_live_trade_log(done_item)
        if int(total_filled) > 0:
            placed += 1
            exit_logs.append(done_item)
            if exit_plan == "partial":
                exit_state[pos_sig] = {
                    "streak": 0,
                    "last_ts_est": fmt_est(now_local),
                    "edge_now_pct": round(float(edge_now), 4),
                    "entry_edge_pct": round(float(entry_edge), 4),
                    "trigger": trigger_name,
                    "candidate_since_ts_est": "",
                    "partial_taken": True,
                }
            elif pos_sig in exit_state:
                del exit_state[pos_sig]

    if exit_logs:
        chunk_size = 6
        for i in range(0, len(exit_logs), chunk_size):
            discord_send(_live_trade_text(now_local, exit_logs[i:i + chunk_size]))
    _save_live_exit_state(today_key)
    return placed

SNAPSHOT_HEADER_V1 = [
    "ts_est", "date_est", "city", "temp_side", "station", "market_day",
    "market_date_selected", "consensus_mu_f", "consensus_sigma_f",
    "best_ticker", "best_side", "best_edge", "best_bucket_label",
    "best_lo", "best_hi", "kalshi_yes_prob", "model_yes_prob", "yes_bid", "yes_ask",
]
SNAPSHOT_HEADER_V2 = [
    "ts_est", "date_est", "city", "temp_side", "station", "market_day",
    "market_date_selected", "consensus_mu_f", "consensus_sigma_f",
    "lead_hours_to_close", "bucket_count", "source_values_json", "source_weights_json",
    "best_ticker", "best_side", "best_edge", "best_bucket_label",
    "best_lo", "best_hi", "kalshi_yes_prob", "model_yes_prob", "spread_cents", "top_size",
    "yes_bid", "yes_ask",
]

def _normalize_snapshot_row_by_length(cols: List[str]) -> Optional[dict]:
    if not cols:
        return None
    if len(cols) >= len(SNAPSHOT_HEADER_V2):
        vals = cols[:len(SNAPSHOT_HEADER_V2)]
        return {SNAPSHOT_HEADER_V2[i]: vals[i] for i in range(len(SNAPSHOT_HEADER_V2))}
    if len(cols) >= len(SNAPSHOT_HEADER_V1):
        out = {k: "" for k in SNAPSHOT_HEADER_V2}
        for i, k in enumerate(SNAPSHOT_HEADER_V1):
            out[k] = cols[i]
        return out
    return None

def load_snapshot_rows_filtered(date: Optional[str] = None, city: Optional[str] = None, temp_side: Optional[str] = None) -> List[dict]:
    path = snapshot_log_path()
    if not os.path.exists(path):
        return []
    out: List[dict] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for idx, cols in enumerate(reader):
            if idx == 0 and cols and cols[0] == "ts_est":
                continue
            row = _normalize_snapshot_row_by_length(cols)
            if row is None:
                continue
            if date and row.get("date_est") != date:
                continue
            if city and row.get("city") != city:
                continue
            if temp_side and normalize_temp_side(row.get("temp_side", "")) != normalize_temp_side(temp_side):
                continue
            out.append(row)
    return out

def ensure_snapshot_log_header() -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = snapshot_log_path()
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(SNAPSHOT_HEADER_V2)

def append_snapshot_row(row: dict) -> None:
    ensure_snapshot_log_header()
    path = snapshot_log_path()
    ordered = [
        row.get("ts_est"), row.get("date_est"), row.get("city"), row.get("temp_side"), row.get("station"), row.get("market_day"),
        row.get("market_date_selected"), row.get("consensus_mu_f"), row.get("consensus_sigma_f"),
        row.get("lead_hours_to_close"), row.get("bucket_count"), row.get("source_values_json"), row.get("source_weights_json"),
        row.get("best_ticker"), row.get("best_side"), row.get("best_edge"), row.get("best_bucket_label"),
        row.get("best_lo"), row.get("best_hi"), row.get("kalshi_yes_prob"), row.get("model_yes_prob"), row.get("spread_cents"), row.get("top_size"),
        row.get("yes_bid"), row.get("yes_ask"),
    ]
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(ordered)

def record_snapshot_metrics(now_local: datetime, market_day: str = "today") -> None:
    if not SNAPSHOT_LOGGING_ENABLED:
        return
    grouped = refresh_markets_cache()
    for city in CITY_CONFIG.keys():
        for side in ("high", "low"):
            city_markets = [m for m in grouped.get(city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
            if not city_markets:
                continue
            selected, selected_date, _ = select_markets_for_day(
                city_markets,
                now_local,
                market_day,
                city=city,
            )
            if not selected:
                continue
            detail = build_city_bucket_comparison(city, selected, now_local, temp_side=side)
            if detail is None or not detail.get("buckets"):
                continue
            best = max(detail["buckets"], key=lambda r: r.get("best_edge", -1.0))
            lead_h = lead_hours_to_market_close(now_local, selected_date)
            append_snapshot_row({
                "ts_est": fmt_est(now_local),
                "date_est": now_local.date().isoformat(),
                "city": city,
                "temp_side": side,
                "station": CITY_CONFIG[city]["station"],
                "market_day": market_day,
                "market_date_selected": selected_date,
                "consensus_mu_f": detail.get("consensus_mu_f"),
                "consensus_sigma_f": detail.get("consensus_sigma_f"),
                "lead_hours_to_close": lead_h,
                "bucket_count": detail.get("bucket_count"),
                "source_values_json": json.dumps(detail.get("source_values_map", {}), sort_keys=True),
                "source_weights_json": json.dumps(detail.get("source_weights_map", {}), sort_keys=True),
                "best_ticker": best.get("ticker"),
                "best_side": best.get("best_side"),
                "best_edge": best.get("best_edge"),
                "best_bucket_label": best.get("bucket_label"),
                "best_lo": best.get("lo"),
                "best_hi": best.get("hi"),
                "kalshi_yes_prob": best.get("kalshi_yes_prob"),
                "model_yes_prob": best.get("source_yes_prob"),
                "spread_cents": best.get("spread_cents"),
                "top_size": best.get("top_size"),
                "yes_bid": best.get("yes_bid"),
                "yes_ask": best.get("yes_ask"),
            })

def nws_day_outcome_f(station_id: str, day_iso: str, temp_side: str) -> Optional[float]:
    feats = nws_get_recent_observations(station_id, limit=500)
    # Resolve station -> city so we evaluate the contract day in that city's LST.
    city_for_station = None
    st_norm = str(station_id or "").strip().upper()
    for city_name, cfg in CITY_CONFIG.items():
        if str(cfg.get("station", "")).strip().upper() == st_norm:
            city_for_station = city_name
            break
    obs_tz = city_lst_tz(city_for_station)
    vals: List[float] = []
    for f in feats:
        props = f.get("properties", {})
        ts = props.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(obs_tz)
        except Exception:
            continue
        if dt.date().isoformat() != day_iso:
            continue
        temp_c = props.get("temperature", {}).get("value")
        if temp_c is None:
            continue
        vals.append(f_from_c(float(temp_c)))
    if not vals:
        return None
    return max(vals) if normalize_temp_side(temp_side) == "high" else min(vals)

def _cli_location_code(cli_code: str) -> str:
    c = (cli_code or "").strip().upper()
    # Some Kalshi/NWS CLI identifiers are not the exact weather.gov location key.
    aliases = {
        "CLICHI": "MDW",
        "CHI": "MDW",
        "CLIPHIL": "PHL",
        "PHIL": "PHL",
    }
    if c in aliases:
        return aliases[c]
    if c.startswith("CLI") and len(c) > 3:
        tail = c[3:]
        return aliases.get(tail, tail)
    return c

def nws_list_cli_product_ids(location_code: str, limit: int = 40) -> List[str]:
    headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
    url = f"https://api.weather.gov/products/types/CLI/locations/{location_code}"
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    payload = r.json()
    ids: List[str] = []
    for row in payload.get("@graph", []) or []:
        pid = str(row.get("id", "")).strip()
        if pid:
            ids.append(pid)
    return ids

def nws_get_product_text(product_id: str) -> str:
    headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
    url = f"https://api.weather.gov/products/{product_id}"
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    payload = r.json()
    text = payload.get("productText")
    if not isinstance(text, str):
        text = ""
    return text

def parse_cli_final_product(text: str) -> Optional[dict]:
    raw = str(text or "")
    if not raw:
        return None
    upper = raw.upper()
    date_re = re.search(
        r"(?:CLIMATE SUMMARY|CLIMATE REPORT)[^\n]*?FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})",
        upper,
        re.IGNORECASE,
    )
    if not date_re:
        return None
    mon_name = date_re.group(1).upper()
    day = int(date_re.group(2))
    year = int(date_re.group(3))
    month_map = {
        "JAN": 1, "JANUARY": 1,
        "FEB": 2, "FEBRUARY": 2,
        "MAR": 3, "MARCH": 3,
        "APR": 4, "APRIL": 4,
        "MAY": 5,
        "JUN": 6, "JUNE": 6,
        "JUL": 7, "JULY": 7,
        "AUG": 8, "AUGUST": 8,
        "SEP": 9, "SEPT": 9, "SEPTEMBER": 9,
        "OCT": 10, "OCTOBER": 10,
        "NOV": 11, "NOVEMBER": 11,
        "DEC": 12, "DECEMBER": 12,
    }
    mm = month_map.get(mon_name)
    if mm is None:
        return None
    try:
        date_iso = datetime(year, mm, day).date().isoformat()
    except Exception:
        return None
    max_re = re.search(r"^\s*MAXIMUM\s+(-?\d+)", upper, re.MULTILINE)
    min_re = re.search(r"^\s*MINIMUM\s+(-?\d+)", upper, re.MULTILINE)
    max_f = float(max_re.group(1)) if max_re else None
    min_f = float(min_re.group(1)) if min_re else None
    return {"date": date_iso, "high_f": max_f, "low_f": min_f}

def nws_cli_final_for_date(cli_code: str, date_iso: str, limit: int = 40) -> Optional[dict]:
    loc = _cli_location_code(cli_code)
    if not loc:
        return None
    ids = nws_list_cli_product_ids(loc, limit=limit)
    for pid in ids:
        text = nws_get_product_text(pid)
        parsed = parse_cli_final_product(text)
        if parsed is None:
            continue
        if parsed.get("date") == date_iso:
            out = dict(parsed)
            out["location"] = loc
            out["product_id"] = pid
            return out
    return None

def _to_float(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None

def american_odds_from_prob(p: float) -> Optional[int]:
    if p <= 0.0 or p >= 1.0:
        return None
    if p >= 0.5:
        return int(round(-100.0 * p / max(1e-9, 1.0 - p)))
    return int(round(100.0 * (1.0 - p) / max(1e-9, p)))

def _bin_label(x: float, edges: List[float], labels: List[str]) -> str:
    for i in range(len(edges) - 1):
        if edges[i] <= x < edges[i + 1]:
            return labels[i]
    return labels[-1]

def _bucket_yes_from_outcome(outcome_f: float, lo: float, hi: float) -> bool:
    return outcome_f >= lo and outcome_f <= hi

def ensure_final_settlements_header() -> None:
    os.makedirs(SNAPSHOT_LOG_DIR, exist_ok=True)
    path = final_settlements_path()
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "city", "temp_side", "station", "outcome_f", "source", "updated_ts_est"])

def load_final_settlement_map() -> Dict[Tuple[str, str, str], dict]:
    ensure_final_settlements_header()
    path = final_settlements_path()
    out: Dict[Tuple[str, str, str], dict] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            key = (str(r.get("date", "")), str(r.get("city", "")), normalize_temp_side(str(r.get("temp_side", "high"))))
            out[key] = r
    return out

def upsert_final_settlements(rows: List[dict]) -> None:
    ensure_final_settlements_header()
    path = final_settlements_path()
    existing = load_final_settlement_map()
    for r in rows:
        key = (str(r.get("date", "")), str(r.get("city", "")), normalize_temp_side(str(r.get("temp_side", "high"))))
        prev = existing.get(key)
        prev_source = str((prev or {}).get("source", "")).strip().lower()
        new_source = str(r.get("source", "")).strip().lower()
        if prev is not None and prev_source == "cli_final" and new_source != "cli_final":
            continue
        existing[key] = {
            "date": r.get("date"),
            "city": r.get("city"),
            "temp_side": normalize_temp_side(str(r.get("temp_side", "high"))),
            "station": r.get("station"),
            "outcome_f": r.get("outcome_f"),
            "source": r.get("source"),
            "updated_ts_est": r.get("updated_ts_est"),
        }
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "city", "temp_side", "station", "outcome_f", "source", "updated_ts_est"])
        for _, r in sorted(existing.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
            w.writerow([r.get("date"), r.get("city"), r.get("temp_side"), r.get("station"), r.get("outcome_f"), r.get("source"), r.get("updated_ts_est")])

def get_outcome_f(date_iso: str, city: str, temp_side: str, station: str) -> Optional[float]:
    side = normalize_temp_side(temp_side)
    key = (date_iso, city, side)
    m = load_final_settlement_map()
    row = m.get(key)
    if row is not None:
        v = _to_float(row.get("outcome_f"))
        if v is not None:
            return v
    # Fallback proxy from observations when no finalized settlement stored yet.
    return nws_day_outcome_f(station, date_iso, side)

def get_final_outcome_f(date_iso: str, city: str, temp_side: str) -> Optional[float]:
    side = normalize_temp_side(temp_side)
    key = (date_iso, city, side)
    m = load_final_settlement_map()
    row = m.get(key)
    if row is None:
        return None
    return _to_float(row.get("outcome_f"))

def lead_time_bin(lead_hours: Optional[float]) -> str:
    if lead_hours is None:
        return "unknown"
    h = float(lead_hours)
    if h < 0:
        return "post"
    if h <= 2:
        return "0-2h"
    if h <= 4:
        return "2-4h"
    if h <= 8:
        return "4-8h"
    if h <= 16:
        return "8-16h"
    return "16h+"

def parse_ts_est(ts_est: str) -> Optional[datetime]:
    s = str(ts_est or "").strip()
    if not s:
        return None
    try:
        dt_naive = datetime.strptime(s, "%Y-%m-%d %I:%M:%S %p EST")
        est_tz = tz.tzoffset("EST", -5 * 3600)
        return dt_naive.replace(tzinfo=est_tz).astimezone(LOCAL_TZ)
    except Exception:
        return None

def effective_market_date_iso(r: dict) -> Optional[str]:
    d = str(r.get("market_date_selected", "")).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", d):
        return d
    d2 = str(r.get("date_est", "")).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", d2):
        return d2
    return None

def infer_lead_hours(r: dict) -> Optional[float]:
    lead_h = _to_float(r.get("lead_hours_to_close"))
    if lead_h is not None:
        return lead_h
    d = effective_market_date_iso(r)
    ts = parse_ts_est(str(r.get("ts_est", "")))
    if not d or ts is None:
        return None
    return lead_hours_to_market_close(ts, d)

def implied_market_win_prob(best_side: str, yes_bid: Optional[float], yes_ask: Optional[float]) -> Optional[float]:
    yb = _to_float(yes_bid)
    ya = _to_float(yes_ask)
    if yb is None or ya is None:
        return None
    if "YES" in str(best_side or ""):
        return clamp(ya / 100.0, 0.0, 1.0)
    return clamp(1.0 - (yb / 100.0), 0.0, 1.0)

def dedupe_snapshot_rows(rows: List[dict]) -> List[dict]:
    keep: Dict[Tuple[str, str, str, str], dict] = {}
    for r in rows:
        d = effective_market_date_iso(r) or ""
        city = str(r.get("city", ""))
        side = normalize_temp_side(str(r.get("temp_side", "high")))
        ticker = str(r.get("best_ticker", ""))
        if not d or not city or not ticker:
            continue
        key = (d, city, side, ticker)
        old = keep.get(key)
        if old is None:
            keep[key] = r
            continue
        old_ts = parse_ts_est(str(old.get("ts_est", "")))
        new_ts = parse_ts_est(str(r.get("ts_est", "")))
        if old_ts is None and new_ts is not None:
            keep[key] = r
        elif old_ts is not None and new_ts is not None and new_ts > old_ts:
            keep[key] = r
    return list(keep.values())

def build_calibration_tables() -> dict:
    path = snapshot_log_path()
    if not os.path.exists(path):
        return {"city_side_lead": {}, "side_lead": {}, "global": None}
    m_final = load_final_settlement_map()
    if not m_final:
        return {"city_side_lead": {}, "side_lead": {}, "global": None}

    raw_rows: List[dict] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            raw_rows.append(r)
    rows = dedupe_snapshot_rows(raw_rows)

    groups_city: Dict[Tuple[str, str, str], List[dict]] = {}
    groups_side: Dict[Tuple[str, str], List[dict]] = {}
    all_samples: List[dict] = []
    for r in rows:
        city = str(r.get("city", ""))
        side = normalize_temp_side(str(r.get("temp_side", "high")))
        if side == "low" and not LOW_SIGNALS_ENABLED:
            continue
        date_iso = effective_market_date_iso(r) or ""
        station = str(r.get("station", ""))
        final_row = m_final.get((date_iso, city, side))
        if final_row is None:
            continue
        outcome = _to_float(final_row.get("outcome_f"))
        lo = _to_float(r.get("best_lo"))
        hi = _to_float(r.get("best_hi"))
        yes_bid = _to_float(r.get("yes_bid"))
        yes_ask = _to_float(r.get("yes_ask"))
        raw_edge = _to_float(r.get("best_edge"))
        best_side = str(r.get("best_side", ""))
        lead_h = infer_lead_hours(r)
        if outcome is None or lo is None or hi is None or yes_bid is None or yes_ask is None or raw_edge is None:
            continue
        market_win_prob = implied_market_win_prob(best_side, yes_bid, yes_ask)
        if market_win_prob is None:
            continue
        if market_win_prob < NO_TRADE_IMPLIED_PROB_MIN or market_win_prob > NO_TRADE_IMPLIED_PROB_MAX:
            continue
        yes_outcome = _bucket_yes_from_outcome(outcome, lo, hi)
        bet_wins = yes_outcome if "YES" in best_side else (not yes_outcome)
        sample = {
            "win": 1.0 if bet_wins else 0.0,
            "market_win_prob": market_win_prob,
            "raw_edge": raw_edge,
            "lead_bin": lead_time_bin(lead_h),
            "city": city,
            "side": side,
        }
        all_samples.append(sample)
        groups_city.setdefault((city, side, sample["lead_bin"]), []).append(sample)
        groups_side.setdefault((side, sample["lead_bin"]), []).append(sample)

    def agg(samples: List[dict]) -> Optional[dict]:
        n = len(samples)
        if n == 0:
            return None
        avg_win = sum(s["win"] for s in samples) / n
        avg_market = sum(s["market_win_prob"] for s in samples) / n
        avg_raw_edge = sum(s["raw_edge"] for s in samples) / n
        empirical_edge = avg_win - avg_market
        if abs(avg_raw_edge) < 1e-9:
            shrink = 0.0
        else:
            shrink = empirical_edge / avg_raw_edge
        shrink = max(0.0, min(2.0, shrink))
        return {
            "n": n,
            "avg_win": avg_win,
            "avg_market": avg_market,
            "avg_raw_edge": avg_raw_edge,
            "empirical_edge": empirical_edge,
            "shrink": shrink,
        }

    city_table = {k: agg(v) for k, v in groups_city.items()}
    side_table = {k: agg(v) for k, v in groups_side.items()}
    global_row = agg(all_samples)
    return {"city_side_lead": city_table, "side_lead": side_table, "global": global_row}

def calibrate_edge(raw_edge: float, city: str, side: str, lead_hours: Optional[float], tables: dict) -> Tuple[float, dict]:
    if not CALIBRATION_ENABLED:
        return raw_edge, {"source": "disabled", "shrink": 1.0}
    lb = lead_time_bin(lead_hours)
    city_key = (city, side, lb)
    side_key = (side, lb)
    ctab = tables.get("city_side_lead", {})
    stab = tables.get("side_lead", {})
    grow = tables.get("global")

    row = ctab.get(city_key)
    if row and row.get("n", 0) >= CALIBRATION_MIN_SAMPLES:
        shrink = float(row.get("shrink", 0.0))
        return raw_edge * shrink, {"source": "city_side_lead", "n": row.get("n"), "lead_bin": lb, "shrink": shrink}
    row = stab.get(side_key)
    if row and row.get("n", 0) >= CALIBRATION_MIN_SAMPLES:
        shrink = float(row.get("shrink", 0.0))
        return raw_edge * shrink, {"source": "side_lead", "n": row.get("n"), "lead_bin": lb, "shrink": shrink}
    if grow and grow.get("n", 0) >= CALIBRATION_MIN_SAMPLES:
        shrink = float(grow.get("shrink", 0.0))
        return raw_edge * shrink, {"source": "global", "n": grow.get("n"), "lead_bin": lb, "shrink": shrink}
    return raw_edge, {"source": "insufficient_data", "lead_bin": lb, "shrink": 1.0}


# -----------------------
# FastAPI + background loop
# -----------------------
app = FastAPI(title="Kalshi Weather EV Alerts", version="1.0.0")

@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Kalshi Weather Trading Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #edf1f7;
      --ink: #0f2135;
      --muted: #546b86;
      --card: #ffffff;
      --line: #d2dce9;
      --accent: #0e5a9b;
      --accent-2: #0b8a73;
      --red: #bc2f45;
      --good: #1f8d57;
      --warn: #9e6a18;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Manrope", "Avenir Next", "Trebuchet MS", sans-serif;
      background:
        radial-gradient(1000px 540px at -8% -18%, #d7e5fa 0%, transparent 57%),
        radial-gradient(980px 520px at 108% -16%, #d8f0e8 0%, transparent 58%),
        var(--bg);
    }
    .wrap { max-width: 1260px; margin: 0 auto; padding: 18px 14px 32px; }
    .hero {
      border: 1px solid var(--line);
      border-radius: 16px;
      background: linear-gradient(125deg, #f8fbff, #f4fffb);
      padding: 16px;
      box-shadow: 0 12px 28px #0e1f3a14;
    }
    h1 { margin: 0; font-size: 40px; letter-spacing: 0.2px; font-weight: 800; }
    .sub { color: var(--muted); margin-top: 6px; }
    .top-grid {
      margin-top: 12px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
    }
    .stat {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: linear-gradient(180deg, #ffffff, #f9fcff);
      padding: 10px;
    }
    .k { font-size: 10px; text-transform: uppercase; color: var(--muted); letter-spacing: 0.6px; font-weight: 700; }
    .v { margin-top: 5px; font-size: 18px; font-weight: 700; word-break: break-word; }
    .v.good-tone { color: var(--good); }
    .v.bad-tone { color: var(--red); }
    .nav-grid {
      margin-top: 10px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
    }
    .nav-box {
      display: block;
      text-decoration: none;
      color: var(--ink);
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
      padding: 10px;
    }
    .nav-box b { display: block; font-size: 14px; }
    .nav-box span { color: var(--muted); font-size: 12px; }
    .tabs {
      margin-top: 10px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .tab-btn {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      border-radius: 999px;
      padding: 7px 12px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .tab-btn.active {
      border-color: var(--accent);
      color: var(--accent);
      background: linear-gradient(180deg, #f4fbff, #eef8ff);
      box-shadow: inset 0 0 0 1px #d7e6f7;
    }
    .tab-content { display: none; margin-top: 12px; }
    .tab-content.active { display: block; }
    .sections {
      display: grid;
      grid-template-columns: 0.9fr 1.1fr;
      gap: 12px;
      align-items: start;
    }
    .card {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--card);
      padding: 12px;
      box-shadow: 0 6px 18px #1233540c;
    }
    .card h2 { margin: 0 0 8px; font-size: 22px; letter-spacing: 0.1px; }
    .meta { color: var(--muted); font-size: 12px; }
    .criteria-panel summary {
      list-style: none;
      cursor: pointer;
      font-size: 22px;
      font-weight: 800;
      margin: 0 0 8px;
    }
    .criteria-panel summary::-webkit-details-marker { display: none; }
    .criteria-panel summary::after {
      content: "Show/Hide";
      float: right;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      margin-top: 6px;
    }
    .list {
      display: grid;
      gap: 7px;
      margin-top: 8px;
    }
    .item {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px;
      background: #fbfffc;
    }
    .item b { font-size: 13px; }
    .item span { display: block; color: var(--muted); font-size: 12px; margin-top: 3px; }
    .split { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .table-wrap {
      margin-top: 8px;
      border: 1px solid var(--line);
      border-radius: 10px;
      overflow: auto;
      max-height: 340px;
      background: #fff;
    }
    table { border-collapse: collapse; width: 100%; min-width: 760px; }
    th, td { padding: 8px; border-bottom: 1px solid #e8f0ea; text-align: left; font-size: 12px; }
    tbody tr:hover { background: #f5f9ff; }
    th {
      position: sticky; top: 0; z-index: 1;
      background: #f5faf6; color: var(--muted); text-transform: uppercase; letter-spacing: 0.4px;
      font-size: 11px;
    }
    th.sortable-th { cursor: pointer; user-select: none; }
    th.sortable-th::after { content: "  <>"; color: #9ab0a2; font-size: 10px; }
    th.sortable-th.active.asc::after { content: "  ^"; color: #2b6a47; }
    th.sortable-th.active.desc::after { content: "  v"; color: #2b6a47; }
    .pill {
      display: inline-block;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 11px;
      font-weight: 700;
    }
    .ok { background: #ddf6e5; color: #0d6a36; }
    .warn { background: #fff3d9; color: #6f5117; }
    .bad { background: #fde2e2; color: #8f1f1f; }
    .mono { font-family: "JetBrains Mono", "Consolas", "Courier New", monospace; }
    .row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 8px;
      margin-top: 8px;
    }
    .row .stat { padding: 8px; }
    .kpi-strip {
      margin-top: 10px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 8px;
    }
    .kpi {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px;
      background: linear-gradient(180deg, #ffffff, #f7fbff);
    }
    .kpi .label {
      color: var(--muted);
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      font-weight: 700;
    }
    .kpi .value {
      margin-top: 6px;
      font-size: 24px;
      font-weight: 800;
      line-height: 1;
    }
    .kpi .value.good-tone { color: var(--good); }
    .kpi .value.bad-tone { color: var(--red); }
    .chart-wrap {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px;
      background: #fbfffc;
      margin-top: 8px;
      position: relative;
    }
    #evChart { width: 100%; height: 260px; display: block; }
    .chart-tip {
      position: absolute;
      pointer-events: none;
      background: #0f2518f2;
      color: #eefaf0;
      border: 1px solid #2f5c44;
      border-radius: 8px;
      padding: 6px 8px;
      font-size: 11px;
      line-height: 1.35;
      box-shadow: 0 6px 16px #0a1a1029;
      transform: translate(8px, -8px);
      white-space: nowrap;
      z-index: 4;
    }
    .legend { margin-top: 6px; display: flex; gap: 12px; font-size: 12px; color: var(--muted); }
    .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 6px; }
    .blue { background: #1d4ed8; }
    .green { background: #15803d; }
    .toolbar { margin-top: 8px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .btn {
      border: 1px solid var(--line); background: #fff; color: var(--ink);
      border-radius: 9px; padding: 6px 10px; font-weight: 700; cursor: pointer; font-size: 12px;
    }
    .btn.active { border-color: var(--accent); color: var(--accent); }
    @media (max-width: 980px) {
      h1 { font-size: 30px; }
      .sections { grid-template-columns: 1fr; }
      .split { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <h1>Kalshi Weather Trade Control</h1>
      <div class="sub">Live criteria, model calibration, and EV vs realized progression in one view.</div>
      <div class="top-grid">
        <div class="stat"><div class="k">System</div><div class="v" id="sys">Loading</div></div>
        <div class="stat"><div class="k">As Of</div><div class="v" id="asOf">-</div></div>
        <div class="stat"><div class="k">Live Orders Today</div><div class="v" id="ordersToday">-</div></div>
        <div class="stat"><div class="k">Cities</div><div class="v" id="cityCount">-</div></div>
        <div class="stat"><div class="k">Host</div><div class="v mono" id="host">-</div></div>
      </div>
      <div class="tabs">
        <button class="tab-btn active" data-tab-target="overviewTab">Overview</button>
        <button class="tab-btn" data-tab-target="cityTab">City Attribution</button>
        <button class="tab-btn" data-tab-target="ladderTab">Edge Ladder</button>
        <button class="tab-btn" data-tab-target="evTab">EV Chart</button>
        <button class="tab-btn" data-tab-target="dailyTab">Daily Stats</button>
        <button class="tab-btn" data-tab-target="manualTab">Manual Positions</button>
      </div>
    </section>

    <section id="overviewTab" class="tab-content active">
    <section class="sections">
      <div class="card criteria-panel" id="criteria">
        <details open>
          <summary>Trade Criteria</summary>
          <div class="meta">Current live strategy parameters pulled from <span class="mono">/health</span>.</div>
          <div class="split">
            <div class="list" id="criteriaList"></div>
            <div class="list">
              <div class="item">
                <b>Ladder Sizing</b>
                <span id="ladderText">Loading...</span>
              </div>
              <div class="item">
                <b>Live Safety</b>
                <span id="safetyText">Loading...</span>
              </div>
            </div>
          </div>
        </details>
      </div>

      <div class="card" id="model">
        <h2>Model Quality</h2>
        <div class="meta">Calibration and attribution from <span class="mono">/analytics/live-insights</span>.</div>
        <div class="kpi-strip">
          <div class="kpi"><div class="label">Net P/L</div><div class="value" id="kpiNetPnl">-</div></div>
          <div class="kpi"><div class="label">EV Gap</div><div class="value" id="kpiEvGap">-</div></div>
          <div class="kpi"><div class="label">Reject Rate</div><div class="value" id="kpiRejectRate">-</div></div>
          <div class="kpi"><div class="label">Settled Win Rate</div><div class="value" id="kpiWinRate">-</div></div>
        </div>
        <div class="toolbar">
          <button class="btn active" data-model-days="7">Model 7D</button>
          <button class="btn" data-model-days="14">Model 14D</button>
          <button class="btn" data-model-days="30">Model 30D</button>
        </div>
        <div class="row">
          <div class="stat"><div class="k">Filled Positions</div><div class="v" id="modelFills">-</div></div>
          <div class="stat"><div class="k">Settled Positions</div><div class="v" id="modelSettled">-</div></div>
          <div class="stat"><div class="k">Settled Win Rate</div><div class="v" id="modelWinRate">-</div></div>
          <div class="stat"><div class="k">Avg Entry Edge</div><div class="v" id="modelAvgEdge">-</div></div>
          <div class="stat"><div class="k">Expected Net P/L</div><div class="v" id="modelExpected">-</div></div>
          <div class="stat"><div class="k">Realized Net P/L</div><div class="v" id="modelRealized">-</div></div>
          <div class="stat"><div class="k">Realized - Expected</div><div class="v" id="modelGap">-</div></div>
          <div class="stat"><div class="k">Order Reject Rate</div><div class="v" id="modelRejectRate">-</div></div>
        </div>
        <div class="meta" style="margin-top:8px;">Detailed city and ladder breakdowns are available in their dedicated tabs.</div>
        <div class="split" style="margin-top:8px;">
          <div class="item">
            <b>Opportunity Funnel</b>
            <span id="funnelText">Loading...</span>
          </div>
          <div class="item">
            <b>Source Health</b>
            <span id="sourceHealthText">Loading...</span>
          </div>
        </div>
        <div class="meta" style="margin-top:8px;">Account Reconciliation</div>
        <div class="row">
          <div class="stat"><div class="k">Deposits</div><div class="v" id="reconDeposits">-</div></div>
          <div class="stat"><div class="k">Equity</div><div class="v" id="reconEquity">-</div></div>
          <div class="stat"><div class="k">Cash</div><div class="v" id="reconCash">-</div></div>
          <div class="stat"><div class="k">Positions Value</div><div class="v" id="reconPositions">-</div></div>
          <div class="stat"><div class="k">Net P/L</div><div class="v" id="reconNet">-</div></div>
          <div class="stat"><div class="k">Bot Realized</div><div class="v" id="reconBotRealized">-</div></div>
          <div class="stat"><div class="k">Manual Realized</div><div class="v" id="reconManualRealized">-</div></div>
          <div class="stat"><div class="k">Unrealized/Residual</div><div class="v" id="reconResidual">-</div></div>
        </div>
        <div class="table-wrap" style="margin-top:8px;">
          <table>
            <thead><tr><th>Time</th><th>City</th><th>Ticker</th><th>Status</th><th>Error</th></tr></thead>
            <tbody id="errorRows"><tr><td colspan="5">Loading...</td></tr></tbody>
          </table>
        </div>
      </div>
    </section>
    </section>

    <section id="cityTab" class="tab-content">
    <section class="card" id="cityAttr">
      <h2>City Attribution</h2>
      <div class="meta">City x side performance sorted by realized P/L (best to worst).</div>
      <div class="toolbar">
        <button class="btn active" data-city-days="7">City 7D</button>
        <button class="btn" data-city-days="14">City 14D</button>
        <button class="btn" data-city-days="30">City 30D</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>City</th><th>Side</th><th>Fills</th><th>Settled</th><th>Avg Edge</th><th>Exp Win%</th><th>Act Win%</th><th>Expected</th><th>Realized</th></tr></thead>
          <tbody id="citySideRows"><tr><td colspan="9">Loading...</td></tr></tbody>
        </table>
      </div>
    </section>
    </section>

    <section id="ladderTab" class="tab-content">
    <section class="card" id="ladderAcc">
      <h2>Edge Ladder Accuracy</h2>
      <div class="meta">Edge bucket hit rates and realized performance.</div>
      <div class="toolbar">
        <button class="btn active" data-ladder-days="7">Ladder 7D</button>
        <button class="btn" data-ladder-days="14">Ladder 14D</button>
        <button class="btn" data-ladder-days="30">Ladder 30D</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th class="sortable-th" data-sort-table="ladder" data-sort-key="bucket" data-sort-type="bucket">Edge Bucket</th><th class="sortable-th" data-sort-table="ladder" data-sort-key="n" data-sort-type="number">N</th><th class="sortable-th" data-sort-table="ladder" data-sort-key="avg_edge_pct" data-sort-type="number">Avg Edge</th><th class="sortable-th" data-sort-table="ladder" data-sort-key="expected_win_rate_pct" data-sort-type="number">Exp Win%</th><th class="sortable-th" data-sort-table="ladder" data-sort-key="actual_win_rate_pct" data-sort-type="number">Act Win%</th><th class="sortable-th" data-sort-table="ladder" data-sort-key="expected_net_dollars" data-sort-type="number">Expected</th><th class="sortable-th" data-sort-table="ladder" data-sort-key="realized_dollars" data-sort-type="number">Realized</th><th class="sortable-th" data-sort-table="ladder" data-sort-key="realized_roi_pct_on_stake" data-sort-type="number">ROI</th></tr></thead>
          <tbody id="ladderRows"><tr><td colspan="8">Loading...</td></tr></tbody>
        </table>
      </div>
    </section>
    </section>

    <section id="evTab" class="tab-content">
    <section class="card" id="ev">
      <h2>Expected Value vs Actual Outcome</h2>
      <div class="meta">Live-only daily totals from <span class="mono">/analytics/live-scorecard</span>.</div>
      <div class="toolbar">
        <button class="btn active" data-ev-days="7">EV 7D</button>
        <button class="btn" data-ev-days="14">EV 14D</button>
        <button class="btn" data-ev-days="30">EV 30D</button>
      </div>
      <div class="chart-wrap">
        <canvas id="evChart" width="1120" height="260"></canvas>
        <div id="chartTip" class="chart-tip" style="display:none;"></div>
      </div>
      <div class="legend">
        <span><i class="dot blue"></i>Expected P/L NET Cumulative ($)</span>
        <span><i class="dot green"></i>Realized P/L NET Cumulative ($)</span>
      </div>
    </section>
    </section>

    <section id="dailyTab" class="tab-content">
    <section class="card" id="daily">
      <h2>Daily Stats</h2>
      <div class="meta">Day-level outcomes and execution quality from <span class="mono">/analytics/live-insights</span>.</div>
      <div class="toolbar">
        <button class="btn active" data-daily-days="7">Daily 7D</button>
        <button class="btn" data-daily-days="14">Daily 14D</button>
        <button class="btn" data-daily-days="30">Daily 30D</button>
      </div>
      <div class="meta" style="margin-top:8px;">Daily Breakdown</div>
      <div class="table-wrap">
        <table>
          <thead><tr><th class="sortable-th" data-sort-table="daily" data-sort-key="date" data-sort-type="date">Date</th><th class="sortable-th" data-sort-table="daily" data-sort-key="fills" data-sort-type="number">Fills</th><th class="sortable-th" data-sort-table="daily" data-sort-key="settled_count" data-sort-type="number">Settled</th><th class="sortable-th" data-sort-table="daily" data-sort-key="expected_net_dollars" data-sort-type="number">Expected</th><th class="sortable-th" data-sort-table="daily" data-sort-key="realized_dollars" data-sort-type="number">Realized</th><th class="sortable-th" data-sort-table="daily" data-sort-key="ev_gap_dollars" data-sort-type="number">Gap</th><th class="sortable-th" data-sort-table="daily" data-sort-key="realized_win_rate_pct" data-sort-type="number">Win Rate</th><th class="sortable-th" data-sort-table="daily" data-sort-key="realized_roi_pct_on_stake" data-sort-type="number">ROI</th><th class="sortable-th" data-sort-table="daily" data-sort-key="orders_attempted" data-sort-type="number">Attempts</th><th class="sortable-th" data-sort-table="daily" data-sort-key="orders_rejected" data-sort-type="number">Rejected</th><th class="sortable-th" data-sort-table="daily" data-sort-key="rejected_rate_pct" data-sort-type="number">Reject Rate</th></tr></thead>
          <tbody id="dailyRows"><tr><td colspan="11">Loading...</td></tr></tbody>
        </table>
      </div>
      <div class="meta" style="margin-top:10px;">Weekly Rollup</div>
      <div class="table-wrap">
        <table>
          <thead><tr><th class="sortable-th" data-sort-table="weekly" data-sort-key="key" data-sort-type="text">Week</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="days" data-sort-type="number">Days</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="fills" data-sort-type="number">Fills</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="settled_count" data-sort-type="number">Settled</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="expected" data-sort-type="number">Expected</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="realized" data-sort-type="number">Realized</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="ev_gap_dollars" data-sort-type="number">Gap</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="realized_win_rate_pct" data-sort-type="number">Win Rate</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="realized_roi_pct_on_stake" data-sort-type="number">ROI</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="attempts" data-sort-type="number">Attempts</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="rejected" data-sort-type="number">Rejected</th><th class="sortable-th" data-sort-table="weekly" data-sort-key="rejected_rate_pct" data-sort-type="number">Reject Rate</th></tr></thead>
          <tbody id="weeklyRows"><tr><td colspan="12">Loading...</td></tr></tbody>
        </table>
      </div>
      <div class="meta" style="margin-top:10px;">Monthly Rollup</div>
      <div class="table-wrap">
        <table>
          <thead><tr><th class="sortable-th" data-sort-table="monthly" data-sort-key="key" data-sort-type="text">Month</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="days" data-sort-type="number">Days</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="fills" data-sort-type="number">Fills</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="settled_count" data-sort-type="number">Settled</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="expected" data-sort-type="number">Expected</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="realized" data-sort-type="number">Realized</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="ev_gap_dollars" data-sort-type="number">Gap</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="realized_win_rate_pct" data-sort-type="number">Win Rate</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="realized_roi_pct_on_stake" data-sort-type="number">ROI</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="attempts" data-sort-type="number">Attempts</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="rejected" data-sort-type="number">Rejected</th><th class="sortable-th" data-sort-table="monthly" data-sort-key="rejected_rate_pct" data-sort-type="number">Reject Rate</th></tr></thead>
          <tbody id="monthlyRows"><tr><td colspan="12">Loading...</td></tr></tbody>
        </table>
      </div>
    </section>
    </section>

    <section id="manualTab" class="tab-content">
    <section class="card" id="manualPos">
      <h2>Manual Positions</h2>
      <div class="meta">Weather (user): <span class="mono" id="manualPath">manual_positions.csv</span> | Weather (auto): <span class="mono" id="manualAutoWeatherPath">manual_positions_auto_weather.csv</span> | BTC file: <span class="mono" id="manualBtcPath">manual_positions_btc.csv</span>.</div>
      <div class="toolbar">
        <button class="btn" data-manual-days="7">Manual 7D</button>
        <button class="btn active" data-manual-days="30">Manual 30D</button>
        <button class="btn" data-manual-days="90">Manual 90D</button>
        <button class="btn" data-manual-days="0">All</button>
      </div>
      <div class="row">
        <div class="stat"><div class="k">Positions</div><div class="v" id="manualPositions">-</div></div>
        <div class="stat"><div class="k">Contracts</div><div class="v" id="manualContracts">-</div></div>
        <div class="stat"><div class="k">Stake</div><div class="v" id="manualStake">-</div></div>
        <div class="stat"><div class="k">Resolved</div><div class="v" id="manualResolved">-</div></div>
        <div class="stat"><div class="k">Open</div><div class="v" id="manualOpen">-</div></div>
        <div class="stat"><div class="k">Realized P/L</div><div class="v" id="manualRealized">-</div></div>
      </div>
      <div class="meta" style="margin-top:10px;">Weather Manual Positions</div>
      <div class="table-wrap" style="margin-top:8px;">
        <table>
          <thead><tr><th>Date</th><th>City</th><th>Side</th><th>Bet</th><th>Line</th><th>Ticker</th><th>Price</th><th>Count</th><th>Stake</th><th>Status</th><th>Realized</th><th>Source</th></tr></thead>
          <tbody id="manualRows"><tr><td colspan="12">Loading...</td></tr></tbody>
        </table>
      </div>
      <div class="meta" style="margin-top:12px;">BTC Up/Down Manual Positions</div>
      <div class="row">
        <div class="stat"><div class="k">BTC Positions</div><div class="v" id="manualBtcPositions">-</div></div>
        <div class="stat"><div class="k">BTC Stake</div><div class="v" id="manualBtcStake">-</div></div>
        <div class="stat"><div class="k">BTC Resolved</div><div class="v" id="manualBtcResolved">-</div></div>
        <div class="stat"><div class="k">BTC Open</div><div class="v" id="manualBtcOpen">-</div></div>
        <div class="stat"><div class="k">BTC Realized P/L</div><div class="v" id="manualBtcRealized">-</div></div>
      </div>
      <div class="table-wrap" style="margin-top:8px;">
        <table>
          <thead><tr><th>Date</th><th>Market</th><th>Bet</th><th>Outcome</th><th>Cost</th><th>Fees</th><th>Payout</th><th>Realized</th><th>Source</th><th>Note</th></tr></thead>
          <tbody id="manualBtcRows"><tr><td colspan="10">Loading...</td></tr></tbody>
        </table>
      </div>
    </section>
    </section>
  </main>

  <script>
    const $ = (id) => document.getElementById(id);
    let rangeDays = 7;
    let cityRangeDays = 7;
    let ladderRangeDays = 7;
    let evRangeDays = 7;
    let dailyRangeDays = 14;
    let manualRangeDays = 30;
    let liveStatusCache = {};
    let ladderRowsData = [];
    let dailyRowsData = [];
    let weeklyRowsData = [];
    let monthlyRowsData = [];
    const tableSortState = {
      ladder: { key: "realized_dollars", dir: "desc", type: "number" },
      daily: { key: "date", dir: "desc", type: "date" },
      weekly: { key: "key", dir: "desc", type: "text" },
      monthly: { key: "key", dir: "desc", type: "text" },
    };

    function esc(v) { return String(v ?? ""); }
    function money(v) { return (v == null || isNaN(v)) ? "-" : `$${Number(v).toFixed(2)}`; }
    function pct(v) { return (v == null || isNaN(v)) ? "-" : `${Number(v).toFixed(1)}%`; }
    function toneValue(elId, v, invert=false) {
      const el = $(elId);
      if (!el) return;
      el.classList.remove("good-tone", "bad-tone");
      if (v == null || isNaN(v)) return;
      const n = Number(v);
      const good = invert ? (n < 0) : (n > 0);
      const bad = invert ? (n > 0) : (n < 0);
      if (good) el.classList.add("good-tone");
      if (bad) el.classList.add("bad-tone");
    }

    function statusPill(status) {
      const s = String(status || "").toLowerCase();
      if (s.includes("submitted") || s.includes("partial")) return '<span class="pill ok">' + esc(status) + '</span>';
      if (s.includes("resolved_win")) return '<span class="pill ok">' + esc(status) + '</span>';
      if (s.includes("open")) return '<span class="pill warn">' + esc(status) + '</span>';
      if (s.includes("not_filled") || s.includes("edge_gone")) return '<span class="pill warn">' + esc(status) + '</span>';
      return '<span class="pill bad">' + esc(status || "unknown") + '</span>';
    }

    function ymd(d) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    }

    function isoWeekKey(dateStr) {
      const dt = new Date(`${dateStr}T00:00:00Z`);
      const day = (dt.getUTCDay() + 6) % 7; // Mon=0..Sun=6
      dt.setUTCDate(dt.getUTCDate() - day + 3); // Thursday of this ISO week
      const isoYear = dt.getUTCFullYear();
      const jan4 = new Date(Date.UTC(isoYear, 0, 4));
      const jan4Day = (jan4.getUTCDay() + 6) % 7;
      jan4.setUTCDate(jan4.getUTCDate() - jan4Day + 3);
      const week = 1 + Math.round((dt - jan4) / (7 * 24 * 3600 * 1000));
      return `${isoYear}-W${String(week).padStart(2, "0")}`;
    }

    function rollupRows(rows, keyFn) {
      const m = new Map();
      for (const r of rows) {
        const key = keyFn(r);
        if (!m.has(key)) {
          m.set(key, {
            key,
            days: 0,
            fills: 0,
            settled_count: 0,
            stake: 0,
            expected: 0,
            realized: 0,
            attempts: 0,
            rejected: 0,
            win_weighted_sum: 0,
            win_weight_den: 0,
          });
        }
        const a = m.get(key);
        const settled = Number(r.settled_count || 0);
        a.days += 1;
        a.fills += Number(r.fills || 0);
        a.settled_count += settled;
        a.stake += Number(r.total_stake_dollars || 0);
        a.expected += Number(r.expected_net_dollars || 0);
        a.realized += Number(r.realized_dollars || 0);
        a.attempts += Number(r.orders_attempted || 0);
        a.rejected += Number(r.orders_rejected || 0);
        if (r.realized_win_rate_pct != null && settled > 0) {
          a.win_weighted_sum += Number(r.realized_win_rate_pct) * settled;
          a.win_weight_den += settled;
        }
      }
      return Array.from(m.values()).sort((a, b) => String(b.key).localeCompare(String(a.key)));
    }

    function ladderBucketRank(v) {
      const s = String(v || "");
      const rank = { "<5%": 0, "5-10%": 1, "10-20%": 2, "20-30%": 3, "30%+": 4 };
      return (s in rank) ? rank[s] : 99;
    }

    function sortedRows(rows, state) {
      const key = String((state && state.key) || "").trim();
      const dir = String((state && state.dir) || "desc").toLowerCase() === "asc" ? 1 : -1;
      const type = String((state && state.type) || "text").toLowerCase();
      if (!key) return rows.slice();
      return rows.slice().sort((a, b) => {
        let av = a ? a[key] : null;
        let bv = b ? b[key] : null;
        let cmp = 0;
        if (type === "number") {
          cmp = Number(av || 0) - Number(bv || 0);
        } else if (type === "date") {
          cmp = String(av || "").localeCompare(String(bv || ""));
        } else if (type === "bucket") {
          cmp = ladderBucketRank(av) - ladderBucketRank(bv);
        } else {
          cmp = String(av || "").localeCompare(String(bv || ""), undefined, { numeric: true, sensitivity: "base" });
        }
        return cmp * dir;
      });
    }

    function setActiveSortHeader(tableName) {
      const st = tableSortState[tableName] || {};
      document.querySelectorAll(`th.sortable-th[data-sort-table="${tableName}"]`).forEach(th => {
        th.classList.remove("active", "asc", "desc");
        if (th.dataset.sortKey === st.key) {
          th.classList.add("active");
          th.classList.add(st.dir === "asc" ? "asc" : "desc");
        }
      });
    }

    function renderLadderRows() {
      const rows = sortedRows(ladderRowsData, tableSortState.ladder);
      $("ladderRows").innerHTML = rows.length ? rows.map(r => `
        <tr>
          <td>${esc(r.bucket)}</td>
          <td>${esc(r.n)}</td>
          <td>${pct(r.avg_edge_pct)}</td>
          <td>${pct(r.expected_win_rate_pct)}</td>
          <td>${pct(r.actual_win_rate_pct)}</td>
          <td>${money(r.expected_net_dollars)}</td>
          <td>${money(r.realized_dollars)}</td>
          <td>${pct(r.realized_roi_pct_on_stake)}</td>
        </tr>
      `).join("") : "<tr><td colspan='8'>No data in this window.</td></tr>";
      setActiveSortHeader("ladder");
    }

    function renderDailyRows() {
      const rows = sortedRows(dailyRowsData, tableSortState.daily);
      $("dailyRows").innerHTML = rows.length ? rows.map(d => `
        <tr>
          <td>${esc(d.date)}</td>
          <td>${esc(d.fills)}</td>
          <td>${esc(d.settled_count)}</td>
          <td>${money(d.expected_net_dollars)}</td>
          <td>${money(d.realized_dollars)}</td>
          <td>${money(d.ev_gap_dollars)}</td>
          <td>${pct(d.realized_win_rate_pct)}</td>
          <td>${pct(d.realized_roi_pct_on_stake)}</td>
          <td>${esc(d.orders_attempted)}</td>
          <td>${esc(d.orders_rejected)}</td>
          <td>${pct(d.rejected_rate_pct)}</td>
        </tr>
      `).join("") : "<tr><td colspan='11'>No daily data in this window.</td></tr>";
      setActiveSortHeader("daily");
    }

    function renderWeeklyRows() {
      const rows = sortedRows(weeklyRowsData, tableSortState.weekly);
      $("weeklyRows").innerHTML = rows.length ? rows.map(w => `
        <tr>
          <td>${esc(w.key)}</td>
          <td>${esc(w.days)}</td>
          <td>${esc(w.fills)}</td>
          <td>${esc(w.settled_count)}</td>
          <td>${money(w.expected)}</td>
          <td>${money(w.realized)}</td>
          <td>${money(w.ev_gap_dollars)}</td>
          <td>${pct(w.realized_win_rate_pct)}</td>
          <td>${pct(w.realized_roi_pct_on_stake)}</td>
          <td>${esc(w.attempts)}</td>
          <td>${esc(w.rejected)}</td>
          <td>${pct(w.rejected_rate_pct)}</td>
        </tr>
      `).join("") : "<tr><td colspan='12'>No weekly data in this window.</td></tr>";
      setActiveSortHeader("weekly");
    }

    function renderMonthlyRows() {
      const rows = sortedRows(monthlyRowsData, tableSortState.monthly);
      $("monthlyRows").innerHTML = rows.length ? rows.map(mo => `
        <tr>
          <td>${esc(mo.key)}</td>
          <td>${esc(mo.days)}</td>
          <td>${esc(mo.fills)}</td>
          <td>${esc(mo.settled_count)}</td>
          <td>${money(mo.expected)}</td>
          <td>${money(mo.realized)}</td>
          <td>${money(mo.ev_gap_dollars)}</td>
          <td>${pct(mo.realized_win_rate_pct)}</td>
          <td>${pct(mo.realized_roi_pct_on_stake)}</td>
          <td>${esc(mo.attempts)}</td>
          <td>${esc(mo.rejected)}</td>
          <td>${pct(mo.rejected_rate_pct)}</td>
        </tr>
      `).join("") : "<tr><td colspan='12'>No monthly data in this window.</td></tr>";
      setActiveSortHeader("monthly");
    }

    function renderSortableTable(tableName) {
      if (tableName === "ladder") renderLadderRows();
      if (tableName === "daily") renderDailyRows();
      if (tableName === "weekly") renderWeeklyRows();
      if (tableName === "monthly") renderMonthlyRows();
    }

    function setupSortableHeaders() {
      document.querySelectorAll("th.sortable-th").forEach(th => {
        if (th.dataset.sortBound === "1") return;
        th.dataset.sortBound = "1";
        th.addEventListener("click", () => {
          const table = String(th.dataset.sortTable || "").trim();
          const key = String(th.dataset.sortKey || "").trim();
          const type = String(th.dataset.sortType || "text").trim();
          if (!table || !key) return;
          const prev = tableSortState[table] || {};
          const nextDir = (prev.key === key && prev.dir === "desc") ? "asc" : "desc";
          tableSortState[table] = { key, dir: nextDir, type };
          renderSortableTable(table);
        });
      });
      setActiveSortHeader("ladder");
      setActiveSortHeader("daily");
      setActiveSortHeader("weekly");
      setActiveSortHeader("monthly");
    }

    let chartCache = { labels: [], exp: [], real: [] };
    let chartGeom = null;

    function drawLineChart(pointsExpected, pointsRealized, labels, hoverIdx = null) {
      const c = $("evChart");
      const ctx = c.getContext("2d");
      const w = c.width, h = c.height;
      ctx.clearRect(0, 0, w, h);
      const pad = { l: 56, r: 18, t: 14, b: 36 };
      const iw = w - pad.l - pad.r;
      const ih = h - pad.t - pad.b;

      const all = [...pointsExpected, ...pointsRealized].filter(v => typeof v === "number");
      if (!all.length) {
        ctx.fillStyle = "#4a5d53";
        ctx.fillText("No EV data for this range yet.", 20, 24);
        return;
      }
      const yMinRaw = Math.min(...all, 0);
      const yMaxRaw = Math.max(...all, 0);
      const yPad = Math.max(5, (yMaxRaw - yMinRaw) * 0.15);
      const yMin = yMinRaw - yPad;
      const yMax = yMaxRaw + yPad;
      const xStep = labels.length > 1 ? (iw / (labels.length - 1)) : 0;
      const x = i => pad.l + i * xStep;
      const y = v => pad.t + ((yMax - v) / Math.max(1e-9, (yMax - yMin))) * ih;
      chartGeom = { pad, iw, ih, w, h, xStep, x, y, yMin, yMax };

      ctx.strokeStyle = "#d4e3d7";
      ctx.lineWidth = 1;
      for (let g = 0; g <= 4; g++) {
        const yy = pad.t + (ih * g / 4);
        ctx.beginPath(); ctx.moveTo(pad.l, yy); ctx.lineTo(w - pad.r, yy); ctx.stroke();
        const val = yMax - ((yMax - yMin) * g / 4);
        ctx.fillStyle = "#4a5d53";
        ctx.font = "11px JetBrains Mono";
        ctx.fillText(val.toFixed(0), 6, yy + 4);
      }
      const yZero = y(0);
      ctx.strokeStyle = "#8ea395";
      ctx.beginPath(); ctx.moveTo(pad.l, yZero); ctx.lineTo(w - pad.r, yZero); ctx.stroke();

      function plot(arr, color) {
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;
        ctx.beginPath();
        arr.forEach((v, i) => {
          const xx = x(i), yy = y(v || 0);
          if (i === 0) ctx.moveTo(xx, yy); else ctx.lineTo(xx, yy);
        });
        ctx.stroke();
        arr.forEach((v, i) => {
          const xx = x(i), yy = y(v || 0);
          ctx.fillStyle = color;
          ctx.beginPath(); ctx.arc(xx, yy, 3, 0, Math.PI * 2); ctx.fill();
        });
      }
      plot(pointsExpected, "#1d4ed8");
      plot(pointsRealized, "#15803d");

      ctx.fillStyle = "#4a5d53";
      ctx.font = "11px JetBrains Mono";
      labels.forEach((lbl, i) => {
        if (labels.length > 10 && (i % 2) !== 0) return;
        const xx = x(i) - 16;
        ctx.fillText(lbl.slice(5), xx, h - 10);
      });
      ctx.fillStyle = "#4a5d53";
      ctx.font = "12px Manrope";
      ctx.fillText("P/L ($)", pad.l, 12);
      ctx.fillText("Date (ET)", w - 72, h - 10);

      if (hoverIdx != null && hoverIdx >= 0 && hoverIdx < labels.length) {
        const xx = x(hoverIdx);
        const yE = y(pointsExpected[hoverIdx] || 0);
        const yR = y(pointsRealized[hoverIdx] || 0);
        ctx.strokeStyle = "#7f9d89";
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(xx, pad.t); ctx.lineTo(xx, h - pad.b); ctx.stroke();
        ctx.fillStyle = "#1d4ed8";
        ctx.beginPath(); ctx.arc(xx, yE, 4, 0, Math.PI * 2); ctx.fill();
        ctx.fillStyle = "#15803d";
        ctx.beginPath(); ctx.arc(xx, yR, 4, 0, Math.PI * 2); ctx.fill();
      }
    }

    async function loadAll() {
      const [healthRes, liveRes] = await Promise.all([
        fetch("/health").then(r => r.json()),
        fetch("/live/status").then(r => r.json())
      ]);
      liveStatusCache = liveRes || {};

      $("sys").textContent = healthRes.ok ? "Online" : "Issue";
      $("asOf").textContent = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
      $("ordersToday").textContent = liveRes.orders_placed_today ?? "-";
      $("cityCount").textContent = Array.isArray(healthRes.cities) ? healthRes.cities.length : "-";
      $("host").textContent = healthRes.kalshi_base_url || "-";

      const criteria = [
        ["Min Net Edge", pct(healthRes.policy_min_net_edge_pct)],
        ["Implied Prob Filter", `${pct(healthRes.no_trade_implied_prob_min_pct)} to ${pct(healthRes.no_trade_implied_prob_max_pct)}`],
        ["Scan Interval", `${healthRes.scan_interval_seconds}s`],
        ["Stability Gate", healthRes.live_stability_gate_enabled ? `${healthRes.live_stability_gate_min_scans_mid} scans (${pct(healthRes.live_stability_gate_edge_min_pct)}-${pct(healthRes.live_stability_gate_edge_max_pct)})` : "off"],
        ["Passive/Active Thresholds", `${pct(healthRes.live_edge_passive_then_aggr_pct)} / ${pct(healthRes.live_edge_immediate_aggressive_pct)}`],
        ["Passive TIF", esc(healthRes.live_passive_time_in_force)],
        ["Aggressive Max Spread", `${healthRes.live_aggressive_max_spread_cents}c`],
        ["Unit Size", money((healthRes.kelly_bankroll_dollars || 0) * (healthRes.ladder_unit_fraction_of_bankroll || 0))],
      ];
      $("criteriaList").innerHTML = criteria.map(([k, v]) => `<div class="item"><b>${esc(k)}</b><span>${esc(v)}</span></div>`).join("");
      $("ladderText").textContent = "10-20%: 0.5u, 20-25%: 1u, 25-40%: 1.5u, 40%+: 2u (capped)";
      $("safetyText").textContent = `max ${healthRes.live_max_orders_per_market_per_day}/market/day, max ${healthRes.live_max_orders_per_scan}/scan, kill switch: ${healthRes.live_kill_switch ? "ON" : "OFF"}`;

      const src = Array.isArray(healthRes.consensus_sources) ? healthRes.consensus_sources.join(", ") : "-";
      const awErr = String(healthRes.accuweather_last_error || "").trim();
      const awHealth = awErr ? `AccuWeather issue: ${awErr.slice(0, 90)}...` : "AccuWeather OK";
      $("sourceHealthText").textContent = `Sources: ${src}. ${awHealth} NWS stale threshold: ${healthRes.nws_obs_stale_minutes}m.`;

      await loadAnalytics();
    }

    async function loadAnalytics() {
      const end = new Date();
      const start = new Date();
      start.setDate(end.getDate() - (rangeDays - 1));
      const q = new URLSearchParams({ start: ymd(start), end: ymd(end) });
      const cityStart = new Date();
      cityStart.setDate(end.getDate() - (cityRangeDays - 1));
      const qCity = new URLSearchParams({ start: ymd(cityStart), end: ymd(end) });
      const ladderStart = new Date();
      ladderStart.setDate(end.getDate() - (ladderRangeDays - 1));
      const qLadder = new URLSearchParams({ start: ymd(ladderStart), end: ymd(end) });
      const dailyStart = new Date();
      dailyStart.setDate(end.getDate() - (dailyRangeDays - 1));
      const qDaily = new URLSearchParams({ start: ymd(dailyStart), end: ymd(end) });
      const evStart = new Date();
      evStart.setDate(end.getDate() - (evRangeDays - 1));
      const firstTradeDate = String((liveStatusCache && liveStatusCache.first_trade_date) || "").trim();
      const historyStart = (firstTradeDate && /^\\d{4}-\\d{2}-\\d{2}$/.test(firstTradeDate)) ? firstTradeDate : ymd(evStart);
      const qEv = new URLSearchParams({ start: historyStart, end: ymd(end) });
      const manualStart = new Date();
      manualStart.setDate(end.getDate() - (Math.max(1, manualRangeDays) - 1));
      const qManual = new URLSearchParams();
      if (manualRangeDays > 0) {
        qManual.set("start", ymd(manualStart));
        qManual.set("end", ymd(end));
      }
      const [dataSettled, dataInsights, dataDaily, dataCity, dataLadder, dataRecon, dataManual] = await Promise.all([
        fetch(`/analytics/live-scorecard?${q.toString()}`).then(r => r.json()),
        fetch(`/analytics/live-insights?${q.toString()}`).then(r => r.json()),
        fetch(`/analytics/live-insights?${qDaily.toString()}`).then(r => r.json()),
        fetch(`/analytics/live-insights?${qCity.toString()}`).then(r => r.json()),
        fetch(`/analytics/live-insights?${qLadder.toString()}`).then(r => r.json()),
        fetch(`/analytics/account-reconciliation`).then(r => r.json()),
        fetch(`/analytics/manual-positions?${qManual.toString()}`).then(r => r.json()),
      ]);
      const iq = (dataInsights && dataInsights.summary) || {};
      $("modelFills").textContent = iq.fills ?? "-";
      $("modelSettled").textContent = iq.settled_count ?? "-";
      $("modelWinRate").textContent = pct(iq.realized_win_rate_pct);
      $("modelAvgEdge").textContent = pct(iq.avg_edge_pct);
      $("modelExpected").textContent = money(iq.expected_net_dollars);
      $("modelRealized").textContent = money(iq.realized_dollars);
      $("modelGap").textContent = money(iq.ev_gap_dollars);
      $("modelRejectRate").textContent = pct(iq.rejected_rate_pct);
      $("kpiEvGap").textContent = money(iq.ev_gap_dollars);
      $("kpiRejectRate").textContent = pct(iq.rejected_rate_pct);
      $("kpiWinRate").textContent = pct(iq.realized_win_rate_pct);
      toneValue("modelRealized", iq.realized_dollars);
      toneValue("modelGap", iq.ev_gap_dollars);
      toneValue("modelRejectRate", iq.rejected_rate_pct, true);
      toneValue("kpiEvGap", iq.ev_gap_dollars);
      toneValue("kpiRejectRate", iq.rejected_rate_pct, true);

      const cityRows = Array.isArray(dataCity.city_side) ? dataCity.city_side : [];
      $("citySideRows").innerHTML = cityRows.length ? cityRows.map(r => `
        <tr>
          <td>${esc(r.city)}</td>
          <td>${esc(r.temp_side)}</td>
          <td>${esc(r.fills)}</td>
          <td>${esc(r.settled_count)}</td>
          <td>${pct(r.avg_edge_pct)}</td>
          <td>${pct(r.expected_win_rate_pct)}</td>
          <td>${pct(r.actual_win_rate_pct)}</td>
          <td>${money(r.expected_net_dollars)}</td>
          <td>${money(r.realized_dollars)}</td>
        </tr>
      `).join("") : "<tr><td colspan='9'>No data in this window.</td></tr>";

      ladderRowsData = Array.isArray(dataLadder.edge_ladder) ? dataLadder.edge_ladder : [];
      renderLadderRows();

      const f = (dataInsights && dataInsights.funnel) || {};
      $("funnelText").textContent = `attempted ${f.orders_attempted ?? 0} -> rejected ${f.orders_rejected ?? 0} -> not filled ${f.orders_not_filled ?? 0} -> filled rows ${f.fill_rows ?? 0} -> positions ${f.positions_filled ?? 0} -> settled ${f.settled_positions ?? 0}`;
      const recon = dataRecon || {};
      $("reconDeposits").textContent = money(recon.deposits_dollars);
      $("reconEquity").textContent = money(recon.equity_dollars);
      $("reconCash").textContent = money(recon.cash_dollars);
      $("reconPositions").textContent = money(recon.positions_dollars);
      $("reconNet").textContent = money(recon.account_net_pnl_dollars);
      $("reconBotRealized").textContent = money(recon.bot_realized_pnl_dollars);
      $("reconManualRealized").textContent = money(recon.manual_realized_pnl_dollars);
      $("reconResidual").textContent = money(recon.unrealized_residual_pnl_dollars);
      $("kpiNetPnl").textContent = money(recon.account_net_pnl_dollars);
      toneValue("reconNet", recon.account_net_pnl_dollars);
      toneValue("reconBotRealized", recon.bot_realized_pnl_dollars);
      toneValue("reconManualRealized", recon.manual_realized_pnl_dollars);
      toneValue("reconResidual", recon.unrealized_residual_pnl_dollars);
      toneValue("kpiNetPnl", recon.account_net_pnl_dollars);

      const errs = Array.isArray(dataInsights.recent_errors) ? dataInsights.recent_errors : [];
      $("errorRows").innerHTML = errs.length ? errs.map(e => `
        <tr>
          <td>${esc(e.ts_est)}</td>
          <td>${esc(e.city)}</td>
          <td class="mono">${esc(e.ticker)}</td>
          <td>${statusPill(e.status)}</td>
          <td>${esc(e.error)}</td>
        </tr>
      `).join("") : "<tr><td colspan='5'>No recent errors in this window.</td></tr>";

      dailyRowsData = Array.isArray(dataDaily.per_day) ? dataDaily.per_day : [];
      weeklyRowsData = rollupRows(dailyRowsData, (r) => isoWeekKey(String(r.date || ""))).map(w => ({
        ...w,
        ev_gap_dollars: Number(w.realized || 0) - Number(w.expected || 0),
        realized_win_rate_pct: w.win_weight_den > 0 ? (w.win_weighted_sum / w.win_weight_den) : null,
        realized_roi_pct_on_stake: w.stake > 0 ? ((100.0 * Number(w.realized || 0)) / Number(w.stake || 0)) : null,
        rejected_rate_pct: w.attempts > 0 ? ((100.0 * Number(w.rejected || 0)) / Number(w.attempts || 0)) : null,
      }));
      monthlyRowsData = rollupRows(dailyRowsData, (r) => String(r.date || "").slice(0, 7)).map(mo => ({
        ...mo,
        ev_gap_dollars: Number(mo.realized || 0) - Number(mo.expected || 0),
        realized_win_rate_pct: mo.win_weight_den > 0 ? (mo.win_weighted_sum / mo.win_weight_den) : null,
        realized_roi_pct_on_stake: mo.stake > 0 ? ((100.0 * Number(mo.realized || 0)) / Number(mo.stake || 0)) : null,
        rejected_rate_pct: mo.attempts > 0 ? ((100.0 * Number(mo.rejected || 0)) / Number(mo.attempts || 0)) : null,
      }));
      renderDailyRows();
      renderWeeklyRows();
      renderMonthlyRows();

      const ms = (dataManual && dataManual.summary) || {};
      const ws = (dataManual && dataManual.weather_summary) || {};
      const bs = (dataManual && dataManual.btc_summary) || {};
      $("manualPath").textContent = String((dataManual && dataManual.path) || "manual_positions.csv");
      $("manualAutoWeatherPath").textContent = String((dataManual && dataManual.auto_weather_path) || "manual_positions_auto_weather.csv");
      $("manualBtcPath").textContent = String((dataManual && dataManual.btc_path) || "manual_positions_btc.csv");
      $("manualPositions").textContent = ms.positions ?? "-";
      $("manualContracts").textContent = ms.contracts ?? "-";
      $("manualStake").textContent = money(ms.stake_dollars);
      $("manualResolved").textContent = ms.resolved_positions ?? "-";
      $("manualOpen").textContent = ms.open_positions ?? "-";
      $("manualRealized").textContent = money(ms.realized_pnl_dollars);
      const manualRows = Array.isArray(dataManual.weather_rows) ? dataManual.weather_rows : (
        Array.isArray(dataManual.rows) ? dataManual.rows : []
      );
      $("manualRows").innerHTML = manualRows.length ? manualRows.map(r => `
        <tr>
          <td>${esc(r.date)}</td>
          <td>${esc(r.city)}</td>
          <td>${esc(r.temp_side)}</td>
          <td>${esc(r.bet)}</td>
          <td>${esc(r.line)}</td>
          <td class="mono">${esc(r.ticker)}</td>
          <td>${esc(Number(r.price_cents || 0).toFixed(1))}c</td>
          <td>${esc(r.count)}</td>
          <td>${money(r.stake_dollars)}</td>
          <td>${statusPill(r.settled ? (r.is_win ? "resolved_win" : "resolved_loss") : "open")}</td>
          <td>${money(r.realized_pnl_dollars)}</td>
          <td>${esc(r.source || "")}</td>
        </tr>
      `).join("") : "<tr><td colspan='12'>No weather manual positions in this window.</td></tr>";
      $("manualBtcPositions").textContent = bs.positions ?? 0;
      $("manualBtcStake").textContent = money(bs.stake_dollars);
      $("manualBtcResolved").textContent = bs.resolved_positions ?? 0;
      $("manualBtcOpen").textContent = bs.open_positions ?? 0;
      $("manualBtcRealized").textContent = money(bs.realized_pnl_dollars);
      toneValue("manualBtcRealized", bs.realized_pnl_dollars);
      const btcRows = Array.isArray(dataManual.btc_rows) ? dataManual.btc_rows : [];
      $("manualBtcRows").innerHTML = btcRows.length ? btcRows.map(r => `
        <tr>
          <td>${esc(r.date)}</td>
          <td>${esc(r.market_name || r.ticker || "BTC Up/Down")}</td>
          <td>${esc(r.bet)}</td>
          <td>${statusPill(r.settled ? (r.is_win ? "resolved_win" : "resolved_loss") : "open")}</td>
          <td>${money(r.stake_dollars)}</td>
          <td>${money(r.fees_dollars)}</td>
          <td>${money(r.total_payout_dollars)}</td>
          <td>${money(r.realized_pnl_dollars)}</td>
          <td>${esc(r.source || "")}</td>
          <td>${esc(r.note || "")}</td>
        </tr>
      `).join("") : "<tr><td colspan='10'>No BTC manual positions in this window.</td></tr>";

      const dataEv = await fetch(`/analytics/live-scorecard?${qEv.toString()}`).then(r => r.json());
      const perAll = Array.isArray(dataEv.per_day) ? dataEv.per_day.filter(x => x.ok) : [];
      let cExp = 0.0;
      let cReal = 0.0;
      const cumAll = perAll.map(x => {
        cExp += Number((x.total_expected_profit_net_dollars ?? x.total_expected_profit_dollars) || 0);
        cReal += Number(x.total_realized_pnl_dollars || 0);
        return { date: String(x.date || ""), exp: cExp, real: cReal };
      });
      const evStartIso = ymd(evStart);
      const visible = cumAll.filter(x => x.date >= evStartIso);
      const labels = visible.map(x => x.date);
      const exp = visible.map(x => Number(x.exp || 0));
      const real = visible.map(x => Number(x.real || 0));
      chartCache = { labels, exp, real };
      drawLineChart(exp, real, labels);
      bindChartHover();
    }

    function bindChartHover() {
      const c = $("evChart");
      const tip = $("chartTip");
      if (!c || !tip) return;
      if (c.dataset.bound === "1") return;
      c.dataset.bound = "1";

      c.addEventListener("mousemove", (ev) => {
        if (!chartGeom || !chartCache.labels.length) return;
        const rect = c.getBoundingClientRect();
        const mx = ev.clientX - rect.left;
        const iRaw = chartGeom.xStep > 0 ? ((mx - chartGeom.pad.l) / chartGeom.xStep) : 0;
        let idx = Math.round(iRaw);
        idx = Math.max(0, Math.min(chartCache.labels.length - 1, idx));

        drawLineChart(chartCache.exp, chartCache.real, chartCache.labels, idx);

        const d = chartCache.labels[idx] || "-";
        const e = chartCache.exp[idx] || 0;
        const r = chartCache.real[idx] || 0;
        tip.innerHTML = `<b>${esc(d)}</b><br/>Expected Cum: ${money(e)}<br/>Realized Cum: ${money(r)}`;
        tip.style.display = "block";
        tip.style.left = Math.max(8, Math.min(rect.width - 170, mx + 10)) + "px";
        tip.style.top = Math.max(8, ev.clientY - rect.top - 10) + "px";
      });

      c.addEventListener("mouseleave", () => {
        tip.style.display = "none";
        if (chartCache.labels.length) drawLineChart(chartCache.exp, chartCache.real, chartCache.labels);
      });
    }

    document.querySelectorAll(".btn[data-model-days]").forEach(btn => {
      btn.addEventListener("click", async () => {
        rangeDays = Number(btn.dataset.modelDays || "7");
        document.querySelectorAll(".btn[data-model-days]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        await loadAnalytics();
      });
    });

    document.querySelectorAll(".btn[data-city-days]").forEach(btn => {
      btn.addEventListener("click", async () => {
        cityRangeDays = Number(btn.dataset.cityDays || "7");
        document.querySelectorAll(".btn[data-city-days]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        await loadAnalytics();
      });
    });

    document.querySelectorAll(".btn[data-ladder-days]").forEach(btn => {
      btn.addEventListener("click", async () => {
        ladderRangeDays = Number(btn.dataset.ladderDays || "7");
        document.querySelectorAll(".btn[data-ladder-days]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        await loadAnalytics();
      });
    });

    document.querySelectorAll(".btn[data-ev-days]").forEach(btn => {
      btn.addEventListener("click", async () => {
        evRangeDays = Number(btn.dataset.evDays || "7");
        document.querySelectorAll(".btn[data-ev-days]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        await loadAnalytics();
      });
    });

    document.querySelectorAll(".btn[data-daily-days]").forEach(btn => {
      btn.addEventListener("click", async () => {
        dailyRangeDays = Number(btn.dataset.dailyDays || "14");
        document.querySelectorAll(".btn[data-daily-days]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        await loadAnalytics();
      });
    });

    document.querySelectorAll(".btn[data-manual-days]").forEach(btn => {
      btn.addEventListener("click", async () => {
        manualRangeDays = Number(btn.dataset.manualDays || "30");
        document.querySelectorAll(".btn[data-manual-days]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        await loadAnalytics();
      });
    });

    document.querySelectorAll(".tab-btn[data-tab-target]").forEach(btn => {
      btn.addEventListener("click", () => {
        const target = String(btn.dataset.tabTarget || "").trim();
        document.querySelectorAll(".tab-btn[data-tab-target]").forEach(b => b.classList.remove("active"));
        document.querySelectorAll(".tab-content").forEach(p => p.classList.remove("active"));
        btn.classList.add("active");
        const panel = document.getElementById(target);
        if (panel) panel.classList.add("active");
      });
    });

    setupSortableHeaders();
    loadAll().catch(() => {
      $("sys").textContent = "Unavailable";
    });
  </script>
</body>
</html>
"""

@app.get("/health")
def health():
    configured_sources = [
        "OpenMeteo-ECMWF",
        "OpenMeteo-GFS",
        "MET-Norway" if ENABLE_METNO_SOURCE else "MET-Norway (disabled via ENABLE_METNO_SOURCE)",
    ]
    if ENABLE_NWS_SOURCE:
        configured_sources.append("NWS")
    else:
        configured_sources.append("NWS (disabled via ENABLE_NWS_SOURCE)")
    if ENABLE_ACCUWEATHER_SOURCE and ACCUWEATHER_API_KEY:
        configured_sources.append("AccuWeather")
    elif ENABLE_ACCUWEATHER_SOURCE and not ACCUWEATHER_API_KEY:
        configured_sources.append("AccuWeather (enabled but missing ACCUWEATHER_API_KEY)")
    else:
        configured_sources.append("AccuWeather (disabled via ENABLE_ACCUWEATHER_SOURCE)")
    return {
        "ok": True,
        "cities": list(CITY_CONFIG.keys()),
        "kalshi_base_url": KALSHI_BASE_URL,
        "kalshi_auth_configured": kalshi_has_auth_config(),
        "scan_use_schedule": SCAN_USE_SCHEDULE,
        "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
        "scan_align_to_interval": SCAN_ALIGN_TO_INTERVAL,
        "scan_schedule_anchor_hour": SCAN_SCHEDULE_ANCHOR_HOUR,
        "scan_schedule_interval_hours": SCAN_SCHEDULE_INTERVAL_HOURS,
        "scan_schedule_minute": SCAN_SCHEDULE_MINUTE,
        "snapshot_logging_enabled": SNAPSHOT_LOGGING_ENABLED,
        "snapshot_log_dir": SNAPSHOT_LOG_DIR,
        "edge_tracking_enabled": EDGE_TRACKING_ENABLED,
        "board_min_top_size": BOARD_MIN_TOP_SIZE,
        "board_max_spread_cents": BOARD_MAX_SPREAD_CENTS,
        "board_min_bucket_count": BOARD_MIN_BUCKET_COUNT,
        "board_min_top_size_low": BOARD_MIN_TOP_SIZE_LOW,
        "board_max_spread_cents_low": BOARD_MAX_SPREAD_CENTS_LOW,
        "board_min_bucket_count_low": BOARD_MIN_BUCKET_COUNT_LOW,
        "no_trade_implied_prob_min_pct": NO_TRADE_IMPLIED_PROB_MIN * 100.0,
        "no_trade_implied_prob_max_pct": NO_TRADE_IMPLIED_PROB_MAX * 100.0,
        "low_signals_enabled": LOW_SIGNALS_ENABLED,
        "calibration_enabled": CALIBRATION_ENABLED,
        "calibration_min_samples": CALIBRATION_MIN_SAMPLES,
        "ev_slippage_pct": EV_SLIPPAGE_PCT,
        "model_win_prob_floor_pct": MODEL_WIN_PROB_FLOOR * 100.0,
        "model_win_prob_ceil_pct": MODEL_WIN_PROB_CEIL * 100.0,
        "high_lock_margin_f": HIGH_LOCK_MARGIN_F,
        "low_lock_margin_f": LOW_LOCK_MARGIN_F,
        "obs_boundary_sigma_f": OBS_BOUNDARY_SIGMA_F,
        "high_hard_lock_extra_margin_f": HIGH_HARD_LOCK_EXTRA_MARGIN_F,
        "low_hard_lock_extra_margin_f": LOW_HARD_LOCK_EXTRA_MARGIN_F,
        "high_early_edge_damping_multiplier": HIGH_EARLY_EDGE_DAMPING_MULTIPLIER,
        "high_early_damping_hour_lst": HIGH_EARLY_DAMPING_HOUR_LST,
        "policy_min_net_edge_pct": POLICY_MIN_NET_EDGE_PCT,
        "live_locked_outcome_capture_enabled": LIVE_LOCKED_OUTCOME_CAPTURE_ENABLED,
        "live_locked_outcome_min_net_edge_pct": LIVE_LOCKED_OUTCOME_MIN_NET_EDGE_PCT,
        "live_locked_outcome_max_spread_cents": LIVE_LOCKED_OUTCOME_MAX_SPREAD_CENTS,
        "live_locked_outcome_min_top_size": LIVE_LOCKED_OUTCOME_MIN_TOP_SIZE,
        "live_locked_outcome_max_obs_age_minutes": LIVE_LOCKED_OUTCOME_MAX_OBS_AGE_MINUTES,
        "live_locked_outcome_max_units": LIVE_LOCKED_OUTCOME_MAX_UNITS,
        "unit_size_dollars": UNIT_SIZE_DOLLARS,
        "paper_trade_discord_enabled": PAPER_TRADE_DISCORD_ENABLED,
        "discord_trade_alerts_enabled": DISCORD_TRADE_ALERTS_ENABLED,
        "paper_trade_post_top_n": PAPER_TRADE_POST_TOP_N,
        "paper_trade_max_alerts_per_market_per_day": PAPER_TRADE_MAX_ALERTS_PER_MARKET_PER_DAY,
        "paper_trade_max_alerts_per_city_side_per_day": PAPER_TRADE_MAX_ALERTS_PER_CITY_SIDE_PER_DAY,
        "paper_trade_min_edge_improvement_pct": PAPER_TRADE_MIN_EDGE_IMPROVEMENT_PCT,
        "paper_trade_min_minutes_between_re_alerts": PAPER_TRADE_MIN_MINUTES_BETWEEN_RE_ALERTS,
        "live_trading_enabled": LIVE_TRADING_ENABLED,
        "live_kill_switch": _live_kill_switch_state,
        "live_kill_switch_default": LIVE_KILL_SWITCH,
        "manual_market_block_enabled": MANUAL_MARKET_BLOCK_ENABLED,
        "manual_auto_sync_enabled": MANUAL_AUTO_SYNC_ENABLED,
        "manual_auto_sync_interval_minutes": MANUAL_AUTO_SYNC_INTERVAL_MINUTES,
        "manual_positions_path": manual_positions_path(),
        "manual_positions_count": len(load_manual_positions_rows()),
        "manual_auto_weather_positions_path": manual_auto_weather_positions_path(),
        "manual_auto_weather_positions_count": len(load_manual_auto_weather_positions_rows()),
        "manual_btc_positions_path": manual_btc_positions_path(),
        "manual_btc_positions_count": len(load_manual_btc_positions_rows()),
        "account_deposits_dollars": ACCOUNT_DEPOSITS_DOLLARS,
        "live_max_orders_per_scan": LIVE_MAX_ORDERS_PER_SCAN,
        "live_max_orders_per_day": LIVE_MAX_ORDERS_PER_DAY,
        "live_max_orders_per_market_per_day": LIVE_MAX_ORDERS_PER_MARKET_PER_DAY,
        "live_max_orders_per_city_side_per_day": LIVE_MAX_ORDERS_PER_CITY_SIDE_PER_DAY,
        "live_order_fill_mode": LIVE_ORDER_FILL_MODE,
        "live_order_time_in_force": LIVE_ORDER_TIME_IN_FORCE,
        "live_order_expiration_seconds": LIVE_ORDER_EXPIRATION_SECONDS,
        "live_max_contracts_per_order": LIVE_MAX_CONTRACTS_PER_ORDER,
        "live_min_stake_dollars": LIVE_MIN_STAKE_DOLLARS,
        "live_max_open_bot_exposure_dollars": LIVE_MAX_OPEN_BOT_EXPOSURE_DOLLARS,
        "live_edge_immediate_aggressive_pct": LIVE_EDGE_IMMEDIATE_AGGRESSIVE_PCT,
        "live_edge_passive_then_aggr_pct": LIVE_EDGE_PASSIVE_THEN_AGGR_PCT,
        "live_aggressive_override_edge_pct": LIVE_AGGRESSIVE_OVERRIDE_EDGE_PCT,
        "live_passive_wait_seconds_mid": LIVE_PASSIVE_WAIT_SECONDS_MID,
        "live_passive_wait_seconds_low": LIVE_PASSIVE_WAIT_SECONDS_LOW,
        "live_passive_allow_resting_limits": LIVE_PASSIVE_ALLOW_RESTING_LIMITS,
        "live_passive_rescan_mode_enabled": LIVE_PASSIVE_RESCAN_MODE_ENABLED,
        "live_passive_rescan_seconds": LIVE_PASSIVE_RESCAN_SECONDS,
        "live_passive_one_tick_from_ask": LIVE_PASSIVE_ONE_TICK_FROM_ASK,
        "live_passive_time_in_force": LIVE_PASSIVE_TIME_IN_FORCE,
        "live_always_passive_first": LIVE_ALWAYS_PASSIVE_FIRST,
        "live_passive_reprice_step_cents": LIVE_PASSIVE_REPRICE_STEP_CENTS,
        "live_passive_reprice_steps_mid": LIVE_PASSIVE_REPRICE_STEPS_MID,
        "live_passive_reprice_steps_low": LIVE_PASSIVE_REPRICE_STEPS_LOW,
        "live_aggressive_max_spread_cents": LIVE_AGGRESSIVE_MAX_SPREAD_CENTS,
        "live_require_cancel_before_aggressive": LIVE_REQUIRE_CANCEL_BEFORE_AGGRESSIVE,
        "live_mid_edge_maker_only": LIVE_MID_EDGE_MAKER_ONLY,
        "live_stability_gate_enabled": LIVE_STABILITY_GATE_ENABLED,
        "live_stability_gate_edge_min_pct": LIVE_STABILITY_GATE_EDGE_MIN_PCT,
        "live_stability_gate_edge_max_pct": LIVE_STABILITY_GATE_EDGE_MAX_PCT,
        "live_stability_gate_min_scans_mid": LIVE_STABILITY_GATE_MIN_SCANS_MID,
        "live_stability_require_change_mid": LIVE_STABILITY_REQUIRE_CHANGE_MID,
        "live_early_session_enabled": LIVE_EARLY_SESSION_ENABLED,
        "live_early_session_start_hour_et": LIVE_EARLY_SESSION_START_HOUR_ET,
        "live_early_session_end_hour_et": LIVE_EARLY_SESSION_END_HOUR_ET,
        "live_early_session_min_edge_pct": LIVE_EARLY_SESSION_MIN_EDGE_PCT,
        "live_early_session_min_scans": LIVE_EARLY_SESSION_MIN_SCANS,
        "live_early_session_size_mult": LIVE_EARLY_SESSION_SIZE_MULT,
        "live_early_session_apply_to_high_only": LIVE_EARLY_SESSION_APPLY_TO_HIGH_ONLY,
        "live_exit_enabled": LIVE_EXIT_ENABLED,
        "live_exit_min_hold_minutes": LIVE_EXIT_MIN_HOLD_MINUTES,
        "live_exit_edge_soft_pct": LIVE_EXIT_EDGE_SOFT_PCT,
        "live_exit_edge_hard_pct": LIVE_EXIT_EDGE_HARD_PCT,
        "live_exit_edge_drop_pct": LIVE_EXIT_EDGE_DROP_PCT,
        "live_exit_soft_max_entry_edge_pct": LIVE_EXIT_SOFT_MAX_ENTRY_EDGE_PCT,
        "live_exit_consecutive_scans": LIVE_EXIT_CONSECUTIVE_SCANS,
        "live_exit_consecutive_minutes": LIVE_EXIT_CONSECUTIVE_MINUTES,
        "live_exit_hysteresis_enabled": LIVE_EXIT_HYSTERESIS_ENABLED,
        "live_exit_hysteresis_min_drop_pct_points": LIVE_EXIT_HYSTERESIS_MIN_DROP_PCT_POINTS,
        "live_exit_hold_to_settle_enabled": LIVE_EXIT_HOLD_TO_SETTLE_ENABLED,
        "live_exit_hold_to_settle_hours_before_close": LIVE_EXIT_HOLD_TO_SETTLE_HOURS_BEFORE_CLOSE,
        "live_exit_hold_to_settle_model_yes_invalidation_pct": LIVE_EXIT_HOLD_TO_SETTLE_MODEL_YES_INVALIDATION_PCT,
        "live_exit_hold_to_settle_edge_invalidation_pct": LIVE_EXIT_HOLD_TO_SETTLE_EDGE_INVALIDATION_PCT,
        "live_exit_max_orders_per_scan": LIVE_EXIT_MAX_ORDERS_PER_SCAN,
        "live_exit_passive_time_in_force": LIVE_EXIT_PASSIVE_TIME_IN_FORCE,
        "live_exit_passive_wait_seconds": LIVE_EXIT_PASSIVE_WAIT_SECONDS,
        "live_exit_passive_reprice_step_cents": LIVE_EXIT_PASSIVE_REPRICE_STEP_CENTS,
        "live_exit_passive_reprice_steps": LIVE_EXIT_PASSIVE_REPRICE_STEPS,
        "live_exit_require_cancel_before_aggressive": LIVE_EXIT_REQUIRE_CANCEL_BEFORE_AGGRESSIVE,
        "live_exit_aggressive_fallback_enabled": LIVE_EXIT_AGGRESSIVE_FALLBACK_ENABLED,
        "live_exit_aggressive_time_in_force": LIVE_EXIT_AGGRESSIVE_TIME_IN_FORCE,
        "live_exit_max_spread_cents": LIVE_EXIT_MAX_SPREAD_CENTS,
        "live_exit_only_when_losing": LIVE_EXIT_ONLY_WHEN_LOSING,
        "live_edge_drop_exit_enabled": LIVE_EDGE_DROP_EXIT_ENABLED,
        "live_edge_drop_trigger_pct_points": LIVE_EDGE_DROP_TRIGGER_PCT_POINTS,
        "live_edge_drop_small_green_max_pct_of_stake": LIVE_EDGE_DROP_SMALL_GREEN_MAX_PCT_OF_STAKE,
        "live_edge_drop_partial_sell_fraction": LIVE_EDGE_DROP_PARTIAL_SELL_FRACTION,
        "live_edge_drop_aggressive_worsen_pct_points": LIVE_EDGE_DROP_AGGRESSIVE_WORSEN_PCT_POINTS,
        "kelly_sizing_enabled": KELLY_SIZING_ENABLED,
        "kelly_fraction": KELLY_FRACTION,
        "kelly_bankroll_dollars": KELLY_BANKROLL_DOLLARS,
        "kelly_max_bet_fraction_of_bankroll": KELLY_MAX_BET_FRACTION_OF_BANKROLL,
        "kelly_min_bet_fraction_of_bankroll": KELLY_MIN_BET_FRACTION_OF_BANKROLL,
        "kelly_price_buffer_pct": KELLY_PRICE_BUFFER_PCT,
        "edge_ladder_sizing_enabled": EDGE_LADDER_SIZING_ENABLED,
        "ladder_unit_fraction_of_bankroll": LADDER_UNIT_FRACTION_OF_BANKROLL,
        "ladder_max_units": LADDER_MAX_UNITS,
        "discord_leaderboard_enabled": DISCORD_LEADERBOARD_ENABLED,
        "discord_discrepancy_enabled": DISCORD_DISCREPANCY_ENABLED,
        "daily_update_discord_enabled": DAILY_UPDATE_DISCORD_ENABLED,
        "daily_update_est_hour": DAILY_UPDATE_EST_HOUR,
        "daily_update_est_minute": DAILY_UPDATE_EST_MINUTE,
        "nyc_forecast_brief_enabled": NYC_FORECAST_BRIEF_ENABLED,
        "nyc_forecast_brief_city": NYC_FORECAST_BRIEF_CITY,
        "nyc_forecast_brief_temp_side": NYC_FORECAST_BRIEF_TEMP_SIDE,
        "nyc_forecast_brief_evening_hour_et": NYC_FORECAST_BRIEF_EVENING_HOUR_ET,
        "nyc_forecast_brief_morning_hour_et": NYC_FORECAST_BRIEF_MORNING_HOUR_ET,
        "nyc_forecast_brief_minute_et": NYC_FORECAST_BRIEF_MINUTE_ET,
        "daily_update_uses_separate_webhook": bool(DAILY_UPDATE_DISCORD_WEBHOOK_URL),
        "nws_obs_stale_minutes": NWS_OBS_STALE_MINUTES,
        "nws_obs_update_minute_hint": NWS_OBS_UPDATE_MINUTE,
        "settlement_day_basis": "city local standard time (fixed offset; DST-safe)",
        "settlement_source": "NWS Daily Climate Report (final)",
        "settlement_time_basis": "local standard time (LST)",
        "settlement_verification_endpoint": "/settlement-map",
        "discrepancy_alert_threshold": DISCREPANCY_ALERT_THRESHOLD,
        "discrepancy_temp_threshold_f": DISCREPANCY_MEAN_TEMP_THRESHOLD_F,
        "accuweather_location_cache_ttl_seconds": ACCUWEATHER_LOCATION_CACHE_TTL_SECONDS,
        "accuweather_forecast_cache_ttl_seconds": ACCUWEATHER_FORECAST_CACHE_TTL_SECONDS,
        "accuweather_stale_fallback_max_age_seconds": ACCUWEATHER_STALE_FALLBACK_MAX_AGE_SECONDS,
        "accuweather_location_lookup_min_seconds": ACCUWEATHER_LOCATION_LOOKUP_MIN_SECONDS,
        "accuweather_location_error_backoff_seconds": ACCUWEATHER_LOCATION_ERROR_BACKOFF_SECONDS,
        "accuweather_last_success_est": _accuweather_last_success_est,
        "accuweather_last_error_est": _accuweather_last_error_est,
        "accuweather_last_error": _accuweather_last_error,
        "live_pretrade_accuweather_refresh_enabled": LIVE_PRETRADE_ACCUWEATHER_REFRESH_ENABLED,
        "live_pretrade_accuweather_max_age_seconds": LIVE_PRETRADE_ACCUWEATHER_MAX_AGE_SECONDS,
        "consensus_sources": configured_sources,
    }

@app.get("/live/status")
def live_status():
    now_local = datetime.now(tz=LOCAL_TZ)
    today_key = now_local.date().isoformat()
    state = _load_live_trade_state(today_key)
    first_trade_date = ""
    last_trade_date = ""
    try:
        rows = load_live_trade_log_rows()
        dates = sorted({
            str(r.get("date", "")).strip()
            for r in rows
            if re.match(r"^\d{4}-\d{2}-\d{2}$", str(r.get("date", "")).strip())
        })
        if dates:
            first_trade_date = dates[0]
            last_trade_date = dates[-1]
    except Exception:
        first_trade_date = ""
        last_trade_date = ""
    total = 0
    by_city_side: Dict[str, int] = {}
    for _, row in state.items():
        c = int(row.get("count", 0))
        total += c
        city = str(row.get("city", "")).strip()
        side = normalize_temp_side(str(row.get("temp_side", "high")))
        if city:
            k = f"{city}|{side}"
            by_city_side[k] = by_city_side.get(k, 0) + c
    return {
        "ok": True,
        "live_trading_enabled": LIVE_TRADING_ENABLED,
        "live_kill_switch": _live_kill_switch_state,
        "date": today_key,
        "orders_placed_today": total,
        "max_orders_per_day": LIVE_MAX_ORDERS_PER_DAY,
        "by_city_side": by_city_side,
        "first_trade_date": first_trade_date,
        "last_trade_date": last_trade_date,
        "log_path": live_trade_log_path(),
    }

@app.post("/live/kill-switch")
def set_live_kill_switch(payload: dict = Body(default={})):
    global _live_kill_switch_state
    enabled = bool(payload.get("enabled", True))
    _live_kill_switch_state = enabled
    return {"ok": True, "live_kill_switch": _live_kill_switch_state}

@app.get("/live/last-orders")
def live_last_orders(limit: int = 20, status: Optional[str] = None):
    paths = list_live_trade_log_paths()
    if not paths:
        return {"ok": True, "count": 0, "orders": [], "path": live_trade_log_path(), "paths": []}
    rows = load_live_trade_log_rows()
    status_filter = (status or "").strip().lower()
    if status_filter:
        rows = [r for r in rows if str(r.get("status", "")).strip().lower() == status_filter]
    lim = max(1, min(500, int(limit)))
    out = rows[-lim:]
    out.reverse()
    return {
        "ok": True,
        "count": len(out),
        "total_matching": len(rows),
        "orders": out,
        "path": live_trade_log_path(),
        "paths": paths,
    }

@app.post("/live/backfill-fees")
def live_backfill_fees(
    force: bool = False,
    limit: int = 0,
):
    if not kalshi_has_auth_config():
        return {"ok": False, "error": "Kalshi auth not configured"}

    paths = list_live_trade_log_paths()
    if not paths:
        return {"ok": True, "updated_rows": 0, "checked_rows": 0, "files": 0, "paths": []}

    max_rows = max(0, int(limit or 0))
    checked = 0
    updated = 0
    unresolved = 0
    file_updates: Dict[str, int] = {}
    order_fee_cache: Dict[str, float] = {}

    for path in paths:
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                rows = [dict(r) for r in csv.DictReader(f)]
        except Exception:
            continue

        changed_here = 0
        for r in rows:
            if max_rows > 0 and checked >= max_rows:
                break
            st = str(r.get("status", "")).strip().lower()
            if st not in ("submitted", "partial", "partial_filled"):
                continue
            oid = str(r.get("order_id", "")).strip()
            if not oid:
                continue
            existing_fee = float(_to_float(r.get("fee_dollars")) or 0.0)
            if existing_fee > 0.0 and not force:
                continue
            checked += 1
            if oid in order_fee_cache:
                fee = order_fee_cache[oid]
            else:
                fee = kalshi_get_order_fee_dollars(oid)
                order_fee_cache[oid] = fee
            if fee > 0.0:
                r["fee_dollars"] = f"{float(fee):.6f}"
                changed_here += 1
                updated += 1
            else:
                unresolved += 1

        if changed_here > 0:
            rewrite_live_trade_log_rows(path, rows)
            file_updates[path] = changed_here
        if max_rows > 0 and checked >= max_rows:
            break

    return {
        "ok": True,
        "checked_rows": checked,
        "updated_rows": updated,
        "unresolved_rows": unresolved,
        "files": len(file_updates),
        "updated_by_file": file_updates,
        "paths": paths,
    }

@app.get("/analytics/edge-durations")
def analytics_edge_durations(date: Optional[str] = None, limit: int = 200):
    target_date = date or datetime.now(tz=LOCAL_TZ).date().isoformat()
    history_path = edge_lifecycle_history_path()
    rows: List[dict] = []
    if os.path.exists(history_path):
        with open(history_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if str(r.get("date", "")) == target_date:
                    rows.append(dict(r))
    rows.sort(key=lambda r: float(r.get("duration_seconds", 0) or 0), reverse=True)

    # Include still-active edges for the target date as provisional durations.
    active_rows: List[dict] = []
    state = _load_edge_lifecycle_state(target_date)
    if str(state.get("date", "")) == target_date:
        now_local = datetime.now(tz=LOCAL_TZ)
        now_ts = now_local.timestamp()
        for sig, e in state.get("entries", {}).items():
            first_ts = float(e.get("first_seen_ts", now_ts))
            last_ts = float(e.get("last_seen_ts", now_ts))
            duration = max(0.0, last_ts - first_ts)
            active_rows.append({
                "date": target_date,
                "sig": sig,
                "city": e.get("city"),
                "temp_type": e.get("temp_type"),
                "ticker": e.get("ticker"),
                "bet": e.get("bet"),
                "line": e.get("line"),
                "first_seen_est": e.get("first_seen_est"),
                "last_seen_est": e.get("last_seen_est"),
                "end_seen_est": None,
                "duration_seconds": int(round(duration)),
                "scan_count": int(e.get("scan_count", 0)),
                "max_edge_pct": float(e.get("max_edge_pct", 0.0)),
                "last_edge_pct": float(e.get("last_edge_pct", 0.0)),
                "close_reason": "active",
            })
    active_rows.sort(key=lambda r: float(r.get("duration_seconds", 0) or 0), reverse=True)

    lim = max(1, min(1000, int(limit)))
    closed_preview = rows[:lim]
    active_preview = active_rows[:lim]

    closed_count = len(rows)
    avg_sec = 0.0
    med_sec = 0.0
    if rows:
        durations = sorted([float(r.get("duration_seconds", 0) or 0) for r in rows])
        avg_sec = sum(durations) / len(durations)
        med_sec = durations[len(durations) // 2]
    return {
        "ok": True,
        "date": target_date,
        "edge_tracking_enabled": EDGE_TRACKING_ENABLED,
        "closed_count": closed_count,
        "active_count": len(active_rows),
        "closed_avg_duration_minutes": round(avg_sec / 60.0, 2),
        "closed_median_duration_minutes": round(med_sec / 60.0, 2),
        "closed_preview": closed_preview,
        "active_preview": active_preview,
        "history_path": history_path,
        "state_path": edge_lifecycle_state_path(),
    }

@app.get("/settlement-map")
def settlement_map(force_refresh: bool = False):
    try:
        series_by_city = _load_weather_series_by_city(force=force_refresh)
        meta = _load_series_metadata_map(force=force_refresh)
    except RuntimeError as e:
        return {"ok": False, "error": str(e), "kalshi_base_url": KALSHI_BASE_URL}

    rows: List[dict] = []
    for city, cfg in CITY_CONFIG.items():
        item = {
            "city": city,
            "station": cfg["station"],
            "cli": cfg["cli"],
            "lat": cfg["lat"],
            "lon": cfg["lon"],
            "high_series": [],
            "low_series": [],
        }
        for side in ("high", "low"):
            for st in series_by_city.get(city, {}).get(side, []):
                m = meta.get(st, {})
                item[f"{side}_series"].append({
                    "series_ticker": st,
                    "series_title": m.get("title"),
                    "series_category": m.get("category"),
                    "contract_terms_url": m.get("contract_terms_url"),
                    "settlement_sources": m.get("settlement_sources", []),
                })
        rows.append(item)

    rows.sort(key=lambda x: x["city"])
    return {
        "ok": True,
        "as_of_est": fmt_est(datetime.now(tz=LOCAL_TZ)),
        "kalshi_base_url": KALSHI_BASE_URL,
        "cities": rows,
    }

@app.post("/settlement/backfill")
def settlement_backfill(date: str, source: str = "cli_final"):
    now_local = datetime.now(tz=LOCAL_TZ)
    rows: List[dict] = []
    src = str(source or "cli_final").strip().lower()
    if src not in ("cli_final", "obs_proxy", "auto"):
        return {"ok": False, "date": date, "error": "source must be one of: cli_final, obs_proxy, auto"}
    missing_cli: List[str] = []
    cli_errors: List[str] = []
    for city, cfg in CITY_CONFIG.items():
        station = cfg["station"]
        cli_hit = None
        if src in ("cli_final", "auto"):
            try:
                cli_hit = nws_cli_final_for_date(cfg.get("cli", ""), date)
            except Exception as e:
                cli_errors.append(f"{city}: {e}")
            if cli_hit:
                for side in ("high", "low"):
                    key = "high_f" if side == "high" else "low_f"
                    v = _to_float(cli_hit.get(key))
                    if v is None:
                        continue
                    rows.append({
                        "date": date,
                        "city": city,
                        "temp_side": side,
                        "station": station,
                        "outcome_f": round(float(v), 3),
                        "source": "cli_final",
                        "updated_ts_est": fmt_est(now_local),
                    })
                continue
            missing_cli.append(city)
        if src in ("obs_proxy", "auto"):
            for side in ("high", "low"):
                v = nws_day_outcome_f(station, date, side)
                if v is None:
                    continue
                rows.append({
                    "date": date,
                    "city": city,
                    "temp_side": side,
                    "station": station,
                    "outcome_f": round(float(v), 3),
                    "source": "obs_proxy",
                    "updated_ts_est": fmt_est(now_local),
                })
    if not rows:
        return {
            "ok": False,
            "date": date,
            "error": "no outcomes available from requested source(s)",
            "source": src,
            "missing_cli_cities": missing_cli,
            "cli_errors": cli_errors[:10],
        }
    upsert_final_settlements(rows)
    return {
        "ok": True,
        "date": date,
        "rows_upserted": len(rows),
        "source": src,
        "missing_cli_cities": missing_cli if src in ("cli_final", "auto") else [],
        "cli_errors": cli_errors[:10],
        "path": final_settlements_path(),
    }

@app.get("/calibration")
def calibration(min_samples: int = CALIBRATION_MIN_SAMPLES):
    tables = build_calibration_tables()
    city_rows = []
    for (city, side, lb), row in tables.get("city_side_lead", {}).items():
        if not row or row.get("n", 0) < int(min_samples):
            continue
        city_rows.append({
            "city": city,
            "temp_side": side,
            "lead_bin": lb,
            "n": row.get("n"),
            "avg_win_pct": 100.0 * float(row.get("avg_win", 0.0)),
            "avg_market_prob_pct": 100.0 * float(row.get("avg_market", 0.0)),
            "avg_raw_edge_pct": 100.0 * float(row.get("avg_raw_edge", 0.0)),
            "empirical_edge_pct": 100.0 * float(row.get("empirical_edge", 0.0)),
            "shrink": float(row.get("shrink", 0.0)),
        })
    city_rows.sort(key=lambda x: (x["city"], x["temp_side"], x["lead_bin"]))

    side_rows = []
    for (side, lb), row in tables.get("side_lead", {}).items():
        if not row or row.get("n", 0) < int(min_samples):
            continue
        side_rows.append({
            "temp_side": side,
            "lead_bin": lb,
            "n": row.get("n"),
            "avg_win_pct": 100.0 * float(row.get("avg_win", 0.0)),
            "avg_market_prob_pct": 100.0 * float(row.get("avg_market", 0.0)),
            "avg_raw_edge_pct": 100.0 * float(row.get("avg_raw_edge", 0.0)),
            "empirical_edge_pct": 100.0 * float(row.get("empirical_edge", 0.0)),
            "shrink": float(row.get("shrink", 0.0)),
        })
    side_rows.sort(key=lambda x: (x["temp_side"], x["lead_bin"]))

    g = tables.get("global")
    global_row = None
    if g:
        global_row = {
            "n": g.get("n"),
            "avg_win_pct": 100.0 * float(g.get("avg_win", 0.0)),
            "avg_market_prob_pct": 100.0 * float(g.get("avg_market", 0.0)),
            "avg_raw_edge_pct": 100.0 * float(g.get("avg_raw_edge", 0.0)),
            "empirical_edge_pct": 100.0 * float(g.get("empirical_edge", 0.0)),
            "shrink": float(g.get("shrink", 0.0)),
        }

    return {
        "ok": True,
        "calibration_enabled": CALIBRATION_ENABLED,
        "min_samples": int(min_samples),
        "city_side_lead": city_rows,
        "side_lead": side_rows,
        "global": global_row,
        "final_settlements_path": final_settlements_path(),
    }

@app.get("/board")
def board(market_day: str = "auto", force_refresh: bool = False):
    now_local = datetime.now(tz=LOCAL_TZ)
    day_pref = normalize_market_day(market_day)
    with _market_cache_lock:
        if not force_refresh:
            ts = float(_board_cache.get("ts", 0.0))
            cached_day = str(_board_cache.get("market_day", ""))
            cached_payload = _board_cache.get("payload")
            if cached_payload is not None and cached_day == day_pref and (time.time() - ts) < BOARD_CACHE_TTL_SECONDS:
                return cached_payload
    try:
        payload = build_odds_board(now_local, market_day=day_pref)
    except RuntimeError as e:
        return {"ok": False, "error": str(e), "kalshi_base_url": KALSHI_BASE_URL}
    out = {
        "ok": True,
        "as_of_est": fmt_est(now_local),
        "market_day_requested": day_pref,
        "rows": payload["rows"],
        "unavailable": payload["unavailable"],
    }
    with _market_cache_lock:
        _board_cache["ts"] = time.time()
        _board_cache["market_day"] = day_pref
        _board_cache["payload"] = out
    return out

@app.get("/bets")
def bets(market_day: str = "auto", top_n: int = 20, force_refresh: bool = False):
    day_pref = normalize_market_day(market_day)
    b = board(market_day=day_pref, force_refresh=force_refresh)
    if not b.get("ok"):
        return b
    rows = b.get("rows", [])[:max(1, min(100, int(top_n)))]
    simplified = []
    for i, r in enumerate(rows, start=1):
        simplified.append({
            "rank": i,
            "date": r.get("market_date_selected"),
            "city": r.get("city"),
            "temp_type": r.get("temp_side"),
            "bet": r.get("best_side"),
            "edge_pct": round(float(r.get("net_calibrated_edge_pct", r.get("edge_pct", 0.0))), 1),
            "raw_edge_pct": round(float(r.get("raw_edge_pct", r.get("edge_pct", 0.0))), 1),
            "calibrated_edge_pct": round(float(r.get("calibrated_edge_pct", r.get("edge_pct", 0.0))), 1),
            "line": r.get("bucket_label"),
            "ticker": r.get("ticker"),
            "calibration_meta": r.get("calibration_meta"),
        })
    return {
        "ok": True,
        "as_of_est": b.get("as_of_est"),
        "market_day_requested": day_pref,
        "count": len(simplified),
        "bets": simplified,
    }

@app.get("/policy")
def policy(
    market_day: str = "auto",
    top_n: int = 20,
    min_edge_pct: float = POLICY_MIN_NET_EDGE_PCT,
    dry_run: bool = False,
    force_refresh: bool = False,
):
    day_pref = normalize_market_day(market_day)
    b = board(market_day=day_pref, force_refresh=force_refresh)
    if not b.get("ok"):
        return b
    executable, excluded = build_policy_bets_from_board_payload(
        b,
        top_n=max(1, min(200, int(top_n))),
        min_edge_pct=float(min_edge_pct),
    )

    out = {
        "ok": True,
        "as_of_est": b.get("as_of_est"),
        "market_day_requested": day_pref,
        "policy": {
            "min_edge_pct": float(min_edge_pct),
            "top_n": max(1, min(200, int(top_n))),
            "uses_board_filters": True,
            "uses_calibrated_net_edge": True,
        },
        "count": len(executable),
        "bets": executable,
    }
    if dry_run:
        out["excluded_count"] = len(excluded) + len(b.get("unavailable", []))
        out["excluded_preview"] = excluded[:50]
        out["board_unavailable_preview"] = b.get("unavailable", [])[:50]
    return out

@app.get("/debug/live-candidate-funnel")
def debug_live_candidate_funnel(market_day: str = "auto", force_refresh: bool = False):
    now_local = datetime.now(tz=LOCAL_TZ)
    return debug_live_candidate_funnel_snapshot(
        now_local,
        market_day=market_day,
        force_refresh=force_refresh,
    )

@app.get("/debug/city-comparison")
def debug_city_comparison(
    city: str,
    temp_side: str = "high",
    market_day: str = "auto",
    force_refresh: bool = False,
):
    now_local = datetime.now(tz=LOCAL_TZ)
    return debug_city_bucket_comparison(
        city=city,
        now_local=now_local,
        temp_side=temp_side,
        market_day=market_day,
        force_refresh=force_refresh,
    )

@app.get("/debug/orderbook")
def debug_orderbook(ticker: str):
    ob = kalshi_get_orderbook(ticker)
    quotes = best_quotes_from_orderbook(ob)
    return {
        "ok": True,
        "ticker": ticker,
        "quotes": quotes,
        "raw": ob,
    }

def build_policy_bets_from_board_payload(board_payload: dict, top_n: int, min_edge_pct: float) -> Tuple[List[dict], List[dict]]:
    max_rows = max(1, min(200, int(top_n)))
    threshold = float(min_edge_pct)
    executable: List[dict] = []
    excluded: List[dict] = []
    for r in board_payload.get("rows", []):
        net_edge_pct = float(r.get("net_calibrated_edge_pct", r.get("edge_pct", 0.0)))
        locked_candidate = bool(r.get("is_locked_capture_candidate", False))
        locked_allowed = (
            LIVE_LOCKED_OUTCOME_CAPTURE_ENABLED
            and locked_candidate
            and str(r.get("best_side", "")).upper() == "BUY NO"
            and float(r.get("market_win_prob_pct", 0.0)) > (NO_TRADE_IMPLIED_PROB_MAX * 100.0)
            and net_edge_pct >= LIVE_LOCKED_OUTCOME_MIN_NET_EDGE_PCT
        )

        if net_edge_pct < threshold and not locked_allowed:
            reason = "below min_edge_pct"
            if locked_candidate and LIVE_LOCKED_OUTCOME_CAPTURE_ENABLED:
                reason = "locked capture below min_locked_net_edge_pct"
            excluded.append({
                "city": r.get("city"),
                "temp_type": r.get("temp_side"),
                "date": r.get("market_date_selected"),
                "line": r.get("bucket_label"),
                "ticker": r.get("ticker"),
                "reason": reason,
                "net_edge_pct": round(net_edge_pct, 2),
            })
            continue
        suggested_units = suggested_units_from_net_edge(net_edge_pct)
        if locked_allowed:
            suggested_units = min(float(suggested_units), float(LIVE_LOCKED_OUTCOME_MAX_UNITS))
        executable.append({
            "date": r.get("market_date_selected"),
            "city": r.get("city"),
            "temp_type": r.get("temp_side"),
            "bet": r.get("best_side"),
            "line": r.get("bucket_label"),
            "ticker": r.get("ticker"),
            "market_implied_win_prob_pct": round(float(r.get("market_win_prob_pct", 0.0)), 2),
            "model_yes_prob_pct": round(float(r.get("model_yes_prob_pct", 0.0)), 2),
            "kalshi_yes_prob_pct": round(float(r.get("kalshi_yes_prob_pct", 0.0)), 2),
            "raw_edge_pct": round(float(r.get("raw_edge_pct", r.get("edge_pct", 0.0))), 2),
            "calibrated_edge_pct": round(float(r.get("calibrated_edge_pct", r.get("edge_pct", 0.0))), 2),
            "net_edge_pct": round(net_edge_pct, 2),
            "suggested_units": suggested_units,
            "calibration_meta": r.get("calibration_meta"),
            "yes_bid": r.get("yes_bid"),
            "yes_ask": r.get("yes_ask"),
            "spread_cents": r.get("spread_cents"),
            "top_size": r.get("top_size"),
            "nws_obs_time_est": r.get("nws_obs_time_est"),
            "nws_obs_age_minutes": r.get("nws_obs_age_minutes"),
            "nws_obs_fresh": r.get("nws_obs_fresh"),
            "locked_outcome": bool(r.get("locked_outcome", False)),
            "locked_reason": r.get("locked_reason"),
            "trade_mode": ("locked_capture" if locked_allowed else "normal"),
            "source_values_key": json.dumps((r.get("source_values_map") or {}), sort_keys=True, separators=(",", ":")),
        })
    executable.sort(key=lambda x: x.get("net_edge_pct", -1e9), reverse=True)
    return executable[:max_rows], excluded

def debug_live_candidate_funnel_snapshot(
    now_local: datetime,
    market_day: str = "auto",
    force_refresh: bool = False,
) -> dict:
    day_pref = normalize_market_day(market_day)
    board_payload = board(market_day=day_pref, force_refresh=force_refresh)
    if not board_payload.get("ok"):
        return board_payload

    board_rows = list(board_payload.get("rows", []) or [])
    board_unavailable = list(board_payload.get("unavailable", []) or [])
    policy_bets, policy_excluded = build_policy_bets_from_board_payload(
        board_payload,
        top_n=200,
        min_edge_pct=POLICY_MIN_NET_EDGE_PCT,
    )

    def _add_reason(reason_counts: Dict[str, int], reason_examples: Dict[str, List[dict]], reason: str, payload: dict) -> None:
        key = str(reason or "unknown").strip() or "unknown"
        reason_counts[key] = reason_counts.get(key, 0) + 1
        bucket = reason_examples.setdefault(key, [])
        if len(bucket) < 5:
            bucket.append(payload)

    unavailable_reason_counts: Dict[str, int] = {}
    unavailable_examples: Dict[str, List[dict]] = {}
    for row in board_unavailable:
        _add_reason(
            unavailable_reason_counts,
            unavailable_examples,
            str(row.get("reason", "unknown")),
            {
                "city": row.get("city"),
                "temp_side": row.get("temp_side"),
                "market_date_selected": row.get("market_date_selected"),
            },
        )

    policy_reason_counts: Dict[str, int] = {}
    policy_examples: Dict[str, List[dict]] = {}
    for row in policy_excluded:
        _add_reason(
            policy_reason_counts,
            policy_examples,
            str(row.get("reason", "unknown")),
            {
                "city": row.get("city"),
                "temp_type": row.get("temp_type"),
                "line": row.get("line"),
                "ticker": row.get("ticker"),
                "net_edge_pct": row.get("net_edge_pct"),
            },
        )

    today_key = now_local.date().isoformat()
    state = _load_live_trade_state(today_key)
    edge_state = _load_edge_lifecycle_state(today_key)
    edge_entries = edge_state.get("entries", {}) if isinstance(edge_state, dict) else {}
    current_bot_exposure_dollars = _current_live_bot_exposure_dollars(now_local, state)
    open_position_sigs = _open_live_position_signatures(now_local)
    blocked_tickers = _manual_blocked_tickers()
    now_et = now_local.astimezone(LOCAL_TZ)
    hour_et = int(now_et.hour)
    early_session = (
        LIVE_EARLY_SESSION_ENABLED
        and (hour_et >= LIVE_EARLY_SESSION_START_HOUR_ET)
        and (hour_et < LIVE_EARLY_SESSION_END_HOUR_ET)
    )

    per_city_side: Dict[Tuple[str, str], int] = {}
    total_orders = 0
    for _, row in state.items():
        total_orders += int(row.get("count", 0) or 0)
        city_k = str(row.get("city", "")).strip()
        side_k = normalize_temp_side(str(row.get("temp_side", "high")))
        if city_k:
            per_city_side[(city_k, side_k)] = per_city_side.get((city_k, side_k), 0) + int(row.get("count", 0) or 0)

    execution_reason_counts: Dict[str, int] = {}
    execution_examples: Dict[str, List[dict]] = {}
    eligible_now: List[dict] = []
    pending_resting: List[dict] = []
    scan_capacity_remaining = max(0, max(1, LIVE_MAX_ORDERS_PER_SCAN) - min(total_orders, max(1, LIVE_MAX_ORDERS_PER_SCAN)))

    for b in policy_bets:
        sig = _live_order_signature(b)
        if sig in open_position_sigs:
            _add_reason(execution_reason_counts, execution_examples, "open_position_already_held", {
                "date": b.get("date"),
                "city": b.get("city"),
                "temp_type": b.get("temp_type"),
                "bet": b.get("bet"),
                "line": b.get("line"),
                "ticker": b.get("ticker"),
            })
            continue
        row = state.get(sig, {}) or {}
        already = int(row.get("count", 0) or 0)
        city_k = str(b.get("city", "")).strip()
        side_k = normalize_temp_side(str(b.get("temp_type", "high")))
        ticker = str(b.get("ticker", "")).strip()
        bet_side = str(b.get("bet", "")).strip().upper()
        edge_pct = float(b.get("net_edge_pct", 0.0) or 0.0)
        sig_entry = edge_entries.get(sig, {}) or {}
        sig_scans = int(sig_entry.get("scan_count", 1) or 1)

        base_payload = {
            "date": b.get("date"),
            "city": city_k,
            "temp_type": side_k,
            "bet": bet_side,
            "line": b.get("line"),
            "ticker": ticker,
            "net_edge_pct": round(edge_pct, 2),
            "spread_cents": b.get("spread_cents"),
            "top_size": b.get("top_size"),
            "scan_count": sig_scans,
        }

        pending_order_id = str(row.get("pending_passive_order_id", "")).strip()
        if pending_order_id:
            pending_resting.append({
                **base_payload,
                "pending_passive_order_id": pending_order_id,
                "pending_passive_price_cents": row.get("pending_passive_price_cents"),
                "pending_passive_requested_count": row.get("pending_passive_requested_count"),
            })
            _add_reason(execution_reason_counts, execution_examples, "existing_resting_passive_order", base_payload)
            continue
        if total_orders >= max(1, LIVE_MAX_ORDERS_PER_DAY):
            _add_reason(execution_reason_counts, execution_examples, "max_orders_per_day_reached", base_payload)
            continue
        if already >= max(1, LIVE_MAX_ORDERS_PER_MARKET_PER_DAY):
            _add_reason(execution_reason_counts, execution_examples, "max_orders_per_market_reached", base_payload)
            continue
        if city_k and per_city_side.get((city_k, side_k), 0) >= max(1, LIVE_MAX_ORDERS_PER_CITY_SIDE_PER_DAY):
            _add_reason(execution_reason_counts, execution_examples, "max_orders_per_city_side_reached", base_payload)
            continue
        if ticker in blocked_tickers:
            _add_reason(execution_reason_counts, execution_examples, "manual_market_blocked", base_payload)
            continue
        order_side, price_field = _bet_side_and_price_field(bet_side)
        if order_side is None or price_field is None:
            _add_reason(execution_reason_counts, execution_examples, "invalid_bet_side", base_payload)
            continue
        if not ticker:
            _add_reason(execution_reason_counts, execution_examples, "missing_ticker", base_payload)
            continue
        if early_session and ((not LIVE_EARLY_SESSION_APPLY_TO_HIGH_ONLY) or (side_k == "high")):
            if edge_pct < float(LIVE_EARLY_SESSION_MIN_EDGE_PCT):
                _add_reason(execution_reason_counts, execution_examples, "early_session_min_edge", base_payload)
                continue
            if sig_scans < max(1, LIVE_EARLY_SESSION_MIN_SCANS):
                _add_reason(execution_reason_counts, execution_examples, "early_session_min_scans", base_payload)
                continue
        if LIVE_STABILITY_GATE_ENABLED and (LIVE_STABILITY_GATE_EDGE_MIN_PCT <= edge_pct < LIVE_STABILITY_GATE_EDGE_MAX_PCT):
            if sig_scans < max(1, LIVE_STABILITY_GATE_MIN_SCANS_MID):
                _add_reason(execution_reason_counts, execution_examples, "stability_gate_min_scans", base_payload)
                continue
            if LIVE_STABILITY_REQUIRE_CHANGE_MID and (not bool(sig_entry.get("fresh_trigger", False))):
                _add_reason(execution_reason_counts, execution_examples, "stability_gate_requires_fresh_trigger", base_payload)
                continue
        stake_dollars, _kelly_units = _compute_stake_dollars_for_bet(b)
        if early_session and ((not LIVE_EARLY_SESSION_APPLY_TO_HIGH_ONLY) or (side_k == "high")):
            size_mult = clamp(float(LIVE_EARLY_SESSION_SIZE_MULT), 0.05, 1.0)
            stake_dollars = max(0.0, stake_dollars * size_mult)
        if (stake_dollars > 0.0) and (
            float(current_bot_exposure_dollars) + float(stake_dollars) > float(LIVE_MAX_OPEN_BOT_EXPOSURE_DOLLARS)
        ):
            _add_reason(execution_reason_counts, execution_examples, "max_open_bot_exposure_reached", {
                **base_payload,
                "stake_dollars": round(stake_dollars, 2),
                "current_bot_exposure_dollars": round(current_bot_exposure_dollars, 2),
            })
            continue
        eligible_now.append(base_payload)

    top_board_preview = []
    for r in board_rows[:10]:
        top_board_preview.append({
            "city": r.get("city"),
            "temp_side": r.get("temp_side"),
            "date": r.get("market_date_selected"),
            "line": r.get("bucket_label"),
            "ticker": r.get("ticker"),
            "best_side": r.get("best_side"),
            "net_edge_pct": round(float(r.get("net_calibrated_edge_pct", r.get("edge_pct", 0.0)) or 0.0), 2),
            "spread_cents": r.get("spread_cents"),
            "top_size": r.get("top_size"),
        })

    return {
        "ok": True,
        "as_of_est": board_payload.get("as_of_est"),
        "market_day_requested": day_pref,
        "scan_context": {
            "today_key": today_key,
            "hour_et": hour_et,
            "early_session_active": early_session,
            "live_trading_enabled": LIVE_TRADING_ENABLED,
            "live_kill_switch": _live_kill_switch_state,
            "policy_min_net_edge_pct": POLICY_MIN_NET_EDGE_PCT,
            "scan_capacity_remaining": scan_capacity_remaining,
            "orders_placed_today": total_orders,
            "current_bot_exposure_dollars": round(current_bot_exposure_dollars, 2),
            "live_max_open_bot_exposure_dollars": float(LIVE_MAX_OPEN_BOT_EXPOSURE_DOLLARS),
        },
        "counts": {
            "board_rows": len(board_rows),
            "board_unavailable": len(board_unavailable),
            "policy_executable": len(policy_bets),
            "policy_excluded": len(policy_excluded),
            "execution_eligible_now": len(eligible_now),
            "execution_filtered_after_policy": sum(execution_reason_counts.values()),
            "pending_resting_orders": len(pending_resting),
        },
        "board_unavailable_reasons": unavailable_reason_counts,
        "policy_excluded_reasons": policy_reason_counts,
        "execution_filtered_reasons": execution_reason_counts,
        "top_board_preview": top_board_preview,
        "eligible_now_preview": eligible_now[:20],
        "pending_resting_preview": pending_resting[:20],
        "examples": {
            "board_unavailable": unavailable_examples,
            "policy_excluded": policy_examples,
            "execution_filtered": execution_examples,
        },
    }

def paper_trade_signature(bet: dict) -> str:
    return f"{bet.get('date','')}|{bet.get('ticker','')}|{bet.get('bet','')}"

def paper_trade_text(now_local: datetime, bets: List[dict]) -> str:
    ts = fmt_est_short(now_local)
    lines = [f"Trade Alert ({ts})", "Date | City | Type | Bet | Edge | Line | Ticker", "---"]
    for b in bets:
        lines.append(
            f"{b.get('date')} | {b.get('city')} | {b.get('temp_type')} | {b.get('bet')} | "
            f"{float(b.get('net_edge_pct', 0.0)):.1f}% | {b.get('line')} | {b.get('ticker')}"
        )
    return "\n".join(lines)

def maybe_post_paper_trades(now_local: datetime, board_payload: dict) -> int:
    bets, _ = build_policy_bets_from_board_payload(
        board_payload,
        top_n=max(1, PAPER_TRADE_POST_TOP_N),
        min_edge_pct=POLICY_MIN_NET_EDGE_PCT,
    )
    # Live execution should evaluate current qualifying bets every scan,
    # not only newly-posted Discord alerts.
    try:
        maybe_execute_live_trades(now_local, bets)
    except Exception:
        pass
    try:
        maybe_execute_live_exits(now_local)
    except Exception:
        pass

    if not PAPER_TRADE_DISCORD_ENABLED or not DISCORD_TRADE_ALERTS_ENABLED:
        return 0

    today_key = now_local.date().isoformat()
    state = _load_paper_trade_alert_state(today_key)
    city_side_counts: Dict[Tuple[str, str], int] = {}
    for _, row in state.items():
        city_k = str(row.get("city", "")).strip()
        side_k = normalize_temp_side(str(row.get("temp_side", "high")))
        if not city_k:
            continue
        cnt = int(row.get("count", 0))
        city_side_counts[(city_k, side_k)] = city_side_counts.get((city_k, side_k), 0) + max(0, cnt)
    min_gap_seconds = max(0, PAPER_TRADE_MIN_MINUTES_BETWEEN_RE_ALERTS * 60)
    new_bets: List[dict] = []
    for b in bets:
        sig = paper_trade_signature(b)
        city_k = str(b.get("city", "")).strip()
        side_k = normalize_temp_side(str(b.get("temp_type", "high")))
        edge = float(b.get("net_edge_pct", 0.0))
        row = state.get(sig, {})
        count = int(row.get("count", 0))
        last_edge = float(row.get("last_edge_pct", -1e9))
        last_ts = float(row.get("last_post_ts_epoch", 0.0))
        if count >= max(1, PAPER_TRADE_MAX_ALERTS_PER_MARKET_PER_DAY):
            continue
        if city_k and city_side_counts.get((city_k, side_k), 0) >= max(1, PAPER_TRADE_MAX_ALERTS_PER_CITY_SIDE_PER_DAY):
            continue
        if count > 0 and (edge - last_edge) < PAPER_TRADE_MIN_EDGE_IMPROVEMENT_PCT:
            continue
        if count > 0 and (now_local.timestamp() - last_ts) < min_gap_seconds:
            continue
        state[sig] = {
            "count": count + 1,
            "last_edge_pct": edge,
            "last_post_ts_epoch": now_local.timestamp(),
            "last_post_ts_est": fmt_est(now_local),
            "city": city_k,
            "temp_side": side_k,
        }
        city_side_counts[(city_k, side_k)] = city_side_counts.get((city_k, side_k), 0) + 1
        new_bets.append(b)
    if not new_bets:
        return 0
    discord_send(paper_trade_text(now_local, new_bets))
    _save_paper_trade_alert_state(today_key)
    return len(new_bets)

@app.get("/bets.txt", response_class=PlainTextResponse)
def bets_txt(market_day: str = "auto", top_n: int = 20, force_refresh: bool = False):
    payload = bets(market_day=market_day, top_n=top_n, force_refresh=force_refresh)
    if not payload.get("ok"):
        return f"ERROR: {payload.get('error', 'unable to build bets')}"
    lines = [f"Kalshi Weather Bets ({payload.get('as_of_est')}) day={payload.get('market_day_requested')}"]
    lines.append("Rank | Date | City | Type | Bet | Edge | Line | Ticker")
    lines.append("---")
    for r in payload.get("bets", []):
        lines.append(
            f"{r['rank']:>2} | {r['date']} | {r['city']} | {r['temp_type']} | {r['bet']} | {r['edge_pct']:.1f}% | {r['line']} | {r['ticker']}"
        )
    return "\n".join(lines)

@app.get("/analytics/day")
def analytics_day(date: str, city: Optional[str] = None, temp_side: Optional[str] = None):
    path = snapshot_log_path()
    if not os.path.exists(path):
        return {"ok": False, "error": "no snapshot log file yet", "path": path}

    rows = load_snapshot_rows_filtered(date=date, city=city, temp_side=temp_side)

    if not rows:
        return {"ok": True, "date": date, "rows": 0, "analytics": []}

    groups: Dict[str, List[dict]] = {}
    for r in rows:
        key = f"{r.get('city')}|{normalize_temp_side(r.get('temp_side','high'))}"
        groups.setdefault(key, []).append(r)

    analytics: List[dict] = []
    for key, grp in groups.items():
        grp.sort(key=lambda x: x.get("ts_est", ""))
        city_name, side = key.split("|", 1)
        station = CITY_CONFIG.get(city_name, {}).get("station", "")
        outcome = get_outcome_f(date, city_name, side, station) if station else None
        first_mu = _to_float(grp[0].get("consensus_mu_f"))
        last_mu = _to_float(grp[-1].get("consensus_mu_f"))
        avg_edge = 0.0
        edge_n = 0
        correct = 0
        evaluated = 0
        for r in grp:
            e = _to_float(r.get("best_edge"))
            if e is not None:
                avg_edge += e
                edge_n += 1
            if outcome is None:
                continue
            lo = _to_float(r.get("best_lo"))
            hi = _to_float(r.get("best_hi"))
            side_bet = str(r.get("best_side", ""))
            if lo is None or hi is None or not side_bet:
                continue
            yes_outcome = (outcome >= lo and outcome <= hi)
            bet_correct = yes_outcome if "YES" in side_bet else (not yes_outcome)
            evaluated += 1
            if bet_correct:
                correct += 1

        analytics.append({
            "city": city_name,
            "temp_side": side,
            "station": station,
            "snapshot_count": len(grp),
            "first_consensus_mu_f": first_mu,
            "last_consensus_mu_f": last_mu,
            "consensus_shift_f": (None if first_mu is None or last_mu is None else (last_mu - first_mu)),
            "avg_recommended_edge_pct": (None if edge_n == 0 else (avg_edge / edge_n) * 100.0),
            "realized_outcome_f": outcome,
            "bet_direction_accuracy_pct": (None if evaluated == 0 else (100.0 * correct / evaluated)),
            "evaluated_snapshots": evaluated,
        })

    analytics.sort(key=lambda x: (x.get("avg_recommended_edge_pct") or -999), reverse=True)
    return {
        "ok": True,
        "date": date,
        "rows": len(rows),
        "analytics": analytics,
    }

@app.get("/analytics/ev")
def analytics_ev(date: str, stake: float = 100.0, city: Optional[str] = None, temp_side: Optional[str] = None):
    path = snapshot_log_path()
    if not os.path.exists(path):
        return {"ok": False, "error": "no snapshot log file yet", "path": path}

    rows = load_snapshot_rows_filtered(date=date, city=city, temp_side=temp_side)

    rows = dedupe_snapshot_rows(rows)
    if not rows:
        return {"ok": True, "date": date, "stake": stake, "rows": 0, "samples": [], "bins": {}}

    outcome_cache: Dict[Tuple[str, str, str], Optional[float]] = {}
    samples: List[dict] = []
    for r in rows:
        side = str(r.get("best_side", ""))
        eval_side = normalize_temp_side(str(r.get("temp_side", "high")))
        if eval_side == "low" and not LOW_SIGNALS_ENABLED:
            continue
        model_yes = _to_float(r.get("model_yes_prob"))
        yes_bid = _to_float(r.get("yes_bid"))
        yes_ask = _to_float(r.get("yes_ask"))
        lo = _to_float(r.get("best_lo"))
        hi = _to_float(r.get("best_hi"))
        if model_yes is None or yes_bid is None or yes_ask is None or lo is None or hi is None:
            continue

        model_yes = clamp(model_yes, MODEL_WIN_PROB_FLOOR, MODEL_WIN_PROB_CEIL)
        yes_bid_p = yes_bid / 100.0
        yes_ask_p = yes_ask / 100.0
        if "YES" in side:
            market_prob = yes_ask_p
            model_win_prob = model_yes
        else:
            market_prob = 1.0 - yes_bid_p  # no ask implied from yes bid
            model_win_prob = 1.0 - model_yes
        if market_prob <= 0.0:
            continue
        if market_prob < NO_TRADE_IMPLIED_PROB_MIN or market_prob > NO_TRADE_IMPLIED_PROB_MAX:
            continue

        edge_pct = (model_win_prob - market_prob) * 100.0
        ev_pct = ((model_win_prob / market_prob) - 1.0) * 100.0
        expected_profit = float(stake) * (ev_pct / 100.0)

        station = str(r.get("station", ""))
        c = str(r.get("city", ""))
        tside = eval_side
        eval_date = effective_market_date_iso(r) or date
        outcome_key = (eval_date, c, tside, station)
        if outcome_key not in outcome_cache:
            outcome_cache[outcome_key] = get_outcome_f(eval_date, c, tside, station) if station else None
        outcome = outcome_cache[outcome_key]

        realized_correct = None
        realized_pnl = None
        if outcome is not None:
            yes_outcome = _bucket_yes_from_outcome(float(outcome), lo, hi)
            bet_wins = yes_outcome if "YES" in side else (not yes_outcome)
            realized_correct = bool(bet_wins)
            if "YES" in side:
                cost = yes_ask_p
            else:
                cost = max(1e-9, 1.0 - yes_bid_p)
            win_pnl = float(stake) * ((1.0 / cost) - 1.0)
            loss_pnl = -float(stake)
            realized_pnl = win_pnl if bet_wins else loss_pnl

        samples.append({
            "ts_est": r.get("ts_est"),
            "market_date_selected": eval_date,
            "city": c,
            "temp_side": tside,
            "ticker": r.get("best_ticker"),
            "line": r.get("best_bucket_label"),
            "bet": side,
            "market_prob_pct": market_prob * 100.0,
            "implied_american_odds": american_odds_from_prob(market_prob),
            "model_win_prob_pct": model_win_prob * 100.0,
            "edge_pct": edge_pct,
            "expected_value_pct": ev_pct,
            "expected_profit_dollars": expected_profit,
            "realized_outcome_f": outcome,
            "realized_correct": realized_correct,
            "realized_pnl_dollars": realized_pnl,
        })

    if not samples:
        return {"ok": True, "date": date, "stake": stake, "rows": len(rows), "samples": [], "bins": {}}

    def aggregate_bins(items: List[dict], key_name: str, edges: List[float], labels: List[str]) -> List[dict]:
        buckets: Dict[str, List[dict]] = {lbl: [] for lbl in labels}
        for x in items:
            v = _to_float(x.get(key_name))
            if v is None:
                continue
            lbl = _bin_label(v, edges, labels)
            buckets[lbl].append(x)
        out = []
        for lbl in labels:
            arr = buckets[lbl]
            if not arr:
                continue
            acc_vals = [1.0 if a.get("realized_correct") is True else 0.0 for a in arr if a.get("realized_correct") is not None]
            pnl_vals = [a.get("realized_pnl_dollars") for a in arr if a.get("realized_pnl_dollars") is not None]
            out.append({
                "bin": lbl,
                "count": len(arr),
                "avg_market_prob_pct": sum(a["market_prob_pct"] for a in arr) / len(arr),
                "avg_model_win_prob_pct": sum(a["model_win_prob_pct"] for a in arr) / len(arr),
                "avg_edge_pct": sum(a["edge_pct"] for a in arr) / len(arr),
                "avg_expected_value_pct": sum(a["expected_value_pct"] for a in arr) / len(arr),
                "avg_expected_profit_dollars": sum(a["expected_profit_dollars"] for a in arr) / len(arr),
                "realized_accuracy_pct": (None if not acc_vals else (100.0 * sum(acc_vals) / len(acc_vals))),
                "realized_avg_pnl_dollars": (None if not pnl_vals else (sum(pnl_vals) / len(pnl_vals))),
            })
        return out

    prob_edges = [0.0, 20.0, 40.0, 60.0, 80.0, 101.0]
    prob_labels = ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"]
    edge_edges = [-1e9, 0.0, 5.0, 10.0, 20.0, 30.0, 40.0, 1e9]
    edge_labels = ["<0%", "0-5%", "5-10%", "10-20%", "20-30%", "30-40%", "40%+"]
    ev_edges = [-1e9, -20.0, -10.0, 0.0, 10.0, 20.0, 50.0, 1e9]
    ev_labels = ["<-20%", "-20% to -10%", "-10% to 0%", "0% to 10%", "10% to 20%", "20% to 50%", "50%+"]

    samples.sort(key=lambda x: x.get("expected_value_pct", -1e9), reverse=True)
    top = samples[:25]
    return {
        "ok": True,
        "date": date,
        "stake": stake,
        "rows": len(rows),
        "sample_count": len(samples),
        "summary": {
            "avg_market_prob_pct": sum(x["market_prob_pct"] for x in samples) / len(samples),
            "avg_model_win_prob_pct": sum(x["model_win_prob_pct"] for x in samples) / len(samples),
            "avg_edge_pct": sum(x["edge_pct"] for x in samples) / len(samples),
            "avg_expected_value_pct": sum(x["expected_value_pct"] for x in samples) / len(samples),
            "avg_expected_profit_dollars": sum(x["expected_profit_dollars"] for x in samples) / len(samples),
            "realized_accuracy_pct": (
                None if not [x for x in samples if x.get("realized_correct") is not None]
                else 100.0 * sum(1.0 if x.get("realized_correct") else 0.0 for x in samples if x.get("realized_correct") is not None) /
                len([x for x in samples if x.get("realized_correct") is not None])
            ),
            "realized_avg_pnl_dollars": (
                None if not [x for x in samples if x.get("realized_pnl_dollars") is not None]
                else sum(x["realized_pnl_dollars"] for x in samples if x.get("realized_pnl_dollars") is not None) /
                len([x for x in samples if x.get("realized_pnl_dollars") is not None])
            ),
        },
        "bins": {
            "implicit_odds_probability": aggregate_bins(samples, "market_prob_pct", prob_edges, prob_labels),
            "edge_pct": aggregate_bins(samples, "edge_pct", edge_edges, edge_labels),
            "expected_value_pct": aggregate_bins(samples, "expected_value_pct", ev_edges, ev_labels),
        },
        "top_expected_value_samples": top,
    }

@app.get("/analytics/policy-sim")
def analytics_policy_sim(
    date: str,
    unit_dollars: float = UNIT_SIZE_DOLLARS,
    min_edge_pct: float = POLICY_MIN_NET_EDGE_PCT,
    fill_mode: str = "touch",
    latency_seconds: int = 0,
    city: Optional[str] = None,
    temp_side: Optional[str] = None,
):
    path = snapshot_log_path()
    if not os.path.exists(path):
        return {"ok": False, "error": "no snapshot log file yet", "path": path}

    raw_rows = load_snapshot_rows_filtered(date=date, city=city, temp_side=temp_side)

    rows = dedupe_snapshot_rows(raw_rows)
    if not rows:
        return {"ok": True, "date": date, "rows": 0, "sample_count": 0, "trades": []}

    mode = str(fill_mode or "touch").strip().lower()
    if mode not in ("touch", "one_cent_worse", "half_spread_worse"):
        return {"ok": False, "error": "fill_mode must be one of: touch, one_cent_worse, half_spread_worse"}
    latency_s = max(0, int(latency_seconds))

    snapshots_by_key: Dict[Tuple[str, str, str, str], List[Tuple[datetime, dict]]] = {}
    for rr in raw_rows:
        d = effective_market_date_iso(rr) or ""
        c = str(rr.get("city", ""))
        s = normalize_temp_side(str(rr.get("temp_side", "high")))
        t = str(rr.get("best_ticker", ""))
        ts = parse_ts_est(str(rr.get("ts_est", "")))
        if not d or not c or not t or ts is None:
            continue
        key = (d, c, s, t)
        snapshots_by_key.setdefault(key, []).append((ts, rr))
    for k in list(snapshots_by_key.keys()):
        snapshots_by_key[k].sort(key=lambda x: x[0])

    tables = build_calibration_tables()
    outcome_cache: Dict[Tuple[str, str, str, str], Optional[float]] = {}
    trades: List[dict] = []
    excluded_counts = {
        "low_disabled": 0,
        "missing_fields": 0,
        "tail_market_prob": 0,
        "below_min_edge": 0,
        "zero_units": 0,
    }
    execution_fallback_count = 0

    for r in rows:
        eval_side = normalize_temp_side(str(r.get("temp_side", "high")))
        if eval_side == "low" and not LOW_SIGNALS_ENABLED:
            excluded_counts["low_disabled"] += 1
            continue
        best_side = str(r.get("best_side", ""))
        lo = _to_float(r.get("best_lo"))
        hi = _to_float(r.get("best_hi"))
        model_yes = _to_float(r.get("model_yes_prob"))
        raw_edge = _to_float(r.get("best_edge"))
        if lo is None or hi is None or model_yes is None or raw_edge is None:
            excluded_counts["missing_fields"] += 1
            continue
        model_yes = clamp(model_yes, MODEL_WIN_PROB_FLOOR, MODEL_WIN_PROB_CEIL)

        c = str(r.get("city", ""))
        eval_date = effective_market_date_iso(r) or date
        tkr = str(r.get("best_ticker", ""))
        signal_ts = parse_ts_est(str(r.get("ts_est", "")))
        exec_row = r
        exec_ts = signal_ts
        if signal_ts is not None and latency_s > 0:
            target_ts = signal_ts + timedelta(seconds=latency_s)
            series = snapshots_by_key.get((eval_date, c, eval_side, tkr), [])
            picked = None
            for ts_i, rr_i in series:
                if ts_i >= target_ts:
                    picked = (ts_i, rr_i)
                    break
            if picked is not None:
                exec_ts, exec_row = picked
            else:
                execution_fallback_count += 1

        yes_bid = _to_float(exec_row.get("yes_bid"))
        yes_ask = _to_float(exec_row.get("yes_ask"))
        if yes_bid is None or yes_ask is None:
            excluded_counts["missing_fields"] += 1
            continue
        base_cost = implied_market_win_prob(best_side, yes_bid, yes_ask)
        if base_cost is None:
            excluded_counts["missing_fields"] += 1
            continue
        spread_cents = max(0.0, float(yes_ask) - float(yes_bid))
        penalty = 0.0
        if mode == "one_cent_worse":
            penalty = 0.01
        elif mode == "half_spread_worse":
            penalty = spread_cents / 200.0
        exec_cost = clamp(base_cost + penalty, 0.001, 0.999)
        market_prob = exec_cost
        if market_prob < NO_TRADE_IMPLIED_PROB_MIN or market_prob > NO_TRADE_IMPLIED_PROB_MAX:
            excluded_counts["tail_market_prob"] += 1
            continue

        lead_h = infer_lead_hours(r)
        cal_edge, meta = calibrate_edge(
            float(raw_edge),
            str(r.get("city", "")),
            eval_side,
            lead_h,
            tables,
        )
        net_edge_pct = (cal_edge - (EV_SLIPPAGE_PCT / 100.0)) * 100.0
        if net_edge_pct < float(min_edge_pct):
            excluded_counts["below_min_edge"] += 1
            continue

        units = suggested_units_from_net_edge(net_edge_pct)
        stake = max(0.0, float(unit_dollars)) * units
        if stake <= 0.0:
            excluded_counts["zero_units"] += 1
            continue

        if "YES" in best_side:
            model_win_prob = model_yes
        else:
            model_win_prob = 1.0 - model_yes
        cost = exec_cost
        ev_pct = ((model_win_prob / max(1e-9, market_prob)) - 1.0) * 100.0
        expected_profit = stake * (ev_pct / 100.0)

        station = str(r.get("station", ""))
        outcome_key = (eval_date, c, eval_side, station)
        if outcome_key not in outcome_cache:
            outcome_cache[outcome_key] = get_outcome_f(eval_date, c, eval_side, station) if station else None
        outcome = outcome_cache[outcome_key]

        realized_correct = None
        realized_pnl = None
        if outcome is not None:
            yes_outcome = _bucket_yes_from_outcome(float(outcome), lo, hi)
            bet_wins = yes_outcome if "YES" in best_side else (not yes_outcome)
            realized_correct = bool(bet_wins)
            win_pnl = stake * ((1.0 / max(1e-9, cost)) - 1.0)
            loss_pnl = -stake
            realized_pnl = win_pnl if bet_wins else loss_pnl

        trades.append({
            "signal_ts_est": r.get("ts_est"),
            "execution_ts_est": (fmt_est(exec_ts) if exec_ts is not None else r.get("ts_est")),
            "market_date_selected": eval_date,
            "city": c,
            "temp_side": eval_side,
            "ticker": tkr,
            "line": r.get("best_bucket_label"),
            "bet": best_side,
            "fill_mode": mode,
            "latency_seconds": latency_s,
            "entry_yes_bid": yes_bid,
            "entry_yes_ask": yes_ask,
            "entry_spread_cents": spread_cents,
            "fill_penalty_pct_points": penalty * 100.0,
            "units": units,
            "stake_dollars": stake,
            "market_prob_pct": market_prob * 100.0,
            "implied_american_odds": american_odds_from_prob(market_prob),
            "model_win_prob_pct": model_win_prob * 100.0,
            "raw_edge_pct": float(raw_edge) * 100.0,
            "calibrated_edge_pct": cal_edge * 100.0,
            "net_edge_pct": net_edge_pct,
            "expected_value_pct": ev_pct,
            "expected_profit_dollars": expected_profit,
            "realized_outcome_f": outcome,
            "realized_correct": realized_correct,
            "realized_pnl_dollars": realized_pnl,
            "calibration_meta": meta,
        })

    trades.sort(key=lambda x: x.get("net_edge_pct", -1e9), reverse=True)
    if not trades:
        return {
            "ok": True,
            "date": date,
            "unit_dollars": float(unit_dollars),
            "min_edge_pct": float(min_edge_pct),
            "fill_mode": mode,
            "latency_seconds": latency_s,
            "rows": len(rows),
            "sample_count": 0,
            "excluded_counts": excluded_counts,
            "execution_fallback_count": execution_fallback_count,
            "trades": [],
        }

    realized = [t for t in trades if t.get("realized_pnl_dollars") is not None]
    wins = [t for t in realized if t.get("realized_correct") is True]
    total_stake = sum(float(t.get("stake_dollars", 0.0)) for t in trades)
    total_expected = sum(float(t.get("expected_profit_dollars", 0.0)) for t in trades)
    total_realized = sum(float(t.get("realized_pnl_dollars", 0.0)) for t in realized) if realized else None

    return {
        "ok": True,
        "date": date,
        "unit_dollars": float(unit_dollars),
        "min_edge_pct": float(min_edge_pct),
        "fill_mode": mode,
        "latency_seconds": latency_s,
        "rows": len(rows),
        "sample_count": len(trades),
        "excluded_counts": excluded_counts,
        "execution_fallback_count": execution_fallback_count,
        "summary": {
            "total_stake_dollars": total_stake,
            "avg_stake_dollars": total_stake / len(trades),
            "avg_units": sum(float(t.get("units", 0.0)) for t in trades) / len(trades),
            "avg_market_prob_pct": sum(float(t.get("market_prob_pct", 0.0)) for t in trades) / len(trades),
            "avg_model_win_prob_pct": sum(float(t.get("model_win_prob_pct", 0.0)) for t in trades) / len(trades),
            "avg_net_edge_pct": sum(float(t.get("net_edge_pct", 0.0)) for t in trades) / len(trades),
            "total_expected_profit_dollars": total_expected,
            "realized_count": len(realized),
            "realized_win_count": len(wins),
            "realized_loss_count": max(0, len(realized) - len(wins)),
            "realized_win_rate_pct": (100.0 * len(wins) / len(realized)) if realized else None,
            "total_realized_pnl_dollars": total_realized,
            "realized_roi_pct_on_stake": ((100.0 * total_realized / total_stake) if (realized and total_stake > 0) else None),
        },
        "top_trades": trades[:50],
    }

@app.get("/analytics/policy-scorecard")
def analytics_policy_scorecard(
    start: str,
    end: str,
    unit_dollars: float = UNIT_SIZE_DOLLARS,
    min_edge_pct: float = POLICY_MIN_NET_EDGE_PCT,
    fill_mode: str = "half_spread_worse",
    latency_seconds: int = 60,
    city: Optional[str] = None,
    temp_side: Optional[str] = None,
):
    try:
        d0 = datetime.fromisoformat(start).date()
        d1 = datetime.fromisoformat(end).date()
    except Exception:
        return {"ok": False, "error": "start/end must be YYYY-MM-DD"}
    if d1 < d0:
        return {"ok": False, "error": "end must be on or after start"}

    per_day: List[dict] = []
    total_trades = 0
    total_stake = 0.0
    total_expected = 0.0
    total_realized = 0.0
    total_realized_count = 0
    total_win_count = 0
    total_fallbacks = 0

    cur = d0
    while cur <= d1:
        day = cur.isoformat()
        r = analytics_policy_sim(
            date=day,
            unit_dollars=unit_dollars,
            min_edge_pct=min_edge_pct,
            fill_mode=fill_mode,
            latency_seconds=latency_seconds,
            city=city,
            temp_side=temp_side,
        )
        if not r.get("ok"):
            per_day.append({"date": day, "ok": False, "error": r.get("error", "unknown error")})
            cur += timedelta(days=1)
            continue

        s = r.get("summary") or {}
        day_trades = int(r.get("sample_count", 0) or 0)
        day_stake = float(s.get("total_stake_dollars", 0.0) or 0.0)
        day_expected = float(s.get("total_expected_profit_dollars", 0.0) or 0.0)
        day_realized = float(s.get("total_realized_pnl_dollars", 0.0) or 0.0)
        day_realized_count = int(s.get("realized_count", 0) or 0)
        day_win_count = int(s.get("realized_win_count", 0) or 0)
        day_fallbacks = int(r.get("execution_fallback_count", 0) or 0)

        total_trades += day_trades
        total_stake += day_stake
        total_expected += day_expected
        total_realized += day_realized
        total_realized_count += day_realized_count
        total_win_count += day_win_count
        total_fallbacks += day_fallbacks

        per_day.append({
            "date": day,
            "ok": True,
            "sample_count": day_trades,
            "total_stake_dollars": day_stake,
            "total_expected_profit_dollars": day_expected,
            "total_realized_pnl_dollars": day_realized,
            "realized_count": day_realized_count,
            "realized_win_count": day_win_count,
            "realized_win_rate_pct": (100.0 * day_win_count / day_realized_count) if day_realized_count > 0 else None,
            "realized_roi_pct_on_stake": ((100.0 * day_realized / day_stake) if day_stake > 0 else None),
            "execution_fallback_count": day_fallbacks,
        })
        cur += timedelta(days=1)

    return {
        "ok": True,
        "start": d0.isoformat(),
        "end": d1.isoformat(),
        "unit_dollars": float(unit_dollars),
        "min_edge_pct": float(min_edge_pct),
        "fill_mode": str(fill_mode),
        "latency_seconds": int(latency_seconds),
        "city": city,
        "temp_side": normalize_temp_side(temp_side) if temp_side else None,
        "summary": {
            "days": len(per_day),
            "sample_count": total_trades,
            "total_stake_dollars": total_stake,
            "total_expected_profit_dollars": total_expected,
            "total_realized_pnl_dollars": total_realized,
            "realized_count": total_realized_count,
            "realized_win_count": total_win_count,
            "realized_loss_count": max(0, total_realized_count - total_win_count),
            "realized_win_rate_pct": (100.0 * total_win_count / total_realized_count) if total_realized_count > 0 else None,
            "realized_roi_pct_on_stake": ((100.0 * total_realized / total_stake) if total_stake > 0 else None),
            "execution_fallback_count": total_fallbacks,
        },
        "per_day": per_day,
    }

@app.get("/analytics/live-scorecard")
def analytics_live_scorecard(
    start: str,
    end: str,
    city: Optional[str] = None,
    temp_side: Optional[str] = None,
    finalized_only: bool = True,
    trade_limit: int = 200,
):
    try:
        d0 = datetime.fromisoformat(start).date()
        d1 = datetime.fromisoformat(end).date()
    except Exception:
        return {"ok": False, "error": "start/end must be YYYY-MM-DD"}
    if d1 < d0:
        return {"ok": False, "error": "end must be on or after start"}

    path = live_trade_log_path()
    if not list_live_trade_log_paths():
        return {"ok": True, "start": d0.isoformat(), "end": d1.isoformat(), "summary": {"days": 0, "fills": 0}, "per_day": []}

    rows = load_live_trade_log_rows()

    def _in_range(day_s: str) -> bool:
        try:
            d = datetime.fromisoformat(str(day_s)).date()
        except Exception:
            return False
        return d0 <= d <= d1

    filt: List[dict] = []
    for r in rows:
        d = str(r.get("date", ""))
        if not _in_range(d):
            continue
        c = str(r.get("city", "")).strip()
        s = normalize_temp_side(str(r.get("temp_type", "high")))
        if city and c.lower() != str(city).strip().lower():
            continue
        if temp_side and s != normalize_temp_side(temp_side):
            continue
        st = str(r.get("status", "")).strip().lower()
        # Only include rows that represent actual fills (full or partial).
        if st not in ("submitted", "partial", "partial_filled"):
            continue
        cnt = int(float(_to_float(r.get("count")) or 0))
        px = _to_float(r.get("limit_price_cents"))
        if cnt <= 0 or px is None:
            continue
        filt.append(r)

    # Aggregate split fills/retries into one logical position row.
    grouped: Dict[Tuple[str, str, str, str, str, str, str], dict] = {}
    for r in filt:
        d = str(r.get("date", ""))
        c = str(r.get("city", "")).strip()
        side = normalize_temp_side(str(r.get("temp_type", "high")))
        station = CITY_CONFIG.get(c, {}).get("station", "")
        bet = str(r.get("bet", "")).strip().upper()
        line = str(r.get("line", ""))
        ticker = str(r.get("ticker", "")).strip()
        if not ticker:
            continue
        key = (d, c, side, station, ticker, bet, line)
        cnt = int(float(_to_float(r.get("count")) or 0))
        px = float(_to_float(r.get("limit_price_cents")) or 0.0)
        stake = (px / 100.0) * cnt
        fee = float(_to_float(r.get("fee_dollars")) or 0.0)
        edge = float(_to_float(r.get("edge_pct")) or 0.0)
        g = grouped.get(key)
        if g is None:
            grouped[key] = {
                "ts_est": r.get("ts_est"),
                "date": d,
                "city": c,
                "temp_side": side,
                "station": station,
                "ticker": ticker,
                "bet": bet,
                "line": line,
                "count": cnt,
                "stake_dollars": stake,
                "fee_dollars": fee,
                "edge_weighted_sum": edge * max(stake, 0.0),
                "stake_for_edge": max(stake, 0.0),
            }
        else:
            g["count"] = int(g["count"]) + cnt
            g["stake_dollars"] = float(g["stake_dollars"]) + stake
            g["fee_dollars"] = float(g["fee_dollars"]) + fee
            g["edge_weighted_sum"] = float(g["edge_weighted_sum"]) + (edge * max(stake, 0.0))
            g["stake_for_edge"] = float(g["stake_for_edge"]) + max(stake, 0.0)
            # keep earliest timestamp for readability
            ts_prev = parse_ts_est(str(g.get("ts_est", "")))
            ts_new = parse_ts_est(str(r.get("ts_est", "")))
            if ts_prev is None or (ts_new is not None and ts_new < ts_prev):
                g["ts_est"] = r.get("ts_est")

    # Optional settlement-truth map (exact stake/fees/revenue as settled by Kalshi).
    settlement_by_ticker: Dict[str, dict] = {}
    if finalized_only and kalshi_has_auth_config():
        try:
            for s in _fetch_kalshi_settlements(max_pages=20, per_page_limit=200):
                ticker = str(s.get("ticker", "")).strip()
                if not ticker:
                    continue
                prev = settlement_by_ticker.get(ticker)
                if prev is None:
                    settlement_by_ticker[ticker] = s
                    continue
                try:
                    t_prev = datetime.fromisoformat(str(prev.get("settled_time", "")).replace("Z", "+00:00"))
                    t_cur = datetime.fromisoformat(str(s.get("settled_time", "")).replace("Z", "+00:00"))
                    if t_cur >= t_prev:
                        settlement_by_ticker[ticker] = s
                except Exception:
                    settlement_by_ticker[ticker] = s
        except Exception:
            settlement_by_ticker = {}

    outcome_cache: Dict[Tuple[str, str, str, str], Optional[float]] = {}
    trades: List[dict] = []
    per_day_acc: Dict[str, dict] = {}

    for _k, r in grouped.items():
        d = str(r.get("date", ""))
        c = str(r.get("city", "")).strip()
        side = str(r.get("temp_side", "high"))
        station = str(r.get("station", ""))
        bet = str(r.get("bet", "")).strip().upper()
        line = str(r.get("line", ""))
        ticker = str(r.get("ticker", "")).strip()
        bucket = parse_bucket_from_line(line)
        if bucket is None:
            continue
        lo, hi = bucket
        cnt = int(float(_to_float(r.get("count")) or 0))
        stake = float(_to_float(r.get("stake_dollars")) or 0.0)
        fee_dollars = float(_to_float(r.get("fee_dollars")) or 0.0)
        price_c = (100.0 * stake / cnt) if cnt > 0 else 0.0
        if stake <= 0:
            continue

        mkt_p = clamp(price_c / 100.0, 0.001, 0.999)
        stake_for_edge = float(_to_float(r.get("stake_for_edge")) or 0.0)
        edge_weighted_sum = float(_to_float(r.get("edge_weighted_sum")) or 0.0)
        edge_pct = (edge_weighted_sum / stake_for_edge) if stake_for_edge > 0 else 0.0
        model_p = clamp(mkt_p + (edge_pct / 100.0), MODEL_WIN_PROB_FLOOR, MODEL_WIN_PROB_CEIL)
        ev_pct = ((model_p / mkt_p) - 1.0) * 100.0
        exp_profit = stake * (ev_pct / 100.0)
        exp_profit_net = exp_profit - fee_dollars

        key = (d, c, side, station)
        if key not in outcome_cache:
            if finalized_only:
                outcome_cache[key] = get_final_outcome_f(d, c, side)
            else:
                outcome_cache[key] = get_outcome_f(d, c, side, station) if station else None
        outcome = outcome_cache[key]

        realized_correct = None
        realized_pnl = None
        if finalized_only and ticker in settlement_by_ticker:
            s = settlement_by_ticker[ticker]
            market_result = str(s.get("market_result", "")).strip().lower()
            yes_cost_c = float(_to_float(s.get("yes_total_cost")) or 0.0)
            no_cost_c = float(_to_float(s.get("no_total_cost")) or 0.0)
            revenue_c = float(_to_float(s.get("revenue")) or 0.0)
            settle_fee_d = float(_to_float(s.get("fee_cost")) or 0.0)
            stake_c = yes_cost_c if "YES" in bet else no_cost_c
            if stake_c > 0:
                stake = stake_c / 100.0
                fee_dollars = settle_fee_d
                price_c = (100.0 * stake / cnt) if cnt > 0 else price_c
                mkt_p = clamp(price_c / 100.0, 0.001, 0.999)
                exp_profit = stake * (ev_pct / 100.0)
                exp_profit_net = exp_profit - fee_dollars
                realized_pnl = ((revenue_c - stake_c) / 100.0) - settle_fee_d
                if market_result in ("yes", "no"):
                    realized_correct = ("YES" in bet and market_result == "yes") or ("NO" in bet and market_result == "no")
        elif outcome is not None:
            yes_outcome = _bucket_yes_from_outcome(float(outcome), float(lo), float(hi))
            win = yes_outcome if "YES" in bet else (not yes_outcome)
            realized_correct = bool(win)
            win_pnl = stake * ((1.0 / mkt_p) - 1.0)
            realized_pnl = win_pnl if win else -stake
            realized_pnl = float(realized_pnl) - fee_dollars

        t = {
            "ts_est": r.get("ts_est"),
            "date": d,
            "city": c,
            "temp_side": side,
            "ticker": ticker,
            "bet": bet,
            "line": line,
            "count": cnt,
            "price_cents": price_c,
            "stake_dollars": stake,
            "fee_dollars": fee_dollars,
            "market_win_prob_pct": mkt_p * 100.0,
            "model_win_prob_pct": model_p * 100.0,
            "edge_pct": edge_pct,
            "expected_value_pct": ev_pct,
            "expected_profit_dollars": exp_profit,
            "expected_profit_net_dollars": exp_profit_net,
            "realized_outcome_f": outcome,
            "realized_correct": realized_correct,
            "realized_pnl_dollars": realized_pnl,
        }
        trades.append(t)

        a = per_day_acc.setdefault(d, {"fills": 0, "stake": 0.0, "fees": 0.0, "exp": 0.0, "exp_net": 0.0, "realized": 0.0, "realized_n": 0, "wins": 0})
        a["fills"] += 1
        a["stake"] += stake
        a["exp"] += exp_profit
        a["fees"] += fee_dollars
        a["exp_net"] += exp_profit_net
        if realized_pnl is not None:
            a["realized"] += float(realized_pnl)
            a["realized_n"] += 1
            if realized_correct:
                a["wins"] += 1

    per_day: List[dict] = []
    cur = d0
    while cur <= d1:
        day = cur.isoformat()
        a = per_day_acc.get(day, {"fills": 0, "stake": 0.0, "fees": 0.0, "exp": 0.0, "exp_net": 0.0, "realized": 0.0, "realized_n": 0, "wins": 0})
        per_day.append({
            "date": day,
            "ok": True,
            "fills": int(a["fills"]),
            "total_stake_dollars": float(a["stake"]),
            "total_fees_dollars": float(a["fees"]),
            "fees_pct_on_stake": ((100.0 * float(a["fees"]) / float(a["stake"])) if float(a["stake"]) > 0 else None),
            "total_expected_profit_dollars": float(a["exp"]),
            "total_expected_profit_net_dollars": float(a["exp_net"]),
            "total_realized_pnl_dollars": float(a["realized"]),
            "realized_count": int(a["realized_n"]),
            "realized_win_count": int(a["wins"]),
            "realized_loss_count": max(0, int(a["realized_n"]) - int(a["wins"])),
            "realized_win_rate_pct": (100.0 * float(a["wins"]) / float(a["realized_n"])) if a["realized_n"] > 0 else None,
            "realized_roi_pct_on_stake": ((100.0 * float(a["realized"]) / float(a["stake"])) if float(a["stake"]) > 0 else None),
        })
        cur += timedelta(days=1)

    total_fills = sum(int(x["fills"]) for x in per_day)
    total_stake = sum(float(x["total_stake_dollars"]) for x in per_day)
    total_fees = sum(float(x["total_fees_dollars"]) for x in per_day)
    total_exp = sum(float(x["total_expected_profit_dollars"]) for x in per_day)
    total_exp_net = sum(float(x["total_expected_profit_net_dollars"]) for x in per_day)
    total_realized = sum(float(x["total_realized_pnl_dollars"]) for x in per_day)
    total_realized_n = sum(int(x["realized_count"]) for x in per_day)
    total_wins = sum(int(x["realized_win_count"]) for x in per_day)

    return {
        "ok": True,
        "start": d0.isoformat(),
        "end": d1.isoformat(),
        "city": city,
        "temp_side": normalize_temp_side(temp_side) if temp_side else None,
        "finalized_only": bool(finalized_only),
        "summary": {
            "days": len(per_day),
            "fills": total_fills,
            "total_stake_dollars": total_stake,
            "total_fees_dollars": total_fees,
            "fees_pct_on_stake": ((100.0 * total_fees / total_stake) if total_stake > 0 else None),
            "total_expected_profit_dollars": total_exp,
            "total_expected_profit_net_dollars": total_exp_net,
            "total_realized_pnl_dollars": total_realized,
            "realized_count": total_realized_n,
            "realized_win_count": total_wins,
            "realized_loss_count": max(0, total_realized_n - total_wins),
            "realized_win_rate_pct": (100.0 * total_wins / total_realized_n) if total_realized_n > 0 else None,
            "realized_roi_pct_on_stake": ((100.0 * total_realized / total_stake) if total_stake > 0 else None),
        },
        "per_day": per_day,
        "trades": (
            trades
            if int(trade_limit) <= 0
            else trades[-max(1, min(5000, int(trade_limit))):]
        ),
    }


@app.get("/analytics/live-insights")
def analytics_live_insights(
    start: str,
    end: str,
    city: Optional[str] = None,
    temp_side: Optional[str] = None,
    finalized_only: bool = True,
):
    base = analytics_live_scorecard(
        start=start,
        end=end,
        city=city,
        temp_side=temp_side,
        finalized_only=finalized_only,
        trade_limit=0,
    )
    if not base.get("ok"):
        return base

    trades = [dict(t) for t in (base.get("trades", []) or [])]
    summary = dict(base.get("summary", {}) or {})

    city_side_acc: Dict[Tuple[str, str], dict] = {}
    ladder_acc: Dict[str, dict] = {}
    ladder_order = ["<5%", "5-10%", "10-20%", "20-30%", "30%+"]

    def _ladder_bucket(edge_pct: float) -> str:
        e = float(edge_pct)
        if e < 5.0:
            return "<5%"
        if e < 10.0:
            return "5-10%"
        if e < 20.0:
            return "10-20%"
        if e < 30.0:
            return "20-30%"
        return "30%+"

    for t in trades:
        city_name = str(t.get("city", "")).strip()
        side = normalize_temp_side(str(t.get("temp_side", "high")))
        edge_pct = float(_to_float(t.get("edge_pct")) or 0.0)
        stake = float(_to_float(t.get("stake_dollars")) or 0.0)
        exp_net = float(_to_float(t.get("expected_profit_net_dollars")) or 0.0)
        realized = _to_float(t.get("realized_pnl_dollars"))
        model_win = float(_to_float(t.get("model_win_prob_pct")) or 0.0)
        realized_correct = t.get("realized_correct")

        k_cs = (city_name, side)
        a = city_side_acc.setdefault(k_cs, {
            "city": city_name,
            "temp_side": side,
            "fills": 0,
            "stake": 0.0,
            "expected_net": 0.0,
            "realized": 0.0,
            "realized_n": 0,
            "wins": 0,
            "edge_sum": 0.0,
            "model_win_sum": 0.0,
        })
        a["fills"] += 1
        a["stake"] += stake
        a["expected_net"] += exp_net
        a["edge_sum"] += edge_pct
        a["model_win_sum"] += model_win
        if realized is not None:
            a["realized"] += float(realized)
            a["realized_n"] += 1
            if bool(realized_correct):
                a["wins"] += 1

        lb = _ladder_bucket(edge_pct)
        b = ladder_acc.setdefault(lb, {
            "bucket": lb,
            "n": 0,
            "stake": 0.0,
            "edge_sum": 0.0,
            "model_win_sum": 0.0,
            "expected_net": 0.0,
            "realized": 0.0,
            "realized_n": 0,
            "wins": 0,
        })
        b["n"] += 1
        b["stake"] += stake
        b["edge_sum"] += edge_pct
        b["model_win_sum"] += model_win
        b["expected_net"] += exp_net
        if realized is not None:
            b["realized"] += float(realized)
            b["realized_n"] += 1
            if bool(realized_correct):
                b["wins"] += 1

    city_side = []
    for _, a in city_side_acc.items():
        n = max(1, int(a["fills"]))
        realized_n = int(a["realized_n"])
        wins = int(a["wins"])
        city_side.append({
            "city": a["city"],
            "temp_side": a["temp_side"],
            "fills": int(a["fills"]),
            "settled_count": realized_n,
            "avg_edge_pct": float(a["edge_sum"]) / n,
            "expected_win_rate_pct": float(a["model_win_sum"]) / n,
            "actual_win_rate_pct": ((100.0 * wins / realized_n) if realized_n > 0 else None),
            "expected_net_dollars": float(a["expected_net"]),
            "realized_dollars": float(a["realized"]),
            "realized_roi_pct_on_stake": ((100.0 * float(a["realized"]) / float(a["stake"])) if float(a["stake"]) > 0 else None),
        })
    city_side.sort(key=lambda x: (x.get("realized_dollars", 0.0), x.get("expected_net_dollars", 0.0)), reverse=True)

    edge_ladder = []
    for lb in ladder_order:
        b = ladder_acc.get(lb)
        if not b:
            edge_ladder.append({
                "bucket": lb, "n": 0, "avg_edge_pct": None, "expected_win_rate_pct": None,
                "actual_win_rate_pct": None, "expected_net_dollars": 0.0, "realized_dollars": 0.0,
                "realized_roi_pct_on_stake": None,
            })
            continue
        n = max(1, int(b["n"]))
        realized_n = int(b["realized_n"])
        wins = int(b["wins"])
        edge_ladder.append({
            "bucket": lb,
            "n": int(b["n"]),
            "avg_edge_pct": float(b["edge_sum"]) / n,
            "expected_win_rate_pct": float(b["model_win_sum"]) / n,
            "actual_win_rate_pct": ((100.0 * wins / realized_n) if realized_n > 0 else None),
            "expected_net_dollars": float(b["expected_net"]),
            "realized_dollars": float(b["realized"]),
            "realized_roi_pct_on_stake": ((100.0 * float(b["realized"]) / float(b["stake"])) if float(b["stake"]) > 0 else None),
        })

    d0 = datetime.fromisoformat(start).date()
    d1 = datetime.fromisoformat(end).date()
    rows = load_live_trade_log_rows()
    attempts = []
    for r in rows:
        d = str(r.get("date", "")).strip()
        try:
            dd = datetime.fromisoformat(d).date()
        except Exception:
            continue
        if not (d0 <= dd <= d1):
            continue
        if city and str(r.get("city", "")).strip().lower() != str(city).strip().lower():
            continue
        if temp_side and normalize_temp_side(str(r.get("temp_type", "high"))) != normalize_temp_side(temp_side):
            continue
        attempts.append(r)

    attempted_orders = len(attempts)
    rejected_orders = 0
    not_filled_orders = 0
    filled_rows = 0
    contracts_filled = 0
    recent_errors = []
    attempts_by_day: Dict[str, dict] = {}
    for r in attempts:
        day_key = str(r.get("date", "")).strip()
        day_acc = attempts_by_day.setdefault(day_key, {
            "orders_attempted": 0,
            "orders_rejected": 0,
            "orders_not_filled": 0,
            "fill_rows": 0,
            "contracts_filled": 0,
        })
        day_acc["orders_attempted"] += 1
        st = str(r.get("status", "")).strip().lower()
        cnt = int(float(_to_float(r.get("count")) or 0))
        if st == "rejected":
            rejected_orders += 1
            day_acc["orders_rejected"] += 1
        elif st in ("not_filled", "edge_gone", "cancel_failed"):
            not_filled_orders += 1
            day_acc["orders_not_filled"] += 1
        elif st in ("submitted", "partial", "partial_filled"):
            filled_rows += 1
            contracts_filled += max(0, cnt)
            day_acc["fill_rows"] += 1
            day_acc["contracts_filled"] += max(0, cnt)
        err = str(r.get("error", "") or "").strip()
        if err:
            recent_errors.append({
                "ts_est": r.get("ts_est"),
                "city": r.get("city"),
                "ticker": r.get("ticker"),
                "status": r.get("status"),
                "error": err[:240],
            })
    recent_errors = recent_errors[-5:][::-1]

    settled_count = int(summary.get("realized_count", 0) or 0)
    fills = int(summary.get("fills", 0) or 0)
    expected_net = float(summary.get("total_expected_profit_net_dollars", 0.0) or 0.0)
    realized_total = float(summary.get("total_realized_pnl_dollars", 0.0) or 0.0)
    ev_gap = realized_total - expected_net
    avg_edge = (sum(float(_to_float(t.get("edge_pct")) or 0.0) for t in trades) / len(trades)) if trades else None
    per_day_base = [dict(x) for x in (base.get("per_day", []) or []) if x.get("ok")]
    per_day_out = []
    for d in per_day_base:
        day_key = str(d.get("date", "")).strip()
        aa = attempts_by_day.get(day_key, {})
        exp_net_day = float(_to_float(d.get("total_expected_profit_net_dollars")) or 0.0)
        realized_day = float(_to_float(d.get("total_realized_pnl_dollars")) or 0.0)
        attempted_day = int(aa.get("orders_attempted", 0))
        rejected_day = int(aa.get("orders_rejected", 0))
        per_day_out.append({
            "date": day_key,
            "fills": int(_to_float(d.get("fills")) or 0),
            "settled_count": int(_to_float(d.get("realized_count")) or 0),
            "total_stake_dollars": float(_to_float(d.get("total_stake_dollars")) or 0.0),
            "total_fees_dollars": float(_to_float(d.get("total_fees_dollars")) or 0.0),
            "expected_net_dollars": exp_net_day,
            "realized_dollars": realized_day,
            "ev_gap_dollars": realized_day - exp_net_day,
            "realized_win_rate_pct": _to_float(d.get("realized_win_rate_pct")),
            "realized_roi_pct_on_stake": _to_float(d.get("realized_roi_pct_on_stake")),
            "orders_attempted": attempted_day,
            "orders_rejected": rejected_day,
            "rejected_rate_pct": ((100.0 * rejected_day / attempted_day) if attempted_day > 0 else None),
            "orders_not_filled": int(aa.get("orders_not_filled", 0)),
            "fill_rows": int(aa.get("fill_rows", 0)),
            "contracts_filled": int(aa.get("contracts_filled", 0)),
        })

    return {
        "ok": True,
        "start": base.get("start"),
        "end": base.get("end"),
        "finalized_only": bool(finalized_only),
        "summary": {
            "fills": fills,
            "settled_count": settled_count,
            "expected_net_dollars": expected_net,
            "realized_dollars": realized_total,
            "ev_gap_dollars": ev_gap,
            "avg_edge_pct": avg_edge,
            "realized_win_rate_pct": summary.get("realized_win_rate_pct"),
            "orders_attempted": attempted_orders,
            "orders_rejected": rejected_orders,
            "rejected_rate_pct": ((100.0 * rejected_orders / attempted_orders) if attempted_orders > 0 else None),
        },
        "funnel": {
            "orders_attempted": attempted_orders,
            "orders_rejected": rejected_orders,
            "orders_not_filled": not_filled_orders,
            "fill_rows": filled_rows,
            "contracts_filled": contracts_filled,
            "positions_filled": fills,
            "settled_positions": settled_count,
        },
        "city_side": city_side,
        "edge_ladder": edge_ladder,
        "recent_errors": recent_errors,
        "per_day": per_day_out,
    }

@app.get("/analytics/manual-positions")
def analytics_manual_positions(
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    weather_src_rows = load_manual_positions_rows()
    auto_weather_src_rows = load_manual_auto_weather_positions_rows()
    btc_src_rows = load_manual_btc_positions_rows()
    rows = list(weather_src_rows)
    if auto_weather_src_rows:
        rows.extend(auto_weather_src_rows)
    if btc_src_rows:
        rows.extend(btc_src_rows)
    d0 = None
    d1 = None
    try:
        if start:
            d0 = datetime.fromisoformat(str(start)).date()
    except Exception:
        d0 = None
    try:
        if end:
            d1 = datetime.fromisoformat(str(end)).date()
    except Exception:
        d1 = None

    out = []
    weather_rows = []
    btc_rows = []
    cost_total = 0.0
    contracts_total = 0
    resolved_count = 0
    realized_total = 0.0
    weather_cost_total = 0.0
    weather_contracts_total = 0
    weather_resolved_count = 0
    weather_realized_total = 0.0
    btc_cost_total = 0.0
    btc_contracts_total = 0
    btc_resolved_count = 0
    btc_realized_total = 0.0

    for r in rows:
        date_iso = str(r.get("date", "")).strip()
        try:
            dd = datetime.fromisoformat(date_iso).date()
        except Exception:
            dd = None
        if d0 and dd and dd < d0:
            continue
        if d1 and dd and dd > d1:
            continue

        market_type = _manual_market_type(r)
        market_name = str(r.get("market_name", "")).strip()
        is_weather = _manual_is_weather_row(r)
        is_btc = _manual_is_btc_row(r)
        city = str(r.get("city", "")).strip()
        side = normalize_temp_side(str(r.get("temp_side", "high")))
        bet = str(r.get("bet", "")).strip().upper()
        ticker = str(r.get("ticker", "")).strip()
        line = str(r.get("line", "")).strip()
        price_cents = float(_to_float(r.get("price_cents")) or 0.0)
        count = int(float(_to_float(r.get("count")) or 0))
        price_dollars = max(0.0, price_cents / 100.0)
        total_cost_dollars = _to_float(r.get("total_cost_dollars"))
        fees_dollars = _to_float(r.get("fees_dollars"))
        total_payout_dollars = _to_float(r.get("total_payout_dollars"))
        total_return_dollars = _to_float(r.get("total_return_dollars"))
        outcome_text = str(r.get("outcome", "")).strip()

        # Hide legacy auto-sync rows that have no economic signal.
        src_l = str(r.get("source", "")).strip().lower()
        if (
            src_l == "auto_kalshi_settlement"
            and (float(total_cost_dollars or 0.0) <= 1e-9)
            and (float(total_payout_dollars or 0.0) <= 1e-9)
            and (abs(float(total_return_dollars or 0.0)) <= 1e-9)
        ):
            continue

        if total_cost_dollars is not None:
            stake_dollars = max(0.0, float(total_cost_dollars))
        else:
            stake_dollars = max(0.0, price_dollars * max(0, count))

        outcome_f = get_final_outcome_f(date_iso, city, side) if (is_weather and date_iso and city) else None
        bucket = parse_bucket_from_line(line) if is_weather else None
        if is_weather:
            settled = bool(
                (outcome_f is not None and bucket is not None and count > 0)
                or (total_return_dollars is not None)
                or (total_payout_dollars is not None and stake_dollars > 0.0)
                or outcome_text
            )
        else:
            settled = bool((total_return_dollars is not None) or (total_payout_dollars is not None) or outcome_text)
        is_win = None
        realized_pnl = None
        if settled and is_weather and (outcome_f is not None) and (bucket is not None):
            yes_outcome = _bucket_yes_from_outcome(float(outcome_f), float(bucket[0]), float(bucket[1]))
            is_buy_yes = "YES" in bet
            is_win = bool(yes_outcome) if is_buy_yes else (not bool(yes_outcome))
            if total_return_dollars is not None:
                realized_pnl = float(total_return_dollars)
            else:
                payout = float(count) if bool(is_win) else 0.0
                fee_amt = max(0.0, float(fees_dollars or 0.0))
                realized_pnl = payout - stake_dollars - fee_amt
        elif settled:
            if total_return_dollars is not None:
                realized_pnl = float(total_return_dollars)
            elif total_payout_dollars is not None:
                realized_pnl = float(total_payout_dollars) - stake_dollars
            out_l = outcome_text.lower()
            if out_l:
                is_win = ("yes" in out_l) or ("win" in out_l) or ("up" in out_l)

        contracts_total += max(0, count)
        cost_total += stake_dollars
        if settled:
            resolved_count += 1
        if realized_pnl is not None:
            realized_total += float(realized_pnl)

        row_out = {
            "manual_trade_id": str(r.get("manual_trade_id", "")).strip(),
            "position_origin": _row_position_origin(r, default_origin=("auto_kalshi_settlement" if str(r.get("source", "")).strip().lower() == "auto_kalshi_settlement" else "user_manual")),
            "opened_ts_est": r.get("opened_ts_est"),
            "date": date_iso,
            "market_type": market_type,
            "market_name": market_name,
            "city": city,
            "temp_side": side,
            "ticker": ticker,
            "bet": bet,
            "line": line,
            "price_cents": price_cents,
            "count": count,
            "stake_dollars": stake_dollars,
            "outcome": outcome_text,
            "fees_dollars": fees_dollars,
            "total_payout_dollars": total_payout_dollars,
            "total_return_dollars": total_return_dollars,
            "source": r.get("source"),
            "note": r.get("note"),
            "settled": bool(settled),
            "outcome_f": outcome_f,
            "realized_pnl_dollars": realized_pnl,
            "is_win": is_win,
        }
        out.append(row_out)
        if is_weather:
            weather_rows.append(row_out)
            weather_contracts_total += max(0, count)
            weather_cost_total += stake_dollars
            if settled:
                weather_resolved_count += 1
            if realized_pnl is not None:
                weather_realized_total += float(realized_pnl)
        if is_btc:
            btc_rows.append(row_out)
            btc_contracts_total += max(0, count)
            btc_cost_total += stake_dollars
            if settled:
                btc_resolved_count += 1
            if realized_pnl is not None:
                btc_realized_total += float(realized_pnl)

    out.sort(key=lambda x: (str(x.get("date", "")), str(x.get("opened_ts_est", ""))), reverse=True)
    weather_rows.sort(key=lambda x: (str(x.get("date", "")), str(x.get("opened_ts_est", ""))), reverse=True)
    btc_rows.sort(key=lambda x: (str(x.get("date", "")), str(x.get("opened_ts_est", ""))), reverse=True)
    origin_counts: Dict[str, int] = {}
    for r in out:
        o = str(r.get("position_origin", "")).strip().lower() or "unknown"
        origin_counts[o] = int(origin_counts.get(o, 0)) + 1
    return {
        "ok": True,
        "path": manual_positions_path(),
        "auto_weather_path": manual_auto_weather_positions_path(),
        "btc_path": manual_btc_positions_path(),
        "count": len(out),
        "summary": {
            "positions": len(out),
            "contracts": contracts_total,
            "stake_dollars": cost_total,
            "resolved_positions": resolved_count,
            "open_positions": max(0, len(out) - resolved_count),
            "realized_pnl_dollars": realized_total,
        },
        "weather_summary": {
            "positions": len(weather_rows),
            "contracts": weather_contracts_total,
            "stake_dollars": weather_cost_total,
            "resolved_positions": weather_resolved_count,
            "open_positions": max(0, len(weather_rows) - weather_resolved_count),
            "realized_pnl_dollars": weather_realized_total,
        },
        "btc_summary": {
            "positions": len(btc_rows),
            "contracts": btc_contracts_total,
            "stake_dollars": btc_cost_total,
            "resolved_positions": btc_resolved_count,
            "open_positions": max(0, len(btc_rows) - btc_resolved_count),
            "realized_pnl_dollars": btc_realized_total,
        },
        "position_origin_counts": origin_counts,
        "rows": out,
        "weather_rows": weather_rows,
        "btc_rows": btc_rows,
    }

@app.post("/manual/sync-kalshi")
def manual_sync_kalshi(
    max_pages: int = 30,
    per_page_limit: int = 200,
    force_update: bool = False,
    dry_run: bool = False,
):
    return sync_manual_positions_from_kalshi(
        max_pages=max_pages,
        per_page_limit=per_page_limit,
        force_update=force_update,
        dry_run=dry_run,
    )

@app.get("/analytics/account-reconciliation")
def analytics_account_reconciliation():
    now_local = datetime.now(tz=LOCAL_TZ)
    today_iso = now_local.date().isoformat()
    first_date, _last_date = live_trade_log_date_bounds()
    start_iso = first_date or today_iso

    bot_realized = 0.0
    bot_settled = 0
    try:
        bot = analytics_live_scorecard(
            start=start_iso,
            end=today_iso,
            finalized_only=True,
            trade_limit=0,
        )
        s = (bot or {}).get("summary", {}) or {}
        bot_realized = float(_to_float(s.get("total_realized_pnl_dollars")) or 0.0)
        bot_settled = int(_to_float(s.get("realized_count")) or 0)
    except Exception:
        bot_realized = 0.0
        bot_settled = 0

    manual_realized = 0.0
    manual_settled = 0
    try:
        m = analytics_manual_positions()
        ms = (m or {}).get("summary", {}) or {}
        manual_realized = float(_to_float(ms.get("realized_pnl_dollars")) or 0.0)
        manual_settled = int(_to_float(ms.get("resolved_positions")) or 0)
    except Exception:
        manual_realized = 0.0
        manual_settled = 0

    comp = _fetch_portfolio_components_dollars()
    cash = _to_float(comp.get("cash_dollars"))
    positions = _to_float(comp.get("positions_dollars"))
    equity = _to_float(comp.get("total_dollars"))
    deposits = float(ACCOUNT_DEPOSITS_DOLLARS)
    account_net = (float(equity) - float(deposits)) if equity is not None else None
    unrealized_residual = (
        float(account_net) - float(bot_realized) - float(manual_realized)
        if account_net is not None
        else None
    )

    return {
        "ok": True,
        "as_of_est": fmt_est(now_local),
        "deposits_dollars": deposits,
        "cash_dollars": cash,
        "positions_dollars": positions,
        "equity_dollars": equity,
        "account_net_pnl_dollars": account_net,
        "bot_realized_pnl_dollars": bot_realized,
        "bot_settled_count": bot_settled,
        "manual_realized_pnl_dollars": manual_realized,
        "manual_settled_count": manual_settled,
        "unrealized_residual_pnl_dollars": unrealized_residual,
        "bot_window_start": start_iso,
        "bot_window_end": today_iso,
    }

def summarize_live_window(
    start_est: datetime,
    end_est: datetime,
    min_edge_pct: float = 0.0,
    allow_provisional_outcomes: bool = False,
) -> dict:
    rows = load_live_trade_log_rows()
    est_tz = tz.tzoffset("EST", -5 * 3600)
    start_est = start_est.astimezone(est_tz)
    end_est = end_est.astimezone(est_tz)

    filt: List[dict] = []
    for r in rows:
        st = str(r.get("status", "")).strip().lower()
        if st not in ("submitted", "partial", "partial_filled"):
            continue
        ts = parse_ts_est(str(r.get("ts_est", "")))
        if ts is None:
            continue
        ts_est = ts.astimezone(est_tz)
        if ts_est < start_est or ts_est > end_est:
            continue
        cnt = int(float(_to_float(r.get("count")) or 0))
        px = _to_float(r.get("limit_price_cents"))
        edge_pct = float(_to_float(r.get("edge_pct")) or 0.0)
        fee_row = float(_to_float(r.get("fee_dollars")) or 0.0)
        if cnt <= 0 or px is None:
            continue
        if edge_pct < float(min_edge_pct):
            continue
        filt.append(r)

    outcome_cache: Dict[Tuple[str, str, str, str], Optional[float]] = {}
    fills = 0
    wins = 0
    losses = 0
    total_stake = 0.0
    total_fees = 0.0
    resolved_stake = 0.0
    unresolved_stake = 0.0
    realized_pnl = 0.0

    for r in filt:
        d = str(r.get("date", ""))
        c = str(r.get("city", "")).strip()
        side = normalize_temp_side(str(r.get("temp_type", "high")))
        station = CITY_CONFIG.get(c, {}).get("station", "")
        bet = str(r.get("bet", "")).strip().upper()
        bucket = parse_bucket_from_line(str(r.get("line", "")))
        if bucket is None:
            continue
        lo, hi = bucket
        price_c = float(_to_float(r.get("limit_price_cents")) or 0.0)
        cnt = int(float(_to_float(r.get("count")) or 0))
        stake = (price_c / 100.0) * cnt
        fee_dollars = float(_to_float(r.get("fee_dollars")) or 0.0)
        if stake <= 0:
            continue
        fills += 1
        total_stake += stake
        total_fees += fee_dollars

        key = (d, c, side, station)
        if key not in outcome_cache:
            outcome_cache[key] = get_final_outcome_f(d, c, side)
            if allow_provisional_outcomes and outcome_cache[key] is None and station:
                outcome_cache[key] = get_outcome_f(d, c, side, station)
        outcome = outcome_cache[key]
        if outcome is None:
            unresolved_stake += stake
            continue

        resolved_stake += stake
        yes_outcome = _bucket_yes_from_outcome(float(outcome), float(lo), float(hi))
        win = yes_outcome if "YES" in bet else (not yes_outcome)
        mkt_p = clamp(price_c / 100.0, 0.001, 0.999)
        win_pnl = stake * ((1.0 / mkt_p) - 1.0)
        pnl = (win_pnl if win else -stake) - fee_dollars
        realized_pnl += float(pnl)
        if win:
            wins += 1
        else:
            losses += 1

    return {
        "window_start_est": fmt_est(start_est),
        "window_end_est": fmt_est(end_est),
        "min_edge_pct": float(min_edge_pct),
        "fills": fills,
        "total_stake_dollars": total_stake,
        "total_fees_dollars": total_fees,
        "fees_pct_on_stake": ((100.0 * total_fees / total_stake) if total_stake > 0 else None),
        "resolved_fills": (wins + losses),
        "wins": wins,
        "losses": losses,
        "resolved_stake_dollars": resolved_stake,
        "unresolved_fills": max(0, fills - (wins + losses)),
        "unresolved_stake_dollars": unresolved_stake,
        "realized_pnl_dollars": realized_pnl,
        "realized_roi_pct_on_total_stake": ((100.0 * realized_pnl / total_stake) if total_stake > 0 else None),
    }

def _fetch_kalshi_settlements(max_pages: int = 20, per_page_limit: int = 200) -> List[dict]:
    rows: List[dict] = []
    cursor = None
    pages = 0
    while pages < max_pages:
        params: Dict[str, object] = {"limit": int(per_page_limit)}
        if cursor:
            params["cursor"] = cursor
        resp = kalshi_get("/portfolio/settlements", params=params, timeout=20, max_retries=2)
        batch = resp.get("settlements", []) or []
        if isinstance(batch, list):
            rows.extend([x for x in batch if isinstance(x, dict)])
        cursor = resp.get("cursor")
        pages += 1
        if not cursor:
            break
    return rows

def summarize_live_window_kalshi(start_est: datetime, end_est: datetime, min_edge_pct: float = 0.0) -> dict:
    est_tz = tz.tzoffset("EST", -5 * 3600)
    start_est = start_est.astimezone(est_tz)
    end_est = end_est.astimezone(est_tz)

    # Daily reporting should reflect bets entered in this window.
    grouped_positions: Dict[Tuple[str, str, str, str, str, str, str], dict] = {}
    for r in load_live_trade_log_rows():
        st = str(r.get("status", "")).strip().lower()
        if st not in ("submitted", "partial", "partial_filled"):
            continue
        if str(r.get("order_action", "buy")).strip().lower() != "buy":
            continue
        ts = parse_ts_est(str(r.get("ts_est", "")))
        if ts is None:
            continue
        ts_est = ts.astimezone(est_tz)
        if ts_est < start_est or ts_est > end_est:
            continue
        cnt = int(float(_to_float(r.get("count")) or 0))
        px = _to_float(r.get("limit_price_cents"))
        edge_pct = float(_to_float(r.get("edge_pct")) or 0.0)
        fee_row = float(_to_float(r.get("fee_dollars")) or 0.0)
        if cnt <= 0 or px is None:
            continue
        if edge_pct < float(min_edge_pct):
            continue
        d = str(r.get("date", "")).strip()
        c = str(r.get("city", "")).strip()
        side = normalize_temp_side(str(r.get("temp_type", "high")))
        station = CITY_CONFIG.get(c, {}).get("station", "")
        ticker = str(r.get("ticker", "")).strip()
        bet = str(r.get("bet", "")).strip().upper()
        line = str(r.get("line", "")).strip()
        if not (d and c and ticker and bet and line):
            continue
        key = (d, c, side, station, ticker, bet, line)
        prev = grouped_positions.get(key)
        stake_row = (px / 100.0) * cnt
        if prev is None:
            grouped_positions[key] = {
                "date": d,
                "city": c,
                "temp_side": side,
                "station": station,
                "ticker": ticker,
                "bet": bet,
                "line": line,
                "count": cnt,
                "stake_dollars": stake_row,
                "fee_dollars": fee_row,
            }
        else:
            prev["count"] = int(prev.get("count", 0) or 0) + cnt
            prev["stake_dollars"] = float(prev.get("stake_dollars", 0.0) or 0.0) + stake_row
            prev["fee_dollars"] = float(prev.get("fee_dollars", 0.0) or 0.0) + fee_row

    settlement_by_ticker: Dict[str, dict] = {}
    if kalshi_has_auth_config():
        try:
            for s in _fetch_kalshi_settlements(max_pages=20, per_page_limit=200):
                ticker = str(s.get("ticker", "")).strip()
                st_iso = str(s.get("settled_time", "")).strip()
                if not ticker or not st_iso:
                    continue
                try:
                    st_dt = datetime.fromisoformat(st_iso.replace("Z", "+00:00")).astimezone(est_tz)
                except Exception:
                    continue
                if st_dt > end_est:
                    continue
                prev = settlement_by_ticker.get(ticker)
                if prev is None:
                    settlement_by_ticker[ticker] = s
                else:
                    try:
                        prev_dt = datetime.fromisoformat(str(prev.get("settled_time", "")).replace("Z", "+00:00")).astimezone(est_tz)
                    except Exception:
                        prev_dt = st_dt
                    if st_dt >= prev_dt:
                        settlement_by_ticker[ticker] = s
        except Exception:
            settlement_by_ticker = {}

    fills = 0
    wins = 0
    losses = 0
    total_stake = 0.0
    total_fees = 0.0
    resolved_stake = 0.0
    unresolved_stake = 0.0
    realized_pnl = 0.0
    market_date_stats: Dict[str, dict] = {}

    for _k, r in grouped_positions.items():
        ticker = str(r.get("ticker", "")).strip()
        cnt = int(float(_to_float(r.get("count")) or 0))
        stake_row = float(_to_float(r.get("stake_dollars")) or 0.0)
        fee_row = float(_to_float(r.get("fee_dollars")) or 0.0)
        if stake_row <= 0:
            continue
        fills += 1
        total_stake += stake_row
        total_fees += fee_row
        md = parse_market_date_iso_from_ticker(ticker) or "unknown"
        md_row = market_date_stats.setdefault(md, {"entered": 0, "resolved": 0, "unresolved": 0, "wins": 0, "losses": 0})
        md_row["entered"] += 1

        s = settlement_by_ticker.get(ticker)
        if s is None:
            unresolved_stake += stake_row
            md_row["unresolved"] += 1
            continue

        market_result = str(s.get("market_result", "")).strip().lower()
        if market_result not in ("yes", "no"):
            unresolved_stake += stake_row
            md_row["unresolved"] += 1
            continue

        bet = str(r.get("bet", "")).strip().upper()
        yes_cost_c = float(_to_float(s.get("yes_total_cost")) or 0.0)
        no_cost_c = float(_to_float(s.get("no_total_cost")) or 0.0)
        revenue_c = float(_to_float(s.get("revenue")) or 0.0)
        fee_settle_d = float(_to_float(s.get("fee_cost")) or 0.0)
        stake_c = yes_cost_c if "YES" in bet else no_cost_c
        if stake_c <= 0:
            stake_c = stake_row * 100.0
        stake = stake_c / 100.0
        pnl = (revenue_c - stake_c) / 100.0 - fee_settle_d

        resolved_stake += stake
        realized_pnl += pnl
        won = ("YES" in bet and market_result == "yes") or ("NO" in bet and market_result == "no")
        md_row["resolved"] += 1
        if won:
            wins += 1
            md_row["wins"] += 1
        else:
            losses += 1
            md_row["losses"] += 1

    return {
        "window_start_est": fmt_est(start_est),
        "window_end_est": fmt_est(end_est),
        "min_edge_pct": float(min_edge_pct),
        "fills": fills,
        "total_stake_dollars": total_stake,
        "total_fees_dollars": total_fees,
        "fees_pct_on_stake": ((100.0 * total_fees / total_stake) if total_stake > 0 else None),
        "resolved_fills": (wins + losses),
        "wins": wins,
        "losses": losses,
        "resolved_stake_dollars": resolved_stake,
        "unresolved_fills": max(0, fills - (wins + losses)),
        "unresolved_stake_dollars": unresolved_stake,
        "realized_pnl_dollars": realized_pnl,
        "realized_roi_pct_on_total_stake": ((100.0 * realized_pnl / total_stake) if total_stake > 0 else None),
        "market_date_breakdown": {k: market_date_stats[k] for k in sorted(market_date_stats.keys())},
    }

def append_daily_update_history(summary: dict) -> None:
    path = daily_update_history_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_header = not os.path.exists(path)
    fields = [
        "posted_ts_est",
        "window_start_est",
        "window_end_est",
        "fills",
        "resolved_fills",
        "wins",
        "losses",
        "unresolved_fills",
        "total_stake_dollars",
        "total_fees_dollars",
        "total_stake_including_fees_dollars",
        "realized_pnl_dollars",
        "realized_roi_pct_on_total_stake",
        "previous_portfolio_balance_dollars",
        "current_portfolio_balance_dollars",
        "market_date_breakdown_json",
    ]
    row = {
        "posted_ts_est": fmt_est(datetime.now(tz=LOCAL_TZ)),
        "window_start_est": summary.get("window_start_est"),
        "window_end_est": summary.get("window_end_est"),
        "fills": int(summary.get("fills", 0) or 0),
        "resolved_fills": int(summary.get("resolved_fills", 0) or 0),
        "wins": int(summary.get("wins", 0) or 0),
        "losses": int(summary.get("losses", 0) or 0),
        "unresolved_fills": int(summary.get("unresolved_fills", 0) or 0),
        "total_stake_dollars": float(summary.get("total_stake_dollars", 0.0) or 0.0),
        "total_fees_dollars": float(summary.get("total_fees_dollars", 0.0) or 0.0),
        "total_stake_including_fees_dollars": float(summary.get("total_stake_including_fees_dollars", 0.0) or 0.0),
        "realized_pnl_dollars": float(summary.get("realized_pnl_dollars", 0.0) or 0.0),
        "realized_roi_pct_on_total_stake": (
            None if summary.get("realized_roi_pct_on_total_stake") is None
            else float(summary.get("realized_roi_pct_on_total_stake"))
        ),
        "previous_portfolio_balance_dollars": (
            None if summary.get("previous_portfolio_balance_dollars") is None
            else float(summary.get("previous_portfolio_balance_dollars"))
        ),
        "current_portfolio_balance_dollars": (
            None if summary.get("current_portfolio_balance_dollars") is None
            else float(summary.get("current_portfolio_balance_dollars"))
        ),
        "market_date_breakdown_json": json.dumps(summary.get("market_date_breakdown", {}), ensure_ascii=True),
    }
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        w.writerow(row)

def summarize_live_last_24h(now_local: datetime, min_edge_pct: float = 0.0) -> dict:
    est_tz = tz.tzoffset("EST", -5 * 3600)
    now_est = now_local.astimezone(est_tz)
    start_est = now_est - timedelta(hours=24)
    return summarize_live_window(start_est, now_est, min_edge_pct=min_edge_pct)

def summarize_live_market_date_kalshi(market_date_iso: str) -> dict:
    r = analytics_live_scorecard(start=market_date_iso, end=market_date_iso, finalized_only=True)
    s = (r or {}).get("summary", {}) if isinstance(r, dict) else {}
    wins = int(s.get("realized_win_count", 0) or 0)
    losses = int(s.get("realized_loss_count", 0) or 0)
    fills = int(s.get("fills", 0) or 0)
    resolved = int(s.get("realized_count", 0) or 0)
    stake = float(s.get("total_stake_dollars", 0.0) or 0.0)
    fees = float(s.get("total_fees_dollars", 0.0) or 0.0)
    stake_incl_fees = stake + fees
    comp = _fetch_portfolio_components_dollars()
    balance_now = comp.get("total_dollars")
    balance_prev = _get_last_daily_update_current_balance()
    if balance_prev is not None and balance_now is not None:
        if balance_prev <= 0 or balance_prev < (0.5 * balance_now) or balance_prev > (1.5 * balance_now):
            balance_prev = None
    if balance_prev is None and balance_now is not None:
        balance_prev = balance_now - float(s.get("total_realized_pnl_dollars", 0.0) or 0.0)
    return {
        "window_start_est": f"{market_date_iso} 12:00:00 AM ET",
        "window_end_est": f"{market_date_iso} 11:59:59 PM ET",
        "min_edge_pct": 0.0,
        "fills": fills,
        "total_stake_dollars": stake,
        "total_fees_dollars": fees,
        "total_stake_including_fees_dollars": stake_incl_fees,
        "fees_pct_on_stake": s.get("fees_pct_on_stake"),
        "resolved_fills": resolved,
        "wins": wins,
        "losses": losses,
        "resolved_stake_dollars": stake,
        "unresolved_fills": max(0, fills - resolved),
        "unresolved_stake_dollars": 0.0,
        "realized_pnl_dollars": float(s.get("total_realized_pnl_dollars", 0.0) or 0.0),
        "realized_roi_pct_on_total_stake": s.get("realized_roi_pct_on_stake"),
        "previous_portfolio_balance_dollars": balance_prev,
        "current_portfolio_balance_dollars": balance_now,
        "current_cash_dollars": comp.get("cash_dollars"),
        "current_positions_dollars": comp.get("positions_dollars"),
        "market_date_breakdown": {
            market_date_iso: {
                "entered": fills,
                "resolved": resolved,
                "unresolved": max(0, fills - resolved),
                "wins": wins,
                "losses": losses,
            }
        },
    }

def summarize_settlements_since(last_posted_est: Optional[datetime], end_est: datetime) -> dict:
    est_tz = tz.tzoffset("EST", -5 * 3600)
    end_est = end_est.astimezone(est_tz)
    if last_posted_est is None:
        start_est = end_est - timedelta(hours=24)
    else:
        start_est = last_posted_est.astimezone(est_tz)
    if start_est >= end_est:
        start_est = end_est - timedelta(hours=24)

    settlements: List[dict] = []
    if kalshi_has_auth_config():
        try:
            settlements = _fetch_kalshi_settlements(max_pages=30, per_page_limit=200)
        except Exception:
            settlements = []

    in_window: List[dict] = []
    for s in settlements:
        st_iso = str(s.get("settled_time", "")).strip()
        if not st_iso:
            continue
        try:
            st = datetime.fromisoformat(st_iso.replace("Z", "+00:00")).astimezone(est_tz)
        except Exception:
            continue
        if st <= start_est or st > end_est:
            continue
        in_window.append(s)

    fills = 0
    wins = 0
    losses = 0
    pushes = 0
    stake = 0.0
    fees = 0.0
    pnl = 0.0
    for s in in_window:
        yes_cost_c = float(_to_float(s.get("yes_total_cost")) or 0.0)
        no_cost_c = float(_to_float(s.get("no_total_cost")) or 0.0)
        revenue_c = float(_to_float(s.get("revenue")) or 0.0)
        fee_d = float(_to_float(s.get("fee_cost")) or 0.0)
        cost_c = yes_cost_c + no_cost_c
        if cost_c <= 0:
            continue
        fills += 1
        stake += (cost_c / 100.0)
        fees += fee_d
        trade_pnl = ((revenue_c - cost_c) / 100.0) - fee_d
        pnl += trade_pnl
        if trade_pnl > 1e-9:
            wins += 1
        elif trade_pnl < -1e-9:
            losses += 1
        else:
            pushes += 1

    comp = _fetch_portfolio_components_dollars()
    balance_now = comp.get("total_dollars")
    balance_prev = _get_last_daily_update_current_balance()
    # Guard against legacy bad rows (e.g., old cents/dollars bug) skewing previous balance.
    if balance_prev is not None and balance_now is not None:
        if balance_prev <= 0 or balance_prev < (0.5 * balance_now) or balance_prev > (1.5 * balance_now):
            balance_prev = None
    if balance_prev is None and balance_now is not None:
        balance_prev = balance_now - pnl

    stake_incl_fees = stake + fees
    return {
        "window_start_est": fmt_est(start_est),
        "window_end_est": fmt_est(end_est),
        "fills": fills,
        "total_stake_dollars": stake,
        "total_fees_dollars": fees,
        "total_stake_including_fees_dollars": stake_incl_fees,
        "resolved_fills": fills,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "realized_pnl_dollars": pnl,
        "realized_roi_pct_on_total_stake": ((100.0 * pnl / stake_incl_fees) if stake_incl_fees > 0 else None),
        "previous_portfolio_balance_dollars": balance_prev,
        "current_portfolio_balance_dollars": balance_now,
        "current_cash_dollars": comp.get("cash_dollars"),
        "current_positions_dollars": comp.get("positions_dollars"),
        "unresolved_fills": 0,
        "market_date_breakdown": {},
    }

def daily_update_text(now_local: datetime, summary: dict, shadow_summary: Optional[dict] = None) -> str:
    est_tz = tz.tzoffset("EST", -5 * 3600)
    now_est = now_local.astimezone(est_tz)
    date_est = now_est.date().isoformat()
    resolved_fills = int(summary.get("resolved_fills", 0) or 0)
    wins = int(summary.get("wins", 0) or 0)
    losses = int(summary.get("losses", 0) or 0)
    stake_incl_fees = float(summary.get("total_stake_including_fees_dollars", 0.0) or 0.0)
    prev_bal = summary.get("previous_portfolio_balance_dollars")
    cur_bal = summary.get("current_portfolio_balance_dollars")
    cur_cash = summary.get("current_cash_dollars")
    cur_pos = summary.get("current_positions_dollars")
    roi_txt = (('%.2f%%' % float(summary.get('realized_roi_pct_on_total_stake')))
               if summary.get('realized_roi_pct_on_total_stake') is not None else 'n/a')
    total_roi_txt = "n/a"
    if cur_bal is not None and DAILY_UPDATE_TOTAL_ROI_BASELINE_DOLLARS > 0:
        total_roi_pct = (100.0 * (float(cur_bal) - DAILY_UPDATE_TOTAL_ROI_BASELINE_DOLLARS) / DAILY_UPDATE_TOTAL_ROI_BASELINE_DOLLARS)
        total_roi_txt = f"{total_roi_pct:.2f}%"
    lines = [
        f"Daily Update - {date_est}",
        f"Time: {summary.get('window_start_est')} to {summary.get('window_end_est')}",
        f"Bets: {summary.get('fills', 0)} | Stake (incl fees): ${stake_incl_fees:.2f}",
        f"Resolved Bets: {resolved_fills}",
        f"Record (W-L): {wins}-{losses} | ROI: {roi_txt}",
        f"Net P/L (incl fees): ${float(summary.get('realized_pnl_dollars', 0.0)):.2f}",
        f"Previous Portfolio Balance: {('$%.2f' % float(prev_bal)) if prev_bal is not None else 'n/a'} | Current Portfolio Balance: {('$%.2f' % float(cur_bal)) if cur_bal is not None else 'n/a'}",
        f"Positions: {('$%.2f' % float(cur_pos)) if cur_pos is not None else 'n/a'} | Cash: {('$%.2f' % float(cur_cash)) if cur_cash is not None else 'n/a'} | Total: {('$%.2f' % float(cur_bal)) if cur_bal is not None else 'n/a'}",
        f"Total ROI since baseline (${DAILY_UPDATE_TOTAL_ROI_BASELINE_DOLLARS:.2f}): {total_roi_txt}",
    ]
    return "\n".join(lines)


def _build_nyc_forecast_brief_text(now_local: datetime, market_day: str, slot_name: str) -> Optional[str]:
    city = canonical_city_name(NYC_FORECAST_BRIEF_CITY) or "New York City"
    side = normalize_temp_side(NYC_FORECAST_BRIEF_TEMP_SIDE)
    grouped = refresh_markets_cache()
    city_markets = [m for m in grouped.get(city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
    if not city_markets:
        return None

    selected_markets, selected_date, _ = select_markets_for_day(city_markets, now_local, market_day, city=city)
    if not selected_markets or not selected_date:
        return None
    detail = build_city_bucket_comparison(city, selected_markets, now_local, temp_side=side)
    if not detail or not detail.get("buckets"):
        return None

    buckets = detail.get("buckets", []) or []
    best = max(buckets, key=lambda r: r.get("best_edge", -1.0))
    target_74p = None
    for r in buckets:
        try:
            lo = float(r.get("lo", -999))
            hi = float(r.get("hi", -999))
        except Exception:
            continue
        if lo >= 74.0 and hi >= 900.0:
            target_74p = r
            break
    source_text = ", ".join(
        f"{str(s.get('name'))}={float(s.get('high_f')):.1f}F"
        for s in (detail.get("sources", []) or [])
        if s.get("high_f") is not None
    )
    slot_title = "Evening Brief" if slot_name == "evening" else "Morning Brief"
    lines = [
        f"NYC High Forecast {slot_title}",
        f"Contract Date: {selected_date}",
        f"As of: {detail.get('as_of_est')}",
        f"Consensus: {float(detail.get('consensus_mu_f', 0.0)):.1f}F +/- {float(detail.get('consensus_sigma_f', 0.0)):.2f}",
    ]
    if source_text:
        lines.append(f"Sources: {source_text}")
    if target_74p is not None:
        lines.append(
            "74+ Focus: "
            f"model={100.0 * float(target_74p.get('source_yes_prob', 0.0)):.1f}% | "
            f"market={100.0 * float(target_74p.get('kalshi_yes_prob', 0.0)):.1f}% | "
            f"best={str(target_74p.get('best_side'))} | "
            f"edge={100.0 * float(target_74p.get('best_edge', 0.0)):.1f}% | "
            f"ticker={str(target_74p.get('ticker'))}"
        )
    lines.append(
        "Top Signal: "
        f"{str(best.get('best_side'))} | "
        f"{str(best.get('bucket_label'))} | "
        f"edge={100.0 * float(best.get('best_edge', 0.0)):.1f}% | "
        f"ticker={str(best.get('ticker'))}"
    )
    return "\n".join(lines)


def maybe_post_nyc_forecast_brief(now_local: datetime) -> bool:
    if not NYC_FORECAST_BRIEF_ENABLED:
        return False
    now_et = now_local.astimezone(LOCAL_TZ)
    minute_et = max(0, min(59, int(NYC_FORECAST_BRIEF_MINUTE_ET)))
    morning_hour = max(0, min(23, int(NYC_FORECAST_BRIEF_MORNING_HOUR_ET)))
    evening_hour = max(0, min(23, int(NYC_FORECAST_BRIEF_EVENING_HOUR_ET)))
    morning_target = now_et.replace(hour=morning_hour, minute=minute_et, second=0, microsecond=0)
    evening_target = now_et.replace(hour=evening_hour, minute=minute_et, second=0, microsecond=0)

    slot_name = ""
    market_day = ""
    slot_target = None
    if now_et >= evening_target:
        slot_name = "evening"
        market_day = "tomorrow"
        slot_target = evening_target
    elif now_et >= morning_target:
        slot_name = "morning"
        market_day = "today"
        slot_target = morning_target
    else:
        return False

    slot_key = f"{now_et.date().isoformat()}|{slot_name}"
    state = _load_nyc_forecast_brief_state()
    if slot_key in state:
        return False
    if slot_target is None or now_et < slot_target:
        return False

    text = _build_nyc_forecast_brief_text(now_local, market_day=market_day, slot_name=slot_name)
    if not text:
        return False
    discord_send_daily(text)
    state[slot_key] = fmt_est(now_local)
    _save_nyc_forecast_brief_state(state)
    return True

def maybe_post_daily_update(now_local: datetime) -> bool:
    if not DAILY_UPDATE_DISCORD_ENABLED:
        return False
    est_tz = tz.tzoffset("EST", -5 * 3600)
    now_est = now_local.astimezone(est_tz)
    today_est = now_est.date().isoformat()
    last_posted = _load_last_daily_update_date()
    target = now_est.replace(hour=max(0, min(23, DAILY_UPDATE_EST_HOUR)), minute=max(0, min(59, DAILY_UPDATE_EST_MINUTE)), second=0, microsecond=0)
    if now_est < target:
        return False
    if last_posted == today_est:
        return False
    market_date_iso = (now_est.date() - timedelta(days=1)).isoformat()
    s = summarize_live_market_date_kalshi(market_date_iso)
    s_shadow = s
    discord_send_daily(daily_update_text(now_local, s, s_shadow))
    append_daily_update_history(s)
    _save_last_daily_update_date(today_est)
    return True

def maybe_auto_sync_manual_positions(now_local: datetime) -> bool:
    global _manual_auto_sync_last_ts
    if not MANUAL_AUTO_SYNC_ENABLED:
        return False
    if not kalshi_has_auth_config():
        return False
    now_ts = float(time.time())
    min_gap_s = max(300.0, float(max(1, MANUAL_AUTO_SYNC_INTERVAL_MINUTES)) * 60.0)
    if _manual_auto_sync_last_ts > 0 and (now_ts - _manual_auto_sync_last_ts) < min_gap_s:
        return False
    _manual_auto_sync_last_ts = now_ts
    try:
        sync_manual_positions_from_kalshi(max_pages=20, per_page_limit=200, force_update=False, dry_run=False)
        return True
    except Exception:
        return False

@app.get("/scan")
def scan():
    now_local = datetime.now(tz=LOCAL_TZ)
    try:
        grouped = refresh_markets_cache()
    except RuntimeError as e:
        return {
            "ok": False,
            "error": str(e),
            "kalshi_base_url": KALSHI_BASE_URL,
        }
    results = build_ranked_results(grouped, now_local)
    discrepancy_alerts = build_discrepancy_alerts(grouped, now_local)
    try:
        record_snapshot_metrics(now_local, market_day="today")
    except Exception:
        pass
    posted = False
    discrepancy_posted = False
    paper_trade_posted_count = 0
    edge_tracking = {"active_count": 0, "closed_count": 0}
    daily_update_posted = False
    nyc_forecast_brief_posted = False
    if DISCORD_LEADERBOARD_ENABLED and should_post(results):
        discord_send(leaderboard_text(results, now_local))
        posted = True
    if DISCORD_DISCREPANCY_ENABLED and should_post_discrepancy(discrepancy_alerts):
        discord_send(discrepancy_text(discrepancy_alerts, now_local))
        discrepancy_posted = True
    try:
        board_payload = build_odds_board(now_local, market_day="today")
        if EDGE_TRACKING_ENABLED:
            try:
                edge_tracking = track_edge_lifecycles(now_local, board_payload)
            except Exception:
                edge_tracking = {"active_count": 0, "closed_count": 0}
        paper_trade_posted_count = maybe_post_paper_trades(now_local, board_payload)
    except Exception:
        paper_trade_posted_count = 0
    try:
        daily_update_posted = maybe_post_daily_update(now_local)
    except Exception:
        daily_update_posted = False
    try:
        nyc_forecast_brief_posted = maybe_post_nyc_forecast_brief(now_local)
    except Exception:
        nyc_forecast_brief_posted = False
    best = results[0] if results else {}
    return {
        "posted": posted,
        "discrepancy_posted": discrepancy_posted,
        "paper_trade_posted_count": paper_trade_posted_count,
        "daily_update_posted": daily_update_posted,
        "nyc_forecast_brief_posted": nyc_forecast_brief_posted,
        "edge_active_count": edge_tracking.get("active_count", 0),
        "edge_closed_count": edge_tracking.get("closed_count", 0),
        "best_city": best.get("city"),
        "best_score": best.get("score", 0.0),
        "discrepancy_count": len(discrepancy_alerts),
    }

@app.get("/discrepancies")
def discrepancies():
    now_local = datetime.now(tz=LOCAL_TZ)
    try:
        grouped = refresh_markets_cache()
    except RuntimeError as e:
        return {
            "ok": False,
            "error": str(e),
            "kalshi_base_url": KALSHI_BASE_URL,
        }
    alerts = build_discrepancy_alerts(grouped, now_local)
    return {
        "count": len(alerts),
        "alerts": alerts[:10],
    }

@app.get("/odds")
def odds(city: str = "New York City", temp_side: str = "high", market_day: str = "today"):
    now_local = datetime.now(tz=LOCAL_TZ)
    side = normalize_temp_side(temp_side)
    day_pref = normalize_market_day(market_day)
    try:
        grouped = refresh_markets_cache()
    except RuntimeError as e:
        return {
            "ok": False,
            "city": resolve_city_name(city) or city,
            "temp_side": side,
            "market_day_requested": day_pref,
            "error": str(e),
            "kalshi_base_url": KALSHI_BASE_URL,
            "hint": "verify live Kalshi API key permissions and matching private key",
        }
    resolved_city = resolve_city_name(city)
    if not resolved_city:
        return {
            "ok": False,
            "error": f"unknown city '{city}'",
            "cities": list(CITY_CONFIG.keys()),
        }
    city_markets = [m for m in grouped.get(resolved_city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
    if not city_markets:
        grouped = refresh_markets_cache(force=True)
        city_markets = [m for m in grouped.get(resolved_city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == side]
    if not city_markets:
        return {
            "ok": False,
            "city": resolved_city,
            "temp_side": side,
            "market_day_requested": day_pref,
            "error": f"no {side}-temperature city markets found in cache",
            "market_count": 0,
            "kalshi_base_url": KALSHI_BASE_URL,
            "hint": "weather series may be unavailable for this city/side right now",
        }

    city_cfg = CITY_CONFIG[resolved_city]
    series_meta = _load_series_metadata_map(force=False)

    by_date: Dict[str, List[Market]] = {}
    for m in city_markets:
        d = getattr(m, "market_date_iso", "") or parse_market_date_iso_from_ticker(m.ticker) or ""
        if not d:
            continue
        by_date.setdefault(d, []).append(m)
    available_dates = sorted(by_date.keys())
    selected_date = None
    city_today_iso = city_lst_now(now_local, resolved_city).date().isoformat()
    if by_date:
        if day_pref == "auto":
            if city_today_iso in by_date:
                selected_date = city_today_iso
            else:
                future_dates = [d for d in available_dates if d >= city_today_iso]
                selected_date = future_dates[0] if future_dates else available_dates[-1]
        else:
            wanted = market_date_for_day(now_local, day_pref, city=resolved_city)
            if wanted in by_date:
                selected_date = wanted
            else:
                return {
                    "ok": False,
                    "city": resolved_city,
                    "temp_side": side,
                    "market_day_requested": day_pref,
                    "market_date_requested": wanted,
                    "error": f"no {side}-temperature markets found for requested day",
                    "available_market_dates": available_dates,
                }
        city_markets = by_date[selected_date]

    consensus = build_expert_consensus(resolved_city, now_local, temp_side=side)
    if consensus is None:
        return {
            "ok": False,
            "city": resolved_city,
            "temp_side": side,
            "market_day_requested": day_pref,
            "market_date_selected": selected_date,
            "error": "no forecast sources available for consensus",
            "market_count": len(city_markets),
        }
    detail = build_city_bucket_comparison(resolved_city, city_markets, now_local, temp_side=side)
    if detail is None and day_pref == "auto" and by_date:
        candidate_dates = [d for d in available_dates if d != selected_date]
        future_candidates = [d for d in candidate_dates if d >= city_today_iso]
        candidate_dates = future_candidates + [d for d in candidate_dates if d < city_today_iso]
        for d in candidate_dates:
            alt_detail = build_city_bucket_comparison(resolved_city, by_date[d], now_local, temp_side=side)
            if alt_detail is not None:
                city_markets = by_date[d]
                selected_date = d
                detail = alt_detail
                break
    if detail is None:
        parsed_bucket_count = 0
        valid_orderbook_count = 0
        for m in city_markets:
            bucket = parse_bucket_from_title(m.title)
            if not bucket:
                continue
            parsed_bucket_count += 1
            try:
                ob = kalshi_get_orderbook(m.ticker)
                yes_bid, yes_ask, _ = best_bid_and_ask_from_orderbook(ob)
                if yes_bid is not None and yes_ask is not None and yes_ask >= yes_bid:
                    valid_orderbook_count += 1
            except Exception:
                pass
        return {
            "ok": False,
            "city": resolved_city,
            "temp_side": side,
            "market_day_requested": day_pref,
            "market_date_selected": selected_date,
            "error": "no odds comparison available",
            "market_count": len(city_markets),
            "parsed_bucket_count": parsed_bucket_count,
            "valid_orderbook_count": valid_orderbook_count,
        }
    return {
        "ok": True,
        "city": resolved_city,
        "temp_side": side,
        "market_day_requested": day_pref,
        "market_date_selected": selected_date,
        "available_market_dates": available_dates,
        "settlement_station": city_cfg["station"],
        "settlement_cli": city_cfg["cli"],
        "settlement_note": "Official settlement is from the NWS climatological report source listed in the contract terms.",
        "series_contract_terms": [
            {
                "series_ticker": st,
                "series_title": series_meta.get(st, {}).get("title"),
                "contract_terms_url": series_meta.get(st, {}).get("contract_terms_url"),
                "settlement_sources": series_meta.get(st, {}).get("settlement_sources", []),
            }
            for st in sorted(list({m.series_ticker for m in city_markets if getattr(m, "series_ticker", "")}))
        ],
        "as_of_est": fmt_est(now_local),
        "kalshi_mean_f": detail["kalshi_mean_f"],
        "consensus_mu_f": detail["consensus_mu_f"],
        "consensus_sigma_f": detail["consensus_sigma_f"],
        "nws_obs_context": detail.get("nws_obs_context"),
        "kalshi_odds_note": "Each bucket includes live Kalshi yes_bid/yes_ask in cents plus kalshi_yes_mid_prob (mid implied YES probability).",
        "sources": detail["sources"],
        "buckets": detail["buckets"],
    }

@app.post("/manual-eval")
def manual_eval(payload: dict = Body(...)):
    now_local = datetime.now(tz=LOCAL_TZ)
    city_raw = str(payload.get("city", "New York City"))
    weathercom_high = payload.get("weathercom_high_f")
    accuweather_high = payload.get("accuweather_high_f")
    sigma_f = payload.get("sigma_f", 2.0)

    resolved_city = resolve_city_name(city_raw)
    if not resolved_city:
        return {
            "ok": False,
            "error": f"unknown city '{city_raw}'",
            "cities": list(CITY_CONFIG.keys()),
        }

    if weathercom_high is None or accuweather_high is None:
        return {
            "ok": False,
            "city": resolved_city,
            "error": "weathercom_high_f and accuweather_high_f are required",
            "example": {
                "city": "NYC",
                "weathercom_high_f": 44,
                "accuweather_high_f": 43,
                "sigma_f": 2.0,
            },
        }

    try:
        wc = float(weathercom_high)
        aw = float(accuweather_high)
        sigma = max(0.5, float(sigma_f))
    except Exception:
        return {
            "ok": False,
            "city": resolved_city,
            "error": "weathercom_high_f, accuweather_high_f, and sigma_f must be numeric",
        }

    grouped = refresh_markets_cache()
    city_markets = [m for m in grouped.get(resolved_city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == "high"]
    if not city_markets:
        grouped = refresh_markets_cache(force=True)
        city_markets = [m for m in grouped.get(resolved_city, []) if normalize_temp_side(getattr(m, "temp_side", "high")) == "high"]
    if not city_markets:
        return {
            "ok": False,
            "city": resolved_city,
            "error": "no city markets found in cache",
            "market_count": 0,
            "kalshi_base_url": KALSHI_BASE_URL,
        }

    consensus = {
        "mu": (wc + aw) / 2.0,
        "sigma": sigma,
        "sources": [
            {"name": "Weather.com (Manual Input)", "high_f": wc, "weight": 1.0},
            {"name": "AccuWeather (Manual Input)", "high_f": aw, "weight": 1.0},
        ],
    }
    detail = build_city_bucket_comparison(resolved_city, city_markets, now_local, consensus_override=consensus)
    if detail is None:
        return {
            "ok": False,
            "city": resolved_city,
            "error": "unable to compute odds comparison from current market data",
            "market_count": len(city_markets),
        }

    plus_ev_rows = []
    for r in detail["buckets"]:
        yes_edge = r["source_yes_prob"] - (r["yes_ask"] / 100.0)
        no_edge = (r["yes_bid"] / 100.0) - r["source_yes_prob"]
        row = {
            "ticker": r["ticker"],
            "bucket_label": r["bucket_label"],
            "kalshi_yes_prob": r["kalshi_yes_prob"],
            "model_yes_prob": r["source_yes_prob"],
            "yes_ask": r["yes_ask"],
            "yes_bid": r["yes_bid"],
            "edge_buy_yes": yes_edge,
            "edge_buy_no": no_edge,
            "best_side": r["best_side"],
            "best_edge": r["best_edge"],
            "is_plus_ev": r["best_edge"] > 0,
        }
        if row["is_plus_ev"]:
            plus_ev_rows.append(row)
        r["is_plus_ev"] = row["is_plus_ev"]

    plus_ev_rows.sort(key=lambda x: x["best_edge"], reverse=True)
    return {
        "ok": True,
        "city": resolved_city,
        "as_of_est": fmt_est(now_local),
        "manual_inputs": {
            "weathercom_high_f": wc,
            "accuweather_high_f": aw,
            "sigma_f": sigma,
            "consensus_mu_f": detail["consensus_mu_f"],
        },
        "market_count": len(city_markets),
        "plus_ev_count": len(plus_ev_rows),
        "top_plus_ev": plus_ev_rows[:5],
        "buckets": detail["buckets"],
    }

def background_loop():
    while True:
        now_local = datetime.now(tz=LOCAL_TZ)
        try:
            grouped = refresh_markets_cache()
            results = build_ranked_results(grouped, now_local)
            discrepancy_alerts = build_discrepancy_alerts(grouped, now_local)
            try:
                record_snapshot_metrics(now_local, market_day="today")
            except Exception:
                pass
            if DISCORD_LEADERBOARD_ENABLED and should_post(results):
                discord_send(leaderboard_text(results, now_local))
            if DISCORD_DISCREPANCY_ENABLED and should_post_discrepancy(discrepancy_alerts):
                discord_send(discrepancy_text(discrepancy_alerts, now_local))
            try:
                board_payload = build_odds_board(now_local, market_day="today")
                _maybe_extend_fast_scan_window(board_payload, time.time())
                if EDGE_TRACKING_ENABLED:
                    track_edge_lifecycles(now_local, board_payload)
                maybe_post_paper_trades(now_local, board_payload)
            except Exception:
                pass
            try:
                maybe_post_daily_update(now_local)
            except Exception:
                pass
            try:
                maybe_post_nyc_forecast_brief(now_local)
            except Exception:
                pass
            try:
                maybe_auto_sync_manual_positions(now_local)
            except Exception:
                pass
        except Exception:
            pass
        time.sleep(compute_sleep_seconds(datetime.now(tz=LOCAL_TZ)))

@app.on_event("startup")
def on_startup():
    try:
        _load_accuweather_cache_state()
    except Exception:
        pass
    threading.Thread(target=background_loop, daemon=True).start()

