"""Mute pipeline settings from ``.github/config/mute_config.json`` (no cross-imports between scripts)."""

import json
import os

_MUTE_CONFIG_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'mute_config.json')
)

_REQUIRED_KEYS = (
    'manual_unmute_window_days',
    'manual_unmute_min_runs',
    'manual_unmute_ttl_calendar_days',
    'mute_window_days',
    'unmute_window_days',
    'delete_window_days',
    'manual_unmute_issue_closed_lookback_days',
    'manual_unmute_currently_muted_lookback_days',
)

_CACHE = None


def _payload():
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    try:
        with open(_MUTE_CONFIG_PATH, 'r', encoding='utf-8') as fp:
            raw = json.load(fp)
    except OSError as exc:
        raise RuntimeError(f'Cannot read mute config {_MUTE_CONFIG_PATH}') from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'Invalid JSON in mute config {_MUTE_CONFIG_PATH}') from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f'Mute config must be a JSON object: {_MUTE_CONFIG_PATH}')
    missing = [k for k in _REQUIRED_KEYS if k not in raw]
    if missing:
        raise RuntimeError(
            f'{_MUTE_CONFIG_PATH}: missing required key(s): {", ".join(sorted(missing))}'
        )
    _CACHE = raw
    return _CACHE


def _positive_int(key):
    v = _payload()[key]
    try:
        n = int(v)
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            f'{_MUTE_CONFIG_PATH}: key {key!r} must be a positive integer, got {v!r}'
        ) from exc
    if n <= 0:
        raise RuntimeError(f'{_MUTE_CONFIG_PATH}: key {key!r} must be positive, got {n}')
    return n


def get_mute_window_days():
    return _positive_int('mute_window_days')


def get_unmute_window_days():
    return _positive_int('unmute_window_days')


def get_delete_window_days():
    return _positive_int('delete_window_days')


def get_manual_unmute_issue_closed_lookback_days():
    return _positive_int('manual_unmute_issue_closed_lookback_days')


def get_manual_unmute_currently_muted_lookback_days():
    return _positive_int('manual_unmute_currently_muted_lookback_days')


def get_manual_unmute_window_days():
    return _positive_int('manual_unmute_window_days')


def get_manual_unmute_min_runs():
    return _positive_int('manual_unmute_min_runs')


def get_manual_unmute_ttl_calendar_days():
    return _positive_int('manual_unmute_ttl_calendar_days')
