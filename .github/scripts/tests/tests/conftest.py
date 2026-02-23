"""Pytest configuration for mute system tests."""
import os
import sys

# Add parent dir so we can import mute_check, mute_utils, create_new_muted_ya, etc.
_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent not in sys.path:
    sys.path.insert(0, _parent)
# Add analytics for ydb_wrapper (tests may mock these)
_analytics = os.path.join(_parent, '..', 'analytics')
if os.path.exists(_analytics) and _analytics not in sys.path:
    sys.path.insert(0, _analytics)
