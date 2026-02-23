"""Pytest configuration for mute package tests."""
import os
import sys

# Ensure scripts dir is in path for analytics, tests (mute_check, etc.)
_here = os.path.dirname(os.path.abspath(__file__))
_scripts = os.path.dirname(os.path.dirname(_here))
if _scripts not in sys.path:
    sys.path.insert(0, _scripts)
_analytics = os.path.join(_scripts, 'analytics')
if _analytics not in sys.path:
    sys.path.insert(0, _analytics)
