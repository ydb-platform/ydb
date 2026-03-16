"""
Utilities for handling time spans and date ranges.
"""

import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_week_start(date, start_of_week="monday"):
    """Get the start of the week for a given date."""
    if start_of_week == "monday":
        days_back = date.weekday()
    else:  # sunday
        days_back = (date.weekday() + 1) % 7

    return date - timedelta(days=days_back)


def get_week_end(date, start_of_week="monday"):
    """Get the end of the week for a given date."""
    week_start = get_week_start(date, start_of_week)
    return week_start + timedelta(days=6)


def detect_time_span(text):
    """Detect time span expressions in text and return span information."""
    span_patterns = [
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:past|last|previous)\s+month\b",
            "type": "month",
            "direction": "past",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:past|last|previous)\s+week\b",
            "type": "week",
            "direction": "past",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:past|last|previous)\s+(\d+)\s+days?\b",
            "type": "days",
            "direction": "past",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:past|last|previous)\s+(\d+)\s+weeks?\b",
            "type": "weeks",
            "direction": "past",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:past|last|previous)\s+(\d+)\s+months?\b",
            "type": "months",
            "direction": "past",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:next|coming|following)\s+month\b",
            "type": "month",
            "direction": "future",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:next|coming|following)\s+week\b",
            "type": "week",
            "direction": "future",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:next|coming|following)\s+(\d+)\s+days?\b",
            "type": "days",
            "direction": "future",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:next|coming|following)\s+(\d+)\s+weeks?\b",
            "type": "weeks",
            "direction": "future",
        },
        {
            "pattern": r"\b(?:for\s+the\s+|during\s+the\s+|in\s+the\s+)?(?:next|coming|following)\s+(\d+)\s+months?\b",
            "type": "months",
            "direction": "future",
        },
    ]

    for pattern_info in span_patterns:
        match = re.search(pattern_info["pattern"], text, re.IGNORECASE)
        if match:
            result = {
                "type": pattern_info["type"],
                "direction": pattern_info["direction"],
                "matched_text": match.group(0),
                "start_pos": match.start(),
                "end_pos": match.end(),
            }

            if match.groups():
                result["number"] = int(match.group(1))

            return result

    return None


def generate_time_span(span_info, base_date=None, settings=None):
    """Generate start and end dates for a time span."""
    if base_date is None:
        base_date = datetime.now()

    if settings is None:
        start_of_week = "monday"
        days_in_month = 30
    else:
        start_of_week = getattr(settings, "DEFAULT_START_OF_WEEK", "monday")
        days_in_month = getattr(settings, "DEFAULT_DAYS_IN_MONTH", 30)

    span_type = span_info["type"]
    direction = span_info["direction"]
    number = span_info.get("number", 1)

    if direction == "past":
        end_date = base_date

        if span_type == "month":
            start_date = end_date - relativedelta(days=days_in_month)
        elif span_type == "week":
            week_start = get_week_start(end_date, start_of_week)
            start_date = week_start - timedelta(days=7)
            end_date = week_start - timedelta(days=1)
        elif span_type == "days":
            start_date = end_date - timedelta(days=number)
        elif span_type == "weeks":
            start_date = end_date - timedelta(weeks=number)
        elif span_type == "months":
            start_date = end_date - relativedelta(months=number)
        else:
            start_date = end_date - timedelta(days=1)

    else:
        start_date = base_date

        if span_type == "month":
            end_date = start_date + relativedelta(days=days_in_month)
        elif span_type == "week":
            week_start = get_week_start(start_date, start_of_week)
            start_date = week_start + timedelta(days=7)
            end_date = start_date + timedelta(days=6)
        elif span_type == "days":
            end_date = start_date + timedelta(days=number)
        elif span_type == "weeks":
            end_date = start_date + timedelta(weeks=number)
        elif span_type == "months":
            end_date = start_date + relativedelta(months=number)
        else:
            end_date = start_date + timedelta(days=1)

    return (start_date, end_date)
