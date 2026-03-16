"""functions to parse the properties of elements according to the value class pattern http://microformats.org/wiki/value-class-pattern """

import re

from .datetime_helpers import (
    DATE_RE,
    DATETIME_RE,
    TIME_RE,
    TIMEZONE_RE,
    normalize_datetime,
)
from .dom_helpers import get_children


def _get_vcp_value(el):
    if "value-title" in el.get("class", []):
        return el.get("title")
    return el.get_text()


def _get_vcp_children(el):
    return [
        c
        for c in get_children(el)
        if c.has_attr("class")
        and ("value" in c["class"] or "value-title" in c["class"])
    ]


def text(el):
    value_els = _get_vcp_children(el)
    if value_els:
        return "".join(_get_vcp_value(el) for el in value_els)


def datetime(el, default_date=None):
    value_els = _get_vcp_children(el)
    if value_els:
        date_parts = []
        for value_el in value_els:
            if "value-title" in value_el.get("class", []):
                title = el.get("title")
                if title:
                    date_parts.append(title.strip())
            elif value_el.name in ("img", "area"):
                alt = value_el.get("alt") or value_el.get_text()
                if alt:
                    date_parts.append(alt.strip())
            elif value_el.name == "data":
                val = value_el.get("value") or value_el.get_text()
                if val:
                    date_parts.append(val.strip())
            elif value_el.name == "abbr":
                title = value_el.get("title") or value_el.get_text()
                if title:
                    date_parts.append(title.strip())
            elif value_el.name in ("del", "ins", "time"):
                dt = value_el.get("datetime") or value_el.get_text()
                if dt:
                    date_parts.append(dt.strip())
            else:
                val = value_el.get_text()
                if val:
                    date_parts.append(val.strip())

        date_part = time_part = tz_part = None

        for part in date_parts:
            match = re.match(DATETIME_RE + "$", part)
            if match:
                # if it's a full datetime, then we're done
                date_part = match.group("date")
                return normalize_datetime(part, match=match), date_part

            # only use first found value
            if re.match(TIME_RE + "$", part) and time_part is None:
                time_part = part
            elif re.match(DATE_RE + "$", part) and date_part is None:
                date_part = part
            elif re.match(TIMEZONE_RE + "$", part) and tz_part is None:
                tz_part = part

        # use default date
        if date_part is None:
            date_part = default_date

        if date_part and time_part:
            date_time_value = "%s %s" % (date_part, time_part)
        else:
            date_time_value = date_part or time_part

        if tz_part:
            date_time_value += tz_part

        return normalize_datetime(date_time_value), date_part
