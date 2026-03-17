"""functions to parse the properties of elements"""

import re

from . import value_class_pattern
from .datetime_helpers import DATETIME_RE, TIME_RE, normalize_datetime
from .dom_helpers import get_attr, get_img, get_textContent, try_urljoin


def text(el, base_url=""):
    """Process p-* properties"""

    # handle value-class-pattern
    prop_value = value_class_pattern.text(el)
    if prop_value is not None:
        return prop_value

    prop_value = get_attr(el, "title", check_name=("abbr", "link"))
    if prop_value is None:
        prop_value = get_attr(el, "value", check_name=("data", "input"))
    if prop_value is None:
        prop_value = get_attr(el, "alt", check_name=("img", "area"))
    if prop_value is None:
        prop_value = get_textContent(el, replace_img=True, base_url=base_url)

    return prop_value


def url(el, base_url=""):
    """Process u-* properties"""

    prop_value = get_attr(el, "href", check_name=("a", "area", "link"))
    if prop_value is None:
        prop_value = get_img(el, base_url)
        if prop_value is not None:
            return prop_value
    if prop_value is None:
        prop_value = get_attr(
            el, "src", check_name=("audio", "video", "source", "iframe")
        )
    if prop_value is None:
        prop_value = get_attr(el, "poster", check_name="video")
    if prop_value is None:
        prop_value = get_attr(el, "data", check_name="object")

    if prop_value is None:
        # handle value-class-pattern
        prop_value = value_class_pattern.text(el)

    if prop_value is None:
        prop_value = get_attr(el, "title", check_name="abbr")
    if prop_value is None:
        prop_value = get_attr(el, "value", check_name=("data", "input"))
    if prop_value is None:
        prop_value = get_textContent(el)

    return try_urljoin(base_url, prop_value)


def datetime(el, default_date=None):
    """Process dt-* properties

    Args:
      el (bs4.element.Tag): Tag containing the dt-value

    Returns:
      a tuple (string string): a tuple of two strings, (datetime, date)
    """

    # handle value-class-pattern
    prop_value = value_class_pattern.datetime(el, default_date)
    if prop_value is not None:
        return prop_value

    prop_value = get_attr(el, "datetime", check_name=("time", "ins", "del"))
    if prop_value is None:
        prop_value = get_attr(el, "title", check_name="abbr")
    if prop_value is None:
        prop_value = get_attr(el, "value", check_name=("data", "input"))
    if prop_value is None:
        prop_value = get_textContent(el)

    # if this is just a time, augment with default date
    match = re.match(TIME_RE + "$", prop_value)
    if match and default_date:
        prop_value = "%s %s" % (default_date, prop_value)
        return normalize_datetime(prop_value), default_date

    # otherwise, treat it as a full date
    match = re.match(DATETIME_RE + "$", prop_value)
    return (
        normalize_datetime(prop_value, match=match),
        match and match.group("date"),
    )


def embedded(el, base_url, root_lang, document_lang, expose_dom):
    """Process e-* properties"""
    for tag in el.find_all():
        for attr in ("href", "src", "cite", "data", "poster"):
            if attr in tag.attrs:
                tag.attrs[attr] = try_urljoin(base_url, tag.attrs[attr])
    prop_value = {
        "value": get_textContent(el, replace_img=True, base_url=base_url),
    }
    if lang := el.attrs.get("lang"):
        prop_value["lang"] = lang
    elif root_lang:
        prop_value["lang"] = root_lang
    elif document_lang:
        prop_value["lang"] = document_lang
    if expose_dom:
        prop_value["dom"] = el
    else:
        prop_value["html"] = el.decode_contents().strip()
    return prop_value
