import os
import re
import warnings
from typing import Any, Dict, Optional, Tuple


class UserAgentParser(object):
    def __init__(
        self, pattern, family_replacement=None, v1_replacement=None, v2_replacement=None
    ):
        """Initialize UserAgentParser.

        Args:
          pattern: a regular expression string
          family_replacement: a string to override the matched family (optional)
          v1_replacement: a string to override the matched v1 (optional)
          v2_replacement: a string to override the matched v2 (optional)
        """
        self.user_agent_re = re.compile(pattern)
        self.family_replacement = family_replacement
        self.v1_replacement = v1_replacement
        self.v2_replacement = v2_replacement

    def Parse(
        self, user_agent_string: str
    ) -> Tuple[
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        family, v1, v2, v3 = None, None, None, None
        match = self.user_agent_re.search(user_agent_string)
        if match:
            if self.family_replacement:
                if re.search(r"\$1", self.family_replacement):
                    family = re.sub(r"\$1", match[1], self.family_replacement)
                else:
                    family = self.family_replacement
            else:
                family = match[1]

            if self.v1_replacement:
                v1 = self.v1_replacement
            elif match.lastindex and match.lastindex >= 2:
                v1 = match[2] or None

            if self.v2_replacement:
                v2 = self.v2_replacement
            elif match.lastindex and match.lastindex >= 3:
                v2 = match[3] or None

            if match.lastindex and match.lastindex >= 4:
                v3 = match[4] or None

        return family, v1, v2, v3


class OSParser(object):
    def __init__(
        self,
        pattern,
        os_replacement=None,
        os_v1_replacement=None,
        os_v2_replacement=None,
        os_v3_replacement=None,
        os_v4_replacement=None,
    ):
        """Initialize UserAgentParser.

        Args:
          pattern: a regular expression string
          os_replacement: a string to override the matched os (optional)
          os_v1_replacement: a string to override the matched v1 (optional)
          os_v2_replacement: a string to override the matched v2 (optional)
          os_v3_replacement: a string to override the matched v3 (optional)
          os_v4_replacement: a string to override the matched v4 (optional)
        """
        self.user_agent_re = re.compile(pattern)
        self.os_replacement = os_replacement
        self.os_v1_replacement = os_v1_replacement
        self.os_v2_replacement = os_v2_replacement
        self.os_v3_replacement = os_v3_replacement
        self.os_v4_replacement = os_v4_replacement

    def Parse(
        self, user_agent_string: str
    ) -> Tuple[
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        os, os_v1, os_v2, os_v3, os_v4 = None, None, None, None, None
        match = self.user_agent_re.search(user_agent_string)
        if match:
            if self.os_replacement:
                os = MultiReplace(self.os_replacement, match)
            elif match.lastindex:
                os = match[1]

            if self.os_v1_replacement:
                os_v1 = MultiReplace(self.os_v1_replacement, match)
            elif match.lastindex and match.lastindex >= 2:
                os_v1 = match[2]

            if self.os_v2_replacement:
                os_v2 = MultiReplace(self.os_v2_replacement, match)
            elif match.lastindex and match.lastindex >= 3:
                os_v2 = match[3]

            if self.os_v3_replacement:
                os_v3 = MultiReplace(self.os_v3_replacement, match)
            elif match.lastindex and match.lastindex >= 4:
                os_v3 = match[4]

            if self.os_v4_replacement:
                os_v4 = MultiReplace(self.os_v4_replacement, match)
            elif match.lastindex and match.lastindex >= 5:
                os_v4 = match[5]

        return os, os_v1, os_v2, os_v3, os_v4


def MultiReplace(string, match):
    def _repl(m):
        index = int(m[1]) - 1
        group = match.groups()
        if index < len(group):
            return group[index]
        return ""

    _string = re.sub(r"\$(\d)", _repl, string).strip()
    return _string or None


class DeviceParser(object):
    def __init__(
        self,
        pattern,
        regex_flag=None,
        device_replacement=None,
        brand_replacement=None,
        model_replacement=None,
    ):
        """Initialize UserAgentParser.

        Args:
          pattern: a regular expression string
          device_replacement: a string to override the matched device (optional)
        """
        self.user_agent_re = re.compile(
            pattern, re.IGNORECASE if regex_flag == "i" else 0
        )
        self.device_replacement = device_replacement
        self.brand_replacement = brand_replacement
        self.model_replacement = model_replacement

    def Parse(
        self, user_agent_string: str
    ) -> Tuple[
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        device, brand, model = None, None, None
        match = self.user_agent_re.search(user_agent_string)
        if match:
            if self.device_replacement:
                device = MultiReplace(self.device_replacement, match)
            else:
                device = match[1]

            if self.brand_replacement:
                brand = MultiReplace(self.brand_replacement, match)

            if self.model_replacement:
                model = MultiReplace(self.model_replacement, match)
            elif len(match.groups()) > 0:
                model = match[1]

        return device, brand, model


MAX_CACHE_SIZE = 200
_PARSE_CACHE: Dict[str, Dict[str, Any]] = {}


def _lookup(ua):
    if not isinstance(ua, str):
        raise TypeError(f"Expected user agent to be a string, got {ua!r}")

    entry = _PARSE_CACHE.get(ua)
    if entry is not None:
        return entry

    if len(_PARSE_CACHE) >= MAX_CACHE_SIZE:
        _PARSE_CACHE.clear()

    v = _PARSE_CACHE[ua] = {"string": ua}
    return v


def _cached(ua, key, fn):
    entry = _lookup(ua)
    r = entry.get(key)
    if not r:
        r = entry[key] = fn(ua)
    return r


def Parse(user_agent_string, **_jsParseBits):
    """Parse all the things
    Args:
      user_agent_string: the full user agent string
    Returns:
      A dictionary containing all parsed bits
    """
    if _jsParseBits:
        warnings.warn(
            "javascript overrides are not used anymore",
            category=DeprecationWarning,
            stacklevel=2,
        )

    entry = _lookup(user_agent_string)
    # entry is complete, return directly
    if len(entry) == 4:
        return entry

    # entry is partially or entirely empty
    if "user_agent" not in entry:
        entry["user_agent"] = _ParseUserAgent(user_agent_string)
    if "os" not in entry:
        entry["os"] = _ParseOS(user_agent_string)
    if "device" not in entry:
        entry["device"] = _ParseDevice(user_agent_string)

    return entry


def ParseUserAgent(user_agent_string, **_jsParseBits):
    """Parses the user-agent string for user agent (browser) info.
    Args:
      user_agent_string: The full user-agent string.
    Returns:
      A dictionary containing parsed bits.
    """
    if _jsParseBits:
        warnings.warn(
            "javascript overrides are not used anymore",
            category=DeprecationWarning,
            stacklevel=2,
        )
    return _cached(user_agent_string, "user_agent", _ParseUserAgent)


def _ParseUserAgent(user_agent_string):
    for uaParser in USER_AGENT_PARSERS:
        family, v1, v2, v3 = uaParser.Parse(user_agent_string)
        if family:
            break

    family = family or "Other"
    return {
        "family": family,
        "major": v1 or None,
        "minor": v2 or None,
        "patch": v3 or None,
    }


def ParseOS(user_agent_string, **_jsParseBits):
    """Parses the user-agent string for operating system info
    Args:
      user_agent_string: The full user-agent string.
    Returns:
      A dictionary containing parsed bits.
    """
    if _jsParseBits:
        warnings.warn(
            "javascript overrides are not used anymore",
            category=DeprecationWarning,
            stacklevel=2,
        )
    return _cached(user_agent_string, "os", _ParseOS)


def _ParseOS(user_agent_string):
    for osParser in OS_PARSERS:
        os, os_v1, os_v2, os_v3, os_v4 = osParser.Parse(user_agent_string)
        if os:
            break
    os = os or "Other"
    return {
        "family": os,
        "major": os_v1,
        "minor": os_v2,
        "patch": os_v3,
        "patch_minor": os_v4,
    }


def ParseDevice(user_agent_string, **_jsParseBits):
    """Parses the user-agent string for device info.
    Args:
        user_agent_string: The full user-agent string.
    Returns:
        A dictionary containing parsed bits.
    """
    if _jsParseBits:
        warnings.warn(
            "javascript overrides are not used anymore",
            category=DeprecationWarning,
            stacklevel=2,
        )
    return _cached(user_agent_string, "device", _ParseDevice)


def _ParseDevice(user_agent_string):
    for deviceParser in DEVICE_PARSERS:
        device, brand, model = deviceParser.Parse(user_agent_string)
        if device:
            break

    if device is None:
        device = "Other"

    return {"family": device, "brand": brand, "model": model}


def PrettyUserAgent(family, v1=None, v2=None, v3=None):
    """Pretty user agent string."""
    if v3:
        if v3[0].isdigit():
            return f"{family} {v1}.{v2}.{v3}"
        else:
            return f"{family} {v1}.{v2}{v3}"
    elif v2:
        return f"{family} {v1}.{v2}"
    elif v1:
        return f"{family} {v1}"
    return family


def PrettyOS(os, os_v1=None, os_v2=None, os_v3=None, os_v4=None):
    """Pretty os string."""
    if os_v4:
        return f"{os} {os_v1}.{os_v2}.{os_v3}.{os_v4}"
    if os_v3:
        if os_v3[0].isdigit():
            return f"{os} {os_v1}.{os_v2}.{os_v3}"
        else:
            return f"{os} {os_v1}.{os_v2}{os_v3}"
    elif os_v2:
        return f"{os} {os_v1}.{os_v2}"
    elif os_v1:
        return f"{os} {os_v1}"
    return os


def ParseWithJSOverrides(
    user_agent_string,
    js_user_agent_string=None,
    js_user_agent_family=None,
    js_user_agent_v1=None,
    js_user_agent_v2=None,
    js_user_agent_v3=None,
):
    warnings.warn(
        "Use Parse (or a specialised parser)", DeprecationWarning, stacklevel=2
    )

    # Override via JS properties.
    if js_user_agent_family is not None and js_user_agent_family != "":
        family = js_user_agent_family
        v1 = None
        v2 = None
        v3 = None
        if js_user_agent_v1 is not None:
            v1 = js_user_agent_v1
        if js_user_agent_v2 is not None:
            v2 = js_user_agent_v2
        if js_user_agent_v3 is not None:
            v3 = js_user_agent_v3
    else:
        for parser in USER_AGENT_PARSERS:
            family, v1, v2, v3 = parser.Parse(user_agent_string)
            if family:
                break

    # Override for Chrome Frame IFF Chrome is enabled.
    if (
        js_user_agent_string
        and js_user_agent_string.find("Chrome/") > -1
        and user_agent_string.find("chromeframe") > -1
    ):
        family = "Chrome Frame (%s %s)" % (family, v1)
        ua_dict = ParseUserAgent(js_user_agent_string)
        v1 = ua_dict["major"]
        v2 = ua_dict["minor"]
        v3 = ua_dict["patch"]

    return family or "Other", v1, v2, v3


def Pretty(family, v1=None, v2=None, v3=None):
    warnings.warn("Use PrettyUserAgent", DeprecationWarning, stacklevel=2)
    if v3:
        if v3[0].isdigit():
            return f"{family} {v1}.{v2}.{v3}"
        else:
            return f"{family} {v1}.{v2}{v3}"
    elif v2:
        return f"{family} {v1}.{v2}"
    elif v1:
        return f"{family} {v1}"
    return family


def GetFilters(
    user_agent_string,
    js_user_agent_string=None,
    js_user_agent_family=None,
    js_user_agent_v1=None,
    js_user_agent_v2=None,
    js_user_agent_v3=None,
):
    warnings.warn("No use case anymore", DeprecationWarning, stacklevel=2)

    filters = {}
    filterdict = {
        "js_user_agent_string": js_user_agent_string,
        "js_user_agent_family": js_user_agent_family,
        "js_user_agent_v1": js_user_agent_v1,
        "js_user_agent_v2": js_user_agent_v2,
        "js_user_agent_v3": js_user_agent_v3,
    }
    for key, value in filterdict.items():
        if value is not None and value != "":
            filters[key] = value
    return filters


# Build the list of user agent parsers from YAML
UA_PARSER_YAML = os.environ.get("UA_PARSER_YAML")
if UA_PARSER_YAML:
    # This will raise an ImportError if missing, obviously since it's no
    # longer a requirement
    import yaml

    try:
        # Try and use libyaml bindings if available since faster,
        # pyyaml doesn't do it by default (yaml/pyyaml#436)
        from yaml import CSafeLoader as SafeLoader
    except ImportError:
        from yaml import SafeLoader  #  type: ignore

    with open(UA_PARSER_YAML, "rb") as fp:
        regexes = yaml.load(fp, Loader=SafeLoader)

    USER_AGENT_PARSERS = []
    for _ua_parser in regexes["user_agent_parsers"]:
        _regex = _ua_parser["regex"]

        _family_replacement = _ua_parser.get("family_replacement")
        _v1_replacement = _ua_parser.get("v1_replacement")
        _v2_replacement = _ua_parser.get("v2_replacement")

        USER_AGENT_PARSERS.append(
            UserAgentParser(
                _regex, _family_replacement, _v1_replacement, _v2_replacement
            )
        )

    OS_PARSERS = []
    for _os_parser in regexes["os_parsers"]:
        _regex = _os_parser["regex"]

        _os_replacement = _os_parser.get("os_replacement")
        _os_v1_replacement = _os_parser.get("os_v1_replacement")
        _os_v2_replacement = _os_parser.get("os_v2_replacement")
        _os_v3_replacement = _os_parser.get("os_v3_replacement")
        _os_v4_replacement = _os_parser.get("os_v4_replacement")

        OS_PARSERS.append(
            OSParser(
                _regex,
                _os_replacement,
                _os_v1_replacement,
                _os_v2_replacement,
                _os_v3_replacement,
                _os_v4_replacement,
            )
        )

    DEVICE_PARSERS = []
    for _device_parser in regexes["device_parsers"]:
        _regex = _device_parser["regex"]

        _regex_flag = _device_parser.get("regex_flag")
        _device_replacement = _device_parser.get("device_replacement")
        _brand_replacement = _device_parser.get("brand_replacement")
        _model_replacement = _device_parser.get("model_replacement")

        DEVICE_PARSERS.append(
            DeviceParser(
                _regex,
                _regex_flag,
                _device_replacement,
                _brand_replacement,
                _model_replacement,
            )
        )

    # Clean our our temporary vars explicitly
    # so they can't be reused or imported
    del regexes
    del yaml
    del SafeLoader
else:
    # Just load our pre-compiled versions
    from ua_parser_builtins.regexes import (
        DEVICE_PARSERS,
        OS_PARSERS,
        USER_AGENT_PARSERS,
    )
