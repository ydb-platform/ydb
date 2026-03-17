import importlib.util
import sys
from datetime import datetime

import regex as re

TIME_MATCHER = re.compile(
    r".*?"
    r"(?P<hour>2[0-3]|[0-1]\d|\d):"
    r"(?P<minute>[0-5]\d|\d):"
    r"(?P<second>6[0-1]|[0-5]\d|\d)"
    r"\.(?P<microsecond>[0-9]{1,6})"
)

MS_SEARCHER = re.compile(r"\.(?P<microsecond>[0-9]{1,6})")


def _exec_module(spec, module):
    if hasattr(spec.loader, "exec_module"):
        spec.loader.exec_module(module)
    else:
        # This can happen before Python 3.10
        # if spec.loader is a zipimporter and the Python runtime is in a zipfile
        code = spec.loader.get_code(module.__name__)
        exec(code, module.__dict__)


def patch_strptime():
    """Monkey patching _strptime to avoid problems related with non-english
    locale changes on the system.

    For example, if system's locale is set to fr_FR. Parser won't recognize
    any date since all languages are translated to english dates.
    """
    _strptime_spec = importlib.util.find_spec("_strptime")
    _strptime = importlib.util.module_from_spec(_strptime_spec)
    _exec_module(_strptime_spec, _strptime)
    sys.modules["strptime_patched"] = _strptime

    _calendar = importlib.util.module_from_spec(_strptime_spec)
    _exec_module(_strptime_spec, _calendar)
    sys.modules["calendar_patched"] = _calendar

    _strptime._getlang = lambda: ("en_US", "UTF-8")
    _strptime.calendar = _calendar
    _strptime.calendar.day_abbr = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    _strptime.calendar.day_name = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    _strptime.calendar.month_abbr = [
        "",
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
    ]
    _strptime.calendar.month_name = [
        "",
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]

    return _strptime._strptime_time


__strptime = patch_strptime()


def _prepare_format(date_string: str, og_format: str) -> tuple[str, str]:
    # Adapted from std lib: https://github.com/python/cpython/blob/e34a5e33049ce845de646cf24a498766a2da3586/Lib/_strptime.py#L448
    format = re.sub(r"([\\.^$*+?\(\){}\[\]|])", r"\\\1", og_format)
    format = re.sub(r"\s+", r"\\s+", format)
    format = re.sub(r"'", "['\u02bc]", format)
    year_in_format = False
    day_of_month_in_format = False

    def repl(m: re.Match[str]) -> str:
        format_char = m[1]
        if format_char in ("Y", "y", "G"):
            nonlocal year_in_format
            year_in_format = True
        elif format_char in ("d",):
            nonlocal day_of_month_in_format
            day_of_month_in_format = True

        return ""

    _ = re.sub(r"%[-_0^#]*[0-9]*([OE]?\\?.?)", repl, format)
    if day_of_month_in_format and not year_in_format:
        current_year = datetime.today().year
        return f"{current_year} {date_string}", f"%Y {og_format}"
    return date_string, og_format


def strptime(date_string: str, format: str) -> datetime:
    date_string, format = _prepare_format(date_string, format)
    obj = datetime(*__strptime(date_string, format)[:-3])

    if "%f" in format:
        try:
            match_groups = TIME_MATCHER.match(date_string).groupdict()
            ms = match_groups["microsecond"]
            ms = ms + ((6 - len(ms)) * "0")
            obj = obj.replace(microsecond=int(ms))
        except AttributeError:
            match_groups = MS_SEARCHER.search(date_string).groupdict()
            ms = match_groups["microsecond"]
            ms = ms + ((6 - len(ms)) * "0")
            obj = obj.replace(microsecond=int(ms))

    return obj
