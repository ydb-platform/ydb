# -*- test-case-name: pytils.test.test_dt -*-
"""
Russian dates without locales
"""

from __future__ import annotations

import datetime

from pytils import numeral
from pytils.utils import check_positive

DAY_ALTERNATIVES = {
    1: ("вчера", "завтра"),
    2: ("позавчера", "послезавтра"),
}  #: Day alternatives (i.e. one day ago -> yesterday)

DAY_VARIANTS = (
    "день",
    "дня",
    "дней",
)  #: Forms (1, 2, 5) for noun 'day'

HOUR_VARIANTS = (
    "час",
    "часа",
    "часов",
)  #: Forms (1, 2, 5) for noun 'hour'

MINUTE_VARIANTS = (
    "минуту",
    "минуты",
    "минут",
)  #: Forms (1, 2, 5) for noun 'minute'

PREFIX_IN = "через"  #: Prefix 'in' (i.e. B{in} three hours)
SUFFIX_AGO = "назад"  #: Prefix 'ago' (i.e. three hours B{ago})

MONTH_NAMES = (
    ("янв", "январь", "января"),
    ("фев", "февраль", "февраля"),
    ("мар", "март", "марта"),
    ("апр", "апрель", "апреля"),
    ("май", "май", "мая"),
    ("июн", "июнь", "июня"),
    ("июл", "июль", "июля"),
    ("авг", "август", "августа"),
    ("сен", "сентябрь", "сентября"),
    ("окт", "октябрь", "октября"),
    ("ноя", "ноябрь", "ноября"),
    ("дек", "декабрь", "декабря"),
)  #: Month names (abbreviated, full, inflected)

DAY_NAMES = (
    ("пн", "понедельник", "понедельник", "в\xa0"),
    ("вт", "вторник", "вторник", "во\xa0"),
    ("ср", "среда", "среду", "в\xa0"),
    ("чт", "четверг", "четверг", "в\xa0"),
    ("пт", "пятница", "пятницу", "в\xa0"),
    ("сб", "суббота", "субботу", "в\xa0"),
    ("вск", "воскресенье", "воскресенье", "в\xa0"),
)  #: Day names (abbreviated, full, inflected, preposition)


def distance_of_time_in_words(
    from_time: int | float | datetime.datetime,
    accuracy: int = 1,
    to_time: int | float | datetime.datetime | None = None,
) -> str:
    """
    Represents distance of time in words

    @param from_time: source time (in seconds from epoch)
    @type from_time: C{int}, C{float} or C{datetime.datetime}

    @param accuracy: level of accuracy (1..3), default=1
    @type accuracy: C{int}

    @param to_time: target time (in seconds from epoch),
        default=None translates to current time
    @type to_time: C{int}, C{float} or C{datetime.datetime}

    @return: distance of time in words
    @rtype: C{str}

    @raise ValueError: accuracy is lesser or equal zero
    """
    current = False

    if to_time is None:
        current = True
        to_time = datetime.datetime.now()

    check_positive(accuracy, strict=True)

    if not isinstance(from_time, datetime.datetime):
        from_time = datetime.datetime.fromtimestamp(from_time)

    if not isinstance(to_time, datetime.datetime):
        to_time = datetime.datetime.fromtimestamp(to_time)

    if from_time.tzinfo and not to_time.tzinfo:
        to_time = to_time.replace(tzinfo=from_time.tzinfo)

    dt_delta = to_time - from_time
    difference = dt_delta.days * 86400 + dt_delta.seconds

    minutes_orig = int(abs(difference) / 60.0)
    hours_orig = int(abs(difference) / 3600.0)
    days_orig = int(abs(difference) / 86400.0)
    in_future = from_time > to_time

    words = []
    values = []
    alternatives = []

    days = days_orig
    hours = hours_orig - days_orig * 24

    words.append("%d %s" % (days, numeral.choose_plural(days, DAY_VARIANTS)))
    values.append(days)

    words.append("%d %s" % (hours, numeral.choose_plural(hours, HOUR_VARIANTS)))
    values.append(hours)

    days == 0 and hours == 1 and current and alternatives.append("час")

    minutes = minutes_orig - hours_orig * 60

    words.append("%d %s" % (minutes, numeral.choose_plural(minutes, MINUTE_VARIANTS)))
    values.append(minutes)

    days == 0 and hours == 0 and minutes == 1 and current and alternatives.append(
        "минуту"
    )

    # убираем из values и words конечные нули
    while values and not values[-1]:
        values.pop()
        words.pop()
    # убираем из values и words начальные нули
    while values and not values[0]:
        values.pop(0)
        words.pop(0)
    limit = min(accuracy, len(words))
    real_words = words[:limit]
    real_values = values[:limit]
    # снова убираем конечные нули
    while real_values and not real_values[-1]:
        real_values.pop()
        real_words.pop()
        limit -= 1

    real_str = " ".join(real_words)

    # альтернативные варианты нужны только если в real_words одно значение
    # и, вдобавок, если используется текущее время
    alter_str = limit == 1 and current and alternatives and alternatives[0]
    _result_str = alter_str or real_str
    result_str = (
        in_future
        and "{} {}".format(PREFIX_IN, _result_str)
        or "{} {}".format(_result_str, SUFFIX_AGO)
    )

    # если же прошло менее минуты, то real_words -- пустой, и поэтому
    # нужно брать alternatives[0], а не result_str
    zero_str = (
        minutes == 0
        and not real_words
        and (in_future and "менее чем через минуту" or "менее минуты назад")
    )

    # нужно использовать вчера/позавчера/завтра/послезавтра
    # если days 1..2 и в real_words одно значение
    day_alternatives = DAY_ALTERNATIVES.get(days, False)
    alternate_day = (
        day_alternatives
        and current
        and limit == 1
        and ((in_future and day_alternatives[1]) or day_alternatives[0])
    )

    final_str = not real_words and zero_str or alternate_day or result_str

    return final_str


def ru_strftime(
    format: str = "%d.%m.%Y",
    date: datetime.date | datetime.datetime | None = None,
    inflected: bool = False,
    inflected_day: bool = False,
    preposition: bool = False,
):
    """
    Russian strftime without locale

    @param format: strftime format, default='%d.%m.%Y'
    @type format: C{str}

    @param date: date value, default=None translates to today
    @type date: C{datetime.date} or C{datetime.datetime}

    @param inflected: is month inflected, default False
    @type inflected: C{bool}

    @param inflected_day: is day inflected, default False
    @type inflected: C{bool}

    @param preposition: is preposition used, default False
        preposition=True automatically implies inflected_day=True
    @type preposition: C{bool}

    @return: strftime string
    @rtype: C{str}
    """
    if date is None:
        date = datetime.datetime.today()

    weekday = date.weekday()

    prepos = preposition and DAY_NAMES[weekday][3] or ""

    month_idx = inflected and 2 or 1
    day_idx = (inflected_day or preposition) and 2 or 1

    # for russian typography standard,
    # 1 April 2007, but 01.04.2007
    if "%b" in format or "%B" in format:
        format = format.replace("%d", str(date.day))

    format = format.replace("%a", prepos + DAY_NAMES[weekday][0])
    format = format.replace("%A", prepos + DAY_NAMES[weekday][day_idx])
    format = format.replace("%b", MONTH_NAMES[date.month - 1][0])
    format = format.replace("%B", MONTH_NAMES[date.month - 1][month_idx])

    u_res = date.strftime(format)
    return u_res
