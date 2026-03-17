#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

__all__ = (
    "country_holidays",
    "CountryHoliday",
    "financial_holidays",
    "list_localized_countries",
    "list_localized_financial",
    "list_long_breaks",
    "list_supported_countries",
    "list_supported_financial",
)

import warnings
from collections.abc import Iterable
from datetime import date
from functools import cache

from holidays.calendars.gregorian import _timedelta
from holidays.holiday_base import CategoryArg, HolidayBase
from holidays.registry import EntityLoader


def country_holidays(
    country: str,
    subdiv: str | None = None,
    years: int | Iterable[int] | None = None,
    expand: bool = True,
    observed: bool = True,
    prov: str | None = None,
    state: str | None = None,
    language: str | None = None,
    categories: CategoryArg | None = None,
) -> HolidayBase:
    """Return a new dictionary-like [HolidayBase][holidays.holiday_base.HolidayBase] object.

    Include public holidays for the country matching `country` and other keyword arguments.

    Args:
        country:
            An ISO 3166-1 Alpha-2 country code.

        subdiv:
            The subdivision (e.g. state or province) as a ISO 3166-2 code
            or its alias; not implemented for all countries (see documentation).

        years:
            The year(s) to pre-calculate public holidays for at instantiation.

        expand:
            Whether the entire year is calculated when one date from that year
            is requested.

        observed:
            Whether to include the dates of when public holidays are observed
            (e.g. a holiday falling on a Sunday being observed the following
            Monday). `False` may not work for all countries.

        prov:
            *deprecated* use `subdiv` instead.

        state:
            *deprecated* use `subdiv` instead.

        language:
            Specifies the language in which holiday names are returned.

            Accepts either:

            * A two-letter ISO 639-1 language code (e.g., 'en' for English, 'fr' for French),
                or
            * A language and entity combination using an underscore (e.g., 'en_US' for U.S.
                English, 'pt_BR' for Brazilian Portuguese).

            !!! warning
                The provided language or locale code must be supported by the holiday
                entity. Unsupported values will result in names being shown in the entity's
                original language.

            If not explicitly set (`language=None`), the system attempts to infer the
            language from the environment's locale settings. The following environment
            variables are checked, in order of precedence: LANGUAGE, LC_ALL, LC_MESSAGES, LANG.

            If none of these are set or they are empty, holiday names will default to the
            original language of the entity's holiday implementation.

            !!! warning
                This fallback mechanism may yield inconsistent results across environments
                (e.g., between a terminal session and a Jupyter notebook).

            To ensure consistent behavior, it is recommended to set the language parameter
            explicitly. If the specified language is not supported, holiday names will remain
            in the original language of the entity's holiday implementation.

            This behavior will be updated and formalized in v1.

        categories:
            Requested holiday categories.

    Returns:
        A `HolidayBase` object matching the `country`.

    The key of the `dict`-like `HolidayBase` object is the
    `date` of the holiday, and the value is the name of the holiday itself.
    Dates where a key is not present are not public holidays (or, if
    `observed` is `False`, days when a public holiday is observed).

    When passing the `date` as a key, the `date` can be expressed in one of the
    following types:

    * `datetime.date`,
    * `datetime.datetime`,
    * a `str` of any format recognized by `dateutil.parser.parse()`,
    * or a `float` or `int` representing a POSIX timestamp.

    The key is always returned as a `datetime.date` object.

    To maximize speed, the list of public holidays is built on the fly as
    needed, one calendar year at a time. When the object is instantiated
    without a `years` parameter, it is empty, but, unless `expand` is set
    to `False`, as soon as a key is accessed the class will calculate that entire
    year's list of holidays and set the keys with them.

    If you need to list the holidays as opposed to querying individual dates,
    instantiate the class with the `years` parameter.

    Example usage:

        >>> from holidays import country_holidays
        >>> us_holidays = country_holidays('US')
        # For a specific subdivision (e.g. state or province):
        >>> calif_holidays = country_holidays('US', subdiv='CA')

    The below will cause 2015 holidays to be calculated on the fly:

        >>> from datetime import date
        >>> assert date(2015, 1, 1) in us_holidays

    This will be faster because 2015 holidays are already calculated:

        >>> assert date(2015, 1, 2) not in us_holidays

    The `HolidayBase` class also recognizes strings of many formats
    and numbers representing a POSIX timestamp:

        >>> assert '2014-01-01' in us_holidays
        >>> assert '1/1/2014' in us_holidays
        >>> assert 1388597445 in us_holidays

    Show the holiday's name:

        >>> us_holidays.get('2014-01-01')
        "New Year's Day"

    Check a range:

        >>> us_holidays['2014-01-01': '2014-01-03']
        [datetime.date(2014, 1, 1)]

    List all 2020 holidays:

        >>> us_holidays = country_holidays('US', years=2020)
        >>> for day in sorted(us_holidays.items()):
        ...     print(day)
        (datetime.date(2020, 1, 1), "New Year's Day")
        (datetime.date(2020, 1, 20), 'Martin Luther King Jr. Day')
        (datetime.date(2020, 2, 17), "Washington's Birthday")
        (datetime.date(2020, 5, 25), 'Memorial Day')
        (datetime.date(2020, 7, 3), 'Independence Day (observed)')
        (datetime.date(2020, 7, 4), 'Independence Day')
        (datetime.date(2020, 9, 7), 'Labor Day')
        (datetime.date(2020, 10, 12), 'Columbus Day')
        (datetime.date(2020, 11, 11), 'Veterans Day')
        (datetime.date(2020, 11, 26), 'Thanksgiving Day')
        (datetime.date(2020, 12, 25), 'Christmas Day')

    Some holidays are only present in parts of a country:

        >>> us_pr_holidays = country_holidays('US', subdiv='PR')
        >>> assert '2018-01-06' not in us_holidays
        >>> assert '2018-01-06' in us_pr_holidays

    Append custom holiday dates by passing one of:

    * a `dict` with date/name key/value pairs (e.g.
      `{'2010-07-10': 'My birthday!'}`),
    * a list of dates (as a `datetime.date`, `datetime.datetime`,
      `str`, `int`, or `float`); "Holiday" will be used as a description,
    * or a single date item (of one of the types above); "Holiday" will be
      used as a description:

    ```python
    >>> custom_holidays = country_holidays('US', years=2015)
    >>> custom_holidays.update({'2015-01-01': "New Year's Day"})
    >>> custom_holidays.update(['2015-07-01', '07/04/2015'])
    >>> custom_holidays.update(date(2015, 12, 25))
    >>> assert date(2015, 1, 1) in custom_holidays
    >>> assert date(2015, 1, 2) not in custom_holidays
    >>> assert '12/25/2015' in custom_holidays
    ```

    For more complex logic, like 4th Monday of January, you can inherit the
    `HolidayBase` class and define your own `_populate` method.
    See documentation for examples.
    """
    import holidays

    try:
        return getattr(holidays, country)(
            years=years,
            subdiv=subdiv,
            expand=expand,
            observed=observed,
            prov=prov,
            state=state,
            language=language,
            categories=categories,
        )
    except AttributeError:
        raise NotImplementedError(f"Country {country} not available")


def financial_holidays(
    market: str,
    subdiv: str | None = None,
    years: int | Iterable[int] | None = None,
    expand: bool = True,
    observed: bool = True,
    language: str | None = None,
    categories: CategoryArg | None = None,
) -> HolidayBase:
    """Return a new dictionary-like [HolidayBase][holidays.holiday_base.HolidayBase] object.

    Include public holidays for the financial market matching `market` and other keyword
    arguments.

    Args:
        market:
            An ISO 10383 MIC code.

        subdiv:
            Currently not implemented for markets (see documentation).

        years:
            The year(s) to pre-calculate public holidays for at instantiation.

        expand:
            Whether the entire year is calculated when one date from that year
            is requested.

        observed:
            Whether to include the dates of when public holidays are observed
            (e.g. a holiday falling on a Sunday being observed the following
            Monday). `False` may not work for all markets.

        language:
            Specifies the language in which holiday names are returned.

            Accepts either:

            * A two-letter ISO 639-1 language code (e.g., 'en' for English, 'fr' for French),
                or
            * A language and entity combination using an underscore (e.g., 'en_US' for U.S.
                English, 'pt_BR' for Brazilian Portuguese).

            !!! warning
                The provided language or locale code must be supported by the holiday
                entity. Unsupported values will result in names being shown in the entity's
                original language.

            If not explicitly set (`language=None`), the system attempts to infer the
            language from the environment's locale settings. The following environment
            variables are checked, in order of precedence: LANGUAGE, LC_ALL, LC_MESSAGES, LANG.

            If none of these are set or they are empty, holiday names will default to the
            original language of the entity's holiday implementation.

            !!! warning
                This fallback mechanism may yield inconsistent results across environments
                (e.g., between a terminal session and a Jupyter notebook).

            To ensure consistent behavior, it is recommended to set the language parameter
            explicitly. If the specified language is not supported, holiday names will remain
            in the original language of the entity's holiday implementation.

            This behavior will be updated and formalized in v1.

        categories:
            Requested holiday categories.

    Returns:
        A `HolidayBase` object matching the `market`.

    Example usage:

        >>> from holidays import financial_holidays
        >>> nyse_holidays = financial_holidays('XNYS')

    See [country_holidays()][holidays.utils.country_holidays] documentation for further
    details and examples.
    """
    import holidays

    try:
        return getattr(holidays, market)(
            years=years,
            subdiv=subdiv,
            expand=expand,
            observed=observed,
            language=language,
            categories=categories,
        )
    except AttributeError:
        raise NotImplementedError(f"Financial market {market} not available")


def CountryHoliday(  # noqa: N802
    country: str,
    subdiv: str | None = None,
    years: int | Iterable[int] | None = None,
    expand: bool = True,
    observed: bool = True,
    prov: str | None = None,
    state: str | None = None,
) -> HolidayBase:
    """
    Note:
        Deprecated name for `country_holidays()`.
    """

    warnings.warn(
        "CountryHoliday is deprecated, use country_holidays instead.", DeprecationWarning
    )
    return country_holidays(country, subdiv, years, expand, observed, prov, state)


def _list_localized_entities(entity_codes: Iterable[str]) -> dict[str, list[str]]:
    """Get all localized entities and languages they support.

    Args:
        entity_codes:
            A list of entity codes.

    Returns:
        A dictionary where key is an entity code and value is a list of supported
        languages (either ISO 639-1 or a combination of ISO 639-1 and ISO 3166-1 codes joined
        with "_").
    """
    import holidays

    localized_countries = {}
    for entity_code in entity_codes:
        languages = getattr(holidays, entity_code).supported_languages
        if not languages:
            continue
        localized_countries[entity_code] = sorted(languages)

    return localized_countries


@cache
def list_localized_countries(include_aliases: bool = True) -> dict[str, list[str]]:
    """Get all localized countries and languages they support.

    Args:
        include_aliases:
            Whether to include entity aliases (e.g. UK for GB).

    Returns:
        A dictionary where key is an ISO 3166-1 alpha-2 country code and value is a
        list of supported languages (either ISO 639-1 or a combination of ISO 639-1
        and ISO 3166-1 codes joined with "_").
    """
    return _list_localized_entities(EntityLoader.get_country_codes(include_aliases))


@cache
def list_localized_financial(include_aliases: bool = True) -> dict[str, list[str]]:
    """Get all localized financial markets and languages they support.

    Args:
        include_aliases:
            Whether to include entity aliases (e.g. TAR for ECB, XNYS for NYSE).

    Returns:
        A dictionary where key is a market code and value is a list of supported
        subdivision codes.
    """
    return _list_localized_entities(EntityLoader.get_financial_codes(include_aliases))


def _list_supported_entities(entity_codes: Iterable[str]) -> dict[str, list[str]]:
    """Get all supported entities and their subdivisions.

    Args:
        entity_codes:
            A list of entity codes.

    Returns:
        A dictionary where key is an entity code and value is a list of supported
        subdivision codes.
    """
    import holidays

    return {
        country_code: list(getattr(holidays, country_code).subdivisions)
        for country_code in entity_codes
    }


@cache
def list_supported_countries(include_aliases: bool = True) -> dict[str, list[str]]:
    """Get all supported countries and their subdivisions.

    Args:
        include_aliases:
            Whether to include entity aliases (e.g. UK for GB).

    Returns:
        A dictionary where key is an ISO 3166-1 alpha-2 country code and value
        is a list of supported subdivision codes.
    """
    return _list_supported_entities(EntityLoader.get_country_codes(include_aliases))


@cache
def list_supported_financial(include_aliases: bool = True) -> dict[str, list[str]]:
    """Get all supported financial markets and their subdivisions.

    Args:
        include_aliases:
            Whether to include entity aliases (e.g. NYSE for XNYS, TAR for XECB).

    Returns:
        A dictionary where key is a market code and value is a list of supported
        subdivision codes.
    """
    return _list_supported_entities(EntityLoader.get_financial_codes(include_aliases))


def list_long_breaks(
    instance: HolidayBase, *, minimum_break_length: int = 3, require_weekend_overlap: bool = True
) -> list[list[date]]:
    """Get consecutive holidays.

    Args:
        instance:
            HolidayBase object containing holidays data.

        minimum_break_length:
            The minimum number of consecutive holidays required for a break period
            to be considered a long one. Defaults to 3.

        require_weekend_overlap:
            Whether to include only consecutive holidays that overlap with a weekend.
            Defaults to True.

    Returns:
        A list of consecutive holidays longer than or equal to the specified minimum length.
    """
    long_breaks = []
    seen_dates = set()

    for dt in sorted(instance.keys()):
        if dt in seen_dates:
            continue

        previous_working_day = instance.get_nth_working_day(dt, -1)
        next_working_day = instance.get_nth_working_day(dt, +1)
        long_break_length = (next_working_day - previous_working_day).days - 1

        if long_break_length < minimum_break_length:
            continue

        is_long_break = not require_weekend_overlap
        long_break_dates = []
        for delta_days in range(1, long_break_length + 1):
            holiday = _timedelta(previous_working_day, delta_days)
            if not is_long_break and instance._is_weekend(holiday):
                is_long_break = True
            long_break_dates.append(holiday)
            seen_dates.add(holiday)

        if is_long_break:
            long_breaks.append(long_break_dates)

    return long_breaks
