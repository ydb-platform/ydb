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

__all__ = ("DateLike", "HolidayBase", "HolidaySum")

import copy
import warnings
from bisect import bisect_left, bisect_right
from calendar import isleap
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from functools import cached_property
from gettext import gettext, translation
from pathlib import Path
from typing import Any, Literal, Union, cast

from dateutil.parser import parse

from holidays.calendars.gregorian import (
    MON,
    TUE,
    WED,
    THU,
    FRI,
    SAT,
    SUN,
    _timedelta,
    _get_nth_weekday_from,
    _get_nth_weekday_of_month,
    DAYS,
    MONTHS,
    WEEKDAYS,
)
from holidays.constants import HOLIDAY_NAME_DELIMITER, PUBLIC, DEFAULT_START_YEAR, DEFAULT_END_YEAR
from holidays.helpers import _normalize_arguments, _normalize_tuple

CategoryArg = str | Iterable[str]
DateArg = date | tuple[int, int] | tuple[int, int, int]
DateLike = date | datetime | str | float | int
NameLookup = Literal["contains", "exact", "startswith", "icontains", "iexact", "istartswith"]
SpecialHoliday = tuple[int, int, str] | tuple[tuple[int, int, str], ...]
SubstitutedHoliday = (
    tuple[int, int, int, int]
    | tuple[int, int, int, int, int]
    | tuple[tuple[int, int, int, int] | tuple[int, int, int, int, int], ...]
)
YearArg = int | Iterable[int]


class HolidayBase(dict[date, str]):
    """Represent a dictionary-like collection of holidays for a specific country or region.

    This class inherits from `dict` and maps holiday dates to their names. It supports
    customization by country and, optionally, by province or state (subdivision). A date
    not present as a key is not considered a holiday (or, if `observed` is `False`, not
    considered an observed holiday).

    Keys are holiday dates, and values are corresponding holiday names. When accessing or
    assigning holidays by date, the following input formats are accepted:

    * `datetime.date`
    * `datetime.datetime`
    * `float` or `int` (Unix timestamp)
    * `str` of any format recognized by `dateutil.parser.parse()`

    Keys are always returned as `datetime.date` objects.

    To maximize performance, the holiday list is lazily populated one year at a time.
    On instantiation, the object is empty. Once a date is accessed, the full calendar
    year for that date is generated, unless `expand` is set to `False`. To pre-populate
    holidays, instantiate the class with the `years` argument:

        us_holidays = holidays.US(years=2020)

    It is recommended to use the
    [country_holidays()][holidays.utils.country_holidays] function for instantiation.

    Example usage:

        >>> from holidays import country_holidays
        >>> us_holidays = country_holidays('US')
        # For a specific subdivisions (e.g. state or province):
        >>> california_holidays = country_holidays('US', subdiv='CA')

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

    Append custom holiday dates by passing one of the following:

    * A dict mapping date values to holiday names (e.g. `{'2010-07-10': 'My birthday!'}`).
    * A list of date values (`datetime.date`, `datetime.datetime`, `str`, `int`, or `float`);
      each will be added with 'Holiday' as the default name.
    * A single date value of any of the supported types above; 'Holiday' will be used as
      the default name.

    ```python
    >>> custom_holidays = country_holidays('US', years=2015)
    >>> custom_holidays.update({'2015-01-01': "New Year's Day"})
    >>> custom_holidays.update(['2015-07-01', '07/04/2015'])
    >>> custom_holidays.update(date(2015, 12, 25))
    >>> assert date(2015, 1, 1) in custom_holidays
    >>> assert date(2015, 1, 2) not in custom_holidays
    >>> assert '12/25/2015' in custom_holidays
    ```

    For special (one-off) country-wide holidays handling use
    `special_public_holidays`:

        special_public_holidays = {
            1977: ((JUN, 7, "Silver Jubilee of Elizabeth II"),),
            1981: ((JUL, 29, "Wedding of Charles and Diana"),),
            1999: ((DEC, 31, "Millennium Celebrations"),),
            2002: ((JUN, 3, "Golden Jubilee of Elizabeth II"),),
            2011: ((APR, 29, "Wedding of William and Catherine"),),
            2012: ((JUN, 5, "Diamond Jubilee of Elizabeth II"),),
            2022: (
                (JUN, 3, "Platinum Jubilee of Elizabeth II"),
                (SEP, 19, "State Funeral of Queen Elizabeth II"),
            ),
        }

        def _populate(self, year):
            super()._populate(year)

            ...

    For more complex logic, like 4th Monday of January, you can inherit the
    [HolidayBase][holidays.holiday_base.HolidayBase] class and define your own `_populate()`
    method.
    See documentation for examples.
    """

    country: str
    """The country's ISO 3166-1 alpha-2 code."""
    market: str
    """The market's ISO 3166-1 alpha-2 code."""
    subdivisions: tuple[str, ...] = ()
    """The subdivisions supported for this country (see documentation)."""
    subdivisions_aliases: dict[str, str] = {}
    """Aliases for the ISO 3166-2 subdivision codes with the key as alias and
    the value the ISO 3166-2 subdivision code."""
    years: set[int]
    """The years calculated."""
    expand: bool
    """Whether the entire year is calculated when one date from that year
    is requested."""
    observed: bool
    """Whether dates when public holiday are observed are included."""
    subdiv: str | None = None
    """The subdiv requested as ISO 3166-2 code or one of the aliases."""
    special_holidays: dict[int, SpecialHoliday | SubstitutedHoliday] = {}
    """A list of the country-wide special (as opposite to regular) holidays for
    a specific year."""
    _deprecated_subdivisions: tuple[str, ...] = ()
    """Other subdivisions whose names are deprecated or aliases of the official
    ones."""
    weekend: set[int] = {SAT, SUN}
    """Country weekend days."""
    weekend_workdays: set[date]
    """Working days moved to weekends."""
    default_category: str = PUBLIC
    """The entity category used by default."""
    default_language: str | None = None
    """The entity language used by default."""
    categories: set[str] = set()
    """Requested holiday categories."""
    supported_categories: tuple[str, ...] = (PUBLIC,)
    """All holiday categories supported by this entity."""
    supported_languages: tuple[str, ...] = ()
    """All languages supported by this entity."""
    start_year: int = DEFAULT_START_YEAR
    """Start year of holidays presence for this entity."""
    end_year: int = DEFAULT_END_YEAR
    """End year of holidays presence for this entity."""
    parent_entity: type["HolidayBase"] | None = None
    """Optional parent entity to reference as a base."""

    def __init__(
        self,
        years: YearArg | None = None,
        expand: bool = True,
        observed: bool = True,
        subdiv: str | None = None,
        prov: str | None = None,  # Deprecated.
        state: str | None = None,  # Deprecated.
        language: str | None = None,
        categories: CategoryArg | None = None,
    ) -> None:
        """
        Args:
            years:
                The year(s) to pre-calculate public holidays for at instantiation.

            expand:
                Whether the entire year is calculated when one date from that year
                is requested.

            observed:
                Whether to include the dates when public holiday are observed
                (e.g. a holiday falling on a Sunday being observed the
                following Monday). This doesn't work for all countries.

            subdiv:
                The subdivision (e.g. state or province) as a ISO 3166-2 code
                or its alias; not implemented for all countries (see documentation).

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
            A `HolidayBase` object matching the `country` or `market`.
        """
        super().__init__()

        # Categories validation.
        if self.default_category and self.default_category not in self.supported_categories:
            raise ValueError("The default category must be listed in supported categories.")

        if not self.default_category and not categories:
            raise ValueError("Categories cannot be empty if `default_category` is not set.")

        categories = _normalize_arguments(str, categories) or {self.default_category}
        if unknown_categories := categories.difference(  # type: ignore[union-attr]
            self.supported_categories
        ):
            raise ValueError(f"Category is not supported: {', '.join(unknown_categories)}.")

        # Subdivision validation.
        if subdiv := subdiv or prov or state:
            # Handle subdivisions passed as integers.
            if isinstance(subdiv, int):
                subdiv = str(subdiv)

            subdivision_aliases = tuple(self.subdivisions_aliases)
            supported_subdivisions = set(
                self.subdivisions
                + subdivision_aliases
                + self._deprecated_subdivisions
                + (self.parent_entity.subdivisions if self.parent_entity else ())
            )

            # Unsupported subdivisions.
            if not isinstance(self, HolidaySum) and subdiv not in supported_subdivisions:
                raise NotImplementedError(
                    f"Entity `{self._entity_code}` does not have subdivision {subdiv}"
                )

            # Deprecated arguments.
            if prov_state := prov or state:
                warnings.warn(
                    f"Arguments prov and state are deprecated, use subdiv='{prov_state}' instead.",
                    DeprecationWarning,
                )

            # Deprecated subdivisions.
            if subdiv in self._deprecated_subdivisions:
                warnings.warn(
                    "This subdivision is deprecated and will be removed after "
                    "Dec, 1 2023. The list of supported subdivisions: "
                    f"{', '.join(sorted(self.subdivisions))}; "
                    "the list of supported subdivisions aliases: "
                    f"{', '.join(sorted(subdivision_aliases))}.",
                    DeprecationWarning,
                )

        # Special holidays validation.
        if (has_substituted_holidays := getattr(self, "has_substituted_holidays", False)) and (
            not getattr(self, "substituted_label", None)
            or not getattr(self, "substituted_date_format", None)
        ):
            raise ValueError(
                f"Entity `{self._entity_code}` class must have `substituted_label` "
                "and `substituted_date_format` attributes set."
            )

        self.categories = categories
        self.expand = expand
        self.has_special_holidays = getattr(self, "has_special_holidays", False)
        self.has_substituted_holidays = has_substituted_holidays
        self.language = language
        self.observed = observed
        self.subdiv = subdiv
        self.weekend_workdays = getattr(self, "weekend_workdays", set())
        self.years = _normalize_arguments(int, years)

        # Configure l10n related attributes.
        self._init_translation()

        # Populate holidays.
        for year in self.years:
            self._populate(year)

    def __add__(self, other: Union[int, "HolidayBase", "HolidaySum"]) -> "HolidayBase":
        """Add another dictionary of public holidays creating a
        [HolidaySum][holidays.holiday_base.HolidaySum] object.

        Args:
            other:
                The dictionary of public holiday to be added.

        Returns:
            A `HolidayBase` object unless the other object cannot be added, then `self`.
        """
        if isinstance(other, int) and other == 0:
            # Required to sum() list of holidays
            # sum([h1, h2]) is equivalent to (0 + h1 + h2).
            return self

        if not isinstance(other, (HolidayBase, HolidaySum)):
            raise TypeError("Holiday objects can only be added with other Holiday objects")

        return HolidaySum(self, other)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __contains__(self, key: object) -> bool:
        """Check if a given date is a holiday.

        The method supports the following input types:

        * `datetime.date`
        * `datetime.datetime`
        * `float` or `int` (Unix timestamp)
        * `str` of any format recognized by `dateutil.parser.parse()`

        Args:
            key:
                The date to check.

        Returns:
            `True` if the date is a holiday, `False` otherwise.
        """

        if not isinstance(key, (date, datetime, float, int, str)):
            raise TypeError(f"Cannot convert type '{type(key)}' to date.")

        return dict.__contains__(cast("dict[Any, Any]", self), self.__keytransform__(key))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HolidayBase):
            return False

        for attribute_name in self.__attribute_names:
            if getattr(self, attribute_name, None) != getattr(other, attribute_name, None):
                return False

        return dict.__eq__(cast("dict[Any, Any]", self), other)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError as e:
            # This part is responsible for _add_holiday_* syntactic sugar support.
            add_holiday_prefix = "_add_holiday_"
            # Raise early if prefix doesn't match to avoid patterns checks.
            if name[: len(add_holiday_prefix)] != add_holiday_prefix:
                raise e

            tokens = name.split("_")

            # Handle <month> <day> patterns (e.g., _add_holiday_jun_15()).
            if len(tokens) == 5:
                *_, month, day = tokens
                if month in MONTHS and day in DAYS:
                    return lambda name: self._add_holiday(
                        name, date(self._year, MONTHS[month], int(day))
                    )

            elif len(tokens) == 7:
                # Handle <last/nth> <weekday> of <month> patterns (e.g.,
                # _add_holiday_last_mon_of_aug() or _add_holiday_3rd_fri_of_aug()).
                *_, number, weekday, of, month = tokens
                if (
                    of == "of"
                    and (number == "last" or number[0].isdigit())
                    and month in MONTHS
                    and weekday in WEEKDAYS
                ):
                    return lambda name: self._add_holiday(
                        name,
                        _get_nth_weekday_of_month(
                            -1 if number == "last" else int(number[0]),
                            WEEKDAYS[weekday],
                            MONTHS[month],
                            self._year,
                        ),
                    )

                # Handle <n> days <past/prior> easter patterns (e.g.,
                # _add_holiday_8_days_past_easter() or
                # _add_holiday_5_days_prior_easter()).
                *_, days, unit, delta_direction, easter = tokens
                if (
                    unit in {"day", "days"}
                    and delta_direction in {"past", "prior"}
                    and easter == "easter"
                    and len(days) < 3
                    and days.isdigit()
                ):
                    return lambda name: self._add_holiday(
                        name,
                        _timedelta(
                            self._easter_sunday,
                            +int(days) if delta_direction == "past" else -int(days),
                        ),
                    )

            # Handle <n> day(s) <past/prior> <last/<nth> <weekday> of <month> patterns (e.g.,
            # _add_holiday_1_day_past_1st_fri_of_aug() or
            # _add_holiday_5_days_prior_last_fri_of_aug()).
            elif len(tokens) == 10:
                *_, days, unit, delta_direction, number, weekday, of, month = tokens
                if (
                    unit in {"day", "days"}
                    and delta_direction in {"past", "prior"}
                    and of == "of"
                    and len(days) < 3
                    and days.isdigit()
                    and (number == "last" or number[0].isdigit())
                    and month in MONTHS
                    and weekday in WEEKDAYS
                ):
                    return lambda name: self._add_holiday(
                        name,
                        _timedelta(
                            _get_nth_weekday_of_month(
                                -1 if number == "last" else int(number[0]),
                                WEEKDAYS[weekday],
                                MONTHS[month],
                                self._year,
                            ),
                            +int(days) if delta_direction == "past" else -int(days),
                        ),
                    )

            # Handle <nth> <weekday> <before/from> <month> <day> patterns (e.g.,
            # _add_holiday_1st_mon_before_jun_15() or _add_holiday_1st_mon_from_jun_15()).
            elif len(tokens) == 8:
                *_, number, weekday, date_direction, month, day = tokens
                if (
                    date_direction in {"before", "from"}
                    and number[0].isdigit()
                    and month in MONTHS
                    and weekday in WEEKDAYS
                    and day in DAYS
                ):
                    return lambda name: self._add_holiday(
                        name,
                        _get_nth_weekday_from(
                            -int(number[0]) if date_direction == "before" else +int(number[0]),
                            WEEKDAYS[weekday],
                            date(self._year, MONTHS[month], int(day)),
                        ),
                    )

            raise e  # No match.

    def __getitem__(self, key: DateLike) -> Any:
        if isinstance(key, slice):
            if not key.start or not key.stop:
                raise ValueError("Both start and stop must be given.")

            start = self.__keytransform__(key.start)
            stop = self.__keytransform__(key.stop)

            if key.step is None:
                step = 1
            elif isinstance(key.step, int):
                step = key.step
            elif isinstance(key.step, timedelta):
                step = key.step.days
            else:
                raise TypeError(f"Cannot convert type '{type(key.step)}' to int.")

            if step == 0:
                raise ValueError("Step value must not be zero.")

            diff_days = (stop - start).days
            if diff_days < 0 <= step or diff_days >= 0 > step:
                step = -step

            return [
                day
                for delta_days in range(0, diff_days, step)
                if (day := _timedelta(start, delta_days)) in self
            ]

        return dict.__getitem__(self, self.__keytransform__(key))

    def __getstate__(self) -> dict[str, Any]:
        """Return the object's state for serialization."""
        state = self.__dict__.copy()
        state.pop("tr", None)
        return state

    def __keytransform__(self, key: DateLike) -> date:
        """Convert various date-like formats to `datetime.date`.

        The method supports the following input types:

        * `datetime.date`
        * `datetime.datetime`
        * `float` or `int` (Unix timestamp)
        * `str` of any format recognized by `dateutil.parser.parse()`

        Args:
            key:
                The date-like object to convert.

        Returns:
            The corresponding `datetime.date` representation.
        """

        dt: date | None = None
        # Try to catch `date` and `str` type keys first.
        # Using type() here to skip date subclasses.
        # Key is `date`.
        if type(key) is date:
            dt = key

        # Key is `str` instance.
        elif isinstance(key, str):
            # key possibly contains a date in YYYY-MM-DD or YYYYMMDD format.
            if len(key) in {8, 10}:
                try:
                    dt = date.fromisoformat(key)
                except ValueError:
                    pass
            if dt is None:
                try:
                    dt = parse(key).date()
                except (OverflowError, ValueError):
                    raise ValueError(f"Cannot parse date from string '{key}'")

        # Key is `datetime` instance.
        elif isinstance(key, datetime):
            dt = key.date()

        # Must go after the `isinstance(key, datetime)` check as datetime is `date` subclass.
        elif isinstance(key, date):
            dt = key

        # Key is `float` or `int` instance.
        elif isinstance(key, (float, int)):
            dt = datetime.fromtimestamp(key, timezone.utc).date()

        # Key is not supported.
        else:
            raise TypeError(f"Cannot convert type '{type(key)}' to date.")

        # Automatically expand for `expand=True` cases.
        if self.expand and dt.year not in self.years:
            self.years.add(dt.year)
            self._populate(dt.year)

        return dt

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, HolidayBase):
            return True

        for attribute_name in self.__attribute_names:
            if getattr(self, attribute_name, None) != getattr(other, attribute_name, None):
                return True

        return dict.__ne__(self, other)

    def __radd__(self, other: Any) -> "HolidayBase":
        return self.__add__(other)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return super().__reduce__()

    def __repr__(self) -> str:
        if self:
            return super().__repr__()

        if hasattr(self, "market"):
            return f"holidays.financial_holidays({self.market!r})"
        elif hasattr(self, "country"):
            if self.subdiv:
                return f"holidays.country_holidays({self.country!r}, subdiv={self.subdiv!r})"
            return f"holidays.country_holidays({self.country!r})"

        return "holidays.HolidayBase()"

    def __setattr__(self, key: str, value: Any) -> None:
        dict.__setattr__(self, key, value)

        if self and key in {"categories", "observed"}:
            self.clear()
            for year in self.years:  # Re-populate holidays for each year.
                self._populate(year)

    def __setitem__(self, key: DateLike, value: str) -> None:
        if key in self:
            # If there are multiple holidays on the same date
            # order their names alphabetically.
            holiday_names = set(self[key].split(HOLIDAY_NAME_DELIMITER))
            holiday_names.update(value.split(HOLIDAY_NAME_DELIMITER))
            value = HOLIDAY_NAME_DELIMITER.join(sorted(holiday_names))

        dict.__setitem__(self, self.__keytransform__(key), value)

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore the object's state after deserialization."""
        self.__dict__.update(state)
        self._init_translation()

    def __str__(self) -> str:
        if self:
            return super().__str__()

        parts = (
            f"'{attribute_name}': {getattr(self, attribute_name, None)}"
            for attribute_name in self.__attribute_names
        )

        return f"{{{', '.join(parts)}}}"

    @property
    def __attribute_names(self):
        return ("country", "expand", "language", "market", "observed", "subdiv", "years")

    @cached_property
    def _entity_code(self):
        return getattr(self, "country", getattr(self, "market", None))

    @cached_property
    def _normalized_subdiv(self):
        return (
            self.subdivisions_aliases.get(self.subdiv, self.subdiv)
            .translate(
                str.maketrans(
                    {
                        "-": "_",
                        " ": "_",
                    }
                )
            )
            .lower()
        )

    @property
    def _sorted_categories(self):
        return (
            [self.default_category] + sorted(self.categories - {self.default_category})
            if self.default_category in self.categories
            else sorted(self.categories)
        )

    @classmethod
    def get_subdivision_aliases(cls) -> dict[str, list]:
        """Get subdivision aliases.

        Returns:
            A dictionary mapping subdivision aliases to their official ISO 3166-2 codes.
        """
        subdivision_aliases: dict[str, list[str]] = {s: [] for s in cls.subdivisions}
        for alias, subdivision in cls.subdivisions_aliases.items():
            subdivision_aliases[subdivision].append(alias)

        return subdivision_aliases

    def _init_translation(self) -> None:
        """Initialize translation function based on language settings."""
        supported_languages = set(self.supported_languages)
        if self._entity_code is not None:
            fallback = self.language not in supported_languages
            languages = [self.language] if self.language in supported_languages else None
            locale_directory = str(Path(__file__).with_name("locale"))

            # Add entity native content translations.
            entity_translation = translation(
                self._entity_code,
                fallback=fallback,
                languages=languages,
                localedir=locale_directory,
            )
            # Add a fallback if entity has parent translations.
            if parent_entity := self.parent_entity:
                entity_translation.add_fallback(
                    translation(
                        getattr(parent_entity, "country", None)
                        or getattr(parent_entity, "market", None),  # type: ignore[arg-type]
                        fallback=fallback,
                        languages=languages,
                        localedir=locale_directory,
                    )
                )
            self.tr = entity_translation.gettext
        else:
            self.tr = gettext

    def _is_leap_year(self) -> bool:
        """Returns True if the year is leap. Returns False otherwise."""
        return isleap(self._year)

    def _add_holiday(self, name: str, *args) -> date | None:
        """Add a holiday."""
        if not args:
            raise TypeError("Incorrect number of arguments.")

        dt = args if len(args) > 1 else args[0]
        dt = dt if isinstance(dt, date) else date(self._year, *dt)

        if dt.year != self._year:
            return None

        self[dt] = self.tr(name)
        return dt

    def _add_multiday_holiday(
        self, start_date: date, duration_days: int, *, name: str | None = None
    ) -> set[date]:
        """Add a multi-day holiday.

        Args:
            start_date:
                First day of the holiday.

            duration_days:
                Number of additional days to add.

            name:
                Optional holiday name; inferred from `start_date` if omitted.

        Returns:
            A set of all added holiday dates.

        Raises:
            ValueError:
                If the holiday name cannot be inferred from `start_date`.
        """
        if (holiday_name := name or self.get(start_date)) is None:
            raise ValueError(f"Cannot infer holiday name for date {start_date!r}.")

        return {
            d
            for delta in range(1, duration_days + 1)
            if (d := self._add_holiday(holiday_name, _timedelta(start_date, delta)))
        }

    def _add_special_holidays(self, mapping_names, *, observed=False):
        """Add special holidays."""
        for mapping_name in mapping_names:
            for data in _normalize_tuple(getattr(self, mapping_name, {}).get(self._year, ())):
                if len(data) == 3:  # Special holidays.
                    month, day, name = data
                    self._add_holiday(
                        self.tr(self.observed_label) % self.tr(name)
                        if observed
                        else self.tr(name),
                        month,
                        day,
                    )
                else:  # Substituted holidays.
                    to_month, to_day, from_month, from_day, *optional = data
                    from_date = date(optional[0] if optional else self._year, from_month, from_day)
                    self._add_holiday(
                        self.tr(self.substituted_label)
                        % from_date.strftime(self.tr(self.substituted_date_format)),
                        to_month,
                        to_day,
                    )
                    self.weekend_workdays.add(from_date)

    def _check_weekday(self, weekday: int, *args) -> bool:
        """
        Returns True if `weekday` equals to the date's week day.
        Returns False otherwise.
        """
        dt = args if len(args) > 1 else args[0]
        dt = dt if isinstance(dt, date) else date(self._year, *dt)
        return dt.weekday() == weekday

    def _get_weekend(self, dt: date) -> set[int]:
        return self.weekend

    def _is_monday(self, *args) -> bool:
        return self._check_weekday(MON, *args)

    def _is_tuesday(self, *args) -> bool:
        return self._check_weekday(TUE, *args)

    def _is_wednesday(self, *args) -> bool:
        return self._check_weekday(WED, *args)

    def _is_thursday(self, *args) -> bool:
        return self._check_weekday(THU, *args)

    def _is_friday(self, *args) -> bool:
        return self._check_weekday(FRI, *args)

    def _is_saturday(self, *args) -> bool:
        return self._check_weekday(SAT, *args)

    def _is_sunday(self, *args) -> bool:
        return self._check_weekday(SUN, *args)

    def _is_weekday(self, *args) -> bool:
        """
        Returns True if date's week day is not a weekend day.
        Returns False otherwise.
        """
        return not self._is_weekend(*args)

    def _is_weekend(self, *args) -> bool:
        """
        Returns True if date's week day is a weekend day.
        Returns False otherwise.
        """
        dt = args if len(args) > 1 else args[0]
        dt = dt if isinstance(dt, date) else date(self._year, *dt)
        return dt.weekday() in self._get_weekend(dt)

    def _populate(self, year: int) -> None:
        """This is a private method that populates (generates and adds) holidays
        for a given year. To keep things fast, it assumes that no holidays for
        the year have already been populated. It is required to be called
        internally by any country `populate()` method, while should not be called
        directly from outside.
        To add holidays to an object, use the [update()][holidays.holiday_base.HolidayBase.update]
        method.

        Args:
            year: The year to populate with holidays.

            >>> from holidays import country_holidays
            >>> us_holidays = country_holidays('US', years=2020)
            # to add new holidays to the object:
            >>> us_holidays.update(country_holidays('US', years=2021))
        """

        if year < self.start_year or year > self.end_year:
            return None

        self._year = year
        self._populate_common_holidays()
        self._populate_subdiv_holidays()

    def _populate_common_holidays(self):
        """Populate entity common holidays."""
        for category in self._sorted_categories:
            if pch_method := getattr(self, f"_populate_{category.lower()}_holidays", None):
                pch_method()

        if self.has_special_holidays:
            self._add_special_holidays(
                f"special_{category}_holidays" for category in self._sorted_categories
            )

    def _populate_subdiv_holidays(self):
        """Populate entity subdivision holidays."""
        if self.subdiv is None:
            return None

        for category in self._sorted_categories:
            if asch_method := getattr(
                self,
                f"_populate_subdiv_{self._normalized_subdiv}_{category.lower()}_holidays",
                None,
            ):
                asch_method()

        if self.has_special_holidays:
            self._add_special_holidays(
                f"special_{self._normalized_subdiv}_{category.lower()}_holidays"
                for category in self._sorted_categories
            )

    def append(self, *args: dict[DateLike, str] | list[DateLike] | DateLike) -> None:
        """Alias for [update()][holidays.holiday_base.HolidayBase.update] to mimic list type.

        Args:
            args:
                Holiday data to add. Can be:

                * A dictionary mapping dates to holiday names.
                * A list of dates (without names).
                * A single date.
        """
        return self.update(*args)

    def copy(self):
        """Return a copy of the object."""
        return copy.copy(self)

    def get(self, key: DateLike, default: str | Any = None) -> str | Any:
        """Retrieve the holiday name(s) for a given date.

        If the date is a holiday, returns the holiday name as a string.
        If multiple holidays fall on the same date, their names are joined by a semicolon (`;`).
        If the date is not a holiday, returns the provided `default` value (defaults to `None`).

        Args:
            key:
                The date expressed in one of the following types:

                * `datetime.date`
                * `datetime.datetime`
                * `float` or `int` (Unix timestamp)
                * `str` of any format recognized by `dateutil.parser.parse()`

            default:
                The default value to return if no value is found.

        Returns:
            The holiday name(s) as a string if the date is a holiday,
                or the `default` value otherwise.
        """
        return dict.get(self, self.__keytransform__(key), default)

    def get_list(self, key: DateLike) -> list[str]:
        """Retrieve all holiday names for a given date.

        Args:
            key:
                The date expressed in one of the following types:

                * `datetime.date`
                * `datetime.datetime`
                * `float` or `int` (Unix timestamp)
                * `str` of any format recognized by `dateutil.parser.parse()`

        Returns:
            A list of holiday names if the date is a holiday, otherwise an empty list.
        """
        return [name for name in self.get(key, "").split(HOLIDAY_NAME_DELIMITER) if name]

    def get_named(
        self,
        holiday_name: str,
        lookup: NameLookup = "icontains",
        split_multiple_names: bool = True,
    ) -> list[date]:
        """Find all holiday dates matching a given name.

        The search by default is case-insensitive and includes partial matches.

        Args:
            holiday_name:
                The holiday's name to try to match.

            lookup:
                The holiday name lookup type:

                * contains - case sensitive contains match;
                * exact - case sensitive exact match;
                * startswith - case sensitive starts with match;
                * icontains - case insensitive contains match;
                * iexact - case insensitive exact match;
                * istartswith - case insensitive starts with match;

            split_multiple_names:
                Either use the exact name for each date or split it by holiday
                name delimiter.

        Returns:
            A list of all holiday dates matching the provided holiday name.
        """
        holiday_name_dates = (
            ((k, name) for k, v in self.items() for name in v.split(HOLIDAY_NAME_DELIMITER))
            if split_multiple_names
            else ((k, v) for k, v in self.items())
        )

        if lookup == "icontains":
            holiday_name_lower = holiday_name.lower()
            return [dt for dt, name in holiday_name_dates if holiday_name_lower in name.lower()]
        elif lookup == "exact":
            return [dt for dt, name in holiday_name_dates if holiday_name == name]
        elif lookup == "contains":
            return [dt for dt, name in holiday_name_dates if holiday_name in name]
        elif lookup == "startswith":
            return [
                dt for dt, name in holiday_name_dates if holiday_name == name[: len(holiday_name)]
            ]
        elif lookup == "iexact":
            holiday_name_lower = holiday_name.lower()
            return [dt for dt, name in holiday_name_dates if holiday_name_lower == name.lower()]
        elif lookup == "istartswith":
            holiday_name_lower = holiday_name.lower()
            return [
                dt
                for dt, name in holiday_name_dates
                if holiday_name_lower == name[: len(holiday_name)].lower()
            ]

        raise AttributeError(f"Unknown lookup type: {lookup}")

    def get_closest_holiday(
        self,
        target_date: DateLike | None = None,
        direction: Literal["forward", "backward"] = "forward",
    ) -> tuple[date, str] | None:
        """Find the closest holiday relative to a given date.

        If `direction` is "forward", returns the next holiday after `target_date`.
        If `direction` is "backward", returns the previous holiday before `target_date`.
        If `target_date` is not provided, the current date is used.

        Args:
            target_date:
                The reference date. If None, defaults to today.

            direction:
                Search direction, either "forward" (next holiday) or
                "backward" (previous holiday).

        Returns:
            A tuple containing the holiday date and its name, or None if no holiday is found.
        """
        if direction not in {"backward", "forward"}:
            raise AttributeError(f"Unknown direction: {direction}")

        dt = self.__keytransform__(target_date or datetime.now().date())
        if direction == "forward" and (next_year := dt.year + 1) not in self.years:
            self._populate(next_year)
        elif direction == "backward" and (previous_year := dt.year - 1) not in self.years:
            self._populate(previous_year)

        sorted_dates = sorted(self.keys())
        position = (
            bisect_right(sorted_dates, dt)
            if direction == "forward"
            else bisect_left(sorted_dates, dt) - 1
        )
        if 0 <= position < len(sorted_dates):
            dt = sorted_dates[position]
            return dt, self[dt]

        return None

    def get_nth_working_day(self, key: DateLike, n: int) -> date:
        """Find the n-th working day from a given date.

        Moves forward if n is positive, or backward if n is negative.
        If n is 0, returns the given date if it is a working day; otherwise the next working day.

        Args:
            key:
                The starting date.

            n:
                The number of working days to move. Positive values move forward,
                negative values move backward.

        Returns:
            The calculated working day after shifting by n working days.
        """
        direction = +1 if n >= 0 else -1
        dt = self.__keytransform__(key)
        for _ in range(abs(n) or 1):
            if n:
                dt = _timedelta(dt, direction)
            while not self.is_working_day(dt):
                dt = _timedelta(dt, direction)
        return dt

    def get_working_days_count(self, start: DateLike, end: DateLike) -> int:
        """Calculate the number of working days between two dates.

        The date range works in a closed interval fashion [start, end] so both
        endpoints are included.

        Args:
            start:
                The range start date.

            end:
                The range end date.

        Returns:
            The total count of working days between the given dates.
        """
        dt1 = self.__keytransform__(start)
        dt2 = self.__keytransform__(end)
        if dt1 > dt2:
            dt1, dt2 = dt2, dt1
        days = (dt2 - dt1).days + 1
        return sum(self.is_working_day(_timedelta(dt1, n)) for n in range(days))

    def is_weekend(self, key: DateLike) -> bool:
        """Check if the given date's week day is a weekend day.

        Args:
            key:
                The date to check.

        Returns:
            True if the date's week day is a weekend day, False otherwise.
        """
        return self._is_weekend(self.__keytransform__(key))

    def is_working_day(self, key: DateLike) -> bool:
        """Check if the given date is considered a working day.

        Args:
            key:
                The date to check.

        Returns:
            True if the date is a working day, False if it is a holiday or weekend.
        """
        dt = self.__keytransform__(key)
        return dt in self.weekend_workdays if self._is_weekend(dt) else dt not in self

    def pop(self, key: DateLike, default: str | Any = None) -> str | Any:
        """Remove a holiday for a given date and return its name.

        If the specified date is a holiday, it will be removed, and its name will
        be returned. If the date is not a holiday, the provided `default` value
        will be returned instead.

        Args:
            key:
                The date expressed in one of the following types:

                * `datetime.date`
                * `datetime.datetime`
                * `float` or `int` (Unix timestamp)
                * `str` of any format recognized by `dateutil.parser.parse()`

            default:
                The default value to return if no match is found.

        Returns:
            The name of the removed holiday if the date was a holiday, otherwise
                the provided `default` value.

        Raises:
            KeyError: if date is not a holiday and default is not given.
        """
        if default is None:
            return dict.pop(self, self.__keytransform__(key))

        return dict.pop(self, self.__keytransform__(key), default)

    def pop_named(self, holiday_name: str, lookup: NameLookup = "icontains") -> list[date]:
        """Remove all holidays matching the given name.

        This method removes all dates associated with a holiday name, so they are
        no longer considered holidays. The search by default is case-insensitive and
        includes partial matches.

        Args:
            holiday_name:
                The holiday's name to try to match.

            lookup:
                The holiday name lookup type:

                * contains - case sensitive contains match;
                * exact - case sensitive exact match;
                * startswith - case sensitive starts with match;
                * icontains - case insensitive contains match;
                * iexact - case insensitive exact match;
                * istartswith - case insensitive starts with match;

        Returns:
            A list of dates removed.

        Raises:
            KeyError: if date is not a holiday.
        """
        use_exact_name = HOLIDAY_NAME_DELIMITER in holiday_name
        if not (
            dts := self.get_named(
                holiday_name, lookup=lookup, split_multiple_names=not use_exact_name
            )
        ):
            raise KeyError(holiday_name)

        popped = []
        for dt in dts:
            holiday_names = self[dt].split(HOLIDAY_NAME_DELIMITER)
            self.pop(dt)
            popped.append(dt)

            # Keep the rest of holidays falling on the same date.
            if use_exact_name:
                continue
            if lookup == "icontains":
                holiday_name_lower = holiday_name.lower()
                holiday_names = [
                    name for name in holiday_names if holiday_name_lower not in name.lower()
                ]
            elif lookup == "iexact":
                holiday_name_lower = holiday_name.lower()
                holiday_names = [
                    name for name in holiday_names if holiday_name_lower != name.lower()
                ]
            elif lookup == "istartswith":
                holiday_name_lower = holiday_name.lower()
                holiday_names = [
                    name
                    for name in holiday_names
                    if holiday_name_lower != name[: len(holiday_name)].lower()
                ]
            elif lookup == "contains":
                holiday_names = [name for name in holiday_names if holiday_name not in name]
            elif lookup == "exact":
                holiday_names = [name for name in holiday_names if holiday_name != name]
            else:  # startswith
                holiday_names = [
                    name for name in holiday_names if holiday_name != name[: len(holiday_name)]
                ]
            if holiday_names:
                self[dt] = HOLIDAY_NAME_DELIMITER.join(holiday_names)

        return popped

    def update(  # type: ignore[override]
        self, *args: dict[DateLike, str] | list[DateLike] | DateLike
    ) -> None:
        """Update the object, overwriting existing dates.

        Args:
            args:
                Either another dictionary object where keys are dates and values
                are holiday names, or a single date (or a list of dates) for which
                the value will be set to "Holiday".

                Dates can be expressed in one or more of the following types:

                * `datetime.date`
                * `datetime.datetime`
                * `float` or `int` (Unix timestamp)
                * `str` of any format recognized by `dateutil.parser.parse()`
        """
        for arg in args:
            if isinstance(arg, dict):
                for key, value in arg.items():
                    self[key] = value
            elif isinstance(arg, list):
                for item in arg:
                    self[item] = "Holiday"
            else:
                self[arg] = "Holiday"


class HolidaySum(HolidayBase):
    """
    Combine multiple holiday collections into a single dictionary-like object.

    This class represents the sum of two or more `HolidayBase` instances.
    The resulting object behaves like a dictionary mapping dates to holiday
    names, with the following behaviors:

    * The `holidays` attribute stores the original holiday collections as a list.
    * The `country` and `subdiv` attributes are combined from all operands and
      may become lists.
    * If multiple holidays fall on the same date, their names are merged.
    * Holidays are generated (expanded) for all years included in the operands.
    """

    country: str | list[str]  # type: ignore[assignment]
    """Countries included in the addition."""
    market: str | list[str]  # type: ignore[assignment]
    """Markets included in the addition."""
    subdiv: str | list[str] | None  # type: ignore[assignment]
    """Subdivisions included in the addition."""
    holidays: list[HolidayBase]
    """The original HolidayBase objects included in the addition."""
    years: set[int]
    """The years calculated."""

    def __init__(
        self, h1: Union[HolidayBase, "HolidaySum"], h2: Union[HolidayBase, "HolidaySum"]
    ) -> None:
        """
        Args:
            h1:
                The first HolidayBase object to add.

            h2:
                The other HolidayBase object to add.

        Example:

            >>> from holidays import country_holidays
            >>> nafta_holidays = country_holidays('US', years=2020) + \
    country_holidays('CA') + country_holidays('MX')
            >>> dates = sorted(nafta_holidays.items(), key=lambda x: x[0])
            >>> from pprint import pprint
            >>> pprint(dates[:10], width=72)
            [(datetime.date(2020, 1, 1), "Año Nuevo; New Year's Day"),
             (datetime.date(2020, 1, 20), 'Martin Luther King Jr. Day'),
             (datetime.date(2020, 2, 3), 'Día de la Constitución'),
             (datetime.date(2020, 2, 17), "Washington's Birthday"),
             (datetime.date(2020, 3, 16), 'Natalicio de Benito Juárez'),
             (datetime.date(2020, 4, 10), 'Good Friday'),
             (datetime.date(2020, 5, 1), 'Día del Trabajo'),
             (datetime.date(2020, 5, 25), 'Memorial Day'),
             (datetime.date(2020, 7, 1), 'Canada Day'),
             (datetime.date(2020, 7, 3), 'Independence Day (observed)')]
        """
        # Store originals in the holidays attribute.
        self.holidays = []
        for operand in (h1, h2):
            if isinstance(operand, HolidaySum):
                self.holidays.extend(operand.holidays)
            else:
                self.holidays.append(operand)

        # Join years, expand and observed.
        kwargs: dict[str, Any] = {
            "expand": h1.expand or h2.expand,
            "observed": h1.observed or h2.observed,
            "years": h1.years | h2.years,
        }
        # Join country and subdivisions data.
        # TODO: this way makes no sense: joining Italy Catania (IT, CA) with
        # USA Mississippi (US, MS) and USA Michigan (US, MI) yields
        # country=["IT", "US"] and subdiv=["CA", "MS", "MI"], which could very
        # well be California and Messina and Milano, or Catania, Mississippi
        # and Milano, or ... you get the picture.
        # Same goes when countries and markets are being mixed (working, yet
        # still nonsensical).
        value: str | list[str] | None
        for attr in ("country", "market", "subdiv"):
            a1 = getattr(h1, attr, None)
            a2 = getattr(h2, attr, None)
            if a1 and a2 and a1 != a2:
                a1 = a1 if isinstance(a1, list) else [a1]
                a2 = a2 if isinstance(a2, list) else [a2]
                value = a1 + a2
            else:
                value = a1 or a2

            if attr == "subdiv":
                kwargs[attr] = value
            else:
                setattr(self, attr, value)

        # Retain language if they match and are strings.
        # If language wasn't assigned, default_language acts as fallback.
        h1_language = h1.language or h1.default_language
        h2_language = h2.language or h2.default_language
        if isinstance(h1_language, str) and h1_language == h2_language:
            kwargs["language"] = h1_language

        HolidayBase.__init__(self, **kwargs)

        # supported_languages is used for iCalExporter language check as well.
        self.supported_languages = (h1_language,) if h1_language else ()

    def _populate(self, year):
        for operand in self.holidays:
            operand._populate(year)
            self.update(cast("dict[DateLike, str]", operand))
