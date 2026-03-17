from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass


@dataclass(frozen=True)
class Sequence:
    start: int | None = None
    stop: int | None = None
    step: int = 1
    nth: int | None = None

    def is_star(self) -> bool:
        """Return True if this sequence describes a wildcard."""
        return self.start is None and self.step == 1

    def is_single(self) -> bool:
        """Return True if this sequence describes a a single, specific value."""
        return self.start is not None and self.start == self.stop


def join(l: list[str]) -> str:
    """Join together strings using commas and 'and' as separators.

    >>> join(["a"])
    'a'
    >>> join(["a", "b"])
    'a and b'
    >>> join(["a", "b", "c"])
    'a, b, and c'
    """
    if len(l) == 1:
        return l[0]

    if len(l) == 2:
        return f"{l[0]} and {l[1]}"

    head = ", ".join(l[:-1])
    return f"{head}, and {l[-1]}"


ORDINALS = {
    -1: "last",
    1: "first",
    2: "second",
    3: "third",
    4: "fourth",
    5: "fifth",
    6: "sixth",
    7: "seventh",
    8: "eighth",
    9: "ninth",
}


def ordinal(x: int) -> str:
    """Format integer as an ordinal number.

    >>> ordinal(1)
    'first'
    >>> ordinal(15)
    '15th'
    """
    return ORDINALS.get(x, f"{x}th")


def format_time(h: int, m: int, s: int | None = None) -> str:
    """Format time as HH:MM or HH:MM:SS (if seconds are specified).

    >>> format_time(0, 0)
    '00:00'
    >>> format_time(1, 23)
    '01:23'
    >>> format_time(11, 22, 33)
    '11:22:33'
    """
    if s is None:
        return f"{h:02d}:{m:02d}"

    return f"{h:02d}:{m:02d}:{s:02d}"


class Field:
    name = "FIXME"
    symbolic: list[str] = []
    min_value = 0
    max_value = 0

    def __init__(self, value: str):
        self.value = value
        self.parsed: list[Sequence] = []

        for seq in self.parse(value):
            if seq not in self.parsed:
                self.parsed.append(seq)

        # If the field contains a single numeric value,
        # store it in self.single_value
        self.single_value = self.parsed[0].start
        for seq in self.parsed:
            if seq.start != self.single_value or seq.stop != self.single_value:
                self.single_value = None
                break

        # Does the field have a single value "0"?
        self.zero = self.single_value == 0
        # Does the field cover the full range?
        self.star = all(seq.is_star() for seq in self.parsed)
        # Are there any single values?
        self.any_singles = any(seq.is_single() for seq in self.parsed)
        # Are all values single values?
        self.all_singles = all(seq.is_single() for seq in self.parsed)

    def parse(self, value: str) -> Generator[Sequence, None, None]:
        """Parse a single field of a cron expression into Sequence objects."""
        for term in value.split(","):
            if term == "*":
                yield Sequence()
            elif isinstance(self, Weekday) and term.endswith("L"):
                v = self._int(term[:-1])
                yield Sequence(start=v, stop=v, nth=-1)
            elif "#" in term:
                term, nth = term.split("#")
                v = self._int(term)
                yield Sequence(start=v, stop=v, nth=int(nth))
            elif "/" in term:
                term, step_str = term.split("/")
                step = int(step_str)
                if term == "*":
                    yield Sequence(step=step)
                elif term == "LW":
                    yield Sequence(start=-2, stop=-2, nth=-1)
                elif term == "L":
                    yield Sequence(start=-1, stop=-1, nth=-1)
                else:
                    if "-" in term:
                        start_str, stop_str = term.split("-")
                        start, stop = self._int(start_str), self._int(stop_str)
                    else:
                        start = self._int(term)
                        stop = self.max_value

                    if start <= self.min_value and stop >= self.max_value:
                        yield Sequence(step=step)
                    else:
                        yield Sequence(start=start, stop=stop, step=step)
            elif "-" in term:
                start_str, stop_str = term.split("-")
                start, stop = self._int(start_str), self._int(stop_str)
                if start + 1 == stop:
                    # treat a 2-long sequence as two single values:
                    yield Sequence(start=start, stop=start)
                    yield Sequence(start=stop, stop=stop)
                elif start <= self.min_value and stop >= self.max_value:
                    yield Sequence()
                else:
                    yield Sequence(start=start, stop=stop)
            elif term == "LW":
                yield Sequence(start=-2, stop=-2, nth=-1)
            elif term == "L":
                yield Sequence(start=-1, stop=-1, nth=-1)
            else:
                v = self._int(term)
                yield Sequence(start=v, stop=v)

    def _int(self, value: str) -> int:
        """Convert a value from a cron expression to an integer.

        For month and weekday fields, this takes care of converting JAN, FEB, ...
        and MON, TUE, ...
        """
        if value in self.symbolic:
            return self.symbolic.index(value)
        return int(value)

    def singles(self) -> list[int]:
        """Return a list only the single values in this field."""
        return [seq.start for seq in self.parsed if isinstance(seq.start, int)]

    def label(self, idx: int) -> str:
        """Convert an integer value to a string for display.

        >>> label(1)
        '1'
        """
        return str(idx)

    def format_single(self, value: int) -> str:
        """Format a single value for display.

        >>> format_single(1)
        'minute 1'
        """
        return f"{self.name} {self.label(value)}"

    def format_nth(self, value: int, nth: int) -> str:
        """Format nth-weekday-of-month and L values.

        Implemented in Month and Weekday subclasses.
        """
        raise NotImplementedError

    def format_every(self, step: int = 1) -> str:
        """Format wildcard and wildcard-with-step values.

        >>> format_every(1)
        'every minute'

        >>> format_every(5)
        'every 5th minute'
        """
        if step == 1:
            return f"every {self.name}"
        return f"every {ordinal(step)} {self.name}"

    def format_seq(self, start: int, stop: int, step: int = 1) -> str:
        """Format a sequence.

        >>> format_seq(1, 10)
        'every minute from 1 through 10'

        >>> format_seq(1, 10, 2)
        'every 2nd minute from 1 through 10'
        """
        start_str = self.label(start)
        stop_str = self.label(stop)
        if step == 1:
            return f"every {self.name} from {start_str} through {stop_str}"
        return f"every {ordinal(step)} {self.name} from {start_str} through {stop_str}"

    def format(self) -> str:
        """Format every component of this field, and join them together."""
        parts = []
        for seq in self.parsed:
            if seq.start is None:
                parts.append(self.format_every(seq.step))
            elif seq.stop != seq.start:
                assert seq.stop is not None
                parts.append(self.format_seq(seq.start, seq.stop, seq.step))
            elif seq.nth is not None:
                parts.append(self.format_nth(seq.start, seq.nth))
            else:
                parts.append(self.format_single(seq.start))

        return join(parts)

    def __str__(self) -> str:
        """Return a human-friendly representation of this field.

        Subclasses can override this method to add prepositions ("at", "on", "in").
        """
        return self.format()


class Second(Field):
    name = "second"
    min_value = 0
    max_value = 59

    def format(self) -> str:
        """Format the minute field.

        This method adds special handling when all values are single values. Instead
        of "second 1, second 3, and second 5", it produces "seconds 1, 3, and 5".
        """
        if self.all_singles and len(self.parsed) > 1:
            labels = [self.label(v) for v in self.singles()]
            return f"seconds {join(labels)}"
        return super().format()

    def __str__(self) -> str:
        """Return a human-friendly representation of the second field.

        This method adds the 'at' preposition to the formatted string if the field has
        any single values.

        For example, if the field has a single range (1-5), the result will be
        "every second from 1 through 5". But if the field also has a single field
        (1-5,10), the result will be "at every second from 1 through 5 and second 10".
        """
        result = super().__str__()
        if self.any_singles:
            return "at " + result

        return result


class Minute(Field):
    name = "minute"
    min_value = 0
    max_value = 59

    def format(self) -> str:
        """Format the minute field.

        This method adds special handling when all values are single values. Instead
        of "minute 1, minute 3, and minute 5", it produces "minutes 1, 3, and 5".
        """
        if self.all_singles and len(self.parsed) > 1:
            labels = [self.label(v) for v in self.singles()]
            return f"minutes {join(labels)}"
        return super().format()

    def __str__(self) -> str:
        """Return a human-friendly representation of the minute field.

        This method adds the 'at' preposition to the formatted string if the field has
        any single values.

        For example, if the field has a single range (1-5), the result will be
        "every minute from 1 through 5". But if the field also has a single field
        (1-5,10), the result will be "at every minute from 1 through 5 and minute 10".
        """
        result = super().__str__()
        if self.any_singles:
            return "at " + result

        return result


class Hour(Field):
    name = "hour"
    min_value = 0
    max_value = 23

    def format(self) -> str:
        """Format the hour field.

        This method adds special handling when all values are single values. Instead
        of "hour 1, hour 3, and hour 5", it produces "hours 1, 3, and 5".
        """
        if self.all_singles and len(self.parsed) > 1:
            labels = [self.label(v) for v in self.singles()]
            return f"hours {join(labels)}"
        return super().format()

    def __str__(self) -> str:
        """Return a human-friendly representation of the minute field.

        This method adds the 'past' preposition to the formatted string. For example,
        if the hour field has a single value (5), the result will be "past hour 5".
        """
        return "past " + super().__str__()


class Day(Field):
    name = "day of month"
    min_value = 1
    max_value = 31

    def format_single(self, value: int) -> str:
        """Format a single day-of-month value for display.

        >>> format_single(2)
        'the 2nd day of month'
        """
        return f"the {ordinal(value)} day of month"

    def format_nth(self, value: int, nth: int) -> str:
        """Format the L and LW values.

        >>> format_nth(-1)
        'the last day of the month'
        >>> format_nth(-2)
        'the last weekday of the month'
        """
        if nth == -1 and value == -1:
            return "the last day of the month"
        if nth == -1 and value == -2:
            return "the last weekday of the month"
        return super().format_nth(value, nth)

    def format(self) -> str:
        """Format the day field.

        This method adds special handling for the case when
        all values are single values. For example, instead of
        "the 1st day of month, the 3rd day of month, and the 5th day of month"
        it produces "the 1st, 3rd, and 5th day of month".

        We cannot apply this optimization if the single values contain "-2"
        (the special value for "last weekday of the month").
        """
        if self.all_singles and len(self.parsed) > 1:
            singles = self.singles()
            if -2 not in singles:
                labels = [f"the {ordinal(v)}" for v in singles]
                return f"{join(labels)} day of month"
        return super().format()

    def __str__(self) -> str:
        """Return a human-friendly representation of the day of month field.

        This method unconditionally adds the 'on' preposition.
        """
        return "on " + super().__str__()


class Month(Field):
    name = "month"
    min_value = 1
    max_value = 12
    symbolic = "_ JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()
    labels = "_ January February March April May June July August September October November December".split()

    def label(self, idx: int) -> str:
        """Convert an integer value to a month name.

        >>> label(1)
        'January'
        """
        return self.labels[idx]

    def format_single(self, value: int) -> str:
        """Format a single month value for display.

        >>> format_single(2)
        'February'
        """
        return self.label(value)

    def __str__(self) -> str:
        """Return a human-friendly representation of the month field.

        This method unconditionally adds the 'in' preposition.
        """
        return "in " + super().__str__()


class Weekday(Field):
    name = "day of week"
    min_value = 0
    max_value = 7
    symbolic = "SUN MON TUE WED THU FRI SAT SUN".split()
    labels = "Sunday Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split()

    def label(self, idx: int) -> str:
        """Convert an integer value to a day of week name.

        >>> label(1)
        'Monday'
        """
        return self.labels[idx]

    def format_single(self, value: int) -> str:
        """Format a single month value for display.

        >>> format_single(2)
        'Tuesday'
        """
        return self.label(value)

    def format_nth(self, value: int, nth: int) -> str:
        """Format the nth-weekday-of-month value.

        >>> format_nth(2, -1)
        'the last Tuesday of the month'
        >>> format_nth(2, 4)
        'the 4th Tuesday of the month'
        """
        label = self.label(value)
        if nth == -1:
            return f"the last {label} of the month"

        return f"the {ordinal(nth)} {label} of the month"

    def format_seq(self, start: int, stop: int, step: int = 1) -> str:
        """Format a sequence of weekdays.

        >>> format_seq(1, 3)
        'Monday through Wednesday'
        >>> format_seq(1, 7, 2)
        'evary 2nd day of week from Monday through Sunday'
        """

        if step == 1:
            # "Monday through Friday"
            # instead of "every day of week from Monday through Friday"
            return f"{self.label(start)} through {self.label(stop)}"

        return super().format_seq(start, stop, step)

    def __str__(self) -> str:
        """Return a human-friendly representation of the month field.

        This method unconditionally adds the 'on' preposition.
        """
        return "on " + super().__str__()


class Expression:
    def __init__(self, parts: list[str]):
        if len(parts) == 5:
            parts.insert(0, "0")

        self.second = Second(parts[0])
        self.minute = Minute(parts[1])
        self.hour = Hour(parts[2])
        self.day = Day(parts[3])
        self.month = Month(parts[4])
        self.dow = Weekday(parts[5])
        self.day_and = parts[3].startswith("*") or parts[5].startswith("*")

    def optimized_times(self) -> tuple[str, bool] | None:
        """Apply formatting optimizations for hours and minutes.

        If both hours and minutes contain only a few single values, format them
        as "at HH:MM, HH:MM, HH:MM, and HH:MM"

        If hours have a single value, and minutes have a single sequence with step 1,
        format them as "every minute from HH:MM through HH:MM".

        If minutes have a single "*/<step>" sequence and hours have a sequence
        with step 1, format them as "every nth minute from HH:00 through HH:59"

        If no special optimizations apply, return None.
        """
        # at 11:00, 11:30, ...
        if (
            self.hour.all_singles
            and self.minute.all_singles
            and self.second.all_singles
        ):
            second_terms = self.second.singles()
            minute_terms = self.minute.singles()
            hour_terms = self.hour.singles()

            if len(second_terms) * len(minute_terms) * len(hour_terms) <= 4:
                times = []
                for h in sorted(hour_terms):
                    for m in sorted(minute_terms):
                        for s in sorted(second_terms):
                            # If the seconds field is a single "0" then do not
                            # pass it to format_time
                            ss = None if self.second.zero else s
                            times.append(format_time(h, m, ss))

                return "at " + join(times), True

        # every minute from 11:00 through 11:10
        if self.hour.single_value and len(self.minute.parsed) == 1 and self.second.zero:
            seq = self.minute.parsed[0]
            if seq.start is not None and seq.stop is not None and seq.step == 1:
                hhmm1 = format_time(self.hour.single_value, seq.start)
                hhmm2 = format_time(self.hour.single_value, seq.stop)
                return f"every minute from {hhmm1} through {hhmm2}", False

        # every minute from 9:00 through 17:59
        if (
            len(self.hour.parsed) == 1
            and len(self.minute.parsed) == 1
            and self.second.zero
        ):
            mseq = self.minute.parsed[0]
            if mseq.start is None:
                hseq = self.hour.parsed[0]
                if hseq.start is not None and hseq.stop is not None and hseq.step == 1:
                    hhmm1 = format_time(hseq.start, 0)
                    hhmm2 = format_time(hseq.stop, 59)
                    return f"{self.minute} from {hhmm1} through {hhmm2}", False

        return None

    def optimized_dates(self) -> str | None:
        """Apply formatting optimizations for specific dates.

        If day-of-month is L, format it as
        "on the last day of <month description here>".

        If day-of-month is LW, format it as
        "on the last weekday of <month description here>".

        If month and day-of-month each have a single value
        (for example, month 2 and day-of-month 1), format them as
        "February 1st".

        If day-of-month has a single value (for example, 2),
        format it as "on the 2nd day of <month description here>".

        If no special optimizations apply, return None.
        """

        if self.dow.star:
            if self.day.single_value == -1:
                return f"on the last day of {self.month.format()}"
            if self.day.single_value == -2:
                return f"on the last weekday of {self.month.format()}"
            if self.month.single_value and self.day.single_value:
                return f"on {self.month.format()} {self.day.single_value}"
            if self.day.single_value:
                date_ord = ordinal(self.day.single_value)
                return f"on the {date_ord} day of {self.month.format()}"

        return None

    def translate_time(self) -> tuple[str, bool]:
        """Convert the minute and hour fields to text."""
        if self.hour.star:
            if self.minute.star and self.second.star:
                return "every second", False
            if self.minute.star and self.second.zero:
                return "every minute", False
            if self.minute.star:
                # Hours and minutes are both *, so only describe seconds
                return f"{self.second} of every minute", False
            if self.minute.single_value == 0 and self.second.zero:
                return "at the start of every hour", False
            if self.second.zero:
                return f"{self.minute} of every hour", False

        if times := self.optimized_times():
            return times

        if self.second.zero:
            return f"{self.minute} {self.hour}", False

        return f"{self.second} {self.minute} {self.hour}", False

    def translate_date(self, allow_every_day: bool) -> str:
        """Convert the day, month, and weekday fields to text."""
        if dates := self.optimized_dates():
            return dates

        if allow_every_day:
            if self.day.star and self.dow.star and self.month.all_singles:
                # At ... every day in January
                return f"every day {self.month}"

        parts: list[Field | str] = []
        if not self.day.star:
            parts.append(self.day)
        if not self.dow.star:
            if not self.day.star and self.day_and:
                parts.append("if it's")
            elif not self.day.star and not self.day_and:
                parts.append("and")
            parts.append(self.dow)
        if not self.month.star:
            parts.append(self.month)

        return " ".join(str(part) for part in parts)

    def explain(self) -> str:
        """Convert the full expression to text."""
        time, allow_every_day = self.translate_time()
        if date := self.translate_date(allow_every_day):
            result = f"{time} {date}"
        elif allow_every_day:
            result = f"{time} every day"
        else:
            result = f"{time}"

        return result[0].upper() + result[1:]


def explain(expr: str) -> str:
    """Convert the given cron expression to human-friendly description.

    >>> explain("0 0 15 JAN-FEB *")
    'At 00:00 on the 15th day of January and February'
    """
    parts = expr.upper().split()
    return Expression(parts).explain()


# For quick testing, you can run this script from shell like so:
# python explain.py "0 0 15 JAN-FEB *"
if __name__ == "__main__":
    import sys

    if len(sys.argv) == 2:
        print(explain(sys.argv[1]))
