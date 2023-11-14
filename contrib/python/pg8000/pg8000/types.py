from datetime import timedelta as Timedelta


class PGInterval:
    UNIT_MAP = {
        "millennia": "millennia",
        "millennium": "millennia",
        "centuries": "centuries",
        "century": "centuries",
        "decades": "decades",
        "decade": "decades",
        "years": "years",
        "year": "years",
        "months": "months",
        "month": "months",
        "mon": "months",
        "mons": "months",
        "weeks": "weeks",
        "week": "weeks",
        "days": "days",
        "day": "days",
        "hours": "hours",
        "hour": "hours",
        "minutes": "minutes",
        "minute": "minutes",
        "mins": "minutes",
        "secs": "seconds",
        "seconds": "seconds",
        "second": "seconds",
        "microseconds": "microseconds",
        "microsecond": "microseconds",
    }

    ISO_LOOKUP = {
        True: {
            "Y": "years",
            "M": "months",
            "D": "days",
        },
        False: {
            "H": "hours",
            "M": "minutes",
            "S": "seconds",
        },
    }

    @classmethod
    def from_str_iso_8601(cls, interval_str):
        # P[n]Y[n]M[n]DT[n]H[n]M[n]S
        kwargs = {}
        lookup = cls.ISO_LOOKUP[True]
        val = []

        for c in interval_str[1:]:
            if c == "T":
                lookup = cls.ISO_LOOKUP[False]
            elif c.isdigit() or c in ("-", "."):
                val.append(c)
            else:
                val_str = "".join(val)
                name = lookup[c]
                v = float(val_str) if name == "seconds" else int(val_str)
                kwargs[name] = v
                val.clear()

        return cls(**kwargs)

    @classmethod
    def from_str_postgres(cls, interval_str):
        """Parses both the postgres and postgres_verbose formats"""

        t = {}

        curr_val = None
        for k in interval_str.split():
            if ":" in k:
                hours_str, minutes_str, seconds_str = k.split(":")
                hours = int(hours_str)
                if hours != 0:
                    t["hours"] = hours
                minutes = int(minutes_str)
                if minutes != 0:
                    t["minutes"] = minutes

                seconds = float(seconds_str)

                if seconds != 0:
                    t["seconds"] = seconds

            elif k == "@":
                continue

            elif k == "ago":
                for k, v in tuple(t.items()):
                    t[k] = -1 * v

            else:
                try:
                    curr_val = int(k)
                except ValueError:
                    t[cls.UNIT_MAP[k]] = curr_val

        return cls(**t)

    @classmethod
    def from_str_sql_standard(cls, interval_str):
        """YYYY-MM
        or
        DD HH:MM:SS.F
        or
        YYYY-MM DD HH:MM:SS.F
        """
        month_part = None
        day_parts = None
        parts = interval_str.split()

        if len(parts) == 1:
            month_part = parts[0]
        elif len(parts) == 2:
            day_parts = parts
        else:
            month_part = parts[0]
            day_parts = parts[1:]

        kwargs = {}

        if month_part is not None:
            if month_part.startswith("-"):
                sign = -1
                p = month_part[1:]
            else:
                sign = 1
                p = month_part

            kwargs["years"], kwargs["months"] = [int(v) * sign for v in p.split("-")]

        if day_parts is not None:
            kwargs["days"] = int(day_parts[0])
            time_part = day_parts[1]

            if time_part.startswith("-"):
                sign = -1
                p = time_part[1:]
            else:
                sign = 1
                p = time_part

            hours, minutes, seconds = p.split(":")
            kwargs["hours"] = int(hours) * sign
            kwargs["minutes"] = int(minutes) * sign
            kwargs["seconds"] = float(seconds) * sign

        return cls(**kwargs)

    @classmethod
    def from_str(cls, interval_str):
        if interval_str.startswith("P"):
            return cls.from_str_iso_8601(interval_str)
        elif interval_str.startswith("@"):
            return cls.from_str_postgres(interval_str)
        else:
            parts = interval_str.split()
            if (len(parts) > 1 and parts[1][0].isalpha()) or (
                len(parts) == 1 and ":" in parts[0]
            ):
                return cls.from_str_postgres(interval_str)
            else:
                return cls.from_str_sql_standard(interval_str)

    def __init__(
        self,
        millennia=None,
        centuries=None,
        decades=None,
        years=None,
        months=None,
        weeks=None,
        days=None,
        hours=None,
        minutes=None,
        seconds=None,
        microseconds=None,
    ):
        self.millennia = millennia
        self.centuries = centuries
        self.decades = decades
        self.years = years
        self.months = months
        self.weeks = weeks
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.microseconds = microseconds

    def __repr__(self):
        return f"<PGInterval {self}>"

    def _value_dict(self):
        return {
            k: v
            for k, v in (
                ("millennia", self.millennia),
                ("centuries", self.centuries),
                ("decades", self.decades),
                ("years", self.years),
                ("months", self.months),
                ("weeks", self.weeks),
                ("days", self.days),
                ("hours", self.hours),
                ("minutes", self.minutes),
                ("seconds", self.seconds),
                ("microseconds", self.microseconds),
            )
            if v is not None
        }

    def __str__(self):
        return " ".join(f"{v} {n}" for n, v in self._value_dict().items())

    def normalize(self):
        months = 0
        if self.months is not None:
            months += self.months
        if self.years is not None:
            months += self.years * 12

        days = 0
        if self.days is not None:
            days += self.days
        if self.weeks is not None:
            days += self.weeks * 7

        seconds = 0
        if self.hours is not None:
            seconds += self.hours * 60 * 60
        if self.minutes is not None:
            seconds += self.minutes * 60
        if self.seconds is not None:
            seconds += self.seconds
        if self.microseconds is not None:
            seconds += self.microseconds / 1000000

        return PGInterval(months=months, days=days, seconds=seconds)

    def __eq__(self, other):
        if isinstance(other, PGInterval):
            s = self.normalize()
            o = other.normalize()
            return s.months == o.months and s.days == o.days and s.seconds == o.seconds
        else:
            return False

    def to_timedelta(self):
        pairs = self._value_dict()
        overlap = pairs.keys() & {
            "weeks",
            "months",
            "years",
            "decades",
            "centuries",
            "millennia",
        }
        if len(overlap) > 0:
            raise ValueError(
                "Can't fit the interval fields {overlap} into a datetime.timedelta."
            )

        return Timedelta(**pairs)


class Range:
    def __init__(
        self,
        lower=None,
        upper=None,
        bounds="[)",
        is_empty=False,
    ):
        self.lower = lower
        self.upper = upper
        self.bounds = bounds
        self.is_empty = is_empty

    def __eq__(self, other):
        if isinstance(other, Range):
            if self.is_empty or other.is_empty:
                return self.is_empty == other.is_empty
            else:
                return (
                    self.lower == other.lower
                    and self.upper == other.upper
                    and self.bounds == other.bounds
                )
        return False

    def __str__(self):
        if self.is_empty:
            return "empty"
        else:
            le, ue = ["" if v is None else v for v in (self.lower, self.upper)]
            return f"{self.bounds[0]}{le},{ue}{self.bounds[1]}"

    def __repr__(self):
        return f"<Range {self}>"
