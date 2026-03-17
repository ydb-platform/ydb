from .property_decorators import property_is_valid_time, property_not_nullable


class IntervalItem:
    """
    This class sets the frequency and start time of the scheduled item. This
    class contains the classes for the hourly, daily, weekly, and monthly
    intervals. This class mirrors the options you can set using the REST API and
    the Tableau Server interface.
    """

    class Frequency:
        Hourly = "Hourly"
        Daily = "Daily"
        Weekly = "Weekly"
        Monthly = "Monthly"

    class Occurrence:
        Minutes = "minutes"
        Hours = "hours"
        WeekDay = "weekDay"
        MonthDay = "monthDay"

    class Day:
        Sunday = "Sunday"
        Monday = "Monday"
        Tuesday = "Tuesday"
        Wednesday = "Wednesday"
        Thursday = "Thursday"
        Friday = "Friday"
        Saturday = "Saturday"
        LastDay = "LastDay"


class HourlyInterval:
    """
    Runs scheduled item hourly. To set the hourly interval, you create an
    instance of the HourlyInterval class and assign the following values:
    start_time, end_time, and interval_value. To set the start_time and
    end_time, assign the time value using this syntax: start_time=time(hour, minute)
    and end_time=time(hour, minute). The hour is specified in 24 hour time.
    The interval_value specifies how often the to run the task within the
    start and end time. The options are expressed in hours. For example,
    interval_value=.25 is every 15 minutes. The values are .25, .5, 1, 2, 4, 6,
    8, 12. Hourly schedules that run more frequently than every 60 minutes must
    have start and end times that are on the hour.
    """

    def __init__(self, start_time, end_time, interval_value):
        self.start_time = start_time
        self.end_time = end_time

        # interval should be a tuple, if it is not, assign as a tuple with single value
        if isinstance(interval_value, tuple):
            self.interval = interval_value
        else:
            self.interval = (interval_value,)

    def __repr__(self):
        return f"<{self.__class__.__name__} start={self.start_time} end={self.end_time} interval={self.interval}>"

    @property
    def _frequency(self):
        return IntervalItem.Frequency.Hourly

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    @property_is_valid_time
    @property_not_nullable
    def start_time(self, value):
        self._start_time = value

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    @property_is_valid_time
    @property_not_nullable
    def end_time(self, value):
        self._end_time = value

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, intervals):
        VALID_INTERVALS = {0.25, 0.5, 1, 2, 4, 6, 8, 12, 24}
        for interval in intervals:
            # if an hourly interval is a string, then it is a weekDay interval
            if isinstance(interval, str) and not interval.isnumeric() and not hasattr(IntervalItem.Day, interval):
                error = f"Invalid weekDay interval {interval}"
                raise ValueError(error)

            # if an hourly interval is a number, it is an hours or minutes interval
            if isinstance(interval, (int, float)) and float(interval) not in VALID_INTERVALS:
                error = f"Invalid interval {interval} not in {str(VALID_INTERVALS)}"
                raise ValueError(error)

        self._interval = intervals

    def _interval_type_pairs(self):
        interval_type_pairs = []
        for interval in self.interval:
            # We use fractional hours for the two minute-based intervals.
            # Need to convert to minutes from hours here
            if interval in {0.25, 0.5}:
                calculated_interval = int(interval * 60)
                interval_type = IntervalItem.Occurrence.Minutes

                interval_type_pairs.append((interval_type, str(calculated_interval)))
            else:
                # if the interval is a non-numeric string, it will always be a weekDay
                if isinstance(interval, str) and not interval.isnumeric():
                    interval_type = IntervalItem.Occurrence.WeekDay

                    interval_type_pairs.append((interval_type, str(interval)))
                # otherwise the interval is hours
                else:
                    interval_type = IntervalItem.Occurrence.Hours

                    interval_type_pairs.append((interval_type, str(interval)))

        return interval_type_pairs


class DailyInterval:
    """
    Runs the scheduled item daily. To set the daily interval, you create an
    instance of the DailyInterval and assign the start_time. The start time uses
    the syntax start_time=time(hour, minute).
    """

    def __init__(self, start_time, *interval_values):
        self.start_time = start_time
        self.interval = interval_values

    def __repr__(self):
        return f"<{self.__class__.__name__} start={self.start_time} interval={self.interval}>"

    @property
    def _frequency(self):
        return IntervalItem.Frequency.Daily

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    @property_is_valid_time
    @property_not_nullable
    def start_time(self, value):
        self._start_time = value

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, intervals):
        VALID_INTERVALS = {0.25, 0.5, 1, 2, 4, 6, 8, 12, 24}

        for interval in intervals:
            # if an hourly interval is a string, then it is a weekDay interval
            if isinstance(interval, str) and not interval.isnumeric() and not hasattr(IntervalItem.Day, interval):
                error = f"Invalid weekDay interval {interval}"
                raise ValueError(error)

            # if an hourly interval is a number, it is an hours or minutes interval
            if isinstance(interval, (int, float)) and float(interval) not in VALID_INTERVALS:
                error = f"Invalid interval {interval} not in {str(VALID_INTERVALS)}"
                raise ValueError(error)

        self._interval = intervals

    def _interval_type_pairs(self):
        interval_type_pairs = []
        for interval in self.interval:
            # We use fractional hours for the two minute-based intervals.
            # Need to convert to minutes from hours here
            if interval in {0.25, 0.5}:
                calculated_interval = int(interval * 60)
                interval_type = IntervalItem.Occurrence.Minutes

                interval_type_pairs.append((interval_type, str(calculated_interval)))
            else:
                # if the interval is a non-numeric string, it will always be a weekDay
                if isinstance(interval, str) and not interval.isnumeric():
                    interval_type = IntervalItem.Occurrence.WeekDay

                    interval_type_pairs.append((interval_type, str(interval)))
                # otherwise the interval is hours
                else:
                    interval_type = IntervalItem.Occurrence.Hours

                    interval_type_pairs.append((interval_type, str(interval)))

        return interval_type_pairs


class WeeklyInterval:
    """
    Runs the scheduled item once a week. To set the weekly interval, you create
    an instance of the WeeklyInterval and assign the start time and multiple
    instances for the interval_value (days of week and start time). The start
    time uses the syntax time(hour, minute). The interval_value is the day of
    the week, expressed as a IntervalItem. For example
    TSC.IntervalItem.Day.Monday for Monday.
    """

    def __init__(self, start_time, *interval_values):
        self.start_time = start_time
        self.interval = interval_values

    def __repr__(self):
        return f"<{self.__class__.__name__} start={self.start_time} interval={self.interval}>"

    @property
    def _frequency(self):
        return IntervalItem.Frequency.Weekly

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    @property_is_valid_time
    @property_not_nullable
    def start_time(self, value):
        self._start_time = value

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, interval_values):
        if not all(hasattr(IntervalItem.Day, day) for day in interval_values):
            raise ValueError("Invalid week day defined " + str(interval_values))

        self._interval = interval_values

    def _interval_type_pairs(self):
        return [(IntervalItem.Occurrence.WeekDay, day) for day in self.interval]


class MonthlyInterval:
    """
    Runs the scheduled item once a month. To set the monthly interval, you
    create an instance of the MonthlyInterval and assign the start time and day.
    """

    def __init__(self, start_time, interval_value):
        self.start_time = start_time

        # interval should be a tuple, if it is not, assign as a tuple with single value
        if isinstance(interval_value, tuple):
            self.interval = interval_value
        else:
            self.interval = (interval_value,)

    def __repr__(self):
        return f"<{self.__class__.__name__} start={self.start_time} interval={self.interval}>"

    @property
    def _frequency(self):
        return IntervalItem.Frequency.Monthly

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    @property_is_valid_time
    @property_not_nullable
    def start_time(self, value):
        self._start_time = value

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, interval_values):
        # Valid monthly intervals strings can contain any of the following
        #   day numbers (1-31) (integer or string)
        #   relative day within the month (First, Second, ... Last)
        #   week days (Sunday, Monday, ... LastDay)
        VALID_INTERVALS = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "LastDay",
            "First",
            "Second",
            "Third",
            "Fourth",
            "Fifth",
            "Last",
            "Customized Monthly",
        ]
        for value in range(1, 32):
            VALID_INTERVALS.append(str(value))
            VALID_INTERVALS.append(value)

        for interval_value in interval_values:
            if interval_value not in VALID_INTERVALS:
                error = f"Invalid monthly interval: {interval_value}"
                raise ValueError(error)

        self._interval = interval_values

    def _interval_type_pairs(self):
        return [(IntervalItem.Occurrence.MonthDay, str(day)) for day in self.interval]
