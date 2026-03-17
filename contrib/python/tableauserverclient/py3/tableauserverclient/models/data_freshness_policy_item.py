import xml.etree.ElementTree as ET

from typing import Optional
from tableauserverclient.models.property_decorators import property_is_enum, property_not_nullable
from .interval_item import IntervalItem


class DataFreshnessPolicyItem:
    class Option:
        AlwaysLive = "AlwaysLive"
        SiteDefault = "SiteDefault"
        FreshEvery = "FreshEvery"
        FreshAt = "FreshAt"

    class FreshEvery:
        class Frequency:
            Minutes = "Minutes"
            Hours = "Hours"
            Days = "Days"
            Weeks = "Weeks"

        def __init__(self, frequency: str, value: int):
            self.frequency: str = frequency
            self.value: int = value

        def __repr__(self):
            return "<FreshEvery frequency={_frequency} value={_value}>".format(**vars(self))

        @property
        def frequency(self) -> str:
            return self._frequency

        @frequency.setter
        @property_is_enum(Frequency)
        def frequency(self, value: str):
            self._frequency = value

        @classmethod
        def from_xml_element(cls, fresh_every_schedule_elem: ET.Element):
            frequency = fresh_every_schedule_elem.get("frequency", None)
            value_str = fresh_every_schedule_elem.get("value", None)
            if (frequency is None) or (value_str is None):
                return None
            value = int(value_str)
            return DataFreshnessPolicyItem.FreshEvery(frequency, value)

    class FreshAt:
        class Frequency:
            Day = "Day"
            Week = "Week"
            Month = "Month"

        def __init__(self, frequency: str, time: str, timezone, interval_item: Optional[list[str]] = None):
            self.frequency = frequency
            self.time = time
            self.timezone = timezone
            self.interval_item: Optional[list[str]] = interval_item

        def __repr__(self):
            return (
                "<FreshAt frequency={_frequency} time={_time}> timezone={_timezone} " "interval_item={_interval_time}"
            ).format(**vars(self))

        @property
        def interval_item(self) -> Optional[list[str]]:
            return self._interval_item

        @interval_item.setter
        def interval_item(self, value: Optional[list[str]]):
            self._interval_item = value

        @property
        def time(self):
            return self._time

        @time.setter
        @property_not_nullable
        def time(self, value):
            self._time = value

        @property
        def timezone(self) -> str:
            return self._timezone

        @timezone.setter
        def timezone(self, value: str):
            self._timezone = value

        @property
        def frequency(self) -> str:
            return self._frequency

        @frequency.setter
        @property_is_enum(Frequency)
        def frequency(self, value: str):
            self._frequency = value

        @classmethod
        def from_xml_element(cls, fresh_at_schedule_elem: ET.Element, ns):
            frequency = fresh_at_schedule_elem.get("frequency", None)
            time = fresh_at_schedule_elem.get("time", None)
            if (frequency is None) or (time is None):
                return None
            timezone = fresh_at_schedule_elem.get("timezone", None)
            interval = parse_intervals(fresh_at_schedule_elem, frequency, ns)
            return DataFreshnessPolicyItem.FreshAt(frequency, time, timezone, interval)

    def __init__(self, option: str):
        self.option = option
        self.fresh_every_schedule: Optional[DataFreshnessPolicyItem.FreshEvery] = None
        self.fresh_at_schedule: Optional[DataFreshnessPolicyItem.FreshAt] = None

    def __repr__(self):
        return "<DataFreshnessPolicy option={_option}>".format(**vars(self))

    @property
    def option(self) -> str:
        return self._option

    @option.setter
    @property_is_enum(Option)
    def option(self, value: str):
        self._option = value

    @property
    def fresh_every_schedule(self) -> Optional[FreshEvery]:
        return self._fresh_every_schedule

    @fresh_every_schedule.setter
    def fresh_every_schedule(self, value: Optional[FreshEvery]):
        self._fresh_every_schedule = value

    @property
    def fresh_at_schedule(self) -> Optional[FreshAt]:
        return self._fresh_at_schedule

    @fresh_at_schedule.setter
    def fresh_at_schedule(self, value: Optional[FreshAt]):
        self._fresh_at_schedule = value

    @classmethod
    def from_xml_element(cls, data_freshness_policy_elem, ns):
        option = data_freshness_policy_elem.get("option", None)
        if option is None:
            return None
        data_freshness_policy = DataFreshnessPolicyItem(option)

        fresh_at_schedule = None
        fresh_every_schedule = None
        if option == "FreshAt":
            fresh_at_schedule_elem = data_freshness_policy_elem.find(".//t:freshAtSchedule", namespaces=ns)
            fresh_at_schedule = DataFreshnessPolicyItem.FreshAt.from_xml_element(fresh_at_schedule_elem, ns)
            data_freshness_policy.fresh_at_schedule = fresh_at_schedule
        elif option == "FreshEvery":
            fresh_every_schedule_elem = data_freshness_policy_elem.find(".//t:freshEverySchedule", namespaces=ns)
            fresh_every_schedule = DataFreshnessPolicyItem.FreshEvery.from_xml_element(fresh_every_schedule_elem)
            data_freshness_policy.fresh_every_schedule = fresh_every_schedule

        return data_freshness_policy


def parse_intervals(intervals_elem, frequency, ns):
    interval_elems = intervals_elem.findall(".//t:intervals/t:interval", namespaces=ns)
    interval = []
    for interval_elem in interval_elems:
        interval.extend(interval_elem.attrib.items())

    # No intervals expected for Day frequency
    if frequency == DataFreshnessPolicyItem.FreshAt.Frequency.Day:
        return None

    if frequency == DataFreshnessPolicyItem.FreshAt.Frequency.Week:
        interval_values = [(i[1]).title() for i in interval]
        return parse_week_intervals(interval_values)

    if frequency == DataFreshnessPolicyItem.FreshAt.Frequency.Month:
        interval_values = [(i[1]) for i in interval]
        return parse_month_intervals(interval_values)


def parse_week_intervals(interval_values):
    # Using existing IntervalItem.Day to check valid weekday string
    if not all(hasattr(IntervalItem.Day, day) for day in interval_values):
        raise ValueError("Invalid week day defined " + str(interval_values))
    return interval_values


def parse_month_intervals(interval_values):
    error = f"Invalid interval value for a monthly frequency: {interval_values}."

    # Month interval can have value either only ['LastDay'] or list of dates e.g. ["1", 20", "30"]
    # First check if the list only have LastDay value. When using LastDay, there shouldn't be
    # any other values, hence checking the first element of the list is enough.
    # If the value is not "LastDay", we assume intervals is on list of dates format.
    # We created this function instead of using existing MonthlyInterval because we allow list of dates interval,

    intervals = []
    if interval_values[0] == "LastDay":
        intervals.append(interval_values[0])
    else:
        for interval in interval_values:
            try:
                if 1 <= int(interval) <= 31:
                    intervals.append(interval)
                else:
                    raise ValueError(error)
            except ValueError:
                if interval_values[0] != "LastDay":
                    raise ValueError(error)
    return intervals
