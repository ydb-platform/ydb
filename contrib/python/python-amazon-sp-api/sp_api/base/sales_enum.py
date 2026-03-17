from enum import Enum


class Granularity(str, Enum):
    HOUR = 'Hour'
    DAY = 'Day'
    WEEK = 'Week'
    MONTH = 'Month'
    YEAR = 'Year'
    TOTAL = 'Total'


class BuyerType(str, Enum):
    B2B = 'B2B' # Business to business.
    B2C = 'B2C' # Business to customer.
    ALL = 'All' # Both of above


class FirstDayOfWeek(str, Enum):
    MO = 'Monday'
    SU = 'Sunday'


