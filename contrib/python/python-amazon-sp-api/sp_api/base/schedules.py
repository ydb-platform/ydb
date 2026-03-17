from enum import Enum


class Schedules(str, Enum):
    MINUTES_5 = "PT5M"
    MINUTES_15 = "PT15M"
    MINUTES_30 = "PT30M"
    HOUR_1 = "PT1H"
    HOURS_2 = "PT2H"
    HOURS_4 = "PT4H"
    HOURS_8 = "PT8H"
    HOURS_12 = "PT12H"
    DAY_1 = "P1D"
    DAYS_2 = "P2D"
    DAYS_3 = "P3D"
    HOURS_84 = "PT84H"
    DAYS_7 = "P7D"
    DAYS_14 = "P14D"
    DAYS_15 = "P15D"
    DAYS_18 = "P18D"
    DAYS_30 = "P30D"
    MONTH_1 = "P1M"
