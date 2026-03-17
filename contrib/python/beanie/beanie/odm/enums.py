from enum import Enum

import pymongo


class SortDirection(int, Enum):
    """
    Sorting directions
    """

    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING


class InspectionStatuses(str, Enum):
    """
    Statuses of the collection inspection
    """

    FAIL = "FAIL"
    OK = "OK"
