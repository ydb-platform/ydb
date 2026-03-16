from enum import Enum


class DetailTypes(Enum):
    low = "low"  # Show only files with complexity above the max_complexity
    normal = "normal"  # Show all files with their complexity


class Level(Enum):
    function = "function"
    file = "file"


class Sort(Enum):
    asc = "asc"
    desc = "desc"
    name = "name"
