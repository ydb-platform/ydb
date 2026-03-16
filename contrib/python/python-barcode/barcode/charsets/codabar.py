from __future__ import annotations

# W = Wide bar
# w = wide space
# N = Narrow bar
# n = narrow space

CODES = {
    "0": "NnNnNwW",
    "1": "NnNnWwN",
    "2": "NnNwNnW",
    "3": "WwNnNnN",
    "4": "NnWnNwN",
    "5": "WnNnNwN",
    "6": "NwNnNnW",
    "7": "NwNnWnN",
    "8": "NwWnNnN",
    "9": "WnNwNnN",
    "-": "NnNwWnN",
    "$": "NnWwNnN",
    ":": "WnNnWnW",
    "/": "WnWnNnW",
    ".": "WnWnWnN",
    "+": "NnWnWnW",
}

STARTSTOP = {"A": "NnWwNwN", "B": "NwNwNnW", "C": "NnNwNwW", "D": "NnNwWwN"}
