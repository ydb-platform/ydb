"""
Misc utilities.
"""

from typing import Union


def ord_(dta: Union[str, int]) -> int:
    if isinstance(dta, str):
        return ord(dta)
    return dta
