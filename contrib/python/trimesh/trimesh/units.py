"""
units.py
--------------
Deal with physical unit systems (i.e. inches, mm)

Very basic conversions, and no requirement for
sympy.physics.units or pint.
"""

from . import resources
from .constants import log
from .parent import Geometry

# scaling factors from various unit systems to inches
_lookup = resources.get_json("units_to_inches.json")


def unit_conversion(current: str, desired: str) -> float:
    """
    Calculate the conversion from one set of units to another.

    Parameters
    ---------
    current : str
        Unit system values are in now (eg 'millimeters')
    desired : str
        Unit system we'd like values in (eg 'inches')

    Returns
    ---------
    conversion : float
        Number to multiply by to put values into desired units
    """
    # convert to common system then return ratio between current and desired
    return to_inch(current.strip().lower()) / to_inch(desired.strip().lower())


def keys() -> set:
    """
    Return a set containing all currently valid units.

    Returns
    --------
    keys
      All units with conversions i.e. {'in', 'm', ...}
    """
    return set(_lookup.keys())


def to_inch(unit: str) -> float:
    """
    Calculate the conversion to an arbitrary common unit.

    Parameters
    ------------
    unit
      Either a key in `units_to_inches.json` or in the simple
      `{float} * {str}` form, i.e. "1.2 * meters". We don't
      support arbitrary `eval` of any math string

    Returns
    ----------
    conversion
      Factor to multiply by to get to an `inch` system.
    """
    # see if the units are just in our lookup table
    lookup = _lookup.get(unit.strip().lower(), None)
    if lookup is not None:
        return lookup

    try:
        # otherwise check to see if they are in the factor * unit form
        value, key = unit.split("*")
        return _lookup[key.strip()] * float(value)
    except BaseException as E:
        # add a helpful error message
        message = (
            f'arbitrary units must be in the form "1.21 * meters", not "{unit}" ({E})'
        )

    raise ValueError(message)


def units_from_metadata(obj: Geometry, guess: bool = True) -> str:
    """
    Try to extract hints from metadata and if that fails
    guess based on the object scale.


    Parameters
    ------------
    obj
      A geometry object.
    guess
      If metadata doesn't have units make a "best guess"

    Returns
    ------------
    units
     A guess of what the units might be
    """

    hints = [obj.metadata.get("name", None)]
    if obj.source is not None:
        hints.append(obj.source.file_name)

    # try to guess from metadata
    for hint in hints:
        if hint is None:
            continue
        hint = hint.lower().replace("units", " ").replace("unit", " ")
        # replace all delimiter options with white space
        for delim in "_-.":
            hint = hint.replace(delim, " ")
        hint = "".join(c for c in hint if c not in "0123456789")
        # loop through each hint
        for h in hint.strip().split():
            # if the hint is a valid unit return it
            if h in _lookup:
                return h

    if not guess:
        raise ValueError("No units and not allowed to guess!")

    # we made it to the wild ass guess section
    # if the scale is larger than 100 mystery units
    # declare the model to be millimeters, otherwise inches
    log.debug("no units: guessing from scale")
    if float(obj.scale) > 100.0:
        return "millimeters"
    else:
        return "inches"


def _convert_units(obj: Geometry, desired: str, guess=False) -> None:
    """
    Given an object with scale and units try to scale
    to different units via the object's `apply_scale`.

    Parameters
    ---------
    obj :  object
        With apply_scale method (i.e. Trimesh, Path2D, etc)
    desired : str
        Units desired (eg 'inches')
    guess:   bool
        Whether we are allowed to guess the units
        if they are not specified.
    """
    if obj.units is None:
        # try to extract units from metadata
        # if nothing specified in metadata and not allowed
        # to guess will raise a ValueError
        obj.units = units_from_metadata(obj, guess=guess)

    log.debug("converting units from %s to %s", obj.units, desired)
    # float, conversion factor
    conversion = unit_conversion(obj.units, desired)

    # apply scale uses transforms which preserve
    # cached properties rather than just multiplying vertices
    obj.apply_scale(conversion)
    # units are now desired units
    obj.units = desired
