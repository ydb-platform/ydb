from beanie.odm.operators.find.array import All, ElemMatch, Size
from beanie.odm.operators.find.bitwise import (
    BitsAllClear,
    BitsAllSet,
    BitsAnyClear,
    BitsAnySet,
)
from beanie.odm.operators.find.comparison import (
    GT,
    GTE,
    LT,
    LTE,
    NE,
    Eq,
    In,
    NotIn,
)
from beanie.odm.operators.find.element import Exists, Type
from beanie.odm.operators.find.evaluation import (
    Expr,
    JsonSchema,
    Mod,
    RegEx,
    Text,
    Where,
)
from beanie.odm.operators.find.geospatial import (
    Box,
    GeoIntersects,
    GeoWithin,
    GeoWithinTypes,
    Near,
    NearSphere,
)
from beanie.odm.operators.find.logical import And, Nor, Not, Or
from beanie.odm.operators.update.array import (
    AddToSet,
    Pop,
    Pull,
    PullAll,
    Push,
)
from beanie.odm.operators.update.bitwise import Bit
from beanie.odm.operators.update.general import (
    CurrentDate,
    Inc,
    Max,
    Min,
    Mul,
    Rename,
    Set,
    SetOnInsert,
    Unset,
)

__all__ = [
    # Find
    # Array
    "All",
    "ElemMatch",
    "Size",
    # Bitwise
    "BitsAllClear",
    "BitsAllSet",
    "BitsAnyClear",
    "BitsAnySet",
    # Comparison
    "Eq",
    "GT",
    "GTE",
    "In",
    "NotIn",
    "LT",
    "LTE",
    "NE",
    # Element
    "Exists",
    "Type",
    "Type",
    # Evaluation
    "Expr",
    "JsonSchema",
    "Mod",
    "RegEx",
    "Text",
    "Where",
    # Geospatial
    "GeoIntersects",
    "GeoWithinTypes",
    "GeoWithin",
    "Box",
    "Near",
    "NearSphere",
    # Logical
    "Or",
    "And",
    "Nor",
    "Not",
    # Update
    # Array
    "AddToSet",
    "Pop",
    "Pull",
    "Push",
    "PullAll",
    # Bitwise
    "Bit",
    # General
    "Set",
    "CurrentDate",
    "Inc",
    "Min",
    "Max",
    "Mul",
    "Rename",
    "SetOnInsert",
    "Unset",
]
