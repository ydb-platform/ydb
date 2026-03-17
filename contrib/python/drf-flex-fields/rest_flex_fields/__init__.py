from django.conf import settings


FLEX_FIELDS_OPTIONS = getattr(settings, "REST_FLEX_FIELDS", {})
EXPAND_PARAM = FLEX_FIELDS_OPTIONS.get("EXPAND_PARAM", "expand")
FIELDS_PARAM = FLEX_FIELDS_OPTIONS.get("FIELDS_PARAM", "fields")
OMIT_PARAM = FLEX_FIELDS_OPTIONS.get("OMIT_PARAM", "omit")
MAXIMUM_EXPANSION_DEPTH = FLEX_FIELDS_OPTIONS.get("MAXIMUM_EXPANSION_DEPTH", None)
RECURSIVE_EXPANSION_PERMITTED = FLEX_FIELDS_OPTIONS.get(
    "RECURSIVE_EXPANSION_PERMITTED", True
)

WILDCARD_ALL = "~all"
WILDCARD_ASTERISK = "*"

if "WILDCARD_EXPAND_VALUES" in FLEX_FIELDS_OPTIONS:
    WILDCARD_VALUES = FLEX_FIELDS_OPTIONS["WILDCARD_EXPAND_VALUES"]
elif "WILDCARD_VALUES" in FLEX_FIELDS_OPTIONS:
    WILDCARD_VALUES = FLEX_FIELDS_OPTIONS["WILDCARD_VALUES"]
else:
    WILDCARD_VALUES = [WILDCARD_ALL, WILDCARD_ASTERISK]

assert isinstance(EXPAND_PARAM, str), "'EXPAND_PARAM' should be a string"
assert isinstance(FIELDS_PARAM, str), "'FIELDS_PARAM' should be a string"
assert isinstance(OMIT_PARAM, str), "'OMIT_PARAM' should be a string"

if type(WILDCARD_VALUES) not in (list, type(None)):
    raise ValueError("'WILDCARD_EXPAND_VALUES' should be a list of strings or None")
if type(MAXIMUM_EXPANSION_DEPTH) not in (int, type(None)):
    raise ValueError("'MAXIMUM_EXPANSION_DEPTH' should be a int or None")
if type(RECURSIVE_EXPANSION_PERMITTED) is not bool:
    raise ValueError("'RECURSIVE_EXPANSION_PERMITTED' should be a bool")

from .utils import *
from .serializers import FlexFieldsModelSerializer
from .views import FlexFieldsModelViewSet
