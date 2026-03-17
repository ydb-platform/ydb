from datetime import datetime, date
from decimal import Decimal
from typing import Any, Optional

from normality.cleaning import remove_unsafe_chars
from normality.encoding import predict_encoding
from normality.encoding import DEFAULT_ENCODING


def _clean_empty(value: str) -> Optional[str]:
    # XXX: is this really a good idea?
    value = value.strip()
    if not len(value):
        return None
    return value


def stringify(
    value: Any, encoding_default: str = DEFAULT_ENCODING, encoding: Optional[str] = None
) -> Optional[str]:
    """Brute-force convert a given object to a string.

    This will attempt an increasingly mean set of conversions to make a given
    object into a unicode string. It is guaranteed to either return unicode or
    None, if all conversions failed (or the value is indeed empty).
    """
    if value is None:
        return None
    if isinstance(value, str):
        return _clean_empty(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    elif isinstance(value, float):
        # Avoid trailing zeros and limit to 3 decimal places:
        return format(value, ".3f").rstrip("0").rstrip(".")
    elif isinstance(value, Decimal):
        return Decimal(value).to_eng_string()
    elif isinstance(value, bytes):
        if encoding is None:
            encoding = predict_encoding(value, default=encoding_default)
        value = value.decode(encoding, "replace")
        value = remove_unsafe_chars(value)
        if value is None:
            return None
        return _clean_empty(value)
    return _clean_empty(str(value))
