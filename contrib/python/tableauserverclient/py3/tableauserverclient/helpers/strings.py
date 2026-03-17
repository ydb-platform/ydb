from defusedxml.ElementTree import fromstring, tostring
from functools import singledispatch
from typing import TypeVar, overload


# the redact method can handle either strings or bytes, but it can't mix them.
# Generic type so we can write the actual logic once, then use singledispatch to
# create the replacement text with the correct type
T = TypeVar("T", str, bytes)


def _redact_any_type(xml: T, sensitive_word: T, replacement: T, encoding=None) -> T:
    try:
        root = fromstring(xml)
        matches = root.findall(".//*[@password]")
        for item in matches:
            item.attrib["password"] = "********"
        matches = root.findall(".//password")
        for item in matches:
            item.text = "********"
        # tostring returns bytes unless an encoding value is passed
        return tostring(root, encoding=encoding)
    except Exception:
        # something about the xml handling failed. Just cut off the text at the first occurrence of "password"
        location = xml.find(sensitive_word)
        return xml[:location] + replacement


@singledispatch
def redact_xml(content):
    # this will only be called if it didn't get directed to the str or bytes overloads
    raise TypeError("Redaction only works on xml saved as str or bytes")


@redact_xml.register
def _(xml: str) -> str:
    out = _redact_any_type(xml, "password", "...[redacted]", encoding="unicode")
    return out


@redact_xml.register  # type: ignore[no-redef]
def _(xml: bytes) -> bytes:
    return _redact_any_type(bytearray(xml), b"password", b"..[redacted]")


@overload
def nullable_str_to_int(value: None) -> None: ...


@overload
def nullable_str_to_int(value: str) -> int: ...


def nullable_str_to_int(value):
    return int(value) if value is not None else None


@overload
def nullable_str_to_bool(value: None) -> None: ...


@overload
def nullable_str_to_bool(value: str) -> bool: ...


def nullable_str_to_bool(value):
    return str(value).lower() == "true" if value is not None else None
