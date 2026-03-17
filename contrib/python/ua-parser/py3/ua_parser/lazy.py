__all__ = ["DeviceMatcher", "OSMatcher", "UserAgentMatcher"]

import re
from functools import cached_property
from typing import Literal, Optional, Pattern

from .core import Device, Matcher, OS, UserAgent
from .utils import get, replacer


class UserAgentMatcher(Matcher[UserAgent]):
    """Lazy user agent matcher, compiles the input ``regex`` on first
    use.

    Requires slightly more memory as it needs to reference both the
    pattern string and the compiled pattern.

    """

    regex: str = ""
    family: str
    major: Optional[str]
    minor: Optional[str]
    patch: Optional[str]
    patch_minor: Optional[str]

    def __init__(
        self,
        regex: str,
        family: Optional[str] = None,
        major: Optional[str] = None,
        minor: Optional[str] = None,
        patch: Optional[str] = None,
        patch_minor: Optional[str] = None,
    ) -> None:
        self.regex = regex
        self.family = family or "$1"
        self.major = major
        self.minor = minor
        self.patch = patch
        self.patch_minor = patch_minor

    def __call__(self, ua: str) -> Optional[UserAgent]:
        if m := self.pattern.search(ua):
            return UserAgent(
                family=(
                    self.family.replace("$1", m[1])
                    if "$1" in self.family
                    else self.family
                ),
                major=self.major or get(m, 2),
                minor=self.minor or get(m, 3),
                patch=self.patch or get(m, 4),
                patch_minor=self.patch_minor or get(m, 5),
            )
        return None

    @cached_property
    def pattern(self) -> Pattern[str]:
        return re.compile(self.regex)

    def __repr__(self) -> str:
        fields = [
            ("family", self.family if self.family != "$1" else None),
            ("major", self.major),
            ("minor", self.minor),
            ("patch", self.patch),
            ("patch_minor", self.patch_minor),
        ]
        args = "".join(f", {k}={v!r}" for k, v in fields if v is not None)

        return f"UserAgentMatcher({self.regex!r}{args})"


class OSMatcher(Matcher[OS]):
    """Lazy OS matcher, compiles the input ``regex`` on first
    use.

    Requires slightly more memory as it needs to reference both the
    pattern string and the compiled pattern.

    """

    regex: str = ""
    family: str
    major: str
    minor: str
    patch: str
    patch_minor: str

    def __init__(
        self,
        regex: str,
        family: Optional[str] = None,
        major: Optional[str] = None,
        minor: Optional[str] = None,
        patch: Optional[str] = None,
        patch_minor: Optional[str] = None,
    ) -> None:
        self.regex = regex
        self.family = family or "$1"
        self.major = major or "$2"
        self.minor = minor or "$3"
        self.patch = patch or "$4"
        self.patch_minor = patch_minor or "$5"

    def __call__(self, ua: str) -> Optional[OS]:
        if m := self.pattern.search(ua):
            family = replacer(self.family, m)
            if family is None:
                raise ValueError(f"Unable to find OS family in {ua}")
            return OS(
                family=family,
                major=replacer(self.major, m),
                minor=replacer(self.minor, m),
                patch=replacer(self.patch, m),
                patch_minor=replacer(self.patch_minor, m),
            )
        return None

    @cached_property
    def pattern(self) -> Pattern[str]:
        return re.compile(self.regex)

    def __repr__(self) -> str:
        fields = [
            ("family", self.family if self.family != "$1" else None),
            ("major", self.major if self.major != "$2" else None),
            ("minor", self.minor if self.minor != "$3" else None),
            ("patch", self.patch if self.patch != "$4" else None),
            ("patch_minor", self.patch_minor if self.patch_minor != "$5" else None),
        ]
        args = "".join(f", {k}={v!r}" for k, v in fields if v is not None)

        return f"OSMatcher({self.regex!r}{args})"


class DeviceMatcher(Matcher[Device]):
    """Lazy device matcher, compiles the input ``regex`` on first
    use.

    Requires slightly more memory as it needs to reference both the
    pattern string and the compiled pattern.

    """

    regex: str = ""
    regex_flag: Optional[Literal["i"]] = None
    family: str
    brand: str
    model: str

    def __init__(
        self,
        regex: str,
        regex_flag: Optional[Literal["i"]] = None,
        family: Optional[str] = None,
        brand: Optional[str] = None,
        model: Optional[str] = None,
    ) -> None:
        self.regex = regex
        self.regex_flag = regex_flag
        self.family = family or "$1"
        self.brand = brand or ""
        self.model = model or "$1"

    def __call__(self, ua: str) -> Optional[Device]:
        if m := self.pattern.search(ua):
            family = replacer(self.family, m)
            if family is None:
                raise ValueError(f"Unable to find device family in {ua}")
            return Device(
                family=family,
                brand=replacer(self.brand, m),
                model=replacer(self.model, m),
            )
        return None

    @property
    def flags(self) -> int:
        return re.IGNORECASE if self.regex_flag == "i" else 0

    @cached_property
    def pattern(self) -> Pattern[str]:
        return re.compile(self.regex, flags=self.flags)

    def __repr__(self) -> str:
        fields = [
            ("family", self.family if self.family != "$1" else None),
            ("brand", self.brand or None),
            ("model", self.model if self.model != "$1" else None),
        ]
        iflag = ', "i"' if self.regex_flag else ""
        args = iflag + "".join(f", {k}={v!r}" for k, v in fields if v is not None)

        return f"DeviceMatcher({self.regex!r}{args})"
