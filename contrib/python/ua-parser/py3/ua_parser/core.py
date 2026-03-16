import abc
from dataclasses import dataclass
from enum import Flag, auto
from typing import Generic, List, Optional, Protocol, Tuple, TypeVar

__all__ = [
    "OS",
    "DefaultedResult",
    "Device",
    "Domain",
    "Matchers",
    "PartialResult",
    "Resolver",
    "Result",
    "UserAgent",
]


@dataclass(frozen=True)
class UserAgent:
    """Browser ("user agent" aka the software responsible for the request)
    information parsed from the user agent string.
    """

    __slots__ = ("family", "major", "minor", "patch", "patch_minor")
    family: str
    major: Optional[str]
    minor: Optional[str]
    patch: Optional[str]
    patch_minor: Optional[str]

    def __init__(
        self,
        family: str = "Other",
        major: Optional[str] = None,
        minor: Optional[str] = None,
        patch: Optional[str] = None,
        patch_minor: Optional[str] = None,
    ) -> None:
        object.__setattr__(self, "family", family)
        object.__setattr__(self, "major", major)
        object.__setattr__(self, "minor", minor)
        object.__setattr__(self, "patch", patch)
        object.__setattr__(self, "patch_minor", patch_minor)


@dataclass(frozen=True)
class OS:
    """OS information parsed from the user agent string."""

    __slots__ = ("family", "major", "minor", "patch", "patch_minor")
    family: str
    major: Optional[str]
    minor: Optional[str]
    patch: Optional[str]
    patch_minor: Optional[str]

    def __init__(
        self,
        family: str = "Other",
        major: Optional[str] = None,
        minor: Optional[str] = None,
        patch: Optional[str] = None,
        patch_minor: Optional[str] = None,
    ) -> None:
        object.__setattr__(self, "family", family)
        object.__setattr__(self, "major", major)
        object.__setattr__(self, "minor", minor)
        object.__setattr__(self, "patch", patch)
        object.__setattr__(self, "patch_minor", patch_minor)


@dataclass(frozen=True)
class Device:
    """Device information parsed from the user agent string."""

    __slots__ = ("brand", "family", "model")
    family: str
    brand: Optional[str]
    model: Optional[str]

    def __init__(
        self,
        family: str = "Other",
        brand: Optional[str] = None,
        model: Optional[str] = None,
    ) -> None:
        object.__setattr__(self, "family", family)
        object.__setattr__(self, "brand", brand)
        object.__setattr__(self, "model", model)


class Domain(Flag):
    """Hint for selecting which domains are requested when asking for a
    :class:`PartialResult`.
    """

    #: browser (user agent) domain
    USER_AGENT = auto()
    #: os domain
    OS = auto()
    #: device domain
    DEVICE = auto()
    #: shortcut for all three domains
    ALL = USER_AGENT | OS | DEVICE


@dataclass(frozen=True)
class DefaultedResult:
    """Variant of :class:`Result` where attributes are set
    to a default value if their resolution failed.

    For all domains, the default value has ``family`` set to
    ``"Other"`` and every other attribute set to ``None``.
    """

    user_agent: UserAgent
    os: OS
    device: Device
    string: str


@dataclass(frozen=True)
class Result:
    """Complete result.

    For each attribute (and domain), either the resolution was a
    success (a match was found) and the corresponding data is set, or
    it was a failure and the value is `None`.

    """

    user_agent: Optional[UserAgent]
    os: Optional[OS]
    device: Optional[Device]
    string: str

    def with_defaults(self) -> DefaultedResult:
        """Replaces every failed domain by its default value.

        Roughly matches pre-1.0 semantics, and can allow for more
        uniform handling by the client if they don't want or need the
        lookup failure information.

        """

        return DefaultedResult(
            user_agent=self.user_agent or UserAgent(),
            os=self.os or OS(),
            device=self.device or Device(),
            string=self.string,
        )


@dataclass(frozen=True)
class PartialResult:
    """Potentially partial (incomplete) result.

    Domain fields (``user_agent``, ``os``, and ``device``) can be:

    - unset if not parsed yet
    - set to a parsing failure
    - set to a parsing success

    The ``domains`` flags specify which is which: if a :class:`Domain`
    flag is set, the corresponding attribute was looked up and is
    either ``None`` for a resolution failure (no match was found) or a
    value for a parsing success.

    If the flag is unset, the field has not been looked up yet, in
    which case it can be anything (but should usually be ``None``).

    """

    __slots__ = ("device", "domains", "os", "string", "user_agent")
    domains: Domain
    user_agent: Optional[UserAgent]
    os: Optional[OS]
    device: Optional[Device]
    string: str

    def complete(self) -> Result:
        """Requires that the result be fully resolved (every attribute is set,
        even if to a lookup failure).

        :raises ValueError: if the result is not fully resolved
        """
        if self.domains != Domain.ALL:
            raise ValueError("Only a result with all attributes set can be completed")

        return Result(
            user_agent=self.user_agent,
            os=self.os,
            device=self.device,
            string=self.string,
        )


class Resolver(Protocol):
    """Resolver()

    The resolver is the thin central abstraction of ua-parser, and
    used to compose various objects into the resolution stack which
    best fits the system's needs.

    A resolver is any callable which takes a string ``ua`` and a
    :class:`Domain`, and returns a :class:`PartialResult` with at
    least the requested domains marked as resolved (whether
    successfully or not).

    A resolver may resolve more domains than requested, but it needs
    to resolve at least the requested domains.

    See :class:`PartialResult` for more information about its
    working.

    """

    @abc.abstractmethod
    def __call__(self, ua: str, domain: Domain, /) -> PartialResult:
        """Resolves the ``ua``."""
        ...


T = TypeVar("T")


class Matcher(abc.ABC, Generic[T]):
    """A matcher is an individual pattern-rule, able to match a user
    agent string and in case of success extract the relevant data.

    Matchers need to expose their pattern for bulk resolvers.

    """

    @abc.abstractmethod
    def __call__(self, ua: str) -> Optional[T]:
        """Applies the matcher to an input."""
        ...

    @property
    @abc.abstractmethod
    def regex(self) -> str:
        """Returns the matcher's pattern."""
        ...

    @property
    def flags(self) -> int:
        """Returns the matcher's pattern flags (only
        :data:`re.IGNORECASE` is supported, and only for
        :class:`Matcher` [:class:`Device`])

        """
        return 0


Matchers = Tuple[
    List[Matcher[UserAgent]],
    List[Matcher[OS]],
    List[Matcher[Device]],
]
