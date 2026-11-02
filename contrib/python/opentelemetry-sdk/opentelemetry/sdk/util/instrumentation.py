# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
import fnmatch
from collections.abc import Callable
from json import dumps

from typing_extensions import deprecated

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.util.types import (  # TODO: see if we can remove F401 when using new sphinx version # noqa: F401 # pylint: disable=unused-import
    AnyValue,
    Attributes,
    _ExtendedAttributes,
)


class InstrumentationInfo:
    """Immutable information about an instrumentation library module.

    See `opentelemetry.trace.TracerProvider.get_tracer` for the meaning of these
    properties.
    """

    __slots__ = ("_name", "_version", "_schema_url")

    @deprecated(
        "You should use InstrumentationScope. Deprecated since version 1.11.1."
    )
    def __init__(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
    ):
        self._name = name
        self._version = version
        if schema_url is None:
            schema_url = ""
        self._schema_url = schema_url

    def __repr__(self):
        return f"{type(self).__name__}({self._name}, {self._version}, {self._schema_url})"

    def __hash__(self):
        return hash((self._name, self._version, self._schema_url))

    def __eq__(self, value):
        return type(value) is type(self) and (
            self._name,
            self._version,
            self._schema_url,
        ) == (value._name, value._version, value._schema_url)

    def __lt__(self, value):
        if type(value) is not type(self):
            return NotImplemented
        return (self._name, self._version, self._schema_url) < (
            value._name,
            value._version,
            value._schema_url,
        )

    @property
    def schema_url(self) -> str | None:
        return self._schema_url

    @property
    def version(self) -> str | None:
        return self._version

    @property
    def name(self) -> str:
        return self._name


class InstrumentationScope:
    """A logical unit of the application code with which the emitted telemetry can be
    associated.

    See `opentelemetry.trace.TracerProvider.get_tracer` for the meaning of these
    properties.
    """

    __slots__ = ("_name", "_version", "_schema_url", "_attributes")

    def __init__(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
        attributes: _ExtendedAttributes | None = None,
    ) -> None:
        self._name = name
        self._version = version
        if schema_url is None:
            schema_url = ""
        self._schema_url = schema_url
        self._attributes = BoundedAttributes(attributes=attributes)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._name}, {self._version}, {self._schema_url}, {self._attributes})"

    def __hash__(self) -> int:
        return hash((self._name, self._version, self._schema_url))

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, InstrumentationScope):
            return NotImplemented
        return (
            self._name,
            self._version,
            self._schema_url,
            self._attributes,
        ) == (
            value._name,
            value._version,
            value._schema_url,
            value._attributes,
        )

    def __lt__(self, value: object) -> bool:
        if not isinstance(value, InstrumentationScope):
            return NotImplemented
        return (
            self._name,
            self._version,
            self._schema_url,
            self._attributes,
        ) < (
            value._name,
            value._version,
            value._schema_url,
            value._attributes,
        )

    @property
    def schema_url(self) -> str | None:
        return self._schema_url

    @property
    def version(self) -> str | None:
        return self._version

    @property
    def name(self) -> str:
        return self._name

    @property
    def attributes(self) -> Attributes:
        return self._attributes

    def to_json(self, indent: int | None = 4) -> str:
        return dumps(
            {
                "name": self._name,
                "version": self._version,
                "schema_url": self._schema_url,
                "attributes": (
                    dict(self._attributes) if bool(self._attributes) else None
                ),
            },
            indent=indent,
        )


_InstrumentationScopePredicateT = Callable[[InstrumentationScope], bool]


def _scope_name_matches_glob(
    glob_pattern: str,
) -> _InstrumentationScopePredicateT:
    def inner(scope: InstrumentationScope) -> bool:
        return fnmatch.fnmatch(scope.name, glob_pattern)

    return inner
