# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from .. import _typing as t  # noqa: TC001


class UnsupportedType:
    """
    Represents a type unknown to the driver, received from the server.

    This type is used, for instance, when a newer DBMS produces a result
    containing a type that the current version of the driver does not yet
    understand.

    Note that this type may only be received from the server but cannot be
    sent to the server (e.g., as a query parameter).

    The attributes exposed by this type are meant for displaying and debugging
    purposes.
    They may change in future versions of the server and should not be relied
    upon for any logic in your application.
    If your application requires handling this type, you must upgrade your
    driver to a version that supports it.
    """

    _name: str
    _minimum_protocol_version: tuple[int, int]
    _message: str | None

    @classmethod
    def _new(
        cls,
        name: str,
        minimum_protocol_version: tuple[int, int],
        message: str | None,
    ) -> t.Self:
        obj = cls.__new__(cls)
        obj._name = name
        obj._minimum_protocol_version = minimum_protocol_version
        obj._message = message
        return obj

    @property
    def name(self) -> str:
        """The name of the type."""
        return self._name

    @property
    def minimum_protocol_version(self) -> tuple[int, int]:
        """
        The minimum required Bolt protocol version that supports this type.

        This is a 2-:class:`tuple` of ``(major, minor)`` integers.

        To understand which driver version this corresponds to, refer to the
        driver's release notes or documentation.

        .. note::
            Bolt versions are not generally equivalent to driver versions.
            See the `driver manual`_ for which driver version is required for
            new types.

        .. _driver manual:
            https://neo4j.com/docs/python-manual/current/data-types/
        """
        return self._minimum_protocol_version

    @property
    def message(self) -> str | None:
        """
        Optional, further details about this type.

        Any additional information provided by the server about this type.
        """
        return self._message

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self._name}>"

    def __repr__(self) -> str:
        args = [
            f" name={self._name!r}",
            f" minimum_protocol_version={self._minimum_protocol_version!r}",
        ]
        if self._message is not None:
            args.append(f" message={self._message!r}")
        return f"<{self.__class__.__name__}{''.join(args)}>"
