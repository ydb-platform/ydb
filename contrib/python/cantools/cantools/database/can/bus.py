# A CAN bus.

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .formats.arxml.database_specifics import AutosarDatabaseSpecifics


class Bus:
    """A CAN bus.

    """

    def __init__(self,
                 name: str,
                 comment: str | dict[str | None, str] | None = None,
                 baudrate: int | None = None,
                 fd_baudrate: int | None = None,
                 autosar_specifics: Optional["AutosarDatabaseSpecifics"] = None) -> None:
        self._name = name

        # If the 'comment' argument is a string, we assume that is an
        # English comment. This is slightly hacky, because the
        # function's behavior depends on the type of the passed
        # argument, but it is quite convenient...
        if isinstance(comment, str):
            # use the first comment in the dictionary as "The" comment
            self._comments: dict[str | None, str] | None = { None: comment }
        else:
            # assume that we have either no comment at all or a
            # multi-lingual dictionary
            self._comments = comment

        self._baudrate = baudrate
        self._fd_baudrate = fd_baudrate

        self._autosar = autosar_specifics

    @property
    def name(self) -> str:
        """The bus name as a string.

        """

        return self._name

    @property
    def comment(self) -> str | None:
        """The bus' comment, or ``None`` if unavailable.

        Note that we implicitly try to return the English comment if
        multiple languages were specified.

        """
        if self._comments is None:
            return None
        elif self._comments.get(None) is not None:
            return self._comments.get(None)
        elif self._comments.get("FOR-ALL") is not None:
            return self._comments.get("FOR-ALL")

        return self._comments.get('EN')

    @property
    def comments(self) -> dict[str | None, str] | None:
        """The dictionary with the descriptions of the bus in multiple
        languages. ``None`` if unavailable.

        """
        return self._comments

    @property
    def baudrate(self) -> int | None:
        """The bus baudrate, or ``None`` if unavailable.

        """

        return self._baudrate

    @property
    def fd_baudrate(self) -> int | None:
        """The baudrate used for the payload of CAN-FD frames, or ``None`` if
        unavailable.

        """

        return self._fd_baudrate

    @property
    def autosar(self) -> Optional["AutosarDatabaseSpecifics"]:
        """An object containing AUTOSAR specific properties of the bus.

        """

        return self._autosar

    @autosar.setter
    def autosar(self, value: Any) -> None:
        self._autosar = value

    def __repr__(self) -> str:
        return "bus('{}', {})".format(
            self._name,
            "'" + self.comment + "'" if self.comment is not None else None)
