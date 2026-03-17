from typing import Any

from ..errors import Error as _Error


class Error(_Error):
    pass


class ParseError(Error):
    pass


class EncodeError(Error):
    pass


class DecodeError(Error):
    pass


class UnsupportedDatabaseFormatError(Error):
    """This exception is raised when
    :func:`~cantools.database.load_file()`,
    :func:`~cantools.database.load()` and
    :func:`~cantools.database.load_string()` are unable to parse given
    database file or string.

    """

    def __init__(self,
                 e_arxml: Exception | None,
                 e_dbc: Exception | None,
                 e_kcd: Exception | None,
                 e_sym: Exception | None,
                 e_cdd: Exception | None) -> None:
        message_chunks: list[str] = []

        if e_arxml is not None:
            message_chunks.append(f'ARXML: "{e_arxml}"')

        if e_dbc is not None:
            message_chunks.append(f'DBC: "{e_dbc}"')

        if e_kcd is not None:
            message_chunks.append(f'KCD: "{e_kcd}"')

        if e_sym is not None:
            message_chunks.append(f'SYM: "{e_sym}"')

        if e_cdd is not None:
            message_chunks.append(f'CDD: "{e_cdd}"')

        message = ', '.join(message_chunks)

        super().__init__(message)

        self.e_arxml = e_arxml
        self.e_dbc = e_dbc
        self.e_kcd = e_kcd
        self.e_sym = e_sym
        self.e_cdd = e_cdd

    def __reduce__(self) -> str | tuple[Any, ...]:
        return type(self), (self.e_arxml, self.e_dbc, self.e_kcd, self.e_sym, self.e_cdd), {}
