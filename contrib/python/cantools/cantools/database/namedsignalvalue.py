from typing import Any


class NamedSignalValue:
    """Represents a named value of a signal.

    Named values map an integer number to a human-readable
    string. Some file formats like ARXML support specifying
    descriptions for the named value.
    """

    def __init__(
        self,
        value: int,
        name: str,
        comments: dict[str, str] | None = None,
    ) -> None:
        #: The text intended for human consumption which the specified integer
        #: is mapped to.
        self.name = name

        #: The integer value that gets mapped.
        self.value = value

        self._comments = comments or {}

    @property
    def comments(self) -> dict[str, str]:
        """The descriptions of the named value

        This is a dictionary containing the descriptions in multiple
        languages. The dictionary is indexed by the language.

        Example:

        .. code:: text

          # retrieve the English comment of the named value or an empty
          # string if none was specified.
          named_value.comments.get("EN", "")

        """

        return self._comments

    def __str__(self) -> str:
        return f"{self.name}"

    def __repr__(self) -> str:
        return f"'{self.name}'"

    def __eq__(self, x: Any) -> bool:
        if isinstance(x, NamedSignalValue):
            return (
                x.value == self.value
                and x.name == self.name
                and x.comments == self.comments
            )
        elif isinstance(x, str):
            return x == self.name

        return False
