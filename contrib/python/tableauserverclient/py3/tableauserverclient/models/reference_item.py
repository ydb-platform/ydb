from typing_extensions import Self


class ResourceReference:
    def __init__(self, id_: str | None, tag_name: str) -> None:
        self.id = id_
        self.tag_name = tag_name

    def __str__(self) -> str:
        return f"<ResourceReference id={self._id} tag={self._tag_name}>"

    __repr__ = __str__

    def __eq__(self, other: object) -> bool:
        if not hasattr(other, "id") or not hasattr(other, "tag_name"):
            return False
        return (self.id == other.id) and (self.tag_name == other.tag_name)

    def __hash__(self: Self) -> int:
        return hash((self.id, self.tag_name))

    @property
    def id(self) -> str | None:
        return self._id

    @id.setter
    def id(self, value: str | None) -> None:
        self._id = value

    @property
    def tag_name(self) -> str:
        return self._tag_name

    @tag_name.setter
    def tag_name(self, value: str) -> None:
        self._tag_name = value
