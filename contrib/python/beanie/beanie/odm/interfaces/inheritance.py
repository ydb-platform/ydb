from typing import (
    ClassVar,
    Dict,
    Optional,
    Type,
)


class InheritanceInterface:
    _children: ClassVar[Dict[str, Type]]
    _parent: ClassVar[Optional[Type]]
    _inheritance_inited: ClassVar[bool]
    _class_id: ClassVar[Optional[str]] = None

    @classmethod
    def add_child(cls, name: str, clas: Type):
        cls._children[name] = clas
        if cls._parent is not None:
            cls._parent.add_child(name, clas)
