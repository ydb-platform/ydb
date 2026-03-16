import sys
from typing import Iterator

import pytest

if sys.version_info < (3, 9):
    from typing_extensions import Literal
else:
    from typing import Literal

from annotated_types import BaseMetadata, GroupedMetadata, Gt


def test_subclass_without_implementing_iter() -> None:
    with pytest.raises(TypeError):

        class Foo1(GroupedMetadata):
            pass

    class Foo2(GroupedMetadata):
        def __iter__(self) -> Iterator[BaseMetadata]:
            raise NotImplementedError

    with pytest.raises(NotImplementedError):
        for _ in Foo2():
            pass


def test_non_subclass_implementer() -> None:
    class Foo:
        __is_annotated_types_grouped_metadata__: Literal[True] = True

        def __iter__(self) -> Iterator[BaseMetadata]:
            return
            yield Gt(0)

    _: GroupedMetadata = Foo()  # type checker will fail if not valid
