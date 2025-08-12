# FIXME:
# mypy: disable-error-code="misc"

from collections.abc import MutableSequence
from copy import deepcopy

import pytest

from frozenlist import FrozenList, PyFrozenList


class FrozenListMixin:
    FrozenList = NotImplemented

    SKIP_METHODS = {
        "__abstractmethods__",
        "__slots__",
        "__static_attributes__",
        "__firstlineno__",
    }

    def test___class_getitem__(self) -> None:
        assert self.FrozenList[str] is not None

    def test_subclass(self) -> None:
        assert issubclass(self.FrozenList, MutableSequence)

    def test_iface(self) -> None:
        for name in set(dir(MutableSequence)) - self.SKIP_METHODS:
            if name.startswith("_") and not name.endswith("_"):
                continue
            assert hasattr(self.FrozenList, name)

    def test_ctor_default(self) -> None:
        _list = self.FrozenList([])
        assert not _list.frozen

    def test_ctor(self) -> None:
        _list = self.FrozenList([1])
        assert not _list.frozen

    def test_ctor_copy_list(self) -> None:
        orig = [1]
        _list = self.FrozenList(orig)
        del _list[0]
        assert _list != orig

    def test_freeze(self) -> None:
        _list = self.FrozenList()
        _list.freeze()
        assert _list.frozen

    def test_repr(self) -> None:
        _list = self.FrozenList([1])
        assert repr(_list) == "<FrozenList(frozen=False, [1])>"
        _list.freeze()
        assert repr(_list) == "<FrozenList(frozen=True, [1])>"

    def test_getitem(self) -> None:
        _list = self.FrozenList([1, 2])
        assert _list[1] == 2

    def test_setitem(self) -> None:
        _list = self.FrozenList([1, 2])
        _list[1] = 3
        assert _list[1] == 3

    def test_delitem(self) -> None:
        _list = self.FrozenList([1, 2])
        del _list[0]
        assert len(_list) == 1
        assert _list[0] == 2

    def test_len(self) -> None:
        _list = self.FrozenList([1])
        assert len(_list) == 1

    def test_iter(self) -> None:
        _list = self.FrozenList([1, 2])
        assert list(iter(_list)) == [1, 2]

    def test_reversed(self) -> None:
        _list = self.FrozenList([1, 2])
        assert list(reversed(_list)) == [2, 1]

    def test_eq(self) -> None:
        _list = self.FrozenList([1])
        assert _list == [1]

    def test_ne(self) -> None:
        _list = self.FrozenList([1])
        assert _list != [2]

    def test_le(self) -> None:
        _list = self.FrozenList([1])
        assert _list <= [1]

    def test_lt(self) -> None:
        _list = self.FrozenList([1])
        assert _list < [3]

    def test_ge(self) -> None:
        _list = self.FrozenList([1])
        assert _list >= [1]

    def test_gt(self) -> None:
        _list = self.FrozenList([2])
        assert _list > [1]

    def test_insert(self) -> None:
        _list = self.FrozenList([2])
        _list.insert(0, 1)
        assert _list == [1, 2]

    def test_frozen_setitem(self) -> None:
        _list = self.FrozenList([1])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list[0] = 2

    def test_frozen_delitem(self) -> None:
        _list = self.FrozenList([1])
        _list.freeze()
        with pytest.raises(RuntimeError):
            del _list[0]

    def test_frozen_insert(self) -> None:
        _list = self.FrozenList([1])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list.insert(0, 2)

    def test_contains(self) -> None:
        _list = self.FrozenList([2])
        assert 2 in _list

    def test_iadd(self) -> None:
        _list = self.FrozenList([1])
        _list += [2]
        assert _list == [1, 2]

    def test_iadd_frozen(self) -> None:
        _list = self.FrozenList([1])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list += [2]
        assert _list == [1]

    def test_index(self) -> None:
        _list = self.FrozenList([1])
        assert _list.index(1) == 0

    def test_remove(self) -> None:
        _list = self.FrozenList([1])
        _list.remove(1)
        assert len(_list) == 0

    def test_remove_frozen(self) -> None:
        _list = self.FrozenList([1])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list.remove(1)
        assert _list == [1]

    def test_clear(self) -> None:
        _list = self.FrozenList([1])
        _list.clear()
        assert len(_list) == 0

    def test_clear_frozen(self) -> None:
        _list = self.FrozenList([1])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list.clear()
        assert _list == [1]

    def test_extend(self) -> None:
        _list = self.FrozenList([1])
        _list.extend([2])
        assert _list == [1, 2]

    def test_extend_frozen(self) -> None:
        _list = self.FrozenList([1])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list.extend([2])
        assert _list == [1]

    def test_reverse(self) -> None:
        _list = self.FrozenList([1, 2])
        _list.reverse()
        assert _list == [2, 1]

    def test_reverse_frozen(self) -> None:
        _list = self.FrozenList([1, 2])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list.reverse()
        assert _list == [1, 2]

    def test_pop(self) -> None:
        _list = self.FrozenList([1, 2])
        assert _list.pop(0) == 1
        assert _list == [2]

    def test_pop_default(self) -> None:
        _list = self.FrozenList([1, 2])
        assert _list.pop() == 2
        assert _list == [1]

    def test_pop_frozen(self) -> None:
        _list = self.FrozenList([1, 2])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list.pop()
        assert _list == [1, 2]

    def test_append(self) -> None:
        _list = self.FrozenList([1, 2])
        _list.append(3)
        assert _list == [1, 2, 3]

    def test_append_frozen(self) -> None:
        _list = self.FrozenList([1, 2])
        _list.freeze()
        with pytest.raises(RuntimeError):
            _list.append(3)
        assert _list == [1, 2]

    def test_hash(self) -> None:
        _list = self.FrozenList([1, 2])
        with pytest.raises(RuntimeError):
            hash(_list)

    def test_hash_frozen(self) -> None:
        _list = self.FrozenList([1, 2])
        _list.freeze()
        h = hash(_list)
        assert h == hash((1, 2))

    def test_dict_key(self) -> None:
        _list = self.FrozenList([1, 2])
        with pytest.raises(RuntimeError):
            {_list: "hello"}
        _list.freeze()
        {_list: "hello"}

    def test_count(self) -> None:
        _list = self.FrozenList([1, 2])
        assert _list.count(1) == 1

    def test_deepcopy_unfrozen(self) -> None:
        orig = self.FrozenList([1, 2, 3])
        copied = deepcopy(orig)
        assert copied == orig
        assert copied is not orig
        assert list(copied) == list(orig)
        assert not copied.frozen
        # Verify the copy is mutable
        copied.append(4)
        assert len(copied) == 4
        assert len(orig) == 3

    def test_deepcopy_frozen(self) -> None:
        orig = self.FrozenList([1, 2, 3])
        orig.freeze()
        copied = deepcopy(orig)
        assert copied == orig
        assert copied is not orig
        assert list(copied) == list(orig)
        assert copied.frozen
        # Verify the copy is also frozen
        with pytest.raises(RuntimeError):
            copied.append(4)

    def test_deepcopy_nested(self) -> None:
        inner = self.FrozenList([1, 2])
        orig = self.FrozenList([inner, 3])
        copied = deepcopy(orig)
        assert copied == orig
        assert copied[0] is not orig[0]
        assert isinstance(copied[0], self.FrozenList)
        # Modify the inner list in the copy
        copied[0].append(3)
        assert len(copied[0]) == 3
        assert len(orig[0]) == 2

    def test_deepcopy_circular(self) -> None:
        orig = self.FrozenList([1, 2])
        orig.append(orig)  # Create circular reference

        copied = deepcopy(orig)

        # Check structure is preserved
        assert len(copied) == 3
        assert copied[0] == 1
        assert copied[1] == 2
        assert copied[2] is copied  # Circular reference preserved

        # Verify they are different objects
        assert copied is not orig
        assert copied[2] is not orig

        # Modify the copy
        copied.append(3)
        assert len(copied) == 4
        assert len(orig) == 3

    def test_deepcopy_circular_frozen(self) -> None:
        orig = self.FrozenList([1, 2])
        orig.append(orig)  # Create circular reference
        orig.freeze()

        copied = deepcopy(orig)

        # Check structure is preserved
        assert len(copied) == 3
        assert copied[0] == 1
        assert copied[1] == 2
        assert copied[2] is copied  # Circular reference preserved
        assert copied.frozen

        # Verify frozen state
        with pytest.raises(RuntimeError):
            copied.append(3)

    def test_deepcopy_nested_circular(self) -> None:
        # Create a complex nested structure with circular references
        inner1 = self.FrozenList([1, 2])
        inner2 = self.FrozenList([3, 4])
        orig = self.FrozenList([inner1, inner2])

        # Add circular references
        inner1.append(inner2)  # inner1 -> inner2
        inner2.append(orig)  # inner2 -> orig (outer list)
        orig.append(orig)  # orig -> orig (self reference)

        copied = deepcopy(orig)

        # Verify structure
        assert len(copied) == 3
        assert isinstance(copied[0], self.FrozenList)
        assert isinstance(copied[1], self.FrozenList)
        assert copied[2] is copied  # Self reference preserved

        # Verify nested circular references
        assert len(copied[0]) == 3
        assert copied[0][2] is copied[1]  # inner1 -> inner2 preserved
        assert len(copied[1]) == 3
        assert copied[1][2] is copied  # inner2 -> orig preserved

        # All objects should be new instances
        assert copied is not orig
        assert copied[0] is not orig[0]
        assert copied[1] is not orig[1]

    def test_deepcopy_multiple_references(self) -> None:
        # Test that multiple references to the same object are preserved
        shared = self.FrozenList([1, 2])
        orig = self.FrozenList([shared, shared, 3])

        copied = deepcopy(orig)

        # Both references should point to the same copied object
        assert copied[0] is copied[1]
        assert copied[0] is not shared
        assert isinstance(copied[0], self.FrozenList)

        # Modify through one reference
        copied[0].append(3)
        assert len(copied[0]) == 3
        assert len(copied[1]) == 3  # Should see the change
        assert len(shared) == 2  # Original unchanged


class TestFrozenList(FrozenListMixin):
    FrozenList = FrozenList  # type: ignore[assignment]  # FIXME


class TestFrozenListPy(FrozenListMixin):
    FrozenList = PyFrozenList  # type: ignore[assignment]  # FIXME
