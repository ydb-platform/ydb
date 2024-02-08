from pyrsistent._compat import Enum

from pyrsistent import field, pvector_field


# NB: This derives from the internal `pyrsistent._compat.Enum` in order to
# simplify coverage across python versions. Since we use
# `pyrsistent._compat.Enum` in `pyrsistent`'s implementation, it's useful to
# use it in the test coverage as well, for consistency.
class TestEnum(Enum):
    x = 1
    y = 2


def test_enum():
    f = field(type=TestEnum)

    assert TestEnum in f.type
    assert len(f.type) == 1


# This is meant to exercise `_seq_field`.
def test_pvector_field_enum_type():
    f = pvector_field(TestEnum)

    assert len(f.type) == 1
    assert TestEnum is list(f.type)[0].__type__
