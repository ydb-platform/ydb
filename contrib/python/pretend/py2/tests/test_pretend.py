import operator

import pytest

from pretend import stub, raiser, call, call_recorder, PY3K


class TestStub(object):
    def test_attribute(self):
        x = stub(attr=3)
        assert hasattr(x, "attr")
        assert x.attr == 3

    def test_function(self):
        x = stub(meth=lambda x, y: x + y)
        assert x.meth(3, 4) == 7

    def test_iter(self):
        x = stub(__iter__=lambda: iter([1, 2, 3]))
        iterator = iter(x)
        assert next(iterator) == 1

    @pytest.mark.skipif("not PY3K")
    def test_next(self):
        x = stub(__next__=lambda: 12)
        assert next(x) == 12

    def test_contains(self):
        x = stub(__contains__=lambda other: True)
        assert "hello world" in x

    @pytest.mark.skipif("PY3K")
    def test_nonzero(self):
        x = stub(__nonzero__=lambda: False)
        assert not bool(x)

    @pytest.mark.skipif("not PY3K")
    def test_bool(self):
        x = stub(__bool__=lambda: False)
        assert not bool(x)

    def test_len(self):
        x = stub(__len__=lambda: 12)
        assert len(x) == 12

    @pytest.mark.parametrize(("func", "op"), [
        (operator.lt, "__lt__"),
        (operator.le, "__le__"),
        (operator.eq, "__eq__"),
        (operator.ne, "__ne__"),
        (operator.gt, "__gt__"),
        (operator.ge, "__ge__"),

        (operator.add, "__add__"),
        (operator.and_, "__and__"),
        (divmod, "__divmod__"),
        (operator.floordiv, "__floordiv__"),
        (operator.lshift, "__lshift__"),
        (operator.mod, "__mod__"),
        (operator.mul, "__mul__"),
        (operator.or_, "__or__"),
        (operator.pow, "__pow__"),
        (operator.rshift, "__rshift__"),
        (operator.sub, "__sub__"),
        (operator.truediv, "__truediv__"),
        (operator.xor, "__xor__"),
    ])
    def test_special_binops(self, func, op):
        x = stub(**{
            op: lambda y: func(2, y)
        })
        assert func(x, 4) == func(2, 4)
        assert func(x, 2) == func(2, 2)

    @pytest.mark.skipif("PY3K")
    def test_div(self):
        x = stub(
            __div__=lambda y: 4
        )
        assert x / 3 == 4

    def test_missing_op_error(self):
        x = stub()
        with pytest.raises(TypeError):
            x + 2

    def test_subscript(self):
        x = stub(
            __getitem__=lambda idx: idx
        )
        assert x[5] == 5
        assert x[1, 2] == (1, 2)

    def test_setitem(self):
        d = {}
        x = stub(
            __setitem__=d.__setitem__
        )
        x[5] = 'a'
        x['b'] = 6
        assert d == {5: 'a', 'b': 6}

    def test_delitem(self):
        d = {5: 'a', 'b': 6}
        x = stub(
            __delitem__=d.__delitem__
        )
        del x['b']
        assert d == {5: 'a'}

    def test_context_manager(self):
        should_reraise = True
        x = stub(
            __enter__=lambda: 3,
            __exit__=lambda exc_type, exc_value, tb: should_reraise
        )
        with x as value:
            assert value == 3
            raise ValueError
        should_reraise = False
        with pytest.raises(ValueError):
            with x as value:
                assert value == 3
                raise ValueError

    def test_default_repr(self):
        x = stub(a=10)

        assert repr(x) == "<stub(a=10)>"

    def test_custom_repr(self):
        x = stub(id=300, __repr__=lambda: '<Something>')

        assert x.id == 300
        assert repr(x) == '<Something>'

    def test_callable(self):
        x = stub(__call__=lambda: 4)

        assert x() == 4


class TestRaiser(object):
    def test_call_raiser(self):
        f = raiser(ValueError)
        with pytest.raises(ValueError):
            f()

    def test_call_raiser_exc_value(self):
        exc = ValueError(14)
        f = raiser(exc)
        with pytest.raises(ValueError) as exc_info:
            f()
        assert exc_info.value is exc

    def test_non_exc_raiser(self):
        with pytest.raises(TypeError):
            raiser("test")


class TestCallRecorder(object):
    def test_call_eq(self):
        assert call(a=2) == call(a=2)
        assert not (call(a=2) != call(a=2))
        assert call(a=2) != call(a=3)
        assert not (call(a=2) == call(a=3))

        assert call() != []

    def test_call_repr(self):
        assert repr(call(1, 2, a=3)) == "<call(1, 2, a=3)>"
        assert repr(call(a=2)) == "<call(a=2)>"

    def test_call_hash(self):
        c1 = call(a=2)
        c2 = call(a=2)
        assert hash(c1) == hash(c2)

    def test_simple(self):
        f = call_recorder(lambda *args, **kwargs: 3)
        assert f() == 3
        assert f.calls == [
            call()
        ]
