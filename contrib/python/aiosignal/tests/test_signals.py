import re
import sys
from unittest import mock

import pytest

from aiosignal import Signal

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack


class Owner:
    def __repr__(self) -> str:
        return "<Owner 0xdeadbeef>"


@pytest.fixture
def owner() -> Owner:
    return Owner()


async def test_signal_positional_args(owner: Owner) -> None:
    async def callback(a: int, b: str) -> None:
        return

    signal = Signal[int, str](owner)
    signal.append(callback)
    signal.freeze()
    await signal.send(42, "foo")


async def test_add_signal_handler_not_a_callable(owner: Owner) -> None:
    callback = True
    signal = Signal(owner)
    signal.append(callback)  # type: ignore[arg-type]
    signal.freeze()
    with pytest.raises(TypeError):
        await signal.send()


async def test_function_signal_dispatch_kwargs(owner: Owner) -> None:
    signal = Signal(owner)
    kwargs = {"foo": 1, "bar": 2}

    callback_mock = mock.Mock()

    async def callback(**kwargs: object) -> None:
        callback_mock(**kwargs)

    signal.append(callback)
    signal.freeze()

    await signal.send(**kwargs)
    callback_mock.assert_called_once_with(**kwargs)


async def test_function_signal_dispatch_args_kwargs(owner: Owner) -> None:
    signal = Signal[Unpack[tuple[str, ...]]](owner)
    kwargs = {"foo": 1, "bar": 2}

    callback_mock = mock.Mock()

    async def callback(*args: str, **kwargs: object) -> None:
        callback_mock(*args, **kwargs)

    signal.append(callback)
    signal.freeze()

    await signal.send("a", "b", **kwargs)
    callback_mock.assert_called_once_with("a", "b", **kwargs)


async def test_non_coroutine(owner: Owner) -> None:
    signal = Signal(owner)
    kwargs = {"foo": 1, "bar": 2}

    callback = mock.Mock()

    signal.append(callback)
    signal.freeze()

    with pytest.raises(TypeError):
        await signal.send(**kwargs)


def test_setitem(owner: Owner) -> None:
    signal = Signal(owner)
    m1 = mock.Mock()
    signal.append(m1)
    assert signal[0] is m1
    m2 = mock.Mock()
    signal[0] = m2
    assert signal[0] is m2


def test_delitem(owner: Owner) -> None:
    signal = Signal(owner)
    m1 = mock.Mock()
    signal.append(m1)
    assert len(signal) == 1
    del signal[0]
    assert len(signal) == 0


def test_cannot_append_to_frozen_signal(owner: Owner) -> None:
    signal = Signal(owner)
    m1 = mock.Mock()
    m2 = mock.Mock()
    signal.append(m1)
    signal.freeze()
    with pytest.raises(RuntimeError):
        signal.append(m2)

    assert list(signal) == [m1]


def test_cannot_setitem_in_frozen_signal(owner: Owner) -> None:
    signal = Signal(owner)
    m1 = mock.Mock()
    m2 = mock.Mock()
    signal.append(m1)
    signal.freeze()
    with pytest.raises(RuntimeError):
        signal[0] = m2

    assert list(signal) == [m1]


def test_cannot_delitem_in_frozen_signal(owner: Owner) -> None:
    signal = Signal(owner)
    m1 = mock.Mock()
    signal.append(m1)
    signal.freeze()
    with pytest.raises(RuntimeError):
        del signal[0]

    assert list(signal) == [m1]


async def test_cannot_send_non_frozen_signal(owner: Owner) -> None:
    signal = Signal(owner)

    callback_mock = mock.Mock()

    async def callback(**kwargs: object) -> None:
        callback_mock(**kwargs)  # pragma: no cover  # mustn't be called

    signal.append(callback)

    with pytest.raises(RuntimeError):
        await signal.send()

    assert not callback_mock.called


def test_repr(owner: Owner) -> None:
    signal = Signal(owner)

    signal.append(mock.Mock(__repr__=lambda *a: "<callback>"))

    assert (
        re.match(
            r"<Signal owner=<Owner 0xdeadbeef>, frozen=False, " r"\[<callback>\]>",
            repr(signal),
        )
        is not None
    )

async def test_decorator_callback_dispatch_args_kwargs(owner: Owner) -> None:
    signal = Signal(owner)
    args = {"a", "b"}
    kwargs = {"foo": 1, "bar": 2}

    callback_mock = mock.Mock()

    @signal
    async def callback(*args: object, **kwargs: object) -> None:
        callback_mock(*args, **kwargs)

    signal.freeze()
    await signal.send(*args, **kwargs)
