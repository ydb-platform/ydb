from typing import Any, ClassVar, Protocol, runtime_checkable


class _CachedProtocolMeta(Protocol.__class__):  # type: ignore[name-defined]
    """Custom implementation of @runtime_checkable

    The native implementation of @runtime_checkable is slow,
    see <https://github.com/zarr-developers/numcodecs/issues/379>.

    This metaclass keeps an unbounded cache of the result of
    isinstance checks using the object's class as the cache key.
    """

    _instancecheck_cache: ClassVar[dict[tuple[type, type], bool]] = {}

    def __instancecheck__(self, instance):
        key = (self, instance.__class__)
        ret = self._instancecheck_cache.get(key)
        if ret is None:
            ret = super().__instancecheck__(instance)
            self._instancecheck_cache[key] = ret
        return ret


@runtime_checkable
class DType(Protocol, metaclass=_CachedProtocolMeta):
    itemsize: int
    name: str
    kind: str


@runtime_checkable
class FlagsObj(Protocol, metaclass=_CachedProtocolMeta):
    c_contiguous: bool
    f_contiguous: bool
    owndata: bool


@runtime_checkable
class NDArrayLike(Protocol, metaclass=_CachedProtocolMeta):
    dtype: DType
    shape: tuple[int, ...]
    strides: tuple[int, ...]
    ndim: int
    size: int
    itemsize: int
    nbytes: int
    flags: FlagsObj

    def __len__(self) -> int: ...  # pragma: no cover

    def __getitem__(self, key) -> Any: ...  # pragma: no cover

    def __setitem__(self, key, value): ...  # pragma: no cover

    def tobytes(self, order: str | None = ...) -> bytes: ...  # pragma: no cover

    def reshape(self, *shape: int, order: str = ...) -> "NDArrayLike": ...  # pragma: no cover

    def view(self, dtype: DType = ...) -> "NDArrayLike": ...  # pragma: no cover


def is_ndarray_like(obj: object) -> bool:
    """Return True when `obj` is ndarray-like"""
    return isinstance(obj, NDArrayLike)
