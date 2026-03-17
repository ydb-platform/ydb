#  Copyright (c) Kuba Szczodrzyński 2023-1-7.

import dataclasses
from dataclasses import Field, is_dataclass
from io import BytesIO
from typing import Any, Callable, Optional, Type, Union

from datastruct.main import DataStruct
from datastruct.types import Adapter, Context, Eval, Hook, IOHook, T, Value
from datastruct.utils.context import evaluate
from datastruct.utils.misc import pad_up

from .special import action, hook, hook_end, io, io_end
from .standard import built, field
from .wrapper import adapter, repeat


def packing(check: Value[T]) -> Eval[T]:
    return lambda ctx: check(ctx) if ctx.G.packing and not ctx.G.sizing else None


def unpacking(check: Value[T]) -> Eval[T]:
    return lambda ctx: check(ctx) if ctx.G.unpacking else None


def virtual(value: Value[T]):
    return adapter(
        encode=lambda v, ctx: b"",
        decode=lambda v, ctx: evaluate(ctx, value),
    )(built(0, builder=value, always=True))


def tell(*, relative: bool = False):
    return virtual(lambda ctx: ctx.P.tell() if relative else ctx.G.tell())


def tell_into(into: str):
    return action(lambda ctx: setattr(ctx, into, ctx.G.tell()))


def const_into(into: str, value: Any):
    return action(lambda ctx: setattr(ctx, into, value))


def eval_into(into: str, value: Value[Any]):
    return action(lambda ctx: setattr(ctx, into, evaluate(ctx, value)))


def const(const_value: Any, doc: str = None):
    class Const(Adapter):
        def encode(self, value: Any, ctx: Context) -> Any:
            return self.decode(value, ctx)

        def decode(self, value: Any, ctx: Context) -> Any:
            if value == const_value:
                return value
            message = f"read {value}; expected {const_value}"
            if not doc:
                raise ValueError(f"Const validation failed; {message}")
            raise ValueError(f"Const validation failed at '{doc}'; {message}")

    def wrap(base: Field):
        base.default = const_value
        return adapter(Const())(base)

    return wrap


def bytestr(length: Value[int], *, default: bytes = ..., padding: bytes = None):
    class ByteStr(Adapter):
        def encode(self, value: bytes, ctx: Context) -> bytes:
            return value.ljust(
                evaluate(ctx, length), padding or ctx.P.config.padding_pattern
            )

        def decode(self, value: bytes, ctx: Context) -> bytes:
            return value.rstrip(padding or ctx.P.config.padding_pattern)

    return adapter(ByteStr())(field(length, default=default))


def varbytes(length: Value[int], *, default: bytes = ...):
    return field(
        lambda ctx: (len(ctx.P.self) if ctx.G.packing else evaluate(ctx, length)),
        default=default,
    )


def text(
    length: Value[int],
    *,
    default: str = ...,
    encoding: str = "utf-8",
    padding: bytes = None,
    variable: bool = False,
):
    class Text(Adapter):
        def encode(self, value: str, ctx: Context) -> bytes:
            if variable:
                return value.encode(encoding)
            return value.encode(encoding).ljust(
                evaluate(ctx, length),
                padding or ctx.P.config.padding_pattern,
            )

        def decode(self, value: bytes, ctx: Context) -> str:
            return value.rstrip(
                padding or ctx.P.config.padding_pattern,
            ).decode(encoding)

    return adapter(Text())(
        field(
            lambda ctx: (
                len(ctx.P.self) if variable and ctx.G.packing else evaluate(ctx, length)
            ),
            default=default,
        )
    )


def vartext(length: Value[int], **kwargs):
    return text(length, variable=True, **kwargs)


def varlist(count: Value[int] = None, *, when: Eval[bool] = None):
    if count is not None:
        return repeat(
            when=lambda ctx: (
                (ctx.G.packing and ctx.P.i < len(ctx.P.self))
                or (ctx.G.unpacking and ctx.P.i < evaluate(ctx, count))
            )
        )
    if when is not None:
        return repeat(
            when=lambda ctx: (
                (ctx.G.packing and ctx.P.i < len(ctx.P.self))
                or (ctx.G.unpacking and when(ctx))
            )
        )
    raise TypeError("Either count or when has to be specified")


def probe():
    def _probe(ctx: Context):
        print(f"Probe: {ctx}")

    return action(_probe)


def validate(check: Eval[bool], doc: str = None):
    def _validate(ctx: Context):
        if not check(ctx):
            if not doc:
                raise ValueError(f"Validation failed")
            raise ValueError(f"Validation failed at '{doc}'")

    return action(_validate)


def buffer_start(end: Callable[[BytesIO, Context], None]):
    class Buffer(Hook):
        io: BytesIO

        def init(self, ctx: Context) -> None:
            self.io = BytesIO()

        def update(self, value: bytes, ctx: Context) -> Optional[bytes]:
            self.io.write(value)
            return value

        def end(self, ctx: Context) -> None:
            end(self.io, ctx)

    return hook(Buffer())


def buffer_end(buffer: Field):
    return hook_end(buffer)


def _checksum_validate(value: Any, calc: Any, doc: str):
    if value != calc:
        message = f"read {value}; calculated {calc}"
        if not doc:
            raise ValueError(f"Checksum invalid; {message}")
        raise ValueError(f"Checksum invalid at '{doc}'; {message}")


def checksum_start(
    init: Callable[[Context], T],
    update: Callable[[bytes, T, Context], Optional[T]],
    end: Callable[[T, Context], Any],
    *,
    target: Any = None,
):
    class Checksum(Hook):
        obj: T
        target: Any

        def init(self, ctx: Context) -> None:
            self.obj = init(ctx)

        def update(self, value: bytes, ctx: Context) -> Optional[bytes]:
            ret = update(value, self.obj, ctx)
            if ret is not None:
                self.obj = ret
            return value

        def end(self, ctx: Context) -> None:
            ctx.P.hook_checksum = end(self.obj, ctx)
            if "hook_checksum_pre" in ctx.P:
                # validate checksum_field() read before data
                _checksum_validate(
                    value=ctx.P.hook_checksum_pre,
                    calc=ctx.P.hook_checksum,
                    doc=ctx.P.hook_checksum_doc,
                )

    hook_obj = Checksum()
    # save target checksum_field() if before data
    hook_obj.target = target
    return hook(hook_obj)


def checksum_end(checksum: Field):
    return hook_end(checksum)


def checksum_field(doc: str):
    target: Field

    class Checksum(Adapter):
        def encode(self, value: Any, ctx: Context) -> Any:
            if "hook_checksum" not in ctx.P:
                # checksum before data - pack data and calculate now
                ds: DataStruct = ctx.self
                start = None
                names = []
                end = None
                data_pos = 0

                # find checksum_start() and checksum_end()
                # list any data fields in between
                for field, meta, _ in ds.fields():
                    if getattr(meta.hook, "target", None) == target:
                        # checksum_start()/_end() field found
                        if meta.end:
                            end = field.name
                            break
                        start = field.name
                        names = []
                        continue
                    if start:
                        names.append(field.name)
                    else:
                        # calculate struct-relative offset of data start
                        data_pos += ds.sizeof(field.name)

                # validate search results
                if not start:
                    raise ValueError("Pass target=<checksum field> to checksum_start()")
                if not names:
                    raise ValueError("Checksum data fields not found")
                if not end:
                    raise ValueError("checksum_end() field not found")

                # list all fields to include in packing
                # include checksum start & end
                names = [start] + names + [end]
                # output context parameter
                ctx_out = []
                # create a dummy IO that can tell() the correct absolute position
                padding = BytesIO()
                padding.seek(ctx.G.io.tell())
                # pack fields into the context
                ds.pack(
                    io=padding,
                    field_names=names,
                    ctx_out=ctx_out,
                    # add a fake offset to account for partial field packing
                    tell_offset=data_pos,
                    **ctx.P.kwargs,
                )
                # return calculation made by checksum_end()
                return ctx_out[0].P.hook_checksum

            # writing - return the valid checksum
            return ctx.P.hook_checksum

        def decode(self, value: Any, ctx: Context) -> Any:
            # reading
            if "hook_checksum" not in ctx.P:
                # checksum before data - schedule for checksum_end()
                ctx.P.hook_checksum_doc = doc
                ctx.P.hook_checksum_pre = value
                return value
            # validate the checksum
            _checksum_validate(
                value=value,
                calc=ctx.P.hook_checksum,
                doc=doc,
            )
            return value

    def wrap(base: Field):
        nonlocal target
        target = base
        return adapter(Checksum())(base)

    return wrap


def bitfield(
    fmt: str,
    cls: Type[T],
    default: Union[bytes, int, None] = None,
    byteswap: str = None,
):
    try:
        import bitstruct
    except (ModuleNotFoundError, ImportError):
        raise ImportError(
            "'bitstruct' package is not found, but required for bitfield()"
        )
    if not is_dataclass(cls):
        raise TypeError("'cls' must be a dataclass")
    size = bitstruct.calcsize(fmt) // 8
    if isinstance(default, int):
        default = default.to_bytes(length=size, byteorder="little")

    def encode(value: T, *_) -> bytes:
        data = dataclasses.astuple(value)
        packed = bitstruct.pack(fmt, *data)
        if byteswap:
            return bitstruct.byteswap(byteswap, packed)
        return packed

    def decode(value: bytes, *_) -> T:
        if byteswap:
            value = bitstruct.byteswap(byteswap, value)
        data = bitstruct.unpack(fmt, value)
        return cls(*data)

    return adapter(encode=encode, decode=decode)(
        field(
            size,
            default_factory=lambda: decode(default),
        )
    )


def crypt(
    block_size: int,
    init: Optional[Callable[[Context], T]],
    decrypt: Optional[Callable[[bytes, T, Context], bytes]],
    encrypt: Optional[Callable[[bytes, T, Context], bytes]],
    end: Optional[Callable[[T, Context], Any]] = None,
    block_single: bool = False,
):
    class Crypt(IOHook):
        obj: T
        read_buf: bytes = b""
        write_buf: bytes = b""

        def init(self, ctx: Context) -> None:
            if init:
                self.obj = init(ctx)
            self.read_buf = b""
            self.write_buf = b""

        def read(self, n: int) -> bytes:
            if not decrypt:
                return self.io_read(n)
            ret = b""
            while n:
                if not self.read_buf:
                    if block_single:
                        read_len = block_size
                    else:
                        read_len = n + pad_up(n, block_size)
                    self.read_buf = self.io_read(read_len)
                    if not self.read_buf:
                        return ret  # not enough data read; fail already
                    self.read_buf = decrypt(self.read_buf, self.obj, self.ctx)
                while self.read_buf and n:
                    chunk = self.read_buf[0:n]
                    self.read_buf = self.read_buf[n:]
                    ret += chunk
                    n -= len(chunk)
            return ret

        def write(self, s: bytes) -> int:
            if not encrypt:
                return self.io_write(s)
            self.write_buf += s
            while len(self.write_buf) >= block_size:
                if block_single:
                    write_len = block_size
                else:
                    write_len = len(self.write_buf)
                    write_len = write_len - (write_len % block_size)
                chunk = self.write_buf[0:write_len]
                self.write_buf = self.write_buf[write_len:]
                chunk = encrypt(chunk, self.obj, self.ctx)
                self.io_write(chunk)
            return len(s)

        def seek(self, offset: int, whence: int) -> int:
            raise NotImplementedError("Seeking with crypt() is not implemented yet")

        def tell(self) -> int:
            return self.io_tell() - len(self.read_buf) + len(self.write_buf)

        def end(self, ctx: Context) -> None:
            if end:
                end(self.obj, ctx)
            self.obj = None
            self.read_buf = b""
            self.write_buf = b""

    return io(Crypt())


def crypt_end(crypt_field: Field):
    return io_end(crypt_field)
