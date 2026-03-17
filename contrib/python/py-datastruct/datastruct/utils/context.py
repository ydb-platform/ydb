#  Copyright (c) Kuba Szczodrzyński 2023-1-3.

from io import SEEK_CUR, SEEK_SET
from typing import IO, Optional

from datastruct.types import Config, Context, FieldMeta, V, Value


def evaluate(ctx: Context, v: Value[V]) -> V:
    value: V = v
    if callable(v):
        # print("Evaluating with ctx:", ctx)
        value = v(ctx)
        return value
    return value


def build_global_context(
    io: IO[bytes],
    packing: bool = False,
    unpacking: bool = False,
    sizing: bool = False,
) -> Context.Global:
    glob = Context.Global(
        io=io,
        io_hook=None,
        packing=packing,
        unpacking=unpacking,
        sizing=sizing,
        root=None,
        hooks=[],
        # tell the current position, relative to IO start
        tell=lambda: glob.io.tell(),
        # seek to a position, relative to IO start
        seek=lambda offset, whence=SEEK_SET: glob.io.seek(offset, whence),
    )
    return glob


def build_context(
    glob: Context.Global,
    parent: Optional[Context],
    config: Config,
    **kwargs,
) -> Context:
    # create a context with some helpers and passed 'values' (from self)
    io_offset = glob.io.tell()
    # build params container
    params = Context.Params(
        # current DataStruct's config
        config=config,
        # tell the current position, relative to struct start
        tell=lambda: glob.io.tell() - io_offset,
        # seek to a position, relative to struct start
        seek=lambda offset, whence=SEEK_SET: glob.io.seek(
            offset + (io_offset if whence == SEEK_SET else 0), whence
        ),
        # skip a number of bytes
        skip=lambda length: glob.io.seek(length, SEEK_CUR),
        # peek data by reading and seeking back
        peek=lambda length: (glob.io.read(length), glob.io.seek(-length, SEEK_CUR))[0],
        # context arguments
        kwargs=kwargs,
    )
    ctx = Context(_=parent, G=glob, P=params, **kwargs)
    # set this context as root, if not already set
    if glob.root is None:
        glob.root = ctx
    return ctx


def ctx_read(ctx: Context, n: int) -> bytes:
    if not n:
        return b""
    s = ctx.G.io.read(n)
    s = hook_do(ctx, "update", s)
    s = hook_do(ctx, "read", s)
    return s


def ctx_write(ctx: Context, s: bytes) -> int:
    if not s:
        return 0
    s = hook_do(ctx, "update", s)
    s = hook_do(ctx, "write", s)
    n = ctx.G.io.write(s)
    return n


def hook_apply(ctx: Context, meta: FieldMeta):
    hook = meta.hook
    if not meta.end:
        # add the hook to the list
        evaluate(ctx, hook.init)
        ctx.G.hooks.append(hook)
    else:
        # remove the hook
        ctx.G.hooks.remove(hook)
        evaluate(ctx, hook.end)


def hook_do(ctx: Context, action: str, data: V) -> V:
    for hook in ctx.G.hooks:
        func = getattr(hook, action, None)
        if not func:
            continue
        value = func(data, ctx)
        if value is None:
            continue
        data = value
    return data


def io_apply(ctx: Context, meta: FieldMeta):
    hook = meta.io
    if not meta.end:
        # set the IO hook
        io = ctx.G.io
        if io.read == hook.read:
            raise ValueError(f"Already attached: {io.read} == {hook.read}")
        hook.ctx = ctx
        hook.io_read = io.read
        hook.io_write = io.write
        hook.io_seek = io.seek
        hook.io_tell = io.tell
        io.read = hook.read
        io.write = hook.write
        io.seek = hook.seek
        io.tell = hook.tell
        evaluate(ctx, hook.init)
    else:
        # remove the hook
        evaluate(ctx, hook.end)
        io = ctx.G.io
        if io.read != hook.read:
            raise ValueError(f"Not attached: {io.read} != {hook.read}")
        io.read = hook.io_read
        io.write = hook.io_write
        io.seek = hook.io_seek
        io.tell = hook.io_tell
        hook.ctx = None
        hook.io_read = None
        hook.io_write = None
        hook.io_seek = None
        hook.io_tell = None
