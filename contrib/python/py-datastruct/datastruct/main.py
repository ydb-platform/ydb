#  Copyright (c) Kuba Szczodrzyński 2023-1-3.

import dataclasses
import struct
from dataclasses import MISSING, Field, dataclass
from functools import lru_cache
from io import BytesIO
from typing import IO, Any, Dict, List, Optional, Sized, Tuple, Type, TypeVar, Union

from .config import datastruct_get_config
from .types import Config, Container, Context, FieldMeta, FieldType, T
from .utils.context import (
    build_context,
    build_global_context,
    ctx_read,
    ctx_write,
    evaluate,
    hook_apply,
    io_apply,
)
from .utils.fields import (
    field_decode,
    field_do_seek,
    field_encode,
    field_get_base,
    field_get_default,
    field_get_meta,
    field_get_padding,
    field_switch_base,
)
from .utils.fmt import fmt_evaluate
from .utils.misc import SizingIO
from .utils.types import ARRAYS, BYTES, EXCEPTIONS, check_value_type
from .utils.validation import field_validate


@dataclass
class DataStruct:
    def __post_init__(self) -> None:
        for field, meta, value in self.fields():
            try:
                field_validate(field, meta)
            except EXCEPTIONS as e:
                suffix = f"; while initializing '{field.name}'"
                e.args = (e.args[0] + suffix,)
                raise e

            # check if value is already set (field has a default=)
            if value != Ellipsis:
                # correct types of simple default values
                # (enums, fields with adapters, etc.)
                if meta.types != ():  # not Any
                    if isinstance(meta.types, type):
                        value = field_decode(value, meta.types)
                    if not check_value_type(value, meta.types):
                        if meta.adapter:
                            # try to adapt default values
                            try:
                                value = meta.adapter.decode(value, None)
                            except Exception:
                                raise ValueError(
                                    f"Couldn't build the default value of {field.name} - the adapter "
                                    "might be requiring the context. Pass 'default' value "
                                    "resulting *after* applying the adapter"
                                )
                        else:
                            raise TypeError(
                                f"Wrong field type - expected {meta.types}, found default {type(value)}"
                            )
                    self.__setattr__(field.name, value)

            # accept special fields and those already having a value
            if value != Ellipsis or not meta.public:
                continue

            default = field_get_default(field, meta, DataStruct)
            if default is not None:
                # print("Got default for", field.name, default)
                self.__setattr__(field.name, default)
                continue

            # forbid creating an instance of fields with no default
            raise ValueError(
                f"Cannot create an instance of {type(self)}: "
                f"field '{field.name}' has no default and "
                f"no value was passed, nor can it be built",
            )

    def _write_value(self, ctx: Context, meta: FieldMeta, value: Any) -> None:
        # pack structures directly
        if isinstance(value, DataStruct):
            kwargs = {k: evaluate(ctx, v) for k, v in meta.kwargs.items()}
            value.pack(io=ctx.G.io, parent=ctx, **kwargs)
            return
        # evaluate and validate the format
        fmt = fmt_evaluate(ctx, meta.fmt, self.config().endianness)
        if isinstance(fmt, int) and isinstance(value, bytes):
            if len(value) < fmt:
                raise ValueError(f"Not enough bytes to write: {len(value)} < {fmt}")
            # assume the field is bytes, write it directly
            ctx_write(ctx, value[:fmt])
            return
        # use struct.pack() to write the raw value
        ctx_write(ctx, struct.pack(fmt, value))

    def _sizeof_value(self, ctx: Context, meta: FieldMeta, value: Any) -> None:
        # size structures directly
        if isinstance(value, DataStruct):
            kwargs = {k: evaluate(ctx, v) for k, v in meta.kwargs.items()}
            value.pack(io=ctx.G.io, parent=ctx, **kwargs)
            return
        # evaluate and validate the format
        fmt = fmt_evaluate(ctx, meta.fmt, self.config().endianness)
        if isinstance(fmt, int):
            # assume the field is bytes, size it directly
            ctx.G.io.write(fmt)
            return
        # use struct.calcsize() to calculate size of the raw value
        ctx.G.io.write(struct.calcsize(fmt))

    def _write_field(
        self,
        ctx: Context,
        field: Field,
        meta: FieldMeta,
        value: Any,
    ) -> Any:
        ctx.P.self = value

        if meta.ftype == FieldType.FIELD:
            # build fields if necessary
            try:
                if meta.builder and (value is Ellipsis or meta.always):
                    value = evaluate(ctx, meta.builder)
            except Exception as e:
                if not ctx.G.sizing:
                    # avoid parent reference errors while sizing
                    raise e
            if ctx.G.sizing:
                self._sizeof_value(ctx, meta, value)
                return value
            # 1. encode the value
            encoded = field_encode(value)
            # 2. run custom adapter
            adapted = meta.adapter.encode(encoded, ctx) if meta.adapter else encoded
            # 3. write the raw value
            self._write_value(ctx, meta, adapted)
            return value

        if meta.ftype == FieldType.SEEK:
            field_do_seek(ctx, meta)
            return Ellipsis

        if meta.ftype == FieldType.PADDING:
            length, padding, _ = field_get_padding(self.config(), ctx, meta)
            if ctx.G.sizing:
                ctx.G.io.write(length)
            else:
                ctx_write(ctx, padding)
            return Ellipsis

        if meta.ftype == FieldType.ACTION:
            return evaluate(ctx, meta.action)

        if meta.ftype == FieldType.HOOK:
            if ctx.G.sizing:
                return Ellipsis
            hook_apply(ctx, meta)
            return Ellipsis

        if meta.ftype == FieldType.IO:
            if ctx.G.sizing:
                return Ellipsis
            io_apply(ctx, meta)
            return Ellipsis

        if meta.ftype == FieldType.REPEAT:
            # repeat() field - value type must be List
            if not isinstance(value, ARRAYS):
                raise TypeError(f"Value is not an array: {value}")

            i = 0
            count = evaluate(ctx, meta.count)
            length = evaluate(ctx, meta.length)
            end = length and (ctx.P.tell() + length)
            base_field, base_meta = field_get_base(meta)

            if isinstance(count, int) and len(value) != count and not base_meta.builder:
                has_default = (
                    base_field.default is not Ellipsis
                    or base_field.default_factory is not MISSING
                )
                if not self.config().repeat_fill or not has_default:
                    # ensure the list size matches expected element count,
                    # apart from built() subfields, which are implicitly trusted here
                    raise ValueError(
                        f"List size ({len(value)}) doesn't match repeat() 'count' "
                        f"parameter value ({count}); to automatically extend lists, "
                        f"enable 'repeat_fill' in config and provide a default "
                        f"value of the base field",
                    )
                # extend list fields with default value
                while len(value) < count:
                    value.append(field_get_default(base_field, base_meta, DataStruct))
                # trim list fields that are too long
                if len(value) > count:
                    value = value[0:count]

            items: Union[list, tuple] = value
            items_iter = iter(items)

            while (count is None or i < count) and (end is None or ctx.P.tell() < end):
                ctx.P.self = value
                ctx.P.i = i
                if evaluate(ctx, meta.when) is False:
                    break

                if not base_meta.builder:
                    try:
                        item = next(items_iter)
                    except StopIteration:
                        raise ValueError(
                            "No more items to pack! "
                            "Check the return value of 'when' and 'last'. "
                            "Perhaps the list is missing the last element?"
                        )
                else:
                    item = Ellipsis
                item = self._write_field(ctx, base_field, base_meta, item)
                if isinstance(items, list):
                    # don't reassign built fields to tuples
                    # only update in lists (which will update self+ctx too)
                    if len(items) <= i:
                        items.append(item)
                    else:
                        items[i] = item

                # provide another value 'item' to context lambdas in 'last'
                ctx.P.item = item
                last = evaluate(ctx, meta.last)
                ctx.P.pop("item")
                if last is True:
                    break
                i += 1
            ctx.P.pop("i", None)
            return items

        if meta.ftype == FieldType.COND:
            if evaluate(ctx, meta.condition) is False:
                if meta.if_not is not Ellipsis:
                    value = evaluate(ctx, meta.if_not)
                    return value
                return Ellipsis
            return self._write_field(ctx, *field_get_base(meta), value)

        if meta.ftype == FieldType.SWITCH:
            field = field_switch_base(self.config(), ctx, meta)
            meta = field_get_meta(field)
            if value is Ellipsis:
                # assign default based on field mode
                value = field_get_default(field, meta, DataStruct)
            return self._write_field(ctx, field, meta, value)

    @classmethod
    def _read_value(cls, ctx: Context, meta: FieldMeta, typ: Type[T]) -> T:
        # unpack structures directly
        if issubclass(typ, DataStruct):
            kwargs = {k: evaluate(ctx, v) for k, v in meta.kwargs.items()}
            return typ.unpack(io=ctx.G.io, parent=ctx, **kwargs)
        # evaluate and validate the format
        fmt = fmt_evaluate(ctx, meta.fmt, cls.config().endianness)
        if isinstance(fmt, int):
            # assume the field is bytes, write it directly
            value = ctx_read(ctx, fmt)
            if len(value) < fmt:
                raise ValueError(f"Not enough bytes to read: {len(value)} < {fmt}")
            return value
        # use struct.unpack() to read the raw value
        length = struct.calcsize(fmt)
        (value,) = struct.unpack(fmt, ctx_read(ctx, length))
        return value

    @classmethod
    def _read_field(
        cls,
        ctx: Context,
        field: Field,
        meta: FieldMeta,
    ) -> Any:
        if meta.ftype == FieldType.FIELD:
            # 3. read the raw value
            adapted = cls._read_value(ctx, meta, meta.types)
            # 2. run custom adapter
            encoded = meta.adapter.decode(adapted, ctx) if meta.adapter else adapted
            # 1. decode the value
            value = field_decode(encoded, meta.types)
            return value

        if meta.ftype == FieldType.SEEK:
            field_do_seek(ctx, meta)
            return Ellipsis

        if meta.ftype == FieldType.PADDING:
            length, padding, check = field_get_padding(cls.config(), ctx, meta)
            if ctx_read(ctx, length) != padding and check:
                raise ValueError(f"Invalid padding found")
            return Ellipsis

        if meta.ftype == FieldType.ACTION:
            return evaluate(ctx, meta.action)

        if meta.ftype == FieldType.HOOK:
            hook_apply(ctx, meta)
            return Ellipsis

        if meta.ftype == FieldType.IO:
            io_apply(ctx, meta)
            return Ellipsis

        if meta.ftype == FieldType.REPEAT:
            i = 0
            count = evaluate(ctx, meta.count)
            length = evaluate(ctx, meta.length)
            end = length and (ctx.P.tell() + length)
            base_field, base_meta = field_get_base(meta)
            items = []

            while (count is None or i < count) and (end is None or ctx.P.tell() < end):
                ctx.P.i = i
                if evaluate(ctx, meta.when) is False:
                    break

                item = cls._read_field(ctx, base_field, base_meta)
                items.append(item)

                # provide another value 'item' to context lambdas in 'last'
                ctx.P.item = item
                last = evaluate(ctx, meta.last)
                ctx.P.pop("item")
                if last is True:
                    break
                i += 1
            ctx.P.pop("i", None)
            return items

        if meta.ftype == FieldType.COND:
            if evaluate(ctx, meta.condition) is False:
                if meta.if_not is not Ellipsis:
                    value = evaluate(ctx, meta.if_not)
                    return value
                return None
            return cls._read_field(ctx, *field_get_base(meta))

        if meta.ftype == FieldType.SWITCH:
            field = field_switch_base(cls.config(), ctx, meta)
            meta = field_get_meta(field)
            return cls._read_field(ctx, field, meta)

    def pack(
        self,
        io: Optional[IO[bytes]] = None,
        field_names: Union[str, List[str]] = None,
        parent: Union[Context, "DataStruct", None] = None,
        ctx_out: List[Context] = None,
        tell_offset: int = 0,
        **kwargs,
    ) -> Optional[bytes]:
        sizing = isinstance(io, SizingIO)
        if isinstance(parent, Context) and not sizing:
            glob = parent.G
        else:
            if io is None:
                io = BytesIO()
            glob = build_global_context(io, packing=True, sizing=sizing)

        if isinstance(parent, DataStruct):
            parent_obj = parent
            fields = parent.fields()
            parent = build_context(glob, None, self.config())
            parent.self = parent_obj

        if not field_names:
            field_names = []
        if isinstance(field_names, str):
            field_names = [field_names]

        fields = self.fields()
        ctx = build_context(glob, parent, self.config(), **kwargs)
        ctx.self = self
        if ctx_out is not None:
            ctx_out.append(ctx)
        if tell_offset:
            ctx.P.seek(tell_offset)
        field_name = type(self).__name__
        try:
            field_found = not field_names
            for field, meta, _ in fields:
                if field_names and field.name not in field_names:
                    continue
                field_found = True
                field_name = f"{type(self).__name__}.{field.name}"
                # print(f"Packing {meta.ftype.name} '{field_name}'")
                value = self._write_field(ctx, field, meta, getattr(self, field.name))
                if value is not Ellipsis and meta.public:
                    setattr(self, field.name, value)
            if not field_found:
                # after packing, the field must have been found
                raise ValueError(f"No such field(s): {field_names}")
        except EXCEPTIONS as e:
            if ctx.G.sizing:
                suffix = f"; while sizing '{field_name}'"
            else:
                suffix = f"; while packing '{field_name}'"
            e.args = (e.args[0] + suffix,)
            raise e
        if isinstance(io, BytesIO):
            return io.getvalue()
        return None

    @classmethod
    def unpack(
        cls: Type["DS"],
        io: Union[IO[bytes], bytes],
        parent: Union[Context, "DataStruct", None] = None,
        ctx_out: List[Context] = None,
        **kwargs,
    ) -> "DS":
        if isinstance(parent, Context):
            glob = parent.G
        else:
            if isinstance(io, BYTES):
                io = BytesIO(io)
            glob = build_global_context(io, unpacking=True)

        if isinstance(parent, DataStruct):
            fields = parent.fields()
            parent = build_context(glob, None, cls.config())

        fields = cls.classfields()
        values = Container()
        ctx = build_context(glob, parent, cls.config(), **kwargs)
        ctx.self = values
        if ctx_out is not None:
            ctx_out.append(ctx)
        field_name = cls.__name__
        try:
            for field, meta in fields:
                field_name = f"{cls.__name__}.{field.name}"
                # print(f"Unpacking {meta.ftype.name} '{field_name}'")
                # validate fields since they weren't validated before
                field_validate(field, meta)
                value = cls._read_field(ctx, field, meta)
                if value is not Ellipsis and meta.public:
                    values[field.name] = value
            field_name = f"{cls.__name__}()"
            # noinspection PyArgumentList
            return cls(**values)
        except EXCEPTIONS as e:
            suffix = f"; while unpacking '{field_name}'"
            e.args = (e.args[0] + suffix,)
            raise e

    def sizeof(
        self,
        field_names: Union[str, List[str]] = None,
        parent: Union[Context, "DataStruct", None] = None,
        ctx_out: List[Context] = None,
        tell_offset: int = 0,
        **kwargs,
    ) -> int:
        io = SizingIO()
        self.pack(
            io=io,
            field_names=field_names,
            parent=parent,
            ctx_out=ctx_out,
            tell_offset=tell_offset,
            **kwargs,
        )
        return io.size

    def fields(self) -> List[Tuple[Field, FieldMeta, Any]]:
        return [
            (
                field,
                field_get_meta(field),
                self.__getattribute__(field.name),
            )
            for field in dataclasses.fields(self)
        ]

    @classmethod
    def classfields(cls) -> List[Tuple[Field, FieldMeta]]:
        return [
            (
                field,
                field_get_meta(field),
            )
            for field in dataclasses.fields(cls)
        ]

    def asdict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    @lru_cache()
    def config(cls) -> Config:
        config = Config(datastruct_get_config())
        config.update(getattr(cls, "_CONFIG", {}))
        return config


DS = TypeVar("DS", bound=DataStruct)


def sizeof(o, ctx: Optional[Context] = None) -> int:
    if isinstance(o, DataStruct):
        return o.sizeof(parent=ctx)
    if isinstance(o, ARRAYS):
        return sum(i.sizeof(parent=ctx) for i in o)
    if isinstance(o, Sized):
        return len(o)
    raise TypeError(f"Unknown type '{type(o)}'")
