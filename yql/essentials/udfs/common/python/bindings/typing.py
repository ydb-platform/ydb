def main():
    import importlib.abc
    import importlib.machinery
    import sys

    class Finder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path, target=None):
            if fullname in sys.builtin_module_names:
                return importlib.machinery.ModuleSpec(
                    fullname,
                    importlib.machinery.BuiltinImporter,
                )

    sys.meta_path.append(Finder())

    try:
        import yandex.type_info.type_base as ti_base
        import yandex.type_info.typing as ti_typing
        import six
    except ImportError as e:
        raise ImportError(
            str(e) + ". Make sure that library/python/type_info is in your PEERDIR list"
        )

    from yql import typing

    AutoMap = ti_base.make_primitive_type("AutoMap")

    def _format_arg(arg):
        res = []
        if arg[0]:
            res.append("{}:".format(ti_base.quote_string(arg[0])))
        res.append(str(arg[1]))
        if arg[2]:
            res.append("{Flags:")
            res.append(",".join(str(x) for x in sorted(list(arg[2]))))
            res.append("}")
        return "".join(res)

    Stream = ti_typing._SingleArgumentGeneric("Stream")

    @six.python_2_unicode_compatible
    class GenericResourceAlias(ti_base.Type):
        REQUIRED_ATTRS = ti_base.Type.REQUIRED_ATTRS + ["tag"]

        def __str__(self):
            return u"{}<{}>".format(self.name, ti_base.quote_string(self.tag))

        def to_yson_type(self):
            return {"type_name": self.yt_type_name, "tag": self.tag}

    class GenericResource(ti_base.Generic):
        def __getitem__(self, params):
            if not isinstance(params, str):
                raise ValueError("Expected str, but got: {}".format(ti_base._with_type(params)))

            attrs = {
                "name": self.name,
                "yt_type_name": self.yt_type_name,
                "tag": params,
            }

            return GenericResourceAlias(attrs)

        def from_dict(self):
            raise NotImplementedError()

    Resource = GenericResource("Resource")

    def _extract_arg_info(param):
        name = ""
        arg_type = param
        flags = set()
        if isinstance(param, slice):
            name = param.start
            if name is None:
                name = ""
            if not isinstance(name, str):
                raise ValueError("Expected str as argument name but got: {}".format(ti_base._with_type(name)))
            arg_type = param.stop
            ti_base.validate_type(arg_type)
            if param.step is not None:
               for x in param.step:
                   if x != AutoMap:
                       raise ValueError("Expected AutoMap as parameter flag but got: {}".format(ti_base._with_type(x)))
                   flags.add(x)
        else:
            ti_base.validate_type(arg_type)
        return (name, arg_type, flags)

    @six.python_2_unicode_compatible
    class GenericCallableAlias(ti_base.Type):
        def __str__(self):
            return ("Callable<(" +
                        ",".join(_format_arg(x) for x in self.args[:len(self.args)-self.optional_args]) +
                        ("," if len(self.args) > self.optional_args and self.optional_args else "") +
                        ("[" if self.optional_args else "") +
                        ",".join(_format_arg(x) for x in self.args[len(self.args)-self.optional_args:]) +
                        ("]" if self.optional_args else "") +
                        ")->" + str(getattr(self, "return")) + ">")

        def to_yson_type(self):
            yson_repr = {
                "optional_args": self.optional_args,
                "return": getattr(self, "return"),
                "args": self.args,
                "type_name": self.yt_type_name,
            }
            return yson_repr


    class GenericCallable(ti_base.Generic):
        def __getitem__(self, params):
            if not isinstance(params, tuple) or len(params) < 2 or not isinstance(params[0], int) or not ti_typing.is_valid_type(params[1]):
                raise ValueError("Expected at least two arguments (integer and type of return value) but got: {}".format(ti_base._with_type(params)))
            args = []
            for param in params[2:]:
                name, arg_type, flags = _extract_arg_info(param)
                args.append((name, arg_type, flags))

            if params[0] < 0 or params[0] > len(args):
                raise ValueError("Optional argument count - " + str(params[0]) + " out of range [0.." + str(len(args)) + "]")

            attrs = {
                "optional_args": params[0],
                "return": params[1],
                "args": args,
                "name": "Tagged",
                "yt_type_name": "tagged",
            }

            return GenericCallableAlias(attrs)

        def from_dict(self):
            raise NotImplementedError()

    Callable = GenericCallable("Callable")

    def parse_slice_arg(arg):
        try:
            return _format_arg(_extract_arg_info(arg))
        except ValueError:
            pass

    typing.Type = ti_base.Type
    typing.is_valid_type = ti_base.is_valid_type
    typing.parse_slice_arg = parse_slice_arg

    typing.Bool = ti_typing.Bool
    typing.Int8 = ti_typing.Int8
    typing.Uint8 = ti_typing.Uint8
    typing.Int16 = ti_typing.Int16
    typing.Uint16 = ti_typing.Uint16
    typing.Int32 = ti_typing.Int32
    typing.Uint32 = ti_typing.Uint32
    typing.Int64 = ti_typing.Int64
    typing.Uint64 = ti_typing.Uint64
    typing.Float = ti_typing.Float
    typing.Double = ti_typing.Double
    typing.String = ti_typing.String
    typing.Utf8 = ti_typing.Utf8
    typing.Yson = ti_typing.Yson
    typing.Json = ti_typing.Json
    typing.Uuid = ti_typing.Uuid
    typing.Date = ti_typing.Date
    typing.Datetime = ti_typing.Datetime
    typing.Timestamp = ti_typing.Timestamp
    typing.Interval = ti_typing.Interval
    typing.TzDate = ti_typing.TzDate
    typing.TzDatetime = ti_typing.TzDatetime
    typing.TzTimestamp = ti_typing.TzTimestamp
    typing.Void = ti_typing.Void
    typing.Null = ti_typing.Null
    typing.EmptyTuple = ti_typing.EmptyTuple
    typing.EmptyStruct = ti_typing.EmptyStruct
    typing.Optional = ti_typing.Optional
    typing.List = ti_typing.List
    typing.Dict = ti_typing.Dict
    typing.Tuple = ti_typing.Tuple
    typing.Struct = ti_typing.Struct
    typing.Variant = ti_typing.Variant
    typing.Tagged = ti_typing.Tagged
    typing.Decimal = ti_typing.Decimal

    typing.Stream = Stream
    typing.Resource = Resource
    typing.Callable = Callable
    typing.AutoMap = AutoMap
