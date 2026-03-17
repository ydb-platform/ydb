import inspect
import sys
import traceback


class DumpResult(str):
    """
    строка, у которой repr = value
    """

    def __new__(cls, value):
        return super().__new__(cls, value)

    __repr__ = str.__str__


class _EnumAllAttrs:
    pass


class DumpableView:
    def _enum_attrs(self):
        for attr in dir(self):
            if not attr.startswith("_") and attr != "dump":
                yield attr

    def _getter_for_dump(self, attr):
        dumper_name = "_dump_" + attr
        if hasattr(self, dumper_name) and callable(getattr(self, dumper_name)):
            return getattr(self, dumper_name)

    @staticmethod
    def strip_dumper(attr, lines=None, chars=None, chars_in_line=None):
        def bound_method(self):
            lines_list = []
            value = getattr(self, attr)
            assert isinstance(value, str)
            total_lines = 0
            total_chars = 0
            add_tail = True
            for line in value.split("\n"):
                if lines and total_lines >= lines:
                    break

                if chars and total_chars + len(line) >= chars:
                    lines_list.append(line[chars - total_chars :])
                    break

                if chars_in_line and len(line) > chars_in_line:
                    lines_list.append(line[chars - total_chars :] + " ...")
                else:
                    lines_list.append(line)
                total_lines += 1
                total_chars += len(line) + 1
            else:
                add_tail = False

            ret = repr("\n".join(lines_list))
            if add_tail:
                ret += " ..."
            return DumpResult(ret)

        return bound_method

    def __dump_value(self, prefix, value, seen):
        ret = []
        op = "="
        if isinstance(value, DumpableView):
            # защита от рекурсии - последующие ссылки на объекты, а не новые дампы
            if id(value) in seen:
                value = DumpResult(seen[id(value)])
                op = "->"
            else:
                seen[id(value)] = prefix

        if isinstance(value, DumpableView):
            ret += value.dump(prefix, seen=seen)  # pylint: disable=no-member
        elif isinstance(value, dict):
            for k, v in value.items():
                ret.extend(self.__dump_value(f"{prefix}[{repr(k)}]", v, seen))
        elif isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                name = getattr(v, "_dump__list_key", None)
                name = repr(name) if name is not None else str(i)
                ret.extend(self.__dump_value(f"{prefix}[{name}]", v, seen))
        else:
            fmt = repr(value)
            vtype = type(value)
            if not isinstance(value, DumpResult) and (vtype.__module__ != "builtins") and vtype.__name__ not in fmt:
                fmt += "  # %s" % (vtype)
            ret += ["%s %s %s" % (prefix, op, fmt)]
        return ret

    def dump(self, prefix="", value=_EnumAllAttrs, seen=None):
        """
        В собственных DumpableView-классах нужно определить dump примерно так:
        def dump(self, prefix, **kwargs):
            ret = super().dump(prefix, **kwargs) + [ your_own_lines ]
        """
        if seen is None:
            seen = {id(self): prefix}

        ret = []
        if value is not _EnumAllAttrs:
            ret.extend(self.__dump_value(prefix, value, seen))
        else:
            for attr in self._enum_attrs():
                getter = self._getter_for_dump(attr)
                try:
                    if getter:
                        value = getter()
                    else:
                        value = getattr(self, attr)
                        if inspect.isroutine(value):
                            continue
                except Exception as exc:
                    exc_tb = sys.exc_info()[2]
                    frame_index = -1
                    tb_list = traceback.extract_tb(exc_tb)
                    value = DumpResult("%r at %s:%s" % (exc, tb_list[frame_index][0], tb_list[frame_index][1]))

                ret.extend(self.__dump_value("%s.%s" % (prefix, attr), value, seen))
        return ret

    # Filter out cached lru_methods from state
    def __getstate__(self):
        state = self.__dict__.copy()
        for key in list(state.keys()):  # For thread-safe: dictionary changed size during iteration
            if inspect.ismethod(state[key]):
                del state[key]
        return state
