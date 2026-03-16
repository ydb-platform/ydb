#-------------------------------------------------------------------------------
# elftools: common/construct_utils.py
#
# Some complementary construct utilities
#
# Eli Bendersky (eliben@gmail.com)
# This code is in the public domain
#-------------------------------------------------------------------------------
from ..construct import (
    Subconstruct, ConstructError, ArrayError, Adapter, Field, RepeatUntil,
    Rename, SizeofError, Construct
    )


class RepeatUntilExcluding(Subconstruct):
    """ A version of construct's RepeatUntil that doesn't include the last
        element (which casued the repeat to exit) in the return value.

        Only parsing is currently implemented.

        P.S. removed some code duplication
    """
    __slots__ = ["predicate"]
    def __init__(self, predicate, subcon):
        Subconstruct.__init__(self, subcon)
        self.predicate = predicate
        self._clear_flag(self.FLAG_COPY_CONTEXT)
        self._set_flag(self.FLAG_DYNAMIC)
    def _parse(self, stream, context):
        obj = []
        try:
            context_for_subcon = context
            if self.subcon.conflags & self.FLAG_COPY_CONTEXT:
                context_for_subcon = context.__copy__()

            while True:
                subobj = self.subcon._parse(stream, context_for_subcon)
                if self.predicate(subobj, context):
                    break
                obj.append(subobj)
        except ConstructError as ex:
            raise ArrayError("missing terminator", ex)
        return obj
    def _build(self, obj, stream, context):
        raise NotImplementedError('no building')
    def _sizeof(self, context):
        raise SizeofError("can't calculate size")


def _LEB128_reader():
    """ Read LEB128 variable-length data from the stream. The data is terminated
        by a byte with 0 in its highest bit.
    """
    return RepeatUntil(
                lambda obj, ctx: ord(obj) < 0x80,
                Field(None, 1))


class _ULEB128Adapter(Adapter):
    """ An adapter for ULEB128, given a sequence of bytes in a sub-construct.
    """
    def _decode(self, obj, context):
        value = 0
        for b in reversed(obj):
            value = (value << 7) + (ord(b) & 0x7F)
        return value


class _SLEB128Adapter(Adapter):
    """ An adapter for SLEB128, given a sequence of bytes in a sub-construct.
    """
    def _decode(self, obj, context):
        value = 0
        for b in reversed(obj):
            value = (value << 7) + (ord(b) & 0x7F)
        if ord(obj[-1]) & 0x40:
            # negative -> sign extend
            value |= - (1 << (7 * len(obj)))
        return value


def ULEB128(name):
    """ A construct creator for ULEB128 encoding.
    """
    return Rename(name, _ULEB128Adapter(_LEB128_reader()))


def SLEB128(name):
    """ A construct creator for SLEB128 encoding.
    """
    return Rename(name, _SLEB128Adapter(_LEB128_reader()))

class StreamOffset(Construct):
    """
    Captures the current stream offset

    Parameters:
    * name - the name of the value

    Example:
    StreamOffset("item_offset")
    """
    __slots__ = []
    def __init__(self, name):
        Construct.__init__(self, name)
        self._set_flag(self.FLAG_DYNAMIC)
    def _parse(self, stream, context):
        return stream.tell()
    def _build(self, obj, stream, context):
        context[self.name] = stream.tell()
    def _sizeof(self, context):
        return 0
