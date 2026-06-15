from yt.common import YtError


class YsonError(YtError):
    pass


def raise_yson_error(message, position_info):
    line_index, position, offset = position_info
    raise YsonError(message, attributes={"line": line_index, "position": position, "offset": offset})


class StreamWrap(object):
    def __init__(self, stream, header, footer):
        self.stream = stream
        self.header = header
        self.footer = footer

        self.pos = 0
        self.state = 0

    def read(self, n):
        if n == 0:
            return self.stream.read(0)

        assert n == 1

        if self.state == 0:
            if self.pos == len(self.header):
                self.state += 1
            else:
                res = bytes([self.header[self.pos]])
                self.pos += 1
                return res

        if self.state == 1:
            sym = self.stream.read(1)
            if sym:
                return sym
            else:
                self.state += 1
                self.pos = 0

        if self.state == 2:
            if self.pos == len(self.footer):
                self.state += 1
            else:
                res = bytes([self.footer[self.pos]])
                self.pos += 1
                return res

        if self.state == 3:
            return b""


_ENCODING_SENTINEL = object()

# Binary literals markers
STRING_MARKER = bytes([1])
INT64_MARKER = bytes([2])
DOUBLE_MARKER = bytes([3])
FALSE_MARKER = bytes([4])
TRUE_MARKER = bytes([5])
UINT64_MARKER = bytes([6])
