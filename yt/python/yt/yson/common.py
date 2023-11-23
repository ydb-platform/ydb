from yt.common import YtError

try:
    from yt.packages.six import int2byte, indexbytes
except ImportError:
    from six import int2byte, indexbytes


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
                res = int2byte(indexbytes(self.header, self.pos))
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
                res = int2byte(indexbytes(self.footer, self.pos))
                self.pos += 1
                return res

        if self.state == 3:
            return b""


_ENCODING_SENTINEL = object()

# Binary literals markers
STRING_MARKER = int2byte(1)
INT64_MARKER = int2byte(2)
DOUBLE_MARKER = int2byte(3)
FALSE_MARKER = int2byte(4)
TRUE_MARKER = int2byte(5)
UINT64_MARKER = int2byte(6)
