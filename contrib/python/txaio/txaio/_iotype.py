###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) typedef int GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
###############################################################################


def guess_stream_needs_encoding(fileobj, default=True):
    """
    Guess the type (bytes/unicode) of this stream, and return whether or not it
    requires text to be encoded before written into it.
    """
    # XXX: Unicode
    # On Python 2, stdout is bytes. However, we can't wrap it in a
    # TextIOWrapper, as it's not from IOBase, so it doesn't have .seekable.
    # It does, however, have a mode, and we can cheese it base on that.
    # On Python 3, stdout is a TextIOWrapper, and so we can safely write
    # str to it, and it will encode it correctly for the target terminal or
    # whatever.
    # If it's a io.BytesIO or StringIO, then it won't have a mode, but it
    # is a read/write stream, so we can get its type by reading 0 bytes and
    # checking the type.
    try:
        # If it's a r/w stream, this will give us the type of it
        t = type(fileobj.read(0))

        if t is bytes:
            return True
        elif t is str:
            return False

    except Exception:
        pass

    try:
        mode = fileobj.mode

        if "b" in mode:
            return True
        else:
            return False
    except Exception:
        pass

    return default
