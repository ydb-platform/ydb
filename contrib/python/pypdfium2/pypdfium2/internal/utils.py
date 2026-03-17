# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import os
import ctypes
import pypdfium2.raw as pdfium_c


def color_tohex(color, rev_byteorder):
    
    if len(color) != 4:
        raise ValueError("Color must consist of exactly 4 values.")
    if not all(0 <= c <= 255 for c in color):
        raise ValueError("Color value exceeds boundaries.")
    
    # different color interpretation with FPDF_REVERSE_BYTE_ORDER might be a bug? at least it's not documented.
    r, g, b, a = color
    channels = (a, b, g, r) if rev_byteorder else (a, r, g, b)
    
    c_color = 0
    shift = 24
    for c in channels:
        c_color |= c << shift
        shift -= 8
    
    return c_color


def set_callback(struct, fname, callback):
    setattr(struct, fname, type( getattr(struct, fname) )(callback))


def is_stream(buf, spec="r"):
    methods = []
    assert set(spec).issubset( set("rw") )
    if "r" in spec:
        methods += ["seek", "tell", "read", "readinto"]
    if "w" in spec:
        methods += ["write"]
    return all(callable(getattr(buf, a, None)) for a in methods)


def get_buffer(ptr, size):
    obj = ptr.contents
    return (type(obj) * size).from_address( ctypes.addressof(obj) )


class _buffer_reader:
    
    def __init__(self, py_buffer):
        self.py_buffer = py_buffer
    
    def __call__(self, _, position, p_buf_first, size):
        c_buffer = get_buffer(p_buf_first, size)
        self.py_buffer.seek(position)
        self.py_buffer.readinto(c_buffer)
        return 1


class _buffer_writer:
    
    def __init__(self, py_buffer):
        self.py_buffer = py_buffer
    
    def __call__(self, _, p_data_first, size):
        # c_void_p has no .contents, need to cast
        p_data_first = ctypes.cast(p_data_first, ctypes.POINTER(ctypes.c_ubyte))
        c_buffer = get_buffer(p_data_first, size)
        self.py_buffer.write(c_buffer)
        return 1


def get_bufreader(buffer):
    
    file_len = buffer.seek(0, os.SEEK_END)
    buffer.seek(0)
    
    reader = pdfium_c.FPDF_FILEACCESS()
    reader.m_FileLen = file_len
    set_callback(reader, "m_GetBlock", _buffer_reader(buffer))
    reader.m_Param = None
    
    to_hold = (reader.m_GetBlock, )
    
    return reader, to_hold


def get_bufwriter(buffer):
    writer = pdfium_c.FPDF_FILEWRITE(version=1)
    set_callback(writer, "WriteBlock", _buffer_writer(buffer))
    return writer


def pages_c_array(pages):
    if not pages:
        return None, 0
    count = len(pages)
    c_array = (pdfium_c.FPDF_PAGE * count)(*[p.raw for p in pages])
    return c_array, count
