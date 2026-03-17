#include "PythonUtils.h"
#include "clickhouse_config.h"

#include <cstddef>
#include <pybind11/gil.h>
#include <pybind11/pytypes.h>
#include <pybind11/numpy.h>
#include <utf8proc.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>

namespace DB_CHDB
{

const char * ConvertPyUnicodeToUtf8(const void * input, int kind, size_t codepoint_cnt, size_t & output_size)
{
    if (input == nullptr)
    {
        return nullptr;
    }

    char * output_buffer = new char[codepoint_cnt * 4 + 1]; // Allocate buffer based on calculated size
    char * target = output_buffer;
    size_t total_size = 0;

    // Encode each Unicode codepoint to UTF-8 using utf8proc
    switch (kind)
    {
        case 1: {
            const auto * start = static_cast<const uint8_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                int sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(target));
                target += sz;
                total_size += sz;
            }
            break;
        }
        case 2: {
            const auto * start = static_cast<const uint16_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                int sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(target));
                target += sz;
                total_size += sz;
            }
            break;
        }
        case 4: {
            const auto * start = static_cast<const uint32_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                int sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(target));
                target += sz;
                total_size += sz;
            }
            break;
        }
    }

    output_buffer[total_size] = '\0'; // Null-terminate the output string
    output_size = total_size;
    return output_buffer;
}

size_t
ConvertPyUnicodeToUtf8(const void * input, int kind, size_t codepoint_cnt, ColumnString::Offsets & offsets, ColumnString::Chars & chars)
{
    if (input == nullptr)
    {
        return 0;
    }

    // Estimate the maximum buffer size required for the UTF-8 output
    // Buffers is reserved from the caller, so we can safely resize it and memory will not be wasted
    size_t estimated_size = codepoint_cnt * 4 + 1; // Allocate buffer for UTF-8 output
    size_t chars_cursor = chars.size();
    size_t target_size = chars_cursor + estimated_size;
    chars.resize(target_size);

    // Resize the character buffer to accommodate the UTF-8 string
    chars.resize(chars_cursor + estimated_size + 1); // +1 for null terminator

    size_t offset = chars_cursor;
    switch (kind)
    {
        case 1: { // Latin1/ASCII
            const auto * start = static_cast<const uint8_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                auto sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(&chars[offset]));
                offset += sz;
            }
            break;
        }
        case 2: { // UTF-16
            const auto * start = static_cast<const uint16_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                auto sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(&chars[offset]));
                offset += sz;
            }
            break;
        }
        case 4: { // UTF-32
            const auto * start = static_cast<const uint32_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                auto sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(&chars[offset]));
                offset += sz;
            }
            break;
        }
    }

    chars[offset++] = '\0'; // Null terminate the output string
    offsets.push_back(offset); // Include the null terminator in the offset
    chars.resize(offset); // Resize to the actual used size, including null terminator

    return offset; // Return the number of bytes written, not including the null terminator
}

int PyString_AsStringAndSize(PyObject * ob, char ** charpp, Py_ssize_t * sizep)
{
    // always convert it to utf8, but this case is rare, here goes the slow path
    py::gil_scoped_acquire acquire;
    if (PyUnicode_Check(ob))
    {
        *charpp = const_cast<char *>(PyUnicode_AsUTF8AndSize(ob, sizep));
        if (*charpp == nullptr)
        {
            return -1;
        }
        return 0;
    }
    else
    {
        return PyBytes_AsStringAndSize(ob, charpp, sizep);
    }
}

void FillColumnString(PyObject * obj, ColumnString * column)
{
    ColumnString::Offsets & offsets = column->getOffsets();
    ColumnString::Chars & chars = column->getChars();
    // if obj is bytes
    // if (PyBytes_Check(obj))
    // {
    //     // convert bytes to string
    //     column->insertData(data, bytes_size);
    // }
    // else
    if (PyUnicode_IS_COMPACT_ASCII(obj))
    {
        // if obj is unicode
        const char * data = reinterpret_cast<const char *>(PyUnicode_DATA(obj));
        size_t unicode_len = PyUnicode_GET_LENGTH(obj);
        column->insertData(data, unicode_len);
    }
    else
    {
        PyCompactUnicodeObject * unicode = reinterpret_cast<PyCompactUnicodeObject *>(obj);
        if (unicode->utf8 != nullptr)
        {
            // It's utf8 string, treat it like ASCII
            const char * data = reinterpret_cast<const char *>(unicode->utf8);
            column->insertData(data, unicode->utf8_length);
        }
        else if (PyUnicode_IS_COMPACT(obj))
        {
            auto kind = PyUnicode_KIND(obj);
            const char * data;
            size_t codepoint_cnt;

            if (kind == PyUnicode_1BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_1BYTE_DATA(obj));
            else if (kind == PyUnicode_2BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_2BYTE_DATA(obj));
            else if (kind == PyUnicode_4BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_4BYTE_DATA(obj));
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported unicode kind {}", kind);
            codepoint_cnt = PyUnicode_GET_LENGTH(obj);
            ConvertPyUnicodeToUtf8(data, kind, codepoint_cnt, offsets, chars);
        }
        else
        {
            Py_ssize_t bytes_size = -1;
            // const char * data = PyUnicode_AsUTF8AndSize(obj, &bytes_size);
            char * data = nullptr;
            bytes_size = PyString_AsStringAndSize(obj, &data, &bytes_size);
            if (bytes_size < 0)
                throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Failed to convert Python unicode object to UTF-8");
            column->insertData(data, bytes_size);
        }
    }
}


const char * GetPyUtf8StrData(PyObject * obj, size_t & buf_len)
{
    // See: https://github.com/python/cpython/blob/3.9/Include/cpython/unicodeobject.h#L81
    if (PyUnicode_IS_COMPACT_ASCII(obj))
    {
        const char * data = reinterpret_cast<const char *>(PyUnicode_1BYTE_DATA(obj));
        buf_len = PyUnicode_GET_LENGTH(obj);
        return data;
    }
    else
    {
        PyCompactUnicodeObject * unicode = reinterpret_cast<PyCompactUnicodeObject *>(obj);
        if (unicode->utf8 != nullptr)
        {
            // It's utf8 string, treat it like ASCII
            const char * data = reinterpret_cast<const char *>(unicode->utf8);
            buf_len = unicode->utf8_length;
            return data;
        }
        else if (PyUnicode_IS_COMPACT(obj))
        {
            auto kind = PyUnicode_KIND(obj);
            /// We could not use the implementation provided by CPython like below because it requires GIL holded by the caller
            // if (kind == PyUnicode_1BYTE_KIND || kind == PyUnicode_2BYTE_KIND || kind == PyUnicode_4BYTE_KIND)
            // {
            //     // always convert it to utf8
            //     const char * data = PyUnicode_AsUTF8AndSize(obj, &unicode->utf8_length);
            //     buf_len = unicode->utf8_length;
            //     // set the utf8 buffer back
            //     unicode->utf8 = const_cast<char *>(data);
            //     return data;
            // }
            const char * data;
            size_t codepoint_cnt;

            if (kind == PyUnicode_1BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_1BYTE_DATA(obj));
            else if (kind == PyUnicode_2BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_2BYTE_DATA(obj));
            else if (kind == PyUnicode_4BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_4BYTE_DATA(obj));
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported unicode kind {}", kind);
            // always convert it to utf8, and we can't use as function provided by CPython because it requires GIL
            // holded by the caller. So we have to do it manually with libicu
            codepoint_cnt = PyUnicode_GET_LENGTH(obj);
            data = ConvertPyUnicodeToUtf8(data, kind, codepoint_cnt, buf_len);
            // set the utf8 buffer back like PyUnicode_AsUTF8AndSize does, so that we can reuse it
            // and also we can avoid the memory leak
            unicode->utf8 = const_cast<char *>(data);
            unicode->utf8_length = buf_len;
            return data;
        }
        else
        {
            // always convert it to utf8, but this case is rare, here goes the slow path
            py::gil_scoped_acquire acquire;
            // PyUnicode_AsUTF8AndSize caches the UTF-8 encoded string in the unicodeobject
            // and subsequent calls will return the same string.  The memory is released
            // when the unicodeobject is deallocated.
            Py_ssize_t bytes_size = -1;
            const char * data = PyUnicode_AsUTF8AndSize(obj, &bytes_size);
            if (bytes_size < 0)
                throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Failed to convert Python unicode object to UTF-8");
            buf_len = bytes_size;
            return data;
        }
    }
}

bool _isInheritsFromPyReader(const py::handle & obj)
{
    try
    {
        // Check directly if obj is an instance of a class named "PyReader"
        if (py::str(obj.attr("__class__").attr("__name__")).cast<std::string>() == "PyReader")
            return true;

        // Check the direct base classes of obj's class for "PyReader"
        py::tuple bases = obj.attr("__class__").attr("__bases__");
        for (auto base : bases)
            if (py::str(base.attr("__name__")).cast<std::string>() == "PyReader")
                return true;
    }
    catch (const py::error_already_set &)
    {
        // Ignore the exception, and return false
    }

    return false;
}

// Will try to get the ref of py::array from pandas Series, or PyArrow Table
// without import numpy or pyarrow. Just from class name for now.
const void * tryGetPyArray(const py::object & obj, py::handle & result, py::handle & tmp, std::string & type_name, size_t & row_count)
{
    py::gil_scoped_acquire acquire;
    type_name = py::str(obj.attr("__class__").attr("__name__")).cast<std::string>();
    if (type_name == "ndarray")
    {
        // Return the handle of py::array directly
        row_count = py::len(obj);
        py::array array = obj.cast<py::array>();
        result = array;
        return array.data();
    }
    else if (type_name == "Series")
    {
        // Try to get the handle of py::array from pandas Series
        py::array array = obj.attr("values");
        row_count = py::len(obj);
        // if element type is bytes or object, we need to convert it to string
        // chdb todo: need more type check
        if (row_count > 0)
        {
            auto elem_type = obj.attr("__getitem__")(0).attr("__class__").attr("__name__").cast<std::string>();
            if (elem_type == "str" || elem_type == "unicode")
            {
                result = array;
                return array.data();
            }
            if (elem_type == "bytes" || elem_type == "object")
            {
                // chdb todo: better handle for bytes and object type
                auto str_obj = obj.attr("astype")(py::dtype("str"));
                array = str_obj.attr("values");
                result = array;
                tmp = array;
                tmp.inc_ref();
                return array.data();
            }
        }

        result = array;
        return array.data();
    }
    else if (type_name == "Table")
    {
        // Try to get the handle of py::array from PyArrow Table
        py::array array = obj.attr("to_pandas")();
        row_count = py::len(obj);
        result = array;
        tmp = array;
        tmp.inc_ref();
        return array.data();
    }
    else if (type_name == "ChunkedArray")
    {
        // Try to get the handle of py::array from PyArrow ChunkedArray
        py::array array = obj.attr("to_numpy")();
        row_count = py::len(obj);
        result = array;
        tmp = array;
        tmp.inc_ref();
        return array.data();
    }
    else if (type_name == "list")
    {
        // Just set the row count for list
        row_count = py::len(obj);
        result = obj;
        return obj.ptr();
    }

    // chdb todo: maybe convert list to py::array?

    return nullptr;
}
}
