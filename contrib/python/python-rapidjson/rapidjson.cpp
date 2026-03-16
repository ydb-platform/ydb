// -*- coding: utf-8 -*-
// :Project:   python-rapidjson -- Python extension module
// :Author:    Ken Robbins <ken@kenrobbins.com>
// :License:   MIT License
// :Copyright: © 2015 Ken Robbins
// :Copyright: © 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 Lele Gaifax
//

#include <locale.h>

#include <Python.h>
#include <datetime.h>
#include <structmember.h>

#include <algorithm>
#include <cmath>
#include <string>
#include <vector>

#include "rapidjson/reader.h"
#include "rapidjson/schema.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/error/en.h"


using namespace rapidjson;


/* On some MacOS combo, using Py_IS_XXX() macros does not work (see
   https://github.com/python-rapidjson/python-rapidjson/issues/78).
   OTOH, MSVC < 2015 does not have std::isxxx() (see
   https://stackoverflow.com/questions/38441740/where-is-isnan-in-msvc-2010).
   Oh well... */

#if defined (_MSC_VER) && (_MSC_VER < 1900)
#define IS_NAN(x) Py_IS_NAN(x)
#define IS_INF(x) Py_IS_INFINITY(x)
#else
#define IS_NAN(x) std::isnan(x)
#define IS_INF(x) std::isinf(x)
#endif


static PyObject* decimal_type = NULL;
static PyObject* timezone_type = NULL;
static PyObject* timezone_utc = NULL;
static PyObject* uuid_type = NULL;
static PyObject* validation_error = NULL;
static PyObject* decode_error = NULL;


/* These are the names of often used methods or literal values, interned in the module
   initialization function, to avoid repeated creation/destruction of PyUnicode values
   from plain C strings.

   We cannot use _Py_IDENTIFIER() because that upsets the GNU C++ compiler in -pedantic
   mode. */

static PyObject* astimezone_name = NULL;
static PyObject* hex_name = NULL;
static PyObject* timestamp_name = NULL;
static PyObject* total_seconds_name = NULL;
static PyObject* utcoffset_name = NULL;
static PyObject* is_infinite_name = NULL;
static PyObject* is_nan_name = NULL;
static PyObject* start_object_name = NULL;
static PyObject* end_object_name = NULL;
static PyObject* default_name = NULL;
static PyObject* end_array_name = NULL;
static PyObject* string_name = NULL;
static PyObject* read_name = NULL;
static PyObject* write_name = NULL;
static PyObject* encoding_name = NULL;

static PyObject* minus_inf_string_value = NULL;
static PyObject* nan_string_value = NULL;
static PyObject* plus_inf_string_value = NULL;


struct HandlerContext {
    PyObject* object;
    const char* key;
    SizeType keyLength;
    bool isObject;
    bool keyValuePairs;
    bool copiedKey;
};


enum DatetimeMode {
    DM_NONE = 0,
    // Formats
    DM_ISO8601 = 1<<0,      // Bidirectional ISO8601 for datetimes, dates and times
    DM_UNIX_TIME = 1<<1,    // Serialization only, "Unix epoch"-based number of seconds
    // Options
    DM_ONLY_SECONDS = 1<<4, // Truncate values to the whole second, ignoring micro seconds
    DM_IGNORE_TZ = 1<<5,    // Ignore timezones
    DM_NAIVE_IS_UTC = 1<<6, // Assume naive datetime are in UTC timezone
    DM_SHIFT_TO_UTC = 1<<7, // Shift to/from UTC
    DM_MAX = 1<<8
};


#define DATETIME_MODE_FORMATS_MASK 0x0f // 0b00001111 in C++14


static inline int
datetime_mode_format(unsigned mode) {
    return mode & DATETIME_MODE_FORMATS_MASK;
}


static inline bool
valid_datetime_mode(int mode) {
    int format = datetime_mode_format(mode);
    return (mode >= 0 && mode < DM_MAX
            && (format <= DM_UNIX_TIME)
            && (mode == 0 || format > 0));
}


static int
days_per_month(int year, int month) {
    assert(month >= 1);
    assert(month <= 12);
    if (month == 1 || month == 3 || month == 5 || month == 7
        || month == 8 || month == 10 || month == 12) {
        return 31;
    } else if (month == 4 || month == 6 || month == 9 || month == 11) {
        return 30;
    } else if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        return 29;
    } else {
        return 28;
    }
}


enum UuidMode {
    UM_NONE = 0,
    UM_CANONICAL = 1<<0, // 4-dashed 32 hex chars: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    UM_HEX = 1<<1,       // canonical OR 32 hex chars in a row
    UM_MAX = 1<<2
};


enum NumberMode {
    NM_NONE = 0,
    NM_NAN = 1<<0,     // allow "not-a-number" values
    NM_DECIMAL = 1<<1, // serialize Decimal instances, deserialize floats as Decimal
    NM_NATIVE = 1<<2,  // use faster native C library number handling
    NM_MAX = 1<<3
};


enum BytesMode {
    BM_NONE = 0,
    BM_UTF8 = 1<<0,             // try to convert to UTF-8
    BM_MAX = 1<<1
};


enum ParseMode {
    PM_NONE = 0,
    PM_COMMENTS = 1<<0,         // Allow one-line // ... and multi-line /* ... */ comments
    PM_TRAILING_COMMAS = 1<<1,  // allow trailing commas at the end of objects and arrays
    PM_MAX = 1<<2
};


enum WriteMode {
    WM_COMPACT = 0,
    WM_PRETTY = 1<<0,            // Use PrettyWriter
    WM_SINGLE_LINE_ARRAY = 1<<1, // Format arrays on a single line
    WM_MAX = 1<<2
};


enum IterableMode {
    IM_ANY_ITERABLE = 0,        // Default, any iterable is dumped as JSON array
    IM_ONLY_LISTS = 1<<0,       // Only list instances are dumped as JSON arrays
    IM_MAX = 1<<1
};


enum MappingMode {
    MM_ANY_MAPPING = 0,                // Default, any mapping is dumped as JSON object
    MM_ONLY_DICTS = 1<<0,              // Only dict instances are dumped as JSON objects
    MM_COERCE_KEYS_TO_STRINGS = 1<<1,  // Convert keys to strings
    MM_SKIP_NON_STRING_KEYS = 1<<2,    // Ignore non-string keys
    MM_SORT_KEYS = 1<<3,               // Sort keys
    MM_MAX = 1<<4
};


//////////////////////////
// Forward declarations //
//////////////////////////


static PyObject* do_decode(PyObject* decoder,
                           const char* jsonStr, Py_ssize_t jsonStrlen,
                           PyObject* jsonStream, size_t chunkSize,
                           PyObject* objectHook,
                           unsigned numberMode, unsigned datetimeMode,
                           unsigned uuidMode, unsigned parseMode);
static PyObject* decoder_call(PyObject* self, PyObject* args, PyObject* kwargs);
static PyObject* decoder_new(PyTypeObject* type, PyObject* args, PyObject* kwargs);


static PyObject* do_encode(PyObject* value, PyObject* defaultFn, bool ensureAscii,
                           unsigned writeMode, char indentChar, unsigned indentCount,
                           unsigned numberMode, unsigned datetimeMode,
                           unsigned uuidMode, unsigned bytesMode,
                           unsigned iterableMode, unsigned mappingMode);
static PyObject* do_stream_encode(PyObject* value, PyObject* stream, size_t chunkSize,
                                  PyObject* defaultFn, bool ensureAscii,
                                  unsigned writeMode, char indentChar,
                                  unsigned indentCount, unsigned numberMode,
                                  unsigned datetimeMode, unsigned uuidMode,
                                  unsigned bytesMode, unsigned iterableMode,
                                  unsigned mappingMode);
static PyObject* encoder_call(PyObject* self, PyObject* args, PyObject* kwargs);
static PyObject* encoder_new(PyTypeObject* type, PyObject* args, PyObject* kwargs);


static PyObject* validator_call(PyObject* self, PyObject* args, PyObject* kwargs);
static void validator_dealloc(PyObject* self);
static PyObject* validator_new(PyTypeObject* type, PyObject* args, PyObject* kwargs);


///////////////////////////////////////////////////
// Stream wrapper around Python file-like object //
///////////////////////////////////////////////////


class PyReadStreamWrapper {
public:
    typedef char Ch;

    PyReadStreamWrapper(PyObject* stream, size_t size)
        : stream(stream) {
        Py_INCREF(stream);
        chunkSize = PyLong_FromUnsignedLong(size);
        buffer = NULL;
        chunk = NULL;
        chunkLen = 0;
        pos = 0;
        offset = 0;
        eof = false;
    }

    ~PyReadStreamWrapper() {
        Py_CLEAR(stream);
        Py_CLEAR(chunkSize);
        Py_CLEAR(chunk);
    }

    Ch Peek() {
        if (!eof && pos == chunkLen) {
            Read();
        }
        return eof ? '\0' : buffer[pos];
    }

    Ch Take() {
        if (!eof && pos == chunkLen) {
            Read();
        }
        return eof ? '\0' : buffer[pos++];
    }

    size_t Tell() const {
        return offset + pos;
    }

    void Flush() {
        assert(false);
    }

    void Put(Ch c) {
        assert(false);
    }

    Ch* PutBegin() {
        assert(false);
        return 0;
    }

    size_t PutEnd(Ch* begin) {
        assert(false);
        return 0;
    }

private:
    void Read() {
        Py_CLEAR(chunk);

        chunk = PyObject_CallMethodObjArgs(stream, read_name, chunkSize, NULL);

        if (chunk == NULL) {
            eof = true;
        } else {
            Py_ssize_t len;

            if (PyBytes_Check(chunk)) {
                len = PyBytes_GET_SIZE(chunk);
                buffer = PyBytes_AS_STRING(chunk);
            } else {
                buffer = PyUnicode_AsUTF8AndSize(chunk, &len);
                if (buffer == NULL) {
                    len = 0;
                }
            }

            if (len == 0) {
                eof = true;
            } else {
                offset += chunkLen;
                chunkLen = len;
                pos = 0;
            }
        }
    }

    PyObject* stream;
    PyObject* chunkSize;
    PyObject* chunk;
    const Ch* buffer;
    size_t chunkLen;
    size_t pos;
    size_t offset;
    bool eof;
};


class PyWriteStreamWrapper {
public:
    typedef char Ch;

    PyWriteStreamWrapper(PyObject* stream, size_t size)
        : stream(stream) {
        Py_INCREF(stream);
        buffer = (char*) PyMem_Malloc(size);
        assert(buffer);
        bufferEnd = buffer + size;
        cursor = buffer;
        multiByteChar = NULL;
        isBinary = !PyObject_HasAttr(stream, encoding_name);
    }

    ~PyWriteStreamWrapper() {
        Py_CLEAR(stream);
        PyMem_Free(buffer);
    }

    Ch Peek() {
        assert(false);
        return 0;
    }

    Ch Take() {
        assert(false);
        return 0;
    }

    size_t Tell() const {
        assert(false);
        return 0;
    }

    void Flush() {
        PyObject* c;
        if (isBinary) {
            c = PyBytes_FromStringAndSize(buffer, (size_t)(cursor - buffer));
            cursor = buffer;
        } else {
            if (multiByteChar == NULL) {
                c = PyUnicode_FromStringAndSize(buffer, (size_t)(cursor - buffer));
                cursor = buffer;
            } else {
                size_t complete = (size_t)(multiByteChar - buffer);
                c = PyUnicode_FromStringAndSize(buffer, complete);
                size_t remaining = (size_t)(cursor - multiByteChar);
                if (RAPIDJSON_LIKELY(remaining < complete))
                    memcpy(buffer, multiByteChar, remaining);
                else
                    std::memmove(buffer, multiByteChar, remaining);
                cursor = buffer + remaining;
                multiByteChar = NULL;
            }
        }
        if (c == NULL) {
            // Propagate the error state, it will be caught by dumps_internal()
        } else {
            PyObject* res = PyObject_CallMethodObjArgs(stream, write_name, c, NULL);
            if (res == NULL) {
                // Likewise
            } else {
                Py_DECREF(res);
            }
            Py_DECREF(c);
        }
    }

    void Put(Ch c) {
        if (cursor == bufferEnd)
            Flush();
        if (!isBinary) {
            if ((c & 0x80) == 0) {
                multiByteChar = NULL;
            } else if (c & 0x40) {
                multiByteChar = cursor;
            }
        }
        *cursor++ = c;
    }

    Ch* PutBegin() {
        assert(false);
        return 0;
    }

    size_t PutEnd(Ch* begin) {
        assert(false);
        return 0;
    }

private:
    PyObject* stream;
    Ch* buffer;
    Ch* bufferEnd;
    Ch* cursor;
    Ch* multiByteChar;
    bool isBinary;
};


inline void PutUnsafe(PyWriteStreamWrapper& stream, char c) {
    stream.Put(c);
}


/////////////
// RawJSON //
/////////////


typedef struct {
    PyObject_HEAD
    PyObject* value;
} RawJSON;


static void
RawJSON_dealloc(RawJSON* self)
{
    Py_XDECREF(self->value);
    Py_TYPE(self)->tp_free((PyObject*) self);
}


static PyObject*
RawJSON_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyObject* self = type->tp_alloc(type, 0);
    static char const* kwlist[] = {
        "value",
        NULL
    };
    PyObject* value = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "U", (char**) kwlist, &value))
        return NULL;

    ((RawJSON*) self)->value = value;

    Py_INCREF(value);

    return self;
}

static PyMemberDef RawJSON_members[] = {
    {"value",
     T_OBJECT_EX, offsetof(RawJSON, value), READONLY,
     "string representing a serialized JSON object"},
    {NULL}  /* Sentinel */
};


PyDoc_STRVAR(rawjson_doc,
             "Raw (preserialized) JSON object\n"
             "\n"
             "When rapidjson tries to serialize instances of this class, it will"
             " use their literal `value`. For instance:\n"
             ">>> rapidjson.dumps(RawJSON('{\"already\": \"serialized\"}'))\n"
             "'{\"already\": \"serialized\"}'");


static PyTypeObject RawJSON_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "rapidjson.RawJSON",            /* tp_name */
    sizeof(RawJSON),                /* tp_basicsize */
    0,                              /* tp_itemsize */
    (destructor) RawJSON_dealloc,   /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    0,                              /* tp_setattr */
    0,                              /* tp_compare */
    0,                              /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    0,                              /* tp_call */
    0,                              /* tp_str */
    0,                              /* tp_getattro */
    0,                              /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,             /* tp_flags */
    rawjson_doc,                    /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    0,                              /* tp_methods */
    RawJSON_members,                /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    0,                              /* tp_init */
    0,                              /* tp_alloc */
    RawJSON_new,                    /* tp_new */
};


static bool
accept_indent_arg(PyObject* arg, unsigned &write_mode, unsigned &indent_count,
                   char &indent_char)
{
    if (arg != NULL && arg != Py_None) {
        write_mode = WM_PRETTY;

        if (PyLong_Check(arg) && PyLong_AsLong(arg) >= 0) {
            indent_count = PyLong_AsUnsignedLong(arg);
        } else if (PyUnicode_Check(arg)) {
            Py_ssize_t len;
            const char* indentStr = PyUnicode_AsUTF8AndSize(arg, &len);

            indent_count = len;
            if (indent_count) {
                indent_char = '\0';
                while (len--) {
                    char ch = indentStr[len];

                    if (ch == '\n' || ch == ' ' || ch == '\t' || ch == '\r') {
                        if (indent_char == '\0') {
                            indent_char = ch;
                        } else if (indent_char != ch) {
                            PyErr_SetString(
                                PyExc_TypeError,
                                "indent string cannot contains different chars");
                            return false;
                        }
                    } else {
                        PyErr_SetString(PyExc_TypeError,
                                        "non-whitespace char in indent string");
                        return false;
                    }
                }
            }
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "indent must be a non-negative int or a string");
            return false;
        }
    }
    return true;
}

static bool
accept_write_mode_arg(PyObject* arg, unsigned &write_mode)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (mode < 0 || mode >= WM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid write_mode");
                return false;
            }
            write_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "write_mode must be a non-negative int");
            return false;
        }
    }
    return true;
}

static bool
accept_number_mode_arg(PyObject* arg, int allow_nan, unsigned &number_mode)
{
    if (arg != NULL) {
        if (arg == Py_None)
            number_mode = NM_NONE;
        else if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (mode < 0 || mode >= NM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid number_mode, out of range");
                return false;
            }
            number_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "number_mode must be a non-negative int");
            return false;
        }
    }
    if (allow_nan != -1) {
        if (allow_nan)
            number_mode |= NM_NAN;
        else
            number_mode &= ~NM_NAN;
    }
    return true;
}

static bool
accept_datetime_mode_arg(PyObject* arg, unsigned &datetime_mode)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (!valid_datetime_mode(mode)) {
                PyErr_SetString(PyExc_ValueError, "Invalid datetime_mode, out of range");
                return false;
            }
            datetime_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "datetime_mode must be a non-negative int");
            return false;
        }
    }
    return true;
}

static bool
accept_uuid_mode_arg(PyObject* arg, unsigned &uuid_mode)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (mode < 0 || mode >= UM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid uuid_mode, out of range");
                return false;
            }
            uuid_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError, "uuid_mode must be a non-negative int");
            return false;
        }
    }
    return true;
}

static bool
accept_bytes_mode_arg(PyObject* arg, unsigned &bytes_mode)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (mode < 0 || mode >= BM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid bytes_mode, out of range");
                return false;
            }
            bytes_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError, "bytes_mode must be a non-negative int");
            return false;
        }
    }
    return true;
}

static bool
accept_iterable_mode_arg(PyObject* arg, unsigned &iterable_mode)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (mode < 0 || mode >= IM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid iterable_mode, out of range");
                return false;
            }
            iterable_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError, "iterable_mode must be a non-negative int");
            return false;
        }
    }
    return true;
}

static bool
accept_mapping_mode_arg(PyObject* arg, unsigned &mapping_mode)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (mode < 0 || mode >= MM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid mapping_mode, out of range");
                return false;
            }
            mapping_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError, "mapping_mode must be a non-negative int");
            return false;
        }
    }
    return true;
}

static bool
accept_chunk_size_arg(PyObject* arg, size_t &chunk_size)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            Py_ssize_t size = PyNumber_AsSsize_t(arg, PyExc_ValueError);
            if (PyErr_Occurred() || size < 4 || size > UINT_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid chunk_size, out of range");
                return false;
            }
            chunk_size = (size_t) size;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "chunk_size must be a non-negative int");
            return false;
        }
    }
    return true;
}

static bool
accept_parse_mode_arg(PyObject* arg, unsigned &parse_mode)
{
    if (arg != NULL && arg != Py_None) {
        if (PyLong_Check(arg)) {
            long mode = PyLong_AsLong(arg);
            if (mode < 0 || mode >= PM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid parse_mode, out of range");
                return false;
            }
            parse_mode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "parse_mode must be a non-negative int");
            return false;
        }
    }
    return true;
}


/////////////
// Decoder //
/////////////


/* Adapted from CPython's Objects/floatobject.c::float_from_string_inner() */

static PyObject*
float_from_string(const char* s, Py_ssize_t len)
{
    double x;
    const char* end;

    /* We don't care about overflow or underflow.  If the platform
     * supports them, infinities and signed zeroes (on underflow) are
     * fine. */
    x = PyOS_string_to_double(s, (char **) &end, NULL);
    if (end != s + len) {
        return NULL;
    } else if (x == -1.0 && PyErr_Occurred()) {
        return NULL;
    } else {
        return PyFloat_FromDouble(x);
    }
}


struct PyHandler {
    PyObject* decoderStartObject;
    PyObject* decoderEndObject;
    PyObject* decoderEndArray;
    PyObject* decoderString;
    PyObject* sharedKeys;
    PyObject* root;
    PyObject* objectHook;
    unsigned datetimeMode;
    unsigned uuidMode;
    unsigned numberMode;
    unsigned recursionLimit;
    std::vector<HandlerContext> stack;

    PyHandler(PyObject* decoder,
              PyObject* hook,
              unsigned dm,
              unsigned um,
              unsigned nm)
        : decoderStartObject(NULL),
          decoderEndObject(NULL),
          decoderEndArray(NULL),
          decoderString(NULL),
          root(NULL),
          objectHook(hook),
          datetimeMode(dm),
          uuidMode(um),
          numberMode(nm)
        {
            stack.reserve(128);
            if (decoder != NULL) {
                assert(!objectHook);
                if (PyObject_HasAttr(decoder, start_object_name)) {
                    decoderStartObject = PyObject_GetAttr(decoder, start_object_name);
                }
                if (PyObject_HasAttr(decoder, end_object_name)) {
                    decoderEndObject = PyObject_GetAttr(decoder, end_object_name);
                }
                if (PyObject_HasAttr(decoder, end_array_name)) {
                    decoderEndArray = PyObject_GetAttr(decoder, end_array_name);
                }
                if (PyObject_HasAttr(decoder, string_name)) {
                    decoderString = PyObject_GetAttr(decoder, string_name);
                }
            }
            sharedKeys = PyDict_New();
            recursionLimit = Py_GetRecursionLimit();
        }

    ~PyHandler() {
        while (!stack.empty()) {
            const HandlerContext& ctx = stack.back();
            if (ctx.copiedKey)
                PyMem_Free((void*) ctx.key);
            if (ctx.object != NULL)
                Py_DECREF(ctx.object);
            stack.pop_back();
        }
        Py_CLEAR(decoderStartObject);
        Py_CLEAR(decoderEndObject);
        Py_CLEAR(decoderEndArray);
        Py_CLEAR(decoderString);
        Py_CLEAR(sharedKeys);
    }

    bool Handle(PyObject* value) {
        if (root) {
            const HandlerContext& current = stack.back();

            if (current.isObject) {
                PyObject* key = PyUnicode_FromStringAndSize(current.key,
                                                            current.keyLength);
                if (key == NULL) {
                    Py_DECREF(value);
                    return false;
                }

                PyObject* shared_key = PyDict_SetDefault(sharedKeys, key, key);
                if (shared_key == NULL) {
                    Py_DECREF(key);
                    Py_DECREF(value);
                    return false;
                }
                Py_INCREF(shared_key);
                Py_DECREF(key);
                key = shared_key;

                int rc;
                if (current.keyValuePairs) {
                    PyObject* pair = PyTuple_Pack(2, key, value);

                    Py_DECREF(key);
                    Py_DECREF(value);
                    if (pair == NULL) {
                        return false;
                    }
                    rc = PyList_Append(current.object, pair);
                    Py_DECREF(pair);
                } else {
                    if (PyDict_CheckExact(current.object))
                        // If it's a standard dictionary, this is +20% faster
                        rc = PyDict_SetItem(current.object, key, value);
                    else
                        rc = PyObject_SetItem(current.object, key, value);
                    Py_DECREF(key);
                    Py_DECREF(value);
                }

                if (rc == -1) {
                    return false;
                }
            } else {
                PyList_Append(current.object, value);
                Py_DECREF(value);
            }
        } else {
            root = value;
        }
        return true;
    }

    bool Key(const char* str, SizeType length, bool copy) {
        HandlerContext& current = stack.back();

        // This happens when operating in stream mode and kParseInsituFlag is not set: we
        // must copy the incoming string in the context, and destroy the duplicate when
        // the context gets reused for the next dictionary key

        if (current.key && current.copiedKey) {
            PyMem_Free((void*) current.key);
            current.key = NULL;
        }

        if (copy) {
            char* copied_str = (char*) PyMem_Malloc(length+1);
            if (copied_str == NULL)
                return false;
            memcpy(copied_str, str, length+1);
            str = copied_str;
            assert(!current.key);
        }

        current.key = str;
        current.keyLength = length;
        current.copiedKey = copy;

        return true;
    }

    bool StartObject() {
        if (recursionLimit-- == 0) {
            PyErr_SetString(PyExc_RecursionError,
                            "Maximum parse recursion depth exceeded");
            return false;
        }

        PyObject* mapping;
        bool key_value_pairs;

        if (decoderStartObject != NULL) {
            mapping = PyObject_CallFunctionObjArgs(decoderStartObject, NULL);
            if (mapping == NULL)
                return false;
            key_value_pairs = PyList_Check(mapping);
            if (!PyMapping_Check(mapping) && !key_value_pairs) {
                Py_DECREF(mapping);
                PyErr_SetString(PyExc_ValueError,
                                "start_object() must return a mapping or a list instance");
                return false;
            }
        } else {
            mapping = PyDict_New();
            if (mapping == NULL) {
                return false;
            }
            key_value_pairs = false;
        }

        if (!Handle(mapping)) {
            return false;
        }

        HandlerContext ctx;
        ctx.isObject = true;
        ctx.keyValuePairs = key_value_pairs;
        ctx.object = mapping;
        ctx.key = NULL;
        ctx.copiedKey = false;
        Py_INCREF(mapping);

        stack.push_back(ctx);

        return true;
    }

    bool EndObject(SizeType member_count) {
        recursionLimit++;

        const HandlerContext& ctx = stack.back();

        if (ctx.copiedKey)
            PyMem_Free((void*) ctx.key);

        PyObject* mapping = ctx.object;
        stack.pop_back();

        if (objectHook == NULL && decoderEndObject == NULL) {
            Py_DECREF(mapping);
            return true;
        }

        PyObject* replacement;
        if (decoderEndObject != NULL) {
            replacement = PyObject_CallFunctionObjArgs(decoderEndObject, mapping, NULL);
        } else /* if (objectHook != NULL) */ {
            replacement = PyObject_CallFunctionObjArgs(objectHook, mapping, NULL);
        }

        Py_DECREF(mapping);
        if (replacement == NULL)
            return false;

        if (!stack.empty()) {
            HandlerContext& current = stack.back();

            if (current.isObject) {
                PyObject* key = PyUnicode_FromStringAndSize(current.key,
                                                            current.keyLength);
                if (key == NULL) {
                    Py_DECREF(replacement);
                    return false;
                }

                PyObject* shared_key = PyDict_SetDefault(sharedKeys, key, key);
                if (shared_key == NULL) {
                    Py_DECREF(key);
                    Py_DECREF(replacement);
                    return false;
                }
                Py_INCREF(shared_key);
                Py_DECREF(key);
                key = shared_key;

                int rc;
                if (current.keyValuePairs) {
                    PyObject* pair = PyTuple_Pack(2, key, replacement);

                    Py_DECREF(key);
                    Py_DECREF(replacement);
                    if (pair == NULL) {
                        return false;
                    }

                    Py_ssize_t listLen = PyList_GET_SIZE(current.object);

                    rc = PyList_SetItem(current.object, listLen - 1, pair);

                    // NB: PyList_SetItem() steals a reference on the replacement, so it
                    // must not be DECREFed when the operation succeeds

                    if (rc == -1) {
                        Py_DECREF(pair);
                        return false;
                    }
                } else {
                    if (PyDict_CheckExact(current.object))
                        // If it's a standard dictionary, this is +20% faster
                        rc = PyDict_SetItem(current.object, key, replacement);
                    else
                        rc = PyObject_SetItem(current.object, key, replacement);
                    Py_DECREF(key);
                    Py_DECREF(replacement);
                    if (rc == -1) {
                        return false;
                    }
                }
            } else {
                // Change these to PySequence_Size() and PySequence_SetItem(),
                // should we implement Decoder.start_array()
                Py_ssize_t listLen = PyList_GET_SIZE(current.object);
                int rc = PyList_SetItem(current.object, listLen - 1, replacement);

                // NB: PyList_SetItem() steals a reference on the replacement, so it must
                // not be DECREFed when the operation succeeds

                if (rc == -1) {
                    Py_DECREF(replacement);
                    return false;
                }
            }
        } else {
            Py_SETREF(root, replacement);
        }

        return true;
    }

    bool StartArray() {
        if (recursionLimit-- == 0) {
            PyErr_SetString(PyExc_RecursionError,
                            "Maximum parse recursion depth exceeded!");
            return false;
        }

        PyObject* list = PyList_New(0);
        if (list == NULL) {
            return false;
        }

        if (!Handle(list)) {
            return false;
        }

        HandlerContext ctx;
        ctx.isObject = false;
        ctx.object = list;
        ctx.key = NULL;
        ctx.copiedKey = false;
        Py_INCREF(list);

        stack.push_back(ctx);

        return true;
    }

    bool EndArray(SizeType elementCount) {
        recursionLimit++;

        const HandlerContext& ctx = stack.back();

        if (ctx.copiedKey)
            PyMem_Free((void*) ctx.key);

        PyObject* sequence = ctx.object;
        stack.pop_back();

        if (decoderEndArray == NULL) {
            Py_DECREF(sequence);
            return true;
        }

        PyObject* replacement = PyObject_CallFunctionObjArgs(decoderEndArray, sequence,
                                                             NULL);
        Py_DECREF(sequence);
        if (replacement == NULL)
            return false;

        if (!stack.empty()) {
            const HandlerContext& current = stack.back();

            if (current.isObject) {
                PyObject* key = PyUnicode_FromStringAndSize(current.key,
                                                            current.keyLength);
                if (key == NULL) {
                    Py_DECREF(replacement);
                    return false;
                }

                int rc;
                if (PyDict_Check(current.object))
                    // If it's a standard dictionary, this is +20% faster
                    rc = PyDict_SetItem(current.object, key, replacement);
                else
                    rc = PyObject_SetItem(current.object, key, replacement);

                Py_DECREF(key);
                Py_DECREF(replacement);

                if (rc == -1) {
                    return false;
                }
            } else {
                // Change these to PySequence_Size() and PySequence_SetItem(),
                // should we implement Decoder.start_array()
                Py_ssize_t listLen = PyList_GET_SIZE(current.object);
                int rc = PyList_SetItem(current.object, listLen - 1, replacement);

                // NB: PyList_SetItem() steals a reference on the replacement, so it must
                // not be DECREFed when the operation succeeds

                if (rc == -1) {
                    Py_DECREF(replacement);
                    return false;
                }
            }
        } else {
            Py_SETREF(root, replacement);
        }

        return true;
    }

    bool NaN() {
        if (!(numberMode & NM_NAN)) {
            PyErr_SetString(PyExc_ValueError,
                            "Out of range float values are not JSON compliant");
            return false;
        }

        PyObject* value;
        if (numberMode & NM_DECIMAL) {
            value = PyObject_CallFunctionObjArgs(decimal_type, nan_string_value, NULL);
        } else {
            value = PyFloat_FromString(nan_string_value);
        }

        if (value == NULL)
            return false;

        return Handle(value);
    }

    bool Infinity(bool minus) {
        if (!(numberMode & NM_NAN)) {
            PyErr_SetString(PyExc_ValueError,
                            "Out of range float values are not JSON compliant");
            return false;
        }

        PyObject* value;
        if (numberMode & NM_DECIMAL) {
            value = PyObject_CallFunctionObjArgs(decimal_type,
                                                 minus
                                                 ? minus_inf_string_value
                                                 : plus_inf_string_value, NULL);
        } else {
            value = PyFloat_FromString(minus
                                       ? minus_inf_string_value
                                       : plus_inf_string_value);
        }

        if (value == NULL)
            return false;

        return Handle(value);
    }

    bool Null() {
        PyObject* value = Py_None;
        Py_INCREF(value);

        return Handle(value);
    }

    bool Bool(bool b) {
        PyObject* value = b ? Py_True : Py_False;
        Py_INCREF(value);

        return Handle(value);
    }

    bool Int(int i) {
        PyObject* value = PyLong_FromLong(i);
        return Handle(value);
    }

    bool Uint(unsigned i) {
        PyObject* value = PyLong_FromUnsignedLong(i);
        return Handle(value);
    }

    bool Int64(int64_t i) {
        PyObject* value = PyLong_FromLongLong(i);
        return Handle(value);
    }

    bool Uint64(uint64_t i) {
        PyObject* value = PyLong_FromUnsignedLongLong(i);
        return Handle(value);
    }

    bool Double(double d) {
        PyObject* value = PyFloat_FromDouble(d);
        return Handle(value);
    }

    bool RawNumber(const char* str, SizeType length, bool copy) {
        PyObject* value;
        bool isFloat = false;

        for (int i = length - 1; i >= 0; --i) {
            // consider it a float if there is at least one non-digit character,
            // it may be either a decimal number or +-infinity or nan
            if (!isdigit(str[i]) && str[i] != '-') {
                isFloat = true;
                break;
            }
        }

        if (isFloat) {

            if (numberMode & NM_DECIMAL) {
                PyObject* pystr = PyUnicode_FromStringAndSize(str, length);
                if (pystr == NULL)
                    return false;
                value = PyObject_CallFunctionObjArgs(decimal_type, pystr, NULL);
                Py_DECREF(pystr);
            } else {
                std::string zstr(str, length);

                value = float_from_string(zstr.c_str(), length);
            }

        } else {
            std::string zstr(str, length);

            value = PyLong_FromString(zstr.c_str(), NULL, 10);
        }

        if (value == NULL) {
            PyErr_SetString(PyExc_ValueError,
                            isFloat
                            ? "Invalid float value"
                            : "Invalid integer value");
            return false;
        } else {
            return Handle(value);
        }
    }

#define digit(idx) (str[idx] - '0')

    bool IsIso8601Date(const char* str, int& year, int& month, int& day) {
        // we've already checked that str is a valid length and that 5 and 8 are '-'
        if (!isdigit(str[0]) || !isdigit(str[1]) || !isdigit(str[2]) || !isdigit(str[3])
            || !isdigit(str[5]) || !isdigit(str[6])
            || !isdigit(str[8]) || !isdigit(str[9])) return false;

        year = digit(0)*1000 + digit(1)*100 + digit(2)*10 + digit(3);
        month = digit(5)*10 + digit(6);
        day = digit(8)*10 + digit(9);

        return year > 0 && month <= 12 && day <= days_per_month(year, month);
    }

    bool IsIso8601Offset(const char* str, int& tzoff) {
        if (!isdigit(str[1]) || !isdigit(str[2]) || str[3] != ':'
            || !isdigit(str[4]) || !isdigit(str[5])) return false;

        int hofs = 0, mofs = 0, tzsign = 1;
        hofs = digit(1)*10 + digit(2);
        mofs = digit(4)*10 + digit(5);

        if (hofs > 23 || mofs > 59) return false;

        if (str[0] == '-') tzsign = -1;
        tzoff = tzsign * (hofs * 3600 + mofs * 60);
        return true;
    }

    bool IsIso8601Time(const char* str, SizeType length,
                       int& hours, int& mins, int& secs, int& usecs, int& tzoff) {
        // we've already checked that str is a minimum valid length, but nothing else
        if (!isdigit(str[0]) || !isdigit(str[1]) || str[2] != ':'
            || !isdigit(str[3]) || !isdigit(str[4]) || str[5] != ':'
            || !isdigit(str[6]) || !isdigit(str[7])) return false;

        hours = digit(0)*10 + digit(1);
        mins = digit(3)*10 + digit(4);
        secs = digit(6)*10 + digit(7);

        if (hours > 23 || mins > 59 || secs > 59) return false;

        if (length == 8 || (length == 9 && str[8] == 'Z')) {
            // just time
            return true;
        }


        if (length == 14 && (str[8] == '-' || str[8] == '+')) {
            return IsIso8601Offset(&str[8], tzoff);
        }

        // at this point we need a . AND at least 1 more digit
        if (length == 9 || str[8] != '.' || !isdigit(str[9])) return false;

        int usecLength;
        if (str[length - 1] == 'Z') {
            usecLength = length - 10;
        } else if (str[length - 3] == ':') {
            if (!IsIso8601Offset(&str[length - 6], tzoff)) return false;
            usecLength = length - 15;
        } else {
            usecLength = length - 9;
        }

        if (usecLength > 9) return false;

        switch (usecLength) {
            case 9: if (!isdigit(str[17])) { return false; }
            case 8: if (!isdigit(str[16])) { return false; }
            case 7: if (!isdigit(str[15])) { return false; }
            case 6: if (!isdigit(str[14])) { return false; } usecs += digit(14);
            case 5: if (!isdigit(str[13])) { return false; } usecs += digit(13)*10;
            case 4: if (!isdigit(str[12])) { return false; } usecs += digit(12)*100;
            case 3: if (!isdigit(str[11])) { return false; } usecs += digit(11)*1000;
            case 2: if (!isdigit(str[10])) { return false; } usecs += digit(10)*10000;
            case 1: if (!isdigit(str[9])) { return false; } usecs += digit(9)*100000;
        }

        return true;
    }

    bool IsIso8601(const char* str, SizeType length,
                   int& year, int& month, int& day,
                   int& hours, int& mins, int &secs, int& usecs, int& tzoff) {
        year = -1;
        month = day = hours = mins = secs = usecs = tzoff = 0;

        // Early exit for values that are clearly not valid (too short or too long)
        if (length < 8 || length > 35) return false;

        bool isDate = str[4] == '-' && str[7] == '-';

        if (!isDate) {
            return IsIso8601Time(str, length, hours, mins, secs, usecs, tzoff);
        }

        if (length == 10) {
            // if it looks like just a date, validate just the date
            return IsIso8601Date(str, year, month, day);
        }
        if (length > 18 && (str[10] == 'T' || str[10] == ' ')) {
            // if it looks like a date + time, validate date + time
            return IsIso8601Date(str, year, month, day)
                && IsIso8601Time(&str[11], length - 11, hours, mins, secs, usecs, tzoff);
        }
        // can't be valid
        return false;
    }

    bool HandleIso8601(const char* str, SizeType length,
                       int year, int month, int day,
                       int hours, int mins, int secs, int usecs, int tzoff) {
        // we treat year 0 as invalid and thus the default when there is no date
        bool hasDate = year > 0;

        if (length == 10 && hasDate) {
            // just a date, handle quickly
            return Handle(PyDate_FromDate(year, month, day));
        }

        bool isZ = str[length - 1] == 'Z';
        bool hasOffset = !isZ && (str[length - 6] == '-' || str[length - 6] == '+');

        PyObject* value;

        if ((datetimeMode & DM_NAIVE_IS_UTC || isZ) && !hasOffset) {
            if (hasDate) {
                value = PyDateTimeAPI->DateTime_FromDateAndTime(
                    year, month, day, hours, mins, secs, usecs, timezone_utc,
                    PyDateTimeAPI->DateTimeType);
            } else {
                value = PyDateTimeAPI->Time_FromTime(
                    hours, mins, secs, usecs, timezone_utc, PyDateTimeAPI->TimeType);
            }
        } else if (datetimeMode & DM_IGNORE_TZ || (!hasOffset && !isZ)) {
            if (hasDate) {
                value = PyDateTime_FromDateAndTime(year, month, day,
                                                   hours, mins, secs, usecs);
            } else {
                value = PyTime_FromTime(hours, mins, secs, usecs);
            }
        } else if (!hasDate && datetimeMode & DM_SHIFT_TO_UTC && tzoff) {
            PyErr_Format(PyExc_ValueError,
                         "Time literal cannot be shifted to UTC: %s", str);
            value = NULL;
        } else if (!hasDate && datetimeMode & DM_SHIFT_TO_UTC) {
            value = PyDateTimeAPI->Time_FromTime(
                hours, mins, secs, usecs, timezone_utc, PyDateTimeAPI->TimeType);
        } else {
            PyObject* offset = PyDateTimeAPI->Delta_FromDelta(0, tzoff, 0, 1,
                                                              PyDateTimeAPI->DeltaType);
            if (offset == NULL) {
                value = NULL;
            } else {
                PyObject* tz = PyObject_CallFunctionObjArgs(timezone_type, offset, NULL);
                Py_DECREF(offset);
                if (tz == NULL) {
                    value = NULL;
                } else {
                    if (hasDate) {
                        value = PyDateTimeAPI->DateTime_FromDateAndTime(
                            year, month, day, hours, mins, secs, usecs, tz,
                            PyDateTimeAPI->DateTimeType);
                        if (value != NULL && datetimeMode & DM_SHIFT_TO_UTC) {
                            PyObject* asUTC = PyObject_CallMethodObjArgs(
                                value, astimezone_name, timezone_utc, NULL);
                            Py_DECREF(value);
                            if (asUTC == NULL) {
                                value = NULL;
                            } else {
                                value = asUTC;
                            }
                        }
                    } else {
                        value = PyDateTimeAPI->Time_FromTime(hours, mins, secs, usecs, tz,
                                                             PyDateTimeAPI->TimeType);
                    }
                    Py_DECREF(tz);
                }
            }
        }

        if (value == NULL)
            return false;

        return Handle(value);
    }

#undef digit

    bool IsUuid(const char* str, SizeType length) {
        if (uuidMode == UM_HEX && length == 32) {
            for (int i = length - 1; i >= 0; --i)
                if (!isxdigit(str[i]))
                    return false;
            return true;
        } else if (length == 36
                   && str[8] == '-' && str[13] == '-'
                   && str[18] == '-' && str[23] == '-') {
            for (int i = length - 1; i >= 0; --i)
                if (i != 8 && i != 13 && i != 18 && i != 23 && !isxdigit(str[i]))
                    return false;
            return true;
        }
        return false;
    }

    bool HandleUuid(const char* str, SizeType length) {
        PyObject* pystr = PyUnicode_FromStringAndSize(str, length);
        if (pystr == NULL)
            return false;

        PyObject* value = PyObject_CallFunctionObjArgs(uuid_type, pystr, NULL);
        Py_DECREF(pystr);

        if (value == NULL)
            return false;
        else
            return Handle(value);
    }

    bool String(const char* str, SizeType length, bool copy) {
        PyObject* value;

        if (datetimeMode != DM_NONE) {
            int year, month, day, hours, mins, secs, usecs, tzoff;

            if (IsIso8601(str, length, year, month, day,
                          hours, mins, secs, usecs, tzoff))
                return HandleIso8601(str, length, year, month, day,
                                     hours, mins, secs, usecs, tzoff);
        }

        if (uuidMode != UM_NONE && IsUuid(str, length))
            return HandleUuid(str, length);

        value = PyUnicode_FromStringAndSize(str, length);
        if (value == NULL)
            return false;

        if (decoderString != NULL) {
            PyObject* replacement = PyObject_CallFunctionObjArgs(decoderString, value,
                                                                 NULL);
            Py_DECREF(value);
            if (replacement == NULL)
                return false;
            value = replacement;
        }

        return Handle(value);
    }
};


typedef struct {
    PyObject_HEAD
    unsigned datetimeMode;
    unsigned uuidMode;
    unsigned numberMode;
    unsigned parseMode;
} DecoderObject;


PyDoc_STRVAR(loads_docstring,
             "loads(string, *, object_hook=None, number_mode=None, datetime_mode=None,"
             " uuid_mode=None, parse_mode=None, allow_nan=True)\n"
             "\n"
             "Decode a JSON string into a Python object.");


static PyObject*
loads(PyObject* self, PyObject* args, PyObject* kwargs)
{
    /* Converts a JSON encoded string to a Python object. */

    static char const* kwlist[] = {
        "string",
        "object_hook",
        "number_mode",
        "datetime_mode",
        "uuid_mode",
        "parse_mode",

        /* compatibility with stdlib json */
        "allow_nan",

        NULL
    };
    PyObject* jsonObject;
    PyObject* objectHook = NULL;
    PyObject* datetimeModeObj = NULL;
    unsigned datetimeMode = DM_NONE;
    PyObject* uuidModeObj = NULL;
    unsigned uuidMode = UM_NONE;
    PyObject* numberModeObj = NULL;
    unsigned numberMode = NM_NAN;
    PyObject* parseModeObj = NULL;
    unsigned parseMode = PM_NONE;
    int allowNan = -1;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|$OOOOOp:rapidjson.loads",
                                     (char**) kwlist,
                                     &jsonObject,
                                     &objectHook,
                                     &numberModeObj,
                                     &datetimeModeObj,
                                     &uuidModeObj,
                                     &parseModeObj,
                                     &allowNan))
        return NULL;

    if (objectHook && !PyCallable_Check(objectHook)) {
        if (objectHook == Py_None) {
            objectHook = NULL;
        } else {
            PyErr_SetString(PyExc_TypeError, "object_hook is not callable");
            return NULL;
        }
    }

    if (!accept_number_mode_arg(numberModeObj, allowNan, numberMode))
        return NULL;
    if (numberMode & NM_DECIMAL && numberMode & NM_NATIVE) {
        PyErr_SetString(PyExc_ValueError,
                        "Invalid number_mode, combining NM_NATIVE with NM_DECIMAL"
                        " is not supported");
        return NULL;
    }

    if (!accept_datetime_mode_arg(datetimeModeObj, datetimeMode))
        return NULL;
    if (datetimeMode && datetime_mode_format(datetimeMode) != DM_ISO8601) {
        PyErr_SetString(PyExc_ValueError,
                        "Invalid datetime_mode, can deserialize only from"
                        " ISO8601");
        return NULL;
    }

    if (!accept_uuid_mode_arg(uuidModeObj, uuidMode))
        return NULL;

    if (!accept_parse_mode_arg(parseModeObj, parseMode))
        return NULL;

    Py_ssize_t jsonStrLen;
    const char* jsonStr;
    PyObject* asUnicode = NULL;

    if (PyUnicode_Check(jsonObject)) {
        jsonStr = PyUnicode_AsUTF8AndSize(jsonObject, &jsonStrLen);
        if (jsonStr == NULL) {
            return NULL;
        }
    } else if (PyBytes_Check(jsonObject) || PyByteArray_Check(jsonObject)) {
        asUnicode = PyUnicode_FromEncodedObject(jsonObject, "utf-8", NULL);
        if (asUnicode == NULL)
            return NULL;
        jsonStr = PyUnicode_AsUTF8AndSize(asUnicode, &jsonStrLen);
        if (jsonStr == NULL) {
            Py_DECREF(asUnicode);
            return NULL;
        }
    } else {
        PyErr_SetString(PyExc_TypeError,
                        "Expected string or UTF-8 encoded bytes or bytearray");
        return NULL;
    }

    PyObject* result = do_decode(NULL, jsonStr, jsonStrLen, NULL, 0, objectHook,
                                 numberMode, datetimeMode, uuidMode, parseMode);

    if (asUnicode != NULL)
        Py_DECREF(asUnicode);

    return result;
}


PyDoc_STRVAR(load_docstring,
             "load(stream, *, object_hook=None, number_mode=None, datetime_mode=None,"
             " uuid_mode=None, parse_mode=None, chunk_size=65536, allow_nan=True)\n"
             "\n"
             "Decode a JSON stream into a Python object.");


static PyObject*
load(PyObject* self, PyObject* args, PyObject* kwargs)
{
    /* Converts a JSON encoded stream to a Python object. */

    static char const* kwlist[] = {
        "stream",
        "object_hook",
        "number_mode",
        "datetime_mode",
        "uuid_mode",
        "parse_mode",
        "chunk_size",

        /* compatibility with stdlib json */
        "allow_nan",

        NULL
    };
    PyObject* jsonObject;
    PyObject* objectHook = NULL;
    PyObject* datetimeModeObj = NULL;
    unsigned datetimeMode = DM_NONE;
    PyObject* uuidModeObj = NULL;
    unsigned uuidMode = UM_NONE;
    PyObject* numberModeObj = NULL;
    unsigned numberMode = NM_NAN;
    PyObject* parseModeObj = NULL;
    unsigned parseMode = PM_NONE;
    PyObject* chunkSizeObj = NULL;
    size_t chunkSize = 65536;
    int allowNan = -1;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|$OOOOOOp:rapidjson.load",
                                     (char**) kwlist,
                                     &jsonObject,
                                     &objectHook,
                                     &numberModeObj,
                                     &datetimeModeObj,
                                     &uuidModeObj,
                                     &parseModeObj,
                                     &chunkSizeObj,
                                     &allowNan))
        return NULL;

    if (!PyObject_HasAttr(jsonObject, read_name)) {
        PyErr_SetString(PyExc_TypeError, "Expected file-like object");
        return NULL;
    }

    if (objectHook && !PyCallable_Check(objectHook)) {
        if (objectHook == Py_None) {
            objectHook = NULL;
        } else {
            PyErr_SetString(PyExc_TypeError, "object_hook is not callable");
            return NULL;
        }
    }

    if (numberModeObj) {
        if (numberModeObj == Py_None) {
            numberMode = NM_NONE;
        } else if (PyLong_Check(numberModeObj)) {
            int mode = PyLong_AsLong(numberModeObj);
            if (mode < 0 || mode >= NM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid number_mode");
                return NULL;
            }
            numberMode = (unsigned) mode;
            if (numberMode & NM_DECIMAL && numberMode & NM_NATIVE) {
                PyErr_SetString(PyExc_ValueError,
                                "Combining NM_NATIVE with NM_DECIMAL is not supported");
                return NULL;
            }
        }
    }
    if (allowNan != -1) {
        if (allowNan)
            numberMode |= NM_NAN;
        else
            numberMode &= ~NM_NAN;
    }

    if (datetimeModeObj) {
        if (datetimeModeObj == Py_None) {
            datetimeMode = DM_NONE;
        } else if (PyLong_Check(datetimeModeObj)) {
            int mode = PyLong_AsLong(datetimeModeObj);
            if (!valid_datetime_mode(mode)) {
                PyErr_SetString(PyExc_ValueError, "Invalid datetime_mode");
                return NULL;
            }
            datetimeMode = (unsigned) mode;
            if (datetimeMode && datetime_mode_format(datetimeMode) != DM_ISO8601) {
                PyErr_SetString(PyExc_ValueError,
                                "Invalid datetime_mode, can deserialize only from"
                                " ISO8601");
                return NULL;
            }
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "datetime_mode must be a non-negative integer value or None");
            return NULL;
        }
    }

    if (uuidModeObj) {
        if (uuidModeObj == Py_None) {
            uuidMode = UM_NONE;
        } else if (PyLong_Check(uuidModeObj)) {
            int mode = PyLong_AsLong(uuidModeObj);
            if (mode < 0 || mode >= UM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid uuid_mode");
                return NULL;
            }
            uuidMode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "uuid_mode must be an integer value or None");
            return NULL;
        }
    }

    if (parseModeObj) {
        if (parseModeObj == Py_None) {
            parseMode = PM_NONE;
        } else if (PyLong_Check(parseModeObj)) {
            int mode = PyLong_AsLong(parseModeObj);
            if (mode < 0 || mode >= PM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid parse_mode");
                return NULL;
            }
            parseMode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "parse_mode must be an integer value or None");
            return NULL;
        }
    }

    if (chunkSizeObj && chunkSizeObj != Py_None) {
        if (PyLong_Check(chunkSizeObj)) {
            Py_ssize_t size = PyNumber_AsSsize_t(chunkSizeObj, PyExc_ValueError);
            if (PyErr_Occurred() || size < 4 || size > UINT_MAX) {
                PyErr_SetString(PyExc_ValueError,
                                "Invalid chunk_size, must be an integer between 4 and"
                                " UINT_MAX");
                return NULL;
            }
            chunkSize = (size_t) size;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "chunk_size must be an unsigned integer value or None");
            return NULL;
        }
    }

    return do_decode(NULL, NULL, 0, jsonObject, chunkSize, objectHook,
                     numberMode, datetimeMode, uuidMode, parseMode);
}


PyDoc_STRVAR(decoder_doc,
             "Decoder(number_mode=None, datetime_mode=None, uuid_mode=None,"
             " parse_mode=None)\n"
             "\n"
             "Create and return a new Decoder instance.");


static PyMemberDef decoder_members[] = {
    {"datetime_mode",
     T_UINT, offsetof(DecoderObject, datetimeMode), READONLY,
     "The datetime mode, whether and how datetime literals will be recognized."},
    {"uuid_mode",
     T_UINT, offsetof(DecoderObject, uuidMode), READONLY,
     "The UUID mode, whether and how UUID literals will be recognized."},
    {"number_mode",
     T_UINT, offsetof(DecoderObject, numberMode), READONLY,
     "The number mode, whether numeric literals will be decoded."},
    {"parse_mode",
     T_UINT, offsetof(DecoderObject, parseMode), READONLY,
     "The parse mode, whether comments and trailing commas are allowed."},
    {NULL}
};


static PyTypeObject Decoder_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "rapidjson.Decoder",                      /* tp_name */
    sizeof(DecoderObject),                    /* tp_basicsize */
    0,                                        /* tp_itemsize */
    0,                                        /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    (ternaryfunc) decoder_call,               /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    decoder_doc,                              /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    0,                                        /* tp_methods */
    decoder_members,                          /* tp_members */
    0,                                        /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    0,                                        /* tp_init */
    0,                                        /* tp_alloc */
    decoder_new,                              /* tp_new */
    PyObject_Del,                             /* tp_free */
};


#define Decoder_CheckExact(v) (Py_TYPE(v) == &Decoder_Type)
#define Decoder_Check(v) PyObject_TypeCheck(v, &Decoder_Type)


#define DECODE(r, f, s, h)                                              \
    do {                                                                \
        /* FIXME: isn't there a cleverer way to write the following?    \
                                                                        \
           Ideally, one would do something like:                        \
                                                                        \
               unsigned flags = kParseInsituFlag;                       \
                                                                        \
               if (! (numberMode & NM_NATIVE))                          \
                   flags |= kParseNumbersAsStringsFlag;                 \
               if (numberMode & NM_NAN)                                 \
                   flags |= kParseNanAndInfFlag;                        \
               if (parseMode & PM_COMMENTS)                             \
                   flags |= kParseCommentsFlag;                         \
               if (parseMode & PM_TRAILING_COMMAS)                      \
                   flags |= kParseTrailingCommasFlag;                   \
                                                                        \
               reader.Parse<flags>(ss, handler);                        \
                                                                        \
           but C++ does not allow that...                               \
        */                                                              \
                                                                        \
        if (numberMode & NM_NAN) {                                      \
            if (numberMode & NM_NATIVE) {                               \
                if (parseMode & PM_TRAILING_COMMAS) {                   \
                    if (parseMode & PM_COMMENTS) {                      \
                        r.Parse<f |                                     \
                                kParseNanAndInfFlag |                   \
                                kParseCommentsFlag |                    \
                                kParseTrailingCommasFlag>(s, h);        \
                    } else {                                            \
                        r.Parse<f |                                     \
                                kParseNanAndInfFlag |                   \
                                kParseTrailingCommasFlag>(s, h);        \
                    }                                                   \
                } else if (parseMode & PM_COMMENTS) {                   \
                    r.Parse<f |                                         \
                            kParseNanAndInfFlag |                       \
                            kParseCommentsFlag>(s, h);                  \
                } else {                                                \
                    r.Parse<f |                                         \
                            kParseNanAndInfFlag>(s, h);                 \
                }                                                       \
            } else if (parseMode & PM_TRAILING_COMMAS) {                \
                if (parseMode & PM_COMMENTS) {                          \
                    r.Parse<f |                                         \
                            kParseNumbersAsStringsFlag |                \
                            kParseNanAndInfFlag |                       \
                            kParseCommentsFlag |                        \
                            kParseTrailingCommasFlag>(s, h);            \
                } else {                                                \
                    r.Parse<f |                                         \
                            kParseNumbersAsStringsFlag |                \
                            kParseNanAndInfFlag |                       \
                            kParseTrailingCommasFlag>(s, h);            \
                }                                                       \
            } else if (parseMode & PM_COMMENTS) {                       \
                r.Parse<f |                                             \
                        kParseNumbersAsStringsFlag |                    \
                        kParseNanAndInfFlag |                           \
                        kParseCommentsFlag>(s, h);                      \
            } else {                                                    \
                r.Parse<f |                                             \
                        kParseNumbersAsStringsFlag |                    \
                        kParseNanAndInfFlag>(s, h);                     \
            }                                                           \
        } else if (numberMode & NM_NATIVE) {                            \
            if (parseMode & PM_TRAILING_COMMAS) {                       \
                if (parseMode & PM_COMMENTS) {                          \
                    r.Parse<f |                                         \
                            kParseCommentsFlag |                        \
                            kParseTrailingCommasFlag>(s, h);            \
                } else {                                                \
                    r.Parse<f |                                         \
                            kParseTrailingCommasFlag>(s, h);            \
                }                                                       \
            } else if (parseMode & PM_COMMENTS) {                       \
                r.Parse<f |                                             \
                        kParseCommentsFlag>(s, h);                      \
            } else {                                                    \
                r.Parse<f>(s, h);                                       \
            }                                                           \
        } else if (parseMode & PM_TRAILING_COMMAS) {                    \
            if (parseMode & PM_COMMENTS) {                              \
                r.Parse<f |                                             \
                        kParseCommentsFlag |                            \
                        kParseNumbersAsStringsFlag>(s, h);              \
            } else {                                                    \
                r.Parse<f |                                             \
                        kParseNumbersAsStringsFlag |                    \
                        kParseTrailingCommasFlag>(s, h);                \
            }                                                           \
        } else {                                                        \
            r.Parse<f | kParseNumbersAsStringsFlag>(s, h);              \
        }                                                               \
    } while(0)


static PyObject*
do_decode(PyObject* decoder, const char* jsonStr, Py_ssize_t jsonStrLen,
          PyObject* jsonStream, size_t chunkSize, PyObject* objectHook,
          unsigned numberMode, unsigned datetimeMode, unsigned uuidMode,
          unsigned parseMode)
{
    PyHandler handler(decoder, objectHook, datetimeMode, uuidMode, numberMode);
    Reader reader;

    if (jsonStr != NULL) {
        char* jsonStrCopy = (char*) PyMem_Malloc(sizeof(char) * (jsonStrLen+1));

        if (jsonStrCopy == NULL)
            return PyErr_NoMemory();

        memcpy(jsonStrCopy, jsonStr, jsonStrLen+1);

        InsituStringStream ss(jsonStrCopy);

        DECODE(reader, kParseInsituFlag, ss, handler);

        PyMem_Free(jsonStrCopy);
    } else {
        PyReadStreamWrapper sw(jsonStream, chunkSize);

        DECODE(reader, kParseNoFlags, sw, handler);
    }

    if (reader.HasParseError()) {
        size_t offset = reader.GetErrorOffset();

        if (PyErr_Occurred()) {
            PyObject* etype;
            PyObject* evalue;
            PyObject* etraceback;
            PyErr_Fetch(&etype, &evalue, &etraceback);

            // Try to add the offset in the error message if the exception
            // value is a string.  Otherwise, use the original exception since
            // we can't be sure the exception type takes a single string.
            if (evalue != NULL && PyUnicode_Check(evalue)) {
                PyErr_Format(etype, "Parse error at offset %zu: %S", offset, evalue);
                Py_DECREF(etype);
                Py_DECREF(evalue);
                Py_XDECREF(etraceback);
            }
            else
                PyErr_Restore(etype, evalue, etraceback);
        }
        else
            PyErr_Format(decode_error, "Parse error at offset %zu: %s",
                         offset, GetParseError_En(reader.GetParseErrorCode()));

        Py_XDECREF(handler.root);
        return NULL;
    } else if (PyErr_Occurred()) {
        // Catch possible error raised in associated stream operations
        Py_XDECREF(handler.root);
        return NULL;
    }

    return handler.root;
}


static PyObject*
decoder_call(PyObject* self, PyObject* args, PyObject* kwargs)
{
    static char const* kwlist[] = {
        "json",
        "chunk_size",
        NULL
    };
    PyObject* jsonObject;
    PyObject* chunkSizeObj = NULL;
    size_t chunkSize = 65536;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|$O",
                                     (char**) kwlist,
                                     &jsonObject,
                                     &chunkSizeObj))
        return NULL;

    if (chunkSizeObj && chunkSizeObj != Py_None) {
        if (PyLong_Check(chunkSizeObj)) {
            Py_ssize_t size = PyNumber_AsSsize_t(chunkSizeObj, PyExc_ValueError);
            if (PyErr_Occurred() || size < 4 || size > UINT_MAX) {
                PyErr_SetString(PyExc_ValueError,
                                "Invalid chunk_size, must be an integer between 4 and"
                                " UINT_MAX");
                return NULL;
            }
            chunkSize = (size_t) size;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "chunk_size must be an unsigned integer value or None");
            return NULL;
        }
    }

    Py_ssize_t jsonStrLen;
    const char* jsonStr;
    PyObject* asUnicode = NULL;

    if (PyUnicode_Check(jsonObject)) {
        jsonStr = PyUnicode_AsUTF8AndSize(jsonObject, &jsonStrLen);
        if (jsonStr == NULL)
            return NULL;
    } else if (PyBytes_Check(jsonObject) || PyByteArray_Check(jsonObject)) {
        asUnicode = PyUnicode_FromEncodedObject(jsonObject, "utf-8", NULL);
        if (asUnicode == NULL)
            return NULL;
        jsonStr = PyUnicode_AsUTF8AndSize(asUnicode, &jsonStrLen);
        if (jsonStr == NULL) {
            Py_DECREF(asUnicode);
            return NULL;
        }
    } else if (PyObject_HasAttr(jsonObject, read_name)) {
        jsonStr = NULL;
        jsonStrLen = 0;
    } else {
        PyErr_SetString(
            PyExc_TypeError,
            "Expected string or UTF-8 encoded bytes or bytearray or a file-like object");
        return NULL;
    }

    DecoderObject* d = (DecoderObject*) self;

    PyObject* result = do_decode(self, jsonStr, jsonStrLen, jsonObject, chunkSize, NULL,
                                 d->numberMode, d->datetimeMode, d->uuidMode,
                                 d->parseMode);

    if (asUnicode != NULL)
        Py_DECREF(asUnicode);

    return result;
}


static PyObject*
decoder_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    DecoderObject* d;
    PyObject* datetimeModeObj = NULL;
    unsigned datetimeMode = DM_NONE;
    PyObject* uuidModeObj = NULL;
    unsigned uuidMode = UM_NONE;
    PyObject* numberModeObj = NULL;
    unsigned numberMode = NM_NAN;
    PyObject* parseModeObj = NULL;
    unsigned parseMode = PM_NONE;
    static char const* kwlist[] = {
        "number_mode",
        "datetime_mode",
        "uuid_mode",
        "parse_mode",
        NULL
    };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOOO:Decoder",
                                     (char**) kwlist,
                                     &numberModeObj,
                                     &datetimeModeObj,
                                     &uuidModeObj,
                                     &parseModeObj))
        return NULL;

    if (numberModeObj) {
        if (numberModeObj == Py_None) {
            numberMode = NM_NONE;
        } else if (PyLong_Check(numberModeObj)) {
            int mode = PyLong_AsLong(numberModeObj);
            if (mode < 0 || mode >= NM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid number_mode");
                return NULL;
            }
            numberMode = (unsigned) mode;
            if (numberMode & NM_DECIMAL && numberMode & NM_NATIVE) {
                PyErr_SetString(PyExc_ValueError,
                                "Combining NM_NATIVE with NM_DECIMAL is not supported");
                return NULL;
            }
        }
    }

    if (datetimeModeObj) {
        if (datetimeModeObj == Py_None) {
            datetimeMode = DM_NONE;
        } else if (PyLong_Check(datetimeModeObj)) {
            int mode = PyLong_AsLong(datetimeModeObj);
            if (!valid_datetime_mode(mode)) {
                PyErr_SetString(PyExc_ValueError, "Invalid datetime_mode");
                return NULL;
            }
            datetimeMode = (unsigned) mode;
            if (datetimeMode && datetime_mode_format(datetimeMode) != DM_ISO8601) {
                PyErr_SetString(PyExc_ValueError,
                                "Invalid datetime_mode, can deserialize only from"
                                " ISO8601");
                return NULL;
            }
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "datetime_mode must be a non-negative integer value or None");
            return NULL;
        }
    }

    if (uuidModeObj) {
        if (uuidModeObj == Py_None) {
            uuidMode = UM_NONE;
        } else if (PyLong_Check(uuidModeObj)) {
            int mode = PyLong_AsLong(uuidModeObj);
            if (mode < 0 || mode >= UM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid uuid_mode");
                return NULL;
            }
            uuidMode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "uuid_mode must be an integer value or None");
            return NULL;
        }
    }

    if (parseModeObj) {
        if (parseModeObj == Py_None) {
            parseMode = PM_NONE;
        } else if (PyLong_Check(parseModeObj)) {
            int mode = PyLong_AsLong(parseModeObj);
            if (mode < 0 || mode >= PM_MAX) {
                PyErr_SetString(PyExc_ValueError, "Invalid parse_mode");
                return NULL;
            }
            parseMode = (unsigned) mode;
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "parse_mode must be an integer value or None");
            return NULL;
        }
    }

    d = (DecoderObject*) type->tp_alloc(type, 0);
    if (d == NULL)
        return NULL;

    d->datetimeMode = datetimeMode;
    d->uuidMode = uuidMode;
    d->numberMode = numberMode;
    d->parseMode = parseMode;

    return (PyObject*) d;
}


/////////////
// Encoder //
/////////////


struct DictItem {
    std::string key;
    PyObject* value;

    DictItem(std::string k,
             PyObject* v)
        : key(k),
          value(v)
        {}

    bool operator<(const DictItem& other) const {
        return key < other.key;
    }
};


static inline bool
all_keys_are_string(PyObject* dict) {
    Py_ssize_t pos = 0;
    PyObject* key;

    while (PyDict_Next(dict, &pos, &key, NULL))
        if (!PyUnicode_Check(key))
            return false;
    return true;
}


template<typename WriterT>
static bool
dumps_internal(
    WriterT* writer,
    PyObject* object,
    PyObject* defaultFn,
    unsigned numberMode,
    unsigned datetimeMode,
    unsigned uuidMode,
    unsigned bytesMode,
    unsigned iterableMode,
    unsigned mappingMode)
{
    int is_decimal;

#define RECURSE(v) dumps_internal(writer, v, defaultFn,                 \
                                  numberMode, datetimeMode, uuidMode,   \
                                  bytesMode, iterableMode, mappingMode)

#define ASSERT_VALID_SIZE(l) do {                                       \
    if (l < 0 || l > UINT_MAX) {                                        \
        PyErr_SetString(PyExc_ValueError, "Out of range string size");  \
        return false;                                                   \
    } } while(0)


    if (object == Py_None) {
        writer->Null();
    } else if (PyBool_Check(object)) {
        writer->Bool(object == Py_True);
    } else if (numberMode & NM_DECIMAL
               && (is_decimal = PyObject_IsInstance(object, decimal_type))) {
        if (is_decimal == -1) {
            return false;
        }

        if (!(numberMode & NM_NAN)) {
            bool is_inf_or_nan;
            PyObject* is_inf = PyObject_CallMethodObjArgs(object, is_infinite_name,
                                                          NULL);

            if (is_inf == NULL) {
                return false;
            }
            is_inf_or_nan = is_inf == Py_True;
            Py_DECREF(is_inf);

            if (!is_inf_or_nan) {
                PyObject* is_nan = PyObject_CallMethodObjArgs(object, is_nan_name,
                                                              NULL);

                if (is_nan == NULL) {
                    return false;
                }
                is_inf_or_nan = is_nan == Py_True;
                Py_DECREF(is_nan);
            }

            if (is_inf_or_nan) {
                PyErr_SetString(PyExc_ValueError,
                                "Out of range decimal values are not JSON compliant");
                return false;
            }
        }

        PyObject* decStrObj = PyObject_Str(object);
        if (decStrObj == NULL)
            return false;

        Py_ssize_t size;
        const char* decStr = PyUnicode_AsUTF8AndSize(decStrObj, &size);
        if (decStr == NULL) {
            Py_DECREF(decStrObj);
            return false;
        }

        writer->RawValue(decStr, size, kNumberType);
        Py_DECREF(decStrObj);
    } else if (PyLong_Check(object)) {
        if (numberMode & NM_NATIVE) {
            int overflow;
            long long i = PyLong_AsLongLongAndOverflow(object, &overflow);
            if (i == -1 && PyErr_Occurred())
                return false;

            if (overflow == 0) {
                writer->Int64(i);
            } else {
                unsigned long long ui = PyLong_AsUnsignedLongLong(object);
                if (PyErr_Occurred())
                    return false;

                writer->Uint64(ui);
            }
        } else {
            // Mimic stdlib json: subclasses of int may override __repr__, but we still
            // want to encode them as integers in JSON; one example within the standard
            // library is IntEnum

            PyObject* intStrObj = PyLong_Type.tp_repr(object);
            if (intStrObj == NULL)
                return false;

            Py_ssize_t size;
            const char* intStr = PyUnicode_AsUTF8AndSize(intStrObj, &size);
            if (intStr == NULL) {
                Py_DECREF(intStrObj);
                return false;
            }

            writer->RawValue(intStr, size, kNumberType);
            Py_DECREF(intStrObj);
        }
    } else if (PyFloat_Check(object)) {
        double d = PyFloat_AS_DOUBLE(object);
        if (IS_NAN(d)) {
            if (numberMode & NM_NAN) {
                writer->RawValue("NaN", 3, kNumberType);
            } else {
                PyErr_SetString(PyExc_ValueError,
                                "Out of range float values are not JSON compliant");
                return false;
            }
        } else if (IS_INF(d)) {
            if (!(numberMode & NM_NAN)) {
                PyErr_SetString(PyExc_ValueError,
                                "Out of range float values are not JSON compliant");
                return false;
            } else if (d < 0) {
                writer->RawValue("-Infinity", 9, kNumberType);
            } else {
                writer->RawValue("Infinity", 8, kNumberType);
            }
        } else {
            // The RJ dtoa() produces "strange" results for particular values, see #101:
            // use Python's repr() to emit a raw value instead of writer->Double(d)

            PyObject* dr = PyFloat_Type.tp_repr(object);

            if (dr == NULL)
                return false;

            Py_ssize_t l;
            const char* rs = PyUnicode_AsUTF8AndSize(dr, &l);
            if (rs == NULL) {
                Py_DECREF(dr);
                return false;
            }

            writer->RawValue(rs, l, kNumberType);
            Py_DECREF(dr);
        }
    } else if (PyUnicode_Check(object)) {
        Py_ssize_t l;
        const char* s = PyUnicode_AsUTF8AndSize(object, &l);
        if (s == NULL)
            return false;
        ASSERT_VALID_SIZE(l);
        writer->String(s, (SizeType) l);
    } else if (bytesMode == BM_UTF8
               && (PyBytes_Check(object) || PyByteArray_Check(object))) {
        PyObject* unicodeObj = PyUnicode_FromEncodedObject(object, "utf-8", NULL);

        if (unicodeObj == NULL)
            return false;

        Py_ssize_t l;
        const char* s = PyUnicode_AsUTF8AndSize(unicodeObj, &l);
        if (s == NULL) {
            Py_DECREF(unicodeObj);
            return false;
        }
        ASSERT_VALID_SIZE(l);
        writer->String(s, (SizeType) l);
        Py_DECREF(unicodeObj);
    } else if (PyList_CheckExact(object)
               ||
               (!(iterableMode & IM_ONLY_LISTS) && PyList_Check(object))) {
        writer->StartArray();

        Py_ssize_t size = PyList_GET_SIZE(object);

        for (Py_ssize_t i = 0; i < size; i++) {
            if (Py_EnterRecursiveCall(" while JSONifying list object"))
                return false;
            PyObject* item = PyList_GET_ITEM(object, i);
            bool r = RECURSE(item);
            Py_LeaveRecursiveCall();
            if (!r)
                return false;
        }

        writer->EndArray();
    } else if (!(iterableMode & IM_ONLY_LISTS) && PyTuple_Check(object)) {
        writer->StartArray();

        Py_ssize_t size = PyTuple_GET_SIZE(object);

        for (Py_ssize_t i = 0; i < size; i++) {
            if (Py_EnterRecursiveCall(" while JSONifying tuple object"))
                return false;
            PyObject* item = PyTuple_GET_ITEM(object, i);
            bool r = RECURSE(item);
            Py_LeaveRecursiveCall();
            if (!r)
                return false;
        }

        writer->EndArray();
    } else if ((PyDict_CheckExact(object)
                ||
                (!(mappingMode & MM_ONLY_DICTS) && PyDict_Check(object)))
               &&
               ((mappingMode & MM_SKIP_NON_STRING_KEYS)
                ||
                (mappingMode & MM_COERCE_KEYS_TO_STRINGS)
                ||
                all_keys_are_string(object))) {
        writer->StartObject();

        Py_ssize_t pos = 0;
        PyObject* key;
        PyObject* item;
        PyObject* coercedKey = NULL;

        if (!(mappingMode & MM_SORT_KEYS)) {
            while (PyDict_Next(object, &pos, &key, &item)) {
                if (mappingMode & MM_COERCE_KEYS_TO_STRINGS) {
                    if (!PyUnicode_Check(key)) {
                        coercedKey = PyObject_Str(key);
                        if (coercedKey == NULL)
                            return false;
                        key = coercedKey;
                    }
                }
                if (coercedKey || PyUnicode_Check(key)) {
                    Py_ssize_t l;
                    const char* key_str = PyUnicode_AsUTF8AndSize(key, &l);
                    if (key_str == NULL) {
                        Py_XDECREF(coercedKey);
                        return false;
                    }
                    ASSERT_VALID_SIZE(l);
                    writer->Key(key_str, (SizeType) l);
                    if (Py_EnterRecursiveCall(" while JSONifying dict object")) {
                        Py_XDECREF(coercedKey);
                        return false;
                    }
                    bool r = RECURSE(item);
                    Py_LeaveRecursiveCall();
                    if (!r) {
                        Py_XDECREF(coercedKey);
                        return false;
                    }
                } else if (!(mappingMode & MM_SKIP_NON_STRING_KEYS)) {
                    PyErr_SetString(PyExc_TypeError, "keys must be strings");
                    // No need to dispose coercedKey here, because it can be set *only*
                    // when mapping_mode is MM_COERCE_KEYS_TO_STRINGS
                    assert(!coercedKey);
                    return false;
                }
                Py_CLEAR(coercedKey);
            }
        } else {
            std::vector<DictItem> items;

            while (PyDict_Next(object, &pos, &key, &item)) {
                if (mappingMode & MM_COERCE_KEYS_TO_STRINGS) {
                    if (!PyUnicode_Check(key)) {
                        coercedKey = PyObject_Str(key);
                        if (coercedKey == NULL)
                            return false;
                        key = coercedKey;
                    }
                }
                if (coercedKey || PyUnicode_Check(key)) {
                    Py_ssize_t l;
                    const char* key_str = PyUnicode_AsUTF8AndSize(key, &l);
                    if (key_str == NULL) {
                        Py_XDECREF(coercedKey);
                        return false;
                    }
                    ASSERT_VALID_SIZE(l);
                    items.push_back(DictItem(std::string(key_str, l), item));
                } else if (!(mappingMode & MM_SKIP_NON_STRING_KEYS)) {
                    PyErr_SetString(PyExc_TypeError, "keys must be strings");
                    assert(!coercedKey);
                    return false;
                }
                Py_CLEAR(coercedKey);
            }

            std::sort(items.begin(), items.end());

            for (size_t i=0, s=items.size(); i < s; i++) {
                writer->Key(items[i].key.c_str(), (SizeType) items[i].key.length());
                if (Py_EnterRecursiveCall(" while JSONifying dict object"))
                    return false;
                bool r = RECURSE(items[i].value);
                Py_LeaveRecursiveCall();
                if (!r)
                    return false;
            }
        }

        writer->EndObject();
    } else if (datetimeMode != DM_NONE
               && (PyTime_Check(object) || PyDateTime_Check(object))) {
        unsigned year, month, day, hour, min, sec, microsec;
        PyObject* dtObject = object;
        PyObject* asUTC = NULL;

        const int ISOFORMAT_LEN = 42;
        char isoformat[ISOFORMAT_LEN];
        memset(isoformat, 0, ISOFORMAT_LEN);

        // The timezone is always shorter than this, but gcc12 emits a warning about
        // sprintf() that *may* produce longer results, because we pass int values when
        // concretely they are constrained to 24*3600 seconds: pacify gcc using a bigger
        // buffer
        const int TIMEZONE_LEN = 24;
        char timeZone[TIMEZONE_LEN] = { 0 };

        if (!(datetimeMode & DM_IGNORE_TZ)
            && PyObject_HasAttr(object, utcoffset_name)) {
            PyObject* utcOffset = PyObject_CallMethodObjArgs(object,
                                                             utcoffset_name,
                                                             NULL);

            if (utcOffset == NULL)
                return false;

            if (utcOffset == Py_None) {
                // Naive value: maybe assume it's in UTC instead of local time
                if (datetimeMode & DM_NAIVE_IS_UTC) {
                    if (PyDateTime_Check(object)) {
                        hour = PyDateTime_DATE_GET_HOUR(dtObject);
                        min = PyDateTime_DATE_GET_MINUTE(dtObject);
                        sec = PyDateTime_DATE_GET_SECOND(dtObject);
                        microsec = PyDateTime_DATE_GET_MICROSECOND(dtObject);
                        year = PyDateTime_GET_YEAR(dtObject);
                        month = PyDateTime_GET_MONTH(dtObject);
                        day = PyDateTime_GET_DAY(dtObject);

                        asUTC = PyDateTimeAPI->DateTime_FromDateAndTime(
                            year, month, day, hour, min, sec, microsec,
                            timezone_utc, PyDateTimeAPI->DateTimeType);
                    } else {
                        hour = PyDateTime_TIME_GET_HOUR(dtObject);
                        min = PyDateTime_TIME_GET_MINUTE(dtObject);
                        sec = PyDateTime_TIME_GET_SECOND(dtObject);
                        microsec = PyDateTime_TIME_GET_MICROSECOND(dtObject);
                        asUTC = PyDateTimeAPI->Time_FromTime(
                            hour, min, sec, microsec,
                            timezone_utc, PyDateTimeAPI->TimeType);
                    }

                    if (asUTC == NULL) {
                        Py_DECREF(utcOffset);
                        return false;
                    }

                    dtObject = asUTC;

                    if (datetime_mode_format(datetimeMode) == DM_ISO8601)
                        strcpy(timeZone, "+00:00");
                }
            } else {
                // Timezone-aware value
                if (datetimeMode & DM_SHIFT_TO_UTC) {
                    // If it's not already in UTC, shift the value
                    if (PyObject_IsTrue(utcOffset)) {
                        asUTC = PyObject_CallMethodObjArgs(object, astimezone_name,
                                                           timezone_utc, NULL);

                        if (asUTC == NULL) {
                            Py_DECREF(utcOffset);
                            return false;
                        }

                        dtObject = asUTC;
                    }

                    if (datetime_mode_format(datetimeMode) == DM_ISO8601)
                        strcpy(timeZone, "+00:00");
                } else if (datetime_mode_format(datetimeMode) == DM_ISO8601) {
                    int seconds_from_utc = 0;

                    if (PyObject_IsTrue(utcOffset)) {
                        PyObject* tsObj = PyObject_CallMethodObjArgs(utcOffset,
                                                                     total_seconds_name,
                                                                     NULL);

                        if (tsObj == NULL) {
                            Py_DECREF(utcOffset);
                            return false;
                        }

                        seconds_from_utc = (int) PyFloat_AsDouble(tsObj);

                        Py_DECREF(tsObj);
                    }

                    char sign = '+';

                    if (seconds_from_utc < 0) {
                        sign = '-';
                        seconds_from_utc = -seconds_from_utc;
                    }

                    unsigned tz_hour = seconds_from_utc / 3600;
                    unsigned tz_min = (seconds_from_utc % 3600) / 60;

                    snprintf(timeZone, TIMEZONE_LEN-1, "%c%02u:%02u",
                             sign, tz_hour, tz_min);
                }
            }
            Py_DECREF(utcOffset);
        }

        if (datetime_mode_format(datetimeMode) == DM_ISO8601) {
            int size;
            if (PyDateTime_Check(dtObject)) {
                year = PyDateTime_GET_YEAR(dtObject);
                month = PyDateTime_GET_MONTH(dtObject);
                day = PyDateTime_GET_DAY(dtObject);
                hour = PyDateTime_DATE_GET_HOUR(dtObject);
                min = PyDateTime_DATE_GET_MINUTE(dtObject);
                sec = PyDateTime_DATE_GET_SECOND(dtObject);
                microsec = PyDateTime_DATE_GET_MICROSECOND(dtObject);

                if (microsec > 0) {
                    size = snprintf(isoformat,
                                    ISOFORMAT_LEN-1,
                                    "\"%04u-%02u-%02uT%02u:%02u:%02u.%06u%s\"",
                                    year, month, day,
                                    hour, min, sec, microsec,
                                    timeZone);
                } else {
                    size = snprintf(isoformat,
                                    ISOFORMAT_LEN-1,
                                    "\"%04u-%02u-%02uT%02u:%02u:%02u%s\"",
                                    year, month, day,
                                    hour, min, sec,
                                    timeZone);
                }
            } else {
                hour = PyDateTime_TIME_GET_HOUR(dtObject);
                min = PyDateTime_TIME_GET_MINUTE(dtObject);
                sec = PyDateTime_TIME_GET_SECOND(dtObject);
                microsec = PyDateTime_TIME_GET_MICROSECOND(dtObject);

                if (microsec > 0) {
                    size = snprintf(isoformat,
                                    ISOFORMAT_LEN-1,
                                    "\"%02u:%02u:%02u.%06u%s\"",
                                    hour, min, sec, microsec,
                                    timeZone);
                } else {
                    size = snprintf(isoformat,
                                    ISOFORMAT_LEN-1,
                                    "\"%02u:%02u:%02u%s\"",
                                    hour, min, sec,
                                    timeZone);
                }
            }
            writer->RawValue(isoformat, size, kStringType);
        } else /* if (datetimeMode & DM_UNIX_TIME) */ {
            if (PyDateTime_Check(dtObject)) {
                PyObject* timestampObj = PyObject_CallMethodObjArgs(dtObject,
                                                                    timestamp_name,
                                                                    NULL);

                if (timestampObj == NULL) {
                    Py_XDECREF(asUTC);
                    return false;
                }

                double timestamp = PyFloat_AsDouble(timestampObj);

                Py_DECREF(timestampObj);

                if (datetimeMode & DM_ONLY_SECONDS) {
                    writer->Int64((int64_t) timestamp);
                } else {
                    // Writer.SetMaxDecimalPlaces(6) truncates the value,
                    // so for example 1514893636.276703 would come out as
                    // 1514893636.276702, because its exact double value is
                    // 1514893636.2767028808593750000000000...
                    char tsStr[12 + 1 + 6 + 1];

                    // Temporarily switch to a POSIX locale, in case the outer world is
                    // configured differently: by chance I got one doctest failure, and I
                    // can only imagine that recent Sphinx (that is, 4.2+) initializes the
                    // locale to something that implies a decimal separator different from
                    // a dot ".", say a comma "," when LANG is "it_IT"... not the best
                    // thing to do when emitting JSON!

                    const char* locale = setlocale(LC_NUMERIC, NULL);
                    setlocale(LC_NUMERIC, "C");

                    int size = snprintf(tsStr, 12 + 1 + 6, "%.6f", timestamp);

                    setlocale(LC_NUMERIC, locale);

                    // Remove trailing 0s
                    while (tsStr[size-2] != '.' && tsStr[size-1] == '0')
                        size--;
                    writer->RawValue(tsStr, size, kNumberType);
                }
            } else {
                hour = PyDateTime_TIME_GET_HOUR(dtObject);
                min = PyDateTime_TIME_GET_MINUTE(dtObject);
                sec = PyDateTime_TIME_GET_SECOND(dtObject);
                microsec = PyDateTime_TIME_GET_MICROSECOND(dtObject);

                long timestamp = hour * 3600 + min * 60 + sec;

                if (datetimeMode & DM_ONLY_SECONDS)
                    writer->Int64(timestamp);
                else
                    writer->Double(timestamp + (microsec / 1000000.0));
            }
        }
        Py_XDECREF(asUTC);
    } else if (datetimeMode != DM_NONE && PyDate_Check(object)) {
        unsigned year = PyDateTime_GET_YEAR(object);
        unsigned month = PyDateTime_GET_MONTH(object);
        unsigned day = PyDateTime_GET_DAY(object);

        if (datetime_mode_format(datetimeMode) == DM_ISO8601) {
            const int ISOFORMAT_LEN = 18;
            char isoformat[ISOFORMAT_LEN];
            int size;
            memset(isoformat, 0, ISOFORMAT_LEN);

            size = snprintf(isoformat, ISOFORMAT_LEN-1, "\"%04u-%02u-%02u\"",
                            year, month, day);
            writer->RawValue(isoformat, size, kStringType);
        } else /* datetime_mode_format(datetimeMode) == DM_UNIX_TIME */ {
            // A date object, take its midnight timestamp
            PyObject* midnightObj;
            PyObject* timestampObj;

            if (datetimeMode & (DM_SHIFT_TO_UTC | DM_NAIVE_IS_UTC))
                midnightObj = PyDateTimeAPI->DateTime_FromDateAndTime(
                    year, month, day, 0, 0, 0, 0,
                    timezone_utc, PyDateTimeAPI->DateTimeType);
            else
                midnightObj = PyDateTime_FromDateAndTime(year, month, day,
                                                         0, 0, 0, 0);

            if (midnightObj == NULL) {
                return false;
            }

            timestampObj = PyObject_CallMethodObjArgs(midnightObj, timestamp_name,
                                                      NULL);

            Py_DECREF(midnightObj);

            if (timestampObj == NULL) {
                return false;
            }

            double timestamp = PyFloat_AsDouble(timestampObj);

            Py_DECREF(timestampObj);

            if (datetimeMode & DM_ONLY_SECONDS) {
                writer->Int64((int64_t) timestamp);
            } else {
                // Writer.SetMaxDecimalPlaces(6) truncates the value,
                // so for example 1514893636.276703 would come out as
                // 1514893636.276702, because its exact double value is
                // 1514893636.2767028808593750000000000...
                char tsStr[12 + 1 + 6 + 1];

                // Temporarily switch to a POSIX locale, in case the outer
                // world is configured differently, see above

                const char* locale = setlocale(LC_NUMERIC, NULL);
                setlocale(LC_NUMERIC, "C");

                setlocale(LC_NUMERIC, locale);

                int size = snprintf(tsStr, 12 + 1 + 6, "%.6f", timestamp);
                // Remove trailing 0s
                while (tsStr[size-2] != '.' && tsStr[size-1] == '0')
                    size--;
                writer->RawValue(tsStr, size, kNumberType);
            }
        }
    } else if (uuidMode != UM_NONE
               && PyObject_TypeCheck(object, (PyTypeObject*) uuid_type)) {
        PyObject* hexval;
        if (uuidMode == UM_CANONICAL)
            hexval = PyObject_Str(object);
        else
            hexval = PyObject_GetAttr(object, hex_name);
        if (hexval == NULL)
            return false;

        Py_ssize_t size;
        const char* s = PyUnicode_AsUTF8AndSize(hexval, &size);
        if (s == NULL) {
            Py_DECREF(hexval);
            return false;
        }
        if (RAPIDJSON_UNLIKELY(size != 32 && size != 36)) {
            PyErr_Format(PyExc_ValueError,
                         "Bad UUID hex, expected a string of either 32 or 36 chars,"
                         " got %.200R", hexval);
            Py_DECREF(hexval);
            return false;
        }

        char quoted[39];
        quoted[0] = quoted[size + 1] = '"';
        memcpy(quoted + 1, s, size);
        writer->RawValue(quoted, (SizeType) size + 2, kStringType);
        Py_DECREF(hexval);
    } else if (!(iterableMode & IM_ONLY_LISTS) && PyIter_Check(object)) {
        PyObject* iterator = PyObject_GetIter(object);
        if (iterator == NULL)
            return false;

        writer->StartArray();

        PyObject* item;
        while ((item = PyIter_Next(iterator))) {
            if (Py_EnterRecursiveCall(" while JSONifying iterable object")) {
                Py_DECREF(item);
                Py_DECREF(iterator);
                return false;
            }
            bool r = RECURSE(item);
            Py_LeaveRecursiveCall();
            Py_DECREF(item);
            if (!r) {
                Py_DECREF(iterator);
                return false;
            }
        }

        Py_DECREF(iterator);

        // PyIter_Next() may exit with an error
        if (PyErr_Occurred())
            return false;

        writer->EndArray();
    } else if (PyObject_TypeCheck(object, &RawJSON_Type)) {
        const char* jsonStr;
        Py_ssize_t l;
        jsonStr = PyUnicode_AsUTF8AndSize(((RawJSON*) object)->value, &l);
        if (jsonStr == NULL)
            return false;
        ASSERT_VALID_SIZE(l);
        writer->RawValue(jsonStr, (SizeType) l, kStringType);
    } else if (defaultFn) {
        PyObject* retval = PyObject_CallFunctionObjArgs(defaultFn, object, NULL);
        if (retval == NULL)
            return false;
        if (Py_EnterRecursiveCall(" while JSONifying default function result")) {
            Py_DECREF(retval);
            return false;
        }
        bool r = RECURSE(retval);
        Py_LeaveRecursiveCall();
        Py_DECREF(retval);
        if (!r)
            return false;
    } else {
        PyErr_Format(PyExc_TypeError, "%R is not JSON serializable", object);
        return false;
    }

    // Catch possible error raised in associated stream operations
    return PyErr_Occurred() ? false : true;

#undef RECURSE
#undef ASSERT_VALID_SIZE
}


typedef struct {
    PyObject_HEAD
    bool ensureAscii;
    unsigned writeMode;
    char indentChar;
    unsigned indentCount;
    unsigned datetimeMode;
    unsigned uuidMode;
    unsigned numberMode;
    unsigned bytesMode;
    unsigned iterableMode;
    unsigned mappingMode;
} EncoderObject;


PyDoc_STRVAR(dumps_docstring,
             "dumps(obj, *, skipkeys=False, ensure_ascii=True, write_mode=WM_COMPACT,"
             " indent=4, default=None, sort_keys=False, number_mode=None,"
             " datetime_mode=None, uuid_mode=None, bytes_mode=BM_UTF8,"
             " iterable_mode=IM_ANY_ITERABLE, mapping_mode=MM_ANY_MAPPING,"
             " allow_nan=True)\n"
             "\n"
             "Encode a Python object into a JSON string.");


static PyObject*
dumps(PyObject* self, PyObject* args, PyObject* kwargs)
{
    /* Converts a Python object to a JSON-encoded string. */

    PyObject* value;
    int ensureAscii = true;
    PyObject* indent = NULL;
    PyObject* defaultFn = NULL;
    PyObject* numberModeObj = NULL;
    unsigned numberMode = NM_NAN;
    PyObject* datetimeModeObj = NULL;
    unsigned datetimeMode = DM_NONE;
    PyObject* uuidModeObj = NULL;
    unsigned uuidMode = UM_NONE;
    PyObject* bytesModeObj = NULL;
    unsigned bytesMode = BM_UTF8;
    PyObject* writeModeObj = NULL;
    unsigned writeMode = WM_COMPACT;
    PyObject* iterableModeObj = NULL;
    unsigned iterableMode = IM_ANY_ITERABLE;
    PyObject* mappingModeObj = NULL;
    unsigned mappingMode = MM_ANY_MAPPING;
    char indentChar = ' ';
    unsigned indentCount = 4;
    static char const* kwlist[] = {
        "obj",
        "skipkeys",             // alias of MM_SKIP_NON_STRING_KEYS
        "ensure_ascii",
        "indent",
        "default",
        "sort_keys",            // alias of MM_SORT_KEYS
        "number_mode",
        "datetime_mode",
        "uuid_mode",
        "bytes_mode",
        "write_mode",
        "iterable_mode",
        "mapping_mode",

        /* compatibility with stdlib json */
        "allow_nan",

        NULL
    };
    int skipKeys = false;
    int sortKeys = false;
    int allowNan = -1;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|$ppOOpOOOOOOOp:rapidjson.dumps",
                                     (char**) kwlist,
                                     &value,
                                     &skipKeys,
                                     &ensureAscii,
                                     &indent,
                                     &defaultFn,
                                     &sortKeys,
                                     &numberModeObj,
                                     &datetimeModeObj,
                                     &uuidModeObj,
                                     &bytesModeObj,
                                     &writeModeObj,
                                     &iterableModeObj,
                                     &mappingModeObj,
                                     &allowNan))
        return NULL;

    if (defaultFn && !PyCallable_Check(defaultFn)) {
        if (defaultFn == Py_None) {
            defaultFn = NULL;
        } else {
            PyErr_SetString(PyExc_TypeError, "default must be a callable");
            return NULL;
        }
    }

    if (!accept_indent_arg(indent, writeMode, indentCount, indentChar))
        return NULL;

    if (!accept_write_mode_arg(writeModeObj, writeMode))
        return NULL;

    if (!accept_number_mode_arg(numberModeObj, allowNan, numberMode))
        return NULL;

    if (!accept_datetime_mode_arg(datetimeModeObj, datetimeMode))
        return NULL;

    if (!accept_uuid_mode_arg(uuidModeObj, uuidMode))
        return NULL;

    if (!accept_bytes_mode_arg(bytesModeObj, bytesMode))
        return NULL;

    if (!accept_iterable_mode_arg(iterableModeObj, iterableMode))
        return NULL;

    if (!accept_mapping_mode_arg(mappingModeObj, mappingMode))
        return NULL;

    if (skipKeys)
        mappingMode |= MM_SKIP_NON_STRING_KEYS;

    if (sortKeys)
        mappingMode |= MM_SORT_KEYS;

    return do_encode(value, defaultFn, ensureAscii ? true : false, writeMode, indentChar,
                     indentCount, numberMode, datetimeMode, uuidMode, bytesMode,
                     iterableMode, mappingMode);
}


PyDoc_STRVAR(dump_docstring,
             "dump(obj, stream, *, skipkeys=False, ensure_ascii=True,"
             " write_mode=WM_COMPACT, indent=4, default=None, sort_keys=False,"
             " number_mode=None, datetime_mode=None, uuid_mode=None, bytes_mode=BM_UTF8,"
             " iterable_mode=IM_ANY_ITERABLE, mapping_mode=MM_ANY_MAPPING,"
             " chunk_size=65536, allow_nan=True)\n"
             "\n"
             "Encode a Python object into a JSON stream.");


static PyObject*
dump(PyObject* self, PyObject* args, PyObject* kwargs)
{
    /* Converts a Python object to a JSON-encoded stream. */

    PyObject* value;
    PyObject* stream;
    int ensureAscii = true;
    PyObject* indent = NULL;
    PyObject* defaultFn = NULL;
    PyObject* numberModeObj = NULL;
    unsigned numberMode = NM_NAN;
    PyObject* datetimeModeObj = NULL;
    unsigned datetimeMode = DM_NONE;
    PyObject* uuidModeObj = NULL;
    unsigned uuidMode = UM_NONE;
    PyObject* bytesModeObj = NULL;
    unsigned bytesMode = BM_UTF8;
    PyObject* writeModeObj = NULL;
    unsigned writeMode = WM_COMPACT;
    PyObject* iterableModeObj = NULL;
    unsigned iterableMode = IM_ANY_ITERABLE;
    PyObject* mappingModeObj = NULL;
    unsigned mappingMode = MM_ANY_MAPPING;
    char indentChar = ' ';
    unsigned indentCount = 4;
    PyObject* chunkSizeObj = NULL;
    size_t chunkSize = 65536;
    int allowNan = -1;
    static char const* kwlist[] = {
        "obj",
        "stream",
        "skipkeys",             // alias of MM_SKIP_NON_STRING_KEYS
        "ensure_ascii",
        "indent",
        "default",
        "sort_keys",            // alias of MM_SORT_KEYS
        "number_mode",
        "datetime_mode",
        "uuid_mode",
        "bytes_mode",
        "chunk_size",
        "write_mode",
        "iterable_mode",
        "mapping_mode",

        /* compatibility with stdlib json */
        "allow_nan",

        NULL
    };
    int skipKeys = false;
    int sortKeys = false;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|$ppOOpOOOOOOOOp:rapidjson.dump",
                                     (char**) kwlist,
                                     &value,
                                     &stream,
                                     &skipKeys,
                                     &ensureAscii,
                                     &indent,
                                     &defaultFn,
                                     &sortKeys,
                                     &numberModeObj,
                                     &datetimeModeObj,
                                     &uuidModeObj,
                                     &bytesModeObj,
                                     &chunkSizeObj,
                                     &writeModeObj,
                                     &iterableModeObj,
                                     &mappingModeObj,
                                     &allowNan))
        return NULL;

    if (defaultFn && !PyCallable_Check(defaultFn)) {
        if (defaultFn == Py_None) {
            defaultFn = NULL;
        } else {
            PyErr_SetString(PyExc_TypeError, "default must be a callable");
            return NULL;
        }
    }

    if (!accept_indent_arg(indent, writeMode, indentCount, indentChar))
        return NULL;

    if (!accept_write_mode_arg(writeModeObj, writeMode))
        return NULL;

    if (!accept_number_mode_arg(numberModeObj, allowNan, numberMode))
        return NULL;

    if (!accept_datetime_mode_arg(datetimeModeObj, datetimeMode))
        return NULL;

    if (!accept_uuid_mode_arg(uuidModeObj, uuidMode))
        return NULL;

    if (!accept_bytes_mode_arg(bytesModeObj, bytesMode))
        return NULL;

    if (!accept_chunk_size_arg(chunkSizeObj, chunkSize))
        return NULL;

    if (!accept_iterable_mode_arg(iterableModeObj, iterableMode))
        return NULL;

    if (!accept_mapping_mode_arg(mappingModeObj, mappingMode))
        return NULL;

    if (skipKeys)
        mappingMode |= MM_SKIP_NON_STRING_KEYS;

    if (sortKeys)
        mappingMode |= MM_SORT_KEYS;

    return do_stream_encode(value, stream, chunkSize, defaultFn,
                            ensureAscii ? true : false, writeMode, indentChar,
                            indentCount, numberMode, datetimeMode, uuidMode, bytesMode,
                            iterableMode, mappingMode);
}


PyDoc_STRVAR(encoder_doc,
             "Encoder(skip_invalid_keys=False, ensure_ascii=True, write_mode=WM_COMPACT,"
             " indent=4, sort_keys=False, number_mode=None, datetime_mode=None,"
             " uuid_mode=None, bytes_mode=None, iterable_mode=IM_ANY_ITERABLE,"
             " mapping_mode=MM_ANY_MAPPING)\n\n"
             "Create and return a new Encoder instance.");


static PyMemberDef encoder_members[] = {
    {"ensure_ascii",
     T_BOOL, offsetof(EncoderObject, ensureAscii), READONLY,
     "whether the output should contain only ASCII characters."},
    {"indent_char",
     T_CHAR, offsetof(EncoderObject, indentChar), READONLY,
     "What will be used as end-of-line character."},
    {"indent_count",
     T_UINT, offsetof(EncoderObject, indentCount), READONLY,
     "The indentation width."},
    {"datetime_mode",
     T_UINT, offsetof(EncoderObject, datetimeMode), READONLY,
     "Whether and how datetime values should be encoded."},
    {"uuid_mode",
     T_UINT, offsetof(EncoderObject, uuidMode), READONLY,
     "Whether and how UUID values should be encoded"},
    {"number_mode",
     T_UINT, offsetof(EncoderObject, numberMode), READONLY,
     "The encoding behavior with regards to numeric values."},
    {"bytes_mode",
     T_UINT, offsetof(EncoderObject, bytesMode), READONLY,
     "How bytes values should be treated."},
    {"write_mode",
     T_UINT, offsetof(EncoderObject, writeMode), READONLY,
     "Whether the output should be pretty printed or not."},
    {"iterable_mode",
     T_UINT, offsetof(EncoderObject, iterableMode), READONLY,
     "Whether iterable values other than lists shall be encoded as JSON arrays or not."},
    {"mapping_mode",
     T_UINT, offsetof(EncoderObject, mappingMode), READONLY,
     "Whether mapping values other than dicts shall be encoded as JSON objects or not."},
    {NULL}
};


static PyObject*
encoder_get_skip_invalid_keys(EncoderObject* e, void* closure)
{
    return PyBool_FromLong(e->mappingMode & MM_SKIP_NON_STRING_KEYS);
}

static PyObject*
encoder_get_sort_keys(EncoderObject* e, void* closure)
{
    return PyBool_FromLong(e->mappingMode & MM_SORT_KEYS);
}

// Backward compatibility, previously they were members of EncoderObject

static PyGetSetDef encoder_props[] = {
    {"skip_invalid_keys", (getter) encoder_get_skip_invalid_keys, NULL,
     "Whether invalid keys shall be skipped."},
    {"sort_keys", (getter) encoder_get_sort_keys, NULL,
     "Whether dictionary keys shall be sorted alphabetically."},
    {NULL}
};

static PyTypeObject Encoder_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "rapidjson.Encoder",                      /* tp_name */
    sizeof(EncoderObject),                    /* tp_basicsize */
    0,                                        /* tp_itemsize */
    0,                                        /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    (ternaryfunc) encoder_call,               /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    encoder_doc,                              /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    0,                                        /* tp_methods */
    encoder_members,                          /* tp_members */
    encoder_props,                            /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    0,                                        /* tp_init */
    0,                                        /* tp_alloc */
    encoder_new,                              /* tp_new */
    PyObject_Del,                             /* tp_free */
};


#define Encoder_CheckExact(v) (Py_TYPE(v) == &Encoder_Type)
#define Encoder_Check(v) PyObject_TypeCheck(v, &Encoder_Type)


#define DUMPS_INTERNAL_CALL                             \
    (dumps_internal(&writer,                            \
                    value,                              \
                    defaultFn,                          \
                    numberMode,                         \
                    datetimeMode,                       \
                    uuidMode,                           \
                    bytesMode,                          \
                    iterableMode,                       \
                    mappingMode)                        \
     ? PyUnicode_FromString(buf.GetString()) : NULL)


static PyObject*
do_encode(PyObject* value, PyObject* defaultFn, bool ensureAscii, unsigned writeMode,
          char indentChar, unsigned indentCount, unsigned numberMode,
          unsigned datetimeMode, unsigned uuidMode, unsigned bytesMode,
          unsigned iterableMode, unsigned mappingMode)
{
    if (writeMode == WM_COMPACT) {
        if (ensureAscii) {
            GenericStringBuffer<ASCII<> > buf;
            Writer<GenericStringBuffer<ASCII<> >, UTF8<>, ASCII<> > writer(buf);
            return DUMPS_INTERNAL_CALL;
        } else {
            StringBuffer buf;
            Writer<StringBuffer> writer(buf);
            return DUMPS_INTERNAL_CALL;
        }
    } else if (ensureAscii) {
        GenericStringBuffer<ASCII<> > buf;
        PrettyWriter<GenericStringBuffer<ASCII<> >, UTF8<>, ASCII<> > writer(buf);
        writer.SetIndent(indentChar, indentCount);
        if (writeMode & WM_SINGLE_LINE_ARRAY) {
            writer.SetFormatOptions(kFormatSingleLineArray);
        }
        return DUMPS_INTERNAL_CALL;
    } else {
        StringBuffer buf;
        PrettyWriter<StringBuffer> writer(buf);
        writer.SetIndent(indentChar, indentCount);
        if (writeMode & WM_SINGLE_LINE_ARRAY) {
            writer.SetFormatOptions(kFormatSingleLineArray);
        }
        return DUMPS_INTERNAL_CALL;
    }
}


#define DUMP_INTERNAL_CALL                      \
    (dumps_internal(&writer,                    \
                    value,                      \
                    defaultFn,                  \
                    numberMode,                 \
                    datetimeMode,               \
                    uuidMode,                   \
                    bytesMode,                  \
                    iterableMode,               \
                    mappingMode)                \
     ? Py_INCREF(Py_None), Py_None : NULL)


static PyObject*
do_stream_encode(PyObject* value, PyObject* stream, size_t chunkSize, PyObject* defaultFn,
                 bool ensureAscii, unsigned writeMode, char indentChar,
                 unsigned indentCount, unsigned numberMode, unsigned datetimeMode,
                 unsigned uuidMode, unsigned bytesMode, unsigned iterableMode,
                 unsigned mappingMode)
{
    PyWriteStreamWrapper os(stream, chunkSize);

    if (writeMode == WM_COMPACT) {
        if (ensureAscii) {
            Writer<PyWriteStreamWrapper, UTF8<>, ASCII<> > writer(os);
            return DUMP_INTERNAL_CALL;
        } else {
            Writer<PyWriteStreamWrapper> writer(os);
            return DUMP_INTERNAL_CALL;
        }
    } else if (ensureAscii) {
        PrettyWriter<PyWriteStreamWrapper, UTF8<>, ASCII<> > writer(os);
        writer.SetIndent(indentChar, indentCount);
        if (writeMode & WM_SINGLE_LINE_ARRAY) {
            writer.SetFormatOptions(kFormatSingleLineArray);
        }
        return DUMP_INTERNAL_CALL;
    } else {
        PrettyWriter<PyWriteStreamWrapper> writer(os);
        writer.SetIndent(indentChar, indentCount);
        if (writeMode & WM_SINGLE_LINE_ARRAY) {
            writer.SetFormatOptions(kFormatSingleLineArray);
        }
        return DUMP_INTERNAL_CALL;
    }
}


static PyObject*
encoder_call(PyObject* self, PyObject* args, PyObject* kwargs)
{
    static char const* kwlist[] = {
        "obj",
        "stream",
        "chunk_size",
        NULL
    };
    PyObject* value;
    PyObject* stream = NULL;
    PyObject* chunkSizeObj = NULL;
    size_t chunkSize = 65536;
    PyObject* defaultFn = NULL;
    PyObject* result;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O$O",
                                     (char**) kwlist,
                                     &value,
                                     &stream,
                                     &chunkSizeObj))
        return NULL;

    EncoderObject* e = (EncoderObject*) self;

    if (stream != NULL && stream != Py_None) {
        if (!PyObject_HasAttr(stream, write_name)) {
            PyErr_SetString(PyExc_TypeError, "Expected a writable stream");
            return NULL;
        }

        if (!accept_chunk_size_arg(chunkSizeObj, chunkSize))
            return NULL;

        if (PyObject_HasAttr(self, default_name)) {
            defaultFn = PyObject_GetAttr(self, default_name);
        }

        result = do_stream_encode(value, stream, chunkSize, defaultFn, e->ensureAscii,
                                  e->writeMode, e->indentChar, e->indentCount,
                                  e->numberMode, e->datetimeMode, e->uuidMode,
                                  e->bytesMode, e->iterableMode, e->mappingMode);
    } else {
        if (PyObject_HasAttr(self, default_name)) {
            defaultFn = PyObject_GetAttr(self, default_name);
        }

        result = do_encode(value, defaultFn, e->ensureAscii, e->writeMode, e->indentChar,
                           e->indentCount, e->numberMode, e->datetimeMode, e->uuidMode,
                           e->bytesMode, e->iterableMode, e->mappingMode);
    }

    if (defaultFn != NULL)
        Py_DECREF(defaultFn);

    return result;
}


static PyObject*
encoder_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    EncoderObject* e;
    int ensureAscii = true;
    PyObject* indent = NULL;
    PyObject* numberModeObj = NULL;
    unsigned numberMode = NM_NAN;
    PyObject* datetimeModeObj = NULL;
    unsigned datetimeMode = DM_NONE;
    PyObject* uuidModeObj = NULL;
    unsigned uuidMode = UM_NONE;
    PyObject* bytesModeObj = NULL;
    unsigned bytesMode = BM_UTF8;
    PyObject* writeModeObj = NULL;
    unsigned writeMode = WM_COMPACT;
    PyObject* iterableModeObj = NULL;
    unsigned iterableMode = IM_ANY_ITERABLE;
    PyObject* mappingModeObj = NULL;
    unsigned mappingMode = MM_ANY_MAPPING;
    char indentChar = ' ';
    unsigned indentCount = 4;
    static char const* kwlist[] = {
        "skip_invalid_keys",    // alias of MM_SKIP_NON_STRING_KEYS
        "ensure_ascii",
        "indent",
        "sort_keys",            // alias of MM_SORT_KEYS
        "number_mode",
        "datetime_mode",
        "uuid_mode",
        "bytes_mode",
        "write_mode",
        "iterable_mode",
        "mapping_mode",
        NULL
    };
    int skipInvalidKeys = false;
    int sortKeys = false;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ppOpOOOOOOO:Encoder",
                                     (char**) kwlist,
                                     &skipInvalidKeys,
                                     &ensureAscii,
                                     &indent,
                                     &sortKeys,
                                     &numberModeObj,
                                     &datetimeModeObj,
                                     &uuidModeObj,
                                     &bytesModeObj,
                                     &writeModeObj,
                                     &iterableModeObj,
                                     &mappingModeObj))
        return NULL;

    if (!accept_indent_arg(indent, writeMode, indentCount, indentChar))
        return NULL;

    if (!accept_write_mode_arg(writeModeObj, writeMode))
        return NULL;

    if (!accept_number_mode_arg(numberModeObj, -1, numberMode))
        return NULL;

    if (!accept_datetime_mode_arg(datetimeModeObj, datetimeMode))
        return NULL;

    if (!accept_uuid_mode_arg(uuidModeObj, uuidMode))
        return NULL;

    if (!accept_bytes_mode_arg(bytesModeObj, bytesMode))
        return NULL;

    if (!accept_iterable_mode_arg(iterableModeObj, iterableMode))
        return NULL;

    if (!accept_mapping_mode_arg(mappingModeObj, mappingMode))
        return NULL;

    if (skipInvalidKeys)
        mappingMode |= MM_SKIP_NON_STRING_KEYS;

    if (sortKeys)
        mappingMode |= MM_SORT_KEYS;

    e = (EncoderObject*) type->tp_alloc(type, 0);
    if (e == NULL)
        return NULL;

    e->ensureAscii = ensureAscii ? true : false;
    e->writeMode = writeMode;
    e->indentChar = indentChar;
    e->indentCount = indentCount;
    e->datetimeMode = datetimeMode;
    e->uuidMode = uuidMode;
    e->numberMode = numberMode;
    e->bytesMode = bytesMode;
    e->iterableMode = iterableMode;
    e->mappingMode = mappingMode;

    return (PyObject*) e;
}


///////////////
// Validator //
///////////////


typedef struct {
    PyObject_HEAD
    SchemaDocument *schema;
} ValidatorObject;


PyDoc_STRVAR(validator_doc,
             "Validator(json_schema)\n"
             "\n"
             "Create and return a new Validator instance from the given `json_schema`"
             " string.");


static PyTypeObject Validator_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "rapidjson.Validator",          /* tp_name */
    sizeof(ValidatorObject),        /* tp_basicsize */
    0,                              /* tp_itemsize */
    (destructor) validator_dealloc, /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    0,                              /* tp_setattr */
    0,                              /* tp_compare */
    0,                              /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    (ternaryfunc) validator_call,   /* tp_call */
    0,                              /* tp_str */
    0,                              /* tp_getattro */
    0,                              /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,             /* tp_flags */
    validator_doc,                  /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    0,                              /* tp_init */
    0,                              /* tp_alloc */
    validator_new,                  /* tp_new */
    PyObject_Del,                   /* tp_free */
};


static PyObject* validator_call(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* jsonObject;

    if (!PyArg_ParseTuple(args, "O", &jsonObject))
        return NULL;

    const char* jsonStr;
    PyObject* asUnicode = NULL;

    if (PyUnicode_Check(jsonObject)) {
        jsonStr = PyUnicode_AsUTF8(jsonObject);
        if (jsonStr == NULL)
            return NULL;
    } else if (PyBytes_Check(jsonObject) || PyByteArray_Check(jsonObject)) {
        asUnicode = PyUnicode_FromEncodedObject(jsonObject, "utf-8", NULL);
        if (asUnicode == NULL)
            return NULL;
        jsonStr = PyUnicode_AsUTF8(asUnicode);
        if (jsonStr == NULL) {
            Py_DECREF(asUnicode);
            return NULL;
        }
    } else {
        PyErr_SetString(PyExc_TypeError,
                        "Expected string or UTF-8 encoded bytes or bytearray");
        return NULL;
    }

    Document d;
    bool error;

    Py_BEGIN_ALLOW_THREADS
    error = d.Parse(jsonStr).HasParseError();
    Py_END_ALLOW_THREADS

    if (error) {
        if (asUnicode != NULL)
            Py_DECREF(asUnicode);
        PyErr_SetString(decode_error, "Invalid JSON");
        return NULL;
    }

    SchemaValidator validator(*((ValidatorObject*) self)->schema);
    bool accept;

    Py_BEGIN_ALLOW_THREADS
    accept = d.Accept(validator);
    Py_END_ALLOW_THREADS

    if (asUnicode != NULL)
        Py_DECREF(asUnicode);

    if (!accept) {
        StringBuffer sptr;
        StringBuffer dptr;

        Py_BEGIN_ALLOW_THREADS
        validator.GetInvalidSchemaPointer().StringifyUriFragment(sptr);
        validator.GetInvalidDocumentPointer().StringifyUriFragment(dptr);
        Py_END_ALLOW_THREADS

        PyObject* error = Py_BuildValue("sss", validator.GetInvalidSchemaKeyword(),
                                        sptr.GetString(), dptr.GetString());
        PyErr_SetObject(validation_error, error);

        if (error != NULL)
            Py_DECREF(error);

        sptr.Clear();
        dptr.Clear();

        return NULL;
    }

    Py_RETURN_NONE;
}


static void validator_dealloc(PyObject* self)
{
    ValidatorObject* s = (ValidatorObject*) self;
    delete s->schema;
    Py_TYPE(self)->tp_free(self);
}


static PyObject* validator_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* jsonObject;

    if (!PyArg_ParseTuple(args, "O", &jsonObject))
        return NULL;

    const char* jsonStr;
    PyObject* asUnicode = NULL;

    if (PyUnicode_Check(jsonObject)) {
        jsonStr = PyUnicode_AsUTF8(jsonObject);
        if (jsonStr == NULL)
            return NULL;
    } else if (PyBytes_Check(jsonObject) || PyByteArray_Check(jsonObject)) {
        asUnicode = PyUnicode_FromEncodedObject(jsonObject, "utf-8", NULL);
        if (asUnicode == NULL)
            return NULL;
        jsonStr = PyUnicode_AsUTF8(asUnicode);
        if (jsonStr == NULL) {
            Py_DECREF(asUnicode);
            return NULL;
        }
    } else {
        PyErr_SetString(PyExc_TypeError,
                        "Expected string or UTF-8 encoded bytes or bytearray");
        return NULL;
    }

    Document d;
    bool error;

    Py_BEGIN_ALLOW_THREADS
    error = d.Parse(jsonStr).HasParseError();
    Py_END_ALLOW_THREADS

    if (asUnicode != NULL)
        Py_DECREF(asUnicode);

    if (error) {
        PyErr_SetString(decode_error, "Invalid JSON");
        return NULL;
    }

    ValidatorObject* v = (ValidatorObject*) type->tp_alloc(type, 0);
    if (v == NULL)
        return NULL;

    v->schema = new SchemaDocument(d);

    return (PyObject*) v;
}


////////////
// Module //
////////////


static PyMethodDef functions[] = {
    {"loads", (PyCFunction) loads, METH_VARARGS | METH_KEYWORDS,
     loads_docstring},
    {"load", (PyCFunction) load, METH_VARARGS | METH_KEYWORDS,
     load_docstring},
    {"dumps", (PyCFunction) dumps, METH_VARARGS | METH_KEYWORDS,
     dumps_docstring},
    {"dump", (PyCFunction) dump, METH_VARARGS | METH_KEYWORDS,
     dump_docstring},
    {NULL, NULL, 0, NULL} /* sentinel */
};


static int
module_exec(PyObject* m)
{
    PyObject* datetimeModule;
    PyObject* decimalModule;
    PyObject* uuidModule;

    if (PyType_Ready(&Decoder_Type) < 0)
        return -1;

    if (PyType_Ready(&Encoder_Type) < 0)
        return -1;

    if (PyType_Ready(&Validator_Type) < 0)
        return -1;

    if (PyType_Ready(&RawJSON_Type) < 0)
        return -1;

    PyDateTime_IMPORT;
    if(!PyDateTimeAPI)
        return -1;

    datetimeModule = PyImport_ImportModule("datetime");
    if (datetimeModule == NULL)
        return -1;

    decimalModule = PyImport_ImportModule("decimal");
    if (decimalModule == NULL)
        return -1;

    decimal_type = PyObject_GetAttrString(decimalModule, "Decimal");
    Py_DECREF(decimalModule);

    if (decimal_type == NULL)
        return -1;

    timezone_type = PyObject_GetAttrString(datetimeModule, "timezone");
    Py_DECREF(datetimeModule);

    if (timezone_type == NULL)
        return -1;

    timezone_utc = PyObject_GetAttrString(timezone_type, "utc");
    if (timezone_utc == NULL)
        return -1;

    uuidModule = PyImport_ImportModule("uuid");
    if (uuidModule == NULL)
        return -1;

    uuid_type = PyObject_GetAttrString(uuidModule, "UUID");
    Py_DECREF(uuidModule);

    if (uuid_type == NULL)
        return -1;

    astimezone_name = PyUnicode_InternFromString("astimezone");
    if (astimezone_name == NULL)
        return -1;

    hex_name = PyUnicode_InternFromString("hex");
    if (hex_name == NULL)
        return -1;

    timestamp_name = PyUnicode_InternFromString("timestamp");
    if (timestamp_name == NULL)
        return -1;

    total_seconds_name = PyUnicode_InternFromString("total_seconds");
    if (total_seconds_name == NULL)
        return -1;

    utcoffset_name = PyUnicode_InternFromString("utcoffset");
    if (utcoffset_name == NULL)
        return -1;

    is_infinite_name = PyUnicode_InternFromString("is_infinite");
    if (is_infinite_name == NULL)
        return -1;

    is_nan_name = PyUnicode_InternFromString("is_nan");
    if (is_infinite_name == NULL)
        return -1;

    minus_inf_string_value = PyUnicode_InternFromString("-Infinity");
    if (minus_inf_string_value == NULL)
        return -1;

    nan_string_value = PyUnicode_InternFromString("nan");
    if (nan_string_value == NULL)
        return -1;

    plus_inf_string_value = PyUnicode_InternFromString("+Infinity");
    if (plus_inf_string_value == NULL)
        return -1;

    start_object_name = PyUnicode_InternFromString("start_object");
    if (start_object_name == NULL)
        return -1;

    end_object_name = PyUnicode_InternFromString("end_object");
    if (end_object_name == NULL)
        return -1;

    default_name = PyUnicode_InternFromString("default");
    if (default_name == NULL)
        return -1;

    end_array_name = PyUnicode_InternFromString("end_array");
    if (end_array_name == NULL)
        return -1;

    string_name = PyUnicode_InternFromString("string");
    if (string_name == NULL)
        return -1;

    read_name = PyUnicode_InternFromString("read");
    if (read_name == NULL)
        return -1;

    write_name = PyUnicode_InternFromString("write");
    if (write_name == NULL)
        return -1;

    encoding_name = PyUnicode_InternFromString("encoding");
    if (encoding_name == NULL)
        return -1;

#define STRINGIFY(x) XSTRINGIFY(x)
#define XSTRINGIFY(x) #x

    if (PyModule_AddIntConstant(m, "DM_NONE", DM_NONE)
        || PyModule_AddIntConstant(m, "DM_ISO8601", DM_ISO8601)
        || PyModule_AddIntConstant(m, "DM_UNIX_TIME", DM_UNIX_TIME)
        || PyModule_AddIntConstant(m, "DM_ONLY_SECONDS", DM_ONLY_SECONDS)
        || PyModule_AddIntConstant(m, "DM_IGNORE_TZ", DM_IGNORE_TZ)
        || PyModule_AddIntConstant(m, "DM_NAIVE_IS_UTC", DM_NAIVE_IS_UTC)
        || PyModule_AddIntConstant(m, "DM_SHIFT_TO_UTC", DM_SHIFT_TO_UTC)

        || PyModule_AddIntConstant(m, "UM_NONE", UM_NONE)
        || PyModule_AddIntConstant(m, "UM_HEX", UM_HEX)
        || PyModule_AddIntConstant(m, "UM_CANONICAL", UM_CANONICAL)

        || PyModule_AddIntConstant(m, "NM_NONE", NM_NONE)
        || PyModule_AddIntConstant(m, "NM_NAN", NM_NAN)
        || PyModule_AddIntConstant(m, "NM_DECIMAL", NM_DECIMAL)
        || PyModule_AddIntConstant(m, "NM_NATIVE", NM_NATIVE)

        || PyModule_AddIntConstant(m, "PM_NONE", PM_NONE)
        || PyModule_AddIntConstant(m, "PM_COMMENTS", PM_COMMENTS)
        || PyModule_AddIntConstant(m, "PM_TRAILING_COMMAS", PM_TRAILING_COMMAS)

        || PyModule_AddIntConstant(m, "BM_NONE", BM_NONE)
        || PyModule_AddIntConstant(m, "BM_UTF8", BM_UTF8)

        || PyModule_AddIntConstant(m, "WM_COMPACT", WM_COMPACT)
        || PyModule_AddIntConstant(m, "WM_PRETTY", WM_PRETTY)
        || PyModule_AddIntConstant(m, "WM_SINGLE_LINE_ARRAY", WM_SINGLE_LINE_ARRAY)

        || PyModule_AddIntConstant(m, "IM_ANY_ITERABLE", IM_ANY_ITERABLE)
        || PyModule_AddIntConstant(m, "IM_ONLY_LISTS", IM_ONLY_LISTS)

        || PyModule_AddIntConstant(m, "MM_ANY_MAPPING", MM_ANY_MAPPING)
        || PyModule_AddIntConstant(m, "MM_ONLY_DICTS", MM_ONLY_DICTS)
        || PyModule_AddIntConstant(m, "MM_COERCE_KEYS_TO_STRINGS",
                                   MM_COERCE_KEYS_TO_STRINGS)
        || PyModule_AddIntConstant(m, "MM_SKIP_NON_STRING_KEYS", MM_SKIP_NON_STRING_KEYS)
        || PyModule_AddIntConstant(m, "MM_SORT_KEYS", MM_SORT_KEYS)

        || PyModule_AddStringConstant(m, "__version__",
                                      STRINGIFY(PYTHON_RAPIDJSON_VERSION))
        || PyModule_AddStringConstant(m, "__author__",
                                      "Ken Robbins <ken@kenrobbins.com>"
                                      ", Lele Gaifax <lele@metapensiero.it>")
        || PyModule_AddStringConstant(m, "__rapidjson_version__",
                                      RAPIDJSON_VERSION_STRING)
        || PyModule_AddStringConstant(m, "__rapidjson_exact_version__",
#ifdef RAPIDJSON_EXACT_VERSION
                                      STRINGIFY(RAPIDJSON_EXACT_VERSION)
#else
                                      // This may happen for several reasons, under CI
                                      // test or when the RJ library does not come from
                                      // the git submodule
                                      "not available"
#endif
            )
        )
        return -1;

    Py_INCREF(&Decoder_Type);
    if (PyModule_AddObject(m, "Decoder", (PyObject*) &Decoder_Type) < 0) {
        Py_DECREF(&Decoder_Type);
        return -1;
    }

    Py_INCREF(&Encoder_Type);
    if (PyModule_AddObject(m, "Encoder", (PyObject*) &Encoder_Type) < 0) {
        Py_DECREF(&Encoder_Type);
        return -1;
    }

    Py_INCREF(&Validator_Type);
    if (PyModule_AddObject(m, "Validator", (PyObject*) &Validator_Type) < 0) {
        Py_DECREF(&Validator_Type);
        return -1;
    }

    Py_INCREF(&RawJSON_Type);
    if (PyModule_AddObject(m, "RawJSON", (PyObject*) &RawJSON_Type) < 0) {
        Py_DECREF(&RawJSON_Type);
        return -1;
    }

    validation_error = PyErr_NewException("rapidjson.ValidationError",
                                          PyExc_ValueError, NULL);
    if (validation_error == NULL)
        return -1;
    Py_INCREF(validation_error);
    if (PyModule_AddObject(m, "ValidationError", validation_error) < 0) {
        Py_DECREF(validation_error);
        return -1;
    }

    decode_error = PyErr_NewException("rapidjson.JSONDecodeError",
                                      PyExc_ValueError, NULL);
    if (decode_error == NULL)
        return -1;
    Py_INCREF(decode_error);
    if (PyModule_AddObject(m, "JSONDecodeError", decode_error) < 0) {
        Py_DECREF(decode_error);
        return -1;
    }

    return 0;
}


static struct PyModuleDef_Slot slots[] = {
    {Py_mod_exec, (void*) module_exec},
    {0, NULL}
};


static PyModuleDef module = {
    PyModuleDef_HEAD_INIT,      /* m_base */
    "rapidjson",                /* m_name */
    PyDoc_STR("Fast, simple JSON encoder and decoder. Based on RapidJSON C++ library."),
    0,                          /* m_size */
    functions,                  /* m_methods */
    slots,                      /* m_slots */
    NULL,                       /* m_traverse */
    NULL,                       /* m_clear */
    NULL                        /* m_free */
};


PyMODINIT_FUNC
PyInit_rapidjson()
{
    return PyModuleDef_Init(&module);
}
