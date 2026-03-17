/*
 * Copyright 2009-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains C implementations of some of the functions
 * needed by the bson module. If possible, these implementations
 * should be used to speed up BSON encoding and decoding.
 */

#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "datetime.h"

#include "buffer.h"
#include "time64.h"
#include "encoding_helpers.h"

#define _CBSON_MODULE
#include "_cbsonmodule.h"

/* New module state and initialization code.
 * See the module-initialization-and-state
 * section in the following doc:
 * http://docs.python.org/release/3.1.3/howto/cporting.html
 * which references the following pep:
 * http://www.python.org/dev/peps/pep-3121/
 * */
struct module_state {
    PyObject* Binary;
    PyObject* Code;
    PyObject* ObjectId;
    PyObject* DBRef;
    PyObject* Regex;
    PyObject* UUID;
    PyObject* Timestamp;
    PyObject* MinKey;
    PyObject* MaxKey;
    PyObject* UTC;
    PyTypeObject* REType;
    PyObject* BSONInt64;
    PyObject* Decimal128;
    PyObject* Mapping;
};

/* The Py_TYPE macro was introduced in CPython 2.6 */
#ifndef Py_TYPE
#define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

/* Maximum number of regex flags */
#define FLAGS_SIZE 7

/* Default UUID representation type code. */
#define PYTHON_LEGACY 3

/* Other UUID representations. */
#define STANDARD 4
#define JAVA_LEGACY   5
#define CSHARP_LEGACY 6
#define UNSPECIFIED 0

#define BSON_MAX_SIZE 2147483647
/* The smallest possible BSON document, i.e. "{}" */
#define BSON_MIN_SIZE 5

/* Get an error class from the bson.errors module.
 *
 * Returns a new ref */
static PyObject* _error(char* name) {
    PyObject* error;
    PyObject* errors = PyImport_ImportModule("bson.errors");
    if (!errors) {
        return NULL;
    }
    error = PyObject_GetAttrString(errors, name);
    Py_DECREF(errors);
    return error;
}

/* Safely downcast from Py_ssize_t to int, setting an
 * exception and returning -1 on error. */
static int
_downcast_and_check(Py_ssize_t size, uint8_t extra) {
    if (size > BSON_MAX_SIZE || ((BSON_MAX_SIZE - extra) < size)) {
        PyObject* InvalidStringData = _error("InvalidStringData");
        if (InvalidStringData) {
            PyErr_SetString(InvalidStringData,
                            "String length must be <= 2147483647");
            Py_DECREF(InvalidStringData);
        }
        return -1;
    }
    return (int)size + extra;
}

static PyObject* elements_to_dict(PyObject* self, const char* string,
                                  unsigned max,
                                  const codec_options_t* options);

static int _write_element_to_buffer(PyObject* self, buffer_t buffer,
                                    int type_byte, PyObject* value,
                                    unsigned char check_keys,
                                    const codec_options_t* options,
                                    unsigned char in_custom_call,
                                    unsigned char in_fallback_call);

/* Write a RawBSONDocument to the buffer.
 * Returns the number of bytes written or 0 on failure.
 */
static int write_raw_doc(buffer_t buffer, PyObject* raw);

/* Date stuff */
static PyObject* datetime_from_millis(long long millis) {
    /* To encode a datetime instance like datetime(9999, 12, 31, 23, 59, 59, 999999)
     * we follow these steps:
     * 1. Calculate a timestamp in seconds:       253402300799
     * 2. Multiply that by 1000:                  253402300799000
     * 3. Add in microseconds divided by 1000     253402300799999
     *
     * (Note: BSON doesn't support microsecond accuracy, hence the rounding.)
     *
     * To decode we could do:
     * 1. Get seconds: timestamp / 1000:          253402300799
     * 2. Get micros: (timestamp % 1000) * 1000:  999000
     * Resulting in datetime(9999, 12, 31, 23, 59, 59, 999000) -- the expected result
     *
     * Now what if the we encode (1, 1, 1, 1, 1, 1, 111111)?
     * 1. and 2. gives:                           -62135593139000
     * 3. Gives us:                               -62135593138889
     *
     * Now decode:
     * 1. Gives us:                               -62135593138
     * 2. Gives us:                               -889000
     * Resulting in datetime(1, 1, 1, 1, 1, 2, 15888216) -- an invalid result
     *
     * If instead to decode we do:
     * diff = ((millis % 1000) + 1000) % 1000:    111
     * seconds = (millis - diff) / 1000:          -62135593139
     * micros = diff * 1000                       111000
     * Resulting in datetime(1, 1, 1, 1, 1, 1, 111000) -- the expected result
     */
    int diff = (int)(((millis % 1000) + 1000) % 1000);
    int microseconds = diff * 1000;
    Time64_T seconds = (millis - diff) / 1000;
    struct TM timeinfo;
    gmtime64_r(&seconds, &timeinfo);

    return PyDateTime_FromDateAndTime(timeinfo.tm_year + 1900,
                                      timeinfo.tm_mon + 1,
                                      timeinfo.tm_mday,
                                      timeinfo.tm_hour,
                                      timeinfo.tm_min,
                                      timeinfo.tm_sec,
                                      microseconds);
}

static long long millis_from_datetime(PyObject* datetime) {
    struct TM timeinfo;
    long long millis;

    timeinfo.tm_year = PyDateTime_GET_YEAR(datetime) - 1900;
    timeinfo.tm_mon = PyDateTime_GET_MONTH(datetime) - 1;
    timeinfo.tm_mday = PyDateTime_GET_DAY(datetime);
    timeinfo.tm_hour = PyDateTime_DATE_GET_HOUR(datetime);
    timeinfo.tm_min = PyDateTime_DATE_GET_MINUTE(datetime);
    timeinfo.tm_sec = PyDateTime_DATE_GET_SECOND(datetime);

    millis = timegm64(&timeinfo) * 1000;
    millis += PyDateTime_DATE_GET_MICROSECOND(datetime) / 1000;
    return millis;
}

/* Just make this compatible w/ the old API. */
int buffer_write_bytes(buffer_t buffer, const char* data, int size) {
    if (buffer_write(buffer, data, size)) {
        return 0;
    }
    return 1;
}

int buffer_write_double(buffer_t buffer, double data) {
    double data_le = BSON_DOUBLE_TO_LE(data);
    return buffer_write_bytes(buffer, (const char*)&data_le, 8);
}

int buffer_write_int32(buffer_t buffer, int32_t data) {
    uint32_t data_le = BSON_UINT32_TO_LE(data);
    return buffer_write_bytes(buffer, (const char*)&data_le, 4);
}

int buffer_write_int64(buffer_t buffer, int64_t data) {
    uint64_t data_le = BSON_UINT64_TO_LE(data);
    return buffer_write_bytes(buffer, (const char*)&data_le, 8);
}

void buffer_write_int32_at_position(buffer_t buffer,
                                    int position,
                                    int32_t data) {
    uint32_t data_le = BSON_UINT32_TO_LE(data);
    memcpy(buffer_get_buffer(buffer) + position, &data_le, 4);
}

static int write_unicode(buffer_t buffer, PyObject* py_string) {
    int size;
    const char* data;
    PyObject* encoded = PyUnicode_AsUTF8String(py_string);
    if (!encoded) {
        return 0;
    }
#if PY_MAJOR_VERSION >= 3
    data = PyBytes_AS_STRING(encoded);
#else
    data = PyString_AS_STRING(encoded);
#endif
    if (!data)
        goto unicodefail;

#if PY_MAJOR_VERSION >= 3
    if ((size = _downcast_and_check(PyBytes_GET_SIZE(encoded), 1)) == -1)
#else
    if ((size = _downcast_and_check(PyString_GET_SIZE(encoded), 1)) == -1)
#endif
        goto unicodefail;

    if (!buffer_write_int32(buffer, (int32_t)size))
        goto unicodefail;

    if (!buffer_write_bytes(buffer, data, size))
        goto unicodefail;

    Py_DECREF(encoded);
    return 1;

unicodefail:
    Py_DECREF(encoded);
    return 0;
}

/* returns 0 on failure */
static int write_string(buffer_t buffer, PyObject* py_string) {
    int size;
    const char* data;
#if PY_MAJOR_VERSION >= 3
    if (PyUnicode_Check(py_string)){
        return write_unicode(buffer, py_string);
    }
    data = PyBytes_AsString(py_string);
#else
    data = PyString_AsString(py_string);
#endif
    if (!data) {
        return 0;
    }

#if PY_MAJOR_VERSION >= 3
    if ((size = _downcast_and_check(PyBytes_Size(py_string), 1)) == -1)
#else
    if ((size = _downcast_and_check(PyString_Size(py_string), 1)) == -1)
#endif
        return 0;

    if (!buffer_write_int32(buffer, (int32_t)size)) {
        return 0;
    }
    if (!buffer_write_bytes(buffer, data, size)) {
        return 0;
    }
    return 1;
}

/*
 * Are we in the main interpreter or a sub-interpreter?
 * Useful for deciding if we can use cached pure python
 * types in mod_wsgi.
 */
static int
_in_main_interpreter(void) {
    static PyInterpreterState* main_interpreter = NULL;
    PyInterpreterState* interpreter;

    if (main_interpreter == NULL) {
        interpreter = PyInterpreterState_Head();

        while (PyInterpreterState_Next(interpreter))
            interpreter = PyInterpreterState_Next(interpreter);

        main_interpreter = interpreter;
    }

    return (main_interpreter == PyThreadState_Get()->interp);
}

/*
 * Get a reference to a pure python type. If we are in the
 * main interpreter return the cached object, otherwise import
 * the object we need and return it instead.
 */
static PyObject*
_get_object(PyObject* object, char* module_name, char* object_name) {
    if (_in_main_interpreter()) {
        Py_XINCREF(object);
        return object;
    } else {
        PyObject* imported = NULL;
        PyObject* module = PyImport_ImportModule(module_name);
        if (!module)
            return NULL;
        imported = PyObject_GetAttrString(module, object_name);
        Py_DECREF(module);
        return imported;
    }
}

/* Load a Python object to cache.
 *
 * Returns non-zero on failure. */
static int _load_object(PyObject** object, char* module_name, char* object_name) {
    PyObject* module;

    module = PyImport_ImportModule(module_name);
    if (!module) {
        return 1;
    }

    *object = PyObject_GetAttrString(module, object_name);
    Py_DECREF(module);

    return (*object) ? 0 : 2;
}

/* Load all Python objects to cache.
 *
 * Returns non-zero on failure. */
static int _load_python_objects(PyObject* module) {
    PyObject* empty_string = NULL;
    PyObject* re_compile = NULL;
    PyObject* compiled = NULL;
    struct module_state *state = GETSTATE(module);

    if (_load_object(&state->Binary, "bson.binary", "Binary") ||
        _load_object(&state->Code, "bson.code", "Code") ||
        _load_object(&state->ObjectId, "bson.objectid", "ObjectId") ||
        _load_object(&state->DBRef, "bson.dbref", "DBRef") ||
        _load_object(&state->Timestamp, "bson.timestamp", "Timestamp") ||
        _load_object(&state->MinKey, "bson.min_key", "MinKey") ||
        _load_object(&state->MaxKey, "bson.max_key", "MaxKey") ||
        _load_object(&state->UTC, "bson.tz_util", "utc") ||
        _load_object(&state->Regex, "bson.regex", "Regex") ||
        _load_object(&state->BSONInt64, "bson.int64", "Int64") ||
        _load_object(&state->Decimal128, "bson.decimal128", "Decimal128") ||
        _load_object(&state->UUID, "uuid", "UUID") ||
#if PY_MAJOR_VERSION >= 3
        _load_object(&state->Mapping, "collections.abc", "Mapping")) {
#else
        _load_object(&state->Mapping, "collections", "Mapping")) {
#endif
        return 1;
    }
    /* Reload our REType hack too. */
#if PY_MAJOR_VERSION >= 3
    empty_string = PyBytes_FromString("");
#else
    empty_string = PyString_FromString("");
#endif
    if (empty_string == NULL) {
        state->REType = NULL;
        return 1;
    }

    if (_load_object(&re_compile, "re", "compile")) {
        state->REType = NULL;
        Py_DECREF(empty_string);
        return 1;
    }

    compiled = PyObject_CallFunction(re_compile, "O", empty_string);
    Py_DECREF(re_compile);
    if (compiled == NULL) {
        state->REType = NULL;
        Py_DECREF(empty_string);
        return 1;
    }
    Py_INCREF(Py_TYPE(compiled));
    state->REType = Py_TYPE(compiled);
    Py_DECREF(empty_string);
    Py_DECREF(compiled);
    return 0;
}

/*
 * Get the _type_marker from an Object.
 *
 * Return the type marker, 0 if there is no marker, or -1 on failure.
 */
static long _type_marker(PyObject* object) {
    PyObject* type_marker = NULL;
    long type = 0;

    if (PyObject_HasAttrString(object, "_type_marker")) {
        type_marker = PyObject_GetAttrString(object, "_type_marker");
        if (type_marker == NULL) {
            return -1;
        }
    }

    /*
     * Python objects with broken __getattr__ implementations could return
     * arbitrary types for a call to PyObject_GetAttrString. For example
     * pymongo.database.Database returns a new Collection instance for
     * __getattr__ calls with names that don't match an existing attribute
     * or method. In some cases "value" could be a subtype of something
     * we know how to serialize. Make a best effort to encode these types.
     */
#if PY_MAJOR_VERSION >= 3
    if (type_marker && PyLong_CheckExact(type_marker)) {
        type = PyLong_AsLong(type_marker);
#else
    if (type_marker && PyInt_CheckExact(type_marker)) {
        type = PyInt_AsLong(type_marker);
#endif
        Py_DECREF(type_marker);
        /*
         * Py(Long|Int)_AsLong returns -1 for error but -1 is a valid value
         * so we call PyErr_Occurred to differentiate.
         */
        if (type == -1 && PyErr_Occurred()) {
            return -1;
        }
    } else {
        Py_XDECREF(type_marker);
    }

    return type;
}

/* Fill out a type_registry_t* from a TypeRegistry object.
 *
 * Return 1 on success. options->document_class is a new reference.
 * Return 0 on failure.
 */
int convert_type_registry(PyObject* registry_obj, type_registry_t* registry) {
    registry->encoder_map = NULL;
    registry->decoder_map = NULL;
    registry->fallback_encoder = NULL;
    registry->registry_obj = NULL;

    registry->encoder_map = PyObject_GetAttrString(registry_obj, "_encoder_map");
    if (registry->encoder_map == NULL) {
        goto fail;
    }
    registry->is_encoder_empty = (PyDict_Size(registry->encoder_map) == 0);

    registry->decoder_map = PyObject_GetAttrString(registry_obj, "_decoder_map");
    if (registry->decoder_map == NULL) {
        goto fail;
    }
    registry->is_decoder_empty = (PyDict_Size(registry->decoder_map) == 0);

    registry->fallback_encoder = PyObject_GetAttrString(registry_obj, "_fallback_encoder");
    if (registry->fallback_encoder == NULL) {
        goto fail;
    }
    registry->has_fallback_encoder = (registry->fallback_encoder != Py_None);

    registry->registry_obj = registry_obj;
    Py_INCREF(registry->registry_obj);
    return 1;

fail:
    Py_XDECREF(registry->encoder_map);
    Py_XDECREF(registry->decoder_map);
    Py_XDECREF(registry->fallback_encoder);
    return 0;
}

/* Fill out a codec_options_t* from a CodecOptions object. Use with the "O&"
 * format spec in PyArg_ParseTuple.
 *
 * Return 1 on success. options->document_class is a new reference.
 * Return 0 on failure.
 */
int convert_codec_options(PyObject* options_obj, void* p) {
    codec_options_t* options = (codec_options_t*)p;
    PyObject* type_registry_obj = NULL;
    long type_marker;

    options->unicode_decode_error_handler = NULL;

    if (!PyArg_ParseTuple(options_obj, "ObbzOO",
                          &options->document_class,
                          &options->tz_aware,
                          &options->uuid_rep,
                          &options->unicode_decode_error_handler,
                          &options->tzinfo,
                          &type_registry_obj))
        return 0;

    type_marker = _type_marker(options->document_class);
    if (type_marker < 0) {
        return 0;
    }

    if (!convert_type_registry(type_registry_obj,
                               &options->type_registry)) {
        return 0;
    }

    options->is_raw_bson = (101 == type_marker);
    options->options_obj = options_obj;

    Py_INCREF(options->options_obj);
    Py_INCREF(options->document_class);
    Py_INCREF(options->tzinfo);

    return 1;
}

void destroy_codec_options(codec_options_t* options) {
    Py_CLEAR(options->document_class);
    Py_CLEAR(options->tzinfo);
    Py_CLEAR(options->options_obj);
    Py_CLEAR(options->type_registry.registry_obj);
    Py_CLEAR(options->type_registry.encoder_map);
    Py_CLEAR(options->type_registry.decoder_map);
    Py_CLEAR(options->type_registry.fallback_encoder);
}

static int write_element_to_buffer(PyObject* self, buffer_t buffer,
                                   int type_byte, PyObject* value,
                                   unsigned char check_keys,
                                   const codec_options_t* options,
                                   unsigned char in_custom_call,
                                   unsigned char in_fallback_call) {
    int result = 0;
    if(Py_EnterRecursiveCall(" while encoding an object to BSON ")) {
        return 0;
    }
    result = _write_element_to_buffer(self, buffer, type_byte,
                                      value, check_keys, options,
                                      in_custom_call, in_fallback_call);
    Py_LeaveRecursiveCall();
    return result;
}

static void
_set_cannot_encode(PyObject* value) {
    PyObject* type = NULL;
    PyObject* InvalidDocument = _error("InvalidDocument");
    if (InvalidDocument == NULL) {
        goto error;
    }

    type = PyObject_Type(value);
    if (type == NULL) {
        goto error;
    }
#if PY_MAJOR_VERSION >= 3
    PyErr_Format(InvalidDocument, "cannot encode object: %R, of type: %R",
                 value, type);
#else
     else {
        PyObject* value_repr = NULL;
        PyObject* type_repr = NULL;
        char* value_str = NULL;
        char* type_str = NULL;

        value_repr = PyObject_Repr(value);
        if (value_repr == NULL) {
            goto py2error;
        }
        value_str = PyString_AsString(value_repr);
        if (value_str == NULL) {
            goto py2error;
        }
        type_repr = PyObject_Repr(type);
        if (type_repr == NULL) {
            goto py2error;
        }
        type_str = PyString_AsString(type_repr);
        if (type_str == NULL) {
            goto py2error;
        }

        PyErr_Format(InvalidDocument, "cannot encode object: %s, of type: %s",
                     value_str, type_str);
py2error:
        Py_XDECREF(type_repr);
        Py_XDECREF(value_repr);
    }
#endif
error:
    Py_XDECREF(type);
    Py_XDECREF(InvalidDocument);
}

/*
 * Encode a builtin Python regular expression or our custom Regex class.
 *
 * Sets exception and returns 0 on failure.
 */
static int _write_regex_to_buffer(
    buffer_t buffer, int type_byte, PyObject* value) {

    PyObject* py_flags;
    PyObject* py_pattern;
    PyObject* encoded_pattern;
    long int_flags;
    char flags[FLAGS_SIZE];
    char check_utf8 = 0;
    const char* pattern_data;
    int pattern_length, flags_length;
    result_t status;

    /*
     * Both the builtin re type and our Regex class have attributes
     * "flags" and "pattern".
     */
    py_flags = PyObject_GetAttrString(value, "flags");
    if (!py_flags) {
        return 0;
    }
#if PY_MAJOR_VERSION >= 3
    int_flags = PyLong_AsLong(py_flags);
#else
    int_flags = PyInt_AsLong(py_flags);
#endif
    Py_DECREF(py_flags);
    if (int_flags == -1 && PyErr_Occurred()) {
        return 0;
    }
    py_pattern = PyObject_GetAttrString(value, "pattern");
    if (!py_pattern) {
        return 0;
    }

    if (PyUnicode_Check(py_pattern)) {
        encoded_pattern = PyUnicode_AsUTF8String(py_pattern);
        Py_DECREF(py_pattern);
        if (!encoded_pattern) {
            return 0;
        }
    } else {
        encoded_pattern = py_pattern;
        check_utf8 = 1;
    }

#if PY_MAJOR_VERSION >= 3
    if (!(pattern_data = PyBytes_AsString(encoded_pattern))) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    if ((pattern_length = _downcast_and_check(PyBytes_Size(encoded_pattern), 0)) == -1) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
#else
    if (!(pattern_data = PyString_AsString(encoded_pattern))) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    if ((pattern_length = _downcast_and_check(PyString_Size(encoded_pattern), 0)) == -1) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
#endif
    status = check_string((const unsigned char*)pattern_data,
                          pattern_length, check_utf8, 1);
    if (status == NOT_UTF_8) {
        PyObject* InvalidStringData = _error("InvalidStringData");
        if (InvalidStringData) {
            PyErr_SetString(InvalidStringData,
                            "regex patterns must be valid UTF-8");
            Py_DECREF(InvalidStringData);
        }
        Py_DECREF(encoded_pattern);
        return 0;
    } else if (status == HAS_NULL) {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyErr_SetString(InvalidDocument,
                            "regex patterns must not contain the NULL byte");
            Py_DECREF(InvalidDocument);
        }
        Py_DECREF(encoded_pattern);
        return 0;
    }

    if (!buffer_write_bytes(buffer, pattern_data, pattern_length + 1)) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    Py_DECREF(encoded_pattern);

    flags[0] = 0;

    if (int_flags & 2) {
        STRCAT(flags, FLAGS_SIZE, "i");
    }
    if (int_flags & 4) {
        STRCAT(flags, FLAGS_SIZE, "l");
    }
    if (int_flags & 8) {
        STRCAT(flags, FLAGS_SIZE, "m");
    }
    if (int_flags & 16) {
        STRCAT(flags, FLAGS_SIZE, "s");
    }
    if (int_flags & 32) {
        STRCAT(flags, FLAGS_SIZE, "u");
    }
    if (int_flags & 64) {
        STRCAT(flags, FLAGS_SIZE, "x");
    }
    flags_length = (int)strlen(flags) + 1;
    if (!buffer_write_bytes(buffer, flags, flags_length)) {
        return 0;
    }
    *(buffer_get_buffer(buffer) + type_byte) = 0x0B;
    return 1;
}

/* Write a single value to the buffer (also write its type_byte, for which
 * space has already been reserved.
 *
 * returns 0 on failure */
static int _write_element_to_buffer(PyObject* self, buffer_t buffer,
                                    int type_byte, PyObject* value,
                                    unsigned char check_keys,
                                    const codec_options_t* options,
                                    unsigned char in_custom_call,
                                    unsigned char in_fallback_call) {
    struct module_state *state = GETSTATE(self);
    PyObject* mapping_type;
    PyObject* new_value = NULL;
    int retval;
    PyObject* uuid_type;
    /*
     * Don't use PyObject_IsInstance for our custom types. It causes
     * problems with python sub interpreters. Our custom types should
     * have a _type_marker attribute, which we can switch on instead.
     */
    long type = _type_marker(value);
    if (type < 0) {
        return 0;
    }

    switch (type) {
    case 5:
        {
            /* Binary */
            PyObject* subtype_object;
            char subtype;
            const char* data;
            int size;

            *(buffer_get_buffer(buffer) + type_byte) = 0x05;
            subtype_object = PyObject_GetAttrString(value, "subtype");
            if (!subtype_object) {
                return 0;
            }
#if PY_MAJOR_VERSION >= 3
            subtype = (char)PyLong_AsLong(subtype_object);
#else
            subtype = (char)PyInt_AsLong(subtype_object);
#endif
            if (subtype == -1) {
                Py_DECREF(subtype_object);
                return 0;
            }
#if PY_MAJOR_VERSION >= 3
            size = _downcast_and_check(PyBytes_Size(value), 0);
#else
            size = _downcast_and_check(PyString_Size(value), 0);
#endif
            if (size == -1) {
                Py_DECREF(subtype_object);
                return 0;
            }

            Py_DECREF(subtype_object);
            if (subtype == 2) {
#if PY_MAJOR_VERSION >= 3
                int other_size = _downcast_and_check(PyBytes_Size(value), 4);
#else
                int other_size = _downcast_and_check(PyString_Size(value), 4);
#endif
                if (other_size == -1)
                    return 0;
                if (!buffer_write_int32(buffer, other_size)) {
                    return 0;
                }
                if (!buffer_write_bytes(buffer, &subtype, 1)) {
                    return 0;
                }
            }
            if (!buffer_write_int32(buffer, size)) {
                return 0;
            }
            if (subtype != 2) {
                if (!buffer_write_bytes(buffer, &subtype, 1)) {
                    return 0;
                }
            }
#if PY_MAJOR_VERSION >= 3
            data = PyBytes_AsString(value);
#else
            data = PyString_AsString(value);
#endif
            if (!data) {
                return 0;
            }
            if (!buffer_write_bytes(buffer, data, size)) {
                    return 0;
            }
            return 1;
        }
    case 7:
        {
            /* ObjectId */
            const char* data;
            PyObject* pystring = PyObject_GetAttrString(value, "binary");
            if (!pystring) {
                return 0;
            }
#if PY_MAJOR_VERSION >= 3
            data = PyBytes_AsString(pystring);
#else
            data = PyString_AsString(pystring);
#endif
            if (!data) {
                Py_DECREF(pystring);
                return 0;
            }
            if (!buffer_write_bytes(buffer, data, 12)) {
                Py_DECREF(pystring);
                return 0;
            }
            Py_DECREF(pystring);
            *(buffer_get_buffer(buffer) + type_byte) = 0x07;
            return 1;
        }
    case 11:
        {
            /* Regex */
            return _write_regex_to_buffer(buffer, type_byte, value);
        }
    case 13:
        {
            /* Code */
            int start_position,
                length_location,
                length;

            PyObject* scope = PyObject_GetAttrString(value, "scope");
            if (!scope) {
                return 0;
            }

            if (scope == Py_None) {
                Py_DECREF(scope);
                *(buffer_get_buffer(buffer) + type_byte) = 0x0D;
                return write_string(buffer, value);
            }

            *(buffer_get_buffer(buffer) + type_byte) = 0x0F;

            start_position = buffer_get_position(buffer);
            /* save space for length */
            length_location = buffer_save_space(buffer, 4);
            if (length_location == -1) {
                Py_DECREF(scope);
                return 0;
            }

            if (!write_string(buffer, value)) {
                Py_DECREF(scope);
                return 0;
            }

            if (!write_dict(self, buffer, scope, 0, options, 0)) {
                Py_DECREF(scope);
                return 0;
            }
            Py_DECREF(scope);

            length = buffer_get_position(buffer) - start_position;
            buffer_write_int32_at_position(
                buffer, length_location, (int32_t)length);
            return 1;
        }
    case 17:
        {
            /* Timestamp */
            PyObject* obj;
            unsigned long i;

            obj = PyObject_GetAttrString(value, "inc");
            if (!obj) {
                return 0;
            }
            i = PyLong_AsUnsignedLong(obj);
            Py_DECREF(obj);
            if (i == (unsigned long)-1 && PyErr_Occurred()) {
                return 0;
            }
            if (!buffer_write_int32(buffer, (int32_t)i)) {
                return 0;
            }

            obj = PyObject_GetAttrString(value, "time");
            if (!obj) {
                return 0;
            }
            i = PyLong_AsUnsignedLong(obj);
            Py_DECREF(obj);
            if (i == (unsigned long)-1 && PyErr_Occurred()) {
                return 0;
            }
            if (!buffer_write_int32(buffer, (int32_t)i)) {
                return 0;
            }

            *(buffer_get_buffer(buffer) + type_byte) = 0x11;
            return 1;
        }
    case 18:
        {
            /* Int64 */
            const long long ll = PyLong_AsLongLong(value);
            if (PyErr_Occurred()) { /* Overflow */
                PyErr_SetString(PyExc_OverflowError,
                                "MongoDB can only handle up to 8-byte ints");
                return 0;
            }
            if (!buffer_write_int64(buffer, (int64_t)ll)) {
                return 0;
            }
            *(buffer_get_buffer(buffer) + type_byte) = 0x12;
            return 1;
        }
    case 19:
        {
            /* Decimal128 */
            const char* data;
            PyObject* pystring = PyObject_GetAttrString(value, "bid");
            if (!pystring) {
                return 0;
            }
#if PY_MAJOR_VERSION >= 3
            data = PyBytes_AsString(pystring);
#else
            data = PyString_AsString(pystring);
#endif
            if (!data) {
                Py_DECREF(pystring);
                return 0;
            }
            if (!buffer_write_bytes(buffer, data, 16)) {
                Py_DECREF(pystring);
                return 0;
            }
            Py_DECREF(pystring);
            *(buffer_get_buffer(buffer) + type_byte) = 0x13;
            return 1;
        }
    case 100:
        {
            /* DBRef */
            PyObject* as_doc = PyObject_CallMethod(value, "as_doc", NULL);
            if (!as_doc) {
                return 0;
            }
            if (!write_dict(self, buffer, as_doc, 0, options, 0)) {
                Py_DECREF(as_doc);
                return 0;
            }
            Py_DECREF(as_doc);
            *(buffer_get_buffer(buffer) + type_byte) = 0x03;
            return 1;
        }
    case 101:
        {
            /* RawBSONDocument */
            if (!write_raw_doc(buffer, value)) {
                return 0;
            }
            *(buffer_get_buffer(buffer) + type_byte) = 0x03;
            return 1;
        }
    case 255:
        {
            /* MinKey */
            *(buffer_get_buffer(buffer) + type_byte) = 0xFF;
            return 1;
        }
    case 127:
        {
            /* MaxKey */
            *(buffer_get_buffer(buffer) + type_byte) = 0x7F;
            return 1;
        }
    }

    /* No _type_marker attibute or not one of our types. */

    if (PyBool_Check(value)) {
        const char c = (value == Py_True) ? 0x01 : 0x00;
        *(buffer_get_buffer(buffer) + type_byte) = 0x08;
        return buffer_write_bytes(buffer, &c, 1);
    }
#if PY_MAJOR_VERSION >= 3
    else if (PyLong_Check(value)) {
        const long long_value = PyLong_AsLong(value);
#else
    else if (PyInt_Check(value)) {
        const long long_value = PyInt_AsLong(value);
#endif

        const int int_value = (int)long_value;
        if (PyErr_Occurred() || long_value != int_value) { /* Overflow */
            long long long_long_value;
            PyErr_Clear();
            long_long_value = PyLong_AsLongLong(value);
            if (PyErr_Occurred()) { /* Overflow AGAIN */
                PyErr_SetString(PyExc_OverflowError,
                                "MongoDB can only handle up to 8-byte ints");
                return 0;
            }
            *(buffer_get_buffer(buffer) + type_byte) = 0x12;
            return buffer_write_int64(buffer, (int64_t)long_long_value);
        }
        *(buffer_get_buffer(buffer) + type_byte) = 0x10;
        return buffer_write_int32(buffer, (int32_t)int_value);
#if PY_MAJOR_VERSION < 3
    } else if (PyLong_Check(value)) {
        const long long long_long_value = PyLong_AsLongLong(value);
        if (PyErr_Occurred()) { /* Overflow */
            PyErr_SetString(PyExc_OverflowError,
                            "MongoDB can only handle up to 8-byte ints");
            return 0;
        }
        *(buffer_get_buffer(buffer) + type_byte) = 0x12;
        return buffer_write_int64(buffer, (int64_t)long_long_value);
#endif
    } else if (PyFloat_Check(value)) {
        const double d = PyFloat_AsDouble(value);
        *(buffer_get_buffer(buffer) + type_byte) = 0x01;
        return buffer_write_double(buffer, d);
    } else if (value == Py_None) {
        *(buffer_get_buffer(buffer) + type_byte) = 0x0A;
        return 1;
    } else if (PyDict_Check(value)) {
        *(buffer_get_buffer(buffer) + type_byte) = 0x03;
        return write_dict(self, buffer, value, check_keys, options, 0);
    } else if (PyList_Check(value) || PyTuple_Check(value)) {
        Py_ssize_t items, i;
        int start_position,
            length_location,
            length;
        char zero = 0;

        *(buffer_get_buffer(buffer) + type_byte) = 0x04;
        start_position = buffer_get_position(buffer);

        /* save space for length */
        length_location = buffer_save_space(buffer, 4);
        if (length_location == -1) {
            return 0;
        }

        if ((items = PySequence_Size(value)) > BSON_MAX_SIZE) {
            PyObject* BSONError = _error("BSONError");
            if (BSONError) {
                PyErr_SetString(BSONError,
                                "Too many items to serialize.");
                Py_DECREF(BSONError);
            }
            return 0;
        }
        for(i = 0; i < items; i++) {
            int list_type_byte = buffer_save_space(buffer, 1);
            char name[16];
            PyObject* item_value;

            if (list_type_byte == -1) {
                return 0;
            }
            INT2STRING(name, (int)i);
            if (!buffer_write_bytes(buffer, name, (int)strlen(name) + 1)) {
                return 0;
            }

            if (!(item_value = PySequence_GetItem(value, i)))
                return 0;
            if (!write_element_to_buffer(self, buffer, list_type_byte,
                                         item_value, check_keys, options,
                                         0, 0)) {
                Py_DECREF(item_value);
                return 0;
            }
            Py_DECREF(item_value);
        }

        /* write null byte and fill in length */
        if (!buffer_write_bytes(buffer, &zero, 1)) {
            return 0;
        }
        length = buffer_get_position(buffer) - start_position;
        buffer_write_int32_at_position(
            buffer, length_location, (int32_t)length);
        return 1;
#if PY_MAJOR_VERSION >= 3
    /* Python3 special case. Store bytes as BSON binary subtype 0. */
    } else if (PyBytes_Check(value)) {
        char subtype = 0;
        int size;
        const char* data = PyBytes_AS_STRING(value);
        if (!data)
            return 0;
        if ((size = _downcast_and_check(PyBytes_GET_SIZE(value), 0)) == -1)
            return 0;
        *(buffer_get_buffer(buffer) + type_byte) = 0x05;
        if (!buffer_write_int32(buffer, (int32_t)size)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, &subtype, 1)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, data, size)) {
            return 0;
        }
        return 1;
#else
    /* PyString_Check only works in Python 2.x. */
    } else if (PyString_Check(value)) {
        result_t status;
        const char* data;
        int size;
        if (!(data = PyString_AS_STRING(value)))
            return 0;
        if ((size = _downcast_and_check(PyString_GET_SIZE(value), 1)) == -1)
            return 0;
        *(buffer_get_buffer(buffer) + type_byte) = 0x02;
        status = check_string((const unsigned char*)data, size - 1, 1, 0);

        if (status == NOT_UTF_8) {
            PyObject* InvalidStringData = _error("InvalidStringData");
            if (InvalidStringData) {
                PyObject* repr = PyObject_Repr(value);
                char* repr_as_cstr = repr ? PyString_AsString(repr) : NULL;
                if (repr_as_cstr) {
                    PyObject *message = PyString_FromFormat(
                        "strings in documents must be valid UTF-8: %s",
                        repr_as_cstr);

                    if (message) {
                        PyErr_SetObject(InvalidStringData, message);
                        Py_DECREF(message);
                    }
                } else {
                    /* repr(value) failed, use a generic message. */
                    PyErr_SetString(
                        InvalidStringData,
                        "strings in documents must be valid UTF-8");
                }
                Py_XDECREF(repr);
                Py_DECREF(InvalidStringData);
            }
            return 0;
        }
        if (!buffer_write_int32(buffer, (int32_t)size)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, data, size)) {
            return 0;
        }
        return 1;
#endif
    } else if (PyUnicode_Check(value)) {
        *(buffer_get_buffer(buffer) + type_byte) = 0x02;
        return write_unicode(buffer, value);
    } else if (PyDateTime_Check(value)) {
        long long millis;
        PyObject* utcoffset = PyObject_CallMethod(value, "utcoffset", NULL);
        if (utcoffset == NULL)
            return 0;
        if (utcoffset != Py_None) {
            PyObject* result = PyNumber_Subtract(value, utcoffset);
            Py_DECREF(utcoffset);
            if (!result) {
                return 0;
            }
            millis = millis_from_datetime(result);
            Py_DECREF(result);
        } else {
            millis = millis_from_datetime(value);
        }
        *(buffer_get_buffer(buffer) + type_byte) = 0x09;
        return buffer_write_int64(buffer, (int64_t)millis);
    } else if (PyObject_TypeCheck(value, state->REType)) {
        return _write_regex_to_buffer(buffer, type_byte, value);
    }

    /*
     * Try Mapping and UUID last since we have to import
     * them if we're in a sub-interpreter.
     */
#if PY_MAJOR_VERSION >= 3
    mapping_type = _get_object(state->Mapping, "collections.abc", "Mapping");
#else
    mapping_type = _get_object(state->Mapping, "collections", "Mapping");
#endif
    if (mapping_type && PyObject_IsInstance(value, mapping_type)) {
        Py_DECREF(mapping_type);
        /* PyObject_IsInstance returns -1 on error */
        if (PyErr_Occurred()) {
            return 0;
        }
        *(buffer_get_buffer(buffer) + type_byte) = 0x03;
        return write_dict(self, buffer, value, check_keys, options, 0);
    }

    uuid_type = _get_object(state->UUID, "uuid", "UUID");
    if (uuid_type && PyObject_IsInstance(value, uuid_type)) {
        PyObject* binary_type = NULL;
        PyObject* binary_value = NULL;
        int result;

        Py_DECREF(uuid_type);
        /* PyObject_IsInstance returns -1 on error */
        if (PyErr_Occurred()) {
            return 0;
        }

        binary_type = _get_object(state->Binary, "bson", "Binary");
        if (binary_type == NULL) {
            return 0;
        }

        binary_value = PyObject_CallMethod(binary_type, "from_uuid", "(Oi)", value, options->uuid_rep);
        if (binary_value == NULL) {
            Py_DECREF(binary_type);
            return 0;
        }

        result = _write_element_to_buffer(self, buffer,
                                          type_byte, binary_value,
                                          check_keys, options,
                                          in_custom_call,
                                          in_fallback_call);
        Py_DECREF(binary_type);
        Py_DECREF(binary_value);
        return result;
    }
    Py_XDECREF(mapping_type);
    Py_XDECREF(uuid_type);

    /* Try a custom encoder if one is provided and we have not already
     * attempted to use a type encoder. */
    if (!in_custom_call && !options->type_registry.is_encoder_empty) {
        PyObject* value_type = NULL;
        PyObject* converter = NULL;
        value_type = PyObject_Type(value);
        if (value_type == NULL) {
            return 0;
        }
        converter = PyDict_GetItem(options->type_registry.encoder_map, value_type);
        Py_XDECREF(value_type);
        if (converter != NULL) {
            /* Transform types that have a registered converter.
             * A new reference is created upon transformation. */
            new_value = PyObject_CallFunctionObjArgs(converter, value, NULL);
            if (new_value == NULL) {
                return 0;
            }
            retval = write_element_to_buffer(self, buffer, type_byte, new_value,
                                             check_keys, options, 1, 0);
            Py_XDECREF(new_value);
            return retval;
        }
    }

    /* Try the fallback encoder if one is provided and we have not already
     * attempted to use the fallback encoder. */
    if (!in_fallback_call && options->type_registry.has_fallback_encoder) {
        new_value = PyObject_CallFunctionObjArgs(
            options->type_registry.fallback_encoder, value, NULL);
        if (new_value == NULL) {
            // propagate any exception raised by the callback
            return 0;
        }
        retval = write_element_to_buffer(self, buffer, type_byte, new_value,
                                         check_keys, options, 0, 1);
        Py_XDECREF(new_value);
        return retval;
    }

    /* We can't determine value's type. Fail. */
    _set_cannot_encode(value);
    return 0;
}

static int check_key_name(const char* name, int name_length) {

    if (name_length > 0 && name[0] == '$') {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
#if PY_MAJOR_VERSION >= 3
            PyObject* errmsg = PyUnicode_FromFormat(
                    "key '%s' must not start with '$'", name);
#else
            PyObject* errmsg = PyString_FromFormat(
                    "key '%s' must not start with '$'", name);
#endif
            if (errmsg) {
                PyErr_SetObject(InvalidDocument, errmsg);
                Py_DECREF(errmsg);
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }
    if (strchr(name, '.')) {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
#if PY_MAJOR_VERSION >= 3
            PyObject* errmsg = PyUnicode_FromFormat(
                    "key '%s' must not contain '.'", name);
#else
            PyObject* errmsg = PyString_FromFormat(
                    "key '%s' must not contain '.'", name);
#endif
            if (errmsg) {
                PyErr_SetObject(InvalidDocument, errmsg);
                Py_DECREF(errmsg);
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }
    return 1;
}

/* Write a (key, value) pair to the buffer.
 *
 * Returns 0 on failure */
int write_pair(PyObject* self, buffer_t buffer, const char* name, int name_length,
               PyObject* value, unsigned char check_keys,
               const codec_options_t* options, unsigned char allow_id) {
    int type_byte;

    /* Don't write any _id elements unless we're explicitly told to -
     * _id has to be written first so we do so, but don't bother
     * deleting it from the dictionary being written. */
    if (!allow_id && strcmp(name, "_id") == 0) {
        return 1;
    }

    type_byte = buffer_save_space(buffer, 1);
    if (type_byte == -1) {
        return 0;
    }
    if (check_keys && !check_key_name(name, name_length)) {
        return 0;
    }
    if (!buffer_write_bytes(buffer, name, name_length + 1)) {
        return 0;
    }
    if (!write_element_to_buffer(self, buffer, type_byte,
                                 value, check_keys, options, 0, 0)) {
        return 0;
    }
    return 1;
}

int decode_and_write_pair(PyObject* self, buffer_t buffer,
                          PyObject* key, PyObject* value,
                          unsigned char check_keys,
                          const codec_options_t* options,
                          unsigned char top_level) {
    PyObject* encoded;
    const char* data;
    int size;
    if (PyUnicode_Check(key)) {
        encoded = PyUnicode_AsUTF8String(key);
        if (!encoded) {
            return 0;
        }
#if PY_MAJOR_VERSION >= 3
        if (!(data = PyBytes_AS_STRING(encoded))) {
            Py_DECREF(encoded);
            return 0;
        }
        if ((size = _downcast_and_check(PyBytes_GET_SIZE(encoded), 1)) == -1) {
            Py_DECREF(encoded);
            return 0;
        }
#else
        if (!(data = PyString_AS_STRING(encoded))) {
            Py_DECREF(encoded);
            return 0;
        }
        if ((size = _downcast_and_check(PyString_GET_SIZE(encoded), 1)) == -1) {
            Py_DECREF(encoded);
            return 0;
        }
#endif
        if (strlen(data) != (size_t)(size - 1)) {
            PyObject* InvalidDocument = _error("InvalidDocument");
            if (InvalidDocument) {
                PyErr_SetString(InvalidDocument,
                                "Key names must not contain the NULL byte");
                Py_DECREF(InvalidDocument);
            }
            Py_DECREF(encoded);
            return 0;
        }
#if PY_MAJOR_VERSION < 3
    } else if (PyString_Check(key)) {
        result_t status;
        encoded = key;
        Py_INCREF(encoded);

        if (!(data = PyString_AS_STRING(encoded))) {
            Py_DECREF(encoded);
            return 0;
        }
        if ((size = _downcast_and_check(PyString_GET_SIZE(encoded), 1)) == -1) {
            Py_DECREF(encoded);
            return 0;
        }
        status = check_string((const unsigned char*)data, size - 1, 1, 1);

        if (status == NOT_UTF_8) {
            PyObject* InvalidStringData = _error("InvalidStringData");
            if (InvalidStringData) {
                PyErr_SetString(InvalidStringData,
                                "strings in documents must be valid UTF-8");
                Py_DECREF(InvalidStringData);
            }
            Py_DECREF(encoded);
            return 0;
        } else if (status == HAS_NULL) {
            PyObject* InvalidDocument = _error("InvalidDocument");
            if (InvalidDocument) {
                PyErr_SetString(InvalidDocument,
                                "Key names must not contain the NULL byte");
                Py_DECREF(InvalidDocument);
            }
            Py_DECREF(encoded);
            return 0;
        }
#endif
    } else {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyObject* repr = PyObject_Repr(key);
            if (repr) {
#if PY_MAJOR_VERSION >= 3
                PyObject* errmsg = PyUnicode_FromString(
                        "documents must have only string keys, key was ");
#else
                PyObject* errmsg = PyString_FromString(
                        "documents must have only string keys, key was ");
#endif
                if (errmsg) {
#if PY_MAJOR_VERSION >= 3
                    PyObject* error = PyUnicode_Concat(errmsg, repr);
                    if (error) {
                        PyErr_SetObject(InvalidDocument, error);
                        Py_DECREF(error);
                    }
                    Py_DECREF(errmsg);
                    Py_DECREF(repr);
#else
                    PyString_ConcatAndDel(&errmsg, repr);
                    if (errmsg) {
                        PyErr_SetObject(InvalidDocument, errmsg);
                        Py_DECREF(errmsg);
                    }
#endif
                } else {
                    Py_DECREF(repr);
                }
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }

    /* If top_level is True, don't allow writing _id here - it was already written. */
    if (!write_pair(self, buffer, data,
                    size - 1, value, check_keys, options, !top_level)) {
        Py_DECREF(encoded);
        return 0;
    }

    Py_DECREF(encoded);
    return 1;
}


/* Write a RawBSONDocument to the buffer.
 * Returns the number of bytes written or 0 on failure.
 */
static int write_raw_doc(buffer_t buffer, PyObject* raw) {
    char* bytes;
    Py_ssize_t len;
    int len_int;
    int bytes_written = 0;
    PyObject* bytes_obj = NULL;

    bytes_obj = PyObject_GetAttrString(raw, "raw");
    if (!bytes_obj) {
        goto fail;
    }

    if (-1 == PyBytes_AsStringAndSize(bytes_obj, &bytes, &len)) {
        goto fail;
    }
    len_int = _downcast_and_check(len, 0);
    if (-1 == len_int) {
        goto fail;
    }
    if (!buffer_write_bytes(buffer, bytes, len_int)) {
        goto fail;
    }
    bytes_written = len_int;
fail:
    Py_XDECREF(bytes_obj);
    return bytes_written;
}

/* returns the number of bytes written or 0 on failure */
int write_dict(PyObject* self, buffer_t buffer,
               PyObject* dict, unsigned char check_keys,
               const codec_options_t* options, unsigned char top_level) {
    PyObject* key;
    PyObject* iter;
    char zero = 0;
    int length;
    int length_location;
    struct module_state *state = GETSTATE(self);
    PyObject* mapping_type;
    long type_marker;

    /* check for RawBSONDocument */
    type_marker = _type_marker(dict);
    if (type_marker < 0) {
        return 0;
    }

    if (101 == type_marker) {
        return write_raw_doc(buffer, dict);
    }

#if PY_MAJOR_VERSION >= 3
    mapping_type = _get_object(state->Mapping, "collections.abc", "Mapping");
#else
    mapping_type = _get_object(state->Mapping, "collections", "Mapping");
#endif

    if (mapping_type) {
        if (!PyObject_IsInstance(dict, mapping_type)) {
            PyObject* repr;
            Py_DECREF(mapping_type);
            if ((repr = PyObject_Repr(dict))) {
#if PY_MAJOR_VERSION >= 3
                PyObject* errmsg = PyUnicode_FromString(
                    "encoder expected a mapping type but got: ");
                if (errmsg) {
                    PyObject* error = PyUnicode_Concat(errmsg, repr);
                    if (error) {
                        PyErr_SetObject(PyExc_TypeError, error);
                        Py_DECREF(error);
                    }
                    Py_DECREF(errmsg);
                    Py_DECREF(repr);
                }
#else
                PyObject* errmsg = PyString_FromString(
                    "encoder expected a mapping type but got: ");
                if (errmsg) {
                    PyString_ConcatAndDel(&errmsg, repr);
                    if (errmsg) {
                        PyErr_SetObject(PyExc_TypeError, errmsg);
                        Py_DECREF(errmsg);
                    }
                }
#endif
                else {
                    Py_DECREF(repr);
                }
            } else {
                PyErr_SetString(PyExc_TypeError,
                                "encoder expected a mapping type");
            }

            return 0;
        }
        Py_DECREF(mapping_type);
        /* PyObject_IsInstance returns -1 on error */
        if (PyErr_Occurred()) {
            return 0;
        }
    }

    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        return 0;
    }

    /* Write _id first if this is a top level doc. */
    if (top_level) {
        /*
         * If "dict" is a defaultdict we don't want to call
         * PyMapping_GetItemString on it. That would **create**
         * an _id where one didn't previously exist (PYTHON-871).
         */
        if (PyDict_Check(dict)) {
            /* PyDict_GetItemString returns a borrowed reference. */
            PyObject* _id = PyDict_GetItemString(dict, "_id");
            if (_id) {
                if (!write_pair(self, buffer, "_id", 3,
                                _id, check_keys, options, 1)) {
                    return 0;
                }
            }
        } else if (PyMapping_HasKeyString(dict, "_id")) {
            PyObject* _id = PyMapping_GetItemString(dict, "_id");
            if (!_id) {
                return 0;
            }
            if (!write_pair(self, buffer, "_id", 3,
                            _id, check_keys, options, 1)) {
                Py_DECREF(_id);
                return 0;
            }
            /* PyMapping_GetItemString returns a new reference. */
            Py_DECREF(_id);
        }
    }

    iter = PyObject_GetIter(dict);
    if (iter == NULL) {
        return 0;
    }
    while ((key = PyIter_Next(iter)) != NULL) {
        PyObject* value = PyObject_GetItem(dict, key);
        if (!value) {
            PyErr_SetObject(PyExc_KeyError, key);
            Py_DECREF(key);
            Py_DECREF(iter);
            return 0;
        }
        if (!decode_and_write_pair(self, buffer, key, value,
                                   check_keys, options, top_level)) {
            Py_DECREF(key);
            Py_DECREF(value);
            Py_DECREF(iter);
            return 0;
        }
        Py_DECREF(key);
        Py_DECREF(value);
    }
    Py_DECREF(iter);
    if (PyErr_Occurred()) {
        return 0;
    }

    /* write null byte and fill in length */
    if (!buffer_write_bytes(buffer, &zero, 1)) {
        return 0;
    }
    length = buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)length);
    return length;
}

static PyObject* _cbson_dict_to_bson(PyObject* self, PyObject* args) {
    PyObject* dict;
    PyObject* result;
    unsigned char check_keys;
    unsigned char top_level = 1;
    codec_options_t options;
    buffer_t buffer;
    PyObject* raw_bson_document_bytes_obj;
    long type_marker;

    if (!PyArg_ParseTuple(args, "ObO&|b", &dict, &check_keys,
                          convert_codec_options, &options, &top_level)) {
        return NULL;
    }

    /* check for RawBSONDocument */
    type_marker = _type_marker(dict);
    if (type_marker < 0) {
        destroy_codec_options(&options);
        return NULL;
    } else if (101 == type_marker) {
        destroy_codec_options(&options);
        raw_bson_document_bytes_obj = PyObject_GetAttrString(dict, "raw");
        if (NULL == raw_bson_document_bytes_obj) {
            return NULL;
        }
        return raw_bson_document_bytes_obj;
    }

    buffer = buffer_new();
    if (!buffer) {
        destroy_codec_options(&options);
        return NULL;
    }

    if (!write_dict(self, buffer, dict, check_keys, &options, top_level)) {
        destroy_codec_options(&options);
        buffer_free(buffer);
        return NULL;
    }

    /* objectify buffer */
    result = Py_BuildValue(BYTES_FORMAT_STRING, buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer));
    destroy_codec_options(&options);
    buffer_free(buffer);
    return result;
}

static PyObject* get_value(PyObject* self, PyObject* name, const char* buffer,
                           unsigned* position, unsigned char type,
                           unsigned max, const codec_options_t* options) {
    struct module_state *state = GETSTATE(self);
    PyObject* value = NULL;
    switch (type) {
    case 1:
        {
            double d;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&d, buffer + *position, 8);
            value = PyFloat_FromDouble(BSON_DOUBLE_FROM_LE(d));
            *position += 8;
            break;
        }
    case 2:
    case 14:
        {
            uint32_t value_length;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&value_length, buffer + *position, 4);
            value_length = BSON_UINT32_FROM_LE(value_length);
            /* Encoded string length + string */
            if (!value_length || max < value_length || max < 4 + value_length) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + value_length - 1]) {
                goto invalid;
            }
            value = PyUnicode_DecodeUTF8(
                buffer + *position, value_length - 1,
                options->unicode_decode_error_handler);
            if (!value) {
                goto invalid;
            }
            *position += value_length;
            break;
        }
    case 3:
        {
            PyObject* collection;
            uint32_t size;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&size, buffer + *position, 4);
            size = BSON_UINT32_FROM_LE(size);
            if (size < BSON_MIN_SIZE || max < size) {
                goto invalid;
            }
            /* Check for bad eoo */
            if (buffer[*position + size - 1]) {
                goto invalid;
            }

            if (options->is_raw_bson) {
                value = PyObject_CallFunction(
                    options->document_class, BYTES_FORMAT_STRING "O",
                    buffer + *position, (Py_ssize_t)size, options->options_obj);
                if (!value) {
                    goto invalid;
                }
                *position += size;
                break;
            }

            value = elements_to_dict(self, buffer + *position + 4,
                                     size - 5, options);
            if (!value) {
                goto invalid;
            }

            /* Decoding for DBRefs */
            if (PyMapping_HasKeyString(value, "$ref")) { /* DBRef */
                PyObject* dbref = NULL;
                PyObject* dbref_type;
                PyObject* id;
                PyObject* database;

                collection = PyMapping_GetItemString(value, "$ref");
                /* PyMapping_GetItemString returns NULL to indicate error. */
                if (!collection) {
                    goto invalid;
                }
                PyMapping_DelItemString(value, "$ref");

                if (PyMapping_HasKeyString(value, "$id")) {
                    id = PyMapping_GetItemString(value, "$id");
                    if (!id) {
                        Py_DECREF(collection);
                        goto invalid;
                    }
                    PyMapping_DelItemString(value, "$id");
                } else {
                    id = Py_None;
                    Py_INCREF(id);
                }

                if (PyMapping_HasKeyString(value, "$db")) {
                    database = PyMapping_GetItemString(value, "$db");
                    if (!database) {
                        Py_DECREF(collection);
                        Py_DECREF(id);
                        goto invalid;
                    }
                    PyMapping_DelItemString(value, "$db");
                } else {
                    database = Py_None;
                    Py_INCREF(database);
                }

                if ((dbref_type = _get_object(state->DBRef, "bson.dbref", "DBRef"))) {
                    dbref = PyObject_CallFunctionObjArgs(dbref_type, collection, id, database, value, NULL);
                    Py_DECREF(dbref_type);
                }
                Py_DECREF(value);
                value = dbref;

                Py_DECREF(id);
                Py_DECREF(collection);
                Py_DECREF(database);
            }

            *position += size;
            break;
        }
    case 4:
        {
            uint32_t size, end;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&size, buffer + *position, 4);
            size = BSON_UINT32_FROM_LE(size);
            if (size < BSON_MIN_SIZE || max < size) {
                goto invalid;
            }
            end = *position + size - 1;
            /* Check for bad eoo */
            if (buffer[end]) {
                goto invalid;
            }
            *position += 4;

            value = PyList_New(0);
            if (!value) {
                goto invalid;
            }
            while (*position < end) {
                PyObject* to_append;

                unsigned char bson_type = (unsigned char)buffer[(*position)++];

                size_t key_size = strlen(buffer + *position);
                if (max < key_size) {
                    Py_DECREF(value);
                    goto invalid;
                }
                /* just skip the key, they're in order. */
                *position += (unsigned)key_size + 1;
                if (Py_EnterRecursiveCall(" while decoding a list value")) {
                    Py_DECREF(value);
                    goto invalid;
                }
                to_append = get_value(self, name, buffer, position, bson_type,
                                      max - (unsigned)key_size, options);
                Py_LeaveRecursiveCall();
                if (!to_append) {
                    Py_DECREF(value);
                    goto invalid;
                }
                if (PyList_Append(value, to_append) < 0) {
                    Py_DECREF(value);
                    Py_DECREF(to_append);
                    goto invalid;
                }
                Py_DECREF(to_append);
            }
            if (*position != end) {
                goto invalid;
            }
            (*position)++;
            break;
        }
    case 5:
        {
            PyObject* data;
            PyObject* st;
            PyObject* type_to_create;
            uint32_t length, length2;
            unsigned char subtype;

            if (max < 5) {
                goto invalid;
            }
            memcpy(&length, buffer + *position, 4);
            length = BSON_UINT32_FROM_LE(length);
            if (max < length) {
                goto invalid;
            }

            subtype = (unsigned char)buffer[*position + 4];
            *position += 5;
            if (subtype == 2) {
                if (length < 4) {
                    goto invalid;
                }
                memcpy(&length2, buffer + *position, 4);
                length2 = BSON_UINT32_FROM_LE(length2);
                if (length2 != length - 4) {
                    goto invalid;
                }
            }
#if PY_MAJOR_VERSION >= 3
            /* Python3 special case. Decode BSON binary subtype 0 to bytes. */
            if (subtype == 0) {
                value = PyBytes_FromStringAndSize(buffer + *position, length);
                *position += length;
                break;
            }
            if (subtype == 2) {
                data = PyBytes_FromStringAndSize(buffer + *position + 4, length - 4);
            } else {
                data = PyBytes_FromStringAndSize(buffer + *position, length);
            }
#else
            if (subtype == 2) {
                data = PyString_FromStringAndSize(buffer + *position + 4, length - 4);
            } else {
                data = PyString_FromStringAndSize(buffer + *position, length);
            }
#endif
            if (!data) {
                goto invalid;
            }
            /* Encode as UUID or Binary based on options->uuid_rep
             * TODO: PYTHON-2245 Decoding should follow UUID spec in PyMongo 4.0  */
            if (subtype == 3 || subtype == 4) {
                PyObject* binary_type = NULL;
                PyObject* binary_value = NULL;
                char uuid_rep = options->uuid_rep;

                /* UUID should always be 16 bytes */
                if (length != 16) {
                    goto uuiderror;
                }

                binary_type = _get_object(state->Binary, "bson", "Binary");
                if (binary_type == NULL) {
                    goto uuiderror;
                }

                binary_value = PyObject_CallFunction(binary_type, "(Oi)", data, subtype);
                if (binary_value == NULL) {
                    goto uuiderror;
                }

                if (uuid_rep == UNSPECIFIED) {
                    value = binary_value;
                    Py_INCREF(value);
                } else {
                    if (subtype == 4) {
                        uuid_rep = STANDARD;
                    } else if (uuid_rep == STANDARD) {
                        uuid_rep = PYTHON_LEGACY;
                    }
                    value = PyObject_CallMethod(binary_value, "as_uuid", "(i)", uuid_rep);
                }

            uuiderror:
                Py_XDECREF(binary_type);
                Py_XDECREF(binary_value);
                Py_DECREF(data);
                if (!value) {
                    goto invalid;
                }
                *position += length;
                break;
            }

#if PY_MAJOR_VERSION >= 3
            st = PyLong_FromLong(subtype);
#else
            st = PyInt_FromLong(subtype);
#endif
            if (!st) {
                Py_DECREF(data);
                goto invalid;
            }
            if ((type_to_create = _get_object(state->Binary, "bson.binary", "Binary"))) {
                value = PyObject_CallFunctionObjArgs(type_to_create, data, st, NULL);
                Py_DECREF(type_to_create);
            }
            Py_DECREF(st);
            Py_DECREF(data);
            if (!value) {
                goto invalid;
            }
            *position += length;
            break;
        }
    case 6:
    case 10:
        {
            value = Py_None;
            Py_INCREF(value);
            break;
        }
    case 7:
        {
            PyObject* objectid_type;
            if (max < 12) {
                goto invalid;
            }
            if ((objectid_type = _get_object(state->ObjectId, "bson.objectid", "ObjectId"))) {
                value = PyObject_CallFunction(objectid_type, BYTES_FORMAT_STRING,
                                              buffer + *position, (Py_ssize_t)12);
                Py_DECREF(objectid_type);
            }
            *position += 12;
            break;
        }
    case 8:
        {
            char boolean_raw = buffer[(*position)++];
            if (0 == boolean_raw) {
                value = Py_False;
            } else if (1 == boolean_raw) {
                value = Py_True;
            } else {
                PyObject* InvalidBSON = _error("InvalidBSON");
                if (InvalidBSON) {
                    PyErr_Format(InvalidBSON, "invalid boolean value: %x", boolean_raw);
                    Py_DECREF(InvalidBSON);
                }
                return NULL;
            }
            Py_INCREF(value);
            break;
        }
    case 9:
        {
            PyObject* utc_type;
            PyObject* naive;
            PyObject* replace;
            PyObject* args;
            PyObject* kwargs;
            PyObject* astimezone;
            int64_t millis;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&millis, buffer + *position, 8);
            millis = (int64_t)BSON_UINT64_FROM_LE(millis);
            naive = datetime_from_millis(millis);
            *position += 8;
            if (!options->tz_aware) { /* In the naive case, we're done here. */
                value = naive;
                break;
            }

            if (!naive) {
                goto invalid;
            }
            replace = PyObject_GetAttrString(naive, "replace");
            Py_DECREF(naive);
            if (!replace) {
                goto invalid;
            }
            args = PyTuple_New(0);
            if (!args) {
                Py_DECREF(replace);
                goto invalid;
            }
            kwargs = PyDict_New();
            if (!kwargs) {
                Py_DECREF(replace);
                Py_DECREF(args);
                goto invalid;
            }
            utc_type = _get_object(state->UTC, "bson.tz_util", "utc");
            if (!utc_type || PyDict_SetItemString(kwargs, "tzinfo", utc_type) == -1) {
                Py_DECREF(replace);
                Py_DECREF(args);
                Py_DECREF(kwargs);
                Py_XDECREF(utc_type);
                goto invalid;
            }
            Py_XDECREF(utc_type);
            value = PyObject_Call(replace, args, kwargs);
            if (!value) {
                Py_DECREF(replace);
                Py_DECREF(args);
                Py_DECREF(kwargs);
                goto invalid;
            }

            /* convert to local time */
            if (options->tzinfo != Py_None) {
                astimezone = PyObject_GetAttrString(value, "astimezone");
                Py_DECREF(value);
                if (!astimezone) {
                    Py_DECREF(replace);
                    Py_DECREF(args);
                    Py_DECREF(kwargs);
                    goto invalid;
                }
                value = PyObject_CallFunctionObjArgs(astimezone, options->tzinfo, NULL);
                Py_DECREF(astimezone);
            }

            Py_DECREF(replace);
            Py_DECREF(args);
            Py_DECREF(kwargs);
            break;
        }
    case 11:
        {
            PyObject* regex_class;
            PyObject* pattern;
            int flags;
            size_t flags_length, i;
            size_t pattern_length = strlen(buffer + *position);
            if (pattern_length > BSON_MAX_SIZE || max < pattern_length) {
                goto invalid;
            }
            pattern = PyUnicode_DecodeUTF8(
                buffer + *position, pattern_length,
                options->unicode_decode_error_handler);
            if (!pattern) {
                goto invalid;
            }
            *position += (unsigned)pattern_length + 1;
            flags_length = strlen(buffer + *position);
            if (flags_length > BSON_MAX_SIZE ||
                    (BSON_MAX_SIZE - pattern_length) < flags_length) {
                Py_DECREF(pattern);
                goto invalid;
            }
            if (max < pattern_length + flags_length) {
                Py_DECREF(pattern);
                goto invalid;
            }
            flags = 0;
            for (i = 0; i < flags_length; i++) {
                if (buffer[*position + i] == 'i') {
                    flags |= 2;
                } else if (buffer[*position + i] == 'l') {
                    flags |= 4;
                } else if (buffer[*position + i] == 'm') {
                    flags |= 8;
                } else if (buffer[*position + i] == 's') {
                    flags |= 16;
                } else if (buffer[*position + i] == 'u') {
                    flags |= 32;
                } else if (buffer[*position + i] == 'x') {
                    flags |= 64;
                }
            }
            *position += (unsigned)flags_length + 1;

            regex_class = _get_object(state->Regex, "bson.regex", "Regex");
            if (regex_class) {
                value = PyObject_CallFunction(regex_class,
                                              "Oi", pattern, flags);
                Py_DECREF(regex_class);
            }
            Py_DECREF(pattern);
            break;
        }
    case 12:
        {
            uint32_t coll_length;
            PyObject* collection;
            PyObject* id = NULL;
            PyObject* objectid_type;
            PyObject* dbref_type;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&coll_length, buffer + *position, 4);
            coll_length = BSON_UINT32_FROM_LE(coll_length);
            /* Encoded string length + string + 12 byte ObjectId */
            if (!coll_length || max < coll_length || max < 4 + coll_length + 12) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + coll_length - 1]) {
                goto invalid;
            }

            collection = PyUnicode_DecodeUTF8(
                buffer + *position, coll_length - 1,
                options->unicode_decode_error_handler);
            if (!collection) {
                goto invalid;
            }
            *position += coll_length;

            if ((objectid_type = _get_object(state->ObjectId, "bson.objectid", "ObjectId"))) {
                id = PyObject_CallFunction(objectid_type, BYTES_FORMAT_STRING,
                                           buffer + *position, (Py_ssize_t)12);
                Py_DECREF(objectid_type);
            }
            if (!id) {
                Py_DECREF(collection);
                goto invalid;
            }
            *position += 12;
            if ((dbref_type = _get_object(state->DBRef, "bson.dbref", "DBRef"))) {
                value = PyObject_CallFunctionObjArgs(dbref_type, collection, id, NULL);
                Py_DECREF(dbref_type);
            }
            Py_DECREF(collection);
            Py_DECREF(id);
            break;
        }
    case 13:
        {
            PyObject* code;
            PyObject* code_type;
            uint32_t value_length;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&value_length, buffer + *position, 4);
            value_length = BSON_UINT32_FROM_LE(value_length);
            /* Encoded string length + string */
            if (!value_length || max < value_length || max < 4 + value_length) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + value_length - 1]) {
                goto invalid;
            }
            code = PyUnicode_DecodeUTF8(
                buffer + *position, value_length - 1,
                options->unicode_decode_error_handler);
            if (!code) {
                goto invalid;
            }
            *position += value_length;
            if ((code_type = _get_object(state->Code, "bson.code", "Code"))) {
                value = PyObject_CallFunctionObjArgs(code_type, code, NULL, NULL);
                Py_DECREF(code_type);
            }
            Py_DECREF(code);
            break;
        }
    case 15:
        {
            uint32_t c_w_s_size;
            uint32_t code_size;
            uint32_t scope_size;
            PyObject* code;
            PyObject* scope;
            PyObject* code_type;

            if (max < 8) {
                goto invalid;
            }

            memcpy(&c_w_s_size, buffer + *position, 4);
            c_w_s_size = BSON_UINT32_FROM_LE(c_w_s_size);
            *position += 4;

            if (max < c_w_s_size) {
                goto invalid;
            }

            memcpy(&code_size, buffer + *position, 4);
            code_size = BSON_UINT32_FROM_LE(code_size);
            /* code_w_scope length + code length + code + scope length */
            if (!code_size || max < code_size || max < 4 + 4 + code_size + 4) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + code_size - 1]) {
                goto invalid;
            }
            code = PyUnicode_DecodeUTF8(
                buffer + *position, code_size - 1,
                options->unicode_decode_error_handler);
            if (!code) {
                goto invalid;
            }
            *position += code_size;

            memcpy(&scope_size, buffer + *position, 4);
            scope_size = BSON_UINT32_FROM_LE(scope_size);
            if (scope_size < BSON_MIN_SIZE) {
                Py_DECREF(code);
                goto invalid;
            }
            /* code length + code + scope length + scope */
            if ((4 + code_size + 4 + scope_size) != c_w_s_size) {
                Py_DECREF(code);
                goto invalid;
            }

            /* Check for bad eoo */
            if (buffer[*position + scope_size - 1]) {
                goto invalid;
            }
            scope = elements_to_dict(self, buffer + *position + 4,
                                     scope_size - 5, options);
            if (!scope) {
                Py_DECREF(code);
                goto invalid;
            }
            *position += scope_size;

            if ((code_type = _get_object(state->Code, "bson.code", "Code"))) {
                value = PyObject_CallFunctionObjArgs(code_type, code, scope, NULL);
                Py_DECREF(code_type);
            }
            Py_DECREF(code);
            Py_DECREF(scope);
            break;
        }
    case 16:
        {
            int32_t i;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&i, buffer + *position, 4);
            i = (int32_t)BSON_UINT32_FROM_LE(i);
#if PY_MAJOR_VERSION >= 3
            value = PyLong_FromLong(i);
#else
            value = PyInt_FromLong(i);
#endif
            if (!value) {
                goto invalid;
            }
            *position += 4;
            break;
        }
    case 17:
        {
            uint32_t time, inc;
            PyObject* timestamp_type;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&inc, buffer + *position, 4);
            memcpy(&time, buffer + *position + 4, 4);
            inc = BSON_UINT32_FROM_LE(inc);
            time = BSON_UINT32_FROM_LE(time);
            if ((timestamp_type = _get_object(state->Timestamp, "bson.timestamp", "Timestamp"))) {
                value = PyObject_CallFunction(timestamp_type, "II", time, inc);
                Py_DECREF(timestamp_type);
            }
            *position += 8;
            break;
        }
    case 18:
        {
            int64_t ll;
            PyObject* bson_int64_type = _get_object(state->BSONInt64,
                                                    "bson.int64", "Int64");
            if (!bson_int64_type)
                goto invalid;
            if (max < 8) {
                Py_DECREF(bson_int64_type);
                goto invalid;
            }
            memcpy(&ll, buffer + *position, 8);
            ll = (int64_t)BSON_UINT64_FROM_LE(ll);
            value = PyObject_CallFunction(bson_int64_type, "L", ll);
            *position += 8;
            Py_DECREF(bson_int64_type);
            break;
        }
    case 19:
        {
            PyObject* dec128;
            if (max < 16) {
                goto invalid;
            }
            if ((dec128 = _get_object(state->Decimal128,
                                      "bson.decimal128",
                                      "Decimal128"))) {
                value = PyObject_CallMethod(dec128,
                                            "from_bid",
                                            BYTES_FORMAT_STRING,
                                            buffer + *position,
                                            (Py_ssize_t)16);
                Py_DECREF(dec128);
            }
            *position += 16;
            break;
        }
    case 255:
        {
            PyObject* minkey_type = _get_object(state->MinKey, "bson.min_key", "MinKey");
            if (!minkey_type)
                goto invalid;
            value = PyObject_CallFunctionObjArgs(minkey_type, NULL);
            Py_DECREF(minkey_type);
            break;
        }
    case 127:
        {
            PyObject* maxkey_type = _get_object(state->MaxKey, "bson.max_key", "MaxKey");
            if (!maxkey_type)
                goto invalid;
            value = PyObject_CallFunctionObjArgs(maxkey_type, NULL);
            Py_DECREF(maxkey_type);
            break;
        }
    default:
        {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyObject* bobj = PyBytes_FromFormat("%c", type);
                if (bobj) {
                    PyObject* repr = PyObject_Repr(bobj);
                    Py_DECREF(bobj);
                    /*
                     * See http://bugs.python.org/issue22023 for why we can't
                     * just use PyUnicode_FromFormat with %S or %R to do this
                     * work.
                     */
                    if (repr) {
                        PyObject* left = PyUnicode_FromString(
                            "Detected unknown BSON type ");
                        if (left) {
                            PyObject* lmsg = PyUnicode_Concat(left, repr);
                            Py_DECREF(left);
                            if (lmsg) {
                                PyObject* errmsg = PyUnicode_FromFormat(
                                    "%U for fieldname '%U'. Are you using the "
                                    "latest driver version?", lmsg, name);
                                if (errmsg) {
                                    PyErr_SetObject(InvalidBSON, errmsg);
                                    Py_DECREF(errmsg);
                                }
                                Py_DECREF(lmsg);
                            }
                        }
                        Py_DECREF(repr);
                    }
                }
                Py_DECREF(InvalidBSON);
            }
            goto invalid;
        }
    }

    if (value) {
        if (!options->type_registry.is_decoder_empty) {
            PyObject* value_type = NULL;
            PyObject* converter = NULL;
            value_type = PyObject_Type(value);
            if (value_type == NULL) {
                goto invalid;
            }
            converter = PyDict_GetItem(options->type_registry.decoder_map, value_type);
            if (converter != NULL) {
                PyObject* new_value = PyObject_CallFunctionObjArgs(converter, value, NULL);
                Py_DECREF(value_type);
                Py_DECREF(value);
                return new_value;
            } else {
                Py_DECREF(value_type);
                return value;
            }
        }
        return value;
    }

    invalid:

    /*
     * Wrap any non-InvalidBSON errors in InvalidBSON.
     */
    if (PyErr_Occurred()) {
        PyObject *etype, *evalue, *etrace;
        PyObject *InvalidBSON;

        /*
         * Calling _error clears the error state, so fetch it first.
         */
        PyErr_Fetch(&etype, &evalue, &etrace);

        /* Dont reraise anything but PyExc_Exceptions as InvalidBSON. */
        if (PyErr_GivenExceptionMatches(etype, PyExc_Exception)) {
            InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                if (!PyErr_GivenExceptionMatches(etype, InvalidBSON)) {
                    /*
                     * Raise InvalidBSON(str(e)).
                     */
                    Py_DECREF(etype);
                    etype = InvalidBSON;

                    if (evalue) {
                        PyObject *msg = PyObject_Str(evalue);
                        Py_DECREF(evalue);
                        evalue = msg;
                    }
                    PyErr_NormalizeException(&etype, &evalue, &etrace);
                } else {
                    /*
                     * The current exception matches InvalidBSON, so we don't
                     * need this reference after all.
                     */
                    Py_DECREF(InvalidBSON);
                }
            }
        }
        /* Steals references to args. */
        PyErr_Restore(etype, evalue, etrace);
    } else {
        PyObject *InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "invalid length or type code");
            Py_DECREF(InvalidBSON);
        }
    }
    return NULL;
}

/*
 * Get the next 'name' and 'value' from a document in a string, whose position
 * is provided.
 *
 * Returns the position of the next element in the document, or -1 on error.
 */
static int _element_to_dict(PyObject* self, const char* string,
                            unsigned position, unsigned max,
                            const codec_options_t* options,
                            PyObject** name, PyObject** value) {
    unsigned char type = (unsigned char)string[position++];
    size_t name_length = strlen(string + position);
    if (name_length > BSON_MAX_SIZE || position + name_length >= max) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "field name too large");
            Py_DECREF(InvalidBSON);
        }
        return -1;
    }
    *name = PyUnicode_DecodeUTF8(
        string + position, name_length,
        options->unicode_decode_error_handler);
    if (!*name) {
        /* If NULL is returned then wrap the UnicodeDecodeError
           in an InvalidBSON error */
        PyObject *etype, *evalue, *etrace;
        PyObject *InvalidBSON;

        PyErr_Fetch(&etype, &evalue, &etrace);
        if (PyErr_GivenExceptionMatches(etype, PyExc_Exception)) {
            InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                Py_DECREF(etype);
                etype = InvalidBSON;

                if (evalue) {
                    PyObject *msg = PyObject_Str(evalue);
                    Py_DECREF(evalue);
                    evalue = msg;
                }
                PyErr_NormalizeException(&etype, &evalue, &etrace);
            }
        }
        PyErr_Restore(etype, evalue, etrace);
        return -1;
    }
    position += (unsigned)name_length + 1;
    *value = get_value(self, *name, string, &position, type,
                       max - position, options);
    if (!*value) {
        Py_DECREF(*name);
        return -1;
    }
    return position;
}

static PyObject* _cbson_element_to_dict(PyObject* self, PyObject* args) {
    /* TODO: Support buffer protocol */
    char* string;
    PyObject* bson;
    codec_options_t options;
    unsigned position;
    unsigned max;
    int new_position;
    PyObject* name;
    PyObject* value;
    PyObject* result_tuple;

    if (!PyArg_ParseTuple(args, "OIIO&", &bson, &position, &max,
                          convert_codec_options, &options)) {
        return NULL;
    }

#if PY_MAJOR_VERSION >= 3
    if (!PyBytes_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to _element_to_dict must be a bytes object");
#else
    if (!PyString_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to _element_to_dict must be a string");
#endif
        return NULL;
    }
#if PY_MAJOR_VERSION >= 3
    string = PyBytes_AS_STRING(bson);
#else
    string = PyString_AS_STRING(bson);
#endif

    new_position = _element_to_dict(self, string, position, max, &options,
                                    &name, &value);
    if (new_position < 0) {
        return NULL;
    }

    result_tuple = Py_BuildValue("NNi", name, value, new_position);
    if (!result_tuple) {
        Py_DECREF(name);
        Py_DECREF(value);
        return NULL;
    }

    destroy_codec_options(&options);
    return result_tuple;
}

static PyObject* _elements_to_dict(PyObject* self, const char* string,
                                   unsigned max,
                                   const codec_options_t* options) {
    unsigned position = 0;
    PyObject* dict = PyObject_CallObject(options->document_class, NULL);
    if (!dict) {
        return NULL;
    }
    while (position < max) {
        PyObject* name = NULL;
        PyObject* value = NULL;
        int new_position;

        new_position = _element_to_dict(
            self, string, position, max, options, &name, &value);
        if (new_position < 0) {
            Py_DECREF(dict);
            return NULL;
        } else {
            position = (unsigned)new_position;
        }

        PyObject_SetItem(dict, name, value);
        Py_DECREF(name);
        Py_DECREF(value);
    }
    return dict;
}

static PyObject* elements_to_dict(PyObject* self, const char* string,
                                  unsigned max,
                                  const codec_options_t* options) {
    PyObject* result;
    if (Py_EnterRecursiveCall(" while decoding a BSON document"))
        return NULL;
    result = _elements_to_dict(self, string, max, options);
    Py_LeaveRecursiveCall();
    return result;
}

static int _get_buffer(PyObject *exporter, Py_buffer *view) {
    if (PyObject_GetBuffer(exporter, view, PyBUF_SIMPLE) == -1) {
        return 0;
    }
    if (!PyBuffer_IsContiguous(view, 'C')) {
        PyErr_SetString(PyExc_ValueError,
                        "must be a contiguous buffer");
        goto fail;
    }
    if (!view->buf || view->len < 0) {
        PyErr_SetString(PyExc_ValueError, "invalid buffer");
        goto fail;
    }
    if (view->itemsize != 1) {
        PyErr_SetString(PyExc_ValueError,
                        "buffer data must be ascii or utf8");
        goto fail;
    }
    return 1;
fail:
    PyBuffer_Release(view);
    return 0;
}

static PyObject* _cbson_bson_to_dict(PyObject* self, PyObject* args) {
    int32_t size;
    Py_ssize_t total_size;
    const char* string;
    PyObject* bson;
    codec_options_t options;
    PyObject* result = NULL;
    PyObject* options_obj;
    Py_buffer view;

    if (! (PyArg_ParseTuple(args, "OO", &bson, &options_obj) &&
            convert_codec_options(options_obj, &options))) {
        return result;
    }

    if (!_get_buffer(bson, &view)) {
        destroy_codec_options(&options);
        return result;
    }

    total_size = view.len;

    if (total_size < BSON_MIN_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON,
                            "not enough data for a BSON document");
            Py_DECREF(InvalidBSON);
        }
        goto done;;
    }

    string = (char*)view.buf;

    memcpy(&size, string, 4);
    size = (int32_t)BSON_UINT32_FROM_LE(size);
    if (size < BSON_MIN_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "invalid message size");
            Py_DECREF(InvalidBSON);
        }
        goto done;
    }

    if (total_size < size || total_size > BSON_MAX_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "objsize too large");
            Py_DECREF(InvalidBSON);
        }
        goto done;
    }

    if (size != total_size || string[size - 1]) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "bad eoo");
            Py_DECREF(InvalidBSON);
        }
        goto done;
    }

    /* No need to decode fields if using RawBSONDocument */
    if (options.is_raw_bson) {
        result = PyObject_CallFunction(
            options.document_class, BYTES_FORMAT_STRING "O", string, (Py_ssize_t)size,
            options_obj);
    }
    else {
        result = elements_to_dict(self, string + 4, (unsigned)size - 5, &options);
    }
done:
    PyBuffer_Release(&view);
    destroy_codec_options(&options);
    return result;
}

static PyObject* _cbson_decode_all(PyObject* self, PyObject* args) {
    int32_t size;
    Py_ssize_t total_size;
    const char* string;
    PyObject* bson;
    PyObject* dict;
    PyObject* result = NULL;
    codec_options_t options;
    PyObject* options_obj = NULL;
    Py_buffer view = {0};

    if (!PyArg_ParseTuple(args, "OO", &bson, &options_obj)) {
        return NULL;
    }
    if (!convert_codec_options(options_obj, &options)) {
        return NULL;
    }

    if (!_get_buffer(bson, &view)) {
        destroy_codec_options(&options);
        return NULL;
    }
    total_size = view.len;
    string = (char*)view.buf;

    if (!(result = PyList_New(0))) {
        goto fail;
    }

    while (total_size > 0) {
        if (total_size < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON,
                                "not enough data for a BSON document");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        memcpy(&size, string, 4);
        size = (int32_t)BSON_UINT32_FROM_LE(size);
        if (size < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "invalid message size");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        if (total_size < size) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "objsize too large");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        if (string[size - 1]) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "bad eoo");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        /* No need to decode fields if using RawBSONDocument. */
        if (options.is_raw_bson) {
            dict = PyObject_CallFunction(
                options.document_class, BYTES_FORMAT_STRING "O", string, (Py_ssize_t)size,
                options_obj);
        } else {
            dict = elements_to_dict(self, string + 4, (unsigned)size - 5, &options);
        }
        if (!dict) {
            Py_DECREF(result);
            goto fail;
        }
        if (PyList_Append(result, dict) < 0) {
            Py_DECREF(dict);
            Py_DECREF(result);
            goto fail;
        }
        Py_DECREF(dict);
        string += size;
        total_size -= size;
    }
    goto done;
fail:
    result = NULL;
done:
    PyBuffer_Release(&view);
    destroy_codec_options(&options);
    return result;
}

static PyMethodDef _CBSONMethods[] = {
    {"_dict_to_bson", _cbson_dict_to_bson, METH_VARARGS,
     "convert a dictionary to a string containing its BSON representation."},
    {"_bson_to_dict", _cbson_bson_to_dict, METH_VARARGS,
     "convert a BSON string to a SON object."},
    {"_decode_all", _cbson_decode_all, METH_VARARGS,
     "convert binary data to a sequence of documents."},
    {"_element_to_dict", _cbson_element_to_dict, METH_VARARGS,
     "Decode a single key, value pair."},
    {NULL, NULL, 0, NULL}
};

#if PY_MAJOR_VERSION >= 3
#define INITERROR return NULL
static int _cbson_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->Binary);
    Py_VISIT(GETSTATE(m)->Code);
    Py_VISIT(GETSTATE(m)->ObjectId);
    Py_VISIT(GETSTATE(m)->DBRef);
    Py_VISIT(GETSTATE(m)->Regex);
    Py_VISIT(GETSTATE(m)->UUID);
    Py_VISIT(GETSTATE(m)->Timestamp);
    Py_VISIT(GETSTATE(m)->MinKey);
    Py_VISIT(GETSTATE(m)->MaxKey);
    Py_VISIT(GETSTATE(m)->UTC);
    Py_VISIT(GETSTATE(m)->REType);
    return 0;
}

static int _cbson_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->Binary);
    Py_CLEAR(GETSTATE(m)->Code);
    Py_CLEAR(GETSTATE(m)->ObjectId);
    Py_CLEAR(GETSTATE(m)->DBRef);
    Py_CLEAR(GETSTATE(m)->Regex);
    Py_CLEAR(GETSTATE(m)->UUID);
    Py_CLEAR(GETSTATE(m)->Timestamp);
    Py_CLEAR(GETSTATE(m)->MinKey);
    Py_CLEAR(GETSTATE(m)->MaxKey);
    Py_CLEAR(GETSTATE(m)->UTC);
    Py_CLEAR(GETSTATE(m)->REType);
    return 0;
}

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_cbson",
    NULL,
    sizeof(struct module_state),
    _CBSONMethods,
    NULL,
    _cbson_traverse,
    _cbson_clear,
    NULL
};

PyMODINIT_FUNC
PyInit__cbson(void)
#else
#define INITERROR return
PyMODINIT_FUNC
init_cbson(void)
#endif
{
    PyObject *m;
    PyObject *c_api_object;
    static void *_cbson_API[_cbson_API_POINTER_COUNT];

    PyDateTime_IMPORT;
    if (PyDateTimeAPI == NULL) {
        INITERROR;
    }

    /* Export C API */
    _cbson_API[_cbson_buffer_write_bytes_INDEX] = (void *) buffer_write_bytes;
    _cbson_API[_cbson_write_dict_INDEX] = (void *) write_dict;
    _cbson_API[_cbson_write_pair_INDEX] = (void *) write_pair;
    _cbson_API[_cbson_decode_and_write_pair_INDEX] = (void *) decode_and_write_pair;
    _cbson_API[_cbson_convert_codec_options_INDEX] = (void *) convert_codec_options;
    _cbson_API[_cbson_destroy_codec_options_INDEX] = (void *) destroy_codec_options;
    _cbson_API[_cbson_buffer_write_double_INDEX] = (void *) buffer_write_double;
    _cbson_API[_cbson_buffer_write_int32_INDEX] = (void *) buffer_write_int32;
    _cbson_API[_cbson_buffer_write_int64_INDEX] = (void *) buffer_write_int64;
    _cbson_API[_cbson_buffer_write_int32_at_position_INDEX] =
        (void *) buffer_write_int32_at_position;
    _cbson_API[_cbson_downcast_and_check_INDEX] = (void *) _downcast_and_check;

#if PY_VERSION_HEX >= 0x03010000
    /* PyCapsule is new in python 3.1 */
    c_api_object = PyCapsule_New((void *) _cbson_API, "_cbson._C_API", NULL);
#else
    c_api_object = PyCObject_FromVoidPtr((void *) _cbson_API, NULL);
#endif
    if (c_api_object == NULL)
        INITERROR;

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_cbson", _CBSONMethods);
#endif
    if (m == NULL) {
        Py_DECREF(c_api_object);
        INITERROR;
    }

    /* Import several python objects */
    if (_load_python_objects(m)) {
        Py_DECREF(c_api_object);
#if PY_MAJOR_VERSION >= 3
        Py_DECREF(m);
#endif
        INITERROR;
    }

    if (PyModule_AddObject(m, "_C_API", c_api_object) < 0) {
        Py_DECREF(c_api_object);
#if PY_MAJOR_VERSION >= 3
        Py_DECREF(m);
#endif
        INITERROR;
    }

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
