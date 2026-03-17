/**
 * CPython 3 C extension.
 */

#include <Python.h>
#include <stdbool.h>
#include "bitstream.h"

#include <stdio.h>

struct field_info_t;

typedef void (*pack_field_t)(struct bitstream_writer_t *self_p,
                             PyObject *value_p,
                             struct field_info_t *field_info_p);

typedef PyObject *(*unpack_field_t)(struct bitstream_reader_t *self_p,
                                    struct field_info_t *field_info_p);

struct field_info_t {
    pack_field_t pack;
    unpack_field_t unpack;
    int number_of_bits;
    bool is_padding;
    union {
        struct {
            int64_t lower;
            int64_t upper;
        } s;
        struct {
            uint64_t upper;
        } u;
    } limits;
};

struct info_t {
    int number_of_bits;
    int number_of_fields;
    int number_of_non_padding_fields;
    struct field_info_t fields[1];
};

struct compiled_format_t {
    PyObject_HEAD
    struct info_t *info_p;
    PyObject *format_p;
};

struct compiled_format_dict_t {
    PyObject_HEAD
    struct info_t *info_p;
    PyObject *format_p;
    PyObject *names_p;
};

static const char* pickle_version_key = "_pickle_version";
static int pickle_version = 1;

static PyObject *compiled_format_new(PyTypeObject *type_p,
                                     PyObject *args_p,
                                     PyObject *kwargs_p);

static int compiled_format_init(struct compiled_format_t *self_p,
                                PyObject *args_p,
                                PyObject *kwargs_p);

static int compiled_format_init_inner(struct compiled_format_t *self_p,
                                      PyObject *format_p);

static void compiled_format_dealloc(struct compiled_format_t *self_p);

static PyObject *m_compiled_format_pack(struct compiled_format_t *self_p,
                                        PyObject *args_p);

static PyObject *m_compiled_format_unpack(struct compiled_format_t *self_p,
                                          PyObject *args_p,
                                          PyObject *kwargs_p);

static PyObject *m_compiled_format_pack_into(struct compiled_format_t *self_p,
                                             PyObject *args_p,
                                             PyObject *kwargs_p);

static PyObject *m_compiled_format_unpack_from(struct compiled_format_t *self_p,
                                               PyObject *args_p,
                                               PyObject *kwargs_p);

static PyObject *m_compiled_format_calcsize(struct compiled_format_t *self_p);

static PyObject *m_compiled_format_copy(struct compiled_format_t *self_p);

static PyObject *m_compiled_format_deepcopy(struct compiled_format_t *self_p,
                                            PyObject *args_p);

static PyObject *m_compiled_format_getstate(struct compiled_format_t *self_p,
                                            PyObject *args_p);

static PyObject *m_compiled_format_setstate(struct compiled_format_t *self_p,
                                            PyObject *args_p);

static PyObject *compiled_format_dict_new(PyTypeObject *type_p,
                                          PyObject *args_p,
                                          PyObject *kwargs_p);

static int compiled_format_dict_init(struct compiled_format_dict_t *self_p,
                                     PyObject *args_p,
                                     PyObject *kwargs_p);

static int compiled_format_dict_init_inner(struct compiled_format_dict_t *self_p,
                                           PyObject *format_p,
                                           PyObject *names_p);

static void compiled_format_dict_dealloc(struct compiled_format_dict_t *self_p);

static PyObject *m_compiled_format_dict_pack(struct compiled_format_dict_t *self_p,
                                             PyObject *data_p);

static PyObject *m_compiled_format_dict_unpack(
    struct compiled_format_dict_t *self_p,
    PyObject *args_p,
    PyObject *kwargs_p);

static PyObject *m_compiled_format_dict_pack_into(
    struct compiled_format_dict_t *self_p,
    PyObject *args_p,
    PyObject *kwargs_p);

static PyObject *m_compiled_format_dict_unpack_from(
    struct compiled_format_dict_t *self_p,
    PyObject *args_p,
    PyObject *kwargs_p);

static PyObject *m_compiled_format_dict_calcsize(
    struct compiled_format_dict_t *self_p);

static PyObject *m_compiled_format_dict_copy(
    struct compiled_format_dict_t *self_p);

static PyObject *m_compiled_format_dict_deepcopy(
    struct compiled_format_dict_t *self_p,
    PyObject *args_p);

static PyObject *m_compiled_format_dict_getstate(struct compiled_format_dict_t *self_p,
                                                 PyObject *args_p);

static PyObject *m_compiled_format_dict_setstate(struct compiled_format_dict_t *self_p,
                                                 PyObject *args_p);

PyDoc_STRVAR(pack___doc__,
             "pack(fmt, *args)\n"
             "--\n"
             "\n");
PyDoc_STRVAR(compiled_format_pack___doc__,
             "pack(*args)\n"
             "--\n"
             "\n");

PyDoc_STRVAR(unpack___doc__,
             "unpack(fmt, data, allow_truncated=False)\n"
             "--\n"
             "\n");
PyDoc_STRVAR(compiled_format_unpack___doc__,
             "unpack(data, allow_truncated=False)\n"
             "--\n"
             "\n");

PyDoc_STRVAR(pack_into___doc__,
             "pack_into(fmt, buf, offset, *args, **kwargs)\n"
             "--\n"
             "\n");
PyDoc_STRVAR(compiled_format_pack_into___doc__,
             "pack_into(buf, offset, *args, **kwargs)\n"
             "--\n"
             "\n");

PyDoc_STRVAR(unpack_from___doc__,
             "unpack_from(fmt, data, offset=0, allow_truncated=False)\n"
             "--\n"
             "\n");
PyDoc_STRVAR(compiled_format_unpack_from___doc__,
             "unpack_from(data, offset=0, allow_truncated=False)\n"
             "--\n"
             "\n");

PyDoc_STRVAR(calcsize___doc__,
             "calcsize(fmt)\n"
             "--\n"
             "\n");
PyDoc_STRVAR(compiled_format_calcsize___doc__,
             "calcsize()\n"
             "--\n"
             "\n");

static PyObject *py_zero_p = NULL;

static struct PyMethodDef compiled_format_methods[] = {
    {
        "pack",
        (PyCFunction)m_compiled_format_pack,
        METH_VARARGS,
        compiled_format_pack___doc__
    },
    {
        "unpack",
        (PyCFunction)m_compiled_format_unpack,
        METH_VARARGS | METH_KEYWORDS,
        compiled_format_unpack___doc__
    },
    {
        "pack_into",
        (PyCFunction)m_compiled_format_pack_into,
        METH_VARARGS | METH_KEYWORDS,
        compiled_format_pack_into___doc__
    },
    {
        "unpack_from",
        (PyCFunction)m_compiled_format_unpack_from,
        METH_VARARGS | METH_KEYWORDS,
        compiled_format_unpack_from___doc__
    },
    {
        "calcsize",
        (PyCFunction)m_compiled_format_calcsize,
        METH_NOARGS,
        compiled_format_calcsize___doc__
    },
    {
        "__copy__",
        (PyCFunction)m_compiled_format_copy,
        METH_NOARGS
    },
    {
        "__deepcopy__",
        (PyCFunction)m_compiled_format_deepcopy,
        METH_VARARGS
    },
    {
        "__getstate__",
        (PyCFunction)m_compiled_format_getstate,
        METH_NOARGS
    },
    {
        "__setstate__",
        (PyCFunction)m_compiled_format_setstate,
        METH_O
    },
    { NULL }
};

static PyTypeObject compiled_format_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "bitstruct.c.CompiledFormat",
    .tp_doc = NULL,
    .tp_basicsize = sizeof(struct compiled_format_t),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = compiled_format_new,
    .tp_init = (initproc)compiled_format_init,
    .tp_dealloc = (destructor)compiled_format_dealloc,
    .tp_methods = compiled_format_methods,
};

static struct PyMethodDef compiled_format_dict_methods[] = {
    {
        "pack",
        (PyCFunction)m_compiled_format_dict_pack,
        METH_O,
        pack___doc__
    },
    {
        "unpack",
        (PyCFunction)m_compiled_format_dict_unpack,
        METH_VARARGS | METH_KEYWORDS,
        unpack___doc__
    },
    {
        "pack_into",
        (PyCFunction)m_compiled_format_dict_pack_into,
        METH_VARARGS | METH_KEYWORDS,
        pack_into___doc__
    },
    {
        "unpack_from",
        (PyCFunction)m_compiled_format_dict_unpack_from,
        METH_VARARGS | METH_KEYWORDS,
        unpack_from___doc__
    },
    {
        "calcsize",
        (PyCFunction)m_compiled_format_dict_calcsize,
        METH_NOARGS,
        calcsize___doc__
    },
    {
        "__copy__",
        (PyCFunction)m_compiled_format_dict_copy,
        METH_NOARGS
    },
    {
        "__deepcopy__",
        (PyCFunction)m_compiled_format_dict_deepcopy,
        METH_VARARGS
    },
    {
        "__getstate__",
        (PyCFunction)m_compiled_format_dict_getstate,
        METH_NOARGS
    },
    {
        "__setstate__",
        (PyCFunction)m_compiled_format_dict_setstate,
        METH_O
    },
    { NULL }
};

static PyTypeObject compiled_format_dict_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "bitstruct.c.CompiledFormatDict",
    .tp_doc = NULL,
    .tp_basicsize = sizeof(struct compiled_format_dict_t),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = compiled_format_dict_new,
    .tp_init = (initproc)compiled_format_dict_init,
    .tp_dealloc = (destructor)compiled_format_dict_dealloc,
    .tp_methods = compiled_format_dict_methods,
};

static bool is_names_list(PyObject *names_p)
{
    if (!PyList_Check(names_p)) {
        PyErr_SetString(PyExc_TypeError, "Names is not a list.");

        return (false);
    }

    return (true);
}

static void pack_signed_integer(struct bitstream_writer_t *self_p,
                                PyObject *value_p,
                                struct field_info_t *field_info_p)
{
    int64_t value;
    int64_t lower;
    int64_t upper;

    value = PyLong_AsLongLong(value_p);

    if ((value == -1) && PyErr_Occurred()) {
        return;
    }

    if (field_info_p->number_of_bits < 64) {
        lower = field_info_p->limits.s.lower;
        upper = field_info_p->limits.s.upper;

        if ((value < lower) || (value > upper)) {
            PyErr_Format(PyExc_OverflowError,
                         "Signed integer value %lld out of range.",
                         (long long)value);
        }

        value &= ((1ull << field_info_p->number_of_bits) - 1);
    }

    bitstream_writer_write_u64_bits(self_p,
                                    (uint64_t)value,
                                    field_info_p->number_of_bits);
}

static PyObject *unpack_signed_integer(struct bitstream_reader_t *self_p,
                                       struct field_info_t *field_info_p)
{
    uint64_t value;
    uint64_t sign_bit;

    value = bitstream_reader_read_u64_bits(self_p, field_info_p->number_of_bits);
    sign_bit = (1ull << (field_info_p->number_of_bits - 1));

    if (value & sign_bit) {
        value |= ~(((sign_bit) << 1) - 1);
    }

    return (PyLong_FromLongLong((long long)value));
}

static void pack_unsigned_integer(struct bitstream_writer_t *self_p,
                                  PyObject *value_p,
                                  struct field_info_t *field_info_p)
{
    uint64_t value;

    value = PyLong_AsUnsignedLongLong(value_p);

    if ((value == (uint64_t)-1) && PyErr_Occurred()) {
        return;
    }

    if (value > field_info_p->limits.u.upper) {
        PyErr_Format(PyExc_OverflowError,
                     "Unsigned integer value %llu out of range.",
                     (unsigned long long)value);
    }

    bitstream_writer_write_u64_bits(self_p,
                                    value,
                                    field_info_p->number_of_bits);
}

static PyObject *unpack_unsigned_integer(struct bitstream_reader_t *self_p,
                                         struct field_info_t *field_info_p)
{
    uint64_t value;

    value = bitstream_reader_read_u64_bits(self_p,
                                           field_info_p->number_of_bits);

    return (PyLong_FromUnsignedLongLong(value));
}

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 6

static void pack_float_16(struct bitstream_writer_t *self_p,
                          PyObject *value_p,
                          struct field_info_t *field_info_p)
{
    uint8_t buf[2];

#if PY_VERSION_HEX >= 0x030B00A7
    PyFloat_Pack2(PyFloat_AsDouble(value_p),
                  (char*)&buf[0],
                  0);
#else
    _PyFloat_Pack2(PyFloat_AsDouble(value_p),
                   &buf[0],
                   0);
#endif
    bitstream_writer_write_bytes(self_p, &buf[0], sizeof(buf));
}

static PyObject *unpack_float_16(struct bitstream_reader_t *self_p,
                                 struct field_info_t *field_info_p)
{
    uint8_t buf[2];
    double value;

    bitstream_reader_read_bytes(self_p, &buf[0], sizeof(buf));
#if PY_VERSION_HEX >= 0x030B00A7
    value = PyFloat_Unpack2((const char*)&buf[0], 0);
#else
    value = _PyFloat_Unpack2(&buf[0], 0);
#endif

    return (PyFloat_FromDouble(value));
}

#endif

static void pack_float_32(struct bitstream_writer_t *self_p,
                          PyObject *value_p,
                          struct field_info_t *field_info_p)
{
    float value;
    uint32_t data;

    value = (float)PyFloat_AsDouble(value_p);
    memcpy(&data, &value, sizeof(data));
    bitstream_writer_write_u32(self_p, data);
}

static PyObject *unpack_float_32(struct bitstream_reader_t *self_p,
                                 struct field_info_t *field_info_p)
{
    float value;
    uint32_t data;

    data = bitstream_reader_read_u32(self_p);
    memcpy(&value, &data, sizeof(value));

    return (PyFloat_FromDouble(value));
}

static void pack_float_64(struct bitstream_writer_t *self_p,
                          PyObject *value_p,
                          struct field_info_t *field_info_p)
{
    double value;
    uint64_t data;

    value = PyFloat_AsDouble(value_p);
    memcpy(&data, &value, sizeof(data));
    bitstream_writer_write_u64_bits(self_p,
                                    data,
                                    field_info_p->number_of_bits);
}

static PyObject *unpack_float_64(struct bitstream_reader_t *self_p,
                                 struct field_info_t *field_info_p)
{
    double value;
    uint64_t data;

    data = bitstream_reader_read_u64(self_p);
    memcpy(&value, &data, sizeof(value));

    return (PyFloat_FromDouble(value));
}

static void pack_bool(struct bitstream_writer_t *self_p,
                      PyObject *value_p,
                      struct field_info_t *field_info_p)
{
    bitstream_writer_write_u64_bits(self_p,
                                    PyObject_IsTrue(value_p),
                                    field_info_p->number_of_bits);
}

static PyObject *unpack_bool(struct bitstream_reader_t *self_p,
                             struct field_info_t *field_info_p)
{
    return (PyBool_FromLong((long)bitstream_reader_read_u64_bits(
                                self_p,
                                field_info_p->number_of_bits)));
}

static void pack_text(struct bitstream_writer_t *self_p,
                      PyObject *value_p,
                      struct field_info_t *field_info_p)
{
    Py_ssize_t size;
    const char* buf_p;

    buf_p = PyUnicode_AsUTF8AndSize(value_p, &size);

    if (buf_p != NULL) {
        if (size < (field_info_p->number_of_bits / 8)) {
            PyErr_SetString(PyExc_NotImplementedError, "Short text.");
        } else {
            bitstream_writer_write_bytes(self_p,
                                         (uint8_t *)buf_p,
                                         field_info_p->number_of_bits / 8);
        }
    }
}

static PyObject *unpack_text(struct bitstream_reader_t *self_p,
                             struct field_info_t *field_info_p)
{
    uint8_t *buf_p;
    PyObject *value_p;
    int number_of_bytes;

    number_of_bytes = (field_info_p->number_of_bits / 8);
    buf_p = PyMem_RawMalloc(number_of_bytes);

    if (buf_p == NULL) {
        return (NULL);
    }

    bitstream_reader_read_bytes(self_p, buf_p, number_of_bytes);
    value_p = PyUnicode_FromStringAndSize((const char *)buf_p, number_of_bytes);
    PyMem_RawFree(buf_p);

    return (value_p);
}

static void pack_raw(struct bitstream_writer_t *self_p,
                     PyObject *value_p,
                     struct field_info_t *field_info_p)
{
    Py_ssize_t size;
    char* buf_p;
    int res;

    res = PyBytes_AsStringAndSize(value_p, &buf_p, &size);

    if (res != -1) {
        if (size < (field_info_p->number_of_bits / 8)) {
            PyErr_SetString(PyExc_NotImplementedError, "Short raw data.");
        } else {
            bitstream_writer_write_bytes(self_p,
                                         (uint8_t *)buf_p,
                                         field_info_p->number_of_bits / 8);
        }
    }
}

static PyObject *unpack_raw(struct bitstream_reader_t *self_p,
                            struct field_info_t *field_info_p)
{
    uint8_t *buf_p;
    PyObject *value_p;
    int number_of_bytes;

    number_of_bytes = (field_info_p->number_of_bits / 8);
    value_p = PyBytes_FromStringAndSize(NULL, number_of_bytes);
    buf_p = (uint8_t *)PyBytes_AS_STRING(value_p);
    bitstream_reader_read_bytes(self_p, buf_p, number_of_bytes);

    return (value_p);
}

static void pack_zero_padding(struct bitstream_writer_t *self_p,
                              PyObject *value_p,
                              struct field_info_t *field_info_p)
{
    bitstream_writer_write_repeated_bit(self_p,
                                        0,
                                        field_info_p->number_of_bits);
}

static void pack_one_padding(struct bitstream_writer_t *self_p,
                             PyObject *value_p,
                             struct field_info_t *field_info_p)
{
    bitstream_writer_write_repeated_bit(self_p,
                                        1,
                                        field_info_p->number_of_bits);
}

static PyObject *unpack_padding(struct bitstream_reader_t *self_p,
                                struct field_info_t *field_info_p)
{
    bitstream_reader_seek(self_p, field_info_p->number_of_bits);

    return (NULL);
}

static int field_info_init_signed(struct field_info_t *self_p,
                                  int number_of_bits)
{
    uint64_t limit;

    self_p->pack = pack_signed_integer;
    self_p->unpack = unpack_signed_integer;

    if (number_of_bits > 64) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "Signed integer over 64 bits.");
        return (-1);
    }

    limit = (1ull << (number_of_bits - 1));
    self_p->limits.s.lower = -limit;
    self_p->limits.s.upper = (limit - 1);

    return (0);
}

static int field_info_init_unsigned(struct field_info_t *self_p,
                                    int number_of_bits)
{
    self_p->pack = pack_unsigned_integer;
    self_p->unpack = unpack_unsigned_integer;

    if (number_of_bits > 64) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "Unsigned integer over 64 bits.");
        return (-1);
    }

    if (number_of_bits < 64) {
        self_p->limits.u.upper = ((1ull << number_of_bits) - 1);
    } else {
        self_p->limits.u.upper = (uint64_t)-1;
    }

    return (0);
}

static int field_info_init_float(struct field_info_t *self_p,
                                 int number_of_bits)
{
    switch (number_of_bits) {

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 6
    case 16:
        self_p->pack = pack_float_16;
        self_p->unpack = unpack_float_16;
        break;
#endif

    case 32:
        self_p->pack = pack_float_32;
        self_p->unpack = unpack_float_32;
        break;

    case 64:
        self_p->pack = pack_float_64;
        self_p->unpack = unpack_float_64;
        break;

    default:
#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 6
        PyErr_SetString(PyExc_NotImplementedError,
                        "Float not 16, 32 or 64 bits.");
#else
        PyErr_SetString(PyExc_NotImplementedError, "Float not 32 or 64 bits.");
#endif
        return (-1);
    }

    return (0);
}

static int field_info_init_bool(struct field_info_t *self_p,
                                int number_of_bits)
{
    self_p->pack = pack_bool;
    self_p->unpack = unpack_bool;

    if (number_of_bits > 64) {
        PyErr_SetString(PyExc_NotImplementedError, "Bool over 64 bits.");
        return (-1);
    }

    return (0);
}

static int field_info_init_text(struct field_info_t *self_p,
                                int number_of_bits)
{
    self_p->pack = pack_text;
    self_p->unpack = unpack_text;

    if ((number_of_bits % 8) != 0) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "Text not multiple of 8 bits.");
        return (-1);
    }

    return (0);
}

static int field_info_init_raw(struct field_info_t *self_p,
                               int number_of_bits)
{
    self_p->pack = pack_raw;
    self_p->unpack = unpack_raw;

    if ((number_of_bits % 8) != 0) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "Raw not multiple of 8 bits.");
        return (-1);
    }

    return (0);
}

static int field_info_init_zero_padding(struct field_info_t *self_p)
{
    self_p->pack = pack_zero_padding;
    self_p->unpack = unpack_padding;

    return (0);
}

static int field_info_init_one_padding(struct field_info_t *self_p)
{
    self_p->pack = pack_one_padding;
    self_p->unpack = unpack_padding;

    return (0);
}

static int field_info_init(struct field_info_t *self_p,
                           int kind,
                           int number_of_bits)
{
    int res;
    bool is_padding;

    is_padding = false;

    switch (kind) {

    case 's':
        res = field_info_init_signed(self_p, number_of_bits);
        break;

    case 'u':
        res = field_info_init_unsigned(self_p, number_of_bits);
        break;

    case 'f':
        res = field_info_init_float(self_p, number_of_bits);
        break;

    case 'b':
        res = field_info_init_bool(self_p, number_of_bits);
        break;

    case 't':
        res = field_info_init_text(self_p, number_of_bits);
        break;

    case 'r':
        res = field_info_init_raw(self_p, number_of_bits);
        break;

    case 'p':
        is_padding = true;
        res = field_info_init_zero_padding(self_p);
        break;

    case 'P':
        is_padding = true;
        res = field_info_init_one_padding(self_p);
        break;

    default:
        PyErr_Format(PyExc_ValueError, "Bad format field type '%c'.", kind);
        res = -1;
        break;
    }

    self_p->number_of_bits = number_of_bits;
    self_p->is_padding = is_padding;

    return (res);
}

static int count_number_of_fields(const char *format_p,
                                  int *number_of_padding_fields_p)
{
    int count;

    count = 0;
    *number_of_padding_fields_p = 0;

    while (*format_p != '\0') {
        if ((*format_p >= 'A') && (*format_p <= 'z')) {
            count++;

            if ((*format_p == 'p') || (*format_p == 'P')) {
                (*number_of_padding_fields_p)++;
            }
        }

        format_p++;
    }

    return (count);
}

const char *parse_field(const char *format_p,
                        int *kind_p,
                        int *number_of_bits_p)
{
    while (isspace(*format_p)) {
        format_p++;
    }

    *kind_p = *format_p;
    *number_of_bits_p = 0;
    format_p++;

    while (isdigit(*format_p)) {
        if (*number_of_bits_p > (INT_MAX / 100)) {
            PyErr_SetString(PyExc_ValueError, "Field too long.");

            return (NULL);
        }

        *number_of_bits_p *= 10;
        *number_of_bits_p += (*format_p - '0');
        format_p++;
    }

    if (*number_of_bits_p == 0) {
        PyErr_SetString(PyExc_ValueError, "Field of size 0.");
        format_p = NULL;
    }

    return (format_p);
}

static struct info_t *parse_format(PyObject *format_obj_p)
{
    int number_of_fields;
    struct info_t *info_p;
    const char *format_p;
    int i;
    int kind;
    int number_of_bits;
    int number_of_padding_fields;
    int res;

    format_p = PyUnicode_AsUTF8(format_obj_p);

    if (format_p == NULL) {
        return (NULL);
    }

    number_of_fields = count_number_of_fields(format_p,
                                              &number_of_padding_fields);

    info_p = PyMem_RawMalloc(
        sizeof(*info_p) + number_of_fields * sizeof(info_p->fields[0]));

    if (info_p == NULL) {
        return (NULL);
    }

    info_p->number_of_bits = 0;
    info_p->number_of_fields = number_of_fields;
    info_p->number_of_non_padding_fields = (
        number_of_fields - number_of_padding_fields);

    for (i = 0; i < info_p->number_of_fields; i++) {
        format_p = parse_field(format_p, &kind, &number_of_bits);

        if (format_p == NULL) {
            PyMem_RawFree(info_p);

            return (NULL);
        }

        res = field_info_init(&info_p->fields[i], kind, number_of_bits);

        if (res != 0) {
            PyMem_RawFree(info_p);

            return (NULL);
        }

        info_p->number_of_bits += number_of_bits;
    }

    return (info_p);
}

static void pack_pack(struct info_t *info_p,
                      PyObject *args_p,
                      int consumed_args,
                      struct bitstream_writer_t *writer_p)
{
    PyObject *value_p;
    int i;
    struct field_info_t *field_p;

    for (i = 0; i < info_p->number_of_fields; i++) {
        field_p = &info_p->fields[i];

        if (field_p->is_padding) {
            value_p = NULL;
        } else {
            value_p = PyTuple_GET_ITEM(args_p, consumed_args);
            consumed_args++;
        }

        info_p->fields[i].pack(writer_p, value_p, field_p);
    }
}

static PyObject *pack_prepare(struct info_t *info_p,
                              struct bitstream_writer_t *writer_p)
{
    PyObject *packed_p;

    packed_p = PyBytes_FromStringAndSize(NULL, (info_p->number_of_bits + 7) / 8);

    if (packed_p == NULL) {
        return (NULL);
    }

    bitstream_writer_init(writer_p, (uint8_t *)PyBytes_AS_STRING(packed_p));

    return (packed_p);
}

static PyObject *pack_finalize(PyObject *packed_p)
{
    if (PyErr_Occurred() != NULL) {
        Py_DECREF(packed_p);
        packed_p = NULL;
    }

    return (packed_p);
}

static PyObject *pack(struct info_t *info_p,
                      PyObject *args_p,
                      int consumed_args,
                      Py_ssize_t number_of_args)
{
    struct bitstream_writer_t writer;
    PyObject *packed_p;

    if (number_of_args < info_p->number_of_non_padding_fields) {
        PyErr_SetString(PyExc_ValueError, "Too few arguments.");

        return (NULL);
    }

    packed_p = pack_prepare(info_p, &writer);

    if (packed_p == NULL) {
        return (NULL);
    }

    pack_pack(info_p, args_p, consumed_args, &writer);

    return (pack_finalize(packed_p));
}

static PyObject *m_pack(PyObject *module_p, PyObject *args_p)
{
    Py_ssize_t number_of_args;
    PyObject *packed_p;
    struct info_t *info_p;

    number_of_args = PyTuple_GET_SIZE(args_p);

    if (number_of_args < 1) {
        PyErr_SetString(PyExc_ValueError, "No format string.");

        return (NULL);
    }

    info_p = parse_format(PyTuple_GET_ITEM(args_p, 0));

    if (info_p == NULL) {
        return (NULL);
    }

    packed_p = pack(info_p, args_p, 1, number_of_args - 1);
    PyMem_RawFree(info_p);

    return (packed_p);
}

static PyObject *unpack(struct info_t *info_p,
                        PyObject *data_p,
                        long offset,
                        PyObject *allow_truncated_p)
{
    struct bitstream_reader_t reader;
    PyObject *unpacked_p = NULL;
    PyObject *value_p;
    Py_buffer view = {NULL, NULL};
    int i;
    int tmp;
    int produced_args;
    int res;
    int allow_truncated;
    int num_result_fields;

    res = PyObject_GetBuffer(data_p, &view, PyBUF_C_CONTIGUOUS);
    if (res == -1) {
        return (NULL);
    }

    allow_truncated = PyObject_IsTrue(allow_truncated_p);

    if (allow_truncated) {
        num_result_fields = 0;
        tmp = 0;
        for (i = 0; i < info_p->number_of_fields; i++) {
            if (view.len*8 < tmp + info_p->fields[i].number_of_bits) {
                break;
            }

            tmp += info_p->fields[i].number_of_bits;

            if (!info_p->fields[i].is_padding) {
                ++num_result_fields;
            }
        }
    }
    else {
        num_result_fields = info_p->number_of_non_padding_fields;

        if (view.len < ((info_p->number_of_bits + offset + 7) / 8)) {
            PyErr_SetString(PyExc_ValueError, "Short data.");
            goto exit;
        }
    }

    unpacked_p = PyTuple_New(num_result_fields);

    if (unpacked_p == NULL) {
        goto exit;
    }

    bitstream_reader_init(&reader, (uint8_t *)view.buf);
    bitstream_reader_seek(&reader, offset);
    produced_args = 0;

    for (i = 0; i < info_p->number_of_fields; i++) {
        if (produced_args == num_result_fields) {
            break;
        }

        value_p = info_p->fields[i].unpack(&reader, &info_p->fields[i]);

        if (value_p != NULL) {
            PyTuple_SET_ITEM(unpacked_p, produced_args, value_p);
            produced_args++;
        }
    }

    /*
 out1:
    if (PyErr_Occurred() != NULL) {
        Py_DECREF(unpacked_p);
        unpacked_p = NULL;
    }
    */
exit:
    PyBuffer_Release(&view);
    return (unpacked_p);
}

static PyObject *m_unpack(PyObject *module_p,
                          PyObject *args_p,
                          PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *data_p;
    PyObject *unpacked_p;
    PyObject *allow_truncated_p;
    struct info_t *info_p;
    int res;
    static char *keywords[] = {
        "fmt",
        "data",
        "allow_truncated",
        NULL
    };

    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OO|O",
                                      &keywords[0],
                                      &format_p,
                                      &data_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    unpacked_p = unpack(info_p, data_p, 0, allow_truncated_p);
    PyMem_RawFree(info_p);

    return (unpacked_p);
}

static long parse_offset(PyObject *offset_p)
{
    unsigned long offset;

    offset = PyLong_AsUnsignedLong(offset_p);

    if (offset == (unsigned long)-1) {
        return (-1);
    }

    if (offset > 0x7fffffff) {
        PyErr_Format(PyExc_ValueError,
                     "Offset must be less or equal to %d bits.",
                     0x7fffffff);

        return (-1);
    }

    return (offset);
}

static int pack_into_prepare(struct info_t *info_p,
                             PyObject *buf_p,
                             PyObject *offset_p,
                             struct bitstream_writer_t *writer_p,
                             struct bitstream_writer_bounds_t *bounds_p)
{
    uint8_t *packed_p;
    Py_ssize_t size;
    long offset;

    offset = parse_offset(offset_p);

    if (offset == -1) {
        return (-1);
    }

    if (!PyByteArray_Check(buf_p)) {
        PyErr_SetString(PyExc_TypeError, "Bytearray needed.");

        return (-1);
    }

    packed_p = (uint8_t *)PyByteArray_AsString(buf_p);

    if (packed_p == NULL) {
        return (-1);
    }

    size = PyByteArray_GET_SIZE(buf_p);

    if (size < ((info_p->number_of_bits + offset + 7) / 8)) {
        PyErr_Format(PyExc_ValueError,
                     "pack_into requires a buffer of at least %ld bits",
                     info_p->number_of_bits + offset);

        return (-1);
    }

    bitstream_writer_init(writer_p, packed_p);
    bitstream_writer_bounds_save(bounds_p,
                                 writer_p,
                                 offset,
                                 info_p->number_of_bits);
    bitstream_writer_seek(writer_p, offset);

    return (0);
}

static PyObject *pack_into_finalize(struct bitstream_writer_bounds_t *bounds_p)
{
    bitstream_writer_bounds_restore(bounds_p);

    if (PyErr_Occurred() != NULL) {
        return (NULL);
    }

    Py_RETURN_NONE;
}

static PyObject *pack_into(struct info_t *info_p,
                           PyObject *buf_p,
                           PyObject *offset_p,
                           PyObject *args_p,
                           Py_ssize_t consumed_args,
                           Py_ssize_t number_of_args)
{
    struct bitstream_writer_t writer;
    struct bitstream_writer_bounds_t bounds;
    int res;

    if ((number_of_args - consumed_args) < info_p->number_of_non_padding_fields) {
        PyErr_SetString(PyExc_ValueError, "Too few arguments.");

        return (NULL);
    }

    res = pack_into_prepare(info_p, buf_p, offset_p, &writer, &bounds);

    if (res != 0) {
        return (NULL);
    }

    pack_pack(info_p, args_p, consumed_args, &writer);

    return (pack_into_finalize(&bounds));
}

static PyObject *m_pack_into(PyObject *module_p,
                             PyObject *args_p,
                             PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *buf_p;
    PyObject *offset_p;
    PyObject *res_p;
    Py_ssize_t number_of_args;
    struct info_t *info_p;

    number_of_args = PyTuple_GET_SIZE(args_p);

    if (number_of_args < 3) {
        PyErr_SetString(PyExc_ValueError, "Too few arguments.");

        return (NULL);
    }

    format_p = PyTuple_GET_ITEM(args_p, 0);
    buf_p = PyTuple_GET_ITEM(args_p, 1);
    offset_p = PyTuple_GET_ITEM(args_p, 2);
    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    res_p = pack_into(info_p,
                      buf_p,
                      offset_p,
                      args_p,
                      3,
                      number_of_args);
    PyMem_RawFree(info_p);

    return (res_p);
}

static PyObject *unpack_from(struct info_t *info_p,
                             PyObject *data_p,
                             PyObject *offset_p,
                             PyObject *allow_truncated_p)
{
    long offset;

    offset = parse_offset(offset_p);

    if (offset == -1) {
        return (NULL);
    }

    return (unpack(info_p, data_p, offset, allow_truncated_p));
}

static PyObject *m_unpack_from(PyObject *module_p,
                               PyObject *args_p,
                               PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *data_p;
    PyObject *offset_p;
    PyObject *unpacked_p;
    PyObject *allow_truncated_p;
    struct info_t *info_p;
    int res;
    static char *keywords[] = {
        "fmt",
        "data",
        "offset",
        "allow_truncated",
        NULL
    };

    offset_p = py_zero_p;
    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OO|OO",
                                      &keywords[0],
                                      &format_p,
                                      &data_p,
                                      &offset_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    unpacked_p = unpack_from(info_p, data_p, offset_p, allow_truncated_p);
    PyMem_RawFree(info_p);

    return (unpacked_p);
}

static void pack_dict_pack(struct info_t *info_p,
                           PyObject *names_p,
                           PyObject *data_p,
                           struct bitstream_writer_t *writer_p)
{
    PyObject *value_p;
    int i;
    int consumed_args;
    struct field_info_t *field_p;

    consumed_args = 0;

    for (i = 0; i < info_p->number_of_fields; i++) {
        field_p = &info_p->fields[i];

        if (field_p->is_padding) {
            value_p = NULL;
        } else {
            value_p = PyDict_GetItem(data_p,
                                     PyList_GET_ITEM(names_p, consumed_args));
            consumed_args++;

            if (value_p == NULL) {
                PyErr_SetString(PyExc_KeyError, "Missing value.");
                break;
            }
        }

        info_p->fields[i].pack(writer_p, value_p, field_p);
    }
}

static PyObject *pack_dict(struct info_t *info_p,
                           PyObject *names_p,
                           PyObject *data_p)
{
    struct bitstream_writer_t writer;
    PyObject *packed_p;

    if (PyList_GET_SIZE(names_p) < info_p->number_of_non_padding_fields) {
        PyErr_SetString(PyExc_ValueError, "Too few names.");

        return (NULL);
    }

    packed_p = pack_prepare(info_p, &writer);

    if (packed_p == NULL) {
        return (NULL);
    }

    pack_dict_pack(info_p, names_p, data_p, &writer);

    return (pack_finalize(packed_p));
}

PyDoc_STRVAR(pack_dict___doc__,
             "pack_dict(fmt, names, data)\n"
             "--\n"
             "\n");

static PyObject *m_pack_dict(PyObject *module_p, PyObject *args_p)
{
    PyObject *format_p;
    PyObject *names_p;
    PyObject *data_p;
    PyObject *packed_p;
    struct info_t *info_p;
    int res;

    res = PyArg_ParseTuple(args_p, "OOO", &format_p, &names_p, &data_p);

    if (res == 0) {
        return (NULL);
    }

    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    if (!is_names_list(names_p)) {
        return (NULL);
    }

    packed_p = pack_dict(info_p, names_p, data_p);
    PyMem_RawFree(info_p);

    return (packed_p);
}

static PyObject *unpack_dict(struct info_t *info_p,
                             PyObject *names_p,
                             PyObject *data_p,
                             long offset,
                             PyObject *allow_truncated_p)
{
    struct bitstream_reader_t reader;
    PyObject *unpacked_p;
    PyObject *value_p;
    Py_buffer view = {NULL, NULL};
    int i;
    int res;
    int produced_args;
    int allow_truncated;

    if (PyList_GET_SIZE(names_p) < info_p->number_of_non_padding_fields) {
        PyErr_SetString(PyExc_ValueError, "Too few names.");

        return (NULL);
    }

    unpacked_p = PyDict_New();

    if (unpacked_p == NULL) {
        return (NULL);
    }

    res = PyObject_GetBuffer(data_p, &view, PyBUF_C_CONTIGUOUS);

    if (res == -1) {
        goto out1;
    }

    allow_truncated = PyObject_IsTrue(allow_truncated_p);

    if (!allow_truncated && view.len < ((info_p->number_of_bits + offset + 7) / 8)) {
        PyErr_SetString(PyExc_ValueError, "Short data.");

        goto out1;
    }

    bitstream_reader_init(&reader, (uint8_t *)view.buf);
    bitstream_reader_seek(&reader, offset);
    produced_args = 0;

    for (i = 0; i < info_p->number_of_fields; i++) {
        if (view.len*8 < reader.bit_offset + info_p->fields[i].number_of_bits)
            break;

        value_p = info_p->fields[i].unpack(&reader, &info_p->fields[i]);

        if (value_p != NULL) {
            PyDict_SetItem(unpacked_p,
                           PyList_GET_ITEM(names_p, produced_args),
                           value_p);
            Py_DECREF(value_p);
            produced_args++;
        }
    }

 out1:
    if (PyErr_Occurred() != NULL) {
        Py_DECREF(unpacked_p);
        unpacked_p = NULL;
    }

    if (view.obj != NULL) {
        PyBuffer_Release(&view);
    }

    return (unpacked_p);
}

PyDoc_STRVAR(unpack_dict___doc__,
             "unpack_dict(fmt, names, data, allow_truncated=False)\n"
             "--\n"
             "\n");

static PyObject *m_unpack_dict(PyObject *module_p,
                               PyObject *args_p,
                               PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *names_p;
    PyObject *data_p;
    PyObject *allow_truncated_p;
    PyObject *unpacked_p;
    struct info_t *info_p;
    int res;
    static char *keywords[] = {
        "fmt",
        "names",
        "data",
        "allow_truncated",
        NULL
    };

    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OOO|O",
                                      &keywords[0],
                                      &format_p,
                                      &names_p,
                                      &data_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    if (!is_names_list(names_p)) {
        return (NULL);
    }

    unpacked_p = unpack_dict(info_p, names_p, data_p, 0, allow_truncated_p);
    PyMem_RawFree(info_p);

    return (unpacked_p);
}

static PyObject *unpack_from_dict(struct info_t *info_p,
                                  PyObject *names_p,
                                  PyObject *data_p,
                                  PyObject *offset_p,
                                  PyObject *allow_truncated_p)
{
    long offset;

    offset = parse_offset(offset_p);

    if (offset == -1) {
        return (NULL);
    }

    return (unpack_dict(info_p, names_p, data_p, offset, allow_truncated_p));
}

static PyObject *pack_into_dict(struct info_t *info_p,
                                PyObject *names_p,
                                PyObject *buf_p,
                                PyObject *offset_p,
                                PyObject *data_p)
{
    struct bitstream_writer_t writer;
    struct bitstream_writer_bounds_t bounds;
    int res;

    res = pack_into_prepare(info_p, buf_p, offset_p, &writer, &bounds);

    if (res != 0) {
        return (NULL);
    }

    pack_dict_pack(info_p, names_p, data_p, &writer);

    return (pack_into_finalize(&bounds));
}

PyDoc_STRVAR(pack_into_dict___doc__,
             "pack_into_dict(fmt, names, buf, offset, data, **kwargs)\n"
             "--\n"
             "\n");

static PyObject *m_pack_into_dict(PyObject *module_p,
                                  PyObject *args_p,
                                  PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *names_p;
    PyObject *buf_p;
    PyObject *offset_p;
    PyObject *data_p;
    PyObject *res_p;
    struct info_t *info_p;
    int res;
    static char *keywords[] = {
        "fmt",
        "names",
        "buf",
        "offset",
        "data",
        NULL
    };

    offset_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OOOOO",
                                      &keywords[0],
                                      &format_p,
                                      &names_p,
                                      &buf_p,
                                      &offset_p,
                                      &data_p);

    if (res == 0) {
        return (NULL);
    }

    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    if (!is_names_list(names_p)) {
        return (NULL);
    }

    res_p = pack_into_dict(info_p, names_p, buf_p, offset_p, data_p);
    PyMem_RawFree(info_p);

    return (res_p);
}

PyDoc_STRVAR(unpack_from_dict___doc__,
             "unpack_from_dict(fmt, names, data, offset=0, allow_truncated=False)\n"
             "--\n"
             "\n");

static PyObject *m_unpack_from_dict(PyObject *module_p,
                                    PyObject *args_p,
                                    PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *names_p;
    PyObject *data_p;
    PyObject *offset_p;
    PyObject *allow_truncated_p;
    PyObject *unpacked_p;
    struct info_t *info_p;
    int res;
    static char *keywords[] = {
        "fmt",
        "names",
        "data",
        "offset",
        "allow_truncated",
        NULL
    };

    offset_p = py_zero_p;
    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OOO|OO",
                                      &keywords[0],
                                      &format_p,
                                      &names_p,
                                      &data_p,
                                      &offset_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    if (!is_names_list(names_p)) {
        return (NULL);
    }

    unpacked_p = unpack_from_dict(info_p, names_p, data_p, offset_p, allow_truncated_p);
    PyMem_RawFree(info_p);

    return (unpacked_p);
}

static PyObject *calcsize(struct info_t *info_p)
{
    return (PyLong_FromLong(info_p->number_of_bits));
}

static PyObject *m_calcsize(PyObject *module_p, PyObject *format_p)
{
    PyObject *size_p;
    struct info_t *info_p;

    info_p = parse_format(format_p);

    if (info_p == NULL) {
        return (NULL);
    }

    size_p = calcsize(info_p);
    PyMem_RawFree(info_p);

    return (size_p);
}

PyDoc_STRVAR(byteswap___doc__,
             "byteswap(fmt, data, offset=0)\n"
             "--\n"
             "\n");

static PyObject *m_byteswap(PyObject *module_p,
                            PyObject *args_p,
                            PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *data_p;
    PyObject *swapped_p;
    const char *c_format_p;
    uint8_t *src_p;
    uint8_t *dst_p;
    Py_ssize_t size;
    int res;
    int offset;

    static char *keywords[] = {
        "fmt",
        "data",
        NULL
    };

    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OO",
                                      &keywords[0],
                                      &format_p,
                                      &data_p);

    if (res == 0) {
        return (NULL);
    }

    c_format_p = PyUnicode_AsUTF8(format_p);

    if (c_format_p == NULL) {
        return (NULL);
    }

    res = PyBytes_AsStringAndSize(data_p, (char **)&src_p, &size);

    if (res == -1) {
        return (NULL);
    }

    swapped_p = PyBytes_FromStringAndSize(NULL, size);

    if (swapped_p == NULL) {
        return (NULL);
    }

    dst_p = (uint8_t *)PyBytes_AS_STRING(swapped_p);
    offset = 0;

    while (*c_format_p != '\0') {
        switch (*c_format_p) {

        case '1':
            if ((size - offset) < 1) {
                goto out1;
            }

            dst_p[offset] = src_p[offset];
            offset += 1;
            break;

        case '2':
            if ((size - offset) < 2) {
                goto out1;
            }

            dst_p[offset + 0] = src_p[offset + 1];
            dst_p[offset + 1] = src_p[offset + 0];
            offset += 2;
            break;

        case '4':
            if ((size - offset) < 4) {
                goto out1;
            }

            dst_p[offset + 0] = src_p[offset + 3];
            dst_p[offset + 1] = src_p[offset + 2];
            dst_p[offset + 2] = src_p[offset + 1];
            dst_p[offset + 3] = src_p[offset + 0];
            offset += 4;
            break;

        case '8':
            if ((size - offset) < 8) {
                goto out1;
            }

            dst_p[offset + 0] = src_p[offset + 7];
            dst_p[offset + 1] = src_p[offset + 6];
            dst_p[offset + 2] = src_p[offset + 5];
            dst_p[offset + 3] = src_p[offset + 4];
            dst_p[offset + 4] = src_p[offset + 3];
            dst_p[offset + 5] = src_p[offset + 2];
            dst_p[offset + 6] = src_p[offset + 1];
            dst_p[offset + 7] = src_p[offset + 0];
            offset += 8;
            break;

        default:
            PyErr_Format(PyExc_ValueError,
                         "Expected 1, 2, 4 or 8, but got %c.",
                         (char)*c_format_p);
            goto out2;
        }

        c_format_p++;
    }

    return (swapped_p);

 out1:
    PyErr_SetString(PyExc_ValueError, "Out of data to swap.");

 out2:

    return (NULL);
}

static PyObject *compiled_format_create(PyTypeObject *type_p,
                                        PyObject *format_p)
{
    PyObject *self_p;

    self_p = compiled_format_new(type_p, NULL, NULL);

    if (self_p == NULL) {
        return (NULL);
    }

    if (compiled_format_init_inner((struct compiled_format_t *)self_p,
                                   format_p) != 0) {
        return (NULL);
    }

    return (self_p);
}

static PyObject *compiled_format_new(PyTypeObject *type_p,
                                     PyObject *args_p,
                                     PyObject *kwargs_p)
{
    return (type_p->tp_alloc(type_p, 0));
}

static int compiled_format_init(struct compiled_format_t *self_p,
                                PyObject *args_p,
                                PyObject *kwargs_p)
{
    int res;
    PyObject *format_p;

    static char *keywords[] = {
        "fmt",
        NULL
    };

    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "O",
                                      &keywords[0],
                                      &format_p);

    if (res == 0) {
        return (-1);
    }

    return (compiled_format_init_inner(self_p, format_p));
}

static int compiled_format_init_inner(struct compiled_format_t *self_p,
                                      PyObject *format_p)
{
    self_p->info_p = parse_format(format_p);

    if (self_p->info_p == NULL) {
        PyObject_Free(self_p);

        return (-1);
    }

    Py_INCREF(format_p);
    self_p->format_p = format_p;

    return (0);
}

static void compiled_format_dealloc(struct compiled_format_t *self_p)
{
    PyMem_RawFree(self_p->info_p);
    Py_DECREF(self_p->format_p);
    Py_TYPE(self_p)->tp_free((PyObject *)self_p);
}

static PyObject *m_compiled_format_pack(struct compiled_format_t *self_p,
                                        PyObject *args_p)
{
    return (pack(self_p->info_p, args_p, 0, PyTuple_GET_SIZE(args_p)));
}

static PyObject *m_compiled_format_unpack(struct compiled_format_t *self_p,
                                          PyObject *args_p,
                                          PyObject *kwargs_p)
{
    PyObject *data_p;
    PyObject *allow_truncated_p;
    int res;
    static char *keywords[] = {
        "data",
        "allow_truncated",
        NULL
    };

    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "O|O",
                                      &keywords[0],
                                      &data_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    return (unpack(self_p->info_p, data_p, 0, allow_truncated_p));
}

static PyObject *m_compiled_format_pack_into(struct compiled_format_t *self_p,
                                             PyObject *args_p,
                                             PyObject *kwargs_p)
{
    PyObject *buf_p;
    PyObject *offset_p;
    Py_ssize_t number_of_args;

    number_of_args = PyTuple_GET_SIZE(args_p);

    if (number_of_args < 2) {
        PyErr_SetString(PyExc_ValueError, "Too few arguments.");

        return (NULL);
    }

    buf_p = PyTuple_GET_ITEM(args_p, 0);
    offset_p = PyTuple_GET_ITEM(args_p, 1);

    return (pack_into(self_p->info_p,
                      buf_p,
                      offset_p,
                      args_p,
                      2,
                      number_of_args));
}

static PyObject *m_compiled_format_unpack_from(struct compiled_format_t *self_p,
                                               PyObject *args_p,
                                               PyObject *kwargs_p)
{
    PyObject *data_p;
    PyObject *offset_p;
    PyObject *allow_truncated_p;
    int res;
    static char *keywords[] = {
        "data",
        "offset",
        "allow_truncated",
        NULL
    };

    offset_p = py_zero_p;
    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "O|OO",
                                      &keywords[0],
                                      &data_p,
                                      &offset_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    return (unpack_from(self_p->info_p, data_p, offset_p, allow_truncated_p));
}

static PyObject *m_compiled_format_calcsize(struct compiled_format_t *self_p)
{
    return (calcsize(self_p->info_p));
}

static PyObject *m_compiled_format_copy(struct compiled_format_t *self_p)
{
    struct compiled_format_t *new_p;
    size_t info_size;

    new_p = (struct compiled_format_t *)compiled_format_new(
        &compiled_format_type,
        NULL,
        NULL);

    if (new_p == NULL) {
        return (NULL);
    }

    info_size = sizeof(*self_p->info_p);
    info_size += (sizeof(self_p->info_p->fields[0])
                  * (self_p->info_p->number_of_fields - 1));

    new_p->info_p = PyMem_RawMalloc(info_size);

    if (new_p->info_p == NULL) {
        /* ToDo: Free new_p. */
        return (NULL);
    }

    memcpy(new_p->info_p, self_p->info_p, info_size);
    Py_INCREF(self_p->format_p);
    new_p->format_p = self_p->format_p;

    return ((PyObject *)new_p);
}

static PyObject *m_compiled_format_deepcopy(struct compiled_format_t *self_p,
                                            PyObject *args_p)
{
    return (m_compiled_format_copy(self_p));
}

static PyObject *m_compiled_format_getstate(struct compiled_format_t *self_p,
                                            PyObject *args_p)
{
    return (Py_BuildValue("{sOsi}",
                          "format",
                          self_p->format_p,
                          pickle_version_key,
                          pickle_version));
}

static PyObject *m_compiled_format_setstate(struct compiled_format_t *self_p,
                                            PyObject *state_p)
{
    PyObject *version_p;
    int version;
    PyObject *format_p;

    if (!PyDict_CheckExact(state_p)) {
        PyErr_SetString(PyExc_ValueError, "Pickled object is not a dict.");

        return (NULL);
    }

    version_p = PyDict_GetItemString(state_p, pickle_version_key);

    if (version_p == NULL) {
        PyErr_Format(PyExc_KeyError,
                     "No \"%s\" in pickled dict.",
                     pickle_version_key);

        return (NULL);
    }

    version = (int)PyLong_AsLong(version_p);

    if (version != pickle_version) {
        PyErr_Format(PyExc_ValueError,
                     "Pickle version mismatch. Got version %d but expected version %d.",
                     version, pickle_version);

        return (NULL);
    }

    format_p = PyDict_GetItemString(state_p, "format");

    if (format_p == NULL) {
        PyErr_SetString(PyExc_KeyError, "No \"format\" in pickled dict.");

        return (NULL);
    }

    if (compiled_format_init_inner(self_p, format_p) != 0) {
        return (NULL);
    }

    Py_RETURN_NONE;
}

static PyObject *compiled_format_dict_create(PyTypeObject *type_p,
                                             PyObject *format_p,
                                             PyObject *names_p)
{
    PyObject *self_p;

    self_p = compiled_format_dict_new(type_p, NULL, NULL);

    if (self_p == NULL) {
        return (NULL);
    }

    if (compiled_format_dict_init_inner((struct compiled_format_dict_t *)self_p,
                                        format_p,
                                        names_p) != 0) {
        return (NULL);
    }

    return (self_p);
}

static PyObject *compiled_format_dict_new(PyTypeObject *type_p,
                                          PyObject *args_p,
                                          PyObject *kwargs_p)
{
    return (type_p->tp_alloc(type_p, 0));
}

static int compiled_format_dict_init(struct compiled_format_dict_t *self_p,
                                     PyObject *args_p,
                                     PyObject *kwargs_p)
{
    int res;
    PyObject *format_p;
    PyObject *names_p;
    static char *keywords[] = {
        "fmt",
        "names",
        NULL
    };

    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OO",
                                      &keywords[0],
                                      &format_p,
                                      &names_p);

    if (res == 0) {
        return (-1);
    }

    return (compiled_format_dict_init_inner(self_p, format_p, names_p));
}

static int compiled_format_dict_init_inner(struct compiled_format_dict_t *self_p,
                                           PyObject *format_p,
                                           PyObject *names_p)
{
    if (!is_names_list(names_p)) {
        return (-1);
    }

    self_p->info_p = parse_format(format_p);

    if (self_p->info_p == NULL) {
        PyObject_Free(self_p);

        return (-1);
    }

    Py_INCREF(format_p);
    self_p->format_p = format_p;
    Py_INCREF(names_p);
    self_p->names_p = names_p;

    return (0);
}

static void compiled_format_dict_dealloc(struct compiled_format_dict_t *self_p)
{
    PyMem_RawFree(self_p->info_p);
    Py_DECREF(self_p->names_p);
    Py_DECREF(self_p->format_p);
    Py_TYPE(self_p)->tp_free((PyObject *)self_p);
}

static PyObject *m_compiled_format_dict_pack(struct compiled_format_dict_t *self_p,
                                             PyObject *data_p)
{
    return (pack_dict(self_p->info_p, self_p->names_p, data_p));
}

static PyObject *m_compiled_format_dict_unpack(
    struct compiled_format_dict_t *self_p,
    PyObject *args_p,
    PyObject *kwargs_p)
{
    PyObject *data_p;
    PyObject *allow_truncated_p;
    int res;
    static char *keywords[] = {
        "data",
        "allow_truncated",
        NULL
    };

    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "O|O",
                                      &keywords[0],
                                      &data_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    return (unpack_dict(self_p->info_p, self_p->names_p, data_p, 0, allow_truncated_p));
}

static PyObject *m_compiled_format_dict_pack_into(
    struct compiled_format_dict_t *self_p,
    PyObject *args_p,
    PyObject *kwargs_p)
{
    PyObject *buf_p;
    PyObject *data_p;
    PyObject *offset_p;
    int res;
    static char *keywords[] = {
        "buf",
        "data",
        "offset",
        NULL
    };

    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "OOO",
                                      &keywords[0],
                                      &buf_p,
                                      &data_p,
                                      &offset_p);

    if (res == 0) {
        return (NULL);
    }

    return (pack_into_dict(self_p->info_p,
                           self_p->names_p,
                           buf_p,
                           data_p,
                           offset_p));
}

static PyObject *m_compiled_format_dict_unpack_from(
    struct compiled_format_dict_t *self_p,
    PyObject *args_p,
    PyObject *kwargs_p)
{
    PyObject *data_p;
    PyObject *offset_p;
    PyObject *allow_truncated_p;
    int res;
    static char *keywords[] = {
        "data",
        "offset",
        NULL
    };

    offset_p = py_zero_p;
    allow_truncated_p = py_zero_p;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "O|OO",
                                      &keywords[0],
                                      &data_p,
                                      &offset_p,
                                      &allow_truncated_p);

    if (res == 0) {
        return (NULL);
    }

    return (unpack_from_dict(self_p->info_p,
                             self_p->names_p,
                             data_p,
                             offset_p,
                             allow_truncated_p));
}

static PyObject *m_compiled_format_dict_calcsize(
    struct compiled_format_dict_t *self_p)
{
    return (calcsize(self_p->info_p));
}

static PyObject *m_compiled_format_dict_copy(struct compiled_format_dict_t *self_p)
{
    struct compiled_format_dict_t *new_p;
    size_t info_size;

    new_p = (struct compiled_format_dict_t *)compiled_format_dict_new(
        &compiled_format_dict_type,
        NULL,
        NULL);

    if (new_p == NULL) {
        return (NULL);
    }

    info_size = sizeof(*self_p->info_p);
    info_size += (sizeof(self_p->info_p->fields[0])
                  * (self_p->info_p->number_of_fields - 1));

    new_p->info_p = PyMem_RawMalloc(info_size);

    if (new_p->info_p == NULL) {
        /* ToDo: Free new_p. */
        return (NULL);
    }

    memcpy(new_p->info_p, self_p->info_p, info_size);
    Py_INCREF(self_p->names_p);
    new_p->names_p = self_p->names_p;
    Py_INCREF(self_p->format_p);
    new_p->format_p = self_p->format_p;

    return ((PyObject *)new_p);
}

static PyObject *m_compiled_format_dict_deepcopy(struct compiled_format_dict_t *self_p,
                                                 PyObject *args_p)
{
    return (m_compiled_format_dict_copy(self_p));
}

PyDoc_STRVAR(compile___doc__,
             "compile(fmt, names=None)\n"
             "--\n"
             "\n");

static PyObject *m_compiled_format_dict_getstate(struct compiled_format_dict_t *self_p,
                                                 PyObject *args_p)
{
    return (Py_BuildValue("{sOsOsi}",
                          "format",
                          self_p->format_p,
                          "names",
                          self_p->names_p,
                          pickle_version_key,
                          pickle_version));
}

static PyObject *m_compiled_format_dict_setstate(struct compiled_format_dict_t *self_p,
                                                 PyObject *state_p)
{
    PyObject *version_p;
    int version;
    PyObject *format_p;
    PyObject *names_p;

    if (!PyDict_CheckExact(state_p)) {
        PyErr_SetString(PyExc_ValueError, "Pickled object is not a dict.");

        return (NULL);
    }

    version_p = PyDict_GetItemString(state_p, pickle_version_key);

    if (version_p == NULL) {
        PyErr_Format(PyExc_KeyError,
                     "No \"%s\" in pickled dict.",
                     pickle_version_key);

        return (NULL);
    }

    version = (int)PyLong_AsLong(version_p);

    if (version != pickle_version) {
        PyErr_Format(PyExc_ValueError,
                     "Pickle version mismatch. Got version %d but expected version %d.",
                     version, pickle_version);

        return (NULL);
    }

    format_p = PyDict_GetItemString(state_p, "format");

    if (format_p == NULL) {
        PyErr_SetString(PyExc_KeyError, "No \"format\" in pickled dict.");

        return (NULL);
    }

    names_p = PyDict_GetItemString(state_p, "names");

    if (names_p == NULL) {
        PyErr_SetString(PyExc_KeyError, "No \"names\" in pickled dict.");

        return (NULL);
    }

    if (compiled_format_dict_init_inner(self_p, format_p, names_p) != 0) {
        return (NULL);
    }

    Py_RETURN_NONE;
}

static PyObject *m_compile(PyObject *module_p,
                           PyObject *args_p,
                           PyObject *kwargs_p)
{
    PyObject *format_p;
    PyObject *names_p;
    int res;
    static char *keywords[] = {
        "fmt",
        "names",
        NULL
    };

    names_p = Py_None;
    res = PyArg_ParseTupleAndKeywords(args_p,
                                      kwargs_p,
                                      "O|O",
                                      &keywords[0],
                                      &format_p,
                                      &names_p);

    if (res == 0) {
        return (NULL);
    }

    if (names_p == Py_None) {
        return (compiled_format_create(&compiled_format_type, format_p));
    } else {
        return (compiled_format_dict_create(&compiled_format_dict_type,
                                            format_p,
                                            names_p));
    }
}

static struct PyMethodDef methods[] = {
    {
        "pack",
        m_pack,
        METH_VARARGS,
        pack___doc__
    },
    {
        "unpack",
        (PyCFunction)m_unpack,
        METH_VARARGS | METH_KEYWORDS,
        unpack___doc__
    },
    {
        "pack_into",
        (PyCFunction)m_pack_into,
        METH_VARARGS | METH_KEYWORDS,
        pack_into___doc__
    },
    {
        "unpack_from",
        (PyCFunction)m_unpack_from,
        METH_VARARGS | METH_KEYWORDS,
        unpack_from___doc__
    },
    {
        "pack_dict",
        m_pack_dict,
        METH_VARARGS,
        pack_dict___doc__
    },
    {
        "unpack_dict",
        (PyCFunction)m_unpack_dict,
        METH_VARARGS  | METH_KEYWORDS,
        unpack_dict___doc__
    },
    {
        "pack_into_dict",
        (PyCFunction)m_pack_into_dict,
        METH_VARARGS | METH_KEYWORDS,
        pack_into_dict___doc__
    },
    {
        "unpack_from_dict",
        (PyCFunction)m_unpack_from_dict,
        METH_VARARGS | METH_KEYWORDS,
        unpack_from_dict___doc__
    },
    {
        "calcsize",
        m_calcsize,
        METH_O,
        calcsize___doc__
    },
    {
        "byteswap",
        (PyCFunction)m_byteswap,
        METH_VARARGS | METH_KEYWORDS,
        byteswap___doc__
    },
    {
        "compile",
        (PyCFunction)m_compile,
        METH_VARARGS | METH_KEYWORDS,
        compile___doc__
    },
    { NULL }
};

static PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "bitstruct.c",
    .m_doc = "bitstruct C extension",
    .m_size = -1,
    .m_methods = methods
};

PyMODINIT_FUNC PyInit_c(void)
{
    PyObject *module_p;

    if (PyType_Ready(&compiled_format_type) < 0) {
        return (NULL);
    }

    if (PyType_Ready(&compiled_format_dict_type) < 0) {
        return (NULL);
    }

    py_zero_p = PyLong_FromLong(0);
    module_p = PyModule_Create(&module);

    if (module_p == NULL) {
        return (NULL);
    }

    Py_INCREF(&compiled_format_type);

    if (PyModule_AddObject(module_p,
                           "CompiledFormat",
                           (PyObject *)&compiled_format_type) < 0) {
        Py_DECREF(&compiled_format_type);
        Py_DECREF(module_p);

        return (NULL);
    }

    if (PyModule_AddObject(module_p,
                           "CompiledFormatDict",
                           (PyObject *)&compiled_format_dict_type) < 0) {
        Py_DECREF(&compiled_format_dict_type);
        Py_DECREF(module_p);

        return (NULL);
    }

    return (module_p);
}
