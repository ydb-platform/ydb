/*
   Copyright (c) 2019 - 2025, Ilan Schnell; All Rights Reserved
   bitarray is published under the PSF license.

   This file contains the C implementation of some useful utility functions.

   Author: Ilan Schnell
*/

#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "pythoncapi_compat.h"
#include "bitarray.h"

/* set during module initialization */
static PyTypeObject *bitarray_type;

#define bitarray_Check(obj)  PyObject_TypeCheck((obj), bitarray_type)

/* Return 0 if obj is bitarray.  If not, set exception and return -1. */
static int
ensure_bitarray(PyObject *obj)
{
    if (bitarray_Check(obj))
        return 0;

    PyErr_Format(PyExc_TypeError, "bitarray expected, not '%s'",
                 Py_TYPE(obj)->tp_name);
    return -1;
}

/* Return new bitarray of length 'nbits', endianness given by the PyObject
   'endian' (which may be Py_None).
   Unless -1, 'c' is placed into all characters of buffer. */
static bitarrayobject *
new_bitarray(Py_ssize_t nbits, PyObject *endian, int c)
{
    PyObject *args;             /* args for bitarray() */
    bitarrayobject *res;

    args = Py_BuildValue("nOO", nbits, endian, Py_Ellipsis);
    if (args == NULL)
        return NULL;

    /* equivalent to: res = bitarray(nbits, endian, Ellipsis) */
    res = (bitarrayobject *) PyObject_CallObject((PyObject *) bitarray_type,
                                                 args);
    Py_DECREF(args);
    if (res == NULL)
        return NULL;

    assert(res->nbits == nbits && res->readonly == 0 && res->buffer == NULL);
    assert(-1 <= c && c < 256);
    if (c >= 0 && nbits)
        memset(res->ob_item, c, (size_t) Py_SIZE(res));

    return res;
}

/* Starting from 64-bit word index i, count remaining population
   in bitarray a.  Basically equivalent to: a[64 * i:].count() */
static Py_ssize_t
count_from_word(bitarrayobject *a, Py_ssize_t i)
{
    const Py_ssize_t nbits = a->nbits;
    Py_ssize_t cnt;

    assert(i >= 0);
    if (64 * i >= nbits)
        return 0;
    cnt = popcnt_words(WBUFF(a) + i, nbits / 64 - i);  /* complete words */
    if (nbits % 64)
        cnt += popcnt_64(zlw(a));                      /* remaining bits */
    return cnt;
}

/* like resize() but without over-allocation or buffer import/export checks */
static int
resize_lite(bitarrayobject *self, Py_ssize_t nbits)
{
    const Py_ssize_t newsize = BYTES(nbits);

    assert(self->allocated >= Py_SIZE(self));
    assert(self->readonly == 0);
    assert(self->ob_exports == 0);
    assert(self->buffer == NULL);

    /* bypass everything when buffer size hasn't changed */
    if (newsize == Py_SIZE(self)) {
        self->nbits = nbits;
        return 0;
    }

    if (newsize == 0) {
        PyMem_Free(self->ob_item);
        self->ob_item = NULL;
        Py_SET_SIZE(self, 0);
        self->allocated = 0;
        self->nbits = 0;
        return 0;
    }

    self->ob_item = PyMem_Realloc(self->ob_item, newsize);
    if (self->ob_item == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    Py_SET_SIZE(self, newsize);
    self->allocated = newsize;
    self->nbits = nbits;
    return 0;
}

/* ---------------------------- zeros / ones --------------------------- */

static PyObject *
zeros(PyObject *module, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"", "endian", NULL};
    PyObject *endian = Py_None;
    Py_ssize_t n;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "n|O:zeros", kwlist,
                                     &n, &endian))
        return NULL;

    return (PyObject *) new_bitarray(n, endian, 0);
}

PyDoc_STRVAR(zeros_doc,
"zeros(n, /, endian=None) -> bitarray\n\
\n\
Create a bitarray of length `n`, with all values `0`, and optional\n\
bit-endianness (`little` or `big`).");


static PyObject *
ones(PyObject *module, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"", "endian", NULL};
    PyObject *endian = Py_None;
    Py_ssize_t n;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "n|O:ones", kwlist,
                                     &n, &endian))
        return NULL;

    return (PyObject *) new_bitarray(n, endian, 0xff);
}

PyDoc_STRVAR(ones_doc,
"ones(n, /, endian=None) -> bitarray\n\
\n\
Create a bitarray of length `n`, with all values `1`, and optional\n\
bit-endianness (`little` or `big`).");

/* ------------------------------- count_n ----------------------------- */

/* Return smallest index i for which a.count(vi, 0, i) == n.  When n exceeds
   the total count, the result is a negative number; the negative of the
   total count + 1, which is useful for displaying error messages. */
static Py_ssize_t
count_n_core(bitarrayobject *a, Py_ssize_t n, int vi)
{
    const Py_ssize_t nbits = a->nbits;
    uint64_t *wbuff = WBUFF(a);
    Py_ssize_t i = 0;         /* index (result) */
    Py_ssize_t t = 0;         /* total count up to index */
    Py_ssize_t m;             /* popcount in each block */

    assert(0 <= n && n <= nbits);

    /* by counting big blocks we save comparisons and updates */
#define BLOCK_BITS  4096      /* block size: 4096 bits = 64 words */
    while (i + BLOCK_BITS < nbits) {
        m = popcnt_words(wbuff + i / 64, BLOCK_BITS / 64);
        if (!vi)
            m = BLOCK_BITS - m;
        if (t + m >= n)
            break;
        t += m;
        i += BLOCK_BITS;
    }
#undef BLOCK_BITS

    while (i + 64 < nbits) {  /* count blocks of single (64-bit) words */
        m = popcnt_64(wbuff[i / 64]);
        if (!vi)
            m = 64 - m;
        if (t + m >= n)
            break;
        t += m;
        i += 64;
    }

    while (i < nbits && t < n) {
        t += getbit(a, i) == vi;
        i++;
    }

    if (t < n) {  /* n exceeds total count */
        assert((vi ? t : nbits - t) == count_from_word(a, 0));
        return -(t + 1);
    }
    return i;
}

static PyObject *
count_n(PyObject *module, PyObject *args)
{
    bitarrayobject *a;
    Py_ssize_t n, i;
    int vi = 1;

    if (!PyArg_ParseTuple(args, "O!n|O&:count_n", bitarray_type,
                          (PyObject *) &a, &n, conv_pybit, &vi))
        return NULL;
    if (n < 0) {
        PyErr_SetString(PyExc_ValueError, "non-negative integer expected");
        return NULL;
    }
    if (n > a->nbits)
        return PyErr_Format(PyExc_ValueError, "n = %zd larger than bitarray "
                            "length %zd", n, a->nbits);

    i = count_n_core(a, n, vi);        /* do actual work here */
    if (i < 0)
        return PyErr_Format(PyExc_ValueError, "n = %zd exceeds total count "
                            "(a.count(%d) = %zd)", n, vi, -(i + 1));

    return PyLong_FromSsize_t(i);
}

PyDoc_STRVAR(count_n_doc,
"count_n(a, n, value=1, /) -> int\n\
\n\
Return lowest index `i` for which `a[:i].count(value) == n`.\n\
Raises `ValueError` when `n` exceeds total count (`a.count(value)`).");

/* --------------------------- unary functions ------------------------- */

static PyObject *
parity(PyObject *module, PyObject *obj)
{
    bitarrayobject *a;
    uint64_t x, *wbuff;
    Py_ssize_t i;

    if (ensure_bitarray(obj) < 0)
        return NULL;

    a = (bitarrayobject *) obj;
    wbuff = WBUFF(a);
    x = zlw(a);
    i = a->nbits / 64;
    while (i--)
        x ^= *wbuff++;
    return PyLong_FromLong(parity_64(x));
}

PyDoc_STRVAR(parity_doc,
"parity(a, /) -> int\n\
\n\
Return parity of bitarray `a`.\n\
`parity(a)` is equivalent to `a.count() % 2` but more efficient.");


/* Internal functions, like sum_indices(), but bitarrays are limited in
   size.  For details see: devel/test_sum_indices.py
*/
static PyObject *
ssqi(PyObject *module, PyObject *args)
{
    static char count_table[256], sum_table[256], sum_sqr_table[256];
    static int setup = -1;      /* endianness of tables */
    bitarrayobject *a;
    uint64_t nbytes, i;
    uint64_t sm = 0;            /* accumulated sum */
    int mode = 1;

    if (!PyArg_ParseTuple(args, "O!|i", bitarray_type,
                          (PyObject *) &a, &mode))
        return NULL;
    if (mode < 1 || mode > 2)
        return PyErr_Format(PyExc_ValueError, "unexpected mode %d", mode);
    if (((uint64_t) a->nbits) > (mode == 1 ? 6074001000LLU : 3810778LLU))
        return PyErr_Format(PyExc_OverflowError, "ssqi %zd", a->nbits);

    if (setup != a->endian) {
        setup_table(count_table, 'c');
        setup_table(sum_table, IS_LE(a) ? 'a' : 'A');
        setup_table(sum_sqr_table, IS_LE(a) ? 's' : 'S');
        setup = a->endian;
    }

    nbytes = Py_SIZE(a);
    set_padbits(a);
    for (i = 0; i < nbytes; i++) {
        unsigned char c = a->ob_item[i];
        if (c) {
            uint64_t k = count_table[c], z1 = sum_table[c];
            if (mode == 1) {
                sm += k * 8LLU * i + z1;
            }
            else {
                uint64_t z2 = (unsigned char) sum_sqr_table[c];
                sm += (k * 64LLU * i + 16LLU * z1) * i + z2;
            }
        }
    }
    return PyLong_FromUnsignedLongLong(sm);
}


static PyObject *
xor_indices(PyObject *module, PyObject *obj)
{
    static char parity_table[256], xor_table[256];
    static int setup = -1;      /* endianness of xor_table */
    bitarrayobject *a;
    Py_ssize_t res = 0, nbytes, i;

    if (ensure_bitarray(obj) < 0)
        return NULL;

    a = (bitarrayobject *) obj;
    nbytes = Py_SIZE(a);
    set_padbits(a);

    if (setup != a->endian) {
        setup_table(xor_table, IS_LE(a) ? 'x' : 'X');
        setup_table(parity_table, 'p');
        setup = a->endian;
    }

    for (i = 0; i < nbytes; i++) {
        unsigned char c = a->ob_item[i];
        if (parity_table[c])
            res ^= i << 3;
        res ^= xor_table[c];
    }
    return PyLong_FromSsize_t(res);
}

PyDoc_STRVAR(xor_indices_doc,
"xor_indices(a, /) -> int\n\
\n\
Return xor reduced indices of all active bits in bitarray `a`.\n\
This is essentially equivalent to\n\
`reduce(operator.xor, (i for i, v in enumerate(a) if v))`.");

/* --------------------------- binary functions ------------------------ */

static PyObject *
binary_function(PyObject *args, const char *format, const char oper)
{
    Py_ssize_t cnt = 0, cwords, i;
    bitarrayobject *a, *b;
    uint64_t *wbuff_a, *wbuff_b;
    int rbits;

    if (!PyArg_ParseTuple(args, format,
                          bitarray_type, (PyObject *) &a,
                          bitarray_type, (PyObject *) &b))
        return NULL;
    if (ensure_eq_size_endian(a, b) < 0)
        return NULL;

    wbuff_a = WBUFF(a);
    wbuff_b = WBUFF(b);
    cwords = a->nbits / 64;     /* number of complete 64-bit words */
    rbits = a->nbits % 64;      /* remaining bits  */

    switch (oper) {
    case '&':                   /* count and */
        for (i = 0; i < cwords; i++)
            cnt += popcnt_64(wbuff_a[i] & wbuff_b[i]);
        if (rbits)
            cnt += popcnt_64(zlw(a) & zlw(b));
        break;

    case '|':                   /* count or */
        for (i = 0; i < cwords; i++)
            cnt += popcnt_64(wbuff_a[i] | wbuff_b[i]);
        if (rbits)
            cnt += popcnt_64(zlw(a) | zlw(b));
        break;

    case '^':                   /* count xor */
        for (i = 0; i < cwords; i++)
            cnt += popcnt_64(wbuff_a[i] ^ wbuff_b[i]);
        if (rbits)
            cnt += popcnt_64(zlw(a) ^ zlw(b));
        break;

    case 'a':                   /* any and */
        for (i = 0; i < cwords; i++) {
            if (wbuff_a[i] & wbuff_b[i])
                Py_RETURN_TRUE;
        }
        return PyBool_FromLong(rbits && (zlw(a) & zlw(b)));

    case 's':                   /* is subset */
        for (i = 0; i < cwords; i++) {
            if ((wbuff_a[i] & wbuff_b[i]) != wbuff_a[i])
                Py_RETURN_FALSE;
        }
        return PyBool_FromLong(rbits == 0 || (zlw(a) & zlw(b)) == zlw(a));

    default:
        Py_UNREACHABLE();
    }
    return PyLong_FromSsize_t(cnt);
}

#define COUNT_FUNC(oper, ostr)                                          \
static PyObject *                                                       \
count_ ## oper (PyObject *module, PyObject *args)                       \
{                                                                       \
    return binary_function(args, "O!O!:count_" #oper, *ostr);           \
}                                                                       \
PyDoc_STRVAR(count_ ## oper ## _doc,                                    \
"count_" #oper "(a, b, /) -> int\n\
\n\
Return `(a " ostr " b).count()` in a memory efficient manner,\n\
as no intermediate bitarray object gets created.")

COUNT_FUNC(and, "&");           /* count_and */
COUNT_FUNC(or,  "|");           /* count_or  */
COUNT_FUNC(xor, "^");           /* count_xor */


static PyObject *
any_and(PyObject *module, PyObject *args)
{
    return binary_function(args, "O!O!:any_and", 'a');
}

PyDoc_STRVAR(any_and_doc,
"any_and(a, b, /) -> bool\n\
\n\
Efficient implementation of `any(a & b)`.");


static PyObject *
subset(PyObject *module, PyObject *args)
{
    return binary_function(args, "O!O!:subset", 's');
}

PyDoc_STRVAR(subset_doc,
"subset(a, b, /) -> bool\n\
\n\
Return `True` if bitarray `a` is a subset of bitarray `b`.\n\
`subset(a, b)` is equivalent to `a | b == b` (and equally `a & b == a`) but\n\
more efficient as no intermediate bitarray object is created and the buffer\n\
iteration is stopped as soon as one mismatch is found.");


static PyObject *
correspond_all(PyObject *module, PyObject *args)
{
    Py_ssize_t nff = 0, nft = 0, ntf = 0, ntt = 0, cwords, i;
    bitarrayobject *a, *b;
    uint64_t u, v, not_u, not_v;
    int rbits;

    if (!PyArg_ParseTuple(args, "O!O!:correspond_all",
                          bitarray_type, (PyObject *) &a,
                          bitarray_type, (PyObject *) &b))
        return NULL;
    if (ensure_eq_size_endian(a, b) < 0)
        return NULL;

    cwords = a->nbits / 64;     /* complete 64-bit words */
    rbits = a->nbits % 64;      /* remaining bits */

    for (i = 0; i < cwords; i++) {
        u = WBUFF(a)[i];
        v = WBUFF(b)[i];
        not_u = ~u;
        not_v = ~v;
        nff += popcnt_64(not_u & not_v);
        nft += popcnt_64(not_u & v);
        ntf += popcnt_64(u & not_v);
        ntt += popcnt_64(u & v);
    }

    if (rbits) {
        u = zlw(a);
        v = zlw(b);
        not_u = ~u;
        not_v = ~v;
        /* for nff we need to substract the number of unused 1 bits */
        nff += popcnt_64(not_u & not_v) - (64 - rbits);
        nft += popcnt_64(not_u & v);
        ntf += popcnt_64(u & not_v);
        ntt += popcnt_64(u & v);
    }
    return Py_BuildValue("nnnn", nff, nft, ntf, ntt);
}

PyDoc_STRVAR(correspond_all_doc,
"correspond_all(a, b, /) -> tuple\n\
\n\
Return tuple with counts of: ~a & ~b, ~a & b, a & ~b, a & b");


static void
byteswap_core(Py_buffer view, Py_ssize_t n)
{
    char *buff = view.buf;
    Py_ssize_t m = view.len / n, k;

    assert(n >= 1 && n * m == view.len);

    if (n == 8 && HAVE_BUILTIN_BSWAP64) {
        uint64_t *w = (uint64_t *) buff;
        for (k = 0; k < m; k++)
            w[k] = builtin_bswap64(w[k]);
    }
#if (defined(__clang__) || (defined(__GNUC__) && (__GNUC__ >= 5)))
    else if (n == 4) {
        uint32_t *w = (uint32_t *) buff;
        for (k = 0; k < m; k++)
            w[k] = __builtin_bswap32(w[k]);
    }
    else if (n == 2) {
        uint16_t *w = (uint16_t *) buff;
        for (k = 0; k < m; k++)
            w[k] = __builtin_bswap16(w[k]);
    }
#endif
    else if (n >= 2) {
        for (k = 0; k < view.len; k += n)
            swap_bytes(buff + k, n);
    }
}

static PyObject *
byteswap(PyObject *module, PyObject *args)
{
    PyObject *buffer;
    Py_buffer view;
    Py_ssize_t n = 0;

    if (!PyArg_ParseTuple(args, "O|n:byteswap", &buffer, &n))
        return NULL;

    if (n < 0)
        return PyErr_Format(PyExc_ValueError,
                            "positive int expect, got %zd", n);

    if (PyObject_GetBuffer(buffer, &view, PyBUF_SIMPLE | PyBUF_WRITABLE) < 0)
        return NULL;

    if (n == 0)
        /* avoid division by zero below when view.len = 0 */
        n = Py_MAX(1, view.len);

    if (view.len % n) {
        PyErr_Format(PyExc_ValueError, "buffer size %zd not multiple of %zd",
                     view.len, n);
        PyBuffer_Release(&view);
        return NULL;
    }

    byteswap_core(view, n);

    PyBuffer_Release(&view);
    Py_RETURN_NONE;
}

PyDoc_STRVAR(byteswap_doc,
"byteswap(a, n=<buffer size>, /)\n\
\n\
Reverse every `n` consecutive bytes of `a` in-place.\n\
By default, all bytes are reversed.  Note that `n` is not limited to 2, 4\n\
or 8, but can be any positive integer.\n\
Also, `a` may be any object that exposes a writable buffer.\n\
Nothing about this function is specific to bitarray objects.");

/* ---------------------------- serialization -------------------------- */

/*
  The binary format used here is similar to the one used for pickling
  bitarray objects.  However, this format has a head byte which encodes both
  the bit-endianness and the number of pad bits, whereas the binary pickle
  blob does not.
*/

static PyObject *
serialize(PyObject *module, PyObject *obj)
{
    bitarrayobject *a;
    PyObject *result;
    Py_ssize_t nbytes;
    char *str;

    if (ensure_bitarray(obj) < 0)
        return NULL;

    a = (bitarrayobject *) obj;
    nbytes = Py_SIZE(a);
    result = PyBytes_FromStringAndSize(NULL, nbytes + 1);
    if (result == NULL)
        return NULL;

    str = PyBytes_AsString(result);
    set_padbits(a);
    *str = (IS_BE(a) ? 0x10 : 0x00) | ((char) PADBITS(a));
    memcpy(str + 1, a->ob_item, (size_t) nbytes);
    return result;
}

PyDoc_STRVAR(serialize_doc,
"serialize(bitarray, /) -> bytes\n\
\n\
Return a serialized representation of the bitarray, which may be passed to\n\
`deserialize()`.  It efficiently represents the bitarray object (including\n\
its bit-endianness) and is guaranteed not to change in future releases.");


static PyObject *
deserialize(PyObject *module, PyObject *buffer)
{
    Py_buffer view;
    bitarrayobject *a;
    unsigned char head;
    Py_ssize_t nbits;

    if (PyObject_GetBuffer(buffer, &view, PyBUF_SIMPLE) < 0)
        return NULL;

    if (view.len == 0) {
        PyErr_SetString(PyExc_ValueError,
                        "non-empty bytes-like object expected");
        goto error;
    }

    head = *((unsigned char *) view.buf);

    if (head & 0xe8 || (view.len == 1 && head & 0xef)) {
        PyErr_Format(PyExc_ValueError, "invalid header byte: 0x%02x", head);
        goto error;
    }
    /* create bitarray of desired length */
    nbits = 8 * (view.len - 1) - ((Py_ssize_t) (head & 0x07));
    if ((a = new_bitarray(nbits, Py_None, -1)) == NULL)
        goto error;
    /* set bit-endianness and buffer */
    a->endian = head & 0x10 ? ENDIAN_BIG : ENDIAN_LITTLE;
    assert(Py_SIZE(a) == view.len - 1);
    memcpy(a->ob_item, ((char *) view.buf) + 1, (size_t) view.len - 1);

    PyBuffer_Release(&view);
    return (PyObject *) a;

 error:
    PyBuffer_Release(&view);
    return NULL;
}

PyDoc_STRVAR(deserialize_doc,
"deserialize(bytes, /) -> bitarray\n\
\n\
Return a bitarray given a bytes-like representation such as returned\n\
by `serialize()`.");

/* ----------------------------- hexadecimal --------------------------- */

static const char hexdigits[] = "0123456789abcdef";

static int
hex_to_int(char c)
{
    if ('0' <= c && c <= '9')
        return c - '0';
    if ('a' <= c && c <= 'f')
        return c - 'a' + 10;
    if ('A' <= c && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

/* return hexadecimal string from bitarray,
   on failure set exception and return NULL */
static char *
ba2hex_core(bitarrayobject *a, Py_ssize_t group, char *sep)
{
    const int be = IS_BE(a);
    size_t strsize = a->nbits / 4, j, nsep;
    Py_ssize_t i;
    char *buff = a->ob_item, *str;

    nsep = (group && strsize) ? strlen(sep) : 0;  /* 0 means no grouping */
    if (nsep)
        strsize += nsep * ((strsize - 1) / group);

    str = PyMem_New(char, strsize + 1);
    if (str == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    for (i = j = 0; i < a->nbits / 4; i++) {
        unsigned char c = buff[i / 2];

        if (nsep && i && i % group == 0) {
            memcpy(str + j, sep, nsep);
            j += nsep;
        }
        str[j++] = hexdigits[(i + be) % 2 ? c >> 4 : 0x0f & c];
    }
    assert(j == strsize);
    str[strsize] = 0;  /* terminate string */
    return str;
}

static PyObject *
ba2hex(PyObject *module, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"", "group", "sep", NULL};
    PyObject *result;
    bitarrayobject *a;
    Py_ssize_t group = 0;
    char *sep = " ", *str;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!|ns:ba2hex", kwlist,
                                     bitarray_type, (PyObject *) &a,
                                     &group, &sep))
        return NULL;

    if (a->nbits % 4) {
        PyErr_Format(PyExc_ValueError, "bitarray length %zd not "
                     "multiple of 4", a->nbits);
        return NULL;
    }
    if (group < 0) {
        PyErr_Format(PyExc_ValueError, "non-negative integer "
                     "expected for group, got: %zd", group);
        return NULL;
    }

    str = ba2hex_core(a, group, sep);
    if (str == NULL)
        return NULL;

    result = PyUnicode_FromString(str);
    PyMem_Free((void *) str);
    return result;
}

PyDoc_STRVAR(ba2hex_doc,
"ba2hex(bitarray, /, group=0, sep=' ') -> hexstr\n\
\n\
Return a string containing the hexadecimal representation of\n\
the bitarray (which has to be multiple of 4 in length).\n\
When grouped, the string `sep` is inserted between groups\n\
of `group` characters, default is a space.");


/* Translate hexadecimal digits from 'hexstr' into the bitarray 'a' buffer,
   which must be initialized to zeros.
   Each digit corresponds to 4 bits in the bitarray.
   Note that the number of hexadecimal digits may be odd. */
static int
hex2ba_core(bitarrayobject *a, Py_buffer hexstr)
{
    const int be = IS_BE(a);
    const char *str = hexstr.buf;
    Py_ssize_t i = 0, j;

    assert(a->nbits == 4 * hexstr.len);

    for (j = 0; j < hexstr.len; j++) {
        unsigned char c = str[j];
        int x = hex_to_int(c);

        if (x < 0) {
            if (Py_UNICODE_ISSPACE(c))
                continue;
            PyErr_Format(PyExc_ValueError, "invalid digit found for "
                         "base16, got '%c' (0x%02x)", c, c);
            return -1;
        }
        assert(x >> 4 == 0);
        a->ob_item[i / 2] |= x << 4 * ((i + be) % 2);
        i++;
    }
    assert(i <= a->nbits);
    return resize_lite(a, 4 * i);  /* in case we ignored whitespace */
}

static PyObject *
hex2ba(PyObject *module, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"", "endian", NULL};
    PyObject *endian = Py_None;
    Py_buffer hexstr;
    bitarrayobject *a;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s*|O:hex2ba", kwlist,
                                     &hexstr, &endian))
        return NULL;

    a = new_bitarray(4 * hexstr.len, endian, 0);
    if (a == NULL)
        goto error;

    if (hex2ba_core(a, hexstr) < 0)
        goto error;

    PyBuffer_Release(&hexstr);
    return (PyObject *) a;

 error:
    PyBuffer_Release(&hexstr);
    Py_XDECREF((PyObject *) a);
    return NULL;
}

PyDoc_STRVAR(hex2ba_doc,
"hex2ba(hexstr, /, endian=None) -> bitarray\n\
\n\
Bitarray of hexadecimal representation.  hexstr may contain any number\n\
(including odd numbers) of hex digits (upper or lower case).\n\
Whitespace is ignored.");

/* ----------------------- base 2, 4, 8, 16, 32, 64 -------------------- */

/* RFC 4648 Base32 alphabet */
static const char base32_alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/* standard base 64 alphabet - also described in RFC 4648 */
static const char base64_alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* Given the length of the base m in [1..6] and a character c, return
   its index in the base 2**m alphabet , or -1 if when c is not included.
   Note: i >> m is true when i is not in range(0, 2**m) */
static int
digit_to_int(int m, char c)
{
    static signed char table[2][128];
    static int setup = 0;
    int i;

    assert(1 <= m && m <= 6);
    if (m < 5) {                                 /* base 2, 4, 8, 16 */
        i = hex_to_int(c);
        return i >> m ? -1 : i;
    }

    if (0x80 & c)  /* non-ASCII */
        return -1;

    if (!setup) {
        memset(table, 0xff, sizeof table);  /* (signed char) 0xff -> -1 */
        for (i = 0; i < 32; i++)
            table[0][(unsigned char) base32_alphabet[i]] = i;
        for (i = 0; i < 64; i++)
            table[1][(unsigned char) base64_alphabet[i]] = i;
        setup = 1;
    }
    return table[m - 5][(unsigned char) c];      /* base 32, 64 */
}

/* return m = log2(n) for m in [1..6] */
static int
base_to_length(int n)
{
    int m = 0;

    if (!n || n & (n - 1)) {
        PyErr_SetString(PyExc_ValueError, "base must be a power of 2");
        return -1;
    }
    while (n >>= 1)
        m++;
    if (1 <= m && m <= 6)
        return m;

    PyErr_SetString(PyExc_ValueError, "base must be 2, 4, 8, 16, 32 or 64");
    return -1;
}

/* return ASCII string from bitarray and base length m,
   on failure set exception and return NULL */
static char *
ba2base_core(bitarrayobject *a, int m, Py_ssize_t group, char *sep)
{
    const int le = IS_LE(a);
    const char *alphabet;
    size_t strsize = a->nbits / m, j, nsep;
    Py_ssize_t i;
    char *str;

    assert(1 <= m && m <= 6 && a->nbits % m == 0);

    switch (m) {
    case 5: alphabet = base32_alphabet; break;
    case 6: alphabet = base64_alphabet; break;
    default: alphabet = hexdigits;
    }

    nsep = (group && strsize) ? strlen(sep) : 0;  /* 0 means no grouping */
    if (nsep)
        strsize += nsep * ((strsize - 1) / group);

    str = PyMem_New(char, strsize + 1);
    if (str == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    for (i = j = 0; i < a->nbits / m; i++) {
        int k, x = 0;

        if (nsep && i && i % group == 0) {
            memcpy(str + j, sep, nsep);
            j += nsep;
        }
        for (k = 0; k < m; k++) {
            int q = le ? k : (m - k - 1);
            x |= getbit(a, i * m + k) << q;
        }
        assert(x >> m == 0);
        str[j++] = alphabet[x];
    }
    assert(j == strsize);
    str[strsize] = 0;  /* terminate string */
    return str;
}

static PyObject *
ba2base(PyObject *module, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"", "", "group", "sep", NULL};
    bitarrayobject *a;
    PyObject *result;
    Py_ssize_t group = 0;
    char *sep = " ", *str;
    int n, m;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "iO!|ns:ba2base", kwlist,
                                     &n, bitarray_type, (PyObject *) &a,
                                     &group, &sep))
        return NULL;

    if ((m = base_to_length(n)) < 0)
        return NULL;

    if (a->nbits % m) {
        PyErr_Format(PyExc_ValueError, "bitarray length %zd not "
                     "multiple of %d", a->nbits, m);
        return NULL;
    }
    if (group < 0) {
        PyErr_Format(PyExc_ValueError, "non-negative integer "
                     "expected for group, got: %zd", group);
        return NULL;
    }

    if (m == 4)
        str = ba2hex_core(a, group, sep);
    else
        str = ba2base_core(a, m, group, sep);

    if (str == NULL)
        return NULL;

    result = PyUnicode_FromString(str);
    PyMem_Free((void *) str);
    return result;
}

PyDoc_STRVAR(ba2base_doc,
"ba2base(n, bitarray, /, group=0, sep=' ') -> str\n\
\n\
Return a string containing the base `n` ASCII representation of\n\
the bitarray.  Allowed values for `n` are 2, 4, 8, 16, 32 and 64.\n\
The bitarray has to be multiple of length 1, 2, 3, 4, 5 or 6 respectively.\n\
For `n=32` the RFC 4648 Base32 alphabet is used, and for `n=64` the\n\
standard base 64 alphabet is used.\n\
When grouped, the string `sep` is inserted between groups\n\
of `group` characters, default is a space.");


/* translate ASCII digits (with base length m) into bitarray buffer */
static int
base2ba_core(bitarrayobject *a, Py_buffer asciistr, int m)
{
    const char *str = asciistr.buf;
    const int le = IS_LE(a);
    Py_ssize_t i = 0, j;

    assert(a->nbits == asciistr.len * m && 1 <= m && m <= 6);

    for (j = 0; j < asciistr.len; j++) {
        unsigned char c = str[j];
        int k, x = digit_to_int(m, c);

        if (x < 0) {
            if (Py_UNICODE_ISSPACE(c))
                continue;
            PyErr_Format(PyExc_ValueError, "invalid digit found for "
                         "base%d, got '%c' (0x%02x)", 1 << m, c, c);
            return -1;
        }
        assert(x >> m == 0);
        for (k = 0; k < m; k++) {
            int q = le ? k : (m - k - 1);
            setbit(a, i++, x & (1 << q));
        }
    }
    assert(i <= a->nbits);
    return resize_lite(a, i);  /* in case we ignored whitespace */
}

static PyObject *
base2ba(PyObject *module, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"", "", "endian", NULL};
    PyObject *endian = Py_None;
    Py_buffer asciistr;
    bitarrayobject *a = NULL;
    int m, n, t;                   /* n = 2**m */

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "is*|O:base2ba", kwlist,
                                     &n, &asciistr, &endian))
        return NULL;

    if ((m = base_to_length(n)) < 0)
        goto error;

    a = new_bitarray(m * asciistr.len, endian, m == 4 ? 0 : -1);
    if (a == NULL)
        goto error;

    t = (m == 4) ? hex2ba_core(a, asciistr) : base2ba_core(a, asciistr, m);
    if (t < 0)
        goto error;

    PyBuffer_Release(&asciistr);
    return (PyObject *) a;

 error:
    PyBuffer_Release(&asciistr);
    Py_XDECREF((PyObject *) a);
    return NULL;
}

PyDoc_STRVAR(base2ba_doc,
"base2ba(n, asciistr, /, endian=None) -> bitarray\n\
\n\
Bitarray of base `n` ASCII representation.\n\
Allowed values for `n` are 2, 4, 8, 16, 32 and 64.\n\
For `n=32` the RFC 4648 Base32 alphabet is used, and for `n=64` the\n\
standard base 64 alphabet is used.  Whitespace is ignored.");

/* ------------------------ utility C functions ------------------------ */

/* Consume one item from iterator and return its value as an integer
   in range(256).  On failure, set an exception and return -1. */
static int
next_char(PyObject *iter)
{
    PyObject *item;
    Py_ssize_t v;

    if ((item = PyIter_Next(iter)) == NULL) {
        if (PyErr_Occurred())   /* from PyIter_Next() */
            return -1;
        PyErr_SetString(PyExc_StopIteration, "unexpected end of stream");
        return -1;
    }

    v = PyNumber_AsSsize_t(item, NULL);
    Py_DECREF(item);
    if (v == -1 && PyErr_Occurred())
        return -1;

    if (v >> 8) {
        PyErr_Format(PyExc_ValueError,
                     "byte must be in range(0, 256), got: %zd", v);
        return -1;
    }
    return (int) v;
}

/* write n bytes (into buffer str) representing non-negative integer i,
   using little endian byte-order */
static void
write_n(char *str, int n, Py_ssize_t i)
{
    int len = 0;

    assert(n <= 8 && i >= 0);
    while (len < n) {
        str[len++] = (char) i & 0xff;
        i >>= 8;
    }
    assert(i == 0);
}

/* read n bytes from iter and return corresponding non-negative integer,
   using little endian byte-order */
static Py_ssize_t
read_n(PyObject *iter, int n)
{
    Py_ssize_t i = 0;
    int j, c;

    assert(PyIter_Check(iter));
    assert(n <= 8);
    for (j = 0; j < n; j++) {
        if ((c = next_char(iter)) < 0)
            return -1;
        i |= ((Py_ssize_t) c) << (8 * j);
    }
    if (i < 0) {
        PyErr_Format(PyExc_ValueError,
                     "read %d bytes got negative value: %zd", n, i);
        return -1;
    }
    return i;
}

/* return number of bytes necessary to represent non-negative integer i */
static int
byte_length(Py_ssize_t i)
{
    int n = 0;

    assert(i >= 0);
    while (i) {
        i >>= 8;
        n++;
    }
    return n;
}

/***********************  sparse bitarray compression  *****************
 *
 * see also: doc/sparse_compression.rst
 */

/* Bitarray buffer size (in bytes) that can be indexed by n bytes.

   A sparse block of type n uses n bytes to index each bit.
   The decoded block size, that is the bitarray buffer size corresponding
   to a sparse block of type n, is given by BSI(n).  Using 1 byte we can
   index 256 bits which have a decoded block size of 32 bytes:

       BSI(1) = 32                         (BSI = Buffer Size Indexable)

   Moving from block type n to n + 1 multiplies the decoded block size
   by a factor of 256 (as the extra byte can index 256 times as much):

       BSI(n + 1) = 256 * BSI(n)
*/
#define BSI(n)  (((Py_ssize_t) 1) << (8 * (n) - 3))

/* segment size in bytes (not to be confused with block size, see below)

   Although of little practical value, the code will work for
   values of SEGSIZE of: 8, 16, 32
   BSI(n) must be divisible by SEGSIZE.  So, 32 must be divisible by SEGSIZE.
   SEGSIZE must also be divisible by the word size sizeof(uint64_t) = 8. */
#define SEGSIZE  32

/* number of segments for given bitarray */
#define NSEG(self)  ((Py_SIZE(self) + SEGSIZE - 1) / SEGSIZE)

/* Calculate an array with the running totals (rts) for 256 bit segments.
   Note that we call these "segments", as opposed to "blocks", in order to
   avoid confusion with encode blocks.

   0           1           2           3           4   index in rts array, i
   +-----------+-----------+-----------+-----------+
   |      5    |      0    |      3    |      4    |   segment population
   |           |           |           |           |
   |  [0:256]  | [256:512] | [512:768] | [768:987] |   bitarray slice
   +-----------+-----------+-----------+-----------+
   0           5           5           8          12   running totals, rts[i]

   In this example we have a bitarray of length nbits = 987.  Note that:

     * The number of segments is given by NSEG(self).
       Here we have 4 segments: NSEG(self) = 4

     * The rts array has always NSEG(self) + 1 elements, such that
       last element is always indexed by NSEG(self).

     * The element rts[0] is always zero.

     * The last element rts[NSEG(self)] is always the total count.
       Here: rts[NSEG(self)] = rts[4] = 12

     * The last segment may be partial.  In that case, its size it given
       by nbits % 256.  Here: nbits % 256 = 987 % 256 = 219

   As each segment (at large) covers 256 bits (32 bytes), and each element
   in the running totals array takes up 8 bytes (on a 64-bit machine) the
   additional memory to accommodate the rts array is therefore 1/4 of the
   bitarray's memory.
   However, calculating this array upfront allows sc_count() to
   simply look up two entries from the array and take their difference.
   Thus, the speedup is significant.

   The function sc_write_indices() also takes advantage of the running
   totals array.  It loops over segments and skips to the next segment as
   soon as the index count (population) of the current segment is reached.
*/
static Py_ssize_t *
sc_rts(bitarrayobject *a)
{
    const Py_ssize_t n_seg = NSEG(a);         /* total number of segments */
    const Py_ssize_t c_seg = a->nbits / (8 * SEGSIZE); /* complete segments */
    char zeros[SEGSIZE];                      /* segment with only zeros */
    char *buff = a->ob_item;                  /* buffer in current segment */
    Py_ssize_t cnt = 0;                       /* current count */
    Py_ssize_t *res, m;

    memset(zeros, 0x00, SEGSIZE);
    res = PyMem_New(Py_ssize_t, n_seg + 1);
    if (res == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    for (m = 0; m < c_seg; m++, buff += SEGSIZE) {  /* complete segments */
        res[m] = cnt;
        assert(buff + SEGSIZE <= a->ob_item + Py_SIZE(a));
        if (memcmp(buff, zeros, SEGSIZE))  /* segment has not only zeros */
            cnt += popcnt_words((uint64_t *) buff, SEGSIZE / 8);
    }
    res[c_seg] = cnt;

    if (n_seg > c_seg) {           /* we have a final partial segment */
        cnt += count_from_word(a, c_seg * SEGSIZE / 8);
        res[n_seg] = cnt;
    }
    return res;
}

/* expose sc_rts() to Python during debug mode for testing */
#ifndef NDEBUG
static PyObject *
module_sc_rts(PyObject *module, PyObject *obj)
{
    PyObject *list;
    bitarrayobject *a;
    Py_ssize_t *rts, i;

    assert(bitarray_Check(obj));
    a = (bitarrayobject *) obj;
    if ((rts = sc_rts(a)) == NULL)
        return NULL;

    if ((list = PyList_New(NSEG(a) + 1)) == NULL)
        goto error;

    for (i = 0; i <= NSEG(a); i++) {
        PyObject *item = PyLong_FromSsize_t(rts[i]);
        if (item == NULL)
            goto error;
        PyList_SET_ITEM(list, i, item);
    }
    PyMem_Free(rts);
    return list;
 error:
    Py_XDECREF(list);
    PyMem_Free(rts);
    return NULL;
}
#endif  /* NDEBUG */


/* Return population count for the decoded block size of type n.
   Roughly equivalent to the Python expression:

      a.count(1, 8 * offset, 8 * offset + (1 << (8 * n)))

   The offset must be divisible by SEGSIZE, as this functions makes use of
   running totals, stored in rts[]. */
static Py_ssize_t
sc_count(bitarrayobject *a, Py_ssize_t *rts, Py_ssize_t offset, int n)
{
    const Py_ssize_t i = offset / SEGSIZE;     /* indices into rts[] */
    const Py_ssize_t j = Py_MIN(i + BSI(n) / SEGSIZE, NSEG(a));

    assert(offset % SEGSIZE == 0 && 1 <= n && n <= 4);
    assert(0 <= i && i <= j && j <= NSEG(a));
    return rts[j] - rts[i];
}

/* Write a raw block, and return number of bytes copied.
   Note that the encoded block size is the return value + 1 (the head byte).

   The header byte is in range(0x01, 0xa0).
     * range(0x01, 0x20) number of raw bytes
     * range(0x20, 0xa0) number of 32-byte segments */
static int
sc_write_raw(char *str, bitarrayobject *a, Py_ssize_t *rts, Py_ssize_t offset)
{
    const Py_ssize_t nbytes = Py_SIZE(a) - offset;  /* remaining bytes */
    Py_ssize_t k = Py_MIN(32, nbytes);

    assert(nbytes > 0);
    if (k == 32) {
        /* The first 32 bytes are better represented using raw bytes.
           Now check up to the next 127 (32-byte) segments. */
        const Py_ssize_t kmax = Py_MIN(32 * 128, nbytes);
        while (k + 32 <= kmax && sc_count(a, rts, offset + k, 1) >= 32)
            k += 32;
    }
    assert(0 < k && k <= 32 * 128 && k <= nbytes);
    assert(k >= 32 || k == nbytes);
    assert(k <= 32 || k % 32 == 0);

    /* block header */
    *str = (char) (k <= 32 ? k : k / 32 + 31);

    /* block data */
    assert(offset + k <= Py_SIZE(a));
    memcpy(str + 1, a->ob_item + offset, (size_t) k);
    return (int) k;
}

/* Write 'k' indices (of 'n' bytes each) into buffer 'str'.
   Note that 'n' (which is also the block type) has been selected
   (in sc_encode_block()) such that:

       k = sc_count(a, rts, offset, n) < 256
*/
static void
sc_write_indices(char *str, bitarrayobject *a, Py_ssize_t *rts,
                 Py_ssize_t offset, int n, int k)
{
    const char *str_stop = str + n * k;  /* stop position in buffer 'str' */
    const char *buff = a->ob_item + offset;
    Py_ssize_t m;

    assert(0 < k && k < 256);  /* note that k cannot be 0 in this function */
    assert(k == sc_count(a, rts, offset, n));   /* see above */

    rts += offset / SEGSIZE;   /* rts index relative to offset now */

    for (m = 0;;) {  /* loop segments */
        Py_ssize_t i, ni;

        assert(m + offset / SEGSIZE < NSEG(a));
        /* number of indices in this segment, i.e. the segment population */
        if ((ni = rts[m + 1] - rts[m]) == 0)
            goto next_segment;

        for (i = m * SEGSIZE;; i++) {  /* loop bytes in segment */
            int j;

            assert(i < (m + 1) * SEGSIZE && i + offset < Py_SIZE(a));
            if (buff[i] == 0x00)
                continue;

            for (j = 0; j < 8; j++) {  /* loop bits */
                assert(8 * (offset + i) + j < a->nbits);
                if (buff[i] & BITMASK(a, j)) {
                    write_n(str, n, 8 * i + j);
                    str += n;
                    if (--ni == 0) {
                        /* we have encountered all indices in this segment */
                        if (str == str_stop)
                            return;
                        goto next_segment;
                    }
                }
            }
        }
        Py_UNREACHABLE();
    next_segment:
        m++;
    }
    Py_UNREACHABLE();
}

/* Write a sparse block of type 'n' with 'k' indices.
   Return number of bytes written to buffer 'str' (encoded block size).
   Note that the decoded block size is always BSI(n). */
static Py_ssize_t
sc_write_sparse(char *str, bitarrayobject *a, Py_ssize_t *rts,
                Py_ssize_t offset, int n, int k)
{
    int len = 0;

    assert(1 <= n && n <= 4);
    assert(0 <= k && k < 256);

    /* write block header */
    if (n == 1) {                        /* type 1 - one header byte */
        assert(k < 32);
        str[len++] = (char) (0xa0 + k);  /* index count in 0xa0 .. 0xbf */
    }
    else {                               /* type 2, 3, 4 - two header bytes */
        str[len++] = (char) (0xc0 + n);  /* block type */
        str[len++] = (char) k;           /* index count */
    }
    if (k == 0)  /* no index bytes - sc_write_sparse() does not allow k = 0 */
        return len;

    /* write block data - k indices, n bytes per index */
    sc_write_indices(str + len, a, rts, offset, n, k);
    return len + n * k;
}

/* Encode one block (starting at offset) and return offset increment,
   i.e. the decoded block size.
   The output is written into buffer 'str' and 'len' is increased.

   Notes:

   - 32 index bytes take up as much space as a raw buffer of 32 bytes.
     Hence, if the bit count of the first 32 bytes of the bitarray buffer
     is greater or equal to 32, we choose a raw block (type 0).

   - Arguably, n index bytes always take up as much space as n raw bytes.
     So what makes 32 special here?  A bitarray with a 32 byte buffer has
     256 items (bits), and these 256 bits can be addressed using one
     index byte.  That is, BSI(1) = 32, see above.
     This is also the reason, why the index count of type 1 blocks is limited
     to below 32.

   - If a raw block is used, we check if up to the next 127 32-byte segments
     are also suitable for raw encoding, see sc_write_raw().
     Therefore, we have type 0 blocks with up to 128 * 32 = 4096 raw bytes.

   - If no raw block was used, we move on to deciding which type of sparse
     representation to use.  Starting at type n = 1, we do this by first
     calculating the population count for the decoded block size of
     the *next* block type n+1.
     If this population is larger than 255 (too large for the count byte) we
     have to stick with type n.
     Otherwise we compare the encoded sizes of (a) sticking with
     many (up to 256) blocks of type n, and (b) moving to a single block of
     type n+1.  These sizes are calculated as follows:

     (a) The encoded size of many blocks of type n is given by:

             header_size  *  number_of_blocks   +   n  *  population

         Regardless of the exact index count for each block, the total size
         of the index bytes is (n * population), as all blocks are of type n.
         The number_of_blocks is 256 (unless limited by the bitarray size).
         The header_size is only 1 byte for type 1 and 2 bytes otherwise.

     (b) The encoded size of a single block of type n+1 is:

             header_size   +   (n + 1)  *  population

         As n >= 1, the header_size will is always 2 bytes here.

   - As we only need to know which of these sizes is bigger, we can
     subtract (n * population) from both sizes.  Hence, the costs are:
       (a)  header_size * number_of_blocks
       (b)  header_size + population

     The question of whether to choose type n or type n+1 ultimately comes
     down to whether the additional byte for each index is less expensive than
     having additional headers.
 */
static Py_ssize_t
sc_encode_block(char *str, Py_ssize_t *len,
                bitarrayobject *a, Py_ssize_t *rts, Py_ssize_t offset)
{
    const Py_ssize_t nbytes = Py_SIZE(a) - offset;  /* remaining bytes */
    int count, n;

    assert(nbytes > 0);

    count = (int) sc_count(a, rts, offset, 1);
    /* the number of index bytes exceeds the number of raw bytes */
    if (count >= Py_MIN(32, nbytes)) {           /* type 0 - raw bytes */
        int k = sc_write_raw(str + *len, a, rts, offset);
        *len += 1 + k;
        return k;
    }

    for (n = 1; n < 4; n++) {
        Py_ssize_t next_count, nblocks, cost_a, cost_b;

        /* population for next block type n+1 */
        next_count = sc_count(a, rts, offset, n + 1);
        if (next_count > 255)
            /* too many index bytes for next block type n+1 - use type n */
            break;

        /* number of blocks of type n */
        nblocks = Py_MIN(256, (nbytes - 1) / BSI(n) + 1);
        /* cost of nblocks blocks of type n */
        cost_a = (n == 1 ? 1 : 2) * nblocks;
        /* cost of a single block of type n+1 */
        cost_b = 2 + next_count;

        if (cost_b >= cost_a)
            /* block type n+1 is equally or more expensive - use type n */
            break;

        /* we proceed with type n+1 - we already calculated its population */
        count = (int) next_count;
    }

    *len += sc_write_sparse(str + *len, a, rts, offset, n, count);
    return BSI(n);
}

/* write header and return number or bytes written to buffer 'str' */
static int
sc_encode_header(char *str, bitarrayobject *a)
{
    int len;

    len = byte_length(a->nbits);
    *str = (IS_BE(a) ? 0x10 : 0x00) | ((char) len);
    write_n(str + 1, len, a->nbits);

    return 1 + len;
}

/* initial size of output buffer, and amount by which we increase our
   allocation if we run out */
#define ALLOC_SIZE  32768

static PyObject *
sc_encode(PyObject *module, PyObject *obj)
{
    PyObject *out;
    char *str;                  /* output buffer */
    Py_ssize_t len = 0;         /* bytes written into output buffer */
    bitarrayobject *a;
    Py_ssize_t offset = 0;      /* block offset into bitarray a in bytes */
    Py_ssize_t *rts;            /* running totals of segments */
    Py_ssize_t total;           /* total population count of bitarray */

    if (ensure_bitarray(obj) < 0)
        return NULL;

    a = (bitarrayobject *) obj;
    set_padbits(a);
    if ((rts = sc_rts(a)) == NULL)
        return NULL;

    if ((out = PyBytes_FromStringAndSize(NULL, ALLOC_SIZE)) == NULL)
        goto error;

    str = PyBytes_AS_STRING(out);
    len += sc_encode_header(str, a);

    total = rts[NSEG(a)];
    /* encode blocks as long as we haven't reached the end of the bitarray
       and haven't reached the total population count yet */
    while (offset < Py_SIZE(a) && rts[offset / SEGSIZE] != total) {
        Py_ssize_t allocated = PyBytes_GET_SIZE(out);

        /* Make sure we have enough memory in output buffer for next block.
           The largest block possible is a type 0 block with 128 segments.
           Its size is: 1 head bytes + 128 * 32 raw bytes.
           Plus, we also may have the stop byte. */
        if (allocated < len + 1 + 128 * 32 + 1) {
            if (_PyBytes_Resize(&out, allocated + ALLOC_SIZE) < 0)
                goto error;
            str = PyBytes_AS_STRING(out);
        }
        offset += sc_encode_block(str, &len, a, rts, offset);
    }
    PyMem_Free(rts);
    str[len++] = 0x00;          /* add stop byte */

    if (_PyBytes_Resize(&out, len) < 0)
        return NULL;

    return out;

 error:
    PyMem_Free(rts);
    return NULL;
}
#undef ALLOC_SIZE

PyDoc_STRVAR(sc_encode_doc,
"sc_encode(bitarray, /) -> bytes\n\
\n\
Compress a sparse bitarray and return its binary representation.\n\
This representation is useful for efficiently storing sparse bitarrays.\n\
Use `sc_decode()` for decompressing (decoding).");


/* read header from 'iter' and set 'endian' and 'nbits', return 0 on success
   and -1 of failure (after setting exception) */
static int
sc_decode_header(PyObject *iter, int *endian, Py_ssize_t *nbits)
{
    int head, len;

    if ((head = next_char(iter)) < 0)
        return -1;

    if (head & 0xe0) {
        PyErr_Format(PyExc_ValueError, "invalid header: 0x%02x", head);
        return -1;
    }

    *endian = head & 0x10 ? ENDIAN_BIG : ENDIAN_LITTLE;
    len = head & 0x0f;

    if (len > (int) sizeof(Py_ssize_t)) {
        PyErr_Format(PyExc_OverflowError, "sizeof(Py_ssize_t) = %d: cannot "
                     "read %d bytes", (int) sizeof(Py_ssize_t), len);
        return -1;
    }
    if ((*nbits = read_n(iter, len)) < 0)
        return -1;

    return 0;
}

/* Read k bytes from iter and set elements in bitarray.
   Return the size of offset increment in bytes, or -1 on failure. */
static Py_ssize_t
sc_read_raw(bitarrayobject *a, Py_ssize_t offset, PyObject *iter, int k)
{
    char *buff = a->ob_item + offset;
    int i, c;

    assert(1 <= k && k <= 32 * 128);
    if (offset + k > Py_SIZE(a)) {
        PyErr_Format(PyExc_ValueError, "decode error (raw): %zd + %d > %zd",
                     offset, k, Py_SIZE(a));
        return -1;
    }
    for (i = 0; i < k; i++) {
        if ((c = next_char(iter)) < 0)
            return -1;
        buff[i] = (char) c;
    }
    return k;
}

/* Read n * k bytes from iter and set elements in bitarray.
   Return size of offset increment in bytes, or -1 on failure. */
static Py_ssize_t
sc_read_sparse(bitarrayobject *a, Py_ssize_t offset, PyObject *iter,
               int n, int k)
{
    assert(1 <= n && n <= 4 && k >= 0);
    while (k--) {
        Py_ssize_t i;

        if ((i = read_n(iter, n)) < 0)
            return -1;

        i += 8 * offset;
        /* also check for negative value as offset might cause overflow */
        if (i < 0 || i >= a->nbits) {
            PyErr_Format(PyExc_ValueError, "decode error (n=%d): %zd >= %zd",
                         n, i, a->nbits);
            return -1;
        }
        setbit(a, i, 1);
    }
    return BSI(n);
}

/* Decode one block: consume iter and set bitarray buffer starting at
   offset.  Return decoded block size, or -1 on failure. */
static Py_ssize_t
sc_decode_block(bitarrayobject *a, Py_ssize_t offset, PyObject *iter)
{
    int head, k;

    if ((head = next_char(iter)) < 0)
        return -1;

    if (head < 0xa0) {                     /* type 0 - 0x00 .. 0x9f */
        if (head == 0)  /* stop byte */
            return 0;

        k = head <= 0x20 ? head : 32 * (head - 31);
        return sc_read_raw(a, offset, iter, k);
    }

    if (head < 0xc0)                       /* type 1 - 0xa0 .. 0xbf */
        return sc_read_sparse(a, offset, iter, 1, head - 0xa0);

    if (0xc2 <= head && head <= 0xc4) {    /* type 2 .. 4 - 0xc2 .. 0xc4 */
        if ((k = next_char(iter)) < 0)     /* index count byte */
            return -1;

        return sc_read_sparse(a, offset, iter, head - 0xc0, k);
    }

    PyErr_Format(PyExc_ValueError, "invalid block head: 0x%02x", head);
    return -1;
}

static PyObject *
sc_decode(PyObject *module, PyObject *obj)
{
    PyObject *iter;
    bitarrayobject *a = NULL;
    Py_ssize_t offset = 0, increase, nbits;
    int endian;

    if ((iter = PyObject_GetIter(obj)) == NULL)
        return PyErr_Format(PyExc_TypeError, "'%s' object is not iterable",
                            Py_TYPE(obj)->tp_name);

    if (sc_decode_header(iter, &endian, &nbits) < 0)
        goto error;

    /* create bitarray of length nbits */
    if ((a = new_bitarray(nbits, Py_None, 0)) == NULL)
        goto error;
    a->endian = endian;

    /* consume blocks until stop byte is encountered */
    while ((increase = sc_decode_block(a, offset, iter))) {
        if (increase < 0)
            goto error;
        offset += increase;
    }
    Py_DECREF(iter);
    return (PyObject *) a;

 error:
    Py_DECREF(iter);
    Py_XDECREF((PyObject *) a);
    return NULL;
}

PyDoc_STRVAR(sc_decode_doc,
"sc_decode(stream, /) -> bitarray\n\
\n\
Decompress binary stream (an integer iterator, or bytes-like object) of a\n\
sparse compressed (`sc`) bitarray, and return the decoded  bitarray.\n\
This function consumes only one bitarray and leaves the remaining stream\n\
untouched.  Use `sc_encode()` for compressing (encoding).");

#undef BSI
#undef NSEG

/* ------------------- variable length bitarray format ----------------- */

/* LEN_PAD_BITS is always 3 - the number of bits (length) that is necessary
   to represent the number of pad bits.  The number of padding bits itself is
   called 'padding' below.

   'padding' refers to the pad bits within the variable length format.
   This is not the same as the pad bits of the actual bitarray.
   For example, b'\x10' has padding = 1, and decodes to bitarray('000'),
   which has 5 pad bits.  'padding' can take values to up 6.
 */
#define LEN_PAD_BITS  3

/* initial number of bits we allocate in vl_decode(), and amount by which
   we increase our allocation by in vl_decode_core() if we run out */
#define ALLOC_BITS  1024

/* Consume 'iter' while extending bitarray 'a'.
   Return 0 on success.  On failure, set exception and return -1. */
static int
vl_decode_core(bitarrayobject *a, PyObject *iter)
{
    Py_ssize_t i = 0;        /* bit counter */
    int padding, k, c;

    if ((c = next_char(iter)) < 0)           /* head byte */
        return -1;

    padding = (c & 0x70) >> 4;
    if (padding == 7 || ((c & 0x80) == 0 && padding > 4)) {
        PyErr_Format(PyExc_ValueError, "invalid head byte: 0x%02x", c);
        return -1;
    }
    for (k = 0; k < 4; k++)
        setbit(a, i++, (0x08 >> k) & c);

    while (c & 0x80) {
        if ((c = next_char(iter)) < 0)
            return -1;

        /* ensure bitarray is large enough to accommodate seven more bits */
        if (a->nbits < i + 7 && resize_lite(a, a->nbits + ALLOC_BITS) < 0)
            return -1;
        assert(i + 6 < a->nbits);

        for (k = 0; k < 7; k++)
            setbit(a, i++, (0x40 >> k) & c);
    }
    /* set final length of bitarray */
    return resize_lite(a, i - padding);
}

static PyObject *
vl_decode(PyObject *module, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"", "endian", NULL};
    PyObject *obj, *iter, *endian = Py_None;
    bitarrayobject *a;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O:vl_decode", kwlist,
                                     &obj, &endian))
        return NULL;

    iter = PyObject_GetIter(obj);
    if (iter == NULL)
        return PyErr_Format(PyExc_TypeError, "'%s' object is not iterable",
                            Py_TYPE(obj)->tp_name);

    a = new_bitarray(ALLOC_BITS, endian, -1);
    if (a == NULL)
        goto error;

    if (vl_decode_core(a, iter) < 0)         /* do actual decoding work */
        goto error;

    Py_DECREF(iter);
    return (PyObject *) a;

 error:
    Py_DECREF(iter);
    Py_XDECREF((PyObject *) a);
    return NULL;
}
#undef ALLOC_BITS

PyDoc_STRVAR(vl_decode_doc,
"vl_decode(stream, /, endian=None) -> bitarray\n\
\n\
Decode binary stream (an integer iterator, or bytes-like object), and\n\
return the decoded bitarray.  This function consumes only one bitarray and\n\
leaves the remaining stream untouched.  Use `vl_encode()` for encoding.");


static PyObject *
vl_encode(PyObject *module, PyObject *obj)
{
    PyObject *result;
    bitarrayobject *a;
    Py_ssize_t nbits, n, i, j = 0;  /* j: byte counter */
    int padding;
    char *str;

    if (ensure_bitarray(obj) < 0)
        return NULL;

    a = (bitarrayobject *) obj;
    nbits = a->nbits;
    n = (nbits + LEN_PAD_BITS + 6) / 7;  /* number of resulting bytes */
    padding = (int) (7 * n - LEN_PAD_BITS - nbits);

    result = PyBytes_FromStringAndSize(NULL, n);
    if (result == NULL)
        return NULL;

    str = PyBytes_AsString(result);
    str[0] = nbits > 4 ? 0x80 : 0x00;  /* lead bit */
    str[0] |= padding << 4;            /* encode padding */
    for (i = 0; i < 4 && i < nbits; i++)
        str[0] |= (0x08 >> i) * getbit(a, i);

    for (i = 4; i < nbits; i++) {
        int k = (i - 4) % 7;

        if (k == 0) {
            j++;
            str[j] = j < n - 1 ? 0x80 : 0x00;  /* lead bit */
        }
        str[j] |= (0x40 >> k) * getbit(a, i);
    }
    assert(j == n - 1);

    return result;
}

PyDoc_STRVAR(vl_encode_doc,
"vl_encode(bitarray, /) -> bytes\n\
\n\
Return variable length binary representation of bitarray.\n\
This representation is useful for efficiently storing small bitarray\n\
in a binary stream.  Use `vl_decode()` for decoding.");

#undef LEN_PAD_BITS

/* ----------------------- canonical Huffman decoder ------------------- */

/*
   The decode iterator object includes the Huffman code decoding tables:
   - count[1..MAXBITS] is the number of symbols of each length, which for a
     canonical code are stepped through in order.  count[0] is not used.
   - symbol is a Python sequence of the symbols in canonical order
     where the number of entries is the sum of the counts in count[].
 */
#define MAXBITS  31                  /* maximum bit length in a code */

typedef struct {
    PyObject_HEAD
    bitarrayobject *array;           /* bitarray we're decoding */
    Py_ssize_t index;                /* current index in bitarray */
    int count[MAXBITS + 1];          /* number of symbols of each length */
    PyObject *symbol;                /* canonical ordered symbols */
} chdi_obj;                          /* canonical Huffman decode iterator */

static PyTypeObject CHDI_Type;

/* set elements in count (from sequence) and return their sum,
   or -1 on error after setting exception */
static Py_ssize_t
set_count(int *count, PyObject *sequence)
{
    Py_ssize_t n, res = 0;
    int i;

    if ((n = PySequence_Size(sequence)) < 0)
        return -1;

    if (n > MAXBITS + 1) {
        PyErr_Format(PyExc_ValueError, "len(count) cannot be larger than %d",
                     MAXBITS + 1);
        return -1;
    }

    memset(count, 0, sizeof(int) * (MAXBITS + 1));
    for (i = 1; i < n; i++) {
        PyObject *item;
        Py_ssize_t c;

        if ((item = PySequence_GetItem(sequence, i)) == NULL)
            return -1;
        c = PyNumber_AsSsize_t(item, PyExc_OverflowError);
        Py_DECREF(item);
        if (c == -1 && PyErr_Occurred())
            return -1;
        if (c >> i && (c - 1) >> i) {
            PyErr_Format(PyExc_ValueError, "count[%d] not in [0..%zu], "
                         "got %zd", i, ((size_t) 1) << i, c);
            return -1;
        }
        count[i] = (int) c;
        res += c;
    }
    return res;
}

/* create a new initialized canonical Huffman decode iterator object */
static PyObject *
chdi_new(PyObject *module, PyObject *args)
{
    PyObject *a, *count, *symbol;
    Py_ssize_t count_sum;
    chdi_obj *it;       /* iterator object to be returned */

    if (!PyArg_ParseTuple(args, "O!OO:canonical_decode",
                          bitarray_type, &a, &count, &symbol))
        return NULL;
    if (!PySequence_Check(count))
        return PyErr_Format(PyExc_TypeError, "count expected to be sequence, "
                            "got '%s'", Py_TYPE(count)->tp_name);

    symbol = PySequence_Fast(symbol, "symbol not iterable");
    if (symbol == NULL)
        return NULL;

    it = PyObject_GC_New(chdi_obj, &CHDI_Type);
    if (it == NULL)
        goto error;

    if ((count_sum = set_count(it->count, count)) < 0)
        goto error;

    if (count_sum != PySequence_Size(symbol)) {
        PyErr_Format(PyExc_ValueError, "sum(count) = %zd, but len(symbol) "
                     "= %zd", count_sum, PySequence_Size(symbol));
        goto error;
    }
    Py_INCREF(a);
    it->array = (bitarrayobject *) a;
    it->index = 0;
    /* PySequence_Fast() returns a new reference, so no Py_INCREF here */
    it->symbol = symbol;

    PyObject_GC_Track(it);
    return (PyObject *) it;

 error:
    it->array = NULL;
    Py_XDECREF(symbol);
    it->symbol = NULL;
    Py_DECREF(it);
    return NULL;
}

PyDoc_STRVAR(chdi_doc,
"canonical_decode(bitarray, count, symbol, /) -> iterator\n\
\n\
Decode bitarray using canonical Huffman decoding tables\n\
where `count` is a sequence containing the number of symbols of each length\n\
and `symbol` is a sequence of symbols in canonical order.");

/* This function is based on the function decode() in:
   https://github.com/madler/zlib/blob/master/contrib/puff/puff.c */
static PyObject *
chdi_next(chdi_obj *it)
{
    Py_ssize_t nbits = it->array->nbits;
    int len;    /* current number of bits in code */
    int code;   /* current code (of len bits) */
    int first;  /* first code of length len */
    int count;  /* number of codes of length len */
    int index;  /* index of first code of length len in symbol list */

    if (it->index >= nbits)           /* no bits - stop iteration */
        return NULL;

    code = first = index = 0;
    for (len = 1; len <= MAXBITS; len++) {
        code |= getbit(it->array, it->index++);
        count = it->count[len];
        assert(code - first >= 0);
        if (code - first < count) {   /* if length len, return symbol */
            return PySequence_ITEM(it->symbol, index + (code - first));
        }
        index += count;               /* else update for next length */
        first += count;
        first <<= 1;
        code <<= 1;

        if (it->index >= nbits && len != MAXBITS) {
            PyErr_SetString(PyExc_ValueError, "reached end of bitarray");
            return NULL;
        }
    }
    PyErr_SetString(PyExc_ValueError, "ran out of codes");
    return NULL;
}

static void
chdi_dealloc(chdi_obj *it)
{
    PyObject_GC_UnTrack(it);
    Py_XDECREF(it->array);
    Py_XDECREF(it->symbol);
    PyObject_GC_Del(it);
}

static int
chdi_traverse(chdi_obj *it, visitproc visit, void *arg)
{
    Py_VISIT(it->array);
    Py_VISIT(it->symbol);
    return 0;
}

#undef MAXBITS

static PyTypeObject CHDI_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "bitarray.util.canonical_decodeiter",     /* tp_name */
    sizeof(chdi_obj),                         /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    (destructor) chdi_dealloc,                /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    PyObject_GenericGetAttr,                  /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,  /* tp_flags */
    0,                                        /* tp_doc */
    (traverseproc) chdi_traverse,             /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    PyObject_SelfIter,                        /* tp_iter */
    (iternextfunc) chdi_next,                 /* tp_iternext */
    0,                                        /* tp_methods */
};

/* ---------- module functions exposed in debug mode for testing ------- */

#ifndef NDEBUG

static PyObject *
module_setup_table(PyObject *module, PyObject *obj)
{
    char table[256];

    assert(PyUnicode_Check(obj));
    assert(PyUnicode_GET_LENGTH(obj) == 1);
    setup_table(table, PyUnicode_READ_CHAR(obj, 0));
    return PyBytes_FromStringAndSize(table, 256);
}

/* Return zlw(a) as a new bitarray, rather than an int object.
   This makes testing easier, because the int result would depend
   on the machine byteorder. */
static PyObject *
module_zlw(PyObject *module, PyObject *obj)
{
    bitarrayobject *a, *res;
    uint64_t w;

    assert(bitarray_Check(obj));
    a = (bitarrayobject *) obj;
    w = zlw(a);
    if ((res = new_bitarray(64, Py_None, -1)) == NULL)
        return NULL;
    res->endian = a->endian;
    memcpy(res->ob_item, &w, 8);
    return (PyObject *) res;
}

static PyObject *
module_cfw(PyObject *module, PyObject *args)  /* count_from_word() */
{
    bitarrayobject *a;
    Py_ssize_t i;

    if (!PyArg_ParseTuple(args, "O!n", bitarray_type, (PyObject *) &a, &i))
        return NULL;
    return PyLong_FromSsize_t(count_from_word(a, i));
}

static PyObject *
module_d2i(PyObject *module, PyObject *args)
{
    int m;
    char c;

    if (!PyArg_ParseTuple(args, "ic", &m, &c))
        return NULL;
    return PyLong_FromLong(digit_to_int(m, c));
}

static PyObject *
module_read_n(PyObject *module, PyObject *args)
{
    PyObject *iter;
    Py_ssize_t i;
    int n;

    if (!PyArg_ParseTuple(args, "Oi", &iter, &n))
        return NULL;
    if ((i = read_n(iter, n)) < 0)
        return NULL;
    return PyLong_FromSsize_t(i);
}

static PyObject *
module_write_n(PyObject *module, PyObject *args)
{
    PyObject *result;
    char *str;
    Py_ssize_t i;
    int n;

    if (!PyArg_ParseTuple(args, "in", &n, &i))
        return NULL;
    if ((result = PyBytes_FromStringAndSize(NULL, n)) == NULL)
        return NULL;
    str = PyBytes_AsString(result);
    write_n(str, n, i);
    return result;
}

#endif  /* NDEBUG */


static PyMethodDef module_functions[] = {
    {"zeros",     (PyCFunction) zeros,     METH_KEYWORDS |
                                           METH_VARARGS, zeros_doc},
    {"ones",      (PyCFunction) ones,      METH_KEYWORDS |
                                           METH_VARARGS, ones_doc},
    {"count_n",   (PyCFunction) count_n,   METH_VARARGS, count_n_doc},
    {"parity",    (PyCFunction) parity,    METH_O,       parity_doc},
    {"_ssqi",     (PyCFunction) ssqi,      METH_VARARGS, 0},
    {"xor_indices", (PyCFunction) xor_indices, METH_O,       xor_indices_doc},
    {"count_and", (PyCFunction) count_and, METH_VARARGS, count_and_doc},
    {"count_or",  (PyCFunction) count_or,  METH_VARARGS, count_or_doc},
    {"count_xor", (PyCFunction) count_xor, METH_VARARGS, count_xor_doc},
    {"any_and",   (PyCFunction) any_and,   METH_VARARGS, any_and_doc},
    {"subset",    (PyCFunction) subset,    METH_VARARGS, subset_doc},
    {"correspond_all",
                  (PyCFunction) correspond_all,
                                           METH_VARARGS, correspond_all_doc},
    {"byteswap",  (PyCFunction) byteswap,  METH_VARARGS, byteswap_doc},
    {"serialize",   (PyCFunction) serialize,   METH_O,   serialize_doc},
    {"deserialize", (PyCFunction) deserialize, METH_O,   deserialize_doc},
    {"ba2hex",    (PyCFunction) ba2hex,    METH_KEYWORDS |
                                           METH_VARARGS, ba2hex_doc},
    {"hex2ba",    (PyCFunction) hex2ba,    METH_KEYWORDS |
                                           METH_VARARGS, hex2ba_doc},
    {"ba2base",   (PyCFunction) ba2base,   METH_KEYWORDS |
                                           METH_VARARGS, ba2base_doc},
    {"base2ba",   (PyCFunction) base2ba,   METH_KEYWORDS |
                                           METH_VARARGS, base2ba_doc},
    {"sc_encode", (PyCFunction) sc_encode, METH_O,       sc_encode_doc},
    {"sc_decode", (PyCFunction) sc_decode, METH_O,       sc_decode_doc},
    {"vl_encode", (PyCFunction) vl_encode, METH_O,       vl_encode_doc},
    {"vl_decode", (PyCFunction) vl_decode, METH_KEYWORDS |
                                           METH_VARARGS, vl_decode_doc},
    {"canonical_decode",
                  (PyCFunction) chdi_new,  METH_VARARGS, chdi_doc},

#ifndef NDEBUG
    /* functions exposed in debug mode for testing */
    {"_setup_table", (PyCFunction) module_setup_table, METH_O,       0},
    {"_zlw",         (PyCFunction) module_zlw,         METH_O,       0},
    {"_cfw",         (PyCFunction) module_cfw,         METH_VARARGS, 0},
    {"_d2i",         (PyCFunction) module_d2i,         METH_VARARGS, 0},
    {"_read_n",      (PyCFunction) module_read_n,      METH_VARARGS, 0},
    {"_write_n",     (PyCFunction) module_write_n,     METH_VARARGS, 0},
    {"_sc_rts",      (PyCFunction) module_sc_rts,      METH_O,       0},
#endif

    {NULL,        NULL}  /* sentinel */
};

/******************************* Install Module ***************************/

static PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_util", 0, -1, module_functions,
};

PyMODINIT_FUNC
PyInit__util(void)
{
    PyObject *m;

    bitarray_type = (PyTypeObject *) bitarray_module_attr("bitarray");
    if (bitarray_type == NULL)
        return NULL;

    if ((m = PyModule_Create(&moduledef)) == NULL)
        return NULL;

#ifdef Py_GIL_DISABLED
    PyUnstable_Module_SetGIL(m, Py_MOD_GIL_NOT_USED);
#endif

    if (PyType_Ready(&CHDI_Type) < 0)
        return NULL;
    Py_SET_TYPE(&CHDI_Type, &PyType_Type);

#ifndef NDEBUG  /* expose segment size in debug mode for testing */
    if (PyModule_AddIntConstant(m, "_SEGSIZE", SEGSIZE) < 0)
        return NULL;
#endif

    return m;
}
