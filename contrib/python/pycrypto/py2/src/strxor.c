/*
 *  strxor.c: string XOR functions
 *
 * Written in 2008 by Dwayne C. Litzenberger <dlitz@dlitz.net>
 *
 * ===================================================================
 * The contents of this file are dedicated to the public domain.  To
 * the extent that dedication to the public domain is not available,
 * everyone is granted a worldwide, perpetual, royalty-free,
 * non-exclusive license to exercise all rights associated with the
 * contents of this file for any purpose whatsoever.
 * No rights are reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ===================================================================
 */
#include "Python.h"
#include <stddef.h>
#include <assert.h>
#include <string.h>

#include "pycrypto_compat.h"

static const char rcsid[] = "$Id$";

/*
 * xor_strings - XOR two strings together to produce a third string
 *
 * dest[0..n-1] := src_a[0..n-1] ^ src_b[0..n-1]
 *
 */
static void
xor_strings(char *dest, const char *src_a, const char *src_b, size_t n)
{
    size_t i;

    /* assert no pointer overflow */
    assert(src_a + n > src_a);
    assert(src_b + n > src_b);
    assert(dest + n > dest);

    for (i = 0; i < n; i++) {
        dest[i] = src_a[i] ^ src_b[i];
    }
}

/*
 * xor_string_with_char - XOR a string with a char to produce another string
 *
 * dest[0..n-1] := src[0..n-1] ^ c
 *
 */
static void
xor_string_with_char(char *dest, const char *src, char c, size_t n)
{
    size_t i;

    /* assert no pointer overflow */
    assert(src + n > src);
    assert(dest + n > dest);

    for (i = 0; i < n; i++) {
        dest[i] = src[i] ^ c;
    }
}

/*
 * "Import assertions"
 *
 * These runtime checks are performed when this module is first initialized
 *
 */

#define IMP_ASSERT(exp) do {\
    if (!(exp)) {\
        PyErr_Format(PyExc_AssertionError, "%s:%d: assertion failure: '%s'", __FILE__, __LINE__, #exp);\
        return;\
    }\
} while(0)

static void
runtime_test(void)
{
    /* size_t should be able to represent the length of any size buffer */
    IMP_ASSERT(sizeof(size_t) == sizeof(void *));

    /* we must be able to perform the assignment (Py_ssize_t) -> (size_t)
     * as long as the value is non-negative. */
    IMP_ASSERT(sizeof(size_t) >= sizeof(Py_ssize_t));

    /* char must be one octet */
    IMP_ASSERT(sizeof(char) == 1);

    /* Perform a basic test of the xor_strings function, including a test for
     * an off-by-one bug. */
    {
        char x[7] = "\x00hello";    /* NUL + "hello" + NUL */
        char y[7] = "\xffworld";    /* 0xff + "world" + NUL */
        char z[9] = "[ABCDEFG]";    /* "[ABCDEFG]" + NUL */

        xor_strings(z+1, x, y, 7);
        IMP_ASSERT(!memcmp(z, "[\xff\x1f\x0a\x1e\x00\x0b\x00]", 9));
    }

    /* Perform a basic test of the xor_string_with_char function, including a test for
     * an off-by-one bug. */
    {
        char x[7] = "\x00hello";    /* NUL + "hello" + NUL */
        char y = 170;               /* 0xaa */
        char z[9] = "[ABCDEFG]";    /* "[ABCDEFG]" + NUL */

        xor_string_with_char(z+1, x, y, 7);
        IMP_ASSERT(!memcmp(z, "[\xaa\xc2\xcf\xc6\xc6\xc5\xaa]", 9));
    }
}

/*
 * The strxor Python function
 */

static char strxor__doc__[] =
"strxor(a:str, b:str) -> str\n"
"\n"
"Return a XOR b.  Both a and b must have the same length.\n";

static PyObject *
strxor_function(PyObject *self, PyObject *args)
{
    PyObject *a, *b, *retval;
    Py_ssize_t len_a, len_b;

    if (!PyArg_ParseTuple(args, "SS", &a, &b))
        return NULL;

    len_a = PyBytes_GET_SIZE(a);
    len_b = PyBytes_GET_SIZE(b);

    assert(len_a >= 0);
    assert(len_b >= 0);

    if (len_a != len_b) {
        PyErr_SetString(PyExc_ValueError, "length of both strings must be equal");
        return NULL;
    }

    /* Create return string */
    retval = PyBytes_FromStringAndSize(NULL, len_a);
    if (!retval) {
        return NULL;
    }

    /* retval := a ^ b */
    xor_strings(PyBytes_AS_STRING(retval), PyBytes_AS_STRING(a), PyBytes_AS_STRING(b), len_a);

    return retval;
}

/*
 * The strxor_c Python function
 */

static char strxor_c__doc__[] =
"strxor_c(s:str, c:int) -> str\n"
"\n"
"Return s XOR chr(c).  c must be in range(256).\n";

static PyObject *
strxor_c_function(PyObject *self, PyObject *args)
{
    PyObject *s, *retval;
    int c;
    Py_ssize_t length;

    if (!PyArg_ParseTuple(args, "Si", &s, &c))
        return NULL;

    if ((c < 0) || (c > 255)) {
        PyErr_SetString(PyExc_ValueError, "c must be in range(256)");
        return NULL;
    }

    length = PyBytes_GET_SIZE(s);
    assert(length >= 0);

    /* Create return string */
    retval = PyBytes_FromStringAndSize(NULL, length);
    if (!retval) {
        return NULL;
    }

    /* retval := a ^ chr(c)*length */
    xor_string_with_char(PyBytes_AS_STRING(retval), PyBytes_AS_STRING(s), (char) c, length);

    return retval;
}

/*
 * Module-level method table and module initialization function
 */

static PyMethodDef strxor_methods[] = {
    {"strxor", strxor_function, METH_VARARGS, strxor__doc__},
    {"strxor_c", strxor_c_function, METH_VARARGS, strxor_c__doc__},

    {NULL, NULL, 0, NULL}   /* end-of-list sentinel value */
};

#ifdef IS_PY3K
static struct PyModuleDef moduledef = {
	PyModuleDef_HEAD_INIT,
	"strxor",
	NULL,
	-1,
	strxor_methods,
	NULL,
	NULL,
	NULL,
	NULL
};
#endif

PyMODINIT_FUNC
#ifdef IS_PY3K
PyInit_strxor(void)
#else
initstrxor(void)
#endif
{
	PyObject *m;

    /* Initialize the module */
#ifdef IS_PY3K
    m = PyModule_Create(&moduledef);
    if (m == NULL)
       return NULL;
#else
    m = Py_InitModule("strxor", strxor_methods);
    if (m == NULL)
        return;
#endif
 
    /* Perform runtime tests */
    runtime_test();

#ifdef IS_PY3K
	return m;
#endif
}

/* vim:set ts=4 sw=4 sts=4 expandtab: */
