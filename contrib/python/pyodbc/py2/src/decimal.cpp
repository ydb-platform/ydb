
#include "pyodbc.h"
#include "wrapper.h"
#include "textenc.h"
#include "decimal.h"

static PyObject* decimal = 0;
// The Decimal constructor.

static PyObject* re_sub = 0;
static PyObject* re_compile = 0;
static PyObject* re_escape = 0;

// In Python 2.7, the 3 strings below are bytes objects.  In 3.x they are Unicode objects.


static PyObject* pDecimalPoint = 0;
// A "." object which we replace pLocaleDecimal with if they are not the same.

static PyObject* pLocaleDecimal = 0;
// The decimal character used by the locale.  This can be overridden by the user.
//
// In 2.7 this is a bytes object, otherwise unicode.

static PyObject* pLocaleDecimalEscaped = 0;
// A version of pLocaleDecimal escaped to be used in a regular expression.  (The character
// could be something special in regular expressions.)  This is zero when pLocaleDecimal is
// ".", indicating no replacement is necessary.

static PyObject* pRegExpRemove = 0;
// A regular expression that matches characters we want to remove before parsing.


bool InitializeDecimal() {
    // This is called when the module is initialized and creates globals.

    Object d(PyImport_ImportModule("decimal"));
    decimal = PyObject_GetAttrString(d, "Decimal");
    if (!decimal)
        return 0;
    Object re(PyImport_ImportModule("re"));
    re_sub     = PyObject_GetAttrString(re, "sub");
    re_escape  = PyObject_GetAttrString(re, "escape");
    re_compile = PyObject_GetAttrString(re, "compile");

    Object module(PyImport_ImportModule("locale"));
    Object ldict(PyObject_CallMethod(module, "localeconv", 0));
    Object point(PyDict_GetItemString(ldict, "decimal_point"));

    if (!point)
        return false;

#if PY_MAJOR_VERSION >= 3
    pDecimalPoint = PyUnicode_FromString(".");
#else
    pDecimalPoint = PyBytes_FromString(".");
#endif

    if (!pDecimalPoint)
        return false;

#if PY_MAJOR_VERSION >= 3
    if (!SetDecimalPoint(point))
        return false;
#else
    // In 2.7, we only support non-Unicode right now.
    if (PyBytes_Check(point))
        if (!SetDecimalPoint(point))
            return false;
#endif

    return true;
}

PyObject* GetDecimalPoint() {
    Py_INCREF(pLocaleDecimal);
    return pLocaleDecimal;
}

bool SetDecimalPoint(PyObject* pNew)
{
    if (PyObject_RichCompareBool(pNew, pDecimalPoint, Py_EQ) == 1)
    {
        // They are the same.
        Py_XDECREF(pLocaleDecimal);
        pLocaleDecimal = pDecimalPoint;
        Py_INCREF(pLocaleDecimal);

        Py_XDECREF(pLocaleDecimalEscaped);
        pLocaleDecimalEscaped = 0;
    }
    else
    {
        // They are different, so we'll need a regular expression to match it so it can be
        // replaced in getdata GetDataDecimal.

        Py_XDECREF(pLocaleDecimal);
        pLocaleDecimal = pNew;
        Py_INCREF(pLocaleDecimal);

        Object e(PyObject_CallFunctionObjArgs(re_escape, pNew, 0));
        if (!e)
            return false;

        Py_XDECREF(pLocaleDecimalEscaped);
        pLocaleDecimalEscaped = e.Detach();
    }

#if PY_MAJOR_VERSION >= 3
    Object s(PyUnicode_FromFormat("[^0-9%U-]+", pLocaleDecimal));
#else
    Object s(PyBytes_FromFormat("[^0-9%s-]+", PyString_AsString(pLocaleDecimal)));
#endif
    if (!s)
        return false;

    Object r(PyObject_CallFunctionObjArgs(re_compile, s.Get(), 0));
    if (!r)
        return false;

    Py_XDECREF(pRegExpRemove);
    pRegExpRemove = r.Detach();

    return true;
}


PyObject* DecimalFromText(const TextEnc& enc, const byte* pb, Py_ssize_t cb)
{
    // Creates a Decimal object from a text buffer.

    // The Decimal constructor requires the decimal point to be '.', so we need to convert the
    // locale's decimal to it.  We also need to remove non-decimal characters such as thousands
    // separators and currency symbols.
    //
    // Remember that the thousands separate will often be '.', so have to do this carefully.
    // We'll create a regular expression with 0-9 and whatever the thousands separator is.

    Object text(TextBufferToObject(enc, pb, cb));
    if (!text)
        return 0;

    Object cleaned(PyObject_CallMethod(pRegExpRemove, "sub", "sO", "", text.Get()));
    if (!cleaned)
        return 0;

    if (pLocaleDecimalEscaped)
    {
        Object c2(PyObject_CallFunctionObjArgs(re_sub, pLocaleDecimalEscaped, pDecimalPoint, 0));
        if (!c2)
            return 0;
        cleaned.Attach(c2.Detach());
    }

    return PyObject_CallFunctionObjArgs(decimal, cleaned.Get(), 0);
}
