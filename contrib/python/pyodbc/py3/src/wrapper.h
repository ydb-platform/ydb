#ifndef _WRAPPER_H_
#define _WRAPPER_H_

class Object
{
    // This is a simple wrapper around PyObject pointers to release them when this object goes
    // out of scope.  Note that it does *not* increment the reference count on acquisition but
    // it *does* decrement the count if you don't use Detach.
    //
    // It also does not have a copy constructor and doesn't try to manage passing pointers
    // around.  This is simply used to simplify functions by allowing early exits.

    Object(const Object& illegal) { }
    void operator=(const Object& illegal) { }

protected:
    PyObject* p;

public:
    Object(PyObject* _p = 0)
    {
        p = _p;
    }

    ~Object()
    {
        Py_XDECREF(p);
    }

    Object& operator=(PyObject* pNew)
    {
        Py_XDECREF(p);
        p = pNew;
        return *this;
    }

    bool IsValid() const { return p != 0; }

    bool Attach(PyObject* _p)
    {
        // Returns true if the new pointer is non-zero.

        Py_XDECREF(p);
        p = _p;
        return (_p != 0);
    }

    PyObject* Detach()
    {
        PyObject* pT = p;
        p = 0;
        return pT;
    }

    operator PyObject*()
    {
        return p;
    }

    operator PyTupleObject*()
    {
        // This is a bit weird.  I'm surprised the PyTuple_ functions and macros don't just use
        // PyObject.
        return (PyTupleObject*)p;
    }

    operator PyVarObject*() { return (PyVarObject*)p; }

    operator const bool() { return p != 0; }

    PyObject* Get()
    {
        return p;
    }
};


#ifdef WINVER
struct RegKey
{
    HKEY hkey;

    RegKey()
    {
        hkey = 0;
    }

    ~RegKey()
    {
        if (hkey != 0)
            RegCloseKey(hkey);
    }

    operator HKEY() { return hkey; }
};
#endif

#endif // _WRAPPER_H_
