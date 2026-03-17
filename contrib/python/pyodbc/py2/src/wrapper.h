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

    operator PyVarObject*() { return (PyVarObject*)p; }

    operator const bool() { return p != 0; }

    PyObject* Get()
    {
        return p;
    }
};


class Tuple
    : public Object
{
private:
    Tuple(const Tuple& other) {}
    void operator=(const Tuple& other) {}

public:

    Tuple(PyObject* _p = 0)
        : Object(_p)
    {
    }

    operator PyTupleObject*()
    {
        return (PyTupleObject*)p;
    }

    PyObject*& operator[](int i)
    {
        I(p != 0);
        return PyTuple_GET_ITEM(p, i);
    }

    Py_ssize_t size() { return p ? PyTuple_GET_SIZE(p) : 0; }
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
