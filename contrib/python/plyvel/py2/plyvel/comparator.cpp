/*
 * Custom Python comparator callback support code for Plyvel.
 */

#include "Python.h"

#include <iostream>

#include <leveldb/comparator.h>
#include <leveldb/slice.h>

#include "comparator.h"


class PlyvelCallbackComparator : public leveldb::Comparator
{
public:

    PlyvelCallbackComparator(const char* name, PyObject* comparator) :
        name(name),
        comparator(comparator)
    {
        Py_INCREF(comparator);
        zero = PyLong_FromLong(0);

        /* LevelDB uses a background thread for compaction, and with custom
         * comparators this background thread calls back into Python code,
         * which means the GIL must be initialized. */
        PyEval_InitThreads();
    }

    ~PlyvelCallbackComparator()
    {
        Py_DECREF(comparator);
        Py_DECREF(zero);
    }

    void bailout(const char* message) const
    {
        PyErr_Print();
        std::cerr << "FATAL ERROR: " << message << std::endl;
        std::cerr << "Aborting to avoid database corruption..." << std::endl;
        abort();
    }

    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
    {
        int ret;
        PyObject* bytes_a;
        PyObject* bytes_b;
        PyObject* compare_result;
        PyGILState_STATE gstate;

        gstate = PyGILState_Ensure();

        /* Create two Python byte strings */
        bytes_a = PyBytes_FromStringAndSize(a.data(), a.size());
        bytes_b = PyBytes_FromStringAndSize(b.data(), b.size());

        if ((bytes_a == NULL) || (bytes_b == NULL)) {
            this->bailout("Plyvel comparator could not allocate byte strings");
        }

        /* Invoke comparator callable */
        compare_result = PyObject_CallFunctionObjArgs(comparator, bytes_a, bytes_b, 0);

        if (compare_result == NULL) {
            this->bailout("Exception raised from custom Plyvel comparator");
        }

        /* The comparator callable can return any Python object. Compare it
         * to our "0" value to get a -1, 0, or 1 for LevelDB. */
        if (PyObject_RichCompareBool(compare_result, zero, Py_GT) == 1) {
            ret = 1;
        } else if (PyObject_RichCompareBool(compare_result, zero, Py_LT) == 1) {
            ret = -1;
        } else {
            ret = 0;
        }

        if (PyErr_Occurred()) {
            this->bailout("Exception raised while comparing custom Plyvel comparator result with 0");
        }

        Py_DECREF(compare_result);
        Py_DECREF(bytes_a);
        Py_DECREF(bytes_b);

        PyGILState_Release(gstate);

        return ret;
    }

    const char* Name() const { return name.c_str(); }
    void FindShortestSeparator(std::string*, const leveldb::Slice&) const { }
    void FindShortSuccessor(std::string*) const { }

private:

    std::string name;
    PyObject* comparator;
    PyObject* zero;
};


/*
 * This function is the only API used by the Plyvel Cython code.
 */
leveldb::Comparator* NewPlyvelCallbackComparator(const char* name, PyObject* comparator)
{
    return new PlyvelCallbackComparator(name, comparator);
}
