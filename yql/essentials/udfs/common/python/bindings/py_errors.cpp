#include "py_errors.h"
#include "py_ptr.h"
#include "py_cast.h"
#include "py_utils.h"

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NPython {

// this function in conjuction with code after Py_Initialize
// does approximately following:
//
//      sys.stderr = StderrProxy(sys.stderr)
//
//      ...
//
//      sys.stderr._toggle_real_mode()
//      sys.excepthook(
//              sys.last_type,
//              sys.last_value,
//              sys.last_traceback)
//      sys.stderr._get_value()
//      sys.stderr._toggle_real_mode()
//
//  where _toggle_real_mode, _get_value & all calls to stderr not in real mode
//  are handled in a thread-safe way
//
TString GetLastErrorAsString()
{
    PyObject* etype;
    PyObject* evalue;
    PyObject* etraceback;

    PyErr_Fetch(&etype, &evalue, &etraceback);

    if (!etype) {
        return {};
    }

    TPyObjectPtr etypePtr {etype, TPyObjectPtr::ADD_REF};
    TPyObjectPtr evaluePtr {evalue, TPyObjectPtr::ADD_REF};
    TPyObjectPtr etracebackPtr {etraceback, TPyObjectPtr::ADD_REF};

    TPyObjectPtr stderrObject {PySys_GetObject("stderr"), TPyObjectPtr::ADD_REF};
    if (!stderrObject) {
        return {};
    }

    TPyObjectPtr unused = PyObject_CallMethod(stderrObject.Get(), "_toggle_real_mode", nullptr);

    PyErr_Restore(etypePtr.Get(), evaluePtr.Get(), etracebackPtr.Get());
    // in unusual situations there may be low-level write to stderr
    // (by direct C FILE* write), but that's OK
    PyErr_Print();

    TPyObjectPtr error = PyObject_CallMethod(stderrObject.Get(), "_get_value", nullptr);
    if (!error) {
        return {};
    }
    unused.ResetSteal(
        PyObject_CallMethod(stderrObject.Get(), "_toggle_real_mode", nullptr)
    );

    TString errorValue;
    if (!TryPyCast(error.Get(), errorValue)) {
        errorValue = TString("can't get error string from: ") += PyObjectRepr(error.Get());
    }
    return errorValue;
}

} // namspace NPython
