#include "py_utils.h"
#include "py_cast.h"
#include "py_errors.h"
#include "py_gil.h"

#include <util/generic/yexception.h>
#include <util/string/split.h>

#include <regex>


namespace NPython {

TPyObjectPtr PyRepr(TStringBuf asciiStr, bool intern) {
    for (auto c : asciiStr) {
        Y_ABORT_UNLESS((c&0x80) == 0, "expected ascii");
    }

    Py_ssize_t size = static_cast<Py_ssize_t>(asciiStr.size());
#if PY_MAJOR_VERSION >= 3
    TPyObjectPtr pyStr = PyUnicode_FromStringAndSize(asciiStr.Data(), size);
#else
    TPyObjectPtr pyStr = PyString_FromStringAndSize(asciiStr.data(), size);
#endif
    Y_ABORT_UNLESS(pyStr, "Can't get repr string");
    if (!intern) {
        return pyStr;
    }

    PyObject* tmp = pyStr.Release();
#if PY_MAJOR_VERSION >= 3
    PyUnicode_InternInPlace(&tmp);
#else
    PyString_InternInPlace(&tmp);
#endif
    return TPyObjectPtr(tmp);
}

TString PyObjectRepr(PyObject* value) {
    static constexpr size_t maxLen = 1000;
    static constexpr std::string_view truncSuffix = "(truncated)";
    const TPyObjectPtr repr(PyObject_Repr(value));
    if (!repr) {
       return TString("repr error: ") + GetLastErrorAsString();
    }

    TString string;
    if (!TryPyCast(repr.Get(), string)) {
        string = "can't get repr as string";
    }
    if (string.size() > maxLen) {
        string.resize(maxLen - truncSuffix.size());
        string += truncSuffix;
    }
    return string;
}

bool HasEncodingCookie(const TString& source) {
    //
    // To define a source code encoding, a magic comment must be placed
    // into the source files either as first or second line in the file.
    //
    // See https://www.python.org/dev/peps/pep-0263 for more details.
    //

    static std::regex encodingRe(
                "^[ \\t\\v]*#.*?coding[:=][ \\t]*[-_.a-zA-Z0-9]+.*");

    int i = 0;
    for (const auto& it: StringSplitter(source).Split('\n')) {
        if (i++ == 2) break;

        TStringBuf line = it.Token();
        if (std::regex_match(line.begin(), line.end(), encodingRe)) {
            return true;
        }
    }
    return false;
}

void PyCleanup() {
    TPyGilLocker lock;
    PyErr_Clear();
    PySys_SetObject("last_type", Py_None);
    PySys_SetObject("last_value", Py_None);
    PySys_SetObject("last_traceback", Py_None);
}

} // namspace NPython
