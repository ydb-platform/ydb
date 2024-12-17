#pragma once

#include <util/generic/fwd.h>

namespace NPython {

TString GetLastErrorAsString();

#define PY_TRY try

#define PY_CATCH(ErrorValue) \
    catch (const yexception& e) { \
        PyErr_SetString(PyExc_RuntimeError, e.what()); \
        return ErrorValue; \
    }

#define PY_ENSURE(condition, message) \
    do { \
        if (Y_UNLIKELY(!(condition))) { \
            throw yexception() << message; \
        } \
    } while (0)

} // namspace NPython
