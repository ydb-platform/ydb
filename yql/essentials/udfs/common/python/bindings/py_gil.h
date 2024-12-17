#pragma once

#include <Python.h>


namespace NPython {

struct TPyGilLocker
{
    TPyGilLocker()
        : Gil(PyGILState_Ensure())
    {
    }

    ~TPyGilLocker() {
        PyGILState_Release(Gil);
    }

private:
    PyGILState_STATE Gil;
};

struct TPyGilUnlocker {
    TPyGilUnlocker()
        : ThreadState(PyEval_SaveThread())
    {
    }

    ~TPyGilUnlocker() {
        PyEval_RestoreThread(ThreadState);
    }

private:
    PyThreadState* ThreadState;
};

} // namespace NPython
