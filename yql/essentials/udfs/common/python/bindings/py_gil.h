#pragma once

#include <Python.h>


namespace NPython {

struct TPyGilLocker
{
    TPyGilLocker()
        : Gil_(PyGILState_Ensure())
    {
    }

    ~TPyGilLocker() {
        PyGILState_Release(Gil_);
    }

private:
    PyGILState_STATE Gil_;
};

struct TPyGilUnlocker {
    TPyGilUnlocker()
        : ThreadState_(PyEval_SaveThread())
    {
    }

    ~TPyGilUnlocker() {
        PyEval_RestoreThread(ThreadState_);
    }

private:
    PyThreadState* ThreadState_;
};

} // namespace NPython
