#pragma once

#include <library/cpp/pybind/ptr.h>
#include <library/cpp/tvmauth/client/logger.h>

extern "C" {
    void cy_call_func(PyObject*, char*, int, const char*, size_t);
}

namespace NTvmAuth {
    class IPyLogger: public ILogger {
    public:
        NPyBind::TPyObjectPtr Obj_;

        IPyLogger(PyObject* obj)
            : Obj_(obj)
        {
        }

        ~IPyLogger() {
        }

        void Log(int lvl, const TString& msg) override {
            if (!Obj_) {
                return;
            }

            cy_call_func(this->Obj_.Get(), (char*)"__log", lvl, msg.data(), msg.size());
        }
    };
}
