#include "thread_extra.h"

#include <util/stream/str.h>
#include <util/system/execpath.h>
#include <util/system/platform.h>
#include <util/system/thread.h>

namespace {
#ifdef _linux_
    TString GetExecName() {
        TString execPath = GetExecPath();
        size_t lastSlash = execPath.find_last_of('/');
        if (lastSlash == TString::npos) {
            return execPath;
        } else {
            return execPath.substr(lastSlash + 1);
        }
    }
#endif
}

void SetCurrentThreadName(const char* name) {
#ifdef _linux_
    TStringStream linuxName;
    linuxName << GetExecName() << "." << name;
    TThread::SetCurrentThreadName(linuxName.Str().data());
#else
    TThread::SetCurrentThreadName(name);
#endif
}
