#pragma once

#include <util/generic/strbuf.h>
#include <util/stream/str.h>
#include <util/system/execpath.h>
#include <util/system/thread.h>
#include <util/system/thread.h>
#include <time.h>

inline void SetCurrentThreadName(const TString& name,
                                 const ui32 maxCharsFromProcessName = 8) {
#if defined(_linux_)
    // linux limits threadname by 15 + \0

    TStringBuf procName(GetExecPath());
    procName = procName.RNextTok('/');
    procName = procName.SubStr(0, maxCharsFromProcessName);

    TStringStream linuxName;
    linuxName << procName << "." << name;
    TThread::SetCurrentThreadName(linuxName.Str().data());
#else
    Y_UNUSED(maxCharsFromProcessName);
    TThread::SetCurrentThreadName(name.data());
#endif
}
