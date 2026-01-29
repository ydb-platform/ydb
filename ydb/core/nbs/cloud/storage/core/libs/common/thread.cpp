#include "thread.h"

#include <util/stream/str.h>
#include <util/system/execpath.h>
#include <util/system/thread.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

void SetCurrentThreadName(const TString& name, ui32 maxCharsFromProcessName)
{
#if defined(_linux_)
    // linux limits threadname by 15 + \0
    TStringBuf procName(GetExecPath());
    procName = procName.RNextTok('/');
    procName = procName.SubStr(0, maxCharsFromProcessName);

    TStringStream linuxName;
    linuxName << procName << '.' << name;
    TThread::SetCurrentThreadName(linuxName.Str().c_str());
#else
    Y_UNUSED(maxCharsFromProcessName);
    TThread::SetCurrentThreadName(name.c_str());
#endif
}

}   // namespace NCloud
