#pragma once

#include <util/stream/str.h>
#include <util/system/yassert.h>
#include <util/system/src_root.h>

namespace NKikimr {
namespace NUtil {

    class TFail : public TStringOutput {
    public:
        using TBuf = NPrivate::TStaticBuf;

        TFail(TBuf fname, ui32 line, const char *func)
            : TStringOutput(Raw)
            , File(fname)
            , Func(func)
            , Line(line)
        {
            Raw.reserve(192);
        }

        [[noreturn]] ~TFail()
        {
            ::NPrivate::Panic(File, Line, Func, nullptr, "%s\n", Raw.data());
        }

    private:
        const TBuf File;
        const char *Func = nullptr;
        const int Line = Max<int>();
        TString Raw;
    };

}
}

#define Y_Fail_Detailed()\
    NKikimr::NUtil::TFail logl(__SOURCE_FILE_IMPL__, __LINE__, __FUNCTION__)

#define Y_Fail(stream) do{ Y_Fail_Detailed(); logl << stream; } while(false)
