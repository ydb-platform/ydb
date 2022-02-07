#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/stream/null.h>

namespace NYdb {

void SetVerbosity(bool isVerbose);
bool GetVerbosity();

#define LOG_NULL(s) Cnull << s
#define EXTEND_MSG(s) TInstant::Now().ToIsoStringLocal() << ": " << s << Endl

#define LOG_INFO(s) Cout << EXTEND_MSG(s)
#define LOG_ERR(s) Cerr << EXTEND_MSG(s)
#define LOG_DEBUG(s) (NYdb::GetVerbosity() ? LOG_INFO(s) : LOG_NULL(s))



// Retrive path relative to database root from absolute
TString RelPathFromAbsolute(TString db, TString path);

// Parses strings from human readable format to ui64
// Suppores decimal prefixes such as K(1000), M, G, T
// Suppores binary prefixes such as Ki(1024), Mi, Gi, Ti
// Example: "2Ki" -> 2048
ui64 SizeFromString(TStringBuf s);

class TScopedTimer {
    TInstant Start;
    TString Msg;

public:
    TScopedTimer(const TString& msg)
        : Start(TInstant::Now())
        , Msg(msg)
    {}

    ~TScopedTimer() {
        LOG_INFO(Msg << (TInstant::Now() - Start).SecondsFloat() << "s");
    }
};

}
