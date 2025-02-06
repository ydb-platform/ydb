#include "yql_emergency_sleep.h"

#include <util/system/env.h>
#include <util/stream/output.h>
#include <util/datetime/base.h>


namespace NYql::NPrivate {

namespace {

struct TIsInsideJob {
    TIsInsideJob()
        : InsideJob(!GetEnv("YT_JOB_ID").empty())
    {
    }
    const bool InsideJob;
};

} // unnamed

bool IsInsideJob() {
    return Singleton<TIsInsideJob>()->InsideJob;
}

void ReportAndSleep(const TString& msg) {
    Cerr << "An abnormal situation found, so consider opening a bug report to YQL (st/YQLSUPPORT)" << Endl << msg << Endl;
    while (true) {
        ::Sleep(TDuration::Seconds(5));
    }
}

} // NYql::NPrivate
