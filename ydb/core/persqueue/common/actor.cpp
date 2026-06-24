#include "actor.h"

#include <ydb/core/base/counters.h>

#define YDB_LOG_THIS_FILE_COMPONENT service

namespace NKikimr::NPQ {

void DoLogUnhandledException(NKikimrServices::EServiceKikimr service, const TStringBuf prefix, const std::exception& exc) {
    YDB_LOG_CRIT("Unhandled exception",
        {"prefix", prefix},
        {"exceptionType", TypeName(exc)},
        {"exceptionMessage", exc.what()},
        {"endl", Endl},
        {"backTrace", TBackTrace::FromCurrentException().PrintToString()});
}

const TString& TConstantLogPrefix::GetLogPrefix() const {
    if (!LogPrefix_.Defined()) {
        LogPrefix_ = BuildLogPrefix();
    }
    return *LogPrefix_;
}

void NPrivate::IncrementUnhandledExceptionCounter(const NActors::TActorContext& ctx) {
    GetServiceCounters(AppData(ctx)->Counters, "tablets")->GetCounter("alerts_exception", true)->Inc();
}

}
