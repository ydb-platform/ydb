#include "actor.h"

#include <ydb/core/base/counters.h>

#define YDB_LOG_THIS_FILE_COMPONENT service

namespace NKikimr::NPQ {

void DoLogUnhandledException(NKikimrServices::EServiceKikimr service, const TStructuredMessage& prefix, const std::exception& exc) {
    YDB_LOG_CRIT("Unhandled exception",
        prefix,
        {"exceptionType", TypeName(exc)},
        {"exceptionMessage", exc.what()},
        {"backTrace", TBackTrace::FromCurrentException().PrintToString()});
}

void DoLogUnhandledException(NKikimrServices::EServiceKikimr service, TStringBuf prefix, const std::exception& exc) {
    DoLogUnhandledException(service, YDB_LOG_CREATE_MESSAGE({"prefix", TString(prefix)}), exc);
}

void IncrementUnhandledExceptionCounter(const NActors::TActorContext& ctx) {
    GetServiceCounters(AppData(ctx)->Counters, "tablets")->GetCounter("alerts_exception", true)->Inc();
}

}
