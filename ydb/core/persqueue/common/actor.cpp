#include "actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NPQ {

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
