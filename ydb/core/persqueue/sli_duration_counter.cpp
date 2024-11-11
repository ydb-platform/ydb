#include "sli_duration_counter.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NPQ {

TPercentileCounter CreateSLIDurationCounter(const NActors::TActorContext& ctx,
                                            const TString& name,
                                            const TVector<ui32>& durations,
                                            const TString& account,
                                            ui32 border)
{
    return CreateSLIDurationCounter(GetServiceCounters(AppData(ctx)->Counters, "pqproxy|SLI"),
                                    TVector<NPersQueue::TPQLabelsInfo>{{{{"Account", account}}, {"total"}}},
                                    name,
                                    border,
                                    durations);
}

}
