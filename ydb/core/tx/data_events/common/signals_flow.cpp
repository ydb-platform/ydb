#include "signals_flow.h"

#include <util/generic/serialized_enum.h>

namespace NKikimr::NEvWrite {

TWriteFlowCounters::TWriteFlowCounters()
    : TBase("CSWriteFlow") {
    DurationToAbort = TBase::GetDeriviative("Aborted/SumDuration");
    DurationToFinish = TBase::GetDeriviative("Finished/SumDuration");
    for (auto&& i : GetEnumAllValues<EWriteStage>()) {
        auto sub = CreateSubGroup("stage", ::ToString(i));
        CountByWriteStage.emplace_back(sub.GetValue("Count"));
        WriteStageAdd.emplace_back(sub.GetDeriviative("Moving/Count"));
        DurationToStage.emplace_back(sub.GetHistogram("DurationToStageMs", NMonitoring::ExponentialHistogram(18, 2, 1)));
        CountByStageMoving.emplace_back();
        CountByStageDuration.emplace_back();
        for (auto&& to : GetEnumAllValues<EWriteStage>()) {
            auto subTo = sub.CreateSubGroup("stage_to", ::ToString(to));
            CountByStageMoving.back().emplace_back(subTo.GetDeriviative("Transfers/Count"));
            CountByStageDuration.back().emplace_back(subTo.GetDeriviative("Transfers/Count"));
        }
    }
}

}   // namespace NKikimr::NEvWrite
