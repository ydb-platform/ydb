#include "signals_flow.h"

#include <util/generic/serialized_enum.h>

namespace NKikimr::NEvWrite {

TWriteFlowCounters::TWriteFlowCounters()
    : TBase("CSWriteFlow")
    , Tracing(*this, "write_state") {
    DurationToAbort = TBase::GetDeriviative("Aborted/SumDuration");
    DurationToFinish = TBase::GetDeriviative("Finished/SumDuration");
}

}   // namespace NKikimr::NEvWrite
