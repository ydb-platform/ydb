#include "task_scheduler.h"

namespace NYql::NDq {

TTaskOperatorTraits DescribeTaskOperator(TStringBuf name, ui64 count) {
    TTaskOperatorTraits traits;
    traits.Name = name;
    traits.Count = count;
    // These entries describe audited implementations, not scheduling rules.
    // In particular EmitsBuffered must not be confused with a source that can
    // manufacture more input: a finalizing aggregation drains accumulated data.
    if (name == "DqBlockHashJoin" || name == "DqScalarHashJoin"
        || name == "GraceJoin" || name == "GraceSelfJoin"
        || name == "GraceJoinWithSpilling" || name == "GraceSelfJoinWithSpilling") {
        traits.Consumes = traits.Accumulates = traits.EmitsBuffered = true;
        traits.Spills = traits.ReportsWaits = traits.Known = true;
    } else if (name == "BlockCombineAll" || name == "BlockCombineHashed"
        || name == "BlockMergeFinalizeHashed" || name == "BlockMergeManyFinalizeHashed") {
        traits.Consumes = traits.Accumulates = traits.EmitsBuffered = true;
        traits.ReportsWaits = traits.Known = true;
    } else if (name == "BlockMapJoinCore" || name == "BlockCrossJoinCore") {
        // External traits for implementations we do not instrument here. Their
        // input/index storage is known, but their Yield remains observational.
        traits.Consumes = traits.Accumulates = traits.EmitsBuffered = traits.Known = true;
    } else if (name == "WideMap") {
        traits.Consumes = traits.Known = true;
    }
    return traits;
}

TStringBuf TaskWorkName(ETaskWorkKind work) {
    switch (work) {
        case ETaskWorkKind::Unknown: return "unknown";
        case ETaskWorkKind::Consume: return "consume";
        case ETaskWorkKind::Compute: return "compute";
        case ETaskWorkKind::Emit: return "emit";
        case ETaskWorkKind::Control: return "control";
    }
    return "unknown";
}

TStringBuf TaskReadinessName(ETaskReadiness readiness) {
    switch (readiness) {
        case ETaskReadiness::Recheck: return "recheck";
        case ETaskReadiness::Runnable: return "runnable";
        case ETaskReadiness::Waiting: return "waiting";
        case ETaskReadiness::OpaqueYield: return "opaque_yield";
        case ETaskReadiness::Finished: return "finished";
        case ETaskReadiness::Cancelled: return "cancelled";
    }
    return "recheck";
}

TStringBuf TaskCoverageName(ETaskWaitCoverage coverage) {
    switch (coverage) {
        case ETaskWaitCoverage::Unknown: return "unknown";
        case ETaskWaitCoverage::Partial: return "partial";
        case ETaskWaitCoverage::Complete: return "complete";
    }
    return "unknown";
}

} // namespace NYql::NDq
