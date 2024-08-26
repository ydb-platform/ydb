#include "dsproxy_put_impl.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

namespace NKikimr {

using TPutResultVec = TPutImpl::TPutResultVec;

void TPutImpl::RunStrategies(TLogContext &logCtx, TPutResultVec &outPutResults,
        const TBlobStorageGroupInfo::TGroupVDisks& expired, bool accelerate) {
    if (accelerate) {
        ChangeAll();
    }

    switch (Info->Type.GetErasure()) {
        case TBlobStorageGroupType::ErasureMirror3dc:
            return accelerate
                ? RunStrategy(logCtx, TAcceleratePut3dcStrategy(Tactic, EnableRequestMod3x3ForMinLatecy), outPutResults, expired)
                : RunStrategy(logCtx, TPut3dcStrategy(Tactic, EnableRequestMod3x3ForMinLatecy), outPutResults, expired);
        case TBlobStorageGroupType::ErasureMirror3of4:
            return accelerate
                ? RunStrategy(logCtx, TPut3of4Strategy(Tactic, true), outPutResults, expired)
                : RunStrategy(logCtx, TPut3of4Strategy(Tactic), outPutResults, expired);
        default:
            return accelerate
                ? RunStrategy(logCtx, TAcceleratePutStrategy(), outPutResults, expired)
                : RunStrategy(logCtx, TRestoreStrategy(), outPutResults, expired);
    }
}

void TPutImpl::RunStrategy(TLogContext &logCtx, const IStrategy& strategy, TPutResultVec &outPutResults,
        const TBlobStorageGroupInfo::TGroupVDisks& expired) {
    Y_VERIFY_S(Blackboard.BlobStates.size(), "State# " << DumpFullState());
    TBatchedVec<TBlackboard::TFinishedBlob> finished;
    const EStrategyOutcome outcome = Blackboard.RunStrategy(logCtx, strategy, AccelerationParams, &finished, &expired);
    for (const TBlackboard::TFinishedBlob& item : finished) {
        Y_ABORT_UNLESS(item.BlobIdx < Blobs.size());
        Y_ABORT_UNLESS(!IsDone[item.BlobIdx]);
        PrepareOneReply(item.Status, item.BlobIdx, logCtx, item.ErrorReason, outPutResults);
        Y_VERIFY_S(IsDone[item.BlobIdx], "State# " << DumpFullState());
    }
    if (outcome == EStrategyOutcome::DONE) {
        for (const auto& done : IsDone) {
            Y_VERIFY_S(done, "finished.size# " << finished.size() << " State# " << DumpFullState());
        }
    }
}

NLog::EPriority GetPriorityForReply(TAtomicLogPriorityMuteChecker<NLog::PRI_ERROR, NLog::PRI_DEBUG> &checker,
        NKikimrProto::EReplyStatus status) {
    NLog::EPriority priority = PriorityForStatusOutbound(status);
    if (priority == NLog::PRI_ERROR) {
        return checker.Register(TActivationContext::Now(), TDuration::Seconds(5));
    } else {
        checker.Unmute();
        return priority;
    }
}

void TPutImpl::PrepareOneReply(NKikimrProto::EReplyStatus status, size_t blobIdx, TLogContext &logCtx,
        TString errorReason, TPutResultVec &outPutResults) {
    if (!std::exchange(IsDone[blobIdx], true)) {
        auto ev = std::make_unique<TEvBlobStorage::TEvPutResult>(status, Blobs[blobIdx].BlobId, StatusFlags,
            Info->GroupID, ApproximateFreeSpaceShare);
        ev->ErrorReason = std::move(errorReason);
        const NLog::EPriority priority = GetPriorityForReply(Info->PutErrorMuteChecker, status);
        A_LOG_LOG_SX(logCtx, priority, "BPP12", "Result# " << ev->Print(false));
        outPutResults.emplace_back(blobIdx, std::move(ev));
    }
}

void TPutImpl::PrepareReply(NKikimrProto::EReplyStatus status, TLogContext &logCtx, TString errorReason,
        TPutResultVec &outPutResults) {
    A_LOG_DEBUG_SX(logCtx, "BPP34", "PrepareReply status# " << status << " errorReason# " << errorReason);
    for (size_t blobIdx = 0; blobIdx < Blobs.size(); ++blobIdx) {
        PrepareOneReply(status, blobIdx, logCtx, errorReason, outPutResults);
    }
}

ui64 TPutImpl::GetTimeToAccelerateNs(TLogContext &logCtx) {
    Y_UNUSED(logCtx);
    Y_ABORT_UNLESS(!Blackboard.BlobStates.empty());
    TBatchedVec<ui64> nthWorstPredictedNsVec(Blackboard.BlobStates.size());
    ui64 idx = 0;
    for (auto &[_, state] : Blackboard.BlobStates) {
        // Find the n'th slowest disk
        TDiskDelayPredictions worstDisks;
        state.GetWorstPredictedDelaysNs(*Info, *Blackboard.GroupQueues, HandleClassToQueueId(Blackboard.PutHandleClass),
                &worstDisks, AccelerationParams.PredictedDelayMultiplier);
        nthWorstPredictedNsVec[idx++] = worstDisks[2].PredictedNs;
    }
    return *MaxElement(nthWorstPredictedNsVec.begin(), nthWorstPredictedNsVec.end());
}

TString TPutImpl::DumpFullState() const {
    TStringStream str;
    str << "{Info# " << Info->ToString();
    str << Endl;
    str << " Blackboard# " << Blackboard.ToString();
    str << Endl;
    str << " Blobs# " << Blobs.ToString();
    str << Endl;
    str << " IsDone# " << IsDone.ToString();
    str << Endl;
    str << " HandoffPartsSent# " << HandoffPartsSent;
    str << Endl;
    str << " VPutRequests# " << VPutRequests;
    str << Endl;
    str << " VPutResponses# " << VPutResponses;
    str << Endl;
    str << " VMultiPutRequests# " << VMultiPutRequests;
    str << Endl;
    str << " VMultiPutResponses# " << VMultiPutResponses;
    str << Endl;
    str << " Tactic# " << TEvBlobStorage::TEvPut::TacticName(Tactic);
    str << Endl;
    str << "}";
    return str.Str();
}

}//NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TPutImpl::TBlobInfo, stream, value) {
    value.Output(stream);
}
