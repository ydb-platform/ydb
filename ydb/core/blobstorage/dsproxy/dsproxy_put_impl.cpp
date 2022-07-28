#include "dsproxy_put_impl.h"
#include "dsproxy_blackboard.h"

#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

namespace NKikimr {

using TPutResultVec = TPutImpl::TPutResultVec;

bool TPutImpl::RunStrategies(TLogContext &logCtx, TPutResultVec &outPutResults) {
    switch (Info->Type.GetErasure()) {
        case TBlobStorageGroupType::ErasureMirror3dc:
            return RunStrategy(logCtx, TPut3dcStrategy(Tactic, EnableRequestMod3x3ForMinLatecy), outPutResults);
        case TBlobStorageGroupType::ErasureMirror3of4:
            return RunStrategy(logCtx, TPut3of4Strategy(Tactic), outPutResults);
        default:
            return RunStrategy(logCtx, TRestoreStrategy(), outPutResults);
    }
}

bool TPutImpl::RunStrategy(TLogContext &logCtx, const IStrategy& strategy, TPutResultVec &outPutResults) {
    TBatchedVec<TBlackboard::TBlobStates::value_type*> finished;
    const EStrategyOutcome outcome = Blackboard.RunStrategy(logCtx, strategy, &finished);
    if (finished) {
        PrepareReply(logCtx, outcome.ErrorReason, finished, outPutResults);
        return true;
    }
    return false;
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

void TPutImpl::PrepareOneReply(NKikimrProto::EReplyStatus status, TLogoBlobID blobId, ui64 blobIdx, TLogContext &logCtx,
        TString errorReason, TPutResultVec &outPutResults) {
    Y_VERIFY(IsInitialized);
    if (!IsDone[blobIdx]) {
        outPutResults.emplace_back(blobIdx, new TEvBlobStorage::TEvPutResult(status, blobId, StatusFlags, Info->GroupID,
                    ApproximateFreeSpaceShare));
        outPutResults.back().second->ErrorReason = errorReason;
        NLog::EPriority priority = GetPriorityForReply(Info->PutErrorMuteChecker, status);
        A_LOG_LOG_SX(logCtx, true, priority, "BPP12", "Result# " << outPutResults.back().second->Print(false));
        MarkBlobAsSent(blobIdx);
    }
}

void TPutImpl::PrepareReply(NKikimrProto::EReplyStatus status, TLogContext &logCtx, TString errorReason,
        TPutResultVec &outPutResults) {
    A_LOG_DEBUG_SX(logCtx, "BPP34", "PrepareReply status# " << status << " errorReason# " << errorReason);
    for (ui64 idx = 0; idx < Blobs.size(); ++idx) {
        if (IsDone[idx]) {
            A_LOG_DEBUG_SX(logCtx, "BPP35", "blob# " << Blobs[idx].ToString() <<
                " idx# " << idx << " is sent, skipped");
            continue;
        }

        outPutResults.emplace_back(idx, new TEvBlobStorage::TEvPutResult(status, Blobs[idx].BlobId, StatusFlags,
            Info->GroupID, ApproximateFreeSpaceShare));
        outPutResults.back().second->ErrorReason = errorReason;

        NLog::EPriority priority = GetPriorityForReply(Info->PutErrorMuteChecker, status);
        A_LOG_LOG_SX(logCtx, true, priority, "BPP38",
                "PrepareReply Result# " << outPutResults.back().second->Print(false));

        if (IsInitialized) {
            MarkBlobAsSent(idx);
        }
    }
}

void TPutImpl::PrepareReply(TLogContext &logCtx, TString errorReason,
        TBatchedVec<TBlackboard::TBlobStates::value_type*>& finished, TPutResultVec &outPutResults) {
    A_LOG_DEBUG_SX(logCtx, "BPP36", "PrepareReply errorReason# " << errorReason);
    Y_VERIFY(IsInitialized);
    for (auto item : finished) {
        auto &[blobId, state] = *item;
        const ui64 idx = state.BlobIdx;
        Y_VERIFY(blobId == Blobs[idx].BlobId, "BlobIdx# %" PRIu64 " BlobState# %s Blackboard# %s",
            idx, state.ToString().c_str(), Blackboard.ToString().c_str());
        Y_VERIFY(!IsDone[idx]);
        Y_VERIFY(state.Status != NKikimrProto::UNKNOWN);
        outPutResults.emplace_back(idx, new TEvBlobStorage::TEvPutResult(state.Status, blobId, StatusFlags,
            Info->GroupID, ApproximateFreeSpaceShare));
        outPutResults.back().second->ErrorReason = errorReason;

        NLog::EPriority priority = GetPriorityForReply(Info->PutErrorMuteChecker, state.Status);
        A_LOG_LOG_SX(logCtx, true, priority, "BPP37",
                "PrepareReply Result# " << outPutResults.back().second->Print(false));
        MarkBlobAsSent(idx);
    }
}

ui64 TPutImpl::GetTimeToAccelerateNs(TLogContext &logCtx) {
    Y_UNUSED(logCtx);
    Y_VERIFY(!Blackboard.BlobStates.empty());
    TBatchedVec<ui64> nextToWorstPredictedNsVec(Blackboard.BlobStates.size());
    ui64 idx = 0;
    for (auto &[_, state] : Blackboard.BlobStates) {
        // Find the slowest disk
        i32 worstSubgroupIdx = -1;
        ui64 worstPredictedNs = 0;
        state.GetWorstPredictedDelaysNs(*Info, *Blackboard.GroupQueues, HandleClassToQueueId(Blackboard.PutHandleClass),
                &worstPredictedNs, &nextToWorstPredictedNsVec[idx], &worstSubgroupIdx);
        idx++;
    }
    return *MaxElement(nextToWorstPredictedNsVec.begin(), nextToWorstPredictedNsVec.end());
}

TString TPutImpl::DumpFullState() const {
    TStringStream str;
    str << "{Deadline# " << Deadline;
    str << Endl;
    str << " Info# " << Info->ToString();
    str << Endl;
    str << " Blackboard# " << Blackboard.ToString();
    str << Endl;
    str << " Blobs# " << Blobs.ToString();
    str << Endl;
    str << "IsDone# " << IsDone.ToString();
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

bool TPutImpl::MarkBlobAsSent(ui64 idx) {
    Y_VERIFY(idx < Blobs.size());
    Y_VERIFY(!IsDone[idx]);
    Blackboard.MoveBlobStateToDone(Blobs[idx].BlobId);
    IsDone[idx] = true;
    DoneBlobs++;
    return true;
}

}//NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TPutImpl::TBlobInfo, stream, value) {
    value.Output(stream);
}
