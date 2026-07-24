#include "flow_control_manager_actor.h"
#include "flow_control_manager_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NColumnShard::NFlowControl {

namespace {

TIntrusivePtr<::NMonitoring::TDynamicCounters> FlowControlCountersGroup;

class TParsedBatchData: public NEvWrite::IShardsSplitter::IEvWriteDataAccessor {
private:
    using TBase = NEvWrite::IShardsSplitter::IEvWriteDataAccessor;
    std::shared_ptr<arrow::RecordBatch> Batch;

public:
    explicit TParsedBatchData(std::shared_ptr<arrow::RecordBatch> batch)
        : TBase(NArrow::GetBatchMemorySize(batch))
        , Batch(std::move(batch))
    {
    }

    std::shared_ptr<arrow::RecordBatch> GetDeserializedBatch() const override {
        return Batch;
    }

    TString GetSerializedData() const override {
        return NArrow::SerializeBatchNoCompression(Batch);
    }
};

bool TryCollectTargetTablets(const TLongTxWrite& tx, TVector<ui64>* tabletIds) {
    Y_ABORT_UNLESS(tabletIds);
    tabletIds->clear();

    const auto& navigate = tx.GetNavigateResult();
    if (!navigate || navigate->ErrorCount > 0 || navigate->ResultSet.empty()) {
        return false;
    }

    const auto& entry = navigate->ResultSet[0];
    auto shardsSplitter = NEvWrite::IShardsSplitter::BuildSplitter(entry);
    if (!shardsSplitter) {
        return false;
    }

    TParsedBatchData accessor(tx.GetBatch());
    const auto initStatus = shardsSplitter->SplitData(entry, accessor);
    if (!initStatus.Ok()) {
        return false;
    }

    for (const auto& [tabletId, _] : shardsSplitter->GetSplitData().GetShardsInfo()) {
        tabletIds->push_back(tabletId);
    }
    return true;
}

TIntrusivePtr<::NMonitoring::TDynamicCounters> CountersGroupOrNull() {
    if (FlowControlCountersGroup) {
        return FlowControlCountersGroup;
    }
    if (HasAppData() && AppData()->Counters) {
        return AppData()->Counters;
    }
    return nullptr;
}

// Runs on the caller's mailbox (BulkUpsert / DoLongTxWriteSameMailbox). Does data split + FCM admit
// RPC here, then starts TLongTxWriteInternal on the same mailbox (forceNoFlowControl).
class TLongTxWriteFlowControlled: public NActors::TActorBootstrapped<TLongTxWriteFlowControlled> {
    TLongTxWrite Tx;
    TCSFlowControlManagerCounters Counters;
    TInstant WaitAdmitStartedAt;

public:
    explicit TLongTxWriteFlowControlled(TLongTxWrite&& tx)
        : Tx(std::move(tx))
        , Counters(CountersGroupOrNull())
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Counters.OnRequestStart();

        const TInstant splitStartedAt = TActivationContext::Now();
        TVector<ui64> tabletIds;
        const bool splitOk = TryCollectTargetTablets(Tx, &tabletIds);
        Counters.OnSplitFinished(TActivationContext::Now() - splitStartedAt);

        if (!splitOk) {
            // Same as legacy FCM path: cannot admit without targets → fail-open into write actor
            // (it will reply with the real navigate/split error).
            Counters.OnAdmitSkippedNoSplit();
            StartWrite(ctx);
            return Finish();
        }

        WaitAdmitStartedAt = TActivationContext::Now();
        Counters.OnWaitingAdmitStart();
        ctx.Send(TFlowControlManagerServiceOperator::MakeServiceId(ctx.SelfID.NodeId()), std::make_unique<TEvTryAdmit>(std::move(tabletIds)));
        Become(&TThis::StateWaitAdmit);
    }

private:
    STRICT_STFUNC(StateWaitAdmit, HFunc(TEvTryAdmitResult, Handle))

    void Handle(TEvTryAdmitResult::TPtr& ev, const TActorContext& ctx) {
        Counters.OnWaitingAdmitFinish(TActivationContext::Now() - WaitAdmitStartedAt);

        switch (ev->Get()->GetDecision()) {
            case EAdmitDecision::Allow:
                StartWrite(ctx);
                break;
            case EAdmitDecision::RejectNow:
                ReplyOverloaded(ctx, "destination node is overloaded");
                break;
        }
        Finish();
    }

    void StartWrite(const TActorContext& ctx) {
        NTxProxy::DoLongTxWriteSameMailbox(ctx, Tx.GetReplyTo(), Tx.GetLongTxId(), Tx.GetDedupId(), Tx.GetDatabaseName(), Tx.GetPath(),
            Tx.GetNavigateResult(), Tx.GetBatch(), Tx.GetIssues(), Tx.GetUserCtx(), /*forceNoFlowControl=*/true);
    }

    void ReplyOverloaded(const TActorContext& ctx, const TString& message) {
        if (!message.empty() && Tx.GetIssues()) {
            Tx.GetIssues()->AddIssue(NYql::TIssue(message));
        }
        ctx.Send(Tx.GetReplyTo(), new NActors::TEvents::TEvCompleted(0, Ydb::StatusIds::OVERLOADED));
    }

    void Finish() {
        Counters.OnRequestFinish();
        PassAway();
    }
};

}   // namespace

TFlowControlManager::TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
    : TActor(&TThis::StateMain)
    , Counters(countersGroup)
{
    FlowControlCountersGroup = countersGroup;
}

void TFlowControlManager::PublishMapSizes() const {
    Counters.SetHotNodesCount(HotNodes.size());
    Counters.SetTabletToNodeCount(TabletToNode.size());
}

EAdmitDecision TFlowControlManager::TryAdmit(const TVector<ui64>& tabletIds) const {
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId) {
            continue;   // fail-open for unknown location
        }
        if (HotNodes.contains(*nodeId)) {
            return EAdmitDecision::RejectNow;
        }
    }
    return EAdmitDecision::Allow;
}

void TFlowControlManager::MaybeStartLocationRechecks(const TVector<ui64>& tabletIds) {
    const TInstant now = TActivationContext::Now();
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId || !HotNodes.contains(*nodeId)) {
            continue;
        }
        if (LocationRecheckInFlight.contains(tabletId)) {
            continue;
        }
        if (const auto* last = LastLocationRecheck.FindPtr(tabletId)) {
            if (now - *last < LocationRecheckPeriod) {
                continue;
            }
        }

        LastLocationRecheck[tabletId] = now;
        LocationRecheckInFlight.insert(tabletId);
        Counters.OnLocationRecheck();

        TEvTabletResolver::TEvForward::TResolveFlags flags;
        flags.SetAllowFollower(false);
        Send(MakeTabletResolverID(), new TEvTabletResolver::TEvForward(tabletId, nullptr, flags));
    }
}

void TFlowControlManager::Handle(const NFlowControl::TEvLongTxWrite::TPtr& ev, const TActorContext& ctx) {
    // Compatibility path: do not run split/write on FCM mailbox. Schedule helper on a separate mailbox.
    auto tx = ev->Get()->DetachLongTxWrite();
    ctx.Register(new TLongTxWriteFlowControlled(std::move(tx)));
}

void TFlowControlManager::Handle(const NFlowControl::TEvTryAdmit::TPtr& ev, const TActorContext& ctx) {
    const TInstant startedAt = TActivationContext::Now();
    const auto& tabletIds = ev->Get()->GetTabletIds();
    const EAdmitDecision decision = TryAdmit(tabletIds);
    const TDuration duration = TActivationContext::Now() - startedAt;

    switch (decision) {
        case EAdmitDecision::Allow:
            Counters.OnAdmitAllowed(duration);
            break;
        case EAdmitDecision::RejectNow:
            Counters.OnAdmitRejected(duration);
            MaybeStartLocationRechecks(tabletIds);
            break;
    }

    ctx.Send(ev->Sender, new TEvTryAdmitResult(decision));
}

void TFlowControlManager::Handle(const NFlowControl::TEvNodeOverloadStatus::TPtr& ev, const TActorContext& /*ctx*/) {
    const auto& record = ev->Get()->Record;
    const ui32 nodeId = record.GetNodeId();
    const ui64 generation = record.GetGeneration();

    switch (record.GetStatus()) {
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED:
            HotNodes[nodeId] = Max(HotNodes[nodeId], generation);
            Counters.OnStatusOverloaded();
            break;
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY: {
            auto it = HotNodes.find(nodeId);
            if (it != HotNodes.end() && generation >= it->second) {
                HotNodes.erase(it);
            }
            Counters.OnStatusReady();
            break;
        }
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_UNSPECIFIED:
            break;
    }
    PublishMapSizes();
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationUpdated::TPtr& ev, const TActorContext& /*ctx*/) {
    TabletToNode[ev->Get()->GetTabletId()] = ev->Get()->GetNodeId();
    PublishMapSizes();
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationInvalidated::TPtr& ev, const TActorContext& /*ctx*/) {
    TabletToNode.erase(ev->Get()->GetTabletId());
    PublishMapSizes();
}

void TFlowControlManager::Handle(const TEvTabletResolver::TEvForwardResult::TPtr& ev, const TActorContext& /*ctx*/) {
    const auto* msg = ev->Get();
    LocationRecheckInFlight.erase(msg->TabletID);
    if (msg->Status != NKikimrProto::OK || !msg->TabletActor) {
        return;
    }
    TabletToNode[msg->TabletID] = msg->TabletActor.NodeId();
    PublishMapSizes();
}

void TFlowControlManagerServiceOperator::StartLongTxWrite(const TActorContext& ctx, TLongTxWrite&& longTxWrite) {
    // Keep split + LongTx write on the caller's mailbox (BulkUpsert upload actor).
    ctx.RegisterWithSameMailbox(new TLongTxWriteFlowControlled(std::move(longTxWrite)));
}

}   // namespace NKikimr::NColumnShard::NFlowControl
