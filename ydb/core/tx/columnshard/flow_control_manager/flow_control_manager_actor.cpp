#include "flow_control_manager_actor.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NColumnShard::NFlowControl {

namespace {

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

}   // namespace

TFlowControlManager::TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
    : TActor(&TThis::StateMain)
    , Counters(countersGroup)
{
}

bool TFlowControlManager::TryCollectTargetTablets(const TLongTxWrite& tx, TVector<ui64>* tabletIds) {
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

        TEvTabletResolver::TEvForward::TResolveFlags flags;
        flags.SetAllowFollower(false);
        Send(MakeTabletResolverID(), new TEvTabletResolver::TEvForward(tabletId, nullptr, flags));
    }
}

void TFlowControlManager::ReplyOverloaded(const TActorContext& ctx, TLongTxWrite& tx, const TString& message) const {
    if (!message.empty() && tx.GetIssues()) {
        tx.GetIssues()->AddIssue(NYql::TIssue(message));
    }
    ctx.Send(tx.GetReplyTo(), new NActors::TEvents::TEvCompleted(0, Ydb::StatusIds::OVERLOADED));
}

void TFlowControlManager::Handle(const NFlowControl::TEvLongTxWrite::TPtr& ev, const TActorContext& ctx) {
    Counters.OnNewRequest();
    auto tx = ev->Get()->DetachLongTxWrite();

    TVector<ui64> tabletIds;
    if (TryCollectTargetTablets(tx, &tabletIds)) {
        switch (TryAdmit(tabletIds)) {
            case EAdmitDecision::Allow:
                break;
            case EAdmitDecision::RejectNow:
                MaybeStartLocationRechecks(tabletIds);
                return ReplyOverloaded(ctx, tx, "destination node is overloaded");
        }
    }

    NTxProxy::DoLongTxWriteSameMailbox(ctx, tx.GetReplyTo(), tx.GetLongTxId(), tx.GetDedupId(), tx.GetDatabaseName(), tx.GetPath(),
        tx.GetNavigateResult(), tx.GetBatch(), tx.GetIssues(), tx.GetUserCtx(), true);
}

void TFlowControlManager::Handle(const NFlowControl::TEvNodeOverloadStatus::TPtr& ev, const TActorContext& /*ctx*/) {
    const auto& record = ev->Get()->Record;
    const ui32 nodeId = record.GetNodeId();
    const ui64 generation = record.GetGeneration();

    switch (record.GetStatus()) {
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED:
            HotNodes[nodeId] = Max(HotNodes[nodeId], generation);
            break;
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY: {
            auto it = HotNodes.find(nodeId);
            if (it != HotNodes.end() && generation >= it->second) {
                HotNodes.erase(it);
            }
            break;
        }
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_UNSPECIFIED:
            break;
    }
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationUpdated::TPtr& ev, const TActorContext& /*ctx*/) {
    TabletToNode[ev->Get()->GetTabletId()] = ev->Get()->GetNodeId();
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationInvalidated::TPtr& ev, const TActorContext& /*ctx*/) {
    TabletToNode.erase(ev->Get()->GetTabletId());
}

void TFlowControlManager::Handle(const TEvTabletResolver::TEvForwardResult::TPtr& ev, const TActorContext& /*ctx*/) {
    const auto* msg = ev->Get();
    LocationRecheckInFlight.erase(msg->TabletID);
    if (msg->Status != NKikimrProto::OK || !msg->TabletActor) {
        return;
    }
    TabletToNode[msg->TabletID] = msg->TabletActor.NodeId();
}

}   // namespace NKikimr::NColumnShard::NFlowControl
