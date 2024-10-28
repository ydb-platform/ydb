#include "datashard_impl.h"
#include "operation.h"

#include <ydb/core/tablet_flat/flat_stat_table.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/mime/types/mime.h>
#include <library/cpp/resource/resource.h>

#include <library/cpp/html/pcdata/pcdata.h>

namespace NKikimr::NDataShard {

void TDataShard::HandleMonIndexPage(NMon::TEvRemoteHttpInfo::TPtr& ev) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS,
                "HTTP request at " << TabletID() << " url="
                << ev->Get()->PathInfo());

    TString blob;
    NResource::FindExact("datashard/index.html", &blob);

    if (blob.empty()) {
        Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPNOTFOUND));
        return;
    }

    TStringBuilder response;
    response << "HTTP/1.1 200 Ok\r\n";
    response << "Content-Type: text/html\r\n";
    response << "Content-Length: " << blob.size() << "\r\n";
    response << "\r\n";
    response.Out.Write(blob.data(), blob.size());
    Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(std::move(response)));
}

void TDataShard::Handle(TEvDataShard::TEvGetInfoRequest::TPtr& ev) {
    auto* response = new TEvDataShard::TEvGetInfoResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    for (auto& pr : GetUserTables()) {
        auto& rec = *response->Record.AddUserTables();
        rec.SetName(pr.second->Name);
        rec.SetPath(pr.second->Path);
        rec.SetLocalId(pr.second->LocalTid);
        rec.SetPathId(pr.first);
        rec.SetSchemaVersion(pr.second->GetTableSchemaVersion());
        pr.second->GetSchema(*rec.MutableDescription());

        if (pr.second->Stats.StatsUpdateTime) {
            auto &stats = *rec.MutableStats();
            stats.SetRowCount(pr.second->Stats.DataStats.RowCount);
            stats.SetDataSize(pr.second->Stats.DataStats.DataSize.Size);
            stats.SetLastAccessTime(pr.second->Stats.AccessTime.ToStringLocalUpToSeconds());
            stats.SetLastUpdateTime(pr.second->Stats.UpdateTime.ToStringLocalUpToSeconds());
            stats.SetLastStatsUpdateTime(LastDbStatsUpdateTime.ToStringLocalUpToSeconds());
            stats.SetLastStatsReportTime(LastDbStatsReportTime.ToStringLocalUpToSeconds());
        }

        auto *resourceMetrics = Executor()->GetResourceMetrics();
        if (resourceMetrics) {
            auto &metrics = *rec.MutableMetrics();
            resourceMetrics->Fill(metrics);
        }
    }

    auto& info = *response->Record.MutableTabletInfo();
    info.SetSchemeShard(PathOwnerId);
    info.SetMediator(LastKnownMediator);
    info.SetGeneration(Generation());
    info.SetIsFollower(IsFollower());
    info.SetState(DatashardStateName(State));
    info.SetIsActive(IsStateActive());
    info.SetHasSharedBlobs(HasSharedBlobs());

    auto addControl = [&](ui64 value, const TString& name) {
        auto& control = *response->Record.AddControls();
        control.SetName(name);
        control.SetValue(ToString(value));
    };

    addControl(MaxTxInFly, "DataShardControls.MaxTxInFly");
    addControl(DisableByKeyFilter, "DataShardControls.DisableByKeyFilter");
    addControl(MaxTxLagMilliseconds, "DataShardControls.MaxTxLagMilliseconds");
    addControl(CanCancelROWithReadSets, "DataShardControls.CanCancelROWithReadSets");
    addControl(DataTxProfileLogThresholdMs, "DataShardControls.DataTxProfile.LogThresholdMs");
    addControl(DataTxProfileBufferThresholdMs, "DataShardControls.DataTxProfile.BufferThresholdMs");
    addControl(DataTxProfileBufferSize, "DataShardControls.DataTxProfile.BufferSize");
    addControl(PerShardReadSizeLimit, "TxLimitControls.PerShardReadSizeLimit");

    auto completed = Pipeline.GetExecutionUnit(EExecutionUnitKind::CompletedOperations).GetInFly();
    auto waiting = Pipeline.GetExecutionUnit(EExecutionUnitKind::BuildAndWaitDependencies).GetInFly();
    auto executing = Pipeline.GetActiveOps().size() - waiting - completed;
    auto& activities = *response->Record.MutableActivities();
    activities.SetInFlyPlanned(TxInFly());
    activities.SetInFlyImmediate(ImmediateInFly());
    activities.SetExecutingOps(executing);
    activities.SetWaitingOps(waiting);
    activities.SetExecuteBlockers(Pipeline.GetExecuteBlockers().size());
    activities.SetDataTxCached(Pipeline.GetDataTxCacheSize());
    activities.SetOutReadSets(OutReadSets.CountReadSets());
    activities.SetOutReadSetsAcks(OutReadSets.CountAcks());
    activities.SetDelayedAcks(Pipeline.GetDelayedAcks().size());
    activities.SetLocks(SysLocks.LocksCount());
    activities.SetBrokenLocks(SysLocks.BrokenLocksCount());
    activities.SetLastPlannedTx(Pipeline.GetLastPlannedTx().TxId);
    activities.SetLastPlannedStep(Pipeline.GetLastPlannedTx().Step);
    activities.SetLastCompletedTx(Pipeline.GetLastCompleteTx().TxId);
    activities.SetLastCompletedStep(Pipeline.GetLastCompleteTx().Step);
    activities.SetUtmostCompletedTx(Pipeline.GetUtmostCompleteTx().TxId);
    activities.SetUtmostCompletedStep(Pipeline.GetUtmostCompleteTx().Step);
    activities.SetDataTxCompleteLag(GetDataTxCompleteLag().MilliSeconds());
    activities.SetScanTxCompleteLag(GetScanTxCompleteLag().MilliSeconds());

    auto& pcfg = *response->Record.MutablePipelineConfig();
    pcfg.SetOutOfOrderEnabled(Pipeline.GetConfig().OutOfOrder());
    pcfg.SetActiveTxsLimit(Pipeline.GetConfig().LimitActiveTx);
    pcfg.SetAllowImmediate(!Pipeline.GetConfig().NoImmediate());
    pcfg.SetForceOnlineRW(Pipeline.GetConfig().ForceOnlineRW());
    pcfg.SetDirtyOnline(Pipeline.GetConfig().DirtyOnline());
    pcfg.SetDirtyImmediate(Pipeline.GetConfig().DirtyImmediate());
    pcfg.SetDataTxCacheSize(Pipeline.GetConfig().LimitDataTxCache);

    Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvListOperationsRequest::TPtr& ev) {
    auto* response = new TEvDataShard::TEvListOperationsResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    auto addOperation = [&](const TOperation::TPtr& op) {
        auto& rec = *response->Record.AddOperations();
        rec.SetTxId(op->GetTxId());
        rec.SetStep(op->GetStep());
        rec.SetKind(ToString(op->GetKind()));
        rec.SetIsImmediate(op->IsImmediate());
        rec.SetIsReadOnly(op->IsReadOnly());
        rec.SetIsWaiting(op->IsWaitingDependencies());
        rec.SetIsExecuting(op->IsExecuting());
        rec.SetIsCompleted(op->IsCompleted());
        rec.SetExecutionUnit(ToString(op->GetCurrentUnit()));
        rec.SetReceivedAt(op->GetReceivedAt().GetValue());
    };

    for (auto& pr : Pipeline.GetImmediateOps())
        addOperation(pr.second);
    for (auto& pr : TransQueue.GetTxsInFly())
        addOperation(pr.second);

    Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvGetOperationRequest::TPtr& ev) {
    auto* response = new TEvDataShard::TEvGetOperationResponse;

    auto op = Pipeline.FindOp(ev->Get()->Record.GetTxId());
    if (!op) {
        response->Record.MutableStatus()->SetCode(Ydb::StatusIds::NOT_FOUND);
        Send(ev->Sender, response);
        return;
    }

    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    auto& resp = response->Record;

    op->Serialize(*resp.MutableBasicInfo());

    auto& plan = *resp.MutableExecutionPlan();
    for (auto kind : op->GetExecutionPlan()) {
        plan.AddUnits(ToString(kind));
    }
    plan.SetIsFinished(op->IsExecutionPlanFinished());
    plan.SetCurrentUnit(static_cast<ui32>(op->GetCurrentUnitIndex()));

    auto& protoDeps = *resp.MutableDependencies();

    auto fillOpDependencies = [&](
        const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>>& deps,
        ::google::protobuf::RepeatedPtrField<NKikimrTxDataShard::TEvGetOperationResponse_TDependency>& arr)
    {
        for (auto& op : deps) {
            auto& dep = *arr.Add();
            dep.SetTarget(op->GetTxId());
            dep.AddTypes("Data");
        }
    };

    auto fillVolatileDependencies = [&](
        const absl::flat_hash_set<ui64>& deps,
        ::google::protobuf::RepeatedPtrField<NKikimrTxDataShard::TEvGetOperationResponse_TDependency>& arr)
    {
        for (ui64 txId : deps) {
            auto& dep = *arr.Add();
            dep.SetTarget(txId);
            dep.AddTypes("Data");
        }
    };

    fillOpDependencies(op->GetDependencies(), *protoDeps.MutableDependencies());
    fillOpDependencies(op->GetDependents(), *protoDeps.MutableDependents());
    fillVolatileDependencies(op->GetVolatileDependencies(), *protoDeps.MutableDependencies());

    if (op->IsExecuting() && !op->InReadSets().empty()) {
        auto& inData = *resp.MutableInputData();
        for (auto& pr : op->InReadSets()) {
            auto& rs = *inData.AddInputRS();
            rs.SetFrom(pr.first.first);
            rs.SetReceived(pr.second.size());
        }
        inData.SetRemainedInputRS(op->GetRemainReadSets());
    }

    auto& profile = *resp.MutableExecutionProfile();
    for (auto& pr : op->GetExecutionProfile().UnitProfiles) {
        auto& unit = *profile.AddUnitProfiles();
        unit.SetUnitKind(ToString(pr.first));
        if (pr.first == op->GetCurrentUnit()) {
            auto waitTime = AppData()->TimeProvider->Now() - pr.second.ExecuteTime
                - pr.second.CommitTime - pr.second.CompleteTime
                - op->GetExecutionProfile().StartUnitAt;
            unit.SetWaitTime(waitTime.GetValue());
        } else {
            unit.SetWaitTime(pr.second.WaitTime.GetValue());
        }
        unit.SetExecuteTime(pr.second.ExecuteTime.GetValue());
        unit.SetCommitTime(pr.second.CommitTime.GetValue());
        unit.SetCompleteTime(pr.second.CompleteTime.GetValue());
        unit.SetExecuteCount(pr.second.ExecuteCount);
    }

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    if (tx) {
        tx->FillState(resp);
    }

    Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvGetDataHistogramRequest::TPtr& ev) {
    auto* response = new TEvDataShard::TEvGetDataHistogramResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
    const auto& rec = ev->Get()->Record;

    if (rec.GetCollectKeySampleMs() > 0) {
        EnableKeyAccessSampling(ActorContext(),
            AppData()->TimeProvider->Now() + TDuration::MilliSeconds(rec.GetCollectKeySampleMs()));
    }

    if (rec.GetActualData()) {
        if (CurrentKeySampler == DisabledKeySampler) {
            // datashard stores expired stats
            Send(ev->Sender, response);
            return;
        }
    }

    for (const auto& pr : TableInfos) {
        const auto& tinfo = *pr.second;
        const NTable::TStats& stats = tinfo.Stats.DataStats;

        auto& hist = *response->Record.AddTableHistograms();
        hist.SetTableName(pr.second->Name);
        for (ui32 ki : tinfo.KeyColumnIds) {
            hist.AddKeyNames(tinfo.Columns.FindPtr(ki)->Name);
        }
        SerializeHistogram(tinfo, stats.DataSizeHistogram, *hist.MutableSizeHistogram());
        SerializeHistogram(tinfo, stats.RowCountHistogram, *hist.MutableCountHistogram());
        SerializeKeySample(tinfo, tinfo.Stats.AccessStats, *hist.MutableKeyAccessSample());
    }

    Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvGetRSInfoRequest::TPtr& ev) {
    auto* response = new TEvDataShard::TEvGetRSInfoResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    for (auto& pr : OutReadSets.CurrentReadSets) {
        auto& rs = *response->Record.AddOutReadSets();
        rs.SetTxId(pr.second.TxId);
        rs.SetOrigin(pr.second.Origin);
        rs.SetSource(pr.second.From);
        rs.SetDestination(pr.second.To);
        rs.SetSeqNo(pr.first);
    }

    for (auto& p : OutReadSets.ReadSetAcks) {
        auto& rec = p->Record;
        auto& ack = *response->Record.AddOutRSAcks();
        ack.SetTxId(rec.GetTxId());
        ack.SetStep(rec.GetStep());
        ack.SetOrigin(rec.GetTabletConsumer());
        ack.SetSource(rec.GetTabletSource());
        ack.SetDestination(rec.GetTabletDest());
        ack.SetSeqNo(rec.GetSeqno());
    }

    for (auto& pr : Pipeline.GetDelayedAcks()) {
        for (auto& ack : pr.second) {
            auto* ev = ack->CastAsLocal<TEvTxProcessing::TEvReadSetAck>();
            if (ev) {
                auto& rec = ev->Record;
                auto& ack = *response->Record.AddDelayedRSAcks();
                ack.SetTxId(rec.GetTxId());
                ack.SetStep(rec.GetStep());
                ack.SetOrigin(rec.GetTabletConsumer());
                ack.SetSource(rec.GetTabletSource());
                ack.SetDestination(rec.GetTabletDest());
                ack.SetSeqNo(rec.GetSeqno());
            }
        }
    }

    for (auto& e : OutReadSets.Expectations) {
        ui64 source = e.first;
        for (auto& pr : e.second) {
            auto& p = *response->Record.AddExpectations();
            p.SetSource(source);
            p.SetTxId(pr.first);
            p.SetStep(pr.second);
        }
    }

    for (auto& pr : PersistentTablets) {
        auto& p = *response->Record.AddPipes();
        p.SetDestination(pr.first);
        p.SetOutReadSets(pr.second.OutReadSets.size());
        p.SetSubscribed(pr.second.Subscribed);
    }

    Send(ev->Sender, response);
}

void TDataShard::Handle(TEvDataShard::TEvGetSlowOpProfilesRequest::TPtr& ev) {
    auto* response = new TEvDataShard::TEvGetSlowOpProfilesResponse;
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
    Pipeline.FillStoredExecutionProfiles(response->Record);
    Send(ev->Sender, response);
}

} // namespace NKikimr::NDataShard
