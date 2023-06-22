#include "datashard_impl.h"
#include "operation.h"

#include <ydb/core/tablet_flat/flat_stat_table.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/mime/types/mime.h>
#include <library/cpp/resource/resource.h>

#include <library/cpp/html/pcdata/pcdata.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard::TTxMonitoring : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    NMon::TEvRemoteHttpInfo::TPtr Ev;

public:
    TTxMonitoring(TDataShard *self, NMon::TEvRemoteHttpInfo::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext &, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                    "HTTP request at " << Self->TabletID() << " url="
                    << Ev->Get()->PathInfo());

        ReplyWithIndex(ctx);

        return true;
    }

    void ReplyWithIndex(const TActorContext &ctx)
    {
        TString blob;
        NResource::FindExact("datashard/index.html", &blob);

        if (blob.empty()) {
            ctx.Send(Ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPNOTFOUND));
            return;
        }

        TStringStream response;
        response << "HTTP/1.1 200 Ok\r\n";
        response << "Content-Type: text/html\r\n";
        response << "Content-Length: " << blob.size() << "\r\n";
        response << "\r\n";
        response.Write(blob.data(), blob.size());
        ctx.Send(Ev->Sender, new NMon::TEvRemoteBinaryInfoRes(response.Str()));
    }

    void Complete(const TActorContext &ctx) override {
        Y_UNUSED(ctx);
    }

    TTxType GetTxType() const override { return TXTYPE_MONITORING; }
};

class TDataShard::TTxGetInfo : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxGetInfo(TDataShard *self,
               TEvDataShard::TEvGetInfoRequest::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext &,
                 const TActorContext &ctx) override
    {
        auto *response = new TEvDataShard::TEvGetInfoResponse;
        response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        for (auto &pr : Self->GetUserTables()) {
            auto &rec = *response->Record.AddUserTables();
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
                stats.SetLastStatsUpdateTime(Self->LastDbStatsUpdateTime.ToStringLocalUpToSeconds());
                stats.SetLastStatsReportTime(Self->LastDbStatsReportTime.ToStringLocalUpToSeconds());
            }

            auto *resourceMetrics = Self->Executor()->GetResourceMetrics();
            if (resourceMetrics) {
                auto &metrics = *rec.MutableMetrics();
                resourceMetrics->Fill(metrics);
            }
        }

        auto &info = *response->Record.MutableTabletInfo();
        info.SetSchemeShard(Self->PathOwnerId);
        info.SetMediator(Self->LastKnownMediator);
        info.SetGeneration(Self->Generation());
        info.SetIsFollower(Self->IsFollower());
        info.SetState(DatashardStateName(Self->State));
        info.SetIsActive(Self->IsStateActive());
        info.SetHasSharedBlobs(Self->HasSharedBlobs());

        AddControl(Self->MaxTxInFly, "DataShardControls.MaxTxInFly", response->Record);
        AddControl(Self->DisableByKeyFilter, "DataShardControls.DisableByKeyFilter", response->Record);
        AddControl(Self->MaxTxLagMilliseconds, "DataShardControls.MaxTxLagMilliseconds", response->Record);
        AddControl(Self->CanCancelROWithReadSets, "DataShardControls.CanCancelROWithReadSets", response->Record);
        AddControl(Self->DataTxProfileLogThresholdMs, "DataShardControls.DataTxProfile.LogThresholdMs", response->Record);
        AddControl(Self->DataTxProfileBufferThresholdMs, "DataShardControls.DataTxProfile.BufferThresholdMs", response->Record);
        AddControl(Self->DataTxProfileBufferSize, "DataShardControls.DataTxProfile.BufferSize", response->Record);
        AddControl(Self->PerShardReadSizeLimit, "TxLimitControls.PerShardReadSizeLimit", response->Record);

        auto completed = Self->Pipeline.GetExecutionUnit(EExecutionUnitKind::CompletedOperations).GetInFly();
        auto waiting = Self->Pipeline.GetExecutionUnit(EExecutionUnitKind::BuildAndWaitDependencies).GetInFly();
        auto executing = Self->Pipeline.GetActiveOps().size() - waiting - completed;
        auto &activities = *response->Record.MutableActivities();
        activities.SetInFlyPlanned(Self->TxInFly());
        activities.SetInFlyImmediate(Self->ImmediateInFly());
        activities.SetExecutingOps(executing);
        activities.SetWaitingOps(waiting);
        activities.SetExecuteBlockers(Self->Pipeline.GetExecuteBlockers().size());
        activities.SetDataTxCached(Self->Pipeline.GetDataTxCacheSize());
        activities.SetOutReadSets(Self->OutReadSets.CountReadSets());
        activities.SetOutReadSetsAcks(Self->OutReadSets.CountAcks());
        activities.SetDelayedAcks(Self->Pipeline.GetDelayedAcks().size());
        activities.SetLocks(Self->SysLocks.LocksCount());
        activities.SetBrokenLocks(Self->SysLocks.BrokenLocksCount());
        activities.SetLastPlannedTx(Self->Pipeline.GetLastPlannedTx().TxId);
        activities.SetLastPlannedStep(Self->Pipeline.GetLastPlannedTx().Step);
        activities.SetLastCompletedTx(Self->Pipeline.GetLastCompleteTx().TxId);
        activities.SetLastCompletedStep(Self->Pipeline.GetLastCompleteTx().Step);
        activities.SetUtmostCompletedTx(Self->Pipeline.GetUtmostCompleteTx().TxId);
        activities.SetUtmostCompletedStep(Self->Pipeline.GetUtmostCompleteTx().Step);
        activities.SetDataTxCompleteLag(Self->GetDataTxCompleteLag().MilliSeconds());
        activities.SetScanTxCompleteLag(Self->GetScanTxCompleteLag().MilliSeconds());

        auto &pcfg = *response->Record.MutablePipelineConfig();
        pcfg.SetOutOfOrderEnabled(Self->Pipeline.GetConfig().OutOfOrder());
        pcfg.SetActiveTxsLimit(Self->Pipeline.GetConfig().LimitActiveTx);
        pcfg.SetAllowImmediate(!Self->Pipeline.GetConfig().NoImmediate());
        pcfg.SetForceOnlineRW(Self->Pipeline.GetConfig().ForceOnlineRW());
        pcfg.SetDirtyOnline(Self->Pipeline.GetConfig().DirtyOnline());
        pcfg.SetDirtyImmediate(Self->Pipeline.GetConfig().DirtyImmediate());
        pcfg.SetDataTxCacheSize(Self->Pipeline.GetConfig().LimitDataTxCache);

        ctx.Send(Ev->Sender, response);
        return true;
    }

    void AddControl(ui64 value,
                    const TString &name,
                    NKikimrTxDataShard::TEvGetInfoResponse &rec)
    {
        auto &control = *rec.AddControls();
        control.SetName(name);
        control.SetValue(ToString(value));
    }

    void Complete(const TActorContext &) override {}

    TTxType GetTxType() const override { return TXTYPE_MONITORING; }

private:
    TEvDataShard::TEvGetInfoRequest::TPtr Ev;
};

class TDataShard::TTxListOperations : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxListOperations(TDataShard *self,
                      TEvDataShard::TEvListOperationsRequest::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext &,
                 const TActorContext &ctx) override
    {
        auto *response = new TEvDataShard::TEvListOperationsResponse;
        response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        for (auto &pr : Self->Pipeline.GetImmediateOps())
            AddOperation(pr.second, response->Record);
        for (auto &pr : Self->TransQueue.GetTxsInFly())
            AddOperation(pr.second, response->Record);

        ctx.Send(Ev->Sender, response);
        return true;
    }

    void AddOperation(TOperation::TPtr op,
                      NKikimrTxDataShard::TEvListOperationsResponse &resp)
    {
        auto &rec = *resp.AddOperations();
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
    }

    void Complete(const TActorContext &) override {}

    TTxType GetTxType() const override { return TXTYPE_MONITORING; }

private:
    TEvDataShard::TEvListOperationsRequest::TPtr Ev;
};

class TDataShard::TTxGetOperation : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxGetOperation(TDataShard *self,
                    TEvDataShard::TEvGetOperationRequest::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext &,
                 const TActorContext &ctx) override
    {
        auto *response = new TEvDataShard::TEvGetOperationResponse;

        auto op = Self->Pipeline.FindOp(Ev->Get()->Record.GetTxId());
        if (!op)
            response->Record.MutableStatus()->SetCode(Ydb::StatusIds::NOT_FOUND);
        else {
            response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
            FillOpInfo(op, response->Record);
        }

        ctx.Send(Ev->Sender, response);
        return true;
    }

    void FillOpInfo(TOperation::TPtr op,
                    NKikimrTxDataShard::TEvGetOperationResponse &resp)
    {
        op->Serialize(*resp.MutableBasicInfo());

        auto &plan = *resp.MutableExecutionPlan();
        for (auto kind : op->GetExecutionPlan())
            plan.AddUnits(ToString(kind));
        plan.SetIsFinished(op->IsExecutionPlanFinished());
        plan.SetCurrentUnit(static_cast<ui32>(op->GetCurrentUnitIndex()));

        auto &deps = *resp.MutableDependencies();
        FillDependencies(op->GetDependencies(), *deps.MutableDependencies());
        FillDependencies(op->GetDependents(), *deps.MutableDependents());
        FillDependencies(op->GetVolatileDependencies(), *deps.MutableDependencies());

        if (op->IsExecuting() && !op->InReadSets().empty()) {
            auto &inData = *resp.MutableInputData();
            for (auto &pr : op->InReadSets()) {
                auto &rs = *inData.AddInputRS();
                rs.SetFrom(pr.first.first);
                rs.SetReceived(pr.second.size());
            }
            inData.SetRemainedInputRS(op->GetRemainReadSets());
        }

        auto &profile = *resp.MutableExecutionProfile();
        for (auto &pr : op->GetExecutionProfile().UnitProfiles) {
            auto &unit = *profile.AddUnitProfiles();
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
        if (tx)
            tx->FillState(resp);
    }

    void FillDependencies(const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &deps,
                          ::google::protobuf::RepeatedPtrField<NKikimrTxDataShard::TEvGetOperationResponse_TDependency> &arr)
    {
        for (auto &op : deps) {
            auto &dep = *arr.Add();
            dep.SetTarget(op->GetTxId());
            dep.AddTypes("Data");
        }
    }

    void FillDependencies(const absl::flat_hash_set<ui64> &deps,
                          ::google::protobuf::RepeatedPtrField<NKikimrTxDataShard::TEvGetOperationResponse_TDependency> &arr)
    {
        for (ui64 txId : deps) {
            auto &dep = *arr.Add();
            dep.SetTarget(txId);
            dep.AddTypes("Data");
        }
    }

    void Complete(const TActorContext &) override {}

    TTxType GetTxType() const override { return TXTYPE_MONITORING; }

private:
    TEvDataShard::TEvGetOperationRequest::TPtr Ev;
};

ITransaction *TDataShard::CreateTxMonitoring(TDataShard *self, NMon::TEvRemoteHttpInfo::TPtr ev)
{
    return new TTxMonitoring(self, ev);
}

ITransaction *TDataShard::CreateTxGetInfo(TDataShard *self, TEvDataShard::TEvGetInfoRequest::TPtr ev)
{
    return new TTxGetInfo(self, ev);
}

ITransaction *TDataShard::CreateTxListOperations(TDataShard *self, TEvDataShard::TEvListOperationsRequest::TPtr ev)
{
    return new TTxListOperations(self, ev);
}

ITransaction *TDataShard::CreateTxGetOperation(TDataShard *self, TEvDataShard::TEvGetOperationRequest::TPtr ev)
{
    return new TTxGetOperation(self, ev);
}

}}
