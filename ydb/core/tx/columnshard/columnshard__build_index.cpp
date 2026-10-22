#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "build_index/column_index_scan.h"

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

THolder<TEvDataShard::TEvBuildIndexProgressResponse> MakeBuildIndexResponse(
    const NKikimrTxDataShard::TEvBuildIndexCreateRequest& request,
    ui64 tabletId)
{
    auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
    response->Record.SetId(request.GetId());
    response->Record.SetTabletId(tabletId);
    response->Record.SetOwnerId(request.GetOwnerId());
    response->Record.SetPathId(request.GetPathId());
    response->Record.SetRequestSeqNoGeneration(request.GetSeqNoGeneration());
    response->Record.SetRequestSeqNoRound(request.GetSeqNoRound());
    return response;
}

void SetBuildIndexError(NKikimrTxDataShard::TEvBuildIndexProgressResponse& record, const TString& error) {
    record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
    auto* issue = record.AddIssues();
    issue->set_message(error);
}

} // namespace

class TTxBuildIndexCreate: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxBuildIndexCreate(TColumnShard* self, TEvDataShard::TEvBuildIndexCreateRequest::TPtr&& ev)
        : TBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Ev->Get()->Record;
        BuildId = request.GetId();
        SeqNoGeneration = request.GetSeqNoGeneration();
        SeqNoRound = request.GetSeqNoRound();

        if (request.GetTabletId() != Self->TabletID()) {
            Error = TStringBuilder() << "Wrong shard " << request.GetTabletId() << " this is " << Self->TabletID();
            return true;
        }
        if (!request.HasSnapshotStep() || !request.HasSnapshotTxId()) {
            Error = "Missing snapshot";
            return true;
        }
        if (!request.GetTargetName()) {
            Error = "Empty target table name";
            return true;
        }
        if (!Self->HasIndex()) {
            Error = "Column shard has no schema";
            return true;
        }

        const auto ssPathId = TSchemeShardLocalPathId::FromRawValue(request.GetPathId());
        if (!Self->GetTablesManager().ResolveInternalPathId(ssPathId, false)) {
            Error = TStringBuilder() << "Unknown table id: " << request.GetPathId();
            return true;
        }

        ReadVersion = NOlap::TSnapshot(request.GetSnapshotStep(), request.GetSnapshotTxId());
        const auto& indexInfo = Self->GetTablesManager().GetIndexInfo(ReadVersion);
        const auto addColumn = [&](const TString& name) -> bool {
            const auto columnId = indexInfo.GetColumnIdOptional(std::string(name));
            if (!columnId) {
                Error = TStringBuilder() << "Unknown column: " << name;
                return false;
            }
            const auto* column = indexInfo.GetColumns().FindPtr(*columnId);
            if (!column) {
                Error = TStringBuilder() << "Unknown column id: " << *columnId;
                return false;
            }
            Columns.push_back(TColumnIndexScanColumn{name, *columnId, column->second});
            return true;
        };
        for (const auto& name : request.GetIndexColumns()) {
            if (!addColumn(name)) {
                return true;
            }
        }
        KeyColumnCount = Columns.size();
        if (KeyColumnCount == 0) {
            Error = "Index columns are empty";
            return true;
        }
        for (const auto& name : request.GetDataColumns()) {
            if (!addColumn(name)) {
                return true;
            }
        }

        if (Self->GetMaxReadVersion() < ReadVersion) {
            Deferred = true;
            return true;
        }

        if (const auto* existing = Self->BuildIndexScans.FindPtr(BuildId)) {
            if (existing->SeqNoGeneration == SeqNoGeneration && existing->SeqNoRound == SeqNoRound) {
                ResendFinal = !existing->FinalResponse.empty();
                return true;
            }
            OldActor = existing->ActorId;
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IndexBuildScans>().Key(BuildId).Update(
            NIceDb::TUpdate<Schema::IndexBuildScans::SeqNoGeneration>(SeqNoGeneration),
            NIceDb::TUpdate<Schema::IndexBuildScans::SeqNoRound>(SeqNoRound),
            NIceDb::TUpdate<Schema::IndexBuildScans::FinalResponse>(TString()));

        TColumnShard::TBuildIndexScanState state;
        state.SeqNoGeneration = SeqNoGeneration;
        state.SeqNoRound = SeqNoRound;
        Self->BuildIndexScans[BuildId] = std::move(state);
        Start = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Error) {
            auto response = MakeBuildIndexResponse(Ev->Get()->Record, Self->TabletID());
            SetBuildIndexError(response->Record, Error);
            YDB_LOG_ERROR("Rejecting column index build scan",
                {"tabletId", Self->TabletID()},
                {"buildId", BuildId},
                {"error", Error});
            ctx.Send(Ev->Sender, std::move(response));
            return;
        }
        if (Deferred) {
            Self->WaitingBuildIndexRequests.emplace(ReadVersion, std::move(Ev));
            Self->WaitPlanStep(ReadVersion.GetPlanStep());
            return;
        }
        if (ResendFinal) {
            const auto* existing = Self->BuildIndexScans.FindPtr(BuildId);
            if (existing && !existing->FinalResponse.empty()) {
                auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
                if (response->Record.ParseFromString(existing->FinalResponse)) {
                    Self->PendingBuildIndexFinalResponses[BuildId] = std::move(response);
                    Self->SendPendingBuildIndexFinalResponses(ctx);
                }
            }
            return;
        }
        if (!Start) {
            return;
        }

        Self->PendingBuildIndexFinalResponses.erase(BuildId);
        if (OldActor) {
            ctx.Send(OldActor, new TEvents::TEvPoisonPill());
        }
        YDB_LOG_NOTICE("Starting column index build scan",
            {"tabletId", Self->TabletID()},
            {"buildId", BuildId},
            {"snapshot", ReadVersion});
        const TActorId actorId = ctx.Register(CreateColumnTableIndexScan(
            Self->SelfId(), Ev->Get()->Record, std::move(Columns), KeyColumnCount));
        Self->BuildIndexScans.at(BuildId).ActorId = actorId;
        Self->ActorsToStop.push_back(actorId);
    }

    TTxType GetTxType() const override {
        return TXTYPE_BUILD_INDEX_SCAN;
    }

private:
    TEvDataShard::TEvBuildIndexCreateRequest::TPtr Ev;
    ui64 BuildId = 0;
    ui64 SeqNoGeneration = 0;
    ui64 SeqNoRound = 0;
    NOlap::TSnapshot ReadVersion = NOlap::TSnapshot::Zero();
    TVector<TColumnIndexScanColumn> Columns;
    ui32 KeyColumnCount = 0;
    TActorId OldActor;
    TString Error;
    bool Deferred = false;
    bool ResendFinal = false;
    bool Start = false;
};

class TTxBuildIndexProgress: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxBuildIndexProgress(TColumnShard* self, TEvDataShard::TEvBuildIndexProgressResponse::TPtr&& ev)
        : TBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Ev->Get()->Record;
        BuildId = record.GetId();
        const auto* info = Self->BuildIndexScans.FindPtr(BuildId);
        if (!info) {
            return true;
        }
        if (info->SeqNoGeneration != record.GetRequestSeqNoGeneration() || info->SeqNoRound != record.GetRequestSeqNoRound()) {
            return true;
        }

        TString serialized;
        if (!record.SerializeToString(&serialized)) {
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IndexBuildScans>().Key(BuildId).Update(
            NIceDb::TUpdate<Schema::IndexBuildScans::FinalResponse>(serialized));
        Self->BuildIndexScans[BuildId].FinalResponse = serialized;
        Self->BuildIndexScans[BuildId].ActorId = {};
        Accepted = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Accepted) {
            return;
        }
        auto copy = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        copy->Record = Ev->Get()->Record;
        Self->PendingBuildIndexFinalResponses[BuildId] = std::move(copy);
        Self->SendPendingBuildIndexFinalResponses(ctx);
    }

    TTxType GetTxType() const override {
        return TXTYPE_BUILD_INDEX_SCAN;
    }

private:
    TEvDataShard::TEvBuildIndexProgressResponse::TPtr Ev;
    ui64 BuildId = 0;
    bool Accepted = false;
};

bool TColumnShard::LoadBuildIndexScans(NIceDb::TNiceDb& db) {
    BuildIndexScans.clear();
    auto rowset = db.Table<Schema::IndexBuildScans>().Select();
    if (!rowset.IsReady()) {
        return false;
    }
    while (!rowset.EndOfSet()) {
        TBuildIndexScanState info;
        const ui64 buildId = rowset.GetValue<Schema::IndexBuildScans::BuildId>();
        info.SeqNoGeneration = rowset.GetValue<Schema::IndexBuildScans::SeqNoGeneration>();
        info.SeqNoRound = rowset.GetValue<Schema::IndexBuildScans::SeqNoRound>();
        info.FinalResponse = rowset.GetValue<Schema::IndexBuildScans::FinalResponse>();
        BuildIndexScans.emplace(buildId, std::move(info));
        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

void TColumnShard::SendPendingBuildIndexFinalResponses(const TActorContext& ctx) {
    if (!CurrentSchemeShardId || PendingBuildIndexFinalResponses.empty()) {
        return;
    }
    if (!BuildIndexPipe) {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        BuildIndexPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, CurrentSchemeShardId, config));
    }
    for (auto& [buildId, response] : PendingBuildIndexFinalResponses) {
        Y_UNUSED(buildId);
        auto copy = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        copy->Record = response->Record;
        NTabletPipe::SendData(ctx, BuildIndexPipe, copy.Release());
    }
}

void TColumnShard::ReportBuildIndexScansAfterRestart(const TActorContext& ctx) {
    for (const auto& [buildId, info] : BuildIndexScans) {
        auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        if (!info.FinalResponse.empty() && response->Record.ParseFromString(info.FinalResponse)) {
            YDB_LOG_NOTICE("Resending persisted column index build result",
                {"tabletId", TabletID()},
                {"buildId", buildId});
        } else {
            response->Record.SetId(buildId);
            response->Record.SetTabletId(TabletID());
            response->Record.SetRequestSeqNoGeneration(info.SeqNoGeneration);
            response->Record.SetRequestSeqNoRound(info.SeqNoRound);
            response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
            YDB_LOG_NOTICE("Column index build scan was interrupted and will be retried",
                {"tabletId", TabletID()},
                {"buildId", buildId});
        }
        PendingBuildIndexFinalResponses[buildId] = std::move(response);
    }
    SendPendingBuildIndexFinalResponses(ctx);
}

void TColumnShard::Handle(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxBuildIndexCreate(this, std::move(ev)), ctx);
}

void TColumnShard::Handle(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& ev, const TActorContext& ctx) {
    const auto status = ev->Get()->Record.GetStatus();
    if (status == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS) {
        if (!CurrentSchemeShardId) {
            return;
        }
        if (!BuildIndexPipe) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            BuildIndexPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, CurrentSchemeShardId, config));
        }
        NTabletPipe::SendData(ctx, BuildIndexPipe, ev->Release().Release());
        return;
    }
    Execute(new TTxBuildIndexProgress(this, std::move(ev)), ctx);
}

}
