#include "schemeshard_impl.h"
#include "index/index_build_info.h"

#include <ydb/core/base/tablet_pipe.h>

#include <util/charset/utf8.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

namespace {

using TStage = NKikimrTxDataShard::TDatabaseFreezeState;

constexpr size_t MaxProgressIssueBytes = 4096;
constexpr size_t MaxParticipantIssueBytes = 256;
constexpr size_t MaxProgressParticipants = 8;

// The persistent fence and participant set live in SchemeShard. The worker is
// disposable: after a reboot a retry starts it at FENCE_WRITES and DataShards
// acknowledge stages already completed without moving backwards.
class TDatabaseFreezeWorker : public TActorBootstrapped<TDatabaseFreezeWorker> {
    const TActorId Owner;
    const ui64 SchemeShardId;
    const TPathId PathId;
    const ui64 Epoch;
    const TVector<ui64> Shards;
    TStage::EStage Stage;
    THashSet<ui64> Pending;
    THashMap<ui64, TActorId> Pipes;
    THashMap<ui64, TString> PendingIssues;

    void ReportProgress() {
        const char* stageName = "UNKNOWN";
        switch (Stage) {
        case TStage::OPEN:
            stageName = "OPEN";
            break;
        case TStage::FENCE_WRITES:
            stageName = "FENCE_WRITES";
            break;
        case TStage::DRAIN_INDEXES:
            stageName = "DRAIN_INDEXES";
            break;
        case TStage::DRAIN_CDC:
            stageName = "DRAIN_CDC";
            break;
        default:
            break;
        }
        TStringBuilder issue;
        issue << stageName << ": acknowledged " << Shards.size() - Pending.size()
            << " of " << Shards.size() << " DataShards; pending " << Pending.size();
        size_t shown = 0;
        // Show participants with diagnostics first, within the same size limit.
        for (const auto& [shard, reason] : PendingIssues) {
            if (shown == MaxProgressParticipants) {
                break;
            }
            issue << "; DataShard " << shard << ": " << reason;
            ++shown;
        }
        for (const auto shard : Pending) {
            if (shown == MaxProgressParticipants) {
                break;
            }
            if (!PendingIssues.contains(shard)) {
                issue << "; DataShard " << shard << ": awaiting acknowledgement";
                ++shown;
            }
        }
        if (shown < Pending.size()) {
            issue << "; " << Pending.size() - shown << " more participants";
        }
        auto result = MakeHolder<TEvSchemeShard::TEvDatabaseFreezeResult>();
        result->Record.SetPathId(PathId.LocalPathId);
        result->Record.SetEpoch(Epoch);
        result->Record.SetReady(false);
        result->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        result->Record.SetIssue(TString(Utf8TruncateRobust(issue, MaxProgressIssueBytes)));
        Send(Owner, result.Release());
    }

    void SendPending() {
        for (const auto shard : Pending) {
            auto& pipe = Pipes[shard];
            if (!pipe) {
                NTabletPipe::TClientConfig config;
                config.RetryPolicy = {.RetryLimitCount = 3};
                pipe = Register(NTabletPipe::CreateClient(SelfId(), shard, config));
            }
            auto request = MakeHolder<TEvDataShard::TEvDatabaseFreeze>();
            request->Record.SetEpoch(Epoch);
            request->Record.SetSchemeShardId(SchemeShardId);
            request->Record.SetDomainOwnerId(PathId.OwnerId);
            request->Record.SetDomainLocalPathId(PathId.LocalPathId);
            request->Record.SetStage(Stage);
            NTabletPipe::SendData(SelfId(), pipe, request.Release());
        }
    }

    void Finish(ui32 status, const TString& issue = {}) {
        auto result = MakeHolder<TEvSchemeShard::TEvDatabaseFreezeResult>();
        result->Record.SetPathId(PathId.LocalPathId);
        result->Record.SetEpoch(Epoch);
        result->Record.SetReady(true);
        result->Record.SetStatus(status);
        result->Record.SetIssue(issue);
        Send(Owner, result.Release());
        PassAway();
    }

    void StartStage() {
        Pending.clear();
        PendingIssues.clear();
        Pending.insert(Shards.begin(), Shards.end());
        if (Pending.empty()) {
            return Finish(Ydb::StatusIds::SUCCESS);
        }
        SendPending();
        ReportProgress();
    }

    void Handle(TEvDataShard::TEvDatabaseFreezeResult::TPtr& ev) {
        const auto& result = ev->Get()->Record;
        if (result.GetEpoch() != Epoch || result.GetStage() != Stage
            || !Pending.contains(result.GetTabletId()))
        {
            return;
        }
        if (result.GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(result.GetStatus(), result.GetIssue());
        }
        if (!result.GetReady()) {
            if (result.GetIssue().empty()) {
                PendingIssues.erase(result.GetTabletId());
            } else {
                PendingIssues[result.GetTabletId()] = TString(Utf8TruncateRobust(result.GetIssue(), MaxParticipantIssueBytes));
            }
            return;
        }
        PendingIssues.erase(result.GetTabletId());
        Pending.erase(result.GetTabletId());
        if (Pending.empty()) {
            if (Stage == TStage::OPEN || Stage == TStage::DRAIN_CDC) {
                return Finish(Ydb::StatusIds::SUCCESS);
            }
            Stage = static_cast<TStage::EStage>(static_cast<int>(Stage) + 1);
            StartStage();
        }
    }

    void Reconnect(ui64 tabletId) {
        auto it = Pipes.find(tabletId);
        if (it != Pipes.end()) {
            NTabletPipe::CloseClient(SelfId(), it->second);
            Pipes.erase(it);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        auto it = Pipes.find(ev->Get()->TabletId);
        if (ev->Get()->Status != NKikimrProto::OK && it != Pipes.end() && it->second == ev->Get()->ClientId) {
            if (Pending.contains(ev->Get()->TabletId)) {
                PendingIssues[ev->Get()->TabletId] = TStringBuilder()
                    << "tablet pipe connection failed, status " << ui32(ev->Get()->Status);
            }
            Reconnect(ev->Get()->TabletId);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        auto it = Pipes.find(ev->Get()->TabletId);
        if (it != Pipes.end() && it->second == ev->Get()->ClientId) {
            if (Pending.contains(ev->Get()->TabletId)) {
                PendingIssues[ev->Get()->TabletId] = "tablet pipe disconnected";
            }
            Reconnect(ev->Get()->TabletId);
        }
    }

    void Wakeup() {
        SendPending();
        ReportProgress();
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    void PassAway() override {
        for (const auto& [_, pipe] : Pipes) {
            NTabletPipe::CloseClient(SelfId(), pipe);
        }
        TActorBootstrapped::PassAway();
    }

public:
    TDatabaseFreezeWorker(TActorId owner, ui64 schemeShardId, TPathId pathId,
            ui64 epoch, bool freeze, TVector<ui64> shards)
        : Owner(owner)
        , SchemeShardId(schemeShardId)
        , PathId(pathId)
        , Epoch(epoch)
        , Shards(std::move(shards))
        , Stage(freeze ? TStage::FENCE_WRITES : TStage::OPEN)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        if (Shards.empty()) {
            return Finish(Ydb::StatusIds::SUCCESS);
        }
        StartStage();
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvDatabaseFreezeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TEvWakeup::EventType, Wakeup);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // anonymous namespace

bool TSchemeShard::IsDatabaseFreezeBlocking(const NKikimrSchemeOp::TModifyScheme& transaction) {
    if (DatabaseFreezes.empty()) {
        return false;
    }
    TVector<TPath> mutationPaths{TPath::Resolve(transaction.GetWorkingDir(), this)};
    auto addPath = [&](const TString& path) {
        mutationPaths.push_back(TPath::Resolve(path, this));
    };
    auto addNamedPath = [&](const TString& name) {
        // Resolve the whole name: operations may be rewritten later to
        // create intermediate directories from a multi-component name.
        addPath(TStringBuilder() << transaction.GetWorkingDir() << '/' << name);
    };
    auto addPathId = [&](const auto& pathId) {
        if constexpr (requires { pathId.GetOwnerId(); }) {
            mutationPaths.push_back(TPath::Init(TPathId::FromProto(pathId), this));
        } else {
            mutationPaths.push_back(TPath::Init(MakeLocalId(pathId), this));
        }
    };
    auto addTarget = [&](const auto& description) {
        if constexpr (requires { description.HasName(); }) {
            if (description.HasName()) {
                addNamedPath(description.GetName());
            }
        }
        if constexpr (requires { description.HasPathName(); }) {
            if (description.HasPathName()) {
                addNamedPath(description.GetPathName());
            }
        }
        if constexpr (requires { description.HasTableName(); }) {
            if (description.HasTableName()) {
                addNamedPath(description.GetTableName());
            }
        }
        if constexpr (requires { description.HasTablePath(); }) {
            if (description.HasTablePath()) {
                addPath(description.GetTablePath());
            }
        }
        if constexpr (requires { description.HasPathId(); }) {
            if (description.HasPathId()) {
                addPathId(description.GetPathId());
            }
        }
    };
    // Check payload targets as well as WorkingDir. In particular, a
    // shared SchemeShard can receive a request addressed by path ID or
    // through the parent of an independently frozen domain.
    [&](const auto&... descriptions) {
        (addTarget(descriptions), ...);
    }(
        transaction.GetMkDir(), transaction.GetCreateTable(),
        transaction.GetCreatePersQueueGroup(), transaction.GetAlterPersQueueGroup(),
        transaction.GetDrop(), transaction.GetModifyACL(),
        transaction.GetAlterTable(), transaction.GetSubDomain(),
        transaction.GetCreateRtmrVolume(), transaction.GetCreateBlockStoreVolume(),
        transaction.GetAlterBlockStoreVolume(), transaction.GetAssignBlockStoreVolume(),
        transaction.GetKesus(), transaction.GetCreateSolomonVolume(),
        transaction.GetAlterUserAttributes(), transaction.GetUpgradeSubDomain(),
        transaction.GetInitiateBuildIndexMainTable(), transaction.GetLockConfig(),
        transaction.GetAlterTableIndex(), transaction.GetFinalizeBuildIndexMainTable(),
        transaction.GetApplyIndexBuild(), transaction.GetAlterSolomonVolume(),
        transaction.GetDropIndex(), transaction.GetCancelIndexBuild(),
        transaction.GetCreateFileStore(), transaction.GetAlterFileStore(),
        transaction.GetBackup(), transaction.GetRestore(),
        transaction.GetCreateColumnStore(), transaction.GetCreateColumnTable(),
        transaction.GetAlterColumnStore(), transaction.GetAlterColumnTable(),
        transaction.GetCreateCdcStream(), transaction.GetAlterCdcStream(),
        transaction.GetDropCdcStream(), transaction.GetRotateCdcStream(),
        transaction.GetSequence(), transaction.GetReplication(),
        transaction.GetBlobDepot(), transaction.GetMoveIndex(),
        transaction.GetAllocatePersQueueGroup(), transaction.GetDeallocatePersQueueGroup(),
        transaction.GetCreateExternalTable(), transaction.GetCreateExternalDataSource(),
        transaction.GetDropColumnBuild(), transaction.GetCreateView(),
        transaction.GetAlterReplication(), transaction.GetCreateContinuousBackup(),
        transaction.GetAlterContinuousBackup(), transaction.GetDropContinuousBackup(),
        transaction.GetCreateResourcePool(), transaction.GetCreateBackupCollection(),
        transaction.GetAlterBackupCollection(), transaction.GetDropBackupCollection(),
        transaction.GetBackupBackupCollection(), transaction.GetBackupIncrementalBackupCollection(),
        transaction.GetRestoreBackupCollection(), transaction.GetCreateSysView(),
        transaction.GetCreateSecret(), transaction.GetAlterSecret(),
        transaction.GetCreateStreamingQuery(), transaction.GetTruncateTable(),
        transaction.GetPrepareIndexValidation(), transaction.GetCreateTestShardSet(),
        transaction.GetCreateIndexedTable().GetTableDescription());
    if (transaction.GetAlterTable().HasId_Deprecated()) {
        addPathId(transaction.GetAlterTable().GetId_Deprecated());
    }
    if (transaction.GetDrop().HasId()) {
        addPathId(transaction.GetDrop().GetId());
    }
    if (transaction.GetBackup().HasTableId()) {
        addPathId(transaction.GetBackup().GetTableId());
    }
    if (transaction.GetRestore().HasTableId()) {
        addPathId(transaction.GetRestore().GetTableId());
    }
    if (transaction.HasInitiateIndexBuild()) {
        addPath(transaction.GetInitiateIndexBuild().GetTable());
    }
    if (transaction.HasSplitMergeTablePartitions()) {
        const auto& split = transaction.GetSplitMergeTablePartitions();
        if (split.HasTablePath()) {
            addPath(split.GetTablePath());
        }
        if (split.HasTableLocalId()) {
            mutationPaths.push_back(TPath::Init(TPathId(
                split.HasTableOwnerId() ? split.GetTableOwnerId() : TabletID(),
                split.GetTableLocalId()), this));
        }
    }
    for (const auto* move : {&transaction.GetMoveTable(), &transaction.GetMoveTableIndex(),
        &transaction.GetMoveSequence()})
    {
        if (move->HasSrcPath()) {
            addPath(move->GetSrcPath());
        }
        if (move->HasDstPath()) {
            addPath(move->GetDstPath());
        }
    }
    if (transaction.HasCreateConsistentCopyTables()) {
        for (const auto& copy : transaction.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
            addPath(copy.GetDstPath());
            if (copy.HasCreateSrcCdcStream() || copy.HasDropSrcCdcStream()) {
                addPath(copy.GetSrcPath());
            }
        }
    }
    if (transaction.HasChangePathState()) {
        addNamedPath(transaction.GetChangePathState().GetPath());
    }
    if (transaction.HasRestoreMultipleIncrementalBackups()) {
        const auto& restore = transaction.GetRestoreMultipleIncrementalBackups();
        if (restore.HasDstTablePath()) {
            addPath(restore.GetDstTablePath());
        }
        if (restore.HasDstPathId()) {
            addPathId(restore.GetDstPathId());
        }
    }
    if (transaction.HasIncrementalRestoreLockTargets()) {
        for (const auto& path : transaction.GetIncrementalRestoreLockTargets().GetDstPaths()) {
            addPath(path);
        }
        for (const auto& path : transaction.GetIncrementalRestoreLockTargets().GetSrcPaths()) {
            addPath(path);
        }
    }
    if (transaction.HasIncrementalRestoreFinalize()) {
        const auto& restore = transaction.GetIncrementalRestoreFinalize();
        if (restore.HasBackupCollectionPathId()) {
            addPathId(restore.GetBackupCollectionPathId());
        }
        for (const auto& path : restore.GetTargetTablePaths()) {
            addPath(path);
        }
        for (const auto& path : restore.GetBackupTablePaths()) {
            addPath(path);
        }
    }
    if (transaction.HasCreateLongIncrementalBackupOp()) {
        for (const auto& stream : transaction.GetCreateLongIncrementalBackupOp().GetStreamPathIds()) {
            addPathId(stream);
        }
    }
    for (auto& path : mutationPaths) {
        path.RiseUntilExisted();
        if (path.IsResolved() && IsDatabaseFreezeBlocking(path.Base()->PathId)) {
            const auto domainId = path.GetPathIdForDomain();
            const auto freeze = DatabaseFreezes.find(domainId);
            // Let operations accepted before the fence finish their internal
            // stages. CDC initial scan finalization also needs a scheme tx.
            const bool continuation = transaction.GetInternal() && freeze != DatabaseFreezes.end()
                && !freeze->second.GetParticipantsFixed();
            const bool finishCdcScan = transaction.HasAlterCdcStream()
                && transaction.GetAlterCdcStream().HasGetReady();
            if (!continuation && !finishCdcScan) {
                return true;
            }
        }
    }
    return false;
}

bool TSchemeShard::IsDatabaseFreezeBlocking(const TPathId& pathId) const {
    if (!PathsById.contains(pathId)) {
        return false;
    }
    const auto domainId = ResolvePathIdForDomain(pathId);
    auto it = DatabaseFreezes.find(domainId);
    if (it == DatabaseFreezes.end()) {
        it = DatabaseFreezes.find(pathId);
    }
    return it != DatabaseFreezes.end() && (it->second.GetFreeze() || !it->second.GetReady()
        || it->second.GetStatus() != Ydb::StatusIds::SUCCESS);
}

bool TSchemeShard::HasDatabaseFreezeSchemaWork(const TPathId& domainId) const {
    for (const auto& [_, tx] : TxInFlight) {
        if ((PathsById.contains(tx.TargetPathId) && ResolvePathIdForDomain(tx.TargetPathId) == domainId)
            || (PathsById.contains(tx.SourcePathId) && ResolvePathIdForDomain(tx.SourcePathId) == domainId))
        {
            return true;
        }
    }
    for (const auto& [_, build] : IndexBuilds) {
        if (build->DomainPathId == domainId && !build->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, import] : Imports) {
        if (import->DomainPathId == domainId && !import->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : SetColumnConstraintOperations) {
        if (operation->DomainPathId == domainId && !operation->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : IncrementalBackups) {
        if (operation->DomainPathId == domainId && !operation->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : FullBackups) {
        if (operation->DomainPathId == domainId && !operation->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : IncrementalRestoreStates) {
        if (operation.State != TIncrementalRestoreState::EState::Completed
            && operation.State != TIncrementalRestoreState::EState::Failed
            && PathsById.contains(operation.BackupCollectionPathId)
            && ResolvePathIdForDomain(operation.BackupCollectionPathId) == domainId)
        {
            return true;
        }
    }
    return false;
}

struct TSchemeShard::TTxDatabaseFreeze : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvDatabaseFreeze::TPtr Request;
    THolder<TEvSchemeShard::TEvDatabaseFreezeResult> Response;
    TPathId PathId;
    bool RestartWorker = false;
    bool StartWorker = false;

    TTxDatabaseFreeze(TSelf* self, TEvSchemeShard::TEvDatabaseFreeze::TPtr ev)
        : TRwTxBase(self), Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_DATABASE_FREEZE; }

    void DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;
        PathId = TPathId(Self->TabletID(), request.GetPathId());
        Response = MakeHolder<TEvSchemeShard::TEvDatabaseFreezeResult>();
        auto& response = Response->Record;
        response.SetPathId(request.GetPathId());
        response.SetEpoch(request.GetEpoch());
        response.SetStatus(Ydb::StatusIds::SUCCESS);

        auto fail = [&](ui32 status, const TString& issue) {
            response.SetStatus(status);
            response.SetIssue(issue);
            response.SetReady(true);
        };
        if (!request.GetEpoch() || !Self->SubDomains.contains(PathId)
            || Self->PathsById.at(PathId)->Dropped())
        {
            return fail(Ydb::StatusIds::NOT_FOUND, "Database does not exist at this SchemeShard");
        }

        auto it = Self->DatabaseFreezes.find(PathId);
        if (it != Self->DatabaseFreezes.end() && request.GetEpoch() < it->second.GetEpoch()) {
            return fail(Ydb::StatusIds::ABORTED, "Stale database freeze epoch");
        }
        if (it != Self->DatabaseFreezes.end() && request.GetEpoch() == it->second.GetEpoch()
            && request.GetFreeze() != it->second.GetFreeze())
        {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Conflicting database freeze request");
        }

        // Refuse unsupported participants before closing any write paths.
        for (const auto& tablePathId : Self->ColumnTables.GetAllPathIds()) {
            if (request.GetFreeze() && !Self->PathsById.at(tablePathId)->Dropped()
                && Self->ResolvePathIdForDomain(tablePathId) == PathId)
            {
                return fail(Ydb::StatusIds::UNSUPPORTED, "Database freeze supports row tables only");
            }
        }
        for (const auto& [_, shard] : Self->ShardInfos) {
            if (request.GetFreeze() && shard.TabletType == TTabletTypes::ColumnShard
                && Self->PathsById.contains(shard.PathId)
                && Self->ResolvePathIdForDomain(shard.PathId) == PathId)
            {
                return fail(Ydb::StatusIds::UNSUPPORTED, "Database freeze supports row tables only");
            }
        }

        NIceDb::TNiceDb db(txc.DB);
        auto& state = Self->DatabaseFreezes[PathId];
        bool changed = false;
        if (request.GetEpoch() > state.GetEpoch()) {
            Self->DatabaseFreezeProgress.erase(PathId);
            state.Clear();
            state.SetEpoch(request.GetEpoch());
            state.SetFreeze(request.GetFreeze());
            RestartWorker = true;
            changed = true;
        }
        if (!state.GetParticipantsFixed() && !Self->HasDatabaseFreezeSchemaWork(PathId)) {
            THashSet<ui64> participants;
            // Retired split sources may still be present in ShardInfos. The
            // completed split protocol has drained their CDC and activated the
            // destinations before acknowledging partitioning changes.
            for (const auto& [tablePathId, table] : Self->Tables) {
                if (!Self->PathsById.at(tablePathId)->Dropped()
                    && Self->ResolvePathIdForDomain(tablePathId) == PathId)
                {
                    for (const auto* partition : table->GetPartitions()) {
                        participants.insert(ui64(Self->ShardInfos.at(partition->ShardIdx).TabletID));
                    }
                }
            }
            for (const auto tablet : participants) {
                state.AddShards(tablet);
            }
            state.SetParticipantsFixed(true);
            changed = true;
        }
        StartWorker = state.GetParticipantsFixed() && !state.GetReady()
            && (RestartWorker || !Self->DatabaseFreezeWorkers.contains(PathId));
        if (changed) {
            db.Table<Schema::DatabaseFreezes>().Key(PathId.LocalPathId)
                .Update<Schema::DatabaseFreezes::State>(state.SerializeAsString());
        }
        response.SetReady(state.GetReady());
        if (state.GetReady()) {
            response.SetStatus(state.GetStatus());
            response.SetIssue(state.GetIssue());
        } else if (!state.GetParticipantsFixed()) {
            response.SetIssue("Waiting for previously accepted schema operations to finish");
        } else if (const auto progress = Self->DatabaseFreezeProgress.find(PathId);
            progress != Self->DatabaseFreezeProgress.end())
        {
            response.SetIssue(progress->second);
        } else {
            response.SetIssue("Waiting for DataShard acknowledgements");
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        // Another request may execute before this transaction's Complete.
        // Never start a superseding epoch using this request's participant
        // readiness, or replace a worker already started by a duplicate poll.
        const auto state = Self->DatabaseFreezes.find(PathId);
        if (state != Self->DatabaseFreezes.end()
            && state->second.GetEpoch() == Request->Get()->Record.GetEpoch())
        {
            if (RestartWorker) {
                auto it = Self->DatabaseFreezeWorkers.find(PathId);
                if (it != Self->DatabaseFreezeWorkers.end()) {
                    ctx.Send(it->second, new TEvents::TEvPoisonPill);
                    Self->DatabaseFreezeWorkers.erase(it);
                }
            }
            if (StartWorker && state->second.GetParticipantsFixed() && !state->second.GetReady()
                && !Self->DatabaseFreezeWorkers.contains(PathId))
            {
                // DataShard stores the domain path in its owning SchemeShard,
                // including a tenant SchemeShard's local root path.
                TVector<ui64> shards(state->second.GetShards().begin(), state->second.GetShards().end());
                Self->DatabaseFreezeWorkers[PathId] = ctx.RegisterWithSameMailbox(new TDatabaseFreezeWorker(
                    Self->SelfId(), Self->TabletID(), PathId, state->second.GetEpoch(),
                    state->second.GetFreeze(), std::move(shards)));
            }
        }
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
    }
};

struct TSchemeShard::TTxDatabaseFreezeDone : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvDatabaseFreezeResult::TPtr Event;
    TPathId PathId;
    NKikimrScheme::TDatabaseFreezeState Completed;

    TTxDatabaseFreezeDone(TSelf* self, TEvSchemeShard::TEvDatabaseFreezeResult::TPtr ev)
        : TRwTxBase(self), Event(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_DATABASE_FREEZE; }

    void DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& result = Event->Get()->Record;
        PathId = TPathId(Self->TabletID(), result.GetPathId());
        auto it = Self->DatabaseFreezes.find(PathId);
        auto worker = Self->DatabaseFreezeWorkers.find(PathId);
        if (it == Self->DatabaseFreezes.end() || worker == Self->DatabaseFreezeWorkers.end()
            || worker->second != Event->Sender || it->second.GetEpoch() != result.GetEpoch())
        {
            PathId = InvalidPathId;
            return;
        }
        Completed.CopyFrom(it->second);
        Completed.SetReady(true);
        Completed.SetStatus(result.GetStatus());
        Completed.SetIssue(result.GetIssue());
        NIceDb::TNiceDb(txc.DB).Table<Schema::DatabaseFreezes>().Key(PathId.LocalPathId)
            .Update<Schema::DatabaseFreezes::State>(Completed.SerializeAsString());
    }

    void DoComplete(const TActorContext&) override {
        const auto state = Self->DatabaseFreezes.find(PathId);
        if (state != Self->DatabaseFreezes.end() && state->second.GetEpoch() == Completed.GetEpoch()) {
            state->second.CopyFrom(Completed);
            Self->DatabaseFreezeProgress.erase(PathId);
            Self->DatabaseFreezeWorkers.erase(PathId);
        }
    }
};

void TSchemeShard::Handle(TEvSchemeShard::TEvDatabaseFreeze::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDatabaseFreeze(this, ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvDatabaseFreezeResult::TPtr& ev, const TActorContext& ctx) {
    if (!ev->Get()->Record.GetReady()) {
        const auto& result = ev->Get()->Record;
        const TPathId pathId(TabletID(), result.GetPathId());
        const auto state = DatabaseFreezes.find(pathId);
        const auto worker = DatabaseFreezeWorkers.find(pathId);
        if (state != DatabaseFreezes.end() && worker != DatabaseFreezeWorkers.end()
            && worker->second == ev->Sender && state->second.GetEpoch() == result.GetEpoch()
            && !state->second.GetReady() && result.GetStatus() == Ydb::StatusIds::SUCCESS)
        {
            DatabaseFreezeProgress[pathId] = TString(Utf8TruncateRobust(result.GetIssue(), MaxProgressIssueBytes));
        }
        return;
    }
    Execute(new TTxDatabaseFreezeDone(this, ev), ctx);
}

} // namespace NKikimr::NSchemeShard
