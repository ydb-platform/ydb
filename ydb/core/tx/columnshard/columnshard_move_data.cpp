#include "columnshard_impl.h"
#include "columnshard_move_data.h"

#include "blobs_action/blob_manager_db.h"
#include "engines/column_engine_logs.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

THashSet<ui32> RequestedGroups(const NKikimrTabletBase::TEvMoveData& record) {
    return { record.GetGroups().begin(), record.GetGroups().end() };
}

// Same contract as keyvalue and blob_depot: a group that is still the latest entry keeps taking writes, so the move could never converge.
std::optional<ui32> FindLiveGroup(const TTabletStorageInfo& info, const THashSet<ui32>& groups) {
    if (groups.empty()) {
        return std::nullopt;
    }
    const auto* channel = FindIfPtr(info.Channels, [&groups](const auto& channel) {
        const auto* latest = channel.LatestEntry();
        return latest != nullptr && groups.contains(latest->GroupID);
    });
    if (channel == nullptr) {
        return std::nullopt;
    }
    return channel->LatestEntry()->GroupID;
}

void RefuseMoveData(const TActorId& sender, const ui64 tabletId, const ui32 liveGroup, const TActorContext& ctx) {
    const TString reason = TStringBuilder() << "group " << liveGroup << " is still the latest history entry at tablet " << tabletId;
    YDB_LOG_WARN("MoveData refused", {"tabletId", tabletId}, {"reason", reason});
    ctx.Send(sender, new TEvTablet::TEvMoveDataResponse(tabletId, NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch, reason));
}

}   // namespace

class TMoveDataDriver::TTxCheckGC: public TTransactionBase<TColumnShard> {
    const TActorId DriverId;
    const ui64 Revision;
    bool Ready = false;

    TMoveDataDriver* GetDriver() const {
        return Self->MoveDataDriverId == DriverId ? Self->MoveDataDriver : nullptr;
    }

public:
    TTxCheckGC(TColumnShard* self, const TActorId driverId, const ui64 revision)
        : TBase(self)
        , DriverId(driverId)
        , Revision(revision)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_MOVE_DATA_CHECK_GC;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Ready = false;
        auto* driver = GetDriver();
        if (driver == nullptr || driver->State.Revision != Revision) {
            return true;
        }
        TBlobGroupSelector selector(Self->Info());
        NOlap::TBlobManagerDb db(txc.DB);
        bool hasBlobs = false;
        if (!db.HasGCBlobsForGroups(driver->State.TargetGroups, &selector, (NOlap::TTabletId)Self->TabletID(), hasBlobs)) {
            return false;
        }
        Ready = !hasBlobs;
        if (hasBlobs) {
            Self->Counters.GetCSCounters().OnMoveDataGateBlockedByGC();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto* driver = GetDriver();
        if (driver == nullptr) {
            return;
        }
        driver->State.GCCheckInFlight = false;
        if (Ready && driver->State.Revision == Revision && driver->CanCompleteMigration(driver->UpdateQueueSizes())) {
            driver->CompleteMigration(ctx);
        }
    }
};

TMoveDataDriver::TMoveDataDriver(TColumnShard* self)
    : Self(self)
    , MetadataTaskSubscription("CS::MOVE_DATA", self->Counters.GetSubscribeCounters())
{
}

void TColumnShard::Handle(TEvTablet::TEvMoveData::TPtr& ev, const TActorContext& ctx) {
    if (!HasAppData() || !AppData()->FeatureFlags.GetEnableColumnshardMoveData()) {
        TTabletExecutedFlat::Handle(ev);
        return;
    }
    TMoveDataDriver::Request(*this, ev->Get()->Record, ev->Sender, ctx);
}

void TMoveDataDriver::Request(
    TColumnShard& owner, const NKikimrTabletBase::TEvMoveData& request, const TActorId& sender, const TActorContext& ctx) {
    const THashSet<ui32> requested = RequestedGroups(request);
    if (const auto liveGroup = FindLiveGroup(*owner.Info(), requested)) {
        RefuseMoveData(sender, owner.TabletID(), *liveGroup, ctx);
        return;
    }
    if (owner.MoveDataDriver == nullptr) {
        owner.MoveDataDriver = new TMoveDataDriver(&owner);
        owner.MoveDataDriverId = ctx.RegisterWithSameMailbox(owner.MoveDataDriver);
    }
    owner.MoveDataDriver->AcceptRequest(sender, requested, ctx);
}

void TMoveDataDriver::AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx) {
    State.HiveSender = sender;
    bool changed = !State.Active;
    for (const auto groupId : requested) {
        changed |= State.TargetGroups.emplace(groupId).second;
    }
    if (!State.Active) {
        State.Active = true;
        Self->Counters.GetCSCounters().OnMoveDataStarted();
        Self->Executor()->StartMoveDataVacuumFromOwner();
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", Self->TabletID()}, {"groups", State.TargetGroups.size()}, {"changed", changed});
    // Admission is synchronous even though the gate runs on the driver's cadence.
    State.TargetsChanged |= changed;
    if (changed) {
        ++State.Revision;
    }
    ctx.Send(SelfId(), new TEvPrivate::TEvMoveDataPoke());
}

void TMoveDataDriver::RestartActualizer() {
    AFL_VERIFY(State.Active);
    State.TargetsChanged = false;
    // A fresh actualizer counts rejections from zero again.
    State.ReportedRejections = 0;
    if (!Self->HasIndex()) {
        return;
    }
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    // A changed target set replaces the actualizers and seeds fresh queues.
    index.StopMoveData();
    if (!State.TargetGroups.empty()) {
        index.StartMoveData(State.TargetGroups);
    }
}

void TMoveDataDriver::Stop(TColumnShard& owner, const TActorContext& ctx) {
    if (!owner.MoveDataDriverId) {
        return;
    }
    AFL_VERIFY(owner.MoveDataDriver != nullptr);
    // Same-mailbox callbacks cannot run between invalidation and tablet cleanup.
    owner.MoveDataDriver->Self = nullptr;
    owner.MoveDataDriver->State.Active = false;
    ctx.Send(owner.MoveDataDriverId, new TEvents::TEvPoison());
    owner.MoveDataDriver = nullptr;
    owner.MoveDataDriverId = {};
}

void TColumnShard::MoveDataCompleted(const TActorContext& ctx) {
    if (MoveDataDriver != nullptr) {
        MoveDataDriver->OnVacuumCompleted(ctx);
    }
}

void TMoveDataDriver::OnVacuumCompleted(const TActorContext& ctx) {
    if (!State.Active) {
        return;
    }
    State.VacuumCompleted = true;
    ctx.Send(SelfId(), new TEvPrivate::TEvMoveDataPoke());
}

NOlap::NActualizer::TMoveDataQueueSizes TMoveDataDriver::UpdateQueueSizes() {
    if (!Self->HasIndex()) {
        return {};
    }
    return Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().UpdateMoveDataQueueSizes();
}

bool TMoveDataDriver::CanCompleteMigration(const NOlap::NActualizer::TMoveDataQueueSizes& queues) {
    if (!State.Active) {
        return false;
    }
    Self->Counters.GetCSCounters().OnMoveDataGateChecked();
    if (State.TargetsChanged) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByReseed();
        return false;
    }

    Self->Counters.GetCSCounters().OnMoveDataQueues(queues.Pending, queues.ConfirmedToMove, queues.InFlight, queues.Uncommitted, queues.Retired);
    if (queues.Rejected > State.ReportedRejections) {
        Self->Counters.GetCSCounters().OnMoveDataPortionsRejected(queues.Rejected - State.ReportedRejections);
        State.ReportedRejections = queues.Rejected;
    }
    if (!State.VacuumCompleted) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByVacuum();
        return false;
    }
    if (queues.GetTotal() != 0) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByPortions();
        if (queues.Uncommitted) {
            YDB_LOG_INFO("MoveData gate waits for uncommitted writes", {"tabletId", Self->TabletID()}, {"uncommitted", queues.Uncommitted});
        }
        return false;
    }
    // A retired seeded portion still in the granule has not reached the GC queues, so its blobs can still sit in a target group.
    if (queues.Retired != 0) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByCleanup();
        YDB_LOG_INFO("MoveData gate waits for cleanup", {"tabletId", Self->TabletID()}, {"retired", queues.Retired});
        return false;
    }
    const auto& defaultOperator = Self->GetStoragesManager()->GetDefaultOperator();
    if (!defaultOperator->HasCollectedBeforeCurrentGeneration()) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByFirstGCRound();
        YDB_LOG_INFO("MoveData gate waits for the first GC round", {"tabletId", Self->TabletID()});
        return false;
    }
    // A shared or borrowed link is not ours to collect, so it gets its own sensor.
    const auto& sharedBlobs = defaultOperator->GetSharedBlobs();
    if (sharedBlobs && sharedBlobs->HasBlobsForGroups(State.TargetGroups)) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByShared();
        YDB_LOG_INFO("MoveData gate waits for shared blobs", {"tabletId", Self->TabletID()});
        return false;
    }
    return true;
}

void TMoveDataDriver::TryCompleteMigration(const TActorContext& ctx, const NOlap::NActualizer::TMoveDataQueueSizes& queues) {
    if (!State.GCCheckInFlight && CanCompleteMigration(queues)) {
        State.GCCheckInFlight = true;
        // Execute can complete synchronously and invalidate Self through Stop.
        Self->Execute(new TTxCheckGC(Self, SelfId(), State.Revision), ctx);
    }
}

void TMoveDataDriver::CompleteMigration(const TActorContext& ctx) {
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", Self->TabletID()});

    if (Self->HasIndex()) {
        Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().StopMoveData();
    }
    // The boot-time CutHistory scan finds drained intervals by itself, so nothing needs persisting before Success.
    ctx.Send(State.HiveSender, new TEvTablet::TEvMoveDataResponse(Self->TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    Self->Counters.GetCSCounters().OnMoveDataFinished();
    State = {};
    Stop(*Self, ctx);
}

void TMoveDataDriver::SetupMetadata() {
    if (!State.Active || !Self->HasIndex() || MetadataRequestsInFlight->Val()) {
        return;
    }
    Self->StartMetadataRequests(Self->GetIndexAs<NOlap::TColumnEngineForLogs>().CollectMoveDataMetadataRequests(), MetadataTaskSubscription,
        MetadataRequestsInFlight, SelfId());
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMetadataAccessorsInfo::TPtr& ev, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    if (!State.Active) {
        return;
    }
    AFL_VERIFY(ev->Get()->GetGeneration() == Self->Generation())("ev", ev->Get()->GetGeneration())("tablet", Self->Generation());
    ev->Get()->GetProcessor()->ApplyResult(
        ev->Get()->ExtractResult(), Self->MutableIndexAs<NOlap::TColumnEngineForLogs>());
    Self->SetupMetadata();
    RunMigrationStep(ctx);
}

void TMoveDataDriver::StartPortionRewrites() {
    if (!State.Active || !Self->HasIndex()) {
        return;
    }
    const ui64 memoryUsageLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit() : 512_MB;
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges = Self->TablesManager.MutablePrimaryIndex().StartTtl(
        {}, Self->DataLocksManager, memoryUsageLimit, NOlap::NActualizer::EActualizationScope::MoveDataOnly);
    if (indexChanges.empty()) {
        return;
    }
    Self->StartTtlChanges(std::move(indexChanges));
}

void TMoveDataDriver::RunMigrationStep(const TActorContext& ctx) {
    if (!State.Active) {
        return;
    }
    if (State.TargetsChanged) {
        RestartActualizer();
    }
    // One index walk per turn: prune retired ids and use the sizes to select work and check the gate.
    const auto queues = UpdateQueueSizes();
    if (queues.Pending) {
        SetupMetadata();
    }
    if (queues.ConfirmedToMove) {
        StartPortionRewrites();
    }
    TryCompleteMigration(ctx, queues);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    RunMigrationStep(ctx);
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    RunMigrationStep(ctx);
}

}   // namespace NKikimr::NColumnShard
