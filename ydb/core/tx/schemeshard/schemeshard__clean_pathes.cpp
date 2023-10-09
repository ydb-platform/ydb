#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxCleanTables : public TTransactionBase<TSchemeShard> {
    static const ui32 BucketSize = 100;
    TVector<TPathId> TablesToClean;
    ui32 RemovedCount;

    TTxCleanTables(TSelf* self, TVector<TPathId> tablesToClean)
        : TTransactionBase<TSchemeShard>(self)
        , TablesToClean(std::move(tablesToClean))
        , RemovedCount(0)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_CLEAN_TABLES;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        ui32 RemovedCount = 0;
        while (RemovedCount < BucketSize && TablesToClean) {
            TPathId tableId = TablesToClean.back();
            Self->PersistRemoveTable(db, tableId, ctx);

            ++RemovedCount;
            TablesToClean.pop_back();
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        if (RemovedCount) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "TTxCleanTables Complete"
                           << ", done PersistRemoveTable for " << RemovedCount << " tables"
                           << ", left " << TablesToClean.size()
                           << ", at schemeshard: "<< Self->TabletID());
        }

        if (TablesToClean) {
            Self->Execute(Self->CreateTxCleanTables(std::move(TablesToClean)), ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCleanTables(TVector<TPathId> tablesToClean) {
    return new TTxCleanTables(this, std::move(tablesToClean));
}

struct TSchemeShard::TTxCleanBlockStoreVolumes : public TTransactionBase<TSchemeShard> {
    static constexpr ui32 BucketSize = 1000;
    TDeque<TPathId> BlockStoreVolumesToClean;
    size_t RemovedCount = 0;

    TTxCleanBlockStoreVolumes(TSchemeShard* self, TDeque<TPathId>&& blockStoreVolumesToClean)
        : TTransactionBase(self)
        , BlockStoreVolumesToClean(std::move(blockStoreVolumesToClean))
    { }

    TTxType GetTxType() const override {
        return TXTYPE_CLEAN_BLOCKSTORE_VOLUMES;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        RemovedCount = 0;
        while (RemovedCount < BucketSize && BlockStoreVolumesToClean) {
            TPathId pathId = BlockStoreVolumesToClean.front();
            BlockStoreVolumesToClean.pop_front();

            Self->PersistRemoveBlockStoreVolume(db, pathId);
            ++RemovedCount;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (RemovedCount) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "TTxCleanBlockStoreVolumes Complete"
                           << ", done PersistRemoveBlockStoreVolume for " << RemovedCount << " volumes"
                           << ", left " << BlockStoreVolumesToClean.size()
                           << ", at schemeshard: "<< Self->TabletID());
        }

        if (BlockStoreVolumesToClean) {
            Self->Execute(Self->CreateTxCleanBlockStoreVolumes(std::move(BlockStoreVolumesToClean)), ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCleanBlockStoreVolumes(TDeque<TPathId>&& blockStoreVolumesToClean) {
    return new TTxCleanBlockStoreVolumes(this, std::move(blockStoreVolumesToClean));
}

struct TSchemeShard::TTxCleanDroppedPaths : public TTransactionBase<TSchemeShard> {
    static constexpr size_t BucketSize = 1000;
    size_t RemovedCount = 0;
    size_t SkippedCount = 0;

    TTxCleanDroppedPaths(TSchemeShard* self)
        : TTransactionBase(self)
    { }

    TTxType GetTxType() const override {
        return TXTYPE_CLEAN_DROPPED_PATHS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxCleanDroppedPaths Execute"
                         << ", " << Self->CleanDroppedPathsCandidates.size()
                         << " paths in candidate queue"
                         << ", at schemeshard: "<< Self->TabletID());

        Y_ABORT_UNLESS(Self->CleanDroppedPathsInFly);

        NIceDb::TNiceDb db(txc.DB);

        while (RemovedCount < BucketSize && !Self->CleanDroppedPathsCandidates.empty()) {
            auto itCandidate = Self->CleanDroppedPathsCandidates.end();
            TPathId pathId = *--itCandidate;
            Self->CleanDroppedPathsCandidates.erase(itCandidate);
            TPathElement::TPtr path = Self->PathsById.at(pathId);
            if (path->DbRefCount == 0 && path->Dropped()) {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxCleanDroppedPaths: PersistRemovePath for PathId# " << pathId
                    << ", at schemeshard: "<< Self->TabletID());
                Self->PersistRemovePath(db, path);
                ++RemovedCount;
            } else {
                // Probably never happens, but if someone has added a new
                // reference to a dropped path (e.g. started a new
                // publication for it), better safe than sorry.
                ++SkippedCount;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_ABORT_UNLESS(Self->CleanDroppedPathsInFly);

        if (RemovedCount || SkippedCount) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxCleanDroppedPaths Complete"
                           << ", done PersistRemovePath for " << RemovedCount << " paths"
                           << ", skipped " << SkippedCount
                           << ", left " << Self->CleanDroppedPathsCandidates.size() << " candidates"
                           << ", at schemeshard: "<< Self->TabletID());
        }

        if (!Self->CleanDroppedPathsCandidates.empty()) {
            Self->Execute(Self->CreateTxCleanDroppedPaths(), ctx);
        } else {
            Self->CleanDroppedPathsInFly = false;
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCleanDroppedPaths() {
    return new TTxCleanDroppedPaths(this);
}

void TSchemeShard::ScheduleCleanDroppedPaths() {
    if (!CleanDroppedPathsCandidates.empty() && !CleanDroppedPathsInFly && !CleanDroppedPathsDisabled) {
        Send(SelfId(), new TEvPrivate::TEvCleanDroppedPaths);
        CleanDroppedPathsInFly = true;
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvCleanDroppedPaths::TPtr&, const TActorContext& ctx) {
    Y_ABORT_UNLESS(CleanDroppedPathsInFly);
    Execute(CreateTxCleanDroppedPaths(), ctx);
}

struct TSchemeShard::TTxCleanDroppedSubDomains : public TTransactionBase<TSchemeShard> {
    static constexpr size_t BucketSize = 1000;
    size_t RemovedCount = 0;
    size_t SkippedCount = 0;

    TTxCleanDroppedSubDomains(TSchemeShard* self)
        : TTransactionBase(self)
    { }

    TTxType GetTxType() const override {
        return TXTYPE_CLEAN_DROPPED_SUBDOMAINS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxCleanDroppedSubDomains Execute"
                         << ", " << Self->CleanDroppedSubDomainsCandidates.size()
                         << " paths in candidate queue"
                         << ", at schemeshard: "<< Self->TabletID());

        Y_ABORT_UNLESS(Self->CleanDroppedSubDomainsInFly);

        NIceDb::TNiceDb db(txc.DB);

        while (RemovedCount < BucketSize && !Self->CleanDroppedSubDomainsCandidates.empty()) {
            auto itCandidate = Self->CleanDroppedSubDomainsCandidates.end();
            TPathId pathId = *--itCandidate;
            Self->CleanDroppedSubDomainsCandidates.erase(itCandidate);
            TPathElement::TPtr path = Self->PathsById.at(pathId);
            TSubDomainInfo::TPtr domain = Self->SubDomains.at(pathId);
            if (path->Dropped() &&
                path->DbRefCount == 1 &&
                path->AllChildrenCount == 0 &&
                domain->GetInternalShards().empty())
            {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxCleanDroppedPaths: PersistRemoveSubDomain for PathId# " << pathId
                    << ", at schemeshard: "<< Self->TabletID());
                Self->PersistRemoveSubDomain(db, pathId);
                ++RemovedCount;
            } else {
                // Probably never happens, but better safe than sorry.
                ++SkippedCount;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_ABORT_UNLESS(Self->CleanDroppedSubDomainsInFly);

        if (RemovedCount || SkippedCount) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxCleanDroppedSubDomains Complete"
                           << ", done PersistRemoveSubDomain for " << RemovedCount << " paths"
                           << ", skipped " << SkippedCount
                           << ", left " << Self->CleanDroppedSubDomainsCandidates.size() << " candidates"
                           << ", at schemeshard: "<< Self->TabletID());
        }

        if (!Self->CleanDroppedSubDomainsCandidates.empty()) {
            Self->Execute(Self->CreateTxCleanDroppedSubDomains(), ctx);
        } else {
            Self->CleanDroppedSubDomainsInFly = false;
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCleanDroppedSubDomains() {
    return new TTxCleanDroppedSubDomains(this);
}

void TSchemeShard::ScheduleCleanDroppedSubDomains() {
    if (!CleanDroppedSubDomainsCandidates.empty() && !CleanDroppedSubDomainsInFly && !CleanDroppedPathsDisabled) {
        Send(SelfId(), new TEvPrivate::TEvCleanDroppedSubDomains);
        CleanDroppedSubDomainsInFly = true;
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvCleanDroppedSubDomains::TPtr&, const TActorContext& ctx) {
    Y_ABORT_UNLESS(CleanDroppedSubDomainsInFly);
    Execute(CreateTxCleanDroppedSubDomains(), ctx);
}

} // NSchemeShard
} // NKikimr
