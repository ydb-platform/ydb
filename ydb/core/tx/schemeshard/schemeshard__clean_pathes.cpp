#include "schemeshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

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
            YDBLOG_CTX_WARN(ctx, "TTxCleanTables Complete, done PersistRemoveTable for  tables, left , at schemeshard: ",
                {"#_RemovedCount", RemovedCount},
                {"#_TablesToClean.size()", TablesToClean.size()},
                {"schemeshard", Self->TabletID()});
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
            YDBLOG_CTX_WARN(ctx, "TTxCleanBlockStoreVolumes Complete, done PersistRemoveBlockStoreVolume for  volumes, left , at schemeshard: ",
                {"#_RemovedCount", RemovedCount},
                {"#_BlockStoreVolumesToClean.size()", BlockStoreVolumesToClean.size()},
                {"schemeshard", Self->TabletID()});
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
        YDBLOG_CTX_DEBUG(ctx, "TTxCleanDroppedPaths Execute,  paths in candidate queue, at schemeshard: ",
            {"#_num_0", Self->CleanDroppedPathsCandidates.size()},
            {"schemeshard", Self->TabletID()});

        Y_ABORT_UNLESS(Self->CleanDroppedPathsInFly);

        NIceDb::TNiceDb db(txc.DB);

        while (RemovedCount < BucketSize && !Self->CleanDroppedPathsCandidates.empty()) {
            auto itCandidate = Self->CleanDroppedPathsCandidates.end();
            TPathId pathId = *--itCandidate;
            Self->CleanDroppedPathsCandidates.erase(itCandidate);
            TPathElement::TPtr path = Self->PathsById.at(pathId);
            if (path->DbRefCount == 0 && path->Dropped()) {
                YDBLOG_CTX_DEBUG(ctx, "TTxCleanDroppedPaths: PersistRemovePath for PathId# , at schemeshard: ",
                    {"PathId", pathId},
                    {"schemeshard", Self->TabletID()});
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
            YDBLOG_CTX_NOTICE(ctx, "TTxCleanDroppedPaths Complete, done PersistRemovePath for  paths, skipped , left  candidates, at schemeshard: ",
                {"#_RemovedCount", RemovedCount},
                {"#_SkippedCount", SkippedCount},
                {"#_num_0", Self->CleanDroppedPathsCandidates.size()},
                {"schemeshard", Self->TabletID()});
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
        YDBLOG_CTX_DEBUG(ctx, "TTxCleanDroppedSubDomains Execute,  paths in candidate queue, at schemeshard: ",
            {"#_num_0", Self->CleanDroppedSubDomainsCandidates.size()},
            {"schemeshard", Self->TabletID()});

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
                YDBLOG_CTX_DEBUG(ctx, "TTxCleanDroppedPaths: PersistRemoveSubDomain for PathId# , at schemeshard: ",
                    {"PathId", pathId},
                    {"schemeshard", Self->TabletID()});
                Self->PersistRemoveSubDomain(db, pathId);
                ++RemovedCount;

                // This is for tests, so that tests could wait for actual lifetime end of a subdomain.
                // It's kinda ok to reply from execute, and actually required for tests with reboots
                // (to not lose event on a tablet reboot).
                {
                    ctx.Send(Self->SelfId(), new TEvPrivate::TEvTestNotifySubdomainCleanup(pathId));
                }
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
            YDBLOG_CTX_NOTICE(ctx, "TTxCleanDroppedSubDomains Complete, done PersistRemoveSubDomain for  subdomains, skipped , left  candidates, at schemeshard: ",
                {"#_RemovedCount", RemovedCount},
                {"#_SkippedCount", SkippedCount},
                {"#_num_0", Self->CleanDroppedSubDomainsCandidates.size()},
                {"schemeshard", Self->TabletID()});

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
