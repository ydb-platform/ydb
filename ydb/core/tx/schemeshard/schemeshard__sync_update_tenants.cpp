#include "schemeshard_impl.h"

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxSyncTenant : public TSchemeShard::TRwTxBase {
    TPathId PathId;
    TSideEffects SideEffects;

    TTxSyncTenant(TSelf *self, TPathId pathId)
        : TRwTxBase(self)
          , PathId(pathId)
    {}

    TTxType GetTxType() const override { return TXTYPE_SYNC_TENANT; }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxSyncTenant DoExecute"
                        << ", pathId: " << PathId
                        << ", at schemeshard: " << Self->TabletID());
        Y_ABORT_UNLESS(Self->IsDomainSchemeShard);

        SideEffects.UpdateTenants({PathId});
        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }
    void DoComplete(const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxSyncTenant DoComplete"
                        << ", pathId: " << PathId
                        << ", at schemeshard: " << Self->TabletID());
        SideEffects.ApplyOnComplete(Self, ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxSyncTenant(TPathId pathId) {
    return new TTxSyncTenant(this, pathId);
}

struct TSchemeShard::TTxUpdateTenant : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvUpdateTenantSchemeShard::TPtr Ev;
    THolder<TEvSchemeShard::TEvSyncTenantSchemeShard> SyncEv;
    TSideEffects SideEffects;

    TTxUpdateTenant(TSelf *self, TEvSchemeShard::TEvUpdateTenantSchemeShard::TPtr &ev)
        : TRwTxBase(self)
          , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_UPDATE_TENANT; }

    bool HasSync() {
        return bool(SyncEv);
    }

    void MakeSync() {
        if (!SyncEv) {
            SyncEv = Self->ParentDomainLink.MakeSyncMsg();
        }
    }

    TEvSchemeShard::TEvSyncTenantSchemeShard* ReleaseSync() {
        Y_ABORT_UNLESS(HasSync());
        return SyncEv.Release();
    }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        NIceDb::TNiceDb db(txc.DB);
        const auto& record = Ev->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxUpdateTenant DoExecute"
                        << ", msg: " << record.ShortDebugString()
                        << ", at schemeshard: " << Self->TabletID());

        Y_ABORT_UNLESS(!Self->IsDomainSchemeShard);
        Y_ABORT_UNLESS(record.GetTabletId() == Self->ParentDomainId.OwnerId);

        if (record.GetEffectiveACLVersion() > Self->ParentDomainEffectiveACLVersion) {
            Self->ParentDomainOwner = record.GetOwner();
            Self->ParentDomainEffectiveACL = record.GetEffectiveACL();
            Self->ParentDomainEffectiveACLVersion = record.GetEffectiveACLVersion();
            Self->ParentDomainCachedEffectiveACL.Init(Self->ParentDomainEffectiveACL);

            Self->PersistParentDomainEffectiveACL(db, record.GetOwner(), record.GetEffectiveACL(), record.GetEffectiveACLVersion());
            for (const TPathId pathId : Self->ListSubTree(Self->RootPathId(), ctx)) {
                SideEffects.PublishToSchemeBoard(InvalidOperationId, pathId);
            }

            MakeSync();
        }

        TSubDomainInfo::TPtr subdomain = Self->SubDomains.at(Self->RootPathId());
        if (record.GetSubdomainVersion() > subdomain->GetVersion()) {
            TStoragePools storagePools(record.GetStoragePools().begin(), record.GetStoragePools().end());
            subdomain->SetStoragePools(storagePools, record.GetSubdomainVersion());

            if (record.HasDeclaredSchemeQuotas()) {
                subdomain->ApplyDeclaredSchemeQuotas(record.GetDeclaredSchemeQuotas(), ctx.Now());
                // Note: subdomain version is persisted in PersistStoragePools below
                Self->PersistSubDomainDeclaredSchemeQuotas(db, Self->RootPathId(), *subdomain);
                Self->PersistSubDomainSchemeQuotas(db, Self->RootPathId(), *subdomain);
            }

            if (record.HasDatabaseQuotas()) {
                subdomain->SetDatabaseQuotas(record.GetDatabaseQuotas(), Self);
                // Note: subdomain version is persisted in PersistStoragePools below
                Self->PersistSubDomainDatabaseQuotas(db, Self->RootPathId(), *subdomain);
            }

            if (record.HasAuditSettings()) {
                subdomain->SetAuditSettings(record.GetAuditSettings());
                Self->PersistSubDomainAuditSettings(db, Self->RootPathId(), *subdomain);
            }

            if (record.HasServerlessComputeResourcesMode()) {
                subdomain->SetServerlessComputeResourcesMode(record.GetServerlessComputeResourcesMode());
                Self->PersistSubDomainServerlessComputeResourcesMode(db, Self->RootPathId(), *subdomain);
            }

            Self->PersistStoragePools(db, Self->RootPathId(), *subdomain);
            SideEffects.PublishToSchemeBoard(InvalidOperationId, Self->RootPathId());
            MakeSync();
        }

        TPathElement::TPtr path = Self->PathsById.at(Self->RootPathId());
        if (record.GetUserAttributesVersion() > path->UserAttrs->AlterVersion) {
            {
                TString errStr;
                TUserAttributes::TPtr userAttrs = new TUserAttributes(record.GetUserAttributesVersion());
                bool isOk = userAttrs->ApplyPatch(EUserAttributesOp::SyncUpdateTenants, record.GetUserAttributes(), errStr);
                Y_VERIFY_S(isOk, errStr);
                path->UserAttrs->AlterData = userAttrs;
            }

            Self->ApplyAndPersistUserAttrs(db, path->PathId);
            SideEffects.PublishToSchemeBoard(InvalidOperationId, Self->RootPathId());
            MakeSync();
        }

        auto addPrivateShard = [&] (TTabletId tabletId, TTabletTypes::EType tabletType) {
            const auto shardIdx = Self->RegisterShardInfo(
                TShardInfo(InvalidTxId, Self->RootPathId(), tabletType)
                    .WithTabletID(tabletId));
            Self->PersistUpdateNextShardIdx(db);

            Self->PersistShardMapping(db, shardIdx, tabletId, Self->RootPathId(), InvalidTxId, tabletType);

            Y_ABORT_UNLESS(record.GetSubdomainVersion() >= subdomain->GetVersion());
            if (record.GetSubdomainVersion() > subdomain->GetVersion()) {
                subdomain->SetVersion(record.GetSubdomainVersion());
            }

            subdomain->AddPrivateShard(shardIdx);
            subdomain->AddInternalShard(shardIdx);

            subdomain->Initialize(Self->ShardInfos);
            Self->PersistSubDomain(db, Self->RootPathId(), *subdomain);

            path->IncShardsInside(1);

            SideEffects.PublishToSchemeBoard(InvalidOperationId, Self->RootPathId());
            MakeSync();
        };

        if (record.HasTenantHive()) {
            TTabletId tenantHive = TTabletId(record.GetTenantHive());
            if (!subdomain->GetTenantHiveID()) {
                addPrivateShard(tenantHive, ETabletType::Hive);
            }
            Y_ABORT_UNLESS(tenantHive == subdomain->GetTenantHiveID());
        }

        if (record.HasTenantSysViewProcessor()) {
            TTabletId tenantSVP = TTabletId(record.GetTenantSysViewProcessor());
            if (!subdomain->GetTenantSysViewProcessorID()) {
                addPrivateShard(tenantSVP, ETabletType::SysViewProcessor);
            }
            Y_ABORT_UNLESS(tenantSVP == subdomain->GetTenantSysViewProcessorID());
        }

        if (record.HasTenantStatisticsAggregator()) {
            TTabletId tenantSA = TTabletId(record.GetTenantStatisticsAggregator());
            if (!subdomain->GetTenantStatisticsAggregatorID()) {
                addPrivateShard(tenantSA, ETabletType::StatisticsAggregator);
            }
            Y_ABORT_UNLESS(tenantSA == subdomain->GetTenantStatisticsAggregatorID());
        }

        if (record.HasTenantGraphShard()) {
            TTabletId tenantGS = TTabletId(record.GetTenantGraphShard());
            if (!subdomain->GetTenantGraphShardID()) {
                addPrivateShard(tenantGS, ETabletType::GraphShard);
            }
            Y_ABORT_UNLESS(tenantGS == subdomain->GetTenantGraphShardID());
        }

        if (record.HasTenantBackupController()) {
            TTabletId tenantSA = TTabletId(record.GetTenantBackupController());
            if (!subdomain->GetTenantBackupControllerID()) {
                addPrivateShard(tenantSA, ETabletType::BackupController);
            }
            Y_ABORT_UNLESS(tenantSA == subdomain->GetTenantBackupControllerID());
        }

        if (record.HasUpdateTenantRootACL()) {
            // KIKIMR-10699: transfer tenants root ACL from GSS to the TSS
            // here TSS sees the ACL from GSS
            // TSS add all the ACL from GSS to self tenant root
            NACLib::TACL tenantRootACL(record.GetUpdateTenantRootACL());

            // this is just a transformation ACL to TDiffACL
            NACLib::TDiffACL diffACL;
            for (auto ace: tenantRootACL.GetACE()) {
                diffACL.AddAccess(ace);
            }

            TString prevACL = path->ACL;
            // and this is the applying them to self tenant root ACL
            path->ApplyACL(diffACL.SerializeAsString());

            if (prevACL != path->ACL) {
                ++path->ACLVersion;
                Self->PersistACL(db, path);

                for (const TPathId pathId : Self->ListSubTree(Self->RootPathId(), ctx)) {
                    SideEffects.PublishToSchemeBoard(InvalidOperationId, pathId);
                }

                MakeSync();
            }
        }

        if (HasSync()) {
            SideEffects.Send(Ev->Sender, ReleaseSync());
        }

        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxUpdateTenant(TEvSchemeShard::TEvUpdateTenantSchemeShard::TPtr& ev) {
    return new TTxUpdateTenant(this, ev);
}

}}
