#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxInitRoot : public TSchemeShard::TRwTxBase {
    TTxInitRoot(TSelf *self)
        : TRwTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_ROOT; }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        const TDomainsInfo::TDomain& selfDomain = Self->GetDomainDescription(ctx);

        TString rootName = selfDomain.Name;

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxInitRoot DoExecute"
                         << ", path: " << rootName
                         << ", pathId: " << Self->RootPathId()
                         << ", at schemeshard: " << Self->TabletID());

        Y_VERIFY_S(Self->IsDomainSchemeShard, "Do not run this transaction on tenant schemeshard"
                                              ", tenant schemeshard needs proper initiation by domain schemeshard");
        Y_ABORT_UNLESS(!Self->IsSchemeShardConfigured());
        Y_VERIFY_S(!rootName.empty(), "invalid root name in domain config");

        TVector<TString> rootPathElements = SplitPath(rootName);
        TString joinedRootPath = JoinPath(rootPathElements);
        Y_VERIFY_S(rootPathElements.size() == 1, "invalid root name in domain config: " << rootName << " parts count: " << rootPathElements.size());

        TString owner;
        const NKikimrConfig::TDomainsConfig::TSecurityConfig& securityConfig = Self->GetDomainsConfig().GetSecurityConfig();

        for (const auto& defaultUser : securityConfig.GetDefaultUsers()) {
            auto response = Self->LoginProvider.CreateUser({
                .User = defaultUser.GetName(),
                .Password = defaultUser.GetPassword(),
            });
            if (response.Error) {
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxInitRoot DoExecute"
                         << ", path: " << rootName
                         << ", error creating user: " << defaultUser.GetName()
                         << ", error: " << response.Error);
            } else {
                auto& sid = Self->LoginProvider.Sids[defaultUser.GetName()];
                db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType, Schema::LoginSids::SidHash>(sid.Type, sid.Hash);
                if (owner.empty()) {
                    owner = defaultUser.GetName();
                }
            }
        }

        for (const auto& defaultGroup : securityConfig.GetDefaultGroups()) {
            auto response = Self->LoginProvider.CreateGroup({
                .Group = defaultGroup.GetName(),
                .Options = {
                    .CheckName = false
                }
            });
            if (response.Error) {
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxInitRoot DoExecute"
                         << ", path: " << rootName
                         << ", error creating group: " << defaultGroup.GetName()
                         << ", error: " << response.Error);
            } else {
                auto& sid = Self->LoginProvider.Sids[defaultGroup.GetName()];
                db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType>(sid.Type);
                for (const auto& member : defaultGroup.GetMembers()) {
                    auto response = Self->LoginProvider.AddGroupMembership({
                        .Group = defaultGroup.GetName(),
                        .Member = member,
                    });
                    if (response.Error) {
                        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "TTxInitRoot DoExecute"
                                << ", path: " << rootName
                                << ", error modifying group: " << defaultGroup.GetName()
                                << ", with member: " << member
                                << ", error: " << response.Error);
                    } else {
                        db.Table<Schema::LoginSidMembers>().Key(defaultGroup.GetName(), member).Update();
                    }
                }
            }
        }

        if (owner.empty()) {
            owner = BUILTIN_ACL_ROOT;
        }

        TPathElement::TPtr newPath = new TPathElement(Self->RootPathId(), Self->RootPathId(), Self->RootPathId(), joinedRootPath, owner);
        newPath->CreateTxId = TTxId(1);
        newPath->PathState = TPathElement::EPathState::EPathStateNoChanges;
        newPath->PathType = TPathElement::EPathType::EPathTypeDir;
        newPath->DirAlterVersion = 1;
        newPath->UserAttrs->AlterVersion = 1;
        Self->PathsById[Self->RootPathId()] = newPath;
        Self->NextLocalPathId = Self->RootPathId().LocalPathId + 1;
        Self->NextLocalShardIdx = 1;
        Self->ShardInfos.clear();
        Self->RootPathElements = std::move(rootPathElements);

        TSubDomainInfo::TPtr newDomain = new TSubDomainInfo(0, Self->RootPathId());
        newDomain->InitializeAsGlobal(Self->CreateRootProcessingParams(ctx));
        Self->SubDomains[Self->RootPathId()] = newDomain;

        NACLib::TDiffACL diffAcl;
        for (const auto& defaultAccess : securityConfig.GetDefaultAccess()) {
            NACLibProto::TACE ace;
            NACLib::TACL::FromString(ace, defaultAccess);
            diffAcl.AddAccess(ace);
        }
        newPath->ApplyACL(diffAcl.SerializeAsString());
        newPath->CachedEffectiveACL.Init(newPath->ACL);
        newDomain->UpdateSecurityState(Self->LoginProvider.GetSecurityState());

        Self->PersistUserAttributes(db, Self->RootPathId(), nullptr, newPath->UserAttrs);
        Self->PersistPath(db, newPath->PathId);
        Self->PersistUpdateNextPathId(db);
        Self->PersistUpdateNextShardIdx(db);
        Self->PersistStoragePools(db, Self->RootPathId(), *newDomain);
        Self->PersistSchemeLimit(db, Self->RootPathId(), *newDomain);
        Self->PersistACL(db, newPath);

        Self->InitState = TTenantInitState::Done;
        Self->PersistInitState(db);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxInitRoot DoComplete"
                         << ", at schemeshard: " << Self->TabletID());

        Self->SignalTabletActive(ctx);
        Self->ActivateAfterInitialization(ctx, {});
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInitRoot() {
    return new TTxInitRoot(this);
}

struct TSchemeShard::TTxInitRootCompatibility : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvInitRootShard::TPtr Ev;
    TSideEffects OnComplete;

    TTxInitRootCompatibility(TSelf *self, TEvSchemeShard::TEvInitRootShard::TPtr &ev)
        : TRwTxBase(self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_ROOT; }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        TPath root = TPath::Root(Self);
        auto answerTo = ActorIdFromProto(Ev->Get()->Record.GetSource());

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxInitRootCompatibility DoExecute"
                         << ", event: " << Ev->Get()->Record.ShortDebugString()
                         << ", pathId: " << Self->RootPathId()
                         << ", at schemeshard: " << Self->TabletID());

        auto reply = MakeHolder<TEvSchemeShard::TEvInitRootShardResult>(Self->TabletID(), TEvSchemeShard::TEvInitRootShardResult::StatusAlreadyInitialized);

        if (!Self->IsDomainSchemeShard) {
            reply->Record.SetStatus(TEvSchemeShard::TEvInitRootShardResult::StatusBadArgument);
            OnComplete.Send(answerTo, reply.Release());
            return;
        }

        if (Ev->Get()->Record.GetRootTagName() != root.Base()->Name) {
            reply->Record.SetStatus(TEvSchemeShard::TEvInitRootShardResult::StatusBadArgument);
            OnComplete.Send(answerTo, reply.Release());
            return;
        }

        if (!Ev->Get()->Record.HasOwner()) {
            OnComplete.Send(answerTo, reply.Release());
            return;
        }

        if (root.Base()->Owner != BUILTIN_ACL_ROOT) {
            OnComplete.Send(answerTo, reply.Release());
            return;
        }

        if (root.Base()->Owner == Ev->Get()->Record.GetOwner()) {
            OnComplete.Send(answerTo, reply.Release());
            return;
        }

        reply->Record.SetStatus(TEvSchemeShard::TEvInitRootShardResult::StatusSuccess);
        OnComplete.Send(answerTo, reply.Release());

        root.Base()->Owner = Ev->Get()->Record.GetOwner();
        Self->PersistOwner(db, root.Base());

        ++root.Base()->DirAlterVersion;
        Self->PersistPathDirAlterVersion(db, root.Base());

        // we already initialized, so we have to publish changes
        OnComplete.PublishToSchemeBoard(TOperationId(0,0), root.Base()->PathId);

        OnComplete.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxInitRootCompatibility DoComplete"
                         << ", at schemeshard: " << Self->TabletID());
        OnComplete.ApplyOnComplete(Self, ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInitRootCompatibility(TEvSchemeShard::TEvInitRootShard::TPtr &ev) {
    return new TTxInitRootCompatibility(this, ev);
}

struct TSchemeShard::TTxInitTenantSchemeShard : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvInitTenantSchemeShard::TPtr Ev;
    TAutoPtr<TEvSchemeShard::TEvInitTenantSchemeShardResult> Reply;

    TTxInitTenantSchemeShard(TSelf *self, TEvSchemeShard::TEvInitTenantSchemeShard::TPtr &ev)
        : TRwTxBase(self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_ROOT; }

    template<class TContainer>
    void RegisterShard(NIceDb::TNiceDb& db, TSubDomainInfo::TPtr& subdomain, const TContainer& tabletIds, TTabletTypes::EType type) {
        for (ui32 i = 0; i < (ui64)tabletIds.size(); ++i) {
            const auto id = TTabletId(tabletIds[i]);
            const auto shardIdx = Self->RegisterShardInfo(
                TShardInfo(InvalidTxId, Self->RootPathId(), type)
                    .WithTabletID(id));

            Self->PersistShardMapping(db, shardIdx, id, Self->RootPathId(), InvalidTxId, type);

            subdomain->AddPrivateShard(shardIdx);
            subdomain->AddInternalShard(shardIdx);
        }
    }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        const auto selfTabletId = Self->SelfTabletId();
        const NKikimrScheme::TEvInitTenantSchemeShard& record = Ev->Get()->Record;

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TEvInitTenantSchemeShard DoExecute"
                         << ", message: " << record.ShortDebugString()
                         << ", at schemeshard: " << selfTabletId);

        const TOwnerId domainSchemeShard = record.GetDomainSchemeShard();
        const TLocalPathId domainPathId = record.GetDomainPathId();

        const TString& rootPath = record.GetRootPath();
        const TString& owner = record.GetOwner();
        const TString& effectiveACL = record.GetEffectiveACL();
        const ui64 effectiveACLVersion = record.GetEffectiveACLVersion();
        const NKikimrSubDomains::TProcessingParams& processingParams = record.GetProcessingParams();
        const auto& storagePools = record.GetStoragePools();
        const auto& userAttrData = record.GetUserAttributes();
        const ui64 userAttrsVersion = record.GetUserAttributesVersion();
        const auto& schemeLimits = record.GetSchemeLimits();
        const bool initiateMigration = record.GetInitiateMigration();

        Reply.Reset(new TEvSchemeShard::TEvInitTenantSchemeShardResult(Self->TabletID(), NKikimrScheme::StatusSuccess));

        if (Self->IsDomainSchemeShard) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "TEvInitTenantSchemeShard DoExecute"
                           << ", this is wrong message for domain SchemeShard "
                           << ", at schemeshard: " << selfTabletId);
            Reply->Record.SetStatus(NKikimrScheme::StatusInvalidParameter);
            return;
        }

        TVector<TString> rootPathElements = SplitPath(rootPath);

        TString joinedRootPath = JoinPath(rootPathElements);
        Y_ABORT_UNLESS(!IsStartWithSlash(joinedRootPath)); //skip lead '/'



        if (Self->IsSchemeShardConfigured()) {
            TPathElement::TPtr root = Self->PathsById.at(Self->RootPathId());
            if (joinedRootPath != root->Name) {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "TEvInitTenantSchemeShard DoExecute"
                               << ", tenant SchemeShard has been already initialized with different root path"
                               << ", at schemeshard: " << selfTabletId);
                Reply->Record.SetStatus(NKikimrScheme::StatusInvalidParameter);
                return;
            }

            if (TTabletId(processingParams.GetSchemeShard()) != selfTabletId) {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "TEvInitTenantSchemeShard DoExecute"
                               << ", tenant SchemeShard has been already initialized, unexpected scheme shard ID in params"
                               << ", at schemeshard: " << selfTabletId);
                Reply->Record.SetStatus(NKikimrScheme::StatusInvalidParameter);
                return;
            }

            Reply->Record.SetStatus(NKikimrScheme::StatusAlreadyExists);
            return;
        }

        if (rootPath.empty() || owner.empty()) {
            Reply->Record.SetStatus(NKikimrScheme::StatusInvalidParameter);
            return;
        }

        if (rootPathElements.size() <= 1) {
            Reply->Record.SetStatus(NKikimrScheme::StatusInvalidParameter);
            return;
        }

        TUserAttributes::TPtr userAttrs = new TUserAttributes(userAttrsVersion);
        {
            TString errStr;
            bool isOk = userAttrs->ApplyPatch(EUserAttributesOp::InitRoot, userAttrData, errStr);
            Y_VERIFY_S(isOk, errStr);
        }


        TPathElement::TPtr newPath = new TPathElement(Self->RootPathId(), Self->RootPathId(), Self->RootPathId(), joinedRootPath, owner);
        newPath->CreateTxId = TTxId(1);
        newPath->PathState = TPathElement::EPathState::EPathStateNoChanges;
        newPath->PathType = TPathElement::EPathType::EPathTypeSubDomain;
        newPath->DirAlterVersion = 1;
        newPath->UserAttrs->AlterData = userAttrs;
        Self->PathsById[Self->RootPathId()] = newPath;
        Self->NextLocalPathId = Self->RootPathId().LocalPathId + 1;
        Self->NextLocalShardIdx = 1;
        Self->ShardInfos.clear();

        Self->RootPathElements = std::move(rootPathElements);

        Self->ParentDomainId = TPathId(domainSchemeShard, domainPathId);
        Self->ParentDomainOwner = owner;
        Self->ParentDomainEffectiveACL = effectiveACL;
        Self->ParentDomainEffectiveACLVersion = effectiveACLVersion;
        Self->ParentDomainCachedEffectiveACL.Init(Self->ParentDomainEffectiveACL);

        newPath->CachedEffectiveACL.Update(Self->ParentDomainCachedEffectiveACL, newPath->ACL, newPath->IsContainer());

        TPathId resourcesDomainId = Self->ParentDomainId;
        if (record.HasResourcesDomainOwnerId() && record.HasResourcesDomainPathId()) {
            resourcesDomainId = TPathId(record.GetResourcesDomainOwnerId(), record.GetResourcesDomainPathId());
        }

        TSubDomainInfo::TPtr subdomain = new TSubDomainInfo(processingParams.GetVersion(),
                                                            processingParams.GetPlanResolution(),
                                                            processingParams.GetTimeCastBucketsPerMediator(),
                                                            resourcesDomainId);

        if (record.HasSharedHive()) {
            subdomain->SetSharedHive(TTabletId(record.GetSharedHive()));
        }

        for (auto& x: storagePools) {
            subdomain->AddStoragePool(x);
        }

        subdomain->SetSchemeLimits(TSchemeLimits::FromProto(schemeLimits));

        if (record.HasDeclaredSchemeQuotas()) {
            subdomain->ApplyDeclaredSchemeQuotas(record.GetDeclaredSchemeQuotas(), ctx.Now());
        }

        if (record.HasDatabaseQuotas()) {
            subdomain->SetDatabaseQuotas(record.GetDatabaseQuotas(), Self);
        }

        if (record.HasAuditSettings()) {
            subdomain->SetAuditSettings(record.GetAuditSettings());
        }

        if (record.HasServerlessComputeResourcesMode()) {
            subdomain->SetServerlessComputeResourcesMode(record.GetServerlessComputeResourcesMode());
        }

        RegisterShard(db, subdomain, processingParams.GetCoordinators(), TTabletTypes::Coordinator);
        RegisterShard(db, subdomain, processingParams.GetMediators(), TTabletTypes::Mediator);
        RegisterShard(db, subdomain, TVector<ui64>{processingParams.GetSchemeShard()}, TTabletTypes::SchemeShard);
        if (processingParams.HasHive()) {
            RegisterShard(db, subdomain, TVector<ui64>{processingParams.GetHive()}, TTabletTypes::Hive);
        }
        if (processingParams.HasSysViewProcessor()) {
            RegisterShard(db, subdomain, TVector<ui64>{processingParams.GetSysViewProcessor()}, TTabletTypes::SysViewProcessor);
        }
        if (processingParams.HasStatisticsAggregator()) {
            RegisterShard(db, subdomain, TVector<ui64>{processingParams.GetStatisticsAggregator()}, TTabletTypes::StatisticsAggregator);
        }
        if (processingParams.HasGraphShard()) {
            RegisterShard(db, subdomain, TVector<ui64>{processingParams.GetGraphShard()}, TTabletTypes::GraphShard);
        }
        if (processingParams.HasBackupController()) {
            RegisterShard(db, subdomain, TVector<ui64>{processingParams.GetBackupController()}, TTabletTypes::BackupController);
        }

        subdomain->Initialize(Self->ShardInfos);

        Self->PersistParentDomain(db, Self->ParentDomainId);
        Self->PersistParentDomainEffectiveACL(db, owner, effectiveACL, effectiveACLVersion);

        Self->PersistPath(db, newPath->PathId);
        Self->ApplyAndPersistUserAttrs(db, newPath->PathId);

        Self->PersistSubDomain(db, Self->RootPathId(), *subdomain);
        Self->PersistSchemeLimit(db, Self->RootPathId(), *subdomain);
        Self->PersistSubDomainSchemeQuotas(db, Self->RootPathId(), *subdomain);

        Self->PersistUpdateNextPathId(db);
        Self->PersistUpdateNextShardIdx(db);

        Self->SubDomains[Self->RootPathId()] = subdomain;

        Self->InitState = initiateMigration ? TTenantInitState::Inprogress : TTenantInitState::Done;
        Self->PersistInitState(db);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TEvInitTenantSchemeShard DoComplete"
                         << ", status: " <<Reply->Record.GetStatus()
                         << ", at schemeshard: " << Self->TabletID());

        if (Reply->Record.GetStatus() != NKikimrScheme::StatusSuccess || !Self->IsSchemeShardConfigured()) {
            ctx.Send(Ev->Sender, Reply.Release());
            return;
        }

        Self->DelayedInitTenantDestination = Ev->Sender;
        Self->DelayedInitTenantReply = Reply.Release();

        auto publications = TSideEffects::TPublications();
        publications[TTxId()] = TSideEffects::TPublications::mapped_type{Self->RootPathId()};

        Self->ActivateAfterInitialization(ctx, {
            .DelayPublications = std::move(publications),
        });
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInitTenantSchemeShard(TEvSchemeShard::TEvInitTenantSchemeShard::TPtr &ev) {
    return new TTxInitTenantSchemeShard(this, ev);
}

struct TSchemeShard::TTxPublishTenantAsReadOnly : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvPublishTenantAsReadOnly::TPtr Ev;
    TAutoPtr<TEvSchemeShard::TEvPublishTenantAsReadOnlyResult> Reply;

    TTxPublishTenantAsReadOnly(TSelf *self, TEvSchemeShard::TEvPublishTenantAsReadOnly::TPtr &ev)
        : TRwTxBase(self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_SUBDOMAIN_MIGRATE; }


    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        const TTabletId selfTabletId = Self->SelfTabletId();
        const NKikimrScheme::TEvPublishTenantAsReadOnly& record = Ev->Get()->Record;
        Y_ABORT_UNLESS(Self->ParentDomainId.OwnerId == record.GetDomainSchemeShard());

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxPublishTenantAsReadOnly DoExecute"
                         << ", message: " << record.ShortDebugString()
                         << ", at schemeshard: " << selfTabletId);

        Reply = new TEvSchemeShard::TEvPublishTenantAsReadOnlyResult(ui64(selfTabletId), NKikimrScheme::StatusSuccess);

        switch (Self->InitState) {
        case TTenantInitState::Inprogress:
            Self->InitState = TTenantInitState::ReadOnlyPreview;;
            Self->PersistInitState(db);

            Self->IsReadOnlyMode = true;
            db.Table<Schema::SysParams>().Key(Schema::SysParam_IsReadOnlyMode).Update(
                NIceDb::TUpdate<Schema::SysParams::Value>("1"));
            break;
        case TTenantInitState::ReadOnlyPreview:
            Reply->Record.SetStatus(NKikimrScheme::StatusAlreadyExists);
            break;
        case TTenantInitState::InvalidState:
        case TTenantInitState::Uninitialized:
        case TTenantInitState::Done:
            Y_ABORT("Invalid state");
        };
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxPublishTenantAsReadOnly DoComplete"
                         << ", at schemeshard: " << Self->TabletID());

        if (Reply->Record.GetStatus() == NKikimrScheme::StatusSuccess) {
            Self->Execute(Self->CreateTxInit(), ctx);
        }

        ctx.Send(Ev->Sender, Reply.Release());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxPublishTenantAsReadOnly(TEvSchemeShard::TEvPublishTenantAsReadOnly::TPtr &ev) {
    return new TTxPublishTenantAsReadOnly(this, ev);
}


struct TSchemeShard::TTxPublishTenant : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvPublishTenant::TPtr Ev;
    TAutoPtr<TEvSchemeShard::TEvPublishTenantResult> Reply;

    TTxPublishTenant(TSelf *self, TEvSchemeShard::TEvPublishTenant::TPtr &ev)
        : TRwTxBase(self)
          , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_SUBDOMAIN_MIGRATE; }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        const TTabletId selfTabletId = Self->SelfTabletId();
        const NKikimrScheme::TEvPublishTenant& record = Ev->Get()->Record;
        Y_ABORT_UNLESS(Self->ParentDomainId.OwnerId == record.GetDomainSchemeShard());

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxPublishTenant DoExecute"
                         << ", message: " << record.ShortDebugString()
                         << ", at schemeshard: " << selfTabletId);

        Reply = new TEvSchemeShard::TEvPublishTenantResult(ui64(selfTabletId));

        switch (Self->InitState) {
        case TTenantInitState::ReadOnlyPreview:
            Self->InitState = TTenantInitState::Done;
            Self->PersistInitState(db);

            Self->IsReadOnlyMode = false;
            db.Table<Schema::SysParams>().Key(Schema::SysParam_IsReadOnlyMode).Update(
                NIceDb::TUpdate<Schema::SysParams::Value>("0"));
            break;
        case TTenantInitState::Done:
            break;
        case TTenantInitState::InvalidState:
        case TTenantInitState::Uninitialized:
        case TTenantInitState::Inprogress:
            Y_ABORT("Invalid state");
        };
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxPublishTenant DoComplete"
                         << ", at schemeshard: " << Self->TabletID());

        ctx.Send(Ev->Sender, Reply.Release());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxPublishTenant(TEvSchemeShard::TEvPublishTenant::TPtr &ev) {
    return new TTxPublishTenant(this, ev);
}

struct TSchemeShard::TTxMigrate : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvMigrateSchemeShard::TPtr Ev;
    TAutoPtr<TEvSchemeShard::TEvMigrateSchemeShardResult> Reply;

    TTxMigrate(TSelf *self, TEvSchemeShard::TEvMigrateSchemeShard::TPtr &ev)
        : TRwTxBase(self)
          , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_SUBDOMAIN_MIGRATE; }


    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {

        const TTabletId selfTabletId = Self->SelfTabletId();
        const NKikimrScheme::TEvMigrate& record = Ev->Get()->Record;

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxMigrate DoExecute"
                         << ", message: " << record.ShortDebugString()
                         << ", at schemeshard: " << selfTabletId);

        Reply = new TEvSchemeShard::TEvMigrateSchemeShardResult(ui64(selfTabletId));

        const NKikimrScheme::TMigratePath& pathDescr = record.GetPath();

        NIceDb::TNiceDb db(txc.DB);

        auto pathId = TPathId(pathDescr.GetPathId().GetOwnerId(), pathDescr.GetPathId().GetLocalId());
        auto parentPathId = TPathId(pathDescr.GetParentPathId().GetOwnerId(), pathDescr.GetParentPathId().GetLocalId());
        if (parentPathId == Self->ParentDomainId) { // relink root
            parentPathId = Self->RootPathId();
        }

        db.Table<Schema::MigratedPaths>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::ParentOwnerId>(parentPathId.OwnerId),
            NIceDb::TUpdate<Schema::MigratedPaths::ParentLocalId>(parentPathId.LocalPathId),
            NIceDb::TUpdate<Schema::MigratedPaths::Name>(pathDescr.GetName()),
            NIceDb::TUpdate<Schema::MigratedPaths::PathType>(pathDescr.GetPathType()),
            NIceDb::TUpdate<Schema::MigratedPaths::StepCreated>(TStepId(pathDescr.GetStepCreated())),
            NIceDb::TUpdate<Schema::MigratedPaths::CreateTxId>(TTxId(pathDescr.GetCreateTxId())),
            NIceDb::TUpdate<Schema::MigratedPaths::Owner>(pathDescr.GetOwner()),
            NIceDb::TUpdate<Schema::MigratedPaths::ACL>(pathDescr.GetACL()),
            NIceDb::TUpdate<Schema::MigratedPaths::DirAlterVersion>(pathDescr.GetDirAlterVersion() + 1), // we need to bump version on 1 to override data at SB
            NIceDb::TUpdate<Schema::MigratedPaths::UserAttrsAlterVersion>(pathDescr.GetUserAttrsAlterVersion()),
            NIceDb::TUpdate<Schema::MigratedPaths::ACLVersion>(pathDescr.GetACLVersion())
         );

        for (auto& userAttr: pathDescr.GetUserAttributes()) {
            db.Table<Schema::MigratedUserAttributes>().Key(pathId.OwnerId, pathId.LocalPathId, userAttr.GetKey()).Update(
                NIceDb::TUpdate<Schema::MigratedUserAttributes::AttrValue>(userAttr.GetValue())
                );
        }


        Reply->Record.MutablePathId()->SetOwnerId(pathId.OwnerId);
        Reply->Record.MutablePathId()->SetLocalId(pathId.LocalPathId);


        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxMigrate DoExecute"
                         << " PathInfo OK"
                         << ", at schemeshard: " << selfTabletId);

        for (const auto& shard: record.GetShards()) {
            auto shardIdx = TShardIdx(
                TOwnerId(shard.GetShardIdx().GetOwnerId()),
                TLocalShardIdx(shard.GetShardIdx().GetLocalId())
                );
            Y_ABORT_UNLESS(shardIdx.GetOwnerId() != Self->TabletID());

            TTabletTypes::EType type = (TTabletTypes::EType)shard.GetType();

            db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::MigratedShards::TabletId>(TTabletId(shard.GetTabletId())),
                NIceDb::TUpdate<Schema::MigratedShards::OwnerPathId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::MigratedShards::LocalPathId>(pathId.LocalPathId),
                NIceDb::TUpdate<Schema::MigratedShards::TabletType>(type)
                );

            for (ui32 channelId = 0; channelId < shard.BindedStoragePoolSize(); ++channelId) {
                const NKikimrStoragePool::TChannelBind& bind = shard.GetBindedStoragePool(channelId);

                db.Table<Schema::MigratedChannelsBinding>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId(), channelId).Update(
                    NIceDb::TUpdate<Schema::MigratedChannelsBinding::PoolName>(bind.GetStoragePoolName()),
                    NIceDb::TUpdate<Schema::MigratedChannelsBinding::Binding>(bind.SerializeAsString())
                    );
            }

            {
                auto item = Reply->Record.MutableShardIds()->Add();
                item->SetOwnerId(shardIdx.GetOwnerId());
                item->SetLocalId(ui64(shardIdx.GetLocalId()));
            }
        }

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxMigrate DoExecute"
                         << " ShardInfo OK"
                         << ", at schemeshard: " << selfTabletId);

        if ((TPathElement::EPathType)pathDescr.GetPathType() == TPathElement::EPathType::EPathTypeTable) {
            Y_ABORT_UNLESS(record.HasTable());
            const NKikimrScheme::TMigrateTable& tableDescr = record.GetTable();

            db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::MigratedTables::NextColId>(tableDescr.GetNextColId()),
                NIceDb::TUpdate<Schema::MigratedTables::PartitionConfig>(tableDescr.GetPartitionConfig()),
                NIceDb::TUpdate<Schema::MigratedTables::AlterVersion>(tableDescr.GetAlterVersion()),
                //NIceDb::TUpdate<Schema::MigratedTables::AlterTable>(),
                //NIceDb::TUpdate<Schema::MigratedTables::AlterTableFull>(),
                NIceDb::TUpdate<Schema::MigratedTables::PartitioningVersion>(tableDescr.GetPartitioningVersion()));

            for (const NKikimrScheme::TMigrateColumn& colDescr: tableDescr.GetColumns()) {
                db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colDescr.GetId()).Update(
                    NIceDb::TUpdate<Schema::MigratedColumns::ColName>(colDescr.GetName()),
                    NIceDb::TUpdate<Schema::MigratedColumns::ColType>(colDescr.GetColType()),
                    NIceDb::TUpdate<Schema::MigratedColumns::ColKeyOrder>(colDescr.GetColKeyOrder()),
                    NIceDb::TUpdate<Schema::MigratedColumns::CreateVersion>(colDescr.GetCreateVersion()),
                    NIceDb::TUpdate<Schema::MigratedColumns::DeleteVersion>(colDescr.GetDeleteVersion()),
                    NIceDb::TUpdate<Schema::MigratedColumns::Family>(colDescr.GetFamily()),
                    NIceDb::TUpdate<Schema::MigratedColumns::DefaultKind>(ETableColumnDefaultKind(colDescr.GetDefaultKind())),
                    NIceDb::TUpdate<Schema::MigratedColumns::DefaultValue>(colDescr.GetDefaultValue()),
                    NIceDb::TUpdate<Schema::MigratedColumns::NotNull>(colDescr.GetNotNull()),
                    NIceDb::TUpdate<Schema::MigratedColumns::IsBuildInProgress>(colDescr.GetIsBuildInProgress()));
            }

            for (const NKikimrScheme::TMigratePartition& partDescr: tableDescr.GetPartitions()) {
                const auto shardIdx = TShardIdx(partDescr.GetShardIdx().GetOwnerId(), partDescr.GetShardIdx().GetLocalId());
                ui64 id = partDescr.GetId();
                db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, id).Update(
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::RangeEnd>(partDescr.GetRangeEnd()),
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::OwnerShardIdx>(shardIdx.GetOwnerId()),
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::LocalShardIdx>(shardIdx.GetLocalId()));

                if (partDescr.HasPartitionConfig()) {
                    db.Table<Schema::MigratedTableShardPartitionConfigs>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
                        NIceDb::TUpdate<Schema::MigratedTableShardPartitionConfigs::PartitionConfig>(partDescr.GetPartitionConfig()));
                }
            }
        }

        if ((TPathElement::EPathType)pathDescr.GetPathType() == TPathElement::EPathType::EPathTypeTableIndex) {
            Y_ABORT_UNLESS(record.HasTableIndex());
            const NKikimrScheme::TMigrateTableIndex& tableIndexDescr = record.GetTableIndex();

            db.Table<Schema::MigratedTableIndex>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::MigratedTableIndex::AlterVersion>(tableIndexDescr.GetAlterVersion()),
                NIceDb::TUpdate<Schema::MigratedTableIndex::IndexType>(NKikimrSchemeOp::EIndexType(tableIndexDescr.GetType())),
                NIceDb::TUpdate<Schema::MigratedTableIndex::State>(NKikimrSchemeOp::EIndexState(tableIndexDescr.GetState())));

            for (ui32 keyId = 0; keyId < tableIndexDescr.KeysSize(); ++keyId) {
                db.Table<Schema::MigratedTableIndexKeys>().Key(pathId.OwnerId, pathId.LocalPathId, keyId).Update(
                    NIceDb::TUpdate<Schema::MigratedTableIndexKeys::KeyName>(tableIndexDescr.GetKeys(keyId)));
            }
        }

        if ((TPathElement::EPathType)pathDescr.GetPathType() == TPathElement::EPathType::EPathTypeKesus) {
            Y_ABORT_UNLESS(record.HasKesus());
            const NKikimrScheme::TMigrateKesus& kesusDescr = record.GetKesus();

            db.Table<Schema::MigratedKesusInfos>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::MigratedKesusInfos::Config>(kesusDescr.GetConfig()),
                NIceDb::TUpdate<Schema::MigratedKesusInfos::Version>(kesusDescr.GetVersion()));
        }

//            Topics,
//            PersQueues,
//            RtmrVolumes,
//            RTMRPartitions,
//            BlockStorePartitions,
//            BlockStoreVolumes,
//            KesusInfos,
//            SolomonVolumes,
//            SolomonPartitions,
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TTxMigrate DoComplete"
                         << ", at schemeshard: " << Self->TabletID());

        ctx.Send(Ev->Sender, Reply.Release());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxMigrate(TEvSchemeShard::TEvMigrateSchemeShard::TPtr &ev) {
    return new TTxMigrate(this, ev);
}
}}
