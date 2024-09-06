#include "console_tenants_manager.h"
#include "console_impl.h"
#include "http.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/util/pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_NOTICE
#error log macro definition clash
#endif

#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CMS_TENANTS, stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CMS_TENANTS, stream)
#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS_TENANTS, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CMS_TENANTS, stream)
#define BLOG_CRIT(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::CMS_TENANTS, stream)


namespace NKikimr::NConsole {

using namespace NOperationId;
using namespace NTenantSlotBroker;

namespace {

class TPoolManip : public TActorBootstrapped<TPoolManip> {
public:
    enum EAction {
        ALLOCATE,
        DEALLOCATE
    };

private:
    using TBase = TActorBootstrapped<TPoolManip>;

    TActorId OwnerId;
    TDomainsInfo::TDomain::TPtr Domain;
    TTenantsManager::TTenant::TPtr Tenant;
    TTenantsManager::TStoragePool::TPtr Pool;
    TActorId BSControllerPipe;
    bool PoolStateAcquired;
    EAction Action;
    ui64 PoolId;
    TString LogPrefix;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_TENANTS_MANAGER;
    }

    TPoolManip(TActorId ownerId, TDomainsInfo::TDomain::TPtr domain,
               TTenantsManager::TTenant::TPtr tenant, TTenantsManager::TStoragePool::TPtr pool,
               EAction action)
        : OwnerId(ownerId)
        , Domain(domain)
        , Tenant(tenant)
        , Pool(pool)
        , PoolStateAcquired(false)
        , Action(action)
        , PoolId(0)
    {
        LogPrefix = Sprintf("TPoolManip(%s) ", Pool->Config.GetName().data());
    }

    void OpenPipe(const TActorContext &ctx)
    {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = FastConnectRetryPolicy();
        auto tid = MakeBSControllerID();
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, tid, pipeConfig);
        BSControllerPipe = ctx.ExecutorThread.RegisterActor(pipe);
    }

    void OnPipeDestroyed(const TActorContext &ctx)
    {
        BLOG_D(LogPrefix << "pipe destroyed");

        if (BSControllerPipe) {
            NTabletPipe::CloseClient(ctx, BSControllerPipe);
            BSControllerPipe = TActorId();
        }

        DoWork(ctx);
    }

    void DoWork(const TActorContext &ctx)
    {
        if (!BSControllerPipe)
            OpenPipe(ctx);

        if (!PoolStateAcquired)
            ReadPoolState(ctx);
        else if (Action == ALLOCATE)
            AllocatePool(ctx);
        else {
            Y_ABORT_UNLESS(Action == DEALLOCATE);
            DeletePool(ctx);
        }
    }

    void ReadPoolState(const TActorContext &ctx)
    {
        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto &read = *request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool();
        read.SetBoxId(Pool->Config.GetBoxId());
        read.AddName(Pool->Config.GetName());

        BLOG_D(LogPrefix << "read pool state: " << request->Record.ShortDebugString());

        NTabletPipe::SendData(ctx, BSControllerPipe, request.Release());
    }

    void AllocatePool(const TActorContext &ctx)
    {
        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto *pool = request->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool();
        pool->CopyFrom(Pool->Config);
        if (!pool->GetKind()) {
            pool->SetKind(Pool->Kind);
        }

        BLOG_D(LogPrefix << "send pool request: " << request->Record.ShortDebugString());

        NTabletPipe::SendData(ctx, BSControllerPipe, request.Release());
    }

    void DeletePool(const TActorContext &ctx)
    {
        if (!PoolId) {
            BLOG_D(LogPrefix << "cannot delete missing pool " << Pool->Config.GetName());
            ctx.Send(OwnerId, new TTenantsManager::TEvPrivate::TEvPoolDeleted(Tenant, Pool));
            Die(ctx);
            return;
        }

        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto &del = *request->Record.MutableRequest()->AddCommand()->MutableDeleteStoragePool();
        del.SetBoxId(Pool->Config.GetBoxId());
        del.SetStoragePoolId(PoolId);
        del.SetItemConfigGeneration(Pool->Config.GetItemConfigGeneration());

        BLOG_D(LogPrefix << "send pool request: " << request->Record.ShortDebugString());

        NTabletPipe::SendData(ctx, BSControllerPipe, request.Release());
    }

    void Bootstrap(const TActorContext &ctx)
    {
        BLOG_D(LogPrefix << "Bootstrap");

        Become(&TThis::StateRead);

        DoWork(ctx);
    }

    void ReplyAndDie(IEventBase *resp,
                     const TActorContext &ctx)
    {
        BLOG_D(LogPrefix << "reply with " << resp->ToString());
        ctx.Send(OwnerId, resp);
        Die(ctx);
    }

    void Die(const TActorContext &ctx) override
    {
        if (BSControllerPipe)
            NTabletPipe::CloseClient(ctx, BSControllerPipe);
        TBase::Die(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx)
    {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->ClientId == BSControllerPipe && msg->Status != NKikimrProto::OK)
            OnPipeDestroyed(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx)
    {
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        if (msg->ClientId == BSControllerPipe)
            OnPipeDestroyed(ctx);
    }

    void HandleRead(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev, const TActorContext& ctx)
    {
        auto &rec = ev->Get()->Record.GetResponse();

        BLOG_D(LogPrefix << "got read response: " << rec.ShortDebugString());

        if (!CheckReadStatus(rec)) {
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvPoolFailed(Tenant, Pool, Pool->Issue), ctx);
            return;
        }

        PoolStateAcquired = true;
        if (rec.GetStatus(0).StoragePoolSize()) {
            auto &pool = rec.GetStatus(0).GetStoragePool(0);
            auto gen = pool.GetItemConfigGeneration();
            Pool->Config.SetItemConfigGeneration(gen);
            PoolId = pool.GetStoragePoolId();
        } else {
            Pool->Config.SetItemConfigGeneration(0);
        }

        if (Action == ALLOCATE)
            Become(&TThis::StateAllocate);
        else {
            Y_ABORT_UNLESS(Action == DEALLOCATE);
            Become(&TThis::StateDelete);
        }
        DoWork(ctx);
    }

    bool CheckReadStatus(const NKikimrBlobStorage::TConfigResponse &resp)
    {
        if (!resp.GetSuccess() || !resp.GetStatus(0).GetSuccess()) {
            TString error = resp.GetErrorDescription();
            if (resp.StatusSize() && resp.GetStatus(0).GetErrorDescription())
                error = resp.GetStatus(0).GetErrorDescription();

            BLOG_D(LogPrefix << "cannot read pool '" << Pool->Config.GetName() << "' ("
                        << Pool->Config.GetStoragePoolId() << "): " << error);
            Pool->Issue = error;
            return false;
        }

        return true;
    }

    void HandleAllocate(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev, const TActorContext& ctx)
    {
        auto &rec = ev->Get()->Record.GetResponse();

        BLOG_D(LogPrefix << "got config response: " << rec.ShortDebugString());

        if (!rec.GetSuccess() || !rec.GetStatus(0).GetSuccess()) {
            TString error = rec.GetErrorDescription();
            if (rec.StatusSize() && rec.GetStatus(0).GetErrorDescription())
                error = rec.GetStatus(0).GetErrorDescription();

            BLOG_ERROR(LogPrefix << "cannot create pool '" << Pool->Config.GetName() << "' ("
                        << Pool->Config.GetStoragePoolId() << "): " << error);
            Pool->Issue = error;
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvPoolFailed(Tenant, Pool, error), ctx);
            return;
        }

        // Check state is required to support rolling update.
        // Older versions of BSC might ignore scope ID field
        // and in this case pool cannot be considered as
        // created/updated. Check should be done only if
        // pool has scope id specified.
        if (Pool->Config.HasScopeId()) {
            Become(&TThis::StateCheck);
            ReadPoolState(ctx);
        } else {
            Pool->Issue.clear();
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvPoolAllocated(Tenant, Pool), ctx);
        }
    }

    void HandleCheck(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev, const TActorContext& ctx)
    {
        auto &rec = ev->Get()->Record.GetResponse();

        BLOG_D(LogPrefix << "got check response: " << rec.ShortDebugString());

        if (!CheckReadStatus(rec)) {
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvPoolFailed(Tenant, Pool, Pool->Issue), ctx);
            return;
        }

        if (rec.GetStatus(0).StoragePoolSize() == 0) {
            BLOG_ERROR(LogPrefix << "check response misses pool status");
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvPoolFailed(Tenant, Pool, Pool->Issue), ctx);
            return;
        }

        const auto &scope = rec.GetStatus(0).GetStoragePool(0).GetScopeId();
        if (TTenantsManager::TDomainId(scope.GetX1(), scope.GetX2()) != Tenant->DomainId) {
            BLOG_ERROR(LogPrefix << "scope id check failure "
                        << Tenant->DomainId
                        << " vs " << scope.ShortDebugString());
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvPoolFailed(Tenant, Pool, Pool->Issue), ctx);
            return;
        }

        Pool->Issue.clear();
        ReplyAndDie(new TTenantsManager::TEvPrivate::TEvPoolAllocated(Tenant, Pool), ctx);
    }

    void HandleDelete(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev, const TActorContext& ctx)
    {
        auto &rec = ev->Get()->Record.GetResponse();

        BLOG_D(LogPrefix << "got config response: " << rec.ShortDebugString());

        if (!rec.GetSuccess() || !rec.GetStatus(0).GetSuccess()) {
            TString error = rec.GetErrorDescription();
            if (rec.StatusSize() && rec.GetStatus(0).GetErrorDescription())
                error = rec.GetStatus(0).GetErrorDescription();

            BLOG_ERROR(LogPrefix << "cannot delete pool '"
                        << Pool->Config.GetName() << "' ("
                        << Pool->Config.GetStoragePoolId() << "): " << error);
            Pool->Issue = error;
            ctx.Send(OwnerId, new TTenantsManager::TEvPrivate::TEvPoolFailed(Tenant, Pool, error));
            Die(ctx);
            return;
        }

        Pool->Issue.clear();
        ctx.Send(OwnerId, new TTenantsManager::TEvPrivate::TEvPoolDeleted(Tenant, Pool));
        Die(ctx);
    }

    STFUNC(StateRead) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvControllerConfigResponse, HandleRead);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

    STFUNC(StateAllocate) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvControllerConfigResponse, HandleAllocate);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

    STFUNC(StateCheck) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvControllerConfigResponse, HandleCheck);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

    STFUNC(StateDelete) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvControllerConfigResponse, HandleDelete);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }
};

class TSubDomainManip : public TActorBootstrapped<TSubDomainManip> {
public:
    enum EAction {
        CREATE,
        CONFIGURE,
        CONFIGURE_ATTR,
        GET_KEY,
        REMOVE
    };

private:
    using TBase = TActorBootstrapped<TSubDomainManip>;

    TActorId OwnerId;
    TTenantsManager::TTenant::TPtr Tenant;
    TTenantsManager::TTenant::TPtr SharedTenant;
    EAction Action;
    // Pair of <WorkDir, DirToCreate>
    std::pair<TString, TString> Subdomain;
    ui64 TxId;
    ui64 TabletId;
    ui64 Version;
    TActorId Pipe;
    // For CREATE/GET_KEY action SchemeshardId and PathId will hold subdomain key.
    ui64 SchemeshardId;
    ui64 PathId;

    static THashMap<ui64, TString> IssuesMap;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_TENANTS_MANAGER;
    }

    TSubDomainManip(TActorId ownerId, TTenantsManager::TTenant::TPtr tenant, EAction action,
            TTenantsManager::TTenant::TPtr sharedTenant = nullptr)
        : OwnerId(ownerId)
        , Tenant(tenant)
        , SharedTenant(sharedTenant)
        , Action(action)
        , TxId(0)
        , TabletId(0)
        , Version(tenant->SubdomainVersion)
        , SchemeshardId(0)
        , PathId(0)
    {
        if (Action == CREATE) {
            TVector<TString> parts = SplitPath(Tenant->Path);
            TString workDir;
            TString pathToCreate;

            for (size_t i = 0; i < parts.size(); ++i) {
                if (i == 0) {
                    workDir = "/" + parts[i];
                } else {
                    if (pathToCreate) {
                        pathToCreate += "/";
                    }
                    pathToCreate += parts[i];
                }
            }

            Subdomain = std::make_pair(workDir, pathToCreate);
        } else {
            Subdomain = std::make_pair(ExtractParent(Tenant->Path), ExtractBase(Tenant->Path));
        }

        if (IssuesMap.empty()) {
            auto &opts = NKikimrClient::TResponse::descriptor()->FindFieldByNumber(5)->options().
                GetExtension(NKikimrClient::EnumValueHint);
            Y_ABORT_UNLESS(opts.HintsSize());
            for (auto &rec : opts.GetHints())
                IssuesMap[rec.GetValue()] = TStringBuilder() << rec.GetName() << ": " << rec.GetMan();
        }
    }

    void FillSubdomainCreationInfo(NKikimrSubDomains::TSubDomainSettings &subdomain)
    {
        subdomain.SetName(Subdomain.second);
        if (Tenant->IsExternalSubdomain) {
            subdomain.SetExternalSchemeShard(true);
            subdomain.SetGraphShard(true);
            if (Tenant->IsExternalHive) {
                subdomain.SetExternalHive(true);
            }
            if (Tenant->IsExternalSysViewProcessor) {
                subdomain.SetExternalSysViewProcessor(true);
            }
            if (Tenant->IsExternalStatisticsAggregator) {
                subdomain.SetExternalStatisticsAggregator(true);
            }
            if (Tenant->IsExternalBackupController) {
                subdomain.SetExternalBackupController(true);
            }
            if (Tenant->IsGraphShardEnabled) {
                subdomain.SetGraphShard(true);
            }
        }

        if (SharedTenant) {
            const auto &resourcesDomainId = SharedTenant->DomainId;
            auto &resourcesDomainKey = *subdomain.MutableResourcesDomainKey();
            resourcesDomainKey.SetSchemeShard(resourcesDomainId.OwnerId);
            resourcesDomainKey.SetPathId(resourcesDomainId.LocalPathId);
        }
    }

    void FillSubdomainAlterInfo(NKikimrSubDomains::TSubDomainSettings &subdomain,
                           bool tablets)
    {
        subdomain.SetName(Subdomain.second);
        if (Tenant->IsExternalSubdomain) {
            subdomain.SetExternalSchemeShard(true);
            if (Tenant->IsExternalHive) {
                subdomain.SetExternalHive(true);
            }
            if (Tenant->IsExternalSysViewProcessor) {
                subdomain.SetExternalSysViewProcessor(true);
            }
            if (Tenant->IsExternalStatisticsAggregator) {
                subdomain.SetExternalStatisticsAggregator(true);
            }
            if (Tenant->IsExternalBackupController) {
                subdomain.SetExternalBackupController(true);
            }
            if (Tenant->IsGraphShardEnabled) {
                subdomain.SetGraphShard(true);
            }
        }
        if (tablets) {
            subdomain.SetCoordinators(Tenant->Coordinators);
            subdomain.SetMediators(Tenant->Mediators);
            subdomain.SetPlanResolution(Tenant->PlanResolution);
            subdomain.SetTimeCastBucketsPerMediator(Tenant->TimeCastBucketsPerMediator);
        }

        for (auto &pr : (SharedTenant ? SharedTenant->StoragePools : Tenant->StoragePools)) {
            // N.B. only provide schemeshard with pools that have at least one allocated group
            if (pr.second->State != TTenantsManager::TStoragePool::NOT_ALLOCATED &&
                pr.second->AllocatedNumGroups > 0)
            {
                auto &pool = *subdomain.AddStoragePools();
                pool.SetName(pr.second->Config.GetName());
                pool.SetKind(pr.second->Kind);
            }
        }

        if (Tenant->SchemaOperationQuotas) {
            auto* dstQuotas = subdomain.MutableDeclaredSchemeQuotas();
            for (const auto& bucket : Tenant->SchemaOperationQuotas->leaky_bucket_quotas()) {
                auto* dst = dstQuotas->AddSchemeQuotas();
                dst->SetBucketSize(bucket.bucket_size());
                dst->SetBucketSeconds(bucket.bucket_seconds());
            }
        }

        if (Tenant->DatabaseQuotas) {
            subdomain.MutableDatabaseQuotas()->CopyFrom(*Tenant->DatabaseQuotas);
        }
    }

    void AlterUserAttribute(const TActorContext &ctx) {
        BLOG_D("TSubDomainManip(" << Tenant->Path << ") alter user attribute ");
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();

        request->Record.SetDatabaseName(TString(ExtractDomain(Subdomain.first)));
        request->Record.SetExecTimeoutPeriod(Max<ui64>());

        if (Tenant->UserToken.GetUserSID())
            request->Record.SetUserToken(Tenant->UserToken.SerializeAsString());

        auto &tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetWorkingDir(Subdomain.first);

        tx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes);

        tx.MutableAlterUserAttributes()->CopyFrom(Tenant->Attributes);
        tx.MutableAlterUserAttributes()->SetPathName(Subdomain.second);

        BLOG_TRACE("TSubdomainManip(" << Tenant->Path << ") send alter user attribute cmd: "
                    << request->ToString());

        ctx.Send(MakeTxProxyID(), request.Release());
    }

    void AlterSubdomain(const TActorContext &ctx)
    {
        BLOG_D("TSubDomainManip(" << Tenant->Path << ") alter subdomain version " << Version);

        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName(TString(ExtractDomain(Subdomain.first)));
        request->Record.SetExecTimeoutPeriod(Max<ui64>());

        if (Tenant->UserToken.GetUserSID())
            request->Record.SetUserToken(Tenant->UserToken.SerializeAsString());

        auto &tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetWorkingDir(Subdomain.first);

        FillSubdomainAlterInfo(*tx.MutableSubDomain(), true);

        if (Tenant->IsExternalSubdomain) {
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterExtSubDomain);
        } else {
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterSubDomain);
        }

        BLOG_TRACE("TSubdomainManip(" << Tenant->Path << ") send alter subdomain cmd: "
                    << request->ToString());

        ctx.Send(MakeTxProxyID(), request.Release());
    }

    void CreateSubdomain(const TActorContext &ctx)
    {
        BLOG_D("TSubDomainManip(" << Tenant->Path << ") create subdomain");

        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName(TString(ExtractDomain(Subdomain.first)));
        request->Record.SetExecTimeoutPeriod(Max<ui64>());

        if (Tenant->UserToken.GetUserSID())
            request->Record.SetUserToken(Tenant->UserToken.SerializeAsString());

        auto &tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetWorkingDir(Subdomain.first);

        FillSubdomainCreationInfo(*tx.MutableSubDomain());

        if (Tenant->IsExternalSubdomain) {
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExtSubDomain);
        } else {
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSubDomain);
        }

        if (Tenant->Attributes.UserAttributesSize())
            tx.MutableAlterUserAttributes()->CopyFrom(Tenant->Attributes);

        BLOG_TRACE("TSubdomainManip(" << Tenant->Path << ") send subdomain creation cmd: "
                    << request->ToString());

        ctx.Send(MakeTxProxyID(), request.Release());
    }

    void DropSubdomain(const TActorContext &ctx)
    {
        BLOG_D("TSubDomainManip(" << Tenant->Path << ") drop subdomain");

        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName(TString(ExtractDomain(Subdomain.first)));
        request->Record.SetExecTimeoutPeriod(Max<ui64>());
        if (Tenant->UserToken.GetUserSID())
            request->Record.SetUserToken(Tenant->UserToken.SerializeAsString());
        auto &tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        if (Tenant->IsExternalSubdomain) {
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain);
        } else {
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpForceDropSubDomain);
        }
        tx.SetWorkingDir(Subdomain.first);
        tx.MutableDrop()->SetName(Subdomain.second);

        BLOG_TRACE("TSubdomainManip(" << Tenant->Path << ") send subdomain drop cmd: "
                    << request->ToString());

        ctx.Send(MakeTxProxyID(), request.Release());
    }

    void ReadSubdomainKey(const TActorContext &ctx)
    {
        auto *domain = AppData(ctx)->DomainsInfo->GetDomainByName(ExtractDomain(Tenant->Path));
        if (!domain) {
            TString error = "cannot find domain info";
            BLOG_CRIT("TSubdomainManip(" << Tenant->Path << ") " << error);
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainFailed(Tenant, error), ctx);
            return;
        }

        TabletId = domain->SchemeRoot;
        RequestSubdomainKey(ctx);
    }

    void Die(const TActorContext &ctx) override
    {
        if (Pipe)
            NTabletPipe::CloseClient(ctx, Pipe);
        TBase::Die(ctx);
    }

    void ActionFinished(const TActorContext &ctx)
    {
        if (Action == CREATE)
            RequestSubdomainKey(ctx);
        else
            ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext &ctx)
    {
        BLOG_D("TSubdomainManip(" << Tenant->Path << ") done");
        if (Action == CREATE)
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainCreated(Tenant, SchemeshardId, PathId), ctx);
        else if (Action == CONFIGURE || Action == CONFIGURE_ATTR)
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainReady(Tenant, Version), ctx);
        else if (Action == GET_KEY)
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainKey(Tenant, SchemeshardId, PathId), ctx);
        else {
            Y_ABORT_UNLESS(Action == REMOVE);
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainRemoved(Tenant), ctx);
        }
    }

    void ReplyAndDie(IEventBase *resp,
                     const TActorContext &ctx)
    {
        BLOG_D("TSubdomainManip(" << Tenant->Path << ") reply with " << resp->ToString());
        ctx.Send(OwnerId, resp);
        Die(ctx);
    }

    void OnPipeDestroyed(const TActorContext &ctx)
    {
        if (Pipe) {
            NTabletPipe::CloseClient(ctx, Pipe);
            Pipe = TActorId();
        }

        SendNotifyRequest(ctx);
    }

    void OpenPipe(const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(TabletId);
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = FastConnectRetryPolicy();
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, TabletId, pipeConfig);
        Pipe = ctx.ExecutorThread.RegisterActor(pipe);
    }

    void SendNotifyRequest(const TActorContext &ctx)
    {
        if (!Pipe)
            OpenPipe(ctx);

        auto request = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(TxId);

        BLOG_TRACE("TSubdomainManip(" << Tenant->Path << ") send notification request: " << request->ToString());

        NTabletPipe::SendData(ctx, Pipe, request.Release());
    }

    void RequestSubdomainKey(const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(TabletId);
        if (!Pipe)
            OpenPipe(ctx);

        auto request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(Tenant->Path);
        NTabletPipe::SendData(ctx, Pipe, request.Release());
    }

    void Bootstrap(const TActorContext &ctx) {
        BLOG_D("TSubdomainManip(" << Tenant->Path << ")::Bootstrap");

        if (Action == CREATE) {
            Become(&TThis::StateSubdomain);
            CreateSubdomain(ctx);
        } else if (Action == CONFIGURE) {
            Become(&TThis::StateSubdomain);
            AlterSubdomain(ctx);
        } else if (Action == GET_KEY) {
            Become(&TThis::GetSubdomainKey);
            ReadSubdomainKey(ctx);
        } else {
            Y_ABORT_UNLESS(Action == REMOVE);
            Become(&TThis::StateSubdomain);
            ReadSubdomainKey(ctx);
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr &ev, const TActorContext&) {
        BLOG_D("TSubdomainManip(" << Tenant->Path << ") got TEvNotifyTxCompletionRegistered: "
                    << ev->Get()->Record.ShortDebugString());
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr &ev, const TActorContext& ctx) {
        BLOG_D("TSubdomainManip(" << Tenant->Path << ") got TEvNotifyTxCompletionResult: "
                    << ev->Get()->Record.ShortDebugString());

        if (Action == CONFIGURE && Tenant->Attributes.UserAttributesSize()) {
            AlterUserAttribute(ctx);
            Action = CONFIGURE_ATTR;
        } else {
            ActionFinished(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            OnPipeDestroyed(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& /*ev*/, const TActorContext& ctx) {
        OnPipeDestroyed(ctx);
    }

    void HandleSubdomain(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx)
    {
        BLOG_D("TSubdomainManip(" << Tenant->Path << ") got propose result: "
                    << ev->Get()->Record.ShortDebugString());

        auto &rec = ev->Get()->Record;
        switch (rec.GetStatus()) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
            TabletId = rec.GetSchemeShardTabletId();
            if (Action == CONFIGURE && Tenant->Attributes.UserAttributesSize()) {
                AlterUserAttribute(ctx);
                Action = CONFIGURE_ATTR;
            } else {
                ActionFinished(ctx);
            }
            break;
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress:
            TxId = rec.GetTxId();
            TabletId = rec.GetSchemeShardTabletId();
            SendNotifyRequest(ctx);
            break;
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied:
            {
                auto issue = BuildErrorMsg(rec);
                ctx.Send(OwnerId, new TTenantsManager::TEvPrivate::TEvSubdomainFailed
                         (Tenant, issue, Ydb::StatusIds::UNAUTHORIZED));
                break;
            }
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError:
            if (Action == REMOVE) {
                // Check if removal finished or in-progress.
                if (rec.GetSchemeShardStatus() == NKikimrScheme::StatusPathDoesNotExist
                    || rec.GetSchemeShardStatus() == NKikimrScheme::StatusMultipleModifications) {
                    BLOG_D("TSubdomainManip(" << Tenant->Path << ") consider dubdomain is removed");
                    ActionFinished(ctx);
                    break;
                }
            }
            [[fallthrough]];
        default:
            {
                auto issue = BuildErrorMsg(rec);
                ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainFailed(Tenant, issue), ctx);
            }
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr ev, const TActorContext &ctx)
    {
        const auto &rec = ev->Get()->GetRecord();

        BLOG_D("TSubdomainManip(" << Tenant->Path << ") got describe result: "
                    << rec.ShortDebugString());

        if (Action == REMOVE) {
            switch (rec.GetStatus()) {
            case NKikimrScheme::EStatus::StatusPathDoesNotExist:
                ActionFinished(ctx);
                break;
            case NKikimrScheme::EStatus::StatusSuccess:
                DropSubdomain(ctx);
                break;
            default:
                ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainFailed(Tenant, rec.GetReason()), ctx);
                break;
            }

            return;
        }

        if (rec.GetStatus() != NKikimrScheme::EStatus::StatusSuccess) {
            BLOG_ERROR("TSubdomainManip(" << Tenant->Path << ") "
                        << "Receive TEvDescribeSchemeResult with bad status "
                        << NKikimrScheme::EStatus_Name(rec.GetStatus()) <<
                        " reason is <" << rec.GetReason() << ">" <<
                        " while resolving subdomain " << Tenant->Path);

            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainFailed(Tenant, rec.GetReason()), ctx);
            return;
        }

        auto pathType = rec.GetPathDescription().GetSelf().GetPathType();
        auto expectedPathType = Tenant->IsExternalSubdomain ? NKikimrSchemeOp::EPathTypeExtSubDomain : NKikimrSchemeOp::EPathTypeSubDomain;
        if (pathType != expectedPathType) {
            BLOG_ERROR("TSubdomainManip(" << Tenant->Path << ") "
                        << "Resolve subdomain fail, tenant path "
                        << Tenant->Path << " has invalid path type "
                        << NKikimrSchemeOp::EPathType_Name(pathType)
                        << " but expected " << NKikimrSchemeOp::EPathType_Name(expectedPathType));
            ReplyAndDie(new TTenantsManager::TEvPrivate::TEvSubdomainFailed(Tenant, "bad path type"), ctx);
            return;
        }

        const auto &key = rec.GetPathDescription().GetDomainDescription().GetDomainKey();
        SchemeshardId = key.GetSchemeShard();
        PathId = key.GetPathId();

        ReplyAndDie(ctx);
    }

    TString BuildErrorMsg(const NKikimrTxUserProxy::TEvProposeTransactionStatus &rec) const
    {
        if (rec.GetSchemeShardReason())
            return rec.GetSchemeShardReason();
        if (IssuesMap.contains(rec.GetStatus()))
            return IssuesMap.at(rec.GetStatus());
        return TString();
    }

    STFUNC(StateSubdomain) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleSubdomain);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

    STFUNC(GetSubdomainKey) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }
};

THashMap<ui64, TString> TSubDomainManip::IssuesMap;

} // anonymous namespace

const std::array<TString, TTenantsManager::COUNTER_COUNT> TTenantsManager::TCounters::SensorNames = {{
        { "Tenants" },
        { "CreateDatabaseRequests" },
        { "AlterDatabaseRequests" },
        { "RemoveDatabaseRequests" },
        { "GetDatabaseStatusRequests" },
        { "ListDatabasesRequests" },
        { "DescribeDatabaseOptionsRequests" },
        { "GetOperationRequests" },
        { "AllocPoolFailed" },
        { "RemovePoolFailed" },
        { "ConfigureSubdomainFailed" },
        { "RemoveSubdomainFailed" },
        { "ComputationalUnitsQuotaExceeded" },
        { "ComputationalUnitsLoadQuotaExceeded" },
        { "TenantsQuotaExceeded" },
        { "RequestedStorageUnits" },
        { "AllocatedStorageUnits" },
        { "RegisteredUnits" },
        { "ComputationalUnits" },
        { "CreateDatabaseResponses" },
        { "AlterDatabaseResponses" },
        { "RemoveDatabaseResponses" },
        { "GetDatabaseStatusResponses" },
        { "GetOperationResponses" },
    }};

const THashSet<TTenantsManager::ECounter> TTenantsManager::TCounters::DerivSensors = {{
        COUNTER_CREATE_REQUESTS,
        COUNTER_ALTER_REQUESTS,
        COUNTER_REMOVE_REQUESTS,
        COUNTER_STATUS_REQUESTS,
        COUNTER_LIST_REQUESTS,
        COUNTER_DESCRIBE_REQUESTS,
        COUNTER_GET_OPERATION_REQUESTS,
        COUNTER_CREATE_RESPONSES,
        COUNTER_ALTER_RESPONSES,
        COUNTER_REMOVE_RESPONSES,
        COUNTER_STATUS_RESPONSES,
        COUNTER_GET_OPERATION_RESPONSES,
        COUNTER_ALLOC_POOL_FAILED,
        COUNTER_REMOVE_POOL_FAILED,
        COUNTER_CONFIGURE_SUBDOMAIN_FAILED,
        COUNTER_REMOVE_SUBDOMAIN_FAILED,
        COUNTER_COMPUTATIONAL_QUOTA_EXCEEDED,
        COUNTER_COMPUTATIONAL_LOAD_QUOTA_EXCEEDED,
        COUNTER_TENANTS_QUOTA_EXCEEDED,
    }};

void TTenantsManager::TCounters::AddUnits(const TUnitsCount &units)
{
    for (auto &pr : units) {
        Inc(pr.first.first, pr.first.second,
            COUNTER_COMPUTATIONAL_UNITS, pr.second);
    }
}

void TTenantsManager::TCounters::RemoveUnits(const TUnitsCount &units)
{
    for (auto &pr : units) {
        Dec(pr.first.first, pr.first.second,
            COUNTER_COMPUTATIONAL_UNITS, pr.second);
    }
}

void TTenantsManager::TSlotStats::Clear()
{
    SlotsByType.clear();
    Total = TSlotCounters();
}

void TTenantsManager::TSlotStats::AllocateSlots(const TSlotsCount &slots)
{
    for (auto &pr : slots) {
        auto &type = pr.first.SlotType;

        if (type)
            SlotsByType[type].Allocated += pr.second;

        Total.Allocated += pr.second;
    }
}

void TTenantsManager::TSlotStats::DeallocateSlots(const TSlotsCount &slots)
{
    for (auto &pr : slots) {
        auto &type = pr.first.SlotType;

        if (type)
            SlotsByType[type].Allocated -= pr.second;

        Total.Allocated -= pr.second;
    }
}

void TTenantsManager::TSlotStats::UpdateStats(const NKikimrTenantSlotBroker::TSlotStats &stats)
{
    for (auto &pr : SlotsByType) {
        pr.second.Connected = 0;
        pr.second.Free = 0;
    }
    Total.Connected = 0;
    Total.Free = 0;

    for (auto &counters : stats.GetSlotCounters()) {
        auto &type = counters.GetType();
        ui64 connected = counters.GetConnected();
        ui64 free = counters.GetFree();

        if (type) {
            SlotsByType[type].Connected += connected;
            SlotsByType[type].Free += free;
        }

        Total.Connected += connected;
        Total.Free += free;
    }
}

TTenantsManager::TTenantsConfig::TTenantsConfig()
    : DefaultStorageUnitsQuota(0)
    , DefaultComputationalUnitsQuota(0)
    , TotalComputationalUnitsQuota(0)
    , TotalComputationalUnitsLoadQuota(0)
    , TenantsQuota(0)
{
}

void TTenantsManager::TTenantsConfig::Clear()
{
    DefaultStorageUnitsQuota = 0;
    DefaultComputationalUnitsQuota = 0;
    AvailabilityZones.clear();
    TenantSlotKinds.clear();
    TotalComputationalUnitsQuota = 0;
    TotalComputationalUnitsLoadQuota = 0;
    TenantsQuota = 0;
}

void TTenantsManager::TTenantsConfig::Parse(const NKikimrConsole::TTenantsConfig &config)
{
    TString error;
    bool res = Parse(config, error);
    Y_ABORT_UNLESS(res);
}

bool TTenantsManager::TTenantsConfig::Parse(const NKikimrConsole::TTenantsConfig &config, TString &error)
{
    Clear();

    DefaultStorageUnitsQuota = config.GetDefaultStorageUnitsQuota();
    DefaultComputationalUnitsQuota = config.GetDefaultComputationalUnitsQuota();

    for (auto &kind : config.GetAvailabilityZoneKinds()) {
        if (AvailabilityZones.contains(kind.GetKind())) {
            error = Sprintf("double definition of zone kind '%s'", kind.GetKind().data());
            return false;
        }

        switch (kind.GetZoneCase()) {
        case NKikimrConsole::TAvailabilityZoneKind::kDataCenterName:
            {
                TAvailabilityZone zone(kind.GetKind(), kind.GetDataCenterName());
                AvailabilityZones[kind.GetKind()] = zone;
                break;
            }
        case NKikimrConsole::TAvailabilityZoneKind::kSlotLocation:
            {
                auto &loc = kind.GetSlotLocation();
                TAvailabilityZone zone(kind.GetKind(), loc.GetDataCenter(),
                                       loc.GetForceLocation(), loc.GetCollocationGroup(),
                                       loc.GetForceCollocation());
                AvailabilityZones[kind.GetKind()] = zone;
                break;
            }
        case NKikimrConsole::TAvailabilityZoneKind::ZONE_NOT_SET:
            {
                TAvailabilityZone zone(kind.GetKind(), ANY_DATA_CENTER);
                AvailabilityZones[kind.GetKind()] = zone;
                break;
            }
        default:
            error = Sprintf("unexpected zone case for zone '%s'", kind.GetKind().data());
            return false;
        }
    }

    THashMap<TString, TSet<TString>> zoneSets;
    for (auto &set : config.GetAvailabilityZoneSets()) {
        if (zoneSets.contains(set.GetName())) {
            error = Sprintf("double definition of zone set '%s'", set.GetName().data());
            return false;
        }

        for (auto &kind : set.GetZoneKinds()) {
            if (!AvailabilityZones.contains(kind)) {
                error = Sprintf("uknown zone kind '%s' in zone set '%s'", kind.data(), set.GetName().data());
                return false;
            }

            zoneSets[set.GetName()].insert(kind);
        }
    }

    for (auto &kind : config.GetComputationalUnitKinds()) {
        switch (kind.GetResourceCase()) {
        case NKikimrConsole::TComputationalUnitKind::kTenantSlotType:
            {
                if (TenantSlotKinds.contains(kind.GetKind())) {
                    error = Sprintf("double definition of computational unit kind '%s'", kind.GetKind().data());
                    return false;
                }

                auto &slotKind = TenantSlotKinds[kind.GetKind()];
                slotKind.Kind = kind.GetKind();
                slotKind.TenantSlotType = kind.GetTenantSlotType();

                if(!zoneSets.contains(kind.GetAvailabilityZoneSet())) {
                    error = Sprintf("unknown zone set '%s' is referred from computational unit kind '%s'",
                                    kind.GetAvailabilityZoneSet().data(), kind.GetKind().data());
                    return false;
                }

                slotKind.AllowedZones = zoneSets[kind.GetAvailabilityZoneSet()];
            }
            break;
        default:
            error = Sprintf("unexpected resource case for computational unit kind '%s'", kind.GetKind().data());
            return false;
        }
    }

    TotalComputationalUnitsQuota = config.GetClusterQuota().GetComputationalUnitsQuota();
    TotalComputationalUnitsLoadQuota = config.GetClusterQuota().GetComputationalUnitsLoadQuota();
    TenantsQuota = config.GetClusterQuota().GetTenantsQuota();

    return true;
}

TSlotDescription TTenantsManager::TTenantsConfig::GetSlotKey(const TString &kind,
                                                             const TString &zone) const
{
    auto zoneKind = zone ? AvailabilityZones.at(zone) : TAvailabilityZone();
    auto &slotKind = TenantSlotKinds.at(kind);
    return TSlotDescription(slotKind.TenantSlotType,
                            zoneKind.DataCenter,
                            zoneKind.ForceLocation,
                            zoneKind.CollocationGroup,
                            zoneKind.ForceCollocation);
}

TSlotDescription TTenantsManager::TTenantsConfig::GetSlotKey(const std::pair<TString, TString> &unit) const
{
    return GetSlotKey(unit.first, unit.second);
}

void TTenantsManager::TTenantsConfig::ParseComputationalUnits(const TUnitsCount &units,
                                                              TSlotsCount &slots) const
{
    for (auto &pr : units) {
        auto key = GetSlotKey(pr.first);
        slots[key] += pr.second;
    }
}

TTenantsManager::TTenant::TTenant(const TString &path,
                                  EState state,
                                  const TString &token)
    : Path(path)
    , State(state)
    , Coordinators(3)
    , Mediators(3)
    , PlanResolution(10)
    , TimeCastBucketsPerMediator(2)
    , SlotsAllocationConfirmed(false)
    , StorageUnitsQuota(0)
    , ComputationalUnitsQuota(0)
    , ErrorCode(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)
    , TxId(0)
    , UserToken(token)
    , SubdomainVersion(1)
    , ConfirmedSubdomain(0)
    , Generation(0)
    , IsExternalSubdomain(false)
    , IsExternalHive(false)
    , IsExternalSysViewProcessor(false)
    , IsExternalStatisticsAggregator(false)
    , IsExternalBackupController(false)
    , AreResourcesShared(false)
{
}

bool TTenantsManager::TTenant::IsConfiguringState(EState state)
{
    return state == CONFIGURING_SUBDOMAIN;
}

bool TTenantsManager::TTenant::IsCreatingState(EState state)
{
    return state == CREATING_POOLS || state == CREATING_SUBDOMAIN;
}

bool TTenantsManager::TTenant::IsRemovingState(EState state)
{
    return state == REMOVING_UNITS || state == REMOVING_POOLS || state == REMOVING_SUBDOMAIN;
}

bool TTenantsManager::TTenant::IsRunningState(EState state)
{
    return state == RUNNING;
}

bool TTenantsManager::TTenant::IsConfiguring() const
{
    return IsConfiguringState(State);
}

bool TTenantsManager::TTenant::IsCreating() const
{
    return IsCreatingState(State);
}

bool TTenantsManager::TTenant::IsRemoving() const
{
    return IsRemovingState(State);
}

bool TTenantsManager::TTenant::IsRunning() const
{
    return IsRunningState(State);
}

bool TTenantsManager::TTenant::HasPoolsToCreate() const
{
    for (auto &pr : StoragePools)
        if (pr.second->State != TStoragePool::ALLOCATED)
            return true;
    return false;
}

bool TTenantsManager::TTenant::HasPoolsToDelete() const
{
    for (auto &pr : StoragePools)
        if (pr.second->State != TStoragePool::DELETED)
            return true;
    return false;
}

bool TTenantsManager::TTenant::HasSubDomainKey() const
{
    return bool(DomainId);
}

TString TTenantsManager::TTenant::MakeStoragePoolName(const TString &poolTypeName)
{
    return Sprintf("%s:%s", Path.data(), poolTypeName.data());
}

bool TTenantsManager::TTenant::CheckComputationalUnitsQuota(const TUnitsCount &units,
                                                            Ydb::StatusIds::StatusCode &code,
                                                            TString &error)
{
    if (ComputationalUnitsQuota) {
        ui64 total = 0;
        for (auto &pr : units)
            total += pr.second;
        if (total > ComputationalUnitsQuota) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("Total count of computational units %" PRIu64 " exceeds quota %" PRIu64,
                            total, ComputationalUnitsQuota);
            return false;
        }
    }

    return true;
}

bool TTenantsManager::TTenant::CheckStorageUnitsQuota(Ydb::StatusIds::StatusCode &code, TString &error,
                                                      ui64 additionalUnits)
{
    if (StorageUnitsQuota) {
        ui64 total = additionalUnits;
        for (auto &pr : StoragePools)
            total += pr.second->Config.GetNumGroups();
        if (total > StorageUnitsQuota) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("Total count of storage units %" PRIu64 " exceeds quota %" PRIu64,
                            total, StorageUnitsQuota);
            return false;
        }
    }

    return true;
}

bool TTenantsManager::TTenant::CheckQuota(Ydb::StatusIds::StatusCode &code, TString &error)
{
    return (CheckStorageUnitsQuota(code, error)
            && CheckComputationalUnitsQuota(ComputationalUnits, code, error));
}

void TTenantsManager::TTenant::ParseComputationalUnits(const TTenantsConfig &config)
{
    Slots.clear();
    SlotsAllocationConfirmed = false;

    config.ParseComputationalUnits(ComputationalUnits, Slots);
}

void TTenantsManager::TTenant::RemoveComputationalUnits()
{
    ComputationalUnits.clear();
    RegisteredComputationalUnits.clear();

    Slots.clear();
    SlotsAllocationConfirmed = false;
}

void TTenantsManager::ClearState()
{
    Tenants.clear();
    TenantIdToName.clear();
    RemovedTenants.clear();
    SlotStats.Clear();
}

void TTenantsManager::Bootstrap(const TActorContext &ctx)
{
    BLOG_D("TTenantsManager::Bootstrap");
    Become(&TThis::StateWork);
    TxProcessor = Self.GetTxProcessor()->GetSubProcessor("tenants",
                                                         ctx,
                                                         false,
                                                         NKikimrServices::CMS_TENANTS);
}

void TTenantsManager::Detach()
{
    PassAway();
}

void TTenantsManager::SetConfig(const NKikimrConsole::TTenantsConfig &config)
{
    Config.Parse(config);
}

TTenantsManager::TTenant::TPtr TTenantsManager::FindZoneKindUsage(const TString &kind)
{
    for (auto &pr1 : Tenants) {
        for (auto &pr2 : pr1.second->ComputationalUnits) {
            if (pr2.first.second == kind)
                return pr1.second;
        }
    }
    return nullptr;
}

TTenantsManager::TTenant::TPtr TTenantsManager::FindComputationalUnitKindUsage(const TString &kind)
{
    for (auto &pr1 : Tenants) {
        for (auto &pr2 : pr1.second->ComputationalUnits) {
            if (pr2.first.first == kind)
                return pr1.second;
        }
    }
    return nullptr;
}

TTenantsManager::TTenant::TPtr TTenantsManager::FindComputationalUnitKindUsage(const TString &kind,
                                                                                const TString &zone)
{
    for (auto &pr : Tenants)
        if (pr.second->ComputationalUnits.contains(std::make_pair(kind, zone)))
            return pr.second;
    return nullptr;
}

TTenantsManager::TTenant::TPtr TTenantsManager::GetTenant(const TString &name)
{
    auto it = Tenants.find(name);
    if (it != Tenants.end())
        return it->second;
    return nullptr;
}

TTenantsManager::TTenant::TPtr TTenantsManager::GetTenant(const TDomainId &domainId)
{
    auto it = TenantIdToName.find(domainId);
    if (it != TenantIdToName.end())
        return GetTenant(it->second);
    return nullptr;
}

void TTenantsManager::AddTenant(TTenant::TPtr tenant)
{
    Y_ABORT_UNLESS(!Tenants.contains(tenant->Path));
    Tenants[tenant->Path] = tenant;
    if (tenant->DomainId) {
        Y_ABORT_UNLESS(!TenantIdToName.contains(tenant->DomainId));
        TenantIdToName[tenant->DomainId] = tenant->Path;
    }
    SlotStats.AllocateSlots(tenant->Slots);

    Counters.Set(COUNTER_TENANTS, Tenants.size());
    Counters.AddUnits(tenant->ComputationalUnits);
    for (auto &pr : tenant->RegisteredComputationalUnits)
        Counters.Inc(pr.second.Kind, COUNTER_REGISTERED_UNITS);
    for (auto &pr : tenant->StoragePools) {
        Counters.Inc(pr.second->Kind, COUNTER_REQUESTED_STORAGE_UNITS, pr.second->GetGroups());
        Counters.Inc(pr.second->Kind, COUNTER_ALLOCATED_STORAGE_UNITS, pr.second->AllocatedNumGroups);
    }
}

void TTenantsManager::RemoveTenant(TTenant::TPtr tenant)
{
    Tenants.erase(tenant->Path);
    TenantIdToName.erase(tenant->DomainId);
    SlotStats.DeallocateSlots(tenant->Slots);

    if (tenant->SharedDomainId) {
        auto sharedTenant = GetTenant(tenant->SharedDomainId);
        Y_ABORT_UNLESS(sharedTenant);
        sharedTenant->HostedTenants.erase(tenant);
    }

    Counters.Set(COUNTER_TENANTS, Tenants.size());
    Counters.RemoveUnits(tenant->ComputationalUnits);
    for (auto &pr : tenant->RegisteredComputationalUnits)
        Counters.Dec(pr.second.Kind, COUNTER_REGISTERED_UNITS);
    for (auto &pr : tenant->StoragePools) {
        Counters.Dec(pr.second->Kind, COUNTER_REQUESTED_STORAGE_UNITS, pr.second->GetGroups());
        Counters.Dec(pr.second->Kind, COUNTER_ALLOCATED_STORAGE_UNITS, pr.second->AllocatedNumGroups);
    }

    TRemovedTenant removed;
    removed.Path = tenant->Path;
    removed.TxId = tenant->TxId;
    removed.ErrorCode = tenant->ErrorCode;
    removed.CreateIdempotencyKey = tenant->CreateIdempotencyKey;
    removed.Issue = tenant->Issue;
    removed.Code = Ydb::StatusIds::SUCCESS;
    RemovedTenants[tenant->Path] = removed;
}

void TTenantsManager::RemoveTenantFailed(TTenant::TPtr tenant,
                                         Ydb::StatusIds::StatusCode code)
{
    TRemovedTenant removed;
    removed.Path = tenant->Path;
    removed.ErrorCode = tenant->ErrorCode;
    removed.CreateIdempotencyKey = tenant->CreateIdempotencyKey;
    removed.TxId = tenant->TxId;
    removed.Issue = tenant->Issue;
    removed.Code = code;
    RemovedTenants[tenant->Path] = removed;

    tenant->TxId = 0;
}

void TTenantsManager::ChangeTenantState(TTenant::TPtr tenant,
                                        TTenant::EState state,
                                        const TActorContext &ctx)
{
    if (TTenant::IsCreatingState(tenant->State)
        && !TTenant::IsCreatingState(state)) {
        auto code = TTenant::IsRemovingState(state)
            ? (tenant->ErrorCode ? tenant->ErrorCode : Ydb::StatusIds::GENERIC_ERROR)
            : Ydb::StatusIds::SUCCESS;
        SendTenantNotifications(tenant, TTenant::CREATE, code, ctx);
    }

    tenant->State = state;
}

bool TTenantsManager::CheckTenantSlots(TTenant::TPtr tenant, const NKikimrTenantSlotBroker::TTenantState &state)
{
    for (auto &slot : state.GetRequiredSlots()) {
        TSlotDescription key(slot);
        auto count = slot.GetCount();
        if (!tenant->Slots.contains(key) || tenant->Slots.at(key) != count)
            return false;
    }

    return state.RequiredSlotsSize() == tenant->Slots.size();
}

bool TTenantsManager::MakeBasicPoolCheck(const TString &kind, ui64 size, Ydb::StatusIds::StatusCode &code, TString &error)
{
    if (!size) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "Zero count for storage units is not allowed";
        return false;
    }

    if (!Domain->StoragePoolTypes.contains(kind)) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = Sprintf("Unsupported storage unit kind '%s'.", kind.data());
        return false;
    }

    return true;
}

bool TTenantsManager::MakeBasicComputationalUnitCheck(const TString &kind, const TString &zone,
                                                      Ydb::StatusIds::StatusCode &code,
                                                      TString &error)
{
    if (!Config.TenantSlotKinds.contains(kind)) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = Sprintf("Unknown computational unit kind '%s'", kind.data());
        return false;
    }

    if (zone) {
        if (!Config.AvailabilityZones.contains(zone)) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("Unknown availability zone '%s'", zone.data());
            return false;
        }

        auto &slotKind = Config.TenantSlotKinds.at(kind);
        if (!slotKind.AllowedZones.contains(zone)) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("Zone '%s' is unavailable for units of kind '%s'", zone.data(), kind.data());
            return false;
        }
    }

    return true;
}

bool TTenantsManager::CheckComputationalUnitsQuota(const TUnitsCount &units,
                                                   TTenant::TPtr tenant,
                                                   Ydb::StatusIds::StatusCode &code,
                                                   TString &error)
{
    if (!Config.TotalComputationalUnitsQuota
        && !Config.TotalComputationalUnitsLoadQuota)
        return true;

    TSlotStats stats = SlotStats;
    if (tenant)
        stats.DeallocateSlots(tenant->Slots);

    TSlotsCount slots;
    Config.ParseComputationalUnits(units, slots);
    stats.AllocateSlots(slots);

    if (Config.TotalComputationalUnitsQuota
        && Config.TotalComputationalUnitsQuota < stats.Total.Allocated) {
        BLOG_NOTICE("Cluster computational units quota is exceeded ("
                     << stats.Total.Allocated << "/"
                     << Config.TotalComputationalUnitsQuota << ")");
        Counters.Inc(COUNTER_COMPUTATIONAL_QUOTA_EXCEEDED);
        code = Ydb::StatusIds::UNAVAILABLE;
        error = "Cluster computational units quota is exceeded";
        return false;
    }

    if (Config.TotalComputationalUnitsLoadQuota) {
        for (auto &pr : stats.SlotsByType) {
            ui64 cur = pr.second.Allocated * 100;
            ui64 quota = pr.second.Connected * Config.TotalComputationalUnitsLoadQuota;
            if (cur > quota) {
                BLOG_NOTICE("Cluster computational units load quota ("
                             << Config.TotalComputationalUnitsLoadQuota
                             << "%) is exceeded for slots '"
                             << pr.first << "' (" << pr.second.Allocated
                             << "/" << pr.second.Connected << ")");
                Counters.Inc(COUNTER_COMPUTATIONAL_LOAD_QUOTA_EXCEEDED);
                code = Ydb::StatusIds::UNAVAILABLE;
                error = "Cluster computational units load quota is exceeded";
                return false;
            }
        }

        ui64 cur = stats.Total.Allocated * 100;
        ui64 quota = stats.Total.Connected * Config.TotalComputationalUnitsLoadQuota;
        if (cur > quota) {
            BLOG_NOTICE("Cluster computational units load quota ("
                         << Config.TotalComputationalUnitsLoadQuota
                         << "%) is exceeded (" << stats.Total.Allocated
                         << "/" << stats.Total.Connected << ")");
            Counters.Inc(COUNTER_COMPUTATIONAL_LOAD_QUOTA_EXCEEDED);
            code = Ydb::StatusIds::UNAVAILABLE;
            error = "Cluster computational units load quota is exceeded";
            return false;
        }
    }

    return true;
}

bool TTenantsManager::CheckComputationalUnitsQuota(const TUnitsCount &units,
                                                   Ydb::StatusIds::StatusCode &code,
                                                   TString &error)
{
    return CheckComputationalUnitsQuota(units, nullptr, code, error);
}

bool TTenantsManager::CheckTenantsConfig(const NKikimrConsole::TTenantsConfig &config,
                                         Ydb::StatusIds::StatusCode &code,
                                         TString &error)
{
    TTenantsConfig newConfig;

    // Check config is consistent.
    if (!newConfig.Parse(config, error)) {
        code = Ydb::StatusIds::BAD_REQUEST;
        return false;
    }

    // Check used availability zones are not modified.
    for (auto &pr : Config.AvailabilityZones) {
        auto it = newConfig.AvailabilityZones.find(pr.first);
        if (it == newConfig.AvailabilityZones.end() || it->second != pr.second) {
            auto tenant = FindZoneKindUsage(pr.first);
            if (tenant) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("cannot remove or modify availability zone '%s' used by tenant '%s'",
                                pr.first.data(), tenant->Path.data());
                return false;
            }
        }
    }

    // Check used computational unit kinds are not modified.
    for (auto &pr : Config.TenantSlotKinds) {
        auto it = newConfig.TenantSlotKinds.find(pr.first);

        // Check if unit kind can be removed.
        if (it == newConfig.TenantSlotKinds.end()) {
            auto tenant = FindComputationalUnitKindUsage(pr.first);
            if (tenant) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("cannot remove computational unit kind '%s' used by tenant '%s'",
                                pr.first.data(), tenant->Path.data());
                return false;
            }
            continue;
        }

        // Check if tenant slot type modification for unit is OK.
        if (it->second.TenantSlotType != pr.second.TenantSlotType) {
            auto tenant = FindComputationalUnitKindUsage(pr.first);
            if (tenant) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("cannot modify computational unit kind '%s' used by tenant '%s'",
                                pr.first.data(), tenant->Path.data());
                return false;
            }
        }

        // Check is some used zone within the unit kind is missing now.
        if (it->second.AllowedZones != pr.second.AllowedZones) {
            for (auto &zone : pr.second.AllowedZones) {
                if (!it->second.AllowedZones.contains(zone)) {
                    auto tenant = FindComputationalUnitKindUsage(pr.first, zone);
                    if (tenant) {
                        code = Ydb::StatusIds::BAD_REQUEST;
                        error = Sprintf("cannot remove allowed availability zone '%s' from computational"
                                        " unit kind '%s' used by tenant '%s'",
                                        zone.data(), pr.first.data(), tenant->Path.data());
                        return false;
                    }
                }
            }
        }
    }

    return true;
}

bool TTenantsManager::CheckAccess(const TString &token,
                                  Ydb::StatusIds::StatusCode &code,
                                  TString &error,
                                  const TActorContext &ctx)
{
    auto *appData = AppData(ctx);
    if (appData->AdministrationAllowedSIDs.empty())
        return true;

    if (token) {
        NACLib::TUserToken userToken(token);
        for (auto &sid : appData->AdministrationAllowedSIDs)
            if (userToken.IsExist(sid))
                return true;
    }

    code = Ydb::StatusIds::UNAUTHORIZED;
    error = "You don't have permission for this operation."
        " Contact service admin for database administration operations.";

    return false;
}

Ydb::TOperationId TTenantsManager::MakeOperationId(const TString &path, ui64 txId, TTenant::EAction action)
{
    Ydb::TOperationId id;
    id.SetKind(Ydb::TOperationId::CMS_REQUEST);
    AddOptionalValue(id, "tenant", path);
    AddOptionalValue(id, "cmstid", ToString(Self.TabletID()));
    AddOptionalValue(id, "txid", ToString(txId));
    AddOptionalValue(id, "action", ToString((ui32)action));
    return id;
}

Ydb::TOperationId TTenantsManager::MakeOperationId(TTenant::TPtr tenant, TTenant::EAction action)
{
    Ydb::TOperationId id;
    id.SetKind(Ydb::TOperationId::CMS_REQUEST);
    AddOptionalValue(id, "tenant", tenant->Path);
    AddOptionalValue(id, "cmstid", ToString(Self.TabletID()));
    AddOptionalValue(id, "txid", ToString(tenant->TxId));
    AddOptionalValue(id, "action", ToString((ui32)action));
    return id;
}

TTenantsManager::TStoragePool::TPtr TTenantsManager::MakeStoragePool(TTenant::TPtr tenant, const TString &kind, ui64 size)
{
    auto poolName = tenant->MakeStoragePoolName(kind);
    auto &config = Domain->StoragePoolTypes.at(kind);

    TStoragePool::TPtr pool = new TStoragePool(poolName, size, kind, config, false);
    if (tenant->HasSubDomainKey())
        pool->SetScopeId(tenant->DomainId);

    return pool;
}

void TTenantsManager::OpenTenantSlotBrokerPipe(const TActorContext &ctx)
{
    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = FastConnectRetryPolicy();
    auto aid = MakeTenantSlotBrokerID();
    auto pipe = NTabletPipe::CreateClient(ctx.SelfID, aid, pipeConfig);
    TenantSlotBrokerPipe = ctx.ExecutorThread.RegisterActor(pipe);
}

void TTenantsManager::OnTenantSlotBrokerPipeDestroyed(const TActorContext &ctx)
{
    if (TenantSlotBrokerPipe) {
        NTabletPipe::CloseClient(ctx, TenantSlotBrokerPipe);
        TenantSlotBrokerPipe = TActorId();
    }

    RetryResourcesRequests(ctx);
    if (DelayedTxs)
        RequestTenantSlotsStats(ctx);
}

void TTenantsManager::AllocateTenantPools(TTenant::TPtr tenant, const TActorContext &ctx)
{
    if (!tenant->HasPoolsToCreate()) {
        if (tenant->State == TTenant::CREATING_POOLS)
            TxProcessor->ProcessTx(CreateTxUpdateTenantState(tenant->Path, TTenant::CREATING_SUBDOMAIN), ctx);
        return;
    }

    // If legacy tenant doesn't have domain key yet then postpone
    // pools update until subdomain key is updated.
    if (tenant->State != TTenant::CREATING_POOLS && !tenant->HasSubDomainKey())
        return;

    for (auto &pr : tenant->StoragePools) {
        if (pr.second->State != TStoragePool::ALLOCATED && !pr.second->Worker)
            pr.second->Worker = ctx.RegisterWithSameMailbox(new TPoolManip(SelfId(), Domain, tenant, pr.second, TPoolManip::ALLOCATE));
    }
}

void TTenantsManager::DeleteTenantPools(TTenant::TPtr tenant, const TActorContext &ctx)
{
    if (!tenant->HasPoolsToDelete()) {
        TxProcessor->ProcessTx(CreateTxRemoveTenantDone(tenant), ctx);
        return;
    }

    for (auto &pr : tenant->StoragePools) {
        if (pr.second->State == TStoragePool::DELETED) {
            continue;
        }

        if (pr.second->Borrowed) {
            BLOG_D("Mark borrowed pool as deleted"
                << ": tenant# " << tenant->Path
                << ", pool# " << pr.second->Config.GetName());

            pr.second->Worker = SelfId();
            ctx.Send(SelfId(), new TTenantsManager::TEvPrivate::TEvPoolDeleted(tenant, pr.second));
        } else {
            pr.second->Worker = ctx.RegisterWithSameMailbox(new TPoolManip(SelfId(), Domain, tenant, pr.second, TPoolManip::DEALLOCATE));
        }
    }
}

void TTenantsManager::RequestTenantResources(TTenant::TPtr tenant, const TActorContext &ctx)
{
    if (!TenantSlotBrokerPipe)
        OpenTenantSlotBrokerPipe(ctx);

    auto request = MakeHolder<TEvTenantSlotBroker::TEvAlterTenant>();
    request->Record.SetTenantName(tenant->Path);
    for (auto &pr : tenant->Slots) {
        auto &slot = *request->Record.AddRequiredSlots();
        pr.first.Serialize(slot);
        slot.SetCount(pr.second);
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Send TEvTenantSlotBroker::TEvAlterTenant: "
                << request->Record.ShortDebugString());

    NTabletPipe::SendData(ctx, TenantSlotBrokerPipe, request.Release());
}

void TTenantsManager::RequestTenantSlotsState(TTenant::TPtr tenant, const TActorContext &ctx)
{
    Y_ABORT_UNLESS(tenant->IsRunning() || tenant->IsConfiguring());
    if (!TenantSlotBrokerPipe)
        OpenTenantSlotBrokerPipe(ctx);

    auto request = MakeHolder<TEvTenantSlotBroker::TEvGetTenantState>();
    request->Record.SetTenantName(tenant->Path);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Send TEvTenantSlotBroker::TEvGetTenantState: "
                << request->Record.ShortDebugString());

    NTabletPipe::SendData(ctx, TenantSlotBrokerPipe, request.Release());
}

void TTenantsManager::RequestTenantSlotsStats(const TActorContext &ctx)
{
    if (!TenantSlotBrokerPipe)
        OpenTenantSlotBrokerPipe(ctx);

    auto request = MakeHolder<TEvTenantSlotBroker::TEvGetSlotStats>();

    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Send TEvTenantSlotBroker::TEvGetSlotStats: "
                << request->Record.ShortDebugString());

    NTabletPipe::SendData(ctx, TenantSlotBrokerPipe, request.Release());
}

void TTenantsManager::RetryResourcesRequests(const TActorContext &ctx)
{
    for (auto &pr : Tenants) {
        auto tenant = pr.second;
        if (tenant->IsCreating())
            continue;

        if (!tenant->SlotsAllocationConfirmed)
            RequestTenantResources(tenant, ctx);
        else if (!tenant->StatusRequests.empty())
            RequestTenantSlotsState(tenant, ctx);
    }
}

void TTenantsManager::FillTenantStatus(TTenant::TPtr tenant, Ydb::Cms::GetDatabaseStatusResult &status)
{
    status.set_path(tenant->Path);
    if (tenant->IsRunning() && tenant->SubdomainVersion != tenant->ConfirmedSubdomain)
        status.set_state(Ydb::Cms::GetDatabaseStatusResult::CONFIGURING);
    else if (tenant->IsRunning())
        status.set_state(Ydb::Cms::GetDatabaseStatusResult::RUNNING);
    else if (tenant->IsConfiguring())
        status.set_state(Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES);
    else if (tenant->IsCreating())
        status.set_state(Ydb::Cms::GetDatabaseStatusResult::CREATING);
    else if (tenant->IsRemoving())
        status.set_state(Ydb::Cms::GetDatabaseStatusResult::REMOVING);
    else
        status.set_state(Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED);

    auto resources = tenant->AreResourcesShared ?
        status.mutable_required_shared_resources() :
        status.mutable_required_resources();

    for (auto &pr : tenant->StoragePools) {
        auto &pool = *resources->add_storage_units();
        pool.set_unit_kind(pr.second->Kind);
        pool.set_count(pr.second->Config.GetNumGroups());
        if (pr.second->AllocatedNumGroups) {
            auto &allocatedPool = *status.mutable_allocated_resources()->add_storage_units();
            allocatedPool.set_unit_kind(pr.second->Kind);
            allocatedPool.set_count(pr.second->AllocatedNumGroups);
        }
    }

    for (auto &pr : tenant->ComputationalUnits) {
        auto &unit = *resources->add_computational_units();
        unit.set_unit_kind(pr.first.first);
        unit.set_availability_zone(pr.first.second);
        unit.set_count(pr.second);
    }

    if (tenant->SharedDomainId) {
        auto sharedTenant = GetTenant(tenant->SharedDomainId);
        Y_ABORT_UNLESS(sharedTenant);
        status.mutable_serverless_resources()->set_shared_database_path(sharedTenant->Path);
    }

    for (auto &pr : tenant->RegisteredComputationalUnits) {
        auto &unit = *status.add_registered_resources();
        unit.set_host(pr.second.Host);
        unit.set_port(pr.second.Port);
        unit.set_unit_kind(pr.second.Kind);
    }

    status.set_generation(tenant->Generation);

    if (tenant->SchemaOperationQuotas) {
        status.mutable_schema_operation_quotas()->CopyFrom(*tenant->SchemaOperationQuotas);
    }

    if (tenant->DatabaseQuotas) {
        status.mutable_database_quotas()->CopyFrom(*tenant->DatabaseQuotas);
    }
}

void TTenantsManager::FillTenantAllocatedSlots(TTenant::TPtr tenant, Ydb::Cms::GetDatabaseStatusResult &status,
                                               const NKikimrTenantSlotBroker::TTenantState &slots)
{
    THashMap<TSlotDescription, ui64> allocated;
    for (auto &slot : slots.GetRequiredSlots()) {
        TSlotDescription key(slot);
        allocated[key] = slot.GetCount();
    }
    for (auto &slot : slots.GetPendingSlots()) {
        TSlotDescription key(slot);
        allocated[key] -= slot.GetCount();
    }
    for (auto &slot : slots.GetMissingSlots()) {
        TSlotDescription key(slot);
        allocated[key] -= slot.GetCount();
    }

    for (auto &pr : tenant->ComputationalUnits) {
        auto &kind = pr.first.first;
        auto &zone = pr.first.second;
        auto count = pr.second;
        auto key = Config.GetSlotKey(kind, zone);

        count = Min(count, allocated[key]);
        allocated[key] -= count;

        if (count) {
            auto &unit = *status.mutable_allocated_resources()->add_computational_units();
            unit.set_unit_kind(kind);
            unit.set_availability_zone(zone);
            unit.set_count(count);
        }
    }
}

void TTenantsManager::CheckSubDomainKey(TTenant::TPtr tenant,
                                        const TActorContext &ctx)
{
    Y_ABORT_UNLESS(tenant->IsRunning() || tenant->IsConfiguring());
    if (tenant->HasSubDomainKey() || tenant->Worker)
        return;

    auto *actor = new TSubDomainManip(SelfId(), tenant, TSubDomainManip::GET_KEY);
    tenant->Worker = ctx.RegisterWithSameMailbox(actor);
}

void TTenantsManager::ConfigureTenantSubDomain(TTenant::TPtr tenant, const TActorContext &ctx)
{
    Y_ABORT_UNLESS(tenant->IsRunning() || tenant->IsConfiguring());
    if (tenant->SubdomainVersion != tenant->ConfirmedSubdomain
        && !tenant->Worker) {

        auto *actor = new TSubDomainManip(SelfId(), tenant, TSubDomainManip::CONFIGURE, GetTenant(tenant->SharedDomainId));
        tenant->Worker = ctx.RegisterWithSameMailbox(actor);
    }
}

void TTenantsManager::CreateTenantSubDomain(TTenant::TPtr tenant, const TActorContext &ctx)
{
    Y_ABORT_UNLESS(tenant->State == TTenant::CREATING_SUBDOMAIN);
    Y_ABORT_UNLESS(!tenant->Worker);
    auto *actor = new TSubDomainManip(SelfId(), tenant, TSubDomainManip::CREATE, GetTenant(tenant->SharedDomainId));
    tenant->Worker = ctx.RegisterWithSameMailbox(actor);
}

void TTenantsManager::DeleteTenantSubDomain(TTenant::TPtr tenant, const TActorContext &ctx)
{
    Y_ABORT_UNLESS(tenant->State == TTenant::REMOVING_SUBDOMAIN);
    if (!tenant->Worker) {
        auto *actor = new TSubDomainManip(SelfId(), tenant, TSubDomainManip::REMOVE);
        tenant->Worker = ctx.RegisterWithSameMailbox(actor);
    }
}

void TTenantsManager::ProcessTenantActions(TTenant::TPtr tenant, const TActorContext &ctx)
{
    if (tenant->State == TTenant::CREATING_POOLS) {
        AllocateTenantPools(tenant, ctx);
    } else if (tenant->State == TTenant::CREATING_SUBDOMAIN) {
        CreateTenantSubDomain(tenant, ctx);
    } else if (tenant->State == TTenant::CONFIGURING_SUBDOMAIN
               || tenant->State == TTenant::RUNNING) {
        // Tenant created using older CMS version has no
        // subdomain key fields filled. Check if update is
        // required.
        CheckSubDomainKey(tenant, ctx);
        // Process altered storage pools.
        AllocateTenantPools(tenant, ctx);
        // Deliver new pools to subdomain configuration.
        ConfigureTenantSubDomain(tenant, ctx);
        // Process slots allocation.
        if (!tenant->SlotsAllocationConfirmed)
            RequestTenantResources(tenant, ctx);
    } else if (tenant->State == TTenant::REMOVING_UNITS) {
        RequestTenantResources(tenant, ctx);
    } else if (tenant->State == TTenant::REMOVING_SUBDOMAIN) {
        DeleteTenantSubDomain(tenant, ctx);
    } else if (tenant->State == TTenant::REMOVING_POOLS) {
        DeleteTenantPools(tenant, ctx);
    } else {
        Y_ABORT("unexpected tenant state %u", (ui32)tenant->State);
    }
}

TTenantsManager::TTenant::TPtr TTenantsManager::FillOperationStatus(const TString &id,
                                                                    Ydb::Operations::Operation &operation)
{
    operation.set_id(id);

    TString path;
    ui64 action = 0;
    ui64 txId = 0;
    TTenant::TPtr tenant = nullptr;
    try {
        TOperationId opId(id);
        const auto& tenants = opId.GetValue("tenant");
        const auto& txIds = opId.GetValue("txid");
        const auto& actions = opId.GetValue("action");
        if (tenants.size() != 1 || txIds.size() != 1 || actions.size() != 1) {
            operation.set_ready(true);
            operation.set_status(Ydb::StatusIds::NOT_FOUND);
        }
        path = *tenants[0];
        if (!TryFromString(*actions[0], action)
            || !TryFromString(*txIds[0], txId)) {
            operation.set_ready(true);
            operation.set_status(Ydb::StatusIds::NOT_FOUND);
        }
    } catch (const yexception& ex) {
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::NOT_FOUND);
    }

    if (action == TTenant::CREATE) {
        tenant = GetTenant(path);
        if (tenant && tenant->TxId == txId) {
            if (tenant->IsConfiguring() || tenant->IsRunning()) {
                operation.set_ready(true);
                operation.set_status(Ydb::StatusIds::SUCCESS);
            } else if (tenant->IsRemoving()) {
                operation.set_ready(true);
                operation.set_status(tenant->ErrorCode
                                     ? tenant->ErrorCode
                                     : Ydb::StatusIds::GENERIC_ERROR);
                auto issue = operation.add_issues();
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(tenant->Issue);
            } else {
                Y_ABORT_UNLESS(tenant->IsCreating() || tenant->IsConfiguring());
                operation.set_ready(false);
            }
        } else if (RemovedTenants.contains(path)
                   && RemovedTenants.at(path).TxId == txId) {
            auto &removed = RemovedTenants.at(path);
            operation.set_ready(true);
            operation.set_status(removed.ErrorCode
                                 ? removed.ErrorCode
                                 : Ydb::StatusIds::GENERIC_ERROR);
            auto issue = operation.add_issues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(removed.Issue);
        } else {
            operation.set_ready(true);
            operation.set_status(Ydb::StatusIds::NOT_FOUND);
        }
    } else if (action == TTenant::REMOVE) {
        tenant = GetTenant(path);
        if (tenant && tenant->TxId == txId) {
            operation.set_ready(false);
        } else if (RemovedTenants.contains(path)
                   && RemovedTenants.at(path).TxId == txId) {
            operation.set_ready(true);
            operation.set_status(RemovedTenants.at(path).Code);
        } else {
            operation.set_ready(true);
            operation.set_status(Ydb::StatusIds::NOT_FOUND);
        }
    } else {
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::NOT_FOUND);
    }

    return tenant;
}

void TTenantsManager::SendTenantNotifications(TTenant::TPtr tenant,
                                              TTenant::EAction action,
                                              Ydb::StatusIds::StatusCode code,
                                              const TActorContext &ctx)
{
    for (auto &subscriber : tenant->Subscribers) {
        auto notification = MakeHolder<TEvConsole::TEvOperationCompletionNotification>();
        auto &operation = *notification->Record.MutableResponse()->mutable_operation();
        Ydb::TOperationId id = MakeOperationId(tenant, action);
        operation.set_id(ProtoToString(id));
        operation.set_ready(true);
        operation.set_status(code);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Send " << tenant->Path << " notification to "
                    << subscriber << ": " << notification->Record.ShortDebugString());

        ctx.Send(subscriber, notification.Release());

        if (action == TTenant::CREATE)
            Counters.Inc(code, COUNTER_CREATE_RESPONSES);
        else if (action == TTenant::REMOVE)
            Counters.Inc(code, COUNTER_REMOVE_RESPONSES);
        else
            Y_ABORT("unexpected action value (%" PRIu32 ")", static_cast<ui32>(action));
    }
    tenant->Subscribers.clear();
}

TString DomainIdToStringSafe(const TTenantsManager::TDomainId& value) {
    // <invalid> cannot be printed inside <pre> tag
    if (!value) {
        return "[Invalid]";
    }

    return value.ToString();
}

void TTenantsManager::DumpStateHTML(IOutputStream &os)
{
    HTML(os) {
        PRE() {
            os << "Used config: " << Endl
               << Self.GetConfig().GetTenantsConfig().DebugString() << Endl
               << "Storage pool types:" << Endl;
            for (auto &pr : Domain->StoragePoolTypes)
                os << "   " << pr.first << ": " << pr.second.ShortDebugString() << Endl;
            os << Endl;

            os << "SlotStats:" << Endl;
            for (auto &pr : SlotStats.SlotsByType)
                os << "  " << pr.first << ": " << pr.second.Allocated << "/" << pr.second.Connected
                   << "(free " << pr.second.Free << ")" << Endl;
            os << "  Total: " << SlotStats.Total.Allocated << "/" << SlotStats.Total.Connected
                   << "(free " << SlotStats.Total.Free << ")" << Endl;

            if (!DelayedTxs.empty())
                os << DelayedTxs.size() << " transaction(s) are waiting for updated slot stats" << Endl;
            os << Endl;

            os << "Tenants:" << Endl;
            for (auto &pr : Tenants) {
                auto tenant = pr.second;
                os << " - " << tenant->Path << " " << tenant->State << Endl
                   << "   Coordinators: " << tenant->Coordinators << Endl
                   << "   Mediators: " << tenant->Mediators << Endl
                   << "   PlanResolution: " << tenant->PlanResolution << Endl
                   << "   TimeCastBucketsPerMediator: " << tenant->TimeCastBucketsPerMediator << Endl
                   << "   StorageUnitsQuota: " << tenant->StorageUnitsQuota << Endl
                   << "   ComputationalUnitsQuota: " << tenant->ComputationalUnitsQuota << Endl
                   << "   Worker: " << tenant->Worker << Endl
                   << "   SubdomainVersion: " << tenant->SubdomainVersion << Endl
                   << "   ConfirmedSubdomain: " << tenant->ConfirmedSubdomain << Endl
                   << "   DomainId: " << DomainIdToStringSafe(tenant->DomainId) << Endl
                   << "   AreResourcesShared: " << tenant->AreResourcesShared << Endl
                   << "   SharedDomainId: " << DomainIdToStringSafe(tenant->SharedDomainId) << Endl
                   << "   Attributes: " << tenant->Attributes.ShortDebugString() << Endl;

                os << "   Storage pools:" << Endl;
                for (auto &pr : tenant->StoragePools) {
                    auto &pool = pr.second;
                    os << "    - " << pool->Kind << Endl
                       << "      State: " << pool->State << Endl
                       << "      Config: " << pool->Config.ShortDebugString() << Endl
                       << "      AllocatedNumGroups: " << pool->AllocatedNumGroups << Endl;
                    if (pool->Issue)
                        os << "      Issue: " << pool->Issue << Endl;
                }

                if (tenant->Worker)
                    os << "   Worker actor ID: " << tenant->Worker << Endl;

                os << "   Computational units:" << Endl;
                for (auto &pr : tenant->ComputationalUnits)
                    os << "    - [" << pr.first.first << ", " << pr.first.second << "]: " << pr.second << Endl;

                os << "   Slots:" << Endl;
                for (auto &slot : tenant->Slots)
                    os << "    - " << slot.first.ToString() << ": " << slot.second << Endl;

                os << "   Registered computational units:" << Endl;
                for (auto &pr : tenant->RegisteredComputationalUnits) {
                    auto &unit = pr.second;
                    os << "    - " << unit.Host << ":" << unit.Port << " - " << unit.Kind << Endl;
                }

                if (!tenant->StatusRequests.empty()) {
                    os << "   Pending status requests from:";
                    for (auto &e : tenant->StatusRequests)
                        os << " " << e->Sender;
                    os << Endl;
                }
            }

            os << Endl
               << "Removed tenants (path txid issue):" << Endl;
            for (auto &pr : RemovedTenants) {
                auto &tenant = pr.second;
                os << " - " << tenant.Path << " " << tenant.TxId << " " << tenant.Code << " " << tenant.Issue << Endl;
            }
        }
    }
}

void TTenantsManager::ProcessOrDelayTx(ITransaction *tx,
                                       const TActorContext &ctx)
{
    // We use this function to delay tx if we have to collect stats
    // before tx execution.
    if (Config.TotalComputationalUnitsLoadQuota) {
        if (DelayedTxs.empty())
            RequestTenantSlotsStats(ctx);
        DelayedTxs.push(THolder<ITransaction>(tx));
    } else {
        TxProcessor->ProcessTx(tx, ctx);
    }
}

void TTenantsManager::DbAddTenant(TTenant::TPtr tenant,
                                  TTransactionContext &txc,
                                  const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Add tenant " << tenant->Path << " to database"
                << " state=" << tenant->State
                << " coordinators=" << tenant->Coordinators
                << " mediators=" << tenant->Mediators
                << " planresolution=" << tenant->PlanResolution
                << " timecastbucketspermediator=" << tenant->TimeCastBucketsPerMediator
                << " issue=" << tenant->Issue
                << " txid=" << tenant->TxId
                << " subdomainversion=" << tenant->SubdomainVersion
                << " confirmedsubdomain=" << tenant->ConfirmedSubdomain
                << " attrs=" << tenant->Attributes.ShortDebugString()
                << " generation=" << tenant->Generation
                << " errorcode=" << tenant->ErrorCode
                << " isExternalSubDomain=" << tenant->IsExternalSubdomain
                << " isExternalHive=" << tenant->IsExternalHive
                << " isExternalSysViewProcessor=" << tenant->IsExternalSysViewProcessor
                << " isExternalStatisticsAggregator=" << tenant->IsExternalStatisticsAggregator
                << " areResourcesShared=" << tenant->AreResourcesShared
                << " sharedDomainId=" << tenant->SharedDomainId);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update(NIceDb::TUpdate<Schema::Tenants::State>(tenant->State),
                NIceDb::TUpdate<Schema::Tenants::Coordinators>(tenant->Coordinators),
                NIceDb::TUpdate<Schema::Tenants::Mediators>(tenant->Mediators),
                NIceDb::TUpdate<Schema::Tenants::PlanResolution>(tenant->PlanResolution),
                NIceDb::TUpdate<Schema::Tenants::TimeCastBucketsPerMediator>(tenant->TimeCastBucketsPerMediator),
                NIceDb::TUpdate<Schema::Tenants::Issue>(tenant->Issue),
                NIceDb::TUpdate<Schema::Tenants::TxId>(tenant->TxId),
                NIceDb::TUpdate<Schema::Tenants::UserToken>(tenant->UserToken.SerializeAsString()),
                NIceDb::TUpdate<Schema::Tenants::SubdomainVersion>(tenant->SubdomainVersion),
                NIceDb::TUpdate<Schema::Tenants::ConfirmedSubdomain>(tenant->ConfirmedSubdomain),
                NIceDb::TUpdate<Schema::Tenants::Attributes>(tenant->Attributes),
                NIceDb::TUpdate<Schema::Tenants::Generation>(tenant->Generation),
                NIceDb::TUpdate<Schema::Tenants::ErrorCode>(tenant->ErrorCode),
                NIceDb::TUpdate<Schema::Tenants::IsExternalSubDomain>(tenant->IsExternalSubdomain),
                NIceDb::TUpdate<Schema::Tenants::IsExternalHive>(tenant->IsExternalHive),
                NIceDb::TUpdate<Schema::Tenants::IsExternalSysViewProcessor>(tenant->IsExternalSysViewProcessor),
                NIceDb::TUpdate<Schema::Tenants::IsExternalStatisticsAggregator>(tenant->IsExternalStatisticsAggregator),
                NIceDb::TUpdate<Schema::Tenants::AreResourcesShared>(tenant->AreResourcesShared),
                NIceDb::TUpdate<Schema::Tenants::CreateIdempotencyKey>(tenant->CreateIdempotencyKey));

    if (tenant->SharedDomainId) {
        db.Table<Schema::Tenants>().Key(tenant->Path)
            .Update(NIceDb::TUpdate<Schema::Tenants::SharedDomainSchemeShardId>(tenant->SharedDomainId.OwnerId),
                    NIceDb::TUpdate<Schema::Tenants::SharedDomainPathId>(tenant->SharedDomainId.LocalPathId));
    }

    if (tenant->SchemaOperationQuotas) {
        TString serialized;
        Y_ABORT_UNLESS(tenant->SchemaOperationQuotas->SerializeToString(&serialized));
        db.Table<Schema::Tenants>().Key(tenant->Path)
            .Update(NIceDb::TUpdate<Schema::Tenants::SchemaOperationQuotas>(serialized));
    }

    if (tenant->DatabaseQuotas) {
        TString serialized;
        Y_ABORT_UNLESS(tenant->DatabaseQuotas->SerializeToString(&serialized));
        db.Table<Schema::Tenants>().Key(tenant->Path)
            .Update(NIceDb::TUpdate<Schema::Tenants::DatabaseQuotas>(serialized));
    }

    for (auto &pr : tenant->StoragePools) {
        auto &pool = *pr.second;

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Add tenant pool " << pool.Config.GetName() << " to database"
                    << " kind=" << pool.Kind
                    << " config=" << pool.Config.ShortDebugString()
                    << " allocatednumgroups=" << pool.AllocatedNumGroups
                    << " state=" << pool.State);

        TString config;
        Y_PROTOBUF_SUPPRESS_NODISCARD pool.Config.SerializeToString(&config);
        db.Table<Schema::TenantPools>().Key(tenant->Path, pool.Kind)
            .Update(NIceDb::TUpdate<Schema::TenantPools::Config>(config),
                    NIceDb::TUpdate<Schema::TenantPools::AllocatedNumGroups>(pool.AllocatedNumGroups),
                    NIceDb::TUpdate<Schema::TenantPools::State>((ui32)pool.State));
    }

    for (auto &pr : tenant->ComputationalUnits) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Add computational unit for " << tenant->Path << " to database"
                    << " kind=" << pr.first.first
                    << " zone=" << pr.first.second
                    << " count=" << pr.second);

        db.Table<Schema::TenantUnits>().Key(tenant->Path, pr.first.first, pr.first.second)
            .Update(NIceDb::TUpdate<Schema::TenantUnits::Count>(pr.second));
    }
}

template <typename SchemeShardIdColumn, typename PathIdColumn, typename TRowSet>
static TTenantsManager::TDomainId LoadDomainId(const TRowSet& rowset) {
    const ui64 schemeShardId = rowset.template GetValueOrDefault<SchemeShardIdColumn>(0);
    const ui64 pathId = rowset.template GetValueOrDefault<PathIdColumn>(0);

    if (!schemeShardId && !pathId) {
        return TTenantsManager::TDomainId();
    }

    return TTenantsManager::TDomainId(schemeShardId, pathId);
}

bool TTenantsManager::DbLoadState(TTransactionContext &txc, const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS_TENANTS, "Loading tenants state");

    NIceDb::TNiceDb db(txc.DB);
    auto tenantRowset = db.Table<Schema::Tenants>().Range().Select<Schema::Tenants::TColumns>();
    auto removedRowset = db.Table<Schema::RemovedTenants>().Range().Select<Schema::RemovedTenants::TColumns>();
    auto poolRowset = db.Table<Schema::TenantPools>().Range().Select<Schema::TenantPools::TColumns>();
    auto slotRowset = db.Table<Schema::TenantUnits>().Range().Select<Schema::TenantUnits::TColumns>();
    auto registeredRowset = db.Table<Schema::RegisteredUnits>().Range().Select<Schema::RegisteredUnits::TColumns>();

    if (!tenantRowset.IsReady()
        || !removedRowset.IsReady()
        || !poolRowset.IsReady()
        || !slotRowset.IsReady()
        || !registeredRowset.IsReady())
        return false;

    while (!tenantRowset.EndOfSet()) {
        TString path = tenantRowset.GetValue<Schema::Tenants::Path>();
        TTenant::EState state = static_cast<TTenant::EState>(tenantRowset.GetValue<Schema::Tenants::State>());
        ui64 coordinators = tenantRowset.GetValue<Schema::Tenants::Coordinators>();
        ui64 mediators = tenantRowset.GetValue<Schema::Tenants::Mediators>();
        ui64 planResolution = tenantRowset.GetValue<Schema::Tenants::PlanResolution>();
        ui32 timeCastBucketsPerMediator = tenantRowset.GetValue<Schema::Tenants::TimeCastBucketsPerMediator>();
        ui64 txId = tenantRowset.GetValue<Schema::Tenants::TxId>();
        TString userToken = tenantRowset.GetValue<Schema::Tenants::UserToken>();
        ui64 subdomainVersion = tenantRowset.GetValueOrDefault<Schema::Tenants::SubdomainVersion>(1);
        ui64 confirmedSubdomain = tenantRowset.GetValueOrDefault<Schema::Tenants::ConfirmedSubdomain>(0);
        NKikimrSchemeOp::TAlterUserAttributes attrs = tenantRowset.GetValueOrDefault<Schema::Tenants::Attributes>({});
        ui64 generation = tenantRowset.GetValueOrDefault<Schema::Tenants::Generation>(1);
        const TDomainId domainId = LoadDomainId<Schema::Tenants::SchemeShardId, Schema::Tenants::PathId>(tenantRowset);
        const TDomainId sharedDomainId = LoadDomainId<Schema::Tenants::SharedDomainSchemeShardId, Schema::Tenants::SharedDomainPathId>(tenantRowset);
        TString issue = tenantRowset.GetValueOrDefault<Schema::Tenants::Issue>("");
        Ydb::StatusIds::StatusCode errorCode
            = static_cast<Ydb::StatusIds::StatusCode>(tenantRowset.GetValueOrDefault<Schema::Tenants::ErrorCode>(0));
        bool isExternalSubDomain = tenantRowset.GetValueOrDefault<Schema::Tenants::IsExternalSubDomain>(false);
        bool isExternalHive = tenantRowset.GetValueOrDefault<Schema::Tenants::IsExternalHive>(false);
        bool isExternalSysViewProcessor = tenantRowset.GetValueOrDefault<Schema::Tenants::IsExternalSysViewProcessor>(false);
        bool isExternalStatisticsAggregator = tenantRowset.GetValueOrDefault<Schema::Tenants::IsExternalStatisticsAggregator>(false);
        const bool areResourcesShared = tenantRowset.GetValueOrDefault<Schema::Tenants::AreResourcesShared>(false);

        TTenant::TPtr tenant = new TTenant(path, state, userToken);
        tenant->Coordinators = coordinators;
        tenant->Mediators = mediators;
        tenant->PlanResolution = planResolution;
        tenant->TimeCastBucketsPerMediator = timeCastBucketsPerMediator;
        tenant->TxId = txId;
        tenant->SubdomainVersion = subdomainVersion;
        tenant->ConfirmedSubdomain = confirmedSubdomain;
        tenant->Attributes.CopyFrom(attrs);
        tenant->Generation = generation;
        tenant->DomainId = domainId;
        tenant->SharedDomainId = sharedDomainId;
        tenant->ErrorCode = errorCode;
        tenant->Issue = issue;
        tenant->IsExternalSubdomain = isExternalSubDomain;
        tenant->IsExternalHive = isExternalHive;
        tenant->IsExternalSysViewProcessor = isExternalSysViewProcessor;
        tenant->IsExternalStatisticsAggregator = isExternalStatisticsAggregator;
        tenant->AreResourcesShared = areResourcesShared;

        if (tenantRowset.HaveValue<Schema::Tenants::SchemaOperationQuotas>()) {
            auto& deserialized = tenant->SchemaOperationQuotas.ConstructInPlace();
            Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(deserialized, tenantRowset.GetValue<Schema::Tenants::SchemaOperationQuotas>()));
        }

        if (tenantRowset.HaveValue<Schema::Tenants::DatabaseQuotas>()) {
            auto& deserialized = tenant->DatabaseQuotas.ConstructInPlace();
            Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(deserialized, tenantRowset.GetValue<Schema::Tenants::DatabaseQuotas>()));
        }

        if (tenantRowset.HaveValue<Schema::Tenants::CreateIdempotencyKey>()) {
            tenant->CreateIdempotencyKey = tenantRowset.GetValue<Schema::Tenants::CreateIdempotencyKey>();
        }

        if (tenantRowset.HaveValue<Schema::Tenants::AlterIdempotencyKey>()) {
            tenant->AlterIdempotencyKey = tenantRowset.GetValue<Schema::Tenants::AlterIdempotencyKey>();
        }

        AddTenant(tenant);

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "Loaded tenant " << path);

        if (!tenantRowset.Next())
            return false;
    }

    for (auto [_, tenant] : Tenants) {
        if (!tenant->SharedDomainId) {
            continue;
        }

        auto sharedTenant = GetTenant(tenant->SharedDomainId);
        Y_ABORT_UNLESS(sharedTenant);
        sharedTenant->HostedTenants.emplace(tenant);
    }

    while (!removedRowset.EndOfSet()) {
        TRemovedTenant tenant;
        tenant.Path = removedRowset.GetValue<Schema::RemovedTenants::Path>();
        tenant.TxId = removedRowset.GetValue<Schema::RemovedTenants::TxId>();
        tenant.Issue = removedRowset.GetValue<Schema::RemovedTenants::Issue>();
        tenant.Code = static_cast<Ydb::StatusIds::StatusCode>(removedRowset.GetValueOrDefault<Schema::RemovedTenants::Code>(Ydb::StatusIds::SUCCESS));
        tenant.ErrorCode = static_cast<Ydb::StatusIds::StatusCode>(removedRowset.GetValueOrDefault<Schema::RemovedTenants::ErrorCode>(0));
        tenant.CreateIdempotencyKey = removedRowset.GetValueOrDefault<Schema::RemovedTenants::CreateIdempotencyKey>(TString());

        RemovedTenants[tenant.Path] = tenant;

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Loaded removed tenant " << tenant.Path << " (txid = " << tenant.TxId << ")");

        if (!removedRowset.Next())
            return false;
    }

    while (!poolRowset.EndOfSet()) {
        TString path = poolRowset.GetValue<Schema::TenantPools::Tenant>();
        TString kind = poolRowset.GetValue<Schema::TenantPools::PoolType>();
        TString configVal = poolRowset.GetValue<Schema::TenantPools::Config>();
        ui32 allocated = poolRowset.GetValue<Schema::TenantPools::AllocatedNumGroups>();
        ui32 stateVal = poolRowset.GetValue<Schema::TenantPools::State>();
        bool borrowed = poolRowset.GetValueOrDefault<Schema::TenantPools::Borrowed>(false);
        TStoragePool::EState state = static_cast<TStoragePool::EState>(stateVal);

        NKikimrBlobStorage::TDefineStoragePool config;
        Y_PROTOBUF_SUPPRESS_NODISCARD config.ParseFromArray(configVal.data(), configVal.size());

        TStoragePool::TPtr pool = new TStoragePool(kind, config, borrowed);
        pool->AllocatedNumGroups = allocated;
        pool->State = state;

        auto tenant = GetTenant(path);
        Y_DEBUG_ABORT_UNLESS(tenant, "loaded pool for unknown tenant %s", path.data());
        if (tenant) {
            tenant->StoragePools[kind] = pool;

            Counters.Inc(pool->Kind, COUNTER_REQUESTED_STORAGE_UNITS, pool->GetGroups());
            Counters.Inc(pool->Kind, COUNTER_ALLOCATED_STORAGE_UNITS, pool->AllocatedNumGroups);

            LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                        "Loaded pool " << pool->Config.GetName() << " for " << path);
        } else {
            LOG_CRIT_S(ctx, NKikimrServices::CMS_TENANTS,
                       "Loaded pool " << pool->Config.GetName() << " for unknown tenant " << path);
        }

        if (!poolRowset.Next())
            return false;
    }

    while (!slotRowset.EndOfSet()) {
        TString path = slotRowset.GetValue<Schema::TenantUnits::Tenant>();
        TString kind = slotRowset.GetValue<Schema::TenantUnits::UnitKind>();
        TString zone = slotRowset.GetValue<Schema::TenantUnits::AvailabilityZone>();
        ui64 count = slotRowset.GetValue<Schema::TenantUnits::Count>();

        auto tenant = GetTenant(path);
        Y_DEBUG_ABORT_UNLESS(tenant, "loaded units <%s, %s>(%" PRIu64 ") for unknown tenant %s",
                       kind.data(), zone.data(), count, path.data());
        if (tenant) {
            tenant->ComputationalUnits[std::make_pair(kind, zone)] = count;

            Counters.Inc(kind, zone, COUNTER_COMPUTATIONAL_UNITS, count);

            LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                        "Loaded units <" << kind << ", " << zone << ">("
                        << count << ") for tenant " << path);
        } else {
            LOG_CRIT_S(ctx, NKikimrServices::CMS_TENANTS,
                       "Loaded units <" << kind << ", " << zone << ">("
                       << count << ") for unknown tenant " << path);
        }

        if (!slotRowset.Next())
            return false;
    }

    while (!registeredRowset.EndOfSet()) {
        TString path = registeredRowset.GetValue<Schema::RegisteredUnits::Tenant>();
        TString host = registeredRowset.GetValue<Schema::RegisteredUnits::Host>();
        ui32 port = registeredRowset.GetValue<Schema::RegisteredUnits::Port>();
        TString kind = registeredRowset.GetValue<Schema::RegisteredUnits::Kind>();

        auto tenant = GetTenant(path);
        Y_DEBUG_ABORT_UNLESS(tenant, "loaded registered unit %s:%" PRIu32 " for unknown tenant %s",
                       host.data(), port, path.data());
        if (tenant) {
            TAllocatedComputationalUnit unit{host, port, kind};
            tenant->RegisteredComputationalUnits[std::make_pair(host, port)] = unit;

            Counters.Inc(kind, COUNTER_REGISTERED_UNITS);

            LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                        "Loaded registered unit " << host << ":" << port << " of kind "
                        << kind << " for tenant " << path);
        } else {
            LOG_CRIT_S(ctx, NKikimrServices::CMS_TENANTS,
                       "Loaded registered unit " << host << ":" << port << " of kind "
                       << kind << " for unknown tenant " << path);
        }

        if (!registeredRowset.Next())
            return false;
    }

    for (auto &pr: Tenants) {
        pr.second->ParseComputationalUnits(Config);
        SlotStats.AllocateSlots(pr.second->Slots);
    }

    return true;
}

void TTenantsManager::DbRemoveComputationalUnit(TTenant::TPtr tenant,
                                                const TString &kind,
                                                const TString &zone,
                                                TTransactionContext &txc,
                                                const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Remove computational unit of " << tenant->Path << " from database"
                << " kind=" << kind
                << " zone=" << zone);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TenantUnits>().Key(tenant->Path, kind, zone).Delete();
}

void TTenantsManager::DbUpdateConfirmedSubdomain(TTenant::TPtr tenant,
                                                 ui64 version,
                                                 TTransactionContext &txc,
                                                 const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update database for " << tenant->Path
                << " confirmedsubdomain=" << version);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update(NIceDb::TUpdate<Schema::Tenants::ConfirmedSubdomain>(version));
}

void TTenantsManager::DbRemoveComputationalUnits(TTenant::TPtr tenant,
                                                 TTransactionContext &txc,
                                                 const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Remove computational units of " << tenant->Path << " from database"
                << " txid=" << tenant->TxId
                << " issue=" << tenant->Issue);

    NIceDb::TNiceDb db(txc.DB);
    for (auto &pr : tenant->ComputationalUnits)
        db.Table<Schema::TenantUnits>().Key(tenant->Path, pr.first.first, pr.first.second).Delete();
    for (auto &pr : tenant->RegisteredComputationalUnits)
        db.Table<Schema::RegisteredUnits>().Key(tenant->Path, pr.first.first, pr.first.second).Delete();
}

void TTenantsManager::DbRemoveRegisteredUnit(TTenant::TPtr tenant,
                                             const TString &host,
                                             ui32 port,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Remove registered computational unit for " << tenant->Path << " from database"
                << " host=" << host
                << " port=" << port);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::RegisteredUnits>().Key(tenant->Path, host, port).Delete();
}

void TTenantsManager::DbRemoveTenantAndPools(TTenant::TPtr tenant,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Remove tenant " << tenant->Path << " from database"
                << " txid=" << tenant->TxId
                << " issue=" << tenant->Issue);

    NIceDb::TNiceDb db(txc.DB);
    for (auto &pr : tenant->StoragePools) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Remove pool " << pr.second->Config.GetName() << " from database");
        db.Table<Schema::TenantPools>().Key(tenant->Path, pr.second->Kind).Delete();
    }
    db.Table<Schema::Tenants>().Key(tenant->Path).Delete();
}

void TTenantsManager::DbUpdateComputationalUnit(TTenant::TPtr tenant,
                                                const TString &kind,
                                                const TString &zone,
                                                ui64 count,
                                                TTransactionContext &txc,
                                                const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Remove computational unit of " << tenant->Path << " from database"
                << " kind=" << kind
                << " zone=" << zone
                << " count=" << count);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TenantUnits>().Key(tenant->Path, kind, zone)
        .Update(NIceDb::TUpdate<Schema::TenantUnits::Count>(count));
}

void TTenantsManager::DbUpdatePool(TTenant::TPtr tenant,
                                   TStoragePool::TPtr pool,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update database for pool " << pool->Config.GetName()
                << " config=" << pool->Config.ShortDebugString()
                << " state=" << pool->State);

    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD pool->Config.SerializeToString(&config);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TenantPools>().Key(tenant->Path, pool->Kind)
        .Update(NIceDb::TUpdate<Schema::TenantPools::Config>(config),
                NIceDb::TUpdate<Schema::TenantPools::State>((ui32)pool->State));
}

void TTenantsManager::DbUpdatePoolConfig(TTenant::TPtr tenant,
                                         TStoragePool::TPtr pool,
                                         const NKikimrBlobStorage::TDefineStoragePool &config,
                                         TTransactionContext &txc,
                                         const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update pool config in database for " << pool->Config.GetName()
                << " config=" << config.ShortDebugString());

    TString val;
    Y_PROTOBUF_SUPPRESS_NODISCARD config.SerializeToString(&val);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TenantPools>().Key(tenant->Path, pool->Kind)
        .Update<Schema::TenantPools::Config>(val);
}

void TTenantsManager::DbUpdatePoolState(TTenant::TPtr tenant,
                                        TStoragePool::TPtr pool,
                                        TStoragePool::EState state,
                                        TTransactionContext &txc,
                                        const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update pool state in database for " << pool->Config.GetName()
                << " state=" << state);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TenantPools>().Key(tenant->Path, pool->Kind)
        .Update<Schema::TenantPools::State>((ui32)state);
}

void TTenantsManager::DbUpdatePoolState(TTenant::TPtr tenant,
                                        TStoragePool::TPtr pool,
                                        TStoragePool::EState state,
                                        ui32 allocatedNumGroups,
                                        TTransactionContext &txc,
                                        const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update pool state in database for " << pool->Config.GetName()
                << " state=" << state
                << " allocatednumgroups=" << allocatedNumGroups);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TenantPools>().Key(tenant->Path, pool->Kind)
        .Update(NIceDb::TUpdate<Schema::TenantPools::AllocatedNumGroups>(allocatedNumGroups),
                NIceDb::TUpdate<Schema::TenantPools::State>((ui32)state));
}

void TTenantsManager::DbUpdateRegisteredUnit(TTenant::TPtr tenant,
                                             const TString &host,
                                             ui32 port,
                                             const TString &kind,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update registered computational unit for " << tenant->Path << " in database"
                << " host=" << host
                << " port=" << port
                << " kind=" << kind);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::RegisteredUnits>().Key(tenant->Path, host, port)
        .Update(NIceDb::TUpdate<Schema::RegisteredUnits::Kind>(kind));
}

void TTenantsManager::DbUpdateRemovedTenant(TTenant::TPtr tenant,
                                            Ydb::StatusIds::StatusCode code,
                                            TTransactionContext &txc,
                                            const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Add tenant removal info for " << tenant->Path
                << " txid=" << tenant->TxId
                << " code=" << code
                << " errorcode=" << tenant->ErrorCode
                << " issue=" << tenant->Issue);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::RemovedTenants>().Key(tenant->Path)
        .Update<Schema::RemovedTenants::TxId>(tenant->TxId)
        .Update<Schema::RemovedTenants::Issue>(tenant->Issue)
        .Update<Schema::RemovedTenants::Code>(code)
        .Update<Schema::RemovedTenants::ErrorCode>(tenant->ErrorCode)
        .Update<Schema::RemovedTenants::CreateIdempotencyKey>(tenant->CreateIdempotencyKey);
}

void TTenantsManager::DbUpdateTenantAlterIdempotencyKey(TTenant::TPtr tenant,
                                                        const TString &idempotencyKey,
                                                        TTransactionContext &txc,
                                                        const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update alter idempotency key for " << tenant->Path
                << " alterIdempotencyKey=" << idempotencyKey);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update<Schema::Tenants::AlterIdempotencyKey>(idempotencyKey);
}

void TTenantsManager::DbUpdateTenantUserAttributes(TTenant::TPtr tenant,
                                                   const NKikimrSchemeOp::TAlterUserAttributes &attributes,
                                                   TTransactionContext &txc,
                                                   const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update alter user attributes for " << tenant->Path
                << " userAttributes=" << attributes.ShortDebugString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update<Schema::Tenants::Attributes>(attributes);
}

void TTenantsManager::DbUpdateTenantGeneration(TTenant::TPtr tenant,
                                               ui64 generation,
                                               TTransactionContext &txc,
                                               const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update generation for " << tenant->Path
                << " generation=" << generation);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update<Schema::Tenants::Generation>(generation);
}

void TTenantsManager::DbUpdateTenantState(TTenant::TPtr tenant,
                                          TTenant::EState state,
                                          TTransactionContext &txc,
                                          const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update tenant state in database for " << tenant->Path
                << " state=" << state
                << " txid=" << tenant->TxId
                << " errorcode=" << tenant->ErrorCode
                << " issue=" << tenant->Issue);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update(NIceDb::TUpdate<Schema::Tenants::State>(state),
                NIceDb::TUpdate<Schema::Tenants::TxId>(tenant->TxId),
                NIceDb::TUpdate<Schema::Tenants::Issue>(tenant->Issue),
                NIceDb::TUpdate<Schema::Tenants::ErrorCode>(tenant->ErrorCode));
}

void TTenantsManager::DbUpdateTenantSubdomain(TTenant::TPtr tenant,
                                              ui64 schemeShardId,
                                              ui64 pathId,
                                              TTransactionContext &txc,
                                              const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update tenant subdomain in database for " << tenant->Path
                << " schemeshardid=" << schemeShardId
                << " pathid=" << pathId);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update<Schema::Tenants::SchemeShardId>(schemeShardId)
        .Update<Schema::Tenants::PathId>(pathId);
}

void TTenantsManager::DbUpdateTenantUserToken(TTenant::TPtr tenant,
                                              const TString &userToken,
                                              TTransactionContext &txc,
                                              const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update user token in database for " << tenant->Path);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update(NIceDb::TUpdate<Schema::Tenants::UserToken>(userToken));
}

void TTenantsManager::DbUpdateSubdomainVersion(TTenant::TPtr tenant,
                                               ui64 version,
                                               TTransactionContext &txc,
                                               const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update subdomain version in database for " << tenant->Path
                << " subdomainversion=" << version);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update(NIceDb::TUpdate<Schema::Tenants::SubdomainVersion>(version));
}

void TTenantsManager::DbUpdateSchemaOperationQuotas(TTenant::TPtr tenant,
                                                    const Ydb::Cms::SchemaOperationQuotas &quotas,
                                                    TTransactionContext &txc,
                                                    const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update schema operation quotas for " << tenant->Path
                << " quotas = " << quotas.DebugString());

    TString serialized;
    Y_ABORT_UNLESS(quotas.SerializeToString(&serialized));

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update(NIceDb::TUpdate<Schema::Tenants::SchemaOperationQuotas>(serialized));
}

void TTenantsManager::DbUpdateDatabaseQuotas(TTenant::TPtr tenant,
                                             const Ydb::Cms::DatabaseQuotas &quotas,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Update database quotas for " << tenant->Path
                << " quotas = " << quotas.DebugString());

    TString serialized;
    Y_ABORT_UNLESS(quotas.SerializeToString(&serialized));

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Tenants>().Key(tenant->Path)
        .Update(NIceDb::TUpdate<Schema::Tenants::DatabaseQuotas>(serialized));
}

void TTenantsManager::Handle(TEvConsole::TEvAlterTenantRequest::TPtr &ev, const TActorContext &ctx)
{
    Counters.Inc(COUNTER_ALTER_REQUESTS);
    ProcessOrDelayTx(CreateTxAlterTenant(ev), ctx);
}

void TTenantsManager::Handle(TEvConsole::TEvCreateTenantRequest::TPtr &ev, const TActorContext &ctx)
{
    Counters.Inc(COUNTER_CREATE_REQUESTS);
    ProcessOrDelayTx(CreateTxCreateTenant(ev), ctx);
}

void TTenantsManager::Handle(TEvConsole::TEvDescribeTenantOptionsRequest::TPtr &ev, const TActorContext &ctx)
{
    Ydb::Cms::DescribeDatabaseOptionsResult result;

    Counters.Inc(COUNTER_DESCRIBE_REQUESTS);

    // Fill pool settings.
    for (auto &pr : Domain->StoragePoolTypes) {
        auto &config = pr.second;
        auto &description = *result.add_storage_units();
        description.set_kind(pr.first);
        (*description.mutable_labels())["erasure"] = config.GetErasureSpecies();
        for (auto &filter : config.GetPDiskFilter()) {
            for (auto &prop : filter.GetProperty()) {
                if (prop.HasType()) {
                    auto diskType = ToString(prop.GetType());
                    (*description.mutable_labels())["disk_type"] = diskType;
                }
            }
        }
    }

    // Fill zone settings.
    for (auto &pr : Config.AvailabilityZones) {
        auto &description = *result.add_availability_zones();
        description.set_name(pr.first);
        TString dc;
        if (pr.second.DataCenter != ANY_DATA_CENTER)
            (*description.mutable_labels())["fixed_data_center"] = pr.second.DataCenter;
        else
            (*description.mutable_labels())["any_data_center"] = "true";
        if (pr.second.CollocationGroup) {
            if (pr.second.ForceCollocation)
                (*description.mutable_labels())["collocation"] = "forced";
            else
                (*description.mutable_labels())["collocation"] = "enabled";
        } else {
            (*description.mutable_labels())["collocation"] = "disabled";
        }
    }

    // Fill slot settings.
    for (auto &pr : Config.TenantSlotKinds) {
        auto &description = *result.add_computational_units();
        description.set_kind(pr.first);
        for (auto &zone : pr.second.AllowedZones)
            description.add_allowed_availability_zones(zone);
        (*description.mutable_labels())["type"] = "dynamic_slot";
        (*description.mutable_labels())["slot_type"] = pr.second.TenantSlotType;
    }

    auto resp = MakeHolder<TEvConsole::TEvDescribeTenantOptionsResponse>();
    auto &operation = *resp->Record.MutableResponse()->mutable_operation();
    operation.set_ready(true);
    operation.set_status(Ydb::StatusIds::SUCCESS);
    operation.mutable_result()->PackFrom(result);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Send TEvConsole::TEvDescribeTenantOptionsResponse: "
                << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TTenantsManager::Handle(TEvConsole::TEvGetOperationRequest::TPtr &ev, const TActorContext &ctx)
{
    Counters.Inc(COUNTER_GET_OPERATION_REQUESTS);

    auto resp = MakeHolder<TEvConsole::TEvGetOperationResponse>();
    auto &operation = *resp->Record.MutableResponse()->mutable_operation();
    FillOperationStatus(ev->Get()->Record.GetRequest().id(), operation);

    if (operation.status())
        Counters.Inc(operation.status(), COUNTER_CREATE_RESPONSES);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Send TEvConsole::TEvGetOperationResponse: "
                << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TTenantsManager::Handle(TEvConsole::TEvGetTenantStatusRequest::TPtr &ev, const TActorContext &ctx)
{
    Counters.Inc(COUNTER_STATUS_REQUESTS);

    auto path = CanonizePath(ev->Get()->Record.GetRequest().path());
    auto tenant = GetTenant(path);

    auto resp = MakeHolder<TEvConsole::TEvGetTenantStatusResponse>();
    auto &operation = *resp->Record.MutableResponse()->mutable_operation();
    operation.set_ready(true);

    if (!tenant) {
        operation.set_status(Ydb::StatusIds::NOT_FOUND);
        auto issue = operation.add_issues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(Sprintf("Unknown tenant %s", path.data()));

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Send TEvConsole::TEvGetTenantStatusResponse: "
                    << resp->Record.ShortDebugString());

        Counters.Inc(Ydb::StatusIds::NOT_FOUND, COUNTER_STATUS_RESPONSES);

        ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    } else if (!tenant->IsRunning()
               && !tenant->IsConfiguring()) {
        Ydb::Cms::GetDatabaseStatusResult result;
        FillTenantStatus(tenant, result);

        operation.set_status(Ydb::StatusIds::SUCCESS);
        operation.mutable_result()->PackFrom(result);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Send TEvConsole::TEvGetTenantStatusResponse: "
                    << resp->Record.ShortDebugString());

        Counters.Inc(Ydb::StatusIds::SUCCESS, COUNTER_STATUS_RESPONSES);

        ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    } else {
        if (tenant->StatusRequests.empty())
            RequestTenantSlotsState(tenant, ctx);

        tenant->StatusRequests.push_back(ev.Release());
    }
}

void TTenantsManager::Handle(TEvConsole::TEvListTenantsRequest::TPtr &ev, const TActorContext &ctx)
{
    Counters.Inc(COUNTER_LIST_REQUESTS);

    Ydb::Cms::ListDatabasesResult result;
    for (auto &pr : Tenants)
        result.add_paths(pr.first);

    auto resp = MakeHolder<TEvConsole::TEvListTenantsResponse>();
    auto &operation = *resp->Record.MutableResponse()->mutable_operation();
    operation.set_ready(true);
    operation.set_status(Ydb::StatusIds::SUCCESS);
    operation.mutable_result()->PackFrom(result);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                "Send TEvConsole::TEvListTenantsResponse: "
                << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TTenantsManager::Handle(TEvConsole::TEvNotifyOperationCompletionRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;
    Ydb::Operations::Operation operation;
    auto tenant = FillOperationStatus(rec.GetRequest().id(), operation);

    if (operation.ready() && operation.status() != Ydb::StatusIds::NOT_FOUND) {
        auto resp = MakeHolder<TEvConsole::TEvOperationCompletionNotification>();
        resp->Record.MutableResponse()->mutable_operation()->CopyFrom(operation);
        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Send TEvConsole::TEvOperationCompletionNotification: "
                    << resp->Record.ShortDebugString());
        ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    } else {
        if (!operation.ready()) {
            Y_ABORT_UNLESS(tenant);
            LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                        "Add subscription to " << tenant->Path << " for " << ev->Sender);
            tenant->Subscribers.push_back(ev->Sender);
        }

        auto resp = MakeHolder<TEvConsole::TEvNotifyOperationCompletionResponse>();
        resp->Record.MutableResponse()->mutable_operation()->CopyFrom(operation);
        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Send TEvConsole::TEvNotifyOperationCompletionResponse: "
                    << resp->Record.ShortDebugString());
        ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    }
}

void TTenantsManager::Handle(TEvConsole::TEvRemoveTenantRequest::TPtr &ev, const TActorContext &ctx)
{
    Counters.Inc(COUNTER_REMOVE_REQUESTS);

    TxProcessor->ProcessTx(CreateTxRemoveTenant(ev), ctx);
}

void TTenantsManager::Handle(TEvConsole::TEvUpdateTenantPoolConfig::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxUpdateTenantPoolConfig(ev), ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvPoolAllocated::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;
    auto pool = ev->Get()->Pool;
    Y_ABORT_UNLESS(pool->State != TStoragePool::ALLOCATED);

    pool->GroupFitErrors = 0;

    TxProcessor->ProcessTx(CreateTxUpdatePoolState(tenant, pool, ev->Sender,
                                                   TStoragePool::ALLOCATED),
                           ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvPoolFailed::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;
    auto pool = ev->Get()->Pool;
    auto &issue = ev->Get()->Issue;

    if (pool->Worker != ev->Sender) {
        LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Ignoring TEvPrivate::TEvPoolFailed from outdated worker of "
                   << pool->Config.GetName() << " pool for tenant "
                   << tenant->Path << ": " << issue);
        return;
    }

    if (tenant->State == TTenant::REMOVING_POOLS) {
        LOG_CRIT_S(ctx, NKikimrServices::CMS_TENANTS,
                   "Couldn't delete storage pool " << pool->Config.GetName()
                   << " for tenant " << tenant->Path << ": " << issue);
        Counters.Inc(COUNTER_REMOVE_POOL_FAILED);

        pool->Worker = TActorId();
        pool->GroupFitErrors = 0;
        ctx.Schedule(TDuration::Seconds(10),
                     new TEvPrivate::TEvRetryAllocateResources(tenant->Path));
    } else if (tenant->IsCreating()) {
        Counters.Inc(COUNTER_ALLOC_POOL_FAILED);
        pool->Worker = TActorId();
        pool->GroupFitErrors = 0;
        tenant->Issue = issue;
        TxProcessor->ProcessTx(CreateTxUpdateTenantState(tenant->Path, TTenant::REMOVING_POOLS), ctx);
    } else {
        LOG_CRIT_S(ctx, NKikimrServices::CMS_TENANTS,
                   "Couldn't update storage pool " << pool->Config.GetName()
                   << " for tenant " << tenant->Path << ": " << issue);

        if (issue.Contains("Group fit error")) {
            if (++pool->GroupFitErrors >= 10) {
                pool->GroupFitErrors = 0;
                TxProcessor->ProcessTx(CreateTxRevertPoolState(tenant, pool, ev->Sender), ctx);
                return;
            }
        } else {
            pool->GroupFitErrors = 0;
        }

        Counters.Inc(COUNTER_ALLOC_POOL_FAILED);
        pool->Worker = TActorId();
        ctx.Schedule(TDuration::Seconds(10),
                     new TEvPrivate::TEvRetryAllocateResources(tenant->Path));
    }
}

void TTenantsManager::Handle(TEvPrivate::TEvPoolDeleted::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;
    auto pool = ev->Get()->Pool;
    Y_ABORT_UNLESS(pool->Worker == ev->Sender);

    TxProcessor->ProcessTx(CreateTxUpdatePoolState(tenant, pool, ev->Sender,
                                                   TStoragePool::DELETED),
                           ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvRetryAllocateResources::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = GetTenant(ev->Get()->TenantName);
    if (tenant)
        ProcessTenantActions(tenant, ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvStateLoaded::TPtr &/*ev*/, const TActorContext &ctx)
{
    for (auto &pr : Tenants)
        ProcessTenantActions(pr.second, ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvSubdomainFailed::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;
    auto &issue = ev->Get()->Issue;
    auto code = ev->Get()->Code;

    if (tenant->Worker != ev->Sender) {
        LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Ignoring TEvPrivate::TEvSubdomainFailed from outdated worker of "
                    << tenant->Path << ": " << issue);
        return;
    }

    tenant->ErrorCode = code;
    tenant->Issue = issue;

    if (tenant->IsRemoving()) {
        Y_ABORT_UNLESS(tenant->State == TTenant::REMOVING_SUBDOMAIN);

        LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Cannot remove subdomain for tenant " << tenant->Path
                    << ": " << code << ": " << issue);

        Counters.Inc(COUNTER_REMOVE_SUBDOMAIN_FAILED);

        TxProcessor->ProcessTx(CreateTxRemoveTenantFailed(tenant, code), ctx);
        return;
    }

    Y_ABORT_UNLESS(tenant == GetTenant(tenant->Path));
    Y_ABORT_UNLESS(tenant->State == TTenant::CREATING_SUBDOMAIN
             || tenant->State == TTenant::CONFIGURING_SUBDOMAIN
             || tenant->State == TTenant::RUNNING);

    Counters.Inc(COUNTER_CONFIGURE_SUBDOMAIN_FAILED);

    if (tenant->State == TTenant::CREATING_SUBDOMAIN) {
        LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Cannot create subdomain for tenant " << tenant->Path
                    << ": " << code << ": " << issue);

        TxProcessor->ProcessTx(CreateTxUpdateTenantState(tenant->Path,
                                                         TTenant::REMOVING_POOLS,
                                                         tenant->Worker),
                               ctx);
    } else {
        LOG_CRIT_S(ctx, NKikimrServices::CMS_TENANTS,
                   "Cannot configure subdomain for tenant " << tenant->Path
                    << ": " << code << ": " << issue);

        tenant->Issue = issue;
        tenant->Worker = TActorId();
        ctx.Schedule(TDuration::Seconds(10),
                     new TEvPrivate::TEvRetryAllocateResources(tenant->Path));
    }
}

void TTenantsManager::Handle(TEvPrivate::TEvSubdomainCreated::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;
    Y_ABORT_UNLESS(tenant == GetTenant(tenant->Path));

    if (tenant->Worker != ev->Sender) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Ignoring created subdomain from outdated worker for tenant " << tenant->Path
                    << " in " << tenant->State << " state");
        return;
    }

    Y_ABORT_UNLESS(tenant->State == TTenant::CREATING_SUBDOMAIN);
    TxProcessor->ProcessTx(CreateTxUpdateSubDomainKey(tenant->Path,
                                                      ev->Get()->SchemeShardId,
                                                      ev->Get()->PathId,
                                                      tenant->Worker),
                           ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvSubdomainKey::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;
    Y_ABORT_UNLESS(tenant == GetTenant(tenant->Path));

    if (tenant->Worker != ev->Sender) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Ignoring subdomain key from outdated worker for tenant " << tenant->Path
                    << " in " << tenant->State << " state");
        return;
    }

    TxProcessor->ProcessTx(CreateTxUpdateSubDomainKey(tenant->Path,
                                                      ev->Get()->SchemeShardId,
                                                      ev->Get()->PathId,
                                                      tenant->Worker),
                           ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvSubdomainReady::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;

    // Subdomain configuration might be too late.
    if (tenant->IsRemoving()) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Ignoring ready subdomain for tenant " << tenant->Path
                    << " in " << tenant->State << " state");
        return;
    }

    if (tenant->Worker != ev->Sender) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Ignoring ready subdomain from outdated worker for tenant " << tenant->Path
                    << " in " << tenant->State << " state");
        return;
    }

    Y_ABORT_UNLESS(tenant == GetTenant(tenant->Path));
    Y_ABORT_UNLESS(tenant->State == TTenant::CONFIGURING_SUBDOMAIN
             || tenant->State == TTenant::RUNNING);

    TxProcessor->ProcessTx(CreateTxUpdateConfirmedSubdomain(tenant->Path,
                                                            ev->Get()->Version,
                                                            tenant->Worker),
                           ctx);
}

void TTenantsManager::Handle(TEvPrivate::TEvSubdomainRemoved::TPtr &ev, const TActorContext &ctx)
{
    auto tenant = ev->Get()->Tenant;
    Y_ABORT_UNLESS(tenant == GetTenant(tenant->Path));
    Y_ABORT_UNLESS(tenant->State == TTenant::REMOVING_SUBDOMAIN);
    Y_ABORT_UNLESS(tenant->Worker == ev->Sender);

    TxProcessor->ProcessTx(CreateTxRemoveComputationalUnits(tenant), ctx);
}

void TTenantsManager::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx)
{
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->ClientId == TenantSlotBrokerPipe
        && msg->Status != NKikimrProto::OK) {
        OnTenantSlotBrokerPipeDestroyed(ctx);
    }
}

void TTenantsManager::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx)
{
    TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
    if (msg->ClientId == TenantSlotBrokerPipe)
        OnTenantSlotBrokerPipeDestroyed(ctx);
}

void TTenantsManager::Handle(TEvTenantSlotBroker::TEvSlotStats::TPtr &ev, const TActorContext &ctx)
{
    SlotStats.UpdateStats(ev->Get()->Record);
    while (!DelayedTxs.empty()) {
        auto &tx = DelayedTxs.front();
        TxProcessor->ProcessTx(tx.Release(), ctx);
        DelayedTxs.pop();
    }
}

void TTenantsManager::Handle(TEvTenantSlotBroker::TEvTenantState::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    TString path = CanonizePath(rec.GetTenantName());
    auto tenant = GetTenant(path);
    if (!tenant) {
        LOG_WARN_S(ctx, NKikimrServices::CMS_TENANTS,
                   "Got state for missing tenant " << path
                   << " (" << rec.GetTenantName() << ") from Tenant Slot Broker");
        return;
    }

    tenant->SlotsAllocationConfirmed = CheckTenantSlots(tenant, rec);
    if (!tenant->SlotsAllocationConfirmed) {
        LOG_INFO_S(ctx, NKikimrServices::CMS_TENANTS,
                   "Wrong resources are assigned for " << tenant->Path);
        ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvRetryAllocateResources(tenant->Path));
    }

    for (auto &req : tenant->StatusRequests) {
        Ydb::Cms::GetDatabaseStatusResult result;
        FillTenantStatus(tenant, result);
        FillTenantAllocatedSlots(tenant, result, rec);

        auto resp = MakeHolder<TEvConsole::TEvGetTenantStatusResponse>();
        auto &operation = *resp->Record.MutableResponse()->mutable_operation();
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::SUCCESS);
        operation.mutable_result()->PackFrom(result);

        Counters.Inc(Ydb::StatusIds::SUCCESS, COUNTER_STATUS_RESPONSES);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Send TEvConsole::TEvGetTenantStatusResponse: "
                    << resp->Record.ShortDebugString());

        ctx.Send(req->Sender, resp.Release(), 0, req->Cookie);
    }
    tenant->StatusRequests.clear();

    if (tenant->State == TTenant::REMOVING_UNITS)
        TxProcessor->ProcessTx(CreateTxUpdateTenantState(tenant->Path, TTenant::REMOVING_POOLS), ctx);
}

TString MakeStoragePoolName(const TString &tenantName, const TString &poolTypeName)
{
    return Sprintf("%s:%s", tenantName.data(), poolTypeName.data());
}

} // namespace NKikimr::NConsole
