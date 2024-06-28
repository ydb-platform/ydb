#include "actors.h"

#include <ydb/core/base/path.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/workload_service/common/events.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/table_creator/table_creator.h>


namespace NKikimr::NKqp::NWorkload {

namespace {

using namespace NActors;


class TPoolFetcherActor : public TSchemeActorBase<TPoolFetcherActor> {
public:
    explicit TPoolFetcherActor(TEvPlaceRequestIntoPool::TPtr event)
        : Event(std::move(event))
    {
        if (!Event->Get()->PoolId) {
            Event->Get()->PoolId = NResourcePool::DEFAULT_POOL_ID;
        }
        UseDefaultPool = Event->Get()->PoolId == NResourcePool::DEFAULT_POOL_ID;
    }

    void DoBootstrap() {
        Become(&TPoolFetcherActor::StateFunc);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != 1) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response");
            return;
        }

        const auto& result = results[0];
        switch (result.Status) {
            case EStatus::Unknown:
            case EStatus::PathNotTable:
            case EStatus::PathNotPath:
            case EStatus::RedirectLookupError:
                Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid resource pool id " << Event->Get()->PoolId);
                return;
            case EStatus::AccessDenied:
            case EStatus::RootUnknown:
            case EStatus::PathErrorUnknown:
                if (!UseDefaultPool) {
                    Reply(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Resource pool " << Event->Get()->PoolId << " not found");
                } else {
                    CreateDefaultPool();
                }
                return;
            case EStatus::LookupError:
            case EStatus::TableCreationNotComplete:
                if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                    Reply(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on scheme error: " << result.Status);
                }
                return;
            case EStatus::Ok:
                if (auto securityObject = result.SecurityObject) {
                    auto token = Event->Get()->UserToken;
                    if (!token || !securityObject->CheckAccess(NACLib::EAccessRights::SelectRow, *token)) {
                        Reply(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for resource pool " << Event->Get()->PoolId);
                        return;
                    }
                }
                Reply(result.ResourcePoolInfo);
                return;
        }
    }

    void Handle(TEvPrivate::TEvCreatePoolResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Failed to create default pool " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            Reply(ev->Get()->Status, GroupIssues(ev->Get()->Issues, "Failed to create default pool"));
            return;
        }

        LOG_D("Successfully created default pool");
        Reply(Ydb::StatusIds::SUCCESS, NYql::TIssues());
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvPrivate::TEvCreatePoolResponse, Handle);
            default:
                StateFuncBase(ev);
        }
    }

protected:
    void StartRequest() override {
        LOG_D("Start pool fetching");
        auto event = NTableCreator::BuildSchemeCacheNavigateRequest(
            {{".resource_pools", Event->Get()->PoolId}},
            Event->Get()->Database,
            MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{})
        );
        event->ResultSet[0].Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode status, NYql::TIssue issue) override {
        Reply(status, {std::move(issue)});
    }

    TString LogPrefix() const override {
        return TStringBuilder() << "[TPoolFetcherActor] ActorId: " << SelfId() << ", Database: " << Event->Get()->Database << ", PoolId: " << Event->Get()->PoolId << ", SessionId: " << Event->Get()->SessionId;
    }

private:
    void CreateDefaultPool() const {
        LOG_D("Start default pool creation");

        NACLib::TDiffACL diffAcl;
        for (const TString& usedSid : AppData()->AdministrationAllowedSIDs) {
            diffAcl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::GenericFull, usedSid);
        }

        auto useAccess = NACLib::EAccessRights::SelectRow | NACLib::EAccessRights::DescribeSchema;
        for (const TString& usedSid : AppData()->DefaultUserSIDs) {
            diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, usedSid);
        }
        diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, AppData()->AllAuthenticatedUsers);
        diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, BUILTIN_ACL_ROOT);  // Used in case of DefaultUserSIDs is empty and AllAuthenticatedUsers is not specified

        auto token = MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{});
        Register(CreatePoolCreatorActor(SelfId(), Event->Get()->Database, Event->Get()->PoolId, PoolConfig, token, diffAcl));
    }

    void Reply(const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& poolInfo) {
        if (!poolInfo) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response");
            return;
        }

        const auto& properties = poolInfo->Description.GetProperties().GetProperties();
        for (auto& [property, value] : NResourcePool::GetPropertiesMap(PoolConfig)) {
            if (auto propertyIt = properties.find(property); propertyIt != properties.end()) {
                std::visit(NResourcePool::TSettingsParser{propertyIt->second}, value);
            }
        }

        Reply(Ydb::StatusIds::SUCCESS);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, {NYql::TIssue(message)});
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Pool info successfully fetched");
        } else {
            LOG_W("Failed to fetch pool info, " << status << ", issues: " << issues.ToOneLineString());
        }

        Issues.AddIssues(std::move(issues));
        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new TEvPrivate::TEvFetchPoolResponse(status, PoolConfig, std::move(Event), std::move(Issues)));
        PassAway();
    }

private:
    TEvPlaceRequestIntoPool::TPtr Event;
    bool UseDefaultPool = false;  // Skip access checks for default pool

    NResourcePool::TPoolSettings PoolConfig;
};


class TPoolCreatorActor : public TSchemeActorBase<TPoolCreatorActor> {
public:
    TPoolCreatorActor(const TActorId& replyActorId, const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, TIntrusiveConstPtr<NACLib::TUserToken> userToken, NACLibProto::TDiffACL diffAcl)
        : ReplyActorId(replyActorId)
        , Database(database)
        , PoolId(poolId)
        , UserToken(userToken)
        , DiffAcl(diffAcl)
        , PoolConfig(poolConfig)
    {}

    void DoBootstrap() {
        Become(&TPoolCreatorActor::StateFunc);
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto ssStatus = ev->Get()->Record.GetSchemeShardStatus();
        switch (ev->Get()->Status()) {
            case NTxProxy::TResultStatus::ExecComplete:
            case NTxProxy::TResultStatus::ExecAlready:
                if (ssStatus == NKikimrScheme::EStatus::StatusSuccess || ssStatus == NKikimrScheme::EStatus::StatusAlreadyExists) {
                    Reply(Ydb::StatusIds::SUCCESS);
                } else {
                    Reply(Ydb::StatusIds::SCHEME_ERROR, TStringBuilder() << "Invalid creation status: " << static_cast<NKikimrScheme::EStatus>(ssStatus));
                }
                return;
            case NTxProxy::TResultStatus::ExecInProgress:
            case NTxProxy::TResultStatus::ProxyShardNotAvailable:
                if (!ScheduleRetry(TStringBuilder() << "Retry shard unavailable error")) {
                    Reply(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on status: " << static_cast<NKikimrScheme::EStatus>(ssStatus));
                }
                return;
            default:
                Reply(Ydb::StatusIds::SCHEME_ERROR, TStringBuilder() << "Failed to create resource pool: " << static_cast<NKikimrScheme::EStatus>(ssStatus));
                return;
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle)
            default:
                StateFuncBase(ev);
        }
    }

protected:
    void StartRequest() override {
        LOG_D("Start pool creating");
        auto event = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

        BuildCreatePoolRequest(*event->Record.MutableTransaction()->MutableModifyScheme());
        BuildModifyAclRequest(*event->Record.MutableTransaction()->AddTransactionalModification());

        if (UserToken) {
            event->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        Send(MakeTxProxyID(), std::move(event));
    }

    void OnFatalError(Ydb::StatusIds::StatusCode status, NYql::TIssue issue) override {
        Reply(status, {std::move(issue)});
    }

    TString LogPrefix() const override {
        return TStringBuilder() << "[TPoolCreatorActor] ActorId: " << SelfId() << ", Database: " << Database << ", PoolId: " << PoolId << ", ";
    }

private:
    void BuildCreatePoolRequest(NKikimrSchemeOp::TModifyScheme& schemeTx) {
        schemeTx.SetWorkingDir(JoinPath({Database, ".resource_pools/"}));
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateResourcePool);
        schemeTx.SetInternal(true);
        schemeTx.SetAllowAccessToPrivatePaths(true);

        auto& poolDescription = *schemeTx.MutableCreateResourcePool();
        poolDescription.SetName(PoolId);
        for (auto& [property, value] : NResourcePool::GetPropertiesMap(PoolConfig)) {
            poolDescription.MutableProperties()->MutableProperties()->insert({
                property,
                std::visit(NResourcePool::TSettingsExtractor{}, value)
            });
        }
    }

    void BuildModifyAclRequest(NKikimrSchemeOp::TModifyScheme& schemeTx) const {
        schemeTx.SetWorkingDir(JoinPath({Database, ".resource_pools/"}));
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpModifyACL);
        schemeTx.SetInternal(true);
        schemeTx.SetAllowAccessToPrivatePaths(true);

        auto& modifyACL = *schemeTx.MutableModifyACL();
        modifyACL.SetName(PoolId);
        modifyACL.SetDiffACL(DiffAcl.SerializeAsString());
        if (UserToken) {
            modifyACL.SetNewOwner(UserToken->GetUserSID());
        }
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, {NYql::TIssue(message)});
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Pool successfully created");
        } else {
            LOG_W("Failed to create pool, " << status << ", issues: " << issues.ToOneLineString());
        }

        Issues.AddIssues(std::move(issues));
        Send(ReplyActorId, new TEvPrivate::TEvCreatePoolResponse(status, std::move(Issues)));
        PassAway();
    }

private:
    const TActorId ReplyActorId;
    const TString Database;
    const TString PoolId;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const NACLibProto::TDiffACL DiffAcl;
    NResourcePool::TPoolSettings PoolConfig;
};

}  // anonymous namespace

IActor* CreatePoolFetcherActor(TEvPlaceRequestIntoPool::TPtr event) {
    return new TPoolFetcherActor(std::move(event));
}

IActor* CreatePoolCreatorActor(const TActorId& replyActorId, const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, TIntrusiveConstPtr<NACLib::TUserToken> userToken, NACLibProto::TDiffACL diffAcl) {
    return new TPoolCreatorActor(replyActorId, database, poolId, poolConfig, userToken, diffAcl);
}

}  // NKikimr::NKqp::NWorkload
