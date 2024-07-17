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


class TPoolResolverActor : public TActorBootstrapped<TPoolResolverActor> {
public:
    TPoolResolverActor(TEvPlaceRequestIntoPool::TPtr event, bool defaultPoolExists)
        : Event(std::move(event))
    {
        if (!Event->Get()->PoolId) {
            Event->Get()->PoolId = NResourcePool::DEFAULT_POOL_ID;
        }
        CanCreatePool = Event->Get()->PoolId == NResourcePool::DEFAULT_POOL_ID && !defaultPoolExists;
    }

    void Bootstrap() {
        Become(&TPoolResolverActor::StateFunc);
        StartPoolFetchRequest();
    }

    void StartPoolFetchRequest() const {
        LOG_D("Start pool fetching");
        Register(CreatePoolFetcherActor(SelfId(), Event->Get()->Database, Event->Get()->PoolId, Event->Get()->UserToken));
    }

    void Handle(TEvPrivate::TEvFetchPoolResponse::TPtr& ev) {
        if (ev->Get()->Status == Ydb::StatusIds::NOT_FOUND && CanCreatePool) {
            CanCreatePool = false;
            StartCreateDefaultPoolRequest();
            return;
        }

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Failed to fetch pool info " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            NYql::TIssues issues = GroupIssues(ev->Get()->Issues, TStringBuilder() << "Failed to resolve pool id " << Event->Get()->PoolId);
            Reply(ev->Get()->Status, std::move(issues));
            return;
        }

        Reply(ev->Get()->PoolConfig, ev->Get()->PathId);
    }

    void StartCreateDefaultPoolRequest() const {
        LOG_I("Start default pool creation");

        NACLib::TDiffACL diffAcl;
        for (const TString& usedSid : AppData()->AdministrationAllowedSIDs) {
            diffAcl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::GenericFull, usedSid);
        }
        diffAcl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::SelectRow | NACLib::EAccessRights::DescribeSchema, AppData()->AllAuthenticatedUsers);

        auto token = MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{});
        Register(CreatePoolCreatorActor(SelfId(), Event->Get()->Database, Event->Get()->PoolId, NResourcePool::TPoolSettings(), token, diffAcl));
    }

    void Handle(TEvPrivate::TEvCreatePoolResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Failed to create default pool " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            Reply(ev->Get()->Status, GroupIssues(ev->Get()->Issues, "Failed to create default pool"));
            return;
        }

        LOG_D("Successfully created default pool");
        DefaultPoolCreated = true;
        StartPoolFetchRequest();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvFetchPoolResponse, Handle);
        hFunc(TEvPrivate::TEvCreatePoolResponse, Handle);
    )

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TPoolResolverActor] ActorId: " << SelfId() << ", Database: " << Event->Get()->Database << ", PoolId: " << Event->Get()->PoolId << ", SessionId: " << Event->Get()->SessionId << ", ";
    }

    void Reply(NResourcePool::TPoolSettings poolConfig, TPathId pathId) {
        LOG_D("Pool info successfully resolved");

        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new TEvPrivate::TEvResolvePoolResponse(Ydb::StatusIds::SUCCESS, poolConfig, pathId, DefaultPoolCreated, std::move(Event)));
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        LOG_W("Failed to resolve pool, " << status << ", issues: " << issues.ToOneLineString());

        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new TEvPrivate::TEvResolvePoolResponse(status, {}, {}, DefaultPoolCreated, std::move(Event), std::move(issues)));
        PassAway();
    }

private:
    TEvPlaceRequestIntoPool::TPtr Event;
    bool CanCreatePool = false;
    bool DefaultPoolCreated = false;
};


class TPoolFetcherActor : public TSchemeActorBase<TPoolFetcherActor> {
public:
    TPoolFetcherActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : ReplyActorId(replyActorId)
        , Database(database)
        , PoolId(poolId)
        , UserToken(userToken)
    {}

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
                Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid resource pool id " << PoolId);
                return;
            case EStatus::AccessDenied:
                Reply(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for resource pool " << PoolId);
                return;
            case EStatus::RootUnknown:
            case EStatus::PathErrorUnknown:
                Reply(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Resource pool " << PoolId << " not found or you don't have access permissions");
                return;
            case EStatus::LookupError:
            case EStatus::TableCreationNotComplete:
                if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                    Reply(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on scheme error: " << result.Status);
                }
                return;
            case EStatus::Ok:
                Reply(result.ResourcePoolInfo);
                return;
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

protected:
    void StartRequest() override {
        LOG_D("Start pool fetching");
        auto event = NTableCreator::BuildSchemeCacheNavigateRequest(
            {{".resource_pools", PoolId}},
            Database,
            UserToken
        );
        event->ResultSet[0].Access |= NACLib::SelectRow;
        event->ResultSet[0].Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode status, NYql::TIssue issue) override {
        Reply(status, {std::move(issue)});
    }

    TString LogPrefix() const override {
        return TStringBuilder() << "[TPoolFetcherActor] ActorId: " << SelfId() << ", Database: " << Database << ", PoolId: " << PoolId << ", ";
    }

private:
    void Reply(const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& poolInfo) {
        if (!poolInfo) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response");
            return;
        }

        PathId = poolInfo->Description.GetPathId();
        ParsePoolSettings(poolInfo->Description, PoolConfig);

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
        Send(ReplyActorId, new TEvPrivate::TEvFetchPoolResponse(status, PoolConfig, PathIdFromPathId(PathId), std::move(Issues)));
        PassAway();
    }

private:
    const TActorId ReplyActorId;
    const TString Database;
    const TString PoolId;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;

    NResourcePool::TPoolSettings PoolConfig;
    NKikimrProto::TPathID PathId;
};


class TPoolCreatorActor : public TSchemeActorBase<TPoolCreatorActor> {
    using TBase = TSchemeActorBase<TPoolCreatorActor>;

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
            case NTxProxy::TResultStatus::ExecError:
                if (ssStatus == NKikimrScheme::EStatus::StatusMultipleModifications || ssStatus == NKikimrScheme::EStatus::StatusInvalidParameter) {
                    ScheduleRetry(ssStatus, "Retry execution error", true);
                } else {
                    Reply(Ydb::StatusIds::SCHEME_ERROR, TStringBuilder() << "Execution error: " << static_cast<NKikimrScheme::EStatus>(ssStatus));
                }
                return;
            case NTxProxy::TResultStatus::ExecInProgress:
                ScheduleRetry(ssStatus, "Retry execution in progress error", true);
                return;
            case NTxProxy::TResultStatus::ProxyShardNotAvailable:
                ScheduleRetry(ssStatus, "Retry shard unavailable error");
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

        auto& schemeTx = *event->Record.MutableTransaction()->MutableModifyScheme();
        schemeTx.SetWorkingDir(JoinPath({Database, ".resource_pools"}));
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateResourcePool);
        schemeTx.SetInternal(true);
        schemeTx.SetAllowAccessToPrivatePaths(true);

        BuildCreatePoolRequest(*schemeTx.MutableCreateResourcePool());
        BuildModifyAclRequest(*schemeTx.MutableModifyACL());

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
    void ScheduleRetry(ui32 status, const TString& message, bool longDelay = false) {
        auto ssStatus = static_cast<NKikimrScheme::EStatus>(status);
        if (!TBase::ScheduleRetry(TStringBuilder() << message << ", status: " << ssStatus, longDelay)) {
            Reply(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on status: " << ssStatus);
        }
    }

    void BuildCreatePoolRequest(NKikimrSchemeOp::TResourcePoolDescription& poolDescription) {
        poolDescription.SetName(PoolId);
        for (auto& [property, value] : NResourcePool::GetPropertiesMap(PoolConfig)) {
            poolDescription.MutableProperties()->MutableProperties()->insert({
                property,
                std::visit(NResourcePool::TSettingsExtractor{}, value)
            });
        }
    }

    void BuildModifyAclRequest(NKikimrSchemeOp::TModifyACL& modifyACL) const {
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

IActor* CreatePoolResolverActor(TEvPlaceRequestIntoPool::TPtr event, bool defaultPoolExists) {
    return new TPoolResolverActor(std::move(event), defaultPoolExists);
}

IActor* CreatePoolFetcherActor(const TActorId& replyActorId, const TString& database, const TString& poolId, TIntrusiveConstPtr<NACLib::TUserToken> userToken) {
    return new TPoolFetcherActor(replyActorId, database, poolId, userToken);
}

IActor* CreatePoolCreatorActor(const TActorId& replyActorId, const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, TIntrusiveConstPtr<NACLib::TUserToken> userToken, NACLibProto::TDiffACL diffAcl) {
    return new TPoolCreatorActor(replyActorId, database, poolId, poolConfig, userToken, diffAcl);
}

}  // NKikimr::NKqp::NWorkload
