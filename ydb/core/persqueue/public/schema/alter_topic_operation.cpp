#include "alter_topic_operation.h"
#include "schema_operation.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>

namespace NKikimr::NPQ::NSchema {

namespace {

class TAlterTopicOperationActor: public TBaseActor<TAlterTopicOperationActor>
                               , public TConstantLogPrefix {
public:
    TAlterTopicOperationActor(TActorId parentId, TAlterTopicOperationSettings&& settings)
        : TBaseActor<TAlterTopicOperationActor>(NKikimrServices::EServiceKikimr::PQ_SCHEMA)
        , ParentId(parentId)
        , Settings(std::move(settings))
    {
    }

    ~TAlterTopicOperationActor() = default;

    void Bootstrap() {
        DoDescribe();
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[" << Settings.Strategy->GetTopicName() << "] ";
    }

    void OnException(const std::exception& exc) override {
        ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR, exc.what());
    }

private:
    void DoDescribe() {
        LOG_D("DoDescribe");
        Become(&TAlterTopicOperationActor::DescribeState);

        RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
            SelfId(),
            Settings.Database,
            { Settings.Strategy->GetTopicName() },
            {
                .UserToken = Settings.UserToken,
                .AccessRights = NACLib::EAccessRights::AlterSchema,
                .ForceSyncVersion = true
            }));
    }

    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
        LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

        auto& topics = ev->Get()->Topics;
        AFL_ENSURE(topics.size() == 1)("s", topics.size());

        TopicInfo = std::move(topics.begin()->second);
        switch(TopicInfo.Status) {
            case NDescriber::EStatus::SUCCESS: {
                if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
                    return DoAlter();
                } else {
                    return DoGetClustersList();
                }
            }
            case NDescriber::EStatus::NOT_FOUND: {
                if (Settings.IfExists) {
                    return ReplyAndDie(Ydb::StatusIds::SUCCESS, "");
                }
                return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Strategy->GetTopicName(), NDescriber::EStatus::NOT_FOUND));
            }
            case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS: {
                return ReplyAndDie(Ydb::StatusIds::UNAUTHORIZED, NDescriber::Description(Settings.Strategy->GetTopicName(), TopicInfo.Status));
            }
            default: {
                return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Strategy->GetTopicName(), TopicInfo.Status));
            }
        }
    }

    STFUNC(DescribeState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void DoGetClustersList() {
        LOG_D("DoGetClustersList");
        Become(&TAlterTopicOperationActor::GetClustersListState);
        Send(NPQ::NClusterTracker::MakeClusterTrackerID(), new NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersList());
    }

    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse::TPtr& ev) {
        LOG_D("Handle NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse: "
            << (ev->Get()->Success ? ev->Get()->ClustersList->DebugString() : "error"));

        auto& response = *ev->Get();
        if (!response.Success) {
            return ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Failed to get clusters list");
        }
        ClustersList = std::move(response.ClustersList);

        return DoAlter();
    }

    STFUNC(GetClustersListState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void DoAlter() {
        LOG_D("DoAlter");

        Become(&TAlterTopicOperationActor::AlterState);

        auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

        proposal->Record.SetDatabaseName(Settings.Database);
        proposal->Record.SetPeerName(Settings.PeerName);
        if (Settings.UserToken) {
            proposal->Record.SetUserToken(Settings.UserToken->GetSerializedToken());
        }

        NKikimrSchemeOp::TModifyScheme& modifyScheme = *proposal->Record.MutableTransaction()->MutableModifyScheme();

        auto [workingDir, _] = GetWorkingDirAndName(TopicInfo.RealPath);
        if (workingDir.empty()) {
            return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, "Wrong topic name");
        }

        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetAllowAccessToPrivatePaths(true);

        auto* config = modifyScheme.MutableAlterPersQueueGroup();

        {
            auto applyIf = modifyScheme.AddApplyIf();
            applyIf->SetPathId(TopicInfo.Self->Info.GetPathId());
            applyIf->SetPathVersion(TopicInfo.Self->Info.GetPathVersion());
        }

        auto result = Settings.Strategy->ApplyChanges(
            GetLocalClusterName(ClustersList),
            TopicInfo,
            modifyScheme,
            *config,
            TopicInfo.Info->Description
        );
        if (result) {
            result = ValidateConfig(config->GetPQTabletConfig(), EOperation::Alter);
        }

        if (!result) {
            return ReplyAndDie(result.GetStatus(), std::move(result.GetErrorMessage()));
        }

        ModifyScheme = modifyScheme;

        if (Settings.PrepareOnly) {
            return ReplyAndDie(Ydb::StatusIds::SUCCESS, "");
        } else {
            RegisterWithSameMailbox(CreateSchemaOperation(
                SelfId(),
                TopicInfo.RealPath,
                std::move(proposal),
                Settings.Cookie
            ));
        }
    }

    void Handle(TEvSchemaOperationResponse::TPtr& ev) {
        LOG_D("Handle TEvSchemaOperationResponse");
        auto& response = *ev->Get();
        return ReplyAndDie(response.Status, std::move(response.ErrorMessage));
    }

    STFUNC(AlterState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSchemaOperationResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void ReplyAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
        LOG_D("ReplyAndDie " << errorCode << " '" << errorMessage << "'");
        if (errorCode == Ydb::StatusIds::SUCCESS && !Settings.PrepareOnly) {
            ModifyScheme = {};
        }
        Send(ParentId, new TEvAlterTopicResponse(errorCode, std::move(errorMessage), std::move(ModifyScheme)), 0, Settings.Cookie);
        PassAway();
    }

private:
    const TActorId ParentId;
    const TAlterTopicOperationSettings Settings;

    NDescriber::TTopicInfo TopicInfo;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
    NPQ::NClusterTracker::TClustersList::TConstPtr ClustersList;
};

}

IActor* CreateAlterTopicOperationActor(TActorId parentId, TAlterTopicOperationSettings&& settings) {
    return new TAlterTopicOperationActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
