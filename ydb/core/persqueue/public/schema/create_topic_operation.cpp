#include "create_topic_operation.h"
#include "schema_operation.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/cluster_tracker/cluster_tracker.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>

namespace NKikimr::NPQ::NSchema {

namespace {

class TCreateTopicOperationActor: public TBaseActor<TCreateTopicOperationActor>
                               , public TConstantLogPrefix {
public:
    TCreateTopicOperationActor(TActorId parentId, TCreateTopicOperationSettings&& settings)
        : TBaseActor<TCreateTopicOperationActor>(NKikimrServices::EServiceKikimr::PQ_SCHEMA)
        , ParentId(parentId)
        , Settings(std::move(settings))
    {
    }

    ~TCreateTopicOperationActor() = default;

    void Bootstrap() {
        if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            return DoCreate();
        } else {
            return DoGetClustersList();
        }
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[" << Settings.Strategy->GetTopicName() << "] ";
    }

    void OnException(const std::exception& exc) override {
        ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR, exc.what());
    }

private:
    void DoGetClustersList() {
        LOG_D("DoGetClustersList");
        Become(&TCreateTopicOperationActor::GetClustersListState);
        Send(NPQ::NClusterTracker::MakeClusterTrackerID(), new NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersList());
    }

    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse::TPtr& ev) {
        LOG_D("Handle NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse");

        auto& response = *ev->Get();
        if (!response.Success) {
            return ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Failed to get clusters list");
        }

        ClustersList = std::move(response.ClustersList);

        return DoCreate();
    }

    STFUNC(GetClustersListState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void DoCreate() {
        LOG_D("DoCreate");
        Become(&TCreateTopicOperationActor::CreateState);

        auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

        proposal->Record.SetDatabaseName(Settings.Database);
        proposal->Record.SetPeerName(Settings.PeerName);
        if (Settings.UserToken) {
            proposal->Record.SetUserToken(Settings.UserToken->GetSerializedToken());
        }

        auto path = NormalizePath(Settings.Database, Settings.Strategy->GetTopicName());

        NKikimrSchemeOp::TModifyScheme& modifyScheme = *proposal->Record.MutableTransaction()->MutableModifyScheme();

        auto [workingDir, name] = GetWorkingDirAndName(path);
        if (workingDir.empty()) {
            return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, "Wrong topic name");
        }

        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
        modifyScheme.SetWorkingDir(workingDir);

        auto* config = modifyScheme.MutableCreatePersQueueGroup();
        config->SetName(name);

        auto result = Settings.Strategy->ApplyChanges(
            GetLocalClusterName(ClustersList),
            Settings.Database,
            modifyScheme,
            *config
        );
        if (result) {
            result = ValidateConfig(config->GetPQTabletConfig(), EOperation::Create);
        }
        if (result) {
            result = ValidateLocalCluster(ClustersList, config->GetPQTabletConfig());
        }

        if (!result) {
            return ReplyAndDie(result.GetStatus(), std::move(result.GetErrorMessage()));
        }

        ModifyScheme = modifyScheme;

        RegisterWithSameMailbox(CreateSchemaOperation(
            SelfId(),
            path,
            std::move(proposal),
            Settings.Cookie
        ));
    }

    void Handle(TEvSchemaOperationResponse::TPtr& ev) {
        LOG_D("Handle TEvSchemaOperationResponse");
        auto& response = *ev->Get();
        return ReplyAndDie(response.Status, std::move(response.ErrorMessage));
    }

    STFUNC(CreateState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSchemaOperationResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void ReplyAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
        LOG_D("ReplyAndDie " << errorCode << " '" << errorMessage << "'");
        if (errorCode == Ydb::StatusIds::SUCCESS) {
            ModifyScheme = {};
        }
        Send(ParentId, new TEvCreateTopicResponse(errorCode, std::move(errorMessage), std::move(ModifyScheme)), 0, Settings.Cookie);
        PassAway();
    }

private:
    const TActorId ParentId;
    const TCreateTopicOperationSettings Settings;

    NKikimrSchemeOp::TModifyScheme ModifyScheme;
    NPQ::NClusterTracker::TClustersList::TConstPtr ClustersList;
};

}

IActor* CreateCreateTopicOperationActor(TActorId parentId, TCreateTopicOperationSettings&& settings) {
    return new TCreateTopicOperationActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
