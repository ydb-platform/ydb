#include "create_topic_operation.h"
#include "schema_operation.h"
#include "schema_propose.h"
#include "check_dlq_topics.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/cluster_tracker/cluster_tracker.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

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
        Send(ParentId, new TEvSchemaResponse(Settings.Strategy->GetTopicName(), Ydb::StatusIds::INTERNAL_ERROR, exc.what(), NKikimrSchemeOp::TModifyScheme()), 0, Settings.Cookie);
    }

private:
    void DoGetClustersList() {
        YDB_LOG_DEBUG("DoGetClustersList",
            {"logPrefix", NPQ_LOG_PREFIX});
        Become(&TCreateTopicOperationActor::GetClustersListState);
        Send(NPQ::NClusterTracker::MakeClusterTrackerID(), new NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersList());
    }

    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse::TPtr& ev) {
        YDB_LOG_DEBUG("Handle NPQ::NClusterTracker::TEvClusterTracker::TEvGetClustersListResponse",
            {"logPrefix", NPQ_LOG_PREFIX});

        auto& response = *ev->Get();
        if (response.Success) {
            ClustersList = std::move(response.ClustersList);
        }

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
        YDB_LOG_DEBUG("DoCreate",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"ifNotExists", Settings.IfNotExists});
        Become(&TCreateTopicOperationActor::CreateState);

        auto database = CanonizePath(Settings.Database);
        // Federation create still expects the original legacy name so ForFederation can
        // extract DC/producer metadata. ResolveName is only for FCC (modern + legacy → path).
        TString path;
        if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            auto resolved = NNameResolver::ResolveName(database, Settings.Strategy->GetTopicName());
            if (!resolved) {
                return ReplyAndDie(Ydb::StatusIds::BAD_REQUEST, TString{resolved.error()});
            }
            path = std::move(resolved->Path);
        } else {
            path = NormalizePath(database, CanonizePath(Settings.Strategy->GetTopicName()));
        }

        auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

        proposal->Record.SetDatabaseName(database);
        proposal->Record.SetPeerName(Settings.PeerName);
        if (Settings.UserToken) {
            proposal->Record.SetUserToken(Settings.UserToken->GetSerializedToken());
        }

        auto [workingDir, name] = GetWorkingDirAndName(path);
        if (workingDir.empty()) {
            return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, "Wrong topic name");
        }

        NKikimrSchemeOp::TModifyScheme& modifyScheme = *proposal->Record.MutableTransaction()->MutableModifyScheme();

        auto result = ProposeCreateTopic(modifyScheme, TProposeCreateTopicSettings{
            .Database = std::move(database),
            .WorkingDir = workingDir,
            .Name = name,
            .ClustersList = ClustersList,
            .Strategy = Settings.Strategy.get(),
            .IfNotExists = Settings.IfNotExists,
        });

        if (!result) {
            return ReplyAndDie(result.GetStatus(), std::move(result.GetErrorMessage()));
        }

        ModifyScheme = modifyScheme;
        TopicPath = path;
        Proposal = std::move(proposal);
        return DoCheckDlqOrPropose();
    }

    void Handle(TEvSchemaOperationResponse::TPtr& ev) {
        YDB_LOG_DEBUG("Handle TEvSchemaOperationResponse",
            {"logPrefix", NPQ_LOG_PREFIX});
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
    void DoCheckDlqOrPropose() {
        const NKikimrPQ::TPQTabletConfig emptyOldConfig;
        if (auto* actor = CreateCheckDlqTopicsActorIfNeeded(
                SelfId(),
                CanonizePath(Settings.Database),
                ModifyScheme.GetCreatePersQueueGroup().GetPQTabletConfig(),
                emptyOldConfig,
                TCheckDlqTopicsSettings{
                    .UserToken = Settings.UserToken
                }))
        {
            Become(&TCreateTopicOperationActor::CheckDlqState);
            RegisterWithSameMailbox(actor);
            return;
        }
        return DoProposeOrReply();
    }

    void Handle(TEvCheckDlqTopicsResponse::TPtr& ev) {
        YDB_LOG_DEBUG("Handle TEvCheckDlqTopicsResponse",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"status", ev->Get()->Status},
            {"errorMessage", ev->Get()->ErrorMessage});
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyAndDie(ev->Get()->Status, std::move(ev->Get()->ErrorMessage));
        }
        return DoProposeOrReply();
    }

    STFUNC(CheckDlqState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvCheckDlqTopicsResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void DoProposeOrReply() {
        if (Settings.PrepareOnly) {
            return ReplyAndDie(Ydb::StatusIds::SUCCESS, "");
        }
        RegisterWithSameMailbox(CreateSchemaOperation(
            SelfId(),
            TopicPath,
            std::move(Proposal),
            Settings.Cookie
        ));
        Become(&TCreateTopicOperationActor::CreateState);
    }

private:
    void ReplyAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
        YDB_LOG_DEBUG("ReplyAndDie",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"errorCode", errorCode},
            {"errorMessage", errorMessage});
        if ((errorCode == Ydb::StatusIds::SUCCESS || errorCode == Ydb::StatusIds::ALREADY_EXISTS) && !Settings.PrepareOnly) {
            ModifyScheme = {};
        }
        if (Settings.IfNotExists && errorCode == Ydb::StatusIds::ALREADY_EXISTS) {
            errorCode = Ydb::StatusIds::SUCCESS;
            errorMessage = "";
        }
        Send(ParentId, new TEvSchemaResponse(Settings.Strategy->GetTopicName(), errorCode, std::move(errorMessage), std::move(ModifyScheme)), 0, Settings.Cookie);
        PassAway();
    }

private:
    const TActorId ParentId;
    const TCreateTopicOperationSettings Settings;

    TString TopicPath;
    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> Proposal;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
    NPQ::NClusterTracker::TClustersList::TConstPtr ClustersList;
};

}

TResult ProposeCreateTopic(NKikimrSchemeOp::TModifyScheme& modifyScheme, TProposeCreateTopicSettings&& settings) {
    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
    modifyScheme.SetWorkingDir(settings.WorkingDir);
    modifyScheme.SetFailedOnAlreadyExists(!settings.IfNotExists);

    auto* config = modifyScheme.MutableCreatePersQueueGroup();
    config->SetName(settings.Name);

    auto result = settings.Strategy->ApplyChanges(
        GetLocalClusterName(settings.ClustersList),
        settings.Database,
        modifyScheme,
        *config
    );
    if (result) {
        result = ValidateConfig(config->GetPQTabletConfig(), EOperation::Create);
    }
    if (result) {
        result = ValidateLocalCluster(settings.ClustersList, config->GetPQTabletConfig());
    }
    return result;
}

IActor* CreateCreateTopicOperationActor(TActorId parentId, TCreateTopicOperationSettings&& settings) {
    return new TCreateTopicOperationActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
