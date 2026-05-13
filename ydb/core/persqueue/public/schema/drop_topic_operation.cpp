#include "common.h"
#include "drop_topic_operation.h"
#include "schema_operation.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NPQ::NSchema {

namespace {

class TDropTopicOperationActor: public TBaseActor<TDropTopicOperationActor>
                              , public TConstantLogPrefix {
public:
    TDropTopicOperationActor(NActors::TActorId parentId, TDropTopicOperationSettings&& settings)
        : TBaseActor<TDropTopicOperationActor>(NKikimrServices::EServiceKikimr::PQ_SCHEMA)
        , ParentId(parentId)
        , Settings(std::move(settings))
    {
    }

    ~TDropTopicOperationActor() = default;

    void Bootstrap() {
        DoDescribe();
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << ParentId << "[" << Settings.Path << "] ";
    }

    void OnException(const std::exception& exc) override {
        ReplyAndDie(Ydb::StatusIds::INTERNAL_ERROR, exc.what());
    }

private:
    void DoDescribe() {
        LOG_D("DoDescribe");
        Become(&TDropTopicOperationActor::DescribeState);

        RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
            SelfId(),
            Settings.Database,
            { Settings.Path },
            {
                .UserToken = Settings.UserToken,
                .AccessRights = NACLib::EAccessRights::RemoveSchema,
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
                if (TopicInfo.CdcStream) {
                    return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Path, NDescriber::EStatus::NOT_FOUND));
                }
                return DoDrop();
            }
            case NDescriber::EStatus::NOT_FOUND: {
                if (Settings.IfExists) {
                    return ReplyAndDie(Ydb::StatusIds::SUCCESS, "");
                }
                return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Path, NDescriber::EStatus::NOT_FOUND));
            }
            case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS: {
                return ReplyAndDie(Ydb::StatusIds::UNAUTHORIZED, NDescriber::Description(Settings.Path, TopicInfo.Status));
            }
            default: {
                return ReplyAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Path, TopicInfo.Status));
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
    void DoDrop() {
        LOG_D("DoDrop");

        Become(&TDropTopicOperationActor::DropState);

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

        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetAllowAccessToPrivatePaths(false);

        auto* config = modifyScheme.MutableDrop();
        config->SetName(TopicInfo.Info->Description.name());

        RegisterWithSameMailbox(CreateSchemaOperation(
            SelfId(),
            TopicInfo.RealPath,
            std::move(proposal),
            Settings.Cookie
        ));
    }

    void Handle(TEvSchemaOperationResponse::TPtr& ev) {
        LOG_D("Handle TEvSchemaOperationResponse");
        auto& response = *ev->Get();
        return ReplyAndDie(response.Status, std::move(response.ErrorMessage));
    }

    STFUNC(DropState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSchemaOperationResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void ReplyAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
        Send(ParentId, new TEvDropTopicResponse(errorCode, std::move(errorMessage)), 0, Settings.Cookie);
        PassAway();
    }

private:
    const TActorId ParentId;
    const TDropTopicOperationSettings Settings;

    NDescriber::TTopicInfo TopicInfo;
};

} // namespace

IActor* CreateDropTopicOperationActor(NActors::TActorId parentId, TDropTopicOperationSettings&& settings) {
    return new TDropTopicOperationActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
