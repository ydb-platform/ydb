#pragma once

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/persqueue/events/global.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

extern const TString CHECK_GROUP_GENERATION_ID;

class TDistributedCommitHelper {
public:
    enum ECurrentStep {
        BEGIN_TRANSACTION_SENDED,
        CHECK_GENERATION,
        OFFSETS_SENDED,
        COMMIT_SENDED,
        /// Split metadata / topic DB (serverless): commit generation-check tx on shared DB, then topic ops on topic DB.
        METADATA_COMMIT_AWAIT,
        TOPIC_AWAIT_BEGIN_TX,
        DONE
    };

    struct TCommitInfo {
        ui64 PartitionId;
        i64 Offset;
        bool KillReadSession;
        bool OnlyCheckCommitedToFinish;
        TString ReadSessionId;
        TString TopicPath;
    };

    struct GenerationIdCheckerSettings {
        ui64 GenerationId;
        TString TopicDatabasePath;
        TString ConsumerMetadataTablePath;
    };

    TDistributedCommitHelper(TString database, TString consumer, std::vector<TCommitInfo> commits, ui64 cookie = 0,
                             std::optional<GenerationIdCheckerSettings> generationCheckerSettings = std::nullopt);

    TDistributedCommitHelper::ECurrentStep Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void SendCreateSessionRequest(const TActorContext& ctx);
    void BeginTransaction(const NActors::TActorContext& ctx);
    bool Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void CloseKqpSession(const TActorContext& ctx);

private:
    const TString& RequestDatabase() const;
    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest();
    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();
    void RetrieveGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void CompareGenerations(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void SendCommits(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void CommitTx(const NActors::TActorContext& ctx);

private:
    TString DataBase;
    TString Consumer;
    std::vector<TCommitInfo> Commits;
    ECurrentStep Step;
    ui64 Cookie;

    TString TxId;
    TString KqpSessionId;
    std::optional<GenerationIdCheckerSettings> CheckerSettings;
    /// When true, generation was checked in `DataBase` (shared); topic commits use `CheckerSettings->TopicDatabasePath`.
    bool SplitGenerationCheckFromTopicCommit = false;
    bool TopicCommitPhaseActive = false;
};

}  // namespace NKikimr::NGRpcProxy::V1
