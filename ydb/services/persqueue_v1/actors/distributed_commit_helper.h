#pragma once

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

class TDistributedCommitHelper {
public:
    enum ECurrentStep {
        BEGIN_TRANSACTION_SENDED,
        CHECK_GENERATION,
        OFFSETS_SENDED,
        COMMIT_SENDED,
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
        i64 GenerationId;
        TString ResourceDatabasePath;
    };

    TDistributedCommitHelper(TString database, TString consumer, std::vector<TCommitInfo> commits, ui64 cookie = 0,
                             std::optional<GenerationIdCheckerSettings> generationCheckerSettings = std::nullopt);

    std::pair<TDistributedCommitHelper::ECurrentStep, bool> Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void SendCreateSessionRequest(const TActorContext& ctx);
    void BeginTransaction(const NActors::TActorContext& ctx);
    bool Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);

private:
    void CloseKqpSession(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest();
    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();
    void RetrieveGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);
    bool CompareGenerations(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);
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

    const TString CHECK_GROUP_GENERATION_ID = R"sql(
        --!syntax_v1
        DECLARE $ConsumerGroup AS Utf8;
        DECLARE $Database AS Utf8;

        SELECT state, generation, last_success_generation
        FROM `%s`
        VIEW PRIMARY KEY
        WHERE database = $Database
        AND consumer_group = $ConsumerGroup;
    )sql";
};

}  // namespace NKikimr::NGRpcProxy::V1
