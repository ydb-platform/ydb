#include "kqp_pq_topic_resolver.h"
#include "kqp_executer.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/guid.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp {

using namespace NActors;

namespace {

// ---------------------------------------------------------------------------
// Internal event: carries the result of a single DescribeFederatedTopic call.
// Sent by the async callback to the resolver actor itself.
// ---------------------------------------------------------------------------
enum EPqResolverPrivateEv {
    EvTopicDescribeResult = EventSpaceBegin(TEvents::ES_PRIVATE),
};

struct TEvTopicDescribeResult
    : public TEventLocal<TEvTopicDescribeResult, EvTopicDescribeResult>
{
    // On success: non-zero total partition count.
    ui32  PartitionsCount = 0;
    // On error:   PartitionsCount == 0 and ErrorMessage is set.
    TString ErrorMessage;

    explicit TEvTopicDescribeResult(ui32 partitionsCount)
        : PartitionsCount(partitionsCount) {}
    explicit TEvTopicDescribeResult(TString errorMessage)
        : ErrorMessage(std::move(errorMessage)) {}
};

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------
class TKqpPqTopicResolver : public TActorBootstrapped<TKqpPqTopicResolver> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_TABLE_RESOLVER; // reuse closest existing type
    }

    TKqpPqTopicResolver(
        const TActorId& owner,
        ui64 txId,
        TVector<TPqTopicResolverSource> sources,
        THashMap<TString, TString> secureParams,
        NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
        std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph> queryPhysicalGraph)
        : Owner(owner)
        , TxId(txId)
        , Sources(std::move(sources))
        , SecureParams(std::move(secureParams))
        , PqGatewayFactory(std::move(pqGatewayFactory))
        , QueryPhysicalGraph(std::move(queryPhysicalGraph))
    {}

    void Bootstrap() {
        if (Sources.empty()) {
            // Nothing to do — should not happen in practice, but handle gracefully.
            ReplyOkAndDie();
            return;
        }

        Pending = Sources.size();

        for (const auto& src : Sources) {
            DescribeTopic(src);
        }

        Become(&TKqpPqTopicResolver::WaitState);
    }

private:
    STATEFN(WaitState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTopicDescribeResult, HandleTopicDescribeResult);
            default: {
                YDB_LOG_CRIT("TKqpPqTopicResolver: unexpected event",
                    {"txId", TxId},
                    {"eventType", ev->GetTypeRewrite()});
            }
        }
    }

    void HandleTopicDescribeResult(TEvTopicDescribeResult::TPtr& ev) {
        if (!ev->Get()->ErrorMessage.empty()) {
            YDB_LOG_ERROR("Failed to describe PQ topic for partition count refresh",
                {"txId", TxId},
                {"error", ev->Get()->ErrorMessage});
            ReplyErrorAndDie(ev->Get()->ErrorMessage);
            return;
        }

        // Accumulate: for now preserve existing behaviour — the last resolved
        // partition count wins (single-topic streaming queries are the common case).
        LastPartitionsCount = ev->Get()->PartitionsCount;

        if (--Pending == 0) {
            PatchAndDie();
        }
    }

private:
    void DescribeTopic(const TPqTopicResolverSource& src) {
        const TString token = [&] {
            auto it = SecureParams.find(src.TokenName);
            return it != SecureParams.end() ? it->second : TString{};
        }();

        const TString sessionId = CreateGuidAsString();
        auto gateway = PqGatewayFactory->CreatePqGateway();

        NYql::TPqClusterConfig clusterConfig;
        clusterConfig.SetName(src.Cluster);
        clusterConfig.SetClusterType(NYql::TPqClusterConfig::CT_DATA_STREAMS);
        clusterConfig.SetEndpoint(src.Endpoint);
        clusterConfig.SetToken(token);
        clusterConfig.SetDatabase(src.DatabaseForClusterConfig);
        clusterConfig.SetUseSsl(src.UseSsl);
        gateway->AddCluster(clusterConfig);
        gateway->OpenSession(sessionId, "username");

        YDB_LOG_DEBUG("Describing PQ topic",
            {"txId", TxId},
            {"cluster", src.Cluster},
            {"database", src.Database},
            {"topicPath", src.TopicPath},
            {"sessionId", sessionId});

        gateway->DescribeFederatedTopic(sessionId, src.Cluster, src.Database, src.TopicPath, token)
            .Subscribe(
                [actorSystem = TActivationContext::ActorSystem(),
                 selfId = SelfId(),
                 gateway = gateway](const auto& future)
                {
                    try {
                        const auto& result = future.GetValue();
                        ui32 totalPartitions = 0;
                        for (const auto& clusterInfo : result) {
                            totalPartitions += clusterInfo.PartitionsCount;
                        }
                        actorSystem->Send(selfId,
                            new TEvTopicDescribeResult(totalPartitions));
                    } catch (const std::exception& ex) {
                        actorSystem->Send(selfId,
                            new TEvTopicDescribeResult(TString(ex.what())));
                    }
                });
    }

    // Patch all PQ ReadRanges in the physical graph with the resolved partition count,
    // then notify the owner that we're done.
    void PatchAndDie() {
        if (QueryPhysicalGraph && LastPartitionsCount > 0) {
            for (int taskIdx = 0;
                 taskIdx < static_cast<int>(QueryPhysicalGraph->TasksSize());
                 ++taskIdx)
            {
                auto* task = QueryPhysicalGraph->MutableTasks(taskIdx);
                auto* dqTask = task->MutableDqTask();
                for (int rangeIdx = 0;
                     rangeIdx < static_cast<int>(dqTask->ReadRangesSize());
                     ++rangeIdx)
                {
                    NYql::NPq::NProto::TDqReadTaskParams params;
                    if (!params.ParseFromString(dqTask->GetReadRanges(rangeIdx))) {
                        continue;
                    }
                    if (params.PartitioningParamsSize() == 0) {
                        continue;
                    }
                    for (int ppIdx = 0;
                         ppIdx < static_cast<int>(params.PartitioningParamsSize());
                         ++ppIdx)
                    {
                        params.MutablePartitioningParams(ppIdx)
                            ->SetTopicPartitionsCount(LastPartitionsCount);
                    }
                    dqTask->SetReadRanges(rangeIdx, params.SerializeAsString());
                }
            }
        }

        ReplyOkAndDie();
    }

    void ReplyOkAndDie() {
        Send(Owner, new TEvKqpExecuter::TEvPqTopicResolveStatus());
        PassAway();
    }

    void ReplyErrorAndDie(const TString& errorMessage) {
        auto* ev = new TEvKqpExecuter::TEvPqTopicResolveStatus();
        ev->Status = Ydb::StatusIds::SCHEME_ERROR;
        ev->Issues.AddIssue(NYql::YqlIssue(
            {}, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
            TStringBuilder() << "Failed to describe topic for partition count refresh: "
                             << errorMessage));
        Send(Owner, ev);
        PassAway();
    }

private:
    const TActorId Owner;
    const ui64 TxId;
    TVector<TPqTopicResolverSource> Sources;
    THashMap<TString, TString> SecureParams;
    NYql::IPqGatewayFactory::TPtr PqGatewayFactory;
    std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph> QueryPhysicalGraph;

    ui32 Pending = 0;
    ui32 LastPartitionsCount = 0;
};

} // anonymous namespace

NActors::IActor* CreateKqpPqTopicResolver(
    const NActors::TActorId& owner,
    ui64 txId,
    TVector<TPqTopicResolverSource> sources,
    THashMap<TString, TString> secureParams,
    NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
    std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph> queryPhysicalGraph)
{
    return new TKqpPqTopicResolver(
        owner, txId,
        std::move(sources),
        std::move(secureParams),
        std::move(pqGatewayFactory),
        std::move(queryPhysicalGraph));
}

} // namespace NKikimr::NKqp
