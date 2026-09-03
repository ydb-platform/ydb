#include "kqp_pq_topic_resolver.h"
#include "kqp_executer.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/guid.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp {

using namespace NActors;

namespace {

// Per-cluster partition counts for one topic.
struct TTopicClusterPartitions {
    // Cluster name (TFederatedTopicClient::TClusterInfo::Info.Name) →
    // partition count for that cluster.
    // For non-federated topics the map has one entry with an empty-string key.
    THashMap<TString, ui32> ByClusterName;
    // Maximum partition count across all clusters in this topic.
    // Used to set TDqReadTaskParams::TPartitioningParams::TopicPartitionsCount.
    ui32 MaxPartitionsCount = 0;
};

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
    // On success: topic key (Endpoint + "|" + TopicPath) and per-cluster data.
    TString TopicKey;
    TTopicClusterPartitions ClusterPartitions;
    // On error: ErrorMessage is non-empty.
    TString ErrorMessage;

    TEvTopicDescribeResult(TString topicKey, TTopicClusterPartitions clusterPartitions)
        : TopicKey(std::move(topicKey))
        , ClusterPartitions(std::move(clusterPartitions)) {}
    explicit TEvTopicDescribeResult(TString errorMessage)
        : ErrorMessage(std::move(errorMessage)) {}
};

// Description of a single PQ topic source that needs to be resolved.
// Used only internally within the resolver.
struct TPqTopicResolverSource {
    TString Cluster;
    TString Endpoint;
    TString Database;     // real YDB database path for the describe RPC
    TString TopicPath;
    TString TokenName;    // key in SecureParams to look up the auth token
    bool    UseSsl = false;
    TString DatabaseForClusterConfig; // raw "database" field from the proto (may be cluster alias)
};

// Collect all PQ sources from a set of physical transactions.
// Topics whose partition list was already fixed at compile time by a
// __ydb_partition_id predicate are skipped — their ReadRanges are authoritative.
TVector<TPqTopicResolverSource> CollectPqSources(
    const TVector<IKqpGateway::TPhysicalTxData>& transactions,
    const TString& database)
{
    TVector<TPqTopicResolverSource> result;
    for (const auto& tx : transactions) {
        for (const auto& stage : tx.Body->GetStages()) {
            if (stage.SourcesSize() == 0) {
                continue;
            }
            const auto& src = stage.GetSources(0);
            if (!src.HasExternalSource()) {
                continue;
            }
            const auto& extSrc = src.GetExternalSource();
            if (extSrc.GetType() != "PqSource") {
                continue;
            }

            // If the partition list was already fixed at compile time by a
            // __ydb_partition_id predicate, the ReadRanges are authoritative
            // and must not be overwritten with the current total partition count.
            NYql::NPq::NProto::TDqPqTopicSource topicSourceProto;
            const bool usedPartitionPredicate =
                extSrc.GetSettings().UnpackTo(&topicSourceProto)
                && topicSourceProto.GetUsedPartitionPredicate();
            if (usedPartitionPredicate) {
                continue;
            }

            NYql::NPq::NProto::TDqPqTopicSource ts;
            extSrc.GetSettings().UnpackTo(&ts);

            TPqTopicResolverSource resolverSrc;
            resolverSrc.Cluster   = extSrc.GetSourceName();
            resolverSrc.Endpoint  = ts.GetEndpoint();
            resolverSrc.Database  = ts.GetEndpoint().empty() ? database : ts.GetDatabase();
            resolverSrc.TopicPath = ts.GetTopicPath();
            resolverSrc.TokenName = ts.GetToken().GetName();
            resolverSrc.UseSsl    = ts.GetUseSsl();
            resolverSrc.DatabaseForClusterConfig = ts.GetDatabase();
            result.push_back(std::move(resolverSrc));
        }
    }
    return result;
}

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

        // Store per-cluster partition counts for this topic, keyed by the topic's
        // unique identifier (Endpoint + "|" + TopicPath). When multiple topics are
        // in the query each gets its own entry in this map.
        TopicClusterPartitions[ev->Get()->TopicKey] = std::move(ev->Get()->ClusterPartitions);

        if (--Pending == 0) {
            PatchAndDie();
        }
    }

private:
    void DescribeTopic(const TPqTopicResolverSource& src) {
        auto it = SecureParams.find(src.TokenName);
        const TString token = it != SecureParams.end() ? it->second : TString{};

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

        // Build a key that uniquely identifies this topic among all topics in
        // the query. Combining Endpoint and TopicPath is sufficient: topics on
        // different clusters have different endpoints, and within a single
        // cluster topic paths are unique (including when the endpoint is empty
        // for local clusters).
        TString topicKey = src.Endpoint + "|" + src.TopicPath;

        gateway->DescribeFederatedTopic(sessionId, src.Cluster, src.Database, src.TopicPath, token)
            .Subscribe(
                [actorSystem = TActivationContext::ActorSystem(),
                 selfId = SelfId(),
                 gateway = gateway,
                 topicKey = topicKey](const auto& future)
                {
                    try {
                        const auto& result = future.GetValue();
                        TTopicClusterPartitions clusterPartitions;
                        for (const auto& clusterInfo : result) {
                            clusterPartitions.ByClusterName[TString(clusterInfo.Info.Name)] =
                                clusterInfo.PartitionsCount;
                            clusterPartitions.MaxPartitionsCount =
                                Max(clusterPartitions.MaxPartitionsCount,
                                    clusterInfo.PartitionsCount);
                        }
                        actorSystem->Send(selfId,
                            new TEvTopicDescribeResult(topicKey,
                                std::move(clusterPartitions)));
                    } catch (const std::exception& ex) {
                        actorSystem->Send(selfId,
                            new TEvTopicDescribeResult(TString(ex.what())));
                    }
                });
    }

    // Patch all PQ tasks in the physical graph:
    //   1. Update TDqPqFederatedCluster::PartitionsCount in the source settings
    //      with the actual per-cluster partition count from the describe result.
    //   2. Update TDqReadTaskParams::TPartitioningParams::TopicPartitionsCount
    //      with the maximum partition count across all clusters (the read actor
    //      uses per-cluster PartitionsCount to limit its range, but rd_read_actor
    //      uses TopicPartitionsCount from the params directly).
    void PatchAndDie() {
        if (QueryPhysicalGraph && !TopicClusterPartitions.empty()) {
            for (int taskIdx = 0;
                 taskIdx < static_cast<int>(QueryPhysicalGraph->TasksSize());
                 ++taskIdx)
            {
                auto* task = QueryPhysicalGraph->MutableTasks(taskIdx);
                auto* dqTask = task->MutableDqTask();

                if (dqTask->ReadRangesSize() == 0) {
                    continue;
                }

                // Determine which topic this task reads from and compute the
                // correct per-cluster and max partition counts. All ReadRanges
                // in a single DqTask come from the same external source.
                ui32 maxPartitionsCount = 0;
                for (auto& input : *dqTask->MutableInputs()) {
                    if (!input.HasSource() || input.GetSource().GetType() != "PqSource") {
                        continue;
                    }

                    NYql::NPq::NProto::TDqPqTopicSource topicSource;
                    if (!input.GetSource().GetSettings().UnpackTo(&topicSource)) {
                        continue;
                    }

                    TString topicKey =
                        topicSource.GetEndpoint() + "|" + topicSource.GetTopicPath();
                    auto it = TopicClusterPartitions.find(topicKey);
                    if (it == TopicClusterPartitions.end()) {
                        break;
                    }

                    const TTopicClusterPartitions& clusterPartitions = it->second;
                    maxPartitionsCount = clusterPartitions.MaxPartitionsCount;

                    // Update per-cluster partition counts in the source settings.
                    // TDqPqTopicSource::FederatedClusters[i].PartitionsCount is
                    // used by the read actor (static discovery path) to limit
                    // which partition IDs belong to each cluster.
                    bool sourceModified = false;
                    for (int ci = 0;
                         ci < static_cast<int>(topicSource.FederatedClustersSize());
                         ++ci)
                    {
                        auto* fc = topicSource.MutableFederatedClusters(ci);
                        auto cit = clusterPartitions.ByClusterName.find(fc->GetName());
                        if (cit != clusterPartitions.ByClusterName.end()) {
                            const ui32 oldCount = fc->GetPartitionsCount();
                            const ui32 newCount = cit->second;
                            // Only increase partition count: if cluster returned 0
                            // (e.g. it is unavailable), keep the compiled value.
                            const ui32 effectiveCount = Max(oldCount, newCount);
                            if (effectiveCount != oldCount) {
                                fc->SetPartitionsCount(effectiveCount);
                                sourceModified = true;
                            }
                        }
                    }

                    if (sourceModified) {
                        // Repack the modified source settings back into the task input.
                        input.MutableSource()->MutableSettings()->PackFrom(topicSource);
                    }
                    // Serialize the (possibly updated) TDqPqTopicSource into TaskParams so
                    // that TPqDqTaskTransform can use the current per-cluster PartitionsCount
                    // from FederatedClusters instead of the compile-time values baked into
                    // the MiniKQL program bytes.
                    (*dqTask->MutableTaskParams())["pq_topic_source"] =
                        topicSource.SerializeAsString();
                    break;
                }

                if (maxPartitionsCount == 0) {
                    continue;
                }

                // Update TopicPartitionsCount in all ReadRanges for this task.
                // This value is used by dq_pq_rd_read_actor to determine the
                // full partition range, and serves as a fallback in dq_pq_read_actor
                // when FederatedClusters.PartitionsCount is zero.
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
                    bool rangeModified = false;
                    for (int ppIdx = 0;
                         ppIdx < static_cast<int>(params.PartitioningParamsSize());
                         ++ppIdx)
                    {
                        auto* pp = params.MutablePartitioningParams(ppIdx);
                        const ui32 oldTotal = pp->GetTopicPartitionsCount();
                        // Only increase: keep compiled value if describe returned less.
                        const ui32 effectiveTotal = Max(oldTotal, maxPartitionsCount);
                        if (effectiveTotal != oldTotal) {
                            pp->SetTopicPartitionsCount(effectiveTotal);
                            rangeModified = true;
                        }
                    }
                    if (rangeModified) {
                        dqTask->SetReadRanges(rangeIdx, params.SerializeAsString());
                    }
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
    // Maps topic key (Endpoint + "|" + TopicPath) to per-cluster partition
    // counts. Populated as describe responses arrive; consumed in PatchAndDie().
    THashMap<TString, TTopicClusterPartitions> TopicClusterPartitions;
};

} // anonymous namespace

NActors::IActor* CreateKqpPqTopicResolver(
    const NActors::TActorId& owner,
    ui64 txId,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions,
    const TString& database,
    THashMap<TString, TString> secureParams,
    NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
    std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph> queryPhysicalGraph)
{
    auto sources = CollectPqSources(transactions, database);
    return new TKqpPqTopicResolver(
        owner, txId,
        std::move(sources),
        std::move(secureParams),
        std::move(pqGatewayFactory),
        std::move(queryPhysicalGraph));
}

} // namespace NKikimr::NKqp
