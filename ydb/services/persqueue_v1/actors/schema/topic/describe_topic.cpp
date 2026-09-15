#include "describe_helpers.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/describe_operation.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

#include <util/generic/maybe.h>

#include <absl/container/flat_hash_map.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

using namespace NPQ::NSchema;

class TDescribeTopicStrategy: public IDescribeStrategy {
public:
    TString GetName() const override {
        return "DescribeTopic";
    }

    TDescribeSchemaResult ValidateSchema(const NPQ::NDescriber::TTopicInfo&) override {
        return {};
    }

    bool NeedProcessPartition(
        const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition&) const override {
        return true;
    }

    std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() const override {
        return nullptr;
    }

    std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() const override {
        return std::make_unique<TEvPersQueue::TEvStatus>("", true);
    }
};

class TDescribeTopicGrpc: public TGrpcProxyActor<TDescribeTopicGrpc, NGRpcService::TEvDescribeTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TDescribeTopicGrpc, NGRpcService::TEvDescribeTopicRequest>;

public:
    TDescribeTopicGrpc(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor(request)
    {
    }

    void DoAction() {
        Become(&TDescribeTopicGrpc::StateWork);

        LogicActorId = RegisterWithSameMailbox(CreateDescribeOperationActor(
            SelfId(),
            {
                .Path = ResolveTopicPath(GetProtoRequest()->path()),
                .Database = GetDatabase(),
                .UserToken = GetUserToken(),
                .AccessRights = NACLib::EAccessRights::DescribeSchema,
                .IncludeStats = GetProtoRequest()->include_stats(),
                .IncludeLocation = GetProtoRequest()->include_location(),
            },
            std::make_unique<TDescribeTopicStrategy>()));
    }

private:
    void Handle(TEvDescribeOperationResponse::TPtr& ev) {
        LogicActorId = {};
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage, ev->Get()->IssueCode);
        }

        const auto includeLocation = GetProtoRequest()->include_location();
        const auto includeStats = GetProtoRequest()->include_stats();

        Ydb::Topic::DescribeTopicResult result;
        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!FillTopicDescription(
                result,
                ev->Get()->TopicInfo.Info->Description,
                ev->Get()->TopicInfo.Self->Info,
                Nothing(),
                status,
                error))
        {
            return ReplyWithError(status, error);
        }
        result.mutable_self()->Swap(&ev->Get()->SelfEntry);

        absl::flat_hash_map<TString, Ydb::Topic::Consumer*> consumersInfo;
        if (includeStats) {
            const auto& pqConfig = AppData()->PQConfig;
            consumersInfo.reserve(result.consumers_size());
            for (auto& consumer : *result.mutable_consumers()) {
                consumersInfo.emplace(
                    NPersQueue::ConvertNewConsumerName(consumer.name(), pqConfig),
                    &consumer);
            }
        }

        Ydb::Topic::DescribeTopicResult::TopicStats* topicStats = nullptr;
        for (auto& partition : *result.mutable_partitions()) {
            auto it = ev->Get()->Partitions.find(partition.partition_id());
            if (it == ev->Get()->Partitions.end()) {
                continue;
            }

            auto& partitionInfo = it->second;

            if (includeStats) {
                auto* partitionStats = partition.mutable_partition_stats();
                partitionStats->Swap(partitionInfo.Stats.mutable_partition_stats());
                partitionStats->set_partition_node_id(partitionInfo.Location.node_id());

                if (!topicStats) {
                    topicStats = result.mutable_topic_stats();
                    topicStats->set_store_size_bytes(partitionStats->store_size_bytes());
                    topicStats->mutable_min_last_write_time()->CopyFrom(partitionStats->last_write_time());
                    topicStats->mutable_max_write_time_lag()->CopyFrom(partitionStats->max_write_time_lag());
                } else {
                    topicStats->set_store_size_bytes(topicStats->store_size_bytes() + partitionStats->store_size_bytes());
                    UpdateProtoTime(*topicStats->mutable_min_last_write_time(), partitionStats->last_write_time(), true);
                    UpdateProtoTime(*topicStats->mutable_max_write_time_lag(), partitionStats->max_write_time_lag(), false);
                }
                AddWindowsStat(
                    topicStats->mutable_bytes_written(),
                    partitionStats->bytes_written().per_minute(),
                    partitionStats->bytes_written().per_hour(),
                    partitionStats->bytes_written().per_day());

                for (auto& [consumerName, consumerPartitionStats] : partitionInfo.Consumers) {
                    auto consumerIt = consumersInfo.find(consumerName);
                    if (consumerIt == consumersInfo.end()) {
                        continue;
                    }

                    auto* consumerProto = consumerIt->second;
                    if (!consumerProto->has_consumer_stats()) {
                        consumerProto->mutable_consumer_stats()->Swap(&consumerPartitionStats);
                    } else {
                        auto* stats = consumerProto->mutable_consumer_stats();
                        UpdateProtoTime(*stats->mutable_min_partitions_last_read_time(), consumerPartitionStats.min_partitions_last_read_time(), true);
                        UpdateProtoTime(*stats->mutable_max_read_time_lag(), consumerPartitionStats.max_read_time_lag(), false);
                        UpdateProtoTime(*stats->mutable_max_write_time_lag(), consumerPartitionStats.max_write_time_lag(), false);
                        UpdateProtoTime(*stats->mutable_max_committed_time_lag(), consumerPartitionStats.max_committed_time_lag(), false);
                        AddWindowsStat(
                            stats->mutable_bytes_read(),
                            consumerPartitionStats.bytes_read().per_minute(),
                            consumerPartitionStats.bytes_read().per_hour(),
                            consumerPartitionStats.bytes_read().per_day());
                    }
                }
            }

            if (includeLocation) {
                partition.mutable_partition_location()->Swap(&partitionInfo.Location);
            }
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result);
    }

    void PassAway() override {
        if (LogicActorId) {
            Send(LogicActorId, new NActors::TEvents::TEvPoison());
            LogicActorId = {};
        }
        TRpcOpBase::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDescribeOperationResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }

private:
    NActors::TActorId LogicActorId;
};

} // namespace

NActors::IActor* CreateDescribeTopicActor(NGRpcService::IRequestOpCtx* request) {
    return new TDescribeTopicGrpc(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
