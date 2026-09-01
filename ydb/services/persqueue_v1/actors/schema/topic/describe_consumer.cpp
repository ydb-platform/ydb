#include "describe_helpers.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/describe_operation.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

using namespace NPQ::NSchema;

class TDescribeConsumerStrategy: public IDescribeStrategy {
public:
    explicit TDescribeConsumerStrategy(TString consumer)
        : RequestedConsumer(std::move(consumer))
    {
    }

    TString GetName() const override {
        return "DescribeConsumer";
    }

    TDescribeSchemaResult ValidateSchema(const NPQ::NDescriber::TTopicInfo& topicInfo) override {
        const auto normalizedConsumerName = NPersQueue::ConvertNewConsumerName(
            RequestedConsumer, AppData()->PQConfig);
        const auto* consumer = NPQ::GetConsumer(
            topicInfo.Info->Description.GetPQTabletConfig(), normalizedConsumerName);
        if (!consumer) {
            return {
                .Error = TDescribeSchemaError{
                    .Status = Ydb::StatusIds::SCHEME_ERROR,
                    .Message = TStringBuilder() << "no consumer '" << RequestedConsumer << "' in topic",
                    .RetryWithSync = true,
                },
            };
        }

        ConsumerName = consumer->GetName();
        return {.ConsumerName = ConsumerName};
    }

    bool NeedProcessPartition(
        const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) const override {
        Y_UNUSED(partition);
        return true;
    }

    std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() const override {
        return std::make_unique<TEvPersQueue::TEvGetReadSessionsInfo>(ConsumerName);
    }

    std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() const override {
        return std::make_unique<TEvPersQueue::TEvStatus>(ConsumerName);
    }

private:
    const TString RequestedConsumer;
    TString ConsumerName;
};

class TDescribeConsumerGrpc: public TGrpcProxyActor<TDescribeConsumerGrpc, NGRpcService::TEvDescribeConsumerRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TDescribeConsumerGrpc, NGRpcService::TEvDescribeConsumerRequest>;

public:
    TDescribeConsumerGrpc(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor(request)
    {
    }

    void DoAction() {
        Become(&TDescribeConsumerGrpc::StateWork);

        LogicActorId = RegisterWithSameMailbox(CreateDescribeOperationActor(
            SelfId(),
            {
                .Path = Request_->GetDatabaseRelativePath(GetProtoRequest()->path()),
                .Database = GetDatabase(),
                .UserToken = GetUserToken(),
                .AccessRights = NACLib::EAccessRights::DescribeSchema,
                .IncludeStats = GetProtoRequest()->include_stats(),
                .IncludeLocation = GetProtoRequest()->include_location(),
            },
            std::make_unique<TDescribeConsumerStrategy>(GetProtoRequest()->consumer())));
    }

private:
    void Handle(TEvDescribeOperationResponse::TPtr& ev) {
        LogicActorId = {};
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage, ev->Get()->IssueCode);
        }

        const auto includeLocation = GetProtoRequest()->include_location();
        const auto includeStats = GetProtoRequest()->include_stats();
        const auto& consumerName = ev->Get()->ConsumerName;

        Ydb::Topic::DescribeConsumerResult result;

        const auto* consumer = NPQ::GetConsumer(
            ev->Get()->TopicInfo.Info->Description.GetPQTabletConfig(),
            consumerName);
        AFL_ENSURE(consumer)("consumer", consumerName);

        Ydb::StatusIds::StatusCode status;
        TString error;
        FillConsumer(
            *result.mutable_consumer(),
            ev->Get()->TopicInfo.Info->Description.GetPQTabletConfig(),
            *consumer,
            status,
            error,
            false);

        result.mutable_self()->CopyFrom(ev->Get()->SelfEntry);
        result.mutable_self()->set_name(TStringBuilder() << result.self().name() << "/" << consumerName);

        for (const auto& p : ev->Get()->TopicInfo.Info->Description.GetPartitions()) {
            auto& partition = *result.add_partitions();
            partition.set_partition_id(p.GetPartitionId());
            partition.set_active(p.GetStatus() == ::NKikimrPQ::ETopicPartitionStatus::Active);

            auto it = ev->Get()->Partitions.find(p.GetPartitionId());
            if (it == ev->Get()->Partitions.end()) {
                continue;
            }

            auto& partitionInfo = it->second;

            if (includeLocation) {
                *partition.mutable_partition_location() = partitionInfo.Location;
            }

            if (includeStats) {
                auto* partitionStats = partition.mutable_partition_stats();
                *partitionStats = partitionInfo.Stats.partition_stats();
                partitionStats->set_partition_node_id(partitionInfo.Location.node_id());

                auto* consumerStats = partition.mutable_partition_consumer_stats();
                *consumerStats = partitionInfo.Stats.partition_consumer_stats();
                consumerStats->set_read_session_id(partitionInfo.ReadSession.GetSession());
                SetProtoTime(consumerStats->mutable_partition_read_session_create_time(), partitionInfo.ReadSession.GetTimestampMs());
                consumerStats->set_connection_node_id(partitionInfo.ReadSession.GetProxyNodeId());
                consumerStats->set_reader_name(partitionInfo.ReadSession.GetClientNode());

                auto* consumerProto = result.mutable_consumer();
                if (!consumerProto->has_consumer_stats()) {
                    auto* stats = consumerProto->mutable_consumer_stats();
                    stats->mutable_min_partitions_last_read_time()->CopyFrom(consumerStats->last_read_time());
                    stats->mutable_max_read_time_lag()->CopyFrom(consumerStats->max_read_time_lag());
                    stats->mutable_max_write_time_lag()->CopyFrom(consumerStats->max_write_time_lag());
                    stats->mutable_max_committed_time_lag()->CopyFrom(consumerStats->max_committed_time_lag());
                } else {
                    auto* stats = consumerProto->mutable_consumer_stats();

                    UpdateProtoTime(*stats->mutable_min_partitions_last_read_time(), consumerStats->last_read_time(), true);
                    UpdateProtoTime(*stats->mutable_max_read_time_lag(), consumerStats->max_read_time_lag(), false);
                    UpdateProtoTime(*stats->mutable_max_write_time_lag(), consumerStats->max_write_time_lag(), false);
                    UpdateProtoTime(*stats->mutable_max_committed_time_lag(), consumerStats->max_committed_time_lag(), false);
                }
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

NActors::IActor* CreateDescribeConsumerActor(NGRpcService::IRequestOpCtx* request) {
    return new TDescribeConsumerGrpc(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
