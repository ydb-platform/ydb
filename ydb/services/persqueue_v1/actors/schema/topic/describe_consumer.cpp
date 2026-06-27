#include "common_describe.h"

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

    class TDescribeActor: public TDescribeBaseActor<TDescribeActor, NGRpcService::TEvDescribeConsumerRequest> {
        using TBase = TDescribeBaseActor<TDescribeActor, NGRpcService::TEvDescribeConsumerRequest>;

    public:
        TDescribeActor(NGRpcService::IRequestOpCtx* request)
            : TBase(request, NACLib::EAccessRights::DescribeSchema)
        {
        }

        bool ValidateSchema() override {
            const auto& consumerName = GetProtoRequest()->consumer();
            const auto normalizedConsumerName = NPersQueue::ConvertNewConsumerName(consumerName, ActorContext());
            const auto* consumer = NPQ::GetConsumer(TopicInfo.Info->Description.GetPQTabletConfig(), normalizedConsumerName);
            if (!consumer) {
                ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "no consumer '" << consumerName << "' in topic");
                return false;
            }

            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!FillConsumer(*Result.mutable_consumer(), *consumer, status, error, false)) {
                ReplyWithError(status, error);
                return false;
            }

            ConsumerName = consumer->GetName();
            return true;
        }

        bool NeedProcessPartition(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) override {
            Y_UNUSED(partition);
            return true;
        }

        std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() override {
            return std::make_unique<TEvPersQueue::TEvGetReadSessionsInfo>(ConsumerName);
        }

        std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() override {
            return std::make_unique<TEvPersQueue::TEvStatus>(ConsumerName);
        }

        const google::protobuf::Message& MakeResult() override {
            const auto includeLocation = GetProtoRequest()->include_location();
            const auto includeStats = GetProtoRequest()->include_stats();

            Result.mutable_self()->CopyFrom(SelfEntry);
            Result.mutable_self()->set_name(TStringBuilder() << Result.self().name() << "/" << ConsumerName);
            for (const auto& p : TopicInfo.Info->Description.GetPartitions()) {
                auto& partition = *Result.add_partitions();
                partition.set_partition_id(p.GetPartitionId());
                partition.set_active(p.GetStatus() == ::NKikimrPQ::ETopicPartitionStatus::Active);

                auto it = Partitions.find(p.GetPartitionId());
                if (it == Partitions.end()) {
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
                    consumerStats->set_read_session_id(std::move(partitionInfo.ReadSession.MutableSession()));
                    SetProtoTime(consumerStats->mutable_partition_read_session_create_time(), partitionInfo.ReadSession.GetTimestampMs());
                    consumerStats->set_connection_node_id(partitionInfo.ReadSession.GetProxyNodeId());
                    consumerStats->set_reader_name(std::move(partitionInfo.ReadSession.MutableClientNode()));

                    auto* consumer = Result.mutable_consumer();
                    if (!consumer->has_consumer_stats()) {
                        auto* stats = consumer->mutable_consumer_stats();
                        stats->mutable_min_partitions_last_read_time()->CopyFrom(consumerStats->last_read_time());
                        stats->mutable_max_read_time_lag()->CopyFrom(consumerStats->max_read_time_lag());
                        stats->mutable_max_write_time_lag()->CopyFrom(consumerStats->max_write_time_lag());
                        stats->mutable_max_committed_time_lag()->CopyFrom(consumerStats->max_committed_time_lag());
                    } else {
                        auto* stats = consumer->mutable_consumer_stats();
    
                        UpdateProtoTime(*stats->mutable_min_partitions_last_read_time(), consumerStats->last_read_time(), true);
                        UpdateProtoTime(*stats->mutable_max_read_time_lag(), consumerStats->max_read_time_lag(), false);
                        UpdateProtoTime(*stats->mutable_max_write_time_lag(), consumerStats->max_write_time_lag(), false);
                        UpdateProtoTime(*stats->mutable_max_committed_time_lag(), consumerStats->max_committed_time_lag(), false);
                    }
                }
            }

            return Result;
        }

    private:
        Ydb::Topic::DescribeConsumerResult Result;
        TString ConsumerName;
    };

} // namespace

NActors::IActor* CreateDescribeConsumerActor(NGRpcService::IRequestOpCtx* request) {
    return new TDescribeActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
