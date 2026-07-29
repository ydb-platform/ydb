#include "common_describe.h"

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

    class TDescribeActor: public TDescribeBaseActor<TDescribeActor, NGRpcService::TEvDescribePartitionRequest> {
        using TBase = TDescribeBaseActor<TDescribeActor, NGRpcService::TEvDescribePartitionRequest>;

    public:
        TDescribeActor(NGRpcService::IRequestOpCtx* request)
            : TBase(request, { NACLib::EAccessRights::DescribeSchema, NACLib::EAccessRights::UpdateRow })
        {
        }

        bool ValidateSchema() override {
            const auto partitionId = GetProtoRequest()->partition_id();
            auto exists = AnyOf(TopicInfo.Info->Description.GetPartitions(), [partitionId](const auto& p) {
                return p.GetPartitionId() == partitionId;
            });
            if (!exists) {
                ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "No partition " << partitionId << " in topic");
                return false;
            }
            return true;
        }

        bool NeedProcessPartition(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) override {
            return partition.GetPartitionId() == GetProtoRequest()->partition_id();
        }

        std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() override {
            return nullptr;
        }

        std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() override {
            return std::make_unique<TEvPersQueue::TEvStatus>();
        }

        const google::protobuf::Message& MakeResult() override {
            const auto includeLocation = GetProtoRequest()->include_location();
            const auto includeStats = GetProtoRequest()->include_stats();

            const auto partitionId = GetProtoRequest()->partition_id();
            auto* p = FindIfPtr(TopicInfo.Info->Description.GetPartitions(), [partitionId](const auto& p) {
                return p.GetPartitionId() == partitionId;
            });

            if (p) {
                auto& partition = *Result.mutable_partition();
                partition.set_partition_id(p->GetPartitionId());
                partition.set_active(p->GetStatus() == ::NKikimrPQ::ETopicPartitionStatus::Active);

                auto it = Partitions.find(p->GetPartitionId());
                if (it != Partitions.end()) {
                    auto& partitionInfo = it->second;

                    if (includeLocation) {
                        *partition.mutable_partition_location() = partitionInfo.Location;
                    }
    
                    if (includeStats) {
                        auto* partitionStats = partition.mutable_partition_stats();
                        *partitionStats = partitionInfo.Stats.partition_stats();
                        partitionStats->set_partition_node_id(partitionInfo.Location.node_id());
                    }
                }
            }

            return Result;
        }

    private:
        Ydb::Topic::DescribePartitionResult Result;
    };

} // namespace

NActors::IActor* CreateDescribePartitionActor(NGRpcService::IRequestOpCtx* request) {
    return new TDescribeActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
