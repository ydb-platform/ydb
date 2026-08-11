#include "common_describe.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

class TDescribePartitionLogic: public TDescribeLogicActor<TDescribePartitionLogic> {
    using TBase = TDescribeLogicActor<TDescribePartitionLogic>;

public:
    TDescribePartitionLogic(const NActors::TActorId& parent, TDescribeSettings&& settings, ui32 partitionId)
        : TBase(parent, std::move(settings))
        , PartitionId(partitionId)
    {
    }

    bool ValidateSchema() override {
        auto exists = AnyOf(TopicInfo.Info->Description.GetPartitions(), [this](const auto& p) {
            return p.GetPartitionId() == PartitionId;
        });
        if (!exists) {
            ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "No partition " << PartitionId << " in topic");
            return false;
        }
        return true;
    }

    bool NeedProcessPartition(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) override {
        return partition.GetPartitionId() == PartitionId;
    }

    std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() override {
        return nullptr;
    }

    std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() override {
        return std::make_unique<TEvPersQueue::TEvStatus>();
    }

private:
    const ui32 PartitionId;
};

class TDescribePartitionGrpc: public TGrpcProxyActor<TDescribePartitionGrpc, NGRpcService::TEvDescribePartitionRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TDescribePartitionGrpc, NGRpcService::TEvDescribePartitionRequest>;

public:
    TDescribePartitionGrpc(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor(request)
    {
    }

    void DoAction() {
        Become(&TDescribePartitionGrpc::StateWork);

        LogicActorId = RegisterWithSameMailbox(new TDescribePartitionLogic(SelfId(), {
            .Path = GetProtoRequest()->path(),
            .Database = GetDatabase(),
            .UserToken = GetUserToken(),
            .AccessRights = { NACLib::EAccessRights::DescribeSchema, NACLib::EAccessRights::UpdateRow },
            .IncludeStats = GetProtoRequest()->include_stats(),
            .IncludeLocation = GetProtoRequest()->include_location(),
        }, GetProtoRequest()->partition_id()));
    }

private:
    void Handle(TEvDescribeResponse::TPtr& ev) {
        LogicActorId = {};
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage, ev->Get()->IssueCode);
        }

        const auto includeLocation = GetProtoRequest()->include_location();
        const auto includeStats = GetProtoRequest()->include_stats();
        const auto partitionId = GetProtoRequest()->partition_id();

        Ydb::Topic::DescribePartitionResult result;
        auto* p = FindIfPtr(ev->Get()->TopicInfo.Info->Description.GetPartitions(), [partitionId](const auto& part) {
            return part.GetPartitionId() == partitionId;
        });

        if (p) {
            auto& partition = *result.mutable_partition();
            partition.set_partition_id(p->GetPartitionId());
            partition.set_active(p->GetStatus() == ::NKikimrPQ::ETopicPartitionStatus::Active);

            auto it = ev->Get()->Partitions.find(p->GetPartitionId());
            if (it != ev->Get()->Partitions.end()) {
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
            hFunc(TEvDescribeResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }

private:
    NActors::TActorId LogicActorId;
};

} // namespace

NActors::IActor* CreateDescribePartitionActor(NGRpcService::IRequestOpCtx* request) {
    return new TDescribePartitionGrpc(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
