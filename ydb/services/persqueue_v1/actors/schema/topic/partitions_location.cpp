#include "common_describe.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

class TPartitionsLocationLogic: public TDescribeLogicActor<TPartitionsLocationLogic> {
    using TBase = TDescribeLogicActor<TPartitionsLocationLogic>;

public:
    TPartitionsLocationLogic(
        const NActors::TActorId& parent,
        TDescribeSettings&& settings,
        absl::flat_hash_set<ui32> partitionIds)
        : TBase(parent, std::move(settings))
        , PartitionIds(std::move(partitionIds))
    {
    }

    bool ValidateSchema() override {
        if (PartitionIds.empty()) {
            return true;
        }

        absl::flat_hash_set<ui32> topicPartitions;
        for (const auto& partition : TopicInfo.Info->Description.GetPartitions()) {
            topicPartitions.insert(partition.GetPartitionId());
        }

        for (const auto partitionId : PartitionIds) {
            if (!topicPartitions.contains(partitionId)) {
                ReplyWithError(
                    Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "No partition " << partitionId << " in topic",
                    Ydb::PersQueue::ErrorCode::BAD_REQUEST);
                return false;
            }
        }
        return true;
    }

    bool NeedProcessPartition(const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) override {
        return PartitionIds.empty() || PartitionIds.contains(partition.GetPartitionId());
    }

    std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() override {
        return nullptr;
    }

    std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() override {
        return nullptr;
    }

private:
    const absl::flat_hash_set<ui32> PartitionIds;
};

class TPartitionsLocationProxy: public NActors::TActorBootstrapped<TPartitionsLocationProxy> {
public:
    TPartitionsLocationProxy(const TGetPartitionsLocationRequest& request, const TActorId& requester)
        : Request(request)
        , Requester(requester)
        , Response(MakeHolder<TEvPQProxy::TEvPartitionLocationResponse>())
    {
        PartitionIds.insert(Request.PartitionIds.begin(), Request.PartitionIds.end());
    }

    void Bootstrap() {
        if (Request.Token.empty()) {
            if (AppData()->EnforceUserTokenRequirement || AppData()->PQConfig.GetRequireCredentialsInNewProtocol()) {
                return ReplyWithError(
                    Ydb::StatusIds::UNAUTHORIZED,
                    "Unauthenticated access is forbidden, please provide credentials",
                    Ydb::PersQueue::ErrorCode::ACCESS_DENIED);
            }
        }

        TIntrusiveConstPtr<NACLib::TUserToken> userToken;
        if (!Request.Token.empty()) {
            userToken = new NACLib::TUserToken(Request.Token);
        }

        LogicActorId = RegisterWithSameMailbox(new TPartitionsLocationLogic(SelfId(), {
            .Path = Request.Topic,
            .Database = Request.Database,
            .UserToken = userToken,
            .AccessRights = NACLib::EAccessRights::DescribeSchema,
            .IncludeStats = false,
            .IncludeLocation = true,
        }, PartitionIds));

        Become(&TPartitionsLocationProxy::StateWork);
    }

private:
    void Handle(TEvDescribeResponse::TPtr& ev) {
        LogicActorId = {};
        if (!Response) {
            return;
        }
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage, ev->Get()->IssueCode);
        }

        Response->PathId = ev->Get()->TopicInfo.Self->Info.GetPathId();
        Response->SchemeShardId = ev->Get()->TopicInfo.Self->Info.GetSchemeshardId();

        Response->Partitions.reserve(ev->Get()->Partitions.size());
        for (const auto& [partitionId, info] : ev->Get()->Partitions) {
            if (!PartitionIds.empty() && !PartitionIds.contains(partitionId)) {
                continue;
            }

            TEvPQProxy::TPartitionLocationInfo partLocation;
            partLocation.PartitionId = partitionId;
            partLocation.Generation = info.Location.generation();
            partLocation.NodeId = info.Location.node_id();
            Response->Partitions.emplace_back(std::move(partLocation));
        }

        if (!PartitionIds.empty()) {
            AFL_ENSURE(Response->Partitions.size() == PartitionIds.size())
                ("l", Response->Partitions.size())
                ("r", PartitionIds.size());
        } else {
            AFL_ENSURE(Response->Partitions.size() >= ev->Get()->TopicInfo.Info->Description.PartitionsSize())
                ("l", Response->Partitions.size())
                ("r", ev->Get()->TopicInfo.Info->Description.PartitionsSize());
        }

        Response->Status = Ydb::StatusIds::SUCCESS;
        Send(Requester, Response.Release());
        PassAway();
    }

    void ReplyWithError(
        Ydb::StatusIds::StatusCode status,
        const TString& messageText,
        Ydb::PersQueue::ErrorCode::ErrorCode issueCode)
    {
        if (!Response) {
            PassAway();
            return;
        }
        Response->Status = status;
        Response->Issues.AddIssue(FillIssue(messageText, issueCode));
        Send(Requester, Response.Release());
        PassAway();
    }

    void HandlePoison() {
        if (LogicActorId) {
            // Logic replies CANCELLED via TEvDescribeResponse; proxy forwards it below.
            Send(LogicActorId, new NActors::TEvents::TEvPoison());
            return;
        }
        if (Response) {
            ReplyWithError(
                Ydb::StatusIds::CANCELLED,
                "Request was cancelled",
                Ydb::PersQueue::ErrorCode::ERROR);
        } else {
            PassAway();
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDescribeResponse, Handle);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        }
    }

private:
    TGetPartitionsLocationRequest Request;
    TActorId Requester;
    THolder<TEvPQProxy::TEvPartitionLocationResponse> Response;
    absl::flat_hash_set<ui32> PartitionIds;
    TActorId LogicActorId;
};

} // namespace

NActors::IActor* CreatePartitionsLocationActor(
    const TGetPartitionsLocationRequest& request,
    const TActorId& requester)
{
    return new TPartitionsLocationProxy(request, requester);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
