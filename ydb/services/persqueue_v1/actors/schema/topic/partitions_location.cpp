#include <ydb/core/persqueue/public/schema/describe_operation.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

#include <absl/container/flat_hash_set.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

using namespace NPQ::NSchema;

class TPartitionsLocationStrategy: public IDescribeStrategy {
public:
    explicit TPartitionsLocationStrategy(absl::flat_hash_set<ui32> partitionIds)
        : PartitionIds(std::move(partitionIds))
    {
    }

    TString GetName() const override {
        return "PartitionsLocation";
    }

    TDescribeSchemaResult ValidateSchema(const NPQ::NDescriber::TTopicInfo& topicInfo) override {
        if (PartitionIds.empty()) {
            return {};
        }

        absl::flat_hash_set<ui32> topicPartitions;
        for (const auto& partition : topicInfo.Info->Description.GetPartitions()) {
            topicPartitions.insert(partition.GetPartitionId());
        }

        for (const auto partitionId : PartitionIds) {
            if (!topicPartitions.contains(partitionId)) {
                return {
                    .Error = TDescribeSchemaError{
                        .Status = Ydb::StatusIds::BAD_REQUEST,
                        .Message = TStringBuilder() << "No partition " << partitionId << " in topic",
                        .IssueCode = Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                        .RetryWithSync = true,
                    },
                };
            }
        }
        return {};
    }

    bool NeedProcessPartition(
        const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) const override {
        return PartitionIds.empty() || PartitionIds.contains(partition.GetPartitionId());
    }

    std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() const override {
        return nullptr;
    }

    std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() const override {
        return nullptr;
    }

private:
    const absl::flat_hash_set<ui32> PartitionIds;
};

class TPartitionsLocationProxy: public NActors::TActorBootstrapped<TPartitionsLocationProxy> {
public:
    TPartitionsLocationProxy(const TActorId& requester, const TGetPartitionsLocationRequest& request)
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

        LogicActorId = RegisterWithSameMailbox(CreateDescribeOperationActor(
            SelfId(),
            {
                .Path = Request.Topic,
                .Database = Request.Database,
                .UserToken = userToken,
                .AccessRights = NACLib::EAccessRights::DescribeSchema,
                .IncludeStats = false,
                .IncludeLocation = true,
            },
            std::make_unique<TPartitionsLocationStrategy>(PartitionIds)));

        Become(&TPartitionsLocationProxy::StateWork);
    }

private:
    void Handle(TEvDescribeOperationResponse::TPtr& ev) {
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
            // Logic replies CANCELLED via TEvDescribeOperationResponse; proxy forwards it below.
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
            hFunc(TEvDescribeOperationResponse, Handle);
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
    const TActorId& requester,
    const TGetPartitionsLocationRequest& request)
{
    return new TPartitionsLocationProxy(requester, request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
