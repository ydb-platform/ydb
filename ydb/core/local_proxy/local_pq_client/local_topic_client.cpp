#include "local_topic_client.h"
#include "local_topic_read_session.h"
#include "local_topic_write_session.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/providers/pq/gateway/clients/message_stream/yql_pq_message_stream_client.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/persqueue_v1/actors/commit_offset_actor.h>
#include <ydb/services/persqueue_v1/actors/schema/topic/actors.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_READ_PROXY

namespace NKikimr::NKqp {

using namespace NGRpcService;
using namespace NRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

namespace {

void DoDescribeConsumerRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& facility) {
    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "New Describe consumer request");
    facility.RegisterActor(NGRpcProxy::V1::NTopic::CreateDescribeConsumerActor(ctx.release()));
}

void DoCommitOffsetRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& facility) {
    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "New Commit Offset request");
    facility.RegisterActor(new NGRpcProxy::V1::TCommitOffsetActor(ctx.release()));
}

} // anonymous namespace

TAsyncDescribeTopicResult TLocalTopicClient::DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) {
    TEvDescribeTopicRequest::TRequest request;
    request.set_path(path);
    request.set_include_stats(settings.IncludeStats_);
    request.set_include_location(settings.IncludeLocation_);

    return DoLocalRpcRequest<TEvDescribeTopicRequest, TDescribeTopicSettings>(std::move(request), settings, &DoDescribeTopicRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
        const auto& [status, response] = f.GetValue();
        Ydb::Topic::DescribeTopicResult result;
        response.UnpackTo(&result);
        return TDescribeTopicResult(TStatus(status), std::move(result));
    });
}

TAsyncDescribeConsumerResult TLocalTopicClient::DescribeConsumer(const TString& path, const TString& consumer, const TDescribeConsumerSettings& settings) {
    TEvDescribeConsumerRequest::TRequest request;
    request.set_path(path);
    request.set_consumer(consumer);
    request.set_include_stats(settings.IncludeStats_);
    request.set_include_location(settings.IncludeLocation_);

    return DoLocalRpcRequest<TEvDescribeConsumerRequest, TDescribeConsumerSettings>(std::move(request), settings, &DoDescribeConsumerRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
        const auto& [status, response] = f.GetValue();
        Ydb::Topic::DescribeConsumerResult result;
        response.UnpackTo(&result);
        return TDescribeConsumerResult(TStatus(status), std::move(result));
    });
}

TAsyncDescribePartitionResult TLocalTopicClient::DescribePartition(const TString& path, i64 partitionId, const TDescribePartitionSettings& settings) {
    Y_UNUSED(path, partitionId, settings);
    Y_VALIDATE(false, __func__ << " is not implemented");
}

std::shared_ptr<IReadSession> TLocalTopicClient::CreateReadSession(const TReadSessionSettings& settings) {
    return CreateLocalTopicReadSession({
        .ActorSystem = ActorSystem,
        .Database = Database,
        .CredentialsProvider = CredentialsProvider,
    }, settings);
}

std::shared_ptr<ISimpleBlockingWriteSession> TLocalTopicClient::CreateSimpleBlockingWriteSession(const TWriteSessionSettings& settings) {
    Y_UNUSED(settings);
    Y_VALIDATE(false, __func__ << " is not implemented");
}

std::shared_ptr<IWriteSession> TLocalTopicClient::CreateWriteSession(const TWriteSessionSettings& settings) {
    return CreateLocalTopicWriteSession({
        .ActorSystem = ActorSystem,
        .Database = Database,
        .CredentialsProvider = CredentialsProvider,
    }, settings);
}

TAsyncStatus TLocalTopicClient::CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const TCommitOffsetSettings& settings) {
    TEvCommitOffsetRequest::TRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);
    request.set_consumer(consumerName);
    request.set_offset(offset);
    if (settings.ReadSessionId_) {
        request.set_read_session_id(*settings.ReadSessionId_);
    }

    return DoLocalRpcRequest<TEvCommitOffsetRequest, TCommitOffsetSettings>(std::move(request), settings, &DoCommitOffsetRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
        return TStatus(f.GetValue().first);
    });
}

namespace {

class TLocalMessageStreamClient final : public NFq::IMessageStreamClient {
public:
    explicit TLocalMessageStreamClient(TIntrusivePtr<TLocalTopicClient> client)
        : Client(std::move(client))
    {}

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamTopicDescription>> DescribeStream(const TString& stream) override {
        return Client->DescribeTopic(stream).Apply([](const auto& future) {
            return NYql::ToMessageStream(future.GetValue());
        });
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& stream, const TString& consumer, const NFq::TMessageStreamDescribeConsumerSettings& settings) override
    {
        return Client->DescribeConsumer(stream, consumer, TDescribeConsumerSettings()
            .IncludeStats(settings.IncludeStats)
            .IncludeLocation(settings.IncludeLocation)).Apply([](const auto& future) {
            return NYql::ToMessageStream(future.GetValue());
        });
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> DescribePartition(const TString& stream, ui64 partitionId) override {
        return Client->DescribePartition(stream, partitionId, TDescribePartitionSettings().IncludeStats(true)).Apply([](const auto& future) {
            return NYql::ToMessageStream(future.GetValue());
        });
    }

    std::shared_ptr<NFq::IMessageStreamReadSession> CreateReadSession(const NFq::TMessageStreamReadSettings& settings) override {
        return NYql::WrapYdbReadSession(Client->CreateReadSession(NYql::ToSdkReadSettings(settings)));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamOffset>> CommitOffset(
        const TString& stream, ui64 partitionId, const TString& consumer, ui64 offset) override
    {
        return Client->CommitOffset(stream, partitionId, consumer, offset).Apply([partitionId, offset](const auto& future) {
            return NYql::ToMessageStreamOffset(future.GetValue(), partitionId, offset);
        });
    }

private:
    const TIntrusivePtr<TLocalTopicClient> Client;
};

} // anonymous namespace

TIntrusivePtr<TLocalTopicClient> CreateLocalTopicClient(const TLocalTopicClientSettings& localSettings, const TTopicClientSettings& clientSettings) {
    return MakeIntrusive<TLocalTopicClient>(localSettings, clientSettings);
}

std::shared_ptr<NFq::IMessageStreamClient> CreateLocalMessageStreamClient(const TLocalTopicClientSettings& localSettings, const TTopicClientSettings& clientSettings) {
    return std::make_shared<TLocalMessageStreamClient>(CreateLocalTopicClient(localSettings, clientSettings));
}

} // namespace NKikimr::NKqp
