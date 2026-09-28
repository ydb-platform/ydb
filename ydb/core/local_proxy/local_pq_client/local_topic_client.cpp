#include "local_topic_client.h"
#include "local_topic_client_helpers.h"
#include "local_topic_read_session.h"
#include "local_topic_write_session.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/persqueue_v1/actors/commit_offset_actor.h>
#include <ydb/services/persqueue_v1/actors/schema/topic/actors.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_READ_PROXY

namespace NKikimr::NKqp {

namespace {

using namespace NGRpcService;
using namespace NRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

class TLocalTopicClient final : public TLocalTopicClientBase, public NYql::ITopicClient {
    using TBase = TLocalTopicClientBase;

public:
    using TBase::TBase;

    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) final {
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

    TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const TDescribeConsumerSettings& settings) final {
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

    TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, const TDescribePartitionSettings& settings) final {
        Y_UNUSED(path, partitionId, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings) final {
        return CreateLocalTopicReadSession({
            .ActorSystem = ActorSystem,
            .Database = Database,
            .CredentialsProvider = CredentialsProvider,
        }, settings);
    }

    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TWriteSessionSettings& settings) final {
        Y_UNUSED(settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings) final {
        return CreateLocalTopicWriteSession({
            .ActorSystem = ActorSystem,
            .Database = Database,
            .CredentialsProvider = CredentialsProvider,
        }, settings);
    }

    TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const TCommitOffsetSettings& settings) final {
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

private:
    static void DoDescribeConsumerRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& facility) {
        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "New Describe consumer request");
        facility.RegisterActor(NGRpcProxy::V1::NTopic::CreateDescribeConsumerActor(ctx.release()));
    }

    static void DoCommitOffsetRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& facility) {
        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "New Commit Offset request");
        facility.RegisterActor(new NGRpcProxy::V1::TCommitOffsetActor(ctx.release()));
    }
};

} // anonymous namespace

NYql::ITopicClient::TPtr CreateLocalTopicClient(const TLocalTopicClientSettings& localSettings, const TTopicClientSettings& clientSettings) {
    return MakeIntrusive<TLocalTopicClient>(localSettings, clientSettings);
}

} // namespace NKikimr::NKqp
