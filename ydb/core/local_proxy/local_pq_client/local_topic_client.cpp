#include "local_topic_client.h"
#include "local_topic_client_helpers.h"
#include "local_topic_read_session.h"
#include "local_topic_write_session.h"

#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

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

    TAsyncStatus CreateTopic(const TString& path, const TCreateTopicSettings& settings) final {
        Y_UNUSED(path, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

    TAsyncStatus AlterTopic(const TString& path, const TAlterTopicSettings& settings) final {
        Y_UNUSED(path, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& settings) final {
        Y_UNUSED(path, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) final {
        using TDescribeTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>;

        TDescribeTopicRequest::TRequest request;
        request.set_path(path);
        request.set_include_stats(settings.IncludeStats_);
        request.set_include_location(settings.IncludeLocation_);

        return DoLocalRpcRequest<TDescribeTopicRequest, TDescribeTopicSettings>(std::move(request), settings, &DoDescribeTopicRequest).Apply([](const NThreading::TFuture<TLocalRpcOperationResult>& f) {
            const auto& [status, response] = f.GetValue();
            Ydb::Topic::DescribeTopicResult result;
            response.UnpackTo(&result);
            return TDescribeTopicResult(TStatus(status), std::move(result));
        });
    }

    TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const TDescribeConsumerSettings& settings) final {
        Y_UNUSED(path, consumer, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
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
        Y_UNUSED(path, partitionId, consumerName, offset, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }
};

} // anonymous namespace

NYql::ITopicClient::TPtr CreateLocalTopicClient(const TLocalTopicClientSettings& localSettings, const TTopicClientSettings& clientSettings) {
    return MakeIntrusive<TLocalTopicClient>(localSettings, clientSettings);
}

} // namespace NKikimr::NKqp
