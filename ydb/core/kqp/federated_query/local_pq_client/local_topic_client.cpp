#include "local_topic_client.h"
#include "local_topic_read_session.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc_operation.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NKqp {

namespace {

using namespace NGRpcService;
using namespace NRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

class TLocalTopicClient final : public NYql::ITopicClient {
public:
    TLocalTopicClient(const TLocalTopicClientSettings& localSettings, const TTopicClientSettings& clientSettings)
        : ActorSystem(localSettings.ActorSystem)
        , ChannelBufferSize(localSettings.ChannelBufferSize)
        , Database(clientSettings.Database_.value_or(""))
        , CredentialsProvider(CreateCredentialsProvider(clientSettings.CredentialsProviderFactory_))
    {
        Y_VALIDATE(ActorSystem, "Actor system is required for local topic client");
        Y_VALIDATE(ChannelBufferSize, "Channel buffer size is not set");
        Y_VALIDATE(Database, "Database is required for local topic client");
        Y_VALIDATE(clientSettings.DiscoveryEndpoint_.value_or("").empty(), "Discovery endpoint is not allowed for local topic client");
    }

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
        Y_UNUSED(settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

    TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const TCommitOffsetSettings& settings) final {
        Y_UNUSED(path, partitionId, consumerName, offset, settings);
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

private:
    static std::shared_ptr<ICredentialsProvider> CreateCredentialsProvider(const std::optional<std::shared_ptr<ICredentialsProviderFactory>>& maybeFactory) {
        if (const auto factory = maybeFactory.value_or(nullptr)) {
            return factory->CreateProvider();
        }
        return nullptr;
    }

    template <typename TRpc, typename TSettings>
    NThreading::TFuture<TLocalRpcOperationResult> DoLocalRpcRequest(typename TRpc::TRequest&& proto, const TOperationRequestSettings<TSettings>& settings, TLocalRpcOperationRequestCreator requestCreator) {
        const auto promise = NThreading::NewPromise<TLocalRpcOperationResult>();
        auto* actor = new TOperationRequestExecuter<TRpc, TSettings>(std::move(proto), {
            .ChannelBufferSize = ChannelBufferSize,
            .OperationSettings = settings,
            .RequestCreator = std::move(requestCreator),
            .Database = Database,
            .Token = CredentialsProvider ? TMaybe<TString>(CredentialsProvider->GetAuthInfo()) : Nothing(),
            .Promise = promise,
            .OperationName = "local_topic_rpc_operation",
        });
        ActorSystem->Register(actor, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
        return promise.GetFuture();
    }

    TActorSystem* const ActorSystem = nullptr;
    const ui64 ChannelBufferSize = 0;
    const TString Database;
    const std::shared_ptr<ICredentialsProvider> CredentialsProvider;
};

} // anonymous namespace

NYql::ITopicClient::TPtr CreateLocalTopicClient(const TLocalTopicClientSettings& localSettings, const TTopicClientSettings& clientSettings) {
    return MakeIntrusive<TLocalTopicClient>(localSettings, clientSettings);
}

} // namespace NKikimr::NKqp
