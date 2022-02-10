#include "client.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <logbroker/public/api/grpc/config_manager.grpc.pb.h>

#include <library/cpp/grpc/client/grpc_client_low.h>

#include <util/string/builder.h>

#include <exception>

namespace NPq::NConfigurationManager {

class TConnections::TImpl : public TAtomicRefCount<TConnections::TImpl> {
public:
    explicit TImpl(size_t numWorkerThreads)
        : Client(numWorkerThreads)
    {
    }

    void Stop(bool wait = false) {
        Client.Stop(wait);
    }

    NGrpc::TGRpcClientLow& GetClient() {
        return Client;
    }

private:
    NGrpc::TGRpcClientLow Client;
};

EStatus ToStatus(const NGrpc::TGrpcStatus& status) {
    if (status.InternalError) {
        return EStatus::INTERNAL_ERROR;
    }

    switch (status.GRpcStatusCode) {
    case grpc::StatusCode::OK:
        return EStatus::SUCCESS;
    case grpc::StatusCode::CANCELLED:
        return EStatus::CANCELLED;
    case grpc::StatusCode::UNKNOWN:
        return EStatus::UNDETERMINED;
    case grpc::StatusCode::INVALID_ARGUMENT:
        return EStatus::BAD_REQUEST;
    case grpc::StatusCode::DEADLINE_EXCEEDED:
        return EStatus::TIMEOUT;
    case grpc::StatusCode::NOT_FOUND:
        return EStatus::NOT_FOUND;
    case grpc::StatusCode::ALREADY_EXISTS:
        return EStatus::ALREADY_EXISTS;
    case grpc::StatusCode::PERMISSION_DENIED:
        return EStatus::UNAUTHORIZED;
    case grpc::StatusCode::UNAUTHENTICATED:
        return EStatus::UNAUTHORIZED;
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
        return EStatus::SESSION_BUSY;
    case grpc::StatusCode::FAILED_PRECONDITION:
        return EStatus::BAD_REQUEST;
    case grpc::StatusCode::ABORTED:
        return EStatus::ABORTED;
    case grpc::StatusCode::OUT_OF_RANGE:
        return EStatus::GENERIC_ERROR;
    case grpc::StatusCode::UNIMPLEMENTED:
        return EStatus::UNSUPPORTED;
    case grpc::StatusCode::INTERNAL:
        return EStatus::INTERNAL_ERROR;
    case grpc::StatusCode::UNAVAILABLE:
        return EStatus::UNAVAILABLE;
    case grpc::StatusCode::DATA_LOSS:
        return EStatus::GENERIC_ERROR;
    default:
        return EStatus::STATUS_CODE_UNSPECIFIED;
    }
}

EStatus ToStatus(Ydb::StatusIds::StatusCode status) {
    switch (status) {
    case Ydb::StatusIds::SUCCESS:
        return EStatus::SUCCESS;
    case Ydb::StatusIds::BAD_REQUEST:
        return EStatus::BAD_REQUEST;
    case Ydb::StatusIds::UNAUTHORIZED:
        return EStatus::UNAUTHORIZED;
    case Ydb::StatusIds::INTERNAL_ERROR:
        return EStatus::INTERNAL_ERROR;
    case Ydb::StatusIds::ABORTED:
        return EStatus::ABORTED;
    case Ydb::StatusIds::UNAVAILABLE:
        return EStatus::UNAVAILABLE;
    case Ydb::StatusIds::OVERLOADED:
        return EStatus::OVERLOADED;
    case Ydb::StatusIds::SCHEME_ERROR:
        return EStatus::SCHEME_ERROR;
    case Ydb::StatusIds::GENERIC_ERROR:
        return EStatus::GENERIC_ERROR;
    case Ydb::StatusIds::TIMEOUT:
        return EStatus::TIMEOUT;
    case Ydb::StatusIds::BAD_SESSION:
        return EStatus::BAD_SESSION;
    case Ydb::StatusIds::PRECONDITION_FAILED:
        return EStatus::PRECONDITION_FAILED;
    case Ydb::StatusIds::ALREADY_EXISTS:
        return EStatus::ALREADY_EXISTS;
    case Ydb::StatusIds::NOT_FOUND:
        return EStatus::NOT_FOUND;
    case Ydb::StatusIds::SESSION_EXPIRED:
        return EStatus::SESSION_EXPIRED;
    case Ydb::StatusIds::CANCELLED:
        return EStatus::CANCELLED;
    case Ydb::StatusIds::UNDETERMINED:
        return EStatus::UNDETERMINED;
    case Ydb::StatusIds::UNSUPPORTED:
        return EStatus::UNSUPPORTED;
    case Ydb::StatusIds::SESSION_BUSY:
        return EStatus::SESSION_BUSY;
    default:
        return EStatus::STATUS_CODE_UNSPECIFIED;
    }
}

TString MakeErrorText(const Ydb::Operations::Operation& operation) {
    NYql::TIssues issues;
    IssuesFromMessage(operation.issues(), issues);
    return issues.ToString();
}

ui64 GetValue(const NLogBroker::IntOrDefaultValue& value) {
    if (value.user_defined()) {
        return value.user_defined();
    } else {
        return value.default_();
    }
}

class TClient : public IClient {
public:
    TClient(const TClientOptions& options, TIntrusivePtr<TConnections::TImpl> connections)
        : Options(options)
        , Connections(std::move(connections))
        , Connection(Connections->GetClient().CreateGRpcServiceConnection<NLogBroker::ConfigurationManagerService>(MakeConnectionConfig()))
    {
    }

    template <class TDescriptionType, class TObjectProps>
    static TDescribePathResult MakeCommonResult(const NLogBroker::DescribePathResult& result, const TObjectProps& props) {
        TDescribePathResult ret = TDescribePathResult::Make<TDescriptionType>(result.path().path());
        TDescriptionType& desc = std::get<TDescriptionType>(ret.GetDescriptionVariant());
        for (const auto& kv : props.metadata()) {
            desc.Metadata[kv.key()] = kv.value();
        }
        return ret;
    }

    static TDescribePathResult MakeResult(const NLogBroker::DescribePathResult& result, const NLogBroker::DescribeAccountResult& props) {
        TDescribePathResult ret = MakeCommonResult<TAccountDescription>(result, props);
        return ret;
    }

    static TDescribePathResult MakeResult(const NLogBroker::DescribePathResult& result, const NLogBroker::DescribeDirectoryResult& props) {
        TDescribePathResult ret = MakeCommonResult<TPathDescription>(result, props);
        return ret;
    }

    static TDescribePathResult MakeResult(const NLogBroker::DescribePathResult& result, const NLogBroker::DescribeTopicResult& props) {
        TDescribePathResult ret = MakeCommonResult<TTopicDescription>(result, props);
        std::get<TTopicDescription>(ret.GetDescriptionVariant()).PartitionsCount = static_cast<size_t>(GetValue(props.properties().partitions_count()));
        return ret;
    }

    static TDescribePathResult MakeResult(const NLogBroker::DescribePathResult& result, const NLogBroker::DescribeConsumerResult& props) {
        TDescribePathResult ret = MakeCommonResult<TConsumerDescription>(result, props);
        return ret;
    }

    static auto DescribePathResultCallback(const NThreading::TPromise<TDescribePathResult>& promise, const TString& path) {
        return [promise = promise, path = path](NGrpc::TGrpcStatus&& status,
                                                NLogBroker::DescribePathResponse&& response) mutable {
            if (!status.Ok()) {
                promise.SetException(
                    std::make_exception_ptr(
                        TException(ToStatus(status))
                            << "Failed to describe path '" << path << "'. Grpc status: " << status.GRpcStatusCode << " (" << status.Msg << ")"));
                return;
            }
            if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
                promise.SetException(
                    std::make_exception_ptr(
                        TException(ToStatus(response.operation().status()))
                            << "Failed to describe path '" << path << "'. Ydb status: " << Ydb::StatusIds::StatusCode_Name(response.operation().status())
                            << " (" << MakeErrorText(response.operation()) << ")"));
                return;
            }
            NLogBroker::DescribePathResult result;
            response.operation().result().UnpackTo(&result);

            switch (result.result_case()) {
            case NLogBroker::DescribePathResult::RESULT_NOT_SET:
                {
                    promise.SetException(
                        std::make_exception_ptr(
                            TException(EStatus::INTERNAL_ERROR)
                                << "Failed to describe path '" << path << "': object type for path is not set by logbroker configuration manager"));
                    break;
                }
            case NLogBroker::DescribePathResult::kAccount:
                {
                    promise.SetValue(MakeResult(result, result.account()));
                    break;
                }
            case NLogBroker::DescribePathResult::kDirectory:
                {
                    promise.SetValue(MakeResult(result, result.directory()));
                    break;
                }
            case NLogBroker::DescribePathResult::kTopic:
                {
                    promise.SetValue(MakeResult(result, result.topic()));
                    break;
                }
            case NLogBroker::DescribePathResult::kConsumer:
                {
                    promise.SetValue(MakeResult(result, result.consumer()));
                    break;
                }
            }
        };
    }

    TAsyncDescribePathResult DescribePath(const TString& path) const override {
        NLogBroker::DescribePathRequest describeRequest;
        describeRequest.set_token(Options.GetCredentialsProviderFactory()->CreateProvider()->GetAuthInfo());
        describeRequest.mutable_path()->set_path(path);

        NThreading::TPromise<TDescribePathResult> promise = NThreading::NewPromise<TDescribePathResult>();
        Connection->DoRequest<NLogBroker::DescribePathRequest, NLogBroker::DescribePathResponse>(
            describeRequest,
            DescribePathResultCallback(promise, path),
            &NLogBroker::ConfigurationManagerService::Stub::AsyncDescribePath);
        return promise.GetFuture();
    }

private:
    NGrpc::TGRpcClientConfig MakeConnectionConfig() const {
        NGrpc::TGRpcClientConfig cfg(Options.GetEndpoint(), Options.GetRequestTimeout());
        cfg.CompressionAlgoritm = GRPC_COMPRESS_GZIP;
        cfg.EnableSsl = Options.GetEnableSsl(); 
        return cfg;
    }

private:
    const TClientOptions Options;
    TIntrusivePtr<TConnections::TImpl> Connections;
    std::shared_ptr<NGrpc::TServiceConnection<NLogBroker::ConfigurationManagerService>> Connection;
};

TConnections::TConnections(size_t numWorkerThreads)
    : Impl(MakeIntrusive<TImpl>(numWorkerThreads))
{
}

TConnections::~TConnections() = default;

void TConnections::Stop(bool wait) {
    Impl->Stop(wait);
}

IClient::TPtr TConnections::GetClient(const TClientOptions& options) {
    return MakeIntrusive<TClient>(options, Impl);
}

} // namespace NPq::NConfigurationManager
