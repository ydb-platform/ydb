#include "ymq.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb::Ymq::V1 {

    class TYmqClient::TImpl : public TClientImplCommon<TYmqClient::TImpl> {
    public:
        TImpl(std::shared_ptr <TGRpcConnectionsImpl> &&connections, const TCommonClientSettings &settings)
                : TClientImplCommon(std::move(connections), settings) {}

        template<class TProtoResult, class TResultWrapper>
        auto MakeResultExtractor(NThreading::TPromise <TResultWrapper> promise) {
            return [promise = std::move(promise)]
                    (google::protobuf::Any *any, TPlainStatus status) mutable {
                std::unique_ptr <TProtoResult> result;
                if (any) {
                    result.reset(new TProtoResult);
                    any->UnpackTo(result.get());
                }

                promise.SetValue(
                        TResultWrapper(
                                TStatus(std::move(status)),
                                std::move(result)));
            };
        }

        template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TSettings, class TFillRequestFn, class TAsyncCall>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> CallImpl(const TSettings& settings, TAsyncCall grpcCall, TFillRequestFn fillRequest) {
            using TResultWrapper = TProtoResultWrapper<TProtoResult>;
            auto request = MakeOperationRequest<TProtoRequest>(settings);
            fillRequest(request);

            auto promise = NThreading::NewPromise<TResultWrapper>();
            auto future = promise.GetFuture();

            auto extractor = MakeResultExtractor<TProtoResult, TResultWrapper>(std::move(promise));

            Connections_->RunDeferred<TProtoService, TProtoRequest, TProtoResponse>(
                    std::move(request),
                    std::move(extractor),
                    grpcCall,
                    DbDriverState_,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings));

            return future;

        }

        template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TSettings, class TAsyncCall>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> CallImpl(const TSettings& settings, TAsyncCall grpcCall) {
            return CallImpl<TProtoService, TProtoRequest, TProtoResponse, TProtoResult>(settings, grpcCall, [](TProtoRequest&) {});
        }

        TAsyncGetQueueUrlResult GetQueueUrl(const TString &queueName, TGetQueueUrlSettings settings) {
            return CallImpl<Ydb::Ymq::V1::YmqService,
                    Ydb::Ymq::V1::GetQueueUrlRequest,
                    Ydb::Ymq::V1::GetQueueUrlResponse,
                    Ydb::Ymq::V1::GetQueueUrlResult>(settings,
                        &Ydb::Ymq::V1::YmqService::Stub::AsyncGetQueueUrl,
                        [&](Ydb::Ymq::V1::GetQueueUrlRequest& req) {
                            req.set_queue_name(queueName);
                        }
            );
        }
        
        TAsyncCreateQueueResult CreateQueue(const TString &queueName, TCreateQueueSettings settings) {
            return CallImpl<Ydb::Ymq::V1::YmqService,
                    Ydb::Ymq::V1::CreateQueueRequest,
                    Ydb::Ymq::V1::CreateQueueResponse,
                    Ydb::Ymq::V1::CreateQueueResult>(settings,
                        &Ydb::Ymq::V1::YmqService::Stub::AsyncCreateQueue,
                        [&](Ydb::Ymq::V1::CreateQueueRequest& req) {
                            req.set_queue_name(queueName);
                        }
            );
        }

        TAsyncSendMessageResult SendMessage(const TString &queueUrl, const TString &body, TSendMessageSettings settings) {
            return CallImpl<Ydb::Ymq::V1::YmqService,
                    Ydb::Ymq::V1::SendMessageRequest,
                    Ydb::Ymq::V1::SendMessageResponse,
                    Ydb::Ymq::V1::SendMessageResult>(settings,
                        &Ydb::Ymq::V1::YmqService::Stub::AsyncSendMessage,
                        [&](Ydb::Ymq::V1::SendMessageRequest& req) {
                            req.set_queue_url(queueUrl);
                            req.set_message_body(body);
                        }
            );
        }

        template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> DoProtoRequest(const TProtoRequest& proto, TMethod method, const TProtoRequestSettings& settings) {
            return CallImpl<Ydb::Ymq::V1::YmqService, TProtoRequest, TProtoResponse, TProtoResult>(settings, method,
               [&](TProtoRequest& req) {
                    req.CopyFrom(proto);
               });
        }
    };

    TYmqClient::TYmqClient(const TDriver& driver, const TCommonClientSettings& settings)
            : Impl_(new TImpl(CreateInternalInterface(driver), settings))
    {
    }

    TAsyncGetQueueUrlResult TYmqClient::GetQueueUrl(const TString& path, TGetQueueUrlSettings& settings) {
        return Impl_->GetQueueUrl(path, settings);
    }

    template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
    NThreading::TFuture<TProtoResultWrapper<TProtoResult>> TYmqClient::DoProtoRequest(const TProtoRequest& request, TMethod method, TProtoRequestSettings settings) {
        return Impl_->DoProtoRequest<TProtoRequest, TProtoResponse, TProtoResult, TMethod>(request, method, settings);
    }

    template NThreading::TFuture<TProtoResultWrapper<Ydb::Ymq::V1::GetQueueUrlResult>> TYmqClient::DoProtoRequest
                <
                    Ydb::Ymq::V1::GetQueueUrlRequest,
                    Ydb::Ymq::V1::GetQueueUrlResponse,
                    Ydb::Ymq::V1::GetQueueUrlResult,
                    decltype(&Ydb::Ymq::V1::YmqService::Stub::AsyncGetQueueUrl)
                >(
                        const Ydb::Ymq::V1::GetQueueUrlRequest& request,
                        decltype(&Ydb::Ymq::V1::YmqService::Stub::AsyncGetQueueUrl) method,
                        TProtoRequestSettings settings
                );
}

