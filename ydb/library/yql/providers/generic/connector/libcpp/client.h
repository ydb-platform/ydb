#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <ydb/library/yql/providers/generic/connector/api/service/connector.grpc.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NYql::NConnector {
    template <typename TResponse>
    struct TResult {
        NYdbGrpc::TGrpcStatus Status;
        std::optional<TResponse> Response;
    };

    template <class TResponse>
    using TAsyncResult = NThreading::TFuture<TResult<TResponse>>;

    using TDescribeTableAsyncResult = TAsyncResult<NApi::TDescribeTableResponse>;

    template <class TResponse>
    class TStreamer {
    public:
        using TSelf = TStreamer;

        using TStreamProcessorPtr = typename NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;

        TStreamer(TStreamProcessorPtr streamProcessor)
            : StreamProcessor_(streamProcessor)
            , Finished_(false){};

        TAsyncResult<TResponse> ReadNext(std::shared_ptr<TSelf> self) {
            auto promise = NThreading::NewPromise<TResult<TResponse>>();
            auto readCallback = [self = std::move(self), promise](NYdbGrpc::TGrpcStatus&& status) mutable {
                if (!status.Ok()) {
                    promise.SetValue({std::move(status), std::nullopt});
                    self->Finished_ = true;
                } else {
                    promise.SetValue({std::move(status), std::move(self->Response_)});
                }
            };

            StreamProcessor_->Read(&Response_, readCallback);
            return promise.GetFuture();
        }

        ~TStreamer() {
            StreamProcessor_->Cancel();
        }

        bool IsFinished() const {
            return Finished_;
        }

    private:
        TStreamProcessorPtr StreamProcessor_;
        TResponse Response_;
        bool Finished_;
    };

    template <class TResponse>
    class IStreamIterator {
    public:
        using TPtr = std::shared_ptr<IStreamIterator<TResponse>>;
        using TResult = TAsyncResult<TResponse>;

        virtual TAsyncResult<TResponse> ReadNext() = 0;

        virtual ~IStreamIterator(){};
    };

    using IListSplitsStreamIterator = IStreamIterator<NApi::TListSplitsResponse>;
    using IReadSplitsStreamIterator = IStreamIterator<NApi::TReadSplitsResponse>;

    template <class TResponse>
    class IStreamIteratorDrainer {
    public:
        using TPtr = std::shared_ptr<IStreamIteratorDrainer<TResponse>>;

        struct TResult {
            TVector<TResponse> Responses;
            TIssues Issues;
        };

        IStreamIteratorDrainer(IStreamIterator<TResponse>::TPtr iterator)
            : Iterator_(iterator)
            , Responses_({})
            , Issues_({}) {
        }

        NThreading::TFuture<TResult> Run() {
            auto promise = NThreading::NewPromise<TResult>();
            Next(promise);
            return promise.GetFuture();
        }

        virtual ~IStreamIteratorDrainer(){};

    private:
        IStreamIterator<TResponse>::TPtr Iterator_;

        // Transport issues and stream messages received during stream flushing are accumulated here
        TVector<TResponse> Responses_;
        TIssues Issues_;

        void Next(NThreading::TPromise<TResult> promise) {
            TPtr self(this);

            Iterator_ = Iterator_->ReadNext().Subscribe([self, promise](const TAsyncResult<TResponse> f1) {
                TAsyncResult<TResponse> f2 = f1;
                auto result = f2.ExtractValue();

                // Check transport error
                if (!result.Status.Ok()) {
                    // It could be either EOF (== success), or unexpected error
                    if (!GrpcStatusEndOfStream(result.Status)) {
                        self->Issues_.AddIssue(result.Status.ToDebugString());
                    }

                    promise.SetValue({std::move(self->Responses_), std::move(self->Issues_)});
                    return;
                }

                Y_ENSURE(result.Response);

                self->Responses_.emplace_back(std::move(*result.Response));
                self->Next(promise);
            });
        };
    };

    template <class TIterator>
    struct TIteratorResult {
        NYdbGrpc::TGrpcStatus Status;
        typename TIterator::TPtr Iterator;
    };

    template <class TIterator>
    using TIteratorAsyncResult = NThreading::TFuture<TIteratorResult<TIterator>>;

    using TListSplitsStreamIteratorAsyncResult = TIteratorAsyncResult<IListSplitsStreamIterator>;
    using TReadSplitsStreamIteratorAsyncResult = TIteratorAsyncResult<IReadSplitsStreamIterator>;

    class IClient {
    public:
        using TPtr = std::shared_ptr<IClient>;

        virtual TDescribeTableAsyncResult DescribeTable(const NApi::TDescribeTableRequest& request,
                                                        TDuration timeout = {}) = 0;
        virtual TListSplitsStreamIteratorAsyncResult ListSplits(const NApi::TListSplitsRequest& request,
                                                                TDuration timeout = {}) = 0;
        virtual TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request,
                                                                TDuration timeout = {}) = 0;
        virtual ~IClient() = default;
    };

    IClient::TPtr MakeClientGRPC(const NYql::TGenericConnectorConfig& cfg);
} // namespace NYql::NConnector
