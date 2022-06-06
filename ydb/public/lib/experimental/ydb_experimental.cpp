#include "ydb_experimental.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_experimental_v1.grpc.pb.h>
#include <library/cpp/grpc/client/grpc_client_low.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb {
namespace NExperimental {

using namespace NThreading;


TStreamPartStatus::TStreamPartStatus(TStatus&& status)
    : TStatus(std::move(status))
{}

bool TStreamPartStatus::EOS() const {
    return GetStatus() == EStatus::CLIENT_OUT_OF_RANGE;
}

class TStreamPartIterator::TReaderImpl {
public:
    using TSelf = TStreamPartIterator::TReaderImpl;
    using TResponse = Ydb::Experimental::ExecuteStreamQueryResponse;
    using TStreamProcessorPtr = NGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    using TReadCallback = NGrpc::IStreamRequestReadProcessor<TResponse>::TReadCallback;
    using TGRpcStatus = NGrpc::TGrpcStatus;
    using TBatchReadResult = std::pair<TResponse, TGRpcStatus>;

    TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint)
        : StreamProcessor_(streamProcessor)
        , Finished_(false)
        , Endpoint_(endpoint)
    {}

    ~TReaderImpl() {
        StreamProcessor_->Cancel();
    }

    bool IsFinished() const {
        return Finished_;
    }

    TAsyncStreamPart ReadNext(std::shared_ptr<TSelf> self) {
        auto promise = NThreading::NewPromise<TStreamPart>();
        // Capture self - guarantee no dtor call during the read
        auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
            if (!grpcStatus.Ok()) {
                self->Finished_ = true;
                promise.SetValue({TStatus(TPlainStatus(grpcStatus, self->Endpoint_))});
            } else {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(self->Response_.issues(), issues);
                EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
                TPlainStatus plainStatus{clientStatus, std::move(issues), self->Endpoint_, {}};
                TStatus status{std::move(plainStatus)};

                if (self->Response_.result().has_result_set()) {
                    promise.SetValue({TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set())),
                                      std::move(status)});
                } else if (!self->Response_.result().profile().empty()) {
                    promise.SetValue({std::move(*self->Response_.mutable_result()->mutable_profile()),
                                      std::move(status)});
                } else if (!self->Response_.result().query_plan().Empty()) {
                    TMaybe<TString> queryPlan = self->Response_.result().query_plan();
                    promise.SetValue({queryPlan, std::move(status)});
                } else if (self->Response_.result().has_progress()) {
                    // skip, not supported yet
                } else {
                    promise.SetValue(std::move(status));
                }
            }
        };
        StreamProcessor_->Read(&Response_, readCb);
        return promise.GetFuture();
    }
private:
    TStreamProcessorPtr StreamProcessor_;
    TResponse Response_;
    bool Finished_;
    TString Endpoint_;
};

TStreamPartIterator::TStreamPartIterator(
    std::shared_ptr<TReaderImpl> impl,
    TPlainStatus&& status)
    : TStatus(std::move(status))
    , ReaderImpl_(impl)
{}

TAsyncStreamPart TStreamPartIterator::ReadNext() {
    if (ReaderImpl_->IsFinished())
        RaiseError("Attempt to perform read on invalid or finished stream");
    return ReaderImpl_->ReadNext(ReaderImpl_);
}

class TStreamQueryClient::TImpl : public TClientImplCommon<TStreamQueryClient::TImpl> {
public:
    using TStreamProcessorPtr = TStreamPartIterator::TReaderImpl::TStreamProcessorPtr;

    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    TFuture<std::pair<TPlainStatus, TStreamProcessorPtr>> ExecuteStreamQueryInternal(const TString& query,
        const TParams* params, const TExecuteStreamQuerySettings& settings)
    {
        auto request = MakeRequest<Ydb::Experimental::ExecuteStreamQueryRequest>();
        request.set_yql_text(query);
        if (params) {
            *request.mutable_parameters() = params->GetProtoMap();
        }

        switch (settings.ProfileMode_) {
            case EStreamQueryProfileMode::None:
                request.set_profile_mode(Ydb::Experimental::ExecuteStreamQueryRequest_ProfileMode_NONE);
                break;
            case EStreamQueryProfileMode::Basic:
                request.set_profile_mode(Ydb::Experimental::ExecuteStreamQueryRequest_ProfileMode_BASIC);
                break;
            case EStreamQueryProfileMode::Full:
                request.set_profile_mode(Ydb::Experimental::ExecuteStreamQueryRequest_ProfileMode_FULL);
                break;
            case EStreamQueryProfileMode::Profile:
                request.set_profile_mode(Ydb::Experimental::ExecuteStreamQueryRequest_ProfileMode_PROFILE);
                break;
        }

        request.set_explain(settings.Explain_);

        auto promise = NewPromise<std::pair<TPlainStatus, TStreamProcessorPtr>>();

        Connections_->StartReadStream<
            Ydb::Experimental::V1::ExperimentalService,
            Ydb::Experimental::ExecuteStreamQueryRequest,
            Ydb::Experimental::ExecuteStreamQueryResponse>
        (
            std::move(request),
            [promise] (TPlainStatus status, TStreamProcessorPtr processor) mutable {
                promise.SetValue(std::make_pair(status, processor));
            },
            &Ydb::Experimental::V1::ExperimentalService::Stub::AsyncExecuteStreamQuery,
            DbDriverState_,
            TRpcRequestSettings::Make(settings)
        );

        return promise.GetFuture();
    }

    TAsyncStreamPartIterator ExecuteStreamQuery(const TString& query, const TParams* params,
        const TExecuteStreamQuerySettings& settings)
    {
        auto promise = NewPromise<TStreamPartIterator>();

        auto iteratorCallback = [promise](TFuture<std::pair<TPlainStatus,
            TStreamQueryClient::TImpl::TStreamProcessorPtr>> future) mutable
        {
            Y_ASSERT(future.HasValue());
            auto pair = future.ExtractValue();
            promise.SetValue(TStreamPartIterator(
                pair.second
                    ? std::make_shared<TStreamPartIterator::TReaderImpl>(pair.second, pair.first.Endpoint)
                    : nullptr,
                std::move(pair.first))
            );
        };

        ExecuteStreamQueryInternal(query, params, settings).Subscribe(iteratorCallback);
        return promise.GetFuture();
    }
};

TStreamQueryClient::TStreamQueryClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings)) {}

TParamsBuilder TStreamQueryClient::GetParamsBuilder() {
    return TParamsBuilder();
}

TAsyncStreamPartIterator TStreamQueryClient::ExecuteStreamQuery(const TString& query,
    const TExecuteStreamQuerySettings& settings)
{
    return Impl_->ExecuteStreamQuery(query, nullptr, settings);
}

TAsyncStreamPartIterator TStreamQueryClient::ExecuteStreamQuery(const TString& query, const TParams& params,
    const TExecuteStreamQuerySettings& settings)
{
    return Impl_->ExecuteStreamQuery(query, &params, settings);
}

} // namespace NExperimental
} // namespace NYdb
