#pragma once

#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NYql::NConnector::NTest {

    template <class TResponse>
    class TStreamIteratorMock: public IStreamIterator<TResponse> {
    public:
        using TPtr = typename std::shared_ptr<TStreamIteratorMock<TResponse>>;

        TStreamIteratorMock<TResponse>()
            : Responses_({})
            , Index_(0)
        {
        }

        virtual TAsyncResult<TResponse> ReadNext() override {
            if (Index_ < Responses_.size()) {
                // return predefined message
                auto future = NThreading::MakeFuture<TResult<TResponse>>({NYdbGrpc::TGrpcStatus(), Responses_[Index_]});
                Index_++;
                return future;
            }

            // special message - mark of the end of the stream
            auto future = NThreading::MakeFuture<TResult<TResponse>>(
                {NYdbGrpc::TGrpcStatus("Read EOF", grpc::StatusCode::OUT_OF_RANGE, false),
                 std::nullopt});

            return future;
        }

        TVector<TResponse>& Responses() {
            return Responses_;
        }

        ~TStreamIteratorMock() {
        }

    private:
        TVector<TResponse> Responses_;
        std::size_t Index_;
    };

    using TListSplitsStreamIteratorMock = TStreamIteratorMock<NApi::TListSplitsResponse>;
    using TReadSplitsStreamIteratorMock = TStreamIteratorMock<NApi::TReadSplitsResponse>;
}
