#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/public/api/grpc/draft/ydb_ymq_v1.grpc.pb.h>

namespace NYdb::Ymq::V1 {

    template<class TProtoResult>
    class TProtoResultWrapper : public NYdb::TStatus {
        friend class TYmqClient;

    private:
        TProtoResultWrapper(
                NYdb::TStatus&& status,
                std::unique_ptr<TProtoResult> result)
                : TStatus(std::move(status))
                , Result(std::move(result))
        { }

    public:
        const TProtoResult& GetResult() const {
            Y_ABORT_UNLESS(Result, "Uninitialized result");
            return *Result;
        }

    private:
        std::unique_ptr<TProtoResult> Result;
    };

    enum EStreamMode {
        ESM_PROVISIONED = 1,
        ESM_ON_DEMAND = 2,
    };

    using TGetQueueUrlResult = TProtoResultWrapper<Ydb::Ymq::V1::GetQueueUrlResult>;
    using TAsyncGetQueueUrlResult = NThreading::TFuture<TGetQueueUrlResult>;

    using TCreateQueueResult = TProtoResultWrapper<Ydb::Ymq::V1::CreateQueueResult>;
    using TAsyncCreateQueueResult = NThreading::TFuture<TCreateQueueResult>;

    using TSendMessageResult = TProtoResultWrapper<Ydb::Ymq::V1::SendMessageResult>;
    using TAsyncSendMessageResult = NThreading::TFuture<TSendMessageResult>;

    struct TDataRecord {
        TString Data;
        TString PartitionKey;
        TString ExplicitHashDecimal;
    };

    struct TGetQueueUrlSettings : public NYdb::TOperationRequestSettings<TGetQueueUrlSettings> {};
    struct TCreateQueueSettings : public NYdb::TOperationRequestSettings<TCreateQueueSettings> {};
    struct TSendMessageSettings : public NYdb::TOperationRequestSettings<TSendMessageSettings> {};
    struct TProtoRequestSettings : public NYdb::TOperationRequestSettings<TProtoRequestSettings> {};

    class TYmqClient {
        class TImpl;

    public:
        TYmqClient(const NYdb::TDriver& driver, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings());

        TAsyncGetQueueUrlResult GetQueueUrl(const TString& queueName, TGetQueueUrlSettings& getQueueUrlSettings);
        TAsyncCreateQueueResult CreateQueue(const TString& queueName, TCreateQueueSettings& createQueueSettings);

        template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> DoProtoRequest(const TProtoRequest& request, TMethod method, TProtoRequestSettings settings = TProtoRequestSettings());

        NThreading::TFuture<void> DiscoveryCompleted();

    private:
        std::shared_ptr<TImpl> Impl_;
    };

}
