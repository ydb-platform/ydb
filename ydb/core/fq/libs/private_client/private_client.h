#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>
#include <ydb/core/fq/libs/grpc/fq_private_v1.grpc.pb.h>

namespace NFq {

template<class TProtoResult>
class TProtoResultInternalWrapper : public NYdb::TStatus {
    friend class TPrivateClient;

public:
    TProtoResultInternalWrapper(
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

    bool IsResultSet() const {
        return Result ? true : false;
    }

private:
    std::unique_ptr<TProtoResult> Result;
};


using TGetTaskResult = TProtoResultInternalWrapper<Fq::Private::GetTaskResult>;
using TPingTaskResult = TProtoResultInternalWrapper<Fq::Private::PingTaskResult>;
using TWriteTaskResult = TProtoResultInternalWrapper<Fq::Private::WriteTaskResultResult>;
using TNodesHealthCheckResult = TProtoResultInternalWrapper<Fq::Private::NodesHealthCheckResult>;
using TCreateRateLimiterResourceResult = TProtoResultInternalWrapper<Fq::Private::CreateRateLimiterResourceResult>;
using TDeleteRateLimiterResourceResult = TProtoResultInternalWrapper<Fq::Private::DeleteRateLimiterResourceResult>;

using TAsyncGetTaskResult = NThreading::TFuture<TGetTaskResult>;
using TAsyncPingTaskResult = NThreading::TFuture<TPingTaskResult>;
using TAsyncWriteTaskResult = NThreading::TFuture<TWriteTaskResult>;
using TAsyncNodesHealthCheckResult = NThreading::TFuture<TNodesHealthCheckResult>;
using TAsyncCreateRateLimiterResourceResult = NThreading::TFuture<TCreateRateLimiterResourceResult>;
using TAsyncDeleteRateLimiterResourceResult = NThreading::TFuture<TDeleteRateLimiterResourceResult>;

struct TGetTaskSettings : public NYdb::TOperationRequestSettings<TGetTaskSettings> {};
struct TPingTaskSettings : public NYdb::TOperationRequestSettings<TPingTaskSettings> {};
struct TWriteTaskResultSettings : public NYdb::TOperationRequestSettings<TWriteTaskResultSettings> {};
struct TNodesHealthCheckSettings : public NYdb::TOperationRequestSettings<TNodesHealthCheckSettings> {};
struct TCreateRateLimiterResourceSettings : public NYdb::TOperationRequestSettings<TCreateRateLimiterResourceSettings> {};
struct TDeleteRateLimiterResourceSettings : public NYdb::TOperationRequestSettings<TDeleteRateLimiterResourceSettings> {};

class TPrivateClient {
    class TImpl;

public:
    TPrivateClient(
        const NYdb::TDriver& driver,
        const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings(),
        const NMonitoring::TDynamicCounterPtr& counters = MakeIntrusive<NMonitoring::TDynamicCounters>());

    TAsyncGetTaskResult GetTask(
        Fq::Private::GetTaskRequest&& request,
        const TGetTaskSettings& settings = TGetTaskSettings());

    TAsyncPingTaskResult PingTask(
        Fq::Private::PingTaskRequest&& request,
        const TPingTaskSettings& settings = TPingTaskSettings());

    TAsyncWriteTaskResult WriteTaskResult(
        Fq::Private::WriteTaskResultRequest&& request,
        const TWriteTaskResultSettings& settings = TWriteTaskResultSettings());

    TAsyncNodesHealthCheckResult NodesHealthCheck(
        Fq::Private::NodesHealthCheckRequest&& request,
        const TNodesHealthCheckSettings& settings = TNodesHealthCheckSettings());

    TAsyncCreateRateLimiterResourceResult CreateRateLimiterResource(
        Fq::Private::CreateRateLimiterResourceRequest&& request,
        const TCreateRateLimiterResourceSettings& settings = TCreateRateLimiterResourceSettings());

    TAsyncDeleteRateLimiterResourceResult DeleteRateLimiterResource(
        Fq::Private::DeleteRateLimiterResourceRequest&& request,
        const TDeleteRateLimiterResourceSettings& settings = TDeleteRateLimiterResourceSettings());

private:
    std::shared_ptr<TImpl> Impl;
};

} // namespace NFq
