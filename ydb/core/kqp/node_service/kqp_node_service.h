#pragma once

#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_pb.h>

namespace NYql {
namespace NDq {
    struct TComputeRuntimeSettings;
    struct TComputeMemoryLimits;
} // namespace NDq
} // namespace NYql

namespace NKikimr {
namespace NKqp {

struct TKqpNodeEvents {
    enum EKqpNodeEvents {
        EvStartKqpTasksRequest = EventSpaceBegin(TKikimrEvents::ES_KQP) + 320,
        EvStartKqpTasksResponse,
        EvFinishKqpTasks,
        EvCancelKqpTasksRequest,
        EvCancelKqpTasksResponse,
    };
};

struct TEvKqpNode {
    struct TEvStartKqpTasksRequest : public TEventPB<TEvStartKqpTasksRequest,
        NKikimrKqp::TEvStartKqpTasksRequest, TKqpNodeEvents::EvStartKqpTasksRequest> {};

    struct TEvStartKqpTasksResponse : public TEventPB<TEvStartKqpTasksResponse,
        NKikimrKqp::TEvStartKqpTasksResponse, TKqpNodeEvents::EvStartKqpTasksResponse> {};

    struct TEvFinishKqpTask : public TEventLocal<TEvFinishKqpTask, TKqpNodeEvents::EvFinishKqpTasks> {
        const ui64 TxId;
        const ui64 TaskId;
        const bool Success;
        const NYql::TIssues Issues;

        TEvFinishKqpTask(ui64 txId, ui64 taskId, bool success, const NYql::TIssues& issues = {})
            : TxId(txId)
            , TaskId(taskId)
            , Success(success)
            , Issues(issues) {}
    };

    struct TEvCancelKqpTasksRequest : public TEventPB<TEvCancelKqpTasksRequest,
        NKikimrKqp::TEvCancelKqpTasksRequest, TKqpNodeEvents::EvCancelKqpTasksRequest> {};

    struct TEvCancelKqpTasksResponse : public TEventPB<TEvCancelKqpTasksResponse,
        NKikimrKqp::TEvCancelKqpTasksResponse, TKqpNodeEvents::EvCancelKqpTasksResponse> {};
};

struct IKqpNodeComputeActorFactory {
    virtual ~IKqpNodeComputeActorFactory() = default;

    virtual IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask&& task,
        const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits) = 0;
};

NActors::IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    TIntrusivePtr<TKqpCounters> counters, IKqpNodeComputeActorFactory* caFactory = nullptr, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory = nullptr);

} // namespace NKqp
} // namespace NKikimr
