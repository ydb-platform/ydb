#pragma once

#include "kqp_node_state.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_actor_factory.h>

#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/tx_datashard.pb.h>

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
    struct TEvStartKqpTasksRequest : public TEventPBWithArena<TEvStartKqpTasksRequest, NKikimrKqp::TEvStartKqpTasksRequest, TKqpNodeEvents::EvStartKqpTasksRequest> {
        using TBaseEv = TEventPBWithArena<TEvStartKqpTasksRequest, NKikimrKqp::TEvStartKqpTasksRequest, TKqpNodeEvents::EvStartKqpTasksRequest>;
        using TBaseEv::TEventPBBase;

        TEvStartKqpTasksRequest() = default;
        explicit TEvStartKqpTasksRequest(TIntrusivePtr<NActors::TProtoArenaHolder> arena)
            : TEventPBBase(std::move(arena))
        {}
    };

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


struct TNodeServiceState : public NKikimr::NKqp::NComputeActor::IKqpNodeState {
    TNodeServiceState() = default;
    static constexpr ui64 BucketsCount = 64;

public:
    void OnTaskTerminate(ui64 txId, ui64 taskId, bool success) {
        auto& bucket = GetStateBucketByTx(txId);
        bucket.RemoveTask(txId, taskId, success);
    }

    NKqpNode::TState& GetStateBucketByTx(ui64 txId) {
        return Buckets[txId % Buckets.size()];
    }

    std::array<NKqpNode::TState, BucketsCount> Buckets;
};

NActors::IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
    TIntrusivePtr<TKqpCounters> counters, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory = nullptr,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup = std::nullopt);

} // namespace NKqp
} // namespace NKikimr
