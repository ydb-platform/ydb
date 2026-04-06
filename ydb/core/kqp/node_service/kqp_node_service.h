#pragma once

#include <ydb/core/kqp/compute_actor/kqp_compute_actor_factory.h>

#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/core/util/stlog.h>

namespace NYql::NDq {
    struct TComputeRuntimeSettings;
    struct TComputeMemoryLimits;
} // namespace NYql::NDq

namespace NKikimr::NKqp {

#define STLOG_C(MESSAGE, ...) STLOG(PRI_CRIT, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_E(MESSAGE, ...) STLOG(PRI_ERROR, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_W(MESSAGE, ...) STLOG(PRI_WARN, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_N(MESSAGE, ...) STLOG(PRI_NOTICE, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_I(MESSAGE, ...) STLOG(PRI_INFO, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_D(MESSAGE, ...) STLOG(PRI_DEBUG, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_T(MESSAGE, ...) STLOG(PRI_TRACE, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)

static constexpr double SecToUsec = 1e6;

struct TKqpNodeEvents {
    enum EKqpNodeEvents {
        EvStartKqpTasksRequest = EventSpaceBegin(TKikimrEvents::ES_KQP) + 320,
        EvStartKqpTasksResponse,
        __EvFinishKqpTasks, // deprecated
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

    struct TEvCancelKqpTasksRequest : public TEventPB<TEvCancelKqpTasksRequest,
        NKikimrKqp::TEvCancelKqpTasksRequest, TKqpNodeEvents::EvCancelKqpTasksRequest> {};

    struct TEvCancelKqpTasksResponse : public TEventPB<TEvCancelKqpTasksResponse,
        NKikimrKqp::TEvCancelKqpTasksResponse, TKqpNodeEvents::EvCancelKqpTasksResponse> {};
};

NYql::NDq::TReportStatsSettings ReportStatsSettingsFromProto(const NYql::NDqProto::TComputeRuntimeSettings& runtimeSettings);

NActors::IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
    TIntrusivePtr<TKqpCounters> counters, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory = nullptr,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup = std::nullopt);

} // namespace NKikimr::NKqp
