#pragma once

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/variant.h>

namespace NYql::NDq {

using TTxId = std::variant<ui64, TString>;

using TLogFunc = std::function<void(const TString& message)>;

using TWakeUpCallback = std::function<void()>;
using TErrorCallback = std::function<void(const TString& error)>;

template <ui32 TEventSpaceBegin, ui32 TEventSpaceDiff = 0>
struct TBaseDqResManEvents {
    enum {
        // New ids must be at the end!
        ES_ALLOCATE_WORKERS_REQUEST = EventSpaceBegin(TEventSpaceBegin) + TEventSpaceDiff,
        ES_ALLOCATE_WORKERS_RESPONSE,
        ES_FREE_WORKERS_NOTIFICATION,
        ES_REGISTER_NODE,
        ES_REGISTER_NODE_RESPONSE,

        ES_CLUSTER_STATUS,
        ES_CLUSTER_STATUS_RESPONSE,
        ES_IS_READY,
        ES_IS_READY_RESPONSE,
        ES_JOB_STOP,
        ES_JOB_STOP_RESPONSE,

        ES_GET_MASTER,
        ES_GET_MASTER_RESPONSE,

        ES_CONFIGURE_FAILURE_INJECTOR,
        ES_CONFIGURE_FAILURE_INJECTOR_RESPONSE,

        ES_QUERY_STATUS,
        ES_QUERY_STATUS_RESPONSE,

        ES_ROUTES,
        ES_ROUTES_RESPONSE,

        ES_OPERATION_STOP,
        ES_OPERATION_STOP_RESPONSE,
    };
};

template <ui32 TEventSpaceBegin, ui32 TEventSpaceDiff = 100>
struct TBaseDqExecuterEvents {
    enum {
        ES_QUERY = EventSpaceBegin(TEventSpaceBegin) + TEventSpaceDiff,
        ES_PROGRAM,
        ES_DQ_TASK,
        ES_READY_TO_PULL,
        ES_PULL_RESULT,
        ES_RESULT_SET,
        ES_DQ_FAILURE,
        ES_GRAPH,
        ES_GRAPH_FINISHED,
        ES_GRAPH_EXECUTION_EVENT,
        ES_STATS,
    };
};

template <ui32 TEventSpaceBegin, ui32 TEventSpaceDiff = 200>
struct TBaseDqDataEvents {
    enum {
        ES_PULL_REQUEST = EventSpaceBegin(TEventSpaceBegin) + TEventSpaceDiff,
        ES_PULL_RESPONSE,
        ES_LOCAL_PULL_RESPONSE,

        ES_PING_REQUEST,
        ES_PING_RESPONSE,
        ES_CONTINUE_RUN,

        ES_FULL_RESULT_WRITER_STATUS_REQUEST,
        ES_FULL_RESULT_WRITER_STATUS_RESPONSE,
        ES_FULL_RESULT_WRITER_WRITE_REQUEST,
        ES_FULL_RESULT_WRITER_ACK,
        ES_MESSAGE_PROCESSED,
    };
};

enum class EHashJoinMode {
    Off         /* "off" */,
    Map         /* "map" */,
    Dict        /* "dict" */,
    Grace       /* "grace" */,
    GraceAndSelf /* "graceandself" */,
};

enum class EEnabledSpillingNodes : ui64 {
    None        = 0ULL      /* "None" */,
    GraceJoin   = 1ULL      /* "GraceJoin" */,
    Aggregation = 2ULL      /* "Aggregation" */,
    All         = ~0ULL     /* "All" */,
};

class TSpillingSettings {
public:
    TSpillingSettings() = default;
    explicit TSpillingSettings(ui64 mask) : Mask(mask) {};

    operator bool() const {
        return Mask;
    }

    bool IsGraceJoinSpillingEnabled() const {
        return Mask & ui64(EEnabledSpillingNodes::GraceJoin);
    }

    bool IsAggregationSpillingEnabled() const {
        return Mask & ui64(EEnabledSpillingNodes::Aggregation);
    }

private:
    const ui64 Mask = 0;
};

} // namespace NYql::NDq

IOutputStream& operator<<(IOutputStream& stream, const NYql::NDq::TTxId& txId);
