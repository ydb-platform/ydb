#pragma once

#include "kqp_event_ids.h"
#include "simple/helpers.h"
#include "simple/query_id.h"
#include "simple/query_ref.h"
#include "simple/settings.h"
#include "simple/services.h"
#include "events/events.h"
#include "compilation/result.h"
#include "shutdown/controller.h"

#include <library/cpp/lwtrace/shuttle.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/cancelation/cancelation.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/guid.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <map>

namespace NKikimr::NKqp {

void ConvertKqpQueryResultToDbResult(const NKikimrMiniKQL::TResult& from, Ydb::ResultSet* to);

TString ScriptExecutionRunnerActorIdString(const NActors::TActorId& actorId);
bool ScriptExecutionRunnerActorIdFromString(const TString& executionId, TActorId& actorId);

template<typename TFrom, typename TTo>
inline void ConvertKqpQueryResultsToDbResult(const TFrom& from, TTo* to) {
    const auto& results = from.GetResults();
    for (const auto& result : results) {
        ConvertKqpQueryResultToDbResult(result, to->add_result_sets());
    }
}

class TKqpRequestInfo {
public:
    TKqpRequestInfo(const TString& traceId, const TString& sessionId)
        : TraceId(traceId)
        , SessionId(sessionId) {}

    TKqpRequestInfo(const TString& traceId)
        : TraceId(traceId)
        , SessionId() {}

    TKqpRequestInfo()
        : TraceId()
        , SessionId() {}

    const TString GetTraceId() const {
        return TraceId;
    }

    const TString GetSessionId() const {
        return SessionId;
    }

private:
    TString TraceId;
    TString SessionId;
};

class IQueryReplayBackend : public TNonCopyable {
public:

    /// Collect details about query:
    /// Accepts query text
    virtual void Collect(const TString& queryData) = 0;

    virtual bool IsNull() { return false; } 

    virtual ~IQueryReplayBackend() {};

    //// Updates configuration onn running backend, if applicable.
    virtual void UpdateConfig(const NKikimrConfig::TTableServiceConfig& serviceConfig) = 0;
};


class TNullQueryReplayBackend : public IQueryReplayBackend {
public:
    void Collect(const TString&) {
    }

    virtual void UpdateConfig(const NKikimrConfig::TTableServiceConfig&) {
    }

    bool IsNull() {
        return true;
    }

    ~TNullQueryReplayBackend() {
    }
};

class IQueryReplayBackendFactory {
public:
    virtual ~IQueryReplayBackendFactory() {}
    virtual IQueryReplayBackend *Create(
        const NKikimrConfig::TTableServiceConfig& serviceConfig,
        TIntrusivePtr<TKqpCounters> counters) = 0;
};

inline IQueryReplayBackend* CreateQueryReplayBackend(
        const NKikimrConfig::TTableServiceConfig& serviceConfig,
        TIntrusivePtr<TKqpCounters> counters,
        std::shared_ptr<IQueryReplayBackendFactory> factory) {
    if (!factory) {
        return new TNullQueryReplayBackend();
    } else {
        return factory->Create(serviceConfig, std::move(counters));
    }
}

static inline IOutputStream& operator<<(IOutputStream& stream, const TKqpRequestInfo& requestInfo) {
    if (!requestInfo.GetTraceId().empty()) {
        stream << "TraceId: \"" << requestInfo.GetTraceId() << "\", ";
    }
    if (!requestInfo.GetSessionId().empty()) {
        stream << "SessionId: " << requestInfo.GetSessionId() << ", ";
    }

    return stream;
}

} // namespace NKikimr::NKqp
