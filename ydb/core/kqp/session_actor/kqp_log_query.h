#pragma once

#include <ydb/core/protos/kqp.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <functional>

namespace NKikimrKqp {
class TEvQueryResponse;
}

namespace NKqpProto {
class TKqpPhyQuery;
}

namespace NKikimr::NKqp {

class TKqpQueryState;

// Returns true if any transaction in the physical query contains a scheme operation
// that may carry sensitive data (CREATE/ALTER USER, CREATE/ALTER SECRET,
// CREATE/UPSERT/ALTER OBJECT with TYPE SECRET).
bool HasSensitiveSchemeOperation(const NKqpProto::TKqpPhyQuery& phyQuery);

class TLogQuery {
public:
    using TAction = std::function<void()>;

    explicit TLogQuery(TAction action)
        : Action(std::move(action))
    {}

    void Log() const { if (Action) Action(); }

    // Logs "started" event, returns the generated req_id for correlation with Completed
    static TString LogStarted(const TKqpQueryState& state);

    static void LogCompleted(const TKqpQueryState& state,
                             const NKikimrKqp::TEvQueryResponse& record,
                             const TString& reqId);

    // Special "completed" path for scripting queries (QUERY_TYPE_SQL_SCRIPT /
    // _SCRIPT_STREAMING). The session actor forwards such requests to KqpWorkerActor,
    // which releases RequestEv during the handoff — so by the time the response arrives
    // LogCompleted can no longer access QueryState fields. The caller must snapshot the
    // required fields before forwarding and pass them here, together with the req_id
    // returned by the original LogStarted call so the "started"/"completed" pair stays
    // correlated. AST sub-queries produced internally by KqpWorkerActor are filtered
    // out in LogStarted/LogCompleted, so this is the only "completed" entry the
    // scripting request produces.
    static void LogForwardedCompleted(const TString& queryText,
                                      const TString& database,
                                      NKikimrKqp::EQueryType queryType,
                                      NKikimrKqp::EQueryAction queryAction,
                                      TInstant startTime,
                                      const NKikimrKqp::TEvQueryResponse& record,
                                      const TString& reqId);

private:
    TAction Action;
};

#define KQP_REQ_LOG_ENABLED() \
    IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_TRACE, NKikimrServices::KQP_REQUEST, 0ull)

} // namespace NKikimr::NKqp
