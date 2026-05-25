#pragma once

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <functional>

namespace NKikimrKqp {
class TEvQueryResponse;
}

namespace NKikimr::NKqp {

class TKqpQueryState;

class TLogQuery {
public:
    using TAction = std::function<void()>;

    explicit TLogQuery(TAction action)
        : Action(std::move(action))
    {}

    void Log() const { if (Action) Action(); }

    static TLogQuery Started(const TKqpQueryState& state);

    // responseByteSize: record.ByteSize() from Reply() (same value as ResponseBytes counter).
    static TLogQuery Completed(const TKqpQueryState& state,
                               const NKikimrKqp::TEvQueryResponse& record,
                               ui64 responseByteSize);

private:
    TAction Action;
};

// KQP_REQUEST log contract (component NKikimrServices::KQP_REQUEST, tag [REQ_JSON]):
//
//   WARN  — failed completed only (default-friendly: broken queries visible).
//   DEBUG — started + successful completed; SQL truncated to 10 KB unless TRACE.
//   TRACE — same events as DEBUG, but full SQL text (no truncation).
//
// Macro gate is WARN; lambdas re-check the priority they actually emit at.
// req_id is ProxyRequestId (proxy cookie), correlates with STLOG proxy_request_id.
// results_size on completed is TEvQueryResponse::ByteSize() (ResponseBytes counter).
//
// UI queries prefixed with /*UI-QUERY-EXCLUDE*/ skip success-path logs; failures
// still emit at WARN.
#define KQP_REQ_LOG(logQuery) \
    do { \
        if (IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_WARN, NKikimrServices::KQP_REQUEST, 0ull)) { \
            (logQuery).Log(); \
        } \
    } while (0)

} // namespace NKikimr::NKqp
