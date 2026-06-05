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

    TLogQuery() = default;

    explicit TLogQuery(TAction action)
        : Action(std::move(action))
    {}

    void Log() const { if (Action) Action(); }

    static TLogQuery Completed(const TKqpQueryState& state,
                               const NKikimrKqp::TEvQueryResponse& record,
                               ui64 responseByteSize);

private:
    TAction Action;
};

// KQP_REQUEST [REQ_JSON] log contract:
//   WARN  — only failed completed envelopes.
//   DEBUG — successful completed too.
//   TRACE — same events as DEBUG.
//
// SQL is truncated to 10 KB at WARN and DEBUG; TRACE emits the full text.
// req_id is the ProxyRequestId, correlates with STLOG proxy_request_id.
// Queries prefixed with /*UI-QUERY-EXCLUDE*/ skip success-path logs;
// failures still emit at WARN.
#define KQP_REQ_LOG(logQuery) \
    do { \
        if (IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_WARN, NKikimrServices::KQP_REQUEST, 0ull)) { \
            (logQuery).Log(); \
        } \
    } while (0)

} // namespace NKikimr::NKqp
