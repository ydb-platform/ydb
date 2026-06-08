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
//   WARN  — failed completed envelopes only, single record.
//   DEBUG — adds successful completed at DEBUG, SQL cut to 10 KB, single record.
//   TRACE — successful completed bumped to TRACE, full SQL, multi-part records.
//
// Failures always emit at WARN regardless of operator-selected verbosity.
// req_id == ProxyRequestId, correlates with STLOG proxy_request_id.
// BUILTIN_ACL_METADATA traffic skips success-path logs; failures still emit.
#define KQP_REQ_LOG(logQuery) \
    do { \
        if (IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_WARN, NKikimrServices::KQP_REQUEST, 0ull)) { \
            (logQuery).Log(); \
        } \
    } while (0)

} // namespace NKikimr::NKqp
