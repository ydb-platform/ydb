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
//   WARN  — failed completed only, single record; SQL/issues cut to 6 KB / 1 KB.
//   DEBUG — adds successful completed at DEBUG, same single-record caps.
//   TRACE — successful completed at TRACE, full SQL/issues, multi-part records.
//
// Part-1 rsyslog budget ($MaxMessageSize=8 KB):
//   ~64 B log prefix + ~10 B [REQ_JSON] + ~950 B envelope/completed fields
//   + up to 6 KB data + up to 1 KB issues ≈ 8.0 KB.
//
// Multi-part layout (TRACE only):
//   part==1: envelope + request{event, data, issues slice, completed fields}.
//   part>1:  envelope + request{data or issues slice}. Reassemble by req_id/part.
// Top-level `kind` = "completed" on part==1, "continuation" on part>1.
#define KQP_REQ_LOG(logQuery) \
    do { \
        if (IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_WARN, NKikimrServices::KQP_REQUEST, 0ull)) { \
            (logQuery).Log(); \
        } \
    } while (0)

} // namespace NKikimr::NKqp
