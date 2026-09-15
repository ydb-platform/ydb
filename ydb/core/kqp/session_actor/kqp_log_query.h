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
//   WARN  — failed completed only, single record; SQL/AST/issues capped at 3/2/1 KB.
//   DEBUG — adds successful completed at DEBUG, same single-record caps.
//   TRACE — successful completed at TRACE, full SQL/AST, issues capped at 64 KB,
//           multi-part records. Byte budgets apply before JSON/log escaping.
//
// Multi-part layout (TRACE only):
//   part==1: envelope + request{event, data, ast, issues slices, completed fields}.
//   part>1:  envelope + request{data, ast and/or issues slices}.
// Reassemble each text field separately by req_id/part.
// Top-level `kind` = "completed" on part==1, "continuation" on part>1.
// Transaction-control actions without SQL carry `query_text_expected=false`.
// Every completed envelope carries `is_streaming`.
// Resource fields are omitted until the first report. CPU/read bytes accumulate
// across executions; table/source bytes may overlap. Memory is compute quota, not RSS.
// compute_memory_bytes is normally zero on completion; observed_peak_compute_memory_bytes
// is the sampled maximum and can miss allocations between reports.
// AST comes from the prepared query, independently of client stats mode.
// ast_statement_index is zero-based; split statements expose their last prepared part.
// Sensitive SQL also suppresses the AST.
#define KQP_REQ_LOG(logQuery) \
    do { \
        if (IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_WARN, NKikimrServices::KQP_REQUEST, 0ull)) { \
            (logQuery).Log(); \
        } \
    } while (0)

} // namespace NKikimr::NKqp
