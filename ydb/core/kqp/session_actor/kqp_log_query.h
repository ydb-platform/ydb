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

// WARN: failed queries only; DEBUG: all completed queries. SQL cap: 6 KB - 32 B; AST: 3 KB; issues: 1 KB.
// TRACE: full SQL/AST in parts, issues capped at 64 KB. Caps exclude JSON/log escaping.
// Only part 1 contains completion metadata; reassemble each text field by req_id/part.
// Resource fields are absent before the first report. CPU/read bytes accumulate across
// executions; table/source bytes may overlap. Memory is compute quota, not RSS.
// observed_peak_compute_memory_bytes can miss allocations between reports.
// ast_statement_index is zero-based; split statements expose their last prepared part.
#define KQP_REQ_LOG(logQuery) \
    do { \
        if (IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_WARN, NKikimrServices::KQP_REQUEST, 0ull)) { \
            (logQuery).Log(); \
        } \
    } while (0)

} // namespace NKikimr::NKqp
