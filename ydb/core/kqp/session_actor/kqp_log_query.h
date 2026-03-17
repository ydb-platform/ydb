#pragma once

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/stream/output.h>
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

    static TLogQuery Completed(const TKqpQueryState& state,
                               const NKikimrKqp::TEvQueryResponse& record);

private:
    TAction Action;
};

#define KQP_REQ_LOG(logQuery) \
    do { \
        if (IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, NActors::NLog::PRI_TRACE, NKikimrServices::KQP_REQUEST, 0ull)) { \
            (logQuery).Log(); \
        } \
    } while (0)

} // namespace NKikimr::NKqp
