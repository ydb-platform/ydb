#include "session_close_command.h"

#include "kqp_session_common.h"
#include "session_client.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <array>

namespace NYdb::inline Dev::NSessionPool {

bool TSessionCloseCommand::Execute(TKqpSessionCommon& session, ISessionClient* client) const {
    const bool firstTerminal = (session.*Transition_)();
    if (firstTerminal && client) {
        client->RecordSessionClosed(Reason_);
    }
    return firstTerminal;
}

namespace NSessionCloseCommands {

const TSessionCloseCommand PoolIdleTimeout(
    "pool_idle_timeout", &TKqpSessionCommon::MarkBroken);
const TSessionCloseCommand PoolGracefulShutdown(
    "pool_graceful_shutdown", &TKqpSessionCommon::MarkBroken);
const TSessionCloseCommand ClientTimeout(
    "client_timeout", &TKqpSessionCommon::MarkBroken);
const TSessionCloseCommand ClientCancelled(
    "client_cancelled", &TKqpSessionCommon::MarkBroken);
const TSessionCloseCommand AttachClosed(
    "attach_closed", &TKqpSessionCommon::MarkBroken);
const TSessionCloseCommand TransportError(
    "transport_error", &TKqpSessionCommon::MarkBroken);
const TSessionCloseCommand NodeShutdown(
    "node_shutdown", &TKqpSessionCommon::MarkAsClosing);
const TSessionCloseCommand SessionShutdown(
    "session_shutdown", &TKqpSessionCommon::MarkAsClosing);
const TSessionCloseCommand BadSession(
    "bad_session", &TKqpSessionCommon::MarkBroken);
const TSessionCloseCommand SessionBusy(
    "session_busy", &TKqpSessionCommon::MarkBroken);

namespace {

using TMatcher = bool (*)(const TStatus&);

struct TStatusRule {
    TMatcher Matches;
    const TSessionCloseCommand* Command;
};

bool HasSessionCloseHint(const TStatus& status) {
    const auto& metadata = status.GetResponseMetadata();
    const auto hints = metadata.equal_range(NYdb::YDB_SERVER_HINTS);
    for (auto it = hints.first; it != hints.second; ++it) {
        if (it->second == NYdb::YDB_SESSION_CLOSE) {
            return true;
        }
    }
    return false;
}

const std::array<TStatusRule, 6> StatusRules = {{
    {[](const TStatus& status) { return status.GetStatus() == EStatus::CLIENT_DEADLINE_EXCEEDED; }, &ClientTimeout},
    {[](const TStatus& status) { return status.GetStatus() == EStatus::CLIENT_CANCELLED; }, &ClientCancelled},
    {[](const TStatus& status) {
        return status.IsTransportError()
            && status.GetStatus() != EStatus::CLIENT_RESOURCE_EXHAUSTED
            && status.GetStatus() != EStatus::CLIENT_OUT_OF_RANGE;
    }, &TransportError},
    {[](const TStatus& status) { return status.GetStatus() == EStatus::SESSION_BUSY; }, &SessionBusy},
    {[](const TStatus& status) {
        return status.GetStatus() == EStatus::BAD_SESSION
            || status.GetStatus() == EStatus::SESSION_EXPIRED;
    }, &BadSession},
    {&HasSessionCloseHint, &SessionShutdown},
}};

} // namespace

const TSessionCloseCommand* FromStatus(const TStatus& status) {
    for (const auto& rule : StatusRules) {
        if (rule.Matches(status)) {
            return rule.Command;
        }
    }
    return nullptr;
}

} // namespace NSessionCloseCommands
} // namespace NYdb::Dev::NSessionPool
