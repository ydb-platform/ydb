#include "session_state_handler.h"

#include <ydb/public/sdk/cpp/src/client/impl/session/session_pool.h>

namespace NYdb::inline Dev::NQuery {

EAttachStreamReadAction HandleAttachSessionState(
    const Ydb::Query::SessionState& state,
    TKqpSessionCommon* session,
    const std::shared_ptr<ISessionClient>& client,
    const std::function<void()>& beforeShutdownTransition)
{
    if (state.has_session_shutdown() || state.has_node_shutdown()) {
        if (!session) {
            return EAttachStreamReadAction::Stop;
        }
        const auto& closeCommand = state.has_node_shutdown()
            ? NSessionPool::NSessionCloseCommands::NodeShutdown
            : NSessionPool::NSessionCloseCommands::SessionShutdown;
        if (state.has_node_shutdown()) {
            const auto nodeId = session->GetEndpointKey().GetNodeId();
            if (nodeId != 0 && client) {
                client->PessimizeNode(nodeId);
            }
        }
        if (beforeShutdownTransition) {
            beforeShutdownTransition();
        }
        // Do not separate the state check from MarkAsClosing: the application
        // can return the session to the pool between those two operations.
        const auto previousState = session->MarkAsClosingIfNotIdle();
        if (previousState == TKqpSessionCommon::S_IDLE) {
            if (client) {
                session->CloseFromServer(client, closeCommand.Reason);
            }
        } else if (previousState != TKqpSessionCommon::S_BROKEN &&
                   previousState != TKqpSessionCommon::S_CLOSING &&
                   client && session->IsOwnedBySessionPool()) {
            client->RecordSessionClosed(closeCommand.Reason);
        }
        return EAttachStreamReadAction::Stop;
    }

    return EAttachStreamReadAction::Continue;
}

}
