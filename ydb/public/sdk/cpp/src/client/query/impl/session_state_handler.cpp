#include "session_state_handler.h"

#include <ydb/public/sdk/cpp/src/client/impl/session/session_pool.h>

namespace NYdb::inline Dev::NQuery {

EAttachStreamReadAction HandleAttachSessionState(
    const Ydb::Query::SessionState& state,
    TKqpSessionCommon* session,
    const std::shared_ptr<ISessionClient>& client)
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
        if (session->GetState() == TKqpSessionCommon::S_IDLE) {
            if (client) {
                session->CloseFromServer(client, closeCommand.Reason);
            }
        } else {
            closeCommand.Execute(*session, client.get());
        }
        return EAttachStreamReadAction::Stop;
    }

    return EAttachStreamReadAction::Continue;
}

}
