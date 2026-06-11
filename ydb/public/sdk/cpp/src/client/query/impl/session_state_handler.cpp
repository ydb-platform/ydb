#include "session_state_handler.h"

namespace NYdb::inline Dev::NQuery {

EAttachStreamReadAction HandleAttachSessionState(
    const Ydb::Query::SessionState& state,
    TKqpSessionCommon* session,
    const std::shared_ptr<ISessionClient>& client)
{
    if (state.has_session_shutdown()) {
        if (session) {
            session->MarkIdle();
        }
        return EAttachStreamReadAction::Continue;
    }

    if (state.has_node_shutdown()) {
        if (!session) {
            return EAttachStreamReadAction::Stop;
        }

        const auto nodeId = session->GetEndpointKey().GetNodeId();
        if (nodeId == 0) {
            return EAttachStreamReadAction::Stop;
        }

        session->MarkIdle();
        if (client) {
            client->PessimizeNode(nodeId);
        }
        return EAttachStreamReadAction::Stop;
    }

    return EAttachStreamReadAction::Continue;
}

}
