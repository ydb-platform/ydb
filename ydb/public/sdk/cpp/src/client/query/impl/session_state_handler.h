#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/session/kqp_session_common.h>

#include <ydb/public/api/protos/ydb_query.pb.h>

#include <functional>
#include <memory>

namespace NYdb::inline Dev::NQuery {

enum class EAttachStreamReadAction {
    Continue,
    Stop,
};

// beforeShutdownTransition is an internal synchronization point for tests.
// It is called without holding session or pool locks.
EAttachStreamReadAction HandleAttachSessionState(
    const Ydb::Query::SessionState& state,
    TKqpSessionCommon* session,
    const std::shared_ptr<ISessionClient>& client,
    const std::function<void()>& beforeShutdownTransition = {});

}
