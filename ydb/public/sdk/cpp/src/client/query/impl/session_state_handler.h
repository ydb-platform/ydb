#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/session/kqp_session_common.h>

#include <ydb/public/api/protos/ydb_query.pb.h>

#include <memory>

namespace NYdb::inline Dev::NQuery {

enum class EAttachStreamReadAction {
    Continue,
    Stop,
};

EAttachStreamReadAction HandleAttachSessionState(
    const Ydb::Query::SessionState& state,
    TKqpSessionCommon* session,
    const std::shared_ptr<ISessionClient>& client);

}
