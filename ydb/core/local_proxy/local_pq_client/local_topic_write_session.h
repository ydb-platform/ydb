#pragma once

#include "local_topic_client_settings.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>

namespace NKikimr::NKqp {

std::shared_ptr<NYdb::NTopic::IWriteSession> CreateLocalTopicWriteSession(const TLocalTopicSessionSettings& localSettings, const NYdb::NTopic::TWriteSessionSettings& sessionSettings);

} // namespace NKikimr::NKqp
