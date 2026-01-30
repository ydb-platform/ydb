#pragma once

#include "local_topic_client_settings.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

namespace NKikimr::NKqp {

std::shared_ptr<NYdb::NTopic::IReadSession> CreateLocalTopicReadSession(const TLocalTopicSessionSettings& localSettings, const NYdb::NTopic::TReadSessionSettings& sessionSettings);

} // namespace NKikimr::NKqp
