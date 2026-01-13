#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

namespace NKikimr::NKqp {

struct TLocalTopicReadSettings {
    NActors::TActorSystem* ActorSystem = nullptr;
    TString Database;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
};

std::shared_ptr<NYdb::NTopic::IReadSession> CreateLocalTopicReadSession(const TLocalTopicReadSettings& localSettings, const NYdb::NTopic::TReadSessionSettings& sessionSettings);

} // namespace NKikimr::NKqp
