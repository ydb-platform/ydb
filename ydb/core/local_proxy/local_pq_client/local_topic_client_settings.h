#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NKqp {

struct TLocalTopicClientSettings {
    NActors::TActorSystem* ActorSystem = nullptr;
    ui64 ChannelBufferSize = 0;
};

struct TLocalTopicSessionSettings {
    NActors::TActorSystem* ActorSystem = nullptr;
    TString Database;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
};

} // namespace NKikimr::NKqp
