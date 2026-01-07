#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <util/system/types.h>

namespace NKikimr::NKqp {

struct TLocalTopicClientSettings {
    NActors::TActorSystem* ActorSystem = nullptr;
    ui64 ChannelBufferSize = 0;
};

} // namespace NKikimr::NKqp
