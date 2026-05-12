#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSasl {

std::unique_ptr<NActors::IActor> CreatePlainLdapAuthProxyActor(
    NActors::TActorId sender, const std::string& database, const std::string& saslPlainAuthMsg, const std::string& peerName);

} // namespace NKikimr::NSasl
