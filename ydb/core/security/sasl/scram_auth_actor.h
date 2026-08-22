#pragma once

#include <ydb/library/actors/core/actor.h>

#include <ydb/library/login/protos/login.pb.h>

namespace NKikimr::NSasl {

std::unique_ptr<NActors::IActor> CreateScramAuthActor(
    NActors::TActorId sender, const std::string& database, NLoginProto::EHashType::HashType hashType,
    const std::string& clientFirstMsg, const std::string& peerName);

} // namespace NKikimr::NSasl
