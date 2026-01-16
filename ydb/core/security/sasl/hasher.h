#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/login/password_checker/password_checker.h>
#include <ydb/library/login/protos/login.pb.h>

namespace NKikimr::NSasl {

struct TStaticCredentials {
    const std::string Username;
    const std::string Password;
};

std::unique_ptr<NActors::IActor> CreateHasher(
    NActors::TActorId sender, const TStaticCredentials& creds,
    const std::vector<NLoginProto::EHashType::HashType>& hashTypes,
    NLogin::TPasswordComplexity passwordComplexity = NLogin::TPasswordComplexity()
);

} // namespace NKikimr::NSasl
