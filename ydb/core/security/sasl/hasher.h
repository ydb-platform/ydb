#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/login/hashes_checker/hash_types.h>
#include <ydb/library/login/password_checker/password_checker.h>

namespace NKikimr::NSasl {

struct TStaticCredentials {
    const std::string Username;
    const std::string Password;
};

std::unique_ptr<NActors::IActor> CreateHasher(
    NActors::TActorId sender, const TStaticCredentials& creds,
    const std::vector<NLogin::EHashType>& hashTypes,
    NLogin::TPasswordComplexity passwordComplexity = NLogin::TPasswordComplexity()
);

} // namespace NKikimr::NSasl
