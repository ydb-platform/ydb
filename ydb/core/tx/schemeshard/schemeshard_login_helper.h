#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/login/login.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

enum EEvLogin {
    EvVerifyPassword,
    EvLoginFinalize,
};

struct TEvVerifyPassword : public NActors::TEventLocal<TEvVerifyPassword, EvVerifyPassword> {
public:
    TEvVerifyPassword(
        const NLogin::TLoginProvider::TLoginUserRequest& request,
        const NLogin::TLoginProvider::TPasswordCheckResult& checkResult,
        const NActors::TActorId source,
        const TString& passwordHash
    )
        : Request(request)
        , CheckResult(checkResult)
        , Source(source)
        , PasswordHash(passwordHash)
    {}

public:
    const NLogin::TLoginProvider::TLoginUserRequest Request;
    NLogin::TLoginProvider::TPasswordCheckResult CheckResult;
    const NActors::TActorId Source;
    const TString PasswordHash;
};

struct TEvLoginFinalize : public NActors::TEventLocal<TEvLoginFinalize, EvLoginFinalize> {
public:
    TEvLoginFinalize(
        const NLogin::TLoginProvider::TLoginUserRequest& request,
        const NLogin::TLoginProvider::TPasswordCheckResult& checkResult,
        const NActors::TActorId source,
        const TString& passwordHash,
        const bool needUpdateCache
    )
        : Request(request)
        , CheckResult(checkResult)
        , Source(source)
        , PasswordHash(passwordHash)
        , NeedUpdateCache(needUpdateCache)
    {}

public:
    const NLogin::TLoginProvider::TLoginUserRequest Request;
    const NLogin::TLoginProvider::TPasswordCheckResult CheckResult;
    const NActors::TActorId Source;
    const TString PasswordHash;
    const bool NeedUpdateCache;
};

THolder<NActors::IActor> CreateLoginHelper(const NLogin::TLoginProvider& loginProvider);

} // namespace NKikimr::NSchemeShard
