#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/core/tx/defs.h>

namespace NKikimr {
    struct TEvUserRegistry {
        enum EEv {
            EvGetUserById = EventSpaceBegin(TKikimrEvents::ES_USER_REGISTRY),
            EvGetUserByIdResult,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_USER_REGISTRY),
                      "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_USER_REGISTRY)");

        struct TEvGetUserById : TEventLocal<TEvGetUserById, EvGetUserById> {
            const ui64 UID;
            const TString UserNameHint;

            TEvGetUserById(const ui64 uid, const TString& userNameHint = "")
                : UID(uid)
                , UserNameHint(userNameHint)
            {}
        };

        struct TEvGetUserByIdResult : TEventLocal<TEvGetUserByIdResult, EvGetUserByIdResult> {
            const ui64 UID;
            const TString UserNameHint;
            const TString User;
            const TString Error;

            TEvGetUserByIdResult(const ui64 uid, const TString& userNameHint, const TString& user = "", const TString& error = "")
                : UID(uid)
                , UserNameHint(userNameHint)
                , User(user)
                , Error(error)
            {}
        };
    };

    inline NActors::TActorId MakeUserRegistryID() {
        const char name[12] = "userregistr";
        return NActors::TActorId(0, TStringBuf(name, 12));
    }

    IActor* CreateUserRegistry(const TString& query);
}
