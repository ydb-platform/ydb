#include "schemeshard_login_helper.h"
#include "schemeshard_private.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/login/login.h>

namespace NKikimr::NSchemeShard {

class TLoginHelper : public NActors::TActorBootstrapped<TLoginHelper> {
public:
    explicit TLoginHelper(const NLogin::TLoginProvider& loginProvider)
        : LoginProvider_(loginProvider)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvVerifyPassword, VerifyPassword);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    void VerifyPassword(TEvPrivate::TEvVerifyPassword::TPtr& ev) {
        const bool isSuccessVerifying = LoginProvider_.VerifyHash(ev->Get()->Request, ev->Get()->PasswordHash);
        if (!isSuccessVerifying) {
            ev->Get()->CheckResult.Status = NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD;
            ev->Get()->CheckResult.Error = "Invalid password";
        }
        Send(
            ev->Sender,
            MakeHolder<TEvPrivate::TEvLoginFinalize>(
                ev->Get()->Request,
                ev->Get()->CheckResult,
                ev->Get()->Source,
                ev->Get()->PasswordHash,
                /*needUpdateCache*/ true
            ),
            0,
            ev->Cookie
        );
    }

private:
    const NLogin::TLoginProvider& LoginProvider_;
};

THolder<NActors::IActor> CreateLoginHelper(const NLogin::TLoginProvider& loginProvider) {
    return MakeHolder<TLoginHelper>(loginProvider);
}

} // namespace NKikimr::NSchemeShard
