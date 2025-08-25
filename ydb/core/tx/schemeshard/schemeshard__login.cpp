#include "schemeshard_impl.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/local_user_token.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/library/login/login.h>
#include <ydb/library/security/util.h>

namespace NKikimr {
namespace NSchemeShard {

struct TSchemeShard::TTxLogin : TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvLogin::TPtr Request;
    NLogin::TLoginProvider::TLoginUserResponse Response;
    TPathId SubDomainPathId;
    bool NeedPublishOnComplete = false;
    bool SendFinalizeEvent = false;
    TString ErrMessage;

    TTxLogin(TSelf *self, TEvSchemeShard::TEvLogin::TPtr &ev)
        : TRwTxBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_LOGIN; }

    NLogin::TLoginProvider::TLoginUserRequest GetLoginRequest() const {
        const auto& record(Request->Get()->Record);
        return {
            .User = record.GetUser(),
            .Password = record.GetPassword(),
            .Options = {
                .ExpiresAfter = record.HasExpiresAfterMs()
                    ? std::chrono::milliseconds(record.GetExpiresAfterMs())
                    : std::chrono::system_clock::duration::zero()
                },
            .ExternalAuth = record.GetExternalAuth(),
            };
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxLogin Execute"
                    << " at schemeshard: " << Self->TabletID());
        NIceDb::TNiceDb db(txc.DB);
        if (Self->LoginProvider.IsItTimeToRotateKeys()) {
            RotateKeys(ctx, db);
            NeedPublishOnComplete = true;
        }

        const auto& loginRequest = GetLoginRequest();
        if (!loginRequest.ExternalAuth) {
            if (!AppData(ctx)->AuthConfig.GetEnableLoginAuthentication()) {
                ErrMessage = "Login authentication is disabled";
            } else {
                CheckLockOutUserAndSetErrorIfAny(loginRequest.User, db);
            }
        }

        if (ErrMessage) {
            SendError();
            return;
        }

        TString passwordHash;
        if (Self->LoginProvider.NeedVerifyHash(loginRequest, &Response, &passwordHash)) {
            ctx.Send(
                Self->LoginHelper,
                MakeHolder<TEvPrivate::TEvVerifyPassword>(loginRequest, Response, Request->Sender, passwordHash),
                0,
                Request->Cookie
            );
        } else {
            SendFinalizeEvent = true;
        }
    }

    void DoComplete(const TActorContext &ctx) override {
        if (NeedPublishOnComplete) {
            Self->PublishToSchemeBoard(TTxId(), {SubDomainPathId}, ctx);
        }

        if (SendFinalizeEvent) {
            auto event = MakeHolder<TEvPrivate::TEvLoginFinalize>(
                GetLoginRequest(), Response, Request->Sender, "", /*needUpdateCache*/ false
            );
            TEvPrivate::TEvLoginFinalize::TPtr eventPtr = (TEventHandle<TEvPrivate::TEvLoginFinalize>*) new IEventHandle(
                Self->SelfId(), Self->SelfId(), event.Release()
            );
            Self->Execute(Self->CreateTxLoginFinalize(eventPtr), ctx);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxLogin Complete"
                    << ", with " << (ErrMessage ? "error: " + ErrMessage : "no errors")
                    << ", at schemeshard: " << Self->TabletID());
}

private:
    void RotateKeys(const TActorContext& ctx, NIceDb::TNiceDb& db) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxLogin RotateKeys at schemeshard: " << Self->TabletID());
        std::vector<ui64> keysExpired;
        std::vector<ui64> keysAdded;
        Self->LoginProvider.RotateKeys(keysExpired, keysAdded);
        SubDomainPathId = Self->GetCurrentSubDomainPathId();
        TSubDomainInfo::TPtr domainPtr = Self->ResolveDomainInfo(SubDomainPathId);

        // TODO(xenoxeno): optimize security state changes
        domainPtr->UpdateSecurityState(Self->LoginProvider.GetSecurityState());
        domainPtr->IncSecurityStateVersion();

        Self->PersistSubDomainSecurityStateVersion(db, SubDomainPathId, *domainPtr);

        for (ui64 keyId : keysExpired) {
            db.Table<Schema::LoginKeys>().Key(keyId).Delete();
        }
        for (ui64 keyId : keysAdded) {
            const auto* key = Self->LoginProvider.FindKey(keyId);
            if (key) {
                db.Table<Schema::LoginKeys>().Key(keyId).Update<Schema::LoginKeys::KeyDataPEM, Schema::LoginKeys::ExpiresAt>(
                    key->PublicKey, ToInstant(key->ExpiresAt).MilliSeconds());
            }
        }
    }

    void CheckLockOutUserAndSetErrorIfAny(const TString& user, NIceDb::TNiceDb& db) {
        using namespace NLogin;
        const TLoginProvider::TCheckLockOutResponse checkLockOutResponse = Self->LoginProvider.CheckLockOutUser({.User = user});
        switch (checkLockOutResponse.Status) {
            case TLoginProvider::TCheckLockOutResponse::EStatus::SUCCESS:
            case TLoginProvider::TCheckLockOutResponse::EStatus::INVALID_USER: {
                ErrMessage = checkLockOutResponse.Error;
                return;
            }
            case TLoginProvider::TCheckLockOutResponse::EStatus::RESET: {
                const auto& sid = Self->LoginProvider.Sids[user];
                db.Table<Schema::LoginSids>().Key(user).Update<Schema::LoginSids::FailedAttemptCount>(sid.FailedLoginAttemptCount);
                break;
            }
            case TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED:
            case TLoginProvider::TCheckLockOutResponse::EStatus::UNSPECIFIED: {
                break;
            }
        }
    }

    void SendError() {
        THolder<TEvSchemeShard::TEvLoginResult> result = MakeHolder<TEvSchemeShard::TEvLoginResult>();
        result->Record.SetError(ErrMessage);
        Self->Send(
            Request->Sender,
            std::move(result),
            0,
            Request->Cookie
        );
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxLogin(TEvSchemeShard::TEvLogin::TPtr &ev) {
    return new TTxLogin(this, ev);
}

}}
