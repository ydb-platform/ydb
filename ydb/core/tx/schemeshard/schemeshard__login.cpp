#include <ydb/library/security/util.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/base/auth.h>

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxLogin : TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvLogin::TPtr Request;
    TPathId SubDomainPathId;
    bool NeedPublishOnComplete = false;
    THolder<TEvSchemeShard::TEvLoginResult> Result = MakeHolder<TEvSchemeShard::TEvLoginResult>();

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

            NeedPublishOnComplete = true;
        }

        LoginAttempt(db, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        if (NeedPublishOnComplete) {
            Self->PublishToSchemeBoard(TTxId(), {SubDomainPathId}, ctx);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxLogin Complete"
                    << ", result: " << Result->Record.ShortDebugString()
                    << ", at schemeshard: " << Self->TabletID());

        ctx.Send(Request->Sender, std::move(Result), 0, Request->Cookie);
    }

private:
    bool IsAdmin() const {
        const auto& user = Request->Get()->Record.GetUser();
        const auto providerGroups = Self->LoginProvider.GetGroupsMembership(user);
        const TVector<NACLib::TSID> groups(providerGroups.begin(), providerGroups.end());
        const auto userToken = NACLib::TUserToken(user, groups);

        return IsAdministrator(AppData(), &userToken);
    }

    void LoginAttempt(NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const auto& loginRequest = GetLoginRequest();
        if (!loginRequest.ExternalAuth && !AppData(ctx)->AuthConfig.GetEnableLoginAuthentication()) {
            Result->Record.SetError("Login authentication is disabled");
            return;
        }
        if (loginRequest.ExternalAuth) {
            HandleExternalAuth(loginRequest);
        } else {
            HandleLoginAuth(loginRequest, db);
        }
    }

    void HandleExternalAuth(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest) {
        const NLogin::TLoginProvider::TLoginUserResponse loginResponse = Self->LoginProvider.LoginUser(loginRequest);
        switch (loginResponse.Status) {
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            Result->Record.SetToken(loginResponse.Token);
            Result->Record.SetSanitizedToken(loginResponse.SanitizedToken);
            Result->Record.SetIsAdmin(IsAdmin());
            break;
        }
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_USER:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNAVAILABLE_KEY:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNSPECIFIED: {
            Result->Record.SetError(loginResponse.Error);
            break;
        }
        }
    }

    void HandleLoginAuth(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest, NIceDb::TNiceDb& db) {
        using namespace NLogin;
        const TLoginProvider::TCheckLockOutResponse checkLockOutResponse = Self->LoginProvider.CheckLockOutUser({.User = loginRequest.User});
        switch (checkLockOutResponse.Status) {
            case TLoginProvider::TCheckLockOutResponse::EStatus::SUCCESS:
            case TLoginProvider::TCheckLockOutResponse::EStatus::INVALID_USER: {
                Result->Record.SetError(checkLockOutResponse.Error);
                return;
            }
            case TLoginProvider::TCheckLockOutResponse::EStatus::RESET: {
                const auto& sid = Self->LoginProvider.Sids[loginRequest.User];
                db.Table<Schema::LoginSids>().Key(loginRequest.User).Update<Schema::LoginSids::FailedAttemptCount>(sid.FailedLoginAttemptCount);
                break;
            }
            case TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED:
            case TLoginProvider::TCheckLockOutResponse::EStatus::UNSPECIFIED: {
                break;
            }
        }

        const TLoginProvider::TLoginUserResponse loginResponse = Self->LoginProvider.LoginUser(loginRequest);
        switch (loginResponse.Status) {
        case TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            const auto& sid = Self->LoginProvider.Sids[loginRequest.User];
            db.Table<Schema::LoginSids>().Key(loginRequest.User).Update<Schema::LoginSids::LastSuccessfulAttempt,
                                                                        Schema::LoginSids::FailedAttemptCount>(ToInstant(sid.LastSuccessfulLogin).MicroSeconds(), sid.FailedLoginAttemptCount);
            Result->Record.SetToken(loginResponse.Token);
            Result->Record.SetSanitizedToken(loginResponse.SanitizedToken);
            Result->Record.SetIsAdmin(IsAdmin());
            break;
        }
        case TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD: {
            const auto& sid = Self->LoginProvider.Sids[loginRequest.User];
            db.Table<Schema::LoginSids>().Key(loginRequest.User).Update<Schema::LoginSids::LastFailedAttempt,
                                                                        Schema::LoginSids::FailedAttemptCount>(ToInstant(sid.LastFailedLogin).MicroSeconds(), sid.FailedLoginAttemptCount);
            Result->Record.SetError(loginResponse.Error);
            break;
        }
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_USER:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNAVAILABLE_KEY:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNSPECIFIED: {
            Result->Record.SetError(loginResponse.Error);
            break;
        }
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxLogin(TEvSchemeShard::TEvLogin::TPtr &ev) {
    return new TTxLogin(this, ev);
}

}}
