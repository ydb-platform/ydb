#include "schemeshard_impl.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/local_user_token.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/library/login/login.h>
#include <ydb/library/security/util.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

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
        NLogin::TLoginProvider::TLoginUserRequest request {
            .User = record.GetUser(),
            .Options = {
                .ExpiresAfter = record.HasExpiresAfterMs()
                    ? std::chrono::milliseconds(record.GetExpiresAfterMs())
                    : std::chrono::system_clock::duration::zero()
                },
        };

        if (record.HasExternalAuth()) {
            request.ExternalAuth = record.GetExternalAuth();
        } else if (record.HasHashToValidate()) {
            NLogin::TLoginProvider::THashToValidate hashToValidate {
                .AuthMech = record.GetHashToValidate().GetAuthMech(),
                .HashType = record.GetHashToValidate().GetHashType(),
                .Hash = record.GetHashToValidate().GetHash(),
                .AuthMessage = record.GetHashToValidate().GetAuthMessage(),
            };

            request.HashToValidate = std::move(hashToValidate);
        }

        return request;
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "TTxLogin Execute",
            {"schemeshard", Self->TabletID()});
        NIceDb::TNiceDb db(txc.DB);
        if (Self->LoginProvider.IsItTimeToRotateKeys()) {
            RotateKeys(ctx, db);
            NeedPublishOnComplete = true;
        }

        const auto& loginRequest = GetLoginRequest();
        if (!loginRequest.ExternalAuth.has_value()) {
            if (!AppData(ctx)->AuthConfig.GetEnableLoginAuthentication()) {
                Result->Record.SetError("Login authentication is disabled");
                return;
            }
            if (CheckLockOutUserAndSetErrorIfAny(loginRequest.User, db)) {
                return;
            }
        }

        const auto response = Self->LoginProvider.LoginUser(loginRequest);
        if (!loginRequest.ExternalAuth.has_value()) {
            UpdateLoginSidsStats(response, db);
        }
        FillResult(response);
    }

    void DoComplete(const TActorContext &ctx) override {
        if (NeedPublishOnComplete) {
            Self->PublishToSchemeBoard(TTxId(), {SubDomainPathId}, ctx);
        }

        const TString& error = Result->Record.GetError();
        YDB_LOG_DEBUG_CTX(ctx, "TTxLogin Complete with",
            {"error", (error ? error : TString("no errors"))},
            {"schemeshard", Self->TabletID()});

        Self->Send(Request->Sender, std::move(Result), 0, Request->Cookie);
    }

private:
    bool IsAdmin() const {
        const auto& user = Request->Get()->Record.GetUser();
        const auto userToken = NKikimr::BuildLocalUserToken(Self->LoginProvider, user);
        return IsAdministrator(AppData(), &userToken);
    }

    void RotateKeys(const TActorContext& ctx, NIceDb::TNiceDb& db) {
        YDB_LOG_DEBUG_CTX(ctx, "TTxLogin RotateKeys",
            {"schemeshard", Self->TabletID()});
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

    // Returns true if the user is locked out and an error has been set into the result.
    bool CheckLockOutUserAndSetErrorIfAny(const TString& user, NIceDb::TNiceDb& db) {
        using namespace NLogin;
        const TLoginProvider::TCheckLockOutResponse checkLockOutResponse = Self->LoginProvider.CheckLockOutUser({.User = user});
        switch (checkLockOutResponse.Status) {
            case TLoginProvider::TCheckLockOutResponse::EStatus::SUCCESS:
            case TLoginProvider::TCheckLockOutResponse::EStatus::INVALID_USER: {
                Result->Record.SetError(checkLockOutResponse.Error);
                return true;
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
        return false;
    }

    void FillResult(const NLogin::TLoginProvider::TLoginUserResponse& response) {
        switch (response.Status) {
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            if (response.ServerSignature.has_value()) {
                Result->Record.SetServerSignature(*response.ServerSignature);
            }
            Result->Record.SetToken(response.Token);
            Result->Record.SetSanitizedToken(response.SanitizedToken);
            Result->Record.SetIsAdmin(IsAdmin());
            break;
        }
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_USER:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNAVAILABLE_KEY:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_HASH_TYPE:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNSUPPORTED_SASL_MECHANISM:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNSPECIFIED: {
            Result->Record.SetError(response.Error);
            break;
        }
        }
    }

    void UpdateLoginSidsStats(const NLogin::TLoginProvider::TLoginUserResponse& response, NIceDb::TNiceDb& db) {
        const TString& user = Request->Get()->Record.GetUser();
        switch (response.Status) {
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            const auto& sid = Self->LoginProvider.Sids[user];
            db.Table<Schema::LoginSids>()
                .Key(user)
                .Update<Schema::LoginSids::LastSuccessfulAttempt, Schema::LoginSids::FailedAttemptCount>(
                    ToMicroSeconds(sid.LastSuccessfulLogin), sid.FailedLoginAttemptCount
                );
            break;
        }
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD: {
            const auto& sid = Self->LoginProvider.Sids[user];
            db.Table<Schema::LoginSids>()
                .Key(user)
                .Update<Schema::LoginSids::LastFailedAttempt, Schema::LoginSids::FailedAttemptCount>(
                    ToMicroSeconds(sid.LastFailedLogin), sid.FailedLoginAttemptCount
                );
        }
        default:
            break;
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxLogin(TEvSchemeShard::TEvLogin::TPtr &ev) {
    return new TTxLogin(this, ev);
}

}}
