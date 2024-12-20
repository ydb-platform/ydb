#include <ydb/library/security/util.h>
#include <ydb/core/protos/auth.pb.h>

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxLogin : TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvLogin::TPtr Request;
    TPathId SubDomainPathId;
    bool NeedPublishOnComplete = false;
    THolder<TEvSchemeShard::TEvLoginResult> Result = MakeHolder<TEvSchemeShard::TEvLoginResult>();
    size_t CurrentFailedAttemptCount = 0;

    TTxLogin(TSelf *self, TEvSchemeShard::TEvLogin::TPtr &ev)
        : TTransactionBase<TSchemeShard>(self)
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

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
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

        return LoginAttempt(db, ctx);
    }

    void Complete(const TActorContext &ctx) override {
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
    bool LoginAttempt(NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const auto& loginRequest = GetLoginRequest();
        if (!loginRequest.ExternalAuth && !AppData(ctx)->AuthConfig.GetEnableLoginAuthentication()) {
            Result->Record.SetError("Login authentication is disabled");
            return true;
        }
        if (loginRequest.ExternalAuth) {
            return HandleExternalAuth(loginRequest);
        }
        return HandleLoginAuth(loginRequest, db, ctx);
    }

    bool HandleExternalAuth(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest) {
        const NLogin::TLoginProvider::TLoginUserResponse loginResponse = Self->LoginProvider.LoginUser(loginRequest);
        switch (loginResponse.Status) {
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            Result->Record.SetToken(loginResponse.Token);
            Result->Record.SetSanitizedToken(loginResponse.SanitizedToken);
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
        return true;
    }

    bool HandleLoginAuth(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        auto row = db.Table<Schema::LoginSids>().Key(loginRequest.User).Select();
        if (!row.IsReady()) {
            return false;
        }
        if (!row.IsValid()) {
            Result->Record.SetError(TStringBuilder() << "Cannot find user: " << loginRequest.User);
            return true;
        }
        CurrentFailedAttemptCount = row.GetValueOrDefault<Schema::LoginSids::FailedAttemptCount>();
        TInstant lastFailedAttempt = TInstant::FromValue(row.GetValue<Schema::LoginSids::LastFailedAttempt>());
        if (CheckAccountLockout()) {
            if (ShouldUnlockAccount(lastFailedAttempt)) {
                UnlockAccount(loginRequest, db);
            } else {
                Result->Record.SetError(TStringBuilder() << "User " << loginRequest.User << " is locked out");
                return true;
            }
        } else if (ShouldResetFailedAttemptCount(lastFailedAttempt)) {
            ResetFailedAttemptCount(loginRequest, db);
        }
        const NLogin::TLoginProvider::TLoginUserResponse loginResponse = Self->LoginProvider.LoginUser(loginRequest);
        switch (loginResponse.Status) {
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            HandleLoginAuthSuccess(loginRequest, loginResponse, db);
            Result->Record.SetToken(loginResponse.Token);
            Result->Record.SetSanitizedToken(loginResponse.SanitizedToken);
            break;
        }
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD: {
            HandleLoginAuthInvalidPassword(loginRequest, loginResponse, db);
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
        return true;
    }

    bool CheckAccountLockout() const {
        return (Self->AccountLockout.AttemptThreshold != 0 && CurrentFailedAttemptCount >= Self->AccountLockout.AttemptThreshold);
    }

    bool ShouldResetFailedAttemptCount(const TInstant& lastFailedAttempt) {
        if (Self->AccountLockout.AttemptResetDuration == TDuration::Zero()) {
            return false;
        }
        return lastFailedAttempt + Self->AccountLockout.AttemptResetDuration < TAppData::TimeProvider->Now();
    }

    bool ShouldUnlockAccount(const TInstant& lastFailedAttempt) {
        return ShouldResetFailedAttemptCount(lastFailedAttempt);
    }

    void ResetFailedAttemptCount(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest, NIceDb::TNiceDb& db) {
        db.Table<Schema::LoginSids>().Key(loginRequest.User).Update<Schema::LoginSids::FailedAttemptCount>(Schema::LoginSids::FailedAttemptCount::Default);
        CurrentFailedAttemptCount = Schema::LoginSids::FailedAttemptCount::Default;
    }

    void UnlockAccount(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest, NIceDb::TNiceDb& db) {
        ResetFailedAttemptCount(loginRequest, db);
    }

    void HandleLoginAuthSuccess(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest, const NLogin::TLoginProvider::TLoginUserResponse& loginResponse, NIceDb::TNiceDb& db) {
        db.Table<Schema::LoginSids>().Key(loginRequest.User).Update<Schema::LoginSids::LastSuccessfulAttempt, Schema::LoginSids::FailedAttemptCount>(TAppData::TimeProvider->Now().MicroSeconds(), Schema::LoginSids::FailedAttemptCount::Default);
    }

    void HandleLoginAuthInvalidPassword(const NLogin::TLoginProvider::TLoginUserRequest& loginRequest, const NLogin::TLoginProvider::TLoginUserResponse& loginResponse, NIceDb::TNiceDb& db) {
        db.Table<Schema::LoginSids>().Key(loginRequest.User).Update<Schema::LoginSids::LastFailedAttempt, Schema::LoginSids::FailedAttemptCount>(TAppData::TimeProvider->Now().MicroSeconds(), CurrentFailedAttemptCount + 1);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxLogin(TEvSchemeShard::TEvLogin::TPtr &ev) {
    return new TTxLogin(this, ev);
}

}}
