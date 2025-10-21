#include "schemeshard_impl.h"
#include <ydb/library/security/util.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/local_user_token.h>

namespace NKikimr {
namespace NSchemeShard {

struct TSchemeShard::TTxLoginFinalize : TSchemeShard::TRwTxBase {
private:
    TEvPrivate::TEvLoginFinalize::TPtr LoginFinalizeEventPtr;
    TString ErrMessage;

public:
    TTxLoginFinalize(TSelf *self, TEvPrivate::TEvLoginFinalize::TPtr &ev)
        : TRwTxBase(self)
        , LoginFinalizeEventPtr(std::move(ev))
    {}

    TTxType GetTxType() const override {
        return TXTYPE_LOGIN_FINALIZE;
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxLoginFinalize Execute"
                    << " at schemeshard: " << Self->TabletID());

        const auto& event = *LoginFinalizeEventPtr->Get();
        if (event.NeedUpdateCache) {
            const auto isSuccessVerifying =
                event.CheckResult.Status == NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS;
            Self->LoginProvider.UpdateCache(
                event.Request,
                event.PasswordHash,
                isSuccessVerifying
            );
        }
        const auto response = Self->LoginProvider.LoginUser(event.Request, event.CheckResult);

        if (!LoginFinalizeEventPtr->Get()->Request.ExternalAuth) {
            UpdateLoginSidsStats(response, txc);
        }
        if (!response.Error.empty()) {
            ErrMessage = response.Error;
            SendError(response.Error);
            return;
        }
        FillResult(response);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxLoginFinalize Completed"
            << ", with " << (ErrMessage ? "error: " + ErrMessage : "no errors")
            << " at schemeshard: " << Self->TabletID());
    }

private:
    bool IsAdmin(const TString& user) const {
        const auto userToken = NKikimr::BuildLocalUserToken(Self->LoginProvider, user);
        return IsAdministrator(AppData(), &userToken);
    }

    void FillResult(const NLogin::TLoginProvider::TLoginUserResponse& response) {
        THolder<TEvSchemeShard::TEvLoginResult> result = MakeHolder<TEvSchemeShard::TEvLoginResult>();
        switch (response.Status) {
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            result->Record.SetToken(response.Token);
            result->Record.SetSanitizedToken(response.SanitizedToken);
            result->Record.SetIsAdmin(IsAdmin(LoginFinalizeEventPtr->Get()->Request.User));
            break;
        }
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_USER:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNAVAILABLE_KEY:
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::UNSPECIFIED: {
            result->Record.SetError(response.Error);
            break;
        }
        }
        Self->Send(
            LoginFinalizeEventPtr->Get()->Source,
            std::move(result),
            0,
            LoginFinalizeEventPtr->Cookie
        );
    }

    void SendError(const TString& error) {
        auto result = MakeHolder<TEvSchemeShard::TEvLoginResult>();
        result->Record.SetError(error);
        Self->Send(
            LoginFinalizeEventPtr->Get()->Source,
            std::move(result),
            0,
            LoginFinalizeEventPtr->Cookie
        );
    }

    void UpdateLoginSidsStats(const NLogin::TLoginProvider::TLoginUserResponse& response, TTransactionContext& txc) {
        NIceDb::TNiceDb db(txc.DB);
        switch (response.Status) {
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS: {
            const auto& sid = Self->LoginProvider.Sids[LoginFinalizeEventPtr->Get()->Request.User];
            db.Table<Schema::LoginSids>()
                .Key(LoginFinalizeEventPtr->Get()->Request.User)
                .Update<Schema::LoginSids::LastSuccessfulAttempt, Schema::LoginSids::FailedAttemptCount>(
                    ToMicroSeconds(sid.LastSuccessfulLogin), sid.FailedLoginAttemptCount
                );
            break;
        }
        case NLogin::TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD: {
            const auto& sid = Self->LoginProvider.Sids[LoginFinalizeEventPtr->Get()->Request.User];
            db.Table<Schema::LoginSids>()
                .Key(LoginFinalizeEventPtr->Get()->Request.User)
                .Update<Schema::LoginSids::LastFailedAttempt, Schema::LoginSids::FailedAttemptCount>(
                        ToMicroSeconds(sid.LastFailedLogin), sid.FailedLoginAttemptCount
                );
        }
        default:
            break;
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxLoginFinalize(TEvPrivate::TEvLoginFinalize::TPtr &ev) {
    return new TTxLoginFinalize(this, ev);
}

}}
