#include "schemeshard_impl.h"
#include <ydb/library/security/util.h>
#include <ydb/core/protos/auth.pb.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxLogin : TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvLogin::TPtr Request;
    TPathId SubDomainPathId;
    bool NeedPublishOnComplete = false;

    TTxLogin(TSelf *self, TEvSchemeShard::TEvLogin::TPtr &ev)
        : TRwTxBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_LOGIN; }

    NLogin::TLoginProvider::TLoginUserRequest GetLoginRequest() const {
        return {
            .User = Request->Get()->Record.GetUser(),
            .Password = Request->Get()->Record.GetPassword(),
            .ExternalAuth = Request->Get()->Record.GetExternalAuth()
            };
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxLogin DoExecute"
                    << " at schemeshard: " << Self->TabletID());
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

            NIceDb::TNiceDb db(txc.DB);

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
    }

    void DoComplete(const TActorContext &ctx) override {
        if (NeedPublishOnComplete) {
            Self->PublishToSchemeBoard(TTxId(), {SubDomainPathId}, ctx);
        }

        THolder<TEvSchemeShard::TEvLoginResult> result = MakeHolder<TEvSchemeShard::TEvLoginResult>();
        const auto& loginRequest = GetLoginRequest();
        if (loginRequest.ExternalAuth || AppData(ctx)->AuthConfig.GetEnableLoginAuthentication()) {
            NLogin::TLoginProvider::TLoginUserResponse LoginResponse = Self->LoginProvider.LoginUser(loginRequest);
            if (LoginResponse.Error) {
                result->Record.SetError(LoginResponse.Error);
            }
            if (LoginResponse.Token) {
                result->Record.SetToken(LoginResponse.Token);
            }
        } else {
            result->Record.SetError("Login authentication is disabled");
        }

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxLogin DoComplete"
                    << ", result: " << result->Record.ShortDebugString()
                    << ", at schemeshard: " << Self->TabletID());

        ctx.Send(Request->Sender, std::move(result), 0, Request->Cookie);
    }

};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxLogin(TEvSchemeShard::TEvLogin::TPtr &ev) {
    return new TTxLogin(this, ev);
}

}}
