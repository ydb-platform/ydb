#include "schemeshard_impl.h"

#include <ydb/library/login/hashes_checker/hashes_checker.h>
#include <ydb/library/login/protos/login.pb.h>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxUserHashesMigration : public TTransactionBase<TSchemeShard> {

    TTxUserHashesMigration(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_USER_HASHES_MIGRATION;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxUserHashesMigration Execute at schemeshard: " << Self->TabletID());

        const auto& securityConfig = Self->GetDomainsConfig().GetSecurityConfig();
        TString defaultPassword;
        if (securityConfig.DefaultUsersSize()) {
            defaultPassword = securityConfig.GetDefaultUsers(0).GetPassword();
        }

        NIceDb::TNiceDb db(txc.DB);
        for (const auto& [sidName, sid] : Self->LoginProvider.Sids) {
            if (sid.Type == NLoginProto::ESidType::USER) {
                if (!sid.PasswordHashes) {
                    auto response = Self->LoginProvider.ModifyUser({
                        .User = sid.Name,
                        .Password = defaultPassword,
                    });

                    if (response.Error) {
                        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxUserHashesMigration Execute"
                            << ", can't set default password in place inacceptable argon hash: "
                            << response.Error << ", at schemeshard: "<< Self->TabletID());
                        continue;
                    }
                }

                db.Table<Schema::LoginSids>().Key(sidName).Update<Schema::LoginSids::SidHash, Schema::LoginSids::PasswordHashes>(
                                                                    sid.ArgonHash, sid.PasswordHashes);
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxUserHashesMigration Complete, at schemeshard: "<< Self->TabletID());
    }
};

ITransaction* TSchemeShard::CreateTxUserHashesMigration() {
    return new TTxUserHashesMigration(this);
}

} // NSchemeShard
} // NKikimr
