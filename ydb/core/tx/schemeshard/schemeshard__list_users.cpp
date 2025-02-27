#include <ydb/library/security/util.h>
#include <ydb/core/protos/auth.pb.h>

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxListUsers : TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvListUsers::TPtr Request;
    THolder<TEvSchemeShard::TEvListUsersResult> Result = MakeHolder<TEvSchemeShard::TEvListUsersResult>();

    TTxListUsers(TSelf *self, TEvSchemeShard::TEvListUsers::TPtr &ev)
        : TTransactionBase<TSchemeShard>(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_LIST_USERS; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxListUsers Execute"
                    << " at schemeshard: " << Self->TabletID());

        for (const auto& [_, sid] : Self->LoginProvider.Sids) {
            if (sid.Type != NLoginProto::ESidType::USER) {
                continue;
            }
            auto user = Result->Record.AddUsers();
            user->SetName(sid.Name);
            user->SetIsEnabled(sid.IsEnabled);
            user->SetIsLockedOut(Self->LoginProvider.IsLockedOut(sid));
            user->SetCreatedAt(ToInstant(sid.CreatedAt).MilliSeconds());
            user->SetLastSuccessfulAttemptAt(ToInstant(sid.LastSuccessfulLogin).MilliSeconds());
            user->SetLastFailedAttemptAt(ToInstant(sid.LastFailedLogin).MilliSeconds());
            user->SetFailedAttemptCount(sid.FailedLoginAttemptCount);
            user->SetPasswordHash(sid.PasswordHash);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) noexcept override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxListUsers Complete"
                    << ", result: " << Result->Record.ShortDebugString()
                    << ", at schemeshard: " << Self->TabletID());

        ctx.Send(Request->Sender, std::move(Result), 0, Request->Cookie);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxListUsers(TEvSchemeShard::TEvListUsers::TPtr &ev) {
    return new TTxListUsers(this, ev);
}

}}
