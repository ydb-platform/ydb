#include "iam_delegation.h"
#include "tablet.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

namespace NKikimr::NIamDelegation {

struct TIamDelegationTablet::TTxInit final
    : NTabletFlatExecutor::TTransactionBase<TIamDelegationTablet>
{
    explicit TTxInit(TIamDelegationTablet* self)
        : TTransactionBase(self)
    {}

    TTxType GetTxType() const override { return static_cast<TTxType>(ETxType::InitSchema); }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<TSchema>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->Become(&TIamDelegationTablet::StateWork);
        Self->SignalTabletActive(ctx);
    }
};

TIamDelegationTablet::TIamDelegationTablet(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{}

void TIamDelegationTablet::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TIamDelegationTablet::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void TIamDelegationTablet::OnActivateExecutor(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

void TIamDelegationTablet::DefaultSignalTabletActive(const TActorContext&) {}

STFUNC(TIamDelegationTablet::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TIamDelegationTablet::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvIamDelegationTablet::TEvRequest, Handle);
        // Requests are self-contained; no state belongs to a pipe connection.
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                Y_ABORT("Unexpected IAM delegation tablet event 0x%x", ev->GetTypeRewrite());
            }
    }
}

IActor* CreateIamDelegationTablet(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TIamDelegationTablet(tablet, info);
}

} // namespace NKikimr::NIamDelegation
