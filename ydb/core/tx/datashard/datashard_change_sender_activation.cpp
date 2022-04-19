#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

class TDataShard::TTxActivateChangeSender: public TTransactionBase<TDataShard> {
public:
    explicit TTxActivateChangeSender(TDataShard* self, ui64 origin, const TActorId& ackTo)
        : TTransactionBase(self)
        , Origin(origin)
        , AckTo(ackTo)
        , AllSrcActivationsReceived(false)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ACTIVATE_CHANGE_SENDER;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxActivateChangeSender Execute"
            << ": origin# " << Origin
            << ", at tablet# " << Self->TabletID());

        if (!Self->ReceiveActivationsFrom.contains(Origin)) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Ignoring received activation"
                << ": origin# " << Origin
                << ", at tablet# " << Self->TabletID());
            return true;
        }

        Self->ReceiveActivationsFrom.erase(Origin);
        AllSrcActivationsReceived = !Self->ReceiveActivationsFrom;

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::DstChangeSenderActivations>().Key(Origin).Delete();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxActivateChangeSender Complete"
            << ": origin# " << Origin
            << ", at tablet# " << Self->TabletID());

        auto ev = MakeHolder<TEvChangeExchange::TEvActivateSenderAck>();
        ev->Record.SetOrigin(Self->TabletID());
        ctx.Send(AckTo, ev.Release());

        if (AllSrcActivationsReceived) {
            if (!Self->GetChangeSender()) {
                // There might be a race between TxInit::Complete and <this>::Complete
                // so just skip it. Change sender will be activated upon TxInit::Complete.
                return;
            }

            Self->MaybeActivateChangeSender(ctx);
        }
    }

private:
    const ui64 Origin;
    const TActorId AckTo;
    bool AllSrcActivationsReceived;

}; // TTxActivateChangeSender

class TDataShard::TTxActivateChangeSenderAck: public TTransactionBase<TDataShard> {
public:
    explicit TTxActivateChangeSenderAck(TDataShard* self, ui64 origin)
        : TTransactionBase(self)
        , Origin(origin)
        , AllDstAcksReceived(false)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ACTIVATE_CHANGE_SENDER_ACK;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxActivateChangeSenderAck Execute"
            << ": origin# " << Origin
            << ", at tablet# " << Self->TabletID());

        Self->ChangeSenderActivator.Ack(Origin, ctx);
        AllDstAcksReceived = Self->ChangeSenderActivator.AllAcked();

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SrcChangeSenderActivations>().Key(Origin).Delete();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxActivateChangeSenderAck Complete"
            << ": origin# " << Origin
            << ", at tablet# " << Self->TabletID());

        if (AllDstAcksReceived && Self->SrcAckPartitioningChangedTo) {
            Self->Execute(Self->CreateTxSplitPartitioningChanged(std::move(Self->SrcAckPartitioningChangedTo)), ctx);
            Self->SrcAckPartitioningChangedTo.clear(); // to be sure
        }
    }

private:
    const ui64 Origin;
    bool AllDstAcksReceived;

}; // TTxActivateChangeSenderAck

void TDataShard::Handle(TEvChangeExchange::TEvActivateSender::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxActivateChangeSender(this, ev->Get()->Record.GetOrigin(), ev->Sender), ctx);
}

void TDataShard::Handle(TEvChangeExchange::TEvActivateSenderAck::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxActivateChangeSenderAck(this, ev->Get()->Record.GetOrigin()), ctx);
}

} // NDataShard
} // NKikimr
