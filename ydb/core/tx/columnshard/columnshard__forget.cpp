#include "columnshard_impl.h"
#include "blob_manager_db.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxForget : public TTransactionBase<TColumnShard> {
public:
    TTxForget(TColumnShard* self, TEvPrivate::TEvForget::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_FORGET; }

private:
    TEvPrivate::TEvForget::TPtr Ev;
};


bool TTxForget::Execute(TTransactionContext& txc, const TActorContext&) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG("TTxForget.Execute at tablet " << Self->TabletID());

    txc.DB.NoMoreReadsForTx();
    //NIceDb::TNiceDb db(txc.DB);

    auto& msg = *Ev->Get();
    auto status = msg.Status;

    if (status == NKikimrProto::OK) {
        TBlobManagerDb blobManagerDb(txc.DB);

        TString strBlobs;
        for (auto& evict : msg.Evicted) {
            bool erased = Self->BlobManager->EraseOneToOne(evict, blobManagerDb);
            if (erased) {
                strBlobs += "'" + evict.Blob.ToStringNew() + "' ";
            } else {
                LOG_S_ERROR("Forget unknown blob " << evict.Blob << " at tablet " << Self->TabletID());
            }
        }
        LOG_S_NOTICE("Forget evicted blobs " << strBlobs << "at tablet " << Self->TabletID());

        Self->IncCounter(COUNTER_FORGET_SUCCESS);
    } else {
        Self->IncCounter(COUNTER_FORGET_FAIL);
    }

    return true;
}

void TTxForget::Complete(const TActorContext&) {
    LOG_S_DEBUG("TTxForget.Complete at tablet " << Self->TabletID());
}


void TColumnShard::Handle(TEvPrivate::TEvForget::TPtr& ev, const TActorContext& ctx) {
    auto status = ev->Get()->Status;
    bool error = status == NKikimrProto::ERROR;

    if (error) {
        LOG_S_WARN("Forget (fail): '" << ev->Get()->ErrorStr << "' at tablet " << TabletID());
    } else if (status == NKikimrProto::OK) {
        LOG_S_DEBUG("Forget (apply) at tablet " << TabletID());

        Execute(new TTxForget(this, ev), ctx);
    } else {
        Y_VERIFY(false);
    }
}

}
