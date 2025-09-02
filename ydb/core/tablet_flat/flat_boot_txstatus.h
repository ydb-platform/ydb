#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_dbase_naked.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TBootTxStatus final : public IStep {
    public:
        TBootTxStatus() = delete;

        TBootTxStatus(IStep *owner, ui32 table, TSwitch::TTxStatus &txStatus)
            : IStep(owner, EStep::TxStatus)
            , Table(table)
            , DataId(txStatus.DataId)
            , Epoch(txStatus.Epoch)
        { }

    private:
        void Start() override {
            if (TSharedData* data = Back->TxStatusCaches.FindPtr(DataId.Lead)) {
                TxStatus = MakeIntrusive<NTable::TTxStatusPartStore>(DataId, Epoch, *data);
            } else {
                LeftBlobs += Spawn<TLoadBlobs>(DataId, 0);
            }
            TryFinish();
        }

        void HandleStep(TIntrusivePtr<IStep> step) override
        {
            auto *load = step->ConsumeAs<TLoadBlobs>(LeftBlobs);
            TSharedData data = load->PlainData();
            Back->TxStatusCaches[DataId.Lead] = data;
            TxStatus = MakeIntrusive<NTable::TTxStatusPartStore>(DataId, Epoch, std::move(data));
            TryFinish();
        }

    private:
        void TryFinish() {
            if (!LeftBlobs) {
                // TODO: we probably want to delay merge as late as possible?
                Back->DatabaseImpl->Merge(Table, std::move(TxStatus));
                Env->Finish(this);
            }
        }

    private:
        const ui32 Table;
        const NPageCollection::TLargeGlobId DataId;
        const NTable::TEpoch Epoch;
        TIntrusiveConstPtr<NTable::TTxStatusPartStore> TxStatus;
        TLeft LeftBlobs;
    };

} // namespace NBoot
} // namespace NTabletFlatExecutor
} // namespace NKikimr
