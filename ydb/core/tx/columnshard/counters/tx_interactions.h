#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>

#include <ydb/library/signals/owner.h>

namespace NKikimr::NColumnShard {

class TTxInteractionCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::THistogramPtr TxLockRanges;

public:
    TTxInteractionCounters()
        : TBase("tx_interactions")
        , TxLockRanges(TBase::GetHistogram("TxLockRanges", NMonitoring::ExponentialHistogram(18, 1, 4)))
    {
    }

    void OnReadStart(const ui64 txLockRanges) const {
        TxLockRanges->Collect(txLockRanges);
    }
};

}   // namespace NKikimr::NColumnShard
