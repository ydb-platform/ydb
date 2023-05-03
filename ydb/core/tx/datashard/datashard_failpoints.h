#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr {
namespace NDataShard {

// Allows to Cancel transaction by TabletID and TxId
struct TCancelTxFailPoint {
    TAtomic Enabled;
    TSpinLock Lock;
    ui64 TabletId;
    ui64 TxId;
    ui64 FailAtCount;
    ui64 CurrentCount;
    bool Hit;

    TCancelTxFailPoint() {
        Disable();
    }

    void Enable(ui64 tabletId, ui64 txId, ui64 count) {
        Disable();

        TGuard<TSpinLock> g(Lock);
        TabletId = tabletId;
        TxId = txId;
        FailAtCount = count;
        CurrentCount = 0;
        Hit = false;
        AtomicSet(Enabled, 1);
    }

    void Disable() {
        TGuard<TSpinLock> g(Lock);
        AtomicSet(Enabled, 0);
        TabletId = 0;
        TxId = 0;
        FailAtCount = -1;
        CurrentCount = 0;
        Hit = false;
    }

    bool Check(ui64 tabletId, ui64 txId) {
        if (!AtomicGet(Enabled))
            return false;

        TGuard<TSpinLock> g(Lock);

        ui64 failTabletId = AtomicGet(TabletId);
        ui64 failTxId = AtomicGet(TxId);
        i64 failCount = AtomicGet(FailAtCount);

        if ((tabletId != failTabletId && failTabletId != (ui64)-1) ||
            (txId != failTxId && failTxId != (ui64)-1)) {
            return false;
        }

        i64 prevCount = CurrentCount++;
        if (prevCount == failCount) {
            Hit = true;
            return true;
        }

        return false;
    }
};

// Allows to skip specified number of replies from datashard by TabletID and TxId
struct TSkipRepliesFailPoint {
    TAtomic Enabled;
    TSpinLock Lock;
    ui64 TabletId;
    ui64 TxId;
    ui64 RepliesToSkip;

    TSkipRepliesFailPoint() {
        Disable();
    }

    void Enable(ui64 tabletId, ui64 txId, ui64 count) {
        Disable();

        TGuard<TSpinLock> g(Lock);
        TabletId = tabletId;
        TxId = txId;
        RepliesToSkip = count;

        AtomicSet(Enabled, 1);
    }

    void Disable() {
        TGuard<TSpinLock> g(Lock);

        TabletId = 0;
        TxId = 0;
        RepliesToSkip = 0;

        AtomicSet(Enabled, 0);
    }

    bool Check(ui64 tabletId, ui64 txId) {
        if (!AtomicGet(Enabled)) {
            return false;
        }

        TGuard<TSpinLock> g(Lock);

        if ((tabletId != TabletId && TabletId != (ui64)-1) || (txId != TxId && TxId != (ui64)-1)) {
            return false;
        }

        if (RepliesToSkip == 0) {
            return false;
        }

        --RepliesToSkip;
        return true;
    }
};

// Allows to skip specified number of replies from datashard by TabletID and TxId
struct TSkipReadIteratorResultFailPoint {
    TAtomic Enabled;
    TSpinLock Lock;
    ui64 TabletId;

    TSkipReadIteratorResultFailPoint() {
        Disable();
    }

    void Enable(ui64 tabletId) {
        Disable();

        TGuard<TSpinLock> g(Lock);
        TabletId = tabletId;

        AtomicSet(Enabled, 1);
    }

    void Disable() {
        TGuard<TSpinLock> g(Lock);
        TabletId = 0;

        AtomicSet(Enabled, 0);
    }

    bool Check(ui64 tabletId) {
        if (!AtomicGet(Enabled)) {
            return false;
        }

        TGuard<TSpinLock> g(Lock);

        if (tabletId != TabletId && TabletId != (ui64)-1) {
            return false;
        }

        return true;
    }
};


extern TCancelTxFailPoint gCancelTxFailPoint;
extern TSkipRepliesFailPoint gSkipRepliesFailPoint;
extern TSkipReadIteratorResultFailPoint gSkipReadIteratorResultFailPoint;

}}
