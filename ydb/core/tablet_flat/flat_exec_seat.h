#pragma once

#include "defs.h"
#include "tablet_flat_executor.h"
#include "flat_sausagecache.h"

#include <util/generic/ptr.h>
#include <util/system/hp_timer.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TSeat {
        using TPinned = THashMap<TLogoBlobID, THashMap<ui32, TIntrusivePtr<TPrivatePageCachePinPad>>>;

        TSeat(const TSeat&) = delete;

        TSeat(ui32 uniqId, TAutoPtr<ITransaction> self)
            : UniqID(uniqId)
            , Self(self)
        {
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out << "Tx{" << UniqID << ", ";
            Self->Describe(out);
            out << "}";
        }

        void Complete(const TActorContext& ctx, bool isRW) noexcept;

        void Terminate(ETerminationReason reason, const TActorContext& ctx) noexcept;

        void StartEnqueuedSpan() noexcept {
            WaitingSpan = NWilson::TSpan(TWilsonTablet::TabletDetailed, Self->TxSpan.GetTraceId(), "Tablet.Transaction.Enqueued");
        }

        void FinishEnqueuedSpan() noexcept {
            WaitingSpan.EndOk();
        }

        void CreatePendingSpan() noexcept {
            WaitingSpan = NWilson::TSpan(TWilsonTablet::TabletDetailed, Self->TxSpan.GetTraceId(), "Tablet.Transaction.Pending");
        }

        void FinishPendingSpan() noexcept {
            WaitingSpan.EndOk();
        }

        NWilson::TTraceId GetTxTraceId() const noexcept {
            return Self->TxSpan.GetTraceId();
        }

        const ui64 UniqID = Max<ui64>();
        const TAutoPtr<ITransaction> Self;
        NWilson::TSpan WaitingSpan;
        ui64 Retries = 0;
        TPinned Pinned;

        THPTimer LatencyTimer;
        THPTimer CommitTimer;

        double CPUExecTime = 0;
        double CPUBookkeepingTime = 0;

        ui64 MemoryTouched = 0;
        ui64 RequestedMemory = 0;

        ui64 CurrentTxDataLimit = 0;
        ui64 CurrentMemoryLimit = 0;
        ui32 NotEnoughMemoryCount = 0;
        ui64 TaskId = 0;

        TAutoPtr<TMemoryToken> AttachedMemory;
        TIntrusivePtr<TMemoryGCToken> CapturedMemory;
        TVector<std::function<void()>> OnPersistent;

        ETerminationReason TerminationReason = ETerminationReason::None;

        TSeat *NextCommitTx = nullptr;
    };


}
}
