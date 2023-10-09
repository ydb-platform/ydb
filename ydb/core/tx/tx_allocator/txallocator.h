#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/tx.pb.h>

namespace NKikimr {
IActor* CreateTxAllocator(const TActorId &tablet, TTabletStorageInfo *info);
}

namespace NKikimr {
struct TEvTxAllocator {
    enum EEv {
        EvAllocate = EventSpaceBegin(TKikimrEvents::ES_TX_ALLOCATOR),
        EvAllocateResult,

        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_ALLOCATOR), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_ALLOCATOR)");

    struct TEvAllocate : public TEventPB<TEvAllocate, NKikimrTx::TEvTxAllocate, EvAllocate> {
        TEvAllocate()
        {}

        TEvAllocate(ui64 count)
        {
            Record.SetRangeSize(count);
        }
    };

    struct TEvAllocateResult : public TEventPB<TEvAllocateResult, NKikimrTx::TEvTxAllocateResult, EvAllocateResult> {
        TEvAllocateResult()
        {}

        TEvAllocateResult(NKikimrTx::TEvTxAllocateResult::EStatus status)
        {
            Y_ABORT_UNLESS(status != NKikimrTx::TEvTxAllocateResult::SUCCESS);
            Record.SetStatus(status);
        }

        TEvAllocateResult(ui64 begin, ui64 end)
        {
            Y_ABORT_UNLESS(ExtractPrivateMarker(begin) != 0, "privat marker hasn't found in allocation");
            Y_ABORT_UNLESS(ExtractPrivateMarker(begin) == ExtractPrivateMarker(end), "privat marker incorrect");

            Record.SetStatus(NKikimrTx::TEvTxAllocateResult::SUCCESS);
            Record.SetRangeBegin(begin);
            Record.SetRangeEnd(end);
        }

        ui64 AllocationMask();
        ui64 ExtractPrivateMarker(ui64 item);
    };
};
}

template<>
inline void Out<NKikimrTx::TEvTxAllocateResult::EStatus>(IOutputStream& o, NKikimrTx::TEvTxAllocateResult::EStatus x) {
    o << NKikimrTx::TEvTxAllocateResult::EStatus_Name(x).data();
    return;
}


