#pragma once

#include "defs.h"

#include <util/generic/vector.h>


namespace NKikimr {
struct TAppData;

struct TEvTxAllocatorClient {
    enum EEv {
        EvAllocate = EventSpaceBegin(TKikimrEvents::ES_TX_ALLOCATOR_CLIENT),
        EvAllocateResult,

        EvEnd,
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_ALLOCATOR_CLIENT),
        "expect End < EventSpaceEnd(ES_TX_ALLOCATOR_CLIENT)"
    );

    struct TEvAllocate: public TEventLocal<TEvAllocate, EvAllocate> {
        const ui32 Count = 1;

        TEvAllocate() = default;

        explicit TEvAllocate(ui32 count)
            : Count(count)
        {
        }
    };

    struct TEvAllocateResult: public TEventLocal<TEvAllocateResult, EvAllocateResult> {
        const TVector<ui64> TxIds;

        TEvAllocateResult() = default;

        explicit TEvAllocateResult(ui64 txId)
            : TxIds(1, txId)
        {
        }

        explicit TEvAllocateResult(TVector<ui64>&& txIds)
            : TxIds(std::move(txIds))
        {
        }
    };

}; // TTxAllocatorClientEvents

IActor* CreateTxAllocatorClient(TVector<ui64> txAllocators);
IActor* CreateTxAllocatorClient(const TAppData* appData);

} // NKikimr
