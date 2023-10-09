#pragma once

#include "defs.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimr {

class TTxAllocatorClient {
    using TTabletId = ui64;

    void RegisterRequest(const TTabletId& from, const TActorContext& ctx) {
        if (AllocationRequestsInFly.insert(from).second) {
            SendRequest(from, ctx);
        }
    }

    void DeregisterRequest(const TTabletId& from) {
        AllocationRequestsInFly.erase(from);
    }

    bool IsActualRequest(const TTabletId& from) const {
        return AllocationRequestsInFly.contains(from);
    }

    void AddAllocationRange(const TTabletId& from, const NKikimrTx::TEvTxAllocateResult& allocation);

public:
    explicit TTxAllocatorClient(
        NKikimrServices::EServiceKikimr service,
        NTabletPipe::IClientCache* pipeClientCache,
        TVector<TTabletId> txAllocators);

    void Bootstrap(const TActorContext& ctx);

    TVector<ui64> AllocateTxIds(ui64 count, const TActorContext& ctx);

    void SendRequest(const TTabletId& txAllocator, const TActorContext& ctx);
    bool OnAllocateResult(TEvTxAllocator::TEvAllocateResult::TPtr& ev, const TActorContext& ctx);

private:
    struct TAllocationRange {
        TTabletId Source = 0;
        ui64 Begin = 0;
        ui64 End = 0;

        TAllocationRange() = default;

        explicit TAllocationRange(const TTabletId& source, ui64 begin, ui64 end)
            : Source(source)
            , Begin(begin)
            , End(end)
        {
        }

        TTabletId AllocatedFrom() const {
            return Source;
        }

        ui64 Capacity() const {
            return End - Begin;
        }

        bool Empty() const {
            return Capacity() == 0;
        }

        ui64 Allocate() {
            Y_ABORT_UNLESS(Capacity() >= 1);
            return ++Begin;
        }
    };

    NKikimrServices::EServiceKikimr Service;
    NTabletPipe::IClientCache* PipeClientCache;

    TVector<TTabletId> TxAllocators;
    THashSet<TTabletId> AllocationRequestsInFly;
    static constexpr ui64 BatchAllocationWarning = 500;
    static constexpr ui64 RequestPerAllocator = 5000;
    static constexpr ui64 PreRequestThreshhold = RequestPerAllocator / 3;

    const ui64 MaxCapacity = 0;
    ui64 Capacity = 0;
    TDeque<TAllocationRange> ReservedRanges;

}; // TTxAllocatorClient

} // NKikimr
