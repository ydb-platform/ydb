#include "client.h"

void NKikimr::TTxAllocatorClient::AddAllocationRange(const NKikimr::TTxAllocatorClient::TTabletId &from, const NKikimrTx::TEvTxAllocateResult &allocation) {
    ReservedRanges.emplace_back(from, allocation.GetRangeBegin(), allocation.GetRangeEnd());
    Capacity += ReservedRanges.back().Capacity();
}

NKikimr::TTxAllocatorClient::TTxAllocatorClient(NKikimrServices::EServiceKikimr service, NKikimr::NTabletPipe::IClientCache *pipeClientCache, TVector<NKikimr::TTxAllocatorClient::TTabletId> txAllocators)
    : Service(service)
    , PipeClientCache(pipeClientCache)
    , TxAllocators(std::move(txAllocators))
    , MaxCapacity(TxAllocators.size() * RequestPerAllocator)
{
    Y_ABORT_UNLESS(!TxAllocators.empty());
}

void NKikimr::TTxAllocatorClient::Bootstrap(const NActors::TActorContext &ctx) {
    for (const TTabletId& tabletId : TxAllocators) {
        RegisterRequest(tabletId, ctx);
    }
}

TVector<ui64> NKikimr::TTxAllocatorClient::AllocateTxIds(ui64 count, const NActors::TActorContext &ctx) {
    Y_VERIFY_S(count < MaxCapacity,
               "AllocateTxIds: requested too many txIds."
               << " Requested: " << count
               << " TxAllocators count: " << TxAllocators.size()
               << " RequestPerAllocator: " << RequestPerAllocator
               << " MaxCapacity: " << MaxCapacity);

    if (count >= BatchAllocationWarning) {
        LOG_WARN_S(ctx, Service,
                   "AllocateTxIds: requested many txIds. Just a warning, request is processed."
                   << " Requested: " << count
                   << " TxAllocators count: " << TxAllocators.size()
                   << " RequestPerAllocator: " << RequestPerAllocator
                   << " MaxCapacity: " << MaxCapacity
                   << " BatchAllocationWarning: " << BatchAllocationWarning);
    }

    if (count > Capacity) {
        return TVector<ui64>();
    }

    TVector<ui64> txIds;
    txIds.reserve(count);

    while (count && ReservedRanges) {
        TAllocationRange& head = ReservedRanges.front();

        while (count && head.Capacity()) {
            txIds.push_back(head.Allocate());
            --Capacity;
            --count;

            if (head.Capacity() == PreRequestThreshhold) {
                RegisterRequest(head.AllocatedFrom(), ctx);
            }
        }

        if (head.Empty()) {
            ReservedRanges.pop_front();
        }
    }

    return txIds;
}

void NKikimr::TTxAllocatorClient::SendRequest(const NKikimr::TTxAllocatorClient::TTabletId &txAllocator, const NActors::TActorContext &ctx) {
    if (!IsActualRequest(txAllocator)) {
        return;
    }

    const ui64 cookie = txAllocator;
    PipeClientCache->Send(ctx, txAllocator, new TEvTxAllocator::TEvAllocate(RequestPerAllocator), cookie);
}

bool NKikimr::TTxAllocatorClient::OnAllocateResult(TEvTxAllocator::TEvAllocateResult::TPtr &ev, const NActors::TActorContext &ctx) {
    const TTabletId txAllocator = ev->Cookie;
    if (!IsActualRequest(txAllocator)) {
        return false;
    }

    const NKikimrTx::TEvTxAllocateResult& record = ev->Get()->Record;
    LOG_DEBUG_S(ctx, Service,
                "Handle TEvAllocateResult ACCEPTED " <<
                " RangeBegin# " << record.GetRangeBegin() <<
                " RangeEnd# " << record.GetRangeEnd() <<
                " txAllocator# " << txAllocator);

    if (record.GetStatus() == NKikimrTx::TEvTxAllocateResult::IMPOSIBLE) {
        LOG_ERROR_S(ctx, Service,
                    "Handle TEvAllocateResult receive IMPOSIBLE status" <<
                    " allocator is exhausted " <<
                    " txAllocator# " << txAllocator);
        DeregisterRequest(txAllocator);

        return false;
    }

    Y_ABORT_UNLESS(record.GetStatus() == NKikimrTx::TEvTxAllocateResult::SUCCESS);

    AddAllocationRange(txAllocator, record);
    DeregisterRequest(txAllocator);

    return true;
}
