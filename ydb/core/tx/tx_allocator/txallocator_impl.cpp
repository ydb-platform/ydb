#include "txallocator_impl.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_ALLOCATOR

namespace NKikimr {
namespace NTxAllocator {

TTxAllocator::TTxAllocator(const TActorId &tablet, TTabletStorageInfo *info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , PrivateMarker((TabletID() & 0xFFFF) << (64 - 16))
{
}

void TTxAllocator::OnActivateExecutor(const TActorContext &ctx) {
    YDB_LOG_CTX_DEBUG(ctx, "OnActivateExecutor",
        {"tablet", TabletID()});
    Executor()->RegisterExternalTabletCounters(new TProtobufTabletCounters<
                                               ESimpleCounters_descriptor,
                                               ECumulativeCounters_descriptor,
                                               EPercentileCounters_descriptor,
                                               ETxTypes_descriptor
                                               >());
    InitCounters(ctx);
    Execute(CreateTxSchema(), ctx);
}

void TTxAllocator::OnDetach(const TActorContext &ctx) {
    return Die(ctx);
}

void TTxAllocator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    return Die(ctx);
}

void TTxAllocator::DefaultSignalTabletActive(const TActorContext &ctx) {
    Y_UNUSED(ctx);
}

ui64 TTxAllocator::ApplyPrivateMarker(const ui64 elem) {
    return PrivateMarker | elem;
}

void TTxAllocator::InitCounters(const TActorContext &ctx) {
    auto &counters = AppData(ctx)->Counters;
        MonCounters.AllocatorCounters = GetServiceCounters(counters, "tablets")->GetSubgroup("type", "TxAllocator");

        MonCounters.Allocated = MonCounters.AllocatorCounters->GetCounter("Allocated", true);
        MonCounters.AllocationsPresence = MonCounters.AllocatorCounters->GetCounter("AllocationPresence", true);
}

void TTxAllocator::Handle(TEvTxAllocator::TEvAllocate::TPtr &ev, const TActorContext &ctx) {
    MonCounters.AllocationsPresence->Inc();

    const ui64 requestedSize = ev->Get()->Record.GetRangeSize();
    YDB_LOG_CTX_DEBUG(ctx, "HANDLE TEvAllocate requested range",
        {"tablet", TabletID()},
        {"Sender", ev->Sender.ToString()},
        {"size", requestedSize});

    Execute(CreateTxReserve(ev), ctx);
}

void TTxAllocator::Reply(const ui64 rangeBegin, const ui64 rangeEnd, const TEvTxAllocator::TEvAllocate::TPtr &ev, const TActorContext &ctx) {
    YDB_LOG_CTX_DEBUG(ctx, "Send TEvAllocateResult",
        {"tablet", TabletID()},
        {"to_Sender", ev->Sender.ToString()},
        {"from", rangeBegin},
        {"to", rangeEnd});
    *MonCounters.Allocated += rangeEnd - rangeBegin;

    const ui64 begin = ApplyPrivateMarker(rangeBegin);
    const ui64 end = ApplyPrivateMarker(rangeEnd);

    ctx.Send(ev->Sender, new TEvTxAllocator::TEvAllocateResult(begin, end), 0, ev->Cookie);
}

void TTxAllocator::ReplyImposible(const TEvTxAllocator::TEvAllocate::TPtr &ev, const TActorContext &ctx) {
    static const auto status = NKikimrTx::TEvTxAllocateResult::IMPOSIBLE;
    YDB_LOG_CTX_ERROR(ctx, "Send TEvAllocateResult",
        {"tablet", TabletID()},
        {"to_Sender", ev->Sender.ToString()},
        {"status", status});
    ctx.Send(ev->Sender, new TEvTxAllocator::TEvAllocateResult(status), 0, ev->Cookie);
}

}
}
