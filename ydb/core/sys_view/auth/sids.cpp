#include "sids.h"

#include <ydb/core/sys_view/common/scan_actor_base_impl.h>

namespace NKikimr::NSysView {

class TSidsScan : public TScanActorBase<TSidsScan> {
public:
    using TBase = TScanActorBase<TSidsScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TSidsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
        if (auto cellsFrom = TableRange.From.GetCells(); cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
            From = TString(cellsFrom[0].Data(), cellsFrom[0].Size());
        }
        FromInclusive = TableRange.FromInclusive;

        if (auto cellsTo = TableRange.To.GetCells(); cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
            To = TString(cellsTo[0].Data(), cellsTo[0].Size());
        }
        ToInclusive = TableRange.ToInclusive;
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TNodesScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TSidsScan::StateScan);
        if (AckReceived) {
            StartScan();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void StartScan() {
        

        RequestDone();
    }

    void RequestDone() {
        if (false) {
            return;
        }

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;        

        SendBatch(std::move(batch));
    }

private:
    TString From;
    bool FromInclusive = false;

    TString To;
    bool ToInclusive = false;
};

THolder<NActors::IActor> CreateSidsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TSidsScan>(ownerId, scanId, tableId, tableRange, columns);
}

}
