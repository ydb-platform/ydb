#include "common.h"
#include "events.h"
#include "keys.h"
#include "schema.h"
#include "scan_actor_base_impl.h"

#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView {

using namespace NActors;

template <typename TEntry, typename TRequest, typename TResponse,
    typename TEvRequest, typename TEvResponse, typename TExtractorMap, typename... T>
class TProcessorScan : public TScanActorBase<
    TProcessorScan<TEntry, TRequest, TResponse, TEvRequest, TEvResponse, TExtractorMap, T...>>
{
public:
    using TScan = TProcessorScan<TEntry, TRequest, TResponse, TEvRequest, TEvResponse, TExtractorMap, T...>;
    using TBase = TScanActorBase<TScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TProcessorScan(
        const NActors::TActorId& ownerId,
        ui32 scanId,
        const TTableId& tableId,
        const TTableRange& tableRange,
        const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        NKikimrSysView::EStatsType type)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
        ConvertKeyRange<TRequest, T...>(Request, this->TableRange);
        Request.SetType(type);
    }

    STATEFN(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, this->HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, this->HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                SVLOG_CRIT("NSysView::TProcessorScan: unexpected event# " << ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        TBase::Become(&TScan::StateScan);
        if (this->AckReceived) {
            RequestBatch();
        }
    }

    void RequestBatch() {
        if (!this->SysViewProcessorId) {
            SVLOG_W("NSysView::TProcessorScan: no sysview processor for database " << this->TenantName
                << ", sending empty response");
            this->ReplyEmptyAndDie();
            return;
        }

        if (this->BatchRequestInFlight) {
            return;
        }

        auto req = MakeHolder<TEvRequest>();
        req->Record.CopyFrom(Request);

        TBase::Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(req.Release(), this->SysViewProcessorId, true),
            IEventHandle::FlagTrackDelivery);

        this->BatchRequestInFlight = true;
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        RequestBatch();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        this->ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
            "NSysView::TProcessorScan: delivery problem");
    }

    void Handle(typename TEvResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.HasOverloaded() && record.GetOverloaded()) {
            this->ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
                "NSysView::TProcessorScan: SysViewProcessor is overloaded");
            return;
        }

        this->template ReplyBatch<TEvResponse, TEntry, TExtractorMap, true>(ev);

        if (!record.GetLastBatch()) {
            Y_ABORT_UNLESS(record.HasNext());
            Request.MutableFrom()->CopyFrom(record.GetNext());
            Request.SetInclusiveFrom(true);
        }

        this->BatchRequestInFlight = false;
    }

    void PassAway() override {
        TBase::Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    TRequest Request;
};

template <typename TEntry>
using TExtractorFunc = std::function<TCell(const TEntry&)>;

} // NKikimr::NSysView
