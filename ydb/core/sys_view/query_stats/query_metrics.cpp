#include "query_metrics.h"

#include <ydb/core/sys_view/common/common.h> 
#include <ydb/core/sys_view/common/events.h> 
#include <ydb/core/sys_view/common/keys.h> 
#include <ydb/core/sys_view/common/schema.h> 
#include <ydb/core/sys_view/common/scan_actor_base_impl.h> 

#include <ydb/core/base/tablet_pipecache.h> 

#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr {
namespace NSysView {

using namespace NActors;

template <>
void SetField<0>(NKikimrSysView::TQueryMetricsKey& key, ui64 value) {
    key.SetIntervalEndUs(value);
}

template <>
void SetField<1>(NKikimrSysView::TQueryMetricsKey& key, ui32 value) {
    key.SetRank(value);
}

class TQueryMetricsScan : public TScanActorBase<TQueryMetricsScan> {
public:
    using TBase = TScanActorBase<TQueryMetricsScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TQueryMetricsScan(const TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
        ConvertKeyRange<NKikimrSysView::TEvGetQueryMetricsRequest, ui64, ui32>(Request, TableRange);
        Request.SetType(NKikimrSysView::METRICS_ONE_MINUTE);
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvSysView::TEvGetQueryMetricsResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(ctx, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TQueryMetricsScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TThis::StateScan);
        if (AckReceived) {
            RequestBatch();
        }
    }

    void RequestBatch() {
        if (!SysViewProcessorId) {
            SVLOG_W("No sysview processor for database " << TenantName
                << ", sending empty response");
            ReplyEmptyAndDie();
            return;
        }

        if (BatchRequestInFlight) {
            return;
        }

        auto request = MakeHolder<TEvSysView::TEvGetQueryMetricsRequest>();
        request->Record.CopyFrom(Request);

        Send(MakePipePeNodeCacheID(false),
            new TEvPipeCache::TEvForward(request.Release(), SysViewProcessorId, true),
            IEventHandle::FlagTrackDelivery);

        BatchRequestInFlight = true;
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        RequestBatch();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Delivery problem in query metrics scan");
    }

    void Handle(TEvSysView::TEvGetQueryMetricsResponse::TPtr& ev) {
        using TEntry = NKikimrSysView::TQueryMetricsEntry;
        using TExtractor = std::function<TCell(const TEntry&)>;
        using TSchema = Schema::QueryMetrics;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::IntervalEnd::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui64>(entry.GetKey().GetIntervalEndUs());
                }});
                insert({TSchema::Rank::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui32>(entry.GetKey().GetRank());
                }});
                insert({TSchema::QueryText::ColumnId, [] (const TEntry& entry) {
                    const auto& text = entry.GetQueryText();
                    return TCell(text.data(), text.size());
                }});
                insert({TSchema::Count::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui64>(entry.GetMetrics().GetCount());
                }});

#define ADD_METRICS(FieldName, MetricsName)                                                     \
                insert({TSchema::Sum ## FieldName::ColumnId, [] (const TEntry& entry) {         \
                    return TCell::Make<ui64>(entry.GetMetrics().Get ## MetricsName().GetSum()); \
                }});                                                                            \
                insert({TSchema::Min ## FieldName::ColumnId, [] (const TEntry& entry) {         \
                    return TCell::Make<ui64>(entry.GetMetrics().Get ## MetricsName().GetMin()); \
                }});                                                                            \
                insert({TSchema::Max ## FieldName::ColumnId, [] (const TEntry& entry) {         \
                    return TCell::Make<ui64>(entry.GetMetrics().Get ## MetricsName().GetMax()); \
                }});

                ADD_METRICS(CPUTime, CpuTimeUs);
                ADD_METRICS(Duration, DurationUs);
                ADD_METRICS(ReadRows, ReadRows);
                ADD_METRICS(ReadBytes, ReadBytes);
                ADD_METRICS(UpdateRows, UpdateRows);
                ADD_METRICS(UpdateBytes, UpdateBytes);
                ADD_METRICS(DeleteRows, DeleteRows);
                ADD_METRICS(RequestUnits, RequestUnits);
#undef ADD_METRICS
            }
        };

        const auto& record = ev->Get()->Record;
        if (record.HasOverloaded() && record.GetOverloaded()) {
            ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "SysViewProcessor is overloaded");
            return;
        }

        ReplyBatch<TEvSysView::TEvGetQueryMetricsResponse, TEntry, TExtractorsMap, true>(ev);

        if (!record.GetLastBatch()) {
            Y_VERIFY(record.HasNext());
            Request.MutableFrom()->CopyFrom(record.GetNext());
            Request.SetInclusiveFrom(true);
        }

        BatchRequestInFlight = false;
    }

    void PassAway() override {
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    NKikimrSysView::TEvGetQueryMetricsRequest Request;
};

THolder<IActor> CreateQueryMetricsScan(const TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TQueryMetricsScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NSysView
} // NKikimr
