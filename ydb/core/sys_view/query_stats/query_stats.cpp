#include "query_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/keys.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/sys_view/service/query_history.h>
#include <ydb/core/sys_view/service/sysview_service.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NSysView {

using namespace NActors;

TQueryStatsBucketRange::TQueryStatsBucketRange(const TSerializedTableRange& range, const TDuration& bucketSize) {
    const ui64 bucketSizeUs = bucketSize.MicroSeconds();

    const auto& cellsFrom = range.From.GetCells();
    const auto& cellsTo = range.To.GetCells();

    if (cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
        auto fromUs = cellsFrom[0].AsValue<ui64>();
        FromBucket = fromUs / bucketSizeUs;
        bool exactBound = (FromBucket * bucketSizeUs == fromUs);

        TMaybe<ui32> rank;
        if (cellsFrom.size() > 1 && !cellsFrom[1].IsNull()) {
            rank = cellsFrom[1].AsValue<ui32>();
            if (!range.FromInclusive && *rank < std::numeric_limits<ui32>::max()) {
                ++*rank;
            }
        }

        if (FromBucket) {
            if (exactBound && (range.FromInclusive || rank)) {
                --FromBucket;
                if (rank) {
                    FromRank = rank;
                }
            }
        }
    }

    if (cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
        auto toUs = cellsTo[0].template AsValue<ui64>();
        ToBucket = toUs / bucketSizeUs;
        bool exactBound = (ToBucket * bucketSizeUs == toUs);

        TMaybe<ui32> rank;
        if (cellsTo.size() > 1 && !cellsTo[1].IsNull()) {
            rank = cellsTo[1].template AsValue<ui32>();
            if (!range.ToInclusive && *rank) {
                --*rank;
            }
        }

        if (ToBucket) {
            --ToBucket;
            if (exactBound) {
                if (rank) {
                    ToRank = rank;
                } else if (!range.ToInclusive) {
                    if (!ToBucket) {
                        IsEmpty = true;
                    } else {
                        --ToBucket;
                    }
                }
            }
        } else {
            IsEmpty = true;
        }
    }

    if (FromBucket > ToBucket) {
        IsEmpty = true;
    }
}

template <typename TGreater>
class TQueryStatsScan : public TScanActorBase<TQueryStatsScan<TGreater>> {
public:
    using TBase = TScanActorBase<TQueryStatsScan<TGreater>>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TQueryStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        NKikimrSysView::EStatsType statsType,
        ui64 bucketCount, const TDuration& bucketSize)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
        , StatsType(statsType)
        , BucketRange(this->TableRange, bucketSize)
    {
        auto now = TAppData::TimeProvider->Now();
        History = MakeHolder<TScanQueryHistory<TGreater>>(bucketCount, bucketSize, now);

        ConvertKeyRange<NKikimrSysView::TEvGetQueryMetricsRequest, ui64, ui32>(Request, this->TableRange);
        Request.SetType(statsType);
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvSysView::TEvGetQueryStatsResult, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeConnected, Connected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvPrivate::TEvRetryNode, RetryNode);
            hFunc(TEvSysView::TEvGetQueryStatsResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, TBase::HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, TBase::HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TQueryStatsScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    struct TEvPrivate {
        enum EEv {
            EvRetryNode = EventSpaceBegin(TEvents::ES_PRIVATE),

            EvEnd
        };

        struct TEvRetryNode : public TEventLocal<TEvRetryNode, EvRetryNode> {
            ui32 NodeId;

            explicit TEvRetryNode(ui32 nodeId)
                : NodeId(nodeId)
            {}
        };
    };

    void ProceedToScan() override {
        this->Become(&TQueryStatsScan<TGreater>::StateScan);
        if (this->AckReceived) {
            StartScan();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        this->ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Delivery problem in query stats scan");
    }

    void RequestBatch() {
        if (this->BatchRequestInFlight) {
            return;
        }

        this->UseProcessor = true;

        auto request = MakeHolder<TEvSysView::TEvGetQueryMetricsRequest>();
        request->Record.CopyFrom(Request);

        this->Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(request.Release(), this->SysViewProcessorId, true),
            IEventHandle::FlagTrackDelivery);

        this->BatchRequestInFlight = true;
    }

    void StartScan() {
        if (AppData()->FeatureFlags.GetEnablePersistentQueryStats() && this->SysViewProcessorId) {
            RequestBatch();
        } else {
            StartOldScan();
        }
    }

    void StartOldScan() {
        if (BucketRange.IsEmpty || this->TenantNodes.empty()) {
            this->ReplyEmptyAndDie();
            return;
        }

        NodesToRequest.reserve(this->TenantNodes.size());
        for (const auto& nodeId : this->TenantNodes) {
            Nodes[nodeId] = TRetryState{false, 0, StartRetryInterval};
            NodesToRequest.push_back(nodeId);
        }

        SendRequests();
    }

    void ScheduleRetry(ui32 nodeId) {
        if (NodesReplied.find(nodeId) != NodesReplied.end()) {
            return;
        }

        auto& retryState = Nodes[nodeId];
        if (++retryState.Attempt == MaxRetryAttempts) {
            NodesReplied.insert(nodeId); // TODO: warnings (answer is partial)
        } else {
            this->Schedule(retryState.Interval, new typename TEvPrivate::TEvRetryNode(nodeId));
            retryState.Interval = TDuration::MilliSeconds((ui64)(retryState.Interval.MilliSeconds() * RetryMultiplier));
        }

        SendRequests();
    }

    void Handle(TEvSysView::TEvGetQueryStatsResult::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;

        History->Merge(std::move(ev->Get()->Record));
        NodesReplied.insert(nodeId);

        auto& retryState = Nodes[nodeId];
        if (retryState.InFly) {
            --RequestsInFly;
            retryState.InFly = false;
        }

        SendRequests();
    }

    void RetryNode(typename TEvPrivate::TEvRetryNode::TPtr& ev) {
        auto nodeId = ev->Get()->NodeId;
        NodesToRequest.push_back(nodeId);
        SendRequests();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        auto& retryState = Nodes[nodeId];

        if (retryState.InFly) {
            retryState.InFly = false;
            --RequestsInFly;
            ScheduleRetry(nodeId);
        }
    }

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr&) {
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        auto& retryState = Nodes[nodeId];

        if (retryState.InFly) {
            --RequestsInFly;
            retryState.InFly = false;
            ScheduleRetry(nodeId);
        }
    }

    void SendRequests() {
        while (!NodesToRequest.empty() && RequestsInFly < MaxRequestsInFly) {
            auto nodeId = NodesToRequest.back();
            NodesToRequest.pop_back();

            auto sysViewServiceID = MakeSysViewServiceID(nodeId);

            auto request = MakeHolder<TEvSysView::TEvGetQueryStats>();
            request->Record.SetStatsType(StatsType);
            request->Record.SetStartBucket(std::max(History->GetStartBucket(), BucketRange.FromBucket));
            request->Record.SetTenantName(this->TenantName);

            this->Send(sysViewServiceID, std::move(request),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);

            Nodes[nodeId].InFly = true;
            ++RequestsInFly;
        }

        if (NodesReplied.size() == Nodes.size()) {
            ReplyAndDie();
        }
    }

    using TEntry = NKikimrSysView::TQueryStatsEntry;
    using TExtractor = std::function<TCell(const TEntry&)>;
    using TSchema = Schema::QueryStats;

    struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
        TExtractorsMap() {
            insert({TSchema::IntervalEnd::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetKey().GetIntervalEndUs());
            }});
            insert({TSchema::Rank::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui32>(entry.GetKey().GetRank());
            }});
            insert({TSchema::QueryText::ColumnId, [] (const TEntry& entry) {
                const auto& text = entry.GetStats().GetQueryText();
                return TCell(text.data(), text.size());
            }});
            insert({TSchema::Duration::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<i64>(entry.GetStats().GetDurationMs() * 1000);
            }});
            insert({TSchema::EndTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetEndTimeMs() * 1000);
            }});
            insert({TSchema::ReadRows::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetStats().GetReadRows());
            }});
            insert({TSchema::ReadBytes::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetStats().GetReadBytes());
            }});
            insert({TSchema::UpdateRows::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetStats().GetUpdateRows());
            }});
            insert({TSchema::UpdateBytes::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetStats().GetUpdateBytes());
            }});
            insert({TSchema::DeleteRows::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetStats().GetDeleteRows());
            }});
            insert({TSchema::DeleteBytes::ColumnId, [] (const TEntry&) {
                return TCell::Make<ui64>(0); // deprecated
            }});
            insert({TSchema::Partitions::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetStats().GetPartitionCount());
            }});
            insert({TSchema::UserSID::ColumnId, [] (const TEntry& entry) {
                const auto& stats = entry.GetStats();
                if (!stats.HasUserSID()) {
                    return TCell();
                }
                const auto& sid = stats.GetUserSID();
                return TCell(sid.data(), sid.size());
            }});
            insert({TSchema::ParametersSize::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetParametersSize());
            }});
            insert({TSchema::CompileDuration::ColumnId, [] (const TEntry& entry) {
                const auto& stats = entry.GetStats();
                return stats.HasCompileDurationMs() ? TCell::Make<ui64>(stats.GetCompileDurationMs() * 1000) : TCell();
            }});
            insert({TSchema::FromQueryCache::ColumnId, [] (const TEntry& entry) {
                const auto& stats = entry.GetStats();
                return stats.HasFromQueryCache() ? TCell::Make<bool>(stats.GetFromQueryCache()) : TCell();
            }});
            insert({TSchema::CPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetTotalCpuTimeUs());
            }});
            insert({TSchema::ShardCount::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetShardsCpuTimeUs().GetCnt());
            }});
            insert({TSchema::SumShardCPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetShardsCpuTimeUs().GetSum());
            }});
            insert({TSchema::MinShardCPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetShardsCpuTimeUs().GetMin());
            }});
            insert({TSchema::MaxShardCPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetShardsCpuTimeUs().GetMax());
            }});
            insert({TSchema::ComputeNodesCount::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetComputeCpuTimeUs().GetCnt());
            }});
            insert({TSchema::SumComputeCPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetComputeCpuTimeUs().GetSum());
            }});
            insert({TSchema::MinComputeCPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetComputeCpuTimeUs().GetMin());
            }});
            insert({TSchema::MaxComputeCPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetComputeCpuTimeUs().GetMax());
            }});
            insert({TSchema::CompileCPUTime::ColumnId, [] (const TEntry& entry) {
                const auto& stats = entry.GetStats();
                return stats.HasCompileCpuTimeUs() ? TCell::Make<ui64>(stats.GetCompileCpuTimeUs()) : TCell();
            }});
            insert({TSchema::ProcessCPUTime::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetProcessCpuTimeUs());
            }});
            insert({TSchema::TypeCol::ColumnId, [] (const TEntry& entry) {
                const auto& type = entry.GetStats().GetType();
                return TCell(type.data(), type.size());
            }});
            insert({TSchema::RequestUnits::ColumnId, [] (const TEntry& entry) {
                return TCell::Make<ui64>(entry.GetStats().GetRequestUnits());
            }});
        }
    };

    void Handle(TEvSysView::TEvGetQueryStatsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.HasOverloaded() && record.GetOverloaded()) {
            this->ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "SysViewProcessor is overloaded");
            return;
        }

        this->template ReplyBatch<TEvSysView::TEvGetQueryStatsResponse, TEntry, TExtractorsMap, true>(ev);

        if (!record.GetLastBatch()) {
            Y_ABORT_UNLESS(record.HasNext());
            Request.MutableFrom()->CopyFrom(record.GetNext());
            Request.SetInclusiveFrom(true);
        }

        this->BatchRequestInFlight = false;
    }

    void ReplyAndDie() {
        static TExtractorsMap extractors;

        auto fromBucket = std::max(History->GetStartBucket(), BucketRange.FromBucket);
        auto toBucket = std::min(History->GetLastBucket(), BucketRange.ToBucket);

        if (fromBucket > toBucket) {
            this->ReplyEmptyAndDie();
        }

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(this->ScanId);
        batch->Finished = true;

        TEntry entry;
        auto& key = *entry.MutableKey();
        key.SetIntervalEndUs((fromBucket + 1) * History->GetBucketSizeMs() * 1000);

        for (ui64 bucket = fromBucket; bucket <= toBucket; ++bucket) {
            const auto& top = History->GetBucketTop(bucket);

            ui32 fromRank = 1;
            if (BucketRange.FromRank && bucket == BucketRange.FromBucket) {
                fromRank = std::max(fromRank, *BucketRange.FromRank);
            }

            ui32 toRank = top.StatsSize();
            if (BucketRange.ToRank && bucket == BucketRange.ToBucket) {
                toRank = std::min(toRank, *BucketRange.ToRank);
            }

            TVector<TCell> cells;
            for (ui32 r = fromRank; r <= toRank; ++r) {
                key.SetRank(r);
                entry.MutableStats()->CopyFrom(top.GetStats(r - 1));

                for (auto column : this->Columns) {
                    auto extractor = extractors.find(column.Tag);
                    if (extractor == extractors.end()) {
                        cells.push_back(TCell());
                    } else {
                        cells.push_back(extractor->second(entry));
                    }
                }

                TArrayRef<const TCell> ref(cells);
                batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
                cells.clear();
            }

            key.SetIntervalEndUs(key.GetIntervalEndUs() + History->GetBucketSizeMs() * 1000);
        }

        TBase::SendBatch(std::move(batch));
    }

    void PassAway() override {
        if (UseProcessor) {
            this->Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        } else {
            for (auto& [nodeId, _] : Nodes) {
                this->Send(TActorContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
            }
        }
        TBase::PassAway();
    }

private:
    static constexpr size_t MaxRequestsInFly = 10;

    static constexpr size_t MaxRetryAttempts = 5;
    static constexpr TDuration StartRetryInterval = TDuration::MilliSeconds(500);
    static constexpr double RetryMultiplier = 2.0;

    const NKikimrSysView::EStatsType StatsType;

    TQueryStatsBucketRange BucketRange;

    struct TRetryState {
        bool InFly = false;
        ui32 Attempt = 0;
        TDuration Interval;
    };

    THashMap<ui32, TRetryState> Nodes;
    THashSet<ui32> NodesReplied;

    TVector<ui32> NodesToRequest;
    size_t RequestsInFly = 0;

    THolder<TScanQueryHistory<TGreater>> History;

    bool UseProcessor = false;
    NKikimrSysView::TEvGetQueryMetricsRequest Request;
};

THolder<NActors::IActor> CreateQueryStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    auto viewName = tableId.SysViewInfo;

    if (viewName == TopQueriesByDuration1MinuteName) {
        return MakeHolder<TQueryStatsScan<TDurationGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_DURATION_ONE_MINUTE,
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE);

    } else if (viewName == TopQueriesByDuration1HourName) {
        return MakeHolder<TQueryStatsScan<TDurationGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_DURATION_ONE_HOUR,
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE);

    } else if (viewName == TopQueriesByReadBytes1MinuteName) {
        return MakeHolder<TQueryStatsScan<TReadBytesGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE,
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE);

    } else if (viewName == TopQueriesByReadBytes1HourName) {
        return MakeHolder<TQueryStatsScan<TReadBytesGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_READ_BYTES_ONE_HOUR,
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE);

    } else if (viewName == TopQueriesByCpuTime1MinuteName) {
        return MakeHolder<TQueryStatsScan<TCpuTimeGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE,
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE);

    } else if (viewName == TopQueriesByCpuTime1HourName) {
        return MakeHolder<TQueryStatsScan<TCpuTimeGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_CPU_TIME_ONE_HOUR,
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE);
    } else if (viewName == TopQueriesByRequestUnits1MinuteName) {
        return MakeHolder<TQueryStatsScan<TRequestUnitsGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE,
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE);

    } else if (viewName == TopQueriesByRequestUnits1HourName) {
        return MakeHolder<TQueryStatsScan<TRequestUnitsGreater>>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR,
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE);
    }
    return {};
}

} // NSysView
} // NKikimr
