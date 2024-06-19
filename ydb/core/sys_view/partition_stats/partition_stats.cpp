#include "partition_stats.h"

#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NSysView {

using namespace NActors;

class TPartitionStatsCollector : public TActorBootstrapped<TPartitionStatsCollector> {
public:
    using TBase = TActorBootstrapped<TPartitionStatsCollector>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SYSTEM_VIEW_PART_STATS_COLLECTOR;
    }

    explicit TPartitionStatsCollector(size_t batchSize, size_t pendingRequestsLimit)
        : BatchSize(batchSize)
        , PendingRequestsLimit(pendingRequestsLimit)
    {}

    void Bootstrap() {
        SVLOG_D("NSysView::TPartitionStatsCollector bootstrapped");

        if (AppData()->UsePartitionStatsCollectorForTests) {
            OverloadedPartitionBound = 0.0;
            ProcessOverloadedInterval = TDuration::Seconds(1);
        }

        Schedule(ProcessOverloadedInterval, new TEvPrivate::TEvProcessOverloaded);

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSysView::TEvSetPartitioning, Handle);
            hFunc(TEvSysView::TEvRemoveTable, Handle);
            hFunc(TEvSysView::TEvSendPartitionStats, Handle);
            hFunc(TEvSysView::TEvUpdateTtlStats, Handle);
            hFunc(TEvSysView::TEvGetPartitionStats, Handle);
            hFunc(TEvPrivate::TEvProcess, Handle);
            hFunc(TEvPrivate::TEvProcessOverloaded, Handle);
            hFunc(TEvSysView::TEvInitPartitionStatsCollector, Handle);
            IgnoreFunc(TEvPipeCache::TEvDeliveryProblem);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                SVLOG_CRIT("NSysView::TPartitionStatsCollector: unexpected event " << ev->GetTypeRewrite());
        }
    }

private:
    struct TEvPrivate {
        enum EEv {
            EvProcess = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvProcessOverloaded,

            EvEnd
        };

        struct TEvProcess : public TEventLocal<TEvProcess, EvProcess> {};

        struct TEvProcessOverloaded : public TEventLocal<TEvProcessOverloaded, EvProcessOverloaded> {};
    };

    void Handle(TEvSysView::TEvSetPartitioning::TPtr& ev) {
        const auto& domainKey = ev->Get()->DomainKey;
        const auto& pathId = ev->Get()->PathId;

        auto& tables = DomainTables[domainKey];
        auto tableFound = tables.Stats.find(pathId);
        if (tableFound != tables.Stats.end()) {
            auto& table = tableFound->second;

            auto& oldPartitions = table.Partitions;
            std::unordered_map<TShardIdx, NKikimrSysView::TPartitionStats> newPartitions;
            std::unordered_set<TShardIdx> overloaded;

            for (auto shardIdx : ev->Get()->ShardIndices) {
                auto old = oldPartitions.find(shardIdx);
                if (old != oldPartitions.end()) {
                    newPartitions[shardIdx] = old->second;
                    if (IsPartitionOverloaded(old->second)) {
                        overloaded.insert(shardIdx);
                    }
                }
            }

            if (!overloaded.empty()) {
                tables.Overloaded[pathId].swap(overloaded);
            } else {
                tables.Overloaded.erase(pathId);
            }

            oldPartitions.swap(newPartitions);
            table.ShardIndices.swap(ev->Get()->ShardIndices);
            table.Path = ev->Get()->Path;

        } else {
            auto& table = tables.Stats[pathId];
            table.ShardIndices.swap(ev->Get()->ShardIndices);
            table.Path = ev->Get()->Path;
        }
    }

    void Handle(TEvSysView::TEvRemoveTable::TPtr& ev) {
        const auto& domainKey = ev->Get()->DomainKey;
        const auto& pathId = ev->Get()->PathId;

        auto& tables = DomainTables[domainKey];
        tables.Stats.erase(pathId);
        tables.Overloaded.erase(pathId);
    }

    void Handle(TEvSysView::TEvSendPartitionStats::TPtr& ev) {
        const auto& domainKey = ev->Get()->DomainKey;
        const auto& pathId = ev->Get()->PathId;
        const auto& shardIdx = ev->Get()->ShardIdx;

        auto& tables = DomainTables[domainKey];
        auto tableFound = tables.Stats.find(pathId);
        if (tableFound == tables.Stats.end()) {
            return;
        }

        auto& table = tableFound->second;
        auto& oldStats = table.Partitions[shardIdx];
        auto& newStats = ev->Get()->Stats;

        if (IsPartitionOverloaded(newStats)) {
            tables.Overloaded[pathId].insert(shardIdx);
        } else {
            auto overloadedFound = tables.Overloaded.find(pathId);
            if (overloadedFound != tables.Overloaded.end()) {
                overloadedFound->second.erase(shardIdx);
                if (overloadedFound->second.empty()) {
                    tables.Overloaded.erase(pathId);
                }
            }
        }

        if (oldStats.HasTtlStats()) {
            newStats.MutableTtlStats()->Swap(oldStats.MutableTtlStats());
        }

        oldStats.Swap(&newStats);
    }

    void Handle(TEvSysView::TEvUpdateTtlStats::TPtr& ev) {
        const auto& domainKey = ev->Get()->DomainKey;
        const auto& pathId = ev->Get()->PathId;
        const auto& shardIdx = ev->Get()->ShardIdx;

        auto& tables = DomainTables[domainKey];
        auto tableFound = tables.Stats.find(pathId);
        if (tableFound == tables.Stats.end()) {
            return;
        }

        tableFound->second.Partitions[shardIdx].MutableTtlStats()->Swap(&ev->Get()->Stats);
    }

    void Handle(TEvSysView::TEvGetPartitionStats::TPtr& ev) {
        if (PendingRequests.size() >= PendingRequestsLimit) {
            auto result = MakeHolder<TEvSysView::TEvGetPartitionStatsResult>();
            result->Record.SetOverloaded(true);
            Send(ev->Sender, std::move(result));
            return;
        }

        PendingRequests.push(std::move(ev));

        if (!ProcessInFly) {
            Send(SelfId(), new TEvPrivate::TEvProcess());
            ProcessInFly = true;
        }
    }

    void Handle(TEvPrivate::TEvProcess::TPtr&) {
        ProcessInFly = false;

        if (PendingRequests.empty()) {
            return;
        }

        TEvSysView::TEvGetPartitionStats::TPtr request = std::move(PendingRequests.front());
        PendingRequests.pop();

        if (!PendingRequests.empty()) {
            Send(SelfId(), new TEvPrivate::TEvProcess());
            ProcessInFly = true;
        }

        auto& record = request->Get()->Record;

        auto result = MakeHolder<TEvSysView::TEvGetPartitionStatsResult>();
        result->Record.SetLastBatch(true);

        if (!record.HasDomainKeyOwnerId() || !record.HasDomainKeyPathId()) {
            Send(request->Sender, std::move(result));
            return;
        }

        auto domainKey = TPathId(record.GetDomainKeyOwnerId(), record.GetDomainKeyPathId());
        auto itTables = DomainTables.find(domainKey);
        if (itTables == DomainTables.end()) {
            Send(request->Sender, std::move(result));
            return;
        }
        auto& tables = itTables->second.Stats;

        auto it = tables.begin();
        auto itEnd = tables.end();

        bool fromInclusive = record.HasFromInclusive() && record.GetFromInclusive();
        bool toInclusive = record.HasToInclusive() && record.GetToInclusive();

        TPathId fromPathId;
        TPathId toPathId;

        ui64 startPartIdx = 0;
        ui64 endPartIdx = 0;

        auto& from = record.GetFrom();

        if (from.HasOwnerId()) {
            if (from.HasPathId()) {
                fromPathId = TPathId(from.GetOwnerId(), from.GetPathId());
                if (fromInclusive || from.HasPartIdx()) {
                    it = tables.lower_bound(fromPathId);
                    if (it != tables.end() && it->first == fromPathId && from.GetPartIdx()) {
                        startPartIdx = from.GetPartIdx();
                        if (!fromInclusive) {
                            ++startPartIdx;
                        }
                    }
                } else {
                    it = tables.upper_bound(fromPathId);
                }
            } else {
                if (fromInclusive) {
                    fromPathId = TPathId(from.GetOwnerId(), 0);
                    it = tables.lower_bound(fromPathId);
                } else {
                    fromPathId = TPathId(from.GetOwnerId(), std::numeric_limits<ui64>::max());
                    it = tables.upper_bound(fromPathId);
                }
            }
        }

        auto& to = record.GetTo();

        if (to.HasOwnerId()) {
            if (to.HasPathId()) {
                toPathId = TPathId(to.GetOwnerId(), to.GetPathId());
                if (toInclusive || to.HasPartIdx()) {
                    itEnd = tables.upper_bound(toPathId);
                    if (to.HasPartIdx()) {
                        endPartIdx = to.GetPartIdx();
                        if (toInclusive) {
                            ++endPartIdx;
                        }
                    }
                } else {
                    itEnd = tables.lower_bound(toPathId);
                }
            } else {
                if (toInclusive) {
                    toPathId = TPathId(to.GetOwnerId(), std::numeric_limits<ui64>::max());
                    itEnd = tables.upper_bound(toPathId);
                } else {
                    toPathId = TPathId(to.GetOwnerId(), 0);
                    itEnd = tables.lower_bound(toPathId);
                }
            }
        }

        bool includePathColumn = !record.HasIncludePathColumn() || record.GetIncludePathColumn();

        for (size_t count = 0; count < BatchSize && it != itEnd && it != tables.end(); ++it) {
            auto& pathId = it->first;
            const auto& tableStats = it->second;

            ui64 end = tableStats.ShardIndices.size();
            if (to.HasPartIdx() && pathId == toPathId) {
                end = std::min(endPartIdx, end);
            }

            bool batchFinished = false;

            for (ui64 partIdx = startPartIdx; partIdx < end; ++partIdx) {
                auto* stats = result->Record.AddStats();
                auto* key = stats->MutableKey();

                key->SetOwnerId(pathId.OwnerId);
                key->SetPathId(pathId.LocalPathId);
                key->SetPartIdx(partIdx);

                if (includePathColumn) {
                    stats->SetPath(tableStats.Path);
                }

                auto shardIdx = tableStats.ShardIndices[partIdx];
                auto part = tableStats.Partitions.find(shardIdx);
                if (part != tableStats.Partitions.end()) {
                    *stats->MutableStats() = part->second;
                }

                if (++count == BatchSize) {
                    auto* next = result->Record.MutableNext();
                    next->SetOwnerId(pathId.OwnerId);
                    next->SetPathId(pathId.LocalPathId);
                    next->SetPartIdx(partIdx + 1);
                    result->Record.SetLastBatch(false);
                    batchFinished = true;
                    break;
                }
            }

            if (batchFinished) {
                break;
            }

            startPartIdx = 0;
        }

        Send(request->Sender, std::move(result));
    }

    void Handle(TEvPrivate::TEvProcessOverloaded::TPtr&) {
        Schedule(ProcessOverloadedInterval, new TEvPrivate::TEvProcessOverloaded);

        if (!SysViewProcessorId) {
            return;
        }

        auto domainFound = DomainTables.find(DomainKey);
        if (domainFound == DomainTables.end()) {
            SVLOG_D("NSysView::TPartitionStatsCollector: TEvProcessOverloaded: no tables");
            return;
        }
        auto& domainTables = domainFound->second;

        struct TPartition {
            TPathId PathId;
            TShardIdx ShardIdx;
            double CPUCores;
        };
        std::vector<TPartition> sorted;

        for (const auto& [pathId, shardIndices] : domainTables.Overloaded) {
            for (const auto& shardIdx : shardIndices) {
                auto& table = domainTables.Stats[pathId];
                auto& partition = table.Partitions[shardIdx];
                sorted.emplace_back(TPartition{pathId, shardIdx, partition.GetCPUCores()});
            }
        }

        std::sort(sorted.begin(), sorted.end(),
            [] (const auto& l, const auto& r) { return l.CPUCores > r.CPUCores; });

        auto now = TActivationContext::Now();
        auto nowUs = now.MicroSeconds();

        size_t count = 0;
        auto sendEvent = MakeHolder<TEvSysView::TEvSendTopPartitions>();
        for (const auto& entry : sorted) {
            auto& table = domainTables.Stats[entry.PathId];
            auto& partition = table.Partitions[entry.ShardIdx];

            auto* result = sendEvent->Record.AddPartitions();
            result->SetTabletId(partition.GetTabletId());
            result->SetPath(table.Path);
            result->SetPeakTimeUs(nowUs);
            result->SetCPUCores(partition.GetCPUCores());
            result->SetNodeId(partition.GetNodeId());
            result->SetDataSize(partition.GetDataSize());
            result->SetRowCount(partition.GetRowCount());
            result->SetIndexSize(partition.GetIndexSize());
            result->SetInFlightTxCount(partition.GetInFlightTxCount());

            if (++count == TOP_PARTITIONS_COUNT) {
                break;
            }
        }

        sendEvent->Record.SetTimeUs(nowUs);

        SVLOG_D("NSysView::TPartitionStatsCollector: TEvProcessOverloaded "
            << "top size# " << sorted.size()
            << ", time# " << now);

        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(sendEvent.Release(), SysViewProcessorId, true));
    }

    void Handle(TEvSysView::TEvInitPartitionStatsCollector::TPtr& ev) {
        DomainKey = ev->Get()->DomainKey;
        SysViewProcessorId = ev->Get()->SysViewProcessorId;

        SVLOG_I("NSysView::TPartitionStatsCollector initialized: "
            << "domain key# " << DomainKey
            << ", sysview processor id# " << SysViewProcessorId);
    }

    void PassAway() override {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

    bool IsPartitionOverloaded(NKikimrSysView::TPartitionStats& stats) {
        return stats.GetCPUCores() >= OverloadedPartitionBound;
    }

private:
    const size_t BatchSize;
    const size_t PendingRequestsLimit;

    TPathId DomainKey;
    ui64 SysViewProcessorId = 0;

    double OverloadedPartitionBound = 0.7;
    TDuration ProcessOverloadedInterval = TDuration::Seconds(15);

    struct TTableStats {
        std::unordered_map<TShardIdx, NKikimrSysView::TPartitionStats> Partitions; // shardIdx -> stats
        std::vector<TShardIdx> ShardIndices;
        TString Path;
    };

    struct TDomainTables {
        std::map<TPathId, TTableStats> Stats;
        std::unordered_map<TPathId, std::unordered_set<TShardIdx>> Overloaded;
    };
    std::unordered_map<TPathId, TDomainTables> DomainTables;

    TQueue<TEvSysView::TEvGetPartitionStats::TPtr> PendingRequests;
    bool ProcessInFly = false;
};

THolder<NActors::IActor> CreatePartitionStatsCollector(size_t batchSize, size_t pendingRequestsLimit)
{
    return MakeHolder<TPartitionStatsCollector>(batchSize, pendingRequestsLimit);
}


class TPartitionStatsScan : public TScanActorBase<TPartitionStatsScan> {
public:
    using TBase = TScanActorBase<TPartitionStatsScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TPartitionStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
        auto extractKey = [] (NKikimrSysView::TPartitionStatsKey& key, const TConstArrayRef<TCell>& cells) {
            if (cells.size() > 0 && !cells[0].IsNull()) {
                key.SetOwnerId(cells[0].AsValue<ui64>());
            }
            if (cells.size() > 1 && !cells[1].IsNull()) {
                key.SetPathId(cells[1].AsValue<ui64>());
            }
            if (cells.size() > 2 && !cells[2].IsNull()) {
                key.SetPartIdx(cells[2].AsValue<ui64>());
            }
        };

        extractKey(From, TableRange.From.GetCells());
        FromInclusive = TableRange.FromInclusive;

        extractKey(To, TableRange.To.GetCells());
        ToInclusive = TableRange.ToInclusive;

        for (auto& column : columns) {
            if (column.Tag == Schema::PartitionStats::Path::ColumnId) {
                IncludePathColumn = true;
                break;
            }
        }
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvSysView::TEvGetPartitionStatsResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TPartitionStatsScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TThis::StateScan);
        if (AckReceived) {
            RequestBatch();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        RequestBatch();
    }

    void RequestBatch() {
        if (BatchRequestInFlight) {
            return;
        }

        auto request = MakeHolder<TEvSysView::TEvGetPartitionStats>();

        request->Record.SetDomainKeyOwnerId(DomainKey.OwnerId);
        request->Record.SetDomainKeyPathId(DomainKey.LocalPathId);

        request->Record.MutableFrom()->CopyFrom(From);
        request->Record.SetFromInclusive(FromInclusive);
        request->Record.MutableTo()->CopyFrom(To);
        request->Record.SetToInclusive(ToInclusive);

        request->Record.SetIncludePathColumn(IncludePathColumn);

        auto pipeCache = MakePipePerNodeCacheID(false);
        Send(pipeCache, new TEvPipeCache::TEvForward(request.Release(), SchemeShardId, true),
            IEventHandle::FlagTrackDelivery);

        BatchRequestInFlight = true;
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Delivery problem in partition stats scan");
    }

    void Handle(TEvSysView::TEvGetPartitionStatsResult::TPtr& ev) {
        auto& record = ev->Get()->Record;

        if (record.HasOverloaded() && record.GetOverloaded()) {
            ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Partition stats collector is overloaded");
            return;
        }

        using TPartitionStats = NKikimrSysView::TPartitionStatsResult;
        using TExtractor = std::function<TCell(const TPartitionStats&)>;
        using TSchema = Schema::PartitionStats;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::OwnerId::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetKey().GetOwnerId());
                }});
                insert({TSchema::PathId::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetKey().GetPathId());
                }});
                insert({TSchema::PartIdx::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetKey().GetPartIdx());
                }});
                insert({TSchema::DataSize::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetDataSize());
                }});
                insert({TSchema::RowCount::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetRowCount());
                }});
                insert({TSchema::IndexSize::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetIndexSize());
                }});
                insert({TSchema::CPUCores::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<double>(s.GetStats().GetCPUCores());
                }});
                insert({TSchema::TabletId::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetTabletId());
                }});
                insert({TSchema::Path::ColumnId, [] (const TPartitionStats& s) {
                    if (!s.HasPath()) {
                        return TCell();
                    }
                    auto& path = s.GetPath();
                    return TCell(path.data(), path.size());
                }});
                insert({TSchema::NodeId::ColumnId, [] (const TPartitionStats& s) {
                    return s.GetStats().HasNodeId() ? TCell::Make<ui32>(s.GetStats().GetNodeId()) : TCell();
                }});
                insert({TSchema::StartTime::ColumnId, [] (const TPartitionStats& s) {
                    return s.GetStats().HasStartTime() ? TCell::Make<ui64>(s.GetStats().GetStartTime() * 1000) : TCell();
                }});
                insert({TSchema::AccessTime::ColumnId, [] (const TPartitionStats& s) {
                    return s.GetStats().HasAccessTime() ? TCell::Make<ui64>(s.GetStats().GetAccessTime() * 1000) : TCell();
                }});
                insert({TSchema::UpdateTime::ColumnId, [] (const TPartitionStats& s) {
                    return s.GetStats().HasUpdateTime() ? TCell::Make<ui64>(s.GetStats().GetUpdateTime() * 1000) : TCell();
                }});
                insert({TSchema::InFlightTxCount::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui32>(s.GetStats().GetInFlightTxCount());
                }});
                insert({TSchema::RowUpdates::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetRowUpdates());
                }});
                insert({TSchema::RowDeletes::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetRowDeletes());
                }});
                insert({TSchema::RowReads::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetRowReads());
                }});
                insert({TSchema::RangeReads::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetRangeReads());
                }});
                insert({TSchema::RangeReadRows::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetRangeReadRows());
                }});
                insert({TSchema::ImmediateTxCompleted::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetImmediateTxCompleted());
                }});
                insert({TSchema::CoordinatedTxCompleted::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetPlannedTxCompleted());
                }});
                insert({TSchema::TxRejectedByOverload::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetTxRejectedByOverload());
                }});
                insert({TSchema::TxRejectedByOutOfStorage::ColumnId, [] (const TPartitionStats& s) {
                    return TCell::Make<ui64>(s.GetStats().GetTxRejectedBySpace());
                }});
                insert({TSchema::LastTtlRunTime::ColumnId, [] (const TPartitionStats& s) {
                    return s.GetStats().HasTtlStats() ? TCell::Make<ui64>(s.GetStats().GetTtlStats().GetLastRunTime() * 1000) : TCell();
                }});
                insert({TSchema::LastTtlRowsProcessed::ColumnId, [] (const TPartitionStats& s) {
                    return s.GetStats().HasTtlStats() ? TCell::Make<ui64>(s.GetStats().GetTtlStats().GetLastRowsProcessed()) : TCell();
                }});
                insert({TSchema::LastTtlRowsErased::ColumnId, [] (const TPartitionStats& s) {
                    return s.GetStats().HasTtlStats() ? TCell::Make<ui64>(s.GetStats().GetTtlStats().GetLastRowsErased()) : TCell();
                }});
            }
        };
        static TExtractorsMap extractors;

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        TVector<TCell> cells;
        for (const auto& s : record.GetStats()) {
            for (auto& column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(s));
                }
            }
            TArrayRef<const TCell> ref(cells);
            batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        batch->Finished = record.GetLastBatch();
        if (!batch->Finished) {
            Y_ABORT_UNLESS(record.HasNext());
            From = record.GetNext();
            FromInclusive = true;
        }

        SendBatch(std::move(batch));

        BatchRequestInFlight = false;
    }

    void PassAway() override {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    NKikimrSysView::TPartitionStatsKey From;
    bool FromInclusive = false;

    NKikimrSysView::TPartitionStatsKey To;
    bool ToInclusive = false;

    bool IncludePathColumn = false;
};

THolder<NActors::IActor> CreatePartitionStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TPartitionStatsScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NSysView
} // NKikimr
