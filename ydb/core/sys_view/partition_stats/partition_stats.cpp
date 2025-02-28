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

        SVLOG_T("TEvSysView::TEvSetPartitioning: domainKey " << domainKey
            << " pathId " << pathId
            << " path " << ev->Get()->Path
            << " ShardIndices size " << ev->Get()->ShardIndices.size());

        auto& tables = DomainTables[domainKey];
        auto tableFound = tables.Stats.find(pathId);
        if (tableFound != tables.Stats.end()) {
            auto& table = tableFound->second;

            auto& oldPartitions = table.Partitions;
            std::unordered_map<TShardIdx, TPartitionStats> newPartitions;
            std::set<TOverloadedFollower> overloaded;

            for (auto shardIdx : ev->Get()->ShardIndices) {
                auto old = oldPartitions.find(shardIdx);
                if (old != oldPartitions.end()) {
                    newPartitions[shardIdx] = old->second;

                    for (const auto& followerStat: old->second.FollowerStats) {
                        if (IsPartitionOverloaded(followerStat.second))
                            overloaded.insert({shardIdx, followerStat.first});
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

        auto& newStats = ev->Get()->Stats;
        const ui32 followerId = newStats.GetFollowerId();

        SVLOG_T("TEvSysView::TEvSendPartitionStats: domainKey " << domainKey
            << " pathId " << pathId
            << " shardIdx " << shardIdx.first << " " << shardIdx.second
            << " followerId " << followerId
            << " stats " << newStats.ShortDebugString());

        auto& tables = DomainTables[domainKey];
        auto tableFound = tables.Stats.find(pathId);
        if (tableFound == tables.Stats.end()) {
            return;
        }

        auto& table = tableFound->second;
        auto& partitionStats = table.Partitions[shardIdx];

        auto& followerStats = partitionStats.FollowerStats[followerId];

        TOverloadedFollower overloadedFollower = {shardIdx, followerId};
        if (IsPartitionOverloaded(newStats)) {
            tables.Overloaded[pathId].insert(overloadedFollower);
        } else {
            auto overloadedFound = tables.Overloaded.find(pathId);
            if (overloadedFound != tables.Overloaded.end()) {
                overloadedFound->second.erase(overloadedFollower);
                if (overloadedFound->second.empty()) {
                    tables.Overloaded.erase(pathId);
                }
            }
        }

        if (followerStats.HasTtlStats()) {
            newStats.MutableTtlStats()->Swap(followerStats.MutableTtlStats());
        }

        followerStats.Swap(&newStats);
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

        auto& followerStats = tableFound->second.Partitions[shardIdx].FollowerStats;
        auto leaderFound = followerStats.find(0);
        if (leaderFound == followerStats.end()) {
            return;
        }

        leaderFound->second.MutableTtlStats()->Swap(&ev->Get()->Stats);
    }

    void Handle(TEvSysView::TEvGetPartitionStats::TPtr& ev) {
        if (PendingRequests.size() >= PendingRequestsLimit) {
            auto result = MakeHolder<TEvSysView::TEvGetPartitionStatsResult>();
            result->Record.SetOverloaded(true);
            Send(ev->Sender, std::move(result), 0, ev->Cookie);
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
            Send(request->Sender, std::move(result), 0, request->Cookie);
            return;
        }

        auto domainKey = TPathId(record.GetDomainKeyOwnerId(), record.GetDomainKeyPathId());
        auto itTables = DomainTables.find(domainKey);
        if (itTables == DomainTables.end()) {
            Send(request->Sender, std::move(result), 0, request->Cookie);
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

        auto matchesFilter = [&](const NKikimrSysView::TPartitionStats& stats) {
                if (record.HasFilter()) {
                    const auto& filter = record.GetFilter();
                    if (filter.HasNotLess()) {
                        if (filter.GetNotLess().HasCPUCores() && stats.GetCPUCores() < filter.GetNotLess().GetCPUCores()) {
                            return false;
                        }
                    }
                }
                return true;
            };

        for (size_t count = 0; count < BatchSize && it != itEnd && it != tables.end(); ++it) {
            auto& pathId = it->first;
            const auto& tableStats = it->second;

            ui64 end = tableStats.ShardIndices.size();
            if (to.HasPartIdx() && pathId == toPathId) {
                end = std::min(endPartIdx, end);
            }

            bool batchFinished = false;

            for (ui64 partIdx = startPartIdx; partIdx < end; ++partIdx) {
                NKikimrSysView::TPartitionStatsResult* stats = nullptr;
                auto shardIdx = tableStats.ShardIndices[partIdx];
                auto part = tableStats.Partitions.find(shardIdx);
                if (part != tableStats.Partitions.end()) {
                    for (const auto& followerStat : part->second.FollowerStats) {
                        if (!matchesFilter(followerStat.second)) {
                            continue;
                        }
                        if (stats == nullptr) {
                            stats = result->Record.AddStats();
                        }
                        *stats->AddStats() = followerStat.second;
                    }
                }

                if (!stats) {
                    continue;
                }

                auto* key = stats->MutableKey();

                key->SetOwnerId(pathId.OwnerId);
                key->SetPathId(pathId.LocalPathId);
                key->SetPartIdx(partIdx);

                if (includePathColumn) {
                    stats->SetPath(tableStats.Path);
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

        Send(request->Sender, std::move(result), 0, request->Cookie);
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
            ui32 FollowerId;
            double CPUCores;
        };
        std::vector<TPartition> sorted;

        for (const auto& [pathId, overloadedFollowers] : domainTables.Overloaded) {
            for (const TOverloadedFollower& overloadedFollower : overloadedFollowers) {
                const auto& table = domainTables.Stats[pathId];
                const auto& partition = table.Partitions.at(overloadedFollower.ShardIdx).FollowerStats.at(overloadedFollower.FollowerId);
                sorted.emplace_back(TPartition{pathId, overloadedFollower.ShardIdx, overloadedFollower.FollowerId, partition.GetCPUCores()});
            }
        }

        std::sort(sorted.begin(), sorted.end(),
            [] (const auto& l, const auto& r) { return l.CPUCores > r.CPUCores; });

        auto now = TActivationContext::Now();
        auto nowUs = now.MicroSeconds();

        size_t count = 0;
        auto sendEvent = MakeHolder<TEvSysView::TEvSendTopPartitions>();
        for (const auto& entry : sorted) {
            const auto& table = domainTables.Stats[entry.PathId];
            const auto& followerStats = table.Partitions.at(entry.ShardIdx).FollowerStats;
            const auto& partition = followerStats.at(entry.FollowerId);
            const auto& leaderPartition = followerStats.at(0);

            auto* result = sendEvent->Record.AddPartitions();
            result->SetTabletId(partition.GetTabletId());
            result->SetPath(table.Path);
            result->SetPeakTimeUs(nowUs);
            result->SetCPUCores(partition.GetCPUCores());
            result->SetNodeId(partition.GetNodeId());
            result->SetDataSize(leaderPartition.GetDataSize());
            result->SetRowCount(leaderPartition.GetRowCount());
            result->SetIndexSize(leaderPartition.GetIndexSize());
            result->SetInFlightTxCount(partition.GetInFlightTxCount());
            result->SetFollowerId(partition.GetFollowerId());

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

    bool IsPartitionOverloaded(const NKikimrSysView::TPartitionStats& stats) const {
        return stats.GetCPUCores() >= OverloadedPartitionBound;
    }

private:
    const size_t BatchSize;
    const size_t PendingRequestsLimit;

    TPathId DomainKey;
    ui64 SysViewProcessorId = 0;

    double OverloadedPartitionBound = 0.7;
    TDuration ProcessOverloadedInterval = TDuration::Seconds(15);

    typedef ui32 TFollowerId;

    struct TPartitionStats {
        std::unordered_map<TFollowerId, NKikimrSysView::TPartitionStats> FollowerStats;
    };

    struct TTableStats {
        std::unordered_map<TShardIdx, TPartitionStats> Partitions; // shardIdx -> stats
        std::vector<TShardIdx> ShardIndices;
        TString Path;
    };

    struct TOverloadedFollower {
        TShardIdx ShardIdx;
        TFollowerId FollowerId;

        bool operator<(const TOverloadedFollower &other) const {
            return std::tie(ShardIdx, FollowerId) < std::tie(other.ShardIdx, other.FollowerId);
        }

        bool operator==(const TOverloadedFollower &other) const {
            return std::tie(ShardIdx, FollowerId) == std::tie(other.ShardIdx, other.FollowerId);
        }
    };

    struct TDomainTables {
        std::map<TPathId, TTableStats> Stats;
        std::unordered_map<TPathId, std::set<TOverloadedFollower>> Overloaded;
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

        using TPartitionStatsResult = NKikimrSysView::TPartitionStatsResult;
        using TPartitionStats = NKikimrSysView::TPartitionStats;
        using TExtractor = std::function<TCell(const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats&)>;
        using TSchema = Schema::PartitionStats;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::OwnerId::ColumnId, [] (const TPartitionStatsResult& shard, const TPartitionStats&, const TPartitionStats&) {
                    return TCell::Make<ui64>(shard.GetKey().GetOwnerId());
                }});
                insert({TSchema::PathId::ColumnId, [] (const TPartitionStatsResult& shard, const TPartitionStats&, const TPartitionStats&) {
                    return TCell::Make<ui64>(shard.GetKey().GetPathId());
                }});
                insert({TSchema::PartIdx::ColumnId, [] (const TPartitionStatsResult& shard, const TPartitionStats&, const TPartitionStats&) {
                    return TCell::Make<ui64>(shard.GetKey().GetPartIdx());
                }});
                insert({TSchema::DataSize::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats& leaderStats, const TPartitionStats&) {
                    return TCell::Make<ui64>(leaderStats.GetDataSize());
                }});
                insert({TSchema::RowCount::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats& leaderStats, const TPartitionStats&) {
                    return TCell::Make<ui64>(leaderStats.GetRowCount());
                }});
                insert({TSchema::IndexSize::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats& leaderStats, const TPartitionStats&) {
                    return TCell::Make<ui64>(leaderStats.GetIndexSize());
                }});
                insert({TSchema::CPUCores::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<double>(stats.GetCPUCores());
                }});
                insert({TSchema::TabletId::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetTabletId());
                }});
                insert({TSchema::Path::ColumnId, [] (const TPartitionStatsResult& shard, const TPartitionStats&, const TPartitionStats&) {
                    if (!shard.HasPath()) {
                        return TCell();
                    }
                    auto& path = shard.GetPath();
                    return TCell(path.data(), path.size());
                }});
                insert({TSchema::NodeId::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return stats.HasNodeId() ? TCell::Make<ui32>(stats.GetNodeId()) : TCell();
                }});
                insert({TSchema::StartTime::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return stats.HasStartTime() ? TCell::Make<ui64>(stats.GetStartTime() * 1000) : TCell();
                }});
                insert({TSchema::AccessTime::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return stats.HasAccessTime() ? TCell::Make<ui64>(stats.GetAccessTime() * 1000) : TCell();
                }});
                insert({TSchema::UpdateTime::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return stats.HasUpdateTime() ? TCell::Make<ui64>(stats.GetUpdateTime() * 1000) : TCell();
                }});
                insert({TSchema::InFlightTxCount::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui32>(stats.GetInFlightTxCount());
                }});
                insert({TSchema::RowUpdates::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetRowUpdates());
                }});
                insert({TSchema::RowDeletes::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetRowDeletes());
                }});
                insert({TSchema::RowReads::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetRowReads());
                }});
                insert({TSchema::RangeReads::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetRangeReads());
                }});
                insert({TSchema::RangeReadRows::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetRangeReadRows());
                }});
                insert({TSchema::ImmediateTxCompleted::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetImmediateTxCompleted());
                }});
                insert({TSchema::CoordinatedTxCompleted::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetPlannedTxCompleted());
                }});
                insert({TSchema::TxRejectedByOverload::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetTxRejectedByOverload());
                }});
                insert({TSchema::TxRejectedByOutOfStorage::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetTxRejectedBySpace());
                }});
                insert({TSchema::LastTtlRunTime::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return stats.HasTtlStats() ? TCell::Make<ui64>(stats.GetTtlStats().GetLastRunTime() * 1000) : TCell();
                }});
                insert({TSchema::LastTtlRowsProcessed::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return stats.HasTtlStats() ? TCell::Make<ui64>(stats.GetTtlStats().GetLastRowsProcessed()) : TCell();
                }});
                insert({TSchema::LastTtlRowsErased::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return stats.HasTtlStats() ? TCell::Make<ui64>(stats.GetTtlStats().GetLastRowsErased()) : TCell();
                }});
                insert({TSchema::FollowerId::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui32>(stats.GetFollowerId());
                }});
            }
        };
        static TExtractorsMap extractors;

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        TVector<TCell> cells;

        auto addCellsToBatch = [&] (const TPartitionStatsResult& shardStats, const TPartitionStats& leaderStats, const TPartitionStats& stats) {
            for (auto& column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(shardStats, leaderStats, stats));
                }
            }
            TArrayRef<const TCell> ref(cells);
            batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        };

        for (const TPartitionStatsResult& shardStats : record.GetStats()) {
            // Get partition stats
            const auto& partitionStats = shardStats.GetStats();

            // Find leader stats
            auto leaderStatsIter = std::find_if(partitionStats.begin(), partitionStats.end(), [](const TPartitionStats& stats) {
                return stats.GetFollowerId() == 0;
            });

            const TPartitionStats& leaderStats = leaderStatsIter != partitionStats.end()
                ? *leaderStatsIter
                : TPartitionStats{};   // Only at the very beginning, when there is no statistics from the leader

            if (!partitionStats.empty()) {
                // Enumerate all stats
                for (const TPartitionStats& stats : partitionStats) {
                    addCellsToBatch(shardStats, leaderStats, stats);
                }
            } else {
                // Only at the very beginning, when there is no statistics from shards
                addCellsToBatch(shardStats, {}, {});
            }
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
