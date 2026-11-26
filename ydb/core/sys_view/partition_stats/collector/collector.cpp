#include "collector.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/util/tuples.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/queue.h>

#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

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
        YDB_LOG_DEBUG("NSysView::TPartitionStatsCollector::Bootstrap",
            {"domainKey", DomainKey});

        if (AppData()->UsePartitionStatsCollectorForTests) {
            OverloadedByCpuPartitionBound = 0.0;
            OverloadedByTliPartitionBound = 0;
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
                YDB_LOG_CRIT("NSysView::TPartitionStatsCollector: unexpected event",
                    {"eventType", ev->GetTypeRewrite()});
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

        YDB_LOG_TRACE("Handle TEvSysView::TEvSetPartitioning: received table partitioning",
            {"domainKey", domainKey},
            {"pathId", pathId},
            {"path", ev->Get()->Path},
            {"shardIndexCount", ev->Get()->ShardIndices.size()});

        auto& tables = DomainTables[domainKey];
        auto tableFound = tables.Stats.find(pathId);
        if (tableFound != tables.Stats.end()) {
            auto& table = tableFound->second;

            auto& oldPartitions = table.Partitions;
            std::unordered_map<TShardIdx, TPartitionStats> newPartitions;
            std::set<TFollowerStats> overloadedByCpu, overloadedByTli;

            for (auto shardIdx : ev->Get()->ShardIndices) {
                auto old = oldPartitions.find(shardIdx);
                if (old != oldPartitions.end()) {
                    newPartitions[shardIdx] = old->second;

                    for (const auto& followerStat: old->second.FollowerStats) {
                        if (IsPartitionOverloadedByCpu(followerStat.second))
                            overloadedByCpu.insert({shardIdx, followerStat.first});
                        if (IsPartitionOverloadedByTli(followerStat.second))
                            overloadedByTli.insert({shardIdx, followerStat.first});
                    }
                }
            }

            if (!overloadedByCpu.empty()) {
                tables.OverloadedByCpu[pathId].swap(overloadedByCpu);
            } else {
                tables.OverloadedByCpu.erase(pathId);
            }
            if (!overloadedByTli.empty()) {
                tables.OverloadedByTli[pathId].swap(overloadedByTli);
            } else {
                tables.OverloadedByTli.erase(pathId);
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
        tables.OverloadedByCpu.erase(pathId);
        tables.OverloadedByTli.erase(pathId);
    }

    void Handle(TEvSysView::TEvSendPartitionStats::TPtr& ev) {
        const auto& domainKey = ev->Get()->DomainKey;
        const auto& pathId = ev->Get()->PathId;
        const auto& shardIdx = ev->Get()->ShardIdx;

        auto& newStats = ev->Get()->Stats;
        const ui32 followerId = newStats.GetFollowerId();

        YDB_LOG_TRACE("Handle TEvSysView::TEvSendPartitionStats: received partition stats",
            {"domainKey", domainKey},
            {"pathId", pathId},
            {"shardIndexFirst", shardIdx.first},
            {"shardIndexSecond", shardIdx.second},
            {"followerId", followerId},
            {"stats", newStats.ShortDebugString()});

        auto& tables = DomainTables[domainKey];
        auto tableFound = tables.Stats.find(pathId);
        if (tableFound == tables.Stats.end()) {
            return;
        }

        auto& table = tableFound->second;
        auto& partitionStats = table.Partitions[shardIdx];

        auto& followerStats = partitionStats.FollowerStats[followerId];

        TFollowerStats overloadedFollower = {shardIdx, followerId};
        if (IsPartitionOverloadedByCpu(newStats)) {
            tables.OverloadedByCpu[pathId].insert(overloadedFollower);
        } else {
            auto overloadedFound = tables.OverloadedByCpu.find(pathId);
            if (overloadedFound != tables.OverloadedByCpu.end()) {
                overloadedFound->second.erase(overloadedFollower);
                if (overloadedFound->second.empty()) {
                    tables.OverloadedByCpu.erase(pathId);
                }
            }
        }
        if (IsPartitionOverloadedByTli(newStats)) {
            tables.OverloadedByTli[pathId].insert(overloadedFollower);
        } else {
            auto overloadedFound = tables.OverloadedByTli.find(pathId);
            if (overloadedFound != tables.OverloadedByTli.end()) {
                overloadedFound->second.erase(overloadedFollower);
                if (overloadedFound->second.empty()) {
                    tables.OverloadedByTli.erase(pathId);
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
        bool hasFilter = record.HasFilter();
        auto matchesFilter = [&](const NKikimrSysView::TPartitionStats& stats) {
            const auto& filter = record.GetFilter();
            if (filter.HasNotLess()) {
                if (filter.GetNotLess().HasCPUCores() && stats.GetCPUCores() < filter.GetNotLess().GetCPUCores()) {
                    return false;
                }
            }
            return true;
        };
        auto addStats = [&](const auto& pathId, const auto& tableStats, ui64 partIdx) {
            auto stats = result->Record.AddStats();
            auto* key = stats->MutableKey();
            key->SetOwnerId(pathId.OwnerId);
            key->SetPathId(pathId.LocalPathId);
            key->SetPartIdx(partIdx);
            if (includePathColumn) {
                stats->SetPath(tableStats.Path);
            }
            return stats;
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
                if (!hasFilter) {
                    stats = addStats(pathId, tableStats, partIdx);
                }
                auto shardIdx = tableStats.ShardIndices[partIdx];
                auto part = tableStats.Partitions.find(shardIdx);
                if (part != tableStats.Partitions.end()) {
                    for (const auto& followerStat : part->second.FollowerStats) {
                        if (hasFilter && !matchesFilter(followerStat.second)) {
                            continue;
                        }
                        if (!stats) {
                            stats = addStats(pathId, tableStats, partIdx);
                        }
                        *stats->AddStats() = followerStat.second;
                    }
                }

                if (stats && ++count == BatchSize) {
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
            YDB_LOG_DEBUG("NSysView::TPartitionStatsCollector::HandleProcessOverloaded: no tables in domain",
                {"domainKey", DomainKey});
            return;
        }
        auto& domainTables = domainFound->second;

        struct TPartitionByCpu {
            TPathId PathId;
            TShardIdx ShardIdx;
            ui32 FollowerId;
            double CPUCores;
        };
        std::vector<TPartitionByCpu> sortedByCpu;

        struct TPartitionByTli {
            TPathId PathId;
            TShardIdx ShardIdx;
            ui32 FollowerId;
            ui64 LocksBroken;
        };
        std::vector<TPartitionByTli> sortedByTli;

        for (const auto& [pathId, overloadedFollowers] : domainTables.OverloadedByCpu) {
            for (const TFollowerStats& overloadedFollower : overloadedFollowers) {
                const auto& table = domainTables.Stats[pathId];
                const auto& partition = table.Partitions.at(overloadedFollower.ShardIdx).FollowerStats.at(overloadedFollower.FollowerId);
                sortedByCpu.emplace_back(TPartitionByCpu{pathId, overloadedFollower.ShardIdx, overloadedFollower.FollowerId, partition.GetCPUCores()});
            }
        }
        for (const auto& [pathId, overloadedFollowers] : domainTables.OverloadedByTli) {
            for (const TFollowerStats& overloadedFollower : overloadedFollowers) {
                const auto& table = domainTables.Stats[pathId];
                const auto& partition = table.Partitions.at(overloadedFollower.ShardIdx).FollowerStats.at(overloadedFollower.FollowerId);
                sortedByTli.emplace_back(TPartitionByTli{pathId, overloadedFollower.ShardIdx, overloadedFollower.FollowerId, partition.GetLocksBroken()});
            }
        }

        std::sort(sortedByCpu.begin(), sortedByCpu.end(),
            [] (const auto& l, const auto& r) { return l.CPUCores > r.CPUCores; });
        std::sort(sortedByTli.begin(), sortedByTli.end(),
            [] (const auto& l, const auto& r) { return l.LocksBroken > r.LocksBroken; });

        auto now = TActivationContext::Now();
        auto nowUs = now.MicroSeconds();

        auto getLeaderStats = [](const auto& followerStats) -> const NKikimrSysView::TPartitionStats& {
            // Follower stats may arrive before the first stats from the leader
            auto leaderFound = followerStats.find(0);
            return leaderFound != followerStats.end()
                ? leaderFound->second
                : NKikimrSysView::TPartitionStats::default_instance();
        };

        size_t count = 0;
        auto sendEvent = MakeHolder<TEvSysView::TEvSendTopPartitions>();
        for (const auto& entry : sortedByCpu) {
            const auto& table = domainTables.Stats[entry.PathId];
            const auto& followerStats = table.Partitions.at(entry.ShardIdx).FollowerStats;
            const auto& partition = followerStats.at(entry.FollowerId);
            const auto& leaderPartition = getLeaderStats(followerStats);

            auto* result = sendEvent->Record.AddPartitionsByCpu();
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
        for (const auto& entry : sortedByTli) {
            const auto& table = domainTables.Stats[entry.PathId];
            const auto& followerStats = table.Partitions.at(entry.ShardIdx).FollowerStats;
            const auto& partition = followerStats.at(entry.FollowerId);
            const auto& leaderPartition = getLeaderStats(followerStats);

            auto* result = sendEvent->Record.AddPartitionsByTli();
            result->SetTabletId(partition.GetTabletId());
            result->SetPath(table.Path);
            result->SetLocksAcquired(partition.GetLocksAcquired());
            result->SetLocksWholeShard(partition.GetLocksWholeShard());
            result->SetLocksBroken(partition.GetLocksBroken());
            result->SetNodeId(partition.GetNodeId());
            result->SetDataSize(leaderPartition.GetDataSize());
            result->SetRowCount(leaderPartition.GetRowCount());
            result->SetIndexSize(leaderPartition.GetIndexSize());
            result->SetFollowerId(partition.GetFollowerId());

            if (++count == TOP_PARTITIONS_COUNT) {
                break;
            }
        }

        sendEvent->Record.SetTimeUs(nowUs);

        YDB_LOG_DEBUG("NSysView::TPartitionStatsCollector::HandleProcessOverloaded: sending top overloaded partitions",
            {"topByCpuCount", sortedByCpu.size()},
            {"topByTliCount", sortedByTli.size()},
            {"timestamp", now});

        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(sendEvent.Release(), SysViewProcessorId, true));
    }

    void Handle(TEvSysView::TEvInitPartitionStatsCollector::TPtr& ev) {
        DomainKey = ev->Get()->DomainKey;
        SysViewProcessorId = ev->Get()->SysViewProcessorId;

        YDB_LOG_INFO("NSysView::TPartitionStatsCollector::HandleConfigure: initialized for domain",
            {"domainKey", DomainKey},
            {"sysViewProcessorId", SysViewProcessorId});
    }

    void PassAway() override {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

    bool IsPartitionOverloadedByCpu(const NKikimrSysView::TPartitionStats& stats) const {
        return stats.GetCPUCores() >= OverloadedByCpuPartitionBound;
    }
    bool IsPartitionOverloadedByTli(const NKikimrSysView::TPartitionStats& stats) const {
        return stats.GetLocksBroken() >= OverloadedByTliPartitionBound;
    }

private:
    const size_t BatchSize;
    const size_t PendingRequestsLimit;

    TPathId DomainKey;
    ui64 SysViewProcessorId = 0;

    double OverloadedByCpuPartitionBound = 0.7;
    ui64 OverloadedByTliPartitionBound = 1;
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

    struct TFollowerStats {
        TShardIdx ShardIdx;
        TFollowerId FollowerId;

        bool operator<(const TFollowerStats &other) const {
            return std::tie(ShardIdx, FollowerId) < std::tie(other.ShardIdx, other.FollowerId);
        }

        bool operator==(const TFollowerStats &other) const {
            return std::tie(ShardIdx, FollowerId) == std::tie(other.ShardIdx, other.FollowerId);
        }
    };

    struct TDomainTables {
        std::map<TPathId, TTableStats> Stats;
        std::unordered_map<TPathId, std::set<TFollowerStats>> OverloadedByCpu;
        std::unordered_map<TPathId, std::set<TFollowerStats>> OverloadedByTli;
    };
    std::unordered_map<TPathId, TDomainTables> DomainTables;

    TQueue<TEvSysView::TEvGetPartitionStats::TPtr> PendingRequests;
    bool ProcessInFly = false;
};

THolder<NActors::IActor> CreatePartitionStatsCollector(size_t batchSize, size_t pendingRequestsLimit)
{
    return MakeHolder<TPartitionStatsCollector>(batchSize, pendingRequestsLimit);
}

} // NSysView
} // NKikimr
