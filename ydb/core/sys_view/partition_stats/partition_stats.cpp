#include "partition_stats.h"

#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/sys_view/scan/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/hfunc.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
namespace NSysView {

using namespace NActors;

class TPartitionStatsScan : public TScanActorBase<TPartitionStatsScan> {
public:
    using TBase = TScanActorBase<TPartitionStatsScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TPartitionStatsScan(const NActors::TActorId& ownerId, ui32 scanId,
        const TString& database, const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, database, sysViewInfo, tableRange, columns)
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
                YDB_LOG_CRIT_CTX(*TlsActivationContext, "NSysView::TPartitionStatsScan: unexpected event",
                    {"eventType", ev->GetTypeRewrite()});
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
                insert({TSchema::TxCompleteLag::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<i64>(stats.GetTxCompleteLagMsec() * 1000);
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
                insert({TSchema::LocksAcquired::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetLocksAcquired());
                }});
                insert({TSchema::LocksWholeShard::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetLocksWholeShard());
                }});
                insert({TSchema::LocksBroken::ColumnId, [] (const TPartitionStatsResult&, const TPartitionStats&, const TPartitionStats& stats) {
                    return TCell::Make<ui64>(stats.GetLocksBroken());
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

THolder<NActors::IActor> CreatePartitionStatsScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const NKikimrSysView::TSysViewDescription& sysViewInfo,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TPartitionStatsScan>(ownerId, scanId, database, sysViewInfo, tableRange, columns);
}

} // NSysView
} // NKikimr
