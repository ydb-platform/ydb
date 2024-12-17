#include "tablets.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/mind/hive/tablet_info.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView {

using namespace NActors;

class TTabletsScan : public TScanActorBase<TTabletsScan> {
public:
    using TBase = TScanActorBase<TTabletsScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TTabletsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvSysView::TEvGetTabletIdsResponse, Handle);
            hFunc(TEvSysView::TEvGetTabletsResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TTabletsScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TThis::StateScan);
        if (AckReceived) {
            RequestTabletIds();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        if (TabletIds.empty()) {
            RequestTabletIds();
        } else {
            RequestBatch();
        }
    }

    bool CalculateRangeFrom() {
        /*
         *  Please note that TabletId and FollowerId do not have NULLs in columns
         */
        const auto& cellsFrom = TableRange.From.GetCells();

        // Empty means that we read from +inf, it is impossible
        if (cellsFrom.empty()) {
            YQL_ENSURE(false, "Range starts from +inf, can't read anything.");
            return false;
        }

        if (cellsFrom[0].IsNull()) {
            return true;
        }

        FromTabletId = cellsFrom[0].AsValue<ui64>();

        if (cellsFrom.size() == 1 && TableRange.FromInclusive) {
            return true;
        }

        if (cellsFrom.size() == 2) {
            if (!cellsFrom[1].IsNull()) {
                FromFollowerId = cellsFrom[1].AsValue<ui32>();
            }

            if (TableRange.FromInclusive) {
                return true;
            }

            // The range start from NULL exclusive. So, the next value after NULL will be used.
            if (!FromFollowerId.has_value()) {
                FromFollowerId = Min<ui32>();
                return true;
            }

            if (FromFollowerId.value() < Max<ui32>()) {
                FromFollowerId = FromFollowerId.value() + 1;
                return true;
            }

            FromFollowerId.reset();
        }

        if (FromTabletId < Max<ui64>()) {
            ++FromTabletId;
            return true;
        }

        return false;
    }

    bool CalculateRangeTo() {
        const auto& cellsTo = TableRange.To.GetCells();

        if (cellsTo.empty()) {
            return true;
        }

        YQL_ENSURE(!cellsTo[0].IsNull(), "Read to -inf range");

        ToTabletId = cellsTo[0].AsValue<ui64>();

        if (cellsTo.size() == 1 && TableRange.ToInclusive) {
            return true;
        }

        auto decreaseTabletId = [this]() {
            if (ToTabletId > Min<ui64>()) {
                --ToTabletId;
                return true;
            }

            return false;
        };

        if (cellsTo.size() == 2) {
            if (!cellsTo[1].IsNull()) {
                ToFollowerId = cellsTo[1].AsValue<ui32>();
            }

            if (TableRange.ToInclusive) {
                return true;
            }

            // The range ends at NULL exclusive. So, the value before NULL will be used.
            if (!ToFollowerId.has_value()) {
                ToFollowerId = Max<ui32>();
                return decreaseTabletId();
            }

            if (ToFollowerId > Min<ui32>()) {
                ToFollowerId = ToFollowerId.value() - 1;
                return true;
            }

            ToFollowerId.reset();
        }

        return decreaseTabletId();
    }

    void RequestTabletIds() {
        auto request = MakeHolder<TEvSysView::TEvGetTabletIdsRequest>();

        if (!CalculateRangeFrom() || !CalculateRangeTo()) {
            ReplyEmptyAndDie();
            return;
        }

        if (ToTabletId < FromTabletId) {
            ReplyEmptyAndDie();
            return;
        }

        auto& record = request->Record;
        record.SetFrom(FromTabletId);
        record.SetTo(ToTabletId);

        auto pipeCache = MakePipePerNodeCacheID(false);
        Send(pipeCache, new TEvPipeCache::TEvForward(request.Release(), HiveId, true),
            IEventHandle::FlagTrackDelivery);
    }

    void RequestBatch() {
        if (BatchRequested) {
            return;
        }
        BatchRequested = true;

        auto request = MakeHolder<TEvSysView::TEvGetTabletsRequest>();
        auto& record = request->Record;

        auto it = FromIterator;
        for (size_t count = 0; count < BatchSize && it != TabletIds.end(); ++count, ++it) {
            record.AddTabletIds(*it);
        }
        FromIterator = it;

        record.SetBatchSizeLimit(BatchSize);

        auto pipeCache = MakePipePerNodeCacheID(false);
        Send(pipeCache, new TEvPipeCache::TEvForward(request.Release(), HiveId, true),
            IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Delivery problem in TTabletsScan");
    }

    void Handle(TEvSysView::TEvGetTabletIdsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.TabletIdsSize() == 0) {
            ReplyEmptyAndDie();
            return;
        }

        TabletIds.reserve(record.TabletIdsSize());
        for (const auto& id : record.GetTabletIds()) {
            TabletIds.push_back(id);
        }
        std::sort(TabletIds.begin(), TabletIds.end());

        FromIterator = TabletIds.begin();
        RequestBatch();
    }

    void Handle(TEvSysView::TEvGetTabletsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        using TEntry = NKikimrSysView::TTabletEntry;
        using TExtractor = std::function<TCell(const TEntry&)>;
        using TSchema = Schema::Tablets;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::TabletId::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui64>(entry.GetTabletId());
                }});
                insert({TSchema::FollowerId::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui32>(entry.GetFollowerId());
                }});
                insert({TSchema::TypeCol::ColumnId, [] (const TEntry& entry) {
                    const auto& type = entry.GetType();
                    return TCell(type.data(), type.size());
                }});
                insert({TSchema::State::ColumnId, [] (const TEntry& entry) {
                    if (!entry.HasState()) {
                        return TCell();
                    }
                    const auto& state = entry.GetState();
                    return TCell(state.data(), state.size());
                }});
                insert({TSchema::VolatileState::ColumnId, [] (const TEntry& entry) {
                    const auto& state = entry.GetVolatileState();
                    return TCell(state.data(), state.size());
                }});
                insert({TSchema::BootState::ColumnId, [] (const TEntry& entry) {
                    const auto& state = entry.GetBootState();
                    return TCell(state.data(), state.size());
                }});
                insert({TSchema::Generation::ColumnId, [] (const TEntry& entry) {
                    if (!entry.HasGeneration()) {
                        return TCell();
                    }
                    return TCell::Make<ui32>(entry.GetGeneration());
                }});
                insert({TSchema::NodeId::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui32>(entry.GetNodeId());
                }});
                insert({TSchema::CPU::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<double>(entry.GetCPU() / 1000000.);
                }});
                insert({TSchema::Memory::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui64>(entry.GetMemory());
                }});
                insert({TSchema::Network::ColumnId, [] (const TEntry& entry) {
                    return TCell::Make<ui64>(entry.GetNetwork());
                }});
            }
        };
        static TExtractorsMap extractors;

        size_t index = 0;
        ui32 fromFollowerId = FromFollowerId.value_or(Min<ui32>());

        if (record.EntriesSize() > 0
            && record.GetEntries(0).GetTabletId() == FromTabletId)
        {
            for (; index < record.EntriesSize(); ++index) {
                const auto& entry = record.GetEntries(index);

                if (entry.GetTabletId() != FromTabletId || entry.GetFollowerId() >= fromFollowerId) {
                    break;
                }
            }
        }

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        TVector<TCell> cells;
        ui32 toFollowerId = ToFollowerId.value_or(Max<ui32>());

        for (; index < record.EntriesSize(); ++index) {
            const auto& entry = record.GetEntries(index);

            if (entry.GetTabletId() == ToTabletId && entry.GetFollowerId() > toFollowerId) {
                break;
            }

            for (auto& column : Columns) {
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

        if (record.HasNextTabletId()) {
            FromIterator = std::lower_bound(TabletIds.begin(), TabletIds.end(),
                record.GetNextTabletId());
        }

        if (FromIterator == TabletIds.end()) {
            batch->Finished = true;
        }

        BatchRequested = false;

        SendBatch(std::move(batch));
    }

    void PassAway() override {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    static constexpr size_t BatchSize = 10000;

    ui64 FromTabletId = 0;
    std::optional<ui32> FromFollowerId;

    ui64 ToTabletId = Max<ui64>();
    std::optional<ui32> ToFollowerId;

    TVector<ui64> TabletIds;
    TVector<ui64>::const_iterator FromIterator;

    bool BatchRequested = false;
};

THolder<NActors::IActor> CreateTabletsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TTabletsScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NKikimr::NSysView
