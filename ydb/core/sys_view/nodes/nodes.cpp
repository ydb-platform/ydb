#include "nodes.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NSysView {

using namespace NActors;
using namespace NNodeWhiteboard;

class TNodesScan : public TScanActorBase<TNodesScan> {
public:
    using TBase  = TScanActorBase<TNodesScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TNodesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
        const auto& cellsFrom = TableRange.From.GetCells();
        if (cellsFrom.size() == 1 && !cellsFrom[0].IsNull()) {
            NodeIdFrom = cellsFrom[0].AsValue<ui32>();
            if (!TableRange.FromInclusive) {
                if (NodeIdFrom < std::numeric_limits<ui32>::max()) {
                    ++NodeIdFrom;
                } else {
                    IsEmptyRange = true;
                }
            }
        }

        const auto& cellsTo = TableRange.To.GetCells();
        if (cellsTo.size() == 1 && !cellsTo[0].IsNull()) {
            NodeIdTo = cellsTo[0].AsValue<ui32>();
            if (!TableRange.ToInclusive) {
                if (NodeIdTo > 0) {
                    --NodeIdTo;
                } else {
                    IsEmptyRange = true;
                }
            }
        }
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeConnected, Connected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
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
        Become(&TNodesScan::StateScan);
        if (AckReceived) {
            StartScan();
        }
    }

    void StartScan() {
        if (IsEmptyRange || TenantNodes.empty()) {
            ReplyEmptyAndDie();
            return;
        }

        const NActors::TActorId nameserviceId = GetNameserviceActorId();
        Send(nameserviceId, new TEvInterconnect::TEvListNodes());

        for (const auto& nodeId : TenantNodes) {
            TActorId whiteboardId = MakeNodeWhiteboardServiceId(nodeId);
            auto request = MakeHolder<TEvWhiteboard::TEvSystemStateRequest>();

            Send(whiteboardId, request.Release(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = ev->Release();

        for (const auto& info : nodesInfo->Nodes) {
            auto nodeId = info.NodeId;

            if (TenantNodes.find(nodeId) != TenantNodes.end() &&
                nodeId >= NodeIdFrom &&
                nodeId <= NodeIdTo)
            {
                NodesInfo.emplace(nodeId, info);
            }
        }

        if (NodesInfo.empty()) {
            ReplyEmptyAndDie();
        }
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        WBSystemInfo[nodeId] = ev->Release();
        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == TEvWhiteboard::EvSystemStateRequest) {
            ui32 nodeId = ev.Get()->Cookie;
            WBSystemInfo[nodeId] = nullptr;
            RequestDone();
        }
    }

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr&) {
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        WBSystemInfo[nodeId] = nullptr;
        RequestDone();
    }

    void RequestDone() {
        if (NodesInfo.empty() || TenantNodes.size() != WBSystemInfo.size()) {
            return;
        }

        using TNodeInfo = TEvInterconnect::TNodeInfo;
        using TWBInfo = TEvWhiteboard::TEvSystemStateResponse;
        using TExtractor = std::function<TCell(const TNodeInfo&, const TWBInfo*)>;
        using TSchema = Schema::Nodes;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            static TMaybe<ui64> GetStartTime(const TWBInfo* wbInfo) {
                if (!wbInfo || wbInfo->Record.SystemStateInfoSize() != 1) {
                    return {};
                }
                const auto& systemState = wbInfo->Record.GetSystemStateInfo(0);
                if (!systemState.HasStartTime()) {
                    return {};
                }
                return systemState.GetStartTime();
            };

            TExtractorsMap() {
                insert({TSchema::NodeId::ColumnId, [] (const TNodeInfo& info, const TWBInfo*) {
                    return TCell::Make<ui32>(info.NodeId);
                }});
                insert({TSchema::Address::ColumnId, [] (const TNodeInfo& info, const TWBInfo*) {
                    return TCell(info.Address.data(), info.Address.size());
                }});
                insert({TSchema::Host::ColumnId, [] (const TNodeInfo& info, const TWBInfo*) {
                    return TCell(info.Host.data(), info.Host.size());
                }});
                insert({TSchema::Port::ColumnId, [] (const TNodeInfo& info, const TWBInfo*) {
                    return TCell::Make<ui32>(info.Port);
                }});
                insert({TSchema::StartTime::ColumnId, [] (const TNodeInfo&, const TWBInfo* wbInfo) {
                    auto time = TExtractorsMap::GetStartTime(wbInfo);
                    return time ? TCell::Make<ui64>(*time * 1000u) : TCell();
                }});
                insert({TSchema::UpTime::ColumnId, [] (const TNodeInfo&, const TWBInfo* wbInfo) {
                    auto time = TExtractorsMap::GetStartTime(wbInfo);
                    if (!time) {
                        return TCell();
                    }
                    auto interval = TInstant::Now().MicroSeconds() - *time * 1000;
                    return TCell::Make<i64>(interval > 0 ? interval : 0);
                }});
                insert({TSchema::CpuThreads::ColumnId, [] (const TNodeInfo&, const TWBInfo* wbInfo) {
                    ui32 threads = 0;
                    if (wbInfo && wbInfo->Record.SystemStateInfoSize() == 1) {
                        const auto& systemState = wbInfo->Record.GetSystemStateInfo(0);
                        for (const auto& poolStat : systemState.GetPoolStats()) {
                            if (poolStat.GetName() != "IO") {
                                threads += poolStat.GetThreads();
                            }
                        }
                    }
                    return TCell::Make<ui32>(threads);
                }});
                insert({TSchema::CpuUsage::ColumnId, [] (const TNodeInfo&, const TWBInfo* wbInfo) {
                    ui32 threads = 0;
                    double usage = 0.0;
                    if (wbInfo && wbInfo->Record.SystemStateInfoSize() == 1) {
                        const auto& systemState = wbInfo->Record.GetSystemStateInfo(0);
                        for (const auto& poolStat : systemState.GetPoolStats()) {
                            if (poolStat.GetName() != "IO") {
                                threads += poolStat.GetThreads();
                                usage += poolStat.GetUsage() * poolStat.GetThreads();
                            }
                        }
                    }
                    return threads ? TCell::Make<double>(usage / threads) : TCell();
                }});
                insert({TSchema::CpuIdle::ColumnId, [] (const TNodeInfo&, const TWBInfo* wbInfo) {
                    double idle = 1.0;
                    if (wbInfo && wbInfo->Record.SystemStateInfoSize() == 1) {
                        const auto& systemState = wbInfo->Record.GetSystemStateInfo(0);
                        for (const auto& poolStat : systemState.GetPoolStats()) {
                            if (poolStat.GetName() != "IO") {
                                idle = std::min(idle, 1.0 - poolStat.GetUsage());
                            }
                        }
                    }
                    return TCell::Make<double>(std::max(idle, 0.0));
                }});
            }
        };
        static TExtractorsMap extractors;

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;

        TVector<TCell> cells;
        for (const auto& info : NodesInfo) {
            auto nodeId = info.first;
            auto* wbInfo = WBSystemInfo[nodeId].Get();

            for (auto column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(info.second, wbInfo));
                }
            }

            TArrayRef<const TCell> ref(cells);
            batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        SendBatch(std::move(batch));
    }

    void PassAway() override {
        for (const auto& info : NodesInfo) {
            Send(TActivationContext::InterconnectProxy(info.first), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

private:
    ui32 NodeIdFrom = 0;
    ui32 NodeIdTo = std::numeric_limits<ui32>::max();
    bool IsEmptyRange = false;

    TMap<ui32, TEvInterconnect::TNodeInfo> NodesInfo;
    THashMap<TNodeId, THolder<TEvWhiteboard::TEvSystemStateResponse>> WBSystemInfo;
};

THolder<NActors::IActor> CreateNodesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TNodesScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NSysView
} // NKikimr
