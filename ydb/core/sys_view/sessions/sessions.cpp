#include "sessions.h"


#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/events/events.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView {

using namespace NActors;
using namespace NNodeWhiteboard;

class TSessionsScan : public NKikimr::NSysView::TScanActorBase<TSessionsScan> {
public:
    using TBase = NKikimr::NSysView::TScanActorBase<TSessionsScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    using TNodeInfo = NKikimrKqp::TSessionInfo;
    using TExtractor = std::function<TCell(const TNodeInfo&, ui32)>;
    using TSchema = Schema::QuerySessions;

    struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
        TExtractorsMap() {
            insert({TSchema::SessionId::ColumnId, [] (const TNodeInfo& info, ui32) {  // 1
                return TCell(info.GetSessionId().data(), info.GetSessionId().size());
            }});

            insert({TSchema::NodeId::ColumnId, [] (const TNodeInfo&, ui32 nodeId) { // 2
                return TCell::Make<ui32>(nodeId);
            }});

            insert({TSchema::State::ColumnId, [] (const TNodeInfo& info, ui32) {  // 3
                return TCell(info.GetState().data(), info.GetState().size());
            }});

            insert({TSchema::Query::ColumnId, [] (const TNodeInfo& info, ui32) {  // 4
                return TCell(info.GetQuery().data(), info.GetQuery().size());
            }});

            insert({TSchema::QueryCount::ColumnId, [] (const TNodeInfo& info, ui32) { // 5
                return TCell::Make<ui32>(info.GetQueryCount());
            }});

            insert({TSchema::ClientAddress::ColumnId, [] (const TNodeInfo& info, ui32) {  // 6
                return TCell(info.GetClientAddress().data(), info.GetClientAddress().size());
            }});

            insert({TSchema::ClientPID::ColumnId, [] (const TNodeInfo& info, ui32) {  // 7
                return TCell(info.GetClientPID().data(), info.GetClientPID().size());
            }});

            insert({TSchema::ClientUserAgent::ColumnId, [] (const TNodeInfo& info, ui32) {  //8
                return TCell(info.GetClientUserAgent().data(), info.GetClientUserAgent().size());
            }});

            insert({TSchema::ClientSdkBuildInfo::ColumnId, [] (const TNodeInfo& info, ui32) {  // 9
                return TCell(info.GetClientSdkBuildInfo().data(), info.GetClientSdkBuildInfo().size());
            }});

            insert({TSchema::ApplicationName::ColumnId, [] (const TNodeInfo& info, ui32) {  // 10
                return TCell(info.GetApplicationName().data(), info.GetApplicationName().size());
            }});

            insert({TSchema::SessionStartAt::ColumnId, [] (const TNodeInfo& info, ui32) {  // 11
                return TCell::Make<ui64>(info.GetSessionStartAt());
            }});

            insert({TSchema::QueryStartAt::ColumnId, [] (const TNodeInfo& info, ui32) {  // 12
                return info.GetQueryStartAt() ? TCell::Make<ui64>(info.GetQueryStartAt()) : TCell();
            }});

            insert({TSchema::StateChangeAt::ColumnId, [] (const TNodeInfo& info, ui32) {  // 13
                return info.GetStateChangeAt() ? TCell::Make<ui64>(info.GetStateChangeAt()) : TCell();
            }});

            insert({TSchema::UserSID::ColumnId, [] (const TNodeInfo& info, ui32) {   // 14
                return TCell(info.GetUserSID().data(), info.GetUserSID().size());
            }});
        }
    };

    TSessionsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
        const auto& cellsFrom = TableRange.From.GetCells();
        if (cellsFrom.size() == 1 && !cellsFrom[0].IsNull()) {
            SessionIdFrom = cellsFrom[0].AsBuf();
            SessionIdFromInclusive = TableRange.FromInclusive;
        }

        const auto& cellsTo = TableRange.To.GetCells();
        if (cellsTo.size() == 1 && !cellsTo[0].IsNull()) {
            SessionIdTo = cellsTo[0].AsBuf();
            SessionIdToInclusive = TableRange.ToInclusive;
        }

        static TExtractorsMap extractors;
        ColumnsExtractors.reserve(Columns.size());
        ColumnsToRead.reserve(Columns.size());
        for(const auto& column: Columns) {
            auto it = extractors.find(column.Tag);
            ColumnsToRead.push_back(column.Tag);
            if (it != extractors.end()) {
                ColumnsExtractors.push_back(it->second);
            } else {
                MissingSchemaColumns.push_back(column.Tag);
            }
        }
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(NActors::TEvInterconnect::TEvNodeConnected, Connected);
            hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            hFunc(NKqp::TEvKqp::TEvListSessionsResponse, Handle);
            hFunc(NKqp::TEvKqp::TEvListProxyNodesResponse, Handle);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TSessionsScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TSessionsScan::StateScan);
        if (!MissingSchemaColumns.empty()) {
            TStringBuilder message;
            message << "Missing schema column tags: ";
            bool first = true;
            for(ui32 tag: MissingSchemaColumns) {
                if (!first) {
                    message << ", ";
                }
                if (first) first = false;
                message << tag;
            }

            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, message);
            return;
        }

        if (AckReceived) {
            StartScan();
        }
    }

    void StartScan() {
        if (IsEmptyRange) {
            ReplyEmptyAndDie();
            return;
        }

        if (!PendingNodesInitialized) {
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKikimr::NKqp::TEvKqp::TEvListProxyNodesRequest());
            return;
        }

        if (!PendingNodes.empty() && !PendingRequest)  {
            const auto& nodeId = PendingNodes.front();
            auto kqpProxyId = NKqp::MakeKqpProxyID(nodeId);
            auto req = std::make_unique<NKikimr::NKqp::TEvKqp::TEvListSessionsRequest>();
            req->Record.SetTenantName(TenantName);
            if (!ContinuationToken.empty()) {
                req->Record.SetSessionIdStart(ContinuationToken);
                req->Record.SetSessionIdStartInclusive(true);
            } else {
                req->Record.SetSessionIdStart(SessionIdFrom);
                req->Record.SetSessionIdStartInclusive(SessionIdFromInclusive);
            }

            req->Record.SetSessionIdEnd(SessionIdTo);
            req->Record.SetSessionIdEndInclusive(SessionIdToInclusive);
            req->Record.MutableColumns()->Add(ColumnsToRead.begin(), ColumnsToRead.end());
            if (FreeSpace == 0) {
                FreeSpace = 1_KB;
            }

            req->Record.SetFreeSpace(FreeSpace);

            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
                "Send request to node, node_id="  << nodeId << ", request: " << req->Record.ShortDebugString());

            Send(kqpProxyId, req.release(), 0, nodeId);
            PendingRequest = true;
        }
    }

    void Handle(NKqp::TEvKqp::TEvListProxyNodesResponse::TPtr& ev) {
        auto& proxies = ev->Get()->ProxyNodes;
        std::sort(proxies.begin(), proxies.end());
        PendingNodes = std::deque<ui32>(proxies.begin(), proxies.end());
        PendingNodesInitialized = true;
        StartScan();
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr& ev) {
        FreeSpace = ev->Get()->FreeSpace;
        StartScan();
    }

    void Handle(NKqp::TEvKqp::TEvListSessionsResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        LastResponse = std::move(record);
        ProcessRows();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NKqp::TKqpEvents::EvListSessionsRequest) {
            ui32 nodeId = ev->Cookie;
            LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
                "Received undelivered response for node_id: " << nodeId);
            StartScan();
        }
    }

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr&) {
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        Y_UNUSED(nodeId);
        ProcessRows();
    }

    void ProcessRows() {
        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        auto nodeId = LastResponse.GetNodeId();
        for(int idx = 0; idx < LastResponse.GetSessions().size(); ++idx) {
            TVector<TCell> cells;
            for (auto extractor : ColumnsExtractors) {
                cells.push_back(extractor(LastResponse.GetSessions(idx), nodeId));
            }

            TArrayRef<const TCell> ref(cells);
            batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        if (LastResponse.GetFinished()) {
            PendingNodes.pop_front();
            ContinuationToken = TString();
            if (PendingNodes.empty()) {
                batch->Finished = true;
            }

        } else {
            ContinuationToken = LastResponse.GetContinuationToken();
        }

        PendingRequest = false;
        SendBatch(std::move(batch));
        StartScan();
    }

    void PassAway() override {
        TBase::PassAway();
    }

private:
    TString SessionIdFrom;
    bool SessionIdFromInclusive = false;
    TString SessionIdTo;
    bool SessionIdToInclusive = false;

    TString ContinuationToken;

    bool PendingRequest = false;
    bool IsEmptyRange = false;
    std::vector<ui32> MissingSchemaColumns;
    std::deque<ui32> PendingNodes;
    i64 FreeSpace = 0;
    bool PendingNodesInitialized = false;
    std::vector<TExtractor> ColumnsExtractors;
    std::vector<ui32> ColumnsToRead;
    NKikimrKqp::TEvListSessionsResponse LastResponse;
};

THolder<NActors::IActor> CreateSessionsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TSessionsScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NKikimr::NSysView
