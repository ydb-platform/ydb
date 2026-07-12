#include "compile_cache.h"

#include <library/cpp/protobuf/interop/cast.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/sys_view/auth/auth_scan_base.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/library/yql/public/ydb_issue/ydb_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_cookie.h>

namespace NKikimr::NSysView {

namespace {

struct TEvPrivate {
    enum EEv {
        EvNodeRequestTimeout = NActors::TEvents::ES_PRIVATE,
    };

    struct TEvNodeRequestTimeout : public NActors::TEventLocal<TEvNodeRequestTimeout, EvNodeRequestTimeout> {
        ui32 NodeId = 0;

        explicit TEvNodeRequestTimeout(ui32 nodeId)
            : NodeId(nodeId)
        {}
    };
};

} // anonymous namespace

using namespace NActors;
using namespace NNodeWhiteboard;

class TCompileCacheQueriesScan : public NKikimr::NSysView::TScanActorBase<TCompileCacheQueriesScan> {
public:
    using TBase = NKikimr::NSysView::TScanActorBase<TCompileCacheQueriesScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    using TCompileCacheQuery = NKikimrKqp::TCompileCacheQueryInfo;
    using TExtractor = std::function<TCell(const TCompileCacheQuery&, ui32)>;
    using TSchema = Schema::CompileCacheQueries;

    struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
        TExtractorsMap() {
            insert({TSchema::NodeId::ColumnId, [] (const TCompileCacheQuery&, ui32 nodeId) {  // 1
                return TCell::Make<ui32>(nodeId);
            }});

            insert({TSchema::QueryId::ColumnId, [] (const TCompileCacheQuery& info, ui32) { // 2
                return TCell(info.GetQueryId().data(), info.GetQueryId().size());
            }});

            insert({TSchema::Query::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 3
                return TCell(info.GetQuery().data(), info.GetQuery().size());
            }});

            insert({TSchema::AccessCount::ColumnId, [] (const TCompileCacheQuery& info, ui32) { // 4
                return TCell::Make<ui64>(info.GetAccessCount());
            }});

            insert({TSchema::CompiledAt::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 5
                return TCell::Make<ui64>(info.GetCompiledQueryAt());
            }});

            insert({TSchema::UserSID::ColumnId, [] (const TCompileCacheQuery& info, ui32) {   // 6
                return TCell(info.GetUserSID().data(), info.GetUserSID().size());
            }});

            insert({TSchema::LastAccessedAt::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 7
                return TCell::Make<ui64>(info.GetLastAccessedAt());
            }});

            insert({TSchema::CompilationDurationMs::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 8
                return TCell::Make<ui64>(NProtoInterop::CastFromProto(info.GetCompilationDuration()).MilliSeconds());
            }});

            insert({TSchema::Warnings::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 9
                return TCell(info.GetWarnings());
            }});

            insert({TSchema::Metadata::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 10
                return TCell(info.GetMetaInfo());
            }});

            insert({TSchema::IsTruncated::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 11
                return TCell::Make<bool>(info.GetIsTruncated());
            }});

            insert({TSchema::QueryType::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 12
                return TCell(info.GetQueryType());
            }});

            insert({TSchema::Syntax::ColumnId, [] (const TCompileCacheQuery& info, ui32) {  // 13
                return TCell(info.GetSyntax());
            }});
        }
    };

    TCompileCacheQueriesScan(const NActors::TActorId& ownerId, ui32 scanId,
        const TString& database, const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TBase(ownerId, scanId, database, sysViewInfo, tableRange, columns)
        , UserToken(std::move(userToken))
    {
        const auto& cellsFrom = TableRange.From.GetCells();
        if (cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
            NodeIdFrom = cellsFrom[0].AsValue<ui32>();
            NodeIdFromInclusive = TableRange.FromInclusive;
            HasNodeIdFrom = true;
        }
        if (cellsFrom.size() > 1 && !cellsFrom[1].IsNull()) {
            QueryIdFrom = cellsFrom[1].AsBuf();
            QueryIdFromInclusive = TableRange.FromInclusive;
        }

        const auto& cellsTo = TableRange.To.GetCells();
        if (cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
            NodeIdTo = cellsTo[0].AsValue<ui32>();
            NodeIdToInclusive = TableRange.ToInclusive;
            HasNodeIdTo = true;
        }
        if (cellsTo.size() > 1 && !cellsTo[1].IsNull()) {
            QueryIdTo = cellsTo[1].AsBuf();
            QueryIdToInclusive = TableRange.ToInclusive;
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
            hFunc(NKqp::TEvKqp::TEvListQueryCacheQueriesResponse, Handle);
            hFunc(NKqp::TEvKqp::TEvListProxyNodesResponse, Handle);
            hFunc(TEvPrivate::TEvNodeRequestTimeout, HandleNodeRequestTimeout);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TCompileCacheQueriesScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TCompileCacheQueriesScan::StateScan);

        if (UserToken) {
            bool isClusterAdmin = IsAdministrator(AppData(), UserToken.Get());
            bool isDatabaseAdmin = AppData()->FeatureFlags.GetEnableDatabaseAdmin()
                && IsDatabaseAdministrator(UserToken.Get(), DatabaseOwner);
            IsAdmin = isClusterAdmin || isDatabaseAdmin;
        }

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

        // if feature flag is not set -- return only for self node
        if (!AppData()->FeatureFlags.GetEnableCompileCacheView()) {
            PendingNodesInitialized = true;
            ui32 selfNodeId = SelfId().NodeId();

            // Check if self node matches the NodeId filter
            bool matchFrom = !HasNodeIdFrom ||
                (NodeIdFromInclusive ? selfNodeId >= NodeIdFrom : selfNodeId > NodeIdFrom);
            bool matchTo = !HasNodeIdTo ||
                (NodeIdToInclusive ? selfNodeId <= NodeIdTo : selfNodeId < NodeIdTo);

            if (matchFrom && matchTo) {
                PendingNodes.emplace_back(selfNodeId);
                HadNodesToScan = true;
            }
            NodesTotal = PendingNodes.size();
        }

        if (AckReceived) {
            StartScan();
        }
    }

    void CancelNodeRequestTimeout() {
        NodeRequestTimeoutCookieHolder.Detach();
    }

    void ScheduleNodeRequestTimeout(ui32 nodeId) {
        CancelNodeRequestTimeout();
        NodeRequestTimeoutCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(NodeRequestTimeout, new TEvPrivate::TEvNodeRequestTimeout(nodeId), NodeRequestTimeoutCookieHolder.Get());
    }

    void SkipCurrentNode(const char* reason, ui32 nodeId, const NYql::TIssues* peerIssues = nullptr) {
        LOG_WARN_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Skipping compile cache scan for node_id=" << nodeId << ": " << reason
            << (peerIssues && !peerIssues->Empty()
                ? TStringBuilder() << ", peer issues: " << peerIssues->ToOneLineString()
                : TString()));

        if (auto& counters = AppData()->Counters) {
            counters
                ->GetSubgroup("counters", "kqp")
                ->GetCounter("CompileCacheView/PeerScanWarnings", true)
                ->Inc();
        }

        // Preserve the peer-reported reason (tenant mismatch, auth, etc.) as sub-issues
        // so the final SendScanWarning summary stays actionable.
        NYql::TIssue skipIssue(
            TStringBuilder() << "compile cache scan skipped node_id=" << nodeId << ": " << reason);
        if (peerIssues) {
            for (const auto& issue : *peerIssues) {
                skipIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            }
        }
        PartialIssues.AddIssue(std::move(skipIssue));

        CancelNodeRequestTimeout();
        PendingRequest = false;
        ContinuationToken.clear();
        if (!PendingNodes.empty()) {
            PendingNodes.pop_front();
        }
        ++NodesFailed;
        if (PendingNodes.empty()) {
            FinishScanOrReplyEmpty();
        } else {
            StartScan();
        }
    }

    void SendScanWarning(const NYql::TIssues& issues) {
        if (issues.Empty()) {
            return;
        }
        auto warn = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>();
        warn->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        NYql::IssuesToMessage(issues, warn->Record.MutableIssues());
        Send(OwnerActorId, warn.Release());
    }

    void MaybeSendPartialScanWarning() {
        if (PartialWarningSent || NodesFailed == 0 || !HadNodesToScan) {
            return;
        }
        PartialWarningSent = true;

        const ui32 nodesTotal = NodesTotal > 0 ? NodesTotal : (NodesSucceeded + NodesFailed);
        LOG_WARN_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Compile cache scan: skipped " << NodesFailed << " of " << nodesTotal << " nodes");

        NYql::TIssue summary(TStringBuilder()
            << "compile cache scan: skipped " << NodesFailed << " of " << nodesTotal << " nodes");
        for (const auto& issue : PartialIssues) {
            summary.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
        }
        NYql::TIssues toSend;
        toSend.AddIssue(std::move(summary));
        SendScanWarning(toSend);
    }

    void FinishScanOrReplyEmpty() {
        if (HadNodesToScan && NodesSucceeded == 0 && NodesFailed > 0) {
            ReplyErrorAndDie(
                Ydb::StatusIds::UNAVAILABLE,
                "Failed to read compile cache from all nodes");
            return;
        }
        MaybeSendPartialScanWarning();
        ReplyEmptyAndDie();
    }

    void StartScan() {
        if (IsEmptyRange) {
            ReplyEmptyAndDie();
            return;
        }

        // Check if database is serverless: for serverless databases, TenantName != AppData()->TenantName
        // This is a simple heuristic - serverless databases use shared compute resources
        if (TenantName != AppData()->TenantName) {
            ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Compile cache is not available for this database");
            return;
        }

        if (!PendingNodesInitialized && !PendingRequest) {
            PendingRequest = true;
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKikimr::NKqp::TEvKqp::TEvListProxyNodesRequest());
            return;
        }

        // If no pending nodes left after initialization/filtering, return empty result
        if (PendingNodesInitialized && PendingNodes.empty()) {
            FinishScanOrReplyEmpty();
            return;
        }

        if (!PendingNodes.empty() && !PendingRequest)  {
            const auto& nodeId = PendingNodes.front();
            CurrentNodeHadSuccessfulResponse = false;
            auto kqpProxyId = NKqp::MakeKqpCompileServiceID(nodeId);
            auto req = std::make_unique<NKikimr::NKqp::TEvKqp::TEvListQueryCacheQueriesRequest>();
            req->Record.SetTenantName(TenantName);
            if (!ContinuationToken.empty()) {
                req->Record.SetQueryIdStart(ContinuationToken);
                req->Record.SetQueryIdStartInclusive(true);
            } else {
                req->Record.SetQueryIdStart(QueryIdFrom);
                req->Record.SetQueryIdStartInclusive(QueryIdFromInclusive);
            }

            req->Record.SetQueryIdEnd(QueryIdTo);
            req->Record.SetQueryIdEndInclusive(QueryIdToInclusive);
            req->Record.MutableColumns()->Add(ColumnsToRead.begin(), ColumnsToRead.end());
            if (FreeSpace == 0) {
                FreeSpace = 1_KB;
            }

            req->Record.SetFreeSpace(FreeSpace);

            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
                "Send request to node_id=" << nodeId << ", request: " << req->Record.ShortDebugString());

            Send(kqpProxyId, req.release(), IEventHandle::FlagTrackDelivery, nodeId);
            PendingRequest = true;
            ScheduleNodeRequestTimeout(nodeId);
        }
    }

    void Handle(NKqp::TEvKqp::TEvListProxyNodesResponse::TPtr& ev) {
        PendingRequest = false;
        if (AppData()->FeatureFlags.GetEnableCompileCacheView()) {
            auto& proxies = ev->Get()->ProxyNodes;
            std::sort(proxies.begin(), proxies.end());

            for (ui32 nodeId : proxies) {
                bool matchFrom = !HasNodeIdFrom ||
                    (NodeIdFromInclusive ? nodeId >= NodeIdFrom : nodeId > NodeIdFrom);
                bool matchTo = !HasNodeIdTo ||
                    (NodeIdToInclusive ? nodeId <= NodeIdTo : nodeId < NodeIdTo);
                if (matchFrom && matchTo) {
                    PendingNodes.push_back(nodeId);
                    HadNodesToScan = true;
                }
            }
            NodesTotal = PendingNodes.size();
        }
        PendingNodesInitialized = true;
        StartScan();
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr& ev) {
        FreeSpace = ev->Get()->FreeSpace;
        StartScan();
    }

    void Handle(NKqp::TEvKqp::TEvListQueryCacheQueriesResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const ui32 responseNodeId = record.GetNodeId();

        // Stale: timeout/disconnect already advanced PendingNodes.
        if (!PendingRequest || PendingNodes.empty() || PendingNodes.front() != responseNodeId) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
                "Ignoring stale TEvListQueryCacheQueriesResponse from node_id=" << responseNodeId);
            return;
        }

        if (record.HasStatus() && record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues peerIssues;
            NYql::IssuesFromMessage(record.GetIssues(), peerIssues);
            SkipCurrentNode("node returned error", responseNodeId, &peerIssues);
            return;
        }

        CancelNodeRequestTimeout();
        if (!CurrentNodeHadSuccessfulResponse) {
            CurrentNodeHadSuccessfulResponse = true;
            ++NodesSucceeded;
        }

        LastResponse = std::move(record);
        ProcessRows();
    }

    void HandleNodeRequestTimeout(TEvPrivate::TEvNodeRequestTimeout::TPtr& ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        if (PendingRequest && !PendingNodes.empty() && PendingNodes.front() == nodeId) {
            SkipCurrentNode("node request timeout", nodeId);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType != NKqp::TKqpEvents::EvListCompileCacheQueriesRequest) {
            return;
        }
        const ui32 nodeId = ev->Cookie;
        if (!PendingRequest || PendingNodes.empty() || PendingNodes.front() != nodeId) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
                "Ignoring stale TEvUndelivered from node_id=" << nodeId);
            return;
        }
        SkipCurrentNode("undelivered", nodeId);
    }

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr&) {
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        if (PendingRequest && !PendingNodes.empty() && PendingNodes.front() == nodeId) {
            SkipCurrentNode("node disconnected", nodeId);
        }
    }

    bool CanAccessEntry(const TCompileCacheQuery& entry) const {
        if (!UserToken || IsAdmin) {
            return true;
        }

        // Filter by database: user can only see queries from their own database
        if (entry.HasDatabase() && entry.GetDatabase() != DatabaseName) {
            return false;
        }

        // Filter by user SID: non-admin user can only see their own queries
        return entry.GetUserSID() == UserToken->GetUserSID();
    }

    void ProcessRows() {
        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        auto nodeId = LastResponse.GetNodeId();

        for(int idx = 0; idx < LastResponse.GetCacheCacheQueries().size(); ++idx) {
            const auto& entry = LastResponse.GetCacheCacheQueries(idx);
            if (!CanAccessEntry(entry)) {
                continue;
            }

            TVector<TCell> cells;
            for (auto extractor : ColumnsExtractors) {
                cells.push_back(extractor(entry, nodeId));
            }

            TArrayRef<const TCell> ref(cells);
            batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        bool shouldContinue = true;
        if (LastResponse.GetFinished()) {
            PendingNodes.pop_front();
            ContinuationToken = TString();
            if (PendingNodes.empty()) {
                batch->Finished = true;
                shouldContinue = false;
            }

        } else {
            ContinuationToken = LastResponse.GetContinuationToken();
        }

        PendingRequest = false;
        if (batch->Finished) {
            MaybeSendPartialScanWarning();
        }
        SendBatch(std::move(batch));
        if (AppData()->FeatureFlags.GetEnableCompileCacheView() && shouldContinue) {
            StartScan();
        }
    }

    void PassAway() override {
        TBase::PassAway();
    }

private:
    ui32 NodeIdFrom = 0;
    bool NodeIdFromInclusive = false;
    bool HasNodeIdFrom = false;
    ui32 NodeIdTo = 0;
    bool NodeIdToInclusive = false;
    bool HasNodeIdTo = false;

    TString QueryIdFrom;
    bool QueryIdFromInclusive = false;
    TString QueryIdTo;
    bool QueryIdToInclusive = false;

    TString ContinuationToken;

    bool PendingRequest = false;
    bool IsEmptyRange = false;
    std::vector<ui32> MissingSchemaColumns;
    std::deque<ui32> PendingNodes;
    i64 FreeSpace = 0;
    bool PendingNodesInitialized = false;
    std::vector<TExtractor> ColumnsExtractors;
    std::vector<ui32> ColumnsToRead;
    NKikimrKqp::TEvListCompileCacheQueriesResponse LastResponse;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool IsAdmin = false;

    static constexpr TDuration NodeRequestTimeout = TDuration::Seconds(10);

    bool HadNodesToScan = false;
    ui32 NodesTotal = 0;
    ui32 NodesSucceeded = 0;
    ui32 NodesFailed = 0;
    bool CurrentNodeHadSuccessfulResponse = false;
    bool PartialWarningSent = false;
    NYql::TIssues PartialIssues;
    NActors::TSchedulerCookieHolder NodeRequestTimeoutCookieHolder;
    };
THolder<NActors::IActor> CreateCompileCacheQueriesScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const NKikimrSysView::TSysViewDescription& sysViewInfo,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TCompileCacheQueriesScan>(ownerId, scanId, database, sysViewInfo, tableRange, columns, std::move(userToken));
}

} // NKikimr::NSysView
