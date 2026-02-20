#include "db_metadata_cache.h"
#include "service_monitoring.h"

#include "rpc_kqp_base.h"
#include "rpc_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/cms.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/random/shuffle.h>

#include <ydb/core/counters_info/counters_info.h>
#include <ydb/core/health_check/health_check.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/core/protos/cluster_state_info.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <google/protobuf/util/json_util.h>

#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvClusterStateRequest = TGrpcRequestOperationCall<Ydb::Monitoring::ClusterStateRequest, Ydb::Monitoring::ClusterStateResponse>;

class TClusterStateRPC : public TRpcRequestActor<TClusterStateRPC, TEvClusterStateRequest, true> {
public:
    using TRpcRequestActor::TRpcRequestActor;
    using TThis = TClusterStateRPC;
    using TBase = TRpcRequestActor<TClusterStateRPC, TEvClusterStateRequest, true>;

    ui32 CountersMaxInflight;
    ui32 MaxCountersSize;
    TVector<ui32> NodeRequested;
    TVector<ui32> NodeReceived;
    ui32 Requested = 0;
    ui32 Received = 0;
    TString SessionId;
    ui32 CountersInflight = 0;
    ui32 CountersNodeToRequest = 0;
    bool CountersNextBlock = true;


    struct TQuery {
        struct TColumn {
            TString Name;
            bool Sensitive = false;

            TColumn() {}

            TColumn(const char* name)
            : Name(name)
            {}

            TColumn(const char* name, bool sensitive)
            : Name(name)
            , Sensitive(sensitive)
            {}

            TString ToSelect(bool sanitize) const {
                if (sanitize && Sensitive) {
                    return TStringBuilder() << "Unicode::ReplaceLast(Unicode::SplitToList(`" << Name << "`, ' ')[0], '', '...') AS `" << Name << '`';
                }
                return TStringBuilder() << '`' << Name << '`';
            }
        };

        TVector<TColumn> Columns;
        TString TableName;
        bool Sensitive = false;

        TString ToSelect(bool sanitize) {
            TStringBuilder sb;
            sb << "SELECT ";
            ui32 cnt = 0;
            for (auto& c : Columns) {
                if (cnt++) {
                    sb << ',';
                }
                sb << c.ToSelect(sanitize);
            }
            sb << " FROM `" << TableName << '`';
            return sb;
        }
    };

    TVector<TQuery::TColumn> TopQueryColumns = { "RequestUnits", "IntervalEnd", "Rank", {"QueryText", true}, "Duration", "EndTime", "ReadRows", "ReadBytes", "UpdateRows", "UpdateBytes", "DeleteRows", "DeleteBytes", "Partitions", "UserSID", "ParametersSize", "CompileDuration", "FromQueryCache", "CPUTime", "ShardCount", "SumShardCPUTime", "MinShardCPUTime", "MaxShardCPUTime", "ComputeNodesCount", "SumComputeCPUTime", "MinComputeCPUTime", "MaxComputeCPUTime", "CompileCPUTime", "ProcessCPUTime", "Type" };
    TVector<TQuery> Queries = {
        {{ "Path", "Sid", "Permission" }, ".sys/auth_effective_permissions", true },
        {{ "GroupSid", "MemberSid" }, ".sys/auth_group_members", true },
        {{ "Sid" }, ".sys/auth_groups", true },
        {{ "Path", "Sid" }, ".sys/auth_owners", true },
        {{ "Path", "Sid", "Permission" }, ".sys/auth_permissions", true },
        {{ "Sid", "IsEnabled", "IsLockedOut", "CreatedAt", "LastSuccessfulAttemptAt", "LastFailedAttemptAt", "FailedAttemptCount", "PasswordHash" }, ".sys/auth_users", true },
        {{ "NodeId", "QueryId", {"Query", true}, "AccessCount", "CompiledAt", "UserSID", "LastAccessedAt", "CompilationDurationMs", "Warnings", "Metadata" }, ".sys/compile_cache_queries" },
        {{ "BridgeSyncRunning", "GroupId", "Generation", "ErasureSpecies", "BoxId", "StoragePoolId", "EncryptionMode", "LifeCyclePhase", "AllocatedSize", "AvailableSize", "SeenOperational", "PutTabletLogLatency", "PutUserDataLatency", "GetFastLatency", "LayoutCorrect", "OperatingStatus", "ExpectedStatus", "ProxyGroupId", "BridgePileId", "GroupSizeInUnits", "BridgeSyncStage", "BridgeDataSyncProgress", "BridgeDataSyncErrors", "BridgeSyncLastError", "BridgeSyncLastErrorTimestamp", "BridgeSyncFirstErrorTimestamp", "BridgeSyncErrorCount" }, ".sys/ds_groups" },
        {{ "NodeId", "PDiskId", "Type", "Kind", "Path", "Guid", "BoxId", "SharedWithOS", "ReadCentric", "AvailableSize", "TotalSize", "Status", "StatusChangeTimestamp", "ExpectedSlotCount", "NumActiveSlots", "DecommitStatus", "State", "SlotSizeInUnits" }, ".sys/ds_pdisks" },
        {{ "BoxId", "StoragePoolId", "Name", "Generation", "ErasureSpecies", "VDiskKind", "Kind", "NumGroups", "EncryptionMode", "SchemeshardId", "PathId", "DefaultGroupSizeInUnits" }, ".sys/ds_storage_pools" },
        {{ "AvailableGroupsToCreate", "AvailableSizeToCreate", "PDiskFilter", "ErasureSpecies", "CurrentGroupsCreated", "CurrentAllocatedSize", "CurrentAvailableSize" }, ".sys/ds_storage_stats" },
        {{ "DiskSpace", "State", "NodeId", "PDiskId", "VSlotId", "GroupId", "GroupGeneration", "FailDomain", "VDisk", "AllocatedSize", "AvailableSize", "Status", "Kind", "FailRealm", "Replicated" }, ".sys/ds_vslots" },
        {{ "TabletId", "FollowerId", "Type", "State", "VolatileState", "BootState", "Generation", "NodeId", "CPU", "Memory", "Network" }, ".sys/hive_tablets" },
        {{ "NodeId", "Address", "Host", "Port", "StartTime", "UpTime", "CpuThreads", "CpuUsage", "CpuIdle" }, ".sys/nodes" },
        {{ "OwnerId", "PathId", "PartIdx", "DataSize", "RowCount", "IndexSize", "CPUCores", "TabletId", "Path", "NodeId", "StartTime", "AccessTime", "UpdateTime", "InFlightTxCount", "RowUpdates", "RowDeletes", "RowReads", "RangeReads", "RangeReadRows", "ImmediateTxCompleted", "CoordinatedTxCompleted", "TxRejectedByOverload", "TxRejectedByOutOfStorage", "LastTtlRunTime", "LastTtlRowsProcessed", "LastTtlRowsErased", "FollowerId", "LocksAcquired", "LocksWholeShard", "LocksBroken", "TxCompleteLag" }, ".sys/partition_stats" },
        {{ "oid", "relacl", "relallvisible", "relam", "relchecks", "relfilenode", "relforcerowsecurity", "relfrozenxid", "relhasindex", "relhasrules", "relhassubclass", "relhastriggers", "relispartition", "relispopulated", "relisshared", "relkind", "relminmxid", "relname", "relnamespace", "relnatts", "reloftype", "reloptions", "relowner", "relpages", "relpartbound", "relpersistence", "relreplident", "relrewrite", "relrowsecurity", "reltablespace", "reltoastrelid", "reltuples", "reltype" }, ".sys/pg_class" },
        {{ "hasindexes", "hasrules", "hastriggers", "rowsecurity", "schemaname", "tablename", "tableowner", "tablespace" }, ".sys/pg_tables", true},
        {{ "IntervalEnd", "Rank", {"QueryText", true}, "Count", "SumCPUTime", "MinCPUTime", "MaxCPUTime", "SumDuration", "MinDuration", "MaxDuration", "MinReadRows", "MaxReadRows", "SumReadRows", "MinReadBytes", "MaxReadBytes", "SumReadBytes", "MinUpdateRows", "MaxUpdateRows", "SumUpdateRows", "MinUpdateBytes", "MaxUpdateBytes", "SumUpdateBytes", "MinDeleteRows", "MaxDeleteRows", "SumDeleteRows", "MinRequestUnits", "MaxRequestUnits", "SumRequestUnits", "LocksBrokenAsBreaker", "LocksBrokenAsVictim" }, ".sys/query_metrics_one_minute" },
        {{ "SessionId", "NodeId", "State", {"Query", true}, "QueryCount", "ClientAddress", "ClientPID", "ClientUserAgent", "ClientSdkBuildInfo", "ApplicationName", "SessionStartAt", "QueryStartAt", "StateChangeAt", "UserSID" }, ".sys/query_sessions" },
        {{ "Name", "Rank", "MemberName", "ResourcePool" }, ".sys/resource_pool_classifiers" },
        {{ "Name", "ConcurrentQueryLimit", "QueueSize", "DatabaseLoadCpuThreshold", "ResourceWeight", "TotalCpuLimitPercentPerNode", "QueryCpuLimitPercentPerNode", "QueryMemoryLimitPercentPerNode" }, ".sys/resource_pools" },
        {{ "Path", "Status", "Issues", "Plan", "Ast", "Text", "Run", "ResourcePool", "RetryCount", "LastFailAt", "SuspendedUntil", "LastExecutionId", "PreviousExecutionIds" }, ".sys/streaming_queries" },
        {{ "commit_action", "is_insertable_into", "is_typed", "reference_generation", "self_referencing_column_name", "table_catalog", "table_name", "table_schema", "table_type", "user_defined_type_catalog", "user_defined_type_name", "user_defined_type_schema" }, ".sys/tables" },
        {{ "IntervalEnd", "Rank", "TabletId", "Path", "LocksAcquired", "LocksWholeShard", "LocksBroken", "NodeId", "DataSize", "RowCount", "IndexSize", "FollowerId" }, ".sys/top_partitions_by_tli_one_hour" },
        {{ "IntervalEnd", "Rank", "TabletId", "Path", "LocksAcquired", "LocksWholeShard", "LocksBroken", "NodeId", "DataSize", "RowCount", "IndexSize", "FollowerId" }, ".sys/top_partitions_by_tli_one_minute" },
        {{ "IntervalEnd", "Rank", "TabletId", "Path", "PeakTime", "CPUCores", "NodeId", "DataSize", "RowCount", "IndexSize", "InFlightTxCount", "FollowerId" }, ".sys/top_partitions_one_hour" },
        {{ "IntervalEnd", "Rank", "TabletId", "Path", "PeakTime", "CPUCores", "NodeId", "DataSize", "RowCount", "IndexSize", "InFlightTxCount", "FollowerId" }, ".sys/top_partitions_one_minute" },
        {TopQueryColumns, ".sys/top_queries_by_cpu_time_one_hour" },
        {TopQueryColumns, ".sys/top_queries_by_cpu_time_one_minute" },
        {TopQueryColumns, ".sys/top_queries_by_duration_one_hour" },
        {TopQueryColumns, ".sys/top_queries_by_duration_one_minute" },
        {TopQueryColumns, ".sys/top_queries_by_read_bytes_one_hour" },
        {TopQueryColumns, ".sys/top_queries_by_read_bytes_one_minute" },
        {TopQueryColumns, ".sys/top_queries_by_request_units_one_hour" },
        {TopQueryColumns, ".sys/top_queries_by_request_units_one_minute" },
    };
    ui32 QueryIdx = 0;

    ui32 CountersSize = 0;
    TVector<TVector<std::pair<TString, TInstant>>> Counters;
    TVector<TEvInterconnect::TNodeInfo> Nodes;
    NKikimrClusterStateInfoProto::TClusterStateInfo State;
    TInstant Started;
    TDuration Duration;
    TDuration Period;
    bool Sanitize;
    bool CountersOnly;
    TActorId Pipe;

    void SendRequest(ui32 i) {
        ui32 nodeId = Nodes[i].NodeId;
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
#define request(NAME) \
        Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::NAME(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, i); \
        NodeRequested[i]++;

        request(TEvVDiskStateRequest);
        request(TEvPDiskStateRequest);
        request(TEvTabletStateRequest);
        request(TEvBSGroupStateRequest);
        request(TEvSystemStateRequest);
        request(TEvNodeStateRequest);
#undef request
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext &ctx) {
        Nodes = ev->Get()->Nodes;
        CountersOnly = GetProtoRequest()->counters_only();
        if (!CountersOnly) {
            RequestSession();
            RequestHealthCheck();
            RequestBaseConfig();
            RequestStorageConfig();
            RequestStateStorageConfig();
            RequestSentinelState(ctx);
        }
        NodeReceived.resize(Nodes.size());
        NodeRequested.resize(Nodes.size());
        for (ui32 i : xrange(Nodes.size())) {
            const auto& ni = Nodes[i];
            auto* node = State.AddNodeInfos();
            node->SetNodeId(ni.NodeId);
            node->SetHost(ni.Host);
            node->SetPort(ni.Port);
            node->SetLocation(ni.Location.ToString());
            if (!CountersOnly) {
                SendRequest(i);
            }
        }
        Counters.resize(Nodes.size());
        RequestCounters();
        Period = TDuration::Seconds(GetProtoRequest()->period_seconds());
        if (Period > TDuration::Zero()) {
            Schedule(Period, new TEvents::TEvWakeup());
        }
        if (NodeRequested.size() > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndPassAway();
        }
    }

    void RequestSession() {
        auto kqpProxyId = NKqp::MakeKqpProxyID(SelfId().NodeId());
        auto remoteRequest = std::make_unique<NKqp::TEvKqp::TEvCreateSessionRequest>();
        remoteRequest->Record.MutableRequest()->SetDatabase("/Root");
        ++Requested;
        Send(kqpProxyId, remoteRequest.release());
    }

    void CloseSession() {
        auto kqpProxyId = NKqp::MakeKqpProxyID(SelfId().NodeId());
        auto remoteRequest = std::make_unique<NKqp::TEvKqp::TEvCloseSessionRequest>();
        remoteRequest->Record.MutableRequest()->SetSessionId(SessionId);
        Send(kqpProxyId, remoteRequest.release());
    }

    void DoQueryRequest() {
        if (QueryIdx >= Queries.size()) {
            CheckReply();
            return;
        }

        auto request = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetDatabase("/Root");
        SetAuthToken(request, *Request);
        request->Record.MutableRequest()->SetSessionId(SessionId);
        ActorIdToProto(SelfId(), request->Record.MutableRequestActorId());
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(Queries[QueryIdx].ToSelect(Sanitize));
        request->Record.MutableRequest()->SetKeepSession(true);
        request->Record.MutableRequest()->MutableTxControl()->Mutablebegin_tx()->Mutablestale_read_only();
        ++Requested;
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), request.release());
    }
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr ev) {
        ++Received;
        auto record = ev->Get()->Record;
        SessionId = record.GetResponse().GetSessionId();
        DoQueryRequest();
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr ev) {
        auto record = ev->Get()->Record;
        auto* q = State.AddQueries();
        q->SetTableName(Queries[QueryIdx].TableName);
        q->SetQuery(Queries[QueryIdx].ToSelect(Sanitize));
        q->MutableResponse()->CopyFrom(record.GetResponse());
        ++Received;
        ++QueryIdx;
        while (QueryIdx < Queries.size() && Queries[QueryIdx].Sensitive && Sanitize) {
            QueryIdx++;
        }
        CloseSession();
        RequestSession();
    }

    void RequestBaseConfig() {
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new NKikimr::NStorage::TEvNodeWardenQueryBaseConfig);
        Requested++;
    }

    void RequestStorageConfig() {
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new NKikimr::TEvNodeWardenQueryStorageConfig(/*subscribe=*/false));
        Requested++;
    }

    void RequestStateStorageConfig() {
        auto request = std::make_unique<NKikimr::NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableGetStateStorageConfig()->SetNodesState(true);
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
        Requested++;
    }

    void RequestSentinelState(const TActorContext &ctx) {
        auto request = std::make_unique<NKikimr::NCms::TEvCms::TEvGetSentinelStateRequest>();
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 10};
        Pipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, MakeCmsID(), pipeConfig));

        NTabletPipe::SendData(ctx, Pipe, request.release());
        Requested++;
    }

    void HandleResult(NKikimr::NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        State.MutableStateStorageConfig()->CopyFrom(ev->Get()->Record.GetStateStorageConfig());
        ++Received;
        CheckReply();
    }

    void HandleResult(NKikimr::NCms::TEvCms::TEvGetSentinelStateResponse::TPtr& ev) {
        State.MutableSentinelState()->CopyFrom(ev->Get()->Record);
        ++Received;
        CheckReply();
    }

    void Handle(NKikimr::NStorage::TEvNodeWardenBaseConfig::TPtr ev) {
        State.MutableBaseConfig()->CopyFrom(ev->Get()->BaseConfig);
        ++Received;
        CheckReply();
    }

    void Handle(NKikimr::TEvNodeWardenStorageConfig::TPtr ev) {
        State.MutableStorageConfig()->CopyFrom(*ev->Get()->Config);
        State.MutableBridgeClusterState()->CopyFrom(ev->Get()->Config->GetClusterState());
        State.MutableBridgeClusterStateDetails()->CopyFrom(ev->Get()->Config->GetClusterStateDetails());
        ++Received;
        CheckReply();
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        ui32 nodeId = ev->Get()->NodeId;
        for (ui32 i : xrange(Nodes.size())) {
            if (Nodes[i].NodeId == nodeId) {
                NodeReceived[i] = NodeRequested[i];
                if (CountersInflight > 0) {
                    CountersInflight--;
                }
                CheckReply();
                return;
            }
        }
    }

#define HandleWhiteboard(NAME, INFO) \
    void Handle(NNodeWhiteboard::TEvWhiteboard::NAME::TPtr& ev) { \
        ui32 idx = ev.Get()->Cookie; \
        State.MutableNodeInfos(idx)->Mutable##INFO()->CopyFrom(ev->Get()->Record); \
        NodeStateInfoReceived(idx); \
    }

    HandleWhiteboard(TEvVDiskStateResponse, VDiskInfo)
    HandleWhiteboard(TEvPDiskStateResponse, PDiskInfo)
    HandleWhiteboard(TEvTabletStateResponse, TabletInfo)
    HandleWhiteboard(TEvBSGroupStateResponse, BSGroupInfo)
    HandleWhiteboard(TEvSystemStateResponse, SystemInfo)
    HandleWhiteboard(TEvNodeStateResponse, NodeStateInfo)

    void Handle(NKikimr::NCountersInfo::TEvCountersInfoResponse::TPtr& ev) {
        ui32 idx = ev.Get()->Cookie;
        auto& response = ev->Get()->Record.GetResponse();
        Counters[idx].push_back(std::make_pair(std::move(response), TInstant::Now()));
        CountersSize += response.size();
        if (CountersSize > MaxCountersSize) {
            ReplyAndPassAway();
        }
        CountersInflight--;
        RequestCounters();
        NodeStateInfoReceived(idx);
    }

    void NodeStateInfoReceived(ui32 idx) {
        NodeReceived[idx]++;
        CheckReply();
    }
    void CheckReply() {
        if (Period > TDuration::Zero() || Received < Requested) {
            return;
        }
        for (ui32 i : xrange(NodeRequested.size())) {
            if (NodeReceived[i] < NodeRequested[i]) {
                return;
            }
        }
        ReplyAndPassAway();
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        State.MutableSelfCheck()->CopyFrom(ev->Get()->Result);
        ++Received;
        CheckReply();
    }

    void RequestHealthCheck() {
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        Send(NHealthCheck::MakeHealthCheckID(), request.Release());
        ++Requested;
    }

    void Bootstrap() {
        MaxCountersSize = AppData()->ClusterDiagnosticsConfig.GetMaxCountersSize();
        CountersMaxInflight = AppData()->ClusterDiagnosticsConfig.GetCountersMaxInflight();
        constexpr ui32 defaultDurationSec = 60;
        const TActorId nameserviceId = GetNameserviceActorId();
        Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        TBase::Become(&TThis::StateRequestedBrowse);
        Sanitize = !GetProtoRequest()->no_sanitize();
        Duration = TDuration::Seconds(GetProtoRequest()->duration_seconds() ? GetProtoRequest()->duration_seconds() : defaultDurationSec);
        Started = TInstant::Now();
        Schedule(Duration, new TEvents::TEvWakeup());
    }

    void RequestCounters() {
        if (CountersNodeToRequest == 0) {
            if (CountersNextBlock) {
                CountersNextBlock = false;
            } else {
                return;
            }
        }
        for (; CountersNodeToRequest < Nodes.size() && CountersInflight < CountersMaxInflight; ++CountersNodeToRequest) {
            const auto& ni = Nodes[CountersNodeToRequest];
            TActorId countersInfoProviderServiceId = NKikimr::NCountersInfo::MakeCountersInfoProviderServiceID(ni.NodeId);
            Send(countersInfoProviderServiceId, new NKikimr::NCountersInfo::TEvCountersInfoRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, CountersNodeToRequest);
            NodeRequested[CountersNodeToRequest]++;
            CountersInflight++;
        }
        if (CountersNodeToRequest >= Nodes.size()) {
            CountersNodeToRequest = 0;
        }

    }
    void Wakeup() {
        if (Period > TDuration::Zero()) {
            CountersNextBlock = true;
            RequestCounters();
            Schedule(Period, new TEvents::TEvWakeup());
        }
        if (TInstant::Now() - Started >= Duration) {
            ReplyAndPassAway();
        }
    }

    void Die(const TActorContext& ctx) override {
        for (const auto& ni : Nodes) {
            ctx.Send(TActivationContext::InterconnectProxy(ni.NodeId), new TEvents::TEvUnsubscribe());
        }
        NTabletPipe::CloseClient(ctx, Pipe);
        TBase::Die(ctx);
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            cFunc(TEvents::TSystem::Wakeup, Wakeup);
        }
    }

    STFUNC(StateRequestedNodeInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle)
            hFunc(NKikimr::NStorage::TEvNodeWardenBaseConfig, Handle);
            hFunc(NKikimr::TEvNodeWardenStorageConfig, Handle);
            hFunc(NKikimr::NCountersInfo::TEvCountersInfoResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            cFunc(TEvents::TSystem::Wakeup, Wakeup);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            hFunc(NKikimr::NStorage::TEvNodeConfigInvokeOnRootResult, HandleResult);
            hFunc(NKikimr::NCms::TEvCms::TEvGetSentinelStateResponse, HandleResult);
        }
    }

    TString Pack(const TString& data) {
        TString dataPack;
        TStringOutput output(dataPack);
        TZstdCompress compress(&output);
        compress.Write(data);
        compress.Finish();
        return dataPack;
    }

    void AddBlock(Ydb::Monitoring::ClusterStateResult& result, const TString& name, const auto& obj) {
        google::protobuf::util::JsonPrintOptions jsonOpts;
        jsonOpts.add_whitespace = true;
        TString data;
        google::protobuf::util::MessageToJsonString(obj, &data, jsonOpts);
        auto* block = result.Addblocks();
        block->Setname(name);
        block->Setcontent(Pack(data));
        block->Mutabletimestamp()->set_seconds(TInstant::Now().Seconds());
    }

    void ReplyAndPassAway() {
        CloseSession();
        TResponse response;
        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::SUCCESS);

        Ydb::Monitoring::ClusterStateResult result;

        if (!CountersOnly) {
            AddBlock(result, "cluster_state", State);
        }
        for (ui32 node : xrange(Counters.size())) {
            for (ui32 i : xrange(Counters[node].size())) {
                auto* counterBlock = result.Addblocks();
                TStringBuilder sb;
                auto nodeId = Nodes[node].NodeId;
                sb << "node_" << nodeId << "_counters";
                if (Counters[node].size() > 1) {
                    sb << "_" << i;
                }
                counterBlock->Setname(sb);
                counterBlock->Setcontent(Counters[node][i].first);
                counterBlock->Mutabletimestamp()->set_seconds(Counters[node][i].second.Seconds());
            }
        }
        operation.mutable_result()->PackFrom(result);
        return Reply(response);
    }
};

void DoClusterStateRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TClusterStateRPC(p.release()));
}
} // namespace NGRpcService
} // namespace NKikimr
