#pragma once

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>

#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>

namespace NKikimrTxDataShard {
    class TKqpTransaction_TDataTaskMeta_TKeyRange;
    class TKqpTransaction_TScanTaskMeta_TReadOpMeta;
    class TKqpReadRangesSourceSettings;
}

namespace NKikimr::NKqp {

class TPartitionPruner;
struct TPartitionPrunerConfig;
struct TQueryExecutionStats;

struct TTransaction : private TMoveOnly {
    NYql::NNodes::TKqpPhysicalTx Node;
    TQueryData::TPtr Params;

    inline TTransaction(const NYql::NNodes::TKqpPhysicalTx& node, TQueryData::TPtr params)
        : Node(node)
        , Params(std::move(params)) {}
};

struct TColumnShardHashV1Params {
    ui64 SourceShardCount = 0;
    std::shared_ptr<TVector<NScheme::TTypeInfo>> SourceTableKeyColumnTypes = nullptr;
    std::shared_ptr<TVector<ui64>> TaskIndexByHash = nullptr; // hash belongs [0; ShardCount]

    TColumnShardHashV1Params DeepCopy() const {
        TColumnShardHashV1Params copy;
        copy.SourceShardCount = SourceShardCount;

        if (SourceTableKeyColumnTypes) {
            copy.SourceTableKeyColumnTypes = std::make_shared<TVector<NScheme::TTypeInfo>>(*SourceTableKeyColumnTypes);
        } else {
            copy.SourceTableKeyColumnTypes = nullptr;
        }

        if (TaskIndexByHash) {
            copy.TaskIndexByHash = std::make_shared<TVector<ui64>>(*TaskIndexByHash);
        } else {
            copy.TaskIndexByHash = nullptr;
        }

        return copy;
    }

    TString KeyTypesToString() const {
        if (SourceTableKeyColumnTypes == nullptr) {
            return "[ NULL ]";
        }

        const auto& keyColumnTypes = *SourceTableKeyColumnTypes;
        TVector<TString> stringNames;
        stringNames.reserve(keyColumnTypes.size());
        for (const auto& keyColumnType: keyColumnTypes) {
            stringNames.push_back(NYql::NProto::TypeIds_Name(keyColumnType.GetTypeId()));
        }

        return "[" + JoinSeq(",", stringNames) + "]";
    }
};

struct TShardKeyRanges {
    // ordered ranges and points
    TVector<TSerializedPointOrRange> Ranges;
    std::optional<TSerializedTableRange> FullRange;

    void AddPoint(TSerializedCellVec&& point);
    void AddRange(TSerializedTableRange&& range);
    void Add(TSerializedPointOrRange&& pointOrRange);

    void CopyFrom(const TVector<TSerializedPointOrRange>& ranges);

    void MakeFullRange(TSerializedTableRange&& range);
    void MakeFullPoint(TSerializedCellVec&& range);
    void MakeFull(TSerializedPointOrRange&& pointOrRange);

    bool HasRanges() const;

    bool IsFullRange() const { return FullRange.has_value(); }
    TVector<TSerializedPointOrRange>& GetRanges() { return Ranges; }

    void MergeWritePoints(TShardKeyRanges&& other, const TVector<NScheme::TTypeInfo>& keyTypes);

    TString ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange* proto) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta_TReadOpMeta* proto) const;
    void SerializeTo(NKikimrTxDataShard::TKqpReadRangesSourceSettings* proto, bool allowPoints = true) const;

    std::pair<const TSerializedCellVec*, bool> GetRightBorder() const;
};

struct TStageInfoMeta {
    const IKqpGateway::TPhysicalTxData& Tx;

    TTableId TableId;
    TString TablePath;
    ETableKind TableKind;
    TIntrusiveConstPtr<TTableConstInfo> TableConstInfo;
    TIntrusiveConstPtr<NKikimr::NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo> ColumnTableInfoPtr;
    std::optional<NKikimrKqp::TKqpTableSinkSettings> ResolvedSinkSettings; // CTAS only

    TVector<bool> SkipNullKeys;

    THashSet<TKeyDesc::ERowOperation> ShardOperations;
    THolder<TKeyDesc> ShardKey;
    NSchemeCache::ETableKind ShardKind = NSchemeCache::ETableKind::KindUnknown;

    struct TIndexMeta {
        TTableId TableId;
        TString TablePath;
        TIntrusiveConstPtr<TTableConstInfo> TableConstInfo;

        THolder<TKeyDesc> ShardKey;
    };

    TVector<TIndexMeta> IndexMetas;

    ////////////////////////////////////////////////////////////////////////////////////////////////

    TColumnShardHashV1Params ColumnShardHashV1Params;
    THashMap<ui32, TColumnShardHashV1Params> HashParamsByOutput;

    TColumnShardHashV1Params& GetColumnShardHashV1Params(ui32 outputIdx) {
        if (!HashParamsByOutput.contains(outputIdx)) {
            HashParamsByOutput[outputIdx] = ColumnShardHashV1Params.DeepCopy();
        }
        return HashParamsByOutput[outputIdx];
    }

    const TColumnShardHashV1Params& GetColumnShardHashV1Params(ui32 outputIdx) const {
        if (HashParamsByOutput.contains(outputIdx)) {
            return HashParamsByOutput.at(outputIdx);
        }
        return ColumnShardHashV1Params;
    }

    /*
     * We want to propogate params for hash func through the stages. In default sutiation we do it by only ColumnShardHashV1Params.
     * But challenges appear when there is CTE in plan. So we must store mapping from the outputStageIdx to params.
     * Otherwise, we will rewrite ColumnShardHashV1Params, when we will meet the same stage again during propogation.
     */

    ////////////////////////////////////////////////////////////////////////////////////////////////

    const NKqpProto::TKqpPhyStage& GetStage(const size_t idx) const {
        auto& txBody = Tx.Body;
        YQL_ENSURE(idx < txBody->StagesSize());
        return txBody->GetStages(idx);
    }

    template <class TStageIdExt>
    const NKqpProto::TKqpPhyStage& GetStage(const TStageIdExt& stageId) const {
        return GetStage(stageId.StageId);
    }

    bool HasReads() const {
        return ShardOperations.contains(TKeyDesc::ERowOperation::Read);
    }

    bool HasWrites() const {
        return ShardOperations.contains(TKeyDesc::ERowOperation::Update) ||
            ShardOperations.contains(TKeyDesc::ERowOperation::Erase);
    }

    explicit TStageInfoMeta(const IKqpGateway::TPhysicalTxData& tx)
        : Tx(tx)
        , TableKind(ETableKind::Unknown)
    {}

    bool IsDatashard() const {
        return TableKind == ETableKind::Datashard;
    }

    bool IsSysView() const {
        if (!ShardKey) {
            return false;
        }
        YQL_ENSURE((TableKind == ETableKind::SysView) == ShardKey->IsSystemView());
        return TableKind == ETableKind::SysView;
    }

    bool IsOlap() const {
        return TableKind == ETableKind::Olap;
    }

};

// things which are common for all tasks in the graph.
struct TGraphMeta {
    bool IsRestored = false;
    IKqpGateway::TKqpSnapshot Snapshot;
    TMaybe<ui64> LockTxId;
    ui64 TxId;
    ui32 LockNodeId = 0;
    NKikimrKqp::EIsolationLevel RequestIsolationLevel;
    TMaybe<NKikimrDataEvents::ELockMode> LockMode;
    std::unordered_map<ui64, TActorId> ResultChannelProxies;
    TActorId ExecuterId;
    bool UseFollowers = false;
    bool AllowInconsistentReads = false;
    bool AllowWithSpilling = false;
    bool SinglePartitionOptAllowed = false;
    bool LocalComputeTasks = false;
    bool MayRunTasksLocally = false;
    TIntrusivePtr<TProtoArenaHolder> Arena;
    TString Database;
    NKikimrConfig::TTableServiceConfig::EChannelTransportVersion ChannelTransportVersion;
    TIntrusivePtr<NKikimr::NKqp::TUserRequestContext> UserRequestContext;
    bool CreateSuspended = false;
    bool CheckDuplicateRows = false;
    bool ShardsResolved = false;
    TMaybe<ui64> MaxBatchSize;
    bool UnknownAffectedShardCount = false; // used by Data executer
    TMap<ui64, ui64> ShardIdToNodeId;
    std::map<TString, TString> SecureParams;
    bool AllowOlapDataQuery = true; // used by Data executer - always true for Scan executer
    bool StreamResult = false;

    const TIntrusivePtr<TProtoArenaHolder>& GetArenaIntrusivePtr() const {
        return Arena;
    }

    template<typename TMessage>
    TMessage* Allocate() {
        return Arena->Allocate<TMessage>();
    }

    void SetSnapshot(ui64 step, ui64 txId) {
        Snapshot = IKqpGateway::TKqpSnapshot(step, txId);
    }

    void SetLockTxId(TMaybe<ui64> lockTxId) {
        LockTxId = lockTxId;
    }

    void SetLockNodeId(ui32 lockNodeId) {
        LockNodeId = lockNodeId;
    }

    void SetLockMode(NKikimrDataEvents::ELockMode lockMode) {
        LockMode = lockMode;
    }
};

struct TTaskInputMeta {
    // these message are allocated using the protobuf arena.
    NKikimrTxDataShard::TKqpReadRangesSourceSettings* SourceSettings = nullptr;
    NKikimrKqp::TKqpStreamLookupSettings* StreamLookupSettings = nullptr;
    NKikimrKqp::TKqpSequencerSettings* SequencerSettings = nullptr;
    NKikimrTxDataShard::TKqpVectorResolveSettings* VectorResolveSettings = nullptr;
};

struct TTaskOutputMeta {
    NKikimrKqp::TKqpTableSinkSettings* SinkSettings = nullptr;
    THashMap<ui64, const TKeyDesc::TPartitionInfo*> ShardPartitions;
};

struct TTaskMeta {
private:
    YDB_OPT(bool, EnableShardsSequentialScan);
public:
    ui64 ShardId = 0; // only in case of non-scans (data-query & legacy scans)
    ui64 NodeId = 0;  // only in case of scans over persistent snapshots
    bool ScanTask = false;
    TActorId ExecuterId;
    ui32 Type = Unknown;

    TActorId ResultChannelActorId;
    bool Completed = false;
    THashMap<TString, TString> TaskParams; // Params for sources/sinks
    TVector<TString> ReadRanges; // Partitioning for sources
    THashMap<TString, TString> SecureParams;

    enum TTaskType : ui32 {
        Unknown = 0,
        Compute = 1,
        Scan = 2,
        DataShard = 3,
    };

    struct TColumn {
        ui32 Id = 0;
        NScheme::TTypeInfo Type;
        TString TypeMod;
        TString Name;
        bool NotNull;
        bool IsPrimary = false;
    };

    struct TColumnWrite {
        TColumn Column;
        ui32 MaxValueSizeBytes = 0;
    };

    struct TShardReadInfo {
        TShardKeyRanges Ranges;
        TVector<TColumn> Columns;
        ui64 ShardId = 0; // in case of persistent scans
    };

    struct TKqpOlapProgram {
        TString Program; // For OLAP scans with process pushdown
        std::set<TString> ParameterNames;
    };

    struct TReadInfo: public NYql::TSortingOperator<NYql::ERequestSorting::NONE> {
    public:
        enum class EReadType {
            Rows,
            Blocks
        };
        ui64 ItemsLimit = 0;
        EReadType ReadType = EReadType::Rows;
        TKqpOlapProgram OlapProgram;
        TVector<NScheme::TTypeInfo> ResultColumnsTypes;
        std::vector<std::string> GroupByColumnNames;
    };

    struct TWriteInfo {
        ui64 UpdateOps = 0;
        ui64 EraseOps = 0;

        TShardKeyRanges Ranges;
        THashMap<ui32, TColumnWrite> ColumnWrites;

        void AddUpdateOp() {
            ++UpdateOps;
        }

        void AddEraseOp() {
            ++EraseOps;
        }

        bool IsPureEraseOp() const {
            return (EraseOps > 0) && (UpdateOps == 0);
        }
    };

    TReadInfo ReadInfo;
    TMaybe<TVector<TShardReadInfo>> Reads; // if not set -> no reads
    TMaybe<TWriteInfo> Writes;             // if not set -> no writes

    TString ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
};

using TStageInfo = NYql::NDq::TStageInfo<TStageInfoMeta>;
using TTaskOutput = NYql::NDq::TTaskOutput<TTaskOutputMeta>;
using TTaskOutputType = NYql::NDq::TTaskOutputType;
using TTaskInput = NYql::NDq::TTaskInput<TTaskInputMeta>;
using TTask = NYql::NDq::TTask<TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta>;

class TKqpTasksGraph : public NYql::NDq::TDqTasksGraph<TGraphMeta, TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta> {
public:
    explicit TKqpTasksGraph(const NKikimr::NKqp::TTxAllocatorState::TPtr& txAlloc,
        const TPartitionPrunerConfig& partitionPrunerConfig,
        const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregationSettings,
        const TKqpRequestCounters::TPtr& counters,
        TActorId bufferActorId
    );

    size_t BuildAllTasks(bool isScan, bool limitTasksPerNode, std::optional<TLlvmSettings> llvmSettings,
        const TVector<IKqpGateway::TPhysicalTxData>& transactions,
        const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot,
        bool collectProfileStats, TQueryExecutionStats* stats,
        size_t nodesCount, THashSet<ui64>* ShardsWithEffects
    );

    void BuildSysViewScanTasks(TStageInfo& stageInfo);
    bool BuildComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount); // returns true if affected shards count is unknown
    void BuildDatashardTasks(TStageInfo& stageInfo, THashSet<ui64>* shardsWithEffects); // returns shards with effects
    void BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination, const TMap<ui64, ui64>& shardIdToNodeId, TQueryExecutionStats* stats);
    void BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount);
    TMaybe<size_t> BuildScanTasksFromSource(TStageInfo& stageInfo, bool limitTasksPerNode, const TMap<ui64, ui64>& shardIdToNodeId, TQueryExecutionStats* stats);

    void FillKqpTasksGraphStages(const TVector<IKqpGateway::TPhysicalTxData>& txs);
    void BuildKqpTaskGraphResultChannels(const TKqpPhyTxHolder::TConstPtr& tx, ui64 txIdx);
    void BuildKqpStageChannels(TStageInfo& stageInfo, ui64 txId, bool enableSpilling, bool enableShuffleElimination);

    NYql::NDqProto::TDqTask* ArenaSerializeTaskToProto(const TTask& task, bool serializeAsyncIoSettings);
    void PersistTasksGraphInfo(NKikimrKqp::TQueryPhysicalGraph& result) const;
    void RestoreTasksGraphInfo(const NKikimrKqp::TQueryPhysicalGraph& graphInfo);

    void FillChannelDesc(NYql::NDqProto::TChannel& channelDesc, const NYql::NDq::TChannel& channel,
        const NKikimrConfig::TTableServiceConfig::EChannelTransportVersion chanTransportVersion, bool enableSpilling) const;
    bool IsCrossShardChannel(const NYql::NDq::TChannel& channel) const;

    void UpdateRemoteTasksNodeId(const THashMap<ui64, TVector<ui64>>& remoteComputeTasks);

public:
    static constexpr ui64 PriorityTxShift = 32;
    THolder<TPartitionPruner> PartitionPruner; // TODO: temporary public

private:
    void BuildTransformChannels(const NYql::NDq::TTransform& transform, const TTaskInputMeta& meta, const TString& name,
        const TStageInfo& stageInfo, ui32 inputIndex,
        const TStageInfo& inputStageInfo, ui32 outputIndex, bool enableSpilling, const NYql::NDq::TChannelLogFunc& logFunc);
    void BuildSequencerChannels(const TStageInfo& stageInfo, ui32 inputIndex,
        const TStageInfo& inputStageInfo, ui32 outputIndex,
        const NKqpProto::TKqpPhyCnSequencer& sequencer, bool enableSpilling, const NYql::NDq::TChannelLogFunc& logFunc);
    void BuildChannelBetweenTasks(const TStageInfo& stageInfo, const TStageInfo& inputStageInfo, ui64 originTaskId,
        ui64 targetTaskId, ui32 inputIndex, ui32 outputIndex, bool enableSpilling, const NYql::NDq::TChannelLogFunc& logFunc);
    void BuildParallelUnionAllChannels(const TStageInfo& stageInfo, ui32 inputIndex,
        const TStageInfo& inputStageInfo, ui32 outputIndex, bool enableSpilling, const NYql::NDq::TChannelLogFunc& logFunc, ui64& nextOriginTaskId);
    void BuildStreamLookupChannels(const TStageInfo& stageInfo, ui32 inputIndex,
        const TStageInfo& inputStageInfo, ui32 outputIndex,
        const NKqpProto::TKqpPhyCnStreamLookup& streamLookup, bool enableSpilling, const NYql::NDq::TChannelLogFunc& logFunc);
    void BuildVectorResolveChannels(const TStageInfo& stageInfo, ui32 inputIndex,
        const TStageInfo& inputStageInfo, ui32 outputIndex,
        const NKqpProto::TKqpPhyCnVectorResolve& vectorResolve, bool enableSpilling, const NYql::NDq::TChannelLogFunc& logFunc);
    void BuildDqSourceStreamLookupChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo,
        ui32 outputIndex, const NKqpProto::TKqpPhyCnDqSourceStreamLookup& dqSourceStreamLookup, const NYql::NDq::TChannelLogFunc& logFunc);

    void FillOutputDesc(NYql::NDqProto::TTaskOutput& outputDesc, const TTaskOutput& output, ui32 outputIdx,
        bool enableSpilling, const TStageInfo& stageInfo) const;
    void FillInputDesc(NYql::NDqProto::TTaskInput& inputDesc, const TTaskInput& input,
        bool serializeAsyncIoSettings, bool& enableMetering) const;

    void SerializeTaskToProto(const TTask& task, NYql::NDqProto::TDqTask* result, bool serializeAsyncIoSettings) const;

    ui32 GetMaxTasksAggregation(TStageInfo& stageInfo, const ui32 previousTasksCount, const ui32 nodesCount);
    ui32 GetScanTasksPerNode(TStageInfo& stageInfo, const bool isOlapScan, const ui64 nodeId, bool enableShuffleElimination = false) const;

    void FillSecureParamsFromStage(THashMap<TString, TString>& secureParams, const NKqpProto::TKqpPhyStage& stage) const;

    void BuildExternalSinks(const NKqpProto::TKqpSink& sink, TKqpTasksGraph::TTaskType& task) const;
    void BuildInternalSinks(const NKqpProto::TKqpSink& sink, const TStageInfo& stageInfo, TKqpTasksGraph::TTaskType& task) const;
    void BuildSinks(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, TKqpTasksGraph::TTaskType& task) const;

private:
    NKikimr::NKqp::TTxAllocatorState::TPtr TxAlloc;
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig AggregationSettings;
    TKqpRequestCounters::TPtr Counters;
    TActorId BufferActorId; // TODO: not sure if it belongs here
};

void FillTableMeta(const TStageInfo& stageInfo, NKikimrTxDataShard::TKqpTransaction_TTableMeta* meta);

template<typename Proto>
TVector<TTaskMeta::TColumn> BuildKqpColumns(const Proto& op, TIntrusiveConstPtr<TTableConstInfo> tableInfo) {
    TVector<TTaskMeta::TColumn> columns;
    columns.reserve(op.GetColumns().size());

    THashSet<TString> keyColumns;
    for (auto column : tableInfo->KeyColumns) {
        keyColumns.insert(std::move(column));
    }

    for (const auto& column : op.GetColumns()) {
        TTaskMeta::TColumn c;

        const auto& tableColumn = tableInfo->Columns.at(column.GetName());
        c.Id = column.GetId();
        c.Type = tableColumn.Type;
        c.TypeMod = tableColumn.TypeMod;
        c.Name = column.GetName();
        c.NotNull = tableColumn.NotNull;
        c.IsPrimary = keyColumns.contains(c.Name);

        columns.emplace_back(std::move(c));
    }

    return columns;
}

struct TKqpTaskOutputType {
    enum : ui32 {
        ShardRangePartition = TTaskOutputType::COMMON_TASK_OUTPUT_TYPE_END
    };
};

void LogStage(const NActors::TActorContext& ctx, const TStageInfo& stageInfo);

} // namespace NKikimr::NKqp
