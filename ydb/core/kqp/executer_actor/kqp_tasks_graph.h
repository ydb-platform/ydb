#pragma once

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
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

namespace NKikimr {
namespace NKqp {

struct TTransaction : private TMoveOnly {
    NYql::NNodes::TKqpPhysicalTx Node;
    TQueryData::TPtr Params;

    inline TTransaction(const NYql::NNodes::TKqpPhysicalTx& node, TQueryData::TPtr params)
        : Node(node)
        , Params(std::move(params)) {}
};

struct TStageInfoMeta {
    const IKqpGateway::TPhysicalTxData& Tx;

    TTableId TableId;
    TString TablePath;
    ETableKind TableKind;
    TIntrusiveConstPtr<TTableConstInfo> TableConstInfo;
    TIntrusiveConstPtr<NKikimr::NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo> ColumnTableInfoPtr;

    TVector<bool> SkipNullKeys;

    THashSet<TKeyDesc::ERowOperation> ShardOperations;
    THolder<TKeyDesc> ShardKey;
    NSchemeCache::TSchemeCacheRequest::EKind ShardKind = NSchemeCache::TSchemeCacheRequest::EKind::KindUnknown;

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
    IKqpGateway::TKqpSnapshot Snapshot;
    TMaybe<ui64> LockTxId;
    std::unordered_map<ui64, TActorId> ResultChannelProxies;
    TActorId ExecuterId;
    bool UseFollowers = false;
    bool AllowInconsistentReads = false;
    bool AllowWithSpilling = false;
    TIntrusivePtr<TProtoArenaHolder> Arena;
    TString Database;
    NKikimrConfig::TTableServiceConfig::EChannelTransportVersion ChannelTransportVersion;
    TIntrusivePtr<NKikimr::NKqp::TUserRequestContext> UserRequestContext;

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
};

struct TTaskInputMeta {
    // these message are allocated using the protubuf arena.
    NKikimrTxDataShard::TKqpReadRangesSourceSettings* SourceSettings = nullptr;
    NKikimrKqp::TKqpStreamLookupSettings* StreamLookupSettings = nullptr;
    NKikimrKqp::TKqpSequencerSettings* SequencerSettings = nullptr;
};

struct TTaskOutputMeta {
    NKikimrKqp::TKqpTableSinkSettings* SinkSettings = nullptr;
    THashMap<ui64, const TKeyDesc::TPartitionInfo*> ShardPartitions;
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

    bool IsFullRange() const { return FullRange.has_value(); }
    TVector<TSerializedPointOrRange>& GetRanges() { return Ranges; }

    void MergeWritePoints(TShardKeyRanges&& other, const TVector<NScheme::TTypeInfo>& keyTypes);

    TString ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange* proto) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta_TReadOpMeta* proto) const;
    void SerializeTo(NKikimrTxDataShard::TKqpReadRangesSourceSettings* proto) const;

    std::pair<const TSerializedCellVec*, bool> GetRightBorder() const;
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

    struct TReadInfo {
        enum class EReadType {
            Rows,
            Blocks
        };
        ui64 ItemsLimit = 0;
        bool Reverse = false;
        bool Sorted = false;
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
    TMaybe<TVector<TShardReadInfo>> Reads;  // if not set -> no reads
    TMaybe<TWriteInfo> Writes;         // if not set -> no writes

    TString ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
};

using TStageInfo = NYql::NDq::TStageInfo<TStageInfoMeta>;
using TTaskOutput = NYql::NDq::TTaskOutput<TTaskOutputMeta>;
using TTaskOutputType = NYql::NDq::TTaskOutputType;
using TTaskInput = NYql::NDq::TTaskInput<TTaskInputMeta>;
using TTask = NYql::NDq::TTask<TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta>;
using TKqpTasksGraph = NYql::NDq::TDqTasksGraph<TGraphMeta, TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta>;

void FillKqpTasksGraphStages(TKqpTasksGraph& tasksGraph, const TVector<IKqpGateway::TPhysicalTxData>& txs);
void BuildKqpTaskGraphResultChannels(TKqpTasksGraph& tasksGraph, const TKqpPhyTxHolder::TConstPtr& tx, ui64 txIdx);
void BuildKqpStageChannels(TKqpTasksGraph& tasksGraph, const TStageInfo& stageInfo,
    ui64 txId, bool enableSpilling);

NYql::NDqProto::TDqTask* ArenaSerializeTaskToProto(TKqpTasksGraph& tasksGraph, const TTask& task, bool serializeAsyncIoSettings);
void FillTableMeta(const TStageInfo& stageInfo, NKikimrTxDataShard::TKqpTransaction_TTableMeta* meta);
void FillChannelDesc(const TKqpTasksGraph& tasksGraph, NYql::NDqProto::TChannel& channelDesc, const NYql::NDq::TChannel& channel,
    const NKikimrConfig::TTableServiceConfig::EChannelTransportVersion chanTransportVersion, bool enableSpilling);

template<typename Proto>
TVector<TTaskMeta::TColumn> BuildKqpColumns(const Proto& op, TIntrusiveConstPtr<TTableConstInfo> tableInfo) {
    TVector<TTaskMeta::TColumn> columns;
    columns.reserve(op.GetColumns().size());

    for (const auto& column : op.GetColumns()) {
        TTaskMeta::TColumn c;

        const auto& tableColumn = tableInfo->Columns.at(column.GetName());
        c.Id = column.GetId();
        c.Type = tableColumn.Type;
        c.TypeMod = tableColumn.TypeMod;
        c.Name = column.GetName();
        c.NotNull = tableColumn.NotNull;

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

bool IsCrossShardChannel(const TKqpTasksGraph& tasksGraph, const NYql::NDq::TChannel& channel);

} // namespace NKqp
} // namespace NKikimr
