#pragma once

#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>

namespace NKikimrTxDataShard {
class TKqpTransaction_TDataTaskMeta_TKeyRange;
class TKqpTransaction_TScanTaskMeta_TReadOpMeta;
}

namespace NKikimr {
namespace NKqp {

struct TTransaction : private TMoveOnly {
    NYql::NNodes::TKqpPhysicalTx Node;
    TKqpParamsMap Params;

    inline TTransaction(const NYql::NNodes::TKqpPhysicalTx& node, TKqpParamsMap&& params)
        : Node(node)
        , Params(std::move(params)) {}
};

struct TStageInfoMeta {
    const IKqpGateway::TPhysicalTxData& Tx;

    TTableId TableId;
    TString TablePath;
    ETableKind TableKind;

    TVector<bool> SkipNullKeys;

    THashSet<TKeyDesc::ERowOperation> ShardOperations;
    THolder<TKeyDesc> ShardKey;
    NSchemeCache::TSchemeCacheRequest::EKind ShardKind = NSchemeCache::TSchemeCacheRequest::EKind::KindUnknown;

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

struct TTaskInputMeta {};

struct TTaskOutputMeta {
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

    void MergeWritePoints(TShardKeyRanges&& other, const TVector<NScheme::TTypeId>& keyTypes);

    TString ToString(const TVector<NScheme::TTypeId>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange* proto) const;
    void SerializeTo(NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta_TReadOpMeta* proto) const;

    std::pair<const TSerializedCellVec*, bool> GetRightBorder() const;
};

// TODO: use two different structs for scans and data queries
struct TTaskMeta {
    ui64 ShardId = 0; // only in case of non-scans (data-query & legacy scans)
    ui64 NodeId = 0;  // only in case of scans over persistent snapshots

    TMap<TString, NYql::NDqProto::TData> Params;
    TMap<TString, NKikimr::NMiniKQL::TType*> ParamTypes;

    struct TColumn {
        ui32 Id = 0;
        ui32 Type = 0;
        TString Name;
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
        ui64 ItemsLimit = 0;
        bool Reverse = false;
        bool Sorted = false;
        TKqpOlapProgram OlapProgram;
        TVector<NUdf::TDataTypeId> ResultColumnsTypes;
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

    TString ToString(const TVector<NScheme::TTypeId>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
};

using TStageInfo = NYql::NDq::TStageInfo<TStageInfoMeta>;
using TTaskOutput = NYql::NDq::TTaskOutput<TTaskOutputMeta>;
using TTaskOutputType = NYql::NDq::TTaskOutputType;
using TTaskInput = NYql::NDq::TTaskInput<TTaskInputMeta>;
using TTask = NYql::NDq::TTask<TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta>;
using TKqpTasksGraph = NYql::NDq::TDqTasksGraph<TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta>;

void FillKqpTasksGraphStages(TKqpTasksGraph& tasksGraph, const TVector<IKqpGateway::TPhysicalTxData>& txs);
void BuildKqpTaskGraphResultChannels(TKqpTasksGraph& tasksGraph, const NKqpProto::TKqpPhyTx& tx, ui64 txIdx);
void BuildKqpStageChannels(TKqpTasksGraph& tasksGraph, const TKqpTableKeys& tableKeys, const TStageInfo& stageInfo,
    ui64 txId, bool enableSpilling, const IKqpGateway::TKqpSnapshot& snapshot = {});
TVector<TTaskMeta::TColumn> BuildKqpColumns(const NKqpProto::TKqpPhyTableOperation& op, const TKqpTableKeys::TTable& table);

struct TKqpTaskOutputType {
    enum : ui32 {
        ShardRangePartition = TTaskOutputType::COMMON_TASK_OUTPUT_TYPE_END
    };
};

const NKqpProto::TKqpPhyStage& GetStage(const TStageInfo& stageInfo);

void LogStage(const NActors::TActorContext& ctx, const TStageInfo& stageInfo);

bool HasReads(const TStageInfo& stageInfo);
bool HasWrites(const TStageInfo& stageInfo);

bool IsCrossShardChannel(TKqpTasksGraph& tasksGraph, const NYql::NDq::TChannel& channel);

} // namespace NKqp
} // namespace NKikimr
