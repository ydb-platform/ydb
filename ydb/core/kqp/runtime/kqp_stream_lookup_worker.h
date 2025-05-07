#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NKikimr {
namespace NKqp {

class TKqpStreamLookupWorker {
public:
    using TReadList = std::vector<std::pair<ui64, THolder<TEvDataShard::TEvRead>>>;
    using TPartitionInfoPtr = std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>;

    struct TShardReadResult {
        const ui64 ShardId;
        THolder<TEventHandle<TEvDataShard::TEvReadResult>> ReadResult;
        size_t UnprocessedResultRow = 0;
    };

    struct TReadResultStats {
        ui64 ReadRowsCount = 0;
        ui64 ReadBytesCount = 0;
        ui64 ResultRowsCount = 0;
        ui64 ResultBytesCount = 0;
        std::unordered_set<ui64> AffectedShards;

        void Add(const TReadResultStats& other) {
            ReadRowsCount += other.ReadRowsCount;
            ReadBytesCount += other.ReadBytesCount;
            ResultRowsCount += other.ResultRowsCount;
            ResultBytesCount += other.ResultBytesCount;
            AffectedShards.insert(other.AffectedShards.begin(), other.AffectedShards.end());
        }

        void Clear() {
            ReadRowsCount = 0;
            ReadBytesCount = 0;
            ResultRowsCount = 0;
            ResultBytesCount = 0;
        }
    };

    struct TSettings {
        TString TablePath;
        TTableId TableId;
        std::unordered_map<TString, TSysTables::TTableColumnInfo> KeyColumns;
        std::vector<TSysTables::TTableColumnInfo*> LookupKeyColumns;
        std::vector<TSysTables::TTableColumnInfo> Columns;
        std::optional<ui32> AllowNullKeysPrefixSize;
        std::optional<bool> KeepRowsOrder;
        NKqpProto::EStreamLookupStrategy LookupStrategy;
    };

public:
    TKqpStreamLookupWorker(TSettings&& settings, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, const NYql::NDqProto::TTaskInput& inputDesc);

    virtual ~TKqpStreamLookupWorker();

    virtual std::string GetTablePath() const;
    virtual TTableId GetTableId() const;
    virtual std::vector<NScheme::TTypeInfo> GetKeyColumnTypes() const;
    virtual void ResetTablePartitioning(const TPartitionInfoPtr& partitioning = {});
    virtual bool HasTablePartitioning() const;
    virtual const TReadResultStats& GetStats() const;

    virtual void AddInputRow(NUdf::TUnboxedValue inputRow) = 0;
    virtual std::vector<THolder<TEvDataShard::TEvRead>> RebuildRequest(const ui64& prevReadId, ui32 firstUnprocessedQuery, 
        TMaybe<TOwnedCellVec> lastProcessedKey, ui64& newReadId) = 0;
    virtual TReadList BuildRequests(ui64& readId) = 0;
    virtual void AddResult(TShardReadResult result) = 0;
    virtual TReadResultStats ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) = 0;
    virtual bool AllRowsProcessed() = 0;
    virtual void ResetRowsProcessing(ui64 readId, ui32 firstUnprocessedQuery, TMaybe<TOwnedCellVec> lastProcessedKey) = 0;

protected:
    const TSettings Settings;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    const NYql::NDqProto::TTaskInput& InputDesc;
    TPartitionInfoPtr TablePartitioning;
    TReadResultStats Stats;
};

std::vector<std::unique_ptr<TKqpStreamLookupWorker>> CreateStreamLookupWorkers(NKikimrKqp::TKqpStreamLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    const NYql::NDqProto::TTaskInput& inputDesc);

} // namespace NKqp
} // namespace NKikimr
