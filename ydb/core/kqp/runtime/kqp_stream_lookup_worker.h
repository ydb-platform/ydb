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

struct TLookupSettings {
    TString TablePath;
    TTableId TableId;

    ui32 AllowNullKeysPrefixSize;
    bool KeepRowsOrder;
    NKqpProto::EStreamLookupStrategy LookupStrategy;

    std::unordered_map<TString, TSysTables::TTableColumnInfo> KeyColumns;
    std::vector<TSysTables::TTableColumnInfo*> LookupKeyColumns;
    std::vector<TSysTables::TTableColumnInfo> Columns;
};

class TKqpStreamLookupWorker {
public:
    using TReadList = std::vector<std::pair<ui64, THolder<TEvDataShard::TEvRead>>>;
    using TPartitionInfo = std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>;

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
        bool SizeLimitExceeded = false;

        void Add(const TReadResultStats& other) {
            ReadRowsCount += other.ReadRowsCount;
            ReadBytesCount += other.ReadBytesCount;
            ResultRowsCount += other.ResultRowsCount;
            ResultBytesCount += other.ResultBytesCount;
        }

        void Clear() {
            ReadRowsCount = 0;
            ReadBytesCount = 0;
            ResultRowsCount = 0;
            ResultBytesCount = 0;
        }
    };

public:
    TKqpStreamLookupWorker(TLookupSettings&& settings, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory);

    virtual ~TKqpStreamLookupWorker();

    virtual std::string GetTablePath() const;
    virtual TTableId GetTableId() const;

    const std::vector<NScheme::TTypeInfo>& GetKeyColumnTypes() const {
        return KeyColumnTypes;
    }

    virtual void AddInputRow(NUdf::TUnboxedValue inputRow) = 0;
    virtual void AddInputRow(TConstArrayRef<TCell> inputRow) = 0;
    virtual std::vector<THolder<TEvDataShard::TEvRead>> RebuildRequest(const ui64& prevReadId, ui64& newReadId) = 0;
    virtual TReadList BuildRequests(const TPartitionInfo& partitioning, ui64& readId) = 0;
    virtual void AddResult(TShardReadResult result) = 0;
    virtual TReadResultStats ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) = 0;
    virtual TReadResultStats ReadAllResult(std::function<void(TConstArrayRef<TCell>)> reader) = 0;
    virtual bool AllRowsProcessed() = 0;
    virtual void ResetRowsProcessing(ui64 readId) = 0;
    virtual bool IsOverloaded() = 0;

protected:
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    const TLookupSettings Settings;

    std::vector<NScheme::TTypeInfo> KeyColumnTypes;
};

std::unique_ptr<TKqpStreamLookupWorker> CreateStreamLookupWorker(NKikimrKqp::TKqpStreamLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    const NYql::NDqProto::TTaskInput& inputDesc);

std::unique_ptr<TKqpStreamLookupWorker> CreateLookupWorker(TLookupSettings&& settings,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory);

} // namespace NKqp
} // namespace NKikimr
