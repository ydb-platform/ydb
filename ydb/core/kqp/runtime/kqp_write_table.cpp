#include "kqp_write_table.h"

#include <deque>
#include <vector>
#include <util/generic/yexception.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/core/engine/mkql_keys.h>
#include <util/generic/size_literals.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/sharding/sharding.h>

namespace NKikimr {
namespace NKqp {

namespace {

constexpr ui64 MaxBatchBytes = 8_MB;
constexpr ui64 MaxUnshardedBatchBytes = 8_MB;

TVector<TSysTables::TTableColumnInfo> BuildColumns(const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    TVector<TSysTables::TTableColumnInfo> result;
    result.reserve(inputColumns.size());
    i32 number = 0;
    for (const auto& column : inputColumns) {
        result.emplace_back(
            column.GetName(),
            column.GetId(),
            NScheme::TTypeInfo {
                static_cast<NScheme::TTypeId>(column.GetTypeId()),
                column.GetTypeId() == NScheme::NTypeIds::Pg
                    ? NPg::TypeDescFromPgTypeId(column.GetTypeInfo().GetPgTypeId())
                    : nullptr
            },
            column.GetTypeInfo().GetPgTypeMod(),
            number++
        );
    }
    return result;
}

std::vector<ui32> BuildWriteIndex(
    const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    YQL_ENSURE(schemeEntry.ColumnTableInfo);
    YQL_ENSURE(schemeEntry.ColumnTableInfo->Description.HasSchema());
    const auto& columns = schemeEntry.ColumnTableInfo->Description.GetSchema().GetColumns();

    THashMap<ui32, ui32> writeColumnIdToIndex;
    {
        i32 number = 0;
        for (const auto& column : columns) {
            writeColumnIdToIndex[column.GetId()] = number++;
        }
    }

    std::vector<ui32> result;
    {
        result.reserve(inputColumns.size());
        for (const auto& column : inputColumns) {
            result.push_back(writeColumnIdToIndex.at(column.GetId()));
        }
    }
    return result;
}

std::vector<ui32> BuildWriteIndexKeyFirst(
    const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    const auto& columns = schemeEntry.Columns;

    THashMap<ui32, ui32> writeColumnIdToIndex;
    {
        for (const auto& [index, column] : columns) {
            if (column.KeyOrder >= 0) {
                writeColumnIdToIndex[column.Id] = column.KeyOrder;
            }
        }
        ui32 number = writeColumnIdToIndex.size();
        for (const auto& [index, column] : columns) {
            if (column.KeyOrder < 0) {
                writeColumnIdToIndex[column.Id] = number++;
            }
        }
    }

    std::vector<ui32> result;
    {
        result.reserve(inputColumns.size());
        for (const auto& column : inputColumns) {
            result.push_back(writeColumnIdToIndex.at(column.GetId()));
        }
    }
    return result;
}


std::vector<ui32> BuildWriteColumnIds(
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const std::vector<ui32>& writeIndex) {
    std::vector<ui32> result;
    result.resize(inputColumns.size(), 0);
    for (size_t index = 0; index < inputColumns.size(); ++index) {
        result[writeIndex.at(index)] = inputColumns.at(index).GetId();
    }
    return result;
}

std::set<std::string> BuildNotNullColumns(const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    std::set<std::string> result;
    for (const auto& column : inputColumns) {
        if (column.GetNotNull()) {
            result.insert(column.GetName());
        }
    }
    return result;
}

std::vector<std::pair<TString, NScheme::TTypeInfo>> BuildBatchBuilderColumns(
    const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry) {
    YQL_ENSURE(schemeEntry.ColumnTableInfo);
    YQL_ENSURE(schemeEntry.ColumnTableInfo->Description.HasSchema());
    const auto& columns = schemeEntry.ColumnTableInfo->Description.GetSchema().GetColumns();

    std::vector<std::pair<TString, NScheme::TTypeInfo>> result;
    result.reserve(columns.size());
    for (const auto& column : columns) {
        Y_ABORT_UNLESS(column.HasTypeId());
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
            column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
        result.emplace_back(column.GetName(), typeInfoMod.TypeInfo);
    }
    return result;
}

TVector<NScheme::TTypeInfo> BuildKeyColumnTypes(
    const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry) {
    TVector<NScheme::TTypeInfo> keyColumnTypes;
    for (const auto& [_, column] : schemeEntry.Columns) {
        if (column.KeyOrder >= 0) {
            keyColumnTypes.resize(Max<size_t>(keyColumnTypes.size(), column.KeyOrder + 1));
            keyColumnTypes[column.KeyOrder] = column.PType;
        }
    }
    return keyColumnTypes;
}

class TColumnShardPayloadSerializer : public IPayloadSerializer {
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    class TBatch : public IPayloadSerializer::IBatch {
    public:
        TString SerializeToString() const override {
            return NArrow::SerializeBatchNoCompression(Data);
        }

        i64 GetSerializedMemory() const override {
            // For columnshard there are no hard 8MB operation limit,
            // so we can use approx estimate.
            return NArrow::GetBatchDataSize(Data);
        }

        i64 GetMemory() const override {
            return NArrow::GetBatchDataSize(Data);
        }

        TBatch(const TRecordBatchPtr& data) : Data(data) {}

    private:
        TRecordBatchPtr Data;
    };

    struct TUnpreparedBatch {
        ui64 TotalDataSize = 0;
        std::deque<TRecordBatchPtr> Batches; 
    };

public:
    TColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns, // key columns then value columns
        const NMiniKQL::TTypeEnvironment& typeEnv)
        : TypeEnv(typeEnv)
        , SchemeEntry(schemeEntry)
        , Columns(BuildColumns(inputColumns))
        , WriteIndex(BuildWriteIndex(SchemeEntry, inputColumns))
        , WriteColumnIds(BuildWriteColumnIds(inputColumns, WriteIndex))
        , BatchBuilder(arrow::Compression::UNCOMPRESSED, BuildNotNullColumns(inputColumns)) {
        TString err;
        if (!BatchBuilder.Start(BuildBatchBuilderColumns(SchemeEntry), 0, 0, err)) {
            yexception() << "Failed to start batch builder: " + err;
        }

        // TODO: Checks from ydb/core/tx/data_events/columnshard_splitter.cpp
        const auto& description = SchemeEntry.ColumnTableInfo->Description;
        const auto& scheme = description.GetSchema();
        const auto& sharding = description.GetSharding();

        NSchemeShard::TOlapSchema olapSchema;
        olapSchema.ParseFromLocalDB(scheme);
        auto shardingConclusion = NSharding::TShardingBase::BuildFromProto(olapSchema, sharding);
        if (shardingConclusion.IsFail()) {
            ythrow yexception() << "Ydb::StatusIds::SCHEME_ERROR : " <<  shardingConclusion.GetErrorMessage();
        }
        YQL_ENSURE(shardingConclusion.GetResult() != nullptr);
        Sharding = shardingConclusion.DetachResult();
    }

    void AddData(NMiniKQL::TUnboxedValueBatch&& data) override {
        YQL_ENSURE(!Closed);
        if (data.empty()) {
            return;
        }

        TVector<TCell> cells(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                cells[WriteIndex[index]] = MakeCell(
                    Columns[index].PType,
                    row.GetElement(index),
                    TypeEnv,
                    /* copy */ false);
            }
            BatchBuilder.AddRow(TConstArrayRef<TCell>{cells.begin(), cells.end()});
        });

        FlushUnsharded(false);
    }

    void ReturnBatch(IPayloadSerializer::IBatchPtr&& batch) override {
        Y_UNUSED(batch);
    }

    void FlushUnsharded(bool force) {
        if ((BatchBuilder.Bytes() > 0 && force) || BatchBuilder.Bytes() >= MaxUnshardedBatchBytes) {
            const auto unshardedBatch = BatchBuilder.FlushBatch(true);
            YQL_ENSURE(unshardedBatch);
            for (auto [shardId, shardBatch] : Sharding->SplitByShardsToArrowBatches(unshardedBatch)) {
                const i64 shardBatchMemory = NArrow::GetBatchDataSize(shardBatch);
                YQL_ENSURE(shardBatchMemory != 0);

                auto& unpreparedBatch = UnpreparedBatches[shardId];
                unpreparedBatch.TotalDataSize += shardBatchMemory;
                unpreparedBatch.Batches.emplace_back(shardBatch);
                Memory += shardBatchMemory;

                FlushUnpreparedBatch(shardId, unpreparedBatch, false);

                ShardIds.insert(shardId);
            }
        }
    }

    void FlushUnpreparedBatch(const ui64 shardId, TUnpreparedBatch& unpreparedBatch, bool force = true) {
        while (!unpreparedBatch.Batches.empty() && (unpreparedBatch.TotalDataSize >= MaxBatchBytes || force)) {
            std::vector<TRecordBatchPtr> toPrepare;
            i64 toPrepareSize = 0;
            while (!unpreparedBatch.Batches.empty()) {
                auto batch = unpreparedBatch.Batches.front();
                unpreparedBatch.Batches.pop_front();

                NArrow::TRowSizeCalculator rowCalculator(8);
                if (!rowCalculator.InitBatch(batch)) {
                    ythrow yexception() << "unexpected column type on batch initialization for row size calculator";
                }

                i64 nextRowSize = 0;
                for (i64 index = 0; index < batch->num_rows(); ++index) {
                    nextRowSize = rowCalculator.GetRowBytesSize(index);

                    if (toPrepareSize + nextRowSize >= (i64)MaxBatchBytes) {
                        toPrepare.push_back(batch->Slice(0, index));
                        unpreparedBatch.Batches.push_front(batch->Slice(index, batch->num_rows() - index));

                        Memory -= NArrow::GetBatchDataSize(batch);
                        Memory += NArrow::GetBatchDataSize(unpreparedBatch.Batches.front());
                        break;
                    } else {
                        toPrepareSize += nextRowSize;
                    }
                }

                if (toPrepareSize + nextRowSize >= (i64)MaxBatchBytes) {
                    break;
                }

                Memory -= NArrow::GetBatchDataSize(batch);
                toPrepare.push_back(batch);
            }

            auto& batch = Batches[shardId].emplace_back(
                MakeIntrusive<TBatch>(NArrow::CombineBatches(toPrepare)));
            Memory += batch->GetMemory();
            YQL_ENSURE(batch->GetMemory() != 0);   
        }
    }

    void FlushUnpreparedForce() {
        for (auto& [shardId, unpreparedBatch] : UnpreparedBatches) {
            FlushUnpreparedBatch(shardId, unpreparedBatch, true);
        }
    }

    NKikimrDataEvents::EDataFormat GetDataFormat() override {
        return NKikimrDataEvents::FORMAT_ARROW;
    }

    virtual std::vector<ui32> GetWriteColumnIds() override {
        return WriteColumnIds;
    }

    i64 GetMemory() override {
        return Memory + BatchBuilder.Bytes();
    }

    void Close() override {
        YQL_ENSURE(!Closed);
        Closed = true;
        FlushUnsharded(true);
    }

    bool IsClosed() override {
        return Closed;
    }

    bool IsEmpty() override {
        return Batches.empty();
    }

    bool IsFinished() override {
        return IsClosed() && IsEmpty();
    }

    TBatches FlushBatchesForce() override {
        FlushUnsharded(true);
        FlushUnpreparedForce();

        TBatches newBatches;
        std::swap(Batches, newBatches);
        for (const auto& [_, batches] : newBatches) {
            for (const auto& batch : batches) {
                Memory -= batch->GetMemory();
            }
        }
        return std::move(newBatches);
    }

    IBatchPtr FlushBatch(ui64 shardId) override {
        if (!Batches.contains(shardId)) {
            return {};
        }
        auto batches = Batches.at(shardId);
        if (batches.empty()) {
            return {};
        }

        const auto batch = std::move(batches.front());
        batches.pop_front();
        Memory -= batch->GetMemory();
        return batch;
    }

    const THashSet<ui64>& GetShardIds() const override {
        return ShardIds;
    }

private:

    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NSchemeCache::TSchemeCacheNavigate::TEntry& SchemeEntry;
    std::shared_ptr<NSharding::TShardingBase> Sharding;

    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    const std::vector<ui32> WriteColumnIds;

    NArrow::TArrowBatchBuilder BatchBuilder;
    THashMap<ui64, TUnpreparedBatch> UnpreparedBatches;
    TBatches Batches;
    THashSet<ui64> ShardIds;

    i64 Memory = 0;

    bool Closed = false;
};

class TDataShardPayloadSerializer : public IPayloadSerializer {
    /*class TBatch : public IPayloadSerializer::IBatch {
    public:
        TString SerializeToString() const override {
            return ;
        }

        i64 GetMemory() const override {
            return Size;
        }

        std::vector<TCell> Extract() {
            Size = 0;
            return std::move(Data);
        }

    private:
        std::vector<TCell> Data;
        ui64 Size = 0;
    };*/

    class TBatch : public IPayloadSerializer::IBatch {
    public:
        TString SerializeToString() const override {
            return Data;
        }

        i64 GetSerializedMemory() const override {
            return Data.size();
        }

        i64 GetMemory() const override {
            return Data.size();
        }

        TBatch(TString&& data) : Data(std::move(data)) {}

        TString Data;
    };

public:
    TDataShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const NSchemeCache::TSchemeCacheRequest::TEntry& partitionsEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const NMiniKQL::TTypeEnvironment& typeEnv)
        : TypeEnv(typeEnv)
        , SchemeEntry(schemeEntry)
        , PartitionsEntry(partitionsEntry)
        , Columns(BuildColumns(inputColumns))
        , WriteIndex(BuildWriteIndexKeyFirst(SchemeEntry, inputColumns))
        , WriteColumnIds(BuildWriteColumnIds(inputColumns, WriteIndex))
        , KeyColumnTypes(BuildKeyColumnTypes(SchemeEntry)) {
    }

    void AddData(NMiniKQL::TUnboxedValueBatch&& data) override {
        YQL_ENSURE(!Closed);

        TVector<TCell> cells(Columns.size());
        data.ForEachRow([&](const auto& row) {
            const auto& keyRange = GetKeyRange();

            for (size_t index = 0; index < Columns.size(); ++index) {
                cells[WriteIndex[index]] = MakeCell(
                    Columns[index].PType,
                    row.GetElement(index),
                    TypeEnv,
                    /* copy */ true);
            }

            auto shardIter = std::lower_bound(
                std::begin(keyRange.GetPartitions()),
                std::end(keyRange.GetPartitions()),
                TArrayRef(cells.data(), KeyColumnTypes.size()),
                [this](const auto &partition, const auto& key) {
                    const auto& range = *partition.Range;
                    return 0 > CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                        range.IsInclusive || range.IsPoint, true, KeyColumnTypes);
                });

            YQL_ENSURE(shardIter != keyRange.GetPartitions().end());

            auto batcherIter = Batchers.find(shardIter->ShardId);
            if (batcherIter == std::end(Batchers)) {
                Batchers.emplace(
                    shardIter->ShardId,
                    TCellsBatcher(Columns.size(), MaxBatchBytes));
            }

            Memory += Batchers.at(shardIter->ShardId).AddRow(std::move(cells));
            ShardIds.insert(shardIter->ShardId);

            cells.resize(Columns.size());
        });
    }

    void ReturnBatch(IPayloadSerializer::IBatchPtr&& batch) override {
        Y_UNUSED(batch);
    }

    NKikimrDataEvents::EDataFormat GetDataFormat() override {
        return NKikimrDataEvents::FORMAT_CELLVEC;
    }

    virtual std::vector<ui32> GetWriteColumnIds() override {
        return WriteColumnIds;
    }

    i64 GetMemory() override {
        return Memory;
    }

    void Close() override {
        YQL_ENSURE(!Closed);
        Closed = true;
    }

    bool IsClosed() override {
        return Closed;
    }

    bool IsEmpty() override {
        return Batchers.empty();
    }

    bool IsFinished() override {
        return IsClosed() && IsEmpty();
    }

    TString ExtractNextBatch(TCellsBatcher& batcher, bool force) {
        auto batchResult = batcher.Flush(force);
        Memory -= batchResult.Memory;
        return batchResult.Data;
    }

    TBatches FlushBatchesForce() override {
        TBatches result;
        for (auto& [shardId, batcher] : Batchers) {
            while (true) {
                auto batch = ExtractNextBatch(batcher, true);
                if (batch.empty()) {
                    break;
                }
                result[shardId].emplace_back(MakeIntrusive<TBatch>(std::move(batch)));
            };
        }
        Batchers.clear();
        return result;
    }

    IBatchPtr FlushBatch(ui64 shardId) override {
        if (!Batchers.contains(shardId)) {
            return {};
        }
        auto& batcher = Batchers.at(shardId);
        return MakeIntrusive<TBatch>(ExtractNextBatch(batcher, false));
    }

    const THashSet<ui64>& GetShardIds() const override {
        return ShardIds;
    }

private:
    const TKeyDesc& GetKeyRange() const {
        return *PartitionsEntry.KeyDescription.Get();
    }

    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NSchemeCache::TSchemeCacheNavigate::TEntry& SchemeEntry;
    const NSchemeCache::TSchemeCacheRequest::TEntry& PartitionsEntry;

    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    const std::vector<ui32> WriteColumnIds;
    const TVector<NScheme::TTypeInfo> KeyColumnTypes;

    THashMap<ui64, TCellsBatcher> Batchers;
    THashSet<ui64> ShardIds;

    i64 Memory = 0;

    bool Closed = false;
};

}

bool IPayloadSerializer::IBatch::IsEmpty() const {
    return GetMemory() == 0;
}

IPayloadSerializerPtr CreateColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const NMiniKQL::TTypeEnvironment& typeEnv) {
    return MakeIntrusive<TColumnShardPayloadSerializer>(
        schemeEntry, inputColumns, typeEnv);
}

IPayloadSerializerPtr CreateDataShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const NSchemeCache::TSchemeCacheRequest::TEntry& partitionsEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const NMiniKQL::TTypeEnvironment& typeEnv) {
    return MakeIntrusive<TDataShardPayloadSerializer>(
        schemeEntry, partitionsEntry, inputColumns, typeEnv);
}

}
}
