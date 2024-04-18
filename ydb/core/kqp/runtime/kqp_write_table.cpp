#include "kqp_write_table.h"

#include <deque>
#include <vector>
#include <util/generic/yexception.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/core/engine/mkql_keys.h>

namespace NKikimr {
namespace NKqp {

namespace {

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

public:
    TColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns, // key columns then value columns
        const NMiniKQL::TTypeEnvironment& typeEnv)
        : TypeEnv(typeEnv)
        , SchemeEntry(schemeEntry)
        , Columns(BuildColumns(inputColumns))
        , WriteIndex(BuildWriteIndex(schemeEntry, inputColumns))
        , WriteColumnIds(BuildWriteColumnIds(inputColumns, WriteIndex))
        , BatchBuilder(arrow::Compression::UNCOMPRESSED, BuildNotNullColumns(inputColumns)) {
        TString err;
        if (!BatchBuilder.Start(BuildBatchBuilderColumns(schemeEntry), 0, 0, err)) {
            yexception() << "Failed to start batch builder: " + err;
        }
    }

    void AddData(NMiniKQL::TUnboxedValueBatch&& data, bool close) override {
        YQL_ENSURE(!Closed);
        Closed = close;

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

        const auto batch = BatchBuilder.FlushBatch(true);
        if (batch) {
            const auto dataAccessor = GetDataAccessor(batch);

            auto shardsSplitter = NKikimr::NEvWrite::IShardsSplitter::BuildSplitter(SchemeEntry);
            if (!shardsSplitter) {
                ythrow yexception() << "Failed to build splitter";
            }
            auto initStatus = shardsSplitter->SplitData(SchemeEntry, *dataAccessor);
            if (!initStatus.Ok()) {
                ythrow yexception() << "Failed to split batch: " << initStatus.GetErrorMessage();
            }

            const auto& splittedData = shardsSplitter->GetSplitData();

            for (auto& [shard, infos] : splittedData.GetShardsInfo()) {
                for (auto&& shardInfo : infos) {
                    auto& batch = Batches[shard].emplace_back();
                    batch = shardInfo->GetData();
                    Memory += batch.size();
                    YQL_ENSURE(!batch.empty());
                }
            }
        }
    }

    NKikimrDataEvents::EDataFormat GetDataFormat() override {
        return NKikimrDataEvents::FORMAT_ARROW;
    }

    virtual std::vector<ui32> GetWriteColumnIds() override {
        return WriteColumnIds;
    }

    i64 GetMemory() override {
        return Memory;
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

    TBatches FlushBatches(const bool force) override {
        Y_UNUSED(force);
        TBatches newBatches;
        std::swap(Batches, newBatches);
        Memory = 0;
        return std::move(newBatches);
    }

private:
    NKikimr::NEvWrite::IShardsSplitter::IEvWriteDataAccessor::TPtr GetDataAccessor(
            const TRecordBatchPtr& batch) const {
        struct TDataAccessor : public NKikimr::NEvWrite::IShardsSplitter::IEvWriteDataAccessor {
            TRecordBatchPtr Batch;

            TDataAccessor(const TRecordBatchPtr& batch)
                : Batch(batch) {
            }

            TRecordBatchPtr GetDeserializedBatch() const override {
                return Batch;
            }

            TString GetSerializedData() const override {
                return NArrow::SerializeBatchNoCompression(Batch);
            }
        };

        return std::make_shared<TDataAccessor>(batch);
    }

    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NSchemeCache::TSchemeCacheNavigate::TEntry& SchemeEntry;

    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    const std::vector<ui32> WriteColumnIds;

    NArrow::TArrowBatchBuilder BatchBuilder;

    TBatches Batches;

    i64 Memory = 0;

    bool Closed = false;
};

class TDataShardPayloadSerializer : public IPayloadSerializer {
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

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

    void AddData(NMiniKQL::TUnboxedValueBatch&& data, bool close) override {
        YQL_ENSURE(!Closed);
        Closed = close;

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

            auto it = std::lower_bound(
                std::begin(keyRange.GetPartitions()),
                std::end(keyRange.GetPartitions()),
                TArrayRef(cells.data(), KeyColumnTypes.size()),
                [this](const auto &partition, const auto& key) {
                    const auto& range = *partition.Range;
                    return 0 > CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                        range.IsInclusive || range.IsPoint, true, KeyColumnTypes);
                });

            YQL_ENSURE(it != keyRange.GetPartitions().end());

            for (auto& cell : cells) {
                Batches[it->ShardId].emplace_back(std::move(cell));
            }
        });
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

    bool IsClosed() override {
        return Closed;
    }

    bool IsEmpty() override {
        return Batches.empty();
    }

    bool IsFinished() override {
        return IsClosed() && IsEmpty();
    }

    TBatches FlushBatches(const bool force) override {
        Y_UNUSED(force);
        TBatches result;
        for (auto& [shardId, batch] : Batches) {
            TSerializedCellMatrix matrix(batch, batch.size() / Columns.size(), Columns.size());
            result[shardId].push_back(matrix.ReleaseBuffer());
        }
        Batches.clear();
        return result;
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

    THashMap<ui64, TVector<TCell>> Batches;

    i64 Memory = 0;

    bool Closed = false;
};

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
