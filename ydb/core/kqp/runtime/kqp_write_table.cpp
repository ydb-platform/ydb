#include "kqp_write_table.h"

#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NKikimr {
namespace NKqp {

namespace {

constexpr ui64 DataShardMaxOperationBytes = 8_MB;
constexpr ui64 ColumnShardMaxOperationBytes = 8_MB;
constexpr ui64 MaxUnshardedBatchBytes = 0_MB;

class IPayloadSerializer : public TThrRefBase {
public:
    class IBatch : public TThrRefBase {
    public:
        virtual TString SerializeToString() const = 0;
        virtual i64 GetMemory() const = 0;
        bool IsEmpty() const;
    };

    using IBatchPtr = TIntrusivePtr<IBatch>;

    virtual void AddData(NMiniKQL::TUnboxedValueBatch&& data) = 0;
    virtual void AddBatch(const IBatchPtr& batch) = 0;

    virtual void Close() = 0;

    virtual bool IsClosed() = 0;
    virtual bool IsEmpty() = 0;
    virtual bool IsFinished() = 0;

    virtual NKikimrDataEvents::EDataFormat GetDataFormat() = 0;
    virtual std::vector<ui32> GetWriteColumnIds() = 0;

    using TBatches = THashMap<ui64, std::deque<IBatchPtr>>;

    virtual TBatches FlushBatchesForce() = 0;

    virtual IBatchPtr FlushBatch(ui64 shardId) = 0;
    virtual const THashSet<ui64>& GetShardIds() const = 0;

    virtual i64 GetMemory() = 0;
};

using IPayloadSerializerPtr = TIntrusivePtr<IPayloadSerializer>;

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

    THashSet<ui32> inputColumnsIds;
    for (const auto& column : inputColumns) {
        inputColumnsIds.insert(column.GetId());
    }

    THashMap<ui32, ui32> writeColumnIdToIndex;
    {
        i32 number = 0;
        for (const auto& column : columns) {
            if (inputColumnsIds.contains(column.GetId())) {
                writeColumnIdToIndex[column.GetId()] = number++;
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

std::vector<ui32> BuildWriteIndexKeyFirst(
    const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    const auto& columns = schemeEntry.Columns;

    THashSet<ui32> inputColumnsIds;
    for (const auto& column : inputColumns) {
        inputColumnsIds.insert(column.GetId());
    }

    THashMap<ui32, ui32> writeColumnIdToIndex;
    {
        for (const auto& [index, column] : columns) {
            if (column.KeyOrder >= 0) {
                writeColumnIdToIndex[column.Id] = column.KeyOrder;
                YQL_ENSURE(inputColumnsIds.contains(column.Id));
            }
        }
        ui32 number = writeColumnIdToIndex.size();
        for (const auto& [index, column] : columns) {
            if (column.KeyOrder < 0 && inputColumnsIds.contains(column.Id)) {
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
    const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    YQL_ENSURE(schemeEntry.ColumnTableInfo);
    YQL_ENSURE(schemeEntry.ColumnTableInfo->Description.HasSchema());
    const auto& columns = schemeEntry.ColumnTableInfo->Description.GetSchema().GetColumns();

    THashSet<ui32> inputColumnsIds;
    for (const auto& column : inputColumns) {
        inputColumnsIds.insert(column.GetId());
    }

    std::vector<std::pair<TString, NScheme::TTypeInfo>> result;
    result.reserve(columns.size());
    for (const auto& column : columns) {
        if (inputColumnsIds.contains(column.GetId())) {
            Y_ABORT_UNLESS(column.HasTypeId());
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            result.emplace_back(column.GetName(), typeInfoMod.TypeInfo);
        }
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

struct TRowWithData {
    TVector<TCell> Cells;
    NUdf::TStringValue Data;
};

class TRowBuilder {
private:
    struct TCellInfo {
        NScheme::TTypeInfo Type;
        NUdf::TUnboxedValuePod Value;
        TString PgBinaryValue;
    };

public:
    explicit TRowBuilder(size_t size)
        : CellsInfo(size) {
    }

    TRowBuilder& AddCell(
            const size_t index,
            const NScheme::TTypeInfo type,
            const NUdf::TUnboxedValuePod& value,
            const i32 typmod = -1) {
        CellsInfo[index].Type = type;
        CellsInfo[index].Value = value;

        if (type.GetTypeId() == NScheme::NTypeIds::Pg) {
            const auto typeDesc = type.GetTypeDesc();
            if (typmod != -1 && NPg::TypeDescNeedsCoercion(typeDesc)) {
                TMaybe<TString> err;
                CellsInfo[index].PgBinaryValue = NYql::NCommon::PgValueCoerce(value, NPg::PgTypeIdFromTypeDesc(typeDesc), typmod, &err);
                if (err) {
                    ythrow yexception() << "PgValueCoerce error: " << *err;
                }
            } else {
                CellsInfo[index].PgBinaryValue = NYql::NCommon::PgValueToNativeBinary(value, NPg::PgTypeIdFromTypeDesc(typeDesc));
            }
        } else {
            CellsInfo[index].PgBinaryValue.clear();
        }
        return *this;
    }

    size_t DataSize() const {
        size_t result = 0;
        for (const auto& cellInfo : CellsInfo) {
            result += GetCellSize(cellInfo);
        }
        return result;
    }

    TRowWithData Build() {
        TVector<TCell> cells;
        cells.reserve(CellsInfo.size());
        const auto size = DataSize();
        auto data = Allocate(size);
        char* ptr = data.Data();

        for (const auto& cellInfo : CellsInfo) {
            cells.push_back(BuildCell(cellInfo, ptr));
        }

        AFL_ENSURE(ptr == data.Data() + size);

        return TRowWithData {
            .Cells = std::move(cells),
            .Data = std::move(data),
        };
    }

private:
    TCell BuildCell(const TCellInfo& cellInfo, char*& dataPtr) {
        if (!cellInfo.Value) {
            return TCell();
        }

        switch(cellInfo.Type.GetTypeId()) {
    #define MAKE_PRIMITIVE_TYPE_CELL_CASE(type, layout) \
        case NUdf::TDataType<type>::Id: return NMiniKQL::MakeCell<layout>(cellInfo.Value);
            KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_CELL_CASE)
        case NUdf::TDataType<NUdf::TDecimal>::Id:
            {
                auto intValue = cellInfo.Value.GetInt128();
                constexpr auto valueSize = sizeof(intValue);

                char* initialPtr = dataPtr;
                std::memcpy(initialPtr, reinterpret_cast<const char*>(&intValue), valueSize);
                dataPtr += valueSize;
                return TCell(initialPtr, valueSize);
            }
        }

        const auto ref = cellInfo.Type.GetTypeId() == NScheme::NTypeIds::Pg
            ? NYql::NUdf::TStringRef(cellInfo.PgBinaryValue)
            : cellInfo.Value.AsStringRef();

        char* initialPtr = dataPtr;
        std::memcpy(initialPtr, ref.Data(), ref.Size());
        dataPtr += ref.Size();
        return TCell(initialPtr, ref.Size());
    }

    size_t GetCellSize(const TCellInfo& cellInfo) const {
        if (!cellInfo.Value) {
            return 0;
        }

        switch(cellInfo.Type.GetTypeId()) {
    #define MAKE_PRIMITIVE_TYPE_CELL_CASE_SIZE(type, layout) \
        case NUdf::TDataType<type>::Id:
            KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_CELL_CASE_SIZE)
            return 0;
        case NUdf::TDataType<NUdf::TDecimal>::Id:
            return sizeof(cellInfo.Value.GetInt128());
        }

        if (cellInfo.Type.GetTypeId() == NScheme::NTypeIds::Pg) {
            return cellInfo.PgBinaryValue.size();
        }
        return cellInfo.Value.AsStringRef().Size();
    }

    NUdf::TStringValue Allocate(size_t size) {
        Y_DEBUG_ABORT_UNLESS(NMiniKQL::TlsAllocState);
        return NUdf::TStringValue(size);
    }

    TVector<TCellInfo> CellsInfo;
};

class TColumnShardPayloadSerializer : public IPayloadSerializer {
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    class TBatch : public IPayloadSerializer::IBatch {
    public:
        TString SerializeToString() const override {
            return NArrow::SerializeBatchNoCompression(Data);
        }

        i64 GetMemory() const override {
            return Memory;
        }

        TRecordBatchPtr Extract() {
            Memory = 0;
            TRecordBatchPtr result = std::move(Data);
            return result;
        }

        TBatch(const TRecordBatchPtr& data)
            : Data(data)
            , Memory(NArrow::GetBatchDataSize(Data)) {
        }

    private:
        TRecordBatchPtr Data;
        i64 Memory;
    };

    struct TUnpreparedBatch {
        ui64 TotalDataSize = 0;
        std::deque<TRecordBatchPtr> Batches; 
    };

public:
    TColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) // key columns then value columns
        : Columns(BuildColumns(inputColumns))
        , WriteIndex(BuildWriteIndex(schemeEntry, inputColumns))
        , WriteColumnIds(BuildWriteColumnIds(inputColumns, WriteIndex))
        , BatchBuilder(arrow::Compression::UNCOMPRESSED, BuildNotNullColumns(inputColumns)) {
        TString err;
        if (!BatchBuilder.Start(BuildBatchBuilderColumns(schemeEntry, inputColumns), 0, 0, err)) {
            yexception() << "Failed to start batch builder: " + err;
        }

        YQL_ENSURE(schemeEntry.ColumnTableInfo);
        const auto& description = schemeEntry.ColumnTableInfo->Description;
        YQL_ENSURE(description.HasSchema());
        const auto& scheme = description.GetSchema();
        YQL_ENSURE(description.HasSharding());
        const auto& sharding = description.GetSharding();

        NSchemeShard::TOlapSchema olapSchema;
        olapSchema.ParseFromLocalDB(scheme);
        auto shardingConclusion = NSharding::IShardingBase::BuildFromProto(olapSchema, sharding);
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

        TRowBuilder rowBuilder(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                rowBuilder.AddCell(WriteIndex[index], Columns[index].PType, row.GetElement(index));
            }
            auto rowWithData = rowBuilder.Build();
            BatchBuilder.AddRow(TConstArrayRef<TCell>{rowWithData.Cells.begin(), rowWithData.Cells.end()});
        });

        FlushUnsharded(false);
    }

    void AddBatch(const IPayloadSerializer::IBatchPtr& batch) override {
        auto columnshardBatch = dynamic_cast<TBatch*>(batch.Get());
        YQL_ENSURE(columnshardBatch);
        auto data = columnshardBatch->Extract();
        YQL_ENSURE(data);
        ShardAndFlushBatch(data, false);
    }

    void FlushUnsharded(bool force) {
        if ((BatchBuilder.Bytes() > 0 && force) || BatchBuilder.Bytes() > MaxUnshardedBatchBytes) {
            const auto unshardedBatch = BatchBuilder.FlushBatch(true);
            YQL_ENSURE(unshardedBatch);
            ShardAndFlushBatch(unshardedBatch, force);
        }
    }

    void ShardAndFlushBatch(const TRecordBatchPtr& unshardedBatch, bool force) {
        for (auto [shardId, shardBatch] : Sharding->SplitByShardsToArrowBatches(unshardedBatch)) {
            const i64 shardBatchMemory = NArrow::GetBatchDataSize(shardBatch);
            YQL_ENSURE(shardBatchMemory != 0);

            auto& unpreparedBatch = UnpreparedBatches[shardId];
            unpreparedBatch.TotalDataSize += shardBatchMemory;
            unpreparedBatch.Batches.emplace_back(shardBatch);
            Memory += shardBatchMemory;

            FlushUnpreparedBatch(shardId, unpreparedBatch, force);

            ShardIds.insert(shardId);
        }
    }

    void FlushUnpreparedBatch(const ui64 shardId, TUnpreparedBatch& unpreparedBatch, bool force) {
        while (!unpreparedBatch.Batches.empty() && (unpreparedBatch.TotalDataSize >= ColumnShardMaxOperationBytes || force)) {
            std::vector<TRecordBatchPtr> toPrepare;
            i64 toPrepareSize = 0;
            while (!unpreparedBatch.Batches.empty()) {
                auto batch = unpreparedBatch.Batches.front();
                unpreparedBatch.Batches.pop_front();
                YQL_ENSURE(batch->num_rows() > 0);
                const auto batchDataSize = NArrow::GetBatchDataSize(batch);
                unpreparedBatch.TotalDataSize -= batchDataSize;
                Memory -= batchDataSize;

                NArrow::TRowSizeCalculator rowCalculator(8);
                if (!rowCalculator.InitBatch(batch)) {
                    ythrow yexception() << "unexpected column type on batch initialization for row size calculator";
                }

                bool splitted = false;
                for (i64 index = 0; index < batch->num_rows(); ++index) {
                    i64 nextRowSize = rowCalculator.GetRowBytesSize(index);

                    if (toPrepareSize + nextRowSize >= (i64)ColumnShardMaxOperationBytes) {
                        YQL_ENSURE(index > 0);

                        toPrepare.push_back(batch->Slice(0, index));
                        unpreparedBatch.Batches.push_front(batch->Slice(index, batch->num_rows() - index));

                        const auto newBatchDataSize = NArrow::GetBatchDataSize(unpreparedBatch.Batches.front());

                        unpreparedBatch.TotalDataSize += batchDataSize;
                        Memory += newBatchDataSize;

                        splitted = true;
                        break;
                    } else {
                        toPrepareSize += nextRowSize;
                    }
                }

                if (splitted) {
                    break;
                }

                toPrepare.push_back(batch);
            }

            auto batch = MakeIntrusive<TBatch>(NArrow::CombineBatches(toPrepare));
            Batches[shardId].emplace_back(batch);
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
        FlushUnpreparedForce();
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
        auto& batches = Batches.at(shardId);
        if (batches.empty()) {
            return {};
        }

        auto batch = std::move(batches.front());
        batches.pop_front();
        Memory -= batch->GetMemory();

        return batch;
    }

    const THashSet<ui64>& GetShardIds() const override {
        return ShardIds;
    }

private:
    std::shared_ptr<NSharding::IShardingBase> Sharding;

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
    class TBatch : public IPayloadSerializer::IBatch {
    public:
        TString SerializeToString() const override {
            return TSerializedCellMatrix::Serialize(Cells, Rows, Columns);
        }

        i64 GetMemory() const override {
            return Size;
        }

        bool IsEmpty() const {
            return Cells.empty();
        }

        std::pair<std::vector<TCell>, std::vector<NUdf::TStringValue>> Extract() {
            Size = 0;
            Rows = 0;
            return {std::move(Cells), std::move(Data)};
        }

        TBatch(std::vector<TCell>&& cells, std::vector<NUdf::TStringValue>&& data, i64 size, ui32 rows, ui16 columns)
            : Cells(std::move(cells))
            , Data(std::move(data))
            , Size(size)
            , Rows(rows)
            , Columns(columns) {
        }

    private:
        std::vector<TCell> Cells;
        std::vector<NUdf::TStringValue> Data;
        ui64 Size = 0;
        ui32 Rows = 0;
        ui16 Columns = 0;
    };

    class TRowsBatcher {
    public:
        explicit TRowsBatcher(ui16 columnCount, ui64 maxBytesPerBatch)
            : ColumnCount(columnCount)
            , MaxBytesPerBatch(maxBytesPerBatch) {
        }

        bool IsEmpty() const {
            return Batches.empty();
        }

        struct TBatch {
            ui64 Memory = 0;
            ui64 MemorySerialized = 0;
            TVector<TCell> Cells;
            TVector<NUdf::TStringValue> Data;
        };

        TBatch Flush(bool force) {
            TBatch res;
            if ((!Batches.empty() && force) || Batches.size() > 1) {
                res = std::move(Batches.front());
                Batches.pop_front();
            }
            return res;
        }

        ui64 AddRow(TRowWithData&& rowWithData) {
            Y_ABORT_UNLESS(rowWithData.Cells.size() == ColumnCount);
            ui64 newMemory = 0;
            for (const auto& cell : rowWithData.Cells) {
                newMemory += cell.Size();
            }
            if (Batches.empty() || newMemory + GetCellHeaderSize() * ColumnCount + Batches.back().MemorySerialized > MaxBytesPerBatch) {
                Batches.emplace_back();
                Batches.back().Memory = 0;
                Batches.back().MemorySerialized = GetCellMatrixHeaderSize();
            }

            for (auto& cell : rowWithData.Cells) {
                Batches.back().Cells.emplace_back(std::move(cell));
            }
            Batches.back().Data.emplace_back(std::move(rowWithData.Data));

            Batches.back().Memory += newMemory;
            Batches.back().MemorySerialized += newMemory + GetCellHeaderSize() * ColumnCount;

            return newMemory;
        }

    private:
        std::deque<TBatch> Batches;

        ui16 ColumnCount;
        ui64 MaxBytesPerBatch;
    };

public:
    TDataShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        NSchemeCache::TSchemeCacheRequest::TEntry&& partitionsEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns)
        : SchemeEntry(schemeEntry)
        , KeyDescription(std::move(partitionsEntry.KeyDescription))
        , Columns(BuildColumns(inputColumns))
        , WriteIndex(BuildWriteIndexKeyFirst(SchemeEntry, inputColumns))
        , WriteColumnIds(BuildWriteColumnIds(inputColumns, WriteIndex))
        , KeyColumnTypes(BuildKeyColumnTypes(SchemeEntry)) {
    }

    void AddRow(TRowWithData&& row, const TKeyDesc& keyRange) {
        auto shardIter = std::lower_bound(
            std::begin(keyRange.GetPartitions()),
            std::end(keyRange.GetPartitions()),
            TArrayRef(row.Cells.data(), KeyColumnTypes.size()),
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
                TRowsBatcher(Columns.size(), DataShardMaxOperationBytes));
        }

        Memory += Batchers.at(shardIter->ShardId).AddRow(std::move(row));
        ShardIds.insert(shardIter->ShardId);
    }

    void AddData(NMiniKQL::TUnboxedValueBatch&& data) override {
        YQL_ENSURE(!Closed);

        TRowBuilder rowBuilder(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                rowBuilder.AddCell(WriteIndex[index], Columns[index].PType, row.GetElement(index));
            }
            auto rowWithData = rowBuilder.Build();
            AddRow(std::move(rowWithData), GetKeyRange());
        });
    }

    void AddBatch(const IPayloadSerializer::IBatchPtr& batch) override {
        auto datashardBatch = dynamic_cast<TBatch*>(batch.Get());
        YQL_ENSURE(datashardBatch);
        auto [cells, data] = datashardBatch->Extract();
        const auto rows = cells.size() / Columns.size();
        YQL_ENSURE(cells.size() == rows * Columns.size());

        for (size_t rowIndex = 0; rowIndex < rows; ++rowIndex) {
            AddRow(
                TRowWithData{
                    TVector<TCell>(cells.begin() + (rowIndex * Columns.size()), cells.begin() + (rowIndex * Columns.size()) + Columns.size()),
                    data[rowIndex],
                },
                GetKeyRange());
        }
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

    IBatchPtr ExtractNextBatch(TRowsBatcher& batcher, bool force) {
        auto batchResult = batcher.Flush(force);
        Memory -= batchResult.Memory;
        const ui32 rows = batchResult.Cells.size() / Columns.size();
        YQL_ENSURE(Columns.size() <= std::numeric_limits<ui16>::max());
        return MakeIntrusive<TBatch>(
            std::move(batchResult.Cells),
            std::move(batchResult.Data),
            static_cast<i64>(batchResult.MemorySerialized),
            rows,
            static_cast<ui16>(Columns.size()));
    }

    TBatches FlushBatchesForce() override {
        TBatches result;
        for (auto& [shardId, batcher] : Batchers) {
            while (true) {
                auto batch = ExtractNextBatch(batcher, true);
                if (batch->IsEmpty()) {
                    break;
                }
                result[shardId].emplace_back(batch);
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
        return ExtractNextBatch(batcher, false);
    }

    const THashSet<ui64>& GetShardIds() const override {
        return ShardIds;
    }

private:
    const TKeyDesc& GetKeyRange() const {
        return *KeyDescription;
    }

    const NSchemeCache::TSchemeCacheNavigate::TEntry SchemeEntry;
    THolder<TKeyDesc> KeyDescription;

    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    const std::vector<ui32> WriteColumnIds;
    const TVector<NScheme::TTypeInfo> KeyColumnTypes;

    THashMap<ui64, TRowsBatcher> Batchers;
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
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    return MakeIntrusive<TColumnShardPayloadSerializer>(
        schemeEntry, inputColumns);
}

IPayloadSerializerPtr CreateDataShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        NSchemeCache::TSchemeCacheRequest::TEntry&& partitionsEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    return MakeIntrusive<TDataShardPayloadSerializer>(
        schemeEntry, std::move(partitionsEntry), inputColumns);
}

namespace {

class TShardsInfo {
public:
    class TShardInfo {
        friend class TShardsInfo;
        TShardInfo(i64& memory, ui64& nextCookie, bool& closed)
            : Memory(memory)
            , NextCookie(nextCookie)
            , Cookie(NextCookie++)
            , Closed(closed) {
        }

    public:
        size_t Size() const {
            return Batches.size();
        }

        bool IsEmpty() const {
            return Batches.empty();
        }

        bool IsClosed() const {
            return Closed;
        }

        bool IsFinished() const {
            return IsClosed() && IsEmpty();
        }

        void MakeNextBatches(i64 maxDataSize, ui64 maxCount) {
            YQL_ENSURE(BatchesInFlight == 0);
            i64 dataSize = 0;
            while (BatchesInFlight < maxCount
                    && BatchesInFlight < Batches.size()
                    && dataSize + GetBatch(BatchesInFlight)->GetMemory() <= maxDataSize) {
                dataSize += GetBatch(BatchesInFlight)->GetMemory();
                ++BatchesInFlight;
            }
            YQL_ENSURE(BatchesInFlight == Batches.size() || GetBatch(BatchesInFlight)->GetMemory() <= maxDataSize); 
        }

        const IPayloadSerializer::IBatchPtr& GetBatch(size_t index) const {
            return Batches.at(index);
        }

        std::optional<ui64> PopBatches(const ui64 cookie) {
            if (BatchesInFlight != 0 && Cookie == cookie) {
                ui64 dataSize = 0;
                for (size_t index = 0; index < BatchesInFlight; ++index) {
                    dataSize += Batches.front()->GetMemory();
                    Batches.pop_front();
                }

                Cookie = NextCookie++;
                SendAttempts = 0;
                BatchesInFlight = 0;

                Memory -= dataSize;
                return dataSize;
            }
            return std::nullopt;
        }

        void PushBatch(IPayloadSerializer::IBatchPtr&& batch) {
            YQL_ENSURE(!IsClosed());
            Batches.emplace_back(std::move(batch));
            Memory += Batches.back()->GetMemory();
        }

        ui64 GetCookie() const {
            return Cookie;
        }

        size_t GetBatchesInFlight() const {
            return BatchesInFlight;
        }

        ui32 GetSendAttempts() const {
            return SendAttempts;
        }

        void IncSendAttempts() {
            ++SendAttempts;
        }

        void ResetSendAttempts() {
            SendAttempts = 0;
        }

    private:
        std::deque<IPayloadSerializer::IBatchPtr> Batches;
        i64& Memory;

        ui64& NextCookie;
        ui64 Cookie;

        bool& Closed;

        ui32 SendAttempts = 0;
        size_t BatchesInFlight = 0;
    };

    TShardInfo& GetShard(const ui64 shard) {
        auto it = ShardsInfo.find(shard);
        if (it != std::end(ShardsInfo)) {
            return it->second;
        }

        auto [insertIt, _] = ShardsInfo.emplace(shard, TShardInfo(Memory, NextCookie, Closed));
        return insertIt->second;
    }

    TVector<ui64> GetPendingShards() const {
        TVector<ui64> result;
        for (const auto& [id, shard] : ShardsInfo) {
            if (!shard.IsEmpty() && shard.GetSendAttempts() == 0) {
                result.push_back(id);
            }
        }
        return result;
    }

    bool Has(ui64 shardId) const {
        return ShardsInfo.contains(shardId);
    }

    bool IsEmpty() const {
        for (const auto& [_, shard] : ShardsInfo) {
            if (!shard.IsEmpty()) {
                return false;
            }
        }
        return true;
    }

    bool IsFinished() const {
        for (const auto& [_, shard] : ShardsInfo) {
            if (!shard.IsFinished()) {
                return false;
            }
        }
        return true;
    }

    THashMap<ui64, TShardInfo>& GetShards() {
        return ShardsInfo;
    }

    i64 GetMemory() const {
        return Memory;
    }

    void Clear() {
        ShardsInfo = {};
        Memory = 0;
        Closed = false;
    }

    void Close() {
        Closed = true;
    }

private:
    THashMap<ui64, TShardInfo> ShardsInfo;
    i64 Memory = 0;
    ui64 NextCookie = 1;
    bool Closed = false;
};

class TShardedWriteController : public IShardedWriteController {
public:
    void OnPartitioningChanged(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry) override {
        BeforePartitioningChanged();
        Serializer = CreateColumnShardPayloadSerializer(
            schemeEntry,
            InputColumnsMetadata);
        AfterPartitioningChanged();
    }

    void OnPartitioningChanged(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        NSchemeCache::TSchemeCacheRequest::TEntry&& partitionsEntry) override {
        BeforePartitioningChanged();
        Serializer = CreateDataShardPayloadSerializer(
            schemeEntry,
            std::move(partitionsEntry),
            InputColumnsMetadata);
        AfterPartitioningChanged();
    }

    void BeforePartitioningChanged() {
        if (Serializer) {
            if (!Closed) {
                Serializer->Close();
            }
            FlushSerializer(true);
        }
    }

    void AfterPartitioningChanged() {
        ShardsInfo.Close();
        ReshardData();
        ShardsInfo.Clear();
        if (Closed) {
            Close();
        } else {
            FlushSerializer(GetMemory() >= Settings.MemoryLimitTotal);
        }
    }

    void AddData(NMiniKQL::TUnboxedValueBatch&& data) override {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);

        auto allocGuard = TypeEnv.BindAllocator();
        YQL_ENSURE(Serializer);
        Serializer->AddData(std::move(data));

        FlushSerializer(GetMemory() >= Settings.MemoryLimitTotal);
    }

    void Close() override {
        auto allocGuard = TypeEnv.BindAllocator();
        YQL_ENSURE(Serializer);
        Closed = true;
        Serializer->Close();
        FlushSerializer(true);
        YQL_ENSURE(Serializer->IsFinished());
        ShardsInfo.Close();
    }

    TVector<ui64> GetPendingShards() const override {
        return ShardsInfo.GetPendingShards();
    }

    std::optional<TMessageMetadata> GetMessageMetadata(ui64 shardId) override {
        auto& shardInfo = ShardsInfo.GetShard(shardId);
        if (shardInfo.IsEmpty()) {
            return {};
        }
        BuildBatchesForShard(shardInfo);

        TMessageMetadata meta;
        meta.Cookie = shardInfo.GetCookie();
        meta.OperationsCount = shardInfo.GetBatchesInFlight();
        meta.IsFinal = shardInfo.IsClosed() && shardInfo.Size() == shardInfo.GetBatchesInFlight();
        meta.SendAttempts = shardInfo.GetSendAttempts();

        return meta;
    }

    TSerializationResult SerializeMessageToPayload(ui64 shardId, NKikimr::NEvents::TDataEvents::TEvWrite& evWrite) override {
        TSerializationResult result;

        const auto& shardInfo = ShardsInfo.GetShard(shardId);
        if (shardInfo.IsEmpty()) {
            return result;
        }

        for (size_t index = 0; index < shardInfo.GetBatchesInFlight(); ++index) {
            const auto& inFlightBatch = shardInfo.GetBatch(index);
            YQL_ENSURE(!inFlightBatch->IsEmpty());
            result.TotalDataSize += inFlightBatch->GetMemory();
            const ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(evWrite)
                    .AddDataToPayload(inFlightBatch->SerializeToString());
            result.PayloadIndexes.push_back(payloadIndex);
        }

        return result;
    }

    NKikimrDataEvents::EDataFormat GetDataFormat() override {
        return Serializer->GetDataFormat();
    }

    std::vector<ui32> GetWriteColumnIds() override {
        return Serializer->GetWriteColumnIds();
    }

    std::optional<i64> OnMessageAcknowledged(ui64 shardId, ui64 cookie) override {
        auto allocGuard = TypeEnv.BindAllocator();
        auto& shardInfo = ShardsInfo.GetShard(shardId);
        const auto removedDataSize = shardInfo.PopBatches(cookie);
        return removedDataSize;
    }

    void OnMessageSent(ui64 shardId, ui64 cookie) override {
        auto& shardInfo = ShardsInfo.GetShard(shardId);
        if (shardInfo.IsEmpty() || shardInfo.GetCookie() != cookie) {
            return;
        }
        shardInfo.IncSendAttempts();
    }

    void ResetRetries(ui64 shardId, ui64 cookie) override {
        auto& shardInfo = ShardsInfo.GetShard(shardId);
        if (shardInfo.IsEmpty() || shardInfo.GetCookie() != cookie) {
            return;
        }
        shardInfo.ResetSendAttempts();
    }

    i64 GetMemory() const override {
        YQL_ENSURE(Serializer);
        return Serializer->GetMemory() + ShardsInfo.GetMemory();
    }

    bool IsClosed() const override {
        return Closed;
    }

    bool IsFinished() const override {
        return IsClosed() && Serializer->IsFinished() && ShardsInfo.IsFinished();
    }

    bool IsReady() const override {
        return Serializer != nullptr;
    }

    TShardedWriteController(
        const TShardedWriteControllerSettings settings,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& inputColumnsMetadata,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : Settings(settings)
        , InputColumnsMetadata(std::move(inputColumnsMetadata))
        , TypeEnv(typeEnv)
        , Alloc(alloc) {
    }

    ~TShardedWriteController() {
        Y_ABORT_UNLESS(Alloc);
        TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
        ShardsInfo.Clear();
        Serializer = nullptr;
    }

private:
    void FlushSerializer(bool force) {
        if (force) {
            for (auto& [shardId, batches] : Serializer->FlushBatchesForce()) {
                for (auto& batch : batches) {
                    ShardsInfo.GetShard(shardId).PushBatch(std::move(batch));
                }
            }
        } else {
            for (const ui64 shardId : Serializer->GetShardIds()) {
                auto& shard = ShardsInfo.GetShard(shardId);
                while (true) {
                    auto batch = Serializer->FlushBatch(shardId);
                    if (!batch || batch->IsEmpty()) {
                        break;
                    }
                    shard.PushBatch(std::move(batch));
                }
            }
        }
    }

    void BuildBatchesForShard(TShardsInfo::TShardInfo& shard) {
        if (shard.GetBatchesInFlight() == 0) {
            shard.MakeNextBatches(
                Settings.MemoryLimitPerMessage,
                Settings.MaxBatchesPerMessage);
        }
    }

    void ReshardData() {
        for (auto& [_, shardInfo] : ShardsInfo.GetShards()) {
            for (size_t index = 0; index < shardInfo.Size(); ++index) {
                Serializer->AddBatch(shardInfo.GetBatch(index));
            }
        }
    }

    TShardedWriteControllerSettings Settings;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> InputColumnsMetadata;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    TShardsInfo ShardsInfo;
    bool Closed = false;

    IPayloadSerializerPtr Serializer = nullptr;
};

}


IShardedWriteControllerPtr CreateShardedWriteController(
        const TShardedWriteControllerSettings& settings,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& inputColumns,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) {
    return MakeIntrusive<TShardedWriteController>(
        settings, std::move(inputColumns), typeEnv, alloc);
}

}
}
