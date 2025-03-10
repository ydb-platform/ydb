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
constexpr ui64 ColumnShardMaxOperationBytes = 64_MB;

class IPayloadSerializer : public TThrRefBase {
public:
    class IBatch : public TThrRefBase {
    public:
        virtual TString SerializeToString() const = 0;
        virtual i64 GetMemory() const = 0;
        bool IsEmpty() const;
    };

    using IBatchPtr = TIntrusivePtr<IBatch>;

    virtual void AddData(const NMiniKQL::TUnboxedValueBatch& data) = 0;
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
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(column.GetTypeId(), column.GetTypeInfo());
        result.emplace_back(
            column.GetName(),
            column.GetId(),
            std::move(typeInfo),
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
            YQL_ENSURE(column.HasTypeId());
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
            auto typeDesc = type.GetPgTypeDesc();
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

        if (TCell::CanInline(ref.Size())) {
            return TCell(ref.Data(), ref.Size());
        } else {
            char* initialPtr = dataPtr;
            std::memcpy(initialPtr, ref.Data(), ref.Size());
            dataPtr += ref.Size();
            return TCell(initialPtr, ref.Size());
        }
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
        const auto s = cellInfo.Value.AsStringRef().Size();
        return TCell::CanInline(s) ? 0 : s;
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

    void AddData(const NMiniKQL::TUnboxedValueBatch& data) override {
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
        if (BatchBuilder.Bytes() > 0 && force) {
            const auto unshardedBatch = BatchBuilder.FlushBatch(true);
            YQL_ENSURE(unshardedBatch);
            ShardAndFlushBatch(unshardedBatch, force);
        }
    }

    void ShardAndFlushBatch(const TRecordBatchPtr& unshardedBatch, bool force) {
        for (auto [shardId, shardBatch] : Sharding->SplitByShardsToArrowBatches(unshardedBatch)) {
            const i64 shardBatchMemory = NArrow::GetBatchDataSize(shardBatch);
            YQL_ENSURE(shardBatchMemory != 0);

            ShardIds.insert(shardId);
            auto& unpreparedBatch = UnpreparedBatches[shardId];
            unpreparedBatch.TotalDataSize += shardBatchMemory;
            unpreparedBatch.Batches.emplace_back(shardBatch);
            Memory += shardBatchMemory;

            FlushUnpreparedBatch(shardId, unpreparedBatch, force);
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
            YQL_ENSURE(rowWithData.Cells.size() == ColumnCount);
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
        const NSchemeCache::TSchemeCacheRequest::TEntry& partitionsEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns)
        : SchemeEntry(schemeEntry)
        , KeyDescription(partitionsEntry.KeyDescription)
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

    void AddData(const NMiniKQL::TUnboxedValueBatch& data) override {
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
    const THolder<TKeyDesc>& KeyDescription;

    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    const std::vector<ui32> WriteColumnIds;
    const TVector<NScheme::TTypeInfo> KeyColumnTypes;

    THashMap<ui64, TRowsBatcher> Batchers;
    THashSet<ui64> ShardIds;

    i64 Memory = 0;

    bool Closed = false;
};

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
        const NSchemeCache::TSchemeCacheRequest::TEntry& partitionsEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    return MakeIntrusive<TDataShardPayloadSerializer>(
        schemeEntry, partitionsEntry, inputColumns);
}

}

namespace {

struct TMetadata {
    const TTableId TableId;
    const NKikimrDataEvents::TEvWrite::TOperation::EOperationType OperationType;
    const TVector<NKikimrKqp::TKqpColumnMetadataProto> InputColumnsMetadata;
    const i64 Priority;
};

struct TBatchWithMetadata {
    IShardedWriteController::TWriteToken Token = std::numeric_limits<IShardedWriteController::TWriteToken>::max();
    IPayloadSerializer::IBatchPtr Data = nullptr;
    bool HasRead = false;

    bool IsCoveringBatch() const {
        return Data == nullptr;
    }

    i64 GetMemory() const {
        return IsCoveringBatch() ? 0 : Data->GetMemory();
    }
};

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
            YQL_ENSURE(!IsEmpty());
            YQL_ENSURE(maxCount != 0);
            i64 dataSize = 0;
            // For columnshard batch can be slightly larger than the limit.
            while (BatchesInFlight < maxCount
                    && BatchesInFlight < Batches.size()
                    && (dataSize + GetBatch(BatchesInFlight).GetMemory() <= maxDataSize || BatchesInFlight == 0)) {
                dataSize += GetBatch(BatchesInFlight).GetMemory();
                ++BatchesInFlight;
            }
            YQL_ENSURE(BatchesInFlight != 0);
            YQL_ENSURE(BatchesInFlight == Batches.size() || BatchesInFlight >= maxCount || dataSize + GetBatch(BatchesInFlight).GetMemory() > maxDataSize);
        }

        const TBatchWithMetadata& GetBatch(size_t index) const {
            return Batches.at(index);
        }

        struct TBatchInfo {
            ui64 DataSize = 0;
        };
        std::optional<TBatchInfo> PopBatches(const ui64 cookie) {
            if (BatchesInFlight != 0 && Cookie == cookie) {
                TBatchInfo result;
                for (size_t index = 0; index < BatchesInFlight; ++index) {
                    result.DataSize += Batches.front().GetMemory();
                    Batches.pop_front();
                }

                Cookie = NextCookie++;
                SendAttempts = 0;
                BatchesInFlight = 0;

                Memory -= result.DataSize;
                return result;
            }
            return std::nullopt;
        }

        void PushBatch(TBatchWithMetadata&& batch) {
            YQL_ENSURE(!IsClosed());
            Batches.emplace_back(std::move(batch));
            Memory += Batches.back().GetMemory();
            HasReadInBatch |= Batches.back().HasRead;
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

        bool HasRead() const {
            return HasReadInBatch;
        }

    private:
        std::deque<TBatchWithMetadata> Batches;
        i64& Memory;
        bool HasReadInBatch = false;

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

    TVector<IShardedWriteController::TPendingShardInfo> GetPendingShards() const {
        TVector<IShardedWriteController::TPendingShardInfo> result;
        for (const auto& [id, shard] : ShardsInfo) {
            if (!shard.IsEmpty() && shard.GetSendAttempts() == 0) {
                result.push_back(IShardedWriteController::TPendingShardInfo{
                    .ShardId = id,
                    .HasRead = shard.HasRead(),
                });
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

    const THashMap<ui64, TShardInfo>& GetShards() const {
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
        IsOlap = true;
        SchemeEntry = schemeEntry;
        BeforePartitioningChanged();
        for (auto& [_, writeInfo] : WriteInfos) {
            writeInfo.Serializer = CreateColumnShardPayloadSerializer(
                *SchemeEntry,
                writeInfo.Metadata.InputColumnsMetadata);
        }
        AfterPartitioningChanged();
    }

    void OnPartitioningChanged(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        NSchemeCache::TSchemeCacheRequest::TEntry&& partitionsEntry) override {
        IsOlap = false;
        SchemeEntry = schemeEntry;
        PartitionsEntry = std::move(partitionsEntry);
        BeforePartitioningChanged();
        for (auto& [_, writeInfo] : WriteInfos) {
            writeInfo.Serializer = CreateDataShardPayloadSerializer(
                *SchemeEntry,
                *PartitionsEntry,
                writeInfo.Metadata.InputColumnsMetadata);
        }
        AfterPartitioningChanged();
    }

    void BeforePartitioningChanged() {
        if (!Settings.Inconsistent) {
            return;
        }
        for (auto& [token, writeInfo] : WriteInfos) {
            if (writeInfo.Serializer) {
                if (!writeInfo.Closed) {
                    writeInfo.Serializer->Close();
                }
                FlushSerializer(token, true);
                writeInfo.Serializer = nullptr;
            }
        }
    }

    void AfterPartitioningChanged() {
        if (!Settings.Inconsistent) {
            return;
        }
        if (!WriteInfos.empty()) {
            ShardsInfo.Close();
            ReshardData();
            ShardsInfo.Clear();
            for (const auto& [token, writeInfo] : WriteInfos) {
                if (writeInfo.Closed) {
                    Close(token);
                } else {
                    FlushSerializer(token, GetMemory() >= Settings.MemoryLimitTotal);
                }
            }
        }
    }

    TWriteToken Open(
        const TTableId tableId,
        const NKikimrDataEvents::TEvWrite::TOperation::EOperationType operationType,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& inputColumns,
        const i64 priority) override {
        auto token = CurrentWriteToken++;
        auto iter = WriteInfos.emplace(
            token,
            TWriteInfo {
                .Metadata = TMetadata {
                    .TableId = tableId,
                    .OperationType = operationType,
                    .InputColumnsMetadata = std::move(inputColumns),
                    .Priority = priority,
                },
                .Serializer = nullptr,
                .Closed = false,
            }).first;
        if (PartitionsEntry) {
            iter->second.Serializer = CreateDataShardPayloadSerializer(
                *SchemeEntry,
                *PartitionsEntry,
                iter->second.Metadata.InputColumnsMetadata);
        } else if (SchemeEntry) {
            iter->second.Serializer = CreateColumnShardPayloadSerializer(
                *SchemeEntry,
                iter->second.Metadata.InputColumnsMetadata);
        }
        return token;
    }

    void Write(TWriteToken token, const NMiniKQL::TUnboxedValueBatch& data) override {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        auto& info = WriteInfos.at(token);
        YQL_ENSURE(!info.Closed);

        auto allocGuard = TypeEnv.BindAllocator();
        YQL_ENSURE(info.Serializer);
        info.Serializer->AddData(data);

        if (info.Metadata.Priority == 0) {
            FlushSerializer(token, GetMemory() >= Settings.MemoryLimitTotal);
        } else {
            YQL_ENSURE(GetMemory() <= Settings.MemoryLimitTotal);
        }
    }

    void Close(TWriteToken token) override {
        auto allocGuard = TypeEnv.BindAllocator();
        auto& info = WriteInfos.at(token);
        YQL_ENSURE(info.Serializer);
        info.Closed = true;
        info.Serializer->Close();
        if (info.Metadata.Priority == 0) {
            FlushSerializer(token, true);
            YQL_ENSURE(info.Serializer->IsFinished());
        }
    }

    void FlushBuffers() override {
        TVector<TWriteToken> writeTokensFoFlush;
        for (const auto& [token, writeInfo] : WriteInfos) {
            YQL_ENSURE(writeInfo.Closed);
            if (writeInfo.Metadata.Priority != 0) {
                if (!writeInfo.Serializer->IsFinished()) {
                    writeTokensFoFlush.push_back(token);
                }
            } else {
                YQL_ENSURE(writeInfo.Serializer->IsFinished());
            }
        }

        std::sort(
            std::begin(writeTokensFoFlush),
            std::end(writeTokensFoFlush),
            [&](const TWriteToken& lhs, const TWriteToken& rhs) {
                const auto& leftWriteInfo = WriteInfos.at(lhs);
                const auto& rightWriteInfo = WriteInfos.at(rhs);
                return leftWriteInfo.Metadata.Priority < rightWriteInfo.Metadata.Priority;
            });
        
        for (const TWriteToken token : writeTokensFoFlush) {
            FlushSerializer(token, true);
            YQL_ENSURE(WriteInfos.at(token).Serializer->IsFinished());
        }
    }

    void Close() override {
        ShardsInfo.Close();
    }

    void AddCoveringMessages() override {
        for (auto& [_, shardInfo] : ShardsInfo.GetShards()) {
            shardInfo.PushBatch(TBatchWithMetadata{});
        }
    }

    TVector<TPendingShardInfo> GetPendingShards() const override {
        return ShardsInfo.GetPendingShards();
    }

    TVector<ui64> GetShardsIds() const override {
        TVector<ui64> result;
        result.reserve(ShardsInfo.GetShards().size());
        for (const auto& [id, _] : ShardsInfo.GetShards()) {
            result.push_back(id);
        }
        return result;
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
            if (inFlightBatch.Data) {
                YQL_ENSURE(!inFlightBatch.Data->IsEmpty());
                result.TotalDataSize += inFlightBatch.Data->GetMemory();
                const ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(evWrite)
                        .AddDataToPayload(inFlightBatch.Data->SerializeToString());
                const auto& writeInfo = WriteInfos.at(inFlightBatch.Token);
                evWrite.AddOperation(
                    writeInfo.Metadata.OperationType,
                    writeInfo.Metadata.TableId,
                    writeInfo.Serializer->GetWriteColumnIds(),
                    payloadIndex,
                    writeInfo.Serializer->GetDataFormat());
            } else {
                YQL_ENSURE(index + 1 == shardInfo.GetBatchesInFlight());   
            }
        }

        return result;
    }

    std::optional<TMessageAcknowledgedResult> OnMessageAcknowledged(ui64 shardId, ui64 cookie) override {
        auto allocGuard = TypeEnv.BindAllocator();
        auto& shardInfo = ShardsInfo.GetShard(shardId);
        const auto result = shardInfo.PopBatches(cookie);
        if (result) {
            return TMessageAcknowledgedResult {
                .DataSize = result->DataSize,
                .IsShardEmpty = shardInfo.IsEmpty(),
            };
        }
        return std::nullopt;
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
        i64 total = ShardsInfo.GetMemory();
        for (const auto& [_, writeInfo] : WriteInfos) {
            if (writeInfo.Serializer) {
                total += writeInfo.Serializer->GetMemory();
            } else {
                YQL_ENSURE(writeInfo.Closed);
            }
        }
        return total;
    }

    bool IsAllWritesClosed() const override {
        for (const auto& [_, writeInfo] : WriteInfos) {
            if (!writeInfo.Closed) {
                return false;
            }
        }
        return true;
    }

    bool IsAllWritesFinished() const override {
        for (const auto& [_, writeInfo] : WriteInfos) {
            if (!writeInfo.Closed || !writeInfo.Serializer->IsFinished()) {
                return false;
            }
        }
        return ShardsInfo.IsFinished();
    }

    bool IsReady() const override {
        for (const auto& [_, writeInfo] : WriteInfos) {
            if (!writeInfo.Serializer && !writeInfo.Closed) {
                return false;
            }
        }
        return true;
    }

    bool IsEmpty() const override {
        for (const auto& [_, writeInfo] : WriteInfos) {
            if (writeInfo.Serializer && !writeInfo.Serializer->IsEmpty()) {
                return false;
            }
        }
        return ShardsInfo.IsEmpty();
    }

    ui64 GetShardsCount() const override {
        return ShardsInfo.GetShards().size();
    }

    TShardedWriteController(
        const TShardedWriteControllerSettings settings,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : Settings(settings)
        , TypeEnv(typeEnv)
        , Alloc(alloc) {
    }

    ~TShardedWriteController() {
        Y_ABORT_UNLESS(Alloc);
        TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
        ShardsInfo.Clear();
        for (auto& [_, writeInfo] : WriteInfos) {
            writeInfo.Serializer = nullptr;
        }
    }

private:
    void FlushSerializer(TWriteToken token, bool force) {
        if (force) {
            const auto& writeInfo = WriteInfos.at(token);
            for (auto& [shardId, batches] : writeInfo.Serializer->FlushBatchesForce()) {
                for (auto& batch : batches) {
                    if (batch && !batch->IsEmpty()) {
                        ShardsInfo.GetShard(shardId).PushBatch(TBatchWithMetadata{
                            .Token = token,
                            .Data = std::move(batch),
                            .HasRead = (writeInfo.Metadata.OperationType != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE
                                && writeInfo.Metadata.OperationType != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT),
                        });
                    }
                }
            }
        } else {
            const auto& writeInfo = WriteInfos.at(token);
            for (const ui64 shardId : writeInfo.Serializer->GetShardIds()) {
                auto& shard = ShardsInfo.GetShard(shardId);
                while (true) {
                    auto batch = writeInfo.Serializer->FlushBatch(shardId);
                    if (!batch || batch->IsEmpty()) {
                        break;
                    }
                    shard.PushBatch(TBatchWithMetadata{
                        .Token = token,
                        .Data = std::move(batch),
                        .HasRead = (writeInfo.Metadata.OperationType != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE
                            && writeInfo.Metadata.OperationType != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT),
                    });
                }
            }
        }
    }

    void BuildBatchesForShard(TShardsInfo::TShardInfo& shard) {
        if (shard.GetBatchesInFlight() == 0) {
            YQL_ENSURE(IsOlap != std::nullopt);
            shard.MakeNextBatches(
                Settings.MemoryLimitPerMessage,
                (*IsOlap) ? 1 : Settings.MaxBatchesPerMessage);
        }
    }

    void ReshardData() {
        YQL_ENSURE(!Settings.Inconsistent);
        for (auto& [_, shardInfo] : ShardsInfo.GetShards()) {
            for (size_t index = 0; index < shardInfo.Size(); ++index) {
                const auto& batch = shardInfo.GetBatch(index);
                const auto& writeInfo = WriteInfos.at(batch.Token);
                // Resharding supported only for inconsistent write,
                // so convering empty batches don't exist in this case.
                YQL_ENSURE(batch.Data);
                writeInfo.Serializer->AddBatch(batch.Data);
            }
        }
    }

    TShardedWriteControllerSettings Settings;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    struct TWriteInfo {
        TMetadata Metadata;
        IPayloadSerializerPtr Serializer = nullptr;
        bool Closed = false;
    };

    std::map<TWriteToken, TWriteInfo> WriteInfos;
    TWriteToken CurrentWriteToken = 0;

    TShardsInfo ShardsInfo;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    std::optional<NSchemeCache::TSchemeCacheRequest::TEntry> PartitionsEntry;
    std::optional<bool> IsOlap;
};

}


IShardedWriteControllerPtr CreateShardedWriteController(
        const TShardedWriteControllerSettings& settings,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) {
    return MakeIntrusive<TShardedWriteController>(
        settings, typeEnv, alloc);
}

}
}
