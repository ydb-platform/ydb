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
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr {
namespace NKqp {

namespace {

constexpr ui64 DataShardMaxOperationBytes = 8_MB;
constexpr ui64 ColumnShardMaxOperationBytes = 64_MB;

using TCharVectorPtr = std::unique_ptr<TVector<char>>;

class TColumnBatch : public IDataBatch {
public:
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    TString SerializeToString() const override {
        return NArrow::SerializeBatchNoCompression(Data);
    }

    i64 GetMemory() const override {
        return Memory;
    }

    bool IsEmpty() const override {
        return GetMemory() == 0;
    }

    TRecordBatchPtr Extract() {
        Memory = 0;
        TRecordBatchPtr result = std::move(Data);
        return result;
    }

    std::shared_ptr<void> ExtractBatch() override {
        return std::dynamic_pointer_cast<void>(Extract());
    }

    explicit TColumnBatch(const TRecordBatchPtr& data)
        : Data(data)
        , Memory(NArrow::GetBatchDataSize(Data)) {
    }

private:
    TRecordBatchPtr Data;
    i64 Memory;
};


class TRowBatch : public IDataBatch {
public:
    TString SerializeToString() const override {
        return TSerializedCellMatrix::Serialize(Cells, Rows, Columns);
    }

    i64 GetMemory() const override {
        return Size;
    }

    bool IsEmpty() const override {
        return Cells.empty();
    }

    std::pair<std::vector<TCell>, std::vector<TCharVectorPtr>> Extract() {
        Size = 0;
        Rows = 0;
        return {std::move(Cells), std::move(Data)};
    }

    std::shared_ptr<void> ExtractBatch() override {
        auto r = std::make_shared<std::pair<std::vector<TCell>, std::vector<TCharVectorPtr>>>(std::move(Extract()));
        return std::reinterpret_pointer_cast<void>(r);
    }

    TRowBatch(std::vector<TCell>&& cells, std::vector<TCharVectorPtr>&& data, i64 size, ui32 rows, ui16 columns)
        : Cells(std::move(cells))
        , Data(std::move(data))
        , Size(size)
        , Rows(rows)
        , Columns(columns) {
    }

private:
    std::vector<TCell> Cells;
    std::vector<TCharVectorPtr> Data;
    ui64 Size = 0;
    ui32 Rows = 0;
    ui16 Columns = 0;
};

class IPayloadSerializer : public TThrRefBase {
public:
    virtual void AddData(IDataBatchPtr&& batch) = 0;
    virtual void AddBatch(IDataBatchPtr&& batch) = 0;

    virtual void Close() = 0;

    virtual bool IsClosed() = 0;
    virtual bool IsEmpty() = 0;
    virtual bool IsFinished() = 0;

    virtual NKikimrDataEvents::EDataFormat GetDataFormat() = 0;
    virtual std::vector<ui32> GetWriteColumnIds() = 0;

    using TBatches = THashMap<ui64, std::deque<IDataBatchPtr>>;

    virtual TBatches FlushBatchesForce() = 0;

    virtual IDataBatchPtr FlushBatch(ui64 shardId) = 0;
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
    const std::vector<ui32>& writeIndex,
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns) {
    std::vector<std::pair<TString, NScheme::TTypeInfo>> result(writeIndex.size());
    for (size_t index = 0; index < inputColumns.size(); ++index) {
        const auto& column = inputColumns[index];
        YQL_ENSURE(column.HasTypeId());
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
            column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
        result[writeIndex[index]].first = column.GetName();
        result[writeIndex[index]].second = typeInfoMod.TypeInfo;
    }
    return result;
}

TVector<NScheme::TTypeInfo> BuildKeyColumnTypes(
    const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns) {
    TVector<NScheme::TTypeInfo> keyColumnTypes;
    keyColumnTypes.reserve(keyColumns.size());
    for (const auto& column : keyColumns) {
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
            column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
        keyColumnTypes.push_back(typeInfoMod.TypeInfo);
    }
    return keyColumnTypes;
}

struct TRowWithData {
    TVector<TCell> Cells;
    TCharVectorPtr Data;
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
            const TString& typeMod) {
        CellsInfo[index].Type = type;
        CellsInfo[index].Value = value;

        if (type.GetTypeId() == NScheme::NTypeIds::Pg && value) {
            auto typeDesc = type.GetPgTypeDesc();
            if (!typeMod.empty() && NPg::TypeDescNeedsCoercion(typeDesc)) {

                auto typeModResult = NPg::BinaryTypeModFromTextTypeMod(typeMod, type.GetPgTypeDesc());
                if (typeModResult.Error) {
                    ythrow yexception() << "BinaryTypeModFromTextTypeMod error: " << *typeModResult.Error;
                }

                YQL_ENSURE(typeModResult.Typmod != -1);
                TMaybe<TString> err;
                CellsInfo[index].PgBinaryValue = NYql::NCommon::PgValueCoerce(value, NPg::PgTypeIdFromTypeDesc(typeDesc), typeModResult.Typmod, &err);
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
        char* ptr = data->data();

        for (const auto& cellInfo : CellsInfo) {
            cells.push_back(BuildCell(cellInfo, ptr));
        }

        AFL_ENSURE(ptr == data->data() + size);

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

        const bool isPg = cellInfo.Type.GetTypeId() == NScheme::NTypeIds::Pg;

        const auto ref = isPg
            ? NYql::NUdf::TStringRef(cellInfo.PgBinaryValue)
            : cellInfo.Value.AsStringRef();

        if (!isPg && TCell::CanInline(ref.Size())) {
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

        const bool isPg = cellInfo.Type.GetTypeId() == NScheme::NTypeIds::Pg;

        const auto ref = isPg
            ? NYql::NUdf::TStringRef(cellInfo.PgBinaryValue)
            : cellInfo.Value.AsStringRef();

        return (!isPg && TCell::CanInline(ref.Size())) ? 0 : ref.Size();
    }

    TCharVectorPtr Allocate(size_t size) {
        return std::make_unique<TVector<char>>(size);
    }

    TVector<TCellInfo> CellsInfo;
};

class TColumnDataBatcher : public IDataBatcher {
public:
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    TColumnDataBatcher(
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex)
            : Columns(BuildColumns(inputColumns))
            , WriteIndex(std::move(writeIndex))
            , BatchBuilder(arrow::Compression::UNCOMPRESSED, BuildNotNullColumns(inputColumns)) {
        TString err;
        if (!BatchBuilder.Start(BuildBatchBuilderColumns(WriteIndex, inputColumns), 0, 0, err)) {
            yexception() << "Failed to start batch builder: " + err;
        }
    }

    void AddData(const NMiniKQL::TUnboxedValueBatch& data) override {
        TRowBuilder rowBuilder(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                rowBuilder.AddCell(
                    WriteIndex[index],
                    Columns[index].PType,
                    row.GetElement(index),
                    Columns[index].PTypeMod);
            }
            auto rowWithData = rowBuilder.Build();
            BatchBuilder.AddRow(TConstArrayRef<TCell>{rowWithData.Cells.begin(), rowWithData.Cells.end()});
        });
    }

    i64 GetMemory() const override {
        return BatchBuilder.Bytes();
    }

    IDataBatchPtr Build() override {
        return MakeIntrusive<TColumnBatch>(BatchBuilder.FlushBatch(true));
    }

private:
    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    NArrow::TArrowBatchBuilder BatchBuilder;
};

class TColumnShardPayloadSerializer : public IPayloadSerializer {
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;
    using TBatch = TColumnBatch;

    struct TUnpreparedBatch {
        ui64 TotalDataSize = 0;
        std::deque<TRecordBatchPtr> Batches; 
    };

public:
    TColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const std::vector<ui32> writeIndex) // key columns then value columns
            : Columns(BuildColumns(inputColumns))
            , WriteColumnIds(BuildWriteColumnIds(inputColumns, writeIndex)) {
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

    void AddData(IDataBatchPtr&& batch) override {
        YQL_ENSURE(!Closed);
        AddBatch(std::move(batch));
    }

    void AddBatch(IDataBatchPtr&& batch) override {
        auto columnshardBatch = dynamic_cast<TBatch*>(batch.Get());
        YQL_ENSURE(columnshardBatch);
        if (columnshardBatch->IsEmpty()) {
            return;
        }
        auto data = columnshardBatch->Extract();
        YQL_ENSURE(data);
        ShardAndFlushBatch(data, false);
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
        return Memory;
    }

    void Close() override {
        YQL_ENSURE(!Closed);
        Closed = true;
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

    IDataBatchPtr FlushBatch(ui64 shardId) override {
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
    const std::vector<ui32> WriteColumnIds;

    THashMap<ui64, TUnpreparedBatch> UnpreparedBatches;
    TBatches Batches;
    THashSet<ui64> ShardIds;

    i64 Memory = 0;

    bool Closed = false;
};

class TRowsBatcher {
public:
    explicit TRowsBatcher(ui16 columnCount, std::optional<ui64> maxBytesPerBatch)
        : ColumnCount(columnCount)
        , MaxBytesPerBatch(maxBytesPerBatch) {
    }

    bool IsEmpty() const {
        return Batches.empty();
    }

    struct TBatch {
        i64 Memory = 0;
        i64 MemorySerialized = 0;
        TVector<TCell> Cells;
        TVector<TCharVectorPtr> Data;
    };

    TBatch Flush(bool force) {
        TBatch res;
        if ((!Batches.empty() && force) || Batches.size() > 1) {
            YQL_ENSURE(MaxBytesPerBatch || Batches.size() == 1);
            res = std::move(Batches.front());
            Batches.pop_front();
            Memory -= res.Memory;
        }
        return res;
    }

    ui64 AddRow(TRowWithData&& rowWithData) {
        YQL_ENSURE(rowWithData.Cells.size() == ColumnCount);
        i64 newMemory = 0;
        for (const auto& cell : rowWithData.Cells) {
            newMemory += cell.Size();
        }
        if (Batches.empty() || (MaxBytesPerBatch && newMemory + GetCellHeaderSize() * ColumnCount + Batches.back().MemorySerialized > *MaxBytesPerBatch)) {
            Batches.emplace_back();
            Batches.back().Memory = 0;
            Batches.back().MemorySerialized = GetCellMatrixHeaderSize();
        }

        for (auto& cell : rowWithData.Cells) {
            Batches.back().Cells.emplace_back(std::move(cell));
        }
        Batches.back().Data.emplace_back(std::move(rowWithData.Data));

        Memory += newMemory;
        Batches.back().Memory += newMemory;
        Batches.back().MemorySerialized += newMemory + GetCellHeaderSize() * ColumnCount;

        return newMemory;
    }

    i64 GetMemory() const {
        return Memory;
    }

private:
    std::deque<TBatch> Batches;
    ui16 ColumnCount;
    std::optional<ui64> MaxBytesPerBatch;
    i64 Memory = 0;
};

class TRowDataBatcher : public IDataBatcher {
public:
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    TRowDataBatcher(
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex)
            : Columns(BuildColumns(inputColumns))
            , WriteIndex(std::move(writeIndex))
            , RowBatcher(Columns.size(), std::nullopt) {
    }

    void AddData(const NMiniKQL::TUnboxedValueBatch& data) override {
        TRowBuilder rowBuilder(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                rowBuilder.AddCell(
                    WriteIndex[index],
                    Columns[index].PType,
                    row.GetElement(index),
                    Columns[index].PTypeMod);
            }
            auto rowWithData = rowBuilder.Build();
            RowBatcher.AddRow(std::move(rowWithData));
        });
    }

    i64 GetMemory() const override {
        return RowBatcher.GetMemory();
    }

    IDataBatchPtr Build() override {
        auto batch = RowBatcher.Flush(true);
        const ui32 rows = batch.Cells.size() / Columns.size();

        return MakeIntrusive<TRowBatch>(
            std::move(batch.Cells),
            std::move(batch.Data),
            batch.MemorySerialized,
            rows,
            static_cast<ui16>(Columns.size()));
    }

private:
    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    TRowsBatcher RowBatcher;
};

class TDataShardPayloadSerializer : public IPayloadSerializer {
    using TBatch = TRowBatch;

public:
    TDataShardPayloadSerializer(
        const TVector<TKeyDesc::TPartitionInfo>& partitioning,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto>& inputColumns,
        std::vector<ui32> writeIndex)
        : Partitioning(partitioning)
        , Columns(BuildColumns(inputColumns))
        , WriteIndex(std::move(writeIndex))
        , WriteColumnIds(BuildWriteColumnIds(inputColumns, WriteIndex))
        , KeyColumnTypes(BuildKeyColumnTypes(keyColumns)) {
    }

    void AddRow(TRowWithData&& row, const TVector<TKeyDesc::TPartitionInfo>& partitioning) {
        YQL_ENSURE(row.Cells.size() >= KeyColumnTypes.size());
        auto shardIter = std::lower_bound(
            std::begin(partitioning),
            std::end(partitioning),
            TArrayRef(row.Cells.data(), KeyColumnTypes.size()),
            [this](const auto &partition, const auto& key) {
                const auto& range = *partition.Range;
                return 0 > CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                    range.IsInclusive || range.IsPoint, true, KeyColumnTypes);
            });

        YQL_ENSURE(shardIter != partitioning.end());

        auto batcherIter = Batchers.find(shardIter->ShardId);
        if (batcherIter == std::end(Batchers)) {
            Batchers.emplace(
                shardIter->ShardId,
                TRowsBatcher(Columns.size(), DataShardMaxOperationBytes));
        }

        Memory += Batchers.at(shardIter->ShardId).AddRow(std::move(row));
        ShardIds.insert(shardIter->ShardId);
    }

    void AddData(IDataBatchPtr&& data) override {
        YQL_ENSURE(!Closed);
        AddBatch(std::move(data));
    }

    void AddBatch(IDataBatchPtr&& batch) override {
        auto datashardBatch = dynamic_cast<TBatch*>(batch.Get());
        YQL_ENSURE(datashardBatch);
        auto [cells, data] = datashardBatch->Extract();
        const auto rows = cells.size() / Columns.size();
        YQL_ENSURE(cells.size() == rows * Columns.size());

        for (size_t rowIndex = 0; rowIndex < rows; ++rowIndex) {
            AddRow(
                TRowWithData{
                    TVector<TCell>(cells.begin() + (rowIndex * Columns.size()), cells.begin() + (rowIndex * Columns.size()) + Columns.size()),
                    std::move(data[rowIndex]),
                },
                Partitioning);
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

    IDataBatchPtr ExtractNextBatch(TRowsBatcher& batcher, bool force) {
        auto batchResult = batcher.Flush(force);
        Memory -= batchResult.Memory;
        const ui32 rows = batchResult.Cells.size() / Columns.size();
        YQL_ENSURE(Columns.size() <= std::numeric_limits<ui16>::max());
        return MakeIntrusive<TRowBatch>(
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

    IDataBatchPtr FlushBatch(ui64 shardId) override {
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
    const TVector<TKeyDesc::TPartitionInfo>& Partitioning;
    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    const std::vector<ui32> WriteColumnIds;
    const TVector<NScheme::TTypeInfo> KeyColumnTypes;

    THashMap<ui64, TRowsBatcher> Batchers;
    THashSet<ui64> ShardIds;

    i64 Memory = 0;

    bool Closed = false;
};
IPayloadSerializerPtr CreateColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const std::vector<ui32> writeIndex) {
    return MakeIntrusive<TColumnShardPayloadSerializer>(
        schemeEntry, inputColumns, std::move(writeIndex));
}

IPayloadSerializerPtr CreateDataShardPayloadSerializer(
        const TVector<TKeyDesc::TPartitionInfo>& partitioning,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> keyColumns,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const std::vector<ui32> writeIndex) {
    return MakeIntrusive<TDataShardPayloadSerializer>(
        partitioning, keyColumns, inputColumns, std::move(writeIndex));
}

}

IDataBatcherPtr CreateColumnDataBatcher(const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex) {
    return MakeIntrusive<TColumnDataBatcher>(inputColumns, std::move(writeIndex));
}

IDataBatcherPtr CreateRowDataBatcher(const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex) {
    return MakeIntrusive<TRowDataBatcher>(inputColumns, std::move(writeIndex));
}

bool IDataBatch::IsEmpty() const {
    return GetMemory() == 0;
}

namespace {

struct TMetadata {
    const TTableId TableId;
    const NKikimrDataEvents::TEvWrite::TOperation::EOperationType OperationType;
    const TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumnsMetadata;
    const TVector<NKikimrKqp::TKqpColumnMetadataProto> InputColumnsMetadata;
    const std::vector<ui32> WriteIndex;
    const i64 Priority;
};

struct TBatchWithMetadata {
    IShardedWriteController::TWriteToken Token = std::numeric_limits<IShardedWriteController::TWriteToken>::max();
    IDataBatchPtr Data = nullptr;
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

        void MakeNextBatches(i64 maxDataSize, std::optional<ui64> maxCount) {
            YQL_ENSURE(BatchesInFlight == 0);
            YQL_ENSURE(!IsEmpty());
            i64 dataSize = 0;
            // For columnshard batch can be slightly larger than the limit.
            while ((!maxCount || BatchesInFlight < *maxCount)
                    && BatchesInFlight < Batches.size()
                    && (dataSize + GetBatch(BatchesInFlight).GetMemory() <= maxDataSize || BatchesInFlight == 0)) {
                dataSize += GetBatch(BatchesInFlight).GetMemory();
                ++BatchesInFlight;
            }
            YQL_ENSURE(BatchesInFlight != 0);
            YQL_ENSURE(BatchesInFlight == Batches.size()
                || (maxCount && BatchesInFlight >= *maxCount)
                || dataSize + GetBatch(BatchesInFlight).GetMemory() > maxDataSize);
        }

        TBatchWithMetadata& GetBatch(size_t index) {
            return Batches.at(index);
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
                writeInfo.Metadata.InputColumnsMetadata,
                writeInfo.Metadata.WriteIndex);
        }
        AfterPartitioningChanged();
    }

    void OnPartitioningChanged(
        const std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>& partitioning) override {
        IsOlap = false;
        Partitioning = partitioning;
        BeforePartitioningChanged();
        for (auto& [_, writeInfo] : WriteInfos) {
            writeInfo.Serializer = CreateDataShardPayloadSerializer(
                *Partitioning,
                writeInfo.Metadata.KeyColumnsMetadata,
                writeInfo.Metadata.InputColumnsMetadata,
                writeInfo.Metadata.WriteIndex);
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
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& keyColumns,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& inputColumns,
        std::vector<ui32>&& writeIndex,
        const i64 priority) override {
        auto token = CurrentWriteToken++;
        auto iter = WriteInfos.emplace(
            token,
            TWriteInfo {
                .Metadata = TMetadata {
                    .TableId = tableId,
                    .OperationType = operationType,
                    .KeyColumnsMetadata = std::move(keyColumns),
                    .InputColumnsMetadata = std::move(inputColumns),
                    .WriteIndex = std::move(writeIndex),
                    .Priority = priority,
                },
                .Serializer = nullptr,
                .Closed = false,
            }).first;
        if (Partitioning) {
            iter->second.Serializer = CreateDataShardPayloadSerializer(
                *Partitioning,
                iter->second.Metadata.KeyColumnsMetadata,
                iter->second.Metadata.InputColumnsMetadata,
                iter->second.Metadata.WriteIndex);
        } else if (SchemeEntry) {
            iter->second.Serializer = CreateColumnShardPayloadSerializer(
                *SchemeEntry,
                iter->second.Metadata.InputColumnsMetadata,
                iter->second.Metadata.WriteIndex);
        }
        return token;
    }

    void Write(TWriteToken token, IDataBatchPtr&& data) override {
        auto& info = WriteInfos.at(token);
        YQL_ENSURE(!info.Closed);

        auto allocGuard = TypeEnv.BindAllocator();
        YQL_ENSURE(info.Serializer);
        info.Serializer->AddData(std::move(data));

        if (info.Metadata.Priority == 0) {
            FlushSerializer(token, GetMemory() >= Settings.MemoryLimitTotal);
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
            if (*IsOlap) {
                shard.MakeNextBatches(Settings.MemoryLimitPerMessage, 1);
            } else {
                shard.MakeNextBatches(Settings.MemoryLimitPerMessage, std::nullopt);
            }
        }
    }

    void ReshardData() {
        YQL_ENSURE(!Settings.Inconsistent);
        for (auto& [_, shardInfo] : ShardsInfo.GetShards()) {
            for (size_t index = 0; index < shardInfo.Size(); ++index) {
                auto& batch = shardInfo.GetBatch(index);
                const auto& writeInfo = WriteInfos.at(batch.Token);
                // Resharding supported only for inconsistent write,
                // so convering empty batches don't exist in this case.
                YQL_ENSURE(batch.Data);
                writeInfo.Serializer->AddBatch(std::move(batch.Data));
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
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
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
