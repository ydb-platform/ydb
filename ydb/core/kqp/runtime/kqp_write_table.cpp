#include "kqp_write_table.h"

#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/runtime/kqp_arrow_memory_pool.h>
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

constexpr i64 DataShardMaxOperationBytes = 8_MB;
constexpr i64 ColumnShardMaxOperationBytes = 64_MB;

constexpr size_t InitialBatchPoolSize = 64_KB;

class TOffloadedPoolAllocator : public IAllocator {
public:
    TOffloadedPoolAllocator(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> scopedAlloc)
        : Alloc(TDefaultAllocator::Instance())
        , ScopedAlloc(std::move(scopedAlloc)) 
        , AllocatedSize(0) {
    }

    ~TOffloadedPoolAllocator() {
        Y_DEBUG_ABORT_UNLESS(AllocatedSize == 0);
    }

    TBlock Allocate(size_t len) override {
        if (ScopedAlloc) {
            TGuard guard(*ScopedAlloc);
            ScopedAlloc->Ref().OffloadAlloc(len);
        }
        AllocatedSize += len;
        return Alloc->Allocate(len);
    }

    void Release(const TBlock& block) override {
        if (ScopedAlloc) {
            TGuard guard(*ScopedAlloc);
            ScopedAlloc->Ref().OffloadFree(block.Len);
        }
        AllocatedSize -= block.Len;
        Alloc->Release(block);
    }

    void Attach(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> scopedAlloc) {
        AFL_ENSURE(!ScopedAlloc);
        {
            TGuard guard(*scopedAlloc);
            scopedAlloc->Ref().OffloadAlloc(AllocatedSize);
        }
        ScopedAlloc = std::move(scopedAlloc);
    }

    void Detach() {
        AFL_ENSURE(ScopedAlloc);
        {
            TGuard guard(*ScopedAlloc);
            ScopedAlloc->Ref().OffloadFree(AllocatedSize);
        }
        ScopedAlloc.reset();
    }

    bool Attached() const {
        return ScopedAlloc != nullptr;
    }

    std::unique_ptr<TMemoryPool> CreateMemoryPool() {
        return std::make_unique<TMemoryPool>(InitialBatchPoolSize, TMemoryPool::TExpGrow::Instance(), this);
    }

private:
    IAllocator* Alloc;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> ScopedAlloc;

    size_t AllocatedSize;
};

using TOffloadedPoolAllocatorPtr = std::shared_ptr<TOffloadedPoolAllocator>;

TOffloadedPoolAllocatorPtr CreateOffloadedPoolAllocator(
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> scopedAlloc) {
    return std::make_shared<TOffloadedPoolAllocator>(std::move(scopedAlloc));
}

template <class T>
struct TNullableAllocLockOps {
    static inline void Acquire(T* t) noexcept {
        if (t) {
            t->Acquire();
        }
    }

    static inline void Release(T* t) noexcept {
        if (t) {
            t->Release();
        }
    }
};

using TNullableAllocGuard = TGuard<NKikimr::NMiniKQL::TScopedAlloc, TNullableAllocLockOps<NKikimr::NMiniKQL::TScopedAlloc>>;


class TColumnBatch : public IDataBatch {
public:
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    TString SerializeToString() const override {
        AFL_ENSURE(!Extracted);
        return NArrow::SerializeBatchNoCompression(Data);
    }

    i64 GetSerializedMemory() const override {
        AFL_ENSURE(!Extracted);
        return SerializedMemory;
    }

    i64 GetMemory() const override {
        AFL_ENSURE(!Extracted);
        return Memory;
    }

    bool IsEmpty() const override {
        AFL_ENSURE(!Extracted);
        return !Data || Data->num_rows() == 0;
    }

    TRecordBatchPtr Extract() {
        AFL_ENSURE(!Extracted);
        Extracted = true;
        SerializedMemory = 0;
        Memory = 0;
        return std::move(Data);
    }

    std::shared_ptr<void> ExtractBatch() override {
        return std::dynamic_pointer_cast<void>(Extract());
    }

    void DetachAlloc() override {
        Y_ABORT_UNLESS(false); // Write to CS doesn't need to move data between allocators.
    }

    void AttachAlloc(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) override {
        Y_ABORT_UNLESS(Alloc == alloc); // Write to CS doesn't need to move data between allocators.
    }

    bool AttachedAlloc() const override {
        return true;
    }

    explicit TColumnBatch(const TRecordBatchPtr& data, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc = nullptr)
        : Alloc(alloc) 
        , Data(data)
        , SerializedMemory(NArrow::GetBatchDataSize(Data))
        , Memory(NArrow::GetBatchMemorySize(Data)) {
    }

    ~TColumnBatch() {
        TNullableAllocGuard guard(Alloc.get());
        Data.reset();
    }

private:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc = nullptr;
    TRecordBatchPtr Data;
    i64 SerializedMemory = 0;
    i64 Memory = 0;

    bool Extracted = false;
};


class TRowBatch : public IDataBatch {
public:
    TString SerializeToString() const override {
        AFL_ENSURE(!Extracted);
        TVector<TCell> cells;
        if (!Rows.empty()) {
            cells.reserve(Rows.Size() * Rows.front().size());
        }
        for (const auto& row : Rows) {
            for (const auto& cell : row) {
                cells.push_back(cell);
            }
        }
        return TSerializedCellMatrix::Serialize(cells, Rows.Size(), !IsEmpty() ? Rows.front().size() : 0);
    }

    i64 GetSerializedMemory() const override {
        AFL_ENSURE(!Extracted);
        return SerializedMemory;
    }

    i64 GetMemory() const override {
        AFL_ENSURE(!Extracted);
        return Memory;
    }

    bool IsEmpty() const override {
        AFL_ENSURE(!Extracted);
        return Rows.empty();
    }

    TOwnedCellVecBatch Extract() {
        AFL_ENSURE(!Extracted);
        Extracted = true;
        return std::move(Rows);
    }

    std::shared_ptr<void> ExtractBatch() override {
        auto r = std::make_shared<TOwnedCellVecBatch>(std::move(Extract()));
        return std::reinterpret_pointer_cast<void>(r);
    }

    void DetachAlloc() override {
        OffloadedAlloc->Detach();
    }

    void AttachAlloc(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) override {
        OffloadedAlloc->Attach(alloc);
    }

    bool AttachedAlloc() const override {
        return OffloadedAlloc->Attached();
    }

    TRowBatch(
        TOwnedCellVecBatch&& rows,
        TOffloadedPoolAllocatorPtr offloadedAlloc)
            : OffloadedAlloc(std::move(offloadedAlloc))
            , Rows(std::move(rows)) {
        SerializedMemory = GetCellMatrixHeaderSize();
        Memory = 0;
        for (const auto& row : Rows) {
            AFL_ENSURE(row.size() == Rows.front().size());
            const auto size = EstimateSize(row);
            SerializedMemory += GetCellHeaderSize() * row.size() + size;
            Memory += size;
        }
    }

private:
    TOffloadedPoolAllocatorPtr OffloadedAlloc;
    TOwnedCellVecBatch Rows;
    i64 SerializedMemory = 0;
    i64 Memory = 0;
    bool Extracted = false;
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
        AFL_ENSURE(column.HasTypeId());
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

class TRowBuilder {
private:
    struct TCellInfo {
        NScheme::TTypeInfo Type;
        NUdf::TUnboxedValuePod Value;
        TString PgBinaryValue;

        NYql::NDecimal::TInt128 DecimalBuf;
    };

public:
    explicit TRowBuilder(size_t size)
        : CellsInfo(size) {
    }

    TRowBuilder& AddCell(
            const size_t index,
            const NScheme::TTypeInfo& type,
            const NUdf::TUnboxedValuePod& value,
            const TString& typeMod) {
        CellsInfo[index].Type = type;
        CellsInfo[index].Value = value;

        if (CellsInfo[index].Type.GetTypeId() == NUdf::TDataType<NUdf::TDecimal>::Id && value) {
            CellsInfo[index].DecimalBuf = value.GetInt128();
        }

        if (type.GetTypeId() == NScheme::NTypeIds::Pg && value) {
            auto typeDesc = type.GetPgTypeDesc();
            if (!typeMod.empty() && NPg::TypeDescNeedsCoercion(typeDesc)) {

                auto typeModResult = NPg::BinaryTypeModFromTextTypeMod(typeMod, type.GetPgTypeDesc());
                if (typeModResult.Error) {
                    ythrow yexception() << "BinaryTypeModFromTextTypeMod error: " << *typeModResult.Error;
                }

                AFL_ENSURE(typeModResult.Typmod != -1);
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

    TConstArrayRef<TCell> BuildCells() {
        Cells.clear();
        Cells.reserve(CellsInfo.size());
        
        for (const auto& cellInfo : CellsInfo) {
            Cells.emplace_back(BuildCell(cellInfo));
        }
        return Cells;
    }

private:
    TCell BuildCell(const TCellInfo& cellInfo) {
        if (!cellInfo.Value) {
            return TCell();
        }

        switch(cellInfo.Type.GetTypeId()) {
    #define MAKE_PRIMITIVE_TYPE_CELL_CASE(type, layout) \
        case NUdf::TDataType<type>::Id: return NMiniKQL::MakeCell<layout>(cellInfo.Value);
            KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_CELL_CASE)
        case NUdf::TDataType<NUdf::TDecimal>::Id:
            {
                constexpr auto valueSize = sizeof(cellInfo.DecimalBuf);
                return TCell(reinterpret_cast<const char*>(&cellInfo.DecimalBuf), valueSize);
            }
        }

        const bool isPg = cellInfo.Type.GetTypeId() == NScheme::NTypeIds::Pg;

        const auto ref = isPg
            ? NYql::NUdf::TStringRef(cellInfo.PgBinaryValue)
            : cellInfo.Value.AsStringRef();

        return TCell(ref.Data(), ref.Size());
    }

    TVector<TCellInfo> CellsInfo;
    TVector<TCell> Cells;
};

class TColumnDataBatcher : public IDataBatcher {
public:
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    TColumnDataBatcher(
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
            : Columns(BuildColumns(inputColumns))
            , WriteIndex(std::move(writeIndex))
            , BatchBuilder(std::make_unique<NArrow::TArrowBatchBuilder>(
                arrow::Compression::UNCOMPRESSED,
                BuildNotNullColumns(inputColumns),
                alloc ? NKikimr::NMiniKQL::GetArrowMemoryPool() : arrow::default_memory_pool()))
            , Alloc(std::move(alloc)) {
        TString err;
        if (!BatchBuilder->Start(BuildBatchBuilderColumns(WriteIndex, inputColumns), 0, 0, err)) {
            yexception() << "Failed to start batch builder: " + err;
        }
    }

    ~TColumnDataBatcher() {
        TNullableAllocGuard guard(Alloc.get());
        BatchBuilder.reset();
    }

    void AddData(const NMiniKQL::TUnboxedValueBatch& data) override {
        TNullableAllocGuard guard(Alloc.get());
        TRowBuilder rowBuilder(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                rowBuilder.AddCell(
                    WriteIndex[index],
                    Columns[index].PType,
                    row.GetElement(index),
                    Columns[index].PTypeMod);
            }
            BatchBuilder->AddRow(rowBuilder.BuildCells());
        });
    }

    i64 GetMemory() const override {
        return BatchBuilder->Bytes();
    }

    IDataBatchPtr Build() override {
        TNullableAllocGuard guard(Alloc.get());
        auto batch = BatchBuilder->FlushBatch(true);
        return MakeIntrusive<TColumnBatch>(std::move(batch), Alloc);
    }

private:
    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    std::unique_ptr<NArrow::TArrowBatchBuilder> BatchBuilder;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};

class TColumnShardPayloadSerializer : public IPayloadSerializer {
    using TRecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

    struct TUnpreparedBatch {
        ui64 TotalDataSize = 0;
        std::deque<TRecordBatchPtr> Batches; 
    };

public:
    TColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const std::vector<ui32> writeIndex,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) // key columns then value columns
            : Columns(BuildColumns(inputColumns))
            , WriteColumnIds(BuildWriteColumnIds(inputColumns, writeIndex))
            , Alloc(std::move(alloc)) {
        AFL_ENSURE(Alloc);
        AFL_ENSURE(schemeEntry.ColumnTableInfo);
        const auto& description = schemeEntry.ColumnTableInfo->Description;
        AFL_ENSURE(description.HasSchema());
        const auto& scheme = description.GetSchema();
        AFL_ENSURE(description.HasSharding());
        const auto& sharding = description.GetSharding();

        NSchemeShard::TOlapSchema olapSchema;
        olapSchema.ParseFromLocalDB(scheme);
        auto shardingConclusion = NSharding::IShardingBase::BuildFromProto(olapSchema, sharding);
        if (shardingConclusion.IsFail()) {
            ythrow yexception() << "Ydb::StatusIds::SCHEME_ERROR : " <<  shardingConclusion.GetErrorMessage();
        }
        AFL_ENSURE(shardingConclusion.GetResult() != nullptr);
        Sharding = shardingConclusion.DetachResult();
    }

    ~TColumnShardPayloadSerializer() {
        TGuard guard(*Alloc);
        UnpreparedBatches.clear();
        Batches.clear();
    }

    void AddData(IDataBatchPtr&& batch) override {
        AFL_ENSURE(!Closed);
        AddBatch(std::move(batch));
    }

    void AddBatch(IDataBatchPtr&& batch) override {
        TGuard guard(*Alloc);
        auto columnshardBatch = dynamic_cast<TColumnBatch*>(batch.Get());
        AFL_ENSURE(columnshardBatch);
        if (columnshardBatch->IsEmpty()) {
            return;
        }
        auto data = columnshardBatch->Extract();
        AFL_ENSURE(data);
        ShardAndFlushBatch(std::move(data), false);
    }

    void ShardAndFlushBatch(TRecordBatchPtr&& unshardedBatch, bool force) {
        for (auto [shardId, shardBatch] : Sharding->SplitByShardsToArrowBatches(
                                                    unshardedBatch, NKikimr::NMiniKQL::GetArrowMemoryPool())) {
            const i64 shardBatchMemory = NArrow::GetBatchDataSize(shardBatch);
            AFL_ENSURE(shardBatchMemory != 0);

            ShardIds.insert(shardId);
            auto& unpreparedBatch = UnpreparedBatches[shardId];
            unpreparedBatch.TotalDataSize += shardBatchMemory;
            Memory += shardBatchMemory;
            unpreparedBatch.Batches.emplace_back(shardBatch);

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
                AFL_ENSURE(batch->num_rows() > 0);
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
                        AFL_ENSURE(index > 0);

                        toPrepare.push_back(batch->Slice(0, index));
                        unpreparedBatch.Batches.push_front(batch->Slice(index, batch->num_rows() - index));

                        const auto newBatchDataSize = NArrow::GetBatchDataSize(unpreparedBatch.Batches.front());

                        unpreparedBatch.TotalDataSize += newBatchDataSize;
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

            auto batch = MakeIntrusive<TColumnBatch>(NArrow::CombineBatches(toPrepare), Alloc);
            Batches[shardId].emplace_back(batch);
            Memory += batch->GetMemory();
            AFL_ENSURE(batch->GetMemory() != 0);
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
        TGuard guard(*Alloc);
        AFL_ENSURE(!Closed);
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
        TGuard guard(*Alloc);
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
        TGuard guard(*Alloc);
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

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    THashMap<ui64, TUnpreparedBatch> UnpreparedBatches;
    TBatches Batches;
    THashSet<ui64> ShardIds;

    i64 Memory = 0;

    bool Closed = false;
};

class TRowsBatcher {
    class TBatch {
    private:
        i64 Memory;
        i64 MemorySerialized;
        TOffloadedPoolAllocatorPtr Alloc;
        TOwnedCellVecBatch Rows;

        TOwnedCellVecBatch Extract() {
            Memory = 0;
            MemorySerialized = 0;
            return std::move(Rows);
        }

    public:
        TBatch(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
            : Memory(0)
            , MemorySerialized(GetCellMatrixHeaderSize())
            , Alloc(CreateOffloadedPoolAllocator(std::move(alloc)))
            , Rows(Alloc->CreateMemoryPool()) {
        }

        i64 AddRow(TConstArrayRef<TCell> row) {
            const i64 memory = EstimateSize(row);
            const i64 memorySerialized = memory + GetCellHeaderSize() * row.size();

            Memory += memory;
            MemorySerialized += memorySerialized;

            Rows.Append(row);

            return memory;
        }

        i64 GetMemorySerialized() {
            return MemorySerialized;
        }

        i64 GetMemory() {
            return Memory;
        }

        IDataBatchPtr Build() {
            return MakeIntrusive<TRowBatch>(Extract(), std::move(Alloc));
        }
    };
    
public:
    explicit TRowsBatcher(
            ui16 columnCount,
            std::optional<i64> maxBytesPerBatch,
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : ColumnCount(columnCount)
        , MaxBytesPerBatch(maxBytesPerBatch)
        , Alloc(std::move(alloc)) {
    }

    bool IsEmpty() const {
        return Batches.empty();
    }

    IDataBatchPtr Flush(bool force) {
        if ((!Batches.empty() && force) || Batches.size() > 1) {
            AFL_ENSURE(MaxBytesPerBatch || Batches.size() == 1);
            Memory -= Batches.front()->GetMemory();
            auto res = Batches.front()->Build();
            Batches.pop_front();

            return res;
        }

        auto poolAlloc = CreateOffloadedPoolAllocator(std::move(Alloc));
        return MakeIntrusive<TRowBatch>(TOwnedCellVecBatch(poolAlloc->CreateMemoryPool()), poolAlloc);
    }

    void AddRow(TConstArrayRef<TCell> row) {
        AFL_ENSURE(row.size() == ColumnCount);

        const i64 newMemory = EstimateSize(row);
        const i64 newMemorySerialized = newMemory + GetCellHeaderSize() * ColumnCount;
        if (Batches.empty() || (MaxBytesPerBatch && newMemorySerialized + Batches.back()->GetMemorySerialized() > *MaxBytesPerBatch)) {
            Batches.emplace_back(std::make_unique<TBatch>(Alloc));
        }
        
        AFL_ENSURE(newMemory == Batches.back()->AddRow(std::move(row)));
        Memory += newMemory;
    }

    i64 GetMemory() const {
        return Memory;
    }

private:
    std::deque<std::unique_ptr<TBatch>> Batches;
    ui16 ColumnCount;
    std::optional<i64> MaxBytesPerBatch;
    i64 Memory = 0;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};

class TRowDataBatcher : public IDataBatcher {
public:
    TRowDataBatcher(
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
            : Columns(BuildColumns(inputColumns))
            , WriteIndex(std::move(writeIndex))
            , RowBatcher(Columns.size(), std::nullopt, alloc)
            , Alloc(alloc) {
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
            auto cells = rowBuilder.BuildCells();
            RowBatcher.AddRow(cells);
        });
    }

    i64 GetMemory() const override {
        return RowBatcher.GetMemory();
    }

    IDataBatchPtr Build() override {
        return RowBatcher.Flush(true);
    }

private:
    const TVector<TSysTables::TTableColumnInfo> Columns;
    const std::vector<ui32> WriteIndex;
    TRowsBatcher RowBatcher;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};

class TDataShardPayloadSerializer : public IPayloadSerializer {
public:
    TDataShardPayloadSerializer(
        const TVector<TKeyDesc::TPartitionInfo>& partitioning,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto>& inputColumns,
        std::vector<ui32> writeIndex,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : Partitioning(partitioning)
        , Columns(BuildColumns(inputColumns))
        , WriteIndex(std::move(writeIndex))
        , WriteColumnIds(BuildWriteColumnIds(inputColumns, WriteIndex))
        , KeyColumnTypes(BuildKeyColumnTypes(keyColumns))
        , Alloc(std::move(alloc)) {
        AFL_ENSURE(Alloc);
        AFL_ENSURE(Columns.size() <= std::numeric_limits<ui16>::max());
    }

    void AddRow(TConstArrayRef<TCell> row, const TVector<TKeyDesc::TPartitionInfo>& partitioning) {
        AFL_ENSURE(row.size() >= KeyColumnTypes.size());
        auto shardIter = std::lower_bound(
            std::begin(partitioning),
            std::end(partitioning),
            TArrayRef(row.data(), KeyColumnTypes.size()),
            [this](const auto &partition, const auto& key) {
                const auto& range = *partition.Range;
                return 0 > CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                    range.IsInclusive || range.IsPoint, true, KeyColumnTypes);
            });

        AFL_ENSURE(shardIter != partitioning.end());

        auto batcherIter = Batchers.find(shardIter->ShardId);
        if (batcherIter == std::end(Batchers)) {
            Batchers.emplace(
                shardIter->ShardId,
                TRowsBatcher(Columns.size(), DataShardMaxOperationBytes, Alloc));
        }

        Batchers.at(shardIter->ShardId).AddRow(row);
        ShardIds.insert(shardIter->ShardId);
    }

    void AddData(IDataBatchPtr&& data) override {
        AFL_ENSURE(!Closed);
        AddBatch(std::move(data));
    }

    void AddBatch(IDataBatchPtr&& batch) override {
        auto datashardBatch = dynamic_cast<TRowBatch*>(batch.Get());
        AFL_ENSURE(datashardBatch);
        auto rows = datashardBatch->Extract();

        for (const auto& row : rows) {
            AddRow(
                row,
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
        i64 memory = 0;
        for (const auto& [_, batcher] : Batchers) {
            memory += batcher.GetMemory();
        }
        return memory;
    }

    void Close() override {
        AFL_ENSURE(!Closed);
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
        return batcher.Flush(force);
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
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    THashMap<ui64, TRowsBatcher> Batchers;
    THashSet<ui64> ShardIds;

    bool Closed = false;
};
IPayloadSerializerPtr CreateColumnShardPayloadSerializer(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const std::vector<ui32> writeIndex,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) {
    return MakeIntrusive<TColumnShardPayloadSerializer>(
        schemeEntry, inputColumns, std::move(writeIndex), std::move(alloc));
}

IPayloadSerializerPtr CreateDataShardPayloadSerializer(
        const TVector<TKeyDesc::TPartitionInfo>& partitioning,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> keyColumns,
        const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        const std::vector<ui32> writeIndex,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) {
    return MakeIntrusive<TDataShardPayloadSerializer>(
        partitioning, keyColumns, inputColumns, std::move(writeIndex), std::move(alloc));
}

}

IDataBatcherPtr CreateColumnDataBatcher(const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) {
    return MakeIntrusive<TColumnDataBatcher>(inputColumns, std::move(writeIndex), std::move(alloc));
}

IDataBatcherPtr CreateRowDataBatcher(const TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> inputColumns,
        std::vector<ui32> writeIndex, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) {
    return MakeIntrusive<TRowDataBatcher>(inputColumns, std::move(writeIndex), std::move(alloc));
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

    i64 GetSerializedMemory() const {
        return IsCoveringBatch() ? 0 : Data->GetSerializedMemory();
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

        void MakeNextBatches(std::optional<ui64> maxCount) {
            AFL_ENSURE(BatchesInFlight == 0);
            AFL_ENSURE(!IsEmpty());

            // For columnshard batch can be slightly larger than the limit.
            while ((!maxCount || BatchesInFlight < *maxCount)
                    && BatchesInFlight < Batches.size()) {
                ++BatchesInFlight;
            }
            AFL_ENSURE(BatchesInFlight != 0);
            AFL_ENSURE(BatchesInFlight == Batches.size()
                || (maxCount && BatchesInFlight >= *maxCount));
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
                    const i64 batchMemory = Batches.front().GetMemory();
                    result.DataSize += batchMemory;
                    Memory -= batchMemory;
                    Batches.pop_front();
                }

                Cookie = NextCookie++;
                SendAttempts = 0;
                BatchesInFlight = 0;

                return result;
            }
            return std::nullopt;
        }

        void PushBatch(TBatchWithMetadata&& batch) {
            AFL_ENSURE(!IsClosed());
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

    void ForEachPendingShard(std::function<void(const IShardedWriteController::TPendingShardInfo&)>&& callback) const {
        for (const auto& [id, shard] : ShardsInfo) {
            if (!shard.IsEmpty() && shard.GetSendAttempts() == 0) {
                callback(IShardedWriteController::TPendingShardInfo{
                    .ShardId = id,
                    .HasRead = shard.HasRead(),
                });
            }
        }
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
                writeInfo.Metadata.WriteIndex,
                Alloc);
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
                writeInfo.Metadata.WriteIndex,
                Alloc);
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
                iter->second.Metadata.WriteIndex,
                Alloc);
        } else if (SchemeEntry) {
            iter->second.Serializer = CreateColumnShardPayloadSerializer(
                *SchemeEntry,
                iter->second.Metadata.InputColumnsMetadata,
                iter->second.Metadata.WriteIndex,
                Alloc);
        }
        return token;
    }

    void Write(TWriteToken token, IDataBatchPtr&& data) override {
        auto& info = WriteInfos.at(token);
        AFL_ENSURE(!info.Closed);

        AFL_ENSURE(info.Serializer);
        if (!data->AttachedAlloc()) {
            AFL_ENSURE(!Settings.Inconsistent);
            data->AttachAlloc(Alloc);
        }
        info.Serializer->AddData(std::move(data));

        if (info.Metadata.Priority == 0) {
            FlushSerializer(token, GetMemory() >= Settings.MemoryLimitTotal);
        }
    }

    void Close(TWriteToken token) override {
        auto& info = WriteInfos.at(token);
        AFL_ENSURE(info.Serializer);
        info.Closed = true;
        info.Serializer->Close();
        if (info.Metadata.Priority == 0) {
            FlushSerializer(token, true);
            AFL_ENSURE(info.Serializer->IsFinished());
        }
    }

    void CleanupClosedTokens() override {
        for (auto it = WriteInfos.begin(); it != WriteInfos.end();) {
            if (it->second.Closed) {
                AFL_ENSURE(it->second.Serializer->IsFinished());
                it = WriteInfos.erase(it);
            } else {
                ++it;
            }
        }
    }

    void FlushBuffers() override {
        TVector<TWriteToken> writeTokensFoFlush;
        for (const auto& [token, writeInfo] : WriteInfos) {
            AFL_ENSURE(writeInfo.Closed);
            if (writeInfo.Metadata.Priority != 0) {
                if (!writeInfo.Serializer->IsFinished()) {
                    writeTokensFoFlush.push_back(token);
                }
            } else {
                AFL_ENSURE(writeInfo.Serializer->IsFinished());
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
            AFL_ENSURE(WriteInfos.at(token).Serializer->IsFinished());
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

    void ForEachPendingShard(std::function<void(const TPendingShardInfo&)>&& callback) const override {
        ShardsInfo.ForEachPendingShard(std::move(callback));
    }

    std::vector<TPendingShardInfo> ExtractShardUpdates() override {
        std::vector<TPendingShardInfo> shardUpdates;
        std::swap(shardUpdates, ShardUpdates);
        return shardUpdates;
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
                AFL_ENSURE(!inFlightBatch.Data->IsEmpty());
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
                AFL_ENSURE(index + 1 == shardInfo.GetBatchesInFlight());   
            }
        }

        return result;
    }

    std::optional<TMessageAcknowledgedResult> OnMessageAcknowledged(ui64 shardId, ui64 cookie) override {
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
                AFL_ENSURE(writeInfo.Closed);
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
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : Settings(settings)
        , Alloc(std::move(alloc)) {
    }

    ~TShardedWriteController() {
        ShardsInfo.Clear();
        for (auto& [_, writeInfo] : WriteInfos) {
            writeInfo.Serializer = nullptr;
        }
    }

private:
    void FlushSerializer(TWriteToken token, bool force) {
        const auto& writeInfo = WriteInfos.at(token);
        if (force) {
            for (auto& [shardId, batches] : writeInfo.Serializer->FlushBatchesForce()) {
                for (auto& batch : batches) {
                    if (batch && !batch->IsEmpty()) {
                        const bool hasRead = (writeInfo.Metadata.OperationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT
                                || writeInfo.Metadata.OperationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                        ShardsInfo.GetShard(shardId).PushBatch(TBatchWithMetadata{
                            .Token = token,
                            .Data = std::move(batch),
                            .HasRead = hasRead,
                        });
                        ShardUpdates.push_back(IShardedWriteController::TPendingShardInfo{
                            .ShardId = shardId,
                            .HasRead = hasRead,
                        });
                    }
                }
            }
        } else {
            for (const ui64 shardId : writeInfo.Serializer->GetShardIds()) {
                auto& shard = ShardsInfo.GetShard(shardId);
                while (true) {
                    auto batch = writeInfo.Serializer->FlushBatch(shardId);
                    if (!batch || batch->IsEmpty()) {
                        break;
                    }
                    const bool hasRead = (writeInfo.Metadata.OperationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT
                        || writeInfo.Metadata.OperationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                    shard.PushBatch(TBatchWithMetadata{
                        .Token = token,
                        .Data = std::move(batch),
                        .HasRead = hasRead,
                    });
                    ShardUpdates.push_back(IShardedWriteController::TPendingShardInfo{
                        .ShardId = shardId,
                        .HasRead = hasRead,
                    });
                }
            }
        }
    }

    void BuildBatchesForShard(TShardsInfo::TShardInfo& shard) {
        if (shard.GetBatchesInFlight() == 0) {
            AFL_ENSURE(IsOlap != std::nullopt);
            if (*IsOlap) {
                shard.MakeNextBatches(1);
            } else {
                shard.MakeNextBatches(std::nullopt);
                AFL_ENSURE(shard.GetBatchesInFlight() == shard.Size());
            }
        }
    }

    void ReshardData() {
        AFL_ENSURE(Settings.Inconsistent);
        for (auto& [_, shardInfo] : ShardsInfo.GetShards()) {
            for (size_t index = 0; index < shardInfo.Size(); ++index) {
                auto& batch = shardInfo.GetBatch(index);
                const auto& writeInfo = WriteInfos.at(batch.Token);
                // Resharding supported only for inconsistent write,
                // so convering empty batches don't exist in this case.
                AFL_ENSURE(batch.Data);
                writeInfo.Serializer->AddBatch(std::move(batch.Data));
            }
        }
    }

    TShardedWriteControllerSettings Settings;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    struct TWriteInfo {
        TMetadata Metadata;
        IPayloadSerializerPtr Serializer = nullptr;
        bool Closed = false;
    };

    std::map<TWriteToken, TWriteInfo> WriteInfos;
    TWriteToken CurrentWriteToken = 0;

    TShardsInfo ShardsInfo;
    std::vector<IShardedWriteController::TPendingShardInfo> ShardUpdates;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    std::optional<bool> IsOlap;
};

}


IShardedWriteControllerPtr CreateShardedWriteController(
        const TShardedWriteControllerSettings& settings,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc) {
    return MakeIntrusive<TShardedWriteController>(settings, std::move(alloc));
}

}
}
