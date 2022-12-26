#include "mkql_block_builder.h"
#include "mkql_bit_utils.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/buffer_builder.h>
#include <arrow/chunked_array.h>


namespace NKikimr {
namespace NMiniKQL {

namespace {

bool AlwaysUseChunks(const TType* type) {
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            if (AlwaysUseChunks(tupleType->GetElementType(i))) {
                return true;
            }
        }
        return false;
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        return (GetDataTypeInfo(slot).Features & NYql::NUdf::EDataTypeFeatures::StringType) != 0u;
    }

    MKQL_ENSURE(false, "Unsupported type");
}

std::shared_ptr<arrow::DataType> GetArrowType(TType* type) {
    std::shared_ptr<arrow::DataType> result;
    bool isOptional;
    Y_VERIFY(ConvertArrowType(type, isOptional, result));
    return result;
}

std::shared_ptr<arrow::Buffer> AllocateBitmapWithReserve(size_t bitCount, arrow::MemoryPool* pool) {
    // align up to 64 bit
    bitCount = (bitCount + 63u) & ~size_t(63u);
    // this simplifies code compression code - we can write single 64 bit word after array boundaries
    bitCount += 64;
    return ARROW_RESULT(arrow::AllocateBitmap(bitCount, pool));
}

std::shared_ptr<arrow::Buffer> MakeDenseBitmap(const ui8* srcSparse, size_t len, arrow::MemoryPool* pool) {
    auto bitmap = AllocateBitmapWithReserve(len, pool);
    CompressSparseBitmap(bitmap->mutable_data(), srcSparse, len);
    return bitmap;
}

// similar to arrow::TypedBufferBuilder, but with UnsafeAdvance() method
template<typename T>
class TTypedBufferBuilder {
    static_assert(std::is_pod_v<T>);
    static_assert(!std::is_same_v<T, bool>);
public:
    explicit TTypedBufferBuilder(arrow::MemoryPool* pool)
        : Builder(pool)
    {
    }

    inline void Reserve(size_t size) {
        ARROW_OK(Builder.Reserve(size * sizeof(T)));
    }

    inline size_t Length() const {
        return Builder.length() / sizeof(T);
    }

    inline T* MutableData() {
        return reinterpret_cast<T*>(Builder.mutable_data());
    }

    inline T* End() {
        return MutableData() + Length();
    }

    inline const T* Data() const {
        return reinterpret_cast<const T*>(Builder.data());
    }

    inline void UnsafeAppend(const T* values, size_t count) {
        std::memcpy(End(), values, count * sizeof(T));
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(size_t count, const T& value) {
        T* target = End();
        std::fill(target, target + count, value);
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(T&& value) {
        *End() = std::move(value);
        UnsafeAdvance(1);
    }

    inline void UnsafeAdvance(size_t count) {
        Builder.UnsafeAdvance(count * sizeof(T));
    }

    inline std::shared_ptr<arrow::Buffer> Finish() {
        bool shrinkToFit = false;
        return ARROW_RESULT(Builder.Finish(shrinkToFit));
    }
private:
    arrow::BufferBuilder Builder;
};

class TBlockBuilderBase : public IBlockBuilder {
public:
    using Ptr = std::unique_ptr<TBlockBuilderBase>;

    struct TBlockArrayTree {
        using Ptr = std::shared_ptr<TBlockArrayTree>;
        std::deque<std::shared_ptr<arrow::ArrayData>> Payload;
        std::vector<TBlockArrayTree::Ptr> Children;
    };


    TBlockBuilderBase(TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : Type(type)
        , Pool(&pool)
        , MaxLen(maxLen)
    {
        Y_VERIFY(type);
        Y_VERIFY(maxLen > 0);
    }

    size_t MaxLength() const final {
        return MaxLen;
    }

    void Add(NUdf::TUnboxedValuePod value) final {
        Y_VERIFY(CurrLen < MaxLen);
        DoAdd(value);
        CurrLen++;
    }

    void AddMany(const arrow::ArrayData& array, size_t popCount, const ui8* sparseBitmap, size_t bitmapSize) final {
        Y_VERIFY(array.length == bitmapSize);
        Y_VERIFY(popCount <= bitmapSize);
        Y_VERIFY(CurrLen + popCount <= MaxLen);

        if (popCount) {
            DoAddMany(array, sparseBitmap, popCount);
        }

        CurrLen += popCount;
    }

    NUdf::TUnboxedValuePod Build(TComputationContext& ctx, bool finish) final {
        auto tree = BuildTree(finish);
        CurrLen = 0;
        arrow::ArrayVector chunks;
        while (size_t size = CalcSliceSize(*tree)) {
            std::shared_ptr<arrow::ArrayData> data = Slice(*tree, size);
            chunks.push_back(arrow::Datum(data).make_array());
        }

        Y_VERIFY(!chunks.empty());

        if (chunks.size() > 1 || AlwaysUseChunks(Type)) {
            auto chunked = ARROW_RESULT(arrow::ChunkedArray::Make(std::move(chunks), GetArrowType(Type)));
            return ctx.HolderFactory.CreateArrowBlock(std::move(chunked));
        }
        return ctx.HolderFactory.CreateArrowBlock(chunks.front());
    }

    virtual TBlockArrayTree::Ptr BuildTree(bool finish) = 0;
    virtual void DoAdd(NUdf::TUnboxedValuePod value) = 0;
    virtual void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) = 0;

private:
    static size_t CalcSliceSize(const TBlockArrayTree& tree) {
        if (tree.Payload.empty()) {
            return 0;
        }

        if (!tree.Children.empty()) {
            Y_VERIFY(tree.Payload.size() == 1);
            size_t result = std::numeric_limits<size_t>::max();
            for (auto& child : tree.Children) {
                size_t childSize = CalcSliceSize(*child);
                result = std::min(result, childSize);
            }
            Y_VERIFY(result <= tree.Payload.front()->length);
            return result;
        }

        int64_t result = std::numeric_limits<int64_t>::max();
        for (auto& data : tree.Payload) {
            result = std::min(result, data->length);
        }

        Y_VERIFY(result > 0);
        return static_cast<size_t>(result);
    }

    static std::shared_ptr<arrow::ArrayData> Slice(TBlockArrayTree& tree, size_t size) {
        Y_VERIFY(size > 0);

        Y_VERIFY(!tree.Payload.empty());
        auto& main = tree.Payload.front();
        std::shared_ptr<arrow::ArrayData> sliced;
        if (size == main->length) {
            sliced = main;
            tree.Payload.pop_front();
        } else {
            Y_VERIFY(size < main->length);
            sliced = Chop(main, size);
        }

        if (!tree.Children.empty()) {
            std::vector<std::shared_ptr<arrow::ArrayData>> children;
            for (auto& child : tree.Children) {
                children.push_back(Slice(*child, size));
            }

            sliced->child_data = std::move(children);
            if (tree.Payload.empty()) {
                tree.Children.clear();
            }
        }
        return sliced;
    }

protected:
    TType* const Type;
    arrow::MemoryPool* const Pool;
    const size_t MaxLen;
private:
    size_t CurrLen = 0;
};

template <typename T>
class TFixedSizeBlockBuilder : public TBlockBuilderBase {
public:
    TFixedSizeBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : TBlockBuilderBase(type, pool, maxLen)
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if (value) {
            NullBuilder->UnsafeAppend(1);
            DataBuilder->UnsafeAppend(value.Get<T>());
        } else {
            NullBuilder->UnsafeAppend(0);
            DataBuilder->UnsafeAppend(T{});
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_VERIFY(array.buffers.size() > 1);
        Y_VERIFY(NullBuilder->Length() == DataBuilder->Length());

        const T* src = array.GetValues<T>(1);

        T* dst = DataBuilder->End();
        CompressArray(src, sparseBitmap, dst, array.length);
        DataBuilder->UnsafeAdvance(popCount);

        if (array.buffers.front()) {
            ui8* dstBitmap = NullBuilder->End();
            CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
            NullBuilder->UnsafeAdvance(popCount);
        } else {
            NullBuilder->UnsafeAppend(popCount, 1);
        }
    }

    TBlockArrayTree::Ptr BuildTree(bool finish) final {
        Y_VERIFY(NullBuilder->Length() == DataBuilder->Length());
        const size_t len = DataBuilder->Length();

        std::shared_ptr<arrow::Buffer> nulls = NullBuilder->Finish();
        std::shared_ptr<arrow::Buffer> data = DataBuilder->Finish();
        nulls = MakeDenseBitmap(nulls->data(), len, Pool);

        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();
        result->Payload.push_back(arrow::ArrayData::Make(GetArrowType(Type), len, {nulls, data}));

        NullBuilder.reset();
        DataBuilder.reset();
        if (!finish) {
            Reserve();
        }
        return result;
    }

private:
    void Reserve() {
        NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        DataBuilder = std::make_unique<TTypedBufferBuilder<T>>(Pool);
        NullBuilder->Reserve(MaxLen + 1);
        DataBuilder->Reserve(MaxLen + 1);
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
    std::unique_ptr<TTypedBufferBuilder<T>> DataBuilder;
};

template<typename TStringType>
class TStringBlockBuilder : public TBlockBuilderBase {
public:
    using TOffset = typename TStringType::offset_type;

    TStringBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : TBlockBuilderBase(type, pool, maxLen)
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if (!value) {
            NullBuilder->UnsafeAppend(0);
            AppendCurrentOffset();
            return;
        }

        const TStringBuf str = value.AsStringRef();

        size_t currentLen = DataBuilder->Length();
        // empty string can always be appended
        if (!str.empty() && currentLen + str.size() > MaxBlockSizeInBytes) {
            if (currentLen) {
                FlushChunk(false);
            }
            if (str.size() > MaxBlockSizeInBytes) {
                DataBuilder->Reserve(str.size());
            }
        }

        NullBuilder->UnsafeAppend(1);
        AppendCurrentOffset();
        DataBuilder->UnsafeAppend((const ui8*)str.data(), str.size());
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_VERIFY(array.buffers.size() > 2);
        Y_VERIFY(NullBuilder->Length() == OffsetsBuilder->Length());

        const ui8* srcNulls = array.GetValues<ui8>(0, 0);
        const TOffset* srcOffset = array.GetValues<TOffset>(1);
        const ui8* srcData = array.GetValues<ui8>(2, 0);

        const ui8* chunkStart = srcData;
        const ui8* chunkEnd = chunkStart;
        size_t dataLen = DataBuilder->Length();

        ui8* dstNulls = NullBuilder->End();
        TOffset* dstOffset = OffsetsBuilder->End();
        size_t countAdded = 0;
        for (size_t i = 0; i < array.length; i++) {
            if (!sparseBitmap[i]) {
                continue;
            }

            const ui8* begin = srcData + srcOffset[i];
            const ui8* end   = srcData + srcOffset[i + 1];
            const size_t strSize = end - begin;

            size_t availBytes = std::max(dataLen, MaxBlockSizeInBytes)  - dataLen;

            for (;;) {
                // try to append ith string
                if (strSize <= availBytes) {
                    if (begin == chunkEnd)  {
                        chunkEnd = end;
                    } else {
                        DataBuilder->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
                        chunkStart = begin;
                        chunkEnd = end;
                    }

                    size_t nullOffset = i + array.offset;
                    *dstNulls++ = (srcNulls[nullOffset >> 3] >> (nullOffset & 7)) & 1;
                    *dstOffset++ = dataLen;

                    dataLen += strSize;
                    ++countAdded;

                    break;
                }

                if (dataLen) {
                    if (chunkStart != chunkEnd) {
                        DataBuilder->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
                        chunkStart = chunkEnd = srcData;
                    }
                    Y_VERIFY(dataLen == DataBuilder->Length());
                    OffsetsBuilder->UnsafeAdvance(countAdded);
                    NullBuilder->UnsafeAdvance(countAdded);
                    FlushChunk(false);

                    dataLen = 0;
                    countAdded = 0;
                    dstNulls = NullBuilder->End();
                    dstOffset = OffsetsBuilder->End();
                } else {
                    DataBuilder->Reserve(strSize);
                    availBytes = strSize;
                }
            }
        }
        if (chunkStart != chunkEnd) {
            DataBuilder->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
        }
        Y_VERIFY(dataLen == DataBuilder->Length());
        OffsetsBuilder->UnsafeAdvance(countAdded);
        NullBuilder->UnsafeAdvance(countAdded);
    }

    TBlockArrayTree::Ptr BuildTree(bool finish) final {
        FlushChunk(finish);
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();
        result->Payload = std::move(Chunks);
        Chunks.clear();
        return result;
    }

private:
    void Reserve() {
        NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        OffsetsBuilder = std::make_unique<TTypedBufferBuilder<TOffset>>(Pool);
        DataBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        NullBuilder->Reserve(MaxLen + 1);
        OffsetsBuilder->Reserve(MaxLen + 1);
        DataBuilder->Reserve(MaxBlockSizeInBytes);
    }

    void AppendCurrentOffset() {
        OffsetsBuilder->UnsafeAppend(DataBuilder->Length());
    }

    void FlushChunk(bool finish) {
        const auto length = OffsetsBuilder->Length();
        Y_VERIFY(length > 0);

        AppendCurrentOffset();
        std::shared_ptr<arrow::Buffer> nullBitmap = NullBuilder->Finish();
        std::shared_ptr<arrow::Buffer> offsets = OffsetsBuilder->Finish();
        std::shared_ptr<arrow::Buffer> data = DataBuilder->Finish();
        nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool);

        auto arrowType = std::make_shared<TStringType>();
        Chunks.push_back(arrow::ArrayData::Make(arrowType, length, { nullBitmap, offsets, data }));
        if (!finish) {
            Reserve();
        }
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
    std::unique_ptr<TTypedBufferBuilder<TOffset>> OffsetsBuilder;
    std::unique_ptr<TTypedBufferBuilder<ui8>> DataBuilder;

    std::deque<std::shared_ptr<arrow::ArrayData>> Chunks;
};

class TTupleBlockBuilder : public TBlockBuilderBase {
public:
    TTupleBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxLen,
                       TVector<TBlockBuilderBase::Ptr>&& children)
        : TBlockBuilderBase(type, pool, maxLen)
        , Children(std::move(children))
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        auto tupleType = AS_TYPE(TTupleType, Type);
        if (!value) {
            NullBuilder->UnsafeAppend(0);
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                Children[i]->DoAdd({});
            }
            return;
        }

        NullBuilder->UnsafeAppend(1);
        auto elements = value.GetElements();
        if (elements) {
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                Children[i]->DoAdd(elements[i]);
            }
        } else {
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                auto element = value.GetElement(i);
                Children[i]->DoAdd(element);
            }
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_VERIFY(!array.buffers.empty());
        Y_VERIFY(array.child_data.size() == Children.size());

        if (array.buffers.front()) {
            ui8* dstBitmap = NullBuilder->End();
            CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
            NullBuilder->UnsafeAdvance(popCount);
        } else {
            NullBuilder->UnsafeAppend(popCount, 1);
        }

        for (size_t i = 0; i < Children.size(); ++i) {
            Children[i]->DoAddMany(*array.child_data[i], sparseBitmap, popCount);
        }
    }

    TBlockArrayTree::Ptr BuildTree(bool finish) final {
        auto tupleType = AS_TYPE(TTupleType, Type);
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();

        auto length = NullBuilder->Length();
        std::shared_ptr<arrow::Buffer> nullBitmap = NullBuilder->Finish();
        nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool);

        result->Payload.push_back(arrow::ArrayData::Make(GetArrowType(Type), length, { nullBitmap }));
        result->Children.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            result->Children.emplace_back(Children[i]->BuildTree(finish));
        }

        if (!finish) {
            Reserve();
        }

        return result;
    }

private:
    void Reserve() {
        NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        NullBuilder->Reserve(MaxLen + 1);
    }

private:
    TVector<std::unique_ptr<TBlockBuilderBase>> Children;
    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
};

std::unique_ptr<TBlockBuilderBase> MakeBlockBuilderImpl(TType* type, arrow::MemoryPool& pool, size_t maxLen) {
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        TVector<std::unique_ptr<TBlockBuilderBase>> children;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            children.emplace_back(MakeBlockBuilderImpl(tupleType->GetElementType(i), pool, maxLen));
        }

        return std::make_unique<TTupleBlockBuilder>(tupleType, pool, maxLen, std::move(children));
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TFixedSizeBlockBuilder<i8>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return std::make_unique<TFixedSizeBlockBuilder<ui8>>(type, pool, maxLen);
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TFixedSizeBlockBuilder<i16>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TFixedSizeBlockBuilder<ui16>>(type, pool, maxLen);
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TFixedSizeBlockBuilder<i32>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TFixedSizeBlockBuilder<ui32>>(type, pool, maxLen);
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<TFixedSizeBlockBuilder<i64>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TFixedSizeBlockBuilder<ui64>>(type, pool, maxLen);
        case NUdf::EDataSlot::Float:
            return std::make_unique<TFixedSizeBlockBuilder<float>>(type, pool, maxLen);
        case NUdf::EDataSlot::Double:
            return std::make_unique<TFixedSizeBlockBuilder<double>>(type, pool, maxLen);
        case NUdf::EDataSlot::String:
            return std::make_unique<TStringBlockBuilder<arrow::BinaryType>>(type, pool, maxLen);
        case NUdf::EDataSlot::Utf8:
            return std::make_unique<TStringBlockBuilder<arrow::StringType>>(type, pool, maxLen);
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }
    }

    MKQL_ENSURE(false, "Unsupported type");
}

} // namespace

size_t CalcMaxBlockItemSize(const TType* type) {
    // we do not count block bitmap size
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        size_t result = 0;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            result = std::max(result, CalcMaxBlockItemSize(tupleType->GetElementType(i)));
        }
        return result;
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Int16:
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
        case NUdf::EDataSlot::Float:
        case NUdf::EDataSlot::Double: {
            size_t sz = GetDataTypeInfo(slot).FixedSize;
            MKQL_ENSURE(sz > 0, "Unexpected fixed data size");
            return sz;
        }
        case NUdf::EDataSlot::String:
            // size of offset part
            return sizeof(arrow::BinaryType::offset_type);
        case NUdf::EDataSlot::Utf8:
            // size of offset part
            return sizeof(arrow::StringType::offset_type);
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }
    }

    MKQL_ENSURE(false, "Unsupported type");
}

std::unique_ptr<IBlockBuilder> MakeBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxBlockLength) {
    return MakeBlockBuilderImpl(type, pool, maxBlockLength);
}

} // namespace NMiniKQL
} // namespace NKikimr
