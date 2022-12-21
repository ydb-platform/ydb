#include "mkql_block_builder.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <arrow/array/builder_primitive.h>
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

// size of each block item in bytes
size_t CalcItemSize(const TType* type) {
    // we do not count block bitmap size
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        size_t result = 0;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            result += CalcItemSize(tupleType->GetElementType(i));
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

std::shared_ptr<arrow::DataType> GetArrowType(TType* type) {
    std::shared_ptr<arrow::DataType> result;
    bool isOptional;
    Y_VERIFY(ConvertArrowType(type, isOptional, result));
    return result;
}

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

    void Add(const NUdf::TUnboxedValue& value) final {
        Y_VERIFY(CurrLen < MaxLen);
        DoAdd(value);
        CurrLen++;
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
    virtual void DoAdd(const NUdf::TUnboxedValue& value) = 0;

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
            sliced = main->Slice(0, size);
            main = main->Slice(size, main->length - size);
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

template <typename T, typename TBuilder>
class TFixedSizeBlockBuilder : public TBlockBuilderBase {
public:
    TFixedSizeBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : TBlockBuilderBase(type, pool, maxLen)
        , Builder(std::make_unique<TBuilder>(&pool))
    {
        Reserve();
    }

    void DoAdd(const NUdf::TUnboxedValue& value) final {
        if (value) {
            Builder->UnsafeAppend(value.Get<T>());
        } else {
            Builder->UnsafeAppendNull();
        }
    }

    TBlockArrayTree::Ptr BuildTree(bool finish) final {
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();
        result->Payload.emplace_back();
        ARROW_OK(Builder->FinishInternal(&result->Payload.back()));
        Builder.reset();
        if (!finish) {
            Builder = std::make_unique<TBuilder>(Pool);
            Reserve();
        }
        return result;
    }

private:
    void Reserve() {
        ARROW_OK(Builder->Reserve(MaxLen));
    }

    std::unique_ptr<TBuilder> Builder;
};

template<typename TStringType>
class TStringBlockBuilder : public TBlockBuilderBase {
public:
    TStringBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : TBlockBuilderBase(type, pool, maxLen)
    {
        Reserve();
    }

    void DoAdd(const NUdf::TUnboxedValue& value) final {
        if (!value) {
            NullBitmapBuilder->UnsafeAppend(false);
            AppendCurrentOffset();
            NullCount++;
            return;
        }

        const TStringBuf str = value.AsStringRef();

        size_t currentLen = DataBuilder->length();
        // empty string can always be appended
        if (!str.empty() && currentLen + str.size() > MaxBlockSizeInBytes) {
            if (currentLen) {
                FlushChunk(false);
            }
            if (str.size() > MaxBlockSizeInBytes) {
                ARROW_OK(DataBuilder->Reserve(str.size()));
            }
        }

        NullBitmapBuilder->UnsafeAppend(true);
        AppendCurrentOffset();
        DataBuilder->UnsafeAppend((const ui8*)str.data(), str.size());
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
        NullBitmapBuilder = std::make_unique<arrow::TypedBufferBuilder<bool>>(Pool);
        OffsetsBuilder = std::make_unique<arrow::TypedBufferBuilder<typename TStringType::offset_type>>(Pool);
        DataBuilder = std::make_unique<arrow::TypedBufferBuilder<ui8>>(Pool);
        ARROW_OK(NullBitmapBuilder->Reserve(MaxLen));
        ARROW_OK(OffsetsBuilder->Reserve(MaxLen + 1));
        ARROW_OK(DataBuilder->Reserve(MaxBlockSizeInBytes));
        NullCount = 0;
    }

    void AppendCurrentOffset() {
        OffsetsBuilder->UnsafeAppend(DataBuilder->length());
    }

    void FlushChunk(bool finish) {
        const auto length = NullBitmapBuilder->length();
        Y_VERIFY(length > 0);

        AppendCurrentOffset();
        std::shared_ptr<arrow::Buffer> nullBitmap;
        std::shared_ptr<arrow::Buffer> offsets;
        std::shared_ptr<arrow::Buffer> data;
        if (NullCount) {
            ARROW_OK(NullBitmapBuilder->Finish(&nullBitmap));
        }
        ARROW_OK(OffsetsBuilder->Finish(&offsets));
        ARROW_OK(DataBuilder->Finish(&data));
        auto arrowType = std::make_shared<TStringType>();
        Chunks.push_back(arrow::ArrayData::Make(std::make_shared<TStringType>(), length,
                                                { nullBitmap, offsets, data }, NullCount, 0));
        if (!finish) {
            Reserve();
        }
    }

    std::unique_ptr<arrow::TypedBufferBuilder<bool>> NullBitmapBuilder;
    std::unique_ptr<arrow::TypedBufferBuilder<typename TStringType::offset_type>> OffsetsBuilder;
    std::unique_ptr<arrow::TypedBufferBuilder<ui8>> DataBuilder;
    std::deque<std::shared_ptr<arrow::ArrayData>> Chunks;
    size_t NullCount = 0;
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

    void DoAdd(const NUdf::TUnboxedValue& value) final {
        auto tupleType = AS_TYPE(TTupleType, Type);
        if (!value) {
            NullBitmapBuilder->UnsafeAppend(false);
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                Children[i]->DoAdd({});
            }
            return;
        }

        NullBitmapBuilder->UnsafeAppend(true);
        auto elements = value.GetElements();
        if (elements) {
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                Children[i]->DoAdd(elements[i]);
            }
        } else {
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                Children[i]->DoAdd(value.GetElement(i));
            }
        }
    }

    TBlockArrayTree::Ptr BuildTree(bool finish) final {
        auto tupleType = AS_TYPE(TTupleType, Type);
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();

        std::shared_ptr<arrow::Buffer> nullBitmap;
        auto length = NullBitmapBuilder->length();
        ARROW_OK(NullBitmapBuilder->Finish(&nullBitmap));
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
        NullBitmapBuilder = std::make_unique<arrow::TypedBufferBuilder<bool>>(Pool);
        ARROW_OK(NullBitmapBuilder->Reserve(MaxLen));
    }

private:
    TVector<std::unique_ptr<TBlockBuilderBase>> Children;
    std::unique_ptr<arrow::TypedBufferBuilder<bool>> NullBitmapBuilder;
};

std::unique_ptr<TBlockBuilderBase> MakeBlockBuilder(TType* type, arrow::MemoryPool& pool, size_t maxLen) {
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        TVector<std::unique_ptr<TBlockBuilderBase>> children;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            children.emplace_back(MakeBlockBuilder(tupleType->GetElementType(i), pool, maxLen));
        }

        return std::make_unique<TTupleBlockBuilder>(tupleType, pool, maxLen, std::move(children));
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TFixedSizeBlockBuilder<i8, arrow::Int8Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return std::make_unique<TFixedSizeBlockBuilder<ui8, arrow::UInt8Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TFixedSizeBlockBuilder<i16, arrow::Int16Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TFixedSizeBlockBuilder<ui16, arrow::UInt16Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TFixedSizeBlockBuilder<i32, arrow::Int32Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TFixedSizeBlockBuilder<ui32, arrow::UInt32Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<TFixedSizeBlockBuilder<i64, arrow::Int64Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TFixedSizeBlockBuilder<ui64, arrow::UInt64Builder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Float:
            return std::make_unique<TFixedSizeBlockBuilder<float, arrow::FloatBuilder>>(type, pool, maxLen);
        case NUdf::EDataSlot::Double:
            return std::make_unique<TFixedSizeBlockBuilder<double, arrow::DoubleBuilder>>(type, pool, maxLen);
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

std::unique_ptr<IBlockBuilder> MakeBlockBuilder(TType* type, arrow::MemoryPool& pool) {
    const auto itemSize = std::max<size_t>(CalcItemSize(type), 1);
    const auto maxLen = std::max<size_t>(MaxBlockSizeInBytes / itemSize, 1);
    return MakeBlockBuilder(type, pool, maxLen);
}


} // namespace NMiniKQL
} // namespace NKikimr
