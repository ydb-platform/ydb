#include "mkql_block_trimmer.h"

#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/arrow/defs.h>
#include <yql/essentials/public/udf/arrow/dispatch_traits.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <arrow/array/data.h>
#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

class TBlockTrimmerBase : public IBlockTrimmer {
protected:
    TBlockTrimmerBase(arrow::MemoryPool* pool)
        : Pool_(pool)
    {}

    TBlockTrimmerBase() = delete;

    std::shared_ptr<arrow::Buffer> TrimNullBitmap(const std::shared_ptr<arrow::ArrayData>& array) {
        auto& nullBitmapBuffer = array->buffers[0];

        std::shared_ptr<arrow::Buffer> result;
        auto nullCount = array->GetNullCount();
        if (nullCount == array->length) {
            result = MakeDenseFalseBitmap(array->length, Pool_);
        } else if (nullCount > 0) {
            result = MakeDenseBitmapCopy(nullBitmapBuffer->data(), array->length, array->offset, Pool_);
        }

        return result;
    }

    template<typename TBuffer = NUdf::TResizeableBuffer>
    std::unique_ptr<arrow::ResizableBuffer> CreateResizableBuffer(size_t size) const {
        auto buffer = NUdf::AllocateResizableBuffer<TBuffer>(size, Pool_);
        ARROW_OK(buffer->Resize(size, false));
        return buffer;
    }

protected:
    arrow::MemoryPool* Pool_;
};

template<typename TLayout, bool Nullable>
class TFixedSizeBlockTrimmer : public TBlockTrimmerBase {
public:
    TFixedSizeBlockTrimmer(arrow::MemoryPool* pool)
        : TBlockTrimmerBase(pool)
    {}

    std::shared_ptr<arrow::ArrayData> Trim(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 2);
        Y_ENSURE(array->child_data.empty());

        std::shared_ptr<arrow::Buffer> trimmedNullBitmap;
        if constexpr (Nullable) {
            trimmedNullBitmap = TrimNullBitmap(array);
        }

        auto origData = array->GetValues<TLayout>(1);
        auto dataSize = sizeof(TLayout) * array->length;

        auto trimmedDataBuffer = CreateResizableBuffer(dataSize);
        memcpy(trimmedDataBuffer->mutable_data(), origData, dataSize);

        return arrow::ArrayData::Make(array->type, array->length, {std::move(trimmedNullBitmap), std::move(trimmedDataBuffer)}, array->GetNullCount());
    }
};

template<bool Nullable>
class TResourceBlockTrimmer : public TBlockTrimmerBase {
public:
    TResourceBlockTrimmer(arrow::MemoryPool* pool)
        : TBlockTrimmerBase(pool)
    {}

    std::shared_ptr<arrow::ArrayData> Trim(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 2);
        Y_ENSURE(array->child_data.empty());

        std::shared_ptr<arrow::Buffer> trimmedNullBitmap;
        if constexpr (Nullable) {
            trimmedNullBitmap = TrimNullBitmap(array);
        }

        auto origData = array->GetValues<NUdf::TUnboxedValue>(1);
        auto dataSize = sizeof(NUdf::TUnboxedValue) * array->length;

        auto trimmedBuffer = CreateResizableBuffer<NUdf::TResizableManagedBuffer<NUdf::TUnboxedValue>>(dataSize);
        auto trimmedBufferData = reinterpret_cast<NUdf::TUnboxedValue*>(trimmedBuffer->mutable_data());

        for (int64_t i = 0; i < array->length; i++) {
            ::new(&trimmedBufferData[i]) NUdf::TUnboxedValue(origData[i]);
        }

        return arrow::ArrayData::Make(array->type, array->length, {std::move(trimmedNullBitmap), std::move(trimmedBuffer)}, array->GetNullCount());
    }
};

class TSingularBlockTrimmer: public TBlockTrimmerBase {
public:
    TSingularBlockTrimmer(arrow::MemoryPool* pool)
        : TBlockTrimmerBase(pool) {
    }

    std::shared_ptr<arrow::ArrayData> Trim(const std::shared_ptr<arrow::ArrayData>& array) override {
        return array;
    }
};

template<typename TStringType, bool Nullable>
class TStringBlockTrimmer : public TBlockTrimmerBase {
    using TOffset = typename TStringType::offset_type;

public:
    TStringBlockTrimmer(arrow::MemoryPool* pool)
        : TBlockTrimmerBase(pool)
    {}

    std::shared_ptr<arrow::ArrayData> Trim(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 3);
        Y_ENSURE(array->child_data.empty());

        std::shared_ptr<arrow::Buffer> trimmedNullBitmap;
        if constexpr (Nullable) {
            trimmedNullBitmap = TrimNullBitmap(array);
        }

        auto origOffsetData = array->GetValues<TOffset>(1);
        auto origStringData = reinterpret_cast<const char*>(array->buffers[2]->data() + origOffsetData[0]);
        auto stringDataSize = origOffsetData[array->length] - origOffsetData[0];

        auto trimmedOffsetBuffer = CreateResizableBuffer(sizeof(TOffset) * (array->length + 1));
        auto trimmedStringBuffer = CreateResizableBuffer(stringDataSize);

        auto trimmedOffsetBufferData = reinterpret_cast<TOffset*>(trimmedOffsetBuffer->mutable_data());
        auto trimmedStringBufferData = reinterpret_cast<char*>(trimmedStringBuffer->mutable_data());

        for (int64_t i = 0; i < array->length + 1; i++) {
            trimmedOffsetBufferData[i] = origOffsetData[i] - origOffsetData[0];
        }
        memcpy(trimmedStringBufferData, origStringData, stringDataSize);

        return arrow::ArrayData::Make(array->type, array->length, {std::move(trimmedNullBitmap), std::move(trimmedOffsetBuffer), std::move(trimmedStringBuffer)}, array->GetNullCount());
    }
};

template<bool Nullable>
class TTupleBlockTrimmer : public TBlockTrimmerBase {
public:
    TTupleBlockTrimmer(std::vector<IBlockTrimmer::TPtr> children, arrow::MemoryPool* pool)
        : TBlockTrimmerBase(pool)
        , Children_(std::move(children))
    {}

    std::shared_ptr<arrow::ArrayData> Trim(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 1);

        std::shared_ptr<arrow::Buffer> trimmedNullBitmap;
        if constexpr (Nullable) {
            trimmedNullBitmap = TrimNullBitmap(array);
        }

        std::vector<std::shared_ptr<arrow::ArrayData>> trimmedChildren;
        Y_ENSURE(array->child_data.size() == Children_.size());
        for (size_t i = 0; i < Children_.size(); i++) {
            trimmedChildren.push_back(Children_[i]->Trim(array->child_data[i]));
        }

        return arrow::ArrayData::Make(array->type, array->length, {std::move(trimmedNullBitmap)}, std::move(trimmedChildren), array->GetNullCount());
    }

protected:
    TTupleBlockTrimmer(arrow::MemoryPool* pool)
        : TBlockTrimmerBase(pool)
    {}

protected:
    std::vector<IBlockTrimmer::TPtr> Children_;
};

template<typename TDate, bool Nullable>
class TTzDateBlockTrimmer : public TTupleBlockTrimmer<Nullable> {
    using TBase = TTupleBlockTrimmer<Nullable>;
    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

public:
    TTzDateBlockTrimmer(arrow::MemoryPool* pool)
        : TBase(pool)
    {
        this->Children_.push_back(std::make_unique<TFixedSizeBlockTrimmer<TDateLayout, false>>(pool));
        this->Children_.push_back(std::make_unique<TFixedSizeBlockTrimmer<ui16, false>>(pool));
    }
};

class TExternalOptionalBlockTrimmer : public TBlockTrimmerBase {
public:
    TExternalOptionalBlockTrimmer(IBlockTrimmer::TPtr inner, arrow::MemoryPool* pool)
        : TBlockTrimmerBase(pool)
        , Inner_(std::move(inner))
    {}

    std::shared_ptr<arrow::ArrayData> Trim(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 1);
        Y_ENSURE(array->child_data.size() == 1);

        auto trimmedNullBitmap = TrimNullBitmap(array);
        auto trimmedInner = Inner_->Trim(array->child_data[0]);

        return arrow::ArrayData::Make(array->type, array->length, {std::move(trimmedNullBitmap)}, {std::move(trimmedInner)}, array->GetNullCount());
    }

private:
    IBlockTrimmer::TPtr Inner_;
};

struct TTrimmerTraits {
    using TResult = IBlockTrimmer;
    template <bool Nullable>
    using TTuple = TTupleBlockTrimmer<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeBlockTrimmer<T, Nullable>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot>
    using TStrings = TStringBlockTrimmer<TStringType, Nullable>;
    using TExtOptional = TExternalOptionalBlockTrimmer;
    template<bool Nullable>
    using TResource = TResourceBlockTrimmer<Nullable>;
    template<typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateBlockTrimmer<TTzDate, Nullable>;
    using TSingular = TSingularBlockTrimmer;

    constexpr static bool PassType = false;

    static TResult::TPtr MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool* pool) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>(pool);
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true, NKikimr::NUdf::EDataSlot::String>>(pool);
        }
    }

    static TResult::TPtr MakeResource(bool isOptional, arrow::MemoryPool* pool) {
        if (isOptional) {
            return std::make_unique<TResource<true>>(pool);
        } else {
            return std::make_unique<TResource<false>>(pool);
        }
    }

    static TResult::TPtr MakeSingular(arrow::MemoryPool* pool) {
        return std::make_unique<TSingular>(pool);
    }

    template<typename TTzDate>
    static TResult::TPtr MakeTzDate(bool isOptional, arrow::MemoryPool* pool) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>(pool);
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>(pool);
        }
    }
};

IBlockTrimmer::TPtr MakeBlockTrimmer(const NUdf::ITypeInfoHelper& typeInfoHelper, const NUdf::TType* type, arrow::MemoryPool* pool) {
    return DispatchByArrowTraits<TTrimmerTraits>(typeInfoHelper, type, nullptr, pool);
}

}
