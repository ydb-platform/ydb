#pragma once

#include "util.h"
#include "bit_util.h"
#include "block_io_buffer.h"
#include "block_item.h"
#include "block_type_helper.h"
#include "dispatch_traits.h"

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>

#include <arrow/array/array_base.h>
#include <arrow/datum.h>
#include <arrow/c/bridge.h>

#include <deque>

namespace NYql::NUdf {

class IArrayBuilder {
public:
    struct TArrayDataItem {
        const arrow::ArrayData* Data = nullptr;
        ui64 StartOffset;
    };
    virtual ~IArrayBuilder() = default;
    virtual size_t MaxLength() const = 0;
    virtual void Add(NUdf::TUnboxedValuePod value) = 0;
    virtual void Add(TBlockItem value) = 0;
    virtual void Add(TBlockItem value, size_t count) = 0;
    virtual void Add(TInputBuffer& input) = 0;
    virtual void AddMany(const arrow::ArrayData& array, size_t popCount, const ui8* sparseBitmap, size_t bitmapSize) = 0;
    virtual void AddMany(const TArrayDataItem* arrays, size_t arrayCount, ui64 beginIndex, size_t count) = 0;
    virtual void AddMany(const TArrayDataItem* arrays, size_t arrayCount, const ui64* indexes, size_t count) = 0;
    virtual arrow::Datum Build(bool finish) = 0;
};

inline const IArrayBuilder::TArrayDataItem* LookupArrayDataItem(const IArrayBuilder::TArrayDataItem* arrays, size_t arrayCount, ui64& idx) {
    IArrayBuilder::TArrayDataItem lookup{nullptr, idx};

    auto it = std::lower_bound(arrays, arrays + arrayCount, lookup, [](const auto& left, const auto& right) {
        return left.StartOffset < right.StartOffset;
    });

    if (it == arrays + arrayCount || it->StartOffset > idx) {
        --it;
    }

    Y_DEBUG_ABORT_UNLESS(it->StartOffset <= idx);
    idx -= it->StartOffset;
    return it;
}

class IScalarBuilder {
public:
    virtual ~IScalarBuilder() = default;
    virtual arrow::Datum Build(TBlockItem value) const = 0;
    virtual arrow::Datum Build(NUdf::TUnboxedValuePod value) const = 0;
};

inline std::shared_ptr<arrow::DataType> GetArrowType(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    auto arrowTypeHandle = typeInfoHelper.MakeArrowType(type);
    Y_ENSURE(arrowTypeHandle);
    ArrowSchema s;
    arrowTypeHandle->Export(&s);
    return ARROW_RESULT(arrow::ImportType(&s));
}

class TArrayBuilderBase: public IArrayBuilder {
    using Self = TArrayBuilderBase;

public:
    using Ptr = std::unique_ptr<TArrayBuilderBase>;

    struct TBlockArrayTree {
        using Ptr = std::shared_ptr<TBlockArrayTree>;
        std::deque<std::shared_ptr<arrow::ArrayData>> Payload;
        std::vector<TBlockArrayTree::Ptr> Children;
    };

    struct TParams {
        size_t* TotalAllocated = nullptr;
        TMaybe<ui8> MinFillPercentage; // if an internal buffer size is smaller than % of capacity, then shrink the buffer.
    };

    TArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, const TParams& params)
        : ArrowType_(std::move(arrowType))
        , Pool_(&pool)
        , MaxLen_(maxLen)
        , MaxBlockSizeInBytes_(typeInfoHelper.GetMaxBlockBytes())
        , MinFillPercentage_(params.MinFillPercentage)
        , TotalAllocated_(params.TotalAllocated)
    {
        Y_ABORT_UNLESS(ArrowType_);
        Y_ABORT_UNLESS(maxLen > 0);
    }

    TArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, const TParams& params)
        : TArrayBuilderBase(typeInfoHelper, GetArrowType(typeInfoHelper, type), pool, maxLen, params)
    {
    }

    size_t MaxLength() const final {
        return MaxLen_;
    }

    void Add(NUdf::TUnboxedValuePod value) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen_ < MaxLen_);
        DoAdd(value);
        CurrLen_++;
    }

    void Add(TBlockItem value) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen_ < MaxLen_);
        DoAdd(value);
        CurrLen_++;
    }

    void Add(TBlockItem value, size_t count) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen_ + count <= MaxLen_);
        DoAdd(value, count);
        CurrLen_ += count;
    }

    void Add(TInputBuffer& input) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen_ < MaxLen_);
        DoAdd(input);
        CurrLen_++;
    }

    void AddDefault() {
        Y_DEBUG_ABORT_UNLESS(CurrLen_ < MaxLen_);
        DoAddDefault();
        CurrLen_++;
    }

    inline void AddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) {
        TArrayDataItem item = {&array, 0};
        Self::AddMany(&item, 1, beginIndex, count);
    }

    inline void AddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) {
        TArrayDataItem item = {&array, 0};
        Self::AddMany(&item, 1, indexes, count);
    }

    void AddMany(const arrow::ArrayData& array, size_t popCount, const ui8* sparseBitmap, size_t bitmapSize) final {
        Y_ABORT_UNLESS(size_t(array.length) == bitmapSize);
        Y_ABORT_UNLESS(popCount <= bitmapSize);
        Y_ABORT_UNLESS(CurrLen_ + popCount <= MaxLen_);

        if (popCount) {
            DoAddMany(array, sparseBitmap, popCount);
        }

        CurrLen_ += popCount;
    }

    void AddMany(const TArrayDataItem* arrays, size_t arrayCount, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(arrays);
        Y_ABORT_UNLESS(arrayCount > 0);
        if (arrayCount == 1) {
            Y_ABORT_UNLESS(arrays->Data);
            DoAddMany(*arrays->Data, beginIndex, count);
        } else {
            ui64 idx = beginIndex;
            auto item = LookupArrayDataItem(arrays, arrayCount, idx);
            size_t avail = item->Data->length;
            size_t toAdd = count;
            Y_ABORT_UNLESS(idx <= avail);
            while (toAdd) {
                size_t adding = std::min(avail, toAdd);
                DoAddMany(*item->Data, idx, adding);
                avail -= adding;
                toAdd -= adding;

                if (!avail && toAdd) {
                    ++item;
                    Y_ABORT_UNLESS(item < arrays + arrayCount);
                    avail = item->Data->length;
                    idx = 0;
                }
            }
        }
        CurrLen_ += count;
    }

    void AddMany(const TArrayDataItem* arrays, size_t arrayCount, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(arrays);
        Y_ABORT_UNLESS(arrayCount > 0);
        Y_ABORT_UNLESS(indexes);
        Y_ABORT_UNLESS(CurrLen_ + count <= MaxLen_);

        if (arrayCount == 1) {
            Y_ABORT_UNLESS(arrays->Data);
            DoAddMany(*arrays->Data, indexes, count);
            CurrLen_ += count;
        } else {
            const IArrayBuilder::TArrayDataItem* currData = nullptr;
            TVector<ui64> currDataIndexes;
            for (size_t i = 0; i < count; ++i) {
                ui64 idx = indexes[i];
                const IArrayBuilder::TArrayDataItem* data = LookupArrayDataItem(arrays, arrayCount, idx);
                if (!currData) {
                    currData = data;
                }

                if (data != currData) {
                    DoAddMany(*currData->Data, currDataIndexes.data(), currDataIndexes.size());
                    CurrLen_ += currDataIndexes.size();
                    currDataIndexes.clear();
                    currData = data;
                }
                currDataIndexes.push_back(idx);
            }
            if (!currDataIndexes.empty()) {
                DoAddMany(*currData->Data, currDataIndexes.data(), currDataIndexes.size());
                CurrLen_ += currDataIndexes.size();
            }
        }
    }

    arrow::Datum Build(bool finish) final {
        auto tree = BuildTree(finish);
        TVector<std::shared_ptr<arrow::ArrayData>> chunks;
        while (size_t size = CalcSliceSize(*tree)) {
            chunks.push_back(Slice(*tree, size));
        }

        return MakeArray(chunks);
    }

    TBlockArrayTree::Ptr BuildTree(bool finish) {
        auto result = DoBuildTree(finish);
        CurrLen_ = 0;
        return result;
    }

protected:
    virtual void DoAdd(NUdf::TUnboxedValuePod value) = 0;
    virtual void DoAdd(TBlockItem value) = 0;
    virtual void DoAdd(TBlockItem value, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            DoAdd(value);
        }
    }
    virtual void DoAdd(TInputBuffer& input) = 0;
    virtual void DoAddDefault() = 0;
    virtual void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) = 0;
    virtual void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) = 0;
    virtual void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) = 0;
    virtual TBlockArrayTree::Ptr DoBuildTree(bool finish) = 0;

    // returns the newly allocated size in bytes
    virtual size_t DoReserve() = 0;

private:
    static size_t CalcSliceSize(const TBlockArrayTree& tree) {
        if (tree.Payload.empty()) {
            return 0;
        }

        if (!tree.Children.empty()) {
            Y_ABORT_UNLESS(tree.Payload.size() == 1);
            size_t result = std::numeric_limits<size_t>::max();
            for (auto& child : tree.Children) {
                size_t childSize = CalcSliceSize(*child);
                result = std::min(result, childSize);
            }
            Y_ABORT_UNLESS(result <= size_t(tree.Payload.front()->length));
            return result;
        }

        int64_t result = tree.Payload.front()->length;
        Y_ABORT_UNLESS(result > 0);
        return static_cast<size_t>(result);
    }

    static std::shared_ptr<arrow::ArrayData> Slice(TBlockArrayTree& tree, size_t size) {
        Y_ABORT_UNLESS(size > 0);

        Y_ABORT_UNLESS(!tree.Payload.empty());
        auto& main = tree.Payload.front();
        std::shared_ptr<arrow::ArrayData> sliced;
        if (size == size_t(main->length)) {
            sliced = main;
            tree.Payload.pop_front();
        } else {
            Y_ABORT_UNLESS(size < size_t(main->length));
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
    size_t GetCurrLen() const {
        return CurrLen_;
    }

    void SetCurrLen(size_t len) {
        Y_ABORT_UNLESS(len <= MaxLen_);
        CurrLen_ = len;
    }

    void Reserve() {
        auto allocated = DoReserve();
        if (TotalAllocated_) {
            *TotalAllocated_ += allocated;
        }
    }

    void AddExtraAllocated(size_t bytes) {
        if (TotalAllocated_) {
            *TotalAllocated_ += bytes;
        }
    }

    const std::shared_ptr<arrow::DataType> ArrowType_;
    arrow::MemoryPool* const Pool_;
    const size_t MaxLen_;
    const size_t MaxBlockSizeInBytes_;
    const TMaybe<ui8> MinFillPercentage_;

private:
    size_t CurrLen_ = 0;
    size_t* TotalAllocated_ = nullptr;
};

template <typename TLayout, bool Nullable, typename TDerived>
class TFixedSizeArrayBuilderBase: public TArrayBuilderBase {
public:
    TFixedSizeArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, const TParams& params)
        : TArrayBuilderBase(typeInfoHelper, std::move(arrowType), pool, maxLen, params)
    {
        Reserve();
    }

    TFixedSizeArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, const TParams& params)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, params)
    {
        Reserve();
    }

    void UnsafeReserve(size_t length) {
        SetCurrLen(length);
    }

    TLayout* MutableData() {
        return DataPtr_;
    }

    ui8* MutableValidMask() {
        return NullPtr_;
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                DoAddNull();
                return;
            }
            NullPtr_[GetCurrLen()] = 1;
        }
        static_cast<TDerived*>(this)->DoAddNotNull(value);
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                DoAddNull();
                return;
            }
            NullPtr_[GetCurrLen()] = 1;
        }
        static_cast<TDerived*>(this)->DoAddNotNull(value);
    }

    void DoAddNull() {
        if constexpr (Nullable) {
            NullPtr_[GetCurrLen()] = 0;
            PlaceItem(TLayout{});
        }
    }

    void DoAdd(TBlockItem value, size_t count) final {
        if constexpr (Nullable) {
            if (!value) {
                std::fill(NullPtr_ + GetCurrLen(), NullPtr_ + GetCurrLen() + count, 0);
                std::fill(DataPtr_ + GetCurrLen(), DataPtr_ + GetCurrLen() + count, TLayout{});
                return;
            }
            std::fill(NullPtr_ + GetCurrLen(), NullPtr_ + GetCurrLen() + count, 1);
        }

        static_cast<TDerived*>(this)->DoAddNotNull(value, count);
    }

    void DoAdd(TInputBuffer& input) final {
        if constexpr (Nullable) {
            if (!input.PopChar()) {
                DoAddNull();
                return;
            }
        }
        static_cast<TDerived*>(this)->DoAddNotNull(input);
    }

    void DoAddDefault() final {
        if constexpr (Nullable) {
            NullPtr_[GetCurrLen()] = 1;
        }
        PlaceItem(TLayout{});
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            if (array.buffers.front()) {
                ui8* dstBitmap = NullPtr_ + GetCurrLen();
                CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
            } else {
                ui8* dstBitmap = NullPtr_ + GetCurrLen();
                std::fill_n(dstBitmap, popCount, 1);
            }
        }

        const TLayout* src = array.GetValues<TLayout>(1);
        TLayout* dst = DataPtr_ + GetCurrLen();
        CompressArray(src, sparseBitmap, dst, array.length);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            for (size_t i = beginIndex; i < beginIndex + count; ++i) {
                NullPtr_[GetCurrLen() + i - beginIndex] = !IsNull(array, i);
            }
        }

        const TLayout* values = array.GetValues<TLayout>(1);
        for (size_t i = beginIndex; i < beginIndex + count; ++i) {
            ::new (DataPtr_ + GetCurrLen() + i - beginIndex) TLayout(values[i]);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            for (size_t i = 0; i < count; ++i) {
                NullPtr_[GetCurrLen() + i] = !IsNull(array, indexes[i]);
            }
        }

        const TLayout* values = array.GetValues<TLayout>(1);
        for (size_t i = 0; i < count; ++i) {
            ::new (DataPtr_ + GetCurrLen() + i) TLayout(values[indexes[i]]);
        }
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        const size_t len = GetCurrLen();
        std::shared_ptr<arrow::Buffer> nulls;
        if constexpr (Nullable) {
            NullBuilder_->UnsafeAdvance(len);
            nulls = NullBuilder_->Finish();
            nulls = MakeDenseBitmap(nulls->data(), len, Pool_);
        }
        DataBuilder_->UnsafeAdvance(len);
        std::shared_ptr<arrow::Buffer> data = DataBuilder_->Finish();

        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();
        result->Payload.push_back(arrow::ArrayData::Make(ArrowType_, len, {nulls, data}));

        NullBuilder_.reset();
        DataBuilder_.reset();
        if (!finish) {
            Reserve();
        }
        return result;
    }

protected:
    void PlaceItem(TLayout&& value) {
        ::new (DataPtr_ + GetCurrLen()) TLayout(std::move(value));
    }

    TLayout* DataPtr_ = nullptr;

private:
    size_t DoReserve() final {
        DataBuilder_ = std::make_unique<TTypedBufferBuilder<TLayout>>(Pool_, MinFillPercentage_);
        DataBuilder_->Reserve(MaxLen_ + 1);
        DataPtr_ = DataBuilder_->MutableData();
        auto result = DataBuilder_->Capacity();
        if constexpr (Nullable) {
            NullBuilder_ = std::make_unique<TTypedBufferBuilder<ui8>>(Pool_, MinFillPercentage_);
            NullBuilder_->Reserve(MaxLen_ + 1);
            NullPtr_ = NullBuilder_->MutableData();
            result += NullBuilder_->Capacity();
        }
        return result;
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder_;
    std::unique_ptr<TTypedBufferBuilder<TLayout>> DataBuilder_;
    ui8* NullPtr_ = nullptr;
};

template <typename TLayout, bool Nullable>
class TFixedSizeArrayBuilder final: public TFixedSizeArrayBuilderBase<TLayout, Nullable, TFixedSizeArrayBuilder<TLayout, Nullable>> {
    using TSelf = TFixedSizeArrayBuilder<TLayout, Nullable>;
    using TBase = TFixedSizeArrayBuilderBase<TLayout, Nullable, TSelf>;
    using TParams = TArrayBuilderBase::TParams;

public:
    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, std::move(arrowType), pool, maxLen, params)
    {
    }

    TFixedSizeArrayBuilder(const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, type, pool, maxLen, params)
    {
    }

    void DoAddNotNull(TUnboxedValuePod value) {
        this->PlaceItem(value.Get<TLayout>());
    }

    void DoAddNotNull(TBlockItem value) {
        this->PlaceItem(value.Get<TLayout>());
    }

    void DoAddNotNull(TInputBuffer& input) {
        this->DoAdd(TBlockItem(input.PopNumber<TLayout>()));
    }

    void DoAddNotNull(TBlockItem value, size_t count) {
        std::fill(this->DataPtr_ + this->GetCurrLen(), this->DataPtr_ + this->GetCurrLen() + count, value.Get<TLayout>());
    }
};

template <bool Nullable>
class TFixedSizeArrayBuilder<NYql::NDecimal::TInt128, Nullable> final: public TFixedSizeArrayBuilderBase<NYql::NDecimal::TInt128, Nullable, TFixedSizeArrayBuilder<NYql::NDecimal::TInt128, Nullable>> {
    using TSelf = TFixedSizeArrayBuilder<NYql::NDecimal::TInt128, Nullable>;
    using TBase = TFixedSizeArrayBuilderBase<NYql::NDecimal::TInt128, Nullable, TSelf>;
    using TParams = TArrayBuilderBase::TParams;

public:
    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, std::move(arrowType), pool, maxLen, params)
    {
    }

    TFixedSizeArrayBuilder(const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, type, pool, maxLen, params)
    {
    }

    void DoAddNotNull(TUnboxedValuePod value) {
        this->PlaceItem(value.GetInt128());
    }

    void DoAddNotNull(TBlockItem value) {
        this->PlaceItem(value.GetInt128());
    }

    void DoAddNotNull(TInputBuffer& input) {
        this->DoAdd(TBlockItem(input.PopNumber<NYql::NDecimal::TInt128>()));
    }

    void DoAddNotNull(TBlockItem value, size_t count) {
        std::fill(this->DataPtr_ + this->GetCurrLen(), this->DataPtr_ + this->GetCurrLen() + count, value.GetInt128());
    }
};

template <bool Nullable>
class TResourceArrayBuilder final: public TFixedSizeArrayBuilderBase<TUnboxedValue, Nullable, TResourceArrayBuilder<Nullable>> {
    using TBase = TFixedSizeArrayBuilderBase<TUnboxedValue, Nullable, TResourceArrayBuilder<Nullable>>;
    using TParams = TArrayBuilderBase::TParams;

public:
    TResourceArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, std::move(arrowType), pool, maxLen, params)
    {
    }

    TResourceArrayBuilder(const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, type, pool, maxLen, params)
    {
    }

    void DoAddNotNull(TUnboxedValuePod value) {
        this->PlaceItem(TUnboxedValue(value));
    }

    TUnboxedValue FromBlockItem(TBlockItem item) {
        TUnboxedValue val;
        std::memcpy(val.GetRawPtr(), item.GetRawPtr(), sizeof(val));
        val.Ref();
        return val;
    }

    void DoAddNotNull(TBlockItem item) {
        this->PlaceItem(FromBlockItem(item));
    }

    void DoAddNotNull(TInputBuffer& input) {
        this->DoAdd(input.PopNumber<TUnboxedValuePod>());
    }

    void DoAddNotNull(TBlockItem item, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            ::new (this->DataPtr_ + this->GetCurrLen() + i) TUnboxedValue(FromBlockItem(item));
        }
    }
};

template <typename TStringType, bool Nullable, EPgStringType PgString = EPgStringType::None>
class TStringArrayBuilder final: public TArrayBuilderBase {
    using TOffset = typename TStringType::offset_type;

public:
    TStringArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TArrayBuilderBase(typeInfoHelper, std::move(arrowType), pool, maxLen, params)
    {
        Reserve();
    }

    TStringArrayBuilder(const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, params)
    {
        Reserve();
    }

    void SetPgBuilder(const NUdf::IPgBuilder* pgBuilder, i32 typeLen) {
        Y_ENSURE(PgString != EPgStringType::None);
        PgBuilder_ = pgBuilder;
        TypeLen_ = typeLen;
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                return DoAdd(TBlockItem{});
            }
        }

        if constexpr (PgString == EPgStringType::CString) {
            static_assert(Nullable);
            auto buf = PgBuilder_->AsCStringBuffer(value);
            AddPgItem(buf);
        } else if constexpr (PgString == EPgStringType::Text) {
            static_assert(Nullable);
            auto buf = PgBuilder_->AsTextBuffer(value);
            AddPgItem(buf);
        } else if constexpr (PgString == EPgStringType::Fixed) {
            static_assert(Nullable);
            auto buf = PgBuilder_->AsFixedStringBuffer(value, TypeLen_);
            AddPgItem(buf);
        } else {
            DoAdd(TBlockItem(value.AsStringRef()));
        }
    }

    template <bool AddCStringZero = false, ui32 AddVarHdr = 0>
    ui8* AddPgItem(TStringRef buf) {
        auto alignedSize = AlignUp(buf.Size() + sizeof(void*) + AddVarHdr + (AddCStringZero ? 1 : 0), sizeof(void*));
        auto ptr = AddNoFill(alignedSize);
        *(void**)ptr = nullptr;
        if (alignedSize > sizeof(void*)) {
            // clear padding too
            *(void**)(ptr + alignedSize - sizeof(void*)) = nullptr;
        }

        std::memcpy(ptr + sizeof(void*) + AddVarHdr, buf.Data(), buf.Size());
        if constexpr (AddCStringZero) {
            ptr[sizeof(void*) + buf.Size()] = 0;
        }

        return ptr;
    }

    ui8* AddNoFill(size_t size) {
        size_t currentLen = DataBuilder_->Length();
        // empty string can always be appended
        if (size > 0 && currentLen + size > MaxBlockSizeInBytes_) {
            if (currentLen) {
                FlushChunk(false);
            }
            if (size > MaxBlockSizeInBytes_) {
                ReserveForLargeString(size);
            }
        }

        AppendCurrentOffset();
        auto ret = DataBuilder_->End();
        DataBuilder_->UnsafeAdvance(size);
        if constexpr (Nullable) {
            NullBuilder_->UnsafeAppend(1);
        }

        return ret;
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder_->UnsafeAppend(0);
                AppendCurrentOffset();
                return;
            }
        }

        const std::string_view str = value.AsStringRef();
        auto ptr = AddNoFill(str.size());
        if (str.data()) {
            std::memcpy(ptr, str.data(), str.size());
        }
    }

    void DoAdd(TInputBuffer& input) final {
        if constexpr (Nullable) {
            if (!input.PopChar()) {
                return DoAdd(TBlockItem{});
            }
        }

        auto str = input.PopString();
        TStringRef ref(str.data(), str.size());
        DoAdd(TBlockItem(ref));
    }

    void DoAddDefault() final {
        if constexpr (Nullable) {
            NullBuilder_->UnsafeAppend(1);
        }
        AppendCurrentOffset();
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_UNUSED(popCount);
        Y_ABORT_UNLESS(array.buffers.size() > 2);
        Y_ABORT_UNLESS(!Nullable || NullBuilder_->Length() == OffsetsBuilder_->Length());

        const ui8* srcNulls = array.GetValues<ui8>(0, 0);
        const TOffset* srcOffset = array.GetValues<TOffset>(1);
        const ui8* srcData = array.GetValues<ui8>(2, 0);

        const ui8* chunkStart = srcData;
        const ui8* chunkEnd = chunkStart;
        size_t dataLen = DataBuilder_->Length();

        ui8* dstNulls = Nullable ? NullBuilder_->End() : nullptr;
        TOffset* dstOffset = OffsetsBuilder_->End();
        size_t countAdded = 0;
        for (size_t i = 0; i < size_t(array.length); i++) {
            if (!sparseBitmap[i]) {
                continue;
            }

            const ui8* begin = srcData + srcOffset[i];
            const ui8* end = srcData + srcOffset[i + 1];
            const size_t strSize = end - begin;

            size_t availBytes = std::max(dataLen, MaxBlockSizeInBytes_) - dataLen;

            for (;;) {
                // try to append ith string
                if (strSize <= availBytes) {
                    if (begin == chunkEnd) {
                        chunkEnd = end;
                    } else {
                        DataBuilder_->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
                        chunkStart = begin;
                        chunkEnd = end;
                    }

                    size_t nullOffset = i + array.offset;
                    if constexpr (Nullable) {
                        *dstNulls++ = srcNulls ? ((srcNulls[nullOffset >> 3] >> (nullOffset & 7)) & 1) : 1u;
                    }
                    *dstOffset++ = dataLen;

                    dataLen += strSize;
                    ++countAdded;

                    break;
                }

                if (dataLen) {
                    if (chunkStart != chunkEnd) {
                        DataBuilder_->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
                        chunkStart = chunkEnd = srcData;
                    }
                    Y_ABORT_UNLESS(dataLen == DataBuilder_->Length());
                    OffsetsBuilder_->UnsafeAdvance(countAdded);
                    if constexpr (Nullable) {
                        NullBuilder_->UnsafeAdvance(countAdded);
                    }
                    FlushChunk(false);

                    dataLen = 0;
                    countAdded = 0;
                    if constexpr (Nullable) {
                        dstNulls = NullBuilder_->End();
                    }
                    dstOffset = OffsetsBuilder_->End();
                } else {
                    ReserveForLargeString(strSize);
                    availBytes = strSize;
                }
            }
        }
        if (chunkStart != chunkEnd) {
            DataBuilder_->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
        }
        Y_ABORT_UNLESS(dataLen == DataBuilder_->Length());
        OffsetsBuilder_->UnsafeAdvance(countAdded);
        if constexpr (Nullable) {
            NullBuilder_->UnsafeAdvance(countAdded);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 2);
        Y_ABORT_UNLESS(!Nullable || NullBuilder_->Length() == OffsetsBuilder_->Length());

        size_t dataLen = DataBuilder_->Length();

        const TOffset* offsets = array.GetValues<TOffset>(1);
        const ui8* srcData = array.GetValues<ui8>(2, 0);
        const ui8* chunkStart = srcData + offsets[beginIndex];
        const ui8* chunkEnd = chunkStart;
        for (size_t i = beginIndex; i < beginIndex + count; ++i) {
            const ui8* begin = srcData + offsets[i];
            const ui8* end = srcData + offsets[i + 1];
            const size_t strSize = end - begin;

            size_t availBytes = std::max(dataLen, MaxBlockSizeInBytes_) - dataLen;
            for (;;) {
                if (strSize <= availBytes) {
                    if constexpr (Nullable) {
                        NullBuilder_->UnsafeAppend(!IsNull(array, i));
                    }
                    OffsetsBuilder_->UnsafeAppend(TOffset(dataLen));
                    chunkEnd = end;
                    dataLen += strSize;
                    break;
                }

                if (dataLen) {
                    DataBuilder_->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
                    chunkStart = begin;
                    chunkEnd = end;
                    FlushChunk(false);
                    dataLen = 0;
                } else {
                    ReserveForLargeString(strSize);
                    availBytes = strSize;
                }
            }
        }
        if (chunkStart != chunkEnd) {
            DataBuilder_->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 2);
        Y_ABORT_UNLESS(!Nullable || NullBuilder_->Length() == OffsetsBuilder_->Length());

        size_t dataLen = DataBuilder_->Length();

        const TOffset* offsets = array.GetValues<TOffset>(1);
        const char* strData = array.GetValues<char>(2, 0);
        for (size_t i = 0; i < count; ++i) {
            ui64 idx = indexes[i];
            std::string_view str(strData + offsets[idx], offsets[idx + 1] - offsets[idx]);

            size_t availBytes = std::max(dataLen, MaxBlockSizeInBytes_) - dataLen;
            for (;;) {
                if (str.size() <= availBytes) {
                    if constexpr (Nullable) {
                        NullBuilder_->UnsafeAppend(!IsNull(array, idx));
                    }
                    OffsetsBuilder_->UnsafeAppend(TOffset(dataLen));
                    DataBuilder_->UnsafeAppend((const ui8*)str.data(), str.size());
                    dataLen += str.size();
                    break;
                }

                if (dataLen) {
                    FlushChunk(false);
                    dataLen = 0;
                } else {
                    ReserveForLargeString(str.size());
                    availBytes = str.size();
                }
            }
        }
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        FlushChunk(finish);
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();
        result->Payload = std::move(Chunks_);
        Chunks_.clear();
        return result;
    }

private:
    size_t DoReserve() final {
        OffsetsBuilder_ = std::make_unique<TTypedBufferBuilder<TOffset>>(Pool_, MinFillPercentage_);
        OffsetsBuilder_->Reserve(MaxLen_ + 1);
        DataBuilder_ = std::make_unique<TTypedBufferBuilder<ui8>>(Pool_, MinFillPercentage_);
        DataBuilder_->Reserve(MaxBlockSizeInBytes_);
        auto result = OffsetsBuilder_->Capacity() + DataBuilder_->Capacity();
        if constexpr (Nullable) {
            NullBuilder_ = std::make_unique<TTypedBufferBuilder<ui8>>(Pool_, MinFillPercentage_);
            NullBuilder_->Reserve(MaxLen_ + 1);
            result += NullBuilder_->Capacity();
        }
        return result;
    }

    void ReserveForLargeString(size_t strSize) {
        size_t before = DataBuilder_->Capacity();
        DataBuilder_->Reserve(strSize);
        size_t after = DataBuilder_->Capacity();
        Y_ENSURE(before <= after);
        AddExtraAllocated(after - before);
    }

    void AppendCurrentOffset() {
        OffsetsBuilder_->UnsafeAppend(DataBuilder_->Length());
    }

    void FlushChunk(bool finish) {
        const auto length = OffsetsBuilder_->Length();
        Y_ABORT_UNLESS(length > 0);

        AppendCurrentOffset();
        std::shared_ptr<arrow::Buffer> nullBitmap;
        if constexpr (Nullable) {
            nullBitmap = NullBuilder_->Finish();
            nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool_);
        }
        std::shared_ptr<arrow::Buffer> offsets = OffsetsBuilder_->Finish();
        std::shared_ptr<arrow::Buffer> data = DataBuilder_->Finish();

        Chunks_.push_back(arrow::ArrayData::Make(ArrowType_, length, {nullBitmap, offsets, data}));
        if (!finish) {
            Reserve();
        }
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder_;
    std::unique_ptr<TTypedBufferBuilder<TOffset>> OffsetsBuilder_;
    std::unique_ptr<TTypedBufferBuilder<ui8>> DataBuilder_;

    std::deque<std::shared_ptr<arrow::ArrayData>> Chunks_;

    const IPgBuilder* PgBuilder_ = nullptr;
    i32 TypeLen_ = 0;
};

template <bool Nullable, typename TDerived>
class TTupleArrayBuilderBase: public TArrayBuilderBase {
public:
    TTupleArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, params)
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder_->UnsafeAppend(0);
                static_cast<TDerived*>(this)->AddToChildrenDefault();
                return;
            }
            NullBuilder_->UnsafeAppend(1);
        }

        static_cast<TDerived*>(this)->AddToChildren(value);
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder_->UnsafeAppend(0);
                static_cast<TDerived*>(this)->AddToChildrenDefault();
                return;
            }
            NullBuilder_->UnsafeAppend(1);
        }

        static_cast<TDerived*>(this)->AddToChildren(value);
    }

    void DoAdd(TInputBuffer& input) final {
        if constexpr (Nullable) {
            if (!input.PopChar()) {
                NullBuilder_->UnsafeAppend(0);
                static_cast<TDerived*>(this)->AddToChildrenDefault();
                return;
            }
            NullBuilder_->UnsafeAppend(1);
        }

        static_cast<TDerived*>(this)->AddToChildren(input);
    }

    void DoAddDefault() final {
        if constexpr (Nullable) {
            NullBuilder_->UnsafeAppend(1);
        }
        static_cast<TDerived*>(this)->AddToChildrenDefault();
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_ABORT_UNLESS(!array.buffers.empty());

        if constexpr (Nullable) {
            if (array.buffers.front()) {
                ui8* dstBitmap = NullBuilder_->End();
                CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
                NullBuilder_->UnsafeAdvance(popCount);
            } else {
                NullBuilder_->UnsafeAppend(popCount, 1);
            }
        }

        static_cast<TDerived*>(this)->AddManyToChildren(array, sparseBitmap, popCount);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());

        if constexpr (Nullable) {
            for (ui64 i = beginIndex; i < beginIndex + count; ++i) {
                NullBuilder_->UnsafeAppend(!IsNull(array, i));
            }
        }

        static_cast<TDerived*>(this)->AddManyToChildren(array, beginIndex, count);
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());

        if constexpr (Nullable) {
            for (size_t i = 0; i < count; ++i) {
                NullBuilder_->UnsafeAppend(!IsNull(array, indexes[i]));
            }
        }

        static_cast<TDerived*>(this)->AddManyToChildren(array, indexes, count);
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();

        std::shared_ptr<arrow::Buffer> nullBitmap;
        const size_t length = GetCurrLen();
        if constexpr (Nullable) {
            Y_ENSURE(length == NullBuilder_->Length(), "Unexpected NullBuilder length");
            nullBitmap = NullBuilder_->Finish();
            nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool_);
        }

        Y_ABORT_UNLESS(length);
        result->Payload.push_back(arrow::ArrayData::Make(ArrowType_, length, {nullBitmap}));
        static_cast<TDerived*>(this)->BuildChildrenTree(finish, result->Children);

        if (!finish) {
            Reserve();
        }

        return result;
    }

private:
    size_t DoReserve() final {
        if constexpr (Nullable) {
            NullBuilder_ = std::make_unique<TTypedBufferBuilder<ui8>>(Pool_, MinFillPercentage_);
            NullBuilder_->Reserve(MaxLen_ + 1);
            return NullBuilder_->Capacity();
        }
        return 0;
    }

private:
    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder_;
};

template <bool Nullable>
class TTupleArrayBuilder final: public TTupleArrayBuilderBase<Nullable, TTupleArrayBuilder<Nullable>> {
    using TBase = TTupleArrayBuilderBase<Nullable, TTupleArrayBuilder<Nullable>>;
    using TParams = TArrayBuilderBase::TParams;

public:
    TTupleArrayBuilder(TVector<TArrayBuilderBase::Ptr>&& children, const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool,
                       size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, type, pool, maxLen, params)
        , Children_(std::move(children))
    {
    }

    void AddToChildrenDefault() {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Children_[i]->AddDefault();
        }
    }

    void AddToChildren(NUdf::TUnboxedValuePod value) {
        auto elements = value.GetElements();
        if (elements) {
            for (ui32 i = 0; i < Children_.size(); ++i) {
                Children_[i]->Add(elements[i]);
            }
        } else {
            for (ui32 i = 0; i < Children_.size(); ++i) {
                auto element = value.GetElement(i);
                Children_[i]->Add(element);
            }
        }
    }

    void AddToChildren(TBlockItem value) {
        auto elements = value.AsTuple();
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Children_[i]->Add(elements[i]);
        }
    }

    void AddToChildren(TInputBuffer& input) {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Children_[i]->Add(input);
        }
    }

    void AddManyToChildren(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) {
        Y_ABORT_UNLESS(array.child_data.size() == Children_.size());
        for (size_t i = 0; i < Children_.size(); ++i) {
            Children_[i]->AddMany(*array.child_data[i], popCount, sparseBitmap, array.length);
        }
    }

    void AddManyToChildren(const arrow::ArrayData& array, ui64 beginIndex, size_t count) {
        Y_ABORT_UNLESS(array.child_data.size() == Children_.size());
        for (size_t i = 0; i < Children_.size(); ++i) {
            Children_[i]->AddMany(*array.child_data[i], beginIndex, count);
        }
    }

    void AddManyToChildren(const arrow::ArrayData& array, const ui64* indexes, size_t count) {
        Y_ABORT_UNLESS(array.child_data.size() == Children_.size());
        for (size_t i = 0; i < Children_.size(); ++i) {
            Children_[i]->AddMany(*array.child_data[i], indexes, count);
        }
    }

    void BuildChildrenTree(bool finish, std::vector<TArrayBuilderBase::TBlockArrayTree::Ptr>& resultChildren) {
        resultChildren.reserve(Children_.size());
        for (ui32 i = 0; i < Children_.size(); ++i) {
            resultChildren.emplace_back(Children_[i]->BuildTree(finish));
        }
    }

private:
    TVector<std::unique_ptr<TArrayBuilderBase>> Children_;
};

template <typename TDate, bool Nullable>
class TTzDateArrayBuilder final: public TTupleArrayBuilderBase<Nullable, TTzDateArrayBuilder<TDate, Nullable>> {
    using TBase = TTupleArrayBuilderBase<Nullable, TTzDateArrayBuilder<TDate, Nullable>>;
    using TParams = TArrayBuilderBase::TParams;
    using TDateLayout = typename TDataType<TDate>::TLayout;
    static constexpr auto DataSlot = TDataType<TDate>::Slot;

public:
    TTzDateArrayBuilder(const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TParams& params = {})
        : TBase(typeInfoHelper, type, pool, maxLen, params)
        , DateBuilder_(typeInfoHelper, MakeTzLayoutArrowType<DataSlot>(), pool, maxLen, params)
        , TimezoneBuilder_(typeInfoHelper, arrow::uint16(), pool, maxLen, params)
    {
    }

    void AddToChildrenDefault() {
        DateBuilder_.AddDefault();
        TimezoneBuilder_.AddDefault();
    }

    void AddToChildren(NUdf::TUnboxedValuePod value) {
        DateBuilder_.Add(value);
        TimezoneBuilder_.Add(TBlockItem(value.GetTimezoneId()));
    }

    void AddToChildren(TBlockItem value) {
        DateBuilder_.Add(value);
        TimezoneBuilder_.Add(TBlockItem(value.GetTimezoneId()));
    }

    void AddToChildren(TInputBuffer& input) {
        DateBuilder_.Add(input);
        TimezoneBuilder_.Add(input);
    }

    void AddManyToChildren(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) {
        Y_ABORT_UNLESS(array.child_data.size() == 2);
        DateBuilder_.AddMany(*array.child_data[0], popCount, sparseBitmap, array.length);
        TimezoneBuilder_.AddMany(*array.child_data[1], popCount, sparseBitmap, array.length);
    }

    void AddManyToChildren(const arrow::ArrayData& array, ui64 beginIndex, size_t count) {
        Y_ABORT_UNLESS(array.child_data.size() == 2);
        DateBuilder_.AddMany(*array.child_data[0], beginIndex, count);
        TimezoneBuilder_.AddMany(*array.child_data[1], beginIndex, count);
    }

    void AddManyToChildren(const arrow::ArrayData& array, const ui64* indexes, size_t count) {
        Y_ABORT_UNLESS(array.child_data.size() == 2);
        DateBuilder_.AddMany(*array.child_data[0], indexes, count);
        TimezoneBuilder_.AddMany(*array.child_data[1], indexes, count);
    }

    void BuildChildrenTree(bool finish, std::vector<TArrayBuilderBase::TBlockArrayTree::Ptr>& resultChildren) {
        resultChildren.emplace_back(DateBuilder_.BuildTree(finish));
        resultChildren.emplace_back(TimezoneBuilder_.BuildTree(finish));
    }

private:
    TFixedSizeArrayBuilder<TDateLayout, false> DateBuilder_;
    TFixedSizeArrayBuilder<ui16, false> TimezoneBuilder_;
};

class TExternalOptionalArrayBuilder final: public TArrayBuilderBase {
public:
    TExternalOptionalArrayBuilder(std::unique_ptr<TArrayBuilderBase>&& inner, const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool,
                                  size_t maxLen, const TParams& params = {})
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, params)
        , Inner_(std::move(inner))
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if (!value) {
            NullBuilder_->UnsafeAppend(0);
            Inner_->AddDefault();
            return;
        }

        NullBuilder_->UnsafeAppend(1);
        Inner_->Add(value.GetOptionalValue());
    }

    void DoAdd(TBlockItem value) final {
        if (!value) {
            NullBuilder_->UnsafeAppend(0);
            Inner_->AddDefault();
            return;
        }

        NullBuilder_->UnsafeAppend(1);
        Inner_->Add(value.GetOptionalValue());
    }

    void DoAdd(TInputBuffer& input) final {
        if (!input.PopChar()) {
            NullBuilder_->UnsafeAppend(0);
            Inner_->AddDefault();
            return;
        }

        NullBuilder_->UnsafeAppend(1);
        Inner_->Add(input);
    }

    void DoAddDefault() final {
        NullBuilder_->UnsafeAppend(1);
        Inner_->AddDefault();
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == 1);

        if (array.buffers.front()) {
            ui8* dstBitmap = NullBuilder_->End();
            CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
            NullBuilder_->UnsafeAdvance(popCount);
        } else {
            NullBuilder_->UnsafeAppend(popCount, 1);
        }

        Inner_->AddMany(*array.child_data[0], popCount, sparseBitmap, array.length);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == 1);

        for (ui64 i = beginIndex; i < beginIndex + count; ++i) {
            NullBuilder_->UnsafeAppend(!IsNull(array, i));
        }

        Inner_->AddMany(*array.child_data[0], beginIndex, count);
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == 1);

        for (size_t i = 0; i < count; ++i) {
            NullBuilder_->UnsafeAppend(!IsNull(array, indexes[i]));
        }

        Inner_->AddMany(*array.child_data[0], indexes, count);
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();

        std::shared_ptr<arrow::Buffer> nullBitmap;
        const size_t length = GetCurrLen();
        Y_ENSURE(length == NullBuilder_->Length(), "Unexpected NullBuilder length");
        nullBitmap = NullBuilder_->Finish();
        nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool_);

        Y_ABORT_UNLESS(length);
        result->Payload.push_back(arrow::ArrayData::Make(ArrowType_, length, {nullBitmap}));
        result->Children.emplace_back(Inner_->BuildTree(finish));

        if (!finish) {
            Reserve();
        }

        return result;
    }

private:
    size_t DoReserve() final {
        NullBuilder_ = std::make_unique<TTypedBufferBuilder<ui8>>(Pool_, MinFillPercentage_);
        NullBuilder_->Reserve(MaxLen_ + 1);
        return NullBuilder_->Capacity();
    }

private:
    std::unique_ptr<TArrayBuilderBase> Inner_;
    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder_;
};

template <bool IsNull>
class TSingularBlockBuilder final: public TArrayBuilderBase {
public:
    TSingularBlockBuilder(const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool,
                          size_t maxLen, const TParams& params = {})
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, params)
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        Y_UNUSED(value);
    }

    void DoAdd(TBlockItem value) final {
        Y_UNUSED(value);
    }

    void DoAdd(TInputBuffer& input) final {
        Y_UNUSED(input);
    }

    void DoAddDefault() final {
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_UNUSED(array, sparseBitmap, popCount);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_UNUSED(array, beginIndex, count);
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_UNUSED(array, indexes, count);
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();
        Y_UNUSED(finish);
        result->Payload.push_back(NYql::NUdf::MakeSingularArray(IsNull, GetCurrLen()));
        return result;
    }

private:
    size_t DoReserve() final {
        return 0;
    }
};

using TArrayBuilderParams = TArrayBuilderBase::TParams;

struct TBuilderTraits {
    using TResult = TArrayBuilderBase;
    template <bool Nullable>
    using TTuple = TTupleArrayBuilder<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeArrayBuilder<T, Nullable>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot TOriginal>
    using TStrings = TStringArrayBuilder<TStringType, Nullable>;
    using TExtOptional = TExternalOptionalArrayBuilder;
    template <bool Nullable>
    using TResource = TResourceArrayBuilder<Nullable>;
    template <typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateArrayBuilder<TTzDate, Nullable>;
    template <bool IsNull>
    using TSingular = TSingularBlockBuilder<IsNull>;

    constexpr static bool PassType = true;

    static std::unique_ptr<TResult> MakePg(const TPgTypeDescription& desc, const IPgBuilder* pgBuilder, const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TArrayBuilderParams& params) {
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>(type, typeInfoHelper, pool, maxLen, params);
        } else {
            if (desc.Typelen == -1) {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::Text>>(type, typeInfoHelper, pool, maxLen, params);
                ret->SetPgBuilder(pgBuilder, desc.Typelen);
                return ret;
            } else if (desc.Typelen == -2) {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::CString>>(type, typeInfoHelper, pool, maxLen, params);
                ret->SetPgBuilder(pgBuilder, desc.Typelen);
                return ret;
            } else {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::Fixed>>(type, typeInfoHelper, pool, maxLen, params);
                ret->SetPgBuilder(pgBuilder, desc.Typelen);
                return ret;
            }
        }
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional, const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TArrayBuilderParams& params) {
        if (isOptional) {
            return std::make_unique<TResource<true>>(type, typeInfoHelper, pool, maxLen, params);
        } else {
            return std::make_unique<TResource<false>>(type, typeInfoHelper, pool, maxLen, params);
        }
    }

    template <typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional, const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TArrayBuilderParams& params) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>(type, typeInfoHelper, pool, maxLen, params);
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>(type, typeInfoHelper, pool, maxLen, params);
        }
    }

    template <bool IsNull>
    static std::unique_ptr<TResult> MakeSingular(const TType* type, const ITypeInfoHelper& typeInfoHelper, arrow::MemoryPool& pool, size_t maxLen, const TArrayBuilderParams& params) {
        return std::make_unique<TSingular<IsNull>>(type, typeInfoHelper, pool, maxLen, params);
    }
};

inline std::unique_ptr<IArrayBuilder> MakeArrayBuilder(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool,
    size_t maxBlockLength, const IPgBuilder* pgBuilder)
{
    return DispatchByArrowTraits<TBuilderTraits>(typeInfoHelper, type, pgBuilder, typeInfoHelper, pool, maxBlockLength, TArrayBuilderParams{});
}

inline std::unique_ptr<IArrayBuilder> MakeArrayBuilder(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool,
    size_t maxBlockLength, const IPgBuilder* pgBuilder, size_t* totalAllocated)
{
    return DispatchByArrowTraits<TBuilderTraits>(typeInfoHelper, type, pgBuilder, typeInfoHelper, pool, maxBlockLength, TArrayBuilderParams{.TotalAllocated = totalAllocated});
}

inline std::unique_ptr<IArrayBuilder> MakeArrayBuilder(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool,
    size_t maxBlockLength, const IPgBuilder* pgBuilder, const TArrayBuilderParams& params)
{
    return DispatchByArrowTraits<TBuilderTraits>(typeInfoHelper, type, pgBuilder, typeInfoHelper, pool, maxBlockLength, params);
}

inline std::unique_ptr<IScalarBuilder> MakeScalarBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    Y_UNUSED(typeInfoHelper);
    Y_UNUSED(type);
    Y_ENSURE(false);
    return nullptr;
}

} // namespace NYql::NUdf
