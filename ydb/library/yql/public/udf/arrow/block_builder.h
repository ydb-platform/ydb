#pragma once

#include "util.h"
#include "bit_util.h"
#include "block_io_buffer.h"
#include "block_item.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include <arrow/datum.h>
#include <arrow/c/bridge.h>

#include <deque>

namespace NYql {
namespace NUdf {

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
    IArrayBuilder::TArrayDataItem lookup{ nullptr, idx };

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
};

inline std::shared_ptr<arrow::DataType> GetArrowType(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    auto arrowTypeHandle = typeInfoHelper.MakeArrowType(type);
    Y_ENSURE(arrowTypeHandle);
    ArrowSchema s;
    arrowTypeHandle->Export(&s);
    return ARROW_RESULT(arrow::ImportType(&s));
}

class TArrayBuilderBase : public IArrayBuilder {
    using Self = TArrayBuilderBase;
public:
    using Ptr = std::unique_ptr<TArrayBuilderBase>;

    struct TBlockArrayTree {
        using Ptr = std::shared_ptr<TBlockArrayTree>;
        std::deque<std::shared_ptr<arrow::ArrayData>> Payload;
        std::vector<TBlockArrayTree::Ptr> Children;
    };

    TArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated)
        : ArrowType(std::move(arrowType))
        , Pool(&pool)
        , MaxLen(maxLen)
        , MaxBlockSizeInBytes(typeInfoHelper.GetMaxBlockBytes())
        , TotalAllocated_(totalAllocated)
    {
        Y_ABORT_UNLESS(ArrowType);
        Y_ABORT_UNLESS(maxLen > 0);
    }

    TArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated)
        : TArrayBuilderBase(typeInfoHelper, GetArrowType(typeInfoHelper, type), pool, maxLen, totalAllocated)
    {
    }

    size_t MaxLength() const final {
        return MaxLen;
    }

    void Add(NUdf::TUnboxedValuePod value) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen < MaxLen);
        DoAdd(value);
        CurrLen++;
    }

    void Add(TBlockItem value) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen < MaxLen);
        DoAdd(value);
        CurrLen++;
    }

    void Add(TBlockItem value, size_t count) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen + count <= MaxLen);
        DoAdd(value, count);
        CurrLen += count;
    }

    void Add(TInputBuffer& input) final {
        Y_DEBUG_ABORT_UNLESS(CurrLen < MaxLen);
        DoAdd(input);
        CurrLen++;
    }

    void AddDefault() {
        Y_DEBUG_ABORT_UNLESS(CurrLen < MaxLen);
        DoAddDefault();
        CurrLen++;
    }

    inline void AddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) {
        TArrayDataItem item = { &array, 0 };
        Self::AddMany(&item, 1, beginIndex, count);
    }

    inline void AddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) {
        TArrayDataItem item = { &array, 0 };
        Self::AddMany(&item, 1, indexes, count);
    }

    void AddMany(const arrow::ArrayData& array, size_t popCount, const ui8* sparseBitmap, size_t bitmapSize) final {
        Y_ABORT_UNLESS(size_t(array.length) == bitmapSize);
        Y_ABORT_UNLESS(popCount <= bitmapSize);
        Y_ABORT_UNLESS(CurrLen + popCount <= MaxLen);

        if (popCount) {
            DoAddMany(array, sparseBitmap, popCount);
        }

        CurrLen += popCount;
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
        CurrLen += count;
    }

    void AddMany(const TArrayDataItem* arrays, size_t arrayCount, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(arrays);
        Y_ABORT_UNLESS(arrayCount > 0);
        Y_ABORT_UNLESS(indexes);
        Y_ABORT_UNLESS(CurrLen + count <= MaxLen);

        if (arrayCount == 1) {
            Y_ABORT_UNLESS(arrays->Data);
            DoAddMany(*arrays->Data, indexes, count);
            CurrLen += count;
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
                    CurrLen += currDataIndexes.size();
                    currDataIndexes.clear();
                    currData = data;
                }
                currDataIndexes.push_back(idx);
            }
            if (!currDataIndexes.empty()) {
                DoAddMany(*currData->Data, currDataIndexes.data(), currDataIndexes.size());
                CurrLen += currDataIndexes.size();
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
        CurrLen = 0;
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

    virtual void DoReserve() = 0;
    // will be called immediately after DoReserve()
    virtual size_t GetAllocatedSize() const = 0;

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
        return CurrLen;
    }

    void SetCurrLen(size_t len) {
        Y_ABORT_UNLESS(len <= MaxLen);
        CurrLen = len;
    }

    void Reserve() {
        DoReserve();
        if (TotalAllocated_) {
            *TotalAllocated_ += GetAllocatedSize();
        }
    }

    void AddExtraAllocated(size_t bytes) {
        if (TotalAllocated_) {
            *TotalAllocated_ += bytes;
        }
    }

    const std::shared_ptr<arrow::DataType> ArrowType;
    arrow::MemoryPool* const Pool;
    const size_t MaxLen;
    const size_t MaxBlockSizeInBytes;
private:
    size_t CurrLen = 0;
    size_t* TotalAllocated_ = nullptr;
};

template<typename TLayout, bool Nullable, typename TDerived>
class TFixedSizeArrayBuilderBase : public TArrayBuilderBase {
public:
    TFixedSizeArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated)
        : TArrayBuilderBase(typeInfoHelper, std::move(arrowType), pool, maxLen, totalAllocated)
    {
        Reserve();
    }

    TFixedSizeArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, totalAllocated)
    {
        Reserve();
    }

    void UnsafeReserve(size_t length) {
        SetCurrLen(length);
    }

    TLayout* MutableData() {
        return DataPtr;
    }

    ui8* MutableValidMask() {
        return NullPtr;
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                DoAddNull();
                return;
            }
            NullPtr[GetCurrLen()] = 1;
        }
        static_cast<TDerived*>(this)->DoAddNotNull(value);
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                DoAddNull();
                return;
            }
            NullPtr[GetCurrLen()] = 1;
        }
        static_cast<TDerived*>(this)->DoAddNotNull(value);
    }
    
    void DoAddNull() {
        if constexpr (Nullable) {
            NullPtr[GetCurrLen()] = 0;
            PlaceItem(TLayout{});
        }
    }

    void DoAdd(TBlockItem value, size_t count) final {
        if constexpr (Nullable) {
            if (!value) {
                std::fill(NullPtr + GetCurrLen(), NullPtr + GetCurrLen() + count, 0);
                std::fill(DataPtr + GetCurrLen(), DataPtr + GetCurrLen() + count, TLayout{});
                return;
            }
            std::fill(NullPtr + GetCurrLen(), NullPtr + GetCurrLen() + count, 1);
        }

        static_cast<TDerived*>(this)->DoAddNotNull(value, count);
    }

    void DoAdd(TInputBuffer &input) final {
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
            NullPtr[GetCurrLen()] = 1;
        }
        PlaceItem(TLayout{});
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            if (array.buffers.front()) {
                ui8* dstBitmap = NullPtr + GetCurrLen();
                CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
            } else {
                ui8* dstBitmap = NullPtr + GetCurrLen();
                std::fill_n(dstBitmap, popCount, 1);
            }
        }

        const TLayout* src = array.GetValues<TLayout>(1);
        TLayout* dst = DataPtr + GetCurrLen();
        CompressArray(src, sparseBitmap, dst, array.length);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            for (size_t i = beginIndex; i < beginIndex + count; ++i) {
                NullPtr[GetCurrLen() + i - beginIndex] = !IsNull(array, i);
            }
        }

        const TLayout* values = array.GetValues<TLayout>(1);
        for (size_t i = beginIndex; i < beginIndex + count; ++i) {
            ::new(DataPtr + GetCurrLen() + i - beginIndex) TLayout(values[i]);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            for (size_t i = 0; i < count; ++i) {
                NullPtr[GetCurrLen() + i] = !IsNull(array, indexes[i]);
            }
        }

        const TLayout* values = array.GetValues<TLayout>(1);
        for (size_t i = 0; i < count; ++i) {
            ::new(DataPtr + GetCurrLen() + i) TLayout(values[indexes[i]]);
        }
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        const size_t len = GetCurrLen();
        std::shared_ptr<arrow::Buffer> nulls;
        if constexpr (Nullable) {
            NullBuilder->UnsafeAdvance(len);
            nulls = NullBuilder->Finish();
            nulls = MakeDenseBitmap(nulls->data(), len, Pool);
        }
        DataBuilder->UnsafeAdvance(len);
        std::shared_ptr<arrow::Buffer> data = DataBuilder->Finish();

        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();
        result->Payload.push_back(arrow::ArrayData::Make(ArrowType, len, {nulls, data}));

        NullBuilder.reset();
        DataBuilder.reset();
        if (!finish) {
            Reserve();
        }
        return result;
    }
protected:
    void PlaceItem(TLayout&& value)  {
        ::new(DataPtr + GetCurrLen()) TLayout(std::move(value));
    }

    TLayout* DataPtr = nullptr;

private:
    void DoReserve() final {
        DataBuilder = std::make_unique<TTypedBufferBuilder<TLayout>>(Pool);
        DataBuilder->Reserve(MaxLen + 1);
        DataPtr = DataBuilder->MutableData();
        if constexpr (Nullable) {
            NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
            NullBuilder->Reserve(MaxLen + 1);
            NullPtr = NullBuilder->MutableData();
        }
    }

    size_t GetAllocatedSize() const final {
        Y_ENSURE(DataBuilder);
        size_t result = DataBuilder->Capacity();
        if constexpr (Nullable) {
            Y_ENSURE(NullBuilder);
            result += NullBuilder->Capacity();
        }
        return result;
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
    std::unique_ptr<TTypedBufferBuilder<TLayout>> DataBuilder;
    ui8* NullPtr = nullptr;
};

template<typename TLayout, bool Nullable>
class TFixedSizeArrayBuilder final: public TFixedSizeArrayBuilderBase<TLayout, Nullable, TFixedSizeArrayBuilder<TLayout, Nullable>> {
    using TSelf = TFixedSizeArrayBuilder<TLayout, Nullable>;
    using TBase = TFixedSizeArrayBuilderBase<TLayout, Nullable, TSelf>;

public:
    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TBase(typeInfoHelper, std::move(arrowType), pool, maxLen, totalAllocated)
    {}

    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TBase(typeInfoHelper, type, pool, maxLen, totalAllocated)
    {}

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
        std::fill(this->DataPtr + this->GetCurrLen(), this->DataPtr + this->GetCurrLen() + count, value.Get<TLayout>());
    }
};

template<bool Nullable>
class TFixedSizeArrayBuilder<NYql::NDecimal::TInt128, Nullable> final: public TFixedSizeArrayBuilderBase<NYql::NDecimal::TInt128, Nullable, TFixedSizeArrayBuilder<NYql::NDecimal::TInt128, Nullable>> {
    using TSelf = TFixedSizeArrayBuilder<NYql::NDecimal::TInt128, Nullable>;
    using TBase = TFixedSizeArrayBuilderBase<NYql::NDecimal::TInt128, Nullable, TSelf>;

public:
    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TBase(typeInfoHelper, std::move(arrowType), pool, maxLen, totalAllocated)
    {}

    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TBase(typeInfoHelper, type, pool, maxLen, totalAllocated)
    {}

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
        std::fill(this->DataPtr + this->GetCurrLen(), this->DataPtr + this->GetCurrLen() + count, value.GetInt128());
    }
};

template<bool Nullable>
class TResourceArrayBuilder final: public TFixedSizeArrayBuilderBase<TUnboxedValue, Nullable, TResourceArrayBuilder<Nullable>> {
public:
    TResourceArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TFixedSizeArrayBuilderBase<TUnboxedValue, Nullable, TResourceArrayBuilder<Nullable>>(typeInfoHelper, std::move(arrowType), pool, maxLen, totalAllocated)
    {}

    TResourceArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TFixedSizeArrayBuilderBase<TUnboxedValue, Nullable, TResourceArrayBuilder<Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated)
    {}

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
            ::new(this->DataPtr + this->GetCurrLen() + i) TUnboxedValue(FromBlockItem(item));
        }
    }
};

template<typename TStringType, bool Nullable, EPgStringType PgString = EPgStringType::None>
class TStringArrayBuilder final : public TArrayBuilderBase {
public:
    using TOffset = typename TStringType::offset_type;
    TStringArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TArrayBuilderBase(typeInfoHelper, std::move(arrowType), pool, maxLen, totalAllocated)
    {
        Reserve();
    }

    TStringArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, totalAllocated)
    {
        Reserve();
    }

    void SetPgBuilder(const NUdf::IPgBuilder* pgBuilder, i32 typeLen) {
        Y_ENSURE(PgString != EPgStringType::None);
        PgBuilder = pgBuilder;
        TypeLen = typeLen;
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                return DoAdd(TBlockItem{});
            }
        }

        if constexpr (PgString == EPgStringType::CString) {
            static_assert(Nullable);
            auto buf = PgBuilder->AsCStringBuffer(value);
            AddPgItem(buf);
        } else if constexpr (PgString == EPgStringType::Text) {
            static_assert(Nullable);
            auto buf = PgBuilder->AsTextBuffer(value);
            AddPgItem(buf);
        } else if constexpr (PgString == EPgStringType::Fixed) {
            static_assert(Nullable);
            auto buf = PgBuilder->AsFixedStringBuffer(value, TypeLen);
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
        size_t currentLen = DataBuilder->Length();
        // empty string can always be appended
        if (size > 0 && currentLen + size > MaxBlockSizeInBytes) {
            if (currentLen) {
                FlushChunk(false);
            }
            if (size > MaxBlockSizeInBytes) {
                ReserveForLargeString(size);
            }
        }

        AppendCurrentOffset();
        auto ret = DataBuilder->End();
        DataBuilder->UnsafeAdvance(size);
        if constexpr (Nullable) {
            NullBuilder->UnsafeAppend(1);
        }

        return ret;
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder->UnsafeAppend(0);
                AppendCurrentOffset();
                return;
            }
        }

        const std::string_view str = value.AsStringRef();
        auto ptr = AddNoFill(str.size());
        std::memcpy(ptr, str.data(), str.size());
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
            NullBuilder->UnsafeAppend(1);
        }
        AppendCurrentOffset();
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_UNUSED(popCount);
        Y_ABORT_UNLESS(array.buffers.size() > 2);
        Y_ABORT_UNLESS(!Nullable || NullBuilder->Length() == OffsetsBuilder->Length());

        const ui8* srcNulls = array.GetValues<ui8>(0, 0);
        const TOffset* srcOffset = array.GetValues<TOffset>(1);
        const ui8* srcData = array.GetValues<ui8>(2, 0);

        const ui8* chunkStart = srcData;
        const ui8* chunkEnd = chunkStart;
        size_t dataLen = DataBuilder->Length();

        ui8* dstNulls = Nullable ? NullBuilder->End() : nullptr;
        TOffset* dstOffset = OffsetsBuilder->End();
        size_t countAdded = 0;
        for (size_t i = 0; i < size_t(array.length); i++) {
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
                        DataBuilder->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
                        chunkStart = chunkEnd = srcData;
                    }
                    Y_ABORT_UNLESS(dataLen == DataBuilder->Length());
                    OffsetsBuilder->UnsafeAdvance(countAdded);
                    if constexpr (Nullable) {
                        NullBuilder->UnsafeAdvance(countAdded);
                    }
                    FlushChunk(false);

                    dataLen = 0;
                    countAdded = 0;
                    if constexpr (Nullable) {
                        dstNulls = NullBuilder->End();
                    }
                    dstOffset = OffsetsBuilder->End();
                } else {
                    ReserveForLargeString(strSize);
                    availBytes = strSize;
                }
            }
        }
        if (chunkStart != chunkEnd) {
            DataBuilder->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
        }
        Y_ABORT_UNLESS(dataLen == DataBuilder->Length());
        OffsetsBuilder->UnsafeAdvance(countAdded);
        if constexpr (Nullable) {
            NullBuilder->UnsafeAdvance(countAdded);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 2);
        Y_ABORT_UNLESS(!Nullable || NullBuilder->Length() == OffsetsBuilder->Length());

        size_t dataLen = DataBuilder->Length();

        const TOffset* offsets = array.GetValues<TOffset>(1);
        const ui8* srcData = array.GetValues<ui8>(2, 0);
        const ui8* chunkStart = srcData + offsets[beginIndex];
        const ui8* chunkEnd = chunkStart;
        for (size_t i = beginIndex; i < beginIndex + count; ++i) {
            const ui8* begin = srcData + offsets[i];
            const ui8* end   = srcData + offsets[i + 1];
            const size_t strSize = end - begin;

            size_t availBytes = std::max(dataLen, MaxBlockSizeInBytes) - dataLen;
            for (;;) {
                if (strSize <= availBytes) {
                    if constexpr (Nullable) {
                        NullBuilder->UnsafeAppend(!IsNull(array, i));
                    }
                    OffsetsBuilder->UnsafeAppend(TOffset(dataLen));
                    chunkEnd = end;
                    dataLen += strSize;
                    break;
                }

                if (dataLen) {
                    DataBuilder->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
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
            DataBuilder->UnsafeAppend(chunkStart, chunkEnd - chunkStart);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 2);
        Y_ABORT_UNLESS(!Nullable || NullBuilder->Length() == OffsetsBuilder->Length());

        size_t dataLen = DataBuilder->Length();

        const TOffset* offsets = array.GetValues<TOffset>(1);
        const char* strData = array.GetValues<char>(2, 0);
        for (size_t i = 0; i < count; ++i) {
            ui64 idx = indexes[i];
            std::string_view str(strData + offsets[idx], offsets[idx + 1] - offsets[idx]);

            size_t availBytes = std::max(dataLen, MaxBlockSizeInBytes) - dataLen;
            for (;;) {
                if (str.size() <= availBytes) {
                    if constexpr (Nullable) {
                        NullBuilder->UnsafeAppend(!IsNull(array, idx));
                    }
                    OffsetsBuilder->UnsafeAppend(TOffset(dataLen));
                    DataBuilder->UnsafeAppend((const ui8*)str.data(), str.size());
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
        result->Payload = std::move(Chunks);
        Chunks.clear();
        return result;
    }

private:
    void DoReserve() final {
        if constexpr (Nullable) {
            NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
            NullBuilder->Reserve(MaxLen + 1);
        }
        OffsetsBuilder = std::make_unique<TTypedBufferBuilder<TOffset>>(Pool);
        OffsetsBuilder->Reserve(MaxLen + 1);
        DataBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        DataBuilder->Reserve(MaxBlockSizeInBytes);
    }

    size_t GetAllocatedSize() const final {
        Y_ENSURE(DataBuilder && OffsetsBuilder);
        size_t result = DataBuilder->Capacity() + OffsetsBuilder->Capacity();
        if constexpr (Nullable) {
            Y_ENSURE(NullBuilder);
            result += NullBuilder->Capacity();
        }
        return result;
    }

    void ReserveForLargeString(size_t strSize) {
        size_t before = DataBuilder->Capacity();
        DataBuilder->Reserve(strSize);
        size_t after = DataBuilder->Capacity();
        Y_ENSURE(before <= after);
        AddExtraAllocated(after - before);
    }

    void AppendCurrentOffset() {
        OffsetsBuilder->UnsafeAppend(DataBuilder->Length());
    }

    void FlushChunk(bool finish) {
        const auto length = OffsetsBuilder->Length();
        Y_ABORT_UNLESS(length > 0);

        AppendCurrentOffset();
        std::shared_ptr<arrow::Buffer> nullBitmap;
        if constexpr (Nullable) {
            nullBitmap = NullBuilder->Finish();
            nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool);
        }
        std::shared_ptr<arrow::Buffer> offsets = OffsetsBuilder->Finish();
        std::shared_ptr<arrow::Buffer> data = DataBuilder->Finish();

        Chunks.push_back(arrow::ArrayData::Make(ArrowType, length, { nullBitmap, offsets, data }));
        if (!finish) {
            Reserve();
        }
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
    std::unique_ptr<TTypedBufferBuilder<TOffset>> OffsetsBuilder;
    std::unique_ptr<TTypedBufferBuilder<ui8>> DataBuilder;

    std::deque<std::shared_ptr<arrow::ArrayData>> Chunks;

    const IPgBuilder* PgBuilder = nullptr;
    i32 TypeLen = 0;
};

template<bool Nullable, typename TDerived>
class TTupleArrayBuilderBase : public TArrayBuilderBase {
public:
    TTupleArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, totalAllocated)
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder->UnsafeAppend(0);
                static_cast<TDerived*>(this)->AddToChildrenDefault();
                return;
            }
            NullBuilder->UnsafeAppend(1);
        }

        static_cast<TDerived*>(this)->AddToChildren(value);
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder->UnsafeAppend(0);
                static_cast<TDerived*>(this)->AddToChildrenDefault();
                return;
            }
            NullBuilder->UnsafeAppend(1);
        }

        static_cast<TDerived*>(this)->AddToChildren(value);
    }

    void DoAdd(TInputBuffer& input) final {
        if constexpr (Nullable) {
            if (!input.PopChar()) {
                NullBuilder->UnsafeAppend(0);
                static_cast<TDerived*>(this)->AddToChildrenDefault();
                return;
            }
            NullBuilder->UnsafeAppend(1);
        }

        static_cast<TDerived*>(this)->AddToChildren(input);
    }

    void DoAddDefault() final {
        if constexpr (Nullable) {
            NullBuilder->UnsafeAppend(1);
        }
        static_cast<TDerived*>(this)->AddToChildrenDefault();
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_ABORT_UNLESS(!array.buffers.empty());

        if constexpr (Nullable) {
            if (array.buffers.front()) {
                ui8* dstBitmap = NullBuilder->End();
                CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
                NullBuilder->UnsafeAdvance(popCount);
            } else {
                NullBuilder->UnsafeAppend(popCount, 1);
            }
        }

        static_cast<TDerived*>(this)->AddManyToChildren(array, sparseBitmap, popCount);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());

        if constexpr (Nullable) {
            for (ui64 i = beginIndex; i < beginIndex + count; ++i) {
                NullBuilder->UnsafeAppend(!IsNull(array, i));
            }
        }

        static_cast<TDerived*>(this)->AddManyToChildren(array, beginIndex, count);
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());

        if constexpr (Nullable) {
            for (size_t i = 0; i < count; ++i) {
                NullBuilder->UnsafeAppend(!IsNull(array, indexes[i]));
            }
        }

        static_cast<TDerived*>(this)->AddManyToChildren(array, indexes, count);
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();

        std::shared_ptr<arrow::Buffer> nullBitmap;
        const size_t length = GetCurrLen();
        if constexpr (Nullable) {
            Y_ENSURE(length == NullBuilder->Length(), "Unexpected NullBuilder length");
            nullBitmap = NullBuilder->Finish();
            nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool);
        }

        Y_ABORT_UNLESS(length);
        result->Payload.push_back(arrow::ArrayData::Make(ArrowType, length, { nullBitmap }));
        static_cast<TDerived*>(this)->BuildChildrenTree(finish, result->Children);

        if (!finish) {
            Reserve();
        }

        return result;
    }

private:
    void DoReserve() final {
        if constexpr (Nullable) {
            NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
            NullBuilder->Reserve(MaxLen + 1);
        }
    }

    size_t GetAllocatedSize() const final {
        size_t result = 0;
        if constexpr (Nullable) {
            Y_ENSURE(NullBuilder);
            result += NullBuilder->Capacity();
        }
        return result;
    }

private:
    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
};

template<bool Nullable>
class TTupleArrayBuilder final : public TTupleArrayBuilderBase<Nullable, TTupleArrayBuilder<Nullable>> {
public:

    TTupleArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen,
                       TVector<TArrayBuilderBase::Ptr>&& children, size_t* totalAllocated = nullptr)
        : TTupleArrayBuilderBase<Nullable, TTupleArrayBuilder<Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated)
        , Children_(std::move(children)) {}

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

template<typename TDate, bool Nullable>
class TTzDateArrayBuilder final : public TTupleArrayBuilderBase<Nullable, TTzDateArrayBuilder<TDate, Nullable>> {
    using TDateLayout = typename TDataType<TDate>::TLayout;
    static constexpr auto DataSlot = TDataType<TDate>::Slot;

public:
    TTzDateArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, size_t* totalAllocated = nullptr)
        : TTupleArrayBuilderBase<Nullable, TTzDateArrayBuilder<TDate, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated)
        , DateBuilder_(typeInfoHelper, GetArrowType(typeInfoHelper, type), pool, maxLen)
        , TimezoneBuilder_(typeInfoHelper, arrow::uint16(), pool, maxLen)
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


class TExternalOptionalArrayBuilder final : public TArrayBuilderBase {
public:
    TExternalOptionalArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen,
        std::unique_ptr<TArrayBuilderBase>&& inner, size_t* totalAllocated)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen, totalAllocated)
        , Inner(std::move(inner))
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if (!value) {
            NullBuilder->UnsafeAppend(0);
            Inner->AddDefault();
            return;
        }

        NullBuilder->UnsafeAppend(1);
        Inner->Add(value.GetOptionalValue());
    }

    void DoAdd(TBlockItem value) final {
        if (!value) {
            NullBuilder->UnsafeAppend(0);
            Inner->AddDefault();
            return;
        }

        NullBuilder->UnsafeAppend(1);
        Inner->Add(value.GetOptionalValue());
    }

    void DoAdd(TInputBuffer& input) final {
        if (!input.PopChar()) {
            NullBuilder->UnsafeAppend(0);
            Inner->AddDefault();
            return;
        }

        NullBuilder->UnsafeAppend(1);
        Inner->Add(input);
    }

    void DoAddDefault() final {
        NullBuilder->UnsafeAppend(1);
        Inner->AddDefault();
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == 1);

        if (array.buffers.front()) {
            ui8* dstBitmap = NullBuilder->End();
            CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
            NullBuilder->UnsafeAdvance(popCount);
        } else {
            NullBuilder->UnsafeAppend(popCount, 1);
        }

        Inner->AddMany(*array.child_data[0], popCount, sparseBitmap, array.length);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == 1);

        for (ui64 i = beginIndex; i < beginIndex + count; ++i) {
            NullBuilder->UnsafeAppend(!IsNull(array, i));
        }

        Inner->AddMany(*array.child_data[0], beginIndex, count);
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == 1);

        for (size_t i = 0; i < count; ++i) {
            NullBuilder->UnsafeAppend(!IsNull(array, indexes[i]));
        }

        Inner->AddMany(*array.child_data[0], indexes, count);
    }

    TBlockArrayTree::Ptr DoBuildTree(bool finish) final {
        TBlockArrayTree::Ptr result = std::make_shared<TBlockArrayTree>();

        std::shared_ptr<arrow::Buffer> nullBitmap;
        const size_t length = GetCurrLen();
        Y_ENSURE(length == NullBuilder->Length(), "Unexpected NullBuilder length");
        nullBitmap = NullBuilder->Finish();
        nullBitmap = MakeDenseBitmap(nullBitmap->data(), length, Pool);

        Y_ABORT_UNLESS(length);
        result->Payload.push_back(arrow::ArrayData::Make(ArrowType, length, { nullBitmap }));
        result->Children.emplace_back(Inner->BuildTree(finish));

        if (!finish) {
            Reserve();
        }

        return result;
    }

private:
    void DoReserve() final {
        NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        NullBuilder->Reserve(MaxLen + 1);
    }

    size_t GetAllocatedSize() const final {
        Y_ENSURE(NullBuilder);
        return NullBuilder->Capacity();
    }

private:
    std::unique_ptr<TArrayBuilderBase> Inner;
    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
};

std::unique_ptr<TArrayBuilderBase> MakeArrayBuilderBase(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxBlockLength, const IPgBuilder* pgBuilder, size_t* totalAllocated);

template<bool Nullable>
inline std::unique_ptr<TArrayBuilderBase> MakeArrayBuilderImpl(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxLen, const IPgBuilder* pgBuilder, size_t* totalAllocated)
{
    if constexpr (Nullable) {
        TOptionalTypeInspector typeOpt(typeInfoHelper, type);
        type = typeOpt.GetItemType();
    }

    TStructTypeInspector typeStruct(typeInfoHelper, type);
    if (typeStruct) {
        TVector<std::unique_ptr<TArrayBuilderBase>> members;
        for (ui32 i = 0; i < typeStruct.GetMembersCount(); i++) {
            const TType* memberType = typeStruct.GetMemberType(i);
            auto memberBuilder = MakeArrayBuilderBase(typeInfoHelper, memberType, pool, maxLen, pgBuilder, totalAllocated);
            members.push_back(std::move(memberBuilder));
        }
        // XXX: Use Tuple array builder for Struct.
        return std::make_unique<TTupleArrayBuilder<Nullable>>(typeInfoHelper, type, pool, maxLen, std::move(members), totalAllocated);
    }

    TTupleTypeInspector typeTuple(typeInfoHelper, type);
    if (typeTuple) {
        TVector<std::unique_ptr<TArrayBuilderBase>> children;
        for (ui32 i = 0; i < typeTuple.GetElementsCount(); ++i) {
            const TType* childType = typeTuple.GetElementType(i);
            auto childBuilder = MakeArrayBuilderBase(typeInfoHelper, childType, pool, maxLen, pgBuilder, totalAllocated);
            children.push_back(std::move(childBuilder));
        }

        return std::make_unique<TTupleArrayBuilder<Nullable>>(typeInfoHelper, type, pool, maxLen, std::move(children), totalAllocated);
    }

    TDataTypeInspector typeData(typeInfoHelper, type);
    if (typeData) {
        auto typeId = typeData.GetTypeId();
        switch (GetDataSlot(typeId)) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TFixedSizeArrayBuilder<i8, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return std::make_unique<TFixedSizeArrayBuilder<ui8, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TFixedSizeArrayBuilder<i16, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TFixedSizeArrayBuilder<ui16, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Date32:
            return std::make_unique<TFixedSizeArrayBuilder<i32, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TFixedSizeArrayBuilder<ui32, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Interval64:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
            return std::make_unique<TFixedSizeArrayBuilder<i64, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TFixedSizeArrayBuilder<ui64, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Float:
            return std::make_unique<TFixedSizeArrayBuilder<float, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Double:
            return std::make_unique<TFixedSizeArrayBuilder<double, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::JsonDocument:
            return std::make_unique<TStringArrayBuilder<arrow::BinaryType, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
            return std::make_unique<TStringArrayBuilder<arrow::StringType, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::TzDate:
            return std::make_unique<TTzDateArrayBuilder<TTzDate, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::TzDatetime:
            return std::make_unique<TTzDateArrayBuilder<TTzDatetime, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::TzTimestamp:
            return std::make_unique<TTzDateArrayBuilder<TTzTimestamp, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::TzDate32:
            return std::make_unique<TTzDateArrayBuilder<TTzDate32, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::TzDatetime64:
            return std::make_unique<TTzDateArrayBuilder<TTzDatetime64, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::TzTimestamp64:
            return std::make_unique<TTzDateArrayBuilder<TTzTimestamp64, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Decimal:
            return std::make_unique<TFixedSizeArrayBuilder<NYql::NDecimal::TInt128, Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        default:
            Y_ENSURE(false, "Unsupported data slot");
        }
    }
    
    TResourceTypeInspector resource(typeInfoHelper, type);
    if (resource) {
        return std::make_unique<TResourceArrayBuilder<Nullable>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
    }

    TPgTypeInspector typePg(typeInfoHelper, type);
    if (typePg) {
        auto desc = typeInfoHelper.FindPgTypeDescription(typePg.GetTypeId());
        if (desc->PassByValue) {
            return std::make_unique<TFixedSizeArrayBuilder<ui64, true>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
        } else {
            if (desc->Typelen == -1) {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::Text>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
                ret->SetPgBuilder(pgBuilder, desc->Typelen);
                return ret;
            } else if (desc->Typelen == -2) {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::CString>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
                ret->SetPgBuilder(pgBuilder, desc->Typelen);
                return ret;
            } else {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::Fixed>>(typeInfoHelper, type, pool, maxLen, totalAllocated);
                ret->SetPgBuilder(pgBuilder, desc->Typelen);
                return ret;
            }
        }
    }

    Y_ENSURE(false, "Unsupported type");
}

inline std::unique_ptr<TArrayBuilderBase> MakeArrayBuilderBase(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxBlockLength, const IPgBuilder* pgBuilder, size_t* totalAllocated) {
    const TType* unpacked = type;
    TOptionalTypeInspector typeOpt(typeInfoHelper, type);
    if (typeOpt) {
        unpacked = typeOpt.GetItemType();
    }

    TOptionalTypeInspector unpackedOpt(typeInfoHelper, unpacked);
    TPgTypeInspector unpackedPg(typeInfoHelper, unpacked);
    if (unpackedOpt || typeOpt && unpackedPg) {
        // at least 2 levels of optionals
        ui32 nestLevel = 0;
        auto currentType = type;
        auto previousType = type;
        TVector<const TType*> types;
        for (;;) {
            ++nestLevel;
            previousType = currentType;
            types.push_back(currentType);
            TOptionalTypeInspector currentOpt(typeInfoHelper, currentType);
            currentType = currentOpt.GetItemType();
            TOptionalTypeInspector nexOpt(typeInfoHelper, currentType);
            if (!nexOpt) {
                break;
            }
        }

        if (TPgTypeInspector(typeInfoHelper, currentType)) {
            previousType = currentType;
            ++nestLevel;
        }

        auto builder = MakeArrayBuilderBase(typeInfoHelper, previousType, pool, maxBlockLength, pgBuilder, totalAllocated);
        for (ui32 i = 1; i < nestLevel; ++i) {
            builder = std::make_unique<TExternalOptionalArrayBuilder>(typeInfoHelper, types[nestLevel - 1 - i], pool, maxBlockLength, std::move(builder), totalAllocated);
        }

        return builder;
    } else {
        if (typeOpt) {
            return MakeArrayBuilderImpl<true>(typeInfoHelper, type, pool, maxBlockLength, pgBuilder, totalAllocated);
        } else {
            return MakeArrayBuilderImpl<false>(typeInfoHelper, type, pool, maxBlockLength, pgBuilder, totalAllocated);
        }
    }
}

inline std::unique_ptr<IArrayBuilder> MakeArrayBuilder(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxBlockLength, const IPgBuilder* pgBuilder, size_t* totalAllocated = nullptr) {
    return MakeArrayBuilderBase(typeInfoHelper, type, pool, maxBlockLength, pgBuilder, totalAllocated);
}

inline std::unique_ptr<IScalarBuilder> MakeScalarBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    Y_UNUSED(typeInfoHelper);
    Y_UNUSED(type);
    Y_ENSURE(false);
    return nullptr;
}

}
}
