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

    TArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen)
        : ArrowType(std::move(arrowType))
        , Pool(&pool)
        , MaxLen(maxLen)
        , MaxBlockSizeInBytes(typeInfoHelper.GetMaxBlockBytes())
    {
        Y_ABORT_UNLESS(ArrowType);
        Y_ABORT_UNLESS(maxLen > 0);
    }

    TArrayBuilderBase(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : TArrayBuilderBase(typeInfoHelper, GetArrowType(typeInfoHelper, type), pool, maxLen)
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

    const std::shared_ptr<arrow::DataType> ArrowType;
    arrow::MemoryPool* const Pool;
    const size_t MaxLen;
    const size_t MaxBlockSizeInBytes;
private:
    size_t CurrLen = 0;
};

template<typename T, bool Nullable>
class TFixedSizeArrayBuilder final : public TArrayBuilderBase {
public:
    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen)
        : TArrayBuilderBase(typeInfoHelper, std::move(arrowType), pool, maxLen)
    {
        Reserve();
    }

    TFixedSizeArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen)
    {
        Reserve();
    }

    void UnsafeReserve(size_t length) {
        SetCurrLen(length);
    }

    T* MutableData() {
        return DataPtr;
    }

    ui8* MutableValidMask() {
        return NullPtr;
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                return DoAdd(TBlockItem{});
            }
        }
        DoAdd(TBlockItem(value.Get<T>()));
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullPtr[GetCurrLen()] = 0;
                DataPtr[GetCurrLen()] = T{};
                return;
            }
            NullPtr[GetCurrLen()] = 1;
        }

        DataPtr[GetCurrLen()] = value.As<T>();
    }

    void DoAdd(TBlockItem value, size_t count) final {
        if constexpr (Nullable) {
            if (!value) {
                std::fill(NullPtr + GetCurrLen(), NullPtr + GetCurrLen() + count, 0);
                std::fill(DataPtr + GetCurrLen(), DataPtr + GetCurrLen() + count, T{});
                return;
            }
            std::fill(NullPtr + GetCurrLen(), NullPtr + GetCurrLen() + count, 1);
        }

        std::fill(DataPtr + GetCurrLen(), DataPtr + GetCurrLen() + count, value.As<T>());
    }

    void DoAdd(TInputBuffer &input) final {
        if constexpr (Nullable) {
            if (!input.PopChar()) {
                return DoAdd(TBlockItem{});
            }
        }
        DoAdd(TBlockItem(input.PopNumber<T>()));
    }

    void DoAddDefault() final {
        if constexpr (Nullable) {
            NullPtr[GetCurrLen()] = 1;
        }
        DataPtr[GetCurrLen()] = T{};
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

        const T* src = array.GetValues<T>(1);
        T* dst = DataPtr + GetCurrLen();
        CompressArray(src, sparseBitmap, dst, array.length);
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            for (size_t i = beginIndex; i < beginIndex + count; ++i) {
                NullPtr[GetCurrLen() + i - beginIndex] = !IsNull(array, i);
            }
        }

        const T* values = array.GetValues<T>(1);
        for (size_t i = beginIndex; i < beginIndex + count; ++i) {
            DataPtr[GetCurrLen() + i - beginIndex] = T(values[i]);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(array.buffers.size() > 1);
        if constexpr (Nullable) {
            for (size_t i = 0; i < count; ++i) {
                NullPtr[GetCurrLen() + i] = !IsNull(array, indexes[i]);
            }
        }

        const T* values = array.GetValues<T>(1);
        for (size_t i = 0; i < count; ++i) {
            DataPtr[GetCurrLen() + i] = T(values[indexes[i]]);
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

private:
    void Reserve() {
        DataBuilder = std::make_unique<TTypedBufferBuilder<T>>(Pool);
        DataBuilder->Reserve(MaxLen + 1);
        DataPtr = DataBuilder->MutableData();
        if constexpr (Nullable) {
            NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
            NullBuilder->Reserve(MaxLen + 1);
            NullPtr = NullBuilder->MutableData();
        }
    }

    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
    std::unique_ptr<TTypedBufferBuilder<T>> DataBuilder;
    ui8* NullPtr = nullptr;
    T* DataPtr = nullptr;
};

template<typename TStringType, bool Nullable, EPgStringType PgString = EPgStringType::None>
class TStringArrayBuilder final : public TArrayBuilderBase {
public:
    using TOffset = typename TStringType::offset_type;
    TStringArrayBuilder(const ITypeInfoHelper& typeInfoHelper, std::shared_ptr<arrow::DataType> arrowType, arrow::MemoryPool& pool, size_t maxLen)
        : TArrayBuilderBase(typeInfoHelper, std::move(arrowType), pool, maxLen)
    {
        Reserve();
    }

    TStringArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen)
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
                DataBuilder->Reserve(size);
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
                    DataBuilder->Reserve(strSize);
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
                    DataBuilder->Reserve(strSize);
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
                    DataBuilder->Reserve(str.size());
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
    void Reserve() {
        if constexpr (Nullable) {
            NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
            NullBuilder->Reserve(MaxLen + 1);
        }
        OffsetsBuilder = std::make_unique<TTypedBufferBuilder<TOffset>>(Pool);
        OffsetsBuilder->Reserve(MaxLen + 1);
        DataBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        DataBuilder->Reserve(MaxBlockSizeInBytes);
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

template<bool Nullable>
class TTupleArrayBuilder final : public TArrayBuilderBase {
public:
    TTupleArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen,
                       TVector<TArrayBuilderBase::Ptr>&& children)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen)
        , Children(std::move(children))
    {
        Reserve();
    }

    void DoAdd(NUdf::TUnboxedValuePod value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder->UnsafeAppend(0);
                for (ui32 i = 0; i < Children.size(); ++i) {
                    Children[i]->AddDefault();
                }
                return;
            }
            NullBuilder->UnsafeAppend(1);
        }

        auto elements = value.GetElements();
        if (elements) {
            for (ui32 i = 0; i < Children.size(); ++i) {
                Children[i]->Add(elements[i]);
            }
        } else {
            for (ui32 i = 0; i < Children.size(); ++i) {
                auto element = value.GetElement(i);
                Children[i]->Add(element);
            }
        }
    }

    void DoAdd(TBlockItem value) final {
        if constexpr (Nullable) {
            if (!value) {
                NullBuilder->UnsafeAppend(0);
                for (ui32 i = 0; i < Children.size(); ++i) {
                    Children[i]->AddDefault();
                }
                return;
            }
            NullBuilder->UnsafeAppend(1);
        }

        auto elements = value.AsTuple();
        for (ui32 i = 0; i < Children.size(); ++i) {
            Children[i]->Add(elements[i]);
        }
    }

    void DoAdd(TInputBuffer& input) final {
        if constexpr (Nullable) {
            if (!input.PopChar()) {
                return DoAdd(TBlockItem{});
            }
            NullBuilder->UnsafeAppend(1);
        }

        for (ui32 i = 0; i < Children.size(); ++i) {
            Children[i]->Add(input);
        }
    }

    void DoAddDefault() final {
        if constexpr (Nullable) {
            NullBuilder->UnsafeAppend(1);
        }
        for (ui32 i = 0; i < Children.size(); ++i) {
            Children[i]->AddDefault();
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui8* sparseBitmap, size_t popCount) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == Children.size());

        if constexpr (Nullable) {
            if (array.buffers.front()) {
                ui8* dstBitmap = NullBuilder->End();
                CompressAsSparseBitmap(array.GetValues<ui8>(0, 0), array.offset, sparseBitmap, dstBitmap, array.length);
                NullBuilder->UnsafeAdvance(popCount);
            } else {
                NullBuilder->UnsafeAppend(popCount, 1);
            }
        }

        for (size_t i = 0; i < Children.size(); ++i) {
            Children[i]->AddMany(*array.child_data[i], popCount, sparseBitmap, array.length);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, ui64 beginIndex, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == Children.size());

        if constexpr (Nullable) {
            for (ui64 i = beginIndex; i < beginIndex + count; ++i) {
                NullBuilder->UnsafeAppend(!IsNull(array, i));
            }
        }

        for (size_t i = 0; i < Children.size(); ++i) {
            Children[i]->AddMany(*array.child_data[i], beginIndex, count);
        }
    }

    void DoAddMany(const arrow::ArrayData& array, const ui64* indexes, size_t count) final {
        Y_ABORT_UNLESS(!array.buffers.empty());
        Y_ABORT_UNLESS(array.child_data.size() == Children.size());

        if constexpr (Nullable) {
            for (size_t i = 0; i < count; ++i) {
                NullBuilder->UnsafeAppend(!IsNull(array, indexes[i]));
            }
        }

        for (size_t i = 0; i < Children.size(); ++i) {
            Children[i]->AddMany(*array.child_data[i], indexes, count);
        }
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
        result->Children.reserve(Children.size());
        for (ui32 i = 0; i < Children.size(); ++i) {
            result->Children.emplace_back(Children[i]->BuildTree(finish));
        }

        if (!finish) {
            Reserve();
        }

        return result;
    }

private:
    void Reserve() {
        if constexpr (Nullable) {
            NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
            NullBuilder->Reserve(MaxLen + 1);
        }
    }

private:
    TVector<std::unique_ptr<TArrayBuilderBase>> Children;
    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
};

class TExternalOptionalArrayBuilder final : public TArrayBuilderBase {
public:
    TExternalOptionalArrayBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, size_t maxLen, std::unique_ptr<TArrayBuilderBase>&& inner)
        : TArrayBuilderBase(typeInfoHelper, type, pool, maxLen)
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
    void Reserve() {
        NullBuilder = std::make_unique<TTypedBufferBuilder<ui8>>(Pool);
        NullBuilder->Reserve(MaxLen + 1);
    }

private:
    std::unique_ptr<TArrayBuilderBase> Inner;
    std::unique_ptr<TTypedBufferBuilder<ui8>> NullBuilder;
};

std::unique_ptr<TArrayBuilderBase> MakeArrayBuilderBase(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxBlockLength, const IPgBuilder* pgBuilder);

template<bool Nullable>
inline std::unique_ptr<TArrayBuilderBase> MakeArrayBuilderImpl(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxLen, const IPgBuilder* pgBuilder) {
    if constexpr (Nullable) {
        TOptionalTypeInspector typeOpt(typeInfoHelper, type);
        type = typeOpt.GetItemType();
    }

    TTupleTypeInspector typeTuple(typeInfoHelper, type);
    if (typeTuple) {
        TVector<std::unique_ptr<TArrayBuilderBase>> children;
        for (ui32 i = 0; i < typeTuple.GetElementsCount(); ++i) {
            const TType* childType = typeTuple.GetElementType(i);
            auto childBuilder = MakeArrayBuilderBase(typeInfoHelper, childType, pool, maxLen, pgBuilder);
            children.push_back(std::move(childBuilder));
        }

        return std::make_unique<TTupleArrayBuilder<Nullable>>(typeInfoHelper, type, pool, maxLen, std::move(children));
    }

    TDataTypeInspector typeData(typeInfoHelper, type);
    if (typeData) {
        auto typeId = typeData.GetTypeId();
        switch (GetDataSlot(typeId)) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TFixedSizeArrayBuilder<i8, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return std::make_unique<TFixedSizeArrayBuilder<ui8, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TFixedSizeArrayBuilder<i16, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TFixedSizeArrayBuilder<ui16, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Date32:
            return std::make_unique<TFixedSizeArrayBuilder<i32, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TFixedSizeArrayBuilder<ui32, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Interval64:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
            return std::make_unique<TFixedSizeArrayBuilder<i64, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TFixedSizeArrayBuilder<ui64, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Float:
            return std::make_unique<TFixedSizeArrayBuilder<float, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Double:
            return std::make_unique<TFixedSizeArrayBuilder<double, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::JsonDocument:
            return std::make_unique<TStringArrayBuilder<arrow::BinaryType, Nullable>>(typeInfoHelper, type, pool, maxLen);
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
            return std::make_unique<TStringArrayBuilder<arrow::StringType, Nullable>>(typeInfoHelper, type, pool, maxLen);
        default:
            Y_ENSURE(false, "Unsupported data slot");
        }
    }

    TPgTypeInspector typePg(typeInfoHelper, type);
    if (typePg) {
        auto desc = typeInfoHelper.FindPgTypeDescription(typePg.GetTypeId());
        if (desc->PassByValue) {
            return std::make_unique<TFixedSizeArrayBuilder<ui64, true>>(typeInfoHelper, type, pool, maxLen);
        } else {
            if (desc->Typelen == -1) {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::Text>>(typeInfoHelper, type, pool, maxLen);
                ret->SetPgBuilder(pgBuilder, desc->Typelen);
                return ret;
            } else if (desc->Typelen == -2) {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::CString>>(typeInfoHelper, type, pool, maxLen);
                ret->SetPgBuilder(pgBuilder, desc->Typelen);
                return ret;
            } else {
                auto ret = std::make_unique<TStringArrayBuilder<arrow::BinaryType, true, EPgStringType::Fixed>>(typeInfoHelper, type, pool, maxLen);
                ret->SetPgBuilder(pgBuilder, desc->Typelen);
                return ret;
            }
        }
    }

    Y_ENSURE(false, "Unsupported type");
}

inline std::unique_ptr<TArrayBuilderBase> MakeArrayBuilderBase(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxBlockLength, const IPgBuilder* pgBuilder) {
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

        auto builder = MakeArrayBuilderBase(typeInfoHelper, previousType, pool, maxBlockLength, pgBuilder);
        for (ui32 i = 1; i < nestLevel; ++i) {
            builder = std::make_unique<TExternalOptionalArrayBuilder>(typeInfoHelper, types[nestLevel - 1 - i], pool, maxBlockLength, std::move(builder));
        }

        return builder;
    } else {
        if (typeOpt) {
            return MakeArrayBuilderImpl<true>(typeInfoHelper, type, pool, maxBlockLength, pgBuilder);
        } else {
            return MakeArrayBuilderImpl<false>(typeInfoHelper, type, pool, maxBlockLength, pgBuilder);
        }
    }
}

inline std::unique_ptr<IArrayBuilder> MakeArrayBuilder(
    const ITypeInfoHelper& typeInfoHelper, const TType* type, arrow::MemoryPool& pool, 
    size_t maxBlockLength, const IPgBuilder* pgBuilder) {
    return MakeArrayBuilderBase(typeInfoHelper, type, pool, maxBlockLength, pgBuilder);
}

inline std::unique_ptr<IScalarBuilder> MakeScalarBuilder(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    Y_UNUSED(typeInfoHelper);
    Y_UNUSED(type);
    Y_ENSURE(false);
    return nullptr;
}

}
}
