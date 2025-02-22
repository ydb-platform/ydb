#include "block_layout_converter.h"

#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
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

#include <util/generic/vector.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

namespace NKikimr::NMiniKQL {

struct IColumnDataExtractor {
    using TPtr = std::unique_ptr<IColumnDataExtractor>;

    virtual ~IColumnDataExtractor() = default;

    virtual TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) = 0;
    virtual TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) = 0;
    virtual ui32 GetElementSize() = 0;
    virtual NPackedTuple::EColumnSizeType GetElementSizeType() = 0;
    virtual std::shared_ptr<arrow::ArrayData> ReserveArray(ui64 bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) = 0;
};

// ------------------------------------------------------------

template <typename TLayout, bool Nullable>
class TFixedSizeColumnDataExtractor : public IColumnDataExtractor {
public:
    TFixedSizeColumnDataExtractor(arrow::MemoryPool* pool, TType* type)
        : Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 2);

        return {array->GetMutableValues<ui8>(1)};
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 2);

        return {array->GetMutableValues<ui8>(0)};
    }

    ui32 GetElementSize() override {
        return sizeof(TLayout);
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(ui64 bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        Y_ENSURE(bytes == len * GetElementSize());

        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto dataBuffer = NUdf::AllocateResizableBuffer(bytes, Pool_);

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap), std::move(dataBuffer)});
    }

protected:
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

template <bool Nullable>
class TResourceColumnDataExtractor : public IColumnDataExtractor {
public:
    TResourceColumnDataExtractor(arrow::MemoryPool* pool, TType* type)
        : Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 2);
        Y_ENSURE(array->child_data.empty());

        return {array->GetMutableValues<ui8>(1)};
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 2);
        Y_ENSURE(array->child_data.empty());

        return {array->GetMutableValues<ui8>(0)};
    }

    ui32 GetElementSize() override {
        return sizeof(NUdf::TUnboxedValue);
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(ui64 bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        Y_ENSURE(bytes == len * GetElementSize());

        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto dataBuffer = NUdf::AllocateResizableBuffer<NUdf::TResizableManagedBuffer<NUdf::TUnboxedValue>>(bytes, Pool_);
        ARROW_OK(dataBuffer->Resize(bytes));

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap), std::move(dataBuffer)});
    }

protected:
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

template <typename TStringType, bool Nullable>
class TStringColumnDataExtractor : public IColumnDataExtractor {
    using TOffset = typename TStringType::offset_type;

public:
    TStringColumnDataExtractor(arrow::MemoryPool* pool, TType* type)
        : Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 3);
        Y_ENSURE(array->child_data.empty());

        return {array->GetMutableValues<ui8>(1), array->GetMutableValues<ui8>(2)};
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 3);
        Y_ENSURE(array->child_data.empty());

        return {array->GetMutableValues<ui8>(0), nullptr};
    }

    ui32 GetElementSize() override {
        return 16; // for now threshold for variable sized types is 16 bytes
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Variable;
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(ui64 bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto offsetBuffer = NUdf::AllocateResizableBuffer(sizeof(TOffset) * len, Pool_);
        auto dataBuffer = NUdf::AllocateResizableBuffer(bytes, Pool_);

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap), std::move(offsetBuffer), std::move(dataBuffer)});
    }

protected:
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

template <bool Nullable>
class TTupleColumnDataExtractor : public IColumnDataExtractor {
public:
    TTupleColumnDataExtractor(
        std::vector<IColumnDataExtractor::TPtr> children, arrow::MemoryPool* pool, TType* type
    )
        : Children_(std::move(children))
        , Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 1);

        TVector<ui8*> childrenData;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            auto data = Children_[i]->GetColumnsData(array->child_data[i]);
            childrenData.insert(childrenData.end(), data.begin(), data.end());
        }

        return childrenData;
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 1);

        TVector<ui8*> childrenData;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            auto data = Children_[i]->GetNullBitmap(array->child_data[i]);
            childrenData.insert(childrenData.end(), data.begin(), data.end());
        }

        return childrenData;
    }

    ui32 GetElementSize() override {
        ui32 totalSize = 0;

        for (size_t i = 0; i < Children_.size(); i++) {
            totalSize += Children_[i]->GetElementSize();
        }

        return totalSize;
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        auto result = NPackedTuple::EColumnSizeType::Fixed;

        for (size_t i = 0; i < Children_.size(); i++) {
            if (Children_[i]->GetElementSizeType() == NPackedTuple::EColumnSizeType::Variable) {
                result = NPackedTuple::EColumnSizeType::Variable;
            }
        }

        return result;
    }

    // This highly likely wont be working, because we need bytes and len per tuple component
    // So do not use tuples)
    std::shared_ptr<arrow::ArrayData> ReserveArray(ui64 bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        std::vector<std::shared_ptr<arrow::ArrayData>> reservedChildren;
        for (size_t i = 0; i < Children_.size(); i++) {
            reservedChildren.push_back(Children_[i]->ReserveArray(bytes, len));
        }

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap)}, std::move(reservedChildren));
    }

protected:
    TTupleColumnDataExtractor() = default;

protected:
    std::vector<IColumnDataExtractor::TPtr> Children_;
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

template<typename TDate, bool Nullable>
class TTzDateColumnDataExtractor : public TTupleColumnDataExtractor<Nullable> {
    using TBase = TTupleColumnDataExtractor<Nullable>;
    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

public:
    TTzDateColumnDataExtractor(arrow::MemoryPool* pool, TType* type) {
        // This highly likely wont be working, because we need another type for datetime components
        // So do not use datetime)
        this->Pool_ = pool;
        this->Type_ = type;
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataExtractor<TDateLayout, false>>(pool, type));
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataExtractor<ui16, false>>(pool, type));
    }
};

class TExternalOptionalColumnDataExtractor : public IColumnDataExtractor {
public:
    TExternalOptionalColumnDataExtractor(
        IColumnDataExtractor::TPtr inner, arrow::MemoryPool* pool, TType* type
    )
        : Inner_(std::move(inner))
        , Pool_(pool)
        , Type_(type)
    {}

    TVector<ui8*> GetColumnsData(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 1);
        Y_ENSURE(array->child_data.size() == 1);

        return Inner_->GetColumnsData(array->child_data[0]);
    }

    TVector<ui8*> GetNullBitmap(std::shared_ptr<arrow::ArrayData> array) override {
        Y_ENSURE(array->buffers.size() == 1);
        Y_ENSURE(array->child_data.size() == 1);

        return Inner_->GetNullBitmap(array->child_data[0]);
    }

    ui32 GetElementSize() override {
        return Inner_->GetElementSize();
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return Inner_->GetElementSizeType();
    }

    std::shared_ptr<arrow::ArrayData> ReserveArray(ui64 bytes, ui32 len, [[maybe_unused]] bool isBitmapNull = false) override {
        std::shared_ptr<arrow::DataType> type;
        auto isConverted = ConvertArrowType(Type_, type);
        Y_ENSURE(isConverted);

        std::shared_ptr<arrow::Buffer> nullBitmap;
        if (!isBitmapNull) {
            nullBitmap = NUdf::AllocateBitmapWithReserve(len, Pool_);
        }
        auto reservedInner = Inner_->ReserveArray(bytes, len);

        return arrow::ArrayData::Make(std::move(type), len, {std::move(nullBitmap)}, {std::move(reservedInner)});
    }

private:
    IColumnDataExtractor::TPtr Inner_;
    arrow::MemoryPool* Pool_;
    TType* Type_;
};

// ------------------------------------------------------------

struct TColumnDataExtractorTraits {
    using TResult = IColumnDataExtractor;
    template <bool Nullable>
    using TTuple = TTupleColumnDataExtractor<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeColumnDataExtractor<T, Nullable>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot>
    using TStrings = TStringColumnDataExtractor<TStringType, Nullable>;
    using TExtOptional = TExternalOptionalColumnDataExtractor;
    template<bool Nullable>
    using TResource = TResourceColumnDataExtractor<Nullable>;
    template<typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateColumnDataExtractor<TTzDate, Nullable>;

    constexpr static bool PassType = false;

    static TResult::TPtr MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool* pool, TType* type) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>(pool, type);
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true, NKikimr::NUdf::EDataSlot::String>>(pool, type);
        }
    }

    static TResult::TPtr MakeResource(bool isOptional, arrow::MemoryPool* pool, TType* type) {
        if (isOptional) {
            return std::make_unique<TResource<true>>(pool, type);
        } else {
            return std::make_unique<TResource<false>>(pool, type);
        }
    }

    template<typename TTzDate>
    static TResult::TPtr MakeTzDate(bool isOptional, arrow::MemoryPool* pool, TType* type) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>(pool, type);
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>(pool, type);
        }
    }
};

// ------------------------------------------------------------

class TBlockLayoutConverter : public IBlockLayoutConverter {
public:
    TBlockLayoutConverter(
        TVector<IColumnDataExtractor::TPtr>&& extractors,
        const TVector<NPackedTuple::EColumnRole>& roles,
        bool rememberNullBitmaps = true // remember bitmaps which are equal to nullptr to not allocate memory for them in unpack
    )
        : Extractors_(std::move(extractors))
        , RememberNullBitmaps_(rememberNullBitmaps)
        , IsBitmapNull_(Extractors_.size(), false)
    {
        Y_ENSURE(roles.size() == Extractors_.size());
        TVector<NPackedTuple::TColumnDesc> columnDescrs(Extractors_.size());

        for (size_t i = 0; i < columnDescrs.size(); ++i) {
            auto& descr = columnDescrs[i];
            descr.Role = roles[i];
            descr.DataSize = Extractors_[i]->GetElementSize();
            descr.SizeType = Extractors_[i]->GetElementSizeType();
        }

        TupleLayout_ = NPackedTuple::TTupleLayout::Create(columnDescrs);
    }

    void Pack(const TVector<arrow::Datum>& columns, PackResult& packed) override {
        Y_ENSURE(columns.size() == Extractors_.size());
        std::fill(IsBitmapNull_.begin(), IsBitmapNull_.end(), false);
        TVector<const ui8*> columnsData;
        TVector<const ui8*> columnsNullBitmap;

        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& column = columns[i];

            auto data = Extractors_[i]->GetColumnsData(column.array());
            columnsData.insert(columnsData.end(), data.begin(), data.end());

            auto nullBitmap = Extractors_[i]->GetNullBitmap(column.array());
            columnsNullBitmap.insert(columnsNullBitmap.end(), nullBitmap.begin(), nullBitmap.end());
            if (nullBitmap.front() == nullptr) {
                IsBitmapNull_[i] = true;
            }
        }

        auto& packedTuples = packed.PackedTuples;
        auto& overflow = packed.Overflow;

        auto nTuples = columns.front().array()->length;
        packed.NTuples = nTuples;
        auto currentSize = packedTuples.size();
        auto bytes = (TupleLayout_->TotalRowSize) * nTuples;
        packedTuples.resize(currentSize + bytes + 64);

        TupleLayout_->Pack(
            columnsData.data(), columnsNullBitmap.data(), packedTuples.data(), overflow, 0, nTuples);
    }
    
    void Unpack(const PackResult& packed, TVector<arrow::Datum>& columns) override {
        columns.resize(TupleLayout_->Columns.size());

        std::vector<ui64, TMKQLAllocator<ui64>> bytesPerColumn;
        TupleLayout_->CalculateColumnSized(
            packed.PackedTuples.data(), packed.NTuples, bytesPerColumn);

        TVector<ui8*> columnsData;
        TVector<ui8*> columnsNullBitmap;
        for (size_t i = 0; i < columns.size(); ++i) {
            bool isBitmapNull = false;
            if (RememberNullBitmaps_) {
                isBitmapNull = IsBitmapNull_[i];
            }
            columns[i] = Extractors_[i]->ReserveArray(bytesPerColumn[i], packed.NTuples, isBitmapNull);

            auto data = Extractors_[i]->GetColumnsData(columns[i].array());
            columnsData.insert(columnsData.end(), data.begin(), data.end());

            auto nullBitmap = Extractors_[i]->GetNullBitmap(columns[i].array());
            columnsNullBitmap.insert(columnsNullBitmap.end(), nullBitmap.begin(), nullBitmap.end());
        }

        TupleLayout_->Unpack(
            columnsData.data(), columnsNullBitmap.data(), packed.PackedTuples.data(), packed.Overflow, 0, packed.NTuples);
    }

    const NPackedTuple::TTupleLayout* GetTupleLayout() const override {
        return TupleLayout_.get();
    }

private:
    TVector<IColumnDataExtractor::TPtr> Extractors_;
    THolder<NPackedTuple::TTupleLayout> TupleLayout_;
    bool RememberNullBitmaps_;
    TVector<bool> IsBitmapNull_;
};

// ------------------------------------------------------------

IBlockLayoutConverter::TPtr MakeBlockLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles, arrow::MemoryPool* pool)
{
    TVector<IColumnDataExtractor::TPtr> extractors;

    for (auto type: types) {
        extractors.emplace_back(DispatchByArrowTraits<TColumnDataExtractorTraits>(typeInfoHelper, type, nullptr, pool, type));
    }

    return std::make_unique<TBlockLayoutConverter>(std::move(extractors), roles);
}

} // namespace NKikimr::NMiniKQL
