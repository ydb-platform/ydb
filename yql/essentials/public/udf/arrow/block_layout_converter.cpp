#include "block_layout_converter.h"

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

#include <util/generic/vector.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

namespace NKikimr::NMiniKQL {

struct IColumnDataExtractor {
    using TPtr = std::unique_ptr<IColumnDataExtractor>;

    virtual ~IColumnDataExtractor() = default;

    virtual TVector<const ui8*> GetColumnsData(const std::shared_ptr<arrow::ArrayData>& array) = 0;
    virtual TVector<const ui8*> GetNullBitmap(const std::shared_ptr<arrow::ArrayData>& array) = 0;
    virtual ui32 GetElementSize() = 0;
    virtual NPackedTuple::EColumnSizeType GetElementSizeType() = 0;
};

// ------------------------------------------------------------

template <typename TLayout, bool Nullable>
class TFixedSizeColumnDataExtractor : public IColumnDataExtractor {
public:
    TFixedSizeColumnDataExtractor() = default;

    TVector<const ui8*> GetColumnsData(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 2);

        return {array->GetValues<ui8*>(1)};
    }

    const TVector<const ui8*> GetNullBitmap(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 2);

        return {array->GetValues<ui8*>(0)};
    }

    ui32 GetElementSize() override {
        return sizeof(TLayout);
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }
};

template <bool Nullable>
class TResourceColumnDataExtractor : public IColumnDataExtractor {
public:
    TResourceColumnDataExtractor() = default;

    TVector<const ui8*> GetColumnsData(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 2);
        Y_ENSURE(array->child_data.empty());

        return {array->GetValues<ui8*>(1)};
    }

    const TVector<const ui8*> GetNullBitmap(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 2);
        Y_ENSURE(array->child_data.empty());

        return {array->GetValues<ui8*>(0)};
    }

    ui32 GetElementSize() override {
        return sizeof(NUdf::TUnboxedValue);
    }

    NPackedTuple::EColumnSizeType GetElementSizeTypes() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }
};

template <typename TStringType, bool Nullable>
class TStringColumnDataExtractor : public IColumnDataExtractor {
    using TOffset = typename TStringType::offset_type;

public:
    TStringColumnDataExtractor() = default;

    TVector<const ui8*> GetColumnsData(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 3);
        Y_ENSURE(array->child_data.empty());

        return {array->GetValues<ui8*>(1), array->GetValues<ui8*>(2)};
    }

    const TVector<const ui8*> GetNullBitmap(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 3);
        Y_ENSURE(array->child_data.empty());

        return {array->GetValues<ui8*>(0), nullptr};
    }

    ui32 GetElementSize() override {
        return 16; // for now threshold for variable sized types is 16 bytes
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Variable;
    }
};

template <bool Nullable>
class TTupleColumnDataExtractor : public IColumnDataExtractor {
public:
    TTupleColumnDataExtractor(std::vector<IBlockTrimmer::TPtr> children)
        : Children_(std::move(children))
    {}

    TVector<const ui8*> GetColumnsData(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 1);

        TVector<const ui8*> childrenData;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            auto data = Children_[i]->GetColumnsData(array->child_data[i]);
            childrenData.insert(childrenData.end(), data.begin(), data.end());
        }

        return childrenData;
    }

    const TVector<const ui8*> GetNullBitmap(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 1);

        TVector<const ui8*> childrenData;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            auto data = Children_[i]->GetNullBitmap(array->child_data[i]);
            childrenData.insert(childrenData.end(), data.begin(), data.end());
        }

        return childrenData;
    }

    ui32 GetElementSize() override {
        ui32 totalSize = 0;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            totalSize += Children_[i]->GetElementSize();
        }

        return totalSize;
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        ui32 result = NPackedTuple::EColumnSizeType::Fixed;
        Y_ENSURE(array->child_data.size() == Children_.size());

        for (size_t i = 0; i < Children_.size(); i++) {
            if (Children_[i]->GetElementSizeType() == NPackedTuple::EColumnSizeType::Variable) {
                result = NPackedTuple::EColumnSizeType::Variable;
            }
        }

        return result;
    }

protected:
    std::vector<IBlockTrimmer::TPtr> Children_;
};

template<typename TDate, bool Nullable>
class TTzDateColumnDataExtractor : public TTupleColumnDataExtractor<Nullable> {
    using TBase = TTupleColumnDataExtractor<Nullable>;
    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

public:
    TTzDateBlockTrimmer() {
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataExtractor<TDateLayout, false>>(pool));
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataExtractor<ui16, false>>(pool));
    }
};

class TExternalOptionalColumnDataExtractor : public IColumnDataExtractor {
public:
    TExternalOptionalColumnDataExtractor(IColumnDataExtractor::TPtr inner)
        : Inner_(std::move(inner))
    {}

    TVector<const ui8*> GetColumnsData(const std::shared_ptr<arrow::ArrayData>& array) override {
        Y_ENSURE(array->buffers.size() == 1);
        Y_ENSURE(array->child_data.size() == 1);

        return Inner_->GetColumnsData(array->child_data[0]);
    }

    const TVector<const ui8*> GetNullBitmap(const std::shared_ptr<arrow::ArrayData>& array) override {
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

private:
    IColumnDataExtractor::TPtr Inner_;
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

    static TResult::TPtr MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>();
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true, NKikimr::NUdf::EDataSlot::String>>();
        }
    }

    static TResult::TPtr MakeResource(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TResource<true>>();
        } else {
            return std::make_unique<TResource<false>>();
        }
    }

    template<typename TTzDate>
    static TResult::TPtr MakeTzDate(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>();
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>();
        }
    }
};

// ------------------------------------------------------------

class TBlockLayoutConverter : public IBlockLayoutConverter {
public:
    TBlockLayoutConverter(
        TVector<IColumnDataExtractor::TPtr>&& extractors,
        const TVector<NPackedTuple::EColumnRole>& roles,
        arrow::MemoryPool* pool
    )
        : Extractors_(std::move(extractors))
        , Pool_(pool)
    {
        Y_ENSURE(roles.size() == Extractors_.size());
        TVector<TColumnDesc> columnDescrs(Extractors_.size());

        for (auto i = 0; i < columnDescrs.size(); ++i) {
            auto& descr = columnDescrs[i];
            descr.Role = roles[i];
            descr.DataSize = Extractors_[i]->GetElementSize();
            descr.SizeType = Extractors_[i]->GetElementSizeType();
        }

        TupleLayout_ = NPackedTuple::TTupleLayout::Create(columnDescrs);
    }

    void Pack(const TVector<arrow::Datum>& columns, PackResult& packed) override {
        Y_ENSURE(columns.size() == Extractors_.size());
        TVector<const ui8*> columnsData;
        TVector<const ui8*> columnsNullBitmap;

        for (auto i = 0; i < columns.size(); ++i) {
            const auto& column = columns[i];

            auto data = Extractors_[i]->GetColumnsData(column.array());
            columnsData.insert(columnsData.end(), data.begin(), data.end());

            auto nullBitmap = Extractors_[i]->GetNullBitmap(column.array());
            columnsNullBitmap.insert(columnsNullBitmap.end(), nullBitmap.begin(), nullBitmap.end());
        }

        auto& packedTuples = packed.PackedTuples;
        auto& overflow = packed.Overflow;

        auto nTuples = columns.front().array()->length;
        auto currentSize = packedTuples.size();
        auto bytes = (TupleLayout_->TotalRowSize) * nTuples;
        packedTuples.reserve(currentSize + bytes + 64);

        TupleLayout_->Pack(columnsData.data(), columnsNullBitmap.data(), packedTuples.data(), overflow, 0, nTuples);
    }
    
    void Unpack(const PackResult& packed, TVector<arrow::Datum>& columns) override {

        TupleLayout_->Unpack();

    }

    const NPackedTuple::TTupleLayout* GetTupleLayout() const override {
        return TupleLayout_.get();
    }

private:
    TVector<IColumnDataExtractor::TPtr> Extractors_;
    arrow::MemoryPool* Pool_;
    std::unique_ptr<NPackedTuple::TTupleLayout> TupleLayout_;
};

// ------------------------------------------------------------

IBlockLayoutConverter::TPtr MakeBlockLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const Tvector<NUdf::TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles, arrow::MemoryPool* pool)
{
    TVector<IColumnDataExtractor::TPtr> extractors;

    for (auto type: types) {
        extractors.emplace_back(DispatchByArrowTraits<IColumnDataExtractorTraits>(typeInfoHelper, type, nullptr));
    }

    return std::make_unique<TBlockLayoutConverter>(std::move(extractors), roles, pool);
}

} // namespace NKikimr::NMiniKQL
