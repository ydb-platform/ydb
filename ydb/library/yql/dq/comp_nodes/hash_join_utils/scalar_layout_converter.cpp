#include "scalar_layout_converter.h"

#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/arrow/defs.h>
#include <yql/essentials/public/udf/arrow/dispatch_traits.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_value_utils.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/system/unaligned_mem.h>
#include "better_mkql_ensure.h"
namespace NKikimr::NMiniKQL {

namespace {

ui32 NullBitmapBytes(ui32 count) {
    return (count + 7) / 8;
}

void SetValidBit(ui8* bitmap, ui32 row) {
    bitmap[row / 8] |= static_cast<ui8>(1u << (row % 8));
}

struct IColumnDataExtractor {
    using TPtr = std::unique_ptr<IColumnDataExtractor>;

    virtual ~IColumnDataExtractor() = default;

    // values[row * stride] is this column. Appends one columnar pointer set for the whole batch:
    // fixed columns contribute data + bitmap, variable columns offsets + bytes + bitmaps.
    virtual void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) = 0;
    
    virtual NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) = 0;

    virtual void CreateColumnFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 count, const THolderFactory& holderFactory, NYql::NUdf::TUnboxedValue* out, size_t stride) {
        for (ui32 row = 0; row < count; ++row) {
            out[row * stride] = CreateFromUnpack(columnsData, columnsNullBitmap, row, holderFactory);
        }
    }
    
    virtual ui32 GetElementSize() = 0;
    virtual NPackedTuple::EColumnSizeType GetElementSizeType() = 0;
    virtual bool IsFloatingPoint() {
        return false;
    }
    // Ugly interface, but I dont care
    virtual void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) = 0;
};

// ------------------------------------------------------------

template <typename TLayout>
void StoreFixedSizeLayout(TLayout& dst, const NYql::NUdf::TUnboxedValue& value) {
    if constexpr (std::is_same_v<TLayout, TGUID>) {
        const auto ref = value.AsStringRef();
        Y_ENSURE(ref.Size() == sizeof(TGUID), "Wrong Uuid size: " << ref.Size());
        dst = ReadUnaligned<TGUID>(ref.Data());
    } else {
        dst = value.Get<TLayout>();
    }
}

template <typename TLayout>
NYql::NUdf::TUnboxedValue CreateFixedSizeValue(const TLayout& data) {
    if constexpr (std::is_same_v<TLayout, TGUID>) {
        return MakeString(NYql::NUdf::TStringRef(reinterpret_cast<const char*>(&data), sizeof(TGUID)));
    } else {
        return NYql::NUdf::TUnboxedValuePod(data);
    }
}

template <typename TLayout, bool Nullable>
class TFixedSizeColumnDataExtractor : public IColumnDataExtractor {
public:
    TFixedSizeColumnDataExtractor(TType* type)
        : Type_(type)
    {}

    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        if (count == 0) {
            return;
        }

        auto& dataStorage = tempStorage.emplace_back(sizeof(TLayout) * count);
        TLayout* dataPtr = reinterpret_cast<TLayout*>(dataStorage.data());

        ui8* bitmapPtr = nullptr;
        if constexpr (Nullable) {
            bitmapPtr = tempStorage.emplace_back(NullBitmapBytes(count)).data();
        }

        for (ui32 row = 0; row < count; ++row) {
            const auto& value = values[row * stride];
            if constexpr (Nullable) {
                if (!value) {
                    std::memset(&dataPtr[row], 0, sizeof(TLayout));
                } else {
                    SetValidBit(bitmapPtr, row);
                    StoreFixedSizeLayout(dataPtr[row], value);
                }
            } else {
                StoreFixedSizeLayout(dataPtr[row], value);
            }
        }

        columnsData.push_back(reinterpret_cast<const ui8*>(dataPtr));
        columnsNullBitmap.push_back(bitmapPtr);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, [[maybe_unused]] const THolderFactory& holderFactory) override {
        Y_UNUSED(holderFactory);

        if constexpr (Nullable) {
            // Check null bitmap (bit at position tupleIndex)
            ui8 byte = columnsNullBitmap[0][tupleIndex / 8];
            bool isNull = !(byte & (1 << (tupleIndex % 8)));

            if (isNull) {
                return NYql::NUdf::TUnboxedValuePod();
            }
        }

        const TLayout* data = reinterpret_cast<const TLayout*>(columnsData[0]) + tupleIndex;
        return CreateFixedSizeValue(*data);
    }

    void CreateColumnFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 count, [[maybe_unused]] const THolderFactory& holderFactory, NYql::NUdf::TUnboxedValue* out, size_t stride) override {
        const TLayout* data = reinterpret_cast<const TLayout*>(columnsData[0]);
        for (ui32 row = 0; row < count; ++row) {
            if constexpr (Nullable) {
                if (!(columnsNullBitmap[0][row / 8] & (1 << (row % 8)))) {
                    out[row * stride] = NYql::NUdf::TUnboxedValuePod();
                    continue;
                }
            }
            out[row * stride] = CreateFixedSizeValue(data[row]);
        }
    }

    ui32 GetElementSize() override {
        return sizeof(TLayout);
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    bool IsFloatingPoint() override {
        return std::is_floating_point_v<TLayout>;
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& packers) override {
        packers.push_back(this);
    }

protected:
    TType* Type_;
};

template <bool Nullable>
class TResourceColumnDataExtractor : public IColumnDataExtractor {
public:
    TResourceColumnDataExtractor(TType* type)
        : Type_(type)
    {}

    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        if (count == 0) {
            return;
        }

        auto& dataStorage = tempStorage.emplace_back(sizeof(NYql::NUdf::TUnboxedValue) * count);
        auto* dataPtr = reinterpret_cast<NYql::NUdf::TUnboxedValue*>(dataStorage.data());

        ui8* bitmapPtr = nullptr;
        if constexpr (Nullable) {
            bitmapPtr = tempStorage.emplace_back(NullBitmapBytes(count)).data();
        }

        for (ui32 row = 0; row < count; ++row) {
            const auto& value = values[row * stride];
            if constexpr (Nullable) {
                if (!value) {
                    new (&dataPtr[row]) NYql::NUdf::TUnboxedValue();
                } else {
                    SetValidBit(bitmapPtr, row);
                    new (&dataPtr[row]) NYql::NUdf::TUnboxedValue(value);
                }
            } else {
                new (&dataPtr[row]) NYql::NUdf::TUnboxedValue(value);
            }
        }

        columnsData.push_back(reinterpret_cast<const ui8*>(dataPtr));
        columnsNullBitmap.push_back(bitmapPtr);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, [[maybe_unused]] const THolderFactory& holderFactory) override {
        Y_UNUSED(holderFactory);

        if constexpr (Nullable) {
            ui8 byte = columnsNullBitmap[0][tupleIndex / 8];
            bool isNull = !(byte & (1 << (tupleIndex % 8)));

            if (isNull) {
                return NYql::NUdf::TUnboxedValuePod();
            }
        }

        NYql::NUdf::TUnboxedValue* data = reinterpret_cast<NYql::NUdf::TUnboxedValue*>(columnsData[0]) + tupleIndex;
        return *data;
    }

    ui32 GetElementSize() override {
        return sizeof(NYql::NUdf::TUnboxedValue);
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& packers) override {
        packers.push_back(this);
    }

protected:
    TType* Type_;
};

template <bool IsNull>
class TSingularColumnDataExtractor : public IColumnDataExtractor {
public:
    TSingularColumnDataExtractor(TType* type) {
        Y_UNUSED(type);
    }

    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        Y_UNUSED(values, stride);
        if (count == 0) {
            return;
        }
        columnsData.push_back(nullptr);
        if constexpr (IsNull) {
            columnsNullBitmap.push_back(tempStorage.emplace_back(NullBitmapBytes(count)).data());
        } else {
            columnsNullBitmap.push_back(nullptr);
        }
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, [[maybe_unused]] const THolderFactory& holderFactory) override {
        Y_UNUSED(columnsData, columnsNullBitmap, tupleIndex, holderFactory);
        return NYql::NUdf::CreateSingularUnboxedValuePod<IsNull>();
    }

    ui32 GetElementSize() override {
        return 0;
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& packers) override {
        packers.push_back(this);
    }
};

template <bool Nullable>
class TStringColumnDataExtractor : public IColumnDataExtractor {
public:
    TStringColumnDataExtractor(TType* type)
        : Type_(type)
    {}

    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        if (count == 0) {
            return;
        }

        ui64 totalSize = 0;
        for (ui32 row = 0; row < count; ++row) {
            const auto& value = values[row * stride];
            if constexpr (Nullable) {
                if (value) {
                    totalSize += value.AsStringRef().Size();
                }
            } else {
                totalSize += value.AsStringRef().Size();
            }
        }
        Y_ENSURE(totalSize <= static_cast<ui64>(Max<ui32>()), "string batch does not fit into 32-bit offsets");

        ui8* bitmapPtr = nullptr;
        if constexpr (Nullable) {
            bitmapPtr = tempStorage.emplace_back(NullBitmapBytes(count)).data();
        }
        // Snapshot buffer pointers immediately: a later emplace can move the outer vector.
        ui8* offsetBytes = tempStorage.emplace_back((static_cast<size_t>(count) + 1) * sizeof(ui32)).data();
        ui8* stringDataPtr = tempStorage.emplace_back(totalSize > 0 ? static_cast<size_t>(totalSize) : 1).data();
        ui32* offsets = reinterpret_cast<ui32*>(offsetBytes);
        if (totalSize == 0) {
            stringDataPtr[0] = 0;
        }

        ui32 currentOffset = 0;
        offsets[0] = 0;
        for (ui32 row = 0; row < count; ++row) {
            const auto& value = values[row * stride];
            if constexpr (Nullable) {
                if (!value) {
                    offsets[row + 1] = currentOffset;
                    continue;
                }
                SetValidBit(bitmapPtr, row);
            }
            const auto ref = value.AsStringRef();
            if (ref.Size() > 0) {
                std::memcpy(stringDataPtr + currentOffset, ref.Data(), ref.Size());
                currentOffset += ref.Size();
            }
            offsets[row + 1] = currentOffset;
        }

        columnsData.push_back(offsetBytes);
        columnsData.push_back(stringDataPtr);
        columnsNullBitmap.push_back(bitmapPtr);
        columnsNullBitmap.push_back(nullptr);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, [[maybe_unused]] const THolderFactory& holderFactory) override {
        if constexpr (Nullable) {
            ui8 byte = columnsNullBitmap[0][tupleIndex / 8];
            bool isNull = !(byte & (1 << (tupleIndex % 8)));

            if (isNull) {
                return NYql::NUdf::TUnboxedValuePod();
            }
        }

        // For string types in tuple layout, we have offsets array and data array
        ui32* offsets = reinterpret_cast<ui32*>(columnsData[0]);
        ui8* data = columnsData[1];

        ui32 start = offsets[tupleIndex];
        ui32 end = offsets[tupleIndex + 1];
        ui32 size = end - start;

        return MakeString(NUdf::TStringRef(reinterpret_cast<const char*>(data + start), size));
    }

    ui32 GetElementSize() override {
        return 16; // threshold for variable sized types
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Variable;
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& packers) override {
        packers.push_back(this);
    }

protected:
    TType* Type_;
};

template <bool Nullable>
class TTupleColumnDataExtractor : public IColumnDataExtractor {
public:
    TTupleColumnDataExtractor(std::vector<IColumnDataExtractor::TPtr> children, TType* type)
        : Children_(std::move(children))
        , Type_(type)
    {}

    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        if (count == 0) {
            return;
        }

        TVector<TVector<NYql::NUdf::TUnboxedValue>> childColumns(Children_.size());
        for (auto& column : childColumns) {
            column.resize(count);
        }
        for (ui32 row = 0; row < count; ++row) {
            const auto& value = values[row * stride];
            if constexpr (Nullable) {
                if (!value) {
                    continue;
                }
            }
            for (size_t child = 0; child < Children_.size(); ++child) {
                childColumns[child][row] = value.GetElement(child);
            }
        }
        for (size_t child = 0; child < Children_.size(); ++child) {
            Children_[child]->ExtractForPackBatch(childColumns[child].data(), count, 1, columnsData, columnsNullBitmap, tempStorage);
        }
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, [[maybe_unused]] const THolderFactory& holderFactory) override {
        NYql::NUdf::TUnboxedValue* items = nullptr;
        auto result = holderFactory.CreateDirectArrayHolder(Children_.size(), items);

        size_t offset = 0;
        for (size_t i = 0; i < Children_.size(); i++) {
            // Calculate how many inner columns this child uses
            std::vector<IColumnDataExtractor*> innerPackers;
            Children_[i]->AppendInnerExtractors(innerPackers);
            size_t innerCount = innerPackers.size();

            items[i] = Children_[i]->CreateFromUnpack(
                columnsData + offset, 
                columnsNullBitmap + offset, 
                tupleIndex, 
                holderFactory);

            offset += innerCount;
        }

        return result;
    }

    ui32 GetElementSize() override {
        THROW yexception() << "Do not call GetElementSize on tuples";
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        THROW yexception() << "Do not call GetElementSizeType on tuples";
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& packers) override {
        for (auto& child: Children_) {
            child->AppendInnerExtractors(packers);
        }
    }

protected:
    std::vector<IColumnDataExtractor::TPtr> Children_;
    TType* Type_;
};

template<typename TDate, bool Nullable>
class TTzDateColumnDataExtractor : public TTupleColumnDataExtractor<Nullable> {
    using TBase = TTupleColumnDataExtractor<Nullable>;
    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

private:
    static std::vector<IColumnDataExtractor::TPtr> MakeChildren(TType* type) {
        std::vector<IColumnDataExtractor::TPtr> children;
        children.push_back(std::make_unique<TFixedSizeColumnDataExtractor<TDateLayout, false>>(type));
        children.push_back(std::make_unique<TFixedSizeColumnDataExtractor<ui16, false>>(type));
        return children;
    }

public:
    TTzDateColumnDataExtractor(TType* type) 
        : TTupleColumnDataExtractor<Nullable>(MakeChildren(type), type)
    {}
};

class TExternalOptionalColumnDataExtractor : public IColumnDataExtractor {
public:
    TExternalOptionalColumnDataExtractor(IColumnDataExtractor::TPtr inner, [[maybe_unused]] TType* type)
        : Inner_(std::move(inner))
    {}

    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        Inner_->ExtractForPackBatch(values, count, stride, columnsData, columnsNullBitmap, tempStorage);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, [[maybe_unused]] const THolderFactory& holderFactory) override {
        return Inner_->CreateFromUnpack(columnsData, columnsNullBitmap, tupleIndex, holderFactory);
    }

    ui32 GetElementSize() override {
        return Inner_->GetElementSize();
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return Inner_->GetElementSizeType();
    }

    bool IsFloatingPoint() override {
        return Inner_->IsFloatingPoint();
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& packers) override {
        packers.push_back(this);
    }

private:
    IColumnDataExtractor::TPtr Inner_;
};

class TVariantColumnDataExtractor : public IColumnDataExtractor {
public:
    TVariantColumnDataExtractor(std::vector<IColumnDataExtractor::TPtr> children, TType* type)
    {
        Y_UNUSED(children, type);
    }

    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, ui32 stride, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        Y_UNUSED(values, count, stride, columnsData, columnsNullBitmap, tempStorage);
        NotImplemented();
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) override {
        Y_UNUSED(columnsData, columnsNullBitmap, tupleIndex, holderFactory);
        NotImplemented();
    }

    ui32 GetElementSize() override {
        NotImplemented();
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        NotImplemented();
    }

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) override {
        Y_UNUSED(extractors);
        NotImplemented();
    }

private:
    [[noreturn]] void NotImplemented() const {
        THROW yexception() << "TVariantColumnDataExtractor: Variant type is not supported in scalar layout converter";
    }
};

// ------------------------------------------------------------

struct TColumnDataExtractorTraits {
    using TResult = IColumnDataExtractor;
    using TVariant = TVariantColumnDataExtractor;
    template <bool Nullable>
    using TTuple = TTupleColumnDataExtractor<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeColumnDataExtractor<T, Nullable>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot Slot>
    using TStrings = TStringColumnDataExtractor<Nullable>;
    using TExtOptional = TExternalOptionalColumnDataExtractor;
    template<bool Nullable>
    using TResource = TResourceColumnDataExtractor<Nullable>;
    template<typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateColumnDataExtractor<TTzDate, Nullable>;
    template <bool IsNull>
    using TSingular = TSingularColumnDataExtractor<IsNull>;

    constexpr static bool PassType = false;

    static TResult::TPtr MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder, TType* type) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>(type);
        } else {
            return std::make_unique<TStringColumnDataExtractor<true>>(type);
        }
    }

    template <bool IsNull>
    static TResult::TPtr MakeSingular(TType* type) {
        return std::make_unique<TSingular<IsNull>>(type);
    }

    static TResult::TPtr MakeResource(bool isOptional, TType* type) {
        if (isOptional) {
            return std::make_unique<TResource<true>>(type);
        } else {
            return std::make_unique<TResource<false>>(type);
        }
    }

    template<typename TTzDate>
    static TResult::TPtr MakeTzDate(bool isOptional, TType* type) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>(type);
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>(type);
        }
    }
};

// ------------------------------------------------------------

class TScalarLayoutConverter : public IScalarLayoutConverter {
public:
    TScalarLayoutConverter(
        TVector<IColumnDataExtractor::TPtr>&& packers,
        const TVector<NPackedTuple::EColumnRole>& roles,
        const THolderFactory& holderFactory
    )
        : Extractors_(std::move(packers))
        , InnerMapping_(Extractors_.size())
        , HolderFactory_(holderFactory)
    {
        Y_ENSURE(roles.size() == Extractors_.size());

        ui32 colCounter = 0;
        TVector<NPackedTuple::TColumnDesc> columnDescrs;
        for (size_t i = 0; i < Extractors_.size(); ++i) {
            auto& extractor = Extractors_[i];
            auto& mapping = InnerMapping_[i];
            auto prevSize = InnerExtractors_.size();
            extractor->AppendInnerExtractors(InnerExtractors_);

            // Each inner extractor corresponds to one column description
            // InnerMapping tracks which InnerExtractors belong to this top-level extractor
            for (size_t j = 0; j < InnerExtractors_.size() - prevSize; ++j) {
                NPackedTuple::TColumnDesc descr;
                descr.Role = roles[i];
                columnDescrs.push_back(descr);
                mapping.push_back(colCounter);
                colCounter++;
            }
        }

        for (size_t i = 0; i < columnDescrs.size(); ++i) {
            auto& descr = columnDescrs[i];
            auto* packer = InnerExtractors_[i];
            descr.DataSize = packer->GetElementSize();
            descr.SizeType = packer->GetElementSizeType();
            descr.FloatingPoint = packer->IsFloatingPoint();
        }

        TupleLayout_ = NPackedTuple::TTupleLayout::Create(columnDescrs);
        
        // Calculate exact number of pointers needed based on column types
        MaxPointersNeeded_ = 0;
        for (auto* innerExtractor : InnerExtractors_) {
            if (innerExtractor->GetElementSizeType() == NPackedTuple::EColumnSizeType::Variable) {
                MaxPointersNeeded_ += 2;  // String: offset + data
            } else {
                MaxPointersNeeded_ += 1;  // Fixed-size: data only
            }
        }
        
        // Reserve memory once to avoid reallocations in hot path
        ReusableColumnsData_.reserve(MaxPointersNeeded_);
        ReusableColumnsNullBitmap_.reserve(MaxPointersNeeded_);
        // For tempStorage we need approximately 3 buffers per column
        // (data + bitmap + optional offset/string data)
        ReusableTempStorage_.reserve(InnerExtractors_.size() * 3);
    }

    void Pack(const NYql::NUdf::TUnboxedValue* values, TPackResult& packed) override {
        PackBatch(values, 1, packed);
    }

    void PackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 numTuples, TPackResult& packed) override {
        if (numTuples == 0) {
            return;
        }

        FillBatchColumns(values, numTuples);

        auto& packedTuples = packed.PackedTuples;
        auto& overflow = packed.Overflow;
        auto& nTuples = packed.NTuples;

        const auto currentSize = TupleLayout_->TotalRowSize * nTuples;
        nTuples += numTuples;
        packedTuples.resize(TupleLayout_->TotalRowSize * nTuples, 0);

        TupleLayout_->Pack(
            ReusableColumnsData_.data(), ReusableColumnsNullBitmap_.data(),
            packedTuples.data() + currentSize, overflow, 0, numTuples);
    }

    void BucketPack(const NYql::NUdf::TUnboxedValue* values, ui32 numTuples, TPaddedPtr<TPackResult> packs, ui32 bucketsLogNum) override {
        if (numTuples == 0) {
            return;
        }

        FillBatchColumns(values, numTuples);

        const auto reses = TPaddedPtr(&packs[0].PackedTuples, packs.Step());
        const auto overflows = TPaddedPtr(&packs[0].Overflow, packs.Step());
        TupleLayout_->BucketPack(
            ReusableColumnsData_.data(), ReusableColumnsNullBitmap_.data(),
            reses, overflows, 0, numTuples, bucketsLogNum);

        for (ui32 bucket = 0; bucket < (1u << bucketsLogNum); ++bucket) {
            auto& pack = packs[bucket];
            pack.NTuples = pack.PackedTuples.size() / TupleLayout_->TotalRowSize;
        }
    }

    void Unpack(const TPackResult& packed, ui32 tupleIndex, NYql::NUdf::TUnboxedValue* values) override {
        Y_ENSURE(tupleIndex < static_cast<ui32>(packed.NTuples));
        UnpackImpl(
            packed,
            packed.PackedTuples.data() + tupleIndex * TupleLayout_->TotalRowSize,
            1,
            values);
    }

    void UnpackBatch(const TPackResult& packed, NYql::NUdf::TUnboxedValue* values) override {
        UnpackImpl(packed, packed.PackedTuples.data(), packed.NTuples, values);
    }

    const NPackedTuple::TTupleLayout* GetTupleLayout() const override {
        return TupleLayout_.get();
    }

    void ApplyEqualNulls(const TVector<ui32>& equalNullsJoinKeys) override {
        TVector<ui32> flattened;
        for (ui32 inputColumn : equalNullsJoinKeys) {
            Y_ENSURE(inputColumn < InnerMapping_.size(), "EqualNulls column index is out of range");
            const auto& mapping = InnerMapping_[inputColumn];
            flattened.insert(flattened.end(), mapping.begin(), mapping.end());
        }
        TupleLayout_->ApplyEqualNulls(flattened);
    }

private:
    void FillBatchColumns(const NYql::NUdf::TUnboxedValue* values, ui32 numTuples) {
        ReusableColumnsData_.clear();
        ReusableColumnsNullBitmap_.clear();
        ReusableTempStorage_.clear();

        const ui32 numColumns = Extractors_.size();
        for (ui32 column = 0; column < numColumns; ++column) {
            Extractors_[column]->ExtractForPackBatch(
                values + column, numTuples, numColumns,
                ReusableColumnsData_, ReusableColumnsNullBitmap_, ReusableTempStorage_);
        }
        Y_ENSURE(ReusableColumnsData_.size() == MaxPointersNeeded_,
                 "batch pack produced " << ReusableColumnsData_.size() << " column pointers, expected " << MaxPointersNeeded_);
        Y_ENSURE(ReusableColumnsNullBitmap_.size() == MaxPointersNeeded_,
                 "batch pack produced " << ReusableColumnsNullBitmap_.size() << " null bitmaps, expected " << MaxPointersNeeded_);
    }

    void UnpackImpl(const TPackResult& packed, const ui8* packedData, ui32 count,
                    NYql::NUdf::TUnboxedValue* values) {
        if (Extractors_.empty() || count == 0) {
            return;
        }

        // We need to unpack all tuples to get proper column pointers
        auto& bytesPerColumn = UnpackBytesPerColumn_;
        bytesPerColumn.clear();
        TupleLayout_->CalculateColumnSizes(
            packedData, count, bytesPerColumn);

        // Calculate total pointers needed (InnerExtractors corresponds to ColumnDescs)
        Y_ENSURE(bytesPerColumn.size() == InnerExtractors_.size(),
            "bytesPerColumn size " << bytesPerColumn.size() << " != InnerExtractors size " << InnerExtractors_.size());

        auto& columnsDataStorage = UnpackDataStorage_;
        auto& columnsNullBitmapStorage = UnpackBitmapStorage_;
        auto& columnsData = UnpackColumnsData_;
        auto& columnsNullBitmap = UnpackColumnsNullBitmap_;
        columnsData.clear();
        columnsNullBitmap.clear();
        size_t dataUsed = 0;
        size_t bitmapUsed = 0;
        auto takeBuffer = [](TVector<TVector<ui8>>& storage, size_t& used, size_t size) -> ui8* {
            if (used == storage.size()) {
                storage.emplace_back();
            }
            auto& buf = storage[used++];
            buf.assign(size, 0);
            return buf.data();
        };

        // Create buffers based on InnerExtractors (= ColumnDescs count)
        // Note: strings will create 2 pointers (offset + data)
        for (size_t i = 0; i < InnerExtractors_.size(); ++i) {
            auto* packer = InnerExtractors_[i];

            // For variable-size columns (strings), we need to create offset buffer first
            if (packer->GetElementSizeType() == NPackedTuple::EColumnSizeType::Variable) {
                // Offset buffer: (NTuples + 1) * sizeof(ui32)
                columnsData.push_back(takeBuffer(columnsDataStorage, dataUsed, (count + 1) * sizeof(ui32)));

                // Null bitmap for offset buffer
                ui32 bitmapBytes = (count + 7) / 8;
                columnsNullBitmap.push_back(takeBuffer(columnsNullBitmapStorage, bitmapUsed, bitmapBytes));

                // Data buffer
                columnsData.push_back(takeBuffer(columnsDataStorage, dataUsed, bytesPerColumn[i]));

                // Null bitmap for data buffer (dummy)
                columnsNullBitmap.push_back(takeBuffer(columnsNullBitmapStorage, bitmapUsed, 1));
            } else {
                // For fixed-size columns, just allocate data buffer
                columnsData.push_back(takeBuffer(columnsDataStorage, dataUsed, bytesPerColumn[i]));

                // Allocate bitmap storage
                ui32 bitmapBytes = (count + 7) / 8;
                columnsNullBitmap.push_back(takeBuffer(columnsNullBitmapStorage, bitmapUsed, bitmapBytes));
            }
        }

        TupleLayout_->Unpack(
            columnsData.data(), columnsNullBitmap.data(),
            packedData, packed.Overflow, 0, count);

        if (PointerOffsets_.size() != Extractors_.size()) {
            PointerOffsets_.assign(Extractors_.size(), 0);
            for (size_t i = 0; i < Extractors_.size(); ++i) {
                for (size_t j = 0; j < InnerMapping_[i].front(); ++j) {
                    PointerOffsets_[i] += InnerExtractors_[j]->GetElementSizeType() == NPackedTuple::EColumnSizeType::Variable ? 2 : 1;
                }
            }
        }

        for (size_t i = 0; i < Extractors_.size(); ++i) {
            Extractors_[i]->CreateColumnFromUnpack(
                columnsData.data() + PointerOffsets_[i],
                columnsNullBitmap.data() + PointerOffsets_[i],
                count,
                HolderFactory_,
                values + i,
                Extractors_.size());
        }
    }

    std::vector<ui64, TMKQLAllocator<ui64>> UnpackBytesPerColumn_;
    TVector<TVector<ui8>> UnpackDataStorage_;
    TVector<TVector<ui8>> UnpackBitmapStorage_;
    TVector<ui8*> UnpackColumnsData_;
    TVector<ui8*> UnpackColumnsNullBitmap_;

    TVector<IColumnDataExtractor::TPtr> Extractors_;
    std::vector<IColumnDataExtractor*> InnerExtractors_;
    TVector<TVector<ui32>> InnerMapping_;
    TVector<size_t> PointerOffsets_;
    THolder<NPackedTuple::TTupleLayout> TupleLayout_;
    const THolderFactory& HolderFactory_;
    
    // Reusable buffers to avoid allocations in hot path
    TVector<const ui8*> ReusableColumnsData_;
    TVector<const ui8*> ReusableColumnsNullBitmap_;
    TVector<TVector<ui8>> ReusableTempStorage_;
    size_t MaxPointersNeeded_;
};

} // anonymous namespace

// ------------------------------------------------------------

IScalarLayoutConverter::TPtr MakeScalarLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles, const THolderFactory& holderFactory)
{
    TVector<IColumnDataExtractor::TPtr> packers;

    for (auto type: types) {
        packers.emplace_back(DispatchByArrowTraits<TColumnDataExtractorTraits>(typeInfoHelper, type, nullptr, type));
    }

    return std::make_unique<TScalarLayoutConverter>(std::move(packers), roles, holderFactory);
}

} // namespace NKikimr::NMiniKQL
