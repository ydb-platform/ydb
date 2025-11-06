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
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/vector.h>
#include <util/stream/str.h>

namespace NKikimr::NMiniKQL {

namespace {

struct IColumnDataExtractor {
    using TPtr = std::unique_ptr<IColumnDataExtractor>;

    virtual ~IColumnDataExtractor() = default;

    virtual void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) = 0;
    
    // Batch version to reduce virtual call overhead - non-virtual wrapper
    void ExtractForPackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 count, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) {
        ExtractForPackBatchImpl(values, count, columnsData, columnsNullBitmap, tempStorage);
    }
    
    virtual NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) = 0;
    
    virtual ui32 GetElementSize() = 0;
    virtual NPackedTuple::EColumnSizeType GetElementSizeType() = 0;
    // Ugly interface, but I dont care
    virtual void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& extractors) = 0;

protected:
    // Virtual implementation that can be overridden
    virtual void ExtractForPackBatchImpl(const NYql::NUdf::TUnboxedValue* values, ui32 count, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) {
        // Default implementation: fallback to single-value calls
        for (ui32 i = 0; i < count; ++i) {
            ExtractForPack(values[i], columnsData, columnsNullBitmap, tempStorage);
        }
    }
};

// ------------------------------------------------------------

template <typename TLayout, bool Nullable>
class TFixedSizeColumnDataExtractor : public IColumnDataExtractor {
public:
    TFixedSizeColumnDataExtractor(TType* type)
        : Type_(type)
    {}

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        auto& dataStorage = tempStorage.emplace_back(sizeof(TLayout));
        auto& bitmapStorage = tempStorage.emplace_back(1);

        if constexpr (Nullable) {
            if (!value) {
                bitmapStorage[0] = 0; // null
                std::memset(dataStorage.data(), 0, sizeof(TLayout));
            } else {
                bitmapStorage[0] = 1; // not null
                *reinterpret_cast<TLayout*>(dataStorage.data()) = value.Get<TLayout>();
            }
        } else {
            bitmapStorage[0] = 1;
            *reinterpret_cast<TLayout*>(dataStorage.data()) = value.Get<TLayout>();
        }

        columnsData.push_back(dataStorage.data());
        columnsNullBitmap.push_back(bitmapStorage.data());
    }

protected:
    void ExtractForPackBatchImpl(const NYql::NUdf::TUnboxedValue* values, ui32 count, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        if (count == 0) return;
        
        // Allocate storage for all values at once
        auto& dataStorage = tempStorage.emplace_back(sizeof(TLayout) * count);
        auto& bitmapStorage = tempStorage.emplace_back(count);
        
        TLayout* dataPtr = reinterpret_cast<TLayout*>(dataStorage.data());
        ui8* bitmapPtr = bitmapStorage.data();
        
        if constexpr (Nullable) {
            for (ui32 i = 0; i < count; ++i) {
                if (!values[i]) {
                    bitmapPtr[i] = 0; // null
                    std::memset(&dataPtr[i], 0, sizeof(TLayout));
                } else {
                    bitmapPtr[i] = 1; // not null
                    dataPtr[i] = values[i].Get<TLayout>();
                }
                columnsData.push_back(reinterpret_cast<const ui8*>(&dataPtr[i]));
                columnsNullBitmap.push_back(&bitmapPtr[i]);
            }
        } else {
            for (ui32 i = 0; i < count; ++i) {
                bitmapPtr[i] = 1;
                dataPtr[i] = values[i].Get<TLayout>();
                columnsData.push_back(reinterpret_cast<const ui8*>(&dataPtr[i]));
                columnsNullBitmap.push_back(&bitmapPtr[i]);
            }
        }
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

        TLayout* data = reinterpret_cast<TLayout*>(columnsData[0]) + tupleIndex;
        return NYql::NUdf::TUnboxedValuePod(*data);
    }

    ui32 GetElementSize() override {
        return sizeof(TLayout);
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

template <bool Nullable>
class TResourceColumnDataExtractor : public IColumnDataExtractor {
public:
    TResourceColumnDataExtractor(TType* type)
        : Type_(type)
    {}

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        auto& dataStorage = tempStorage.emplace_back(sizeof(NYql::NUdf::TUnboxedValue));
        auto& bitmapStorage = tempStorage.emplace_back(1);

        if constexpr (Nullable) {
            if (!value) {
                bitmapStorage[0] = 0;
                std::memset(dataStorage.data(), 0, sizeof(NYql::NUdf::TUnboxedValue));
            } else {
                bitmapStorage[0] = 1;
                new (dataStorage.data()) NYql::NUdf::TUnboxedValue(value);
            }
        } else {
            bitmapStorage[0] = 1;
            new (dataStorage.data()) NYql::NUdf::TUnboxedValue(value);
        }

        columnsData.push_back(dataStorage.data());
        columnsNullBitmap.push_back(bitmapStorage.data());
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

class TSingularColumnDataExtractor : public IColumnDataExtractor {
public:
    TSingularColumnDataExtractor(TType* type) {
        Y_UNUSED(type);
    }

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        Y_UNUSED(value);
        auto& dataStorage = tempStorage.emplace_back(1);
        dataStorage[0] = 0;

        columnsData.push_back(dataStorage.data());
        columnsNullBitmap.push_back(nullptr);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, [[maybe_unused]] const THolderFactory& holderFactory) override {
        Y_UNUSED(columnsData, columnsNullBitmap, tupleIndex, holderFactory);
        return NYql::NUdf::TUnboxedValuePod::Void();
    }

    ui32 GetElementSize() override {
        return 1;
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

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        auto& bitmapStorage = tempStorage.emplace_back(1);

        // Create offset buffer for variable-size column (2 offsets: start=0, end=size)
        auto& offsetStorage = tempStorage.emplace_back(2 * sizeof(ui32));
        ui32* offsets = reinterpret_cast<ui32*>(offsetStorage.data());

        if constexpr (Nullable) {
            if (!value) {
                bitmapStorage[0] = 0;
                offsets[0] = 0;
                offsets[1] = 0; // empty string

                // For null strings, we still need a valid pointer (not nullptr)
                // Create 1-byte buffer to avoid nullptr
                auto& emptyData = tempStorage.emplace_back(1);
                emptyData[0] = 0;

                columnsData.push_back(offsetStorage.data());
                columnsData.push_back(emptyData.data());
                columnsNullBitmap.push_back(bitmapStorage.data());
                columnsNullBitmap.push_back(nullptr);
                return;
            }
        }

        bitmapStorage[0] = 1;
        auto ref = value.AsStringRef();
        ui32 size = ref.Size();

        // Set offsets: [0, size]
        offsets[0] = 0;
        offsets[1] = size;

        // Copy string data to temp storage to keep it alive
        // Always allocate at least 1 byte to avoid nullptr from data()
        auto& stringData = tempStorage.emplace_back(size > 0 ? size : 1);
        if (size > 0) {
            std::memcpy(stringData.data(), ref.Data(), size);
        } else {
            stringData[0] = 0; // Just to have valid data
        }

        columnsData.push_back(offsetStorage.data());
        columnsData.push_back(stringData.data());
        columnsNullBitmap.push_back(bitmapStorage.data());
        columnsNullBitmap.push_back(nullptr);
    }

protected:
    void ExtractForPackBatchImpl(const NYql::NUdf::TUnboxedValue* values, ui32 count, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        if (count == 0) return;
        
        // First pass: calculate total string data size
        ui32 totalSize = 0;
        for (ui32 i = 0; i < count; ++i) {
            if constexpr (Nullable) {
                if (values[i]) {
                    totalSize += values[i].AsStringRef().Size();
                }
            } else {
                totalSize += values[i].AsStringRef().Size();
            }
        }
        
        // Allocate storage for all strings at once
        auto& bitmapStorage = tempStorage.emplace_back(count);
        auto& offsetStorage = tempStorage.emplace_back((count + 1) * sizeof(ui32));
        auto& stringDataStorage = tempStorage.emplace_back(totalSize > 0 ? totalSize : 1);
        
        ui8* bitmapPtr = bitmapStorage.data();
        ui32* offsets = reinterpret_cast<ui32*>(offsetStorage.data());
        ui8* stringDataPtr = stringDataStorage.data();
        
        ui32 currentOffset = 0;
        offsets[0] = 0;
        
        for (ui32 i = 0; i < count; ++i) {
            if constexpr (Nullable) {
                if (!values[i]) {
                    bitmapPtr[i] = 0;
                    offsets[i + 1] = currentOffset; // empty string
                } else {
                    bitmapPtr[i] = 1;
                    auto ref = values[i].AsStringRef();
                    ui32 size = ref.Size();
                    if (size > 0) {
                        std::memcpy(stringDataPtr + currentOffset, ref.Data(), size);
                        currentOffset += size;
                    }
                    offsets[i + 1] = currentOffset;
                }
            } else {
                bitmapPtr[i] = 1;
                auto ref = values[i].AsStringRef();
                ui32 size = ref.Size();
                if (size > 0) {
                    std::memcpy(stringDataPtr + currentOffset, ref.Data(), size);
                    currentOffset += size;
                }
                offsets[i + 1] = currentOffset;
            }
            
            // Create individual offset buffers for each value
            auto& individualOffsetStorage = tempStorage.emplace_back(2 * sizeof(ui32));
            ui32* individualOffsets = reinterpret_cast<ui32*>(individualOffsetStorage.data());
            individualOffsets[0] = offsets[i];
            individualOffsets[1] = offsets[i + 1];
            
            columnsData.push_back(individualOffsetStorage.data());
            columnsData.push_back(stringDataPtr);
            columnsNullBitmap.push_back(&bitmapPtr[i]);
            columnsNullBitmap.push_back(nullptr);
        }
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

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        if constexpr (Nullable) {
            if (!value) {
                // For null tuple, we need to extract nulls for all children
                for (auto& child : Children_) {
                    child->ExtractForPack(NYql::NUdf::TUnboxedValuePod(), columnsData, columnsNullBitmap, tempStorage);
                }
                return;
            }
        }

        for (size_t i = 0; i < Children_.size(); i++) {
            auto element = value.GetElement(i);
            Children_[i]->ExtractForPack(element, columnsData, columnsNullBitmap, tempStorage);
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

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        Inner_->ExtractForPack(value, columnsData, columnsNullBitmap, tempStorage);
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

    void AppendInnerExtractors(std::vector<IColumnDataExtractor*>& packers) override {
        packers.push_back(this);
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
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot Slot>
    using TStrings = TStringColumnDataExtractor<Nullable>;
    using TExtOptional = TExternalOptionalColumnDataExtractor;
    template<bool Nullable>
    using TResource = TResourceColumnDataExtractor<Nullable>;
    template<typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateColumnDataExtractor<TTzDate, Nullable>;
    using TSingular = TSingularColumnDataExtractor;

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
        Y_UNUSED(IsNull);
        return std::make_unique<TSingular>(type);
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
        const TVector<NPackedTuple::EColumnRole>& roles
    )
        : Extractors_(std::move(packers))
        , InnerMapping_(Extractors_.size())
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
        }

        TupleLayout_ = NPackedTuple::TTupleLayout::Create(columnDescrs);
    }

    void Pack(const NYql::NUdf::TUnboxedValue* values, TPackResult& packed) override {
        // Static dummy buffer to use instead of nullptr
        static ui8 DummyBuffer[8] = {0};

        TVector<const ui8*> columnsData;
        TVector<const ui8*> columnsNullBitmap;
        TVector<TVector<ui8>> tempStorage;

        // Reserve space to avoid reallocation which could invalidate pointers
        columnsData.reserve(InnerExtractors_.size() * 2);
        columnsNullBitmap.reserve(InnerExtractors_.size() * 2);
        tempStorage.reserve(InnerExtractors_.size() * 4); // Estimate

        for (size_t i = 0; i < Extractors_.size(); ++i) {
            Extractors_[i]->ExtractForPack(values[i], columnsData, columnsNullBitmap, tempStorage);
        }

        // Replace any nullptr with dummy buffer to avoid segfaults
        for (size_t i = 0; i < columnsData.size(); ++i) {
            if (columnsData[i] == nullptr) {
                columnsData[i] = DummyBuffer;
            }
        }
        for (size_t i = 0; i < columnsNullBitmap.size(); ++i) {
            if (columnsNullBitmap[i] == nullptr) {
                columnsNullBitmap[i] = DummyBuffer;
            }
        }

        auto& packedTuples = packed.PackedTuples;
        auto& overflow = packed.Overflow;
        auto& nTuples = packed.NTuples;

        auto currentSize = (TupleLayout_->TotalRowSize) * nTuples;
        nTuples += 1; // pack one tuple
        auto newSize = (TupleLayout_->TotalRowSize) * nTuples;
        packedTuples.resize(newSize, 0);

        TupleLayout_->Pack(
            columnsData.data(), columnsNullBitmap.data(),
            packedTuples.data() + currentSize, overflow, 0, 1);
    }

    void PackBatch(const NYql::NUdf::TUnboxedValue* values, ui32 numTuples, ui32 numColumns, TPackResult& packed) override {
        Y_ENSURE(numColumns == Extractors_.size(), "Number of columns must match number of extractors");
        
        if (numTuples == 0) return;
        
        // For now, use simple loop over Pack to ensure correctness
        // TODO: Optimize by doing batch extraction and single Pack call
        for (ui32 tupleIdx = 0; tupleIdx < numTuples; ++tupleIdx) {
            Pack(values + tupleIdx * numColumns, packed);
        }
    }

    void Unpack(const TPackResult& packed, ui32 tupleIndex, NYql::NUdf::TUnboxedValue* values, const THolderFactory& holderFactory) override {
        Y_ENSURE(tupleIndex < static_cast<ui32>(packed.NTuples));

        // We need to unpack all tuples to get proper column pointers
        std::vector<ui64, TMKQLAllocator<ui64>> bytesPerColumn;
        TupleLayout_->CalculateColumnSizes(
            packed.PackedTuples.data(), packed.NTuples, bytesPerColumn);

        // Calculate total pointers needed (InnerExtractors corresponds to ColumnDescs)
        Y_ENSURE(bytesPerColumn.size() == InnerExtractors_.size(),
            "bytesPerColumn size " << bytesPerColumn.size() << " != InnerExtractors size " << InnerExtractors_.size());

        TVector<TVector<ui8>> columnsDataStorage;
        TVector<TVector<ui8>> columnsNullBitmapStorage;

        TVector<ui8*> columnsData;
        TVector<ui8*> columnsNullBitmap;

        // Create buffers based on InnerExtractors (= ColumnDescs count)
        // Note: strings will create 2 pointers (offset + data)
        for (size_t i = 0; i < InnerExtractors_.size(); ++i) {
            auto* packer = InnerExtractors_[i];

            // For variable-size columns (strings), we need to create offset buffer first
            if (packer->GetElementSizeType() == NPackedTuple::EColumnSizeType::Variable) {
                // Offset buffer: (NTuples + 1) * sizeof(ui32)
                columnsDataStorage.emplace_back((packed.NTuples + 1) * sizeof(ui32));
                std::memset(columnsDataStorage.back().data(), 0, (packed.NTuples + 1) * sizeof(ui32));
                columnsData.push_back(columnsDataStorage.back().data());

                // Null bitmap for offset buffer
                ui32 bitmapBytes = (packed.NTuples + 7) / 8;
                columnsNullBitmapStorage.emplace_back(bitmapBytes);
                columnsNullBitmap.push_back(columnsNullBitmapStorage.back().data());

                // Data buffer
                columnsDataStorage.emplace_back(bytesPerColumn[i]);
                columnsData.push_back(columnsDataStorage.back().data());

                // Null bitmap for data buffer (dummy)
                columnsNullBitmapStorage.emplace_back(1);
                columnsNullBitmap.push_back(columnsNullBitmapStorage.back().data());
            } else {
                // For fixed-size columns, just allocate data buffer
                columnsDataStorage.emplace_back(bytesPerColumn[i]);
                columnsData.push_back(columnsDataStorage.back().data());

                // Allocate bitmap storage
                ui32 bitmapBytes = (packed.NTuples + 7) / 8;
                columnsNullBitmapStorage.emplace_back(bitmapBytes);
                columnsNullBitmap.push_back(columnsNullBitmapStorage.back().data());
            }
        }

        TupleLayout_->Unpack(
            columnsData.data(), columnsNullBitmap.data(),
            packed.PackedTuples.data(), packed.Overflow, 0, packed.NTuples);

        // Now extract the specific tuple for each extractor
        // We need to calculate pointer offsets based on inner extractors
        for (size_t i = 0; i < Extractors_.size(); ++i) {
            const auto& mapping = InnerMapping_[i];
            
            // Calculate how many pointers we need to pass
            // (strings have 2 pointers per column: offset + data)
            size_t pointerOffset = 0;
            for (size_t j = 0; j < mapping.front(); ++j) {
                auto* innerExtractor = InnerExtractors_[j];
                if (innerExtractor->GetElementSizeType() == NPackedTuple::EColumnSizeType::Variable) {
                    pointerOffset += 2; // offset + data
                } else {
                    pointerOffset += 1;
                }
            }

            values[i] = Extractors_[i]->CreateFromUnpack(
                columnsData.data() + pointerOffset,
                columnsNullBitmap.data() + pointerOffset,
                tupleIndex,
                holderFactory);
        }
    }

    const NPackedTuple::TTupleLayout* GetTupleLayout() const override {
        return TupleLayout_.get();
    }

private:
    TVector<IColumnDataExtractor::TPtr> Extractors_;
    std::vector<IColumnDataExtractor*> InnerExtractors_;
    TVector<TVector<ui32>> InnerMapping_;
    THolder<NPackedTuple::TTupleLayout> TupleLayout_;
};

} // anonymous namespace

// ------------------------------------------------------------

IScalarLayoutConverter::TPtr MakeScalarLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles)
{
    TVector<IColumnDataExtractor::TPtr> packers;

    for (auto type: types) {
        packers.emplace_back(DispatchByArrowTraits<TColumnDataExtractorTraits>(typeInfoHelper, type, nullptr, type));
    }

    return std::make_unique<TScalarLayoutConverter>(std::move(packers), roles);
}

} // namespace NKikimr::NMiniKQL
