#include "scalar_layout_converter.h"

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

#include <util/generic/vector.h>

namespace NKikimr::NMiniKQL {

struct IColumnDataPacker {
    using TPtr = std::unique_ptr<IColumnDataPacker>;

    virtual ~IColumnDataPacker() = default;

    // Extract data from UnboxedValue for packing
    virtual void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) = 0;
    
    // Create UnboxedValue from unpacked data
    virtual NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) = 0;
    
    virtual ui32 GetElementSize() = 0;
    virtual NPackedTuple::EColumnSizeType GetElementSizeType() = 0;
    
    virtual void AppendInnerPackers(std::vector<IColumnDataPacker*>& packers) = 0;
};

// ------------------------------------------------------------

template <typename TLayout, bool Nullable>
class TFixedSizeColumnDataPacker : public IColumnDataPacker {
public:
    TFixedSizeColumnDataPacker(TType* type)
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

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) override {
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

    void AppendInnerPackers(std::vector<IColumnDataPacker*>& packers) override {
        packers.push_back(this);
    }

protected:
    TType* Type_;
};

template <bool Nullable>
class TResourceColumnDataPacker : public IColumnDataPacker {
public:
    TResourceColumnDataPacker(TType* type)
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

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) override {
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

    void AppendInnerPackers(std::vector<IColumnDataPacker*>& packers) override {
        packers.push_back(this);
    }

protected:
    TType* Type_;
};

class TSingularColumnDataPacker : public IColumnDataPacker {
public:
    TSingularColumnDataPacker(TType* type) {
        Y_UNUSED(type);
    }

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        Y_UNUSED(value);
        auto& dataStorage = tempStorage.emplace_back(1);
        dataStorage[0] = 0;
        
        columnsData.push_back(dataStorage.data());
        columnsNullBitmap.push_back(nullptr);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) override {
        Y_UNUSED(columnsData, columnsNullBitmap, tupleIndex, holderFactory);
        return NYql::NUdf::TUnboxedValuePod::Void();
    }

    ui32 GetElementSize() override {
        return 1;
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Fixed;
    }

    void AppendInnerPackers(std::vector<IColumnDataPacker*>& packers) override {
        packers.push_back(this);
    }
};

template <bool Nullable>
class TStringColumnDataPacker : public IColumnDataPacker {
public:
    TStringColumnDataPacker(TType* type)
        : Type_(type)
    {}

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        auto& bitmapStorage = tempStorage.emplace_back(1);
        
        if constexpr (Nullable) {
            if (!value) {
                bitmapStorage[0] = 0;
                // For string, we store empty data
                auto& emptyData = tempStorage.emplace_back(0);
                
                columnsData.push_back(nullptr); // offset
                columnsData.push_back(emptyData.data()); // empty data
                columnsNullBitmap.push_back(bitmapStorage.data());
                columnsNullBitmap.push_back(nullptr);
                return;
            }
        }
        
        bitmapStorage[0] = 1;
        auto ref = value.AsStringRef();
        
        // Copy string data to temp storage to keep it alive
        auto& stringData = tempStorage.emplace_back(ref.Size());
        std::memcpy(stringData.data(), ref.Data(), ref.Size());
        
        columnsData.push_back(nullptr); // offset will be calculated during pack
        columnsData.push_back(stringData.data());
        columnsNullBitmap.push_back(bitmapStorage.data());
        columnsNullBitmap.push_back(nullptr);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) override {
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
        
        return holderFactory.CreateString(TStringBuf(reinterpret_cast<const char*>(data + start), size));
    }

    ui32 GetElementSize() override {
        return 16; // threshold for variable sized types
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return NPackedTuple::EColumnSizeType::Variable;
    }

    void AppendInnerPackers(std::vector<IColumnDataPacker*>& packers) override {
        packers.push_back(this);
    }

protected:
    TType* Type_;
};

template <bool Nullable>
class TTupleColumnDataPacker : public IColumnDataPacker {
public:
    TTupleColumnDataPacker(std::vector<IColumnDataPacker::TPtr> children, TType* type)
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

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) override {
        NYql::NUdf::TUnboxedValue* items = nullptr;
        auto result = holderFactory.CreateDirectArrayHolder(Children_.size(), items);
        
        size_t offset = 0;
        for (size_t i = 0; i < Children_.size(); i++) {
            // Calculate how many inner columns this child uses
            std::vector<IColumnDataPacker*> innerPackers;
            Children_[i]->AppendInnerPackers(innerPackers);
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

    void AppendInnerPackers(std::vector<IColumnDataPacker*>& packers) override {
        for (auto& child: Children_) {
            child->AppendInnerPackers(packers);
        }
    }

protected:
    std::vector<IColumnDataPacker::TPtr> Children_;
    TType* Type_;
};

template<typename TDate, bool Nullable>
class TTzDateColumnDataPacker : public TTupleColumnDataPacker<Nullable> {
    using TBase = TTupleColumnDataPacker<Nullable>;
    using TDateLayout = typename NUdf::TDataType<TDate>::TLayout;

public:
    TTzDateColumnDataPacker(TType* type) {
        this->Type_ = type;
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataPacker<TDateLayout, false>>(type));
        this->Children_.push_back(std::make_unique<TFixedSizeColumnDataPacker<ui16, false>>(type));
    }
};

class TExternalOptionalColumnDataPacker : public IColumnDataPacker {
public:
    TExternalOptionalColumnDataPacker(IColumnDataPacker::TPtr inner, TType* type)
        : Inner_(std::move(inner))
        , Type_(type)
    {}

    void ExtractForPack(const NYql::NUdf::TUnboxedValue& value, TVector<const ui8*>& columnsData, TVector<const ui8*>& columnsNullBitmap, TVector<TVector<ui8>>& tempStorage) override {
        Inner_->ExtractForPack(value, columnsData, columnsNullBitmap, tempStorage);
    }

    NYql::NUdf::TUnboxedValue CreateFromUnpack(ui8** columnsData, ui8** columnsNullBitmap, ui32 tupleIndex, const THolderFactory& holderFactory) override {
        return Inner_->CreateFromUnpack(columnsData, columnsNullBitmap, tupleIndex, holderFactory);
    }

    ui32 GetElementSize() override {
        return Inner_->GetElementSize();
    }

    NPackedTuple::EColumnSizeType GetElementSizeType() override {
        return Inner_->GetElementSizeType();
    }

    void AppendInnerPackers(std::vector<IColumnDataPacker*>& packers) override {
        packers.push_back(this);
    }

private:
    IColumnDataPacker::TPtr Inner_;
    TType* Type_;
};

// ------------------------------------------------------------

struct TColumnDataPackerTraits {
    using TResult = IColumnDataPacker;
    template <bool Nullable>
    using TTuple = TTupleColumnDataPacker<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeColumnDataPacker<T, Nullable>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot>
    using TStrings = TStringColumnDataPacker<Nullable>;
    using TExtOptional = TExternalOptionalColumnDataPacker;
    template<bool Nullable>
    using TResource = TResourceColumnDataPacker<Nullable>;
    template<typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateColumnDataPacker<TTzDate, Nullable>;
    using TSingular = TSingularColumnDataPacker;

    constexpr static bool PassType = false;

    static TResult::TPtr MakePg(const NUdf::TPgTypeDescription& desc, const NUdf::IPgBuilder* pgBuilder, TType* type) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>(type);
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true, NKikimr::NUdf::EDataSlot::String>>(type);
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
        TVector<IColumnDataPacker::TPtr>&& packers,
        const TVector<NPackedTuple::EColumnRole>& roles
    )
        : Packers_(std::move(packers))
        , InnerMapping_(Packers_.size())
    {
        Y_ENSURE(roles.size() == Packers_.size());

        ui32 colCounter = 0;
        TVector<NPackedTuple::TColumnDesc> columnDescrs;
        for (size_t i = 0; i < Packers_.size(); ++i) {
            auto& packer = Packers_[i];
            auto& mapping = InnerMapping_[i];
            auto prevSize = InnerPackers_.size();
            packer->AppendInnerPackers(InnerPackers_);

            for (size_t j = 0; j < InnerPackers_.size() - prevSize; ++j) {
                NPackedTuple::TColumnDesc descr;
                descr.Role = roles[i];
                columnDescrs.push_back(descr);
                mapping.push_back(colCounter);
                colCounter++;
            }
        }

        for (size_t i = 0; i < columnDescrs.size(); ++i) {
            auto& descr = columnDescrs[i];
            descr.DataSize = InnerPackers_[i]->GetElementSize();
            descr.SizeType = InnerPackers_[i]->GetElementSizeType();
        }

        TupleLayout_ = NPackedTuple::TTupleLayout::Create(columnDescrs);
    }

    void Pack(const NYql::NUdf::TUnboxedValue* values, TPackResult& packed) override {
        TVector<const ui8*> columnsData;
        TVector<const ui8*> columnsNullBitmap;
        TVector<TVector<ui8>> tempStorage;
        
        for (size_t i = 0; i < Packers_.size(); ++i) {
            Packers_[i]->ExtractForPack(values[i], columnsData, columnsNullBitmap, tempStorage);
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

    void Unpack(const TPackResult& packed, ui32 tupleIndex, NYql::NUdf::TUnboxedValue* values, const THolderFactory& holderFactory) override {
        Y_ENSURE(tupleIndex < static_cast<ui32>(packed.NTuples));
        
        // We need to unpack all tuples to get proper column pointers
        std::vector<ui64, TMKQLAllocator<ui64>> bytesPerColumn;
        TupleLayout_->CalculateColumnSizes(
            packed.PackedTuples.data(), packed.NTuples, bytesPerColumn);

        TVector<TVector<ui8>> columnsDataStorage;
        TVector<TVector<ui8>> columnsNullBitmapStorage;
        
        TVector<ui8*> columnsData;
        TVector<ui8*> columnsNullBitmap;
        
        for (size_t i = 0; i < InnerPackers_.size(); ++i) {
            columnsDataStorage.emplace_back(bytesPerColumn[i]);
            columnsData.push_back(columnsDataStorage.back().data());
            
            // Allocate bitmap storage
            ui32 bitmapBytes = (packed.NTuples + 7) / 8;
            columnsNullBitmapStorage.emplace_back(bitmapBytes);
            columnsNullBitmap.push_back(columnsNullBitmapStorage.back().data());
        }

        TupleLayout_->Unpack(
            columnsData.data(), columnsNullBitmap.data(),
            packed.PackedTuples.data(), packed.Overflow, 0, packed.NTuples);
        
        // Now extract the specific tuple
        for (size_t i = 0; i < Packers_.size(); ++i) {
            const auto& mapping = InnerMapping_[i];
            size_t offset = mapping.front();
            
            values[i] = Packers_[i]->CreateFromUnpack(
                columnsData.data() + offset,
                columnsNullBitmap.data() + offset,
                tupleIndex,
                holderFactory);
        }
    }

    const NPackedTuple::TTupleLayout* GetTupleLayout() const override {
        return TupleLayout_.get();
    }

private:
    TVector<IColumnDataPacker::TPtr> Packers_;
    std::vector<IColumnDataPacker*> InnerPackers_;
    TVector<TVector<ui32>> InnerMapping_;
    THolder<NPackedTuple::TTupleLayout> TupleLayout_;
};

// ------------------------------------------------------------

IScalarLayoutConverter::TPtr MakeScalarLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles)
{
    TVector<IColumnDataPacker::TPtr> packers;

    for (auto type: types) {
        packers.emplace_back(DispatchByArrowTraits<TColumnDataPackerTraits>(typeInfoHelper, type, nullptr, type));
    }

    return std::make_unique<TScalarLayoutConverter>(std::move(packers), roles);
}

} // namespace NKikimr::NMiniKQL

