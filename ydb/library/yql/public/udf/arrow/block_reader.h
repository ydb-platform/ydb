#pragma once

#include "block_item.h"
#include "block_io_buffer.h"
#include "util.h"
#include <arrow/datum.h>

#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

namespace NYql {
namespace NUdf {

class IBlockReader : private TNonCopyable {
public:
    virtual ~IBlockReader() = default;
    // result will reference to Array/Scalar internals and will be valid until next call to GetItem/GetScalarItem
    virtual TBlockItem GetItem(const arrow::ArrayData& data, size_t index) = 0;
    virtual TBlockItem GetScalarItem(const arrow::Scalar& scalar) = 0;

    virtual ui64 GetDataWeight(const arrow::ArrayData& data) const = 0;
    virtual ui64 GetDataWeight(TBlockItem item) const = 0;
    virtual ui64 GetDefaultValueWeight() const = 0;

    virtual void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const = 0;
    virtual void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const = 0;
};

struct TBlockItemSerializeProps {
    TMaybe<ui32> MaxSize = 0; // maximum size each block item can occupy in TOutputBuffer
                              // (will be undefined for dynamic object like string)
    bool IsFixed = true;      // true if each block item takes fixed size
};

template<typename T, bool Nullable, typename TDerived> 
class TFixedSizeBlockReaderBase : public IBlockReader {
public:
    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return {};
            }
        }
        return static_cast<TDerived*>(this)->MakeBlockItem(data.GetValues<T>(1)[index]);
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        using namespace arrow::internal;

        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return {};
            }
        }

        if constexpr(std::is_same_v<T, NYql::NDecimal::TInt128>) {
            auto& fixedScalar = checked_cast<const arrow::FixedSizeBinaryScalar&>(scalar);
            T value; memcpy((void*)&value, fixedScalar.value->data(), sizeof(T));
            return static_cast<TDerived*>(this)->MakeBlockItem(value);
        } else {
            return static_cast<TDerived*>(this)->MakeBlockItem(
                *static_cast<const T*>(checked_cast<const PrimitiveScalarBase&>(scalar).data())
            );
        }
    }

    ui64 GetDataWeight(const arrow::ArrayData& data) const final {
        if constexpr (Nullable) {
            return (1 + sizeof(T)) * data.length;
        }
        return sizeof(T) * data.length;
    }

    ui64 GetDataWeight(TBlockItem item) const final {
        Y_UNUSED(item);
        return GetDefaultValueWeight();
    }

    ui64 GetDefaultValueWeight() const final {
        if constexpr (Nullable) {
            return 1 + sizeof(T);
        }
        return sizeof(T);
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        out.PushNumber(data.GetValues<T>(1)[index]);
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        if constexpr(std::is_same_v<T, NYql::NDecimal::TInt128>) {
            auto& fixedScalar = arrow::internal::checked_cast<const arrow::FixedSizeBinaryScalar&>(scalar);
            T value; memcpy((void*)&value, fixedScalar.value->data(), sizeof(T));
            out.PushNumber(value);
        } else {
            out.PushNumber(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
        }
    }
};

template<typename T, bool Nullable>
class TFixedSizeBlockReader : public TFixedSizeBlockReaderBase<T, Nullable, TFixedSizeBlockReader<T, Nullable>> {
public:
    TBlockItem MakeBlockItem(const T& item) const {
        return TBlockItem(item);
    }
};

template<bool Nullable>
class TResourceBlockReader : public TFixedSizeBlockReaderBase<TUnboxedValuePod, Nullable, TResourceBlockReader<Nullable>> {
public:
    TBlockItem MakeBlockItem(const TUnboxedValuePod& pod) const {
        TBlockItem item;
        std::memcpy(item.GetRawPtr(), pod.GetRawPtr(), sizeof(TBlockItem));
        return item;
    }
};

template<typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot TOriginal = NKikimr::NUdf::EDataSlot::String>
class TStringBlockReader final : public IBlockReader {
public:
    using TOffset = typename TStringType::offset_type;

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        Y_DEBUG_ABORT_UNLESS(data.buffers.size() == 3);
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return {};
            }
        }

        const TOffset* offsets = data.GetValues<TOffset>(1);
        const char* strData = data.GetValues<char>(2, 0);

        std::string_view str(strData + offsets[index], offsets[index + 1] - offsets[index]);
        return TBlockItem(str);
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return {};
            }
        }

        auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(scalar).value;
        std::string_view str(reinterpret_cast<const char*>(buffer->data()), buffer->size());
        return TBlockItem(str);
    }

    ui64 GetDataWeight(const arrow::ArrayData& data) const final {
        ui64 size = 0;
        if constexpr (Nullable) {
            size += data.length;
        }
        size += data.buffers[2] ? data.buffers[2]->size() : 0;
        return size;
    }

    ui64 GetDataWeight(TBlockItem item) const final {
        if constexpr (Nullable) {
            return 1 + (item ? item.AsStringRef().Size() : 0);
        }
        return item.AsStringRef().Size();
    }

    ui64 GetDefaultValueWeight() const final {
        if constexpr (Nullable) {
            return 1;
        }
        return 0;
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        Y_DEBUG_ABORT_UNLESS(data.buffers.size() == 3);
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        const TOffset* offsets = data.GetValues<TOffset>(1);
        const char* strData = data.GetValues<char>(2, 0);

        std::string_view str(strData + offsets[index], offsets[index + 1] - offsets[index]);
        out.PushString(str);
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(scalar).value;
        std::string_view str(reinterpret_cast<const char*>(buffer->data()), buffer->size());
        out.PushString(str);
    }
};

template<bool Nullable, typename TDerived>
class TTupleBlockReaderBase : public IBlockReader {
public:
    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return {};
            }
        }
        return static_cast<TDerived*>(this)->GetChildrenItems(data, index);
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return {};
            }
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        return static_cast<TDerived*>(this)->GetChildrenScalarItems(structScalar);
    }

    ui64 GetDataWeight(const arrow::ArrayData& data) const final {
        ui64 size = 0;
        if constexpr (Nullable) {
            size += data.length;
        }

        size += static_cast<const TDerived*>(this)->GetChildrenDataWeight(data);
        return size;
    }

    ui64 GetDataWeight(TBlockItem item) const final {
        return static_cast<const TDerived*>(this)->GetDataWeightImpl(item);
    }

    ui64 GetDefaultValueWeight() const final {
        ui64 size = 0;
        if constexpr (Nullable) {
            size = 1;
        }
        size += static_cast<const TDerived*>(this)->GetChildrenDefaultDataWeight();
        return size;
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        static_cast<const TDerived*>(this)->SaveChildrenItems(data, index, out);
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);

        static_cast<const TDerived*>(this)->SaveChildrenScalarItems(structScalar, out);
    }
};

template<bool Nullable>
class TTupleBlockReader final : public TTupleBlockReaderBase<Nullable, TTupleBlockReader<Nullable>> {
public:
    TTupleBlockReader(TVector<std::unique_ptr<IBlockReader>>&& children)
        : Children(std::move(children))
        , Items(Children.size())
    {}

    TBlockItem GetChildrenItems(const arrow::ArrayData& data, size_t index) {
        for (ui32 i = 0; i < Children.size(); ++i) {
            Items[i] = Children[i]->GetItem(*data.child_data[i], index);
        }

        return TBlockItem(Items.data());
    }

    TBlockItem GetChildrenScalarItems(const arrow::StructScalar& structScalar) {
        for (ui32 i = 0; i < Children.size(); ++i) {
            Items[i] = Children[i]->GetScalarItem(*structScalar.value[i]);
        }

        return TBlockItem(Items.data());
    }
    
    size_t GetDataWeightImpl(const TBlockItem& item) const {
        const TBlockItem* items = nullptr;
        ui64 size = 0;
        if constexpr (Nullable) {
            if (!item) {
                return this->GetDefaultValueWeight();
            }
            size = 1;
            items = item.GetOptionalValue().GetElements();
        } else {
            items = item.GetElements();
        }

        for (ui32 i = 0; i < Children.size(); ++i) {
            size += Children[i]->GetDataWeight(items[i]);
        }

        return size;
    }

    size_t GetChildrenDataWeight(const arrow::ArrayData& data) const {
        size_t size = 0;
        for (ui32 i = 0; i < Children.size(); ++i) {
            size += Children[i]->GetDataWeight(*data.child_data[i]);
        }

        return size;
    }

    size_t GetChildrenDefaultDataWeight() const {
        size_t size = 0;
        for (ui32 i = 0; i < Children.size(); ++i) {
            size += Children[i]->GetDefaultValueWeight();
        }
        return size;
    }

    void SaveChildrenItems(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const {
        for (ui32 i = 0; i < Children.size(); ++i) {
            Children[i]->SaveItem(*data.child_data[i], index, out);
        }
    }
    
    void SaveChildrenScalarItems(const arrow::StructScalar& structScalar, TOutputBuffer& out) const {
        for (ui32 i = 0; i < Children.size(); ++i) {
            Children[i]->SaveScalarItem(*structScalar.value[i], out);
        }
    }

private:
    const TVector<std::unique_ptr<IBlockReader>> Children;
    TVector<TBlockItem> Items;
};

template<typename TTzDate, bool Nullable>
class TTzDateBlockReader final : public TTupleBlockReaderBase<Nullable, TTzDateBlockReader<TTzDate, Nullable>> {
public:
    TBlockItem GetChildrenItems(const arrow::ArrayData& data, size_t index) {
        Y_DEBUG_ABORT_UNLESS(data.child_data.size() == 2);

        TBlockItem item {DateReader_.GetItem(*data.child_data[0], index)};
        item.SetTimezoneId(TimezoneReader_.GetItem(*data.child_data[1], index).Get<ui16>());
        return item;
    }

    TBlockItem GetChildrenScalarItems(const arrow::StructScalar& structScalar) {
        Y_DEBUG_ABORT_UNLESS(structScalar.value.size() == 2);

        TBlockItem item {DateReader_.GetScalarItem(*structScalar.value[0])};
        item.SetTimezoneId(TimezoneReader_.GetScalarItem(*structScalar.value[1]).Get<ui16>());
        return item;
    }

    size_t GetChildrenDataWeight(const arrow::ArrayData& data) const {
        Y_DEBUG_ABORT_UNLESS(data.child_data.size() == 2);

        size_t size = 0;
        size += DateReader_.GetDataWeight(*data.child_data[0]);
        size += TimezoneReader_.GetDataWeight(*data.child_data[1]);
        return size;
    }

    size_t GetDataWeightImpl(const TBlockItem& item) const {
        Y_UNUSED(item);
        return GetChildrenDefaultDataWeight();
    }
    
    size_t GetChildrenDefaultDataWeight() const {
        ui64 size = 0;
        if constexpr (Nullable) {
            size = 1;
        }

        size += DateReader_.GetDefaultValueWeight();
        size += TimezoneReader_.GetDefaultValueWeight();
        return size;
    }

    void SaveChildrenItems(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const {
        DateReader_.SaveItem(*data.child_data[0], index, out);
        TimezoneReader_.SaveItem(*data.child_data[1], index, out);
    }
    
    void SaveChildrenScalarItems(const arrow::StructScalar& structScalar, TOutputBuffer& out) const {
        DateReader_.SaveScalarItem(*structScalar.value[0], out);
        TimezoneReader_.SaveScalarItem(*structScalar.value[1], out);
    }

private:
    TFixedSizeBlockReader<typename TDataType<TTzDate>::TLayout, /* Nullable */false> DateReader_;
    TFixedSizeBlockReader<ui16, /* Nullable */false> TimezoneReader_;
};

class TExternalOptionalBlockReader final : public IBlockReader {
public:
    TExternalOptionalBlockReader(std::unique_ptr<IBlockReader>&& inner)
        : Inner(std::move(inner))
    {}

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if (IsNull(data, index)) {
            return {};
        }

        return Inner->GetItem(*data.child_data.front(), index).MakeOptional();
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        return Inner->GetScalarItem(*structScalar.value.front()).MakeOptional();
    }

    ui64 GetDataWeight(const arrow::ArrayData& data) const final {
        return data.length + Inner->GetDataWeight(*data.child_data.front());
    }

    ui64 GetDataWeight(TBlockItem item) const final {
        if (!item) {
            return GetDefaultValueWeight();
        }
        return 1 + Inner->GetDataWeight(item.GetOptionalValue());
    }

    ui64 GetDefaultValueWeight() const final {
        return 1 + Inner->GetDefaultValueWeight();
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        if (IsNull(data, index)) {
            return out.PushChar(0);
        }
        out.PushChar(1);

        Inner->SaveItem(*data.child_data.front(), index, out);
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const final {
        if (!scalar.is_valid) {
            return out.PushChar(0);
        }
        out.PushChar(1);

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        Inner->SaveScalarItem(*structScalar.value.front(), out);
    }

private:
    const std::unique_ptr<IBlockReader> Inner;
};

struct TReaderTraits {
    using TResult = IBlockReader;
    template <bool Nullable>
    using TTuple = TTupleBlockReader<Nullable>;
    template <typename T, bool Nullable>
    using TFixedSize = TFixedSizeBlockReader<T, Nullable>;
    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot TOriginal>
    using TStrings = TStringBlockReader<TStringType, Nullable, TOriginal>;
    using TExtOptional = TExternalOptionalBlockReader;
    template<bool Nullable>
    using TResource = TResourceBlockReader<Nullable>;
    template<typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateBlockReader<TTzDate, Nullable>;

    static std::unique_ptr<TResult> MakePg(const TPgTypeDescription& desc, const IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>();
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true, NKikimr::NUdf::EDataSlot::String>>();
        }
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TResource<true>>();
        } else {
            return std::make_unique<TResource<false>>();
        }
    }

    template<typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>();
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>();
        }
    }
};

template <typename TTraits>
std::unique_ptr<typename TTraits::TResult> MakeTupleBlockReaderImpl(bool isOptional, TVector<std::unique_ptr<typename TTraits::TResult>>&& children) {
    if (isOptional) {
        return std::make_unique<typename TTraits::template TTuple<true>>(std::move(children));
    } else {
        return std::make_unique<typename TTraits::template TTuple<false>>(std::move(children));
    }
}

template <typename TTraits, typename T>
std::unique_ptr<typename TTraits::TResult> MakeFixedSizeBlockReaderImpl(bool isOptional) {
    if (isOptional) {
        return std::make_unique<typename TTraits::template TFixedSize<T, true>>();
    } else {
        return std::make_unique<typename TTraits::template TFixedSize<T, false>>();
    }
}


template <typename TTraits, typename T, NKikimr::NUdf::EDataSlot TOriginal>
std::unique_ptr<typename TTraits::TResult> MakeStringBlockReaderImpl(bool isOptional) {
    if (isOptional) {
        return std::make_unique<typename TTraits::template TStrings<T, true, TOriginal>>();
    } else {
        return std::make_unique<typename TTraits::template TStrings<T, false, TOriginal>>();
    }
}

template<typename T>
concept CanInstantiateBlockReaderForDecimal = requires {
    T::template TFixedSize<NYql::NDecimal::TInt128, true>();
};

template <typename TTraits>
std::unique_ptr<typename TTraits::TResult> MakeBlockReaderImpl(const ITypeInfoHelper& typeInfoHelper, const TType* type, const IPgBuilder* pgBuilder) {
    const TType* unpacked = type;
    TOptionalTypeInspector typeOpt(typeInfoHelper, type);
    bool isOptional = false;
    if (typeOpt) {
        unpacked = typeOpt.GetItemType();
        isOptional = true;
    }

    TOptionalTypeInspector unpackedOpt(typeInfoHelper, unpacked);
    TPgTypeInspector unpackedPg(typeInfoHelper, unpacked);
    if (unpackedOpt || typeOpt && unpackedPg) {
        // at least 2 levels of optionals
        ui32 nestLevel = 0;
        auto currentType = type;
        auto previousType = type;
        for (;;) {
            ++nestLevel;
            previousType = currentType;
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

        auto reader = MakeBlockReaderImpl<TTraits>(typeInfoHelper, previousType, pgBuilder);
        for (ui32 i = 1; i < nestLevel; ++i) {
            reader = std::make_unique<typename TTraits::TExtOptional>(std::move(reader));
        }

        return reader;
    }
    else {
        type = unpacked;
    }

    TStructTypeInspector typeStruct(typeInfoHelper, type);
    if (typeStruct) {
        TVector<std::unique_ptr<typename TTraits::TResult>> members;
        for (ui32 i = 0; i < typeStruct.GetMembersCount(); i++) {
            members.emplace_back(MakeBlockReaderImpl<TTraits>(typeInfoHelper, typeStruct.GetMemberType(i), pgBuilder));
        }
        // XXX: Use Tuple block reader for Struct.
        return MakeTupleBlockReaderImpl<TTraits>(isOptional, std::move(members));
    }

    TTupleTypeInspector typeTuple(typeInfoHelper, type);
    if (typeTuple) {
        TVector<std::unique_ptr<typename TTraits::TResult>> children;
        for (ui32 i = 0; i < typeTuple.GetElementsCount(); ++i) {
            children.emplace_back(MakeBlockReaderImpl<TTraits>(typeInfoHelper, typeTuple.GetElementType(i), pgBuilder));
        }

        return MakeTupleBlockReaderImpl<TTraits>(isOptional, std::move(children));
    }

    TDataTypeInspector typeData(typeInfoHelper, type);
    if (typeData) {
        auto typeId = typeData.GetTypeId();
        switch (GetDataSlot(typeId)) {
        case NUdf::EDataSlot::Int8:
            return MakeFixedSizeBlockReaderImpl<TTraits, i8>(isOptional);
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            return MakeFixedSizeBlockReaderImpl<TTraits, ui8>(isOptional);
        case NUdf::EDataSlot::Int16:
            return MakeFixedSizeBlockReaderImpl<TTraits, i16>(isOptional);
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return MakeFixedSizeBlockReaderImpl<TTraits, ui16>(isOptional);
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Date32:
            return MakeFixedSizeBlockReaderImpl<TTraits, i32>(isOptional);
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return MakeFixedSizeBlockReaderImpl<TTraits, ui32>(isOptional);
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Interval64:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
            return MakeFixedSizeBlockReaderImpl<TTraits, i64>(isOptional);
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return MakeFixedSizeBlockReaderImpl<TTraits, ui64>(isOptional);
        case NUdf::EDataSlot::Float:
            return MakeFixedSizeBlockReaderImpl<TTraits, float>(isOptional);
        case NUdf::EDataSlot::Double:
            return MakeFixedSizeBlockReaderImpl<TTraits, double>(isOptional);
        case NUdf::EDataSlot::String:
            return MakeStringBlockReaderImpl<TTraits, arrow::BinaryType, NUdf::EDataSlot::String>(isOptional);
        case NUdf::EDataSlot::Yson:
            return MakeStringBlockReaderImpl<TTraits, arrow::BinaryType, NUdf::EDataSlot::Yson>(isOptional);
        case NUdf::EDataSlot::JsonDocument:
            return MakeStringBlockReaderImpl<TTraits, arrow::BinaryType, NUdf::EDataSlot::JsonDocument>(isOptional);
        case NUdf::EDataSlot::Utf8:
            return MakeStringBlockReaderImpl<TTraits, arrow::StringType, NUdf::EDataSlot::Utf8>(isOptional);
        case NUdf::EDataSlot::Json:
            return MakeStringBlockReaderImpl<TTraits, arrow::StringType, NUdf::EDataSlot::Json>(isOptional);
        case NUdf::EDataSlot::TzDate:
            return TTraits::template MakeTzDate<TTzDate>(isOptional);
        case NUdf::EDataSlot::TzDatetime:
            return TTraits::template MakeTzDate<TTzDatetime>(isOptional);
        case NUdf::EDataSlot::TzTimestamp:
            return TTraits::template MakeTzDate<TTzTimestamp>(isOptional);
        case NUdf::EDataSlot::TzDate32:
            return TTraits::template MakeTzDate<TTzDate32>(isOptional);
        case NUdf::EDataSlot::TzDatetime64:
            return TTraits::template MakeTzDate<TTzDatetime64>(isOptional);
        case NUdf::EDataSlot::TzTimestamp64:
            return TTraits::template MakeTzDate<TTzTimestamp64>(isOptional);
        case NUdf::EDataSlot::Decimal: {
            if constexpr (CanInstantiateBlockReaderForDecimal<TTraits>) {
                return MakeFixedSizeBlockReaderImpl<TTraits, NYql::NDecimal::TInt128>(isOptional);
            } else {
                Y_ENSURE(false, "Unsupported data slot");
            }
        }
        case NUdf::EDataSlot::Uuid:
        case NUdf::EDataSlot::DyNumber:
            Y_ENSURE(false, "Unsupported data slot");
        }
    }

    TResourceTypeInspector resource(typeInfoHelper, type);
    if (resource) {
        return TTraits::MakeResource(isOptional);
    }

    TPgTypeInspector typePg(typeInfoHelper, type);
    if (typePg) {
        auto desc = typeInfoHelper.FindPgTypeDescription(typePg.GetTypeId());
        return TTraits::MakePg(*desc, pgBuilder);
    }

    Y_ENSURE(false, "Unsupported type");
}

inline std::unique_ptr<IBlockReader> MakeBlockReader(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    return MakeBlockReaderImpl<TReaderTraits>(typeInfoHelper, type, nullptr);
}

inline void UpdateBlockItemSerializeProps(const ITypeInfoHelper& typeInfoHelper, const TType* type, TBlockItemSerializeProps& props) {
    if (!props.MaxSize.Defined()) {
        return;
    }

    for (;;) {
        TOptionalTypeInspector typeOpt(typeInfoHelper, type);
        if (!typeOpt) {
            break;
        }
        props.MaxSize = *props.MaxSize + 1;
        props.IsFixed = false;
        type = typeOpt.GetItemType();
    }

    TStructTypeInspector typeStruct(typeInfoHelper, type);
    if (typeStruct) {
        for (ui32 i = 0; i < typeStruct.GetMembersCount(); ++i) {
            UpdateBlockItemSerializeProps(typeInfoHelper, typeStruct.GetMemberType(i), props);
        }
        return;
    }

    TTupleTypeInspector typeTuple(typeInfoHelper, type);
    if (typeTuple) {
        for (ui32 i = 0; i < typeTuple.GetElementsCount(); ++i) {
            UpdateBlockItemSerializeProps(typeInfoHelper, typeTuple.GetElementType(i), props);
        }
        return;
    }

    TDataTypeInspector typeData(typeInfoHelper, type);
    if (typeData) {
        auto typeId = typeData.GetTypeId();
        auto slot = GetDataSlot(typeId);
        auto& dataTypeInfo = GetDataTypeInfo(slot);
        if (dataTypeInfo.Features & DecimalType) {
            *props.MaxSize += 16;
        } else if (dataTypeInfo.Features & StringType) {
            props.MaxSize = {};
            props.IsFixed = false;
        } else if (dataTypeInfo.Features & TzDateType) {
            *props.MaxSize += dataTypeInfo.FixedSize + sizeof(TTimezoneId);
        }
        else {
            *props.MaxSize += dataTypeInfo.FixedSize;
        }
        return;
    }

    TPgTypeInspector typePg(typeInfoHelper, type);
    if (typePg) {
        auto desc = typeInfoHelper.FindPgTypeDescription(typePg.GetTypeId());
        if (desc->PassByValue) {
            *props.MaxSize += 1 + 8;
        } else {
            props.MaxSize = {};
            props.IsFixed = false;
        }

        return;
    }

    Y_ENSURE(false, "Unsupported type");
}

}
}
