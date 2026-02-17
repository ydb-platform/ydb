#pragma once

#include "block_item.h"
#include "block_io_buffer.h"
#include "dispatch_traits.h"
#include "util.h"

#include <arrow/datum.h>

#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_value_utils.h>

namespace NYql::NUdf {

class IBlockReader: private TNonCopyable {
public:
    virtual ~IBlockReader() = default;
    // result will reference to Array/Scalar internals and will be valid until next call to GetItem/GetScalarItem
    virtual TBlockItem GetItem(const arrow::ArrayData& data, size_t index) = 0;
    virtual TBlockItem GetScalarItem(const arrow::Scalar& scalar) = 0;

    virtual ui64 GetDataWeight(const arrow::ArrayData& data) const = 0;
    virtual ui64 GetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const = 0;
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

class TBlockReaderBase: public IBlockReader {
public:
    ui64 GetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const final {
        Y_ENSURE(0 <= offset && offset < data.length);
        Y_ENSURE(offset + length >= offset);
        Y_ENSURE(offset + length <= data.length);
        return DoGetSliceDataWeight(data, offset, length);
    }

protected:
    virtual ui64 DoGetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const = 0;

    static ui64 GetBitmaskDataWeight(int64_t dataLength) {
        if (dataLength <= 0) {
            return 0;
        }
        return (dataLength - 1) / 8 + 1;
    }
};

template <typename T, bool Nullable, typename TDerived>
class TFixedSizeBlockReaderBase: public TBlockReaderBase {
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

        if constexpr (std::is_same_v<T, NYql::NDecimal::TInt128>) {
            auto& fixedScalar = checked_cast<const arrow::FixedSizeBinaryScalar&>(scalar);
            T value;
            memcpy((void*)&value, fixedScalar.value->data(), sizeof(T));
            return static_cast<TDerived*>(this)->MakeBlockItem(value);
        } else {
            return static_cast<TDerived*>(this)->MakeBlockItem(
                *static_cast<const T*>(checked_cast<const PrimitiveScalarBase&>(scalar).data()));
        }
    }

    ui64 GetDataWeight(const arrow::ArrayData& data) const final {
        return GetDataWeightImpl(data.length);
    }

    ui64 DoGetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const final {
        Y_UNUSED(data, offset);
        return GetDataWeightImpl(length);
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

        if constexpr (std::is_same_v<T, NYql::NDecimal::TInt128>) {
            auto& fixedScalar = arrow::internal::checked_cast<const arrow::FixedSizeBinaryScalar&>(scalar);
            T value;
            memcpy((void*)&value, fixedScalar.value->data(), sizeof(T));
            out.PushNumber(value);
        } else {
            out.PushNumber(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
        }
    }

private:
    ui64 GetDataWeightImpl(int64_t dataLength) const {
        ui64 size = sizeof(T) * dataLength;
        if constexpr (Nullable) {
            size += GetBitmaskDataWeight(dataLength);
        }
        return size;
    }
};

template <typename T, bool Nullable>
class TFixedSizeBlockReader: public TFixedSizeBlockReaderBase<T, Nullable, TFixedSizeBlockReader<T, Nullable>> {
public:
    TBlockItem MakeBlockItem(const T& item) const {
        return TBlockItem(item);
    }
};

template <bool Nullable>
class TResourceBlockReader: public TFixedSizeBlockReaderBase<TUnboxedValuePod, Nullable, TResourceBlockReader<Nullable>> {
public:
    TBlockItem MakeBlockItem(const TUnboxedValuePod& pod) const {
        TBlockItem item;
        std::memcpy(item.GetRawPtr(), pod.GetRawPtr(), sizeof(TBlockItem));
        return item;
    }
};

template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot TOriginal = NKikimr::NUdf::EDataSlot::String>
class TStringBlockReader final: public TBlockReaderBase {
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
        return GetDataWeightImpl(data.length, data.GetValues<TOffset>(1));
    }

    ui64 DoGetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const final {
        return GetDataWeightImpl(length, data.GetValues<TOffset>(1, offset));
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

private:
    ui64 GetDataWeightImpl(int64_t dataLength, const TOffset* offsets) const {
        ui64 size = 0;
        if constexpr (Nullable) {
            size += GetBitmaskDataWeight(dataLength);
        }
        size += offsets[dataLength] - offsets[0];
        size += sizeof(TOffset) * dataLength;
        return size;
    }
};

template <bool Nullable, typename TDerived>
class TTupleBlockReaderBase: public TBlockReaderBase {
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
            size += GetBitmaskDataWeight(data.length);
        }
        size += static_cast<const TDerived*>(this)->GetChildrenDataWeight(data);
        return size;
    }

    ui64 DoGetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const final {
        ui64 size = 0;
        if constexpr (Nullable) {
            size += GetBitmaskDataWeight(length);
        }
        size += static_cast<const TDerived*>(this)->GetChildrenDataWeight(data, offset, length);
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

template <bool Nullable>
class TTupleBlockReader final: public TTupleBlockReaderBase<Nullable, TTupleBlockReader<Nullable>> {
public:
    explicit TTupleBlockReader(TVector<std::unique_ptr<IBlockReader>>&& children)
        : Children_(std::move(children))
        , Items_(Children_.size())
    {
    }

    TBlockItem GetChildrenItems(const arrow::ArrayData& data, size_t index) {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Items_[i] = Children_[i]->GetItem(*data.child_data[i], index);
        }

        return TBlockItem(Items_.data());
    }

    TBlockItem GetChildrenScalarItems(const arrow::StructScalar& structScalar) {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Items_[i] = Children_[i]->GetScalarItem(*structScalar.value[i]);
        }

        return TBlockItem(Items_.data());
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

        for (ui32 i = 0; i < Children_.size(); ++i) {
            size += Children_[i]->GetDataWeight(items[i]);
        }

        return size;
    }

    size_t GetChildrenDataWeight(const arrow::ArrayData& data) const {
        size_t size = 0;
        for (ui32 i = 0; i < Children_.size(); ++i) {
            size += Children_[i]->GetDataWeight(*data.child_data[i]);
        }

        return size;
    }

    size_t GetChildrenDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const {
        size_t size = 0;
        for (ui32 i = 0; i < Children_.size(); ++i) {
            size += Children_[i]->GetSliceDataWeight(*data.child_data[i], offset, length);
        }
        return size;
    }

    size_t GetChildrenDefaultDataWeight() const {
        size_t size = 0;
        for (ui32 i = 0; i < Children_.size(); ++i) {
            size += Children_[i]->GetDefaultValueWeight();
        }
        return size;
    }

    void SaveChildrenItems(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Children_[i]->SaveItem(*data.child_data[i], index, out);
        }
    }

    void SaveChildrenScalarItems(const arrow::StructScalar& structScalar, TOutputBuffer& out) const {
        for (ui32 i = 0; i < Children_.size(); ++i) {
            Children_[i]->SaveScalarItem(*structScalar.value[i], out);
        }
    }

private:
    const TVector<std::unique_ptr<IBlockReader>> Children_;
    TVector<TBlockItem> Items_;
};

template <typename TTzDate, bool Nullable>
class TTzDateBlockReader final: public TTupleBlockReaderBase<Nullable, TTzDateBlockReader<TTzDate, Nullable>> {
public:
    TBlockItem GetChildrenItems(const arrow::ArrayData& data, size_t index) {
        Y_DEBUG_ABORT_UNLESS(data.child_data.size() == 2);

        TBlockItem item{DateReader_.GetItem(*data.child_data[0], index)};
        item.SetTimezoneId(TimezoneReader_.GetItem(*data.child_data[1], index).Get<ui16>());
        return item;
    }

    TBlockItem GetChildrenScalarItems(const arrow::StructScalar& structScalar) {
        Y_DEBUG_ABORT_UNLESS(structScalar.value.size() == 2);

        TBlockItem item{DateReader_.GetScalarItem(*structScalar.value[0])};
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

    size_t GetChildrenDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const {
        Y_DEBUG_ABORT_UNLESS(data.child_data.size() == 2);

        size_t size = 0;
        size += DateReader_.GetSliceDataWeight(*data.child_data[0], offset, length);
        size += TimezoneReader_.GetSliceDataWeight(*data.child_data[1], offset, length);
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
    TFixedSizeBlockReader<typename TDataType<TTzDate>::TLayout, /* Nullable */ false> DateReader_;
    TFixedSizeBlockReader<ui16, /* Nullable */ false> TimezoneReader_;
};

// NOTE: For null singular type we use arrow::null() data type.
// This data type DOES NOT support bit mask so for optional type
// we have to use |TExternalOptional| wrapper.
//
// For non-null singular types we use arrow::Struct({}).
// We do not allow using bitmask too to be consistent with arrow::null().
template <bool IsNull>
class TSingularTypeBlockReader: public TBlockReaderBase {
public:
    TSingularTypeBlockReader() = default;

    ~TSingularTypeBlockReader() override = default;

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) override {
        Y_UNUSED(data, index);
        return CreateSingularBlockItem<IsNull>();
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) override {
        Y_UNUSED(scalar);
        return CreateSingularBlockItem<IsNull>();
    }

    ui64 GetDataWeight(const arrow::ArrayData& data) const override {
        Y_UNUSED(data);
        return 0;
    }

    ui64 DoGetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const final {
        Y_UNUSED(data, offset, length);
        return 0;
    }

    ui64 GetDataWeight(TBlockItem item) const override {
        Y_UNUSED(item);
        return 0;
    }

    ui64 GetDefaultValueWeight() const override {
        return 0;
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const override {
        Y_UNUSED(index, data, out);
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const override {
        Y_UNUSED(scalar, out);
    }
};

class TExternalOptionalBlockReader final: public TBlockReaderBase {
public:
    explicit TExternalOptionalBlockReader(std::unique_ptr<IBlockReader>&& inner)
        : Inner_(std::move(inner))
    {
    }

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if (IsNull(data, index)) {
            return {};
        }

        return Inner_->GetItem(*data.child_data.front(), index).MakeOptional();
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        return Inner_->GetScalarItem(*structScalar.value.front()).MakeOptional();
    }

    ui64 GetDataWeight(const arrow::ArrayData& data) const final {
        return GetBitmaskDataWeight(data.length) + Inner_->GetDataWeight(*data.child_data.front());
    }

    ui64 DoGetSliceDataWeight(const arrow::ArrayData& data, int64_t offset, int64_t length) const final {
        return GetBitmaskDataWeight(length) + Inner_->GetSliceDataWeight(*data.child_data.front(), offset, length);
    }

    ui64 GetDataWeight(TBlockItem item) const final {
        if (!item) {
            return GetDefaultValueWeight();
        }
        return 1 + Inner_->GetDataWeight(item.GetOptionalValue());
    }

    ui64 GetDefaultValueWeight() const final {
        return 1 + Inner_->GetDefaultValueWeight();
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        if (IsNull(data, index)) {
            return out.PushChar(0);
        }
        out.PushChar(1);

        Inner_->SaveItem(*data.child_data.front(), index, out);
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const final {
        if (!scalar.is_valid) {
            return out.PushChar(0);
        }
        out.PushChar(1);

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        Inner_->SaveScalarItem(*structScalar.value.front(), out);
    }

private:
    const std::unique_ptr<IBlockReader> Inner_;
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
    template <bool Nullable>
    using TResource = TResourceBlockReader<Nullable>;
    template <typename TTzDate, bool Nullable>
    using TTzDateReader = TTzDateBlockReader<TTzDate, Nullable>;
    template <bool IsNull>
    using TSingularType = TSingularTypeBlockReader<IsNull>;

    constexpr static bool PassType = false;

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

    template <bool IsNull>
    static std::unique_ptr<TResult> MakeSingular() {
        return std::make_unique<TSingularType<IsNull>>();
    }

    template <typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional) {
        if (isOptional) {
            return std::make_unique<TTzDateReader<TTzDate, true>>();
        } else {
            return std::make_unique<TTzDateReader<TTzDate, false>>();
        }
    }
};

inline std::unique_ptr<IBlockReader> MakeBlockReader(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    return DispatchByArrowTraits<TReaderTraits>(typeInfoHelper, type, nullptr);
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
        } else {
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

    if (IsSingularType(typeInfoHelper, type)) {
        return;
    }

    Y_ENSURE(false, "Unsupported type");
}

} // namespace NYql::NUdf
