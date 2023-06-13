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

    virtual void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const = 0;
    virtual void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const = 0;
};

struct TBlockItemSerializeProps {
    TMaybe<ui32> MaxSize = 0; // maximum size each block item can occupy in TOutputBuffer
                              // (will be undefined for dynamic object like string)
    bool IsFixed = true;      // true if each block item takes fixed size
};

template<typename T, bool Nullable>
class TFixedSizeBlockReader final : public IBlockReader {
public:
    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return {};
            }
        }

        return TBlockItem(data.GetValues<T>(1)[index]);
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return {};
            }
        }

        return TBlockItem(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
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

        out.PushNumber(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
    }
};

template<typename TStringType, bool Nullable>
class TStringBlockReader final : public IBlockReader {
public:
    using TOffset = typename TStringType::offset_type;

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        Y_VERIFY_DEBUG(data.buffers.size() == 3);
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

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        Y_VERIFY_DEBUG(data.buffers.size() == 3);
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

template<bool Nullable>
class TTupleBlockReader final : public IBlockReader {
public:
    TTupleBlockReader(TVector<std::unique_ptr<IBlockReader>>&& children)
        : Children(std::move(children))
        , Items(Children.size())
    {}

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return {};
            }
        }

        for (ui32 i = 0; i < Children.size(); ++i) {
            Items[i] = Children[i]->GetItem(*data.child_data[i], index);
        }

        return TBlockItem(Items.data());
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return {};
            }
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);

        for (ui32 i = 0; i < Children.size(); ++i) {
            Items[i] = Children[i]->GetScalarItem(*structScalar.value[i]);
        }

        return TBlockItem(Items.data());
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        if constexpr (Nullable) {
            if (IsNull(data, index)) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        for (ui32 i = 0; i < Children.size(); ++i) {
            Children[i]->SaveItem(*data.child_data[i], index, out);
        }
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const final {
        if constexpr (Nullable) {
            if (!scalar.is_valid) {
                return out.PushChar(0);
            }
            out.PushChar(1);
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);

        for (ui32 i = 0; i < Children.size(); ++i) {
            Children[i]->SaveScalarItem(*structScalar.value[i], out);
        }
    }

private:
    const TVector<std::unique_ptr<IBlockReader>> Children;
    TVector<TBlockItem> Items;
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

        return Inner->GetItem(*data.child_data[0], index).MakeOptional();
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        return Inner->GetScalarItem(*structScalar.value[0]).MakeOptional();
    }

    void SaveItem(const arrow::ArrayData& data, size_t index, TOutputBuffer& out) const final {
        if (IsNull(data, index)) {
            return out.PushChar(0);
        }
        out.PushChar(1);

        Inner->SaveItem(*data.child_data[0], index, out);
    }

    void SaveScalarItem(const arrow::Scalar& scalar, TOutputBuffer& out) const final {
        if (!scalar.is_valid) {
            return out.PushChar(0);
        }
        out.PushChar(1);

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);
        Inner->SaveScalarItem(*structScalar.value[0], out);
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
    template <typename TStringType, bool Nullable>
    using TStrings = TStringBlockReader<TStringType, Nullable>;
    using TExtOptional = TExternalOptionalBlockReader;

    static std::unique_ptr<TResult> MakePg(const TPgTypeDescription& desc, const IPgBuilder* pgBuilder) {
        Y_UNUSED(pgBuilder);
        if (desc.PassByValue) {
            return std::make_unique<TFixedSize<ui64, true>>();
        } else {
            return std::make_unique<TStrings<arrow::BinaryType, true>>();
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

template <typename TTraits, typename T>
std::unique_ptr<typename TTraits::TResult> MakeStringBlockReaderImpl(bool isOptional) {
    if (isOptional) {
        return std::make_unique<typename TTraits::template TStrings<T, true>>();
    } else {
        return std::make_unique<typename TTraits::template TStrings<T, false>>();
    }
}

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
            return MakeFixedSizeBlockReaderImpl<TTraits, i32>(isOptional);
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return MakeFixedSizeBlockReaderImpl<TTraits, ui32>(isOptional);
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return MakeFixedSizeBlockReaderImpl<TTraits, i64>(isOptional);
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return MakeFixedSizeBlockReaderImpl<TTraits, ui64>(isOptional);
        case NUdf::EDataSlot::Float:
            return MakeFixedSizeBlockReaderImpl<TTraits, float>(isOptional);
        case NUdf::EDataSlot::Double:
            return MakeFixedSizeBlockReaderImpl<TTraits, double>(isOptional);
        case NUdf::EDataSlot::String:
            return MakeStringBlockReaderImpl<TTraits, arrow::BinaryType>(isOptional);
        case NUdf::EDataSlot::Utf8:
            return MakeStringBlockReaderImpl<TTraits, arrow::StringType>(isOptional);
        default:
            Y_ENSURE(false, "Unsupported data slot");
        }
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
        if (dataTypeInfo.Features & StringType) {
            props.MaxSize = {};
            props.IsFixed = false;
        } else {
            *props.MaxSize += dataTypeInfo.FixedSize;
        }
        return;
    }

    TPgTypeInspector typePg(typeInfoHelper, type);
    if (typePg) {
        auto desc = typeInfoHelper.FindPgTypeDescription(typePg.GetTypeId());
        if (desc->PassByValue) {
            *props.MaxSize += desc->Typelen;
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
