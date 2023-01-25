#pragma once

#include "block_item.h"
#include "util.h"
#include <arrow/datum.h>

#include <ydb/library/yql/public/udf/udf_type_inspection.h>

namespace NYql {
namespace NUdf {

class IBlockReader : private TNonCopyable {
public:
    virtual ~IBlockReader() = default;
    // result will reference to Array/Scalar internals and will be valid until next call to GetItem/GetScalarItem
    virtual TBlockItem GetItem(const arrow::ArrayData& data, size_t index) = 0;
    virtual TBlockItem GetScalarItem(const arrow::Scalar& scalar) = 0;
};

template <typename T>
class TFixedSizeBlockReader : public IBlockReader {
public:
    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if (IsNull(data, index)) {
            return {};
        }

        return TBlockItem(data.GetValues<T>(1)[index]);
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        return TBlockItem(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
    }
};

template<typename TStringType>
class TStringBlockReader : public IBlockReader {
public:
    using TOffset = typename TStringType::offset_type;

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        Y_VERIFY_DEBUG(data.buffers.size() == 3);
        if (IsNull(data, index)) {
            return {};
        }

        const TOffset* offsets = data.GetValues<TOffset>(1);
        const char* strData = data.GetValues<char>(2, 0);

        std::string_view str(strData + offsets[index], offsets[index + 1] - offsets[index]);
        return TBlockItem(str);
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(scalar).value;
        std::string_view str(reinterpret_cast<const char*>(buffer->data()), buffer->size());
        return TBlockItem(str);
    }
};

class TTupleBlockReader : public IBlockReader {
public:
    TTupleBlockReader(TVector<std::unique_ptr<IBlockReader>>&& children)
        : Children(std::move(children))
        , Items(Children.size())
    {}

    TBlockItem GetItem(const arrow::ArrayData& data, size_t index) final {
        if (IsNull(data, index)) {
            return {};
        }

        for (ui32 i = 0; i < Children.size(); ++i) {
            Items[i] = Children[i]->GetItem(*data.child_data[i], index);
        }

        return TBlockItem(Items.data());
    }

    TBlockItem GetScalarItem(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);

        for (ui32 i = 0; i < Children.size(); ++i) {
            Items[i] = Children[i]->GetScalarItem(*structScalar.value[i]);
        }

        return TBlockItem(Items.data());
    }

private:
    const TVector<std::unique_ptr<IBlockReader>> Children;
    TVector<TBlockItem> Items;
};

class TExternalOptionalBlockReader : public IBlockReader {
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

private:
    const std::unique_ptr<IBlockReader> Inner;
};

struct TReaderTraits {
    using TResult = IBlockReader;
    using TTuple = TTupleBlockReader;
    template <typename T>
    using TFixedSize = TFixedSizeBlockReader<T>;
    template <typename TStringType>
    using TStrings = TStringBlockReader<TStringType>;
    using TExtOptional = TExternalOptionalBlockReader;
};

template <typename TTraits>
std::unique_ptr<typename TTraits::TResult> MakeBlockReaderImpl(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    const TType* unpacked = type;
    TOptionalTypeInspector typeOpt(typeInfoHelper, type);
    if (typeOpt) {
        unpacked = typeOpt.GetItemType();
    }

    TOptionalTypeInspector unpackedOpt(typeInfoHelper, unpacked);
    if (unpackedOpt) {
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

        auto reader = MakeBlockReaderImpl<TTraits>(typeInfoHelper, previousType);
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
            children.emplace_back(MakeBlockReaderImpl<TTraits>(typeInfoHelper, typeTuple.GetElementType(i)));
        }

        return std::make_unique<typename TTraits::TTuple>(std::move(children));
    }

    TDataTypeInspector typeData(typeInfoHelper, type);
    if (typeData) {
        auto typeId = typeData.GetTypeId();
        switch (GetDataSlot(typeId)) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<typename TTraits::template TFixedSize<i8>>();
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            return std::make_unique<typename TTraits::template TFixedSize<ui8>>();
        case NUdf::EDataSlot::Int16:
            return std::make_unique<typename TTraits::template TFixedSize<i16>>();
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<typename TTraits::template TFixedSize<ui16>>();
        case NUdf::EDataSlot::Int32:
            return std::make_unique<typename TTraits::template TFixedSize<i32>>();
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<typename TTraits::template TFixedSize<ui32>>();
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<typename TTraits::template TFixedSize<i64>>();
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<typename TTraits::template TFixedSize<ui64>>();
        case NUdf::EDataSlot::Float:
            return std::make_unique<typename TTraits::template TFixedSize<float>>();
        case NUdf::EDataSlot::Double:
            return std::make_unique<typename TTraits::template TFixedSize<double>>();
        case NUdf::EDataSlot::String:
            return std::make_unique<typename TTraits::template TStrings<arrow::BinaryType>>();
        case NUdf::EDataSlot::Utf8:
            return std::make_unique<typename TTraits::template TStrings<arrow::StringType>>();
        default:
            Y_ENSURE(false, "Unsupported data slot");
        }
    }

    Y_ENSURE(false, "Unsupported type");
}

inline std::unique_ptr<IBlockReader> MakeBlockReader(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    return MakeBlockReaderImpl<TReaderTraits>(typeInfoHelper, type);
}

}
}
