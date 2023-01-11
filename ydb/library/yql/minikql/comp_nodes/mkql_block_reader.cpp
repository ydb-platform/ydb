#include "mkql_block_reader.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/array/array_binary.h>
#include <arrow/chunked_array.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

inline bool IsNull(const arrow::ArrayData& data, size_t index) {
    return data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset);
}

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

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        return item ? NUdf::TUnboxedValuePod(item.As<T>()) : NUdf::TUnboxedValuePod{};
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

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        Y_UNUSED(holderFactory);
        if (!item) {
            return {};
        }
        return MakeString(item.AsStringRef());
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

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        if (!item) {
            return {};
        }

        NUdf::TUnboxedValue* values;
        auto result = Cache.NewArray(holderFactory, Children.size(), values);
        const TBlockItem* childItems = item.AsTuple();
        for (ui32 i = 0; i < Children.size(); ++i) {
            values[i] = Children[i]->MakeValue(childItems[i], holderFactory);
        }

        return result;
    }

private:
    const TVector<std::unique_ptr<IBlockReader>> Children;
    TVector<TBlockItem> Items;
    mutable TPlainContainerCache Cache;
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

    NUdf::TUnboxedValuePod MakeValue(TBlockItem item, const THolderFactory& holderFactory) const final {
        if (!item) {
            return {};
        }
        return Inner->MakeValue(item.GetOptionalValue(), holderFactory).MakeOptional();
    }

private:
    const std::unique_ptr<IBlockReader> Inner;
};

} // namespace

std::unique_ptr<IBlockReader> MakeBlockReader(TType* type) {
    TType* unpacked = type;
    if (type->IsOptional()) {
        unpacked = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (unpacked->IsOptional()) {
        // at least 2 levels of optionals
        ui32 nestLevel = 0;
        auto currentType = type;
        auto previousType = type;
        do {
            ++nestLevel;
            previousType = currentType;
            currentType = AS_TYPE(TOptionalType, currentType)->GetItemType();
        } while (currentType->IsOptional());

        std::unique_ptr<IBlockReader> reader = MakeBlockReader(previousType);
        for (ui32 i = 1; i < nestLevel; ++i) {
            reader = std::make_unique<TExternalOptionalBlockReader>(std::move(reader));
        }

        return reader;
    } else {
        type = unpacked;
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        TVector<std::unique_ptr<IBlockReader>> children;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            children.emplace_back(MakeBlockReader(tupleType->GetElementType(i)));
        }

        return std::make_unique<TTupleBlockReader>(std::move(children));
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TFixedSizeBlockReader<i8>>();
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            return std::make_unique<TFixedSizeBlockReader<ui8>>();
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TFixedSizeBlockReader<i16>>();
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TFixedSizeBlockReader<ui16>>();
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TFixedSizeBlockReader<i32>>();
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TFixedSizeBlockReader<ui32>>();
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<TFixedSizeBlockReader<i64>>();
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TFixedSizeBlockReader<ui64>>();
        case NUdf::EDataSlot::Float:
            return std::make_unique<TFixedSizeBlockReader<float>>();
        case NUdf::EDataSlot::Double:
            return std::make_unique<TFixedSizeBlockReader<double>>();
        case NUdf::EDataSlot::String:
            return std::make_unique<TStringBlockReader<arrow::BinaryType>>();
        case NUdf::EDataSlot::Utf8:
            return std::make_unique<TStringBlockReader<arrow::StringType>>();
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }
    }

    MKQL_ENSURE(false, "Unsupported type");
}

} // namespace NMiniKQL
} // namespace NKikimr
