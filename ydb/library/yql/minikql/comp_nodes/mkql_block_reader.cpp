#include "mkql_block_reader.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/array/array_binary.h>
#include <arrow/chunked_array.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockReaderBase : public IBlockReader {
public:
    using Ptr = std::unique_ptr<TBlockReaderBase>;

    void Reset(const arrow::Datum& datum) final {
        Index = 0;
        ArrayValues.clear();
        if (datum.is_scalar()) {
            ScalarValue = GetScalar(*datum.scalar());
        } else {
            Y_VERIFY(datum.is_arraylike());
            ScalarValue = {};
            for (auto& arr : datum.chunks()) {
                ArrayValues.push_back(arr->data());
            }
        }
    }

    TMaybe<NUdf::TUnboxedValuePod> GetNextValue() final {
        if (ScalarValue.Defined()) {
            return ScalarValue;
        }

        TMaybe<NUdf::TUnboxedValuePod> result;
        while (!ArrayValues.empty()) {
            if (Index < ArrayValues.front()->length) {
                result = Get(*ArrayValues.front(), Index++);
                break;
            }
            ArrayValues.pop_front();
            Index = 0;
        }
        return result;
    }

    virtual NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) = 0;
    virtual NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) = 0;

private:
    std::deque<std::shared_ptr<arrow::ArrayData>> ArrayValues;
    TMaybe<NUdf::TUnboxedValuePod> ScalarValue;
    size_t Index = 0;
};

template <typename T>
class TFixedSizeBlockReader : public TBlockReaderBase {
public:
    NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) final {
        if (data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset)) {
            return {};
        }
    
        return NUdf::TUnboxedValuePod(data.GetValues<T>(1)[index]);
    }

    NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        return NUdf::TUnboxedValuePod(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
    }
};

class TStringBlockReader : public TBlockReaderBase {
public:
    NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) final {
        Y_VERIFY_DEBUG(data.buffers.size() == 3);
        if (data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset)) {
            return {};
        }

        arrow::util::string_view result;
        if (data.type->id() == arrow::Type::BINARY) {
            arrow::BinaryArray arr(std::make_shared<arrow::ArrayData>(data));
            result = arr.GetView(index);
        } else {
            Y_VERIFY(data.type->id() == arrow::Type::STRING);
            arrow::StringArray arr(std::make_shared<arrow::ArrayData>(data));
            result = arr.GetView(index);
        }

        return MakeString(NUdf::TStringRef(result.data(), result.size()));
    }

    NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(scalar).value;
        return MakeString(NUdf::TStringRef(reinterpret_cast<const char*>(buffer->data()), buffer->size()));
    }
};

class TTupleBlockReader : public TBlockReaderBase {
public:
    TTupleBlockReader(TVector<std::unique_ptr<TBlockReaderBase>>&& children, const THolderFactory& holderFactory)
        : Children(std::move(children))
        , HolderFactory(holderFactory)
    {}

    NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) final {
        if (data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset)) {
            return {};
        }

        NUdf::TUnboxedValue* items;
        auto result = Cache.NewArray(HolderFactory, Children.size(), items);
        for (ui32 i = 0; i < Children.size(); ++i) {
            items[i] = Children[i]->Get(*data.child_data[i], index);
        }

        return result;
    }

    NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);

        NUdf::TUnboxedValue* items;
        auto result = Cache.NewArray(HolderFactory, Children.size(), items);
        for (ui32 i = 0; i < Children.size(); ++i) {
            items[i] = Children[i]->GetScalar(*structScalar.value[i]);
        }

        return result;
    }

private:
    TVector<std::unique_ptr<TBlockReaderBase>> Children;
    const THolderFactory& HolderFactory;
    TPlainContainerCache Cache;
};

std::unique_ptr<TBlockReaderBase> MakeBlockReaderBase(TType* type, const THolderFactory& holderFactory) {
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        TVector<std::unique_ptr<TBlockReaderBase>> children;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            children.emplace_back(MakeBlockReaderBase(tupleType->GetElementType(i), holderFactory));
        }

        return std::make_unique<TTupleBlockReader>(std::move(children), holderFactory);
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
        case NUdf::EDataSlot::Utf8:
            return std::make_unique<TStringBlockReader>();
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }
    }

    MKQL_ENSURE(false, "Unsupported type");
}


} // namespace

std::unique_ptr<IBlockReader> MakeBlockReader(TType* type, const THolderFactory& holderFactory) {
    return MakeBlockReaderBase(type, holderFactory);
}


} // namespace NMiniKQL
} // namespace NKikimr
