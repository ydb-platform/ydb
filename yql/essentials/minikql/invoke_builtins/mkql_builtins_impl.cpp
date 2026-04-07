#include "mkql_builtins_impl.h"                       // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_builder.h> // UnpackOptionalData

namespace NKikimr {
namespace NMiniKQL {
namespace {

std::unique_ptr<arrow::ResizableBuffer> AllocateResizableBufferAndResize(size_t size, arrow::MemoryPool* pool) {
    auto result = NYql::NUdf::AllocateResizableBuffer(size, pool);
    ARROW_OK(result->Resize(size));
    return result;
}

} // namespace
template <typename T>
arrow::compute::InputType GetPrimitiveInputArrowType(bool tz) {
    return arrow::compute::InputType(AddTzType(tz, GetPrimitiveDataType<T>()), arrow::ValueDescr::ANY);
}

template <typename T>
arrow::compute::OutputType GetPrimitiveOutputArrowType(bool tz) {
    return arrow::compute::OutputType(AddTzType(tz, GetPrimitiveDataType<T>()));
}

template arrow::compute::InputType GetPrimitiveInputArrowType<bool>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<i8>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<ui8>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<i16>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<ui16>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<i32>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<ui32>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<i64>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<ui64>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<float>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<double>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<char*>(bool tz);
template arrow::compute::InputType GetPrimitiveInputArrowType<NYql::NUdf::TUtf8>(bool tz);

template arrow::compute::OutputType GetPrimitiveOutputArrowType<bool>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<i8>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<ui8>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<i16>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<ui16>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<i32>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<ui32>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<i64>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<ui64>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<float>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<double>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<char*>(bool tz);
template arrow::compute::OutputType GetPrimitiveOutputArrowType<NYql::NUdf::TUtf8>(bool tz);

arrow::compute::InputType GetPrimitiveInputArrowType(NUdf::EDataSlot slot) {
    switch (slot) {
        case NUdf::EDataSlot::Bool:
            return GetPrimitiveInputArrowType<bool>();
        case NUdf::EDataSlot::Int8:
            return GetPrimitiveInputArrowType<i8>();
        case NUdf::EDataSlot::Uint8:
            return GetPrimitiveInputArrowType<ui8>();
        case NUdf::EDataSlot::Int16:
            return GetPrimitiveInputArrowType<i16>();
        case NUdf::EDataSlot::Uint16:
            return GetPrimitiveInputArrowType<ui16>();
        case NUdf::EDataSlot::Int32:
            return GetPrimitiveInputArrowType<i32>();
        case NUdf::EDataSlot::Uint32:
            return GetPrimitiveInputArrowType<ui32>();
        case NUdf::EDataSlot::Int64:
            return GetPrimitiveInputArrowType<i64>();
        case NUdf::EDataSlot::Uint64:
            return GetPrimitiveInputArrowType<ui64>();
        case NUdf::EDataSlot::Float:
            return GetPrimitiveInputArrowType<float>();
        case NUdf::EDataSlot::Double:
            return GetPrimitiveInputArrowType<double>();
        case NUdf::EDataSlot::String:
            return GetPrimitiveInputArrowType<char*>();
        case NUdf::EDataSlot::Utf8:
            return GetPrimitiveInputArrowType<NYql::NUdf::TUtf8>();
        case NUdf::EDataSlot::Date:
            return GetPrimitiveInputArrowType<ui16>();
        case NUdf::EDataSlot::TzDate:
            return GetPrimitiveInputArrowType<ui16>(true);
        case NUdf::EDataSlot::Datetime:
            return GetPrimitiveInputArrowType<ui32>();
        case NUdf::EDataSlot::TzDatetime:
            return GetPrimitiveInputArrowType<ui32>(true);
        case NUdf::EDataSlot::Timestamp:
            return GetPrimitiveInputArrowType<ui64>();
        case NUdf::EDataSlot::TzTimestamp:
            return GetPrimitiveInputArrowType<ui64>(true);
        case NUdf::EDataSlot::Interval:
            return GetPrimitiveInputArrowType<i64>();
        case NUdf::EDataSlot::Date32:
            return GetPrimitiveInputArrowType<i32>();
        case NUdf::EDataSlot::TzDate32:
            return GetPrimitiveInputArrowType<i32>(true);
        case NUdf::EDataSlot::Datetime64:
            return GetPrimitiveInputArrowType<i64>();
        case NUdf::EDataSlot::TzDatetime64:
            return GetPrimitiveInputArrowType<i64>(true);
        case NUdf::EDataSlot::Timestamp64:
            return GetPrimitiveInputArrowType<i64>();
        case NUdf::EDataSlot::TzTimestamp64:
            return GetPrimitiveInputArrowType<i64>(true);
        case NUdf::EDataSlot::Interval64:
            return GetPrimitiveInputArrowType<i64>();
        case NUdf::EDataSlot::Decimal:
            return GetPrimitiveInputArrowType<NYql::NDecimal::TInt128>();
        default:
            ythrow yexception() << "Unexpected data slot: " << slot;
    }
}

arrow::compute::OutputType GetPrimitiveOutputArrowType(NUdf::EDataSlot slot) {
    switch (slot) {
        case NUdf::EDataSlot::Bool:
            return GetPrimitiveOutputArrowType<bool>();
        case NUdf::EDataSlot::Int8:
            return GetPrimitiveOutputArrowType<i8>();
        case NUdf::EDataSlot::Uint8:
            return GetPrimitiveOutputArrowType<ui8>();
        case NUdf::EDataSlot::Int16:
            return GetPrimitiveOutputArrowType<i16>();
        case NUdf::EDataSlot::Uint16:
            return GetPrimitiveOutputArrowType<ui16>();
        case NUdf::EDataSlot::Int32:
            return GetPrimitiveOutputArrowType<i32>();
        case NUdf::EDataSlot::Uint32:
            return GetPrimitiveOutputArrowType<ui32>();
        case NUdf::EDataSlot::Int64:
            return GetPrimitiveOutputArrowType<i64>();
        case NUdf::EDataSlot::Uint64:
            return GetPrimitiveOutputArrowType<ui64>();
        case NUdf::EDataSlot::Float:
            return GetPrimitiveOutputArrowType<float>();
        case NUdf::EDataSlot::Double:
            return GetPrimitiveOutputArrowType<double>();
        case NUdf::EDataSlot::String:
            return GetPrimitiveOutputArrowType<char*>();
        case NUdf::EDataSlot::Utf8:
            return GetPrimitiveOutputArrowType<NYql::NUdf::TUtf8>();
        case NUdf::EDataSlot::Date:
            return GetPrimitiveOutputArrowType<ui16>();
        case NUdf::EDataSlot::TzDate:
            return GetPrimitiveOutputArrowType<ui16>(true);
        case NUdf::EDataSlot::Datetime:
            return GetPrimitiveOutputArrowType<ui32>();
        case NUdf::EDataSlot::TzDatetime:
            return GetPrimitiveOutputArrowType<ui32>(true);
        case NUdf::EDataSlot::Timestamp:
            return GetPrimitiveOutputArrowType<ui64>();
        case NUdf::EDataSlot::TzTimestamp:
            return GetPrimitiveOutputArrowType<ui64>(true);
        case NUdf::EDataSlot::Interval:
            return GetPrimitiveOutputArrowType<i64>();
        case NUdf::EDataSlot::Date32:
            return GetPrimitiveOutputArrowType<i32>();
        case NUdf::EDataSlot::TzDate32:
            return GetPrimitiveOutputArrowType<i32>(true);
        case NUdf::EDataSlot::Datetime64:
            return GetPrimitiveOutputArrowType<i64>();
        case NUdf::EDataSlot::TzDatetime64:
            return GetPrimitiveOutputArrowType<i64>(true);
        case NUdf::EDataSlot::Timestamp64:
            return GetPrimitiveOutputArrowType<i64>();
        case NUdf::EDataSlot::TzTimestamp64:
            return GetPrimitiveOutputArrowType<i64>(true);
        case NUdf::EDataSlot::Interval64:
            return GetPrimitiveOutputArrowType<i64>();
        case NUdf::EDataSlot::Decimal:
            return GetPrimitiveOutputArrowType<NYql::NDecimal::TInt128>();
        default:
            ythrow yexception() << "Unexpected data slot: " << slot;
    }
}

std::shared_ptr<arrow::DataType> AddTzType(bool addTz, const std::shared_ptr<arrow::DataType>& type) {
    if (!addTz) {
        return type;
    }

    std::vector<std::shared_ptr<arrow::Field>> fields{
        std::make_shared<arrow::Field>("datetime", type, false),
        std::make_shared<arrow::Field>("timezoneId", arrow::uint16(), false)};

    return std::make_shared<arrow::StructType>(fields);
}

std::shared_ptr<arrow::DataType> AddTzType(EPropagateTz propagateTz, const std::shared_ptr<arrow::DataType>& type) {
    return AddTzType(propagateTz != EPropagateTz::None, type);
}

std::shared_ptr<arrow::Scalar> ExtractTz(bool isTz, const std::shared_ptr<arrow::Scalar>& value) {
    if (!isTz) {
        return value;
    }

    const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(*value);
    return structScalar.value[0];
}

std::shared_ptr<arrow::ArrayData> ExtractTz(bool isTz, const std::shared_ptr<arrow::ArrayData>& value) {
    if (!isTz) {
        return value;
    }

    return value->child_data[0];
}

std::shared_ptr<arrow::Scalar> WithTz(bool propagateTz, const std::shared_ptr<arrow::Scalar>& input,
                                      const std::shared_ptr<arrow::Scalar>& value) {
    if (!propagateTz) {
        return value;
    }

    const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(*input);
    auto tzId = structScalar.value[1];
    return std::make_shared<arrow::StructScalar>(arrow::StructScalar::ValueType{value, tzId}, input->type);
}

std::shared_ptr<arrow::Scalar> WithTz(EPropagateTz propagateTz,
                                      const std::shared_ptr<arrow::Scalar>& input1,
                                      const std::shared_ptr<arrow::Scalar>& input2,
                                      const std::shared_ptr<arrow::Scalar>& value) {
    if (propagateTz == EPropagateTz::None) {
        return value;
    }

    const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(
        propagateTz == EPropagateTz::FromLeft
            ? *input1
            : *input2);
    const auto tzId = structScalar.value[1];
    return std::make_shared<arrow::StructScalar>(
        arrow::StructScalar::ValueType{value, tzId},
        propagateTz == EPropagateTz::FromLeft ? input1->type : input2->type);
}

std::shared_ptr<arrow::ArrayData> CopyTzImpl(const std::shared_ptr<arrow::ArrayData>& res, bool propagateTz,
                                             const std::shared_ptr<arrow::ArrayData>& input, arrow::MemoryPool* pool,
                                             size_t sizeOf, const std::shared_ptr<arrow::DataType>& outputType) {
    if (!propagateTz) {
        return res;
    }

    Y_ENSURE(res->child_data.empty());
    std::shared_ptr<arrow::Buffer> buffer(AllocateResizableBufferAndResize(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, {nullptr, buffer}));
    res->child_data.push_back(input->child_data[1]);
    return res->child_data[0];
}

std::shared_ptr<arrow::ArrayData> CopyTzImpl(const std::shared_ptr<arrow::ArrayData>& res, EPropagateTz propagateTz,
                                             const std::shared_ptr<arrow::ArrayData>& input1,
                                             const std::shared_ptr<arrow::Scalar>& input2,
                                             arrow::MemoryPool* pool,
                                             size_t sizeOf, const std::shared_ptr<arrow::DataType>& outputType) {
    if (propagateTz == EPropagateTz::None) {
        return res;
    }

    Y_ENSURE(res->child_data.empty());
    std::shared_ptr<arrow::Buffer> buffer(AllocateResizableBufferAndResize(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, {nullptr, buffer}));
    if (propagateTz == EPropagateTz::FromLeft) {
        res->child_data.push_back(input1->child_data[1]);
    } else {
        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(*input2);
        auto tzId = ARROW_RESULT(arrow::MakeArrayFromScalar(*structScalar.value[1], res->length, pool))->data();
        res->child_data.push_back(tzId);
    }

    return res->child_data[0];
}

std::shared_ptr<arrow::ArrayData> CopyTzImpl(const std::shared_ptr<arrow::ArrayData>& res, EPropagateTz propagateTz,
                                             const std::shared_ptr<arrow::Scalar>& input1,
                                             const std::shared_ptr<arrow::ArrayData>& input2,
                                             arrow::MemoryPool* pool,
                                             size_t sizeOf, const std::shared_ptr<arrow::DataType>& outputType) {
    if (propagateTz == EPropagateTz::None) {
        return res;
    }

    Y_ENSURE(res->child_data.empty());
    std::shared_ptr<arrow::Buffer> buffer(AllocateResizableBufferAndResize(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, {nullptr, buffer}));
    if (propagateTz == EPropagateTz::FromLeft) {
        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(*input1);
        auto tzId = ARROW_RESULT(arrow::MakeArrayFromScalar(*structScalar.value[1], res->length, pool))->data();
        res->child_data.push_back(tzId);
    } else {
        res->child_data.push_back(input2->child_data[1]);
    }

    return res->child_data[0];
}

std::shared_ptr<arrow::ArrayData> CopyTzImpl(const std::shared_ptr<arrow::ArrayData>& res, EPropagateTz propagateTz,
                                             const std::shared_ptr<arrow::ArrayData>& input1,
                                             const std::shared_ptr<arrow::ArrayData>& input2,
                                             arrow::MemoryPool* pool,
                                             size_t sizeOf, const std::shared_ptr<arrow::DataType>& outputType) {
    if (propagateTz == EPropagateTz::None) {
        return res;
    }

    Y_ENSURE(res->child_data.empty());
    std::shared_ptr<arrow::Buffer> buffer(AllocateResizableBufferAndResize(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, {nullptr, buffer}));
    if (propagateTz == EPropagateTz::FromLeft) {
        res->child_data.push_back(input1->child_data[1]);
    } else {
        res->child_data.push_back(input2->child_data[1]);
    }

    return res->child_data[0];
}

TPlainKernel::TPlainKernel(const TKernelFamily& family, const std::vector<NUdf::TDataTypeId>& argTypes,
                           NUdf::TDataTypeId returnType, std::unique_ptr<arrow::compute::ScalarKernel>&& arrowKernel,
                           TKernel::ENullMode nullMode)
    : TKernel(family, argTypes, returnType, nullMode)
    , ArrowKernel(std::move(arrowKernel))
{
}

const arrow::compute::ScalarKernel& TPlainKernel::GetArrowKernel() const {
    return *ArrowKernel;
}

std::shared_ptr<arrow::compute::ScalarKernel> TPlainKernel::MakeArrowKernel(const TVector<TType*>&, TType*) const {
    ythrow yexception() << "Unsupported kernel";
}

bool TPlainKernel::IsPolymorphic() const {
    return false;
}

TDecimalKernel::TDecimalKernel(const TKernelFamily& family, const std::vector<NUdf::TDataTypeId>& argTypes,
                               NUdf::TDataTypeId returnType, TStatelessArrayKernelExec exec,
                               TKernel::ENullMode nullMode)
    : TKernel(family, argTypes, returnType, nullMode)
    , Exec(exec)
{
}

const arrow::compute::ScalarKernel& TDecimalKernel::GetArrowKernel() const {
    ythrow yexception() << "Unsupported kernel";
}

std::shared_ptr<arrow::compute::ScalarKernel> TDecimalKernel::MakeArrowKernel(const TVector<TType*>& argTypes, TType* resultType) const {
    MKQL_ENSURE(argTypes.size() == 2, "Require 2 arguments");
    MKQL_ENSURE(argTypes[0]->GetKind() == TType::EKind::Block, "Require block");
    MKQL_ENSURE(argTypes[1]->GetKind() == TType::EKind::Block, "Require block");
    MKQL_ENSURE(resultType->GetKind() == TType::EKind::Block, "Require block");

    bool isOptional = false;
    auto dataType1 = UnpackOptionalData(static_cast<TBlockType*>(argTypes[0])->GetItemType(), isOptional);
    auto dataType2 = UnpackOptionalData(static_cast<TBlockType*>(argTypes[1])->GetItemType(), isOptional);
    auto dataResultType = UnpackOptionalData(static_cast<TBlockType*>(resultType)->GetItemType(), isOptional);

    MKQL_ENSURE(*dataType1->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");
    MKQL_ENSURE(*dataType2->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");

    auto decimalType1 = static_cast<TDataDecimalType*>(dataType1);
    auto decimalType2 = static_cast<TDataDecimalType*>(dataType2);

    MKQL_ENSURE(decimalType1->GetParams() == decimalType2->GetParams(), "Require same precision/scale");

    ui8 precision = decimalType1->GetParams().first;
    MKQL_ENSURE(precision >= 1 && precision <= 35, TStringBuilder() << "Wrong precision: " << (int)precision);

    auto k = std::make_shared<arrow::compute::ScalarKernel>(
        std::vector<arrow::compute::InputType>{
            GetPrimitiveInputArrowType(NUdf::EDataSlot::Decimal),
            GetPrimitiveInputArrowType(NUdf::EDataSlot::Decimal)},
        GetPrimitiveOutputArrowType(*dataResultType->GetDataSlot()),
        Exec);
    k->null_handling = arrow::compute::NullHandling::INTERSECTION;
    k->init = [precision](arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&) {
        auto state = std::make_unique<TDecimalKernel::TKernelState>();
        state->Precision = precision;
        return arrow::Result(std::move(state));
    };

    return k;
}

bool TDecimalKernel::IsPolymorphic() const {
    return true;
}

void AddUnaryKernelImpl(TKernelFamilyBase& owner, NUdf::EDataSlot arg1, NUdf::EDataSlot res,
                        TStatelessArrayKernelExec exec, TKernel::ENullMode nullMode) {
    auto type1 = NUdf::GetDataTypeInfo(arg1).TypeId;
    auto returnType = NUdf::GetDataTypeInfo(res).TypeId;
    std::vector<NUdf::TDataTypeId> argTypes({type1});

    auto k = std::make_unique<arrow::compute::ScalarKernel>(
        std::vector<arrow::compute::InputType>{
            GetPrimitiveInputArrowType(arg1)},
        GetPrimitiveOutputArrowType(res),
        exec);

    switch (nullMode) {
        case TKernel::ENullMode::Default:
            k->null_handling = arrow::compute::NullHandling::INTERSECTION;
            break;
        case TKernel::ENullMode::AlwaysNull:
            k->null_handling = arrow::compute::NullHandling::COMPUTED_PREALLOCATE;
            break;
        case TKernel::ENullMode::AlwaysNotNull:
            k->null_handling = arrow::compute::NullHandling::OUTPUT_NOT_NULL;
            break;
    }

    owner.Adopt(argTypes, returnType, std::make_unique<TPlainKernel>(owner, argTypes, returnType, std::move(k), nullMode));
}

void AddBinaryKernelImpl(TKernelFamilyBase& owner, NUdf::EDataSlot arg1, NUdf::EDataSlot arg2, NUdf::EDataSlot res,
                         TStatelessArrayKernelExec exec, TKernel::ENullMode nullMode) {
    auto type1 = NUdf::GetDataTypeInfo(arg1).TypeId;
    auto type2 = NUdf::GetDataTypeInfo(arg2).TypeId;
    auto returnType = NUdf::GetDataTypeInfo(res).TypeId;
    std::vector<NUdf::TDataTypeId> argTypes({type1, type2});

    auto k = std::make_unique<arrow::compute::ScalarKernel>(
        std::vector<arrow::compute::InputType>{
            GetPrimitiveInputArrowType(arg1),
            GetPrimitiveInputArrowType(arg2)},
        GetPrimitiveOutputArrowType(res),
        exec);

    switch (nullMode) {
        case TKernel::ENullMode::Default:
            k->null_handling = arrow::compute::NullHandling::INTERSECTION;
            break;
        case TKernel::ENullMode::AlwaysNull:
            k->null_handling = arrow::compute::NullHandling::COMPUTED_PREALLOCATE;
            break;
        case TKernel::ENullMode::AlwaysNotNull:
            k->null_handling = arrow::compute::NullHandling::OUTPUT_NOT_NULL;
            break;
    }

    owner.Adopt(argTypes, returnType, std::make_unique<TPlainKernel>(owner, argTypes, returnType, std::move(k), nullMode));
}

} // namespace NMiniKQL
} // namespace NKikimr
