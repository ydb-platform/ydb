#include "mkql_builtins_impl.h"  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_builder.h> // UnpackOptionalData

namespace NKikimr {
namespace NMiniKQL {

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
    case NUdf::EDataSlot::Bool: return GetPrimitiveInputArrowType<bool>();
    case NUdf::EDataSlot::Int8: return GetPrimitiveInputArrowType<i8>();
    case NUdf::EDataSlot::Uint8: return GetPrimitiveInputArrowType<ui8>();
    case NUdf::EDataSlot::Int16: return GetPrimitiveInputArrowType<i16>();
    case NUdf::EDataSlot::Uint16: return GetPrimitiveInputArrowType<ui16>();
    case NUdf::EDataSlot::Int32: return GetPrimitiveInputArrowType<i32>();
    case NUdf::EDataSlot::Uint32: return GetPrimitiveInputArrowType<ui32>();
    case NUdf::EDataSlot::Int64: return GetPrimitiveInputArrowType<i64>();
    case NUdf::EDataSlot::Uint64: return GetPrimitiveInputArrowType<ui64>();
    case NUdf::EDataSlot::Float: return GetPrimitiveInputArrowType<float>();
    case NUdf::EDataSlot::Double: return GetPrimitiveInputArrowType<double>();
    case NUdf::EDataSlot::String: return GetPrimitiveInputArrowType<char*>();
    case NUdf::EDataSlot::Utf8: return GetPrimitiveInputArrowType<NYql::NUdf::TUtf8>();
    case NUdf::EDataSlot::Date: return GetPrimitiveInputArrowType<ui16>();
    case NUdf::EDataSlot::TzDate: return GetPrimitiveInputArrowType<ui16>(true);
    case NUdf::EDataSlot::Datetime: return GetPrimitiveInputArrowType<ui32>();
    case NUdf::EDataSlot::TzDatetime: return GetPrimitiveInputArrowType<ui32>(true);
    case NUdf::EDataSlot::Timestamp: return GetPrimitiveInputArrowType<ui64>();
    case NUdf::EDataSlot::TzTimestamp: return GetPrimitiveInputArrowType<ui64>(true);
    case NUdf::EDataSlot::Interval: return GetPrimitiveInputArrowType<i64>();
    case NUdf::EDataSlot::Date32: return GetPrimitiveInputArrowType<i32>();
    case NUdf::EDataSlot::TzDate32: return GetPrimitiveInputArrowType<i32>(true);
    case NUdf::EDataSlot::Datetime64: return GetPrimitiveInputArrowType<i64>();
    case NUdf::EDataSlot::TzDatetime64: return GetPrimitiveInputArrowType<i64>(true);
    case NUdf::EDataSlot::Timestamp64: return GetPrimitiveInputArrowType<i64>();
    case NUdf::EDataSlot::TzTimestamp64: return GetPrimitiveInputArrowType<i64>(true);
    case NUdf::EDataSlot::Interval64: return GetPrimitiveInputArrowType<i64>();
    case NUdf::EDataSlot::Decimal: return GetPrimitiveInputArrowType<NYql::NDecimal::TInt128>();
    default:
        ythrow yexception() << "Unexpected data slot: " << slot;
    }
}

arrow::compute::OutputType GetPrimitiveOutputArrowType(NUdf::EDataSlot slot) {
    switch (slot) {
    case NUdf::EDataSlot::Bool: return GetPrimitiveOutputArrowType<bool>();
    case NUdf::EDataSlot::Int8: return GetPrimitiveOutputArrowType<i8>();
    case NUdf::EDataSlot::Uint8: return GetPrimitiveOutputArrowType<ui8>();
    case NUdf::EDataSlot::Int16: return GetPrimitiveOutputArrowType<i16>();
    case NUdf::EDataSlot::Uint16: return GetPrimitiveOutputArrowType<ui16>();
    case NUdf::EDataSlot::Int32: return GetPrimitiveOutputArrowType<i32>();
    case NUdf::EDataSlot::Uint32: return GetPrimitiveOutputArrowType<ui32>();
    case NUdf::EDataSlot::Int64: return GetPrimitiveOutputArrowType<i64>();
    case NUdf::EDataSlot::Uint64: return GetPrimitiveOutputArrowType<ui64>();
    case NUdf::EDataSlot::Float: return GetPrimitiveOutputArrowType<float>();
    case NUdf::EDataSlot::Double: return GetPrimitiveOutputArrowType<double>();
    case NUdf::EDataSlot::String: return GetPrimitiveOutputArrowType<char*>();
    case NUdf::EDataSlot::Utf8: return GetPrimitiveOutputArrowType<NYql::NUdf::TUtf8>();
    case NUdf::EDataSlot::Date: return GetPrimitiveOutputArrowType<ui16>();
    case NUdf::EDataSlot::TzDate: return GetPrimitiveOutputArrowType<ui16>(true);
    case NUdf::EDataSlot::Datetime: return GetPrimitiveOutputArrowType<ui32>();
    case NUdf::EDataSlot::TzDatetime: return GetPrimitiveOutputArrowType<ui32>(true);
    case NUdf::EDataSlot::Timestamp: return GetPrimitiveOutputArrowType<ui64>();
    case NUdf::EDataSlot::TzTimestamp: return GetPrimitiveOutputArrowType<ui64>(true);
    case NUdf::EDataSlot::Interval: return GetPrimitiveOutputArrowType<i64>();
    case NUdf::EDataSlot::Date32: return GetPrimitiveOutputArrowType<i32>();
    case NUdf::EDataSlot::TzDate32: return GetPrimitiveOutputArrowType<i32>(true);
    case NUdf::EDataSlot::Datetime64: return GetPrimitiveOutputArrowType<i64>();
    case NUdf::EDataSlot::TzDatetime64: return GetPrimitiveOutputArrowType<i64>(true);
    case NUdf::EDataSlot::Timestamp64: return GetPrimitiveOutputArrowType<i64>();
    case NUdf::EDataSlot::TzTimestamp64: return GetPrimitiveOutputArrowType<i64>(true);
    case NUdf::EDataSlot::Interval64: return GetPrimitiveOutputArrowType<i64>();
    case NUdf::EDataSlot::Decimal: return GetPrimitiveOutputArrowType<NYql::NDecimal::TInt128>();
    default:
        ythrow yexception() << "Unexpected data slot: " << slot;
    }
}

std::shared_ptr<arrow::DataType> AddTzType(bool addTz, const std::shared_ptr<arrow::DataType>& type) {
    if (!addTz) {
        return type;
    }

    std::vector<std::shared_ptr<arrow::Field>> fields {
        std::make_shared<arrow::Field>("datetime", type, false),
        std::make_shared<arrow::Field>("timezoneId", arrow::uint16(), false)
    };

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

    const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(propagateTz == EPropagateTz::FromLeft ? *input1 : *input2);
    const auto tzId = structScalar.value[1];
    return std::make_shared<arrow::StructScalar>(arrow::StructScalar::ValueType{value,tzId}, propagateTz == EPropagateTz::FromLeft ? input1->type : input2->type);
}    

std::shared_ptr<arrow::ArrayData> CopyTzImpl(const std::shared_ptr<arrow::ArrayData>& res, bool propagateTz,
    const std::shared_ptr<arrow::ArrayData>& input, arrow::MemoryPool* pool,
    size_t sizeOf, const std::shared_ptr<arrow::DataType>& outputType) {
    if (!propagateTz) {
        return res;
    }

    Y_ENSURE(res->child_data.empty());
    std::shared_ptr<arrow::Buffer> buffer(NUdf::AllocateResizableBuffer(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, { nullptr, buffer }));
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
    std::shared_ptr<arrow::Buffer> buffer(NUdf::AllocateResizableBuffer(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, { nullptr, buffer }));
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
    std::shared_ptr<arrow::Buffer> buffer(NUdf::AllocateResizableBuffer(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, { nullptr, buffer }));
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
    std::shared_ptr<arrow::Buffer> buffer(NUdf::AllocateResizableBuffer(sizeOf * res->length, pool));
    res->child_data.push_back(arrow::ArrayData::Make(outputType, res->length, { nullptr, buffer }));
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
    auto resultDataType = UnpackOptionalData(static_cast<TBlockType*>(resultType)->GetItemType(), isOptional);

    MKQL_ENSURE(*dataType1->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");
    MKQL_ENSURE(*dataType2->GetDataSlot() == NUdf::EDataSlot::Decimal, "Require decimal");
    
    auto decimalType1 = static_cast<TDataDecimalType*>(dataType1);
    auto decimalType2 = static_cast<TDataDecimalType*>(dataType2);

    MKQL_ENSURE(decimalType1->GetParams() == decimalType2->GetParams(), "Require same precision/scale");

    ui8 precision = decimalType1->GetParams().first;
    MKQL_ENSURE(precision >= 1&& precision <= 35, TStringBuilder() << "Wrong precision: " << (int)precision);

    auto k = std::make_shared<arrow::compute::ScalarKernel>(std::vector<arrow::compute::InputType>{
        GetPrimitiveInputArrowType(NUdf::EDataSlot::Decimal), GetPrimitiveInputArrowType(NUdf::EDataSlot::Decimal)
    }, GetPrimitiveOutputArrowType(*resultDataType->GetDataSlot()), Exec);
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
    std::vector<NUdf::TDataTypeId> argTypes({ type1 });

    auto k = std::make_unique<arrow::compute::ScalarKernel>(std::vector<arrow::compute::InputType>{
        GetPrimitiveInputArrowType(arg1)
    }, GetPrimitiveOutputArrowType(res), exec);

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
    std::vector<NUdf::TDataTypeId> argTypes({ type1, type2 });

    auto k = std::make_unique<arrow::compute::ScalarKernel>(std::vector<arrow::compute::InputType>{
        GetPrimitiveInputArrowType(arg1), GetPrimitiveInputArrowType(arg2)
    }, GetPrimitiveOutputArrowType(res), exec);

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

arrow::Status ExecScalarImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter, TUntypedUnaryScalarFuncPtr func,
    bool tz, bool propagateTz) {
    if (const auto& arg = batch.values.front(); !arg.scalar()->is_valid) {
        *res = arrow::MakeNullScalar(AddTzType(propagateTz, typeGetter()));
    } else {
        const auto argTz = ExtractTz(tz, arg.scalar());
        const auto valPtr = GetPrimitiveScalarValuePtr(*argTz);
        auto resDatum = scalarGetter();
        const auto resPtr = GetPrimitiveScalarValueMutablePtr(*resDatum.scalar());
        func(valPtr, resPtr);
        *res = WithTz(propagateTz, arg.scalar(), resDatum.scalar());
    }

    return arrow::Status::OK();
}

arrow::Status ExecArrayImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedUnaryArrayFuncPtr func, size_t outputSizeOf, TPrimitiveDataTypeGetter outputTypeGetter,
    bool tz, bool propagateTz) {
    const auto& arg = batch.values.front();
    auto& resArr = *CopyTzImpl(res->array(), propagateTz, arg.array(), kernelCtx->memory_pool(),
        outputSizeOf, outputTypeGetter());

    const auto& arr = *ExtractTz(tz, arg.array());
    auto length = arr.length;
    const auto valPtr = arr.buffers[1]->data();
    auto resPtr = resArr.buffers[1]->mutable_data();
    func(valPtr, resPtr, length, arr.offset);
    return arrow::Status::OK();
}

arrow::Status ExecUnaryImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter,
    bool tz, bool propagateTz, size_t outputSizeOf,
    TUntypedUnaryScalarFuncPtr scalarFunc, TUntypedUnaryArrayFuncPtr arrayFunc) {
    MKQL_ENSURE(batch.values.size() == 1, "Expected single argument");
    const auto& arg = batch.values[0];
    if (arg.is_scalar()) {
        return ExecScalarImpl(batch, res, typeGetter, scalarGetter, scalarFunc, tz, propagateTz);
    } else {
        return ExecArrayImpl(kernelCtx, batch, res, arrayFunc, outputSizeOf, typeGetter, tz, propagateTz);
    }
}

arrow::Status ExecScalarScalarImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter, TUntypedBinaryScalarFuncPtr func,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
        *res = arrow::MakeNullScalar(AddTzType(propagateTz, typeGetter()));
    } else {
        const auto arg1tz = ExtractTz(tz1, arg1.scalar());
        const auto arg2tz = ExtractTz(tz2, arg2.scalar());
        const auto val1Ptr = GetPrimitiveScalarValuePtr(*arg1tz);
        const auto val2Ptr = GetPrimitiveScalarValuePtr(*arg2tz);
        auto resDatum = scalarGetter();
        const auto resPtr = GetPrimitiveScalarValueMutablePtr(*resDatum.scalar());
        func(val1Ptr, val2Ptr, resPtr);
        *res = WithTz(propagateTz, arg1.scalar(), arg2.scalar(), resDatum.scalar());
    }

    return arrow::Status::OK();
}

arrow::Status ExecScalarArrayImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayFuncPtr func, size_t outputSizeOf, TPrimitiveDataTypeGetter outputTypeGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *CopyTzImpl(res->array(), propagateTz, arg1.scalar(), arg2.array(), kernelCtx->memory_pool(),
        outputSizeOf, outputTypeGetter());
    if (arg1.scalar()->is_valid) {
        const auto arg1tz = ExtractTz(tz1, arg1.scalar());
        const auto val1Ptr = GetPrimitiveScalarValuePtr(*arg1tz);
        const auto& arr2 = *ExtractTz(tz2, arg2.array());
        auto length = arr2.length;
        const auto val2Ptr = arr2.buffers[1]->data();
        auto resPtr = resArr.buffers[1]->mutable_data();
        func(val1Ptr, val2Ptr, resPtr, length, 0, arr2.offset);
    }

    return arrow::Status::OK();
}

arrow::Status ExecArrayScalarImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayFuncPtr func, size_t outputSizeOf, TPrimitiveDataTypeGetter outputTypeGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *CopyTzImpl(res->array(), propagateTz, arg1.array(), arg2.scalar(), kernelCtx->memory_pool(),
        outputSizeOf, outputTypeGetter());
    if (arg2.scalar()->is_valid) {
        const auto& arr1 = *ExtractTz(tz1, arg1.array());
        auto length = arr1.length;
        const auto val1Ptr = arr1.buffers[1]->data();
        const auto arg2tz = ExtractTz(tz2, arg2.scalar());
        const auto val2Ptr = GetPrimitiveScalarValuePtr(*arg2tz);
        auto resPtr = resArr.buffers[1]->mutable_data();
        func(val1Ptr, val2Ptr, resPtr, length, arr1.offset, 0);
    }

    return arrow::Status::OK();
}

arrow::Status ExecArrayArrayImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayFuncPtr func, size_t outputSizeOf, TPrimitiveDataTypeGetter outputTypeGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    const auto& arr1 = *ExtractTz(tz1, arg1.array());
    const auto& arr2 = *ExtractTz(tz2, arg2.array());
    auto& resArr = *CopyTzImpl(res->array(), propagateTz, arg1.array(), arg2.array(), kernelCtx->memory_pool(),
        outputSizeOf, outputTypeGetter());
    MKQL_ENSURE(arr1.length == arr2.length, "Expected same length");
    auto length = arr1.length;
    const auto val1Ptr = arr1.buffers[1]->data();
    const auto val2Ptr = arr2.buffers[1]->data();
    auto resPtr = resArr.buffers[1]->mutable_data();
    func(val1Ptr, val2Ptr, resPtr, length, arr1.offset, arr2.offset);
    return arrow::Status::OK();
}

arrow::Status ExecBinaryImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz, size_t outputSizeOf,
    TUntypedBinaryScalarFuncPtr scalarScalarFunc,
    TUntypedBinaryArrayFuncPtr scalarArrayFunc,
    TUntypedBinaryArrayFuncPtr arrayScalarFunc,
    TUntypedBinaryArrayFuncPtr arrayArrayFunc) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (arg1.is_scalar()) {
        if (arg2.is_scalar()) {
            return ExecScalarScalarImpl(batch, res, typeGetter, scalarGetter, scalarScalarFunc, tz1, tz2, propagateTz);
        } else {
            return ExecScalarArrayImpl(kernelCtx, batch, res, scalarArrayFunc, outputSizeOf, typeGetter, tz1, tz2, propagateTz);
        }
    } else {
        if (arg2.is_scalar()) {
            return ExecArrayScalarImpl(kernelCtx, batch, res, arrayScalarFunc, outputSizeOf, typeGetter, tz1, tz2, propagateTz);
        } else {
            return ExecArrayArrayImpl(kernelCtx, batch, res, arrayArrayFunc, outputSizeOf, typeGetter, tz1, tz2, propagateTz);
        }
    }
}

arrow::Status ExecScalarScalarOptImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter, TUntypedBinaryScalarOptFuncPtr func,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
        *res = arrow::MakeNullScalar(AddTzType(propagateTz, typeGetter()));
    } else {
        const auto arg1tz = ExtractTz(tz1, arg1.scalar());
        const auto arg2tz = ExtractTz(tz2, arg2.scalar());
        const auto val1Ptr = GetPrimitiveScalarValuePtr(*arg1tz);
        const auto val2Ptr = GetPrimitiveScalarValuePtr(*arg2tz);
        auto resDatum = scalarGetter();
        const auto resPtr = GetPrimitiveScalarValueMutablePtr(*resDatum.scalar());
        if (!func(val1Ptr, val2Ptr, resPtr)) {
            *res = arrow::MakeNullScalar(AddTzType(propagateTz, typeGetter()));
        } else {
            *res = WithTz(propagateTz, arg1.scalar(), arg2.scalar(), resDatum.scalar());
        }
    }

    return arrow::Status::OK();
}

arrow::Status ExecScalarArrayOptImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayOptFuncPtr func, size_t outputSizeOf, TPrimitiveDataTypeGetter outputTypeGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *CopyTzImpl(res->array(), propagateTz, arg1.scalar(), arg2.array(), kernelCtx->memory_pool(),
        outputSizeOf, outputTypeGetter());
    if (arg1.scalar()->is_valid) {
        const auto arg1tz = ExtractTz(tz1, arg1.scalar());
        const auto val1Ptr = GetPrimitiveScalarValuePtr(*arg1tz);
        const auto& arr2 = *ExtractTz(tz2, arg2.array());
        auto length = arr2.length;
        const auto val2Ptr = arr2.buffers[1]->data();
        const auto nullCount2 = arr2.GetNullCount();
        const auto valid2 = (nullCount2 == 0) ? nullptr : arr2.GetValues<uint8_t>(0);
        auto resPtr = resArr.buffers[1]->mutable_data();
        auto resValid = res->array()->GetMutableValues<uint8_t>(0);
        func(val1Ptr, nullptr, val2Ptr, valid2, resPtr, resValid, length, 0, arr2.offset);
    } else {
        GetBitmap(resArr, 0).SetBitsTo(false);
    }

    return arrow::Status::OK();
}

arrow::Status ExecArrayScalarOptImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayOptFuncPtr func, size_t outputSizeOf, TPrimitiveDataTypeGetter outputTypeGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *CopyTzImpl(res->array(), propagateTz, arg1.array(), arg2.scalar(), kernelCtx->memory_pool(),
        outputSizeOf, outputTypeGetter());
    if (arg2.scalar()->is_valid) {
        const auto& arr1 = *ExtractTz(tz1, arg1.array());
        const auto val1Ptr = arr1.buffers[1]->data();
        auto length = arr1.length;
        const auto nullCount1 = arr1.GetNullCount();
        const auto valid1 = (nullCount1 == 0) ? nullptr : arr1.GetValues<uint8_t>(0);
        const auto arg2tz = ExtractTz(tz2, arg2.scalar());
        const auto val2Ptr = GetPrimitiveScalarValuePtr(*arg2tz);
        auto resPtr = resArr.buffers[1]->mutable_data();
        auto resValid = res->array()->GetMutableValues<uint8_t>(0);
        func(val1Ptr, valid1, val2Ptr, nullptr, resPtr, resValid, length, arr1.offset, 0);
    } else {
        GetBitmap(resArr, 0).SetBitsTo(false);
    }

    return arrow::Status::OK();
}

arrow::Status ExecArrayArrayOptImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayOptFuncPtr func, size_t outputSizeOf, TPrimitiveDataTypeGetter outputTypeGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    const auto& arr1 = *ExtractTz(tz1, arg1.array());
    const auto& arr2 = *ExtractTz(tz2, arg2.array());
    MKQL_ENSURE(arr1.length == arr2.length, "Expected same length");
    auto length = arr1.length;
    const auto val1Ptr = arr1.buffers[1]->data();
    const auto nullCount1 = arr1.GetNullCount();
    const auto valid1 = (nullCount1 == 0) ? nullptr : arr1.GetValues<uint8_t>(0);
    const auto val2Ptr = arr2.buffers[1]->data();
    const auto nullCount2 = arr2.GetNullCount();
    const auto valid2 = (nullCount2 == 0) ? nullptr : arr2.GetValues<uint8_t>(0);
    auto& resArr = *CopyTzImpl(res->array(), propagateTz, arg1.array(), arg2.array(), kernelCtx->memory_pool(),
        outputSizeOf, outputTypeGetter());
    auto resPtr = resArr.buffers[1]->mutable_data();
    auto resValid = res->array()->GetMutableValues<uint8_t>(0);
    func(val1Ptr, valid1, val2Ptr, valid2, resPtr, resValid, length, arr1.offset, arr2.offset);
    return arrow::Status::OK();
}

arrow::Status ExecBinaryOptImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter,
    bool tz1, bool tz2, EPropagateTz propagateTz, size_t outputSizeOf,
    TUntypedBinaryScalarOptFuncPtr scalarScalarFunc,
    TUntypedBinaryArrayOptFuncPtr scalarArrayFunc,
    TUntypedBinaryArrayOptFuncPtr arrayScalarFunc,
    TUntypedBinaryArrayOptFuncPtr arrayArrayFunc) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (arg1.is_scalar()) {
        if (arg2.is_scalar()) {
            return ExecScalarScalarOptImpl(batch, res, typeGetter, scalarGetter, scalarScalarFunc, tz1, tz2, propagateTz);
        } else {
            return ExecScalarArrayOptImpl(kernelCtx, batch, res, scalarArrayFunc, outputSizeOf, typeGetter, tz1, tz2, propagateTz);
        }
    } else {
        if (arg2.is_scalar()) {
            return ExecArrayScalarOptImpl(kernelCtx, batch, res, arrayScalarFunc, outputSizeOf, typeGetter, tz1, tz2, propagateTz);
        } else {
            return ExecArrayArrayOptImpl(kernelCtx, batch, res, arrayArrayFunc, outputSizeOf, typeGetter, tz1, tz2, propagateTz);
        }
    }
}

arrow::Status ExecDecimalArrayScalarOptImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayOptFuncPtr func) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *res->array();
    if (arg2.scalar()->is_valid) {
        const auto& arr1 = *arg1.array();
        const auto val1Ptr = arr1.buffers[1]->data();
        auto length = arr1.length;
        const auto nullCount1 = arr1.GetNullCount();
        const auto valid1 = (nullCount1 == 0) ? nullptr : arr1.GetValues<uint8_t>(0);
        const auto val2Ptr = GetStringScalarValue(*arg2.scalar());
        auto resPtr = resArr.buffers[1]->mutable_data();
        auto resValid = res->array()->GetMutableValues<uint8_t>(0);
        func(val1Ptr, valid1, val2Ptr.data(), nullptr, resPtr, resValid, length, arr1.offset, 0);
    } else {
        GetBitmap(resArr, 0).SetBitsTo(false);
    }

    return arrow::Status::OK();
}

arrow::Status ExecDecimalScalarArrayOptImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedBinaryArrayOptFuncPtr func) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *res->array();
    if (arg1.scalar()->is_valid) {
        const auto val1Ptr = GetStringScalarValue(*arg1.scalar());
        const auto& arr2 = *arg2.array();
        auto length = arr2.length;
        const auto val2Ptr = arr2.buffers[1]->data();
        const auto nullCount2 = arr2.GetNullCount();
        const auto valid2 = (nullCount2 == 0) ? nullptr : arr2.GetValues<uint8_t>(0);
        auto resPtr = resArr.buffers[1]->mutable_data();
        auto resValid = res->array()->GetMutableValues<uint8_t>(0);
        func(val1Ptr.data(), nullptr, val2Ptr, valid2, resPtr, resValid, length, 0, arr2.offset);
    } else {
        GetBitmap(resArr, 0).SetBitsTo(false);
    }

    return arrow::Status::OK();
}

arrow::Status ExecDecimalScalarScalarOptImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetterWithMemPool scalarGetter,
    TUntypedBinaryScalarOptFuncPtr func) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
        *res = arrow::MakeNullScalar(typeGetter());
    } else {
        const auto val1Ptr = GetStringScalarValue(*arg1.scalar());
        const auto val2Ptr = GetStringScalarValue(*arg2.scalar());
        void* resMem;
        auto resDatum = scalarGetter(&resMem, kernelCtx->memory_pool());
        if (!func(val1Ptr.data(), val2Ptr.data(), resMem)) {
            *res = arrow::MakeNullScalar(typeGetter());
        } else {
            *res = resDatum.scalar();
        }
    }

    return arrow::Status::OK();
}

arrow::Status ExecDecimalBinaryOptImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetterWithMemPool scalarGetter,
    size_t outputSizeOf,
    TUntypedBinaryScalarOptFuncPtr scalarScalarFunc,
    TUntypedBinaryArrayOptFuncPtr scalarArrayFunc,
    TUntypedBinaryArrayOptFuncPtr arrayScalarFunc,
    TUntypedBinaryArrayOptFuncPtr arrayArrayFunc) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (arg1.is_scalar()) {
        if (arg2.is_scalar()) {
            return ExecDecimalScalarScalarOptImpl(kernelCtx, batch, res, typeGetter, scalarGetter, scalarScalarFunc);
        } else {
            return ExecDecimalScalarArrayOptImpl(batch, res, scalarArrayFunc);
        }
    } else {
        if (arg2.is_scalar()) {
            return ExecDecimalArrayScalarOptImpl(batch, res, arrayScalarFunc);
        } else {
            return ExecArrayArrayOptImpl(kernelCtx, batch, res, arrayArrayFunc, outputSizeOf, typeGetter, false, false, EPropagateTz::None);
        }
    }
}

arrow::Status ExecDecimalScalarImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TUntypedUnaryScalarFuncPtr func) {
    if (const auto& arg = batch.values.front(); !arg.scalar()->is_valid) {
        *res = arrow::MakeNullScalar(typeGetter());
    } else {
        const auto valPtr = GetPrimitiveScalarValuePtr(*arg.scalar());
        std::shared_ptr<arrow::Buffer> buffer(ARROW_RESULT(arrow::AllocateBuffer(16, kernelCtx->memory_pool())));
        auto resDatum = arrow::Datum(std::make_shared<TPrimitiveDataType<NYql::NDecimal::TInt128>::TScalarResult>(buffer));
        func(valPtr, buffer->mutable_data());
        *res = resDatum.scalar();
    }

    return arrow::Status::OK();
}

arrow::Status ExecDecimalArrayImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedUnaryArrayFuncPtr func) {
    const auto& arg = batch.values.front();
    auto& resArr = *res->array();

    const auto& arr = *arg.array();
    auto length = arr.length;
    const auto valPtr = arr.buffers[1]->data();
    auto resPtr = resArr.buffers[1]->mutable_data();
    func(valPtr, resPtr, length, arr.offset);
    return arrow::Status::OK();
}

arrow::Status ExecDecimalUnaryImpl(arrow::compute::KernelContext* kernelCtx,
    const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter,
    TUntypedUnaryScalarFuncPtr scalarFunc, TUntypedUnaryArrayFuncPtr arrayFunc) {
    MKQL_ENSURE(batch.values.size() == 1, "Expected single argument");
    const auto& arg = batch.values[0];
    if (arg.is_scalar()) {
        return ExecDecimalScalarImpl(kernelCtx, batch, res, typeGetter, scalarFunc);
    } else {
        return ExecDecimalArrayImpl(batch, res, arrayFunc);
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
