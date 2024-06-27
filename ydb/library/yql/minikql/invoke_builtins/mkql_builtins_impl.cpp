#include "mkql_builtins_impl.h"  // Y_IGNORE

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

} // namespace NMiniKQL
} // namespace NKikimr
