#include "mkql_functions.h"
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_function_metadata.h>

#include <arrow/datum.h>
#include <arrow/visitor.h>
#include <arrow/compute/registry.h>
#include <arrow/compute/function.h>
#include <arrow/compute/cast.h>

namespace NKikimr::NMiniKQL {

bool ConvertInputArrowType(TType* blockType, arrow::ValueDescr& descr) {
    auto asBlockType = AS_TYPE(TBlockType, blockType);
    descr.shape = asBlockType->GetShape() == TBlockType::EShape::Scalar ? arrow::ValueDescr::SCALAR : arrow::ValueDescr::ARRAY;
    return ConvertArrowType(asBlockType->GetItemType(), descr.type);
}

class TOutputTypeVisitor : public arrow::TypeVisitor
{
public:
    TOutputTypeVisitor(TTypeEnvironment& env)
        : Env_(env)
    {}

    arrow::Status Visit(const arrow::BooleanType&) {
        SetDataType(NUdf::EDataSlot::Bool);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int8Type&) {
        SetDataType(NUdf::EDataSlot::Int8);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::UInt8Type&) {
        SetDataType(NUdf::EDataSlot::Uint8);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int16Type&) {
        SetDataType(NUdf::EDataSlot::Int16);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::UInt16Type&) {
        SetDataType(NUdf::EDataSlot::Uint16);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int32Type&) {
        SetDataType(NUdf::EDataSlot::Int32);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::UInt32Type&) {
        SetDataType(NUdf::EDataSlot::Uint32);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int64Type&) {
        SetDataType(NUdf::EDataSlot::Int64);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::UInt64Type&) {
        SetDataType(NUdf::EDataSlot::Uint64);
        return arrow::Status::OK();
    }

    TType* GetType() const {
        return Type_;
    }

private:
    void SetDataType(NUdf::EDataSlot slot) {
        Type_ = TDataType::Create(NUdf::GetDataTypeInfo(slot).TypeId, Env_);
    }

private:
    TTypeEnvironment& Env_;
    TType* Type_ = nullptr;
};

bool ConvertOutputArrowType(const arrow::compute::OutputType& outType, const std::vector<arrow::ValueDescr>& values,
    bool optional, TType*& outputType, TTypeEnvironment& env) {
    arrow::ValueDescr::Shape shape;
    std::shared_ptr<arrow::DataType> dataType;

    auto execContext = arrow::compute::ExecContext();
    auto kernelContext = arrow::compute::KernelContext(&execContext);
    auto descrRes = outType.Resolve(&kernelContext, values);
    if (!descrRes.ok()) {
        return false;
    }

    const auto& descr = *descrRes;
    dataType = descr.type;
    shape = descr.shape;

    TOutputTypeVisitor visitor(env);
    if (!dataType->Accept(&visitor).ok()) {
        return false;
    }

    TType* itemType = visitor.GetType();
    if (optional) {
        itemType = TOptionalType::Create(itemType, env);
    }

    switch (shape) {
    case arrow::ValueDescr::SCALAR:
        outputType = TBlockType::Create(itemType, TBlockType::EShape::Scalar, env);
        return true;
    case arrow::ValueDescr::ARRAY:
        outputType = TBlockType::Create(itemType, TBlockType::EShape::Many, env);
        return true;
    default:
        return false;
    }
}

bool FindArrowFunction(TStringBuf name, const TArrayRef<TType*>& inputTypes, TType* outputType, const IBuiltinFunctionRegistry& registry) {
    bool hasOptionals = false;
    bool many = false;
    std::vector<NUdf::TDataTypeId> argTypes;
    for (const auto& t : inputTypes) {
        auto asBlockType = AS_TYPE(TBlockType, t);
        if (asBlockType->GetShape() == TBlockType::EShape::Many) {
            many = true;
        }

        bool isOptional;
        auto baseType = UnpackOptional(asBlockType->GetItemType(), isOptional);
        if (!baseType->IsData()) {
            return false;
        }

        hasOptionals = hasOptionals || isOptional;
        argTypes.push_back(AS_TYPE(TDataType, baseType)->GetSchemeType());
    }

    NUdf::TDataTypeId returnType;
    bool returnIsOptional;
    {
        auto asBlockType = AS_TYPE(TBlockType, outputType);
        MKQL_ENSURE(many ^ (asBlockType->GetShape() == TBlockType::EShape::Scalar), "Output shape is inconsistent with input shapes");
        auto baseType = UnpackOptional(asBlockType->GetItemType(), returnIsOptional);
        if (!baseType->IsData()) {
            return false;
        }
        returnType = AS_TYPE(TDataType, baseType)->GetSchemeType();
    }

    auto kernel = registry.FindKernel(name, argTypes.data(), argTypes.size(), returnType);
    if (!kernel) {
        return false;
    }

    bool match = false;
    switch (kernel->NullMode) {
        case TKernel::ENullMode::Default:
            match = returnIsOptional == hasOptionals;
            break;
        case TKernel::ENullMode::AlwaysNull:
            match = returnIsOptional;
            break;
        case TKernel::ENullMode::AlwaysNotNull:
            match = !returnIsOptional;
            break;
    }
    return match;
}

bool HasArrowCast(TType* from, TType* to) {
    std::shared_ptr<arrow::DataType> fromArrowType, toArrowType;
    if (!ConvertArrowType(from, fromArrowType)) {
        return false;
    }

    if (!ConvertArrowType(to, toArrowType)) {
        return false;
    }

    return arrow::compute::CanCast(*fromArrowType, *toArrowType);
}

}
