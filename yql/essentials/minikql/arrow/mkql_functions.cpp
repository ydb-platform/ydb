#include "mkql_functions.h"
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_function_metadata.h>

#include <arrow/datum.h>
#include <arrow/visitor.h>
#include <arrow/compute/registry.h>
#include <arrow/compute/function.h>
#include <arrow/compute/cast.h>

namespace NKikimr::NMiniKQL {

bool ConvertInputArrowType(TType* blockType, arrow20::ValueDescr& descr) {
    auto asBlockType = AS_TYPE(TBlockType, blockType);
    descr.shape = asBlockType->GetShape() == TBlockType::EShape::Scalar ? arrow20::ValueDescr::SCALAR : arrow20::ValueDescr::ARRAY;
    return ConvertArrowType(asBlockType->GetItemType(), descr.type);
}

class TOutputTypeVisitor: public arrow20::TypeVisitor {
public:
    explicit TOutputTypeVisitor(TTypeEnvironment& env)
        : Env_(env)
    {
    }

    arrow20::Status Visit(const arrow20::BooleanType&) override {
        SetDataType(NUdf::EDataSlot::Bool);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::Int8Type&) override {
        SetDataType(NUdf::EDataSlot::Int8);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::UInt8Type&) override {
        SetDataType(NUdf::EDataSlot::Uint8);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::Int16Type&) override {
        SetDataType(NUdf::EDataSlot::Int16);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::UInt16Type&) override {
        SetDataType(NUdf::EDataSlot::Uint16);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::Int32Type&) override {
        SetDataType(NUdf::EDataSlot::Int32);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::UInt32Type&) override {
        SetDataType(NUdf::EDataSlot::Uint32);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::Int64Type&) override {
        SetDataType(NUdf::EDataSlot::Int64);
        return arrow20::Status::OK();
    }

    arrow20::Status Visit(const arrow20::UInt64Type&) override {
        SetDataType(NUdf::EDataSlot::Uint64);
        return arrow20::Status::OK();
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

bool ConvertOutputArrowType(const arrow20::compute::OutputType& outType, const std::vector<arrow20::ValueDescr>& values,
                            bool optional, TType*& outputType, TTypeEnvironment& env) {
    arrow20::ValueDescr::Shape shape;
    std::shared_ptr<arrow20::DataType> dataType;

    auto execContext = arrow20::compute::ExecContext();
    auto kernelContext = arrow20::compute::KernelContext(&execContext);
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
        case arrow20::ValueDescr::SCALAR:
            outputType = TBlockType::Create(itemType, TBlockType::EShape::Scalar, env);
            return true;
        case arrow20::ValueDescr::ARRAY:
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
    std::shared_ptr<arrow20::DataType> fromArrowType, toArrowType;
    if (!ConvertArrowType(from, fromArrowType)) {
        return false;
    }

    if (!ConvertArrowType(to, toArrowType)) {
        return false;
    }

    return arrow20::compute::CanCast(*fromArrowType, *toArrowType);
}

} // namespace NKikimr::NMiniKQL
