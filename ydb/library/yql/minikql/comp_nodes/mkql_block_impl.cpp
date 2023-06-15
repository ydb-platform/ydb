#include "mkql_block_impl.h"
#include "mkql_block_builder.h"
#include "mkql_block_reader.h"

#include <ydb/library/yql/minikql/arrow/mkql_functions.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/public/udf/arrow/args_dechunker.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

#include <arrow/compute/exec_internal.h>

namespace NKikimr::NMiniKQL {

arrow::Datum ConvertScalar(TType* type, const NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool) {
    if (!value) {
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(type, arrowType), "Unsupported type of scalar");
        return arrow::MakeNullScalar(arrowType);
    }

    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(type, arrowType), "Unsupported type of scalar");

        std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            arrowValue.emplace_back(ConvertScalar(tupleType->GetElementType(i), value.GetElement(i), pool).scalar());
        }

        return arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, arrowType));
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
            return arrow::Datum(static_cast<int8_t>(value.Get<i8>()));
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            return arrow::Datum(static_cast<uint8_t>(value.Get<ui8>()));
        case NUdf::EDataSlot::Int16:
            return arrow::Datum(static_cast<int16_t>(value.Get<i16>()));
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return arrow::Datum(static_cast<uint16_t>(value.Get<ui16>()));
        case NUdf::EDataSlot::Int32:
            return arrow::Datum(static_cast<int32_t>(value.Get<i32>()));
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return arrow::Datum(static_cast<uint32_t>(value.Get<ui32>()));
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return arrow::Datum(static_cast<int64_t>(value.Get<i64>()));
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return arrow::Datum(static_cast<uint64_t>(value.Get<ui64>()));
        case NUdf::EDataSlot::Float:
            return arrow::Datum(static_cast<float>(value.Get<float>()));
        case NUdf::EDataSlot::Double:
            return arrow::Datum(static_cast<double>(value.Get<double>()));
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8: {
            const auto& str = value.AsStringRef();
            std::shared_ptr<arrow::Buffer> buffer(ARROW_RESULT(arrow::AllocateBuffer(str.Size(), &pool)));
            std::memcpy(buffer->mutable_data(), str.Data(), str.Size());
            auto type = (slot == NUdf::EDataSlot::String) ? arrow::binary() : arrow::utf8();
            std::shared_ptr<arrow::Scalar> scalar = std::make_shared<arrow::BinaryScalar>(buffer, type);
            return arrow::Datum(scalar);
        }
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }
    }

    if (type->IsPg()) {
        return NYql::MakePgScalar(AS_TYPE(TPgType, type), value, pool);
    }

    MKQL_ENSURE(false, "Unsupported type");
}

arrow::Datum MakeArrayFromScalar(const arrow::Scalar& scalar, size_t len, TType* type, arrow::MemoryPool& pool) {
    MKQL_ENSURE(len > 0, "Invalid block size");
    auto reader = MakeBlockReader(TTypeInfoHelper(), type);
    auto builder = MakeArrayBuilder(TTypeInfoHelper(), type, pool, len, nullptr);

    auto scalarItem = reader->GetScalarItem(scalar);
    for (size_t i = 0; i < len; ++i) {
        builder->Add(scalarItem);
    }

    return builder->Build(true);
}

arrow::ValueDescr ToValueDescr(TType* type) {
    arrow::ValueDescr ret;
    MKQL_ENSURE(ConvertInputArrowType(type, ret), "can't get arrow type");
    return ret;
}

std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types) {
    std::vector<arrow::ValueDescr> res;
    res.reserve(types.size());
    for (const auto& type : types) {
        res.emplace_back(ToValueDescr(type));
    }

    return res;
}

std::vector<arrow::compute::InputType> ConvertToInputTypes(const TVector<TType*>& argTypes) {
    std::vector<arrow::compute::InputType> result;
    result.reserve(argTypes.size());
    for (auto& type : argTypes) {
        result.emplace_back(ToValueDescr(type));
    }
    return result;
}

arrow::compute::OutputType ConvertToOutputType(TType* output) {
    return arrow::compute::OutputType(ToValueDescr(output));
}

TBlockFuncNode::TBlockFuncNode(TComputationMutables& mutables, TStringBuf name, TVector<IComputationNode*>&& argsNodes,
    const TVector<TType*>& argsTypes, const arrow::compute::ScalarKernel& kernel,
    std::shared_ptr<arrow::compute::ScalarKernel> kernelHolder,
    const arrow::compute::FunctionOptions* functionOptions)
    : TMutableComputationNode(mutables)
    , StateIndex(mutables.CurValueIndex++)
    , ArgsNodes(std::move(argsNodes))
    , ArgsValuesDescr(ToValueDescr(argsTypes))
    , Kernel(kernel)
    , KernelHolder(std::move(kernelHolder))
    , Options(functionOptions)
    , ScalarOutput(GetResultShape(argsTypes) == TBlockType::EShape::Scalar)
    , Name(name.starts_with("Block") ? name.substr(5) : name)
{
}

NUdf::TUnboxedValuePod TBlockFuncNode::DoCalculate(TComputationContext& ctx) const {
    auto& state = GetState(ctx);

    std::vector<arrow::Datum> argDatums;
    for (ui32 i = 0; i < ArgsNodes.size(); ++i) {
        argDatums.emplace_back(TArrowBlock::From(ArgsNodes[i]->GetValue(ctx)).GetDatum());
        Y_VERIFY_DEBUG(ArgsValuesDescr[i] == argDatums.back().descr());
    }

    auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
    ARROW_OK(executor->Init(&state.KernelContext, { &Kernel, ArgsValuesDescr, Options }));

    if (ScalarOutput) {
        auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
        ARROW_OK(executor->Execute(argDatums, listener.get()));
        auto output = executor->WrapResults(argDatums, listener->values());
        return ctx.HolderFactory.CreateArrowBlock(std::move(output));
    }

    NYql::NUdf::TArgsDechunker dechunker(std::move(argDatums));
    std::vector<arrow::Datum> chunk;
    TVector<std::shared_ptr<arrow::ArrayData>> arrays;

    while (dechunker.Next(chunk)) {
        arrow::compute::detail::DatumAccumulator listener;
        ARROW_OK(executor->Execute(chunk, &listener));
        auto output = executor->WrapResults(chunk, listener.values());

        ForEachArrayData(output, [&](const auto& arr) { arrays.push_back(arr); });
    }

    return ctx.HolderFactory.CreateArrowBlock(MakeArray(arrays));
}


void TBlockFuncNode::RegisterDependencies() const {
    for (const auto& arg : ArgsNodes) {
        DependsOn(arg);
    }
}

TBlockFuncNode::TState& TBlockFuncNode::GetState(TComputationContext& ctx) const {
    auto& result = ctx.MutableValues[StateIndex];
    if (!result.HasValue()) {
        result = ctx.HolderFactory.Create<TState>(Options, Kernel, ArgsValuesDescr, ctx);
    }

    return *static_cast<TState*>(result.AsBoxed().Get());
}

std::unique_ptr<IArrowKernelComputationNode> TBlockFuncNode::PrepareArrowKernelComputationNode(TComputationContext& ctx) const {
    return std::make_unique<TArrowNode>(this);
}

TBlockFuncNode::TArrowNode::TArrowNode(const TBlockFuncNode* parent)
    : Parent_(parent)
{}

TStringBuf TBlockFuncNode::TArrowNode::GetKernelName() const {
    return Parent_->Name;
}

const arrow::compute::ScalarKernel& TBlockFuncNode::TArrowNode::GetArrowKernel() const {
    return Parent_->Kernel;
}

const std::vector<arrow::ValueDescr>& TBlockFuncNode::TArrowNode::GetArgsDesc() const {
    return Parent_->ArgsValuesDescr;
}

const IComputationNode* TBlockFuncNode::TArrowNode::GetArgument(ui32 index) const {
    MKQL_ENSURE(index < Parent_->ArgsNodes.size(), "Wrong index");
    return Parent_->ArgsNodes[index];
}

}
