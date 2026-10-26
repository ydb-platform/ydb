#include "custom_registry.h"
#include "execution.h"
#include "stream_logic.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernel.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <yql/essentials/core/arrow_kernels/registry/registry.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<TExecutionResult> TStreamLogicProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    AFL_VERIFY(context.GetResources().GetAccessorOptional(GetOutputColumnIdOnce()));
    return TExecutionResult::Done();
}

// Returns whether no further inputs can change the result.
bool TStreamLogicProcessor::HandleMonoValue(const bool monoValue, const bool isAccumulatorInput,
    const std::shared_ptr<IChunkedArray>& accumulatedResult, const TProcessorContext& context) const {
    const bool isAbsorbingValue = Operation == NKernels::EOperation::And ? !monoValue : monoValue;
    if ((!isAbsorbingValue && accumulatedResult) || (isAbsorbingValue && isAccumulatorInput)) {
        return isAbsorbingValue;
    }

    const auto recordsCount = context.GetResources().GetRecordsCountRobustVerified();
    auto accessor = monoValue ? NAccessor::TSparsedArray::BuildTrueArrayUI8(recordsCount)
                              : NAccessor::TSparsedArray::BuildFalseArrayUI8(recordsCount);
    if (accumulatedResult) {
        context.MutableResources().Upsert(GetOutputColumnIdOnce(), accessor, false);
    } else {
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(), accessor, false);
    }
    return isAbsorbingValue;
}

TConclusion<bool> TStreamLogicProcessor::CombineInput(const ui32 inputId, const TProcessorContext& context) const {
    auto result = Function->Call(TColumnChainInfo::BuildVector({ GetOutputColumnIdOnce(), inputId }), context.GetResources());
    if (result.IsFail()) {
        return result;
    }
    const auto accessor = result.DetachResult().GetAccessorVerified();
    context.MutableResources().Remove(GetOutputColumnIdOnce());
    context.MutableResources().AddVerified(GetOutputColumnIdOnce(), accessor, false);
    return IsFinishAccessor(accessor);
}

TConclusion<bool> TStreamLogicProcessor::OnInputReady(
    const ui32 inputId, const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    AFL_VERIFY(!context.GetResources().HasMarker(FinishMarker));
    const auto& accumulatedResult = context.GetResources().GetAccessorOptional(GetOutputColumnIdOnce());
    std::optional<bool> monoResult;
    if (accumulatedResult) {
        auto isMonoResult = GetMonoInput(accumulatedResult);
        if (isMonoResult.IsFail()) {
            return isMonoResult;
        }
        monoResult = isMonoResult.DetachResult();
    }

    if (monoResult && HandleMonoValue(*monoResult, true, accumulatedResult, context)) {
        return true;
    }

    if (auto accessorInput = context.GetResources().GetAccessorOptional(inputId)) {
        TConclusion<std::optional<bool>> isMonoInput = GetMonoInput(accessorInput);
        if (isMonoInput.IsFail()) {
            return isMonoInput;
        }
        if (isMonoInput.GetResult()) {
            return HandleMonoValue(*isMonoInput.GetResult(), false, accumulatedResult, context);
        }
        if (!accumulatedResult) {
            AFL_VERIFY(accessorInput->GetDataType()->id() == arrow::uint8()->id())("type", accessorInput->GetDataType()->ToString());
            context.MutableResources().AddVerified(GetOutputColumnIdOnce(), accessorInput, false);
        } else if (monoResult) {
            AFL_VERIFY(accessorInput->GetDataType()->id() == arrow::uint8()->id())("type", accessorInput->GetDataType()->ToString());
            context.MutableResources().Upsert(GetOutputColumnIdOnce(), accessorInput, false);
            return IsFinishAccessor(accessorInput);
        } else {
            return CombineInput(inputId, context);
        }
        return false;
    } else {
        const auto& scalarInput = context.GetResources().GetConstantScalarVerified(inputId);
        if (!scalarInput->is_valid) {
            AFL_VERIFY(scalarInput->type->id() == arrow::uint8()->id())("type", scalarInput->type->ToString());
            if (!accumulatedResult) {
                context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
                    std::make_shared<NAccessor::TTrivialArray>(TStatusValidator::GetValid(arrow::MakeArrayFromScalar(
                        *scalarInput, context.GetResources().GetRecordsCountRobustVerified()))), false);
                return false;
            }
            return CombineInput(inputId, context);
        }

        TConclusion<bool> isMonoInput = GetMonoInput(scalarInput);
        if (isMonoInput.IsFail()) {
            return isMonoInput;
        }

        return HandleMonoValue(isMonoInput.GetResult(), false, accumulatedResult, context);
    }
}

ui64 TStreamLogicProcessor::DoGetWeight() const {
    return 1;
}

class TSpecFunctionsOperator {
private:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> Registry;
    NMiniKQL::TComputationNodeFactory Factory;
    std::vector<std::shared_ptr<const arrow::compute::ScalarKernel>> Kernels;
    std::vector<std::shared_ptr<const arrow::compute::ScalarFunction>> Functions;

public:
    const std::shared_ptr<const arrow::compute::ScalarFunction>& GetANDKernel() const {
        return Functions[0];
    }

    const std::shared_ptr<const arrow::compute::ScalarFunction>& GetORKernel() const {
        return Functions[1];
    }

    TSpecFunctionsOperator() {
        auto mutableRegistry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
        NMiniKQL::FillStaticModules(*mutableRegistry);
        Registry = mutableRegistry;
        Factory = NMiniKQL::GetBuiltinFactory();
        NYql::TKernelRequestBuilder b(*Registry);

        NYql::TExprContext ctx;

        auto boolType = ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool);
        auto blockBoolType = ctx.template MakeType<NYql::TBlockExprType>(boolType);
        auto scalarBoolType = ctx.template MakeType<NYql::TScalarExprType>(boolType);
        constexpr size_t kernelsPerOperation = 4;
        const auto operations = {NYql::TKernelRequestBuilder::EBinaryOp::And, NYql::TKernelRequestBuilder::EBinaryOp::Or};
        for (const auto op : operations) {
            b.AddBinaryOp(op, blockBoolType, blockBoolType, blockBoolType);
            b.AddBinaryOp(op, blockBoolType, scalarBoolType, blockBoolType);
            b.AddBinaryOp(op, scalarBoolType, blockBoolType, blockBoolType);
            b.AddBinaryOp(op, scalarBoolType, scalarBoolType, scalarBoolType);
        }
        auto s = b.Serialize();
        Kernels = NYql::LoadKernels(s, *Registry, Factory);
        AFL_VERIFY(Kernels.size() == operations.size() * kernelsPerOperation);

        for (size_t operationIndex = 0; operationIndex < operations.size(); ++operationIndex) {
            const auto& firstKernel = Kernels[operationIndex * kernelsPerOperation];
            arrow::compute::Arity arity(firstKernel->signature->in_types().size(), firstKernel->signature->is_varargs());
            auto func = std::make_shared<arrow::compute::ScalarFunction>("local_function", arity, nullptr);
            for (size_t shapeIndex = 0; shapeIndex < kernelsPerOperation; ++shapeIndex) {
                TStatusValidator::Validate(func->AddKernel(*Kernels[operationIndex * kernelsPerOperation + shapeIndex]));
            }
            Functions.push_back(func);
        }
    }
};

TStreamLogicProcessor::TStreamLogicProcessor(
    std::vector<TColumnChainInfo>&& input, const TColumnChainInfo& output, const NKernels::EOperation op)
    : TBase(std::move(input), { output }, EProcessorType::StreamLogic)
    , Operation(op) {
    if (Operation == NKernels::EOperation::And) {
        Function = std::make_shared<TKernelFunction>(Singleton<TSpecFunctionsOperator>()->GetANDKernel());
    } else if (Operation == NKernels::EOperation::Or) {
        Function = std::make_shared<TKernelFunction>(Singleton<TSpecFunctionsOperator>()->GetORKernel());
    } else {
        AFL_VERIFY(false);
    }
}

NJson::TJsonValue TStreamLogicProcessor::DoDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("op", ::ToString(Operation));
    return result;
}

bool TStreamLogicProcessor::IsFinishAccessor(const std::shared_ptr<IChunkedArray>& accessor) const {
    const auto arrChecker = [&](const arrow::Array& arr) {
        AFL_VERIFY(arr.type()->id() == arrow::uint8()->id());
        const arrow::UInt8Array& ui8Arr = static_cast<const arrow::UInt8Array&>(arr);
        const ui8* values = ui8Arr.raw_values();
        if (Operation == NKernels::EOperation::And) {
            for (ui32 i = 0; i < ui8Arr.length(); ++i) {
                if (ui8Arr.IsNull(i) || values[i] != 0) {
                    return false;
                }
            }
        } else if (Operation == NKernels::EOperation::Or) {
            for (ui32 i = 0; i < ui8Arr.length(); ++i) {
                if (ui8Arr.IsNull(i) || values[i] == 0) {
                    return false;
                }
            }
        } else {
            AFL_VERIFY(false)("op", Operation);
        }
        return true;
    };
    bool result = true;
    accessor->VisitDistinctValues([&](const std::shared_ptr<arrow::Array>& array) {
        if (!arrChecker(*array)) {
            result = false;
        }
    });
    return result;
}

TConclusion<std::optional<bool>> TStreamLogicProcessor::GetMonoInput(const std::shared_ptr<IChunkedArray>& inputArray) const {
    std::shared_ptr<arrow::Scalar> monoValue;
    const auto isMonoValue = inputArray->CheckOneValueAccessor(monoValue);
    if (!isMonoValue || !*isMonoValue || !monoValue->is_valid) {
        return std::optional<bool>();
    }
    const auto isFalseConclusion = ScalarIsFalse(monoValue);
    if (isFalseConclusion.IsFail()) {
        return isFalseConclusion;
    }
    return !*isFalseConclusion;
}

TConclusion<bool> TStreamLogicProcessor::GetMonoInput(const std::shared_ptr<arrow::Scalar>& scalar) const {
    AFL_VERIFY(scalar);
    AFL_VERIFY(scalar->type->id() == arrow::uint8()->id())("type", scalar->type->ToString());
    return static_cast<const arrow::UInt8Scalar*>(scalar.get())->value != 0;
}

}   // namespace NKikimr::NArrow::NSSA
