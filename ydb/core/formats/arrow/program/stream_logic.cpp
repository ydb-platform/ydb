#include "custom_registry.h"
#include "execution.h"
#include "stream_logic.h"

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

TConclusion<IResourceProcessor::EExecutionResult> TStreamLogicProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    AFL_VERIFY(context.GetResources().GetAccessorOptional(GetOutputColumnIdOnce()));
    return IResourceProcessor::EExecutionResult::Success;
}

TConclusion<bool> TStreamLogicProcessor::OnInputReady(
    const ui32 inputId, const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto accInput = context.GetResources().GetAccessorVerified(inputId);

    AFL_VERIFY(!context.GetResources().HasMarker(FinishMarker));
    const auto accResult = context.GetResources().GetAccessorOptional(GetOutputColumnIdOnce());

    TConclusion<std::optional<bool>> isMonoInput = GetMonoInput(accInput);
    if (isMonoInput.IsFail()) {
        return isMonoInput;
    }

    if (isMonoInput.GetResult()) {
        const bool monoValue = *isMonoInput.GetResult();
        if (Operation == NKernels::EOperation::And) {
            if (monoValue) {
                if (!accResult) {
                    context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
                        NAccessor::TSparsedArray::BuildTrueArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
                }
                return false;
            } else {
                context.MutableResources().Upsert(GetOutputColumnIdOnce(),
                    NAccessor::TSparsedArray::BuildFalseArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
                return true;
            }
        } else if (Operation == NKernels::EOperation::Or) {
            if (!monoValue) {
                if (!accResult) {
                    context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
                        NAccessor::TSparsedArray::BuildFalseArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
                }
                return false;
            } else {
                context.MutableResources().Upsert(GetOutputColumnIdOnce(),
                    NAccessor::TSparsedArray::BuildTrueArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
                return true;
            }
        }
    }

    if (!accResult) {
        AFL_VERIFY(accInput->GetDataType()->id() == arrow::uint8()->id())("type", accInput->GetDataType()->ToString());
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(), accInput, false);
    } else {
        auto result = Function->Call(TColumnChainInfo::BuildVector({ GetOutputColumnIdOnce(), inputId }), context.GetResources());
        if (result.IsFail()) {
            return result;
        }
        auto datum = result.DetachResult();
        context.MutableResources().Remove(GetOutputColumnIdOnce());
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(), datum, false);
        if (IsFinishDatum(datum)) {
            return true;
        }
    }

    return false;
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

        auto blockBoolType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
        b.AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::And, blockBoolType, blockBoolType, blockBoolType);
        b.AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Or, blockBoolType, blockBoolType, blockBoolType);
        auto s = b.Serialize();
        Kernels = NYql::LoadKernels(s, *Registry, Factory);
        AFL_VERIFY(Kernels.size() == 2);

        for (auto&& i : Kernels) {
            arrow::compute::Arity arity(i->signature->in_types().size(), i->signature->is_varargs());
            auto func = std::make_shared<arrow::compute::ScalarFunction>("local_function", arity, nullptr);
            TStatusValidator::Validate(func->AddKernel(*i));
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

bool TStreamLogicProcessor::IsFinishDatum(const arrow::Datum& datum) const {
    const auto arrChecker = [&](const arrow::Array& arr) {
        AFL_VERIFY(arr.type()->id() == arrow::uint8()->id());
        const arrow::UInt8Array& ui8Arr = static_cast<const arrow::UInt8Array&>(arr);
        const ui8* values = ui8Arr.raw_values();
        if (Operation == NKernels::EOperation::And) {
            for (ui32 i = 0; i < ui8Arr.length(); ++i) {
                if (values[i] != 0) {
                    return false;
                }
            }
        } else if (Operation == NKernels::EOperation::Or) {
            for (ui32 i = 0; i < ui8Arr.length(); ++i) {
                if (values[i] == 0) {
                    return false;
                }
            }
        } else {
            AFL_VERIFY(false)("op", Operation);
        }
        return true;
    };
    if (datum.is_array()) {
        auto arr = datum.make_array();
        return arrChecker(*arr);
    } else if (datum.is_arraylike()) {
        auto arr = datum.chunked_array();
        AFL_VERIFY(arr->type()->id() == arrow::uint8()->id());
        for (auto&& chunk : arr->chunks()) {
            if (!arrChecker(*chunk)) {
                return false;
            }
        }
        return true;
    } else {
        AFL_VERIFY(false)("kind", (ui32)datum.kind());
        return false;
    }
}

TConclusion<std::optional<bool>> TStreamLogicProcessor::GetMonoInput(const std::shared_ptr<IChunkedArray>& inputArray) const {
    std::shared_ptr<arrow::Scalar> monoValue;
    const auto isMonoValue = inputArray->CheckOneValueAccessor(monoValue);
    if (!isMonoValue || !*isMonoValue) {
        return std::optional<bool>();
    }
    const auto isFalseConclusion = ScalarIsFalse(monoValue);
    if (isFalseConclusion.IsFail()) {
        return isFalseConclusion;
    }
    return !*isFalseConclusion;
}

}   // namespace NKikimr::NArrow::NSSA
