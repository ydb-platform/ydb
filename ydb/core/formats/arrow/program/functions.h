#pragma once
#include "abstract.h"
#include "aggr_common.h"
#include "collection.h"
#include "custom_registry.h"

#include <ydb/library/arrow_kernels/operations.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/function.h>

namespace NKikimr::NArrow::NSSA {

class TExecFunctionContext {
private:
    YDB_READONLY_DEF(std::vector<TColumnChainInfo>, Columns);

public:
    TExecFunctionContext(const std::vector<TColumnChainInfo>& columns)
        : Columns(columns) {
    }
};

class IStepFunction {
protected:
    bool NeedConcatenation = false;

public:
    virtual bool IsAggregation() const = 0;

    arrow::compute::ExecContext* GetContext() const {
        return GetCustomExecContext();
    }

    IStepFunction(const bool needConcatenation)
        : NeedConcatenation(needConcatenation) {
    }

    virtual ~IStepFunction() = default;
    virtual TConclusion<arrow::Datum> Call(
        const TExecFunctionContext& context, const std::shared_ptr<TAccessorsCollection>& resources) const = 0;
    virtual TConclusionStatus CheckIO(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output) const = 0;
};

class TInternalFunction: public IStepFunction {
private:
    using TBase = IStepFunction;
    std::shared_ptr<arrow::compute::FunctionOptions> FunctionOptions;

private:
    virtual std::vector<std::string> GetRegistryFunctionNames() const = 0;
    virtual TConclusion<arrow::Datum> PrepareResult(arrow::Datum&& datum) const {
        return std::move(datum);
    }

public:
    TInternalFunction(const std::shared_ptr<arrow::compute::FunctionOptions>& functionOptions, const bool needConcatenation = false)
        : TBase(needConcatenation)
        , FunctionOptions(functionOptions) {
    }
    virtual TConclusion<arrow::Datum> Call(
        const TExecFunctionContext& context, const std::shared_ptr<TAccessorsCollection>& resources) const override;
};

class TSimpleFunction: public TInternalFunction {
private:
    using EOperation = NKernels::EOperation;
    using TBase = TInternalFunction;
    using TBase::TBase;
    const EOperation OperationId;
    virtual std::vector<std::string> GetRegistryFunctionNames() const override {
        return { GetFunctionName(OperationId) };
    }

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    static const char* GetFunctionName(const EOperation op) {
        switch (op) {
            case EOperation::CastBoolean:
            case EOperation::CastInt8:
            case EOperation::CastInt16:
            case EOperation::CastInt32:
            case EOperation::CastInt64:
            case EOperation::CastUInt8:
            case EOperation::CastUInt16:
            case EOperation::CastUInt32:
            case EOperation::CastUInt64:
            case EOperation::CastFloat:
            case EOperation::CastDouble:
            case EOperation::CastBinary:
            case EOperation::CastFixedSizeBinary:
            case EOperation::CastString:
            case EOperation::CastTimestamp:
                return "ydb.cast";

            case EOperation::IsValid:
                return "is_valid";
            case EOperation::IsNull:
                return "is_null";

            case EOperation::Equal:
                return "equal";
            case EOperation::NotEqual:
                return "not_equal";
            case EOperation::Less:
                return "less";
            case EOperation::LessEqual:
                return "less_equal";
            case EOperation::Greater:
                return "greater";
            case EOperation::GreaterEqual:
                return "greater_equal";

            case EOperation::Invert:
                return "invert";
            case EOperation::And:
                return "and";
            case EOperation::Or:
                return "or";
            case EOperation::Xor:
                return "xor";

            case EOperation::Add:
                return "add";
            case EOperation::Subtract:
                return "subtract";
            case EOperation::Multiply:
                return "multiply";
            case EOperation::Divide:
                return "divide";
            case EOperation::Abs:
                return "abs";
            case EOperation::Negate:
                return "negate";
            case EOperation::Gcd:
                return "gcd";
            case EOperation::Lcm:
                return "lcm";
            case EOperation::Modulo:
                return "mod";
            case EOperation::ModuloOrZero:
                return "modOrZero";
            case EOperation::AddNotNull:
                return "add_checked";
            case EOperation::SubtractNotNull:
                return "subtract_checked";
            case EOperation::MultiplyNotNull:
                return "multiply_checked";
            case EOperation::DivideNotNull:
                return "divide_checked";

            case EOperation::BinaryLength:
                return "binary_length";
            case EOperation::MatchSubstring:
                return "match_substring";
            case EOperation::MatchLike:
                return "match_like";
            case EOperation::StartsWith:
                return "starts_with";
            case EOperation::EndsWith:
                return "ends_with";

            case EOperation::Acosh:
                return "acosh";
            case EOperation::Atanh:
                return "atanh";
            case EOperation::Cbrt:
                return "cbrt";
            case EOperation::Cosh:
                return "cosh";
            case EOperation::E:
                return "e";
            case EOperation::Erf:
                return "erf";
            case EOperation::Erfc:
                return "erfc";
            case EOperation::Exp:
                return "exp";
            case EOperation::Exp2:
                return "exp2";
            case EOperation::Exp10:
                return "exp10";
            case EOperation::Hypot:
                return "hypot";
            case EOperation::Lgamma:
                return "lgamma";
            case EOperation::Pi:
                return "pi";
            case EOperation::Sinh:
                return "sinh";
            case EOperation::Sqrt:
                return "sqrt";
            case EOperation::Tgamma:
                return "tgamma";

            case EOperation::Floor:
                return "floor";
            case EOperation::Ceil:
                return "ceil";
            case EOperation::Trunc:
                return "trunc";
            case EOperation::Round:
                return "round";
            case EOperation::RoundBankers:
                return "roundBankers";
            case EOperation::RoundToExp2:
                return "roundToExp2";

                // TODO: "is_in", "index_in"

            default:
                break;
        }
        return "";
    }

    static TConclusionStatus ValidateArgumentsCount(const EOperation op, const ui32 argsSize) {
        switch (op) {
            case EOperation::Equal:
            case EOperation::NotEqual:
            case EOperation::Less:
            case EOperation::LessEqual:
            case EOperation::Greater:
            case EOperation::GreaterEqual:
            case EOperation::And:
            case EOperation::Or:
            case EOperation::Xor:
            case EOperation::Add:
            case EOperation::Subtract:
            case EOperation::Multiply:
            case EOperation::Divide:
            case EOperation::Modulo:
            case EOperation::AddNotNull:
            case EOperation::SubtractNotNull:
            case EOperation::MultiplyNotNull:
            case EOperation::DivideNotNull:
            case EOperation::ModuloOrZero:
            case EOperation::Gcd:
            case EOperation::Lcm:
                if (argsSize != 2) {
                    return TConclusionStatus::Fail("incorrect arguments count: " + ::ToString(argsSize) + " != 2 (expected).");
                }
                break;

            case EOperation::CastBoolean:
            case EOperation::CastInt8:
            case EOperation::CastInt16:
            case EOperation::CastInt32:
            case EOperation::CastInt64:
            case EOperation::CastUInt8:
            case EOperation::CastUInt16:
            case EOperation::CastUInt32:
            case EOperation::CastUInt64:
            case EOperation::CastFloat:
            case EOperation::CastDouble:
            case EOperation::CastBinary:
            case EOperation::CastFixedSizeBinary:
            case EOperation::CastString:
            case EOperation::CastTimestamp:
            case EOperation::IsValid:
            case EOperation::IsNull:
            case EOperation::BinaryLength:
            case EOperation::Invert:
            case EOperation::Abs:
            case EOperation::Negate:
            case EOperation::StartsWith:
            case EOperation::EndsWith:
            case EOperation::MatchSubstring:
            case EOperation::MatchLike:
                if (argsSize != 1) {
                    return TConclusionStatus::Fail("incorrect arguments count: " + ::ToString(argsSize) + " != 1 (expected).");
                }
                break;

            case EOperation::Acosh:
            case EOperation::Atanh:
            case EOperation::Cbrt:
            case EOperation::Cosh:
            case EOperation::E:
            case EOperation::Erf:
            case EOperation::Erfc:
            case EOperation::Exp:
            case EOperation::Exp2:
            case EOperation::Exp10:
            case EOperation::Hypot:
            case EOperation::Lgamma:
            case EOperation::Pi:
            case EOperation::Sinh:
            case EOperation::Sqrt:
            case EOperation::Tgamma:
            case EOperation::Floor:
            case EOperation::Ceil:
            case EOperation::Trunc:
            case EOperation::Round:
            case EOperation::RoundBankers:
            case EOperation::RoundToExp2:
                if (argsSize != 1) {
                    return TConclusionStatus::Fail("incorrect arguments count: " + ::ToString(argsSize) + " != 1 (expected).");
                }
                break;
            default:
                return TConclusionStatus::Fail("non supported method " + TString(GetFunctionName(op)));
        }
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus CheckIO(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output) const override {
        if (output.size() != 1) {
            return TConclusionStatus::Fail("output size != 1 (" + ::ToString(output.size()) + ")");
        }
        return ValidateArgumentsCount(OperationId, input.size());
    }

    TSimpleFunction(const EOperation operationId, const std::shared_ptr<arrow::compute::FunctionOptions>& functionOptions = nullptr,
        const bool needConcatenation = false)
        : TBase(functionOptions, needConcatenation)
        , OperationId(operationId) {
    }
};

class TKernelFunction: public IStepFunction {
private:
    using TBase = IStepFunction;
    const std::shared_ptr<const arrow::compute::ScalarFunction> Function;
    std::shared_ptr<arrow::compute::FunctionOptions> FunctionOptions;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TKernelFunction(const std::shared_ptr<const arrow::compute::ScalarFunction> kernelsFunction,
        const std::shared_ptr<arrow::compute::FunctionOptions>& functionOptions = nullptr, const bool needConcatenation = false)
        : TBase(needConcatenation)
        , Function(kernelsFunction)
        , FunctionOptions(functionOptions) {
        AFL_VERIFY(Function);
    }

    TConclusion<arrow::Datum> Call(const TExecFunctionContext& context, const std::shared_ptr<TAccessorsCollection>& resources) const override {
        auto argumentsReader = resources->GetArguments(TColumnChainInfo::ExtractColumnIds(context.GetColumns()), NeedConcatenation);
        TAccessorsCollection::TChunksMerger merger;
        while (auto args = argumentsReader.ReadNext()) {
            try {
                auto result = Function->Execute(*args, FunctionOptions.get(), GetContext());
                if (result.ok()) {
                    merger.AddChunk(*result);
                } else {
                    return TConclusionStatus::Fail(result.status().message());
                }
            } catch (const std::exception& ex) {
                return TConclusionStatus::Fail(ex.what());
            }
        }
        return merger.Execute();
    }

    virtual TConclusionStatus CheckIO(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output) const override {
        if (output.size() != 1) {
            return TConclusionStatus::Fail("output size != 1 (" + ::ToString(output.size()) + ")");
        }
        if (!input.size()) {
            return TConclusionStatus::Fail("input size == 0!!!");
        }
        return TConclusionStatus::Success();
    }
};
}   // namespace NKikimr::NArrow::NSSA
