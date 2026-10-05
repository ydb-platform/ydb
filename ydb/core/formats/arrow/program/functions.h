#pragma once
#include "abstract.h"
#include "aggr_common.h"
#include "collection.h"
#include "custom_registry.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/library/arrow_kernels/operations.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/function.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>

namespace NKikimr::NArrow::NSSA {

// Either an arrow scalar or an IChunkedArray.
// Arrow computations return arrow::Datum, but we want to be able to return a dictionary-encoded array from a function.
// arrow::DictionaryArray is not suitable, because its element type is `dictionary<index, value>` instead of `value`,
// so all arrays are returned as IChunkedArray.
class TFunctionResult {
private:
    std::variant<std::shared_ptr<arrow::Scalar>, std::shared_ptr<NAccessor::IChunkedArray>> Data;

public:
    TFunctionResult(const std::shared_ptr<arrow::Scalar>& data)
        : Data(data) {
    }

    TFunctionResult(const std::shared_ptr<NAccessor::IChunkedArray>& data)
        : Data(data) {
    }

    bool IsScalar() const {
        return std::holds_alternative<std::shared_ptr<arrow::Scalar>>(Data);
    }

    const std::shared_ptr<NAccessor::IChunkedArray>& GetAccessorVerified() const {
        if (const auto* accessor = std::get_if<std::shared_ptr<NAccessor::IChunkedArray>>(&Data)) {
            return *accessor;
        }
        AFL_VERIFY(false);
        return Default<std::shared_ptr<NAccessor::IChunkedArray>>();
    }

    const std::shared_ptr<arrow::Scalar>& GetScalarVerified() const {
        if (const auto* scalar = std::get_if<std::shared_ptr<arrow::Scalar>>(&Data)) {
            return *scalar;
        }
        AFL_VERIFY(false);
        return Default<std::shared_ptr<arrow::Scalar>>();
    }

    static TFunctionResult FromDatum(arrow::Datum&& data) {
        if (data.is_scalar()) {
            return data.scalar();
        }
        return NAccessor::TAccessorCollectedContainer(data).GetData();
    }
};

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
    virtual NJson::TJsonValue DoDebugJson() const {
        return NJson::JSON_MAP;
    }

public:
    virtual bool IsAggregation() const = 0;

    virtual std::shared_ptr<IResourcesAggregator> BuildResultsAggregator(const TColumnChainInfo& /*output*/) const {
        return nullptr;
    }

    arrow::compute::ExecContext* GetContext() const {
        return GetCustomExecContext();
    }

    IStepFunction(const bool needConcatenation)
        : NeedConcatenation(needConcatenation) {
    }

    virtual ~IStepFunction() = default;
    virtual TConclusion<TFunctionResult> Call(const TExecFunctionContext& context, const TAccessorsCollection& resources) const = 0;
    virtual TConclusionStatus CheckIO(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output) const = 0;
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = DoDebugJson();
        if (NeedConcatenation) {
            result.InsertValue("need_concatenation", NeedConcatenation);
        }
        return result;
    }
};

class TInternalFunction: public IStepFunction {
private:
    using TBase = IStepFunction;
    YDB_READONLY_DEF(std::shared_ptr<arrow::compute::FunctionOptions>, FunctionOptions);

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
    virtual TConclusion<TFunctionResult> Call(const TExecFunctionContext& context, const TAccessorsCollection& resources) const override;
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

    static std::shared_ptr<arrow::DataType> GetKernelType(const std::shared_ptr<arrow::DataType>& type) {
        if (type->id() == arrow::Type::TIMESTAMP &&
            std::static_pointer_cast<arrow::TimestampType>(type)->unit() == arrow::TimeUnit::MICRO) {
            return arrow::uint64();
        }
        return type;
    }

    TConclusion<arrow::Datum> Execute(std::vector<arrow::Datum>& arguments) const {
        try {
            for (auto& arg : arguments) {
                const auto type = GetKernelType(arg.descr().type);
                if (type != arg.descr().type) {
                    if (arg.is_array()) {
                        // ArrayData is shared with the source accessor (TableBatchReader hands out unsliced
                        // chunks as-is). Retyping it in place would turn the column into uint64 for every
                        // later consumer (PK comparisons in SYNC_LIMIT, merge, result), so retype a shallow copy.
                        auto retyped = arg.array()->Copy();
                        retyped->type = type;
                        arg = arrow::Datum(std::move(retyped));
                    } else if (arg.kind() == arrow::Datum::CHUNKED_ARRAY) {
                        auto retyped = arg.chunked_array()->View(type);
                        if (!retyped.ok()) {
                            return TConclusionStatus::Fail(retyped.status().message());
                        }
                        arg = std::move(*retyped);
                    }
                }
            }
            auto result = Function->Execute(arguments, FunctionOptions.get(), GetContext());
            if (result.ok()) {
                return std::move(*result);
            }
            return TConclusionStatus::Fail(result.status().message());
        } catch (const std::exception& ex) {
            return TConclusionStatus::Fail(ex.what());
        }
    }

    TConclusion<std::shared_ptr<arrow::Scalar>> GetKernelOutputForNullInput(
        const TAccessorsCollection::TChunkedArguments::TBatch& batch) const {
        AFL_VERIFY(batch.Dictionary);
        std::vector<arrow::Datum> nullArguments = batch.Arguments;
        const auto nullInputType = GetKernelType(batch.Dictionary->GetDictionary()->type());
        auto nullArgument = arrow::MakeArrayFromScalar(*arrow::MakeNullScalar(nullInputType), 1);
        if (!nullArgument.ok()) {
            return TConclusionStatus::Fail(nullArgument.status().message());
        }
        nullArguments[batch.DictionaryIndex] = *nullArgument;
        auto nullResult = Execute(nullArguments);
        if (nullResult.IsFail()) {
            return nullResult.GetError();
        }
        if (!nullResult->is_array()) {
            return TConclusionStatus::Fail("dictionary null kernel result is not an array");
        }
        const auto& nullValue = nullResult->make_array();
        if (nullValue->length() != 1) {
            return TConclusionStatus::Fail("dictionary null kernel result has incorrect length");
        }
        return TStatusValidator::GetValid(nullValue->GetScalar(0));
    }

public:
    TKernelFunction(const std::shared_ptr<const arrow::compute::ScalarFunction> kernelsFunction,
        const std::shared_ptr<arrow::compute::FunctionOptions>& functionOptions = nullptr, const bool needConcatenation = false)
        : TBase(needConcatenation)
        , Function(kernelsFunction)
        , FunctionOptions(functionOptions) {
        AFL_VERIFY(Function);
    }

    TConclusion<TFunctionResult> Call(const TExecFunctionContext& context, const TAccessorsCollection& resources) const override {
        // GetArguments may select one TDictionaryArray or TCompositeChunkedArray arg for special handling.
        auto argumentsReader = resources.GetArguments(
            TColumnChainInfo::ExtractColumnIds(context.GetColumns()), NeedConcatenation, !NeedConcatenation);
        TAccessorsCollection::TChunksMerger merger;
        std::shared_ptr<arrow::Scalar> nullResult;
        while (auto batch = argumentsReader.ReadNext()) {
            // Some kernels may produce non-null output for null input.
            // We shall restore that output at null positions after expanding the mapped dictionary values.
            if (batch->Dictionary && batch->Dictionary->GetPositions()->null_count() && !nullResult) {
                auto result = GetKernelOutputForNullInput(*batch);
                if (result.IsFail()) {
                    return result.GetError();
                }
                nullResult = result.DetachResult();
            }
            auto result = Execute(batch->Arguments);
            if (result.IsFail()) {
                return result.GetError();
            }
            auto datum = result.DetachResult();
            if (const auto& dictionary = batch->Dictionary) {
                if (!datum.is_array()) {
                    return TConclusionStatus::Fail("dictionary scalar kernel result is not an array");
                }
                auto materialized = arrow::compute::Take(datum, arrow::Datum(dictionary->GetPositions()));
                if (!materialized.ok()) {
                    return TConclusionStatus::Fail(materialized.status().message());
                }
                datum = std::move(*materialized);
                if (nullResult && nullResult->is_valid && dictionary->GetPositions()->null_count()) {
                    auto nullPositions = arrow::compute::IsNull(arrow::Datum(dictionary->GetPositions()));
                    if (!nullPositions.ok()) {
                        return TConclusionStatus::Fail(nullPositions.status().message());
                    }
                    auto replaced = arrow::compute::IfElse(*nullPositions, arrow::Datum(nullResult), datum);
                    if (!replaced.ok()) {
                        return TConclusionStatus::Fail(replaced.status().message());
                    }
                    datum = std::move(*replaced);
                }
            }
            merger.AddChunk(datum);
        }
        auto result = merger.Execute();
        if (result.IsFail()) {
            return result.GetError();
        }
        return TFunctionResult::FromDatum(result.DetachResult());
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
