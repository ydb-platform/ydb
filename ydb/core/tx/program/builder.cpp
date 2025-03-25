#include "builder.h"

#include <ydb/core/formats/arrow/program/aggr_keys.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/projection.h>
#include <ydb/core/formats/arrow/program/stream_logic.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>

#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/string/join.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<std::shared_ptr<IStepFunction>> TProgramBuilder::MakeFunction(
    const TColumnInfo& name, const NKikimrSSA::TProgram::TAssignment::TFunction& func, std::vector<TColumnChainInfo>& arguments) const {
    using TId = NKikimrSSA::TProgram::TAssignment;

    arguments.clear();
    for (auto& col : func.GetArguments()) {
        arguments.emplace_back(col.GetId());
    }

    if (func.GetFunctionType() == NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL) {
        auto kernelFunction = KernelsRegistry.GetFunction(func.GetKernelIdx());
        if (!kernelFunction) {
            return TConclusionStatus::Fail(
                TStringBuilder() << "Unknown kernel for " << name.GetColumnName() << ";kernel_idx=" << func.GetKernelIdx());
        }
        return std::make_shared<TKernelFunction>(kernelFunction);
    }

    auto mkLikeOptions = [&](bool ignoreCase) {
        if (arguments.size() != 2 || !Constants.contains(arguments[1].GetColumnId())) {
            return std::shared_ptr<arrow::compute::MatchSubstringOptions>();
        }
        auto patternScalar = Constants[arguments[1].GetColumnId()];
        if (!arrow::is_base_binary_like(patternScalar->type->id())) {
            return std::shared_ptr<arrow::compute::MatchSubstringOptions>();
        }
        arguments.pop_back();
        auto& pattern = static_cast<arrow::BaseBinaryScalar&>(*patternScalar).value;
        return std::make_shared<arrow::compute::MatchSubstringOptions>(pattern->ToString(), ignoreCase);
    };

    auto mkCastOptions = [](std::shared_ptr<arrow::DataType> dataType) {
        // TODO: support CAST with OrDefault/OrNull logic (second argument is default value)
        auto castOpts = std::make_shared<arrow::compute::CastOptions>(false);
        castOpts->to_type = dataType;
        return castOpts;
    };

    using EOperation = NKernels::EOperation;

    switch (func.GetId()) {
        case TId::FUNC_CMP_EQUAL:
            return std::make_shared<TSimpleFunction>(EOperation::Equal);
        case TId::FUNC_CMP_NOT_EQUAL:
            return std::make_shared<TSimpleFunction>(EOperation::NotEqual);
        case TId::FUNC_CMP_LESS:
            return std::make_shared<TSimpleFunction>(EOperation::Less);
        case TId::FUNC_CMP_LESS_EQUAL:
            return std::make_shared<TSimpleFunction>(EOperation::LessEqual);
        case TId::FUNC_CMP_GREATER:
            return std::make_shared<TSimpleFunction>(EOperation::Greater);
        case TId::FUNC_CMP_GREATER_EQUAL:
            return std::make_shared<TSimpleFunction>(EOperation::GreaterEqual);
        case TId::FUNC_IS_NULL:
            return std::make_shared<TSimpleFunction>(EOperation::IsNull);
        case TId::FUNC_STR_LENGTH:
            return std::make_shared<TSimpleFunction>(EOperation::BinaryLength);
        case TId::FUNC_STR_MATCH: {
            if (auto opts = mkLikeOptions(false)) {
                return std::make_shared<TSimpleFunction>(EOperation::MatchSubstring, opts);
            }
            break;
        }
        case TId::FUNC_STR_MATCH_LIKE: {
            if (auto opts = mkLikeOptions(false)) {
                return std::make_shared<TSimpleFunction>(EOperation::MatchLike, opts);
            }
            break;
        }
        case TId::FUNC_STR_STARTS_WITH: {
            if (auto opts = mkLikeOptions(false)) {
                return std::make_shared<TSimpleFunction>(EOperation::StartsWith, opts);
            }
            break;
        }
        case TId::FUNC_STR_ENDS_WITH: {
            if (auto opts = mkLikeOptions(false)) {
                return std::make_shared<TSimpleFunction>(EOperation::EndsWith, opts);
            }
            break;
        }
        case TId::FUNC_STR_MATCH_IGNORE_CASE: {
            if (auto opts = mkLikeOptions(true)) {
                return std::make_shared<TSimpleFunction>(EOperation::MatchSubstring, opts);
            }
            break;
        }
        case TId::FUNC_STR_STARTS_WITH_IGNORE_CASE: {
            if (auto opts = mkLikeOptions(true)) {
                return std::make_shared<TSimpleFunction>(EOperation::StartsWith, opts);
            }
            break;
        }
        case TId::FUNC_STR_ENDS_WITH_IGNORE_CASE: {
            if (auto opts = mkLikeOptions(true)) {
                return std::make_shared<TSimpleFunction>(EOperation::EndsWith, opts);
            }
            break;
        }
        case TId::FUNC_BINARY_NOT:
            return std::make_shared<TSimpleFunction>(EOperation::Invert);
        case TId::FUNC_BINARY_AND:
            return std::make_shared<TSimpleFunction>(EOperation::And);
        case TId::FUNC_BINARY_OR:
            return std::make_shared<TSimpleFunction>(EOperation::Or);
        case TId::FUNC_BINARY_XOR:
            return std::make_shared<TSimpleFunction>(EOperation::Xor);
        case TId::FUNC_MATH_ADD:
            return std::make_shared<TSimpleFunction>(EOperation::Add);
        case TId::FUNC_MATH_SUBTRACT:
            return std::make_shared<TSimpleFunction>(EOperation::Subtract);
        case TId::FUNC_MATH_MULTIPLY:
            return std::make_shared<TSimpleFunction>(EOperation::Multiply);
        case TId::FUNC_MATH_DIVIDE:
            return std::make_shared<TSimpleFunction>(EOperation::Divide);
        case TId::FUNC_CAST_TO_INT8:
            return std::make_shared<TSimpleFunction>(EOperation::CastInt8, mkCastOptions(std::make_shared<arrow::Int8Type>()));
        case TId::FUNC_CAST_TO_BOOLEAN:
            return std::make_shared<TSimpleFunction>(EOperation::CastBoolean, mkCastOptions(std::make_shared<arrow::BooleanType>()));
        case TId::FUNC_CAST_TO_INT16:
            return std::make_shared<TSimpleFunction>(EOperation::CastInt16, mkCastOptions(std::make_shared<arrow::Int16Type>()));
        case TId::FUNC_CAST_TO_INT32:
            return std::make_shared<TSimpleFunction>(EOperation::CastInt32, mkCastOptions(std::make_shared<arrow::Int32Type>()));
        case TId::FUNC_CAST_TO_INT64:
            return std::make_shared<TSimpleFunction>(EOperation::CastInt64, mkCastOptions(std::make_shared<arrow::Int64Type>()));
        case TId::FUNC_CAST_TO_UINT8:
            return std::make_shared<TSimpleFunction>(EOperation::CastUInt8, mkCastOptions(std::make_shared<arrow::UInt8Type>()));
        case TId::FUNC_CAST_TO_UINT16:
            return std::make_shared<TSimpleFunction>(EOperation::CastUInt16, mkCastOptions(std::make_shared<arrow::UInt16Type>()));
        case TId::FUNC_CAST_TO_UINT32:
            return std::make_shared<TSimpleFunction>(EOperation::CastUInt32, mkCastOptions(std::make_shared<arrow::UInt32Type>()));
        case TId::FUNC_CAST_TO_UINT64:
            return std::make_shared<TSimpleFunction>(EOperation::CastUInt64, mkCastOptions(std::make_shared<arrow::UInt64Type>()));
        case TId::FUNC_CAST_TO_FLOAT:
            return std::make_shared<TSimpleFunction>(EOperation::CastFloat, mkCastOptions(std::make_shared<arrow::FloatType>()));
        case TId::FUNC_CAST_TO_DOUBLE:
            return std::make_shared<TSimpleFunction>(EOperation::CastDouble, mkCastOptions(std::make_shared<arrow::DoubleType>()));
        case TId::FUNC_CAST_TO_TIMESTAMP:
            return std::make_shared<TSimpleFunction>(
                EOperation::CastTimestamp, mkCastOptions(std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO)));
        case TId::FUNC_CAST_TO_BINARY:
        case TId::FUNC_CAST_TO_FIXED_SIZE_BINARY:
        case TId::FUNC_UNSPECIFIED:
            break;
    }

    return TConclusionStatus::Fail("incompatible method type");
}

TConclusion<std::shared_ptr<TConstProcessor>> TProgramBuilder::MakeConstant(
    const TColumnInfo& name, const NKikimrSSA::TProgram::TConstant& constant) const {
    using TId = NKikimrSSA::TProgram::TConstant;

    switch (constant.GetValueCase()) {
        case TId::kBool:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::BooleanScalar>(constant.GetBool()), name.GetColumnId());
        case TId::kInt8:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::Int8Scalar>(i8(constant.GetInt8())), name.GetColumnId());
        case TId::kUint8:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::UInt8Scalar>(ui8(constant.GetUint8())), name.GetColumnId());
        case TId::kInt16:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::Int16Scalar>(i16(constant.GetInt16())), name.GetColumnId());
        case TId::kUint16:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::UInt16Scalar>(ui16(constant.GetUint16())), name.GetColumnId());
        case TId::kInt32:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::Int32Scalar>(constant.GetInt32()), name.GetColumnId());
        case TId::kUint32:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::UInt32Scalar>(constant.GetUint32()), name.GetColumnId());
        case TId::kInt64:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::Int64Scalar>(constant.GetInt64()), name.GetColumnId());
        case TId::kUint64:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::UInt64Scalar>(constant.GetUint64()), name.GetColumnId());
        case TId::kFloat:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::FloatScalar>(constant.GetFloat()), name.GetColumnId());
        case TId::kDouble:
            return std::make_shared<TConstProcessor>(std::make_shared<arrow::DoubleScalar>(constant.GetDouble()), name.GetColumnId());
        case TId::kTimestamp:
            return std::make_shared<TConstProcessor>(
                std::make_shared<arrow::TimestampScalar>(constant.GetTimestamp(), arrow::timestamp(arrow::TimeUnit::MICRO)), name.GetColumnId());
        case TId::kBytes: {
            TString str = constant.GetBytes();
            return std::make_shared<TConstProcessor>(
                std::make_shared<arrow::BinaryScalar>(std::make_shared<arrow::Buffer>((const ui8*)str.data(), str.size()), arrow::binary()),
                name.GetColumnId());
        }
        case TId::kText: {
            TString str = constant.GetText();
            return std::make_shared<TConstProcessor>(
                std::make_shared<arrow::StringScalar>(std::string(str.data(), str.size())), name.GetColumnId());
        }
        case TId::VALUE_NOT_SET:
            break;
    }
    return TConclusionStatus::Fail("incompatible constant type");
}

TConclusion<std::shared_ptr<IStepFunction>> TProgramBuilder::MakeAggrFunction(
    const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& func) const {
    if (func.GetFunctionType() == NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL) {
        auto kernelFunction = KernelsRegistry.GetFunction(func.GetKernelIdx());
        if (!kernelFunction) {
            return TConclusionStatus::Fail(TStringBuilder() << "Unknown kernel for " << func.GetId() << ";kernel_idx=" << func.GetKernelIdx());
        }
        return std::make_shared<TKernelFunction>(kernelFunction, nullptr, true);
    }

    const TConclusion<NAggregation::EAggregate> aggrType = GetAggregationType(func);
    if (aggrType.IsFail()) {
        return aggrType;
    }
    return std::make_shared<NAggregation::TAggregateFunction>(*aggrType);
}

TConclusion<NAggregation::EAggregate> TProgramBuilder::GetAggregationType(
    const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& func) const {
    using TId = NKikimrSSA::TProgram::TAggregateAssignment;

    if (func.ArgumentsSize() == 1) {
        TColumnInfo argument = GetColumnInfo(func.GetArguments()[0]);

        switch (func.GetId()) {
            case TId::AGG_SOME:
                return NAggregation::EAggregate::Some;
            case TId::AGG_COUNT:
                return NAggregation::EAggregate::Count;
            case TId::AGG_MIN:
                return NAggregation::EAggregate::Min;
            case TId::AGG_MAX:
                return NAggregation::EAggregate::Max;
            case TId::AGG_SUM:
                return NAggregation::EAggregate::Sum;
            default:
                return TConclusionStatus::Fail("incorrect function case for aggregation construct: " + ::ToString(func.GetId()));
        }
    } else if (func.ArgumentsSize() == 0 && func.GetId() == TId::AGG_COUNT) {
        return NAggregation::EAggregate::NumRows;
    }
    return TConclusionStatus::Fail("incorrect case for aggregation construct");
}

TConclusion<std::shared_ptr<TConstProcessor>> TProgramBuilder::MaterializeParameter(const TColumnInfo& name,
    const NKikimrSSA::TProgram::TParameter& parameter, const std::shared_ptr<arrow::RecordBatch>& parameterValues) const {
    auto parameterName = parameter.GetName();
    auto column = parameterValues->GetColumnByName(parameterName);
    if (!column || column->length() != 1) {
        return TConclusionStatus::Fail("incorrect column data as parameter: " + name.GetColumnName());
    }
    return std::make_shared<TConstProcessor>(TStatusValidator::GetValid(column->GetScalar(0)), name.GetColumnId());
}

TConclusionStatus TProgramBuilder::ReadAssign(
    const NKikimrSSA::TProgram::TAssignment& assign, const std::shared_ptr<arrow::RecordBatch>& parameterValues) {
    using TId = NKikimrSSA::TProgram::TAssignment;

    const TColumnInfo columnName = GetColumnInfo(assign.GetColumn());

    switch (assign.GetExpressionCase()) {
        case TId::kFunction: {
            std::shared_ptr<IKernelLogic> kernelLogic;
            if (assign.GetFunction().GetKernelName()) {
                kernelLogic.reset(IKernelLogic::TFactory::Construct(assign.GetFunction().GetKernelName()));
            }

            std::vector<TColumnChainInfo> arguments;
            auto function = MakeFunction(columnName, assign.GetFunction(), arguments);
            if (function.IsFail()) {
                return function;
            }

            if (assign.GetFunction().HasYqlOperationId() && assign.GetFunction().GetYqlOperationId() ==
                (ui32)NYql::TKernelRequestBuilder::EBinaryOp::And) {
                auto processor =
                    std::make_shared<TStreamLogicProcessor>(std::move(arguments), columnName.GetColumnId(), NKernels::EOperation::And);
                Builder.Add(processor);
            } else if (assign.GetFunction().HasYqlOperationId() &&
                       assign.GetFunction().GetYqlOperationId() == (ui32)NYql::TKernelRequestBuilder::EBinaryOp::Or) {
                auto processor =
                    std::make_shared<TStreamLogicProcessor>(std::move(arguments), columnName.GetColumnId(), NKernels::EOperation::Or);
                Builder.Add(processor);
            } else {
                auto processor = TCalculationProcessor::Build(std::move(arguments), columnName.GetColumnId(), function.DetachResult(), kernelLogic);
                if (processor.IsFail()) {
                    return processor;
                }
                if (assign.GetFunction().HasYqlOperationId()) {
                    processor.GetResult()->SetYqlOperationId(assign.GetFunction().GetYqlOperationId());
                }
                Builder.Add(processor.DetachResult());
            }
            break;
        }
        case TId::kConstant: {
            auto constProcessing = MakeConstant(columnName, assign.GetConstant());
            if (constProcessing.IsFail()) {
                return constProcessing;
            }
            Constants[columnName.GetColumnId()] = constProcessing.GetResult()->GetScalarConstant();
            Builder.Add(constProcessing.DetachResult());
            break;
        }
        case TId::kParameter: {
            auto param = MaterializeParameter(columnName, assign.GetParameter(), parameterValues);
            if (param.IsFail()) {
                return param;
            }
            Builder.Add(param.DetachResult());
            break;
        }
        case TId::kExternalFunction:
        case TId::kNull:
        case TId::EXPRESSION_NOT_SET:
            return TConclusionStatus::Fail("unsupported functions");
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TProgramBuilder::ReadFilter(const NKikimrSSA::TProgram::TFilter& filter) {
    auto& column = filter.GetPredicate();
    if (!column.HasId() || !column.GetId()) {
        return TConclusionStatus::Fail("incorrect column in filter predicate");
    }
    Builder.Add(std::make_shared<TFilterProcessor>(TColumnChainInfo(column.GetId())));
    return TConclusionStatus::Success();
}

TConclusionStatus TProgramBuilder::ReadProjection(const NKikimrSSA::TProgram::TProjection& projection) {
    std::vector<TColumnChainInfo> columns;
    if (projection.GetColumns().size() == 0) {
        return TConclusionStatus::Success();
    }
    for (auto& col : projection.GetColumns()) {
        columns.emplace_back(col.GetId());
    }
    Builder.Add(std::make_shared<TProjectionProcessor>(std::move(columns)));
    return TConclusionStatus::Success();
}

TConclusionStatus TProgramBuilder::ReadGroupBy(const NKikimrSSA::TProgram::TGroupBy& groupBy) {
    if (!groupBy.AggregatesSize()) {
        return TConclusionStatus::Success();
    }

    const auto extractColumnIds = [](const auto& protoArguments) {
        std::vector<TColumnChainInfo> ids;
        for (auto&& i : protoArguments) {
            ids.emplace_back(TColumnChainInfo(i.GetId()));
        }
        return ids;
    };

    if (groupBy.GetKeyColumns().size()) {
        NAggregation::TWithKeysAggregationProcessor::TBuilder aggrBuilder;
        for (auto& key : groupBy.GetKeyColumns()) {
            aggrBuilder.AddKey(key.GetId());
        }
        for (auto& agg : groupBy.GetAggregates()) {
            const TColumnInfo columnName = GetColumnInfo(agg.GetColumn());

            auto func = GetAggregationType(agg.GetFunction());
            if (func.IsFail()) {
                return func;
            }
            auto argsVector = extractColumnIds(agg.GetFunction().GetArguments());
            auto addStatus = aggrBuilder.AddGroupBy(argsVector, columnName.GetColumnId(), func.DetachResult());
            if (addStatus.IsFail()) {
                return addStatus;
            }
        }
        auto finishResult = aggrBuilder.Finish();
        if (finishResult.IsFail()) {
            return finishResult;
        }
        Builder.Add(finishResult.DetachResult());
    } else {
        for (auto& agg : groupBy.GetAggregates()) {
            const TColumnInfo columnName = GetColumnInfo(agg.GetColumn());
            auto func = MakeAggrFunction(agg.GetFunction());
            if (func.IsFail()) {
                return func;
            }
            auto aggrType = GetAggregationType(agg.GetFunction());
            auto argColumnIds = extractColumnIds(agg.GetFunction().GetArguments());
            auto status = TCalculationProcessor::Build(std::move(argColumnIds), columnName.GetColumnId(), func.DetachResult(), nullptr);
            if (status.IsFail()) {
                return status;
            }
            Builder.Add(status.DetachResult());
        }
    }

    return TConclusionStatus::Success();
}

TColumnInfo TProgramBuilder::GetColumnInfo(const NKikimrSSA::TProgram::TColumn& column) const {
    AFL_VERIFY(column.HasId() && column.GetId());
    if (column.HasId() && column.GetId()) {
        const ui32 columnId = column.GetId();
        const TString name = ColumnResolver.GetColumnName(columnId, false);
        if (name.empty()) {
            return TColumnInfo::Generated(columnId, GenerateName(column));
        } else {
            Sources.emplace(columnId, TColumnInfo::Original(columnId, name));
            return TColumnInfo::Original(columnId, name);
        }
    } else {
        return TColumnInfo::Generated(0, GenerateName(column));
    }
}

std::string TProgramBuilder::GenerateName(const NKikimrSSA::TProgram::TColumn& column) const {
    AFL_VERIFY(column.HasId() && column.GetId());
    const auto name = ToString(column.GetId());
    return std::string(name.data(), name.size());
}

}   // namespace NKikimr::NArrow::NSSA
