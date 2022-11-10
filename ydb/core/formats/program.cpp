#include <memory>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <algorithm>

#include "program.h"
#include "arrow_helpers.h"

#ifndef WIN32
#include <AggregateFunctions/IAggregateFunction.h>
#else
namespace CH {
enum class AggFunctionId {
    AGG_UNSPECIFIED = 0,
    AGG_ANY = 1,
    AGG_COUNT = 2,
    AGG_MIN = 3,
    AGG_MAX = 4,
    AGG_SUM = 5,
};
struct GroupByOptions : public arrow::compute::ScalarAggregateOptions {
    struct Assign {
        AggFunctionId function = AggFunctionId::AGG_UNSPECIFIED;
        std::string result_column;
        std::vector<std::string> arguments;
    };

    std::shared_ptr<arrow::Schema> schema;
    std::vector<Assign> assigns;
};
}
#endif

#include <util/system/yassert.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <ydb/core/util/yverify_stream.h>

namespace NKikimr::NSsa {

const char * GetFunctionName(EOperation op) {
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

EOperation ValidateOperation(EOperation op, ui32 argsSize) {
    switch (op) {
        case EOperation::Equal:
        case EOperation::NotEqual:
        case EOperation::Less:
        case EOperation::LessEqual:
        case EOperation::Greater:
        case EOperation::GreaterEqual:
        case EOperation::MatchSubstring:
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
            if (argsSize == 2) {
                return op;
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
            if (argsSize == 1) {
                return op;
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
            return op; // TODO: check

        default:
            break;
    }
    return EOperation::Unspecified;
}

const char * GetFunctionName(EAggregate op) {
    switch (op) {
        case EAggregate::Count:
            return "count";
        case EAggregate::Min:
            return "min_max";
        case EAggregate::Max:
            return "min_max";
        case EAggregate::Sum:
            return "sum";
#if 0 // TODO
        case EAggregate::Avg:
            return "mean";
#endif
        default:
            break;
    }
    return "";
}

const char * GetHouseFunctionName(EAggregate op) {
    switch (op) {
        case EAggregate::Some:
            return "ch.any";
        case EAggregate::Count:
            return "ch.count";
        case EAggregate::Min:
            return "ch.min";
        case EAggregate::Max:
            return "ch.max";
        case EAggregate::Sum:
            return "ch.sum";
#if 0 // TODO
        case EAggregate::Avg:
            return "ch.avg";
#endif
        default:
            break;
    }
    return "";
}

namespace {

CH::AggFunctionId GetHouseFunction(EAggregate op) {
    switch (op) {
        case EAggregate::Some:
            return CH::AggFunctionId::AGG_ANY;
        case EAggregate::Count:
            return CH::AggFunctionId::AGG_COUNT;
        case EAggregate::Min:
            return CH::AggFunctionId::AGG_MIN;
        case EAggregate::Max:
            return CH::AggFunctionId::AGG_MAX;
        case EAggregate::Sum:
            return CH::AggFunctionId::AGG_SUM;
#if 0 // TODO
        case EAggregate::Avg:
            return CH::AggFunctionId::AGG_AVG;
#endif
        default:
            break;
    }
    return CH::AggFunctionId::AGG_UNSPECIFIED;
}

arrow::Status AddColumn(
    TProgramStep::TDatumBatch& batch,
    const std::string& name,
    arrow::Datum&& column)
{
    if (batch.schema->GetFieldIndex(name) != -1) {
        return arrow::Status::Invalid("Trying to add duplicate column '" + name + "'");
    }

    auto field = arrow::field(name, column.type());
    if (!field || !field->type()->Equals(column.type())) {
        return arrow::Status::Invalid("Cannot create field.");
    }
    if (!column.is_scalar() && column.length() != batch.rows) {
        return arrow::Status::Invalid("Wrong column length.");
    }

    batch.schema = *batch.schema->AddField(batch.schema->num_fields(), field);
    batch.datums.emplace_back(column);
    return arrow::Status::OK();
}

arrow::Result<arrow::Datum> GetColumnByName(const TProgramStep::TDatumBatch& batch, const std::string& name) {
    int i = batch.schema->GetFieldIndex(name);
    if (i == -1) {
        return arrow::Status::Invalid("Not found column '" + name + "' or duplicate");
    } else {
        return batch.datums[i];
    }
}

std::shared_ptr<TProgramStep::TDatumBatch> ToTDatumBatch(std::shared_ptr<arrow::RecordBatch>& batch) {
    std::vector<arrow::Datum> datums;
    datums.reserve(batch->num_columns());
    for (int64_t i = 0; i < batch->num_columns(); ++i) {
        datums.push_back(arrow::Datum(batch->column(i)));
    }
    return std::make_shared<TProgramStep::TDatumBatch>(TProgramStep::TDatumBatch{std::make_shared<arrow::Schema>(*batch->schema()), batch->num_rows(), std::move(datums)});
}

std::shared_ptr<arrow::RecordBatch> ToRecordBatch(TProgramStep::TDatumBatch& batch) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(batch.datums.size());
    for (auto col : batch.datums) {
        if (col.is_scalar()) {
            columns.push_back(*arrow::MakeArrayFromScalar(*col.scalar(), batch.rows));
        }
        else if (col.is_array()){
            if (col.length() == -1) {
                return {};
            }
            columns.push_back(col.make_array());
        }
    }
    return arrow::RecordBatch::Make(batch.schema, batch.rows, columns);
}

template <bool houseFunction, typename TOpId, typename TOptions>
arrow::Result<arrow::Datum> CallFunctionById(
    TOpId funcId, const std::vector<std::string>& args,
    const TOptions* funcOpts,
    const TProgramStep::TDatumBatch& batch,
    arrow::compute::ExecContext* ctx)
{
    std::vector<arrow::Datum> arguments;
    arguments.reserve(args.size());

    for (auto& colName : args) {
        auto column = GetColumnByName(batch, colName);
        if (!column.ok()) {
            return column.status();
        }
        arguments.push_back(*column);
    }
    std::string funcName;
    if constexpr (houseFunction) {
        funcName = GetHouseFunctionName(funcId);
    } else {
        funcName = GetFunctionName(funcId);
    }

    if (ctx && ctx->func_registry()->GetFunction(funcName).ok()) {
        return arrow::compute::CallFunction(funcName, arguments, funcOpts, ctx);
    }
    return arrow::compute::CallFunction(funcName, arguments, funcOpts);
}

arrow::Result<arrow::Datum> CallFunctionByAssign(
    const TAssign& assign,
    const TProgramStep::TDatumBatch& batch,
    arrow::compute::ExecContext* ctx)
{
    return CallFunctionById<false>(assign.GetOperation(), assign.GetArguments(), assign.GetFunctionOptions(),
                                   batch, ctx);
}

arrow::Result<arrow::Datum> CallFunctionByAssign(
    const TAggregateAssign& assign,
    const TProgramStep::TDatumBatch& batch,
    arrow::compute::ExecContext* ctx)
{
    return CallFunctionById<false>(assign.GetOperation(), assign.GetArguments(), &assign.GetAggregateOptions(),
                                   batch, ctx);
}

arrow::Result<arrow::Datum> CallHouseFunctionByAssign(
    const TAggregateAssign& assign,
    TProgramStep::TDatumBatch& batch,
    arrow::compute::ExecContext* ctx)
{
    try {
        return CallFunctionById<true>(assign.GetOperation(), assign.GetArguments(), &assign.GetAggregateOptions(),
                                      batch, ctx);
    } catch (const std::exception& ex) {
        Y_VERIFY_S(false, ex.what());
    }
}

CH::GroupByOptions::Assign GetGroupByAssign(const TAggregateAssign& assign) {
    CH::GroupByOptions::Assign descr;
    descr.function = GetHouseFunction(assign.GetOperation());
    descr.result_column = assign.GetName();
    descr.arguments.reserve(assign.GetArguments().size());

    for (auto& colName : assign.GetArguments()) {
        descr.arguments.push_back(colName);
    }
    return descr;
}

}


arrow::Status TProgramStep::ApplyAssignes(
    TProgramStep::TDatumBatch& batch,
    arrow::compute::ExecContext* ctx) const
{
    if (Assignes.empty()) {
        return arrow::Status::OK();
    }
    batch.datums.reserve(batch.datums.size() + Assignes.size());
    for (auto& assign : Assignes) {
        if (GetColumnByName(batch, assign.GetName()).ok()) {
            return arrow::Status::Invalid("Assign to existing column '" + assign.GetName() + "'.");
        }

        arrow::Datum column;
        if (assign.IsConstant()) {
            column = assign.GetConstant();
        } else {
            auto funcResult = CallFunctionByAssign(assign, batch, ctx);
            if (!funcResult.ok()) {
                return funcResult.status();
            }
            column = *funcResult;
        }
        auto status = AddColumn(batch, assign.GetName(), std::move(column));
        if (!status.ok()) {
            return status;
        }
    }
    //return batch->Validate();
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyAggregates(
    TDatumBatch& batch,
    arrow::compute::ExecContext* ctx) const
{
    if (GroupBy.empty()) {
        return arrow::Status::OK();
    }

    ui32 numResultColumns = GroupBy.size() + GroupByKeys.size();
    TDatumBatch res;
    res.datums.reserve(numResultColumns);

    arrow::FieldVector fields;
    fields.reserve(numResultColumns);

    if (GroupByKeys.empty()) {
        for (auto& assign : GroupBy) {
            auto funcResult = CallFunctionByAssign(assign, batch, ctx);
            if (!funcResult.ok()) {
                auto houseResult = CallHouseFunctionByAssign(assign, batch, ctx);
                if (!houseResult.ok()) {
                    return funcResult.status();
                }
                funcResult = houseResult;
            }

            res.datums.push_back(*funcResult);
            auto& column = res.datums.back();
            if (!column.is_scalar()) {
                return arrow::Status::Invalid("Aggregate result is not a scalar.");
            }

            if (column.scalar()->type->id() == arrow::Type::STRUCT) {
                auto op = assign.GetOperation();
                if (op == EAggregate::Min) {
                    const auto& minMax = column.scalar_as<arrow::StructScalar>();
                    column = minMax.value[0];
                } else if (op == EAggregate::Max) {
                    const auto& minMax = column.scalar_as<arrow::StructScalar>();
                    column = minMax.value[1];
                } else {
                    return arrow::Status::Invalid("Unexpected struct result for aggregate function.");
                }
            }

            if (!column.type()) {
                return arrow::Status::Invalid("Aggregate result has no type.");
            }
            fields.emplace_back(std::make_shared<arrow::Field>(assign.GetName(), column.type()));
        }

        res.rows = 1;
    } else {
        CH::GroupByOptions funcOpts;
        funcOpts.schema = batch.schema;
        funcOpts.assigns.reserve(numResultColumns);

        for (auto& assign : GroupBy) {
            funcOpts.assigns.emplace_back(GetGroupByAssign(assign));
        }

        for (auto& key : GroupByKeys) {
            funcOpts.assigns.emplace_back(CH::GroupByOptions::Assign{
                .result_column = key
            });
        }

        auto gbRes = arrow::compute::CallFunction(GetHouseGroupByName(), batch.datums, &funcOpts, ctx);
        if (!gbRes.ok()) {
            return gbRes.status();
        }
        auto gbBatch = (*gbRes).record_batch();

        for (auto& assign : funcOpts.assigns) {
            auto column = gbBatch->GetColumnByName(assign.result_column);
            if (!column) {
                return arrow::Status::Invalid("No expected column in GROUP BY result.");
            }
            fields.emplace_back(std::make_shared<arrow::Field>(assign.result_column, column->type()));
            res.datums.push_back(column);
        }

        res.rows = gbBatch->num_rows();
    }

    res.schema = std::make_shared<arrow::Schema>(fields);
    batch = std::move(res);
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyFilters(TDatumBatch& batch) const {
    if (Filters.empty()) {
        return arrow::Status::OK();
    }
    std::vector<std::vector<bool>> filters;
    filters.reserve(Filters.size());
    for (auto& colName : Filters) {
        auto column = GetColumnByName(batch, colName);
        if (!column.ok()) {
            return column.status();
        }
        if (!column->is_array() || column->type() != arrow::boolean()) {
            return arrow::Status::Invalid("Column '" + colName + "' is not a boolean array.");
        }

        auto boolColumn = std::static_pointer_cast<arrow::BooleanArray>(column->make_array());
        filters.push_back(std::vector<bool>(boolColumn->length()));
        auto& bits = filters.back();
        for (size_t i = 0; i < bits.size(); ++i) {
            bits[i] = boolColumn->Value(i);
        }
    }

    std::vector<bool> bits;
    for (auto& f : filters) {
        bits = NArrow::CombineFilters(std::move(bits), std::move(f));
    }

    if (bits.size()) {
        auto filter = NArrow::MakeFilter(bits);

        std::unordered_set<std::string_view> neededColumns;
        bool allColumns = Projection.empty() && GroupBy.empty();
        if (!allColumns) {
            for (auto& aggregate : GroupBy) {
                for (auto& arg : aggregate.GetArguments()) {
                    neededColumns.insert(arg);
                }
            }
            for (auto& key : GroupByKeys) {
                neededColumns.insert(key);
            }
            for (auto& str : Projection) {
                neededColumns.insert(str);
            }
        }

        for (int64_t i = 0; i < batch.schema->num_fields(); ++i) {
            bool needed = (allColumns || neededColumns.contains(batch.schema->field(i)->name()));
            if (batch.datums[i].is_array() && needed) {
                auto res = arrow::compute::Filter(batch.datums[i].make_array(), filter);
                if (!res.ok()) {
                    return res.status();
                }
                if ((*res).kind() != batch.datums[i].kind()) {
                    return arrow::Status::Invalid("Unexpected filter result.");
                }

                batch.datums[i] = *res;
            }
        }

        int newRows = 0;
        for (int64_t i = 0; i < filter->length(); ++i) {
            newRows += filter->Value(i);
        }
        batch.rows = newRows;
    }
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyProjection(TDatumBatch& batch) const {
    if (Projection.empty()) {
        return arrow::Status::OK();
    }
    std::unordered_set<std::string_view> projSet;
    for (auto& str: Projection) {
        projSet.insert(str);
    }
    std::vector<std::shared_ptr<arrow::Field>> newFields;
    std::vector<arrow::Datum> newDatums;
    for (int64_t i = 0; i < batch.schema->num_fields(); ++i) {
        auto& cur_field_name = batch.schema->field(i)->name();
        if (projSet.contains(cur_field_name)) {
            newFields.push_back(batch.schema->field(i));
            if (!newFields.back()) {
                return arrow::Status::Invalid("Wrong projection.");
            }
            newDatums.push_back(batch.datums[i]);
        }
    }
    batch.schema = std::make_shared<arrow::Schema>(newFields);
    batch.datums = std::move(newDatums);
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const {
    if (Projection.empty()) {
        return arrow::Status::OK();
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto& column : Projection) {
        fields.push_back(batch->schema()->GetFieldByName(column));
        if (!fields.back()) {
            return arrow::Status::Invalid("Wrong projection column '" + column + "'.");
        }
    }
    batch = NArrow::ExtractColumns(batch, std::make_shared<arrow::Schema>(fields));
    return arrow::Status::OK();
}

arrow::Status TProgramStep::Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const {
    auto rb = ToTDatumBatch(batch);

    auto status = ApplyAssignes(*rb, ctx);
    //Y_VERIFY_S(status.ok(), status.message());
    if (!status.ok()) {
        return status;
    }

    status = ApplyFilters(*rb);
    //Y_VERIFY_S(status.ok(), status.message());
    if (!status.ok()) {
        return status;
    }

    status = ApplyAggregates(*rb, ctx);
    //Y_VERIFY_S(status.ok(), status.message());
    if (!status.ok()) {
        return status;
    }

    status = ApplyProjection(*rb);
    //Y_VERIFY_S(status.ok(), status.message());
    if (!status.ok()) {
        return status;
    }

    batch = ToRecordBatch(*rb);
    if (!batch) {
        return arrow::Status::Invalid("Failed to create program result.");
    }
    return arrow::Status::OK();
}

}
