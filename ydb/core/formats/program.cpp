#include <memory>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <algorithm>

#include "program.h"
#include "arrow_helpers.h"
#include <util/system/yassert.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <ydb/core/util/yverify_stream.h>

namespace NKikimr::NArrow {

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

void AddColumn(std::shared_ptr<TProgramStep::TDatumBatch>& batch,
                                                      std::string field_name,
                                                      const arrow::Datum& column) {
    auto field = ::arrow::field(std::move(field_name), column.type());
    Y_VERIFY(field != nullptr);
    Y_VERIFY(field->type()->Equals(column.type()));
    Y_VERIFY(column.is_scalar() || column.length() == batch->rows);
    auto new_schema = *batch->fields->AddField(batch->fields->num_fields(), field);
    batch->datums.push_back(column);
    batch->fields = new_schema;
}

arrow::Result<arrow::Datum> GetColumnByName(const std::shared_ptr<TProgramStep::TDatumBatch>& batch, const std::string& name) {
    int i = batch->fields->GetFieldIndex(name);
    if (i == -1) {
        return arrow::Status::Invalid("Not found or duplicate");
    }
    else {
        return batch->datums[i];
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

std::shared_ptr<arrow::RecordBatch> ToRecordBatch(std::shared_ptr<TProgramStep::TDatumBatch>& batch) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(batch->datums.size());
    for (auto col : batch->datums) {
        if (col.is_scalar()) {
            columns.push_back(*arrow::MakeArrayFromScalar(*col.scalar(), batch->rows));
        }
        else if (col.is_array()){
            Y_VERIFY(col.length() != -1);
            columns.push_back(col.make_array());
        }
    }
    return arrow::RecordBatch::Make(batch->fields, batch->rows, columns);
}


std::shared_ptr<arrow::Array> MakeConstantColumn(const arrow::Scalar& value, int64_t size) {
    auto res = arrow::MakeArrayFromScalar(value, size);
    Y_VERIFY(res.ok());
    return *res;
}

//firstly try to call function from custom registry, if fails call from default
arrow::Result<arrow::Datum> CallFromCustomOrDefaultRegistry(EOperation funcId, const std::vector<arrow::Datum>& arguments, arrow::compute::ExecContext* ctx) {
    std::string funcName = GetFunctionName(funcId);
    if (ctx != nullptr && ctx->func_registry()->GetFunction(funcName).ok()) {
        return arrow::compute::CallFunction(GetFunctionName(funcId), arguments, ctx);
    } else {
        return arrow::compute::CallFunction(GetFunctionName(funcId), arguments);
    }
}

std::shared_ptr<arrow::Array> CallArrayFunction(EOperation funcId, const std::vector<std::string>& args,
                                           std::shared_ptr<arrow::RecordBatch> batch, arrow::compute::ExecContext* ctx) {
    std::vector<arrow::Datum> arguments;
    arguments.reserve(args.size());

    for (auto& colName : args) {
        auto column = batch->GetColumnByName(colName);
        Y_VERIFY(column);
        arguments.push_back(arrow::Datum(*column));
    }
    std::string funcName = GetFunctionName(funcId);
    arrow::Result<arrow::Datum> result;
    result = CallFromCustomOrDefaultRegistry(funcId, arguments, ctx);
    Y_VERIFY(result.ok());
    Y_VERIFY(result->is_array());
    return result->make_array();
}


std::shared_ptr<arrow::Scalar> CallScalarFunction(EOperation funcId, const std::vector<std::string>& args,
                                           std::shared_ptr<arrow::RecordBatch> batch, arrow::compute::ExecContext* ctx)  {
    std::vector<arrow::Datum> arguments;
    arguments.reserve(args.size());

    for (auto& colName : args) {
        auto column = batch->GetColumnByName(colName);
        Y_VERIFY(column);
        arguments.push_back(arrow::Datum{column});
    }
    std::string funcName = GetFunctionName(funcId);
    arrow::Result<arrow::Datum> result;
    result = CallFromCustomOrDefaultRegistry(funcId, arguments, ctx);
    Y_VERIFY(result.ok());
    Y_VERIFY(result->is_scalar());
    return result->scalar();
}

arrow::Datum CallFunctionById(EOperation funcId, const std::vector<std::string>& args, const arrow::compute::FunctionOptions* funcOpts,
                                           std::shared_ptr<TProgramStep::TDatumBatch> batch, arrow::compute::ExecContext* ctx) {
    std::vector<arrow::Datum> arguments;
    arguments.reserve(args.size());

    for (auto& colName : args) {
        auto column = GetColumnByName(batch, colName);
        Y_VERIFY(column.ok());
        arguments.push_back(*column);
    }
    std::string funcName = GetFunctionName(funcId);

    arrow::Result<arrow::Datum> result;
    if (ctx != nullptr && ctx->func_registry()->GetFunction(funcName).ok()) {
        result = arrow::compute::CallFunction(GetFunctionName(funcId), arguments, funcOpts, ctx);
    } else {
        result = arrow::compute::CallFunction(GetFunctionName(funcId), arguments, funcOpts);
    }
    Y_VERIFY(result.ok());
    return result.ValueOrDie();
}

arrow::Datum CallFunctionByAssign(const TAssign& assign, std::shared_ptr<TProgramStep::TDatumBatch> batch, arrow::compute::ExecContext* ctx) {
    return CallFunctionById(assign.GetOperation(), assign.GetArguments(), assign.GetFunctionOptions(), batch, ctx);
}

void TProgramStep::ApplyAssignes(std::shared_ptr<TProgramStep::TDatumBatch>& batch, arrow::compute::ExecContext* ctx) const {
    if (Assignes.empty()) {
        return;
    }
    batch->datums.reserve(batch->datums.size() + Assignes.size());
    for (auto& assign : Assignes) {
        Y_VERIFY(!GetColumnByName(batch, assign.GetName()).ok());

        arrow::Datum column;
        if (assign.IsConstant()) {
            column = assign.GetConstant();
        } else {
            column = CallFunctionByAssign(assign, batch, ctx);
        }
        AddColumn(batch, assign.GetName(), column);
    }
    //Y_VERIFY(batch->Validate().ok());
}

void TProgramStep::ApplyFilters(std::shared_ptr<TDatumBatch>& batch) const {
    if (Filters.empty()) {
        return;
    }
    std::vector<std::vector<bool>> filters;
    filters.reserve(Filters.size());
    for (auto& colName : Filters) {
        auto column = GetColumnByName(batch, colName);
        Y_VERIFY_S(column.ok(), TStringBuilder() << "Column " << colName << " is not ok.");
        Y_VERIFY_S(column->is_array(), TStringBuilder() << "Column " << colName << " is not an array.");
        Y_VERIFY_S(column->type() == arrow::boolean(), TStringBuilder() << "Column " << colName << " type is not bool.");
        auto boolColumn = std::static_pointer_cast<arrow::BooleanArray>(column->make_array());
        filters.push_back(std::vector<bool>(boolColumn->length()));
        auto& bits = filters.back();
        for (size_t i = 0; i < bits.size(); ++i) {
            bits[i] = boolColumn->Value(i);
        }
    }

    std::vector<bool> bits;
    for (auto& f : filters) {
        bits = CombineFilters(std::move(bits), std::move(f));
    }

    if (bits.size()) {
        auto filter = NArrow::MakeFilter(bits);
        std::unordered_set<std::string_view> projSet;
        for (auto& str: Projection) {
            projSet.insert(str);
        }
        for (int64_t i = 0; i < batch->fields->num_fields(); ++i) {
            //only array filtering, scalar cannot be filtered
            auto& cur_field_name = batch->fields->field(i)->name();
            bool is_proj = (Projection.empty() || projSet.contains(cur_field_name));
            if (batch->datums[i].is_array() && is_proj) {
                auto res = arrow::compute::Filter(batch->datums[i].make_array(), filter);
                Y_VERIFY_S(res.ok(), res.status().message());
                Y_VERIFY((*res).kind() == batch->datums[i].kind());
                batch->datums[i] = *res;
            }
        }
        int newRows = 0;
        for (int64_t i = 0; i < filter->length(); ++i) {
            newRows += filter->Value(i);
        }
        batch->rows = newRows;
    }
}

void TProgramStep::ApplyProjection(std::shared_ptr<TDatumBatch>& batch) const {
    if (Projection.empty()) {
        return;
    }
    std::unordered_set<std::string_view> projSet;
    for (auto& str: Projection) {
        projSet.insert(str);
    }
    std::vector<std::shared_ptr<arrow::Field>> newFields;
    std::vector<arrow::Datum> newDatums;
    for (int64_t i = 0; i < batch->fields->num_fields(); ++i) {
        auto& cur_field_name = batch->fields->field(i)->name();
        if (projSet.contains(cur_field_name)) {
            newFields.push_back(batch->fields->field(i));
            Y_VERIFY(newFields.back());
            newDatums.push_back(batch->datums[i]);
        }
    }
    batch->fields = std::make_shared<arrow::Schema>(newFields);
    batch->datums = std::move(newDatums);
}

void TProgramStep::ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const {
    if (Projection.empty()) {
        return;
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto& column : Projection) {
        fields.push_back(batch->schema()->GetFieldByName(column));
        Y_VERIFY(fields.back());
    }
    batch = NArrow::ExtractColumns(batch, std::make_shared<arrow::Schema>(fields));
}

void TProgramStep::Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const {
    auto rb = ToTDatumBatch(batch);
    ApplyAssignes(rb, ctx);
    ApplyFilters(rb);
    ApplyProjection(rb);
    batch = ToRecordBatch(rb);
}

}
