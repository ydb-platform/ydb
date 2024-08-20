#include <memory>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <algorithm>

#include "program.h"
#include "custom_registry.h"
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
struct GroupByOptions: public arrow::compute::ScalarAggregateOptions {
    struct Assign {
        AggFunctionId function = AggFunctionId::AGG_UNSPECIFIED;
        std::string result_column;
        std::vector<std::string> arguments;
    };

    std::shared_ptr<arrow::Schema> schema;
    std::vector<Assign> assigns;
    bool has_nullable_key = true;
};
}
#endif
#include "common/container.h"

#include <util/system/yassert.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/yql/core/arrow_kernels/request/request.h>

namespace NKikimr::NSsa {

template <class TAssignObject>
class TInternalFunction : public IStepFunction<TAssignObject> {
    using TBase = IStepFunction<TAssignObject>;
public:
    using TBase::TBase;
    arrow::Result<arrow::Datum> Call(const TAssignObject& assign,  const TDatumBatch& batch) const override {
        auto arguments = TBase::BuildArgs(batch, assign.GetArguments());
        if (!arguments) {
            return arrow::Status::Invalid("Error parsing args.");
        }
        auto funcNames = GetRegistryFunctionNames(assign.GetOperation());

        arrow::Result<arrow::Datum> result = arrow::Status::UnknownError<std::string>("unknown function");
        for (const auto& funcName : funcNames) {
            if (TBase::Ctx && TBase::Ctx->func_registry()->GetFunction(funcName).ok()) {
                result = arrow::compute::CallFunction(funcName, *arguments, assign.GetOptions(), TBase::Ctx);
            } else {
                result = arrow::compute::CallFunction(funcName, *arguments, assign.GetOptions());
            }
            if (result.ok() && funcName == "count"sv) {
                result = result->scalar()->CastTo(std::make_shared<arrow::UInt64Type>());
            }
            if (result.ok()) {
                return PrepareResult(std::move(*result), assign);
            }
        }
        return result;
    }
private:
    virtual std::vector<std::string> GetRegistryFunctionNames(const typename TAssignObject::TOperationType& opId) const = 0;
    virtual arrow::Result<arrow::Datum> PrepareResult(arrow::Datum&& datum, const TAssignObject& assign) const {
        Y_UNUSED(assign);
        return std::move(datum);
    }
};

class TConstFunction : public IStepFunction<TAssign>  {
    using TBase = IStepFunction<TAssign>;
public:
    using TBase::TBase;
    arrow::Result<arrow::Datum> Call(const TAssign& assign, const TDatumBatch& batch) const override {
        Y_UNUSED(batch);
        return assign.GetConstant();
    }
};

class TAggregateFunction : public TInternalFunction<TAggregateAssign> {
    using TBase = TInternalFunction<TAggregateAssign>;
private:
    using TBase::TBase;
    std::vector<std::string> GetRegistryFunctionNames(const EAggregate& opId) const override {
        return { GetFunctionName(opId), GetHouseFunctionName(opId)};
    }
    arrow::Result<arrow::Datum> PrepareResult(arrow::Datum&& datum, const TAggregateAssign& assign) const override {
        if (!datum.is_scalar()) {
            return arrow::Status::Invalid("Aggregate result is not a scalar.");
        }

        if (datum.scalar()->type->id() == arrow::Type::STRUCT) {
            auto op = assign.GetOperation();
            if (op == EAggregate::Min) {
                const auto& minMax = datum.scalar_as<arrow::StructScalar>();
                return minMax.value[0];
            } else if (op == EAggregate::Max) {
                const auto& minMax = datum.scalar_as<arrow::StructScalar>();
                return minMax.value[1];
            } else {
                return arrow::Status::Invalid("Unexpected struct result for aggregate function.");
            }
        }
        if (!datum.type()) {
            return arrow::Status::Invalid("Aggregate result has no type.");
        }
        return std::move(datum);
    }
};

class TSimpleFunction : public TInternalFunction<TAssign> {
    using TBase = TInternalFunction<TAssign>;
private:
    using TBase::TBase;
    virtual std::vector<std::string> GetRegistryFunctionNames(const EOperation& opId) const override {
        return { GetFunctionName(opId) };
    }
};

template <class TAssignObject>
class TKernelFunction : public IStepFunction<TAssignObject> {
    using TBase = IStepFunction<TAssignObject>;
    const TFunctionPtr Function;

public:
    TKernelFunction(const TFunctionPtr kernelsFunction, arrow::compute::ExecContext* ctx)
        : TBase(ctx)
        , Function(kernelsFunction)
    {
        AFL_VERIFY(Function);
    }

    arrow::Result<arrow::Datum> Call(const TAssignObject& assign, const TDatumBatch& batch) const override {
        auto arguments = TBase::BuildArgs(batch, assign.GetArguments());
        if (!arguments) {
            return arrow::Status::Invalid("Error parsing args.");
        }
        try {
            return Function->Execute(*arguments, assign.GetOptions(), TBase::Ctx);
        } catch (const std::exception& ex) {
            return arrow::Status::ExecutionError(ex.what());
        }
    }
};

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

EOperation ValidateOperation(EOperation op, ui32 argsSize) {
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
        case EOperation::StartsWith:
        case EOperation::EndsWith:
        case EOperation::MatchSubstring:
        case EOperation::MatchLike:
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

CH::GroupByOptions::Assign GetGroupByAssign(const TAggregateAssign& assign) {
    CH::GroupByOptions::Assign descr;
    descr.function = GetHouseFunction(assign.GetOperation());
    descr.result_column = assign.GetName();
    descr.arguments.reserve(assign.GetArguments().size());

    for (auto& colName : assign.GetArguments()) {
        descr.arguments.push_back(colName.GetColumnName());
    }
    return descr;
}

class TFilterVisitor : public arrow::ArrayVisitor {
    std::vector<bool> FiltersMerged;
    ui32 CursorIdx = 0;
    bool Started = false;
public:
    void BuildColumnFilter(NArrow::TColumnFilter& result) {
        result = NArrow::TColumnFilter(std::move(FiltersMerged));
    }

    arrow::Status Visit(const arrow::BooleanArray& array) override {
        return VisitImpl(array);
    }

    arrow::Status Visit(const arrow::Int8Array& array) override {
        return VisitImpl(array);
    }

    arrow::Status Visit(const arrow::UInt8Array& array) override {
        return VisitImpl(array);
    }

    TFilterVisitor(const ui32 rowsCount) {
        FiltersMerged.resize(rowsCount, true);
    }

    class TModificationGuard: public TNonCopyable {
    private:
        TFilterVisitor& Owner;
    public:
        TModificationGuard(TFilterVisitor& owner)
            : Owner(owner)
        {
            Owner.CursorIdx = 0;
            AFL_VERIFY(!Owner.Started);
            Owner.Started = true;
        }

        ~TModificationGuard() {
            AFL_VERIFY(Owner.CursorIdx == Owner.FiltersMerged.size());
            Owner.Started = false;
        }
    };

    TModificationGuard StartVisit() {
        return TModificationGuard(*this);
    }

private:
    template <class TArray>
    arrow::Status VisitImpl(const TArray& array) {
        AFL_VERIFY(Started);
        for (ui32 i = 0; i < FiltersMerged.size(); ++i) {
            const bool columnValue = (bool)array.Value(i);
            const ui32 currentIdx = CursorIdx++;
            FiltersMerged[currentIdx] = FiltersMerged[currentIdx] && columnValue;
        }
        AFL_VERIFY(CursorIdx <= FiltersMerged.size());
        return arrow::Status::OK();
    }
};

}


arrow::Status TDatumBatch::AddColumn(const std::string& name, arrow::Datum&& column) {
    if (HasColumn(name)) {
        return arrow::Status::Invalid("Trying to add duplicate column '" + name + "'");
    }

    auto field = arrow::field(name, column.type());
    if (!field || !field->type()->Equals(column.type())) {
        return arrow::Status::Invalid("Cannot create field.");
    }
    if (!column.is_scalar() && column.length() != Rows) {
        return arrow::Status::Invalid("Wrong column length.");
    }

    NewColumnIds.emplace(name, NewColumnsPtr.size());
    NewColumnsPtr.emplace_back(field);

    Datums.emplace_back(column);
    return arrow::Status::OK();
}

arrow::Result<arrow::Datum> TDatumBatch::GetColumnByName(const std::string& name) const {
    auto it = NewColumnIds.find(name);
    if (it != NewColumnIds.end()) {
        AFL_VERIFY(SchemaBase->num_fields() + it->second < Datums.size());
        return Datums[SchemaBase->num_fields() + it->second];
    }
    auto i = SchemaBase->GetFieldIndex(name);
    if (i < 0) {
        return arrow::Status::Invalid("Not found column '" + name + "' or duplicate");
    }
    return Datums[i];
}

std::shared_ptr<arrow::Table> TDatumBatch::ToTable() {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    columns.reserve(Datums.size());
    for (auto col : Datums) {
        if (col.is_scalar()) {
            columns.push_back(std::make_shared<arrow::ChunkedArray>(NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(*col.scalar(), Rows))));
        } else if (col.is_array()) {
            if (col.length() == -1) {
                return {};
            }
            columns.push_back(std::make_shared<arrow::ChunkedArray>(col.make_array()));
        } else if (col.is_arraylike()) {
            if (col.length() == -1) {
                return {};
            }
            columns.push_back(col.chunked_array());
        } else {
            AFL_VERIFY(false);
        }
    }
    return arrow::Table::Make(GetSchema(), columns, Rows);
}

std::shared_ptr<arrow::RecordBatch> TDatumBatch::ToRecordBatch() {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(Datums.size());
    for (auto col : Datums) {
        if (col.is_scalar()) {
            columns.push_back(NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(*col.scalar(), Rows)));
        } else if (col.is_array()) {
            if (col.length() == -1) {
                return {};
            }
            columns.push_back(col.make_array());
        } else {
            AFL_VERIFY(false);
        }
    }
    return arrow::RecordBatch::Make(GetSchema(), Rows, columns);
}

std::shared_ptr<TDatumBatch> TDatumBatch::FromRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::vector<arrow::Datum> datums;
    datums.reserve(batch->num_columns());
    for (int64_t i = 0; i < batch->num_columns(); ++i) {
        datums.push_back(arrow::Datum(batch->column(i)));
    }
    return std::make_shared<TDatumBatch>(std::make_shared<arrow::Schema>(*batch->schema()), std::move(datums), batch->num_rows());
}

std::shared_ptr<TDatumBatch> TDatumBatch::FromTable(const std::shared_ptr<arrow::Table>& batch) {
    std::vector<arrow::Datum> datums;
    datums.reserve(batch->num_columns());
    for (int64_t i = 0; i < batch->num_columns(); ++i) {
        datums.push_back(arrow::Datum(batch->column(i)));
    }
    return std::make_shared<TDatumBatch>(std::make_shared<arrow::Schema>(*batch->schema()), std::move(datums), batch->num_rows());
}

TDatumBatch::TDatumBatch(const std::shared_ptr<arrow::Schema>& schema, std::vector<arrow::Datum>&& datums, const i64 rows)
    : SchemaBase(schema)
    , Rows(rows)
    , Datums(std::move(datums)) {
    AFL_VERIFY(SchemaBase);
    AFL_VERIFY(Datums.size() == (ui32)SchemaBase->num_fields());
}

TAssign TAssign::MakeTimestamp(const TColumnInfo& column, ui64 value) {
    return TAssign(column, std::make_shared<arrow::TimestampScalar>(value, arrow::timestamp(arrow::TimeUnit::MICRO)));
}

IStepFunction<TAssign>::TPtr TAssign::GetFunction(arrow::compute::ExecContext* ctx) const {
    if (KernelFunction) {
        return std::make_shared<TKernelFunction<TAssign>>(KernelFunction, ctx);
    }
    if (IsConstant()) {
        return std::make_shared<TConstFunction>(ctx);
    }
    return std::make_shared<TSimpleFunction>(ctx);
}

TString TAssign::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    if (Operation != EOperation::Unspecified) {
        sb << "op=" << Operation << ";";
    }
    if (YqlOperationId) {
        sb << "yql_op=" << (NYql::TKernelRequestBuilder::EBinaryOp)*YqlOperationId << ";";
    }
    if (Arguments.size()) {
        sb << "arguments=[";
        for (auto&& i : Arguments) {
            sb << i.DebugString() << ";";
        }
        sb << "];";
    }
    if (Constant) {
        sb << "const=" << Constant->ToString() << ";";
    }
    if (KernelFunction) {
        sb << "kernel=" << KernelFunction->name() << ";";
    }
    sb << "column=" << Column.DebugString() << ";";
    sb << "}";
    return sb;
}

IStepFunction<TAggregateAssign>::TPtr TAggregateAssign::GetFunction(arrow::compute::ExecContext* ctx) const {
    if (KernelFunction) {
        return std::make_shared<TKernelFunction<TAggregateAssign>>(KernelFunction, ctx);
    }
    return std::make_shared<TAggregateFunction>(ctx);
}

TString TAggregateAssign::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    if (Operation != EAggregate::Unspecified) {
        sb << "op=" << GetFunctionName(Operation) << ";";
    }
    if (Arguments.size()) {
        sb << "arguments=[";
        for (auto&& i : Arguments) {
            sb << i.DebugString() << ";";
        }
        sb << "];";
    }
    sb << "options=" << ScalarOpts.ToString() << ";";
    if (KernelFunction) {
        sb << "kernel=" << KernelFunction->name() << ";";
    }
    sb << "column=" << Column.DebugString() << ";";
    sb << "}";
    return sb;
}

arrow::Status TProgramStep::ApplyAssignes(TDatumBatch& batch, arrow::compute::ExecContext* ctx) const {
    if (Assignes.empty()) {
        return arrow::Status::OK();
    }
    batch.Datums.reserve(batch.Datums.size() + Assignes.size());
    for (auto& assign : Assignes) {
        if (batch.HasColumn(assign.GetName())) {
            return arrow::Status::Invalid("Assign to existing column '" + assign.GetName() + "'.");
        }

        auto funcResult = assign.GetFunction(ctx)->Call(assign, batch);
        if (!funcResult.ok()) {
            return funcResult.status();
        }
        arrow::Datum column = *funcResult;
        auto status = batch.AddColumn(assign.GetName(), std::move(column));
        if (!status.ok()) {
            return status;
        }
    }
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyAggregates(TDatumBatch& batch, arrow::compute::ExecContext* ctx) const {
    if (GroupBy.empty()) {
        return arrow::Status::OK();
    }

    ui32 numResultColumns = GroupBy.size() + GroupByKeys.size();
    std::vector<arrow::Datum> datums;
    datums.reserve(numResultColumns);
    std::optional<ui32> resultRecordsCount;

    arrow::FieldVector fields;
    fields.reserve(numResultColumns);

    if (GroupByKeys.empty()) {
        for (auto& assign : GroupBy) {
            auto funcResult = assign.GetFunction(ctx)->Call(assign, batch);
            if (!funcResult.ok()) {
                return funcResult.status();
            }
            datums.push_back(*funcResult);
            fields.emplace_back(std::make_shared<arrow::Field>(assign.GetName(), datums.back().type()));
        }
        resultRecordsCount = 1;
    } else {
        CH::GroupByOptions funcOpts;
        funcOpts.schema = batch.GetSchema();
        funcOpts.assigns.reserve(numResultColumns);
        funcOpts.has_nullable_key = false;

        for (auto& assign : GroupBy) {
            funcOpts.assigns.emplace_back(GetGroupByAssign(assign));
        }

        for (auto& key : GroupByKeys) {
            funcOpts.assigns.emplace_back(CH::GroupByOptions::Assign{
                .result_column = key.GetColumnName()
            });

            if (!funcOpts.has_nullable_key) {
                auto res = batch.GetColumnByName(key.GetColumnName());
                if (!res.ok()) {
                    return arrow::Status::Invalid("No such key for GROUP BY.");
                }
                if (!(*res).is_array()) {
                    return arrow::Status::Invalid("Unexpected GROUP BY key type.");
                }

                funcOpts.has_nullable_key = (*res).array()->MayHaveNulls();
            }
        }

        auto gbRes = arrow::compute::CallFunction(GetHouseGroupByName(), batch.Datums, &funcOpts, ctx);
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
            datums.push_back(column);
        }

        resultRecordsCount = gbBatch->num_rows();
    }
    AFL_VERIFY(resultRecordsCount);
    batch = TDatumBatch(std::make_shared<arrow::Schema>(std::move(fields)), std::move(datums), *resultRecordsCount);
    return arrow::Status::OK();
}

arrow::Status TProgramStep::MakeCombinedFilter(TDatumBatch& batch, NArrow::TColumnFilter& result) const {
    TFilterVisitor filterVisitor(batch.GetRecordsCount());
    for (auto& colName : Filters) {
        auto column = batch.GetColumnByName(colName.GetColumnName());
        if (!column.ok()) {
            return column.status();
        }
        if (column->is_array()) {
            auto g = filterVisitor.StartVisit();
            auto columnArray = column->make_array();
            NArrow::TStatusValidator::Validate(columnArray->Accept(&filterVisitor));
        } else if (column->is_arraylike()) {
            auto columnArray = column->chunked_array();
            auto g = filterVisitor.StartVisit();
            for (auto&& i : columnArray->chunks()) {
                NArrow::TStatusValidator::Validate(i->Accept(&filterVisitor));
            }
        } else {
            AFL_VERIFY(false)("column", colName.GetColumnName());
        }
    }
    filterVisitor.BuildColumnFilter(result);
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyFilters(TDatumBatch& batch) const {
    if (Filters.empty()) {
        return arrow::Status::OK();
    }

    NArrow::TColumnFilter bits = NArrow::TColumnFilter::BuildAllowFilter();
    NArrow::TStatusValidator::Validate(MakeCombinedFilter(batch, bits));
    if (bits.IsTotalAllowFilter()) {
        return arrow::Status::OK();
    }
    std::unordered_set<std::string_view> neededColumns;
    const bool allColumns = Projection.empty() && GroupBy.empty();
    if (!allColumns) {
        for (auto& aggregate : GroupBy) {
            for (auto& arg : aggregate.GetArguments()) {
                neededColumns.insert(arg.GetColumnName());
            }
        }
        for (auto& key : GroupByKeys) {
            neededColumns.insert(key.GetColumnName());
        }
        for (auto& str : Projection) {
            neededColumns.insert(str.GetColumnName());
        }
    }
    std::vector<arrow::Datum*> filterDatums;
    for (int64_t i = 0; i < batch.GetSchema()->num_fields(); ++i) {
        if (batch.Datums[i].is_arraylike() && (allColumns || neededColumns.contains(batch.GetSchema()->field(i)->name()))) {
            filterDatums.emplace_back(&batch.Datums[i]);
        }
    }
    bits.Apply(batch.GetRecordsCount(), filterDatums);
    batch.SetRecordsCount(bits.GetFilteredCount().value_or(batch.GetRecordsCount()));
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyProjection(TDatumBatch& batch) const {
    if (Projection.empty()) {
        return arrow::Status::OK();
    }
    std::vector<std::shared_ptr<arrow::Field>> newFields;
    std::vector<arrow::Datum> newDatums;
    for (size_t i = 0; i < Projection.size(); ++i) {
        int schemaFieldIndex = batch.GetSchema()->GetFieldIndex(Projection[i].GetColumnName());
        if (schemaFieldIndex == -1) {
            return arrow::Status::Invalid("Could not find column " + Projection[i].GetColumnName() + " in record batch schema.");
        }
        newFields.push_back(batch.GetSchema()->field(schemaFieldIndex));
        newDatums.push_back(batch.Datums[schemaFieldIndex]);
    }
    batch = TDatumBatch(std::make_shared<arrow::Schema>(std::move(newFields)), std::move(newDatums), batch.GetRecordsCount());
    return arrow::Status::OK();
}

arrow::Status TProgramStep::ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const {
    if (Projection.empty()) {
        return arrow::Status::OK();
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto& column : Projection) {
        fields.push_back(batch->schema()->GetFieldByName(column.GetColumnName()));
        if (!fields.back()) {
            return arrow::Status::Invalid("Wrong projection column '" + column.GetColumnName() + "'.");
        }
    }
    batch = NArrow::TColumnOperator().Adapt(batch, std::make_shared<arrow::Schema>(std::move(fields))).DetachResult();
    return arrow::Status::OK();
}

arrow::Status TProgramStep::Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const {
    auto rb = TDatumBatch::FromRecordBatch(batch);

    {
        auto status = ApplyAssignes(*rb, ctx);
        if (!status.ok()) {
            return status;
        }
    }
    {
        auto status = ApplyFilters(*rb);
        if (!status.ok()) {
            return status;
        }
    }
    {
        auto status = ApplyAggregates(*rb, ctx);
        if (!status.ok()) {
            return status;
        }
    }
    {
        auto status = ApplyProjection(*rb);
        if (!status.ok()) {
            return status;
        }
    }

    batch = (*rb).ToRecordBatch();
    if (!batch) {
        return arrow::Status::Invalid("Failed to create program result.");
    }
    return arrow::Status::OK();
}

std::set<std::string> TProgramStep::GetColumnsInUsage(const bool originalOnly/* = false*/) const {
    std::set<std::string> result;
    for (auto&& i : Filters) {
        if (!originalOnly || !i.IsGenerated()) {
            result.emplace(i.GetColumnName());
        }
    }
    for (auto&& i : Assignes) {
        for (auto&& f : i.GetArguments()) {
            if (!originalOnly || !f.IsGenerated()) {
                result.emplace(f.GetColumnName());
            }
        }
    }
    return result;
}

arrow::Result<std::shared_ptr<NArrow::TColumnFilter>> TProgramStep::BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& t) const {
    if (Filters.empty()) {
        return nullptr;
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = NArrow::SliceToRecordBatches(t->BuildTableVerified(GetColumnsInUsage(true)));
    NArrow::TColumnFilter fullLocal = NArrow::TColumnFilter::BuildAllowFilter();
    for (auto&& rb : batches) {
        auto datumBatch = TDatumBatch::FromRecordBatch(rb);
        {
            auto statusAssign = ApplyAssignes(*datumBatch, NArrow::GetCustomExecContext());
            if (!statusAssign.ok()) {
                return statusAssign;
            }
        }
        NArrow::TColumnFilter local = NArrow::TColumnFilter::BuildAllowFilter();
        NArrow::TStatusValidator::Validate(MakeCombinedFilter(*datumBatch, local));
        AFL_VERIFY(local.Size() == datumBatch->GetRecordsCount())("local", local.Size())("datum", datumBatch->GetRecordsCount());
        fullLocal.Append(local);
    }
    AFL_VERIFY(fullLocal.Size() == t->num_rows())("filter", fullLocal.Size())("t", t->num_rows());
    return std::make_shared<NArrow::TColumnFilter>(std::move(fullLocal));
}

const std::set<ui32>& TProgramStep::GetFilterOriginalColumnIds() const {
//    AFL_VERIFY(IsFilterOnly());
    return FilterOriginalColumnIds;
}

std::set<std::string> TProgram::GetEarlyFilterColumns() const {
    std::set<std::string> result;
    for (ui32 i = 0; i < Steps.size(); ++i) {
        auto stepFields = Steps[i]->GetColumnsInUsage(true);
        result.insert(stepFields.begin(), stepFields.end());
        if (!Steps[i]->IsFilterOnly()) {
            break;
        }
    }
    return result;
}

std::set<std::string> TProgram::GetProcessingColumns() const {
    std::set<std::string> result;
    for (auto&& i : SourceColumns) {
        result.emplace(i.second.GetColumnName());
    }
    return result;
}

}
