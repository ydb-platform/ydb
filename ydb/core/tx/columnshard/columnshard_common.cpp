#include "columnshard_common.h"
#include <ydb/core/formats/arrow_batch_builder.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NColumnShard {

namespace {

TVector<NScheme::TTypeInfo> ExtractTypes(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    TVector<NScheme::TTypeInfo> types;
    types.reserve(columns.size());
    for (auto& [name, type] : columns) {
        types.push_back(type);
    }
    return types;
}

TString FromCells(const TConstArrayRef<TCell>& cells, const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    Y_VERIFY(cells.size() == columns.size());
    if (cells.empty()) {
        return {};
    }

    TVector<NScheme::TTypeInfo> types = ExtractTypes(columns);

    NArrow::TArrowBatchBuilder batchBuilder;
    batchBuilder.Reserve(1);
    bool ok = batchBuilder.Start(columns);
    Y_VERIFY(ok);

    batchBuilder.AddRow(NKikimr::TDbTupleRef(), NKikimr::TDbTupleRef(types.data(), cells.data(), cells.size()));

    auto batch = batchBuilder.FlushBatch(false);
    Y_VERIFY(batch);
    Y_VERIFY(batch->num_columns() == (int)cells.size());
    Y_VERIFY(batch->num_rows() == 1);
    return NArrow::SerializeBatchNoCompression(batch);
}

struct TContext {
    const IColumnResolver& ColumnResolver;
    mutable THashMap<ui32, TString> Sources;

    explicit TContext(const IColumnResolver& columnResolver)
        : ColumnResolver(columnResolver)
    {}

    std::string GetName(const NKikimrSSA::TProgram::TColumn& column) const {
        ui32 columnId = column.GetId();
        TString name = ColumnResolver.GetColumnName(columnId, false);
        if (name.Empty()) {
            return GenerateName(column);
        } else {
            Sources[columnId] = name;
        }
        return std::string(name.data(), name.size());
    }

    std::string GenerateName(const NKikimrSSA::TProgram::TColumn& column) const {
        TString name;
        if (column.HasName()) {
            name = column.GetName();
        } else {
            name = ToString(column.GetId());
        }
        return std::string(name.data(), name.size());
    }
};

NArrow::TAssign MakeFunction(const TContext& info, const std::string& name,
                            const NKikimrSSA::TProgram::TAssignment::TFunction& func) {
    using TId = NKikimrSSA::TProgram::TAssignment;
    using EOperation = NArrow::EOperation;
    using TAssign = NArrow::TAssign;

    std::vector<std::string> arguments;
    for (auto& col : func.GetArguments()) {
        arguments.push_back(info.GetName(col));
    }

    switch (func.GetId()) {
        case TId::FUNC_CMP_EQUAL:
            return TAssign(name, EOperation::Equal, std::move(arguments));
        case TId::FUNC_CMP_NOT_EQUAL:
            return TAssign(name, EOperation::NotEqual, std::move(arguments));
        case TId::FUNC_CMP_LESS:
            return TAssign(name, EOperation::Less, std::move(arguments));
        case TId::FUNC_CMP_LESS_EQUAL:
            return TAssign(name, EOperation::LessEqual, std::move(arguments));
        case TId::FUNC_CMP_GREATER:
            return TAssign(name, EOperation::Greater, std::move(arguments));
        case TId::FUNC_CMP_GREATER_EQUAL:
            return TAssign(name, EOperation::GreaterEqual, std::move(arguments));
        case TId::FUNC_IS_NULL:
            return TAssign(name, EOperation::IsNull, std::move(arguments));
        case TId::FUNC_STR_LENGTH:
            return TAssign(name, EOperation::BinaryLength, std::move(arguments));
        case TId::FUNC_STR_MATCH:
            return TAssign(name, EOperation::MatchSubstring, std::move(arguments));
        case TId::FUNC_BINARY_NOT:
            return TAssign(name, EOperation::Invert, std::move(arguments));
        case TId::FUNC_BINARY_AND:
            return TAssign(name, EOperation::And, std::move(arguments));
        case TId::FUNC_BINARY_OR:
            return TAssign(name, EOperation::Or, std::move(arguments));
        case TId::FUNC_BINARY_XOR:
            return TAssign(name, EOperation::Xor, std::move(arguments));
        case TId::FUNC_MATH_ADD:
            return TAssign(name, EOperation::Add, std::move(arguments));
        case TId::FUNC_MATH_SUBTRACT:
            return TAssign(name, EOperation::Subtract, std::move(arguments));
        case TId::FUNC_MATH_MULTIPLY:
            return TAssign(name, EOperation::Multiply, std::move(arguments));
        case TId::FUNC_MATH_DIVIDE:
            return TAssign(name, EOperation::Divide, std::move(arguments));
        case TId::FUNC_CAST_TO_INT32:
        {
            // TODO: support CAST with OrDefault/OrNull logic (second argument is default value)
            auto castOpts = std::make_shared<arrow::compute::CastOptions>(false);
            castOpts->to_type = std::make_shared<arrow::Int32Type>();
            return TAssign(name, EOperation::CastInt32, std::move(arguments), castOpts);
        }
        case TId::FUNC_CAST_TO_TIMESTAMP:
        {
            auto castOpts = std::make_shared<arrow::compute::CastOptions>(false);
            castOpts->to_type = std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO);
            return TAssign(name, EOperation::CastTimestamp, std::move(arguments), castOpts);
        }
        case TId::FUNC_CAST_TO_INT8:
        case TId::FUNC_CAST_TO_INT16:
        case TId::FUNC_CAST_TO_INT64:
        case TId::FUNC_CAST_TO_UINT8:
        case TId::FUNC_CAST_TO_UINT16:
        case TId::FUNC_CAST_TO_UINT32:
        case TId::FUNC_CAST_TO_UINT64:
        case TId::FUNC_CAST_TO_FLOAT:
        case TId::FUNC_CAST_TO_DOUBLE:
        case TId::FUNC_CAST_TO_BINARY:
        case TId::FUNC_CAST_TO_FIXED_SIZE_BINARY:
        case TId::FUNC_UNSPECIFIED:
            break;
    }
    return TAssign(name, EOperation::Unspecified, std::move(arguments));
}

NArrow::TAssign MakeConstant(const std::string& name, const NKikimrSSA::TProgram::TConstant& constant) {
    using TId = NKikimrSSA::TProgram::TConstant;
    using EOperation = NArrow::EOperation;
    using TAssign = NArrow::TAssign;

    switch (constant.GetValueCase()) {
        case TId::kBool:
            return TAssign(name, constant.GetBool());
        case TId::kInt32:
            return TAssign(name, constant.GetInt32());
        case TId::kUint32:
            return TAssign(name, constant.GetUint32());
        case TId::kInt64:
            return TAssign(name, constant.GetInt64());
        case TId::kUint64:
            return TAssign(name, constant.GetUint64());
        case TId::kFloat:
            return TAssign(name, constant.GetFloat());
        case TId::kDouble:
            return TAssign(name, constant.GetDouble());
        case TId::kBytes:
        {
            TString str = constant.GetBytes();
            return TAssign(name, std::string(str.data(), str.size()));
        }
        case TId::kText:
        {
            TString str = constant.GetText();
            return TAssign(name, std::string(str.data(), str.size()));
        }
        case TId::VALUE_NOT_SET:
            break;
    }
    return TAssign(name, EOperation::Unspecified, {});
}

NArrow::TAggregateAssign MakeAggregate(const TContext& info, const std::string& name,
                                       const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& func)
{
    using TId = NKikimrSSA::TProgram::TAggregateAssignment;
    using EAggregate = NArrow::EAggregate;
    using TAggregateAssign = NArrow::TAggregateAssign;

    if (func.ArgumentsSize() == 1) {
        std::string argument = info.GetName(func.GetArguments()[0]);

        switch (func.GetId()) {
            case TId::AGG_SOME:
                return TAggregateAssign(name, EAggregate::Some, std::move(argument));
            case TId::AGG_COUNT:
                return TAggregateAssign(name, EAggregate::Count, std::move(argument));
            case TId::AGG_MIN:
                return TAggregateAssign(name, EAggregate::Min, std::move(argument));
            case TId::AGG_MAX:
                return TAggregateAssign(name, EAggregate::Max, std::move(argument));
            case TId::AGG_SUM:
                return TAggregateAssign(name, EAggregate::Sum, std::move(argument));
#if 0 // TODO
            case TId::AGG_AVG:
                return TAggregateAssign(name, EAggregate::Avg, std::move(argument));
#endif
            case TId::AGG_UNSPECIFIED:
                break;
        }
    } else if (func.ArgumentsSize() == 0 && func.GetId() == TId::AGG_COUNT) {
        // COUNT(*) case
        return TAggregateAssign(name, EAggregate::Count, {});
    }
    return TAggregateAssign(name, EAggregate::Unspecified, {});
}

NArrow::TAssign MaterializeParameter(const std::string& name, const NKikimrSSA::TProgram::TParameter& parameter,
    const std::shared_ptr<arrow::RecordBatch>& parameterValues)
{
    using TAssign = NArrow::TAssign;

    auto parameterName = parameter.GetName();
    auto column = parameterValues->GetColumnByName(parameterName);
#if 0
    Y_VERIFY(
        column,
        "No parameter %s in serialized parameters.", parameterName.c_str()
    );
    Y_VERIFY(
        column->length() == 1,
        "Incorrect values count in parameter array"
    );
#else
    if (!column || column->length() != 1) {
        return TAssign(name, NArrow::EOperation::Unspecified, {});
    }
#endif
    return TAssign(name, *column->GetScalar(0));
}

bool ExtractAssign(const TContext& info, NArrow::TProgramStep& step, const NKikimrSSA::TProgram::TAssignment& assign,
    const std::shared_ptr<arrow::RecordBatch>& parameterValues)
{
    using TId = NKikimrSSA::TProgram::TAssignment;

    std::string columnName = info.GetName(assign.GetColumn());

    switch (assign.GetExpressionCase()) {
        case TId::kFunction:
        {
            auto func = MakeFunction(info, columnName, assign.GetFunction());
            if (!func.IsOk()) {
                return false;
            }
            step.Assignes.emplace_back(std::move(func));
            break;
        }
        case TId::kConstant:
        {
            auto cnst = MakeConstant(columnName, assign.GetConstant());
            if (!cnst.IsConstant()) {
                return false;
            }
            step.Assignes.emplace_back(std::move(cnst));
            break;
        }
        case TId::kParameter:
        {
            auto param = MaterializeParameter(columnName, assign.GetParameter(), parameterValues);
            if (!param.IsConstant()) {
                return false;
            }
            step.Assignes.emplace_back(std::move(param));
            break;
        }
        case TId::kExternalFunction:
        case TId::kNull:
        case TId::EXPRESSION_NOT_SET:
            return false;
    }
    return true;
}

bool ExtractFilter(const TContext& info, NArrow::TProgramStep& step, const NKikimrSSA::TProgram::TFilter& filter) {
    auto& column = filter.GetPredicate();
    if (!column.HasId() && !column.HasName()) {
        return false;
    }
    // NOTE: Name maskes Id for column. If column assigned with name it's accessible only by name.
    step.Filters.push_back(info.GetName(column));
    return true;
}

bool ExtractProjection(const TContext& info, NArrow::TProgramStep& step,
                       const NKikimrSSA::TProgram::TProjection& projection) {
    step.Projection.reserve(projection.ColumnsSize());
    for (auto& col : projection.GetColumns()) {
        // NOTE: Name maskes Id for column. If column assigned with name it's accessible only by name.
        step.Projection.push_back(info.GetName(col));
    }
    return true;
}

bool ExtractGroupBy(const TContext& info, NArrow::TProgramStep& step, const NKikimrSSA::TProgram::TGroupBy& groupBy) {
    if (!groupBy.AggregatesSize()) {
        return false;
    }

    // It adds implicit projection with aggregates and keys. Remove non aggregated columns.
    step.Projection.reserve(groupBy.KeyColumnsSize() + groupBy.AggregatesSize());
    for (auto& col : groupBy.GetKeyColumns()) {
        step.Projection.push_back(info.GetName(col));
    }

    step.GroupBy.reserve(groupBy.AggregatesSize());
    step.GroupByKeys.reserve(groupBy.KeyColumnsSize());
    for (auto& agg : groupBy.GetAggregates()) {
        auto& resColumn = agg.GetColumn();
        TString columnName = info.GenerateName(resColumn);

        auto func = MakeAggregate(info, columnName, agg.GetFunction());
        if (!func.IsOk()) {
            return false;
        }
        step.GroupBy.push_back(std::move(func));
        step.Projection.push_back(columnName);
    }
    for (auto& key : groupBy.GetKeyColumns()) {
        step.GroupByKeys.push_back(info.GetName(key));
    }

    return true;
}

}

using EOperation = NArrow::EOperation;
using TPredicate = NOlap::TPredicate;

std::pair<TPredicate, TPredicate> RangePredicates(const TSerializedTableRange& range,
                                                  const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    TVector<TCell> leftCells;
    TVector<std::pair<TString, NScheme::TTypeInfo>> leftColumns;
    bool leftTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.From.GetCells();
        const size_t size = cells.size();
        Y_ASSERT(size <= columns.size());
        leftCells.reserve(size);
        leftColumns.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (!cells[i].IsNull()) {
                leftCells.push_back(cells[i]);
                leftColumns.push_back(columns[i]);
                leftTrailingNull = false;
            } else {
                leftTrailingNull = true;
            }
        }
    }

    TVector<TCell> rightCells;
    TVector<std::pair<TString, NScheme::TTypeInfo>> rightColumns;
    bool rightTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.To.GetCells();
        const size_t size = cells.size();
        Y_ASSERT(size <= columns.size());
        rightCells.reserve(size);
        rightColumns.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (!cells[i].IsNull()) {
                rightCells.push_back(cells[i]);
                rightColumns.push_back(columns[i]);
                rightTrailingNull = false;
            } else {
                rightTrailingNull = true;
            }
        }
    }

    bool fromInclusive = range.FromInclusive || leftTrailingNull;
    bool toInclusive = range.ToInclusive && !rightTrailingNull;

    TString leftBorder = FromCells(leftCells, leftColumns);
    TString rightBorder = FromCells(rightCells, rightColumns);
    return std::make_pair(
        TPredicate(EOperation::Greater, leftBorder, NArrow::MakeArrowSchema(leftColumns), fromInclusive),
        TPredicate(EOperation::Less, rightBorder, NArrow::MakeArrowSchema(rightColumns), toInclusive));
}

std::shared_ptr<NArrow::TSsaProgramSteps> TReadDescription::AddProgram(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program)
{
    using TId = NKikimrSSA::TProgram::TCommand;

    auto programSteps = std::make_shared<NArrow::TSsaProgramSteps>();
    TContext info(columnResolver);
    auto step = std::make_shared<NArrow::TProgramStep>();
    for (auto& cmd : program.GetCommand()) {
        switch (cmd.GetLineCase()) {
            case TId::kAssign:
                if (!ExtractAssign(info, *step, cmd.GetAssign(), ProgramParameters)) {
                    return nullptr;
                }
                break;
            case TId::kFilter:
                if (!ExtractFilter(info, *step, cmd.GetFilter())) {
                    return nullptr;
                }
                break;
            case TId::kProjection:
                if (!ExtractProjection(info, *step, cmd.GetProjection())) {
                    return nullptr;
                }
                programSteps->Program.push_back(step);
                step = std::make_shared<NArrow::TProgramStep>();
                break;
            case TId::kGroupBy:
                if (!ExtractGroupBy(info, *step, cmd.GetGroupBy())) {
                    return nullptr;
                }
                programSteps->Program.push_back(step);
                step = std::make_shared<NArrow::TProgramStep>();
                break;
            case TId::LINE_NOT_SET:
                return nullptr;
        }
    }

    // final step without final projection
    if (!step->Empty()) {
        programSteps->Program.push_back(step);
    }

    programSteps->ProgramSourceColumns = std::move(info.Sources);
    return programSteps;
}

}
