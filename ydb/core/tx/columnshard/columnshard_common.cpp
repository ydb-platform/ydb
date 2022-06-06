#include "columnshard_common.h"
#include <ydb/core/formats/arrow_batch_builder.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NColumnShard {

namespace {

TVector<NScheme::TTypeId> ExtractTypes(const TVector<std::pair<TString, NScheme::TTypeId>>& columns) {
    TVector<NScheme::TTypeId> types;
    types.reserve(columns.size());
    for (auto& [name, type] : columns) {
        types.push_back(type);
    }
    return types;
}

TString FromCells(const TConstArrayRef<TCell>& cells, const TVector<std::pair<TString, NScheme::TTypeId>>& columns) {
    Y_VERIFY(cells.size() == columns.size());
    if (cells.empty()) {
        return {};
    }

    TVector<NScheme::TTypeId> types = ExtractTypes(columns);

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
    THashMap<ui32, TString> Sources;

    explicit TContext(const IColumnResolver& columnResolver)
        : ColumnResolver(columnResolver)
    {}

    std::string GetName(ui32 columnId) {
        TString name = ColumnResolver.GetColumnName(columnId, false);
        if (name.Empty()) {
            name = ToString(columnId);
        } else {
            Sources[columnId] = name;
        }
        return std::string(name.data(), name.size());
    }
};

NArrow::TAssign MakeFunction(TContext& info, const std::string& name,
                            const NKikimrSSA::TProgram::TAssignment::TFunction& func) {
    using TId = NKikimrSSA::TProgram::TAssignment;
    using EOperation = NArrow::EOperation;
    using TAssign = NArrow::TAssign;

    auto& args = func.GetArguments();
    TVector<std::string> arguments;
    for (auto& col : func.GetArguments()) {
        ui32 columnId = col.GetId();
        arguments.push_back(info.GetName(columnId));
    }

    switch (func.GetId()) {
        case TId::FUNC_CMP_EQUAL:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Equal, std::move(arguments));
        case TId::FUNC_CMP_NOT_EQUAL:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::NotEqual, std::move(arguments));
        case TId::FUNC_CMP_LESS:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Less, std::move(arguments));
        case TId::FUNC_CMP_LESS_EQUAL:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::LessEqual, std::move(arguments));
        case TId::FUNC_CMP_GREATER:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Greater, std::move(arguments));
        case TId::FUNC_CMP_GREATER_EQUAL:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::GreaterEqual, std::move(arguments));
        case TId::FUNC_IS_NULL:
            Y_VERIFY(args.size() == 1);
            return TAssign(name, EOperation::IsNull, std::move(arguments));
        case TId::FUNC_STR_LENGTH:
            Y_VERIFY(args.size() == 1);
            return TAssign(name, EOperation::BinaryLength, std::move(arguments));
        case TId::FUNC_STR_MATCH:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::MatchSubstring, std::move(arguments));
        case TId::FUNC_BINARY_NOT:
            Y_VERIFY(args.size() == 1);
            return TAssign(name, EOperation::Invert, std::move(arguments));
        case TId::FUNC_BINARY_AND:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::And, std::move(arguments));
        case TId::FUNC_BINARY_OR:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Or, std::move(arguments));
        case TId::FUNC_BINARY_XOR:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Xor, std::move(arguments));
        case TId::FUNC_MATH_ADD:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Add, std::move(arguments));
        case TId::FUNC_MATH_SUBTRACT:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Subtract, std::move(arguments));
        case TId::FUNC_MATH_MULTIPLY:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Multiply, std::move(arguments));
        case TId::FUNC_MATH_DIVIDE:
            Y_VERIFY(args.size() == 2);
            return TAssign(name, EOperation::Divide, std::move(arguments));
        case TId::FUNC_UNSPECIFIED:
            break;
    }
    Y_VERIFY(false); // unexpected
}

NArrow::TAssign MakeConstant(const std::string& name, const NKikimrSSA::TProgram::TConstant& constant) {
    using TId = NKikimrSSA::TProgram::TConstant;
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
            Y_VERIFY(false); // unexpected
            break;
    }
}

NArrow::TAssign MaterializeParameter(const std::string& name, const NKikimrSSA::TProgram::TParameter& parameter,
    const std::shared_ptr<arrow::RecordBatch>& parameterValues)
{
    using TAssign = NArrow::TAssign;

    auto parameterName = parameter.GetName();
    auto column = parameterValues->GetColumnByName(parameterName);

    Y_VERIFY(
        column,
        "No parameter %s in serialized parameters.", parameterName.c_str()
    );
    Y_VERIFY(
        column->length() == 1,
        "Incorrect values count in parameter array"
    );

    return TAssign(name, *column->GetScalar(0));
}

void ExtractAssign(TContext& info, NArrow::TProgramStep& step, const NKikimrSSA::TProgram::TAssignment& assign,
    const std::shared_ptr<arrow::RecordBatch>& parameterValues)
{
    using TId = NKikimrSSA::TProgram::TAssignment;

    ui32 columnId = assign.GetColumn().GetId();
    std::string columnName = info.GetName(columnId);

    switch (assign.GetExpressionCase()) {
        case TId::kFunction:
        {
            step.Assignes.emplace_back(MakeFunction(info, columnName, assign.GetFunction()));
            break;
        }
        case TId::kConstant:
        {
            step.Assignes.emplace_back(MakeConstant(columnName, assign.GetConstant()));
            break;
        }
        case TId::kParameter:
        {
            step.Assignes.emplace_back(MaterializeParameter(columnName, assign.GetParameter(), parameterValues));
            break;
        }
        case TId::kExternalFunction:
        case TId::kNull:
        case TId::EXPRESSION_NOT_SET:
            Y_VERIFY(false); // not implemented
            break;
    }
}

void ExtractFilter(TContext& info, NArrow::TProgramStep& step, const NKikimrSSA::TProgram::TFilter& filter) {
    ui32 columnId = filter.GetPredicate().GetId();
    step.Filters.push_back(info.GetName(columnId));
}

void ExtractProjection(TContext& info, NArrow::TProgramStep& step, const NKikimrSSA::TProgram::TProjection& projection) {
    for (auto& col : projection.GetColumns()) {
        step.Projection.push_back(info.GetName(col.GetId()));
    }
}

}

using EOperation = NArrow::EOperation;
using TPredicate = NOlap::TPredicate;

std::pair<TPredicate, TPredicate> RangePredicates(const TSerializedTableRange& range,
                                                  const TVector<std::pair<TString, NScheme::TTypeId>>& columns) {
    TVector<TCell> leftCells;
    TVector<std::pair<TString, NScheme::TTypeId>> leftColumns;
    bool leftTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.From.GetCells();
        size_t size = cells.size();
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
    TVector<std::pair<TString, NScheme::TTypeId>> rightColumns;
    bool rightTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.To.GetCells();
        size_t size = cells.size();
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

bool TReadDescription::AddProgram(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program)
{
    using TId = NKikimrSSA::TProgram::TCommand;

    TContext info(columnResolver);
    auto step = std::make_shared<NArrow::TProgramStep>();
    for (auto& cmd : program.GetCommand()) {
        switch (cmd.GetLineCase()) {
            case TId::kAssign:
                ExtractAssign(info, *step, cmd.GetAssign(), ProgramParameters);
                break;
            case TId::kFilter:
                ExtractFilter(info, *step, cmd.GetFilter());
                break;
            case TId::kProjection:
                ExtractProjection(info, *step, cmd.GetProjection());
                Program.push_back(step);
                step = std::make_shared<NArrow::TProgramStep>();
                break;
            case TId::kGroupBy:
                // TODO
                return false; // not implemented
            case TId::LINE_NOT_SET:
                Y_VERIFY(false);
                break;
        }
    }

    // final step without final projection
    if (!step->Empty()) {
        Program.push_back(step);
    }

    ProgramSourceColumns = std::move(info.Sources);
    return true;
}

}
