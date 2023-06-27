#include "program.h"

#include <ydb/core/formats/arrow/ssa_program_optimizer.h>
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_scalar.h>
#include <google/protobuf/text_format.h>

namespace NKikimr::NOlap {

namespace {

using EOperation = NArrow::EOperation;
using EAggregate = NArrow::EAggregate;
using TAssign = NSsa::TAssign;
using TAggregateAssign = NSsa::TAggregateAssign;

class TProgramBuilder {
    const IColumnResolver& ColumnResolver;
    const TKernelsRegistry& KernelsRegistry;
    mutable THashMap<TString, std::shared_ptr<arrow::Scalar>> Constants;
    TString Error;
public:
    mutable THashMap<ui32, TString> Sources;
    
    explicit TProgramBuilder(const IColumnResolver& columnResolver, const TKernelsRegistry& kernelsRegistry)
        : ColumnResolver(columnResolver)
        , KernelsRegistry(kernelsRegistry)
    {}

    const TString& GetErrorMessage() const {
        return Error;
    }
private:
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
    TAssign MakeFunction(const std::string& name,
                                const NKikimrSSA::TProgram::TAssignment::TFunction& func);
    NSsa::TAssign MakeConstant(const std::string& name, const NKikimrSSA::TProgram::TConstant& constant); 
    NSsa::TAggregateAssign MakeAggregate(const std::string& name, const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& func);
    NSsa::TAssign MaterializeParameter(const std::string& name, const NKikimrSSA::TProgram::TParameter& parameter, const std::shared_ptr<arrow::RecordBatch>& parameterValues);

public:
    bool ExtractAssign(NSsa::TProgramStep& step, const NKikimrSSA::TProgram::TAssignment& assign,
        const std::shared_ptr<arrow::RecordBatch>& parameterValues);
    bool ExtractFilter(NSsa::TProgramStep& step, const NKikimrSSA::TProgram::TFilter& filter);
    bool ExtractProjection(NSsa::TProgramStep& step,
                        const NKikimrSSA::TProgram::TProjection& projection);
    bool ExtractGroupBy(NSsa::TProgramStep& step, const NKikimrSSA::TProgram::TGroupBy& groupBy);
};

TAssign TProgramBuilder::MakeFunction(const std::string& name,
                                const NKikimrSSA::TProgram::TAssignment::TFunction& func) {
    using TId = NKikimrSSA::TProgram::TAssignment;

    std::vector<std::string> arguments;
    for (auto& col : func.GetArguments()) {
        arguments.push_back(GetName(col));
    }

    auto mkCastOptions = [](std::shared_ptr<arrow::DataType> dataType) {
        // TODO: support CAST with OrDefault/OrNull logic (second argument is default value)
        auto castOpts = std::make_shared<arrow::compute::CastOptions>(false);
        castOpts->to_type = dataType;
        return castOpts;
    };

    auto mkLikeOptions = [&](bool ignoreCase) {
        if (arguments.size() != 2 || !Constants.contains(arguments[1])) {
            return std::shared_ptr<arrow::compute::MatchSubstringOptions>();
        }
        auto patternScalar = Constants[arguments[1]];
        if (!arrow::is_base_binary_like(patternScalar->type->id())) {
            return std::shared_ptr<arrow::compute::MatchSubstringOptions>();
        }
        arguments.resize(1);
        auto& pattern = static_cast<arrow::BaseBinaryScalar&>(*patternScalar).value;
        return std::make_shared<arrow::compute::MatchSubstringOptions>(pattern->ToString(), ignoreCase);
    };

    if (func.GetFunctionType() == NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL) {
        auto kernelFunction = KernelsRegistry.GetFunction(func.GetKernelIdx());
        if (!kernelFunction) {
            Error = TStringBuilder() << "Unknown kernel for " << name << ";kernel_idx=" << func.GetKernelIdx();
            return TAssign(name, EOperation::Unspecified, std::move(arguments));
        }
        return TAssign(name, kernelFunction, std::move(arguments), nullptr);
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
        case TId::FUNC_STR_MATCH: {
            if (auto opts = mkLikeOptions(false)) {
                return TAssign(name, EOperation::MatchSubstring, std::move(arguments), opts);
            }
            break;
        }
        case TId::FUNC_STR_MATCH_LIKE: {
            if (auto opts = mkLikeOptions(false)) {
                return TAssign(name, EOperation::MatchLike, std::move(arguments), opts);
            }
            break;
        }
        case TId::FUNC_STR_STARTS_WITH: {
            if (auto opts = mkLikeOptions(false)) {
                return TAssign(name, EOperation::StartsWith, std::move(arguments), opts);
            }
            break;
        }
        case TId::FUNC_STR_ENDS_WITH: {
            if (auto opts = mkLikeOptions(false)) {
                return TAssign(name, EOperation::EndsWith, std::move(arguments), opts);
            }
            break;
        }
        case TId::FUNC_STR_MATCH_IGNORE_CASE: {
            if (auto opts = mkLikeOptions(true)) {
                return TAssign(name, EOperation::MatchSubstring, std::move(arguments), opts);
            }
            break;
        }
        case TId::FUNC_STR_STARTS_WITH_IGNORE_CASE: {
            if (auto opts = mkLikeOptions(true)) {
                return TAssign(name, EOperation::StartsWith, std::move(arguments), opts);
            }
            break;
        }
        case TId::FUNC_STR_ENDS_WITH_IGNORE_CASE: {
            if (auto opts = mkLikeOptions(true)) {
                return TAssign(name, EOperation::EndsWith, std::move(arguments), opts);
            }
            break;
        }
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
        case TId::FUNC_CAST_TO_INT8:
            return TAssign(name, EOperation::CastInt8, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::Int8Type>()));
        case TId::FUNC_CAST_TO_INT16:
            return TAssign(name, EOperation::CastInt16, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::Int16Type>()));
        case TId::FUNC_CAST_TO_INT32:
            return TAssign(name, EOperation::CastInt32, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::Int32Type>()));
        case TId::FUNC_CAST_TO_INT64:
            return TAssign(name, EOperation::CastInt64, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::Int64Type>()));
        case TId::FUNC_CAST_TO_UINT8:
            return TAssign(name, EOperation::CastUInt8, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::UInt8Type>()));
        case TId::FUNC_CAST_TO_UINT16:
            return TAssign(name, EOperation::CastUInt16, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::UInt16Type>()));
        case TId::FUNC_CAST_TO_UINT32:
            return TAssign(name, EOperation::CastUInt32, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::UInt32Type>()));
        case TId::FUNC_CAST_TO_UINT64:
            return TAssign(name, EOperation::CastUInt64, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::UInt64Type>()));
        case TId::FUNC_CAST_TO_FLOAT:
            return TAssign(name, EOperation::CastFloat, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::FloatType>()));
        case TId::FUNC_CAST_TO_DOUBLE:
            return TAssign(name, EOperation::CastDouble, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::DoubleType>()));
        case TId::FUNC_CAST_TO_TIMESTAMP:
            return TAssign(name, EOperation::CastTimestamp, std::move(arguments),
                        mkCastOptions(std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO)));
        case TId::FUNC_CAST_TO_BINARY:
        case TId::FUNC_CAST_TO_FIXED_SIZE_BINARY:
        case TId::FUNC_UNSPECIFIED:
            break;
    }
    return TAssign(name, EOperation::Unspecified, std::move(arguments));
}

NSsa::TAssign TProgramBuilder::MakeConstant(const std::string& name, const NKikimrSSA::TProgram::TConstant& constant) {
    using TId = NKikimrSSA::TProgram::TConstant;

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
            return TAssign(name, std::string(str.data(), str.size()), true);
        }
        case TId::kText:
        {
            TString str = constant.GetText();
            return TAssign(name, std::string(str.data(), str.size()), false);
        }
        case TId::VALUE_NOT_SET:
            break;
    }
    return TAssign(name, EOperation::Unspecified, {});
}

NSsa::TAggregateAssign TProgramBuilder::MakeAggregate(const std::string& name, const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& func) {
    using TId = NKikimrSSA::TProgram::TAggregateAssignment;

    if (func.GetFunctionType() == NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL) {
        std::string argument = GetName(func.GetArguments()[0]);
        auto kernelFunction = KernelsRegistry.GetFunction(func.GetKernelIdx());
        if (!kernelFunction) {
            Error = TStringBuilder() << "Unknown kernel for " << func.GetId() << ";kernel_idx=" << func.GetKernelIdx();
            return TAggregateAssign(name);
        }
        return TAggregateAssign(name, kernelFunction, { argument });
    }

    if (func.ArgumentsSize() == 1) {
        std::string argument = GetName(func.GetArguments()[0]);

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
        return TAggregateAssign(name, EAggregate::Count);
    }
    return TAggregateAssign(name); // !ok()
}

NSsa::TAssign TProgramBuilder::MaterializeParameter(const std::string& name, const NKikimrSSA::TProgram::TParameter& parameter, const std::shared_ptr<arrow::RecordBatch>& parameterValues) {
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

bool TProgramBuilder::ExtractAssign(NSsa::TProgramStep& step, const NKikimrSSA::TProgram::TAssignment& assign,
    const std::shared_ptr<arrow::RecordBatch>& parameterValues) {

    using TId = NKikimrSSA::TProgram::TAssignment;

    std::string columnName = GetName(assign.GetColumn());

    switch (assign.GetExpressionCase()) {
        case TId::kFunction:
        {
            auto func = MakeFunction(columnName, assign.GetFunction());
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
            Constants[columnName] = cnst.GetConstant();
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

bool TProgramBuilder::ExtractFilter(NSsa::TProgramStep& step, const NKikimrSSA::TProgram::TFilter& filter) {
    auto& column = filter.GetPredicate();
    if (!column.HasId() && !column.HasName()) {
        return false;
    }
    // NOTE: Name maskes Id for column. If column assigned with name it's accessible only by name.
    step.Filters.push_back(GetName(column));
    return true;
}

bool TProgramBuilder::ExtractProjection(NSsa::TProgramStep& step,
                    const NKikimrSSA::TProgram::TProjection& projection) {
    step.Projection.reserve(projection.ColumnsSize());
    for (auto& col : projection.GetColumns()) {
        // NOTE: Name maskes Id for column. If column assigned with name it's accessible only by name.
        step.Projection.push_back(GetName(col));
    }
    return true;
}

bool TProgramBuilder::ExtractGroupBy(NSsa::TProgramStep& step, const NKikimrSSA::TProgram::TGroupBy& groupBy) {
    if (!groupBy.AggregatesSize()) {
        return false;
    }

    step.GroupBy.reserve(groupBy.AggregatesSize());
    step.GroupByKeys.reserve(groupBy.KeyColumnsSize());
    for (auto& agg : groupBy.GetAggregates()) {
        auto& resColumn = agg.GetColumn();
        TString columnName = GenerateName(resColumn);

        auto func = MakeAggregate(columnName, agg.GetFunction());
        if (!func.IsOk()) {
            return false;
        }
        step.GroupBy.push_back(std::move(func));
    }
    for (auto& key : groupBy.GetKeyColumns()) {
        step.GroupByKeys.push_back(GetName(key));
    }

    return true;
}
}

const THashMap<ui32, TString>& TProgramContainer::GetSourceColumns() const {
    if (!Program) {
        return Default<THashMap<ui32, TString>>();
    }
    return Program->SourceColumns;
}

bool TProgramContainer::HasProgram() const {
    return !!Program;
}

std::shared_ptr<NArrow::TColumnFilter> TProgramContainer::BuildEarlyFilter(std::shared_ptr<arrow::RecordBatch> batch) const {
    if (Program) {
        return std::make_shared<NArrow::TColumnFilter>(NOlap::EarlyFilter(batch, Program));
    }
    return nullptr;
}

std::set<std::string> TProgramContainer::GetEarlyFilterColumns() const {
    if (Program) {
        return Program->GetEarlyFilterColumns();
    }
    return Default<std::set<std::string>>();
}

bool TProgramContainer::HasEarlyFilterOnly() const {
    if (!Program) {
        return true;
    }
    for (ui32 i = 1; i < Program->Steps.size(); ++i) {
        if (Program->Steps[i]->Filters.size()) {
            return false;
        }
    }
    return true;
}

bool TProgramContainer::Init(const IColumnResolver& columnResolver, NKikimrSchemeOp::EOlapProgramType programType, TString serializedProgram, TString& error) {
    Y_VERIFY(serializedProgram);

    NKikimrSSA::TProgram programProto;
    NKikimrSSA::TOlapProgram olapProgramProto;

    switch (programType) {
        case NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS:
            if (!olapProgramProto.ParseFromString(serializedProgram)) {
                error = TStringBuilder() << "Can't parse TOlapProgram";
                return false;
            }

            if (!programProto.ParseFromString(olapProgramProto.GetProgram())) {
                error = TStringBuilder() << "Can't parse TProgram";
                return false;
            }

            break;
        default:
            error = TStringBuilder() << "Unsupported olap program version: " << (ui32)programType;
            return false;
    }

    if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        TString out;
        ::google::protobuf::TextFormat::PrintToString(programProto, &out);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("program", out);
    }
    
    if (olapProgramProto.HasParameters()) {
        Y_VERIFY(olapProgramProto.HasParametersSchema(), "Parameters are present, but there is no schema.");

        auto schema = NArrow::DeserializeSchema(olapProgramProto.GetParametersSchema());
        ProgramParameters = NArrow::DeserializeBatch(olapProgramProto.GetParameters(), schema);
    }

    if (programProto.HasKernels()) {
        KernelsRegistry.Parse(programProto.GetKernels());
    }

    NOlap::TProgramContainer ssaProgram;
    if (!ParseProgram(columnResolver, programProto, error)) {
        if (!error) {
            error = TStringBuilder() << "Wrong olap program";
        }
        return false;
    }
    return true;
}

bool TProgramContainer::ParseProgram(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program, TString& error) {
    using TId = NKikimrSSA::TProgram::TCommand;

    auto ssaProgram = std::make_shared<NSsa::TProgram>();
    TProgramBuilder programBuilder(columnResolver, KernelsRegistry);
    auto step = std::make_shared<NSsa::TProgramStep>();
    for (auto& cmd : program.GetCommand()) {
        switch (cmd.GetLineCase()) {
            case TId::kAssign:
                if (!programBuilder.ExtractAssign(*step, cmd.GetAssign(), ProgramParameters)) {
                    error = programBuilder.GetErrorMessage();
                    return false;
                }
                break;
            case TId::kFilter:
                if (!programBuilder.ExtractFilter(*step, cmd.GetFilter())) {
                    error = programBuilder.GetErrorMessage();
                    return false;
                }
                break;
            case TId::kProjection:
                if (!programBuilder.ExtractProjection(*step, cmd.GetProjection())) {
                    error = programBuilder.GetErrorMessage();
                    return false;
                }
                ssaProgram->Steps.push_back(step);
                step = std::make_shared<NSsa::TProgramStep>();
                break;
            case TId::kGroupBy:
                if (!programBuilder.ExtractGroupBy(*step, cmd.GetGroupBy())) {
                    error = programBuilder.GetErrorMessage();
                    return false;
                }
                ssaProgram->Steps.push_back(step);
                step = std::make_shared<NSsa::TProgramStep>();
                break;
            case TId::LINE_NOT_SET:
                return false;
        }
    }

    // final step without final projection
    if (!step->Empty()) {
        ssaProgram->Steps.push_back(step);
    }

    ssaProgram->SourceColumns = std::move(programBuilder.Sources);

    // Query 'SELECT count(*) FROM table' needs a column
    if (ssaProgram->SourceColumns.empty()) {
        auto& ydbSchema = columnResolver.GetSchema();

        Y_VERIFY(!ydbSchema.KeyColumns.empty());
        ui32 key = ydbSchema.KeyColumns[0];

        auto it = ydbSchema.Columns.find(key);
        Y_VERIFY(it != ydbSchema.Columns.end());

        ssaProgram->SourceColumns[key] = it->second.Name;
    }

    if (!ssaProgram->Steps.empty()) {
        NSsa::OptimizeProgram(*ssaProgram);
    }
    Program = ssaProgram;
    return true;
 }

}
