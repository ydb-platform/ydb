#include "kqp_olap_compiler.h"

#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/formats/ssa_runtime_version.h>

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimrSSA;

using EAggFunctionType = TProgram::TAggregateAssignment::EAggregateFunction;

namespace {

struct TAggColInfo {
    ui64 AggColId = 0;
    ui64 BaseColId = 0;
    std::string AggColName;
    std::string BaseColName;
    std::string Operation;
};

class TKqpOlapCompileContext {
public:
    TKqpOlapCompileContext(const TCoArgument& row, const TKikimrTableMetadata& tableMeta,
        NKqpProto::TKqpPhyOpReadOlapRanges& readProto, const std::vector<std::string>& resultColNames)
        : Row(row)
        , MaxColumnId(0)
        , ReadProto(readProto)
        , ResultColNames(resultColNames)
    {
        for (const auto& [_, columnMeta] : tableMeta.Columns) {
            YQL_ENSURE(ReadColumns.emplace(columnMeta.Name, columnMeta.Id).second);
            MaxColumnId = std::max(MaxColumnId, columnMeta.Id);
        }

        Program.SetVersion(NKikimr::NSsa::RuntimeVersion);
    }

    ui32 GetColumnId(const std::string& name) const {
        auto columnIt = ReadColumns.find(name);
        if (columnIt == ReadColumns.end()) {
            auto resColNameIt = std::find(ResultColNames.begin(), ResultColNames.end(), name);
            YQL_ENSURE(resColNameIt != ResultColNames.end());

            columnIt = KqpAggColNameToId.find(*resColNameIt);
            YQL_ENSURE(columnIt != KqpAggColNameToId.end());
        }
        return columnIt->second;
    }

    ui32 NewColumnId() {
        return ++MaxColumnId;
    }

    const TExprNode* GetRowExpr() const {
        return Row.Raw();
    }

    TProgram::TAssignment* CreateAssignCmd() {
        auto* cmd = Program.AddCommand();
        auto* assign = cmd->MutableAssign();
        assign->MutableColumn()->SetId(NewColumnId());

        return assign;
    }

    TProgram::TFilter* CreateFilter() {
        return Program.AddCommand()->MutableFilter();
    }

    TProgram::TGroupBy* CreateGroupBy() {
        return Program.AddCommand()->MutableGroupBy();
    }

    TProgram::TProjection* CreateProjection() {
        return Program.AddCommand()->MutableProjection();
    }

    void AddParameterName(const TString& name) {
        ReadProto.AddOlapProgramParameterNames(name);
    }

    void SerializeToProto() {
        TString programBytes;
        TStringOutput stream(programBytes);
        Program.SerializeToArcadiaStream(&stream);
        ReadProto.SetOlapProgram(programBytes);
    }

    EAggFunctionType GetAggFuncType(const std::string& funcName) const {
        YQL_ENSURE(AggFuncTypesMap.find(funcName) != AggFuncTypesMap.end());
        return AggFuncTypesMap.at(funcName);
    }

    void MapKqpAggColNameToId(const std::string& colName, ui32 id) {
        KqpAggColNameToId.emplace(colName, id);
    }

    std::vector<std::string> GetResultColNames() {
        return ResultColNames;
    }

    bool IsEmptyProgram() {
        return Program.GetCommand().empty();
    }

private:
    static std::unordered_map<std::string, EAggFunctionType> AggFuncTypesMap;

    TCoArgument Row;
    std::unordered_map<std::string, ui32> ReadColumns;
    ui32 MaxColumnId;
    TProgram Program;
    NKqpProto::TKqpPhyOpReadOlapRanges& ReadProto;
    const std::vector<std::string>& ResultColNames;
    std::unordered_map<std::string, ui32> KqpAggColNameToId;
};

std::unordered_map<std::string, EAggFunctionType> TKqpOlapCompileContext::AggFuncTypesMap = {
    { "count", TProgram::TAggregateAssignment::AGG_COUNT },
    { "sum", TProgram::TAggregateAssignment::AGG_SUM },
    { "some", TProgram::TAggregateAssignment::AGG_SOME },
    { "min", TProgram::TAggregateAssignment::AGG_MIN },
    { "max", TProgram::TAggregateAssignment::AGG_MAX },
};

TProgram::TAssignment* CompileCondition(const TExprBase& condition, TKqpOlapCompileContext& ctx);
ui64 GetOrCreateColumnId(const TExprBase& node, TKqpOlapCompileContext& ctx);

ui32 ConvertValueToColumn(const TCoDataCtor& value, TKqpOlapCompileContext& ctx)
{
    TProgram::TAssignment* ssaValue = ctx.CreateAssignCmd();

    if (value.Maybe<TCoUtf8>()) {
        auto nodeValue = value.Cast<TCoUtf8>().Literal().Value();
        ssaValue->MutableConstant()->SetText(TString(nodeValue));
    } else if (value.Maybe<TCoString>()) {
        auto nodeValue = value.Cast<TCoString>().Literal().Value();
        ssaValue->MutableConstant()->SetText(TString(nodeValue));
    } else if (value.Maybe<TCoBool>()) {
        auto nodeValue = value.Cast<TCoBool>().Literal().Value();
        ssaValue->MutableConstant()->SetBool(FromString<bool>(nodeValue));
    } else if (value.Maybe<TCoFloat>()) {
        auto nodeValue = value.Cast<TCoFloat>().Literal().Value();
        ssaValue->MutableConstant()->SetFloat(FromString<float>(nodeValue));
    } else if (value.Maybe<TCoDouble>()) {
        auto nodeValue = value.Cast<TCoDouble>().Literal().Value();
        ssaValue->MutableConstant()->SetDouble(FromString<double>(nodeValue));
    } else if (value.Maybe<TCoInt8>()) {
        auto nodeValue = value.Cast<TCoInt8>().Literal().Value();
        ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue));
    } else if (value.Maybe<TCoInt16>()) {
        auto nodeValue = value.Cast<TCoInt16>().Literal().Value();
        ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue));
    } else if (value.Maybe<TCoInt32>()) {
        auto nodeValue = value.Cast<TCoInt32>().Literal().Value();
        ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue));
    } else if (value.Maybe<TCoInt64>()) {
        auto nodeValue = value.Cast<TCoInt64>().Literal().Value();
        ssaValue->MutableConstant()->SetInt64(FromString<i64>(nodeValue));
    } else if (value.Maybe<TCoUint8>()) {
        auto nodeValue = value.Cast<TCoUint8>().Literal().Value();
        ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue));
    } else if (value.Maybe<TCoUint16>()) {
        auto nodeValue = value.Cast<TCoUint16>().Literal().Value();
        ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue));
    } else if (value.Maybe<TCoUint32>()) {
        auto nodeValue = value.Cast<TCoUint32>().Literal().Value();
        ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue));
    } else if (value.Maybe<TCoUint64>()) {
        auto nodeValue = value.Cast<TCoUint64>().Literal().Value();
        ssaValue->MutableConstant()->SetUint64(FromString<ui64>(nodeValue));
    } else {
        YQL_ENSURE(false, "Unsupported content: " << value.Ptr()->Content());
    }

    return ssaValue->GetColumn().GetId();
}

ui32 ConvertParameterToColumn(const TCoParameter& parameter, TKqpOlapCompileContext& ctx)
{
    TProgram::TAssignment* ssaValue = ctx.CreateAssignCmd();

    auto name = TString(parameter.Name().Value());
    auto maybeType = parameter.Type().Maybe<TCoDataType>();

    YQL_ENSURE(maybeType.IsValid(), "Unknown type content in conversion: " << parameter.Type().Ptr()->Content());

    auto newParameter = ssaValue->MutableParameter();
    newParameter->SetName(name);

    ctx.AddParameterName(name);

    return ssaValue->GetColumn().GetId();
}

ui64 ConvertSafeCastToColumn(const TExprBase& colName, const std::string& targetType, TKqpOlapCompileContext& ctx)
{
    auto columnId = GetOrCreateColumnId(colName, ctx);
    TProgram::TAssignment* assignCmd = ctx.CreateAssignCmd();
    ui32 castFunction = TProgram::TAssignment::FUNC_UNSPECIFIED;

    if (targetType == "Boolean") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_BOOLEAN;
    } else if (targetType == "Int8") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_INT8;
    } else if (targetType == "Int16") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_INT16;
    } else if (targetType == "Int32") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_INT32;
    } else if (targetType == "Int64") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_INT64;
    } else if (targetType == "Uint8") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_UINT8;
    } else if (targetType == "Uint16") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_UINT16;
    } else if (targetType == "Uint32") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_UINT32;
    } else if (targetType == "Uint64") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_UINT64;
    } else if (targetType == "Float") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_FLOAT;
    } else if (targetType == "Double") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_DOUBLE;
    } else if (targetType == "Timestamp") {
        castFunction = TProgram::TAssignment::FUNC_CAST_TO_TIMESTAMP;
    } else {
        YQL_ENSURE(false, "Unsupported data type for pushed down safe cast: " << targetType);
    }

    auto newCast = assignCmd->MutableFunction();
    newCast->SetId(castFunction);
    newCast->AddArguments()->SetId(columnId);
    return assignCmd->GetColumn().GetId();
}

ui64 ConvertSafeCastToColumn(const TCoSafeCast& cast, TKqpOlapCompileContext& ctx)
{
    auto maybeDataType = cast.Type().Maybe<TCoDataType>();
    YQL_ENSURE(maybeDataType.IsValid());
    return ConvertSafeCastToColumn(cast.Value(), maybeDataType.Cast().Type().StringValue(), ctx);
}

ui64 GetOrCreateColumnId(const TExprBase& node, TKqpOlapCompileContext& ctx) {
    if (auto maybeData = node.Maybe<TCoDataCtor>()) {
        return ConvertValueToColumn(maybeData.Cast(), ctx);
    }

    if (auto maybeAtom = node.Maybe<TCoAtom>()) {
        return ctx.GetColumnId(maybeAtom.Cast().StringValue());
    }

    if (auto maybeParameter = node.Maybe<TCoParameter>()) {
        return ConvertParameterToColumn(maybeParameter.Cast(), ctx);
    }

    if (auto maybeCast = node.Maybe<TCoSafeCast>()) {
        return ConvertSafeCastToColumn(maybeCast.Cast(), ctx);
    }

    YQL_ENSURE(false, "Unknown node in OLAP comparison compiler: " << node.Ptr()->Content());
}

TProgram::TAssignment* CompileComparison(const TKqpOlapFilterCompare& comparison,
    TKqpOlapCompileContext& ctx)
{
    // Columns should be created before comparison, otherwise comparison fail to find columns
    ui32 leftColumnId = GetOrCreateColumnId(comparison.Left(), ctx);
    ui32 rightColumnId = GetOrCreateColumnId(comparison.Right(), ctx);

    TProgram::TAssignment* command = ctx.CreateAssignCmd();
    auto* cmpFunc = command->MutableFunction();

    ui32 function = TProgram::TAssignment::FUNC_UNSPECIFIED;

    if (comparison.Operator() == "eq") {
        function = TProgram::TAssignment::FUNC_CMP_EQUAL;
    } else if (comparison.Operator() == "neq") {
        function = TProgram::TAssignment::FUNC_CMP_NOT_EQUAL;
    } else if (comparison.Operator() == "lt") {
        function = TProgram::TAssignment::FUNC_CMP_LESS;
    } else if (comparison.Operator() == "lte") {
        function = TProgram::TAssignment::FUNC_CMP_LESS_EQUAL;
    } else if (comparison.Operator() == "gt") {
        function = TProgram::TAssignment::FUNC_CMP_GREATER;
    } else if (comparison.Operator() == "gte") {
        function = TProgram::TAssignment::FUNC_CMP_GREATER_EQUAL;
    } else if (comparison.Operator() == "string_contains") {
        function = TProgram::TAssignment::FUNC_STR_MATCH;
    } else if (comparison.Operator() == "starts_with") {
        function = TProgram::TAssignment::FUNC_STR_STARTS_WITH;
    } else if (comparison.Operator() == "ends_with") {
        function = TProgram::TAssignment::FUNC_STR_ENDS_WITH;
    }

    cmpFunc->SetId(function);
    cmpFunc->AddArguments()->SetId(leftColumnId);
    cmpFunc->AddArguments()->SetId(rightColumnId);

    return command;
}

TProgram::TAssignment* CompileExists(const TKqpOlapFilterExists& exists,
    TKqpOlapCompileContext& ctx)
{
    ui32 columnId = GetOrCreateColumnId(exists.Column(), ctx);

    TProgram::TAssignment* command = ctx.CreateAssignCmd();
    auto* isNullFunc = command->MutableFunction();

    isNullFunc->SetId(TProgram::TAssignment::FUNC_IS_NULL);
    isNullFunc->AddArguments()->SetId(columnId);

    TProgram::TAssignment *notCommand = ctx.CreateAssignCmd();
    auto *notFunc = notCommand->MutableFunction();

    notFunc->SetId(TProgram::TAssignment::FUNC_BINARY_NOT);
    notFunc->AddArguments()->SetId(command->GetColumn().GetId());

    return notCommand;
}

TProgram::TAssignment* BuildLogicalProgram(const TExprNode::TChildrenType& args, ui32 function,
    TKqpOlapCompileContext& ctx)
{
    ui32 childrenCount = args.size();

    if (childrenCount == 1) {
        // NOT operation is handled separately, thus only one available situation here:
        // this is binary operation with only one node, just build this node and return.
        return CompileCondition(TExprBase(args[0]), ctx);
    }

    TProgram::TAssignment* left = nullptr;
    TProgram::TAssignment* right = nullptr;

    if (childrenCount == 2) {
        // Nice, we can build logical operation with two child as expected
        left = CompileCondition(TExprBase(args[0]), ctx);
        right = CompileCondition(TExprBase(args[1]), ctx);
    } else {
        // >2 children - split incoming vector in the middle call this function recursively.
        auto leftArgs = args.Slice(0, childrenCount / 2);
        auto rightArgs = args.Slice(childrenCount / 2);

        left = BuildLogicalProgram(leftArgs, function, ctx);
        right = BuildLogicalProgram(rightArgs, function, ctx);
    }

    TProgram::TAssignment *logicalOp = ctx.CreateAssignCmd();
    auto *logicalFunc = logicalOp->MutableFunction();

    logicalFunc->SetId(function);
    logicalFunc->AddArguments()->SetId(left->GetColumn().GetId());
    logicalFunc->AddArguments()->SetId(right->GetColumn().GetId());

    return logicalOp;
}

TProgram::TAssignment* CompileCondition(const TExprBase& condition, TKqpOlapCompileContext& ctx) {
    auto maybeCompare = condition.Maybe<TKqpOlapFilterCompare>();

    if (maybeCompare.IsValid()) {
        return CompileComparison(maybeCompare.Cast(), ctx);
    }

    auto maybeExists = condition.Maybe<TKqpOlapFilterExists>();

    if (maybeExists.IsValid()) {
        return CompileExists(maybeExists.Cast(), ctx);
    }

    if (auto maybeNot = condition.Maybe<TKqpOlapNot>()) {
        // Not is a special way in case it has only one child
        TProgram::TAssignment *value = CompileCondition(maybeNot.Cast().Value(), ctx);

        TProgram::TAssignment *notOp = ctx.CreateAssignCmd();
        auto *notFunc = notOp->MutableFunction();

        notFunc->SetId(TProgram::TAssignment::FUNC_BINARY_NOT);
        notFunc->AddArguments()->SetId(value->GetColumn().GetId());

        return notOp;
    }

    ui32 function = TProgram::TAssignment::FUNC_UNSPECIFIED;

    if (condition.Maybe<TKqpOlapAnd>()) {
        function = TProgram::TAssignment::FUNC_BINARY_AND;
    } else if (condition.Maybe<TKqpOlapOr>()) {
        function = TProgram::TAssignment::FUNC_BINARY_OR;
    } else if (condition.Maybe<TKqpOlapXor>()) {
        function = TProgram::TAssignment::FUNC_BINARY_XOR;
    } else {
        YQL_ENSURE(false, "Unsuppoted logical operation: " << condition.Ptr()->Content());
    }

    return BuildLogicalProgram(condition.Ptr()->Children(), function, ctx);
}

void CompileFilter(const TKqpOlapFilter& filterNode, TKqpOlapCompileContext& ctx) {
    TProgram::TAssignment* condition = CompileCondition(filterNode.Condition(), ctx);

    auto* filter = ctx.CreateFilter();
    filter->MutablePredicate()->SetId(condition->GetColumn().GetId());
}

std::vector<TAggColInfo> CollectAggregationInfos(const TKqpOlapAgg& aggNode, TKqpOlapCompileContext& ctx) {
    std::vector<TAggColInfo> aggColInfos;
    aggColInfos.reserve(aggNode.Aggregates().Size());
    for (auto aggIt : aggNode.Aggregates()) {
        // We need to collect all this info because probably we need add CAST functions before Aggregations
        auto aggKqp = aggIt.Cast<TKqpOlapAggOperation>();
        TAggColInfo colInfo;
        colInfo.AggColName = aggKqp.Name().StringValue().c_str();
        colInfo.AggColId = ctx.NewColumnId();
        colInfo.BaseColName = aggKqp.Column().StringValue().c_str();
        colInfo.Operation = aggKqp.Type().StringValue();
        ctx.MapKqpAggColNameToId(colInfo.AggColName, colInfo.AggColId);

        auto opType = aggKqp.Type().StringValue();
        if (opType != "count" || (opType == "count" && colInfo.BaseColName != "*")) {
            colInfo.BaseColId = GetOrCreateColumnId(aggKqp.Column(), ctx);
        }
        aggColInfos.push_back(colInfo);
    }
    return aggColInfos;
}

void CompileAggregates(const TKqpOlapAgg& aggNode, TKqpOlapCompileContext& ctx) {
    std::vector<TAggColInfo> aggColInfos = CollectAggregationInfos(aggNode, ctx);
    auto* groupBy = ctx.CreateGroupBy();

    for (auto aggColInfo : aggColInfos) {
        auto* agg = groupBy->AddAggregates();
        auto* aggCol = agg->MutableColumn();
        aggCol->SetId(aggColInfo.AggColId);

        auto* aggFunc = agg->MutableFunction();
        aggFunc->SetId(ctx.GetAggFuncType(aggColInfo.Operation));

        if (aggColInfo.BaseColId != 0) {
            aggFunc->AddArguments()->SetId(aggColInfo.BaseColId);
        }
    }

    for (auto keyCol : aggNode.KeyColumns()) {
        auto aggKeyCol = groupBy->AddKeyColumns();
        auto keyColName = keyCol.StringValue();
        auto aggKeyColId = GetOrCreateColumnId(keyCol, ctx);
        aggKeyCol->SetId(aggKeyColId);
    }
}

void CompileFinalProjection(TKqpOlapCompileContext& ctx) {
    auto resultColNames = ctx.GetResultColNames();
    if (resultColNames.empty()) {
        return;
    }

    auto* projection = ctx.CreateProjection();
    for (auto colName : resultColNames) {
        auto colId = ctx.GetColumnId(colName);

        auto* projCol = projection->AddColumns();
        projCol->SetId(colId);
    }
}

void CompileOlapProgramImpl(TExprBase operation, TKqpOlapCompileContext& ctx) {
    if (operation.Raw() == ctx.GetRowExpr()) {
        return;
    }

    if (auto maybeOlapOperation = operation.Maybe<TKqpOlapOperationBase>()) {
        CompileOlapProgramImpl(maybeOlapOperation.Cast().Input(), ctx);
        if (auto maybeFilter = operation.Maybe<TKqpOlapFilter>()) {
            // On the first level of filters we apply fast and light filters: <, >, !=, == etc.
            // On the second level we apply high-cost filters (LIKE operation) on top of filtered data from the 1st level.
            if (maybeFilter.Cast().Input().Maybe<TKqpOlapFilter>()) {
                // The 2nd level of filters use the result of 1st level as input.
                // We create an empty projection to run first level apply.
                // Because real execution of filters is done on Projection and GroupBy steps.
                ctx.CreateProjection();
            }
            CompileFilter(maybeFilter.Cast(), ctx);
        } else if (auto maybeAgg = operation.Maybe<TKqpOlapAgg>()) {
            CompileAggregates(maybeAgg.Cast(), ctx);
        }
        return;
    }

    YQL_ENSURE(operation.Maybe<TCallable>(), "Unexpected OLAP operation node type: " << operation.Ref().Type());
    YQL_ENSURE(false, "Unexpected OLAP operation: " << operation.Cast<TCallable>().CallableName());
}

} // namespace

void CompileOlapProgram(const TCoLambda& lambda, const TKikimrTableMetadata& tableMeta,
    NKqpProto::TKqpPhyOpReadOlapRanges& readProto, const std::vector<std::string>& resultColNames)
{
    YQL_ENSURE(lambda.Args().Size() == 1);

    TKqpOlapCompileContext ctx(lambda.Args().Arg(0), tableMeta, readProto, resultColNames);

    CompileOlapProgramImpl(lambda.Body(), ctx);
    CompileFinalProjection(ctx);

    ctx.SerializeToProto();
}

} // namespace NKqp
} // namespace NKikimr
