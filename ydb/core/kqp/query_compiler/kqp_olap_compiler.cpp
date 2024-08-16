#include "kqp_olap_compiler.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/ssa_runtime_version.h>

#include <ydb/library/yql/core/arrow_kernels/request/request.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <memory>

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
        NKqpProto::TKqpPhyOpReadOlapRanges& readProto, const std::vector<std::string>& resultColNames,
        TExprContext &exprCtx)
        : Row(row)
        , MaxColumnId(0)
        , ReadProto(readProto)
        , ResultColNames(resultColNames)
        , ExprContext(exprCtx)
        , YqlKernelsFuncRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone())
        , YqlKernelRequestBuilder(nullptr)
    {
        NKikimr::NMiniKQL::FillStaticModules(*YqlKernelsFuncRegistry);
        YqlKernelRequestBuilder = std::make_unique<TKernelRequestBuilder>(*YqlKernelsFuncRegistry);

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

    const TTypeAnnotationNode* GetReturnType(const TTypeAnnotationNode& argument, const TTypeAnnotationNode* resultItemType) const {
        bool isScalar;
        const auto itemType = GetBlockItemType(argument, isScalar);

        if (ETypeAnnotationKind::Optional == itemType->GetKind()) {
            resultItemType = ExprContext.MakeType<TOptionalExprType>(resultItemType);
        }

        if (isScalar)
            return ExprContext.MakeType<TScalarExprType>(resultItemType);
        else
            return ExprContext.MakeType<TBlockExprType>(resultItemType);
    }

    const TTypeAnnotationNode* GetReturnType(TPositionHandle pos, const TTypeAnnotationNode& left, const TTypeAnnotationNode& right, const TTypeAnnotationNode* resultItemType) const {
        bool isScalarLeft, isScalarRight;
        const auto leftItemType = GetBlockItemType(left, isScalarLeft);
        const auto rightItemType = GetBlockItemType(right, isScalarRight);

        if (!resultItemType) {
            const auto& leftCleanType = RemoveOptionality(*leftItemType);
            const auto& rightCleanType = RemoveOptionality(*rightItemType);
            resultItemType = CommonType<true>(pos, &leftCleanType, &rightCleanType, ExprContext);
        }

        if (ETypeAnnotationKind::Optional == leftItemType->GetKind() || ETypeAnnotationKind::Optional == rightItemType->GetKind()) {
            resultItemType = ExprContext.MakeType<TOptionalExprType>(resultItemType);
        }

        if (isScalarLeft && isScalarRight)
            return ExprContext.MakeType<TScalarExprType>(resultItemType);
        else
            return ExprContext.MakeType<TBlockExprType>(resultItemType);
    }

    std::pair<ui32, const TTypeAnnotationNode*> AddYqlKernelBinaryFunc(TPositionHandle pos, TKernelRequestBuilder::EBinaryOp op, const TTypeAnnotationNode& argTypeOne, const TTypeAnnotationNode& argTypeTwo, const TTypeAnnotationNode* retType) const {
        const auto retBlockType = GetReturnType(pos, argTypeOne, argTypeTwo, retType);
        return std::make_pair(YqlKernelRequestBuilder->AddBinaryOp(op, &argTypeOne, &argTypeTwo, retBlockType), retBlockType);
    }

    ui32 AddYqlKernelBinaryFunc(TPositionHandle pos, TKernelRequestBuilder::EBinaryOp op, const TExprBase& arg1, const TExprBase& arg2, const TTypeAnnotationNode* retType) const {
        const auto arg1Type = GetArgType(arg1);
        const auto arg2Type = GetArgType(arg2);
        return AddYqlKernelBinaryFunc(pos, op, *arg1Type, *arg2Type, retType).first;
    }

    ui32 AddYqlKernelJsonExists(const TExprBase& arg1, const TExprBase& arg2, const TTypeAnnotationNode* retType) const {
        const auto arg1Type = GetArgType(arg1);
        const auto arg2Type = GetArgType(arg2);
        const auto retBlockType = ConvertToBlockType(retType);
        return YqlKernelRequestBuilder->JsonExists(arg1Type, arg2Type, retBlockType);
    }

    ui32 AddYqlKernelJsonValue(const TExprBase& arg1, const TExprBase& arg2, const TTypeAnnotationNode* retType) const {
        const auto arg1Type = GetArgType(arg1);
        const auto arg2Type = GetArgType(arg2);
        const auto retBlockType = ConvertToBlockType(retType);
        return YqlKernelRequestBuilder->JsonValue(arg1Type, arg2Type, retBlockType);
    }

    void AddParameterName(const TString& name) const {
        ReadProto.AddOlapProgramParameterNames(name);
    }

    void SerializeToProto() {
        TString programBytes;
        TStringOutput stream(programBytes);
        Program.SetKernels(YqlKernelRequestBuilder->Serialize());
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

    std::vector<std::string> GetResultColNames() const {
        return ResultColNames;
    }

    bool IsEmptyProgram() const {
        return Program.GetCommand().empty();
    }

    TExprContext& ExprCtx() {
        return ExprContext;
    }

    bool CheckYqlCompatibleArgType(const TExprBase& expression) const {
        if (const auto maybe = expression.Maybe<TCoAtom>()) {
            if (const auto type = GetColumnTypeByName(maybe.Cast().Value()); type->GetKind() == ETypeAnnotationKind::Data) {
                if (const auto info = GetDataTypeInfo(type->Cast<TDataExprType>()->GetSlot()); !(info.Features & (NUdf::EDataTypeFeatures::StringType | NUdf::EDataTypeFeatures::NumericType))) {
                    return false;
                }
            }
        }
        return true;
    }

    bool CheckYqlCompatibleArgsTypes(const TKqpOlapFilterBinaryOp& operation) const {
        return CheckYqlCompatibleArgType(operation.Left()) && CheckYqlCompatibleArgType(operation.Right());
    }

    TKernelRequestBuilder& GetKernelRequestBuilder() const {
        return *YqlKernelRequestBuilder;
    }

    const TTypeAnnotationNode* GetArgType(const TExprBase& arg) const {
        const auto argType = arg.Ptr()->GetTypeAnn();
        if (arg.Maybe<TCoAtom>() && argType->GetKind() == ETypeAnnotationKind::Unit) {
            // Column name
            return ConvertToBlockType(GetColumnTypeByName(arg.Cast<TCoAtom>().Value()));
        }
        return ExprContext.MakeType<TScalarExprType>(argType);
    }

    const TTypeAnnotationNode* ConvertToBlockType(const TTypeAnnotationNode* type) const {
        if (!type->IsBlock()) {
            return ExprContext.MakeType<TBlockExprType>(type);
        }
        return type;
    }
private:
    const TTypeAnnotationNode* GetColumnTypeByName(const std::string_view &name) const {
        auto *rowItemType = GetSeqItemType(Row.Ptr()->GetTypeAnn());
        YQL_ENSURE(rowItemType->GetKind() == ETypeAnnotationKind::Struct, "Input for OLAP lambda must contain Struct inside.");
        auto structType = rowItemType->Cast<TStructExprType>();
        return structType->FindItemType(name);
    }

    static std::unordered_map<std::string, EAggFunctionType> AggFuncTypesMap;

    TCoArgument Row;
    std::unordered_map<std::string, ui32> ReadColumns;
    ui32 MaxColumnId;
    TProgram Program;
    NKqpProto::TKqpPhyOpReadOlapRanges& ReadProto;
    const std::vector<std::string>& ResultColNames;
    std::unordered_map<std::string, ui32> KqpAggColNameToId;
    TExprContext& ExprContext;
    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> YqlKernelsFuncRegistry;
    std::unique_ptr<TKernelRequestBuilder> YqlKernelRequestBuilder;
};

std::unordered_map<std::string, EAggFunctionType> TKqpOlapCompileContext::AggFuncTypesMap = {
    { "count", TProgram::TAggregateAssignment::AGG_COUNT },
    { "sum", TProgram::TAggregateAssignment::AGG_SUM },
    { "some", TProgram::TAggregateAssignment::AGG_SOME },
    { "min", TProgram::TAggregateAssignment::AGG_MIN },
    { "max", TProgram::TAggregateAssignment::AGG_MAX },
};

std::unordered_set<std::string> SimpleArrowCmpFuncs = {
    "eq", "neq", "lt", "lte", "gt", "gte"
};

std::unordered_set<std::string> YqlKernelCmpFuncs = {
    "string_contains", "starts_with", "ends_with"
};

ui64 CompileCondition(const TExprBase& condition, TKqpOlapCompileContext& ctx);
ui64 GetOrCreateColumnId(const TExprBase& node, TKqpOlapCompileContext& ctx);

ui64 ConvertValueToColumn(const TCoDataCtor& value, TKqpOlapCompileContext& ctx)
{
    constexpr bool yqlTypes = NKikimr::NSsa::RuntimeVersion >= 4U;
    auto *const ssaValue = ctx.CreateAssignCmd();
    const auto& nodeValue = value.Cast<TCoDataCtor>().Literal().Value();
    if (value.Maybe<TCoUtf8>()) {
        ssaValue->MutableConstant()->SetText(TString(nodeValue));
    } else if (value.Maybe<TCoString>()) {
        ssaValue->MutableConstant()->SetBytes(TString(nodeValue));
    } else if (value.Maybe<TCoBool>()) {
        if constexpr (yqlTypes)
            ssaValue->MutableConstant()->SetUint8(FromString<bool>(nodeValue) ? 1U : 0U);
        else
            ssaValue->MutableConstant()->SetBool(FromString<bool>(nodeValue));
    } else if (value.Maybe<TCoFloat>()) {
        ssaValue->MutableConstant()->SetFloat(FromString<float>(nodeValue));
    } else if (value.Maybe<TCoDouble>()) {
        ssaValue->MutableConstant()->SetDouble(FromString<double>(nodeValue));
    } else if (value.Maybe<TCoInt8>()) {
        if constexpr (yqlTypes)
            ssaValue->MutableConstant()->SetInt8(FromString<i8>(nodeValue));
        else
            ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue));
    } else if (value.Maybe<TCoInt16>()) {
        if constexpr (yqlTypes)
            ssaValue->MutableConstant()->SetInt16(FromString<i16>(nodeValue));
        else
            ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue));
    } else if (value.Maybe<TCoInt32>()) {
        ssaValue->MutableConstant()->SetInt32(FromString<i32>(nodeValue));
    } else if (value.Maybe<TCoInt64>()) {
        ssaValue->MutableConstant()->SetInt64(FromString<i64>(nodeValue));
    } else if (value.Maybe<TCoUint8>()) {
        if constexpr (yqlTypes)
            ssaValue->MutableConstant()->SetUint8(FromString<ui8>(nodeValue));
        else
            ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue));
    } else if (value.Maybe<TCoUint16>()) {
        if constexpr (yqlTypes)
            ssaValue->MutableConstant()->SetUint16(FromString<ui16>(nodeValue));
        else
            ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue));
    } else if (value.Maybe<TCoUint32>()) {
        ssaValue->MutableConstant()->SetUint32(FromString<ui32>(nodeValue));
    } else if (value.Maybe<TCoUint64>()) {
        ssaValue->MutableConstant()->SetUint64(FromString<ui64>(nodeValue));
    } else {
        YQL_ENSURE(false, "Unsupported content: " << value.Ref().Content());
    }

    return ssaValue->GetColumn().GetId();
}

ui64 ConvertParameterToColumn(const TCoParameter& parameter, TKqpOlapCompileContext& ctx)
{
    auto *const ssaValue = ctx.CreateAssignCmd();

    const auto& name = parameter.Name().StringValue();
    const auto maybeType = parameter.Type().Maybe<TCoDataType>();

    auto newParameter = ssaValue->MutableParameter();
    newParameter->SetName(name);

    ctx.AddParameterName(name);
    return ssaValue->GetColumn().GetId();
}

ui64 ConvertSafeCastToColumn(const ui64 &columnId, const std::string& targetType, TKqpOlapCompileContext& ctx) {
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

ui64 ConvertSafeCastToColumn(const TExprBase& colName, const std::string& targetType, TKqpOlapCompileContext& ctx)
{
    return ConvertSafeCastToColumn(GetOrCreateColumnId(colName, ctx), targetType, ctx);
}

ui64 ConvertSafeCastToColumn(const TCoSafeCast& cast, TKqpOlapCompileContext& ctx)
{
    auto maybeDataType = cast.Type().Maybe<TCoDataType>();
    if (!maybeDataType) {
        if (const auto maybeOptionalType = cast.Type().Maybe<TCoOptionalType>()) {
            maybeDataType = maybeOptionalType.Cast().ItemType().Maybe<TCoDataType>();
        }
    }
    return ConvertSafeCastToColumn(cast.Value(), maybeDataType.Cast().Type().StringValue(), ctx);
}

struct TTypedColumn {
    const ui64 Id = 0ULL;
    const TTypeAnnotationNode *const Type = nullptr;
};

const TTypedColumn ConvertJsonValueToColumn(const TKqpOlapJsonValue& jsonValueCallable, TKqpOlapCompileContext& ctx) {
    Y_ABORT_UNLESS(NKikimr::NSsa::RuntimeVersion >= 3, "JSON_VALUE pushdown is supported starting from the v3 of SSA runtime.");

    const auto columnId = GetOrCreateColumnId(jsonValueCallable.Column(), ctx);
    const auto pathId = GetOrCreateColumnId(jsonValueCallable.Path(), ctx);

    auto *const command = ctx.CreateAssignCmd();
    auto *const jsonValueFunc = command->MutableFunction();

    jsonValueFunc->AddArguments()->SetId(columnId);
    jsonValueFunc->AddArguments()->SetId(pathId);

    jsonValueFunc->SetFunctionType(TProgram::YQL_KERNEL);
    const auto returningTypeArg = jsonValueCallable.ReturningType();
    const auto type = ctx.ExprCtx().MakeType<TOptionalExprType>(returningTypeArg.Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType());
    const auto idx = ctx.AddYqlKernelJsonValue(
        jsonValueCallable.Column(),
        jsonValueCallable.Path(),
        type);
    jsonValueFunc->SetKernelIdx(idx);

    return {command->GetColumn().GetId(), ctx.ConvertToBlockType(type)};
}

const TTypedColumn CompileJsonExists(const TKqpOlapJsonExists& jsonExistsCallable, TKqpOlapCompileContext& ctx) {
    Y_ABORT_UNLESS(NKikimr::NSsa::RuntimeVersion >= 3, "JSON_EXISTS pushdown is supported starting from the v3 of SSA runtime.");

    const auto columnId = GetOrCreateColumnId(jsonExistsCallable.Column(), ctx);
    const auto pathId = GetOrCreateColumnId(jsonExistsCallable.Path(), ctx);

    auto *const command = ctx.CreateAssignCmd();
    auto *const jsonExistsFunc = command->MutableFunction();

    jsonExistsFunc->AddArguments()->SetId(columnId);
    jsonExistsFunc->AddArguments()->SetId(pathId);

    jsonExistsFunc->SetFunctionType(TProgram::YQL_KERNEL);
    const auto type = ctx.ExprCtx().MakeType<TOptionalExprType>(ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool));
    const auto idx = ctx.AddYqlKernelJsonExists(
        jsonExistsCallable.Column(),
        jsonExistsCallable.Path(),
        type);
    jsonExistsFunc->SetKernelIdx(idx);

    if constexpr (NSsa::RuntimeVersion >= 4U) {
        return {ConvertSafeCastToColumn(command->GetColumn().GetId(), "Uint8", ctx), ctx.ConvertToBlockType(type)};
    } else {
        return {command->GetColumn().GetId(), ctx.ConvertToBlockType(type)};
    }
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

    if (auto maybeJsonValue = node.Maybe<TKqpOlapJsonValue>()) {
        return ConvertJsonValueToColumn(maybeJsonValue.Cast(), ctx).Id;
    }

    if (const auto maybeJsonExists = node.Maybe<TKqpOlapJsonExists>()) {
        return CompileJsonExists(maybeJsonExists.Cast(), ctx).Id;
    }
    YQL_ENSURE(false, "Unknown node in OLAP comparison compiler: " << node.Ref().Content());
}

ui64 CompileSimpleArrowComparison(const TKqpOlapFilterBinaryOp& comparison, TKqpOlapCompileContext& ctx)
{
    // Columns should be created before comparison, otherwise comparison fail to find columns
    const auto leftColumnId = GetOrCreateColumnId(comparison.Left(), ctx);
    const auto rightColumnId = GetOrCreateColumnId(comparison.Right(), ctx);

    auto *const command = ctx.CreateAssignCmd();
    auto *const cmpFunc = command->MutableFunction();

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

    return command->GetColumn().GetId();
}

ui64 CompileYqlKernelComparison(const TKqpOlapFilterBinaryOp& comparison, TKqpOlapCompileContext& ctx)
{
    // Columns should be created before comparison, otherwise comparison fail to find columns
    const auto leftColumnId = GetOrCreateColumnId(comparison.Left(), ctx);
    const auto rightColumnId = GetOrCreateColumnId(comparison.Right(), ctx);

    auto *const command = ctx.CreateAssignCmd();
    auto *const cmpFunc = command->MutableFunction();

    ui32 function = TProgram::TAssignment::FUNC_UNSPECIFIED;
    bool isYqlKernelsSupported = (NKikimr::NSsa::RuntimeVersion >= 3);
    bool needCastToBool = false;

    if (comparison.Operator() == "string_contains") {
        function = TProgram::TAssignment::FUNC_STR_MATCH;
        if (isYqlKernelsSupported) {
            cmpFunc->SetFunctionType(TProgram::YQL_KERNEL);
            auto idx = ctx.AddYqlKernelBinaryFunc(comparison.Pos(), TKernelRequestBuilder::EBinaryOp::StringContains,
                comparison.Left(),
                comparison.Right(),
                ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool));
            cmpFunc->SetKernelIdx(idx);
            needCastToBool = true;
        }
    } else if (comparison.Operator() == "starts_with") {
        function = TProgram::TAssignment::FUNC_STR_STARTS_WITH;
        if (isYqlKernelsSupported) {
            cmpFunc->SetFunctionType(TProgram::YQL_KERNEL);
            auto idx = ctx.AddYqlKernelBinaryFunc(comparison.Pos(), TKernelRequestBuilder::EBinaryOp::StartsWith,
                comparison.Left(),
                comparison.Right(),
                ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool));
            cmpFunc->SetKernelIdx(idx);
            needCastToBool = true;
        }
    } else if (comparison.Operator() == "ends_with") {
        function = TProgram::TAssignment::FUNC_STR_ENDS_WITH;
        if (isYqlKernelsSupported) {
            cmpFunc->SetFunctionType(TProgram::YQL_KERNEL);
            auto idx = ctx.AddYqlKernelBinaryFunc(comparison.Pos(), TKernelRequestBuilder::EBinaryOp::EndsWith,
                comparison.Left(),
                comparison.Right(),
                ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool));
            cmpFunc->SetKernelIdx(idx);
            needCastToBool = true;
        }
    }

    cmpFunc->SetId(function);
    cmpFunc->AddArguments()->SetId(leftColumnId);
    cmpFunc->AddArguments()->SetId(rightColumnId);

    if (needCastToBool) {
        return ConvertSafeCastToColumn(command->GetColumn().GetId(), "Boolean", ctx);
    }

    return command->GetColumn().GetId();
}



const TProgram::TAssignment* InvertResult(TProgram::TAssignment* command, TKqpOlapCompileContext& ctx)
{
    auto *const notCommand = ctx.CreateAssignCmd();
    auto *const notFunc = notCommand->MutableFunction();

    notFunc->SetId(TProgram::TAssignment::FUNC_BINARY_NOT);
    notFunc->AddArguments()->SetId(command->GetColumn().GetId());
    return notCommand;
}

TTypedColumn GetOrCreateColumnIdAndType(const TExprBase& node, TKqpOlapCompileContext& ctx);

template<bool Empty = false>
const TTypedColumn CompileExists(const TExprBase& arg, TKqpOlapCompileContext& ctx)
{
    const auto type = ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool);
    const auto column = GetOrCreateColumnIdAndType(arg, ctx);
    auto *const command = ctx.CreateAssignCmd();
    auto *const isNullFunc = command->MutableFunction();

    isNullFunc->SetId(TProgram::TAssignment::FUNC_IS_NULL);
    isNullFunc->AddArguments()->SetId(column.Id);

    if constexpr (Empty) {
        if constexpr (NSsa::RuntimeVersion >= 4U) {
            return {ConvertSafeCastToColumn(command->GetColumn().GetId(), "Uint8", ctx), ctx.ConvertToBlockType(type)};
        } else {
            return {command->GetColumn().GetId(), type};
        }
    }

    auto *const notCommand = InvertResult(command, ctx);
    if constexpr (NSsa::RuntimeVersion >= 4U) {
        return {ConvertSafeCastToColumn(notCommand->GetColumn().GetId(), "Uint8", ctx), ctx.ConvertToBlockType(type)};
    } else {
        return {notCommand->GetColumn().GetId(), type};
    }
}

TTypedColumn CompileYqlKernelUnaryOperation(const TKqpOlapFilterUnaryOp& operation, TKqpOlapCompileContext& ctx)
{
    auto oper = operation.Operator().StringValue();
    if (oper == "exists"sv) {
        return CompileExists<false>(operation.Arg(), ctx);
    } else if (oper == "empty"sv) {
        return CompileExists<true>(operation.Arg(), ctx);
    }

    const auto argument = GetOrCreateColumnIdAndType(operation.Arg(), ctx);
    auto *const command = ctx.CreateAssignCmd();
    auto *const function = command->MutableFunction();
    YQL_ENSURE(oper.to_title());
    const auto op = FromString<TKernelRequestBuilder::EUnaryOp>(oper);
    const auto resultType = TKernelRequestBuilder::EUnaryOp::Size == op ? ctx.GetReturnType(*argument.Type, ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Uint32)) : argument.Type;
    const auto idx = ctx.GetKernelRequestBuilder().AddUnaryOp(op, argument.Type, resultType);
    function->AddArguments()->SetId(argument.Id);
    function->SetKernelIdx(idx);
    function->SetFunctionType(TProgram::YQL_KERNEL);
    return {command->GetColumn().GetId(), resultType};
}

[[maybe_unused]]
TTypedColumn CompileYqlKernelBinaryOperation(const TKqpOlapFilterBinaryOp& operation, TKqpOlapCompileContext& ctx)
{
    // Columns should be created before operation, otherwise operation fail to find columns
    const auto leftColumn = GetOrCreateColumnIdAndType(operation.Left(), ctx);
    const auto rightColumn = GetOrCreateColumnIdAndType(operation.Right(), ctx);

    auto *const command = ctx.CreateAssignCmd();
    auto *const cmpFunc = command->MutableFunction();

    TKernelRequestBuilder::EBinaryOp op;
    const TTypeAnnotationNode* type = ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool);
    if (const std::string_view& oper = operation.Operator().Value(); oper == "string_contains"sv) {
        op = TKernelRequestBuilder::EBinaryOp::StringContains;
    } else if (oper == "starts_with"sv) {
        op = TKernelRequestBuilder::EBinaryOp::StartsWith;
    } else if (oper == "ends_with"sv) {
        op = TKernelRequestBuilder::EBinaryOp::EndsWith;
    } else if (oper == "eq"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Equals;
    } else if (oper == "neq"sv) {
        op = TKernelRequestBuilder::EBinaryOp::NotEquals;
    } else if (oper == "lt"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Less;
    } else if (oper == "lte"sv) {
        op = TKernelRequestBuilder::EBinaryOp::LessOrEqual;
    } else if (oper == "gt"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Greater;
    } else if (oper == "gte"sv) {
        op = TKernelRequestBuilder::EBinaryOp::GreaterOrEqual;
    } else if (oper == "+"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Add;
        type = nullptr;
    } else if (oper == "-"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Sub;
        type = nullptr;
    } else if (oper == "*"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Mul;
        type = nullptr;
    } else if (oper == "/"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Div;
        type = nullptr;
    } else if (oper == "%"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Mod;
        type = nullptr;
    } else if (oper == "??"sv) {
        op = TKernelRequestBuilder::EBinaryOp::Coalesce;
        bool stub;
        type = GetBlockItemType(*rightColumn.Type, stub);
    } else {
        YQL_ENSURE(false, "Unknown binary OLAP operation: " << oper);
    }

    const auto kernel = ctx.AddYqlKernelBinaryFunc(operation.Pos(), op, *leftColumn.Type, *rightColumn.Type, type);
    cmpFunc->SetYqlOperationId((ui32)op);
    cmpFunc->SetFunctionType(TProgram::YQL_KERNEL);
    cmpFunc->SetKernelIdx(kernel.first);
    cmpFunc->AddArguments()->SetId(leftColumn.Id);
    cmpFunc->AddArguments()->SetId(rightColumn.Id);
    return {command->GetColumn().GetId(), kernel.second};
}

template<typename TFunc>
const TTypedColumn BuildLogicalProgram(TPositionHandle pos, const TExprNode::TChildrenType& args, const TFunc function, TKqpOlapCompileContext& ctx)
{
    const auto childrenCount = args.size();
    if (childrenCount == 1) {
        // NOT operation is handled separately, thus only one available situation here:
        // this is binary operation with only one node, just build this node and return.
        return GetOrCreateColumnIdAndType(TExprBase(args.front()), ctx);
    }

    const bool twoArgs = 2U == childrenCount;
    const auto half = childrenCount >> 1U;
    const auto left = twoArgs ? GetOrCreateColumnIdAndType(TExprBase(args.front()), ctx) : BuildLogicalProgram(pos, args.subspan(0U, half), function, ctx);
    const auto right = twoArgs ? GetOrCreateColumnIdAndType(TExprBase(args.back()), ctx) : BuildLogicalProgram(pos, args.subspan(half), function, ctx);

    auto *const logicalOp = ctx.CreateAssignCmd();
    auto *const logicalFunc = logicalOp->MutableFunction();
    logicalFunc->AddArguments()->SetId(left.Id);
    logicalFunc->AddArguments()->SetId(right.Id);

    const TTypeAnnotationNode* block = nullptr;
    if constexpr (std::is_same<TFunc, TKernelRequestBuilder::EBinaryOp>()) {
        const auto kernel = ctx.AddYqlKernelBinaryFunc(pos, function, *left.Type, *right.Type, nullptr);
        block = kernel.second;
        logicalFunc->SetKernelIdx(kernel.first);
        logicalFunc->SetFunctionType(TProgram::YQL_KERNEL);
        logicalFunc->SetYqlOperationId((ui32)function);
    } else {
        logicalFunc->SetId((ui32)function);
        block = ctx.ExprCtx().MakeType<TBlockExprType>(ctx.ExprCtx().MakeType<TOptionalExprType>(ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool)));
    }

    return {logicalOp->GetColumn().GetId(), block};
}

const TTypedColumn BuildLogicalNot(const TExprBase& arg, TKqpOlapCompileContext& ctx) {
    if (const auto maybeExists = arg.Maybe<TKqpOlapFilterExists>()) {
        return CompileExists<true>(maybeExists.Cast().Column(), ctx);
    }

    // Not is a special way in case it has only one child
    const auto value = GetOrCreateColumnIdAndType(arg, ctx);
    auto *const notOp = ctx.CreateAssignCmd();
    auto *const notFunc = notOp->MutableFunction();

    notFunc->AddArguments()->SetId(value.Id);

    if constexpr (NSsa::RuntimeVersion >= 4U) {
        const auto block = ctx.ExprCtx().MakeType<TBlockExprType>(ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool));
        const auto idx = ctx.GetKernelRequestBuilder().AddUnaryOp(TKernelRequestBuilder::EUnaryOp::Not, block, block);
        notFunc->SetKernelIdx(idx);
        notFunc->SetFunctionType(TProgram::YQL_KERNEL);
    } else
        notFunc->SetId(TProgram::TAssignment::FUNC_BINARY_NOT);

    return {notOp->GetColumn().GetId(), value.Type};
}

TTypedColumn GetOrCreateColumnIdAndType(const TExprBase& node, TKqpOlapCompileContext& ctx) {
    if (const auto& maybeBinaryOp = node.Maybe<TKqpOlapFilterBinaryOp>()) {
        if constexpr (NSsa::RuntimeVersion >= 4U) {
            if (const auto& binaryOp = maybeBinaryOp.Cast(); ctx.CheckYqlCompatibleArgsTypes(binaryOp)) {
                return CompileYqlKernelBinaryOperation(binaryOp, ctx);
            } else {
                return {
                    ConvertSafeCastToColumn(CompileSimpleArrowComparison(binaryOp, ctx), "Uint8", ctx),
                    ctx.ExprCtx().MakeType<TBlockExprType>(ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool))
                };
            }
        } else {
            return {
                CompileSimpleArrowComparison(maybeBinaryOp.Cast(), ctx),
                ctx.ExprCtx().MakeType<TBlockExprType>(ctx.ExprCtx().MakeType<TDataExprType>(EDataSlot::Bool))
            };
        }
    } else if (const auto& maybeUnaryOp = node.Maybe<TKqpOlapFilterUnaryOp>()) {
        return CompileYqlKernelUnaryOperation(maybeUnaryOp.Cast(), ctx);
    } else if (const auto& maybeAnd = node.Maybe<TKqpOlapAnd>()) {
        if constexpr (NSsa::RuntimeVersion >= 4U)
            return BuildLogicalProgram(node.Pos(), maybeAnd.Ref().Children(), TKernelRequestBuilder::EBinaryOp::And, ctx);
        else
            return BuildLogicalProgram(node.Pos(), maybeAnd.Ref().Children(), TProgram::TAssignment::FUNC_BINARY_AND, ctx);
    } else if (const auto& maybeOr = node.Maybe<TKqpOlapOr>()) {
        if constexpr (NSsa::RuntimeVersion >= 4U)
            return BuildLogicalProgram(node.Pos(), maybeOr.Ref().Children(), TKernelRequestBuilder::EBinaryOp::Or, ctx);
        else
            return BuildLogicalProgram(node.Pos(), maybeOr.Ref().Children(), TProgram::TAssignment::FUNC_BINARY_OR, ctx);
    } else if (const auto& maybeXor = node.Maybe<TKqpOlapXor>()) {
        if constexpr (NSsa::RuntimeVersion >= 4U)
            return BuildLogicalProgram(node.Pos(), maybeXor.Ref().Children(), TKernelRequestBuilder::EBinaryOp::Xor, ctx);
        else
            return BuildLogicalProgram(node.Pos(), maybeXor.Ref().Children(), TProgram::TAssignment::FUNC_BINARY_XOR, ctx);
    } else if (const auto& maybeNot = node.Maybe<TKqpOlapNot>()) {
        return BuildLogicalNot(maybeNot.Cast().Value(), ctx);
    } else if (const auto& maybeJsonValue = node.Maybe<TKqpOlapJsonValue>()) {
        return ConvertJsonValueToColumn(maybeJsonValue.Cast(), ctx);
    } else if (const auto& maybeJsonValue = node.Maybe<TKqpOlapJsonExists>()) {
        return CompileJsonExists(maybeJsonValue.Cast(), ctx);
    }
    return {GetOrCreateColumnId(node, ctx), ctx.GetArgType(node)};
}

ui64 CompileComparison(const TKqpOlapFilterBinaryOp& comparison, TKqpOlapCompileContext& ctx)
{
    if constexpr (NKikimr::NSsa::RuntimeVersion >= 4U) {
        if (ctx.CheckYqlCompatibleArgsTypes(comparison)) {
            return CompileYqlKernelBinaryOperation(comparison, ctx).Id;
        } else {
            return ConvertSafeCastToColumn(CompileSimpleArrowComparison(comparison, ctx), "Uint8", ctx);
        }
    }

    std::string op = comparison.Operator().StringValue().c_str();
    if (SimpleArrowCmpFuncs.contains(op)) {
        return CompileSimpleArrowComparison(comparison, ctx);
    } else if (YqlKernelCmpFuncs.contains(op)) {
        return CompileYqlKernelComparison(comparison, ctx);
    } else {
        YQL_ENSURE(false, "Unknown comparison operator: " << op);
    }
}

ui64 CompileCondition(const TExprBase& condition, TKqpOlapCompileContext& ctx) {
    if constexpr (NKikimr::NSsa::RuntimeVersion >= 4U) {
        if (const auto maybeCompare = condition.Maybe<TKqpOlapFilterUnaryOp>()) {
            return CompileYqlKernelUnaryOperation(maybeCompare.Cast(), ctx).Id;
        }
    }

    if (const auto maybeCompare = condition.Maybe<TKqpOlapFilterBinaryOp>()) {
        return CompileComparison(maybeCompare.Cast(), ctx);
    }

    if (const auto maybeExists = condition.Maybe<TKqpOlapFilterExists>()) {
        return CompileExists(maybeExists.Cast().Column(), ctx).Id;
    }

    if (const auto maybeJsonExists = condition.Maybe<TKqpOlapJsonExists>()) {
        return CompileJsonExists(maybeJsonExists.Cast(), ctx).Id;
    }

    if (const auto maybeNot = condition.Maybe<TKqpOlapNot>()) {
        return BuildLogicalNot(maybeNot.Cast().Value(), ctx).Id;
    }

    ui32 function = TProgram::TAssignment::FUNC_UNSPECIFIED;
    TKernelRequestBuilder::EBinaryOp op;

    if (condition.Maybe<TKqpOlapAnd>()) {
        function = TProgram::TAssignment::FUNC_BINARY_AND;
        op = TKernelRequestBuilder::EBinaryOp::And;
    } else if (condition.Maybe<TKqpOlapOr>()) {
        function = TProgram::TAssignment::FUNC_BINARY_OR;
        op = TKernelRequestBuilder::EBinaryOp::Or;
    } else if (condition.Maybe<TKqpOlapXor>()) {
        function = TProgram::TAssignment::FUNC_BINARY_XOR;
        op = TKernelRequestBuilder::EBinaryOp::Xor;
    } else {
        YQL_ENSURE(false, "Unsuppoted logical operation: " << condition.Ref().Content());
    }

    if constexpr (NSsa::RuntimeVersion >= 4U)
        return BuildLogicalProgram(condition.Pos(), condition.Ref().Children(), op, ctx).Id;
    else
        return BuildLogicalProgram(condition.Pos(), condition.Ref().Children(), function, ctx).Id;
}

void CompileFilter(const TKqpOlapFilter& filterNode, TKqpOlapCompileContext& ctx) {
    const auto condition = CompileCondition(filterNode.Condition(), ctx);
    auto* filter = ctx.CreateFilter();
    filter->MutablePredicate()->SetId(condition);
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
    NKqpProto::TKqpPhyOpReadOlapRanges& readProto, const std::vector<std::string>& resultColNames,
    TExprContext &exprCtx)
{
    YQL_ENSURE(lambda.Args().Size() == 1);

    TKqpOlapCompileContext ctx(lambda.Args().Arg(0), tableMeta, readProto, resultColNames, exprCtx);

    CompileOlapProgramImpl(lambda.Body(), ctx);
    CompileFinalProjection(ctx);

    ctx.SerializeToProto();
}

} // namespace NKqp
} // namespace NKikimr
