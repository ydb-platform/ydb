#include "yql_opt_json_peephole_physical.h"

#include <ydb/library/yql/core/yql_atom_enums.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql {

namespace {

using namespace NNodes;

TExprNode::TPtr BuildJsonParse(const TExprNode::TPtr& jsonExpr, TExprContext& ctx) {
    auto jsonPos = jsonExpr->Pos();

    auto argumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        ctx.MakeType<TDataExprType>(EDataSlot::Json),
    });

    auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        argumentsType,
        ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
        ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
    });

    auto parse = Build<TCoUdf>(ctx, jsonPos)
        .MethodName()
            .Build("Json2.Parse")
        .RunConfigValue<TCoVoid>()
            .Build()
        .UserType(ExpandType(jsonPos, *udfArgumentsType, ctx))
        .Done().Ptr();

    return Build<TCoApply>(ctx, jsonPos)
        .Callable(parse)
        .FreeArgs()
            .Add(jsonExpr)
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr GetJsonDocumentOrParseJson(const TExprNode::TPtr& jsonExpr, TExprContext& ctx, EDataSlot& argumentDataSlot) {
    const TTypeAnnotationNode* type = jsonExpr->GetTypeAnn();
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    argumentDataSlot = type->Cast<TDataExprType>()->GetSlot();

    // If jsonExpr has JsonDocument type, there is no need to parse it
    if (argumentDataSlot == EDataSlot::JsonDocument) {
        return jsonExpr;
    }

    // Otherwise jsonExpr has Json type and we need to wrap it in Json2::Parse
    return BuildJsonParse(jsonExpr, ctx);
}

TExprNode::TPtr GetJsonDocumentOrParseJson(const TCoJsonQueryBase& jsonExpr, TExprContext& ctx, EDataSlot& argumentDataSlot) {
    return GetJsonDocumentOrParseJson(jsonExpr.Json().Ptr(), ctx, argumentDataSlot);
}

TExprNode::TPtr BuildJsonCompilePath(const TCoJsonQueryBase& jsonExpr, TExprContext& ctx) {
    auto jsonPathPos = jsonExpr.JsonPath().Pos();

    auto argumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        ctx.MakeType<TDataExprType>(EDataSlot::Utf8)
    });

    auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        argumentsType,
        ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
        ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
    });

    auto compilePath = Build<TCoUdf>(ctx, jsonPathPos)
        .MethodName()
            .Build("Json2.CompilePath")
        .RunConfigValue<TCoVoid>()
            .Build()
        .UserType(ExpandType(jsonPathPos, *udfArgumentsType, ctx))
        .Done().Ptr();

    return Build<TCoApply>(ctx, jsonPathPos)
        .Callable(compilePath)
        .FreeArgs()
            .Add(jsonExpr.JsonPath())
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr BuildJsonSerialize(const TExprNode::TPtr& resourceExpr, TExprContext& ctx) {
    auto resourcePos = resourceExpr->Pos();

    auto argumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        ctx.MakeType<TOptionalExprType>(ctx.MakeType<TResourceExprType>("JsonNode")),
    });

    auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        argumentsType,
        ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
        ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
    });

    auto parse = Build<TCoUdf>(ctx, resourcePos)
        .MethodName()
            .Build("Json2.Serialize")
        .RunConfigValue<TCoVoid>()
            .Build()
        .UserType(ExpandType(resourcePos, *udfArgumentsType, ctx))
        .Done().Ptr();

    return Build<TCoApply>(ctx, resourcePos)
        .Callable(parse)
        .FreeArgs()
            .Add(resourceExpr)
            .Build()
        .Done().Ptr();
}

} // namespace


TExprNode::TPtr ExpandJsonValue(const TExprNode::TPtr& node, TExprContext& ctx) {
    /*
    Here we rewrite expression
        JSON_VALUE(
            <json>, <jsonPath>
            [PASSING <variableExpr1> AS <variableName1>, ...]
            [RETURNING <resultType>]
            [(NULL | DEFAULT <onEmptyExpr>) ON EMPTY]
            [(NULL | DEFAULT <onErrorExpr>) ON ERROR]
        )
    Generated depends on the <resultType> specified in RETURNING section:
        1. No RETURNING section
            Default returning type of JsonValue is Utf8 and it must convert
            result of JsonPath expression into Utf8 string.
            Json2::SqlValueConvertToUtf8 is used
        2. <resultType> is a numeric type (Int16, Uint16, Float, etc.)
            Json2::SqlValueNumber is used with additional CAST to corresponding type
        3. <resultType> is a date type (Date, Datetime, Timestamp)
            Json2::SqlValueInt64 is used with additional CAST to corresponding type
        4. <resultType> is Bool
            Json2::SqlValueBool is used
        5. <resultType> is String
            Json2::SqlValueUtf8 is used with additional CAST to String
        6. <resultType> is Utf8
            Json2::SqlValueUtf8 is used
    Returning type of all Json2::SqlValue* functions is Variant<Tuple<Uint8, String?>, <resultType>?>:
        1. If variant holds first type, either error happened or the result is empty.
            If first tuple element is 0, result is empty.
            If first tuple element is 1, error happened.
            Second tuple element contains message that can be displayed to the user.
        2. If variant hold second type, execution was successful and it is a result.
    We process result of Json2::SqlValue* function by using Visit callable with lambdas handling each type.
    Note that in some cases we need to CAST result of Json2::SqlValue* and it can fail. So:
        1. If the result of Json2::SqlValue* is NULL, we return Nothing(<resultType>)
        2. Otherwise we check the result of SafeCast callable. If it is NULL, cast has failed and it is an error.
            If it holds some value, we return it to the user.
    If no CAST is needed, we just return the result of Json2::SqlValue*.
    What is more, <onEmptyExpr> and <onErrorExpr> must be casted to <resultType> and this CAST can fail too.
    ANSI SQL specification is unclear about what to do with this situation. If we failed to cast <onEmptyExpr> to
    target type, we return <onErrorExpr>. If we failed to cast <onErrorExpr> to target type, we throw an exception.

    I know all this sounds very clumsy and a lot of logic to handle in s-expressions. If you have a better idea
    of a way to handle all this ***, please write to laplab@.
    */
    TCoJsonValue jsonValue(node);

    // <json expr> or Json2::Parse(<json expr>)
    EDataSlot jsonDataSlot;
    TExprNode::TPtr jsonExpr = GetJsonDocumentOrParseJson(jsonValue, ctx, jsonDataSlot);

    // Json2::CompilePath(<jsonPath>)
    TExprNode::TPtr compilePathExpr = BuildJsonCompilePath(jsonValue, ctx);

    // Json2::SqlValue...(<parsedJson>, <compiledJsonPath>)
    TExprNode::TPtr sqlValueExpr;
    const auto returnTypeAnn = node->GetTypeAnn()->Cast<TOptionalExprType>();
    const auto unwrappedSlot = returnTypeAnn->GetItemType()->Cast<TDataExprType>()->GetSlot();
    bool needCast = false;
    const auto jsonValuePos = jsonValue.Pos();
    {
        TString sqlValueUdfName;
        if (IsDataTypeNumeric(unwrappedSlot)) {
            sqlValueUdfName = "SqlValueNumber";
            needCast = true;
        } else if (IsDataTypeDate(unwrappedSlot)) {
            sqlValueUdfName = "SqlValueInt64";
            needCast = true;
        } else if (unwrappedSlot == EDataSlot::Utf8 || unwrappedSlot == EDataSlot::String) {
            if (jsonValue.ReturningType()) {
                sqlValueUdfName = "SqlValueUtf8";
            } else {
                sqlValueUdfName = "SqlValueConvertToUtf8";
            }
            needCast = unwrappedSlot == EDataSlot::String;
        } else if (unwrappedSlot == EDataSlot::Bool) {
            sqlValueUdfName = "SqlValueBool";
        } else {
            YQL_ENSURE(false, "Unsupported type");
        }

        const TTypeAnnotationNode* inputType = nullptr;
        if (jsonDataSlot == EDataSlot::JsonDocument) {
            inputType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::JsonDocument));
            sqlValueUdfName = "JsonDocument" + sqlValueUdfName;
        } else {
            inputType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TResourceExprType>("JsonNode"));
        }
        sqlValueUdfName = "Json2." + sqlValueUdfName;

        TTypeAnnotationNode::TListType arguments = {
            inputType,
            ctx.MakeType<TResourceExprType>("JsonPath")
        };

        auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TTupleExprType>(arguments),
            ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
            ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
        });

        auto sqlValue = Build<TCoUdf>(ctx, jsonValuePos)
            .MethodName()
                .Build(sqlValueUdfName)
            .RunConfigValue<TCoVoid>()
                .Build()
            .UserType(ExpandType(jsonValuePos, *udfArgumentsType, ctx))
            .Done().Ptr();

        sqlValueExpr = Build<TCoApply>(ctx, jsonValuePos)
            .Callable(sqlValue)
            .FreeArgs()
                .Add(jsonExpr)
                .Add(compilePathExpr)
                .Add(jsonValue.Variables())
                .Build()
            .Done().Ptr();
    }

    auto makeCastOrValue = [&](TPositionHandle pos, const TExprNode::TPtr& source, const TExprNode::TPtr& onCastFail) {
        /*
        if Exists($source)
        then
            return IfPresent(
                CAST($source as <resultType>),
                ($x) -> { return Just($x); },
                $onCastFail
            )
        else
            return Nothing(<resultType>)
        */
        TExprNode::TPtr returnTypeNode = ExpandType(pos, *returnTypeAnn, ctx);
        return Build<TCoIf>(ctx, pos)
            .Predicate<TCoExists>()
                .Optional(source)
                .Build()
            .ThenValue<TCoIfPresent>()
                .Optional<TCoSafeCast>()
                    .Value(source)
                    .Type(returnTypeNode)
                        .Build()
                .PresentHandler<TCoLambda>()
                    .Args({"unwrappedValue"})
                    .Body<TCoJust>()
                        .Input("unwrappedValue")
                        .Build()
                    .Build()
                .MissingValue(onCastFail)
                .Build()
            .ElseValue<TCoNothing>()
                .OptionalType(returnTypeNode)
                .Build()
            .Done().Ptr();
    };

    auto makeThrow = [&](TPositionHandle pos, const TExprNode::TPtr& message) {
        return Build<TCoEnsure>(ctx, pos)
            .Value<TCoNothing>()
                .OptionalType(ExpandType(pos, *returnTypeAnn, ctx))
                .Build()
            .Predicate<TCoBool>()
                .Literal()
                    .Build("false")
                .Build()
            .Message(message)
        .Done().Ptr();
    };

    auto makeHandler = [&](EJsonValueHandlerMode mode, const TExprNode::TPtr& node, const TExprNode::TPtr& errorMessage, const TExprNode::TPtr& onCastFail) -> TExprNode::TPtr {
        const auto pos = node->Pos();
        if (mode == EJsonValueHandlerMode::Error) {
            return makeThrow(pos, errorMessage);
        }

        // Convert NULL to Nothing(<resultType>)
        if (IsNull(*node)) {
            return Build<TCoNothing>(ctx, pos)
                    .OptionalType(ExpandType(pos, *returnTypeAnn, ctx))
                    .Done().Ptr();
        }

        // If type is not Optional, wrap expression in Just call
        TExprNode::TPtr result = node;
        const auto typeAnn = node->GetTypeAnn();
        if (typeAnn->GetKind() != ETypeAnnotationKind::Optional) {
            result = Build<TCoJust>(ctx, pos)
                .Input(result)
                .Done().Ptr();
        }

        // Perform CAST to <resultType> or return onCastFail
        return makeCastOrValue(pos, result, onCastFail);
    };

    const auto onEmptyMode = FromString<EJsonValueHandlerMode>(jsonValue.OnEmptyMode().Ref().Content());
    const auto onErrorMode = FromString<EJsonValueHandlerMode>(jsonValue.OnErrorMode().Ref().Content());
    auto makeOnErrorHandler = [&](const TExprNode::TPtr& errorMessage) {
        const auto onError = jsonValue.OnError();
        const auto throwCastError = makeThrow(
            onError.Pos(),
            Build<TCoString>(ctx, onError.Pos())
                .Literal()
                    .Build(TStringBuilder() << "Failed to cast default value from ON ERROR clause to target type " << FormatType(returnTypeAnn))
            .Done().Ptr()
        );

        return makeHandler(onErrorMode, onError.Ptr(), errorMessage, throwCastError);
    };
    auto makeOnEmptyHandler = [&](const TExprNode::TPtr& errorMessage) {
        const auto onEmptyDefaultCastError = Build<TCoString>(ctx, jsonValue.OnEmpty().Pos())
            .Literal()
                .Build(TStringBuilder() << "Failed to cast default value from ON EMPTY clause to target type " << FormatType(returnTypeAnn))
        .Done().Ptr();
        return makeHandler(onEmptyMode, jsonValue.OnEmpty().Ptr(), errorMessage, makeOnErrorHandler(onEmptyDefaultCastError));
    };

    /*
    Lambda for handling first type of variant

    ($errorTuple) -> {
        if $errorTuple[0] == 0
        then
            return onEmptyHandler
        else
            return onErrorHandler
    }
    */
    auto errorTupleArgument = ctx.NewArgument(jsonValuePos, "errorTuple");
    auto sqlValueMessage = Build<TCoNth>(ctx, jsonValuePos)
        .Tuple(errorTupleArgument)
        .Index()
            .Build("1")
        .Done().Ptr();
    const auto errorLambda = Build<TCoLambda>(ctx, jsonValuePos)
        .Args(TExprNode::TListType{errorTupleArgument})
        .Body<TCoIf>()
            .Predicate<TCoCmpEqual>()
                .Left<TCoNth>()
                    .Tuple(errorTupleArgument)
                    .Index()
                        .Build("0")
                    .Build()
                .Right<TCoUint8>()
                    .Literal()
                        .Build("0")
                    .Build()
                .Build()
            .ThenValue(makeOnEmptyHandler(sqlValueMessage))
            .ElseValue(makeOnErrorHandler(sqlValueMessage))
            .Build()
        .Done().Ptr();

    // Lambda for handling second type of variant
    TExprNode::TPtr sqlValueResultLambda;
    if (needCast) {
        const auto errorMessage = Build<TCoString>(ctx, jsonValuePos)
            .Literal()
                .Build(TStringBuilder() << "Failed to cast extracted JSON value to target type " << FormatType(returnTypeAnn))
            .Done().Ptr();
        const auto inputArgument = ctx.NewArgument(jsonValuePos, "sqlValueResult");
        sqlValueResultLambda = Build<TCoLambda>(ctx, jsonValuePos)
            .Args(TExprNode::TListType{inputArgument})
            .Body(makeCastOrValue(jsonValuePos, inputArgument, makeOnErrorHandler(errorMessage)))
            .Done().Ptr();
    } else {
        /*
        ($sqlValueResult) -> {
            return $sqlValueResult;
        }
        */
        sqlValueResultLambda = Build<TCoLambda>(ctx, jsonValuePos)
            .Args({"sqlValueResult"})
            .Body("sqlValueResult")
            .Done().Ptr();
    }

    // Visit call to get the result
    const auto visitResult = Build<TCoVisit>(ctx, jsonValuePos)
        .Input(sqlValueExpr)
        .FreeArgs()
            .Add<TCoAtom>()
                .Build("0")
            .Add(errorLambda)
            .Add<TCoAtom>()
                .Build("1")
            .Add(sqlValueResultLambda)
            .Build()
        .Done().Ptr();

    return visitResult;
}

TExprNode::TPtr ExpandJsonExists(const TExprNode::TPtr& node, TExprContext& ctx) {
    /*
    Here we rewrite expression
        JSON_EXISTS(<json expr>, <jsonpath> [PASSING <variableExpr1> AS <variableName1>, ...] {TRUE | FALSE | UNKNOWN} ON ERROR)
    into
        Json2::SqlExists(Json2::Parse(<json expr>), Json2::CompilePath(<jsonpath>), <dict with variables>, <on error value>)
    and its sibling
        JSON_EXISTS(<json expr>, <jsonpath> [PASSING <variableExpr1> AS <variableName1>, ...] ERROR ON ERROR)
    into
        Json2::SqlTryExists(Json2::Parse(<json expr>), <dict with variables>, Json2::CompilePath(<jsonpath>))
    */
    TCoJsonExists jsonExists(node);

    // <json expr> or Json2::Parse(<json expr>)
    EDataSlot jsonDataSlot;
    TExprNode::TPtr parseJsonExpr = GetJsonDocumentOrParseJson(jsonExists, ctx, jsonDataSlot);

    // Json2::CompilePath(<jsonPath>)
    TExprNode::TPtr compilePathExpr = BuildJsonCompilePath(jsonExists, ctx);

    // Json2::SqlExists(<json>, <compiled jsonpath>, [<default value>])
    // or
    // Json2::SqlTryExists(<json>, <compiled jsonpath>)
    const bool needThrow = !jsonExists.OnError().IsValid();

    TString sqlExistsUdfName = "SqlExists";
    if (needThrow) {
        sqlExistsUdfName = "SqlTryExists";
    }

    const TTypeAnnotationNode* inputType = nullptr;
    if (jsonDataSlot == EDataSlot::JsonDocument) {
        inputType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::JsonDocument));
        sqlExistsUdfName = "JsonDocument" + sqlExistsUdfName;
    } else {
        inputType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TResourceExprType>("JsonNode"));
    }
    sqlExistsUdfName = "Json2." + sqlExistsUdfName;

    TTypeAnnotationNode::TListType arguments = {
        inputType,
        ctx.MakeType<TResourceExprType>("JsonPath")
    };

    if (!needThrow) {
        const auto boolType = ctx.MakeType<TDataExprType>(EDataSlot::Bool);
        const auto optionalBoolType = ctx.MakeType<TOptionalExprType>(boolType);
        arguments.push_back(optionalBoolType);
    }

    auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        ctx.MakeType<TTupleExprType>(arguments),
        ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
        ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
    });

    const auto jsonExistsPos = jsonExists.Pos();
    auto sqlExists = Build<TCoUdf>(ctx, jsonExistsPos)
        .MethodName()
            .Build(sqlExistsUdfName)
        .RunConfigValue<TCoVoid>()
            .Build()
        .UserType(ExpandType(jsonExistsPos, *udfArgumentsType, ctx))
        .Done().Ptr();

    if (needThrow) {
        return Build<TCoApply>(ctx, jsonExistsPos)
            .Callable(sqlExists)
            .FreeArgs()
                .Add(parseJsonExpr)
                .Add(compilePathExpr)
                .Add(jsonExists.Variables())
                .Build()
            .Done().Ptr();
    }

    return Build<TCoApply>(ctx, jsonExistsPos)
        .Callable(sqlExists)
        .FreeArgs()
            .Add(parseJsonExpr)
            .Add(compilePathExpr)
            .Add(jsonExists.Variables())
            .Add(jsonExists.OnError().Cast())
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr ExpandJsonQuery(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    /*
    Here we rewrite expression
        JSON_QUERY(
            <json expr>,
            <jsonpath>
            [PASSING <variableExpr1> AS <variableName1>, ...]
            [{WITHOUT [ARRAY] | WITH [CONDITIONAL | UNCONDITIONAL] [ARRAY]} WRAPPER]
            [{ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT} ON EMPTY]
            [{ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT} ON ERROR]
        )
    into something like
        Json2::SqlQuery...(
            Json2::Parse(<json expr>),
            Json2::CompilePath(<jsonpath>),
            <dict with variables>,
            <do we have ERROR ON EMPTY?>,
            <default value depending on {NULL | EMPTY ARRAY | EMPTY OBJECT} ON EMPTY>,
            <do we have ERROR ON ERROR?>,
            <default value depending on {NULL | EMPTY ARRAY | EMPTY OBJECT} ON ERROR>
        )
    Exact UDF name is choosen depending on wrap config:
        - WITHOUT [ARRAY] WRAPPER -> Json2::SqlQuery
        - WITH [UNCONDITIONAL] [ARRAY] WRAPPER -> Json2::SqlQueryWrap
        - WITH CONDITIONAL [ARRAY] WRAPPER -> Json2::SqlQueryConditionalWrap
    */
    TCoJsonQuery jsonQuery(node);

    // <json expr> or Json2::Parse(<json expr>)
    EDataSlot jsonDataSlot;
    TExprNode::TPtr parseJsonExpr = GetJsonDocumentOrParseJson(jsonQuery, ctx, jsonDataSlot);

    // Json2::CompilePath(<jsonPath>)
    TExprNode::TPtr compilePathExpr = BuildJsonCompilePath(jsonQuery, ctx);

    // Json2::SqlQuery...(<json expr>, <jsonpath>, ...)
    const auto wrapMode = FromString<EJsonQueryWrap>(jsonQuery.WrapMode().Ref().Content());
    TString sqlQueryUdfName = "SqlQuery";
    switch (wrapMode) {
        case EJsonQueryWrap::NoWrap:
            sqlQueryUdfName = "SqlQuery";
            break;
        case EJsonQueryWrap::Wrap:
            sqlQueryUdfName = "SqlQueryWrap";
            break;
        case EJsonQueryWrap::ConditionalWrap:
            sqlQueryUdfName = "SqlQueryConditionalWrap";
            break;
    }

    const TTypeAnnotationNode* inputType = nullptr;
    if (jsonDataSlot == EDataSlot::JsonDocument) {
        inputType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::JsonDocument));
        sqlQueryUdfName = "JsonDocument" + sqlQueryUdfName;
    } else {
        inputType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TResourceExprType>("JsonNode"));
    }
    inputType = ctx.MakeType<TOptionalExprType>(inputType);
    sqlQueryUdfName = "Json2." + sqlQueryUdfName;

    const auto optionalJsonResourceType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TResourceExprType>("JsonNode"));
    TTypeAnnotationNode::TListType arguments{
        inputType,
        ctx.MakeType<TResourceExprType>("JsonPath"),
        ctx.MakeType<TDataExprType>(EDataSlot::Bool),
        optionalJsonResourceType,
        ctx.MakeType<TDataExprType>(EDataSlot::Bool),
        optionalJsonResourceType,
    };

    auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        ctx.MakeType<TTupleExprType>(arguments),
        ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
        ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
    });

    auto buildShouldThrow = [&](EJsonQueryHandler handler, TPositionHandle pos) {
        return Build<TCoBool>(ctx, pos)
                .Literal()
                    .Build(handler == EJsonQueryHandler::Error ? "true" : "false")
                .Done().Ptr();
    };

    auto buildHandler = [&](EJsonQueryHandler handler, TPositionHandle pos) {
        switch (handler) {
            case EJsonQueryHandler::Error:
            case EJsonQueryHandler::Null: {
                // Nothing(Resource<JsonNode>)
                return Build<TCoNothing>(ctx, pos)
                        .OptionalType(ExpandType(pos, *optionalJsonResourceType, ctx))
                        .Done().Ptr();
            }
            case EJsonQueryHandler::EmptyArray: {
                auto value = Build<TCoJson>(ctx, pos)
                    .Literal()
                        .Build("[]")
                    .Done().Ptr();
                return BuildJsonParse(value, ctx);
            }
            case EJsonQueryHandler::EmptyObject: {
                auto value = Build<TCoJson>(ctx, pos)
                    .Literal()
                        .Build("{}")
                    .Done().Ptr();
                return BuildJsonParse(value, ctx);
            }
        }
    };

    const auto jsonQueryPos = jsonQuery.Pos();
    auto sqlQuery = Build<TCoUdf>(ctx, jsonQueryPos)
        .MethodName()
            .Build(sqlQueryUdfName)
        .RunConfigValue<TCoVoid>()
            .Build()
        .UserType(ExpandType(jsonQueryPos, *udfArgumentsType, ctx))
        .Done().Ptr();

    const auto onEmpty = FromString<EJsonQueryHandler>(jsonQuery.OnEmpty().Ref().Content());
    const auto onError = FromString<EJsonQueryHandler>(jsonQuery.OnError().Ref().Content());
    const auto onEmptyPos = jsonQuery.OnEmpty().Pos();
    const auto onErrorPos = jsonQuery.OnError().Pos();

    auto sqlQueryApply = Build<TCoApply>(ctx, jsonQueryPos)
        .Callable(sqlQuery)
        .FreeArgs()
            .Add(parseJsonExpr)
            .Add(compilePathExpr)
            .Add(jsonQuery.Variables())
            .Add(buildShouldThrow(onEmpty, onEmptyPos))
            .Add(buildHandler(onEmpty, onEmptyPos))
            .Add(buildShouldThrow(onError, onErrorPos))
            .Add(buildHandler(onError, onErrorPos))
            .Build()
        .Done().Ptr();

    // In this case we need to serialize Resource<JsonNode> to Json type
    if (!typesCtx.JsonQueryReturnsJsonDocument) {
        return BuildJsonSerialize(sqlQueryApply, ctx);
    }

    // Now we need to serialize Resource<JsonNode> from sqlQueryApply to JsonDocument
    {
        auto resourcePos = sqlQueryApply->Pos();

        auto argumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TOptionalExprType>(ctx.MakeType<TResourceExprType>("JsonNode")),
        });

        auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            argumentsType,
            ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
            ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
        });

        TStringBuf serializeUdfName = "Json2.Serialize";
        if (typesCtx.JsonQueryReturnsJsonDocument) {
            serializeUdfName = "Json2.SerializeToJsonDocument";
        }
        auto parse = Build<TCoUdf>(ctx, resourcePos)
            .MethodName()
                .Build(serializeUdfName)
            .RunConfigValue<TCoVoid>()
                .Build()
            .UserType(ExpandType(resourcePos, *udfArgumentsType, ctx))
            .Done().Ptr();

        return Build<TCoApply>(ctx, resourcePos)
            .Callable(parse)
            .FreeArgs()
                .Add(sqlQueryApply)
                .Build()
            .Done().Ptr();
    }
};

} // namespace NYql