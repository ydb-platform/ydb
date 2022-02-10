#include "type_ann_core.h"
#include "type_ann_expr.h"
#include "type_ann_impl.h"
#include "type_ann_list.h"
#include "type_ann_columnorder.h"
#include "type_ann_wide.h"
#include "type_ann_types.h"

#include <ydb/library/yql/core/yql_atom_enums.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_callable_transform.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/utils/utf8.h>

#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>

#include <util/generic/serialized_enum.h>
#include <util/generic/singleton.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <util/stream/null.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <algorithm>
#include <functional>

namespace NYql {
    TString DotJoin(const TStringBuf& sourceName, const TStringBuf& columnName) {
        TStringBuilder sb;
        sb << sourceName << "." << columnName;
        return sb;
    }

namespace NTypeAnnImpl {
    enum class EDictItems {
        Both = 0,
        Keys = 1,
        Payloads = 2
    };

    template <typename T>
    bool IsValidSmallData(TExprNode& atomNode, const TStringBuf& type, TExprContext& ctx, NKikimr::NUdf::EDataSlot slot, TMaybe<TString>& plainValue) {
        bool isValid;
        if (atomNode.Flags() & TNodeFlags::BinaryContent) {
            if (atomNode.Content().size() != NKikimr::NUdf::GetDataTypeInfo(slot).FixedSize) {
                isValid = false;
            } else {
                const auto data = *reinterpret_cast<const T*>(atomNode.Content().data());
                isValid = NKikimr::NMiniKQL::IsValidValue(slot, NKikimr::NUdf::TUnboxedValuePod(data));
                if (isValid) {
                    plainValue = ToString(data);
                }
            }
        } else {
            isValid = NKikimr::NMiniKQL::IsValidStringValue(slot, atomNode.Content());
            if (!isValid) {
                if (slot == NKikimr::NUdf::EDataSlot::Bool) {
                    isValid = (atomNode.Content() == TStringBuf("0")) || (atomNode.Content() == TStringBuf("1"));
                } else if (NKikimr::NUdf::GetDataTypeInfo(slot).Features &
                    (NKikimr::NUdf::EDataTypeFeatures::DateType | NKikimr::NUdf::EDataTypeFeatures::TimeIntervalType)) {
                    T data;
                    isValid = TryFromString(atomNode.Content(), data)
                        && NKikimr::NMiniKQL::IsValidValue(slot, NKikimr::NUdf::TUnboxedValuePod(data));
                }
            }

            if (slot == NKikimr::NUdf::EDataSlot::Bool && isValid && atomNode.Content() != "true" && atomNode.Content() != "false") {
                bool value = AsciiEqualsIgnoreCase(atomNode.Content(), "true") || atomNode.Content() == "1";
                plainValue = value ? "true" : "false";
            }
        }

        if (!isValid) {
            ctx.AddError(TIssue(ctx.GetPosition(atomNode.Pos()), TStringBuilder() << "Bad atom format for type: "
                << type << ", value: " << TString(atomNode.Content()).Quote()));
        }

        return isValid;
    }

    bool EnsureNotInDiscoveryMode(const TExprNode& input, TExtContext& ctx) {
        if (ctx.Types.DiscoveryMode && ctx.Types.EvaluationInProgress) {
            ctx.Expr.AddError(YqlIssue(ctx.Expr.GetPosition(input.Pos()), TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY,
                TStringBuilder() << input.Content() << " is not allowed in Discovery mode"));
            return false;
        }
        return true;
    }

    TMaybe<TString> SerializeTzComponents(bool isValid, ui64 value, ui16 tzId) {
        if (isValid) {
            return ToString(value) + "," + ToString(*NKikimr::NMiniKQL::FindTimezoneIANAName(tzId));
        }
        return {};
    }

    template <typename T>
    bool IsValidTzData(TExprNode& atomNode, const TStringBuf& type, TExprContext& ctx, NKikimr::NUdf::EDataSlot slot, TMaybe<TString>& plainValue) {
        plainValue = {};
        bool isValid;
        if (atomNode.Flags() & TNodeFlags::BinaryContent) {
            // just deserialize
            switch (sizeof(T)) {
            case sizeof(ui16): {
                ui16 value;
                ui16 tzId;
                isValid = NKikimr::NMiniKQL::DeserializeTzDate(atomNode.Content(), value, tzId);
                plainValue = SerializeTzComponents(isValid, value, tzId);
                break;
            }
            case sizeof(ui32): {
                ui32 value;
                ui16 tzId;
                isValid = NKikimr::NMiniKQL::DeserializeTzDatetime(atomNode.Content(), value, tzId);
                plainValue = SerializeTzComponents(isValid, value, tzId);
                break;
            }
            case sizeof(ui64): {
                ui64 value;
                ui16 tzId;
                isValid = NKikimr::NMiniKQL::DeserializeTzTimestamp(atomNode.Content(), value, tzId);
                plainValue = SerializeTzComponents(isValid, value, tzId);
                break;
            }
            }
        } else {
            TStringBuf atom = atomNode.Content();
            TStringBuf value;
            GetNext(atom, ',', value);
            T data;
            isValid = TryFromString(value, data)
                && NKikimr::NMiniKQL::IsValidValue(slot, NKikimr::NUdf::TUnboxedValuePod(data));
            isValid = isValid && NKikimr::NMiniKQL::FindTimezoneId(atom).Defined();
        }

        if (!isValid) {
            ctx.AddError(TIssue(ctx.GetPosition(atomNode.Pos()), TStringBuilder() << "Bad atom format for type: "
                << type << ", value: " << TString(atomNode.Content()).Quote()));
        }

        return isValid;
    }

    // TODO: Use ExpandType
    TExprNode::TPtr MakeNothingData(TExprContext& ctx, TPositionHandle pos, TStringBuf data) {
        return ctx.Builder(pos)
            .Callable("Nothing")
                .Callable(0, "OptionalType")
                    .Callable(0, "DataType")
                        .Atom(0, data, TNodeFlags::Default)
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    std::pair<TExprNode::TPtr, const TTypeAnnotationNode*> MakeRepr(const TExprNode::TPtr& input, const TTypeAnnotationNode* type, TExprContext& ctx) {
        if (!type) {
            return {nullptr, nullptr};
        }

        switch (type->GetKind()) {
        case ETypeAnnotationKind::Data:
        case ETypeAnnotationKind::Void:
        case ETypeAnnotationKind::Null:
        case ETypeAnnotationKind::EmptyList:
        case ETypeAnnotationKind::EmptyDict:
            return { input, type };

        case ETypeAnnotationKind::Optional: {
            auto optionalType = type->Cast<TOptionalExprType>();
            auto arg = ctx.NewArgument(input->Pos(), "x");
            auto inner = MakeRepr(arg, optionalType->GetItemType(), ctx);
            if (!inner.first) {
                return { nullptr, nullptr };
            }

            return { ctx.NewCallable(input->Pos(), "Map", {
                input,
                ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), { arg }), std::move(inner.first))
            }), ctx.MakeType<TOptionalExprType>(inner.second) };
        }

        case ETypeAnnotationKind::List: {
            auto optionalType = type->Cast<TListExprType>();
            auto arg = ctx.NewArgument(input->Pos(), "x");
            auto inner = MakeRepr(arg, optionalType->GetItemType(), ctx);
            if (!inner.first) {
                return { nullptr, nullptr };
            }

            return { ctx.NewCallable(input->Pos(), "OrderedMap", {
                input,
                ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), { arg }), std::move(inner.first))
                }), ctx.MakeType<TListExprType>(inner.second) };
        }

        case ETypeAnnotationKind::Struct: {
            auto structType = type->Cast<TStructExprType>();
            TExprNodeList items;
            TVector<const TItemExprType*> typeItems;
            for (const auto& x : structType->GetItems()) {
                auto name = ctx.NewAtom(input->Pos(), x->GetName());
                auto item = MakeRepr(ctx.NewCallable(input->Pos(), "Member", { input, name }), x->GetItemType(), ctx);
                if (!item.first) {
                    return { nullptr, nullptr };
                }

                items.push_back(ctx.NewList(input->Pos(), { name, item.first }));
                typeItems.push_back(ctx.MakeType<TItemExprType>(x->GetName(), item.second));
            }

            auto resultType = ctx.MakeType<TStructExprType>(typeItems);
            return { ctx.NewCallable(input->Pos(), "AsStruct", std::move(items)), resultType };
        }

        case ETypeAnnotationKind::Tuple: {
            auto tupleType = type->Cast<TTupleExprType>();
            TExprNodeList items;
            TVector<const TTypeAnnotationNode*> typeItems;
            ui32 index = 0;
            for (const auto& x : tupleType->GetItems()) {
                auto name = ctx.NewAtom(input->Pos(), ToString(index), TNodeFlags::Default);
                auto item = MakeRepr(ctx.NewCallable(input->Pos(), "Nth", { input, name }), x, ctx);
                if (!item.first) {
                    return { nullptr, nullptr };
                }

                items.push_back(item.first);
                typeItems.push_back(item.second);
                ++index;
            }

            auto resultType = ctx.MakeType<TTupleExprType>(typeItems);
            return { ctx.NewList(input->Pos(), std::move(items)), resultType };
        }

        case ETypeAnnotationKind::Tagged: {
            auto taggedType = type->Cast<TTaggedExprType>();
            auto name = ctx.NewAtom(input->Pos(), taggedType->GetTag());
            auto item = MakeRepr(ctx.NewCallable(input->Pos(), "Untag", { input, name }), taggedType->GetBaseType(), ctx);
            if (!item.first) {
                return { nullptr, nullptr };
            }

            auto resultType = ctx.MakeType<TTaggedExprType>(item.second, taggedType->GetTag());
            return { ctx.NewCallable(input->Pos(), "AsTagged", { item.first, name }), resultType };
        }

        case ETypeAnnotationKind::Dict: {
            auto dictType = type->Cast<TDictExprType>();
            auto arg = ctx.NewArgument(input->Pos(), "x");
            auto tuple0 = ctx.NewCallable(input->Pos(), "Nth", { arg, ctx.NewAtom(input->Pos(), "0", TNodeFlags::Default) });
            auto tuple1 = ctx.NewCallable(input->Pos(), "Nth", { arg, ctx.NewAtom(input->Pos(), "1", TNodeFlags::Default) });
            auto inner = MakeRepr(tuple1, dictType->GetPayloadType(), ctx);
            if (!inner.first) {
                return { nullptr, nullptr };
            }

            auto body = ctx.NewList(input->Pos(), { tuple0, inner.first });
            auto mapped = ctx.NewCallable(input->Pos(), "Map", {
                ctx.NewCallable(input->Pos(), "DictItems", { input }),
                ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), { arg }), std::move(body))
                });

            return { ctx.Builder(input->Pos())
                .Callable("ToDict")
                    .Add(0, mapped)
                    .Lambda(1)
                        .Param("x")
                        .Callable("Nth")
                            .Arg(0, "x")
                            .Atom(1, "0", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Lambda(2)
                        .Param("x")
                        .Callable("Nth")
                            .Arg(0, "x")
                            .Atom(1, "1", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .List(3)
                        .Atom(0, "One", TNodeFlags::Default)
                        .Atom(1, "Hashed", TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Build(), ctx.MakeType<TDictExprType>(dictType->GetKeyType(), inner.second) };
        }

        case ETypeAnnotationKind::Resource: {
            auto resType = type->Cast<TResourceExprType>();
            if (resType->GetTag() == "Yson2.Node") {
                return { ctx.Builder(input->Pos())
                    .Callable("Apply")
                        .Callable(0, "Udf")
                            .Atom(0, "Yson2.Serialize", TNodeFlags::Default)
                        .Seal()
                        .Add(1, input)
                    .Seal()
                    .Build(), ctx.MakeType<TDataExprType>(EDataSlot::Yson) };
            }

            if (resType->GetTag() == "Yson.Node") {
                return { ctx.Builder(input->Pos())
                    .Callable("Apply")
                        .Callable(0, "Udf")
                            .Atom(0, "Yson.Serialize", TNodeFlags::Default)
                        .Seal()
                        .Add(1, input)
                    .Seal()
                    .Build(), ctx.MakeType<TDataExprType>(EDataSlot::Yson) };
            }

            if (resType->GetTag() == "DateTime2.TM") {
                return { ctx.Builder(input->Pos())
                    .Callable("Apply")
                        .Callable(0, "Udf")
                            .Atom(0, "DateTime2.MakeTzTimestamp", TNodeFlags::Default)
                        .Seal()
                        .Add(1, input)
                    .Seal()
                    .Build(), ctx.MakeType<TDataExprType>(EDataSlot::TzTimestamp) };
            }

            if (resType->GetTag() == "JsonNode") {
                return { ctx.Builder(input->Pos())
                    .Callable("Apply")
                        .Callable(0, "Udf")
                            .Atom(0, "Json2.Serialize", TNodeFlags::Default)
                        .Seal()
                        .Add(1, input)
                    .Seal()
                    .Build(), ctx.MakeType<TDataExprType>(EDataSlot::Json) };
            }

            return { nullptr, nullptr };
        }

        case ETypeAnnotationKind::Variant: {
            TExprNodeList visitArgs;
            visitArgs.push_back(input);
            auto varType = type->Cast<TVariantExprType>();
            auto underlyingType = varType->GetUnderlyingType();
            const TTypeAnnotationNode* newVType = nullptr;
            TExprNode::TPtr newVTypeNode;
            if (underlyingType->GetKind() == ETypeAnnotationKind::Tuple) {
                auto tupleType = underlyingType->Cast<TTupleExprType>();
                TVector<const TTypeAnnotationNode*> typeItems;
                for (ui32 pass = 0; pass < 2; ++pass) {
                    for (ui32 i = 0; i < tupleType->GetSize(); ++i) {
                        auto arg = ctx.NewArgument(input->Pos(), "x");
                        auto inner = MakeRepr(arg, tupleType->GetItems()[i], ctx);
                        if (!inner.first) {
                            return { nullptr, nullptr };
                        }

                        if (pass == 0) {
                            typeItems.push_back(inner.second);
                        } else {
                            auto name = ctx.NewAtom(input->Pos(), ToString(i));
                            auto body = ctx.NewCallable(input->Pos(), "Variant", { inner.first, name, newVTypeNode });
                            visitArgs.push_back(name);
                            visitArgs.push_back(ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), { arg }), std::move(body)));
                        }
                    }

                    if (pass == 0) {
                        newVType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(typeItems));
                        newVTypeNode = ExpandType(input->Pos(), *newVType, ctx);
                    }
                }
            } else {
                auto structType = underlyingType->Cast<TStructExprType>();
                TVector<const TItemExprType*> typeItems;
                for (ui32 pass = 0; pass < 2; ++pass) {
                    for (ui32 i = 0; i < structType->GetSize(); ++i) {
                        auto arg = ctx.NewArgument(input->Pos(), "x");
                        auto inner = MakeRepr(arg, structType->GetItems()[i]->GetItemType(), ctx);
                        if (!inner.first) {
                            return { nullptr, nullptr };
                        }

                        if (pass == 0) {
                            typeItems.push_back(ctx.MakeType<TItemExprType>(structType->GetItems()[i]->GetName(), inner.second));
                        } else {
                            auto name = ctx.NewAtom(input->Pos(), structType->GetItems()[i]->GetName());
                            auto body = ctx.NewCallable(input->Pos(), "Variant", { inner.first, name, newVTypeNode });
                            visitArgs.push_back(name);
                            visitArgs.push_back(ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), { arg }), std::move(body)));
                        }
                    }

                    if (pass == 0) {
                        newVType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TStructExprType>(typeItems));
                        newVTypeNode = ExpandType(input->Pos(), *newVType, ctx);
                    }
                }
            }

            return { ctx.NewCallable(input->Pos(), "Visit", std::move(visitArgs)), newVType };
        }

        default:
            return { nullptr, nullptr };
        }
    }

    TContext::TContext(TExprContext& expr)
        : Expr(expr)
        {}

    TExtContext::TExtContext(TExprContext& expr, TTypeAnnotationContext& types)
        : TContext(expr)
        , Types(types) {}

    bool TExtContext::LoadUdfMetadata(const TVector<IUdfResolver::TFunction*>& functions) {
        TVector<IUdfResolver::TImport*> imports;
        imports.reserve(Types.UdfImports.size());
        for (auto& x : Types.UdfImports) {
            imports.push_back(&x.second);
        }

        if (!Types.UdfResolver->LoadMetadata(imports, functions, Expr)) {
            return false;
        }

        for (auto& import : imports) {
            RegisterResolvedImport(*import);
        }

        return true;
    }

    void TExtContext::RegisterResolvedImport(const IUdfResolver::TImport& import) {
        YQL_ENSURE(import.Modules);
        for (auto& m : *import.Modules) {
            auto p = Types.UdfModules.emplace(m, import.FileAlias);
            // rework this place when user tries to override another module
            if (!p.second && p.first->second != import.FileAlias) {
                ythrow yexception() << "Module name duplicated : module = " << m << ", existing alias = " << p.first->second << ", new alis = " << import.FileAlias;
            }
        }
    }

    bool EnsureJsonQueryFunction(const NNodes::TCoJsonQueryBase& function, TContext& ctx) {
        // first argument must be "Json", "Json?", "JsonDocument" or "JsonDocument?" type
        const auto& jsonArg = function.Json().Ref();
        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(jsonArg, isOptional, dataType, ctx.Expr)) {
            return false;
        }

        if (dataType->GetSlot() != EDataSlot::Json && dataType->GetSlot() != EDataSlot::JsonDocument) {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(jsonArg.Pos()),
                TStringBuilder() << "Expected Json, Json?, JsonDocument or JsonDocument?, but got: " << *jsonArg.GetTypeAnn()
            ));
            return false;
        }

        // second argument must be "Utf8" type
        const auto& jsonPathArg = function.JsonPath().Ref();
        if (!EnsureSpecificDataType(jsonPathArg, EDataSlot::Utf8, ctx.Expr)) {
            return false;
        }

        // third argument must be "Dict" type
        const auto& variablesArg = function.Variables().Ref();
        if (!variablesArg.GetTypeAnn()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(variablesArg.Pos()), "Expected dict, but got lambda"));
            return false;
        }

        if (variablesArg.GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyDict) {
            return true;
        }

        if (!EnsureDictType(variablesArg, ctx.Expr)) {
            return false;
        }

        const TDictExprType* dictType = variablesArg.GetTypeAnn()->Cast<TDictExprType>();

        if (!EnsureSpecificDataType(variablesArg.Pos(), *dictType->GetKeyType(), EDataSlot::Utf8, ctx.Expr)) {
            return false;
        }

        const auto* payloadType = dictType->GetPayloadType();
        if (payloadType->GetKind() != ETypeAnnotationKind::Resource
            || payloadType->Cast<TResourceExprType>()->GetTag() != "JsonNode") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(variablesArg.Pos()), TStringBuilder() << "Dict payload type must be Resource<'JsonNode'>, not " << *payloadType));
            return false;
        }

        return true;
    }

    typedef std::function<IGraphTransformer::TStatus(const TExprNode::TPtr&, TExprNode::TPtr&, TContext& ctx)>
        TAnnotationFunc;

    typedef std::function<IGraphTransformer::TStatus(const TExprNode::TPtr&, TExprNode::TPtr&, TExtContext& ctx)>
        TExtendedAnnotationFunc;

    struct TSyncFunctionsMap {
        TSyncFunctionsMap();

        THashMap<TString, TAnnotationFunc> Functions;
        THashMap<TString, TExtendedAnnotationFunc> ExtFunctions;
        THashMap<TString, TExtendedAnnotationFunc> ColumnOrderFunctions;
        THashMap<TString, ETypeAnnotationKind> TypeKinds;
        THashSet<TString> AllNames;

        static const TSyncFunctionsMap& Instance() {
            return *Singleton<TSyncFunctionsMap>();
        }
    };

    IGraphTransformer::TStatus DataSourceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DataWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureDataType(input->Head().Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.NewCallable(input->Pos(), type->Cast<TDataExprType>()->GetName(), { input->ChildPtr(1) });
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus DataConstructorWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (input->Content() == "Decimal") {
            if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!NYql::NDecimal::IsValid(input->Head().Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Bad atom format for type: "
                    << input->Content() << ", value: " << TString{input->Head().Content()}.Quote()));

                return IGraphTransformer::TStatus::Error;
            }

            input->SetTypeAnn(ctx.Expr.MakeType<TDataExprParamsType>(EDataSlot::Decimal, input->Child(1)->Content(), input->Child(2)->Content()));
            if (!input->GetTypeAnn()->Cast<TDataExprParamsType>()->Validate(input->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            return IGraphTransformer::TStatus::Ok;
        } else {
            if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            TMaybe<TString> textValue;
            if (input->Content() == "Bool") {
                if (!IsValidSmallData<bool>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Bool, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Uint8") {
                if (!IsValidSmallData<ui8>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Uint8, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Int8") {
                if (!IsValidSmallData<i8>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Int8, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Int16") {
                if (!IsValidSmallData<i16>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Int16, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Uint16") {
                if (!IsValidSmallData<ui16>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Uint16, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Int32") {
                if (!IsValidSmallData<i32>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Int32, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Uint32") {
                if (!IsValidSmallData<ui32>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Uint32, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Int64") {
                if (!IsValidSmallData<i64>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Int64, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Uint64") {
                if (!IsValidSmallData<ui64>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Uint64, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Float") {
                if (!IsValidSmallData<float>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Float, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Double") {
                if (!IsValidSmallData<double>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Double, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Yson") {
                if (!NDom::IsValidYson(input->Head().Content())) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Bad atom format for type: "
                        << input->Content() << ", value: " << TString(input->Head().Content()).Quote()));

                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Json") {
                if (!NDom::IsValidJson(input->Head().Content())) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Bad atom format for type: "
                        << input->Content() << ", value: " << TString(input->Head().Content()).Quote()));

                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Utf8") {
                if (!IsUtf8(input->Head().Content())) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Bad atom format for type: "
                        << input->Content() << ", value: " << TString(input->Head().Content()).Quote()));

                    return IGraphTransformer::TStatus::Error;
                }
            }
            else if (input->Content() == "String") {
                // nothing to do
            }
            else if (input->Content() == "Date") {
                if (!IsValidSmallData<ui16>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Date, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }
            else if (input->Content() == "Datetime") {
                if (!IsValidSmallData<ui32>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Datetime, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }
            else if (input->Content() == "Timestamp") {
                if (!IsValidSmallData<ui64>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Timestamp, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Interval") {
                if (!IsValidSmallData<i64>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Interval, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "TzDate") {
                if (!IsValidTzData<ui16>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Date, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "TzDatetime") {
                if (!IsValidTzData<ui32>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Datetime, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "TzTimestamp") {
                if (!IsValidTzData<ui64>(input->Head(), input->Content(), ctx.Expr, NKikimr::NUdf::EDataSlot::Timestamp, textValue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "Uuid") {
                if (input->Head().Content().size() != 16) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Bad atom format for type: "
                        << input->Content() << ", value: " << TString(input->Head().Content()).Quote()));

                    return IGraphTransformer::TStatus::Error;
                }
            } else if (input->Content() == "JsonDocument") {
                // check will be performed in JsonDocument callable
            } else if (input->Content() == "DyNumber") {
                if (!NKikimr::NMiniKQL::IsValidStringValue(EDataSlot::DyNumber, input->Head().Content())) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Bad atom format for type: "
                        << input->Content() << ", value: " << TString(input->Head().Content()).Quote()));

                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                ythrow yexception() << "Unknown data type: " << input->Content();
            }

            if (textValue) {
                // need to replace binary arg with text one
                output = ctx.Expr.Builder(input->Pos())
                    .Callable(input->Content())
                        .Atom(0, *textValue)
                    .Seal()
                    .Build();
                return IGraphTransformer::TStatus::Repeat;
            }

            input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(NKikimr::NUdf::GetDataSlot(input->Content())));
            return IGraphTransformer::TStatus::Ok;
        }
    }

    IGraphTransformer::TStatus KeyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        for (auto& child : input->Children()) {
            if (!EnsureTuple(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (child->ChildrenSize() != 1 && child->ChildrenSize() != 2) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected tuple of size 1 or 2, but got: " <<
                    input->ChildrenSize()));
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(*child->Child(0), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (child->ChildrenSize() == 2) {
                if (child->Head().Content() == TStringBuf("epoch") || child->Head().Content() == TStringBuf("commitEpoch")) {
                    if (!EnsureAtom(*child->Child(1), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else {
                    if (!EnsureComposable(*child->Child(1), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus LeftWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleTypeSize(input->Head(), 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto worldType = input->Head().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[0];
        if (worldType->GetKind() != ETypeAnnotationKind::World) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected world type as type of first tuple element, but got: "
                << *worldType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(worldType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RightWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleTypeSize(input->Head(), 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto worldType = input->Head().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[0];
        if (worldType->GetKind() != ETypeAnnotationKind::World) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected world type as type of first tuple element, but got: "
                << *worldType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ConsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureWorldType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Head().GetTypeAnn(),
            input->Tail().GetTypeAnn()
        }));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DataSinkWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    TMaybe<ui32> FindOrReportMissingMember(TStringBuf memberName, const TStructExprType& structType, TString& errStr)
    {
        auto result = structType.FindItem(memberName);
        if (!result) {
            TStringBuilder sb;
            sb << "Member not found: " << memberName;
            auto mistype = structType.FindMistype(memberName);
            if (mistype) {
                YQL_ENSURE(mistype.GetRef() != memberName);
                sb << ". Did you mean " << mistype.GetRef() << "?";
            } else {
                auto dotPos = memberName.find_first_of('.');
                if (dotPos != TStringBuf::npos) {
                    TStringBuf rest = memberName.SubStr(dotPos + 1);
                    if (rest && structType.FindItem(rest)) {
                        sb << ". Did you mean " << rest << "?";
                    }
                }
            }
            errStr = sb;
        }
        return result;
    }

    TMaybe<ui32> FindOrReportMissingMember(TStringBuf memberName, TPositionHandle pos, const TStructExprType& structType, TContext& ctx) {
        TString errStr;
        auto result = FindOrReportMissingMember(memberName, structType, errStr);
        if (!result) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(pos), errStr));
        }
        return result;
    }

    IGraphTransformer::TStatus MemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TStructExprType* structType;
        bool isOptional;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            structType = itemType->Cast<TStructExprType>();
            isOptional = true;
        }
        else {
            if (!EnsureStructType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
            isOptional = false;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto memberName = input->Tail().Content();
        auto pos = FindOrReportMissingMember(memberName, input->Pos(), *structType, ctx);
        if (!pos) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(structType->GetItems()[*pos]->GetItemType());
        if (isOptional && input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional && input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Null) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SingleMemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto arg = input->HeadPtr();

        if (IsNull(*arg)) {
            output = arg;
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* item = arg->GetTypeAnn();
        if (item->GetKind() == ETypeAnnotationKind::Optional) {
            item = item->Cast<TOptionalExprType>()->GetItemType();
        }

        if (!EnsureStructType(arg->Pos(), *item, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = item->Cast<TStructExprType>();

        if (structType->GetSize() != 1) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expecting single member struct, but got: " << *item));
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("Member")
                .Add(0, arg)
                .Atom(1, structType->GetItems()[0]->GetName())
            .Seal()
            .Build();
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SqlColumnWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        const bool columnOrType = input->IsCallable({"SqlColumnOrType", "SqlPlainColumnOrType"});
        const bool isPlain = input->IsCallable({"SqlPlainColumn", "SqlPlainColumnOrType"});
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, isPlain ? 2 : 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            if (columnOrType) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                    TStringBuilder() << "Expected (optional) struct type, but got Null"));
                return IGraphTransformer::TStatus::Error;
            }
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto rowNode = input->Child(0);
        const auto columnNameNode = input->Child(1);
        TExprNode::TPtr sourceNameNode;
        if (input->ChildrenSize() > 2) {
            sourceNameNode = input->ChildPtr(2);
        }

        if (!EnsureStructOrOptionalStructType(*rowNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const TStructExprType* structType = RemoveAllOptionals(rowNode->GetTypeAnn())->Cast<TStructExprType>();

        if (!EnsureAtom(*columnNameNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (sourceNameNode && !EnsureAtom(*sourceNameNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto columnName = columnNameNode->Content();
        const auto sourceName = sourceNameNode ? sourceNameNode->Content() : "";

        TString effectiveColumnName;
        if (sourceName) {
            effectiveColumnName = DotJoin(sourceName, columnName);
        } else if (isPlain) {
            effectiveColumnName = ToString(columnName);
        } else {
            TStringBuf goalItemName;
            for (auto& item: structType->GetItems()) {
                const auto& itemName = item->GetName();
                TStringBuf columnItemName;
                auto dotPos = itemName.find_first_of('.');
                if (dotPos == TStringBuf::npos) {
                    columnItemName = itemName;
                } else {
                    columnItemName = itemName.SubStr(dotPos + 1);
                }
                if (columnItemName == columnName) {
                    if (goalItemName) {
                        auto issue = TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "column name: " <<
                            columnName << " conflicted without correlation name it may be one of: " <<
                            goalItemName << ", " << itemName);
                        if (columnOrType) {
                            input->SetTypeAnn(ctx.Expr.MakeType<TErrorExprType>(issue));
                            return IGraphTransformer::TStatus::Ok;
                        }
                        ctx.Expr.AddError(issue);
                        return IGraphTransformer::TStatus::Error;
                    } else {
                        goalItemName = itemName;
                    }
                }
            }
            effectiveColumnName = goalItemName ? goalItemName : columnName;
        }

        TString errStr;
        auto pos = FindOrReportMissingMember(effectiveColumnName, *structType, errStr);
        if (columnOrType) {
            if (!pos) {
                input->SetTypeAnn(ctx.Expr.MakeType<TErrorExprType>(TIssue(ctx.Expr.GetPosition(input->Pos()), errStr)));
                return IGraphTransformer::TStatus::Ok;
            }
            output = ctx.Expr.Builder(input->Pos())
                .Callable("SqlColumnFromType")
                    .Add(0, input->HeadPtr())
                    .Atom(1, effectiveColumnName)
                    .Add(2, input->ChildPtr(1)) // original column/type name
                .Seal()
                .Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!pos) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), errStr));
            return IGraphTransformer::TStatus::Error;
        }
        output = ctx.Expr.Builder(input->Pos())
            .Callable("Member")
                .Add(0, std::move(rowNode))
                .Atom(1, structType->GetItems()[*pos]->GetName())
            .Seal().Build();
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SqlColumnFromTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructOrOptionalStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto columnNameNode = input->Child(1);
        if (!EnsureAtom(*columnNameNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto originalTypeNode = input->Child(2);
        if (!EnsureAtom(*originalTypeNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional = input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional;
        const TStructExprType& structType = *RemoveAllOptionals(input->Head().GetTypeAnn())->Cast<TStructExprType>();

        auto pos = FindOrReportMissingMember(columnNameNode->Content(), input->Pos(), structType, ctx);
        if (!pos) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = structType.GetItems()[*pos]->GetItemType();
        if (isOptional && resultType->GetKind() != ETypeAnnotationKind::Optional && resultType->GetKind() != ETypeAnnotationKind::Null) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn());
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TryMemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TStructExprType* structType;
        bool isStructOptional;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            isStructOptional = true;
            structType = itemType->Cast<TStructExprType>();
        } else {
            if (!EnsureStructType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
            isStructOptional = false;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto otherType = input->Child(2)->GetTypeAnn();
        const bool isOptional = otherType->GetKind() == ETypeAnnotationKind::Optional;
        auto memberName = input->Child(1)->Content();
        const TTypeAnnotationNode* foundFieldType = otherType;
        bool fieldFound = false;
        for (auto& field : structType->GetItems()) {
            if (field->GetName() == memberName) {
                fieldFound = true;
                auto fieldType = field->GetItemType();
                if (otherType->GetKind() == ETypeAnnotationKind::Null) {
                    foundFieldType = fieldType;
                    break;
                }

                if (IsSameAnnotation(*otherType, *fieldType) ||
                    isOptional && IsSameAnnotation(*otherType, *ctx.Expr.MakeType<TOptionalExprType>(fieldType))) {
                    break;
                }
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Mismatch member '" << memberName
                    << "' type, expected: " << *otherType << ", got: " << *fieldType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!fieldFound && otherType->GetKind() == ETypeAnnotationKind::Null) {
            output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (isStructOptional && foundFieldType->GetKind() != ETypeAnnotationKind::Optional) {
            foundFieldType = ctx.Expr.MakeType<TOptionalExprType>(foundFieldType);
        }

        input->SetTypeAnn(foundFieldType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FlattenMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        TVector<const TItemExprType*> allItems;
        for (auto& child : input->Children()) {
            if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto prefix = child->Child(0);
            if (!EnsureAtom(*prefix, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto structObj = child->Child(1);
            if (!EnsureStructOrOptionalStructType(*structObj, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto type = structObj->GetTypeAnn();
            const bool optional = type->GetKind() == ETypeAnnotationKind::Optional;
            if (optional) {
                type = type->Cast<TOptionalExprType>()->GetItemType();
            }
            for (auto& field: type->Cast<TStructExprType>()->GetItems()) {
                auto itemType = field->GetItemType();
                if (optional && itemType->GetKind() != ETypeAnnotationKind::Optional && itemType->GetKind() != ETypeAnnotationKind::Null) {
                    itemType = ctx.Expr.MakeType<TOptionalExprType>(itemType);
                }
                auto newField = ctx.Expr.MakeType<TItemExprType>(
                    TString::Join(prefix->Content(), field->GetName()),
                    itemType
                );
                allItems.push_back(newField);
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(allItems));
        if (!input->GetTypeAnn()->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FlattenStructsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<const TItemExprType*> allItems;
        if (!EnsureStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
        for (auto& x : structType->GetItems()) {
            bool isOptional = false;
            auto itemType = x->GetItemType();
            if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
                isOptional = true;
                itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
            }

            if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
                allItems.push_back(x);
                continue;
            }

            auto subStructType = itemType->Cast<TStructExprType>();
            for (const auto& y : subStructType->GetItems()) {
                if (!isOptional || y->GetItemType()->GetKind() == ETypeAnnotationKind::Optional) {
                    allItems.push_back(y);
                } else {
                    auto newItem = ctx.Expr.MakeType<TItemExprType>(y->GetName(), ctx.Expr.MakeType<TOptionalExprType>(y->GetItemType()));
                    allItems.push_back(newItem);
                }
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(allItems));
        if (!input->GetTypeAnn()->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NormalizeAtomListForDiveOrSelect(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        YQL_ENSURE(input->IsCallable({"DivePrefixMembers", "SelectMembers", "FilterMembers", "RemovePrefixMembers"}));

        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto prefixes = input->Child(1);
        if (!EnsureTuple(*prefixes, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (auto& node : prefixes->Children()) {
            if (!EnsureAtom(*node, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto descending = [](const TExprNode::TPtr& left, const TExprNode::TPtr& right) {
            return left->Content() > right->Content();
        };

        if (!IsSorted(prefixes->Children().begin(), prefixes->Children().end(), descending)) {
            auto list = prefixes->ChildrenList();
            Sort(list, descending);

            output = ctx.Expr.Builder(input->Pos())
                .Callable(input->Content())
                    .Add(0, input->HeadPtr())
                    .Add(1, ctx.Expr.NewList(prefixes->Pos(), std::move(list)))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    template<bool ByPrefix>
    IGraphTransformer::TStatus SelectMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structObj = input->Child(0);
        if (!EnsureStructOrOptionalStructType(*structObj, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = NormalizeAtomListForDiveOrSelect(input, output, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = structObj->GetTypeAnn();
        const bool optional = type->GetKind() == ETypeAnnotationKind::Optional;
        if (optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }
        TVector<const TItemExprType*> allItems;
        auto structExprType = type->Cast<TStructExprType>();
        for (auto& field: structExprType->GetItems()) {
            const auto& fieldName = field->GetName();
            auto prefixes = input->Child(1);
            for (const auto& prefixNode: prefixes->Children()) {
                const auto& prefix = prefixNode->Content();
                if (ByPrefix ? fieldName.StartsWith(prefix) : fieldName == prefix) {
                    allItems.push_back(field);
                    break;
                }
            }
        }

        if (allItems.size() < structExprType->GetSize()) {
            input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(allItems));
        } else {
            input->SetTypeAnn(structExprType);
        }
        if (!input->GetTypeAnn()->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DivePrefixMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structObj = input->Child(0);
        if (!EnsureStructOrOptionalStructType(*structObj, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = NormalizeAtomListForDiveOrSelect(input, output, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = structObj->GetTypeAnn();
        const bool optional = type->GetKind() == ETypeAnnotationKind::Optional;
        if (optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }
        TVector<const TItemExprType*> allItems;
        auto structExprType = type->Cast<TStructExprType>();
        for (auto& field: structExprType->GetItems()) {
            const auto& fieldName = field->GetName();
            auto prefixes = input->Child(1);
            for (const auto& prefixNode: prefixes->Children()) {
                const auto& prefix = prefixNode->Content();
                if (fieldName.StartsWith(prefix)) {
                    auto itemType = field->GetItemType();
                    if (optional && itemType->GetKind() !=  ETypeAnnotationKind::Optional) {
                        itemType = ctx.Expr.MakeType<TOptionalExprType>(itemType);
                    }
                    auto newField = ctx.Expr.MakeType<TItemExprType>(fieldName.substr(prefix.length()), itemType);
                    allItems.push_back(newField);
                    break;
                }
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(allItems));
        if (!input->GetTypeAnn()->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FlattenByColumns(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto iter = input->Children().begin();
        TString mode = "auto";
        if ((*iter)->IsAtom()) {
            mode = (*iter)->Content();
            if (mode != "auto" && mode != "optional" && mode != "list" && mode != "dict") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition((*iter)->Pos()), TStringBuilder() << "Unsupported flatten by mode: " << mode));
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            ++iter;
        }

        auto structObj = *iter;
        if (!EnsureStructOrOptionalStructType(*structObj, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        TSet<TString> aliases;
        TMap<TString, TString> flattenByColumns;
        for (++iter; iter != input->Children().end(); ++iter) {
            const auto& child = *iter;
            TString alias;
            const auto& argType = child->Type();
            if (argType != TExprNode::List && argType != TExprNode::Atom) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Expected atom or tuple, but got: " << argType));
                return IGraphTransformer::TStatus::Error;
            }
            TExprNode* columnNameNode = child.Get();
            if (argType == TExprNode::List) {
                if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                columnNameNode = child->Child(0);
                auto aliasNode = child->Child(1);
                if (!EnsureAtom(*aliasNode, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                alias = aliasNode->Content();
                if (!aliases.emplace(alias).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(aliasNode->Pos()), TStringBuilder() <<
                        "Duplicate flatten alias found: " << alias));
                    return IGraphTransformer::TStatus::Error;
                }
                if (flattenByColumns.contains(alias)) { 
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(columnNameNode->Pos()), TStringBuilder() <<
                        "Collision between alias and column name: " << alias));
                    return IGraphTransformer::TStatus::Error;
                }
            }
            if (!EnsureAtom(*columnNameNode, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            auto columnName = columnNameNode->Content();
            if (!flattenByColumns.emplace(TString(columnName), alias).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(columnNameNode->Pos()), TStringBuilder() <<
                    "Duplicate flatten field found: " << columnName));
                return IGraphTransformer::TStatus::Error;
            }
            if (aliases.contains(columnName)) { 
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(columnNameNode->Pos()), TStringBuilder() <<
                    "Collision between alias and column name: " << columnName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto type = structObj->GetTypeAnn();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        bool allFieldOptional = true;
        TVector<const TItemExprType*> allItems;
        for (auto& field: type->Cast<TStructExprType>()->GetItems()) {
            const auto& fieldName = field->GetName();
            auto flattenIter = flattenByColumns.find(fieldName);
            if (flattenIter == flattenByColumns.end()) {
                if (aliases.contains(fieldName)) { 
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(structObj->Pos()), TStringBuilder() <<
                        "Conflict flatten alias and column name: '" << fieldName << "'"));
                    return IGraphTransformer::TStatus::Error;
                }
                /// left fields untouched by flatten
                allItems.push_back(field);
                continue;
            }
            const bool isAliasExists = !flattenIter->second.empty();
            if (isAliasExists) {
                /// left original fields if alias exists
                allItems.push_back(field);
            }
            const auto flattenItemName = isAliasExists ? flattenIter->second : fieldName;
            auto fieldType = field->GetItemType();
            const bool fieldOptional = fieldType->GetKind() == ETypeAnnotationKind::Optional;
            if (fieldOptional) {
                fieldType = fieldType->Cast<TOptionalExprType>()->GetItemType();
            } else {
                allFieldOptional = false;
            }

            if (mode == "optional" && !fieldOptional) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(structObj->Pos()), TStringBuilder() <<
                    "Expected optional type in field of struct: '" << fieldName <<
                    "', but got: " << *field->GetItemType()));
                return IGraphTransformer::TStatus::Error;
            }

            if (mode == "list" && RemoveOptionalType(field->GetItemType())->GetKind() != ETypeAnnotationKind::List) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(structObj->Pos()), TStringBuilder() <<
                    "Expected (optional) list type in field of struct: '" << fieldName <<
                    "', but got: " << *field->GetItemType()));
                return IGraphTransformer::TStatus::Error;
            }

            if (mode == "dict" && RemoveOptionalType(field->GetItemType())->GetKind() != ETypeAnnotationKind::Dict) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(structObj->Pos()), TStringBuilder() <<
                    "Expected (optional) dict type in field of struct: '" << fieldName <<
                    "', but got: " << *field->GetItemType()));
                return IGraphTransformer::TStatus::Error;
            }

            if (mode == "auto" && fieldOptional && RemoveOptionalType(field->GetItemType())->GetKind() == ETypeAnnotationKind::List) {
                auto issue = TIssue(ctx.Expr.GetPosition(structObj->Pos()), "Ambiguous FLATTEN BY statement, please choose FLATTEN LIST BY or FLATTEN OPTIONAL BY");
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_FLATTEN_BY_OPT, issue);
                if (!ctx.Expr.AddWarning(issue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            if (mode == "auto" && fieldOptional && RemoveOptionalType(field->GetItemType())->GetKind() == ETypeAnnotationKind::Dict) {
                auto issue = TIssue(ctx.Expr.GetPosition(structObj->Pos()), "Ambiguous FLATTEN BY statement, please choose FLATTEN DICT BY or FLATTEN OPTIONAL BY");
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_FLATTEN_BY_OPT, issue);
                if (!ctx.Expr.AddWarning(issue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            const TTypeAnnotationNode* flattenItemType = nullptr;
            if (fieldOptional) {
                flattenItemType = fieldType;
                if (mode == "list") {
                    auto listType = fieldType->Cast<TListExprType>();
                    flattenItemType = listType->GetItemType();
                    allFieldOptional = false;
                } else if (mode == "dict") {
                    auto dictType = fieldType->Cast<TDictExprType>();
                    const auto keyType = dictType->GetKeyType();
                    const auto payloadType = dictType->GetPayloadType();
                    flattenItemType = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType({ keyType, payloadType }));
                    allFieldOptional = false;
                }
            } else if (fieldType->GetKind() == ETypeAnnotationKind::List) {
                auto listType = fieldType->Cast<TListExprType>();
                flattenItemType = listType->GetItemType();
            } else if (fieldType->GetKind() == ETypeAnnotationKind::Dict) {
                auto dictType = fieldType->Cast<TDictExprType>();
                const auto keyType = dictType->GetKeyType();
                const auto payloadType = dictType->GetPayloadType();
                flattenItemType = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType({keyType, payloadType}));
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(structObj->Pos()), TStringBuilder() <<
                    "Expected list, dict or optional types in field of struct: '" << fieldName <<
                    "', but got: " << *field->GetItemType()));
                return IGraphTransformer::TStatus::Error;
            }

            allItems.push_back(ctx.Expr.MakeType<TItemExprType>(flattenItemName, flattenItemType));
            flattenByColumns.erase(flattenIter);
        }
        if (!flattenByColumns.empty()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(structObj->Pos()), TStringBuilder() <<
                "Column for flatten \"" << flattenByColumns.begin()->first << "\" does not exist"));
            return IGraphTransformer::TStatus::Error;
        }

        auto resultStruct = ctx.Expr.MakeType<TStructExprType>(allItems);
        if (!resultStruct->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (allFieldOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(resultStruct));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultStruct));
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NthWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTupleExprType* tupleType;
        bool isOptional;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureTupleType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            tupleType = itemType->Cast<TTupleExprType>();
            isOptional = true;
        }
        else {
            if (!EnsureTupleType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            tupleType = input->Head().GetTypeAnn()->Cast<TTupleExprType>();
            isOptional = false;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 index = 0;
        if (!TryFromString(input->Tail().Content(), index)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Tail().Content()));
            return IGraphTransformer::TStatus::Error;
        }

        if (index >= tupleType->GetSize()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Index out of range. Index: " <<
                index << ", size: " << tupleType->GetSize()));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputableType(input->Head().Pos(), *tupleType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(tupleType->GetItems()[index]);
        if (isOptional && input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional && input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Null) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AddMemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto memberName = input->Child(1)->Content();
        auto newField = ctx.Expr.MakeType<TItemExprType>(memberName, input->Child(2)->GetTypeAnn());
        auto newItems = input->Head().GetTypeAnn()->Cast<TStructExprType>()->GetItems();
        newItems.push_back(newField);
        input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(newItems));
        if (!input->GetTypeAnn()->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    template <bool Forced>
    IGraphTransformer::TStatus RemoveMemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
        auto memberName = input->Tail().Content();
        TVector<const TItemExprType*> newItems = structType->GetItems();
        EraseIf(newItems, [&](const auto& item) { return item->GetName() == memberName; });

        if (!Forced && !FindOrReportMissingMember(memberName, input->Pos(), *structType, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(newItems));
        if (!input->GetTypeAnn()->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RemovePrefixMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto& firstChild = input->Head();
        auto firstChildType = firstChild.GetTypeAnn();

        if (HasError(firstChildType, ctx.Expr) || !firstChildType) {
            YQL_ENSURE(firstChild.Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(firstChild.Pos()),
                TStringBuilder() << "Expected struct, variant, or sequence type, but got lambda"
            ));
            return IGraphTransformer::TStatus::Error;
        }

        auto status = NormalizeAtomListForDiveOrSelect(input, output, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const TTypeAnnotationNode* resultType = nullptr;
        bool isSequence = true;
        const TTypeAnnotationNode* itemType = GetItemType(*firstChildType);
        if (!itemType) {
            itemType = firstChildType;
            isSequence = false;
        }

        auto prefixes = input->Child(1);
        auto rebuildStructType = [&ctx, prefixes](const TTypeAnnotationNode* structType) {
            TVector<const TItemExprType*> newItems;
            for (auto& field : structType->Cast<TStructExprType>()->GetItems()) {
                if (!AnyOf(prefixes->Children(), [field](const auto& prefixNode) { return field->GetName().StartsWith(prefixNode->Content()); })) {
                    newItems.push_back(field);
                }
            }

            return ctx.Expr.MakeType<TStructExprType>(newItems);
        };

        if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
            resultType = rebuildStructType(itemType);
            if (resultType == itemType) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            }
            if (!resultType->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
            auto varType = itemType->Cast<TVariantExprType>();
            if (varType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
                auto tupleType = varType->GetUnderlyingType()->Cast<TTupleExprType>();
                TTypeAnnotationNode::TListType newTupleItems;
                for (size_t i = 0; i < tupleType->GetSize(); ++i) {
                    auto tupleItemType = tupleType->GetItems()[i];
                    if (tupleItemType->GetKind() != ETypeAnnotationKind::Struct) {
                        output = input->HeadPtr();
                        return IGraphTransformer::TStatus::Repeat;
                    }
                    newTupleItems.push_back(rebuildStructType(tupleItemType));
                }
                resultType = ctx.Expr.MakeType<TVariantExprType>(ctx.Expr.MakeType<TTupleExprType>(newTupleItems));
            } else {
                YQL_ENSURE(varType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Struct);
                auto structType = varType->GetUnderlyingType()->Cast<TStructExprType>();
                TVector<const TItemExprType*> newStructItems;
                for (size_t i = 0; i < structType->GetSize(); ++i) {
                    auto structItemType = structType->GetItems()[i];
                    if (structItemType->GetItemType()->GetKind() != ETypeAnnotationKind::Struct) {
                        output = input->HeadPtr();
                        return IGraphTransformer::TStatus::Repeat;
                    }
                    newStructItems.push_back(ctx.Expr.MakeType<TItemExprType>(structItemType->GetName(), rebuildStructType(structItemType->GetItemType())));
                }
                resultType = ctx.Expr.MakeType<TVariantExprType>(ctx.Expr.MakeType<TStructExprType>(newStructItems));
            }
            if (resultType == itemType) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            }
        } else {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (isSequence) {
            resultType = MakeSequenceType(firstChildType->GetKind(), *resultType, ctx.Expr);
        }
        input->SetTypeAnn(resultType);

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RemoveSystemMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto& firstChild = input->Head();
        auto firstChildType = firstChild.GetTypeAnn();

        if (HasError(firstChildType, ctx.Expr) || !firstChildType) {
            YQL_ENSURE(firstChild.Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(firstChild.Pos()),
                TStringBuilder() << "Expected struct, variant, or sequence type, but got lambda"
            ));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = GetItemType(*firstChildType);
        if (!itemType) {
            itemType = firstChildType;
        }
        switch (itemType->GetKind()) {
        case ETypeAnnotationKind::Variant: {
            auto varType = itemType->Cast<TVariantExprType>();
            if (varType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
                auto tupleType = varType->GetUnderlyingType()->Cast<TTupleExprType>();
                if (AnyOf(tupleType->GetItems(), [](auto tupleItemType) { return tupleItemType->GetKind() != ETypeAnnotationKind::Struct; })) {
                    output = input->HeadPtr();
                    break;
                }
            } else {
                auto structType = varType->GetUnderlyingType()->Cast<TStructExprType>();
                if (AnyOf(structType->GetItems(), [](auto structItemType) { return structItemType->GetItemType()->GetKind() != ETypeAnnotationKind::Struct; })) {
                    output = input->HeadPtr();
                    break;
                }
            }
            [[fallthrough]];
        }
        // passthrough with Struct
        case ETypeAnnotationKind::Struct:
            output = ctx.Expr.Builder(input->Pos())
                .Callable("RemovePrefixMembers")
                    .Add(0, input->HeadPtr())
                    .List(1)
                        .Atom(0, "_yql_", TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Build();
            break;
        default:
            output = input->HeadPtr();
        }
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ReplaceMemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto memberName = input->Child(1)->Content();
        auto structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
        auto pos = FindOrReportMissingMember(memberName, input->Pos(), *structType, ctx);
        if (!pos) {
            return IGraphTransformer::TStatus::Error;
        }

        auto newItems = input->Head().GetTypeAnn()->Cast<TStructExprType>()->GetItems();
        newItems[*pos] = ctx.Expr.MakeType<TItemExprType>(memberName, input->Child(2)->GetTypeAnn());
        input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(newItems));
        if (!input->GetTypeAnn()->Cast<TStructExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    template <bool Equality>
    IGraphTransformer::TStatus CompareWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (input->Content() == "Equal") {
            output = ctx.Expr.RenameNode(*input, "==");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Content() == "NotEqual") {
            output = ctx.Expr.RenameNode(*input, "!=");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Content() == "Less") {
            output = ctx.Expr.RenameNode(*input, "<");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Content() == "LessOrEqual") {
            output = ctx.Expr.RenameNode(*input, "<=");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Content() == "Greater") {
            output = ctx.Expr.RenameNode(*input, ">");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Content() == "GreaterOrEqual") {
            output = ctx.Expr.RenameNode(*input, ">=");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!(EnsurePersistable(input->Head(), ctx.Expr) && EnsurePersistable(input->Tail(), ctx.Expr))) {
            return IGraphTransformer::TStatus::Error;
        }

        switch (CanCompare<Equality>(input->Head().GetTypeAnn(), input->Tail().GetTypeAnn())) {
            case ECompareOptions::Null:
                output = MakeBoolNothing(input->Pos(), ctx.Expr);
                return IGraphTransformer::TStatus::Repeat;

            case ECompareOptions::Uncomparable:
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Uncompatible types in compare: " <<
                    *input->Head().GetTypeAnn() << " '" << input->Content() << "' " << *input->Tail().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;


            case ECompareOptions::Comparable:
                input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
                break;

            case ECompareOptions::Optional:
                input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool)));
                break;
        }

        if (Equality) {
            input->SetUnorderedChildren();
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AbsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!(IsDataTypeNumeric(dataType->GetSlot()) || IsDataTypeDecimal(dataType->GetSlot()) || dataType->GetSlot() == EDataSlot::Interval)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected numeric, decimal or interval type, but got: "
                << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MinMaxWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (1U == input->ChildrenSize()) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        bool isOptional1;
        const TDataExprType* dataType1;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional1, dataType1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isSomeOptional = isOptional1;
        for (ui32 index = 1; index < input->ChildrenSize(); ++index) {
            bool isOptional2;
            const TDataExprType* dataType2;
            if (!EnsureDataOrOptionalOfData(*input->Child(index), isOptional2, dataType2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            isSomeOptional = isSomeOptional || isOptional2;
            const bool isLeftNumeric = IsDataTypeNumeric(dataType1->GetSlot());
            const bool isRightNumeric = IsDataTypeNumeric(dataType2->GetSlot());
            if (isLeftNumeric != isRightNumeric) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "mismatch of data types: "
                    << *input->Head().GetTypeAnn() << " != " << *input->Child(index)->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            if (isLeftNumeric) {
                auto commonTypeSlot = GetNumericDataTypeByLevel(Max(GetNumericDataTypeLevel(dataType1->GetSlot()),
                    GetNumericDataTypeLevel(dataType2->GetSlot())));
                dataType1 = (commonTypeSlot == dataType1->GetSlot()) ? dataType1 : dataType2;
            }
            else {
                if (!IsSameAnnotation(*dataType1, *dataType2)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "mismatch of data types: "
                        << *input->Head().GetTypeAnn() << " != " << *input->Child(index)->GetTypeAnn()));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        input->SetTypeAnn(dataType1);
        if (isSomeOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }
        input->SetUnorderedChildren();
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool Equal, bool Order>
    IGraphTransformer::TStatus AggrCompareWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!(Order ? EnsureComparableType : EnsureEquatableType)(input->Pos(), *input->Head().GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*input->Head().GetTypeAnn(), *input->Tail().GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Type mismatch, left: "
                << *input->Head().GetTypeAnn() << ", right:" << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (IsInstantEqual(*input->Head().GetTypeAnn())) {
            output = MakeBool(input->Pos(), Equal, ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        if (!Order) {
            input->SetUnorderedChildren();
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggrMinMaxWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComparableType(input->Pos(), *input->Head().GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*input->Head().GetTypeAnn(), *input->Tail().GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Type mismatch, left: "
                << *input->Head().GetTypeAnn() << ", right:" << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (IsInstantEqual(*input->Head().GetTypeAnn())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        input->SetUnorderedChildren();
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DistinctFromWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!(EnsurePersistable(input->Head(), ctx.Expr) && EnsurePersistable(input->Tail(), ctx.Expr))) {
            return IGraphTransformer::TStatus::Error;
        }

        if (CanCompare<true>(input->Head().GetTypeAnn(), input->Tail().GetTypeAnn()) == ECompareOptions::Uncomparable) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Uncompatible types in compare: " << *input->Head().GetTypeAnn() << " '" << input->Content() << "' " << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        input->SetUnorderedChildren();
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggrAddWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->TailPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsNull(input->Tail())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional1;
        const TDataExprType* dataType1;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional1, dataType1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional2;
        const TDataExprType* dataType2;
        if (!EnsureDataOrOptionalOfData(input->Tail(), isOptional2, dataType2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*input->Head().GetTypeAnn(), *input->Tail().GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Type mismatch, left: "
                << *input->Head().GetTypeAnn() << ", right:" << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        const bool isInterval = dataType1->GetSlot() == EDataSlot::Interval;

        if (!(IsDataTypeNumeric(dataType1->GetSlot()) || IsDataTypeDecimal(dataType1->GetSlot()) || isInterval)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected numeric or decimal data type, but got: "
                << *input->Head().GetTypeAnn()));

            return IGraphTransformer::TStatus::Error;
        }

        auto resultType = input->Head().GetTypeAnn();
        if (isInterval && !isOptional1) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }
        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AddWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType[2];
        bool isOptional[2];
        bool haveOptional = false;
        const TDataExprType* commonType = nullptr;
        for (ui32 i = 0; i < 2; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional[i], dataType[i], ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            haveOptional |= isOptional[i];
        }

        const bool isLeftNumeric = IsDataTypeNumeric(dataType[0]->GetSlot());
        const bool isRightNumeric = IsDataTypeNumeric(dataType[1]->GetSlot());
        bool isOk = false;
        if (isLeftNumeric && isRightNumeric) {
            isOk = true;
            auto commonTypeSlot = GetNumericDataTypeByLevel(Max(GetNumericDataTypeLevel(dataType[0]->GetSlot()),
                GetNumericDataTypeLevel(dataType[1]->GetSlot())));
            commonType = ctx.Expr.MakeType<TDataExprType>(commonTypeSlot);
        } else if ((IsDataTypeDate(dataType[0]->GetSlot()) || IsDataTypeTzDate(dataType[0]->GetSlot())) && dataType[1]->GetSlot() == EDataSlot::Interval) {
            commonType = dataType[0];
            haveOptional = true;
        } else if (dataType[0]->GetSlot() == EDataSlot::Interval && (IsDataTypeDate(dataType[1]->GetSlot()) || IsDataTypeTzDate(dataType[1]->GetSlot()))) {
            commonType = dataType[1];
            haveOptional = true;
        } else if (dataType[0]->GetSlot() == EDataSlot::Interval && dataType[1]->GetSlot() == EDataSlot::Interval) {
            commonType = dataType[0];
            haveOptional = true;
        } else if (IsDataTypeDecimal(dataType[0]->GetSlot()) && IsDataTypeDecimal(dataType[1]->GetSlot())) {
            const auto dataTypeOne = static_cast<const TDataExprParamsType*>(dataType[0]);
            const auto dataTypeTwo = static_cast<const TDataExprParamsType*>(dataType[1]);

            if (!(*dataTypeOne == *dataTypeTwo)) {
                ctx.Expr.AddError(TIssue(
                    ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Cannot add different decimals."
                ));

                return IGraphTransformer::TStatus::Error;
            }

            commonType = dataType[0];
        } else {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Cannot add type " << *input->Head().GetTypeAnn() << " and " << *input->Tail().GetTypeAnn()
            ));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = commonType;
        if (haveOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SubWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType[2];
        bool isOptional[2];
        bool haveOptional = false;
        const TDataExprType* commonType = nullptr;
        for (ui32 i = 0; i < 2; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional[i], dataType[i], ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            haveOptional |= isOptional[i];
        }

        const bool isLeftNumeric = IsDataTypeNumeric(dataType[0]->GetSlot());
        const bool isRightNumeric = IsDataTypeNumeric(dataType[1]->GetSlot());
        bool isOk = false;
        if (isLeftNumeric && isRightNumeric) {
            isOk = true;
            auto commonTypeSlot = GetNumericDataTypeByLevel(Max(GetNumericDataTypeLevel(dataType[0]->GetSlot()),
                GetNumericDataTypeLevel(dataType[1]->GetSlot())));
            commonType = ctx.Expr.MakeType<TDataExprType>(commonTypeSlot);
        } else if ((IsDataTypeDate(dataType[0]->GetSlot()) || IsDataTypeTzDate(dataType[0]->GetSlot())) &&
            (IsDataTypeDate(dataType[1]->GetSlot()) || IsDataTypeTzDate(dataType[1]->GetSlot()))) {
            commonType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Interval);
        } else if (IsDataTypeDateOrTzDateOrInterval(dataType[0]->GetSlot()) &&
            dataType[1]->GetSlot() == EDataSlot::Interval) {
            commonType = dataType[0];
            haveOptional = true;
        } else if (IsDataTypeDecimal(dataType[0]->GetSlot()) && IsDataTypeDecimal(dataType[1]->GetSlot())) {
            const auto dataTypeOne = static_cast<const TDataExprParamsType*>(dataType[0]);
            const auto dataTypeTwo = static_cast<const TDataExprParamsType*>(dataType[1]);

            if (!(*dataTypeOne == *dataTypeTwo)) {
                ctx.Expr.AddError(TIssue(
                    ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Cannot substract different decimals."
                ));

                return IGraphTransformer::TStatus::Error;
            }

            commonType = dataType[0];
        } else {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Cannot substract type " << *input->Head().GetTypeAnn() << " and " << *input->Tail().GetTypeAnn()
            ));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = commonType;
        if (haveOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MulWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType[2];
        bool isOptional[2];
        bool haveOptional = false;
        const TDataExprType* commonType = nullptr;
        for (ui32 i = 0; i < 2; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional[i], dataType[i], ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            haveOptional |= isOptional[i];
        }

        if (IsDataTypeNumeric(dataType[0]->GetSlot()) && IsDataTypeNumeric(dataType[1]->GetSlot())) {
            auto commonTypeSlot = GetNumericDataTypeByLevel(Max(GetNumericDataTypeLevel(dataType[0]->GetSlot()),
                GetNumericDataTypeLevel(dataType[1]->GetSlot())));
            commonType = ctx.Expr.MakeType<TDataExprType>(commonTypeSlot);
        } else if (IsDataTypeIntegral(dataType[0]->GetSlot()) && dataType[1]->GetSlot() == EDataSlot::Interval) {
            commonType = dataType[1];
            haveOptional = true;
        } else if (dataType[0]->GetSlot() == EDataSlot::Interval && IsDataTypeIntegral(dataType[1]->GetSlot())) {
            commonType = dataType[0];
            haveOptional = true;
        } else if (IsDataTypeDecimal(dataType[0]->GetSlot()) || IsDataTypeDecimal(dataType[1]->GetSlot())) {
            output = ctx.Expr.RenameNode(*input, "DecimalMul");
            if (!IsDataTypeDecimal(dataType[0]->GetSlot())) {
                output->ChildRef(0).Swap(output->ChildRef(1));
            }
            return IGraphTransformer::TStatus::Repeat;
        } else {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Cannot multiply type " << *input->Head().GetTypeAnn() << " and " << *input->Tail().GetTypeAnn()
            ));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = commonType;
        if (haveOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DivWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType[2];
        bool isOptional[2];
        bool haveOptional = false;
        const TDataExprType* commonType = nullptr;
        for (ui32 i = 0; i < 2; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional[i], dataType[i], ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            haveOptional |= isOptional[i];
        }

        if (IsDataTypeNumeric(dataType[0]->GetSlot()) && IsDataTypeNumeric(dataType[1]->GetSlot())) {
            auto commonTypeSlot = GetNumericDataTypeByLevel(Max(GetNumericDataTypeLevel(dataType[0]->GetSlot()),
                GetNumericDataTypeLevel(dataType[1]->GetSlot())));
            commonType = ctx.Expr.MakeType<TDataExprType>(commonTypeSlot);
            if (!IsDataTypeFloat(commonTypeSlot)) {
                haveOptional = true;
            }
        } else if (dataType[0]->GetSlot() == EDataSlot::Interval && IsDataTypeIntegral(dataType[1]->GetSlot())) {
            commonType = dataType[0];
            haveOptional = true;
        } else if (IsDataTypeDecimal(dataType[0]->GetSlot()) && (IsDataTypeDecimal(dataType[1]->GetSlot()) || IsDataTypeIntegral(dataType[1]->GetSlot()))) {
            output = ctx.Expr.RenameNode(*input, "DecimalDiv");
            return IGraphTransformer::TStatus::Repeat;
        } else {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Cannot divide type " << *input->Head().GetTypeAnn() << " and " << *input->Tail().GetTypeAnn()
            ));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = commonType;
        if (haveOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ModWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType[2];
        bool isOptional[2];
        bool haveOptional = false;
        const TDataExprType* commonType = nullptr;
        for (ui32 i = 0; i < 2; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional[i], dataType[i], ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            haveOptional |= isOptional[i];
        }

        if (IsDataTypeNumeric(dataType[0]->GetSlot()) && IsDataTypeNumeric(dataType[1]->GetSlot())) {
            auto commonTypeSlot = GetNumericDataTypeByLevel(Max(GetNumericDataTypeLevel(dataType[0]->GetSlot()),
                GetNumericDataTypeLevel(dataType[1]->GetSlot())));
            commonType = ctx.Expr.MakeType<TDataExprType>(commonTypeSlot);
            if (!IsDataTypeFloat(commonTypeSlot)) {
                haveOptional = true;
            }
        } else if (IsDataTypeDecimal(dataType[0]->GetSlot()) && (IsDataTypeDecimal(dataType[1]->GetSlot()) || IsDataTypeIntegral(dataType[1]->GetSlot()))) {
            output = ctx.Expr.RenameNode(*input, "DecimalMod");
            return IGraphTransformer::TStatus::Repeat;
        } else {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Cannot mod type " << *input->Head().GetTypeAnn() << " and " << *input->Tail().GetTypeAnn()
            ));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = commonType;
        if (haveOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DecimalBinaryWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType[2];
        bool isOptional[2];
        bool haveOptional = false;
        const TDataExprType* commonType = nullptr;
        for (ui32 i = 0; i < 2; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional[i], dataType[i], ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            haveOptional |= isOptional[i];
        }

        if (IsDataTypeDecimal(dataType[0]->GetSlot())) {
            if (IsDataTypeDecimal(dataType[1]->GetSlot())) {
                const auto dataTypeOne = static_cast<const TDataExprParamsType*>(dataType[0]);
                const auto dataTypeTwo = static_cast<const TDataExprParamsType*>(dataType[1]);

                if (!(*dataTypeOne == *dataTypeTwo)) {
                    ctx.Expr.AddError(TIssue(
                        ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Cannot calculate with different decimals."
                    ));

                    return IGraphTransformer::TStatus::Error;
                }
            } else if (!IsDataTypeIntegral(dataType[1]->GetSlot())) {
                ctx.Expr.AddError(TIssue(
                    ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Cannot operate with decimal and " << *input->Tail().GetTypeAnn()
                ));

                return IGraphTransformer::TStatus::Error;
            }

            commonType = dataType[0];
        } else {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Cannot use type " << *input->Head().GetTypeAnn() << " and " << *input->Tail().GetTypeAnn()
            ));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = commonType;
        if (haveOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CountBitsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsDataTypeIntegral(dataType->GetSlot())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected integral data type, but got: "
                << *input->Head().GetTypeAnn()));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = dataType;
        if (isOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool IncOrDec>
    IGraphTransformer::TStatus IncDecWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto dataSlot = dataType->GetSlot();
        if (IsDataTypeDecimal(dataSlot)) {
            const auto params = static_cast<const TDataExprParamsType*>(dataType);
            if (const auto scale = FromString<ui8>(params->GetParamTwo())) {
                    output = ctx.Expr.Builder(input->Pos())
                        .Callable(IncOrDec ? "Add" : "Sub")
                            .Add(0, input->Child(0))
                            .Callable(1, "Decimal")
                                .Atom(0, "1")
                                .Atom(1, params->GetParamOne())
                                .Atom(2, params->GetParamTwo())
                            .Seal()
                        .Seal()
                        .Build();
                return IGraphTransformer::TStatus::Repeat;
            }
        } else if (!IsDataTypeNumeric(dataSlot)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected numeric or decimal data type, but got: "
                << *input->Head().GetTypeAnn()));

            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = dataType;
        if (isOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PlusMinusWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataSlot = dataType->GetSlot();
        if (!(IsDataTypeNumeric(dataSlot) || IsDataTypeDecimal(dataSlot) || dataSlot == EDataSlot::Interval)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected numeric, decimal or interval data type, but got: "
                << *input->Head().GetTypeAnn()));

            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    template <size_t N>
    IGraphTransformer::TStatus BitOpsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, N, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional[N];
        const TDataExprType* dataType[N];
        bool haveOptional = false;
        const TDataExprType* commonType = nullptr;
        for (ui32 i = 0; i < N; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional[i], dataType[i], ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto dataSlot = dataType[i]->GetSlot();
            if (!IsDataTypeUnsigned(dataSlot)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected unsigned data type, but got: "
                    << *input->Child(i)->GetTypeAnn()));

                return IGraphTransformer::TStatus::Error;
            }

            haveOptional |= isOptional[i];
        }

        if (N == 2 && dataType[0]->GetSlot() != dataType[1]->GetSlot()) {
            auto commonTypeSlot = GetNumericDataTypeByLevel(Max(GetNumericDataTypeLevel(dataType[0]->GetSlot()),
                GetNumericDataTypeLevel(dataType[1]->GetSlot())));
            commonType = dataType[commonTypeSlot == dataType[0]->GetSlot() ? 0 : 1];
        }
        else {
            commonType = dataType[0];
        }

        const TTypeAnnotationNode* resultType = commonType;
        if (haveOptional) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ShiftWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto dataSlot = dataType->GetSlot();
        if (!IsDataTypeIntegral(dataSlot)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Unsupported type: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        const auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint8);
        const auto convertStatus = TryConvertTo(input->TailRef(), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), "Shift count must be Uint8"));
            return IGraphTransformer::TStatus::Error;
        } else if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        if (IsDataTypeSigned(dataSlot)) {
            auto dataTypeName = TString("U") += NKikimr::NUdf::GetDataTypeInfo(dataSlot).Name;
            dataTypeName[1] = 'i';
            output = ctx.Expr.Builder(input->Pos())
                .Callable(input->Content())
                    .Callable(0, "BitCast")
                        .Add(0, input->HeadPtr())
                        .Callable(1, "DataType")
                            .Atom(0, dataTypeName, TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Add(1, input->TailPtr())
                .Seal().Build();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SyncWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (auto& child : input->Children()) {
            if (!EnsureWorldType(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ConcatWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (ui32 i = 0; i < 2; ++i) {
            if (IsNull(*input->Child(i))) {
                output = input->ChildPtr(i);
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        bool isOptional1;
        const TDataExprType* data1;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional1, data1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional2;
        const TDataExprType* data2;
        if (!EnsureDataOrOptionalOfData(input->Tail(), isOptional2, data2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataType1 = data1->GetSlot();
        auto dataType2 = data2->GetSlot();
        if (dataType1 != EDataSlot::String && dataType1 != EDataSlot::Utf8) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected (optional) String or Utf8, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (dataType2 != EDataSlot::String && dataType2 != EDataSlot::Utf8) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder() << "Expected (optional) String or Utf8, but got: " << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(data1);
        if (dataType1 == EDataSlot::Utf8 && dataType2 == EDataSlot::String) {
            input->SetTypeAnn(data2);
        }

        if (isOptional1 || isOptional2) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggrConcatWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*input->Head().GetTypeAnn(), *input->Tail().GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Type mismatch, left: "
                << *input->Head().GetTypeAnn() << ", right:" << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* data;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, data, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (data->GetSlot() != EDataSlot::String && data->GetSlot() != EDataSlot::Utf8) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected (optional) String or Utf8, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Child(1)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SubstringWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head()) || IsNull(*input->Child(1)) && IsNull(*input->Child(2))) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            auto child = input->Child(i);
            if (!child->GetTypeAnn()) {
                YQL_ENSURE(child->Type() == TExprNode::Lambda);
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                    << "Expected (optional) " << (i ? "Uint32" : "String") << ", but got lambda"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto originalType = input->Head().GetTypeAnn();
        auto type = originalType;
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        if (!EnsureSpecificDataType(input->Head().Pos(), *type, EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedTypeOne = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32);
        const auto kindOne = input->Child(1U)->GetTypeAnn()->GetKind();
        if (kindOne == ETypeAnnotationKind::Optional || kindOne == ETypeAnnotationKind::Null) {
            expectedTypeOne = ctx.Expr.MakeType<TOptionalExprType>(expectedTypeOne);
        }
        const auto convertStatus1 = TryConvertTo(input->ChildRef(1), *expectedTypeOne, ctx.Expr);
        if (convertStatus1.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedTypeTwo = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32);
        const auto kindTwo = input->Child(2U)->GetTypeAnn()->GetKind();
        if (kindTwo == ETypeAnnotationKind::Optional || kindTwo == ETypeAnnotationKind::Null) {
            expectedTypeTwo = ctx.Expr.MakeType<TOptionalExprType>(expectedTypeTwo);
        }
        const auto convertStatus2 = TryConvertTo(input->ChildRef(2), *expectedTypeTwo, ctx.Expr);
        if (convertStatus2.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto combinedStatus = convertStatus1.Combine(convertStatus2);
        if (combinedStatus.Level != IGraphTransformer::TStatus::Ok) {
            return combinedStatus;
        }

        input->SetTypeAnn(originalType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FindWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) String, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        auto type = input->Head().GetTypeAnn();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        if (!EnsureStringOrUtf8Type(input->Head().Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (const auto convertStatus = TryConvertTo(input->ChildRef(1), *type, ctx.Expr); convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32);
        if (const auto kind = input->Child(2)->GetTypeAnn()->GetKind(); kind == ETypeAnnotationKind::Optional || kind == ETypeAnnotationKind::Null) {
            expectedType = ctx.Expr.MakeType<TOptionalExprType>(expectedType);
        }

        if (const auto convertStatus = TryConvertTo(input->ChildRef(2), *expectedType, ctx.Expr); convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32)));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WithWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head()) || IsNull(input->Tail())) {
            output = MakeBoolNothing(input->Pos(), ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional1, isOptional2;
        if (const TDataExprType *dataTypeOne, *dataTypeTwo;
            !(EnsureDataOrOptionalOfData(input->Head(), isOptional1, dataTypeOne, ctx.Expr) && EnsureDataOrOptionalOfData(input->Tail(), isOptional2, dataTypeTwo, ctx.Expr)
            && EnsureStringOrUtf8Type(input->Head().Pos(), *dataTypeOne, ctx.Expr) && EnsureStringOrUtf8Type(input->Tail().Pos(), *dataTypeTwo, ctx.Expr))) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isOptional1 || isOptional2)
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool)));
        else
            input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ByteAtWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected (optional) String, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        auto originalType = input->Head().GetTypeAnn();
        auto type = originalType;
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        if (!EnsureDataType(input->Head().Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataType = type->Cast<TDataExprType>()->GetSlot();
        if (dataType != EDataSlot::String && dataType != EDataSlot::Utf8) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() <<
                "Expected either String or Utf8, but got: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32);
        auto convertStatus = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint8)));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ListIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(input->Tail().GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool IsStrict>
    IGraphTransformer::TStatus AsListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (input->ChildrenSize() == 0) {
            output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if constexpr (IsStrict) {
            std::set<const TTypeAnnotationNode*> set;
            input->ForEachChild([&](const TExprNode& item) { set.emplace(item.GetTypeAnn()); });
            if (1U != set.size()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() <<
                "List items types isn't same: " << **set.cbegin() << " and " << **set.crbegin()));
                return IGraphTransformer::TStatus::Error;
            }

            output = ctx.Expr.RenameNode(*input, "AsList");
            return IGraphTransformer::TStatus::Repeat;
        } else if (const auto commonItemType = CommonTypeForChildren(*input, ctx.Expr)) {
            if (const auto status = ConvertChildrenToType(input, commonItemType, ctx.Expr); status != IGraphTransformer::TStatus::Ok)
                return status;
        } else
            return IGraphTransformer::TStatus::Error;

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(input->Head().GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().IsLambda()) {
            output = ctx.Expr.RenameNode(*input, "Iterable");
            return IGraphTransformer::TStatus::Repeat;
        }

        switch (const auto argType = input->Head().GetTypeAnn(); argType->GetKind()) {
            case ETypeAnnotationKind::List:
            case ETypeAnnotationKind::EmptyList:
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            case ETypeAnnotationKind::Tuple:
                if (argType->GetKind() == ETypeAnnotationKind::Tuple) {
                    if (const auto size = argType->Cast<TTupleExprType>()->GetSize()) {
                        TExprNodeList list;
                        list.reserve(size);
                        for (auto i = 0U; i < size; ++i) {
                            list.emplace_back(
                                input->Head().IsList() ? input->Head().ChildPtr(i) :
                                ctx.Expr.NewCallable(input->Pos(), "Nth", { input->HeadPtr(), ctx.Expr.NewAtom(input->Pos(), ToString(i), TNodeFlags::Default) })
                            );
                        }
                        output = ctx.Expr.NewCallable(input->Pos(), "AsList", std::move(list));
                    } else {
                        output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
                    }
                }
                return IGraphTransformer::TStatus::Repeat;
            case ETypeAnnotationKind::Optional:
                input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(argType->Cast<TOptionalExprType>()->GetItemType()));
                return IGraphTransformer::TStatus::Ok;
            default:
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                    TStringBuilder() << "Expecting list, optional or tuple argument, but got: " << *argType));
                return IGraphTransformer::TStatus::Error;
        }
    }

    IGraphTransformer::TStatus ToOptionalWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(itemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus HeadWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        output = ctx.Expr.RenameNode(*input, "ToOptional");
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ToFlowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (ETypeAnnotationKind::Flow == input->Head().GetTypeAnn()->GetKind()) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(itemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FromFlowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureFlowType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto itemType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(itemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus BuildTablePathWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Tail(), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WithOptionalArgsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AsTaggedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TTaggedExprType>(input->Head().GetTypeAnn(), input->Tail().Content()));
        if (!input->GetTypeAnn()->Cast<TTaggedExprType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus UntagWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTaggedType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto tagType = input->Head().GetTypeAnn()->Cast<TTaggedExprType>();
        if (tagType->GetTag() != input->Tail().Content()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder() <<
                "Expected tag: " << tagType->GetTag() << ", but got: " << input->Tail().Content()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(tagType->GetBaseType());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus BoolOpt1Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = MakeBoolNothing(input->Head().Pos(), ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head().Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool Xor>
    IGraphTransformer::TStatus LogicalWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptionalResult = false;
        for (ui32 i = 0U; i < input->ChildrenSize() ; ++i) {
            if (IsNull(*input->Child(i))) {
                (Xor ? output : input->ChildRef(i)) = MakeBoolNothing(input->Child(i)->Pos(), ctx.Expr);
                return IGraphTransformer::TStatus::Repeat;
            }

            bool isOptional;
            const TDataExprType* dataType;
            if (!EnsureDataOrOptionalOfData(*input->Child(i), isOptional, dataType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureSpecificDataType(input->Child(i)->Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            isOptionalResult = isOptionalResult || isOptional;
        }

        if (1U == input->ChildrenSize()) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (isOptionalResult) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool)));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus StructWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (input->ChildrenSize() > 0) {
            if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (type->GetKind() != ETypeAnnotationKind::Struct) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                    TStringBuilder() << "Expected struct type, but got: " << *type));
                return IGraphTransformer::TStatus::Error;
            }

            auto structType = type->Cast<TStructExprType>();
            THashMap<TStringBuf, const TItemExprType*> expectedMembers;
            for (auto& item : structType->GetItems()) {
                expectedMembers[item->GetName()] = item;
            }

            THashSet<TStringBuf> foundMembers;
            for (size_t i = 1; i < input->ChildrenSize(); ++i) {
                if (!EnsureTupleSize(*input->Child(i), 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto& nameNode = input->Child(i)->Head();
                if (!EnsureAtom(nameNode, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto& name = nameNode.Content();
                if (!foundMembers.insert(name).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(nameNode.Pos()), TStringBuilder() << "Duplicated member: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                const auto member = expectedMembers.FindPtr(name);
                if (!member) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(nameNode.Pos()), TStringBuilder() << "Unknown member: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                const auto& valueNode = input->Child(i)->Tail();
                if (!IsSameAnnotation(*(*member)->GetItemType(), *valueNode.GetTypeAnn())) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(nameNode.Pos()), TStringBuilder() << "Mismatch type of member: " << name
                        << ", expected type: " << *(*member)->GetItemType() << ", but got: " << *valueNode.GetTypeAnn()));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            bool hasMissingMembers = false;
            for (auto& x : expectedMembers) {
                if (!foundMembers.contains(x.first)) { 
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Missing member: " << x.first));
                    hasMissingMembers = true;
                }
            }

            if (hasMissingMembers) {
                return IGraphTransformer::TStatus::Error;
            }

            input->SetTypeAnn(structType);
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(TVector<const TItemExprType*>()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FlatListIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Tail().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FlatOptionalIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureOptionalType(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Tail().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SizeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = MakeNothingData(ctx.Expr, input->Head().Pos(), "Uint32");
            return IGraphTransformer::TStatus::Repeat;
        }

        const TDataExprType* dataType;
        bool isOptional;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32));
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FromStringWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType;
        bool isOptional;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head().Pos(), *dataType, EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataTypeName = input->Child(1)->Content();
        auto slot = NKikimr::NUdf::FindDataSlot(dataTypeName);
        if (!slot) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Unknown datatype: " << dataTypeName));
            return IGraphTransformer::TStatus::Error;
        }

        const bool isDecimal = IsDataTypeDecimal(*slot);
        if (!EnsureArgsCount(*input, isDecimal ? 4 : 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataTypeAnn;
        if (isDecimal) {
            auto ret = ctx.Expr.MakeType<TDataExprParamsType>(*slot, input->Child(2)->Content(), input->Child(3)->Content());
            if (!ret->Validate(input->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            dataTypeAnn = ret;
        } else {
            dataTypeAnn= ctx.Expr.MakeType<TDataExprType>(*slot);
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(dataTypeAnn));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus StrictFromStringWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType;
        bool isOptional;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head().Pos(), *dataType, EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataTypeName = input->Child(1)->Content();
        auto slot = NKikimr::NUdf::FindDataSlot(dataTypeName);
        if (!slot) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Unknown datatype: " << dataTypeName));
            return IGraphTransformer::TStatus::Error;
        }

        const bool isDecimal = IsDataTypeDecimal(*slot);
        if (!EnsureArgsCount(*input, isDecimal ? 4 : 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }


        const TDataExprType* dataTypeAnn;
        if (isDecimal) {
            dataTypeAnn = ctx.Expr.MakeType<TDataExprParamsType>(*slot, input->Child(2)->Content(), input->Child(3)->Content());
        } else {
            dataTypeAnn= ctx.Expr.MakeType<TDataExprType>(*slot);
        }

        input->SetTypeAnn(dataTypeAnn);
        if (isDecimal && !input->GetTypeAnn()->Cast<TDataExprParamsType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FromBytesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head().Pos(), *dataType, EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataTypeName = input->Child(1)->Content();
        auto slot = NKikimr::NUdf::FindDataSlot(dataTypeName);
        if (!slot) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Unknown datatype: " << dataTypeName));
            return IGraphTransformer::TStatus::Error;
        }

        const bool isDecimal = IsDataTypeDecimal(*slot);
        if (!EnsureArgsCount(*input, isDecimal ? 4 : 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataTypeAnn = isDecimal ?
            ctx.Expr.MakeType<TDataExprParamsType>(*slot, input->Child(2)->Content(), input->Child(3)->Content()):
            ctx.Expr.MakeType<TDataExprType>(*slot);
        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(dataTypeAnn));
        if (isDecimal && !input->GetTypeAnn()->Cast<TDataExprParamsType>()->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        return IGraphTransformer::TStatus::Ok;
    }

    bool CanConvert(EDataSlot sourceType, EDataSlot targetType) {
        bool canConvert = false;
        if ((IsDataTypeIntegral(sourceType) || sourceType == EDataSlot::Bool) &&
            (IsDataTypeNumeric(targetType) || targetType == EDataSlot::Bool)) {
            canConvert = true;
        }
        else if (IsDataTypeFloat(sourceType) && IsDataTypeFloat(targetType)) {
            canConvert = true;
        }
        else if (sourceType == targetType && !IsDataTypeDecimal(sourceType)) {
            canConvert = true;
        }
        else if ((sourceType == EDataSlot::Yson || sourceType == EDataSlot::Utf8 || sourceType == EDataSlot::Json) && targetType == EDataSlot::String) {
            canConvert = true;
        }
        else if (sourceType == EDataSlot::Json && targetType == EDataSlot::Utf8) {
            canConvert = true;
        }
        else if (IsDataTypeDateOrTzDateOrInterval(sourceType) && IsDataTypeNumeric(targetType)) {
            canConvert = true;
        }

        return canConvert;
    }

    IGraphTransformer::TStatus ConvertWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType;
        bool isOptional;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeOrAtomRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        EDataSlot targetSlot;
        if (input->Tail().Type() == TExprNode::Atom) {
            const auto targetTypeStr = input->Tail().Content();
            auto slot = NKikimr::NUdf::FindDataSlot(targetTypeStr);
            if (!slot) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder() << "Unknown datatype: " << targetTypeStr));
                return IGraphTransformer::TStatus::Error;
            }

            targetSlot = *slot;
        } else {
            const auto type = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();

            if (!EnsureDataType(input->Tail().Pos(), *type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            targetSlot = type->Cast<TDataExprType>()->GetSlot();
        }

        const auto sourceSlot = dataType->GetSlot();

        if (sourceSlot == targetSlot) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!CanConvert(sourceSlot, targetSlot)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Cannot convert type " <<
                *input->Head().GetTypeAnn() << " into " << NKikimr::NUdf::GetDataTypeInfo(targetSlot).Name));
            return IGraphTransformer::TStatus::Error;
        }

        const auto dataTypeAnn = ctx.Expr.MakeType<TDataExprType>(targetSlot);
        input->SetTypeAnn(dataTypeAnn);
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus BitCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType;
        bool isOptional;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeOrAtomRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        EDataSlot targetSlot;
        if (input->Tail().Type() == TExprNode::Atom) {
            const auto targetTypeStr = input->Tail().Content();
            auto slot = NKikimr::NUdf::FindDataSlot(targetTypeStr);
            if (!slot) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder() << "Unknown datatype: " << targetTypeStr));
                return IGraphTransformer::TStatus::Error;
            }

            targetSlot = *slot;
        } else {
            const auto type = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();

            if (!EnsureDataType(input->Tail().Pos(), *type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            targetSlot = type->Cast<TDataExprType>()->GetSlot();
        }

        const auto sourceSlot = dataType->GetSlot();
        if (!((EDataSlot::Bool == sourceSlot || IsDataTypeIntegral(sourceSlot) || IsDataTypeDateOrTzDateOrInterval(sourceSlot)) && IsDataTypeIntegral(targetSlot))) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Cannot bit cast type " <<
                *input->Head().GetTypeAnn() << " into " << NKikimr::NUdf::GetDataTypeInfo(targetSlot).Name));
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.RenameNode(*input, "Convert");
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus AlterToWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& source = input->Head();
        auto& targetTypeNode = input->ChildRef(1);
        auto& lambda = input->ChildRef(2);
        const auto& alterFailValue = input->Tail();

        if (!EnsureComputable(source, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(targetTypeNode, ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureComputable(alterFailValue, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto sourceType = source.GetTypeAnn();
        const auto targetType = targetTypeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

        const auto status = ConvertToLambda(lambda, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, {targetType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto alterSuccessType = lambda->GetTypeAnn();
        const auto alterFailType = alterFailValue.GetTypeAnn();
        if (!IsSameAnnotation(*alterSuccessType, *alterFailType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "mismatch of success/fail types, success type: "
                                                                    << *alterSuccessType << ", fail type: " << *alterFailType));
            return IGraphTransformer::TStatus::Error;
        }

        if (IsSameAnnotation(*sourceType, *targetType)) {
            output = ctx.Expr.ReplaceNode(lambda->TailPtr(), lambda->Head().Head(), input->HeadPtr());
            return IGraphTransformer::TStatus::Repeat;
        }

        if (NKikimr::NUdf::ECastOptions::Impossible & CastResult<true>(sourceType, targetType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                                     TStringBuilder() << "Impossible alter " << *sourceType << " to " << *targetType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(alterSuccessType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToIntegralWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType;
        bool isOptional;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeOrAtomRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        EDataSlot targetSlot;

        if (input->Tail().Type() == TExprNode::Atom) {
            const auto targetTypeStr = input->Tail().Content();
            auto slot = NKikimr::NUdf::FindDataSlot(targetTypeStr);
            if (!slot) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder() << "Unknown datatype: " << targetTypeStr));
                return IGraphTransformer::TStatus::Error;
            }

            targetSlot = *slot;

            auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Deprecated ToIntegral signature, use Type instead of Atom.");
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_DEPRECATED_FUNCTION_OR_SIGNATURE, issue);
            if (!ctx.Expr.AddWarning(issue)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            const auto type = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const TDataExprType* dataType;

            bool isOptional;
            if (!EnsureDataOrOptionalOfData(input->Tail().Pos(), type, isOptional, dataType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            targetSlot = dataType->GetSlot();
        }

        const auto sourceSlot = dataType->GetSlot();

        if (IsDataTypeIntegral(sourceSlot) && IsDataTypeIntegral(targetSlot)) {
            const auto& srcInfo = NKikimr::NUdf::GetDataTypeInfo(sourceSlot);
            const auto& dstInfo = NKikimr::NUdf::GetDataTypeInfo(targetSlot);
            if (srcInfo.FixedSize < dstInfo.FixedSize && (IsDataTypeUnsigned(sourceSlot) || IsDataTypeSigned(targetSlot)) ||
                srcInfo.FixedSize == dstInfo.FixedSize && IsDataTypeSigned(sourceSlot) == IsDataTypeSigned(targetSlot)) {
                output = ctx.Expr.RenameNode(*input, "BitCast");
                return IGraphTransformer::TStatus::Repeat;
            }
        } else if (!IsDataTypeFloat(sourceSlot) || !(IsDataTypeIntegral(targetSlot) || targetSlot == EDataSlot::Bool)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Cannot make an integral type " << NKikimr::NUdf::GetDataTypeInfo(targetSlot).Name
                << " from type " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        const auto dataTypeAnn = ctx.Expr.MakeType<TDataExprType>(targetSlot);
        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(dataTypeAnn));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus OldCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* sourceDataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, sourceDataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeOrAtomRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        EDataSlot targetType;

        if (input->Child(1)->Type() == TExprNode::Atom) {
            const auto targetTypeStr = input->Child(1)->Content();
            auto slot = NKikimr::NUdf::FindDataSlot(targetTypeStr);
            if (!slot) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Unknown datatype: " << targetTypeStr));
                return IGraphTransformer::TStatus::Error;
            }

            targetType = *slot;
        } else {
            const auto type = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const TDataExprType* dataType;

            bool retIsOptional = false;
            if (!EnsureDataOrOptionalOfData(input->Tail().Pos(), type, retIsOptional, dataType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            targetType = dataType->GetSlot();
        }

        const auto sourceType = sourceDataType->GetSlot();
        output = ctx.Expr.RenameNode(*input, IsDataTypeIntegral(sourceType) && IsDataTypeIntegral(targetType) ? "BitCast" : "SafeCast");
        return IGraphTransformer::TStatus::Repeat;
    }

    template <bool Strong>
    IGraphTransformer::TStatus CastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = (Strong ? &EnsureTypeRewrite : &EnsureTypeOrAtomRewrite)(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const TTypeAnnotationNode* targetType = nullptr;

        if (input->Child(1)->Type() == TExprNode::Atom) {
            const auto targetTypeStr = input->Child(1)->Content();
            auto slot = NKikimr::NUdf::FindDataSlot(targetTypeStr);
            if (!slot) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Unknown datatype: " << targetTypeStr));
                return IGraphTransformer::TStatus::Error;
            }

            if (IsDataTypeDecimal(*slot)) {
                if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!(EnsureAtom(*input->Child(2), ctx.Expr) && EnsureAtom(*input->Child(3), ctx.Expr))) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto paramOne = input->Child(2)->Content();
                const auto paramTwo = input->Child(3)->Content();
                targetType = ctx.Expr.MakeType<TDataExprParamsType>(EDataSlot::Decimal, paramOne, paramTwo);
            } else {
                if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                targetType = ctx.Expr.MakeType<TDataExprType>(*slot);
            }
        } else {
            if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            targetType = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();

            if (!EnsureComputableType(input->Tail().Pos(), *targetType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto sourceType = input->Head().GetTypeAnn();
        const auto options = CastResult<Strong>(sourceType, targetType);
        if (!(options & NKikimr::NUdf::ECastOptions::Impossible)) {
            auto type = targetType;
            if (targetType->GetKind() != ETypeAnnotationKind::Optional && (options & NKikimr::NUdf::ECastOptions::MayFail)) {
                if (!EnsurePersistableType(input->Tail().Pos(), *targetType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                type = ctx.Expr.MakeType<TOptionalExprType>(targetType);
            }

            if (ETypeAnnotationKind::Null == type->GetKind()) {
                output = ctx.Expr.NewCallable(input->Tail().Pos(), "Null", {});
                return IGraphTransformer::TStatus::Repeat;
            }

            if (IsNull(input->Head())) {
                output = ctx.Expr.NewCallable(input->Head().Pos(), "Nothing", {ExpandType(input->Tail().Pos(), *type, ctx.Expr)});
                return IGraphTransformer::TStatus::Repeat;
            }

            input->SetTypeAnn(type);
            if (IsSameAnnotation(*sourceType, *input->GetTypeAnn())) {
                output = input->HeadPtr();
                return IGraphTransformer::TStatus::Repeat;
            }

            const TDataExprType* sourceDataType  = nullptr;
            const TDataExprType* targetDataType  = nullptr;
            bool isOptional;
            if (IsDataOrOptionalOfData(sourceType, isOptional, sourceDataType) && IsDataOrOptionalOfData(targetType, isOptional, targetDataType)) {
                if (sourceDataType->GetSlot() == EDataSlot::Json && targetDataType->GetSlot() == EDataSlot::String) {
                    auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Consider using ToBytes to get internal representation of Json type");
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_CAST_YSON_JSON_BYTES, issue);
                    if (!ctx.Expr.AddWarning(issue)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }

                if (sourceDataType->GetSlot() == EDataSlot::Yson && targetDataType->GetSlot() == EDataSlot::String) {
                    auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Consider using ToBytes to get internal representation of Yson type,"
                        " or Yson::ConvertToString* methods to get string values");
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_CAST_YSON_JSON_BYTES, issue);
                    if (!ctx.Expr.AddWarning(issue)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }

                auto fromFeatures = NUdf::GetDataTypeInfo(sourceDataType->GetSlot()).Features;
                auto toFeatures = NUdf::GetDataTypeInfo(targetDataType->GetSlot()).Features;
                if ((fromFeatures & NUdf::DateType) && (toFeatures & NUdf::TzDateType)) {
                    auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Consider using AddTimezone to convert from UTC time");
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_MIXED_TZ, issue);
                    if (!ctx.Expr.AddWarning(issue)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }

                if ((fromFeatures & NUdf::TzDateType) && (toFeatures & NUdf::DateType)) {
                    auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Consider using RemoveTimezone to convert into UTC time");
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_MIXED_TZ, issue);
                    if (!ctx.Expr.AddWarning(issue)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }

            return IGraphTransformer::TStatus::Ok;
        }

        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Cannot cast type "
            << *sourceType << " into " << *targetType));

        return IGraphTransformer::TStatus::Error;
    }

    IGraphTransformer::TStatus WidenIntegralWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional;
        const TDataExprType* sourceDataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, sourceDataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const bool isDecimal = IsDataTypeDecimal(sourceDataType->GetSlot());
        const bool isInterval = sourceDataType->GetSlot() == EDataSlot::Interval;
        if (!(isDecimal || IsDataTypeNumeric(sourceDataType->GetSlot()) || isInterval)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected numeric type, but got " <<
                *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (isDecimal) {
            const auto decimalType = sourceDataType->Cast<TDataExprParamsType>();
            output = ctx.Expr.Builder(input->Pos())
                .Callable("SafeCast")
                    .Add(0, input->HeadPtr())
                    .Callable(1, "DataType")
                        .Atom(0, decimalType->GetName(), TNodeFlags::Default)
                        .Atom(1, "35", TNodeFlags::Default)
                        .Atom(2, decimalType->GetParamTwo(), TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Build();
        } else if (isInterval) {
            output = isOptional ? input->HeadPtr() : ctx.Expr.Builder(input->Pos()).Callable("Just").Add(0, input->HeadPtr()).Seal().Build();
        } else if (!IsDataTypeIntegral(sourceDataType->GetSlot())) {
            output = input->HeadPtr();
        } else {
            output = ctx.Expr.Builder(input->Pos())
                .Callable("SafeCast")
                    .Add(0, input->HeadPtr())
                    .Callable(1, "DataType")
                        .Atom(0, IsDataTypeUnsigned(sourceDataType->GetSlot()) ? "Uint64" : "Int64", TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Build();
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus UnsafeTimestampCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!ctx.Types.DeprecatedSQL) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Unsafe timestamp cast restricted from SQL v1."));
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* sourceDataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, sourceDataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::Uint64, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Timestamp));
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DefaultWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head().Pos(), type, isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto dataSlot = dataType->GetSlot();
        if (dataSlot == EDataSlot::Yson || dataSlot == EDataSlot::Json) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "No default value supported for type: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PickleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsurePersistable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus UnpickleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsurePersistableType(input->Head().Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(*input->Child(1), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ExistsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = MakeBool(input->Pos(), false, ctx.Expr);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CoalesceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (auto& child : input->Children()) {
            if (!EnsureComputable(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (input->ChildrenSize() == 1) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->ChildrenSize() > 2) {
            // split into pairs
            auto current = input->HeadPtr();
            for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
                current = ctx.Expr.NewCallable(input->Pos(), input->Content(), { std::move(current), input->ChildPtr(i) });
            }

            output = current;
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsNull(input->Head())) {
            output = input->TailPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsNull(input->Tail())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto leftType = input->Head().GetTypeAnn();
        auto leftItemType = leftType;
        if (leftType->GetKind() == ETypeAnnotationKind::Optional) {
            leftItemType = leftType->Cast<TOptionalExprType>()->GetItemType();
        } else {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto rightType = input->Tail().GetTypeAnn();
        auto rightItemType = rightType;
        if (leftType->GetKind() != ETypeAnnotationKind::Optional &&
            rightType->GetKind() == ETypeAnnotationKind::Optional) {
            rightItemType = rightType->Cast<TOptionalExprType>()->GetItemType();
        }

        auto arg1 = ctx.Expr.NewArgument(input->Pos(), "arg1");
        auto arg2 = input->ChildPtr(1);
        auto convertedArg1 = arg1;
        auto convertedArg2 = arg2;
        const TTypeAnnotationNode* commonItemType = nullptr;
        if (SilentInferCommonType(convertedArg2, *rightItemType, convertedArg1, *leftItemType, ctx.Expr, commonItemType,
            TConvertFlags().Set(NConvertFlags::AllowUnsafeConvert)) == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "uncompatible coalesce types, first type: " <<
                *leftType << ", second type: " << *rightType));
            return IGraphTransformer::TStatus::Error;
        }

        auto arg3 = ctx.Expr.NewArgument(input->Pos(), "arg3");
        bool isNarrowing =
            leftType->GetKind() != ETypeAnnotationKind::Optional ||
            TrySilentConvertTo(arg3, *leftType, *commonItemType, ctx.Expr) == IGraphTransformer::TStatus::Error;
        bool changedArg2 = (convertedArg2 != arg2);
        bool changedArg1 = isNarrowing ? (convertedArg1 != arg1) : (!convertedArg1->IsCallable("Just") || convertedArg1->HeadPtr() != arg1);

        auto retType = commonItemType;
        if (changedArg1 || changedArg2) {
            if (changedArg1) {
                auto lambda1 = ctx.Expr.NewLambda(input->Pos(), ctx.Expr.NewArguments(input->Pos(), { arg1 }), std::move(convertedArg1));
                if (leftType->GetKind() == ETypeAnnotationKind::Optional) {
                    input->ChildRef(0) = ctx.Expr.Builder(input->Pos())
                        .Callable(isNarrowing ? "Map" : "FlatMap")
                            .Add(0, input->HeadPtr())
                            .Add(1, lambda1)
                        .Seal()
                        .Build();
                } else {
                    input->HeadRef() = ctx.Expr.ReplaceNode(lambda1->TailPtr(), lambda1->Head().Head(), input->HeadPtr());
                }
            }

            if (changedArg2) {
                input->ChildRef(1) = convertedArg2;
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(retType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NvlWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        output = ctx.Expr.RenameNode(*input, "Coalesce");
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus CoalesceMembersWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto inputStruct = input->HeadPtr();
        if (IsNull(*inputStruct)) {
            output = inputStruct;
            return IGraphTransformer::TStatus::Repeat;
        }

        const TStructExprType* structType;
        if (inputStruct->GetTypeAnn() && inputStruct->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            structType = itemType->Cast<TStructExprType>();
        } else {
            if (!EnsureStructType(*inputStruct, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            structType = inputStruct->GetTypeAnn()->Cast<TStructExprType>();
        }

        auto coalesceColumns = input->ChildPtr(1);
        if (!EnsureTuple(*coalesceColumns, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TExprNodeList realCoalesceColumns;
        THashSet<TStringBuf> uniqColumns;
        for (auto coalesceColumn : coalesceColumns->ChildrenList()) {
            if (!EnsureAtom(*coalesceColumn, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            auto col = coalesceColumn->Content();
            if (structType->FindItem(col) && !uniqColumns.contains(col)) {
                realCoalesceColumns.push_back(coalesceColumn);
                uniqColumns.insert(col);
            }
        }

        if (realCoalesceColumns.size() < 2) {
            output = inputStruct;
        } else {
            TExprNodeList coalesceArgs;
            for (const auto& column : realCoalesceColumns) {
                coalesceArgs.push_back(
                    ctx.Expr.Builder(input->Pos())
                        .Callable("Member")
                            .Add(0, inputStruct)
                            .Add(1, column)
                        .Seal()
                        .Build()
                );
            }

            output = ctx.Expr.Builder(input->Pos())
                .Callable("ReplaceMember")
                    .Add(0, inputStruct)
                    .Add(1, realCoalesceColumns[0])
                    .Add(2, ctx.Expr.NewCallable(input->Pos(), "Coalesce", std::move(coalesceArgs)))
                .Seal()
                .Build();

            for (size_t i = 1; i < realCoalesceColumns.size(); ++i) {
                output = ctx.Expr.Builder(input->Pos())
                    .Callable("RemoveMember")
                        .Add(0, output)
                        .Add(1, realCoalesceColumns[i])
                    .Seal()
                    .Build();
            }
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus NanvlWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr) || !EnsureComputable(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional1 = false;
        const TDataExprType* dataType1 = nullptr;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional1, dataType1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto isDecimal = IsDataTypeDecimal(dataType1->GetSlot());
        if (!(isDecimal || IsDataTypeFloat(dataType1->GetSlot()))) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected Float, Double or Decimal, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Tail())) {
            auto type = ExpandType(input->Tail().Pos(), *ctx.Expr.MakeType<TOptionalExprType>(dataType1), ctx.Expr);
            output = ctx.Expr.ChangeChild(*input, 1, ctx.Expr.NewCallable(input->Tail().Pos(), "Nothing", {std::move(type)}));
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isOptional2 = false;
        const TDataExprType* dataType2 = nullptr;
        if (!EnsureDataOrOptionalOfData(input->Tail(), isOptional2, dataType2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isDecimal) {
            if (!IsDataTypeDecimal(dataType2->GetSlot())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder()
                    << "Expected Decimal, but got: " << *input->Tail().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            const auto paramsDataType1 = dataType1->Cast<TDataExprParamsType>();
            const auto paramsDataType2 = dataType2->Cast<TDataExprParamsType>();

            if (std::tie(paramsDataType1->GetParamOne(), paramsDataType1->GetParamTwo()) != std::tie(paramsDataType2->GetParamOne(), paramsDataType2->GetParamTwo())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder()
                    << "Expected Decimal parameters: (" << paramsDataType1->GetParamOne() <<  "," << paramsDataType1->GetParamTwo()
                    << ")  but got: (" << paramsDataType2->GetParamOne() << "," << paramsDataType2->GetParamTwo() << ")!"));
                return IGraphTransformer::TStatus::Error;
            }

            const TDataExprType* retDataType = ctx.Expr.MakeType<TDataExprParamsType>(
                paramsDataType1->GetSlot(), paramsDataType1->GetParamOne(), paramsDataType1->GetParamTwo());

            if (!retDataType->Cast<TDataExprParamsType>()->Validate(input->Pos(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            input->SetTypeAnn((isOptional1 || isOptional2) ?
                (const TTypeAnnotationNode*)ctx.Expr.MakeType<TOptionalExprType>(retDataType) :
                (const TTypeAnnotationNode*)retDataType);
        } else {
            if (!IsDataTypeFloat(dataType2->GetSlot())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder()
                    << "Expected Float or Double, but got: " << *input->Tail().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            const TDataExprType* retDataType = ctx.Expr.MakeType<TDataExprType>(
                (dataType1->GetSlot() == EDataSlot::Double || dataType2->GetSlot() == EDataSlot::Double) ? EDataSlot::Double : EDataSlot::Float);

            input->SetTypeAnn((isOptional1 || isOptional2) ?
                (const TTypeAnnotationNode*)ctx.Expr.MakeType<TOptionalExprType>(retDataType) :
                (const TTypeAnnotationNode*)retDataType);
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus UnwrapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null) {
            output = ctx.Expr.ChangeChild(*input, 0, ctx.Expr.Builder(input->Pos())
                .Callable("Nothing")
                    .Callable(0, "OptionalType")
                        .Callable(0, "VoidType").Seal()
                    .Seal()
                .Seal()
                .Build());

            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->ChildrenSize() > 1) {
            if (!EnsureStringOrUtf8Type(input->Tail(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PresortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().GetTypeAnn()->IsComparable()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected comparable type, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    template <NKikimr::NUdf::EDataSlot DataSlot>
    IGraphTransformer::TStatus DataGeneratorWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureNotInDiscoveryMode(*input, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureDependsOnTail(*input, ctx.Expr, 0)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(DataSlot));
        input->SetUnorderedChildren();
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TablePathWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (ctx.Types.StrictTableProps) {
            if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!EnsureMaxArgsCount(*input, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }
        if (input->ChildrenSize() > 0) {
            if (!EnsureDependsOn(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto depOn = input->Head().HeadPtr();
            if (ctx.Types.StrictTableProps && !EnsureStructType(*depOn, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (depOn->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
                auto structType = depOn->GetTypeAnn()->Cast<TStructExprType>();
                if (auto pos = structType->FindItem("_yql_sys_tablepath")) {
                    bool isOptional = false;
                    const TDataExprType* dataType = nullptr;
                    // Optional type may appear after UnionAll/UnionMerge with empty list
                    if (!EnsureDataOrOptionalOfData(depOn->Pos(), structType->GetItems()[*pos]->GetItemType(), isOptional, dataType, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!EnsureSpecificDataType(depOn->Pos(), *dataType, NUdf::EDataSlot::String, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    output = ctx.Expr.Builder(input->Pos())
                        .Callable("Member")
                            .Add(0, depOn)
                            .Atom(1, "_yql_sys_tablepath", TNodeFlags::Default)
                        .Seal()
                        .Build();
                    if (isOptional) {
                        output = ctx.Expr.Builder(input->Pos())
                            .Callable("Coalesce")
                                .Add(0, output)
                                .Callable(1, "String")
                                    .Atom(0, "", TNodeFlags::Default)
                                .Seal()
                            .Seal()
                            .Build();
                    }
                    return IGraphTransformer::TStatus::Repeat;
                }
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

template <NKikimr::NUdf::EDataSlot DataSlot>
    IGraphTransformer::TStatus CurrentTzWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        if (!EnsureNotInDiscoveryMode(*input, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureDependsOnTail(*input, ctx.Expr, 1)) {
            return IGraphTransformer::TStatus::Error;
        }

        TExprNode::TListType deps = input->ChildrenList();
        deps.erase(deps.begin());
        auto source = ctx.Expr.NewCallable(input->Pos(), "CurrentUtcTimestamp", std::move(deps));
        output = ctx.Expr.Builder(input->Pos())
            .Callable("Apply")
                .Callable(0, "Udf")
                    .Atom(0, TString("DateTime2.Make") + NUdf::GetDataTypeInfo(DataSlot).Name)
                .Seal()
                .Callable(1, "AddTimezone")
                    .Add(0, source)
                    .Callable(1, "TimezoneId")
                        .Add(0, input->ChildPtr(0))
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus TableRecordWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (ctx.Types.StrictTableProps) {
            if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!EnsureMaxArgsCount(*input, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }
        if (input->ChildrenSize() > 0) {
            if (!EnsureDependsOn(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto depOn = input->Head().HeadPtr();
            if (ctx.Types.StrictTableProps && !EnsureStructType(*depOn, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (depOn->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
                auto structType = depOn->GetTypeAnn()->Cast<TStructExprType>();
                if (auto pos = structType->FindItem("_yql_sys_tablerecord")) {
                    bool isOptional = false;
                    const TDataExprType* dataType = nullptr;
                    // Optional type may appear after UnionAll/UnionMerge with empty list
                    if (!EnsureDataOrOptionalOfData(depOn->Pos(), structType->GetItems()[*pos]->GetItemType(), isOptional, dataType, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!EnsureSpecificDataType(depOn->Pos(), *dataType, NUdf::EDataSlot::Uint64, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    output = ctx.Expr.Builder(input->Pos())
                        .Callable("Member")
                            .Add(0, depOn)
                            .Atom(1, "_yql_sys_tablerecord", TNodeFlags::Default)
                        .Seal()
                        .Build();
                    if (isOptional) {
                        output = ctx.Expr.Builder(input->Pos())
                            .Callable("Coalesce")
                                .Add(0, output)
                                .Callable(1, "String")
                                    .Atom(0, "0", TNodeFlags::Default)
                                .Seal()
                            .Seal()
                            .Build();
                    }
                    return IGraphTransformer::TStatus::Repeat;
                }
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus IsKeySwitchWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->ChildPtr(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto status = ConvertToLambda(input->ChildRef(2), ctx.Expr, 1).Combine(ConvertToLambda(input->ChildRef(3), ctx.Expr, 1));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(input->ChildRef(2), { input->Child(0)->GetTypeAnn() }, ctx.Expr) ||
            !UpdateLambdaAllArgumentsTypes(input->ChildRef(3), { input->Child(1)->GetTypeAnn() }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Child(2)->GetTypeAnn() || !input->Tail().GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*input->Child(2)->GetTypeAnn(), *input->Tail().GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                << "Key extractors must return same type from row and state, but got: "
                << *input->Child(2)->GetTypeAnn() << " from row, and " << *input->Tail().GetTypeAnn() << " from state."));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureEquatableType(input->Pos(), *input->Tail().GetTypeAnn(), ctx.Expr)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Key extractor must return equatable value, but got: " << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TableNameWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<TString> supportedSystems = { "yt", "kikimr", "rtmr" };
        if (auto p = FindPtr(supportedSystems, input->Tail().Content())) {
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder() << "Unknown system: "
                << input->Tail().Content() << ", supported: " << JoinSeq(",", supportedSystems)));
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.NewCallable(input->Pos(), to_title(TString(input->Tail().Content().substr(0, 2))) + "TableName",
            { input->HeadPtr() });
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus OptionalIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->Tail().GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus JustWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->Head().GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NothingWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() == ETypeAnnotationKind::Null) {
            output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (type->GetKind() != ETypeAnnotationKind::Optional) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected optional or Null type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus OptionalWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Optional) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected optional type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Tail().GetTypeAnn() || !IsSameAnnotation(*type->Cast<TOptionalExprType>()->GetItemType(),
            *input->Tail().GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()), TStringBuilder() << "Mismatch item type, expected: " <<
                *type->Cast<TOptionalExprType>()->GetItemType() << " but got: " << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToStringWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus VariantWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(2), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(2)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Variant) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Expected variant type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        auto varType = type->Cast<TVariantExprType>();
        const TTypeAnnotationNode* itemType;
        if (varType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Struct) {
            auto structType = varType->GetUnderlyingType()->Cast<TStructExprType>();
            auto pos = FindOrReportMissingMember(input->Child(1)->Content(), input->Pos(), *structType, ctx);
            if (!pos) {
                return IGraphTransformer::TStatus::Error;
            }

            itemType = structType->GetItems()[*pos]->GetItemType();
        } else {
            auto tupleType = varType->GetUnderlyingType()->Cast<TTupleExprType>();
            ui32 index = 0;
            if (!TryFromString(input->Child(1)->Content(), index)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (index >= tupleType->GetSize()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Index out of range. Index: " <<
                    index << ", size: " << tupleType->GetSize()));
                return IGraphTransformer::TStatus::Error;
            }

            itemType = tupleType->GetItems()[index];
        }

        auto convertStatus = TryConvertTo(input->ChildRef(0), *itemType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            return IGraphTransformer::TStatus::Error;
        } else if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus EnumWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("Variant")
                .Callable(0, "Void")
                .Seal()
                .Add(1, input->HeadPtr())
                .Add(2, input->TailPtr())
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus AsVariantWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("Variant")
                .Add(0, input->HeadPtr())
                .Add(1, input->TailPtr())
                .Callable(2, "VariantType")
                    .Callable(0, "StructType")
                        .List(0)
                            .Add(0, input->TailPtr())
                            .Callable(1, "TypeOf")
                                .Add(0, input->HeadPtr())
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus AsEnumWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("Variant")
                .Callable(0, "Void")
                .Seal()
                .Add(1, input->HeadPtr())
                .Callable(2, "VariantType")
                    .Callable(0, "StructType")
                        .List(0)
                            .Add(0, input->HeadPtr())
                            .Callable(1, "VoidType")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ToBytesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Dict) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected dict type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        auto dictType = type->Cast<TDictExprType>();
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        for (size_t i = 1; i < input->ChildrenSize(); ++i) {
            if (!EnsureTupleSize(*input->Child(i), 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!IsSameAnnotation(*input->Child(i)->Head().GetTypeAnn(), *keyType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Head().Pos()), TStringBuilder()
                    << "Mismatch type of key, expected: " << *keyType << ", but got: " << *input->Child(i)->Head().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            if (!IsSameAnnotation(*input->Child(i)->Child(1)->GetTypeAnn(), *payloadType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Child(1)->Pos()), TStringBuilder()
                    << "Mismatch type of payload, expected: " << *payloadType << ", but got: " << *input->Child(i)->Child(1)->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(dictType);
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool ContainsOrLookup, bool InList = false>
    IGraphTransformer::TStatus ContainsLookupWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if constexpr (ContainsOrLookup) {
            if (IsDataOrOptionalOfData(input->Head().GetTypeAnn())) {
                return WithWrapper(input, output, ctx);
            }
        }

        if (IsNull(input->Head())) {
            output = ContainsOrLookup ? MakeBool(input->Pos(), false, ctx.Expr) : input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        auto dictType = input->Head().GetTypeAnn();
        if (!dictType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                TStringBuilder() << "Expected (optional) " << (InList ? "List" : "Dict") << " type, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        if (dictType->GetKind() == ETypeAnnotationKind::Optional) {
            dictType = dictType->Cast<TOptionalExprType>()->GetItemType();
        }

        if constexpr (InList) {
            if (dictType->GetKind() == ETypeAnnotationKind::EmptyList) {
                output = MakeBool(input->Pos(), false, ctx.Expr);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureListType(input->Head().Pos(), *dictType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (dictType->GetKind() == ETypeAnnotationKind::EmptyDict) {
                if constexpr (ContainsOrLookup) {
                    output = MakeBool(input->Pos(), false, ctx.Expr);
                } else {
                    output = MakeNull(input->Pos(), ctx.Expr);
                }

                return IGraphTransformer::TStatus::Repeat;

            }

            if (!EnsureDictType(input->Head().Pos(), *dictType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!EnsurePersistable(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto keyType = InList ? dictType->Cast<TListExprType>()->GetItemType() : dictType->Cast<TDictExprType>()->GetKeyType();
        const auto payloadType = InList ? keyType : dictType->Cast<TDictExprType>()->GetPayloadType();
        const auto lookupType = input->Tail().GetTypeAnn();

        if (!EnsureEquatableType(input->Head().Pos(), *keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (NKikimr::NUdf::ECastOptions::Impossible & CastResult<true>(lookupType, keyType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Can't lookup " << *lookupType << " in set of " << *keyType));
            return IGraphTransformer::TStatus::Error;
        }

        if constexpr (ContainsOrLookup) {
            input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(payloadType));
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SqlInWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto collection = input->Child(0);
        const auto lookup = input->Child(1);
        const auto options = input->Child(2);

        if (!EnsureComputable(*collection, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsurePersistable(*lookup, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTuple(*options, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& option : options->Children()) {
            if (!EnsureTupleSize(*option, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(option->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto optionName = option->Head().Content();
            static const THashSet<TStringBuf> supportedOptions =
                {"isCompact", "tableSource", "nullsProcessed", "ansi", "warnNoAnsi"};
            if (!supportedOptions.contains(optionName)) {
                ctx.Expr.AddError(
                    TIssue(ctx.Expr.GetPosition(option->Pos()), TStringBuilder() << "Unknown IN option '" << optionName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const auto lookupType = lookup->GetTypeAnn();
        const bool isAnsi = HasSetting(*options, "ansi");

        auto collectionType = collection->GetTypeAnn();
        if (collectionType->GetKind() == ETypeAnnotationKind::Optional) {
            collectionType = collectionType->Cast<TOptionalExprType>()->GetItemType();
        }
        const auto collectionKind = collectionType->GetKind();

        auto issueWarningAndRestart = [&](const TString& warnPrefix) {
            auto issue = TIssue(ctx.Expr.GetPosition(input->Pos()),
                                warnPrefix + " Consider adding 'PRAGMA AnsiInForEmptyOrNullableItemsCollections;'");
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_LEGACY_IN_FOR_EMPTY_OR_NULLABLE, issue);
            if (!ctx.Expr.AddWarning(issue)) {
                return IGraphTransformer::TStatus::Error;
            }
            output = ctx.Expr.Builder(input->Pos())
                .Callable("SqlIn")
                    .Add(0, collection)
                    .Add(1, lookup)
                    .Add(2, RemoveSetting(*options, "warnNoAnsi", ctx.Expr))
                .Seal()
                .Build();
            return IGraphTransformer::TStatus::Repeat;
        };

        const TTypeAnnotationNode* collectionItemType = nullptr;
        if (!HasSetting(*options, "tableSource")) {
            switch (collectionKind) {
                case ETypeAnnotationKind::Tuple:
                {
                    const auto tupleType = collectionType->Cast<TTupleExprType>();
                    for (auto itemType : tupleType->GetItems()) {
                        YQL_ENSURE(itemType);
                        if (ECompareOptions::Uncomparable == CanCompare<true>(lookupType, itemType)) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                                                     TStringBuilder() << "Can't compare " << *lookupType << " with " << *itemType));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                    break;
                }
                case ETypeAnnotationKind::Dict:
                    collectionItemType = collectionType->Cast<TDictExprType>()->GetKeyType();
                    break;
                case ETypeAnnotationKind::List:
                    collectionItemType = collectionType->Cast<TListExprType>()->GetItemType();
                    break;
                case ETypeAnnotationKind::EmptyDict:
                case ETypeAnnotationKind::EmptyList:
                case ETypeAnnotationKind::Null:
                    break;
                default:
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(collection->Pos()),
                        TStringBuilder() << "IN only supports lookup in (optional) Tuple, List or Dict, "
                                         << "but got: " << *collectionType));
                    return IGraphTransformer::TStatus::Error;
            }
        } else {
            const TTypeAnnotationNode* listItemType = (collectionKind == ETypeAnnotationKind::List) ?
                                                      collectionType->Cast<TListExprType>()->GetItemType() : nullptr;
            if (!listItemType ||
                listItemType->GetKind() != ETypeAnnotationKind::Struct ||
                listItemType->Cast<TStructExprType>()->GetSize() != 1)
            {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(collection->Pos()),
                    TStringBuilder() << "expecting single column table source, but got: " << *collectionType));
                return IGraphTransformer::TStatus::Error;
            }

            collectionItemType = listItemType->Cast<TStructExprType>()->GetItems()[0]->GetItemType();
        }

        if (collectionItemType && ECompareOptions::Uncomparable == CanCompare<true>(lookupType, collectionItemType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Can't lookup " << *lookupType << " in collection of " << *collectionItemType
                                 << ": types " << *lookupType << " and " << *collectionItemType << " are not comparable"));
            return IGraphTransformer::TStatus::Error;
        }

        const bool collectionItemIsNullable = IsSqlInCollectionItemsNullable(NNodes::TCoSqlIn(input));
        if (!isAnsi && HasSetting(*options, "warnNoAnsi") && (collectionItemIsNullable || lookupType->HasOptionalOrNull())) {
            return issueWarningAndRestart("IN may produce unexpected result when used with nullable arguments.");
        }

        const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool);
        const bool collectionIsOptionalOrNull = collection->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional ||
                                                collection->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null;

        if (collectionIsOptionalOrNull || lookupType->HasOptionalOrNull() || (isAnsi && collectionItemIsNullable)) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    template <EDictItems Mode>
    IGraphTransformer::TStatus DictItemsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TDictExprType* dictType;
        bool isOptional;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (itemType->GetKind() == ETypeAnnotationKind::EmptyDict) {
                dictType = nullptr;
            } else {
                if (!EnsureDictType(input->Head().Pos(), *itemType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                dictType = itemType->Cast<TDictExprType>();
            }

            isOptional = true;
        }
        else {
            if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyDict) {
                dictType = nullptr;
            } else {
                if (!EnsureDictType(input->Head(), ctx.Expr)) {
                   return IGraphTransformer::TStatus::Error;
                }

                dictType = input->Head().GetTypeAnn()->Cast<TDictExprType>();
            }

            isOptional = false;
        }

        if (!dictType) {
            output = ctx.Expr.NewCallable(input->Pos(), "EmptyList", {});
            if (isOptional) {
               output = MakeConstMap(input->Pos(), input->HeadPtr(), output, ctx.Expr);
            }

            return IGraphTransformer::TStatus::Repeat;
        } else if (Mode == EDictItems::Both) {
            TTypeAnnotationNode::TListType items;
            items.push_back(dictType->GetKeyType());
            items.push_back(dictType->GetPayloadType());
            auto tupleType = ctx.Expr.MakeType<TTupleExprType>(items);
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(tupleType));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(Mode == EDictItems::Keys ?
                dictType->GetKeyType() : dictType->GetPayloadType()));
        }

        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AsStructWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (input->IsCallable("AsStructUnordered")) {
            // obsolete callable
            output = ctx.Expr.RenameNode(*input, "AsStruct");
            return IGraphTransformer::TStatus::Repeat;
        }

        TVector<const TItemExprType*> items;
        for (auto& child : input->Children()) {
            if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto nameNode = child->Child(0);
            if (!EnsureAtom(*nameNode, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto valueNode = child->Child(1);
            if (!EnsureComputable(*valueNode, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto memberType = valueNode->GetTypeAnn();
            if (!EnsureComputableType(valueNode->Pos(), *memberType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            items.push_back(ctx.Expr.MakeType<TItemExprType>(nameNode->Content(), memberType));
        }

        auto structType = ctx.Expr.MakeType<TStructExprType>(items);
        if (!structType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto less = [](const TExprNode::TPtr& left, const TExprNode::TPtr& right) {
            return left->Head().Content() < right->Head().Content();
        };

        if (!IsSorted(input->Children().begin(), input->Children().end(), less)) {
            auto list = input->ChildrenList();
            Sort(list.begin(), list.end(), less);
            output = ctx.Expr.ChangeChildren(*input, std::move(list));
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(structType);
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool IsStrict, bool IsSet>
    IGraphTransformer::TStatus AsDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (input->ChildrenSize() == 0) {
            output = ctx.Expr.NewCallable(input->Pos(), "EmptyDict", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSet && !EnsureTupleSize(input->Head(), 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto commonType = input->Head().GetTypeAnn();
        bool needRetype = false;
        for (size_t i = 1; i < input->ChildrenSize(); ++i) {
            const auto child = input->Child(i);
            if (!EnsureComputable(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!IsSet && !EnsureTupleSize(*child, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (IsSameAnnotation(*commonType, *child->GetTypeAnn())) {
                continue;
            }

            if (!IsStrict) {
                auto arg1 = ctx.Expr.NewArgument(input->Pos(), "arg");
                auto& arg2 = input->ChildRef(i);
                auto item1 = arg1;
                auto item2 = arg2;
                if (SilentInferCommonType(item1, *commonType, item2, *arg2->GetTypeAnn(), ctx.Expr, commonType)
                    != IGraphTransformer::TStatus::Error) {
                    needRetype = needRetype || (item2 != arg2);
                    arg2 = item2;
                    if (item1 != arg1) {
                        // need update all previous items
                        for (ui32 index = 0; index < i; ++index) {
                            input->ChildRef(index) = ctx.Expr.ReplaceNode(TExprNode::TPtr(item1), *arg1, input->ChildPtr(index));
                        }

                        return IGraphTransformer::TStatus::Repeat;
                    }

                    continue;
                }
            }

            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() <<
                "Cannot infer common type for : " << *commonType << " and " << *child->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (needRetype) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (IsStrict) {
            output = ctx.Expr.RenameNode(*input, IsSet ? "AsSet" : "AsDict");
            return IGraphTransformer::TStatus::Repeat;
        }

        const TDictExprType* dictType;
        if (!IsSet) {
            auto tupleType = commonType->Cast<TTupleExprType>();
            dictType = ctx.Expr.MakeType<TDictExprType>(tupleType->GetItems()[0], tupleType->GetItems()[1]);
        } else {
            dictType = ctx.Expr.MakeType<TDictExprType>(commonType, ctx.Expr.MakeType<TVoidExprType>());
        }

        if (!dictType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(dictType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DictFromKeysWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto typeNode = input->Child(0);
        auto keysListNode = input->Child(1);

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        const auto keyType = typeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureComputableType(typeNode->Pos(), *keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTuple(*keysListNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TExprNode::TListType newChildren;
        IGraphTransformer::TStatus totalStatus = IGraphTransformer::TStatus::Ok;
        for (size_t i = 0; i < keysListNode->ChildrenSize(); ++i) {
            newChildren.push_back(keysListNode->ChildPtr(i));
            auto& childRef = newChildren.back();
            auto status = TryConvertTo(childRef, *keyType, ctx.Expr);
            totalStatus = totalStatus.Combine(status);
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return status;
            }
        }

        if (totalStatus.Level != IGraphTransformer::TStatus::Ok) {
            input->ChildRef(1) = ctx.Expr.ChangeChildren(*keysListNode, std::move(newChildren));
            return totalStatus;
        }

        const TDictExprType* dictType = ctx.Expr.MakeType<TDictExprType>(keyType, ctx.Expr.MakeType<TVoidExprType>());
        if (!dictType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(dictType);
        return IGraphTransformer::TStatus::Ok;
    }

    template <bool IsStrict>
    IGraphTransformer::TStatus IfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto thenNode = input->Child(1);
        const auto elseNode = input->Child(2);
        const auto thenType = thenNode->GetTypeAnn();
        const auto elseType = elseNode->GetTypeAnn();
        if (IsStrict) {
            if (IsSameAnnotation(*thenType, *elseType)) {
                output = ctx.Expr.RenameNode(*input, "If");
                return IGraphTransformer::TStatus::Repeat;
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Different types of branches, then type: " <<
                    *thenType << ", else type: " << *elseType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const TTypeAnnotationNode* commonType = nullptr;
        if (SilentInferCommonType(input->ChildRef(1), input->ChildRef(2), ctx.Expr, commonType) == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Uncompatible types of branches, then type: " <<
                *thenType << ", else type: " << *elseType));
            return IGraphTransformer::TStatus::Error;
        }

        if (thenNode != input->Child(1) || elseNode != input->Child(2)) {
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(commonType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus IfWorldWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureWorldType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(*input->Child(1), EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() == 3) {
            auto children = input->ChildrenList();
            children.push_back(input->HeadPtr());
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureWorldType(*input->Child(3), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ForWorldWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureWorldType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() == 3) {
            auto children = input->ChildrenList();
            auto idLambda = ctx.Expr.Builder(input->Pos())
                .Lambda()
                    .Param("world")
                    .Arg("world")
                .Seal()
                .Build();

            children.push_back(idLambda);
            output = ctx.Expr.ChangeChildren(*input, std::move(children));
            return IGraphTransformer::TStatus::Repeat;
        }

        auto status = ConvertToLambda(input->ChildRef(3), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda2 = input->ChildRef(3);
        if (!UpdateLambdaAllArgumentsTypes(lambda2, { ctx.Expr.MakeType<TWorldExprType>() }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda2->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureWorldType(*lambda2->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsurePersistable(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const bool isNull = input->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null;
        if (isNull) {
            if (input->ChildrenSize() == 3) {
                output = input->HeadPtr();
            } else {
                output = ctx.Expr.ReplaceNode(lambda2->TailPtr(), lambda2->Head().Head(), input->HeadPtr());
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        auto listType = RemoveOptionalType(input->Child(1)->GetTypeAnn());
        if (!EnsureListType(input->Child(1)->Pos(), *listType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = listType->Cast<TListExprType>()->GetItemType();
        status = ConvertToLambda(input->ChildRef(2), ctx.Expr, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda1 = input->ChildRef(2);
        if (!UpdateLambdaAllArgumentsTypes(lambda1, { ctx.Expr.MakeType<TWorldExprType>(), itemType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda1->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureWorldType(*lambda1->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus IfPresentWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto argsCount = input->ChildrenSize() - 2U;
        auto args = input->ChildrenList();
        args.resize(argsCount);

        TTypeAnnotationNode::TListType types;
        types.reserve(args.size());
        for (const auto& arg : args) {
            if (IsNull(*arg)) {
                output = input->TailPtr();
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureOptionalType(*arg, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            types.emplace_back(arg->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType());
        }

        auto& lambda = input->ChildRef(argsCount);
        if (const auto status = ConvertToLambda(lambda, ctx.Expr, argsCount); status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, types, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        const auto thenType = lambda->GetTypeAnn();
        const auto elseType = input->Tail().GetTypeAnn();
        if (!IsSameAnnotation(*thenType, *elseType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "mismatch of then/else types, then type: "
                << *thenType << ", else type: " << *elseType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(thenType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus StaticMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (HasError(input->Head().GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Head().Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected either struct or tuple, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        bool isStruct = input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct;
        bool isTuple = input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple;
        if (!isStruct && !isTuple) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected either struct or tuple, but got: "
                << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (isTuple) {
            auto tupleType = input->Head().GetTypeAnn()->Cast<TTupleExprType>();
            TExprNode::TListType children;
            for (ui32 pos = 0; pos < tupleType->GetSize(); ++pos) {
                children.push_back(ctx.Expr.Builder(input->Pos())
                    .Apply(input->Child(1))
                        .With(0)
                            .Callable("Nth")
                                .Add(0, input->Child(0))
                                .Atom(1, ToString(pos))
                            .Seal()
                        .Done()
                    .Seal()
                    .Build());
            }

            output = ctx.Expr.NewList(input->Pos(), std::move(children));
        } else {
            auto structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
            TExprNode::TListType children;
            for (auto item : structType->GetItems()) {
                children.push_back(ctx.Expr.Builder(input->Pos())
                    .List()
                        .Atom(0, item->GetName())
                        .Apply(1, input->Child(1))
                            .With(0)
                                .Callable("Member")
                                    .Add(0, input->Child(0))
                                    .Atom(1, item->GetName())
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Build());
            }

            output = ctx.Expr.NewCallable(input->Pos(), "AsStruct", std::move(children));
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus StaticZipWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TStructExprType* argStruct = nullptr;
        TVector<TStringBuf> argNames;
        const TTupleExprType* argTuple = nullptr;

        auto getMemberNames = [](const TStructExprType& type) {
            TVector<TStringBuf> result;
            for (auto& item : type.GetItems()) {
                result.push_back(item->GetName());
            }
            Sort(result);
            return result;
        };

        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            auto child = input->Child(i);
            const TTypeAnnotationNode* childType = child->GetTypeAnn();
            if (HasError(childType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!childType) {
                YQL_ENSURE(child->Type() == TExprNode::Lambda);
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Expected either struct or tuple, but got lambda"));
                return IGraphTransformer::TStatus::Error;
            }

            if (argStruct) {
                if (childType->GetKind() != ETypeAnnotationKind::Struct) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                        TStringBuilder() << "Expected all arguments to be of Struct type, but got: " << *childType << " for " << i << "th argument"));
                    return IGraphTransformer::TStatus::Error;
                }
                auto childNames = getMemberNames(*childType->Cast<TStructExprType>());
                if (childNames != argNames) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                        TStringBuilder() << "Struct members mismatch. Members for first argument: " << JoinSeq(",", argNames)
                                         << ". Members for " << i << "th argument: " << JoinSeq(", ", childNames)));
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (argTuple) {
                if (childType->GetKind() != ETypeAnnotationKind::Tuple) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                        TStringBuilder() << "Expected all arguments to be of Tuple type, but got: " << *childType << " for " << i << "th argument"));
                    return IGraphTransformer::TStatus::Error;
                }
                auto childSize = childType->Cast<TTupleExprType>()->GetSize();
                if (childSize != argTuple->GetSize()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                        TStringBuilder() << "Tuple size mismatch. Got " << argTuple->GetSize()
                                         << " for first argument and " << childSize << " for " << i << "th argument"));
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (childType->GetKind() == ETypeAnnotationKind::Struct) {
                argStruct = childType->Cast<TStructExprType>();
                argNames = getMemberNames(*argStruct);
            } else if (childType->GetKind() == ETypeAnnotationKind::Tuple) {
                argTuple = childType->Cast<TTupleExprType>();
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Expected either struct or tuple, but got: " << *childType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (argStruct) {
            TExprNodeList asStructArgs;
            for (auto& name : argNames) {
                auto nameAtom = ctx.Expr.NewAtom(input->Pos(), name);
                TExprNodeList zippedValues;
                for (auto child : input->ChildrenList()) {
                    zippedValues.push_back(ctx.Expr.NewCallable(child->Pos(), "Member", { child , nameAtom }));
                }
                auto zipped = ctx.Expr.NewList(input->Pos(), std::move(zippedValues));
                asStructArgs.push_back(ctx.Expr.NewList(input->Pos(), { nameAtom, zipped }));
            }
            output = ctx.Expr.NewCallable(input->Pos(), "AsStruct", std::move(asStructArgs));
        } else {
            YQL_ENSURE(argTuple);
            TExprNodeList tupleArgs;
            for (size_t i = 0; i < argTuple->GetSize(); ++i) {
                auto idxAtom = ctx.Expr.NewAtom(input->Pos(), ToString(i), TNodeFlags::Default);
                TExprNodeList zippedValues;
                for (auto child : input->ChildrenList()) {
                    zippedValues.push_back(ctx.Expr.NewCallable(child->Pos(), "Nth", { child , idxAtom }));
                }
                auto zipped = ctx.Expr.NewList(input->Pos(), std::move(zippedValues));
                tupleArgs.push_back(zipped);
            }
            output = ctx.Expr.NewList(input->Pos(), std::move(tupleArgs));
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus TryRemoveAllOptionalsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto value = input->HeadPtr();
        if (HasError(value->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!value->GetTypeAnn()) {
            YQL_ENSURE(value->Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(value->Pos()), TStringBuilder() << "Expected either struct or tuple, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        bool isStruct = value->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct;
        bool isTuple = value->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple;
        if (!isStruct && !isTuple) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(value->Pos()), TStringBuilder() << "Expected either struct or tuple, but got: "
                                                                    << *value->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* resultType = nullptr;
        if (isTuple) {
            auto outputTypes = value->GetTypeAnn()->Cast<TTupleExprType>()->GetItems();
            for (auto& item : outputTypes) {
                if (item->GetKind() == ETypeAnnotationKind::Optional) {
                    item = item->Cast<TOptionalExprType>()->GetItemType();
                }
            }

            resultType = ctx.Expr.MakeType<TTupleExprType>(std::move(outputTypes));
        } else if (isStruct) {
            auto outputTypes = value->GetTypeAnn()->Cast<TStructExprType>()->GetItems();
            for (auto& item : outputTypes) {
                if (item->GetItemType()->GetKind() == ETypeAnnotationKind::Optional) {
                    item = ctx.Expr.MakeType<TItemExprType>(item->GetName(), item->GetItemType()->Cast<TOptionalExprType>()->GetItemType());
                }
            }

            resultType = ctx.Expr.MakeType<TStructExprType>(std::move(outputTypes));
        }

        resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        auto type = ExpandType(input->Pos(), *resultType, ctx.Expr);
        output = ctx.Expr.NewCallable(input->Pos(), "StrictCast", {std::move(value), std::move(type)});
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus HasNullWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TypeOfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().IsInspectable()) {
            output = ctx.Expr.NewCallable(input->Pos(), "UnitType", {});
            return IGraphTransformer::TStatus::Repeat;
        }

        output = ExpandType(input->Pos(), *input->Head().GetTypeAnn(), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ConstraintsOfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Json));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus InstanceOfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SourceOfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (ETypeAnnotationKind::Flow == type->GetKind()) {
            if (type->Cast<TFlowExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Null) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                    << "Expected flow of null, but got: " << *type));
                return IGraphTransformer::TStatus::Error;
            }
        } else if (ETypeAnnotationKind::Stream == type->GetKind()) {
            if (type->Cast<TStreamExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Null) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                    << "Expected stream of null, but got: " << *type));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                << "Expected flow or stream type, but got: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MatchTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() % 2U) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(4)->Pos()), TStringBuilder() << "Expected even arguments."));
            return IGraphTransformer::TStatus::Error;
        }

        const auto& param = input->Head();
        auto argType = param.GetTypeAnn();
        if (!argType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(param.Pos()), TStringBuilder() << "Lambda is unexpected here"));
            return IGraphTransformer::TStatus::Error;
        }

        auto argKind = argType->GetKind();
        if (argKind == ETypeAnnotationKind::Type) {
            argType = argType->Cast<TTypeExprType>()->GetType();
            argKind = argType->GetKind();
        }

        const auto argDataType = argKind == ETypeAnnotationKind::Data ? TMaybe<EDataSlot>(argType->Cast<TDataExprType>()->GetSlot()) : Nothing();

        auto index = input->ChildrenSize() - 1U;
        for (auto i = 1U; i < index; ++++i) {
            if (!EnsureAtom(*input->Child(i), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const TString matchType(input->Child(i)->Content());
            const auto find = TSyncFunctionsMap::Instance().TypeKinds.FindPtr(matchType);
            const auto dataType = !find ? NKikimr::NUdf::FindDataSlot(matchType) : Nothing();
            if (!find && !dataType) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), TStringBuilder() << "Unknown type: " << matchType));
                return IGraphTransformer::TStatus::Error;
            }

            const auto kind = find ? *find : ETypeAnnotationKind::Data;

            if (argKind == kind && argDataType == dataType) {
                index = ++i;
                break;
            }
        }

        const auto selected = input->Child(index);

        if (!EnsureLambda(*selected, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& args = selected->Head();
        auto body = selected->TailPtr();
        if (!EnsureMaxArgsCount(args, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = args.ChildrenSize() ? ctx.Expr.ReplaceNode(std::move(body), args.Head(), input->HeadPtr()) : std::move(body);
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus IfTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto param = input->HeadPtr();
        const auto type = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        const ui32 idx = IsSameAnnotation(*param->GetTypeAnn(), *type) ? 2 : 3;
        const auto selected = input->Child(idx);

        if (!EnsureLambda(*selected, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& args = selected->Head();
        auto body = selected->ChildPtr(1);
        if (!EnsureMaxArgsCount(args, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = args.ChildrenSize() ? ctx.Expr.ReplaceNode(std::move(body), args.Head(), std::move(param)) : std::move(body);
        return IGraphTransformer::TStatus::Repeat;
    }

    template <bool Strict>
    IGraphTransformer::TStatus TypeAssertWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        TString message;
        if (input->ChildrenSize() == 3) {
            if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            message += " (";
            message += input->Child(2)->Content();
            message += ")";
        }

        if (HasError(input->Head().GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().GetTypeAnn()) {
            YQL_ENSURE(input->Head().Type() == TExprNode::Lambda);
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected either type or computable value, but got lambda" << message));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* valueType;
        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Type) {
            valueType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        } else if (input->Head().GetTypeAnn()->IsComputable()) {
            valueType = input->Head().GetTypeAnn();
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected either type or computable value, but got: " << *input->Head().GetTypeAnn() << message));
            return IGraphTransformer::TStatus::Error;
        }

        auto targetType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (Strict) {
            if (!IsSameAnnotation(*valueType, *targetType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Mismatch types: "
                    << *valueType << " != " << *targetType << message));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            auto arg = ctx.Expr.NewArgument(input->Pos(), "arg");
            auto status = TrySilentConvertTo(arg, *valueType, *targetType, ctx.Expr, TConvertFlags().Set(NConvertFlags::AllowUnsafeConvert));
            if (status.Level == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Cannot convert type "
                    << *valueType << " into " << *targetType << message));
                return IGraphTransformer::TStatus::Error;
            }
        }

        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus PersistableAssertWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto nodePtr = input->HeadPtr();
        if (!nodePtr->IsPersistable()) {
            auto issue = TIssue(ctx.Expr.GetPosition(nodePtr->Pos()), "Persistable required. Atom, key, world, datasink, datasource, callable, resource, stream and lambda are not persistable");
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_NON_PERSISTABLE_ENTITY, issue);
            if (!ctx.Expr.AddWarning(issue)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        output = nodePtr;
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus PersistableReprWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto nodePtr = input->HeadPtr();
        if (nodePtr->IsPersistable()) {
            output = nodePtr;
            return IGraphTransformer::TStatus::Repeat;
        }

        auto converted = MakeRepr(nodePtr, nodePtr->GetTypeAnn(), ctx.Expr);
        if (converted.first) {
            output = converted.first;
            return IGraphTransformer::TStatus::Repeat;
        }

        auto issue = TIssue(ctx.Expr.GetPosition(nodePtr->Pos()), "Persistable required. Atom, key, world, datasink, datasource, callable, resource, stream and lambda are not persistable");
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_NON_PERSISTABLE_ENTITY, issue);
        if (!ctx.Expr.AddWarning(issue)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = nodePtr;
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus TupleSizeAssertWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        ui32 tupleSize = 0;
        if (!TryFromString(input->Child(1)->Content(), tupleSize)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Expected number, but got: "
                << input->Child(1)->Content()));
            return IGraphTransformer::TStatus::Error;
        }
        auto nodePtr = input->HeadPtr();
        if (!EnsureTupleTypeSize(*nodePtr, tupleSize, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = nodePtr;
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus EnsureWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinMaxArgsCount(*input, 2, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TDataExprType* dataType;
        if (bool isOptional; !EnsureDataOrOptionalOfData(*input->Child(1), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Child(1)->Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() > 2) {
            if (!EnsureStringOrUtf8Type(input->Tail(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToIndexDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TListExprType* listType;
        bool isOptional;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureListType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            listType = itemType->Cast<TListExprType>();
            isOptional = true;
        }
        else {
            if (!EnsureListType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            listType = input->Head().GetTypeAnn()->Cast<TListExprType>();
            isOptional = false;
        }

        auto ui64Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        auto dictType = ctx.Expr.MakeType<TDictExprType>(ui64Type, listType->GetItemType());
        input->SetTypeAnn(dictType);
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ToDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TListExprType* listType;
        bool isOptional;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (IsEmptyList(*itemType)) {
                output = ctx.Expr.NewCallable(input->Pos(), "EmptyDict", {});
                output = MakeConstMap(input->Pos(), input->HeadPtr(), output, ctx.Expr);
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureListType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            listType = itemType->Cast<TListExprType>();
            isOptional = true;
        } else {
            if (IsEmptyList(input->Head())) {
                output = ctx.Expr.NewCallable(input->Pos(), "EmptyDict", {});
                return IGraphTransformer::TStatus::Repeat;
            }

            if (!EnsureListType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            listType = input->Head().GetTypeAnn()->Cast<TListExprType>();
            isOptional = false;
        }

        const TTypeAnnotationNode* itemType = listType->Cast<TListExprType>()->GetItemType();
        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        status = ConvertToLambda(input->ChildRef(2), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda1 = input->ChildRef(1);
        if (!UpdateLambdaAllArgumentsTypes(lambda1, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambda2 = input->ChildRef(2);
        if (!UpdateLambdaAllArgumentsTypes(lambda2, {itemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda1->GetTypeAnn() || !lambda2->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        TMaybe<bool> isMany;
        TMaybe<bool> isHashed;
        TMaybe<ui64> itemsCount;
        bool isCompact;
        TMaybe<TIssue> error = ParseToDictSettings(*input, ctx.Expr, isMany, isHashed, itemsCount, isCompact);
        if (error) {
            ctx.Expr.AddError(*error);
            return IGraphTransformer::TStatus::Error;
        }

        auto keyType = lambda1->GetTypeAnn();
        auto payloadType = lambda2->GetTypeAnn();
        if (*isMany) {
            payloadType = ctx.Expr.MakeType<TListExprType>(payloadType);
        }

        auto dictType = ctx.Expr.MakeType<TDictExprType>(keyType, payloadType);
        if (!dictType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (*isHashed) {
            if (!keyType->IsEquatable() || !keyType->IsHashable()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                    << "Expected equatable and hashable key type for hashed dict, but got: " << *keyType));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!keyType->IsComparable()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                    << "Expected comparable key type for sorted dict, but got: " << *keyType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(dictType);
        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    template<bool Narrow>
    IGraphTransformer::TStatus SqueezeToDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if constexpr (Narrow) {
            if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            itemType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        } else {
            if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        auto& lambda1 = input->ChildRef(1);
        auto& lambda2 = input->ChildRef(2);

        const auto width = Narrow ? itemType->Cast<TMultiExprType>()->GetSize() : 1U;
        if (const auto status = ConvertToLambda(lambda1, ctx.Expr, width); status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (const auto status = ConvertToLambda(lambda2, ctx.Expr, width); status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (const auto& items = Narrow ? itemType->Cast<TMultiExprType>()->GetItems() : TTypeAnnotationNode::TListType{itemType};
            !(UpdateLambdaAllArgumentsTypes(lambda1, items, ctx.Expr) && UpdateLambdaAllArgumentsTypes(lambda2, items, ctx.Expr))) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda1->GetTypeAnn() || !lambda2->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        TMaybe<bool> isMany;
        TMaybe<bool> isHashed;
        TMaybe<ui64> itemsCount;
        bool isCompact;
        if (const auto error = ParseToDictSettings(*input, ctx.Expr, isMany, isHashed, itemsCount, isCompact)) {
            ctx.Expr.AddError(*error);
            return IGraphTransformer::TStatus::Error;
        }

        auto keyType = lambda1->GetTypeAnn();
        auto payloadType = lambda2->GetTypeAnn();
        if (*isMany) {
            payloadType = ctx.Expr.MakeType<TListExprType>(payloadType);
        }

        auto dictType = ctx.Expr.MakeType<TDictExprType>(keyType, payloadType);
        if (!dictType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!*isHashed && !keyType->IsComparable()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                << "Expected comparable key type for sorted dict, but got: " << *keyType));
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow) {
            input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(dictType));
        } else {
            input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(dictType));
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus VoidWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TVoidExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NullWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TNullExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus EmptyListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TEmptyListExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus EmptyDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TEmptyDictExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ErrorWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected error type, but got: " << *type));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus OptionalReduceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*input->Head().GetTypeAnn(), *input->Child(1)->GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Mismatch types: "
                << *input->Head().GetTypeAnn() << " != " << *input->Child(1)->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* innerType = input->Head().GetTypeAnn();
        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            innerType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
        }

        auto status = ConvertToLambda(input->ChildRef(2), ctx.Expr, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto& lambda = input->ChildRef(2);
        if (!UpdateLambdaAllArgumentsTypes(lambda, {innerType, innerType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambda->GetTypeAnn(), *innerType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Mismatch lambda return type, "
                << *lambda->GetTypeAnn() << " != " << *innerType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    bool ValidateFileAlias(const TExprNode& aliasNode, TExtContext& ctx) {
        if (!EnsureAtom(aliasNode, ctx.Expr)) {
            return false;
        }

        const auto content = aliasNode.Content();
        if (!ctx.Types.UserDataStorage->ContainsUserDataBlock(content)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(aliasNode.Pos()), TStringBuilder() << "File not found: " << content));
            return false;
        }
        return true;
    }

    bool ValidateFolderAlias(const TExprNode& aliasNode, TExtContext& ctx) {
        if (!EnsureAtom(aliasNode, ctx.Expr)) {
            return false;
        }

        const auto content = aliasNode.Content();
        if (!ctx.Types.UserDataStorage->ContainsUserDataFolder(content)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(aliasNode.Pos()), TStringBuilder() << "Folder not found: " << content));
            return false;
        }

        return true;
    }

    IGraphTransformer::TStatus FilePathWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr) || !ValidateFileAlias(input->Head(), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureNotInDiscoveryMode(*input, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FolderPathWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr) || !ValidateFolderAlias(input->Head(), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureNotInDiscoveryMode(*input, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FileContentWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr) || !ValidateFileAlias(input->Head(), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureNotInDiscoveryMode(*input, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus FilesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            ctx.Expr.MakeType<TItemExprType>("Name", ctx.Expr.MakeType<TDataExprType>(EDataSlot::String)),
            ctx.Expr.MakeType<TItemExprType>("IsFolder", ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool)),
            ctx.Expr.MakeType<TItemExprType>("Url", ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String))),
            ctx.Expr.MakeType<TItemExprType>("Path", ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String)))
        })));

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AuthTokensWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            ctx.Expr.MakeType<TItemExprType>("Name", ctx.Expr.MakeType<TDataExprType>(EDataSlot::String)),
            ctx.Expr.MakeType<TItemExprType>("Category", ctx.Expr.MakeType<TDataExprType>(EDataSlot::String)),
            ctx.Expr.MakeType<TItemExprType>("Subcategory", ctx.Expr.MakeType<TDataExprType>(EDataSlot::String))
        })));

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus UdfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 7, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // (0) function name
        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto name = input->Head().Content();
        TStringBuf moduleName, funcName;
        if (!SplitUdfName(name, moduleName, funcName)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Invalid function name: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        // (1) run config value
        if (input->ChildrenSize() > 1) {
            if (!EnsureComputable(*input->Child(1), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        // (2) user type
        const TTypeAnnotationNode* userType = nullptr;
        if (input->ChildrenSize() > 2) {
            if (!input->Child(2)->IsCallable("Void")) {
                if (auto status = EnsureTypeRewrite(input->ChildRef(2), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                    return status;
                }

                userType = input->Child(2)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (userType->GetKind() == ETypeAnnotationKind::Void) {
                    userType = nullptr;
                }
            }
        }

        // (3) type config
        TStringBuf typeConfig = "";
        if (input->ChildrenSize() > 3) {
            if (!EnsureAtom(*input->Child(3), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            typeConfig = input->Child(3)->Content();
        }

        // (4) cached callable type
        const TCallableExprType* cachedType = nullptr;
        if (input->ChildrenSize() > 4) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(4), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            auto type = input->Child(4)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!EnsureCallableType(input->Child(4)->Pos(), *type, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            cachedType = type->Cast<TCallableExprType>();
        }

        // (5) cached run config type
        const TTypeAnnotationNode* cachedRunConfigType = nullptr;
        if (input->ChildrenSize() > 5) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(5), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            cachedRunConfigType = input->Child(5)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        }

        // (6) file alias
        TStringBuf fileAlias = "";
        if (input->ChildrenSize() > 6) {
            if (!EnsureAtom(*input->Child(6), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            fileAlias = input->Child(6)->Content();
        }

        if (input->ChildrenSize() != 7) {
            YQL_PROFILE_SCOPE(DEBUG, "ResolveUdfs");
            auto& cacheItem = ctx.Types.UdfTypeCache[std::make_tuple(TString(name), TString(typeConfig), userType)];
            auto& cachedFuncType = std::get<0>(cacheItem);
            auto& cachedRunConfigType = std::get<1>(cacheItem);
            auto& cachedNormalizedUserType = std::get<2>(cacheItem);
            if (!cachedFuncType) {
                IUdfResolver::TFunction description;
                description.Pos = ctx.Expr.GetPosition(input->Pos());
                description.Name = TString(name);
                description.UserType = userType;
                description.TypeConfig = typeConfig;
                for (const auto& cred : ctx.Types.Credentials) {
                    for (const auto& x : *cred) {
                        description.SecureParams[TString("token:") + x.first] = x.second.Content;
                        if (x.first.StartsWith("default_")) {
                            description.SecureParams[TString("cluster:") + x.first] = x.second.Content;
                        }
                    }
                }

                for (const auto& x : ctx.Types.DataSources) {
                    auto tokens = x->GetClusterTokens();
                    if (tokens) {
                        for (const auto& t : *tokens) {
                            description.SecureParams.insert(std::make_pair(TString("cluster:default_") + t.first, t.second));
                        }
                    }
                }

                if (ctx.Types.UserCredentials.OauthToken) {
                    description.SecureParams["api:oauth"] = ctx.Types.UserCredentials.OauthToken;
                }

                if (ctx.Types.UserCredentials.BlackboxSessionIdCookie) {
                    description.SecureParams["api:cookie"] = ctx.Types.UserCredentials.BlackboxSessionIdCookie;
                }

                TVector<IUdfResolver::TFunction*> functions;
                functions.push_back(&description);

                TIssueScopeGuard issueScope(ctx.Expr.IssueManager, [input, &ctx]() -> TIssuePtr {
                    return MakeIntrusive<TIssue>(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "At " << input->Head().Content());
                });

                if (!ctx.LoadUdfMetadata(functions)) {
                    return IGraphTransformer::TStatus::Error;
                }

                cachedFuncType = description.CallableType;
                cachedRunConfigType = description.RunConfigType ? description.RunConfigType : ctx.Expr.MakeType<TVoidExprType>();
                cachedNormalizedUserType = description.UserType ? description.NormalizedUserType : ctx.Expr.MakeType<TVoidExprType>();
            }

            TStringBuf typeConfig = "";
            if (input->ChildrenSize() > 3) {
                typeConfig = input->Child(3)->Content();
            }

            const auto callableTypeNode = ExpandType(input->Pos(), *cachedFuncType, ctx.Expr);
            const auto runConfigTypeNode = ExpandType(input->Pos(), *cachedRunConfigType, ctx.Expr);
            TExprNode::TPtr runConfigValue;
            if (input->ChildrenSize() > 1 && !input->Child(1)->IsCallable("Void")) {
                runConfigValue = input->ChildPtr(1);
            } else {
                if (cachedRunConfigType->GetKind() == ETypeAnnotationKind::Void) {
                    runConfigValue = ctx.Expr.NewCallable(input->Pos(), "Void", {});
                } else if (cachedRunConfigType->GetKind() == ETypeAnnotationKind::Optional) {
                    runConfigValue = ctx.Expr.NewCallable(input->Pos(), "Nothing", { runConfigTypeNode });
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Missing run config value for type: "
                        << *cachedRunConfigType << " in function " << name));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            auto foundAlias = ctx.Types.UdfModules.FindPtr(moduleName);
            TStringBuf fileAlias = foundAlias ? *foundAlias : "";
            auto ret = ctx.Expr.Builder(input->Pos())
                .Callable("Udf")
                    .Add(0, input->HeadPtr())
                    .Add(1, runConfigValue)
                    .Add(2, ExpandType(input->Pos(), *cachedNormalizedUserType, ctx.Expr))
                    .Atom(3, typeConfig)
                    .Add(4, callableTypeNode)
                    .Add(5, runConfigTypeNode)
                    .Atom(6, fileAlias)
                .Seal()
                .Build();

            output = ret;
            return IGraphTransformer::TStatus::Repeat;
        }

        auto status = TryConvertTo(input->ChildRef(1), *cachedRunConfigType, ctx.Expr);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            if (status.Level == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Mismatch type of run config in UDF function "
                    << name << ", typeConfig:" << typeConfig));
            }

            return status;
        }

        static const std::unordered_map<std::string_view, std::string_view> deprecated = {
            {"String.Reverse", "'Unicode::Reverse'"},
            {"String.ToLower", "'String::AsciiToLower' or 'Unicode::ToLower'"},
            {"String.ToUpper", "'String::AsciiToUpper' or 'Unicode::ToUpper'"},
            {"String.ToTitle", "'String::AsciiToTitle' or 'Unicode::ToTitle'"},
            {"String.Substring", "'SUBSTRING' builtin function"},
            {"String.Find", "'FIND' builtin function"},
            {"String.ReverseFind", "'RFIND' builtin function"},
        };

        if (const auto bad = deprecated.find(name); deprecated.cend() != bad) {
            auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Deprecated UDF function '" << moduleName << "::" << funcName << "', use " << bad->second << " instead.");
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_DEPRECATED_UDF_FUNCTION, issue);
            if (!ctx.Expr.AddWarning(issue)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(cachedType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ScriptUdfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureMaxArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // script type
        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // function name
        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // function type
        const TCallableExprType* callableType;
        {
            if (auto status = EnsureTypeRewrite(input->ChildRef(2), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
            const TTypeAnnotationNode* tn = input->Child(2)->GetTypeAnn();
            const TTypeExprType* type = tn->Cast<TTypeExprType>();
            if (!EnsureCallableType(input->Child(2)->Pos(), *type->GetType(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            callableType = type->GetType()->Cast<TCallableExprType>();
            if (!EnsureComputableType(input->Child(2)->Pos(), *callableType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        // script body
        if (!EnsureSpecificDataType(*input->Child(3), EDataSlot::String, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto moduleName = input->Head().Content();
        auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(moduleName);
        if (scriptType == NKikimr::NMiniKQL::EScriptType::Unknown) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Unknown script type: " << moduleName));
            return IGraphTransformer::TStatus::Error;
        }

        scriptType = NKikimr::NMiniKQL::CanonizeScriptType(scriptType);
        auto canonizedModuleName = NKikimr::NMiniKQL::ScriptTypeAsStr(scriptType);
        bool isCustomPython = NKikimr::NMiniKQL::IsCustomPython(scriptType);
        bool foundModule = false;

        // resolve script udf from external resources (files / urls)
        // (main usage of CustomPython)
        {
            TVector<IUdfResolver::TFunction*> functions;

            if (!ctx.LoadUdfMetadata(functions)) {
                return IGraphTransformer::TStatus::Error;
            }

            foundModule = ctx.Types.UdfModules.find(canonizedModuleName) != ctx.Types.UdfModules.end();
        }

        // fallback for preinstalled CustomPython case
        if (!foundModule) {
            foundModule = static_cast<bool>(ctx.Types.UdfResolver->GetSystemModulePath(canonizedModuleName));
        }

        if (!foundModule) {
            if (isCustomPython) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                    << "Module with CustomPython UDF not found\n"
                   "Provide it via url / file link or add it to your YQL installation"));
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                    << "Module not loaded for script type: " << canonizedModuleName));
            }
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() == 5) {
            if (!EnsureTuple(*input->Child(4), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            for (auto setting: input->Child(4)->Children()) {
                if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                auto nameNode = setting->Child(0);
                if (!EnsureAtom(*nameNode, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                if (nameNode->Content() == "cpu") {
                    if (!EnsureTupleSize(*setting, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    double val = 0.;
                    if (!TryFromString(setting->Child(1)->Content(), val) || val == 0.) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(nameNode->Pos()), TStringBuilder()
                            << "Bad " << TString{nameNode->Content()}.Quote() << " setting value: " << setting->Child(1)->Content()));
                        return IGraphTransformer::TStatus::Error;
                    }
                }
                else if (nameNode->Content() == "extraMem") {
                    ui64 val = 0;
                    if (!TryFromString(setting->Child(1)->Content(), val)) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(nameNode->Pos()), TStringBuilder()
                            << "Bad " << TString{nameNode->Content()}.Quote() << " setting value: " << setting->Child(1)->Content()));
                        return IGraphTransformer::TStatus::Error;
                    }
                }
                else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(nameNode->Pos()), TStringBuilder() << "Unsupported setting: " << nameNode->Content()));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        input->SetTypeAnn(callableType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ApplyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureCallableType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TIssueScopeGuard issueScope(ctx.Expr.IssueManager, [&]() {
            if (input->Head().IsCallable("Udf")) {
                return MakeIntrusive<TIssue>(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                    << "Callable is produced by Udf: " << input->Head().Head().Content());
            } else {
                return MakeIntrusive<TIssue>(ctx.Expr.GetPosition(input->Head().Pos()), "Callable is produced here");
            }
        });

        auto type = input->Head().GetTypeAnn()->Cast<TCallableExprType>();
        auto autoMapFunction = type->GetReturnType()->GetKind() == ETypeAnnotationKind::Optional ? "FlatMap" : "Map";
        auto tmpArg = ctx.Expr.NewArgument(input->Pos(), "tmp");
        if (!EnsureCallableMaxArgsCount(input->Pos(), input->ChildrenSize() - 1, type->GetArgumentsSize(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureCallableMinArgsCount(input->Pos(), input->ChildrenSize() - 1, type->GetArgumentsSize() - type->GetOptionalArgumentsCount(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto combinedStatus = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Ok);
        TVector<ui32> autoMapArgs;
        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            const auto& arg = type->GetArguments()[i - 1];
            const bool isAutoMap = arg.Flags & NKikimr::NUdf::ICallablePayload::TArgumentFlags::AutoMap;
            const auto srcType = input->Child(i)->GetTypeAnn();
            if (HasError(srcType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto convertStatus = TrySilentConvertTo(input->ChildRef(i), *arg.Type, ctx.Expr);
            if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                if (isAutoMap && srcType && (srcType->GetKind() == ETypeAnnotationKind::Optional || srcType->GetKind() == ETypeAnnotationKind::Null)) {
                    if (srcType->GetKind() == ETypeAnnotationKind::Null) {
                        auto retType = type->GetReturnType();
                        input->SetTypeAnn(retType);
                        if (retType->GetKind() != ETypeAnnotationKind::Optional) {
                            retType = ctx.Expr.MakeType<TOptionalExprType>(retType);
                        }

                        output = ctx.Expr.NewCallable(input->Pos(), "Nothing", { ExpandType(input->Pos(), *retType, ctx.Expr) });
                        return IGraphTransformer::TStatus::Repeat;
                    }

                    auto tmp = tmpArg;
                    convertStatus = TrySilentConvertTo(tmp, *srcType->Cast<TOptionalExprType>()->GetItemType(),
                        *arg.Type, ctx.Expr);

                    if (convertStatus.Level != IGraphTransformer::TStatus::Error) {
                        autoMapArgs.push_back(i);
                        continue;
                    }
                }
            }

            combinedStatus = combinedStatus.Combine(convertStatus);
            if (combinedStatus.Level == IGraphTransformer::TStatus::Error) {
                if (!srcType) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), TStringBuilder() << "Mismatch type argument #" << i
                        << ", source type: lambda, target type: " << *arg.Type));
                }
                else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(i)->Pos()), TStringBuilder() << "Mismatch type argument #" << i
                        << ", source type: " << *srcType << ", target type: " << *arg.Type));
                }
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (combinedStatus.Level != IGraphTransformer::TStatus::Ok) {
            return combinedStatus;
        }

        if (!autoMapArgs.empty()) {
            // build output
            TExprNode::TListType args;
            for (ui32 i = 0; i < autoMapArgs.size(); ++i) {
                args.push_back(ctx.Expr.NewArgument(input->Pos(), TStringBuilder() << "automap_" << i));
            }

            auto newChildren = input->ChildrenList();
            for (ui32 i = 0; i < autoMapArgs.size(); ++i) {
                newChildren[autoMapArgs[i]] = args[i];
            }

            auto call = ctx.Expr.NewCallable(input->Pos(), "Apply", std::move(newChildren));
            output = call;
            for (ui32 i = 0; i < autoMapArgs.size(); ++i) {
                auto lambda = ctx.Expr.NewLambda(input->Pos(),
                    ctx.Expr.NewArguments(input->Pos(), { args[i] }),
                    std::move(output));

                output = ctx.Expr.Builder(input->Pos())
                    .Callable(i == 0 ? autoMapFunction : "FlatMap")
                        .Add(0, input->Child(autoMapArgs[i]))
                        .Add(1, lambda)
                    .Seal()
                    .Build();
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(type->GetReturnType());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NamedApplyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureDependsOnTail(*input, ctx.Expr, 3)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureCallableType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TIssueScopeGuard issueScope(ctx.Expr.IssueManager, [&]() {
            if (input->Head().IsCallable("Udf")) {
                return MakeIntrusive<TIssue>(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
                    << "Callable is produced by Udf: " << input->Head().Head().Content());
            }
            else {
                return MakeIntrusive<TIssue>(ctx.Expr.GetPosition(input->Head().Pos()), "Callable is produced here");
            }
        });

        auto type = input->Head().GetTypeAnn()->Cast<TCallableExprType>();
        auto autoMapFunction = type->GetReturnType()->GetKind() == ETypeAnnotationKind::Optional ? "FlatMap" : "Map";
        auto tmpArg = ctx.Expr.NewArgument(input->Pos(), "tmp");
        // positional args
        if (!EnsureTupleType(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto tupleType = input->Child(1)->GetTypeAnn()->Cast<TTupleExprType>();
        auto tupleSize = tupleType->GetSize();
        if (!EnsureStructType(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = input->Child(2)->GetTypeAnn()->Cast<TStructExprType>();
        auto structSize = structType->GetSize();
        auto totalArgs = tupleSize + structSize;
        if (totalArgs > type->GetArgumentsSize()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Too many arguments, expected at most: "
                << type->GetArgumentsSize() << ", but got: " << totalArgs));
            return IGraphTransformer::TStatus::Error;
        }

        if (totalArgs < (type->GetArgumentsSize() - type->GetOptionalArgumentsCount())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Too few arguments, expected at least: "
                << (type->GetArgumentsSize() - type->GetOptionalArgumentsCount()) << ", but got: " << totalArgs));
            return IGraphTransformer::TStatus::Error;
        }

        TTypeAnnotationNode::TListType expectedTupleTypeItems;
        for (ui32 i = 0; i < tupleType->GetSize(); ++i) {
            expectedTupleTypeItems.push_back(type->GetArguments()[i].Type);
        }

        TVector<ui32> tupleAutoMaps;
        auto expectedTupleType = ctx.Expr.MakeType<TTupleExprType>(expectedTupleTypeItems);
        auto convertStatus = TrySilentConvertTo(input->ChildRef(1), *expectedTupleType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            bool hasError = false;
            for (ui32 i = 0; i < tupleType->GetSize(); ++i) {
                auto srcType = tupleType->GetItems()[i];
                auto dstType = expectedTupleType->GetItems()[i];
                auto tmp = tmpArg;
                auto convertStatus = TrySilentConvertTo(tmp, *srcType, *dstType, ctx.Expr);
                if (convertStatus == IGraphTransformer::TStatus::Error) {
                    const bool isAutoMap = type->GetArguments()[i].Flags & NKikimr::NUdf::ICallablePayload::TArgumentFlags::AutoMap;
                    if (isAutoMap && srcType->GetKind() == ETypeAnnotationKind::Optional) {
                        tmp = tmpArg;
                        convertStatus = TrySilentConvertTo(tmp, *srcType->Cast<TOptionalExprType>()->GetItemType(), *dstType, ctx.Expr);
                        if (convertStatus != IGraphTransformer::TStatus::Error) {
                            tupleAutoMaps.push_back(i);
                            continue;
                        }
                    }

                    hasError = true;
                }
            }

            if (hasError) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() <<
                    "Mismatch positional argument types, expected: " <<
                    *static_cast<const TTypeAnnotationNode*>(expectedTupleType) << ", but got: " <<
                    *input->Child(1)->GetTypeAnn()));
            } else {
                convertStatus = IGraphTransformer::TStatus::Ok;
            }
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        TVector<const TItemExprType*> expectedStructTypeItems;
        TSet<ui32> usedIndices;
        for (const auto& structItem : structType->GetItems()) {
            auto foundIndex = type->ArgumentIndexByName(structItem->GetName());
            if (!foundIndex) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Unknown argument name: " << structItem->GetName()));
                return IGraphTransformer::TStatus::Error;
            }

            if (*foundIndex < tupleSize) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Argument with name " << structItem->GetName()
                    << " was already used for positional argument #" << (1 + *foundIndex)));
                return IGraphTransformer::TStatus::Error;
            }

            expectedStructTypeItems.push_back(ctx.Expr.MakeType<TItemExprType>(structItem->GetName(),
                type->GetArguments()[*foundIndex].Type));

            usedIndices.insert(*foundIndex);
        }

        for (ui32 i = tupleSize; i < type->GetArgumentsSize() - type->GetOptionalArgumentsCount(); ++i) {
            if (!usedIndices.contains(i)) { 
                const auto& arg = type->GetArguments()[i];
                if (arg.Name.empty()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Argument # " << (i + 1)
                        << " is required, but has not been set"));
                }
                else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Argument '" << arg.Name
                        << "' is required, but has not been set"));
                }

                return IGraphTransformer::TStatus::Error;
            }
        }

        auto expectedStructType = ctx.Expr.MakeType<TStructExprType>(expectedStructTypeItems);
        if (!expectedStructType->Validate(input->Child(2)->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        convertStatus = TrySilentConvertTo(input->ChildRef(2), *expectedStructType, ctx.Expr);
        TVector<ui32> structAutoMaps;
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            bool hasError = false;
            for (ui32 i = 0; i < structType->GetSize(); ++i) {
                auto srcType = structType->GetItems()[i]->GetItemType();
                auto dstType = expectedStructType->GetItems()[i]->GetItemType();
                auto tmp = tmpArg;
                auto convertStatus = TrySilentConvertTo(tmp, *srcType, *dstType, ctx.Expr);
                if (convertStatus == IGraphTransformer::TStatus::Error) {
                    const bool isAutoMap = type->GetArguments()[i].Flags & NKikimr::NUdf::ICallablePayload::TArgumentFlags::AutoMap;
                    if (isAutoMap && srcType->GetKind() == ETypeAnnotationKind::Optional) {
                        tmp = tmpArg;
                        convertStatus = TrySilentConvertTo(tmp, *srcType->Cast<TOptionalExprType>()->GetItemType(), *dstType, ctx.Expr);
                        if (convertStatus != IGraphTransformer::TStatus::Error) {
                            structAutoMaps.push_back(i);
                            continue;
                        }
                    }

                    hasError = true;
                }
            }

            if (hasError) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() <<
                    "Mismatch named argument types, expected: " <<
                    *static_cast<const TTypeAnnotationNode*>(expectedStructType) << ", but got: " <<
                    *input->Child(2)->GetTypeAnn()));
            } else {
                convertStatus = IGraphTransformer::TStatus::Ok;
            }
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        if (!tupleAutoMaps.empty() || !structAutoMaps.empty()) {
            auto totalAutoMapArgs = tupleAutoMaps.size() + structAutoMaps.size();
            // build output
            TExprNode::TListType args;
            TExprNode::TListType inputArgs;
            TMap<ui32, ui32> indexToTupleIndex;
            for (ui32 i = 0; i < totalAutoMapArgs; ++i) {
                args.push_back(ctx.Expr.NewArgument(input->Pos(), TStringBuilder() << "automap_" << i));
            }

            for (ui32 i = 0; i < tupleAutoMaps.size(); ++i) {
                indexToTupleIndex[tupleAutoMaps[i]] = i;
                inputArgs.push_back(ctx.Expr.NewCallable(input->Pos(), "Nth", {
                    input->ChildPtr(1),
                    ctx.Expr.NewAtom(input->Pos(), ToString(tupleAutoMaps[i])) }));
            }

            TMap<ui32, ui32> indexToStructIndex;
            for (ui32 i = 0; i < structAutoMaps.size(); ++i) {
                auto name = structType->GetItems()[structAutoMaps[i]]->GetName();
                indexToStructIndex[structAutoMaps[i]] = i + tupleAutoMaps.size();
                inputArgs.push_back(ctx.Expr.NewCallable(input->Pos(), "Member", {
                    input->ChildPtr(2),
                    ctx.Expr.NewAtom(input->Pos(), name) }));
            }

            auto newChildren = input->ChildrenList();
            if (!tupleAutoMaps.empty()) {
                // update tuple arg
                TExprNode::TListType newTupleItems;
                for (ui32 i = 0; i < tupleType->GetSize(); ++i) {
                    auto it = indexToTupleIndex.find(i);
                    if (it == indexToTupleIndex.end()) {
                        newTupleItems.push_back(ctx.Expr.NewCallable(input->Pos(), "Nth", {
                            input->ChildPtr(1),
                            ctx.Expr.NewAtom(input->Pos(), ToString(i)) }));
                    } else {
                        newTupleItems.push_back(args[it->second]);
                    }
                }

                auto newTuple = ctx.Expr.NewList(input->Pos(), std::move(newTupleItems));
                newChildren[1] = std::move(newTuple);
            }

            if (!structAutoMaps.empty()) {
                // update struct arg
                TExprNode::TListType newStructItems;
                for (ui32 i = 0; i < structType->GetSize(); ++i) {
                    auto name = structType->GetItems()[i]->GetName();
                    auto it = indexToStructIndex.find(i);
                    TExprNode::TPtr value;
                    if (it == indexToStructIndex.end()) {
                        value = ctx.Expr.NewCallable(input->Pos(), "Member", {
                            input->ChildPtr(2),
                            ctx.Expr.NewAtom(input->Pos(), name) });
                    } else {
                        value = args[it->second];
                    }

                    newStructItems.push_back(ctx.Expr.NewList(input->Pos(), {
                        ctx.Expr.NewAtom(input->Pos(), name), value
                    }));
                }

                auto newStruct = ctx.Expr.NewCallable(input->Pos(), "AsStruct", std::move(newStructItems));
                newChildren[2] = std::move(newStruct);
            }

            auto call = ctx.Expr.NewCallable(input->Pos(), "NamedApply", std::move(newChildren));
            output = call;
            for (ui32 i = 0; i < totalAutoMapArgs; ++i) {
                auto lambda = ctx.Expr.NewLambda(input->Pos(),
                    ctx.Expr.NewArguments(input->Pos(), { args[i] }),
                    std::move(output));

                output = ctx.Expr.Builder(input->Pos())
                    .Callable(i == 0 ? autoMapFunction : "FlatMap")
                        .Add(0, inputArgs[i])
                        .Add(1, lambda)
                    .Seal()
                    .Build();
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(type->GetReturnType());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PositionalArgsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SqlCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 2, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto udfName = input->ChildPtr(0);

        if (!EnsureTupleMinSize(*input->Child(1), 1, ctx.Expr) || !EnsureTupleMaxSize(*input->Child(1), 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& positionalArgsNode = input->Child(1)->Head();
        if (!EnsureCallable(positionalArgsNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!positionalArgsNode.IsCallable("PositionalArgs")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Head().Pos()),
                TStringBuilder() << "Expecting PositionalArgs callable, but got: " << positionalArgsNode.Content()));
            return IGraphTransformer::TStatus::Error;
        }

        TExprNodeList positionalArgs = positionalArgsNode.ChildrenList();
        TExprNode::TPtr namedArgs;
        if (input->Child(1)->ChildrenSize() == 2) {
            namedArgs = input->Child(1)->TailPtr();
            if (!EnsureStructType(*namedArgs, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        TExprNode::TPtr customUserType;
        if (input->ChildrenSize() > 2) {
            if (auto status = EnsureTypeRewrite(input->ChildRef(2), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
            customUserType = input->ChildPtr(2);
        } else {
            customUserType = ctx.Expr.NewCallable(input->Pos(), "TupleType", {});
        }

        TExprNode::TPtr typeConfig;
        if (input->ChildrenSize() == 4) {
            typeConfig = input->TailPtr();
            if (!EnsureAtom(*typeConfig, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        TExprNode::TPtr udf = ctx.Expr.Builder(input->Pos())
            .Callable("Udf")
                .Add(0, udfName)
                .Callable(1, "Void").Seal()
                .Callable(2, "TupleType")
                    .Callable(0, "TupleType")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            size_t idx = 0;
                            for (auto& arg : positionalArgs) {
                                parent
                                    .Callable(idx++, "TypeOf")
                                        .Add(0, arg)
                                    .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        if (namedArgs) {
                            parent
                                .Callable(1, "TypeOf")
                                    .Add(0, namedArgs)
                                .Seal();
                        } else {
                            parent
                                .Callable(1, "StructType")
                                .Seal();
                        }
                        return parent;
                    })
                    .Add(2, customUserType)
                .Seal()
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    if (typeConfig) {
                        parent.Add(3, typeConfig);
                    }
                    return parent;
                })
            .Seal()
            .Build();

        TExprNodeList applyArgs = { udf };
        if (namedArgs) {
            applyArgs.push_back(ctx.Expr.NewList(input->Pos(), std::move(positionalArgs)));
            applyArgs.push_back(namedArgs);
        } else {
            applyArgs.insert(applyArgs.end(), positionalArgs.begin(), positionalArgs.end());
        }

        output = ctx.Expr.NewCallable(input->Pos(), namedArgs ? "NamedApply" : "Apply", std::move(applyArgs));
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus CallableWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureCallableType(input->Head().Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto callableType = type->Cast<TCallableExprType>();
        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, callableType->GetArgumentsSize());
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const auto lambda = input->Child(1);
        auto args = lambda->Child(0);
        if (const auto size = args->ChildrenSize()) {
            std::vector<const TTypeAnnotationNode*> argumentsAnnotations;
            argumentsAnnotations.reserve(size);
            for (const auto& arg : callableType->GetArguments()) {
                argumentsAnnotations.emplace_back(arg.Type);
            }
            if (!UpdateLambdaAllArgumentsTypes(input->ChildRef(1), argumentsAnnotations, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!UpdateLambdaArgumentsType(*lambda, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!IsSameAnnotation(*lambda->GetTypeAnn(), *callableType->GetReturnType())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() << "Mismatch of lambda return type: "
                << *lambda->GetTypeAnn() << " != " << *callableType->GetReturnType()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(callableType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NewMTRandWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        auto convertStatus = TryConvertTo(input->ChildRef(0), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TResourceExprType>("MTRand"));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NextMTRandWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureResourceType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TTypeAnnotationNode::TListType tupleItems(2);
        tupleItems[0] = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        tupleItems[1] = input->Head().GetTypeAnn();
        input->SetTypeAnn(ctx.Expr.MakeType<TTupleExprType>(tupleItems));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CastStructWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto type = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (type->GetKind() != ETypeAnnotationKind::Struct) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Expected struct type, but got: "
                << *type));
            return IGraphTransformer::TStatus::Error;
        }

        auto newStructType = type->Cast<TStructExprType>();
        auto oldStructType = input->Head().GetTypeAnn()->Cast<TStructExprType>();
        for (auto item : newStructType->GetItems()) {
            auto oldItem = oldStructType->FindItem(item->GetName());
            if (!oldItem) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to find member with name: " <<
                    item->GetName() << " in struct " << *input->Head().GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            auto oldItemType = oldStructType->GetItems()[oldItem.GetRef()]->GetItemType();
            if (!IsSameAnnotation(*item->GetItemType(), *oldItemType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert member with name: " <<
                    item->GetName() << " from type " << *oldItemType << " to type " << *item->GetItemType()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus GuessWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TVariantExprType* variantType;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureVariantType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            variantType = itemType->Cast<TVariantExprType>();
        }
        else {
            if (!EnsureVariantType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            variantType = input->Head().GetTypeAnn()->Cast<TVariantExprType>();
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            auto tupleType = variantType->GetUnderlyingType()->Cast<TTupleExprType>();
            ui32 index = 0;
            if (!TryFromString(input->Child(1)->Content(), index)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (index >= tupleType->GetSize()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Index out of range. Index: " <<
                    index << ", size: " << tupleType->GetSize()));
                return IGraphTransformer::TStatus::Error;
            }

            input->SetTypeAnn(tupleType->GetItems()[index]);
        } else {
            auto structType = variantType->GetUnderlyingType()->Cast<TStructExprType>();
            auto pos = FindOrReportMissingMember(input->Child(1)->Content(), input->Pos(), *structType, ctx);
            if (!pos) {
                return IGraphTransformer::TStatus::Error;
            }

            input->SetTypeAnn(structType->GetItems()[*pos]->GetItemType());
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus VariantItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional = false;
        const TVariantExprType* variantType;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            isOptional = true;
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureVariantType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            variantType = itemType->Cast<TVariantExprType>();
        }
        else {
            if (!EnsureVariantType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            variantType = input->Head().GetTypeAnn()->Cast<TVariantExprType>();
        }

        if (variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            auto tupleType = variantType->GetUnderlyingType()->Cast<TTupleExprType>();
            auto firstType = tupleType->GetItems()[0];
            for (size_t i = 1; i < tupleType->GetSize(); ++i) {
                if (firstType != tupleType->GetItems()[i]) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                        << "All Variant item types should be equal: " << GetTypeDiff(*firstType, *tupleType->GetItems()[i])));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(firstType);
        } else {
            auto structType = variantType->GetUnderlyingType()->Cast<TStructExprType>();
            auto firstType = structType->GetItems()[0]->GetItemType();
            for (size_t i = 1; i < structType->GetSize(); ++i) {
                if (firstType != structType->GetItems()[i]->GetItemType()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                        << "All Variant item types should be equal: " << GetTypeDiff(*firstType, *structType->GetItems()[i]->GetItemType())));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            input->SetTypeAnn(firstType);
        }

        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus VisitWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureVariantType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto variantType = input->Head().GetTypeAnn()->Cast<TVariantExprType>();
        TVector<bool> usedFields;
        ui32 usedCount = 0;
        const TTupleExprType* tupleType = nullptr;
        const TStructExprType* structType = nullptr;
        if (variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            tupleType = variantType->GetUnderlyingType()->Cast<TTupleExprType>();
            usedFields.resize(tupleType->GetSize());
        } else {
            structType = variantType->GetUnderlyingType()->Cast<TStructExprType>();
            usedFields.resize(structType->GetSize());
        }

        const TTypeAnnotationNode* resultType = nullptr;
        bool needRepeat = false;
        bool hasDefaultValue = false;
        TVector<std::pair<ui32, size_t>> indexOrder;
        for (ui32 index = 1; index < input->ChildrenSize(); ++index) {
            const TTypeAnnotationNode* currentType = nullptr;
            auto child = input->Child(index);
            if (child->IsAtom()) {
                const TTypeAnnotationNode* itemType;
                ui32 itemIndex;
                if (tupleType) {
                    ui32 index = 0;
                    if (!TryFromString(child->Content(), index)) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Failed to convert to integer: " << child->Content()));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (index >= tupleType->GetSize()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder()
                            << "Index out of range. Index: "
                            << index << ", size: " << tupleType->GetSize()));
                        return IGraphTransformer::TStatus::Error;
                    }

                    itemType = tupleType->GetItems()[index];
                    itemIndex = index;
                } else {
                    auto pos = FindOrReportMissingMember(child->Content(), child->Pos(), *structType, ctx);
                    if (!pos) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    itemType = structType->GetItems()[*pos]->GetItemType();
                    itemIndex = *pos;
                }

                if (usedFields[itemIndex]) {
                    if (tupleType) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Position "
                            << itemIndex << " was already used"));
                    } else {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Member "
                            << structType->GetItems()[itemIndex]->GetName() << " was already used"));
                    }

                    return IGraphTransformer::TStatus::Error;
                }
                indexOrder.emplace_back(itemIndex, index);

                usedFields[itemIndex] = true;
                ++usedCount;

                ++index;
                if (index == input->ChildrenSize()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), "Expected lambda after this argument"));
                    return IGraphTransformer::TStatus::Error;
                }

                auto status = ConvertToLambda(input->ChildRef(index), ctx.Expr, 1);
                if (status.Level != IGraphTransformer::TStatus::Ok) {
                    return status;
                }

                auto& lambda = input->ChildRef(index);
                if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!lambda->GetTypeAnn()) {
                    needRepeat = true;
                    continue;
                }

                currentType = lambda->GetTypeAnn();
            } else {
                if (index != input->ChildrenSize() - 1) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), "Default value should be in the end"));
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureComputable(*child, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                currentType = child->GetTypeAnn();
                hasDefaultValue = true;
            }

            if (currentType) {
                if (!resultType) {
                    resultType = currentType;
                } else {
                    if (!IsSameAnnotation(*resultType, *currentType)) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "mismatch of handler/default types: "
                            << *currentType << " != " << *resultType));
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }
        }

        if (!hasDefaultValue && usedCount != usedFields.size()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder()
                << "Not all alternatives are handled, total: "
                << usedFields.size() << ", handled: " << usedCount));
            return IGraphTransformer::TStatus::Error;
        }

        auto less = [](const std::pair<ui32, size_t>& left, const std::pair<ui32, size_t>& right) {
            return left.first < right.first;
        };

        if (!IsSorted(indexOrder.begin(), indexOrder.end(), less)) {
            Sort(indexOrder.begin(), indexOrder.end(), less);
            TExprNode::TListType list = input->ChildrenList();
            for (size_t i = 0; i < indexOrder.size(); ++i) {
                list[i * 2 + 1] = input->ChildPtr(indexOrder[i].second);
                list[i * 2 + 2] = input->ChildPtr(indexOrder[i].second + 1);
            }

            output = ctx.Expr.ChangeChildren(*input, std::move(list));
            needRepeat = true;
        }

        if (needRepeat) {
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus WayWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TVariantExprType* variantType;
        bool isOptional = false;
        if (input->Head().GetTypeAnn() && input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            isOptional = true;
            auto itemType = input->Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureVariantType(input->Head().Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            variantType = itemType->Cast<TVariantExprType>();
        }
        else {
            if (!EnsureVariantType(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            variantType = input->Head().GetTypeAnn()->Cast<TVariantExprType>();
        }

        if (variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint32));
        }
        else {
            input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Utf8));
        }

        if (isOptional) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SqlAccessWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(*input->Child(1))) {
            output = input->ChildPtr(1);
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* unpacked = RemoveOptionalType(input->Child(1)->GetTypeAnn());
        bool isYson = false;
        bool isYsonNode = false;
        bool isJson = false;
        bool isYsonAutoConvert = false;
        bool isYsonStrict = false;
        bool isYsonFast = false;

        if (input->ChildrenSize() == 4) {
            auto options = input->ChildPtr(3);
            for (ui32 i = 0; i < options->ChildrenSize(); ++i) {
                const TStringBuf optionName = options->Child(i)->Content();
                if (optionName == "yson_auto_convert") {
                    isYsonAutoConvert = true;
                } else if (optionName == "yson_strict") {
                    isYsonStrict = true;
                } else if (optionName == "yson_fast") {
                    isYsonFast = true;
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Unknown SqlAccess option: " << optionName));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (unpacked->GetKind() == ETypeAnnotationKind::Data && unpacked->Cast<TDataExprType>()->GetSlot() == EDataSlot::Yson) {
            isYson = true;
        }

        if (unpacked->GetKind() == ETypeAnnotationKind::Data && unpacked->Cast<TDataExprType>()->GetSlot() == EDataSlot::Json) {
            isJson = true;
        }

        if (unpacked->GetKind() == ETypeAnnotationKind::Resource && unpacked->Cast<TResourceExprType>()->GetTag() == (isYsonFast ? "Yson2.Node" : "Yson.Node")) {
            isYsonNode = true;
        }

        if (isYson || isJson || isYsonNode) {
            auto key = input->ChildPtr(2);
            if (input->Head().Content() == "tuple" || input->Head().Content() == "struct") {
                key = ctx.Expr.NewCallable(input->Pos(), "String", { std::move(key) });
            } else if (input->Head().Content() == "dict") {
                key = ctx.Expr.Builder(input->Pos())
                    .Callable("SafeCast")
                        .Add(0, std::move(key))
                        .Callable(1, "DataType")
                            .Atom(0, "String", TNodeFlags::Default)
                        .Seal()
                .Seal().Build();
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Unknown access mode: " << input->Head().Content()));
                return IGraphTransformer::TStatus::Error;
            }
            if (isYsonAutoConvert || isYsonStrict) {
                auto asStruct = ctx.Expr.Builder(input->Pos())
                    .Callable("AsStruct")
                        .List(0)
                            .Atom(0, "AutoConvert")
                            .Add(1, MakeBool(input->Pos(), isYsonAutoConvert, ctx.Expr))
                        .Seal()
                        .List(1)
                            .Atom(0, "Strict")
                            .Add(1, MakeBool(input->Pos(), isYsonStrict, ctx.Expr))
                        .Seal()
                    .Seal()
                    .Build();

                auto ysonOptions = ctx.Expr.Builder(input->Pos())
                    .Callable("NamedApply")
                        .Callable(0, "Udf")
                            .Atom(0, isYsonFast ? "Yson2.Options" : "Yson.Options", TNodeFlags::Default)
                        .Seal()
                        .List(1).Seal()
                        .Add(2, std::move(asStruct))
                    .Seal()
                    .Build();

                output = ctx.Expr.Builder(input->Pos())
                    .Callable("Apply")
                        .Callable(0, "Udf")
                            .Atom(0, isYsonFast ? "Yson2.Lookup" : "Yson.Lookup", TNodeFlags::Default)
                        .Seal()
                        .Add(1, input->ChildPtr(1))
                        .Add(2, std::move(key))
                        .Add(3, std::move(ysonOptions))
                    .Seal()
                    .Build();

            } else {
                output = ctx.Expr.Builder(input->Pos())
                    .Callable("Apply")
                        .Callable(0, "Udf")
                            .Atom(0, isYsonFast ? "Yson2.Lookup" : "Yson.Lookup", TNodeFlags::Default)
                        .Seal()
                        .Add(1, input->ChildPtr(1))
                        .Add(2, std::move(key))
                    .Seal()
                    .Build();
                }
            return IGraphTransformer::TStatus::Repeat;
        }

        if (input->Head().Content() == "tuple") {
            if (unpacked->GetKind() == ETypeAnnotationKind::Tuple) {
                output = ctx.Expr.NewCallable(input->Pos(), "Nth", { input->ChildPtr(1), input->ChildPtr(2) });
            }
            else if (unpacked->GetKind() == ETypeAnnotationKind::Variant) {
                output = ctx.Expr.NewCallable(input->Pos(), "Guess", { input->ChildPtr(1), input->ChildPtr(2) });
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder()
                    << "Expected (optional) tuple or variant based on it, but got: " << *input->Child(1)->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }
        else if (input->Head().Content() == "struct") {
            if (unpacked->GetKind() == ETypeAnnotationKind::Struct) {
                output = ctx.Expr.NewCallable(input->Pos(), "Member", { input->ChildPtr(1), input->ChildPtr(2) });
            }
            else if (unpacked->GetKind() == ETypeAnnotationKind::Tuple) {
                output = ctx.Expr.NewCallable(input->Pos(), "Nth", { input->ChildPtr(1), input->ChildPtr(2) });
            }
            else if (unpacked->GetKind() == ETypeAnnotationKind::Variant) {
                output = ctx.Expr.NewCallable(input->Pos(), "Guess", { input->ChildPtr(1), input->ChildPtr(2) });
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder()
                    << "Expected (optional) struct/tuple or variant based on it, but got: " << *input->Child(1)->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        }
        else if (input->Head().Content() == "dict") {
            if (unpacked->GetKind() == ETypeAnnotationKind::Dict) {
                output = ctx.Expr.NewCallable(input->Pos(), "Lookup", { input->ChildPtr(1), input->ChildPtr(2) });
            }
            else if (unpacked->GetKind() == ETypeAnnotationKind::List) {
                output = ctx.Expr.NewCallable(input->Pos(), "Lookup", {
                    ctx.Expr.NewCallable(input->Pos(), "ToIndexDict", { input->ChildPtr(1) }), input->ChildPtr(2) });
            } else if (unpacked->GetKind() == ETypeAnnotationKind::EmptyList || unpacked->GetKind() == ETypeAnnotationKind::EmptyDict) {
                output = ctx.Expr.NewCallable(input->Pos(), "Null", {});
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder()
                    << "Expected (optional) list or dict, but got: " << *input->Child(1)->GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Unknown access mode: " << input->Head().Content()));
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SqlProcessWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const size_t lastPos = input->ChildrenSize() - 1;
        if (!EnsureAtom(*input->Child(lastPos), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        size_t listArg = 0;
        if (!TryFromString<size_t>(input->Child(lastPos)->Content(), listArg) || listArg >= input->ChildrenSize() - 2) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(lastPos)->Pos()), TStringBuilder()
                << "Invalid value of list argument position: " << input->Child(lastPos)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        TExprNode::TListType applyChildren = input->ChildrenList();
        applyChildren.pop_back(); // Remove position of list argument

        if (input->Head().Type() != TExprNode::Lambda) {
            const TCallableExprType* callableType = input->Head().GetTypeAnn()->Cast<TCallableExprType>();

            if (applyChildren.size() < callableType->GetArgumentsSize() + 1 - callableType->GetOptionalArgumentsCount()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Invalid number of arguments "
                    << (applyChildren.size() - 1) << " to use with callable type " << FormatType(callableType)));
                return IGraphTransformer::TStatus::Error;
            }

            const bool expectList = callableType->GetArguments()[listArg].Type->GetKind() == ETypeAnnotationKind::List;
            if (expectList) {
                auto issue = TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "The Udf used in PROCESS accepts List argument type, which prevents some optimizations."
                    " Consider to rewrite it using Stream argument type");
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_NON_STREAM_BATCH_UDF, issue);
                if (!ctx.Expr.AddWarning(issue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            output = ctx.Expr.Builder(input->Pos())
                .Callable("OrderedLMap")
                    .Add(0, applyChildren[listArg + 1])
                    .Lambda(1)
                        .Param("stream")
                        .Callable("ToSequence")
                            .Callable(0, "Apply")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    for (size_t i = 0; i < applyChildren.size(); ++i) {
                                        if (i != listArg + 1) {
                                            parent.Add(i, applyChildren[i]);
                                        } else if (expectList) {
                                            parent.Callable(i, "ForwardList")
                                                .Arg(0, "stream")
                                                .Seal();
                                        } else {
                                            parent.Arg(i, "stream");
                                        }
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }
        else {
            auto lambda = input->HeadPtr();
            const auto args = lambda->Child(0);
            if (input->ChildrenSize() - 2 != args->ChildrenSize()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Different arguments count, lambda has "
                    << args->ChildrenSize() << " arguments, but provided " << (input->ChildrenSize() - 2)));
                return IGraphTransformer::TStatus::Error;
            }

            output = ctx.Expr.Builder(input->Pos())
                .Callable("OrderedLMap")
                    .Add(0, applyChildren[listArg + 1])
                    .Lambda(1)
                        .Param("stream")
                        .Callable("ToSequence")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                auto replacer = parent.Apply(0, lambda);
                                for (size_t i = 1; i < applyChildren.size(); ++i) {
                                    if (i != listArg + 1) {
                                        replacer.With(i - 1, applyChildren[i]);
                                    } else {
                                        replacer.With(i - 1, "stream");
                                    }
                                }
                                replacer.Seal();
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SqlReduceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TPositionHandle pos = input->Pos();

        TExprNode::TPtr extractKeyLambda = input->ChildPtr(1);
        TExprNode::TPtr udf = input->ChildPtr(2);
        TExprNode::TPtr udfInput = input->ChildPtr(3);

        if (udf->IsCallable("SqlReduceUdf")) {
            const TTypeAnnotationNode* positionalArgsUdfType = nullptr;
            if (extractKeyLambda->IsAtom()) {
                if (!udfInput->GetTypeAnn()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(pos), TStringBuilder() << "Lambda is not expected as last argument of " << input->Content()));
                    return IGraphTransformer::TStatus::Error;
                }
                positionalArgsUdfType = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{udfInput->GetTypeAnn()});
            } else {
                const TTypeAnnotationNode* itemType = nullptr;
                if (!EnsureNewSeqType<true>(input->Head(), ctx.Expr, &itemType)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto& keyExtractor = input->ChildRef(1U);
                auto& udfInputLambda = input->ChildRef(3U);

                auto status = ConvertToLambda(keyExtractor, ctx.Expr, 1);
                status = status.Combine(ConvertToLambda(udfInputLambda, ctx.Expr, 1));
                if (status.Level != IGraphTransformer::TStatus::Ok) {
                    return status;
                }

                if (!UpdateLambdaAllArgumentsTypes(keyExtractor, { itemType }, ctx.Expr) ||
                    !UpdateLambdaAllArgumentsTypes(udfInputLambda, { itemType }, ctx.Expr))
                {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!keyExtractor->GetTypeAnn() || !udfInputLambda->GetTypeAnn()) {
                    return IGraphTransformer::TStatus::Repeat;
                }

                positionalArgsUdfType = ctx.Expr.MakeType<TTupleExprType>(
                    TTypeAnnotationNode::TListType{ keyExtractor->GetTypeAnn(), ctx.Expr.MakeType<TStreamExprType>(udfInputLambda->GetTypeAnn()) }
                );
            }

            TExprNodeList udfArgs;
            // name
            udfArgs.push_back(udf->HeadPtr());
            // runConfig
            udfArgs.push_back(ctx.Expr.NewCallable(pos, "Void", {}));
            // userType
            udfArgs.push_back(
                ctx.Expr.Builder(pos)
                    .Callable("TupleType")
                        .Add(0, ExpandType(pos, *positionalArgsUdfType, ctx.Expr))
                        .Callable(1, "StructType")
                        .Seal()
                        .Add(2, udf->ChildPtr(1))
                    .Seal()
                    .Build()
            );

            if (udf->ChildrenSize() == 3)  {
                // typeConfig
                udfArgs.push_back(udf->TailPtr());
            }
            output = ctx.Expr.ChangeChild(*input, 2, ctx.Expr.NewCallable(pos, "Udf", std::move(udfArgs)));
            return IGraphTransformer::TStatus::Repeat;
        }

        if (extractKeyLambda->IsAtom()) {
            TExprNode::TPtr applied;
            if (udf->IsLambda()) {
                applied = ctx.Expr.Builder(pos)
                    .Apply(udf)
                        .With(0, udfInput)
                    .Seal()
                    .Build();
            } else {
                applied = ctx.Expr.Builder(pos)
                    .Callable("Apply")
                        .Add(0, udf)
                        .Add(1, udfInput)
                    .Seal()
                    .Build();
            }
            if (extractKeyLambda->Content() == "byAll") {
                output = ctx.Expr.NewCallable(pos, "ToSequence", { applied });
            } else if (extractKeyLambda->Content() == "byAllList") {
                output = ctx.Expr.Builder(pos)
                    .Callable("ForwardList")
                        .Callable(0, "ToStream")
                            .Add(0, applied)
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(pos), TStringBuilder() << "Expected 'byAll' ot 'byAllList' as second argument"));
                return IGraphTransformer::TStatus::Error;
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TExprNode::TPtr handler;

        if (input->Child(2)->Type() != TExprNode::Lambda) {
            const auto callableType = input->Child(2)->GetTypeAnn()->Cast<TCallableExprType>();

            if (callableType->GetArgumentsSize() != 2) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected callable with 2 arguments"));
                return IGraphTransformer::TStatus::Error;
            }

            const bool expectList = callableType->GetArguments()[1].Type->GetKind() == ETypeAnnotationKind::List;
            if (expectList) {
                auto issue = TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), "The Udf used in REDUCE accepts List argument type, which prevents some optimizations."
                    " Consider to rewrite it using Stream argument type");
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_NON_STREAM_BATCH_UDF, issue);
                if (!ctx.Expr.AddWarning(issue)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            handler = expectList ?
                ctx.Expr.Builder(input->Child(2)->Pos())
                    .Lambda()
                        .Param("key")
                        .Param("stream")
                        .Callable("ToStream")
                            .Callable(0, "ToSequence")
                                .Callable(0, "Apply")
                                    .Add(0, input->ChildPtr(2))
                                    .Arg(1, "key")
                                    .Callable(2, "ForwardList")
                                        .Callable(0, "Map")
                                            .Arg(0, "stream")
                                            .Add(1, input->TailPtr())
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal().Build():
                ctx.Expr.Builder(input->Child(2)->Pos())
                    .Lambda()
                        .Param("key")
                        .Param("stream")
                        .Callable("ToStream")
                            .Callable(0, "ToSequence")
                                .Callable(0, "Apply")
                                    .Add(0, input->ChildPtr(2))
                                    .Arg(1, "key")
                                    .Callable(2, "Map")
                                        .Arg(0, "stream")
                                        .Add(1, input->TailPtr())
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal().Build();
        } else {
            if (const auto& lambda = *input->Child(2); lambda.Head().ChildrenSize() != 2) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda.Pos()), TStringBuilder() << "Expected lambda with 2 arguments"));
                return IGraphTransformer::TStatus::Error;
            }

            handler = ctx.Expr.Builder(input->Child(2)->Pos())
                .Lambda()
                    .Param("key")
                    .Param("stream")
                    .Callable("ToStream")
                        .Callable(0, "ToSequence")
                            .Apply(0, *input->Child(2))
                                .With(0, "key")
                                .With(1)
                                    .Callable("Map")
                                        .Arg(0, "stream")
                                        .Add(1, input->TailPtr())
                                    .Seal()
                                .Done()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal().Build();
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("ForwardList")
                .Callable(0, "Chopper")
                    .Callable(0, "ToStream")
                        .Add(0, input->HeadPtr())
                    .Seal()
                    .Add(1, input->ChildPtr(1))
                    .Lambda(2)
                        .Param("key")
                        .Param("item")
                        .Callable("IsKeySwitch")
                            .Arg(0, "key")
                            .Arg(1, "item")
                            .Lambda(2)
                                .Param("k")
                                .Arg("k")
                            .Seal()
                            .Add(3, input->ChildPtr(1))
                        .Seal()
                    .Seal()
                    .Add(3, std::move(handler))
                .Seal()
            .Seal().Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    // 0 - function kind
    // 1 - function name
    // 2 - list of pair, settings ("key", value)
    IGraphTransformer::TStatus SqlExternalFunctionWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStringOrUtf8Type(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStringOrUtf8Type(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleMinSize(*input->Child(2), 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* outputType = nullptr;
        const TTypeAnnotationNode* inputType = nullptr;
        TSet<TString> usedParams;
        for (const auto &tuple: input->Child(2)->Children()) {
            if (!EnsureTupleSize(*tuple, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!EnsureAtom(tuple->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto paramName = ToString(tuple->Head().Content());
            if (!usedParams.insert(paramName).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(tuple->Pos()),
                                         TStringBuilder() << "WITH " << to_upper(paramName).Quote()
                                         << " clause should be specified only once"));
                return IGraphTransformer::TStatus::Error;
            } else if (paramName == "input_type" || paramName == "output_type") {
                if (!EnsureTypeWithStructType(*tuple->Child(1), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                if (paramName == "output_type") {
                    outputType = tuple->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                } else if (paramName == "input_type") {
                    inputType = tuple->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                }
            } else if (paramName == "concurrency" || paramName == "batch_size") {
                if (!EnsureSpecificDataType(*tuple->Child(1), EDataSlot::Int32, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                /*
                ui64 number = 0;
                if (!TryFromString(tuple->Child(1)->Content(), number)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(tuple->Pos()),
                                             TStringBuilder() << "Failed to convert to integer: " << tuple->Child(1)->Content()));
                    return IGraphTransformer::TStatus::Error;
                }*/
            } else if (paramName == "optimize_for") {
                if (!EnsureStringOrUtf8Type(*tuple->Child(1), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                /*
                if (const auto optimize = tuple->Child(1)->Content(); optimize != "call" && optimize != "latency") {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(tuple->Child(1)->Pos()), TStringBuilder() <<
                        "Unknown OPTIMIZE_FOR value, expected call or latency, but got: " << optimize));
                    return IGraphTransformer::TStatus::Error;
                }*/
            } else if (paramName == "connection") {
                // FindCredential
                if (!EnsureStringOrUtf8Type(*tuple->Child(1), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (paramName == "init") {
                if (!EnsureComputable(*tuple->Child(1), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(tuple->Pos()), TStringBuilder() <<
                    "Unknown param name: " << paramName.Quote()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (inputType == nullptr && outputType == nullptr) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() <<
                    "EXTERNAL FUNCTION should have INPUT_TYPE/OUTPUT_TYPE parameter"));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* nodeType;
        if (outputType != nullptr && inputType != nullptr) {
            // as transformation
            TCallableExprType::TArgumentInfo inputArgument;
            inputArgument.Flags = NKikimr::NUdf::ICallablePayload::TArgumentFlags::AutoMap;
            inputArgument.Name = "input";
            inputArgument.Type = ctx.Expr.MakeType<TListExprType>(inputType);
            TVector<TCallableExprType::TArgumentInfo> args(1, inputArgument);
            nodeType = ctx.Expr.MakeType<TCallableExprType>(
                    ctx.Expr.MakeType<TListExprType>(outputType),
                    args, 0, TStringBuf(""));
        } else if (outputType != nullptr) {
            // as source
            TVector<TCallableExprType::TArgumentInfo> args;
            nodeType = ctx.Expr.MakeType<TCallableExprType>(
                    ctx.Expr.MakeType<TListExprType>(outputType),
                    args, 0, TStringBuf(""));
        } else {
            // as writer
            nodeType = ctx.Expr.MakeType<TListExprType>(inputType);
        }

        input->SetTypeAnn(nodeType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SqlExtractKeyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(input->ChildRef(1), ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
            output = ctx.Expr.Builder(input->Pos())
                .Apply(input->ChildPtr(1))
                    .With(0, input->HeadPtr())
                .Seal()
                .Build();
        }
        else if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Variant) {
            auto underlyingType = input->Head().GetTypeAnn()->Cast<TVariantExprType>()->GetUnderlyingType();
            if (underlyingType->GetKind() == ETypeAnnotationKind::Tuple) {
                auto tupleTypeItems = underlyingType->Cast<TTupleExprType>()->GetItems();
                if (std::adjacent_find(tupleTypeItems.cbegin(), tupleTypeItems.cend(), std::not_equal_to<const TTypeAnnotationNode*>()) == tupleTypeItems.cend()) {
                    // All types are the same
                    output = ctx.Expr.Builder(input->Pos())
                        .Apply(input->ChildPtr(1))
                            .With(0)
                                .Callable("VariantItem")
                                    .Add(0, input->HeadPtr())
                                .Seal()
                            .Done()
                        .Seal()
                        .Build();
                }
                else { // Non equal types
                    output = ctx.Expr.Builder(input->Pos())
                        .Callable("Visit")
                            .Add(0, input->HeadPtr())
                            .Do([&input, &tupleTypeItems](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                for (size_t i = 0; i < tupleTypeItems.size(); ++i) {
                                    parent
                                        .Atom(i * 2 + 1, ToString(i))
                                        .Lambda(i * 2 + 2)
                                            .Param("item")
                                            .Apply(input->ChildPtr(1))
                                                .With(0, "item")
                                            .Seal()
                                        .Seal()
                                    .Seal();
                                }
                                return parent;
                            })
                        .Seal()
                        .Build();
                }
            }
            else { // underlyingType->GetKind() == ETypeAnnotationKind::Struct
                auto structTypeItems = underlyingType->Cast<TStructExprType>()->GetItems();
                if (std::adjacent_find(structTypeItems.cbegin(), structTypeItems.cend(),
                    [](const TItemExprType* t1, const TItemExprType* t2) { return t1->GetItemType() != t2->GetItemType(); }) == structTypeItems.cend())
                {
                    // All types are the same
                    output = ctx.Expr.Builder(input->Pos())
                        .Apply(input->ChildPtr(1))
                            .With(0)
                                .Callable("VariantItem")
                                    .Add(0, input->HeadPtr())
                                .Seal()
                            .Done()
                        .Seal()
                        .Build();
                }
                else { // Non equal types
                    output = ctx.Expr.Builder(input->Pos())
                        .Callable("Visit")
                            .Add(0, input->HeadPtr())
                            .Do([&input, &structTypeItems](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                for (size_t i = 0; i < structTypeItems.size(); ++i) {
                                    parent
                                        .Atom(i * 2 + 1, structTypeItems[i]->GetName())
                                        .Lambda(i * 2 + 2)
                                            .Param("item")
                                            .Apply(input->ChildPtr(1))
                                                .With(0, "item")
                                            .Seal()
                                        .Seal()
                                    .Seal();
                                }
                                return parent;
                            })
                        .Seal()
                        .Build();
                }
            }
        }
        else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected Struct or Variant type, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SqlReduceUdfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinMaxArgsCount(*input, 2, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (input->ChildrenSize() == 3 && !EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SqlProjectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsEmptyList(input->Head())) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head(), ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!itemType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                TStringBuilder() << "Expected Struct as a sequence item type, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }


        if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleMinSize(*input->Child(1), 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<const TItemExprType*> allItems;
        for (auto& item : input->Child(1)->Children()) {
            if (!item->IsCallable({"SqlProjectItem", "SqlProjectStarItem"})) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(item->Pos()),
                    TStringBuilder() << "Expected SqlProjectItem or SqlProjectStarItem as argument"));
                return IGraphTransformer::TStatus::Error;
            }

            if (item->IsCallable("SqlProjectStarItem")) {
                auto& structItems = item->GetTypeAnn()->Cast<TStructExprType>()->GetItems();
                allItems.insert(allItems.end(), structItems.begin(), structItems.end());
            } else {
                YQL_ENSURE(item->Child(1)->IsAtom());
                allItems.push_back(ctx.Expr.MakeType<TItemExprType>(item->Child(1)->Content(), item->GetTypeAnn()));
            }
        }

        auto resultStructType = ctx.Expr.MakeType<TStructExprType>(allItems);
        if (!resultStructType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resultType = MakeSequenceType(input->Child(0)->GetTypeAnn()->GetKind(),
            static_cast<const TTypeAnnotationNode&>(*resultStructType), ctx.Expr);

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    struct TPgFuncDesc {
        ui32 MinArgs;
        ui32 MaxArgs;
        EDataSlot ReturnType;
        TVector<EDataSlot> DataTypes;
    };

    class TPgFuncMap {
    public:
        static const TPgFuncMap& Instance() {
            return *Singleton<TPgFuncMap>();
        }

        THashMap<TString, TPgFuncDesc> Funcs;

        TPgFuncMap() {
            Funcs["substring"] = { 3, 3, EDataSlot::Utf8, { EDataSlot::Utf8, EDataSlot::Int32, EDataSlot::Int32 } };
        }
    };

    IGraphTransformer::TStatus PgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto name = input->Head().Content();
        const TTypeAnnotationNode* result = nullptr;
        TVector<const TTypeAnnotationNode*> argTypes;
        bool isNull = false;
        bool isOptional = false;
        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            auto type = input->Child(i)->GetTypeAnn();
            if (type->GetKind() == ETypeAnnotationKind::Null) {
                argTypes.push_back(type);
                isNull = true;
                result = type;
                continue;
            }

            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                type = RemoveOptionalType(type);
                isOptional = true;
            }

            argTypes.push_back(type);
        }

        const auto& funcs = TPgFuncMap::Instance().Funcs;
        auto it = funcs.find(name);
        if (it != funcs.end()) {
            const auto& desc = it->second;
            if (argTypes.size() > desc.MaxArgs || argTypes.size() < desc.MinArgs) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Incorrect arguments count: " << argTypes.size() << " for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            for (ui32 i = 0; i < argTypes.size(); ++i) {
                auto expectedType = desc.DataTypes[i];
                if (argTypes[i]->GetKind() != ETypeAnnotationKind::Null) {
                    if (argTypes[i]->GetKind() != ETypeAnnotationKind::Data) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                            TStringBuilder() << "Expected type " << expectedType << " for argument " << (i + 1) << ", but got: " << argTypes[i]->GetKind() << " for function: " << name));
                        return IGraphTransformer::TStatus::Error;
                    } else {
                        auto dataType = argTypes[i]->Cast<TDataExprType>()->GetSlot();
                        if (dataType != expectedType) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                                TStringBuilder() << "Expected type " << expectedType << " for argument " << (i + 1) << ", but got: " << dataType << " for function: " << name));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                }
            }

            result = ctx.Expr.MakeType<TDataExprType>(desc.ReturnType);
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported function: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        if (!isNull && isOptional && result->GetKind() != ETypeAnnotationKind::Optional) {
            result = ctx.Expr.MakeType<TOptionalExprType>(result);
        }

        input->SetTypeAnn(result);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgWindowCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto name = input->Child(0)->Content();
        if (!input->Child(1)->IsAtom() && !input->Child(1)->IsCallable("PgAnonWindow")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Expected either window name or reference to an inline window"));
            return IGraphTransformer::TStatus::Error;
        }

        if (name == "lead" || name == "lag") {
            if (input->ChildrenSize() != 3) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected one argument in " << name << " function"));
                return IGraphTransformer::TStatus::Error;
            }

            auto arg = input->Child(2)->GetTypeAnn();
            if (arg->GetKind() == ETypeAnnotationKind::Null) {
                input->SetTypeAnn(arg);
            } else if (arg->GetKind() == ETypeAnnotationKind::Optional) {
                input->SetTypeAnn(arg);
            } else {
                input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(arg));
            }
        } else if (name == "row_number") {
            if (input->ChildrenSize() != 2) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    "Expected no arguments in row_number function"));
                return IGraphTransformer::TStatus::Error;
            }

            auto result = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64));
            input->SetTypeAnn(result);
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported function: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgAggWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        bool overWindow = (input->Content() == "PgAggWindowCall");
        if (!EnsureMinArgsCount(*input, overWindow ? 2 : 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto name = input->Head().Content();
        if (overWindow) {
            if (!input->Child(1)->IsAtom() && !input->Child(1)->IsCallable("PgAnonWindow")) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    "Expected either window name or reference to an inline window"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const TTypeAnnotationNode* result = nullptr;
        TVector<const TTypeAnnotationNode*> argTypes;
        bool isNull = false;
        bool isOptional = false;
        for (ui32 i = overWindow ? 2 : 1; i < input->ChildrenSize(); ++i) {
            auto type = input->Child(i)->GetTypeAnn();
            if (type->GetKind() == ETypeAnnotationKind::Null) {
                argTypes.push_back(type);
                isNull = true;
                result = type;
                continue;
            }

            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                type = RemoveOptionalType(type);
                isOptional = true;
            }

            argTypes.push_back(type);
        }

        if (name == "count") {
            if (argTypes.size() > 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Too many arguments for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            isNull = false;
            isOptional = true;
            result = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64);
        } else if (name == "min" || name == "max") {
            if (argTypes.size() != 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected one argument for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            if (!isNull) {
                auto argType = argTypes[0];
                if (argType->GetKind() != ETypeAnnotationKind::Data) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected comparable type, but got: " << argType->GetKind() << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                auto slot = argType->Cast<TDataExprType>()->GetSlot();
                if (slot == EDataSlot::Utf8 || slot == EDataSlot::Int32 || slot == EDataSlot::Double || slot == EDataSlot::Bool) {
                    result = argType;
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected comparable type, but got: " << slot << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            isOptional = true;
        } else if (name == "sum") {
            if (argTypes.size() != 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    TStringBuilder() << "Expected one argument for function: " << name));
                return IGraphTransformer::TStatus::Error;
            }

            if (!isNull) {
                auto argType = argTypes[0];
                if (argType->GetKind() != ETypeAnnotationKind::Data) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected additive type, but got: " << argType->GetKind() << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                auto slot = argType->Cast<TDataExprType>()->GetSlot();
                if (slot == EDataSlot::Int32) {
                    result = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64);
                } else if (slot == EDataSlot::Double) {
                    result = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Double);
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                        TStringBuilder() << "Expected additive type, but got: " << slot << " for function: " << name));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            isOptional = true;
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported function: " << name));
            return IGraphTransformer::TStatus::Error;
        }

        if (!isNull && isOptional && result->GetKind() != ETypeAnnotationKind::Optional) {
            result = ctx.Expr.MakeType<TOptionalExprType>(result);
        }

        input->SetTypeAnn(result);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgQualifiedStarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgColumnRefWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& child : input->Children()) {
            if (!EnsureAtom(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgResultItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Head().IsList()) {
            for (const auto& x : input->Head().Children()) {
                if (!EnsureAtom(*x, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }
        } else {
            if (!EnsureAtom(input->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        bool hasType = false;
        if (!input->Child(1)->IsCallable("Void")) {
            hasType = true;
            if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        auto& lambda = input->ChildRef(2);
        const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 1 : 0);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!hasType) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, { input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType() }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(lambda->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgWhereWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool hasType = false;
        if (!input->Child(0)->IsCallable("Void")) {
            hasType = true;
            if (auto status = EnsureTypeRewrite(input->ChildRef(0), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        auto& lambda = input->ChildRef(1);
        const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 1 : 0);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!hasType) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, { input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType() }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(lambda->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Child(2)->Content() != "asc" && input->Child(2)->Content() != "desc") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()),
                TStringBuilder() << "Unsupported sort direction: " << input->Child(2)->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        bool hasType = false;
        if (!input->Child(0)->IsCallable("Void")) {
            hasType = true;
            if (auto status = EnsureTypeRewrite(input->ChildRef(0), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        auto& lambda = input->ChildRef(1);
        const auto status = ConvertToLambda(lambda, ctx.Expr, hasType ? 1 : 0);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!hasType) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, { input->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType() }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(lambda->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Child(1)->Content()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Window reference is not supported"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTuple(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& x : input->Child(2)->Children()) {
            if (!x->IsCallable("PgGroup")) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgGroup"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!EnsureTuple(*input->Child(3), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& x : input->Child(3)->Children()) {
            if (!x->IsCallable("PgSort")) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgSort"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!EnsureTuple(*input->Child(4), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool hasType = false;
        bool hasFrom = false;
        bool hasTo = false;
        bool hasFromValue = false;
        bool hasToValue = false;
        bool needFromValue = false;
        bool needToValue = false;

        for (const auto& x : input->Child(4)->Children()) {
            if (!EnsureTupleMinSize(*x, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(x->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto optionName = x->Head().Content();
            if (optionName == "exclude") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Excludes are not supported"));
                return IGraphTransformer::TStatus::Error;
            } else if (optionName == "from_value" || optionName == "to_value") {
                hasFromValue = hasFromValue || (optionName == "from_value");
                hasToValue = hasToValue || (optionName == "to_value");
                if (!EnsureTupleSize(*x, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!x->Tail().IsCallable("Int32")) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected Int32 as frame offset"));
                    return IGraphTransformer::TStatus::Error;
                }

                auto val = FromString<i32>(x->Tail().Head().Content());
                if (val < 0) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected non-negative value as frame offset"));
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (optionName == "type") {
                hasType = true;
                if (!EnsureTupleSize(*x, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(x->Tail(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto type = x->Tail().Content();
                if (type != "rows") {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), TStringBuilder() << "Unsupported frame type: " << type));
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (optionName == "from" || optionName == "to") {
                hasFrom = hasFrom || (optionName == "from");
                hasTo = hasTo || (optionName == "to");
                if (!EnsureTupleSize(*x, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(x->Tail(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto bound = x->Tail().Content();
                if (!(bound == "up" || bound == "p" || bound == "c" || bound == "f" || bound == "uf")) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), TStringBuilder() << "Unsupported frame bound: " << bound));
                    return IGraphTransformer::TStatus::Error;
                }

                if (bound == "p" || bound == "f") {
                    needFromValue = needFromValue || (optionName == "from");
                    needToValue = needToValue || (optionName == "to");
                }

                if (optionName == "from" && bound == "uf") {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Unbounded following is unsupported as start offset"));
                    return IGraphTransformer::TStatus::Error;
                }

                if (optionName == "to" && bound == "up") {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Unbounded preceding is unsupported as end offset"));
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), TStringBuilder() << "Unknown option: " << optionName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (hasType) {
            if (!hasFrom || !hasTo) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing offset specification in the frame"));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (hasFrom || hasTo) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Unexpected offset specification in the frame"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (needFromValue != hasFromValue || needToValue != hasToValue) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Wrong offset value in the frame"));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgAnonWindowWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 n;
        if (!TryFromString(input->Child(0)->Content(), n)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected number"));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    using TInputs = TVector<std::tuple<TString, const TStructExprType*, TMaybe<TColumnOrder>>>;

    bool ScanColumns(TExprNode::TPtr root, const TInputs& inputs, const THashSet<TString>& possibleAliases,
        bool* hasStar, bool& hasColumnRef, THashSet<TString>& refs, THashMap<TString, THashSet<TString>>* qualifiedRefs,
        TExtContext& ctx) {
        bool isError = false;
        VisitExpr(root, [&](const TExprNode::TPtr& node) {
            if (node->IsCallable("PgStar")) {
                if (!hasStar) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is not allowed here"));
                    isError = true;
                    return false;
                }

                if (*hasStar) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Duplicate star"));
                    isError = true;
                    return false;
                }

                if (hasColumnRef) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is incompatible to column reference"));
                    isError = true;
                    return false;
                }

                *hasStar = true;
                if (inputs.empty()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star can't be used without FROM"));
                    isError = true;
                    return false;
                }
            }
            else if (node->IsCallable("PgQualifiedStar")) {
                if (!hasStar || !qualifiedRefs) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is not allowed here"));
                    isError = true;
                    return false;
                }

                if (*hasStar) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is incompatible to column reference"));
                    isError = true;
                    return false;
                }

                hasColumnRef = true;
                if (inputs.empty()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Column reference can't be used without FROM"));
                    isError = true;
                    return false;
                }

                TString alias(node->Head().Content());
                if (possibleAliases.find(alias) == possibleAliases.end()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), TStringBuilder() << "Unknown alias: " << alias));
                    isError = true;
                    return false;
                }

                for (const auto& x : inputs) {
                    if (std::get<0>(x).empty() || alias != std::get<0>(x)) {
                        continue;
                    }

                    for (const auto& item : std::get<1>(x)->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            (*qualifiedRefs)[alias].insert(TString(item->GetName()));
                        }
                    }
                }
            }
            else if (node->IsCallable("PgColumnRef")) {
                if (hasStar && *hasStar) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Star is incompatible to column reference"));
                    isError = true;
                    return false;
                }

                hasColumnRef = true;
                if (inputs.empty()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), "Column reference can't be used without FROM"));
                    isError = true;
                    return false;
                }

                if (node->ChildrenSize() == 2 && possibleAliases.find(node->Head().Content()) == possibleAliases.end()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()), TStringBuilder() << "Unknown alias: " << node->Head().Content()));
                    isError = true;
                    return false;
                }

                ui32 matches = 0;
                for (const auto& x : inputs) {
                    if (node->ChildrenSize() == 2) {
                        if (std::get<0>(x).empty() || node->Head().Content() != std::get<0>(x)) {
                            continue;
                        }
                    }

                    auto pos = std::get<1>(x)->FindItem(node->Tail().Content());
                    if (pos) {
                        ++matches;
                        if (matches > 1) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node->Pos()),
                                TStringBuilder() << "Column reference is ambiguous: " << node->Tail().Content()));
                            isError = true;
                            return false;
                        }
                    }
                }

                refs.insert(TString(node->Tail().Content()));
            }

            return true;
        });

        return !isError;
    }

    bool ValidateWindowRefs(const TExprNode::TPtr& root, const TExprNode* windows, TExprContext& ctx) {
        bool isError = false;
        VisitExpr(root, [&](const TExprNode::TPtr& node) {
            if (node->IsCallable("PgWindowCall")) {
                if (!windows) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                        "No window definitions"));
                    isError = true;
                    return false;
                }

                auto ref = node->Child(1);
                if (ref->IsAtom()) {
                    auto name = ref->Content();
                    if (!name) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                            "Empty window name is not allowed"));
                        isError = true;
                        return false;
                    }

                    bool found = false;
                    for (const auto& x : windows->Children()) {
                        if (x->Head().Content() == name) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                            TStringBuilder() << "Not found window name: " << name));
                        isError = true;
                        return false;
                    }
                } else {
                    YQL_ENSURE(ref->IsCallable("PgAnonWindow"));
                    auto index = FromString<ui32>(ref->Head().Content());
                    if (index >= windows->ChildrenSize()) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                            "Wrong index of window"));
                        isError = true;
                        return false;
                    }
                }
            }

            return true;
        });

        return !isError;
    }

    void AddColumns(const TInputs& inputs, const bool* hasStar, const THashSet<TString>& refs,
        const THashMap<TString, THashSet<TString>>* qualifiedRefs,
        TVector<const TItemExprType*>& items) {
        for (const auto& x : inputs) {
            if (hasStar && *hasStar) {
                for (ui32 i = 0; i < std::get<1>(x)->GetSize(); ++i) {
                    auto item = std::get<1>(x)->GetItems()[i];
                    if (!item->GetName().StartsWith("_yql_")) {
                        items.push_back(item);
                    }
                }

                continue;
            }

            for (const auto& ref : refs) {
                auto pos = std::get<1>(x)->FindItem(ref);
                if (pos) {
                    items.push_back(std::get<1>(x)->GetItems()[*pos]);
                }
            }

            if (qualifiedRefs && qualifiedRefs->contains(std::get<0>(x))) {
                for (const auto& ref : qualifiedRefs->find(std::get<0>(x))->second) {
                    auto pos = std::get<1>(x)->FindItem(ref);
                    if (pos) {
                        items.push_back(std::get<1>(x)->GetItems()[*pos]);
                    }
                }
            }
        }
    }

    IGraphTransformer::TStatus RebuildLambdaColumns(const TExprNode::TPtr& root, const TExprNode::TPtr& argNode,
        TExprNode::TPtr& newRoot, const TInputs& inputs, TExprNode::TPtr* expandedColumns, TExtContext& ctx) {
        return OptimizeExpr(root, newRoot, [&](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
            if (node->IsCallable("PgStar")) {
                TExprNode::TListType orderAtoms;
                for (const auto& x : inputs) {
                    auto order = std::get<2>(x);
                    for (const auto& item : std::get<1>(x)->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            if (!order) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), item->GetName()));
                            }
                        }
                    }

                    if (order) {
                        for (const auto& o : *order) {
                            if (!o.StartsWith("_yql_")) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), o));
                            }
                        }
                    }
                }

                if (expandedColumns) {
                    *expandedColumns = ctx.Expr.NewList(node->Pos(), std::move(orderAtoms));
                }

                return argNode;
            }

            if (node->IsCallable("PgColumnRef")) {
                return ctx.Expr.Builder(node->Pos())
                    .Callable("Member")
                    .Add(0, argNode)
                    .Atom(1, node->Tail().Content())
                    .Seal()
                    .Build();
            }

            if (node->IsCallable("PgQualifiedStar")) {
                TExprNode::TListType members;
                for (const auto& x : inputs) {
                    if (std::get<0>(x).empty() || node->Head().Content() != std::get<0>(x)) {
                        continue;
                    }

                    auto order = std::get<2>(x);
                    TExprNode::TListType orderAtoms;
                    for (const auto& item : std::get<1>(x)->GetItems()) {
                        if (!item->GetName().StartsWith("_yql_")) {
                            if (!order) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), item->GetName()));
                            }

                            members.push_back(ctx.Expr.Builder(node->Pos())
                                .List()
                                .Atom(0, item->GetName())
                                .Callable(1, "Member")
                                .Add(0, argNode)
                                .Atom(1, item->GetName())
                                .Seal()
                                .Seal()
                                .Build());
                        }
                    }

                    if (order) {
                        for (const auto& o : *order) {
                            if (!o.StartsWith("_yql_")) {
                                orderAtoms.push_back(ctx.Expr.NewAtom(node->Pos(), o));
                            }
                        }
                    }

                    if (expandedColumns) {
                        *expandedColumns = ctx.Expr.NewList(node->Pos(), std::move(orderAtoms));
                    }

                    return ctx.Expr.NewCallable(node->Pos(), "AsStruct", std::move(members));
                }

                YQL_ENSURE(false, "missing input");
            }

            return node;
        }, ctx.Expr, TOptimizeExprSettings(nullptr));
    }

    void MakeOptionalColumns(const TStructExprType*& structType, TExprContext& ctx) {
        bool needRebuild = false;
        for (const auto& item : structType->GetItems()) {
            if (item->GetItemType()->GetKind() != ETypeAnnotationKind::Optional
                && item->GetItemType()->GetKind() != ETypeAnnotationKind::Null) {
                needRebuild = true;
                break;
            }
        }

        if (!needRebuild) {
            return;
        }

        auto newItems = structType->GetItems();
        for (auto& item : newItems) {
            if (item->GetItemType()->GetKind() != ETypeAnnotationKind::Optional
                && item->GetItemType()->GetKind() != ETypeAnnotationKind::Null) {
                item = ctx.MakeType<TItemExprType>(item->GetName(), ctx.MakeType<TOptionalExprType>(item->GetItemType()));
            }
        }

        structType = ctx.MakeType<TStructExprType>(newItems);
    }

    bool ValidateGroups(const TInputs& inputs, const THashSet<TString>& possibleAliases,
        const TExprNode& data, TExtContext& ctx, TExprNode::TListType& newGroups) {
        newGroups.clear();
        bool hasColumnRef = false;
        for (const auto& group : data.Children()) {
            if (!group->IsCallable("PgGroup")) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(group->Pos()), "Expected PgGroup"));
                return false;
            }

            YQL_ENSURE(group->Tail().IsLambda());
            THashSet<TString> refs;
            THashMap<TString, THashSet<TString>> qualifiedRefs;
            if (group->Child(0)->IsCallable("Void")) {
                // no effective type yet, scan lambda body
                if (!ScanColumns(group->Tail().TailPtr(), inputs, possibleAliases, nullptr, hasColumnRef,
                    refs, &qualifiedRefs, ctx)) {
                    return false;
                }

                TVector<const TItemExprType*> items;
                AddColumns(inputs, nullptr, refs, &qualifiedRefs, items);
                auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                if (!effectiveType->Validate(group->Pos(), ctx.Expr)) {
                    return false;
                }

                auto typeNode = ExpandType(group->Pos(), *effectiveType, ctx.Expr);

                auto argNode = ctx.Expr.NewArgument(group->Pos(), "row");
                auto arguments = ctx.Expr.NewArguments(group->Pos(), { argNode });
                TExprNode::TPtr newRoot;
                auto status = RebuildLambdaColumns(group->Tail().TailPtr(), argNode, newRoot, inputs, nullptr, ctx);
                if (status == IGraphTransformer::TStatus::Error) {
                    return false;
                }

                auto newLambda = ctx.Expr.NewLambda(group->Pos(), std::move(arguments), std::move(newRoot));

                auto newChildren = group->ChildrenList();
                newChildren[0] = typeNode;
                newChildren[1] = newLambda;
                auto newGroup = ctx.Expr.NewCallable(group->Pos(), "PgGroup", std::move(newChildren));
                newGroups.push_back(newGroup);
            }
        }

        return true;
    }

    bool ValidateSort(const TInputs& inputs, const THashSet<TString>& possibleAliases,
        const TExprNode& data, TExtContext& ctx, TExprNode::TListType& newSorts) {
        newSorts.clear();
        for (auto oneSort : data.Children()) {
            bool hasColumnRef;
            THashSet<TString> refs;
            THashMap<TString, THashSet<TString>> qualifiedRefs;
            if (!ScanColumns(oneSort->Child(1)->TailPtr(), inputs, possibleAliases, nullptr, hasColumnRef,
                refs, &qualifiedRefs, ctx)) {
                return false;
            }

            TVector<const TItemExprType*> items;
            AddColumns(inputs, nullptr, refs, &qualifiedRefs, items);
            auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
            if (!effectiveType->Validate(oneSort->Pos(), ctx.Expr)) {
                return false;
            }

            auto typeNode = ExpandType(oneSort->Pos(), *effectiveType, ctx.Expr);

            auto argNode = ctx.Expr.NewArgument(oneSort->Pos(), "row");
            auto arguments = ctx.Expr.NewArguments(oneSort->Pos(), { argNode });
            TExprNode::TPtr newRoot;
            auto status = RebuildLambdaColumns(oneSort->Child(1)->TailPtr(), argNode, newRoot, inputs, nullptr, ctx);
            if (status == IGraphTransformer::TStatus::Error) {
                return false;
            }

            auto newLambda = ctx.Expr.NewLambda(oneSort->Pos(), std::move(arguments), std::move(newRoot));

            auto newChildren = oneSort->ChildrenList();
            newChildren[0] = typeNode;
            newChildren[1] = newLambda;
            auto newSort = ctx.Expr.ChangeChildren(*oneSort, std::move(newChildren));
            newSorts.push_back(newSort);
        }

        return true;
    }

    IGraphTransformer::TStatus PgSetItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& options = input->Head();
        if (!EnsureTuple(options, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TStructExprType* outputRowType = nullptr;
        TInputs inputs;
        TInputs joinInputs;
        THashSet<TString> possibleAliases;
        bool hasResult = false;
        bool hasValues = false;
        bool hasJoinOps = false;

        // pass 0 - from/values
        // pass 1 - join
        // pass 2 - where, group_by,
        // pass 3 - window
        // pass 4 - result
        for (ui32 pass = 0; pass < 5; ++pass) {
            if (pass > 1 && !inputs.empty() && !hasJoinOps) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing join_ops"));
                return IGraphTransformer::TStatus::Error;
            }

            for (const auto& option : options.Children()) {
                if (!EnsureTupleMinSize(*option, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(option->Head(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto optionName = option->Head().Content();
                if (optionName == "values") {
                    hasValues = true;
                    if (pass != 0) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 3, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto values = option->Child(2);
                    if (!EnsureListType(*values, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto listType = values->GetTypeAnn()->Cast<TListExprType>();
                    if (!EnsureTupleType(values->Pos(), *listType->GetItemType(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto tupleType = listType->GetItemType()->Cast<TTupleExprType>();
                    auto names = option->Child(1);
                    if (!EnsureTupleSize(*names, tupleType->GetSize(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TVector<const TItemExprType*> outputItems;
                    TVector<TString> columns;
                    for (ui32 i = 0; i < names->ChildrenSize(); ++i) {
                        if (!EnsureAtom(*names->Child(i), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        outputItems.push_back(ctx.Expr.MakeType<TItemExprType>(names->Child(i)->Content(), tupleType->GetItems()[i]));
                    }

                    outputRowType = ctx.Expr.MakeType<TStructExprType>(outputItems);
                    if (!outputRowType->Validate(names->Pos(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else if (optionName == "result") {
                    hasResult = true;
                    if (pass != 4) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TVector<const TItemExprType*> outputItems;
                    TExprNode::TListType newResult;
                    bool hasStar = false;
                    bool hasColumnRef = false;
                    for (const auto& column : data.Children()) {
                        if (!column->IsCallable("PgResultItem")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(column->Pos()), "Expected PgResultItem"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        YQL_ENSURE(column->Tail().IsLambda());
                        THashSet<TString> refs;
                        THashMap<TString, THashSet<TString>> qualifiedRefs;
                        if (column->Child(1)->IsCallable("Void")) {
                            // no effective type yet, scan lambda body
                            if (!ScanColumns(column->Tail().TailPtr(), joinInputs, possibleAliases, &hasStar, hasColumnRef,
                                refs, &qualifiedRefs, ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            TVector<const TItemExprType*> items;
                            AddColumns(joinInputs, &hasStar, refs, &qualifiedRefs, items);
                            auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                            if (!effectiveType->Validate(column->Pos(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto expandedColumns = column->HeadPtr();
                            auto typeNode = ExpandType(column->Pos(), *effectiveType, ctx.Expr);

                            auto argNode = ctx.Expr.NewArgument(column->Pos(), "row");
                            auto arguments = ctx.Expr.NewArguments(column->Pos(), { argNode });
                            TExprNode::TPtr newRoot;
                            auto status = RebuildLambdaColumns(column->Tail().TailPtr(), argNode, newRoot, joinInputs, &expandedColumns, ctx);
                            if (status == IGraphTransformer::TStatus::Error) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto newLambda = ctx.Expr.NewLambda(column->Pos(), std::move(arguments), std::move(newRoot));

                            auto newColumnChildren = column->ChildrenList();
                            newColumnChildren[0] = expandedColumns;
                            newColumnChildren[1] = typeNode;
                            newColumnChildren[2] = newLambda;
                            auto newColumn = ctx.Expr.NewCallable(column->Pos(), "PgResultItem", std::move(newColumnChildren));
                            newResult.push_back(newColumn);
                        } else {
                            if (column->Head().IsAtom()) {
                                outputItems.push_back(ctx.Expr.MakeType<TItemExprType>(column->Head().Content(), column->Tail().GetTypeAnn()));
                            } else {
                                // star or qualified star
                                for (const auto& item : column->Tail().GetTypeAnn()->Cast<TStructExprType>()->GetItems()) {
                                    outputItems.push_back(item);
                                }
                            }

                            // scan lambda for window references
                            auto windows = GetSetting(options, "window");
                            if (!ValidateWindowRefs(column->TailPtr(), windows ? &windows->Tail() : nullptr, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                        }
                    }

                    if (!newResult.empty()) {
                        auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newResult));
                        auto newSettings = ReplaceSetting(options, {}, "result", resultValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }

                    outputRowType = ctx.Expr.MakeType<TStructExprType>(outputItems);
                    if (!outputRowType->Validate(data.Pos(), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else if (optionName == "from") {
                    if (pass != 0) {
                        continue;
                    }

                    if (!EnsureTuple(*option, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!EnsureTupleMinSize(data, 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& p : data.Children()) {
                        if (!EnsureTupleSize(*p, 3, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureAtom(*p->Child(1), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureTuple(*p->Child(2), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        for (const auto& name : p->Child(2)->Children()) {
                            if (!EnsureAtom(*name, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        auto columnOrder = ctx.Types.LookupColumnOrder(p->Head());
                        if (!EnsureListType(p->Head(), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto inputRowType = p->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                        if (!EnsureStructType(p->Head().Pos(), *inputRowType, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto inputStructType = inputRowType->Cast<TStructExprType>();
                        auto alias = TString(p->Child(1)->Content());
                        if (!alias.empty()) {
                            if (!possibleAliases.insert(alias).second) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    TStringBuilder() << "Duplicated alias: " << alias));
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        if (p->Child(2)->ChildrenSize() > 0) {
                            // explicit columns
                            ui32 realColumns = 0;
                            for (const auto& item : inputStructType->GetItems()) {
                                if (!item->GetName().StartsWith("_yql_")) {
                                    ++realColumns;
                                }
                            }

                            if (realColumns != p->Child(2)->ChildrenSize()) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    TStringBuilder() << "Wrong number of columns, expected: " << realColumns
                                        << ", got: " << p->Child(2)->ChildrenSize()));
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!columnOrder) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    "No column order at source"));
                                return IGraphTransformer::TStatus::Error;
                            }

                            TVector<const TItemExprType*> newStructItems;
                            TColumnOrder newOrder;
                            for (ui32 i = 0; i < p->Child(2)->ChildrenSize(); ++i) {
                                auto pos = inputStructType->FindItem((*columnOrder)[i]);
                                YQL_ENSURE(pos);
                                auto type = inputStructType->GetItems()[*pos]->GetItemType();
                                newOrder.push_back(TString(p->Child(2)->Child(i)->Content()));
                                newStructItems.push_back(ctx.Expr.MakeType<TItemExprType>(p->Child(2)->Child(i)->Content(), type));
                            }

                            auto newStructType = ctx.Expr.MakeType<TStructExprType>(newStructItems);
                            if (!newStructType->Validate(p->Child(2)->Pos(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            inputs.push_back(std::make_tuple(alias, newStructType, newOrder));
                        } else {
                            inputs.push_back(std::make_tuple(alias, inputStructType, columnOrder));
                        }
                    }
                } else if (optionName == "where" || optionName == "having") {
                    if (pass != 2) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!data.IsCallable("PgWhere")) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()), "Expected PgWhere"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (data.Child(0)->IsCallable("Void")) {
                        // no effective type yet, scan lambda body
                        bool hasColumnRef;
                        THashSet<TString> refs;
                        if (!ScanColumns(data.Child(1)->TailPtr(), joinInputs, possibleAliases, nullptr, hasColumnRef,
                            refs, nullptr, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        TVector<const TItemExprType*> items;
                        AddColumns(joinInputs, nullptr, refs, nullptr, items);
                        auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                        if (!effectiveType->Validate(data.Pos(), ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto typeNode = ExpandType(data.Pos(), *effectiveType, ctx.Expr);

                        auto argNode = ctx.Expr.NewArgument(data.Pos(), "row");
                        auto arguments = ctx.Expr.NewArguments(data.Pos(), { argNode });
                        TExprNode::TPtr newRoot;
                        auto status = RebuildLambdaColumns(data.Child(1)->TailPtr(), argNode, newRoot, joinInputs, nullptr, ctx);
                        if (status == IGraphTransformer::TStatus::Error) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto predicate = ctx.Expr.Builder(data.Pos())
                            .Callable("Coalesce")
                                .Add(0, newRoot)
                                .Callable(1, "Bool")
                                    .Atom(0, "0")
                                .Seal()
                            .Seal()
                            .Build();

                        auto newLambda = ctx.Expr.NewLambda(data.Pos(), std::move(arguments), std::move(predicate));

                        auto newChildren = data.ChildrenList();
                        newChildren[0] = typeNode;
                        newChildren[1] = newLambda;
                        auto newWhere= ctx.Expr.NewCallable(data.Pos(), "PgWhere", std::move(newChildren));
                        auto newSettings = ReplaceSetting(options, {}, TString(optionName), newWhere, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    } else {
                        if (!EnsureSpecificDataType(data, EDataSlot::Bool, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                } else if (optionName == "join_ops") {
                    if (pass != 1) {
                        continue;
                    }

                    hasJoinOps = true;
                    if (hasValues) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Join and values options are not compatible"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (inputs.empty()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "At least one input expected"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    ui32 totalTupleSizes = 0;
                    for (auto child: data.Children()) {
                        if (!EnsureTuple(*child, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        totalTupleSizes += child->ChildrenSize() + 1;
                    }

                    if (totalTupleSizes != inputs.size()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                            TStringBuilder() << "Unexpected number of joins, got: " << totalTupleSizes
                            << ", expected:" << inputs.size()));
                        return IGraphTransformer::TStatus::Error;
                    }

                    bool needRewrite = false;
                    ui32 inputIndex = 0;
                    for (ui32 joinGroupNo = 0; joinGroupNo < data.ChildrenSize(); ++joinGroupNo) {
                        joinInputs.push_back(inputs[inputIndex]);
                        ++inputIndex;
                        for (ui32 i = 0; i < data.Child(joinGroupNo)->ChildrenSize(); ++i) {
                            auto child = data.Child(joinGroupNo)->Child(i);
                            if (!EnsureTupleMinSize(*child, 1, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!EnsureAtom(child->Head(), ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto joinType = child->Head().Content();
                            if (joinType != "cross" && joinType != "inner" && joinType != "left"
                                && joinType != "right" && joinType != "full") {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                                    TStringBuilder() << "Unsupported join type: " << joinType));
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (joinType == "cross") {
                                if (!EnsureTupleSize(*child, 1, ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                joinInputs.push_back(inputs[inputIndex]);
                                ++inputIndex;
                            } else {
                                if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                                bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                                if (leftSideIsOptional) {
                                    for (ui32 j = 0; j < inputIndex; ++j) {
                                        MakeOptionalColumns(std::get<1>(joinInputs[j]), ctx.Expr);
                                    }
                                }

                                joinInputs.push_back(inputs[inputIndex]);
                                ++inputIndex;
                                if (rightSideIsOptional) {
                                    MakeOptionalColumns(std::get<1>(joinInputs.back()), ctx.Expr);
                                }

                                const auto& quals = child->Tail();
                                if (!quals.IsCallable("PgWhere")) {
                                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(quals.Pos()), "Expected PgWhere"));
                                    return IGraphTransformer::TStatus::Error;
                                }

                                needRewrite = needRewrite || quals.Child(0)->IsCallable("Void");
                            }
                        }
                    }

                    if (needRewrite) {
                        TExprNode::TListType newJoinGroups;
                        inputIndex = 0;
                        for (ui32 joinGroupNo = 0; joinGroupNo < data.ChildrenSize(); ++joinGroupNo) {
                            TExprNode::TListType newGroupItems;
                            TInputs groupInputs;
                            THashSet<TString> groupPossibleAliases;
                            if (data.Child(joinGroupNo)->ChildrenSize() > 0) {
                                groupInputs.push_back(inputs[inputIndex]);
                                auto alias = std::get<0>(inputs[inputIndex]);
                                if (!alias.empty()) {
                                    groupPossibleAliases.insert(alias);
                                }
                            }

                            ++inputIndex;
                            for (ui32 i = 0; i < data.Child(joinGroupNo)->ChildrenSize(); ++i, ++inputIndex) {
                                groupInputs.push_back(inputs[inputIndex]);
                                auto alias = std::get<0>(inputs[inputIndex]);
                                if (!alias.empty()) {
                                    groupPossibleAliases.insert(alias);
                                }

                                auto child = data.Child(joinGroupNo)->Child(i);
                                auto joinType = child->Head().Content();
                                if (joinType == "cross") {
                                    newGroupItems.push_back(data.Child(joinGroupNo)->ChildPtr(i));
                                } else {
                                    const auto& quals = child->Tail();
                                    bool hasColumnRef;
                                    THashSet<TString> refs;
                                    if (!ScanColumns(quals.Child(1)->TailPtr(), groupInputs, groupPossibleAliases, nullptr, hasColumnRef,
                                        refs, nullptr, ctx)) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    TVector<const TItemExprType*> items;
                                    AddColumns(groupInputs, nullptr, refs, nullptr, items);
                                    auto effectiveType = ctx.Expr.MakeType<TStructExprType>(items);
                                    if (!effectiveType->Validate(quals.Pos(), ctx.Expr)) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    auto typeNode = ExpandType(quals.Pos(), *effectiveType, ctx.Expr);

                                    auto argNode = ctx.Expr.NewArgument(quals.Pos(), "row");
                                    auto arguments = ctx.Expr.NewArguments(quals.Pos(), { argNode });
                                    TExprNode::TPtr newRoot;
                                    auto status = RebuildLambdaColumns(quals.Child(1)->TailPtr(), argNode, newRoot, groupInputs, nullptr, ctx);
                                    if (status == IGraphTransformer::TStatus::Error) {
                                        return IGraphTransformer::TStatus::Error;
                                    }

                                    auto predicate = ctx.Expr.Builder(quals.Pos())
                                        .Callable("Coalesce")
                                            .Add(0, newRoot)
                                            .Callable(1, "Bool")
                                                .Atom(0, "0")
                                            .Seal()
                                        .Seal()
                                        .Build();

                                    auto newLambda = ctx.Expr.NewLambda(quals.Pos(), std::move(arguments), std::move(predicate));

                                    auto newChildren = quals.ChildrenList();
                                    newChildren[0] = typeNode;
                                    newChildren[1] = newLambda;
                                    auto newWhere= ctx.Expr.NewCallable(quals.Pos(), "PgWhere", std::move(newChildren));
                                    newGroupItems.push_back(ctx.Expr.ChangeChild(*child, 1, std::move(newWhere)));
                                }

                                // after left,right,full join type of inputs in current group may be changed for next predicates
                                bool leftSideIsOptional = (joinType == "right" || joinType == "full");
                                bool rightSideIsOptional = (joinType == "left" || joinType == "full");
                                if (leftSideIsOptional) {
                                    for (ui32 j = 0; j < inputIndex; ++j) {
                                        MakeOptionalColumns(std::get<1>(groupInputs[j]), ctx.Expr);
                                    }
                                }

                                if (rightSideIsOptional) {
                                    MakeOptionalColumns(std::get<1>(groupInputs[inputIndex]), ctx.Expr);
                                }
                            }

                            auto newGroup = ctx.Expr.NewList(option->Pos(), std::move(newGroupItems));
                            newJoinGroups.push_back(newGroup);
                        }

                        auto newJoinGroupsNode = ctx.Expr.NewList(option->Pos(), std::move(newJoinGroups));
                        auto newSettings = ReplaceSetting(options, {}, TString(optionName), newJoinGroupsNode, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                } else if (optionName == "group_by") {
                    if (pass != 2) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TExprNode::TListType newGroups;
                    if (!ValidateGroups(joinInputs, possibleAliases, data, ctx, newGroups)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (!newGroups.empty()) {
                        auto resultValue = ctx.Expr.NewList(options.Pos(), std::move(newGroups));
                        auto newSettings = ReplaceSetting(options, {}, "group_by", resultValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                } else if (optionName == "window") {
                    if (pass != 3) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!EnsureTupleMinSize(data, 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    THashSet<TStringBuf> windowNames;
                    TExprNode::TListType newWindow;
                    bool hasChanges = false;
                    for (ui32 i = 0; i < data.ChildrenSize(); ++i) {
                        auto x = data.ChildPtr(i);
                        if (!x->IsCallable("PgWindow")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgWindow"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (x->Head().Content() && !windowNames.insert(x->Head().Content()).second) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()),
                                TStringBuilder() << "Duplicated window name: " << x->Head().Content()));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto partitions = x->Child(2);
                        auto sort = x->Child(3);
                        bool needRebuildSort = false;
                        bool needRebuildPartition = false;
                        for (const auto& p : partitions->Children()) {
                            if (p->Child(0)->IsCallable("Void")) {
                                needRebuildPartition = true;
                                break;
                            }
                        }

                        for (const auto& s : sort->Children()) {
                            if (s->Child(0)->IsCallable("Void")) {
                                needRebuildSort = true;
                                break;
                            }
                        }

                        if (!needRebuildSort && !needRebuildPartition) {
                            newWindow.push_back(x);
                            continue;
                        }

                        hasChanges = true;
                        auto newChildren = x->ChildrenList();
                        if (needRebuildPartition) {
                            TExprNode::TListType newGroups;
                            if (!ValidateGroups(joinInputs, possibleAliases, *partitions, ctx, newGroups)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            newChildren[2] = ctx.Expr.NewList(x->Pos(), std::move(newGroups));
                        }

                        if (needRebuildSort) {
                            TExprNode::TListType newSorts;
                            if (!ValidateSort(joinInputs, possibleAliases, *sort, ctx, newSorts)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            newChildren[3] = ctx.Expr.NewList(x->Pos(), std::move(newSorts));
                        }

                        newWindow.push_back(ctx.Expr.ChangeChildren(*x, std::move(newChildren)));
                    }

                    if (hasChanges) {
                        auto windowValue = ctx.Expr.NewList(options.Pos(), std::move(newWindow));
                        auto newSettings = ReplaceSetting(options, {}, "window", windowValue, ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                        TStringBuilder() << "Unsupported option: " << optionName));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (!hasResult && !hasValues) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing result and values"));
            return IGraphTransformer::TStatus::Error;
        }

        if (hasResult && hasValues) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Either result or values should be specified"));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(outputRowType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PgSelectWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& options = input->Head();
        if (!EnsureTuple(options, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TStructExprType* outputRowType = nullptr;
        TExprNode* setItems = nullptr;
        TExprNode* setOps = nullptr;
        bool hasSort = false;

        for (ui32 pass = 0; pass < 2; ++pass) {
            for (const auto& option : options.Children()) {
                if (!EnsureTupleMinSize(*option, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(option->Head(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto optionName = option->Head().Content();
                if (optionName == "set_ops") {
                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (pass == 0) {
                        if (!EnsureTupleMinSize(option->Tail(), 1, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        for (const auto& child : option->Tail().Children()) {
                            if (!EnsureAtom(*child, ctx.Expr)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (child->Content() != "push" && child->Content() != "union_all") {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                                    TStringBuilder() << "Unexpected operation: " << child->Content()));
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        setOps = &option->Tail();
                    }
                } else if (optionName == "set_items") {
                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (pass == 0) {
                        if (!EnsureTupleMinSize(option->Tail(), 1, ctx.Expr)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        for (const auto& child : option->Tail().Children()) {
                            if (!child->IsCallable("PgSetItem")) {
                                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), "Expected PgSetItem"));
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        setItems = &option->Tail();
                    } else {
                        outputRowType = option->Tail().Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->
                            Cast<TStructExprType>();
                    }
                } else if (optionName == "limit" || optionName == "offset") {
                    if (pass != 0) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TOptionalExprType>(
                        ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64));
                    auto convertStatus = TryConvertTo(option->ChildRef(1), *expectedType, ctx.Expr);
                    if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Child(1)->Pos()), "Mismatch argument types"));
                        return IGraphTransformer::TStatus::Error;
                    }

                    if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
                        auto newSettings = ReplaceSetting(options, {}, TString(optionName), option->ChildPtr(1), ctx.Expr);
                        output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                        return IGraphTransformer::TStatus::Repeat;
                    }
                } else if (optionName == "sort") {
                    if (pass != 1) {
                        continue;
                    }

                    if (!EnsureTupleSize(*option, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    const auto& data = option->Tail();
                    if (!EnsureTuple(data, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    for (const auto& x : data.Children()) {
                        if (!x->IsCallable("PgSort")) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(x->Pos()), "Expected PgSort"));
                        }
                    }

                    hasSort = true;
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(option->Head().Pos()),
                        TStringBuilder() << "Unsupported option: " << optionName));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (!setItems) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing set_items"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!setOps) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Missing set_ops"));
            return IGraphTransformer::TStatus::Error;
        }

        if (setOps->ChildrenSize() != setItems->ChildrenSize() * 2 - 1) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Mismatched count of items in set_items and set_ops"));
            return IGraphTransformer::TStatus::Error;
        }

        ui32 balance = 0;
        for (const auto& op : setOps->Children()) {
            if (op->Content() == "push") {
                balance += 1;
            } else {
                if (balance < 2) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Disbalanced set_ops"));
                    return IGraphTransformer::TStatus::Error;
                }

                balance -= 1;
            }
        }

        if (balance != 1) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Disbalanced set_ops"));
            return IGraphTransformer::TStatus::Error;
        }

        TColumnOrder resultColumnOrder;
        const TStructExprType* resultStructType = nullptr;
        auto status = InferPositionalUnionType(input->Pos(), setItems->ChildrenList(), resultColumnOrder, resultStructType, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (hasSort) {
            auto option = GetSetting(options, "sort");
            YQL_ENSURE(option);
            const auto& data = option->Tail();
            TInputs projectionInputs;
            projectionInputs.push_back(std::make_tuple(TString(), resultStructType, resultColumnOrder));
            TExprNode::TListType newSortTupleItems;

            if (data.ChildrenSize() > 0 && data.Child(0)->Child(0)->IsCallable("Void")) {
                // no effective types yet, scan lambda bodies
                if (!ValidateSort(projectionInputs, {}, data, ctx, newSortTupleItems)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto newSortTuple = ctx.Expr.NewList(data.Pos(), std::move(newSortTupleItems));
                auto newSettings = ReplaceSetting(options, {}, "sort", newSortTuple, ctx.Expr);
                output = ctx.Expr.ChangeChild(*input, 0, std::move(newSettings));
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultStructType));
        return ctx.Types.SetColumnOrder(*input, resultColumnOrder, ctx.Expr);
    }

    IGraphTransformer::TStatus SqlProjectItemWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        YQL_ENSURE(input->IsCallable({"SqlProjectItem", "SqlProjectStarItem"}));
        const bool isStar = input->IsCallable("SqlProjectStarItem");
        if (!EnsureMinMaxArgsCount(*input, 3, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (input->ChildrenSize() == 4) {
            TExprNode::TPtr normalized;
            auto status = NormalizeKeyValueTuples(input->ChildPtr(3), 0, normalized, ctx.Expr);
            if (status.Level == IGraphTransformer::TStatus::Repeat) {
                output = ctx.Expr.ChangeChild(*input, 3, std::move(normalized));
                return status;
            }

            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        const auto seqType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (seqType->GetKind() == ETypeAnnotationKind::EmptyList) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (!EnsureNewSeqType<false>(input->Head().Pos(), *seqType, ctx.Expr, &itemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        YQL_ENSURE(itemType);
        if (!EnsureStructType(input->Head().Pos(), *itemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() == 4) {
            // validate options
            THashSet<TStringBuf> seenOptions;
            for (auto& optionNode : input->Child(3)->ChildrenList()) {
                if (!EnsureTupleMinSize(*optionNode, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                TStringBuf name = optionNode->Child(0)->Content();
                if (!seenOptions.insert(name).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(optionNode->Pos()),
                        TStringBuilder() << "Duplicate option " << name));
                    return IGraphTransformer::TStatus::Error;
                }

                if (isStar && name == "divePrefix") {
                    if (!EnsureTupleSize(*optionNode, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    if (!EnsureTupleOfAtoms(*optionNode->Child(1), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    THashSet<TStringBuf> prefixes;
                    for (auto& prefix : optionNode->Child(1)->ChildrenList()) {
                        if (!prefixes.insert(prefix->Content()).second) {
                            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(optionNode->Pos()),
                                TStringBuilder() << "Duplicate prefix " << prefix->Content()));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                } else if (isStar && name == "addPrefix") {
                    if (!EnsureTupleSize(*optionNode, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    if (!EnsureAtom(*optionNode->Child(1), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else if (!isStar && name == "warnShadow") {
                    // no params
                    if (!EnsureTupleSize(*optionNode, 1, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(optionNode->Pos()),
                        TStringBuilder() << "Unknown option: " << name));
                    return IGraphTransformer::TStatus::Error;
                }
            }

            if (isStar && seenOptions.size() > 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()),
                    TStringBuilder() << "Options addPrefix and divePrefix cannot be used at the same time"));
                return IGraphTransformer::TStatus::Error;
            }

            if (seenOptions.contains("warnShadow")) {
                auto alias = input->Child(1)->Content();
                if (itemType->Cast<TStructExprType>()->FindItem(alias)) {
                    auto issue = TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()),
                        TStringBuilder() << "Alias `" << alias << "` shadows column with the same name. It looks like comma is missed here. "
                                            "If not, it is recommended to use ... AS `" << alias << "` to avoid confusion");
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_ALIAS_SHADOWS_COLUMN, issue);
                    if (!ctx.Expr.AddWarning(issue)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
                auto newChildren = input->ChildrenList();
                // drop options
                newChildren.pop_back();
                output = ctx.Expr.ChangeChildren(*input, std::move(newChildren));
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        auto& lambda = input->ChildRef(2);
        auto convertStatus = ConvertToLambda(lambda, ctx.Expr, 1);
        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, { itemType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto lambdaResult = lambda->GetTypeAnn();
        if (!lambdaResult) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (isStar && !EnsureStructType(lambda->Pos(), *lambdaResult, ctx.Expr)) {
            // lambda should return struct
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(lambdaResult);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SqlTypeFromYsonWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto type = NCommon::ParseTypeFromYson(input->Head().Content(), ctx.Expr, ctx.Expr.GetPosition(input->Pos()));
        if (!type) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ExpandType(input->Pos(), *type, ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SqlColumnOrderFromYsonWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<TString> topLevelColumns;
        auto type = NCommon::ParseOrderAwareTypeFromYson(input->Head().Content(), topLevelColumns, ctx.Expr, ctx.Expr.GetPosition(input->Pos()));
        if (!type) {
            return IGraphTransformer::TStatus::Error;
        }

        TExprNodeList items;
        for (auto& col : topLevelColumns) {
            items.push_back(ctx.Expr.NewAtom(input->Pos(), col));
        }

        output = ctx.Expr.NewList(input->Pos(), std::move(items));
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus AutoDemuxListWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listType = input->Head().GetTypeAnn()->Cast<TListExprType>();
        if (listType->GetItemType()->GetKind() == ETypeAnnotationKind::Variant) {
            output = ctx.Expr.RenameNode(*input, "Demux");
        } else {
            output = input->HeadPtr();
        }

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus AggrCountInitWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = ctx.Expr.NewCallable(input->Pos(), "Uint64", {ctx.Expr.NewAtom(input->Pos(), "0", TNodeFlags::Default)});
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AggrCountUpdateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(*input->Child(1), EDataSlot::Uint64, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(input->Head())) {
            output = input->TailPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(input->Tail().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus QueueCreateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureDependsOnTail(*input, ctx.Expr, 3)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        auto typeArg = input->Child(0);

        const auto queueType = typeArg->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!queueType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(typeArg->Pos()), TStringBuilder() << "Expecting computable type, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputableType(typeArg->Pos(), *queueType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& capacityArg = input->ChildRef(1);
        auto& initSizeArg = input->ChildRef(2);

        if (!capacityArg->IsCallable("Uint64") && !capacityArg->IsCallable("Void")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(capacityArg->Pos()), TStringBuilder() << "Queue capacity should be Uint64 literal or Void"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!initSizeArg->IsCallable("Uint64")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(initSizeArg->Pos()), TStringBuilder() << "Queue initial size should be Uint64 literal"));
            return IGraphTransformer::TStatus::Error;
        }

        if (capacityArg->IsCallable("Uint64")) {
            auto capacity = FromString<ui64>(capacityArg->Child(0)->Content());
            auto initSize = FromString<ui64>(initSizeArg->Child(0)->Content());
            if (initSize > capacity) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Queue initial size should not be bigger than capacity"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TResourceExprType>(TStringBuilder() <<
                NKikimr::NMiniKQL::ResourceQueuePrefix << FormatType(queueType)));
        return IGraphTransformer::TStatus::Ok;
    }

    bool EnsureQueueResource(const TExprNode* resourceArg, const TTypeAnnotationNode*& elementType, TExtContext& ctx) {
        if (!EnsureResourceType(*resourceArg, ctx.Expr)) {
            return false;
        }
        const auto resourceType = resourceArg->GetTypeAnn()->Cast<TResourceExprType>();
        const auto resourceTag = resourceType->GetTag();
        using NKikimr::NMiniKQL::ResourceQueuePrefix;
        if (!resourceTag.StartsWith(ResourceQueuePrefix)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(resourceArg->Pos()), "You should use resource from QueueCreate"));
            return false;
        }
        auto typeExpr = ctx.Expr.Builder(resourceArg->Pos()).Callable("ParseType")
                .Atom(0, TString(resourceTag.data()+ResourceQueuePrefix.size(), resourceTag.size()-ResourceQueuePrefix.size()))
            .Seal().Build();
        auto parseTypeResult = ParseTypeWrapper(typeExpr, typeExpr, ctx);
        if (parseTypeResult == IGraphTransformer::TStatus::Error) {
            return false;
        }
        if (!EnsureType(*typeExpr, ctx.Expr)) {
            return false;
        }
        elementType = typeExpr->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        return true;
    }

    IGraphTransformer::TStatus QueuePushWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto resourceArg = input->Child(0);
        auto& valueArg = input->ChildRef(1);

        if (!EnsureComputable(*valueArg, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const TTypeAnnotationNode* expectedValueType;
        if (!EnsureQueueResource(resourceArg, expectedValueType, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto convertStatus = TryConvertTo(valueArg, *expectedValueType, ctx.Expr);
        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }
        input->SetTypeAnn(resourceArg->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus QueuePopWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto resourceArg = input->Child(0);

        if (!EnsureResourceType(*resourceArg, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(resourceArg->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus QueuePeekWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureDependsOnTail(*input, ctx.Expr, 2)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto resourceArg = input->Child(0);
        auto& indexArg = input->ChildRef(1);

        const TTypeAnnotationNode* expectedValueType;
        if (!EnsureQueueResource(resourceArg, expectedValueType, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto expectedIndexType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        auto convertStatus = TryConvertTo(indexArg, *expectedIndexType, ctx.Expr);
        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(expectedValueType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus QueueRangeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureDependsOnTail(*input, ctx.Expr, 3)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto resourceArg = input->Child(0);
        auto& beginArg = input->ChildRef(1);
        auto& endArg = input->ChildRef(2);

        const TTypeAnnotationNode* expectedValueType;
        if (!EnsureQueueResource(resourceArg, expectedValueType, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto expectedIndexType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        auto convertStatus = TryConvertTo(beginArg, *expectedIndexType, ctx.Expr);
        convertStatus = convertStatus.Combine(TryConvertTo(endArg, *expectedIndexType, ctx.Expr));
        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TOptionalExprType>(expectedValueType)));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus PreserveStreamWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto streamArg = input->Child(0);
        auto resourceArg = input->Child(1);
        if (!EnsureStreamType(*streamArg, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const TTypeAnnotationNode* expectedValueType;
        if (!EnsureQueueResource(resourceArg, expectedValueType, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        const TTypeAnnotationNode* streamType = streamArg->GetTypeAnn();
        const TTypeAnnotationNode* itemType = streamType->Cast<TStreamExprType>()->GetItemType();
        if (!IsSameAnnotation(*itemType, *expectedValueType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "mismatch of stream and queue types: "
                << *itemType << " != " << *expectedValueType));
            return IGraphTransformer::TStatus::Error;
        }

        auto outpaceArg = input->Child(2);
        if (!outpaceArg->IsCallable("Uint64")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(outpaceArg->Pos()), TStringBuilder() << "Outpace arg should be Uint64 literal"));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(streamType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DependsOnWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputable(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus SeqWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (auto& arg : input->Children()) {
            if (!EnsureComputable(*arg, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(input->Tail().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus ByteStringWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.RenameNode(*input, "String");
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus Utf8StringWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.RenameNode(*input, "Utf8");
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus ParameterWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
        return IGraphTransformer::TStatus::Ok;
    }

    TMaybe<EDataSlot> ExtractDataType(const TExprNode& node, TExprContext& ctx) {
        if (node.IsAtom()) {
            auto dataType = node.Content();
            auto slot = NKikimr::NUdf::FindDataSlot(dataType);
            if (!slot) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Unknown datatype: " << dataType));
                return {};
            }

            return slot;
        } else {
            if (!node.GetTypeAnn() || node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either atom or type"));
                return {};
            }

            auto type = node.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            bool isOptional;
            const TDataExprType* dataType;
            if (!EnsureDataOrOptionalOfData(node.Pos(), type, isOptional, dataType, ctx)) {
                return {};
            }

            return dataType->GetSlot();
        }
    }

    IGraphTransformer::TStatus WeakFieldWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 3, ctx.Expr) || !EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const TStructExprType* structType = input->Head().GetTypeAnn()->Cast<TStructExprType>();

        auto targetSlot = ExtractDataType(*input->Child(1), ctx.Expr);
        if (!targetSlot) {
            return IGraphTransformer::TStatus::Error;
        }

        auto targetType = NKikimr::NUdf::GetDataTypeInfo(*targetSlot).Name;
        auto targetTypeExpr = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(*targetSlot));
        if (!EnsureTupleMinSize(*input->Child(2), 1, ctx.Expr) && !EnsureTupleMaxSize(*input->Child(2), 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        for (const auto& child: input->Child(2)->Children()) {
            if (!EnsureAtom(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        const bool isDefault = input->ChildrenSize() == 4;
        if (isDefault) {
            auto convertStatus = TryConvertTo(input->ChildRef(3), *targetTypeExpr, ctx.Expr);
            if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() <<
                    "Default value not correspond weak field type: " << targetType));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const auto sourceName = input->Child(2)->ChildrenSize() == 2 ? input->Child(2)->Child(1)->Content() : "";
        const auto memberName = input->Child(2)->Head().Content();
        const auto fullMemberName = sourceName ? DotJoin(sourceName, memberName) : TString(memberName);
        const auto otherField = sourceName ? DotJoin(sourceName, "_other") : "_other";
        const auto restField = sourceName ? DotJoin(sourceName, "_rest") : "_rest";
        const TTypeAnnotationNode* fieldType = nullptr;
        const TTypeAnnotationNode* otherType = nullptr;
        const TTypeAnnotationNode* restType = nullptr;
        TExprNode::TPtr otherMember;
        TExprNode::TPtr restMember;
        if (auto pos = structType->FindItem(otherField)) {
            otherType = structType->GetItems()[*pos]->GetItemType();
            const TTypeAnnotationNode* unpackedOtherType = otherType;
            if (otherType->GetKind() == ETypeAnnotationKind::Optional) {
                unpackedOtherType = otherType->Cast<TOptionalExprType>()->GetItemType();
            }

            auto strType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::String);
            auto expectedOtherType = ctx.Expr.MakeType<TDictExprType>(strType, strType);
            if (IsSameAnnotation(*unpackedOtherType, *expectedOtherType)) {
                otherMember = ctx.Expr.Builder(input->Pos())
                    .Callable("Member")
                    .Add(0, input->Child(0))
                    .Atom(1, otherField)
                    .Seal().Build();
            } else {
                otherType = nullptr;
            }
        }

        if (!otherMember) {
            otherMember = ctx.Expr.NewCallable(input->Pos(), "Null", {});
        }

        if (auto pos = structType->FindItem(restField)) {
            restType = structType->GetItems()[*pos]->GetItemType();
            auto strType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::String);
            auto ysonType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Yson);
            auto expectedRestType1 = ctx.Expr.MakeType<TDictExprType>(strType, ysonType);
            auto expectedRestType1opt = ctx.Expr.MakeType<TOptionalExprType>(expectedRestType1);
            auto expectedRestType2 = ysonType;
            auto expectedRestType2opt = ctx.Expr.MakeType<TOptionalExprType>(expectedRestType2);
            auto member = ctx.Expr.Builder(input->Pos())
                .Callable("Member")
                .Add(0, input->Child(0))
                .Atom(1, restField)
                .Seal().Build();

            if (IsSameAnnotation(*restType, *expectedRestType1) || IsSameAnnotation(*restType, *expectedRestType1opt)) {
                restMember = member;
            } else if (IsSameAnnotation(*restType, *expectedRestType2) || IsSameAnnotation(*restType, *expectedRestType2opt)) {
                auto parsedDict = ctx.Expr.Builder(input->Pos())
                    .Callable("Apply")
                        .Callable(0, "Udf")
                            .Atom(0, "Yson2.ConvertToDict")
                        .Seal()
                        .Add(1, member)
                    .Seal()
                    .Build();

                restMember = ctx.Expr.Builder(input->Pos())
                    .Callable("ToDict")
                        .Callable(0, "Map")
                            .Callable(0, "DictItems")
                                .Add(0, parsedDict)
                            .Seal()
                            .Lambda(1)
                                .Param("pair")
                                .List()
                                    .Callable(0, "Nth")
                                        .Arg(0, "pair")
                                        .Atom(1, "0")
                                    .Seal()
                                    .Callable(1, "Apply")
                                        .Callable(0, "Udf")
                                            .Atom(0, "Yson2.Serialize")
                                        .Seal()
                                        .Callable(1, "Nth")
                                            .Arg(0, "pair")
                                            .Atom(1, "1")
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Lambda(1) // keyExtractor
                            .Param("pair")
                            .Callable("Nth")
                                .Arg(0, "pair")
                                .Atom(1, "0")
                            .Seal()
                        .Seal()
                        .Lambda(2) // payloadExtractor
                            .Param("pair")
                            .Callable("Nth")
                                .Arg(0, "pair")
                                .Atom(1, "1")
                            .Seal()
                        .Seal()
                        .List(3)
                            .Atom(0, "Hashed")
                            .Atom(1, "One")
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                restType = nullptr;
            }
        }

        if (!restMember) {
            restMember = ctx.Expr.NewCallable(input->Pos(), "Null", {});
        }

        if (auto pos = structType->FindItem(fullMemberName)) {
            fieldType = structType->GetItems()[*pos]->GetItemType();
            auto checkType = fieldType->GetKind() == ETypeAnnotationKind::Optional ?
                fieldType : ctx.Expr.MakeType<TOptionalExprType>(fieldType);
            if (!IsSameAnnotation(*targetTypeExpr, *checkType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "incompatible WeakField types: "
                    << GetTypeDiff(*targetTypeExpr, *checkType)));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (fieldType) {
            if (fieldType->GetKind() == ETypeAnnotationKind::Optional) {
                output = ctx.Expr.Builder(input->Pos())
                    .Callable("Coalesce")
                        .Callable(0, "Member")
                            .Add(0, input->Child(0))
                            .Atom(1, fullMemberName)
                        .Seal()
                        .Callable(1, "TryWeakMemberFromDict")
                            .Add(0, otherMember)
                            .Add(1, restMember)
                            .Atom(2, targetType)
                            .Atom(3, memberName)
                        .Seal()
                    .Seal().Build();
            } else {
                output = ctx.Expr.Builder(input->Pos())
                    .Callable("Just")
                        .Callable(0, "Member")
                            .Add(0, input->Child(0))
                            .Atom(1, fullMemberName)
                        .Seal()
                    .Seal().Build();
            }
        } else if (otherType || restType) {
            output = ctx.Expr.Builder(input->Pos())
                .Callable("TryWeakMemberFromDict")
                    .Add(0, otherMember)
                    .Add(1, restMember)
                    .Atom(2, targetType)
                    .Atom(3, memberName)
                .Seal().Build();
            if (isDefault) {
                output = ctx.Expr.Builder(input->Pos())
                    .Callable("Coalesce")
                        .Add(0, output)
                        .Add(1, input->Child(3))
                    .Seal().Build();
            }
        } else {
            if (isDefault) {
                output = input->ChildPtr(3);
            } else {
                output = ctx.Expr.Builder(input->Pos())
                    .Callable("Nothing")
                        .Callable(0, "OptionalType")
                            .Callable(0, "DataType")
                                .Atom(0, targetType)
                            .Seal()
                        .Seal()
                    .Seal().Build();
            }
        }
        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus TryWeakMemberFromDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (unsigned index = 0; index < 2; ++index) {
            auto& dictNodeRef = input->ChildRef(index);
            const bool isYsonPayload = index > 0;
            const EDataSlot payloadStr = isYsonPayload ? EDataSlot::Yson : EDataSlot::String;
            auto expectedType = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDictExprType>(
                ctx.Expr.MakeType<TDataExprType>(EDataSlot::String), ctx.Expr.MakeType<TDataExprType>(payloadStr)));
            auto convertStatus = TryConvertTo(dictNodeRef, *expectedType, ctx.Expr);
            if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
                return convertStatus;
            }
        }

        auto targetTypeNode = input->Child(2);
        auto memberNameNode = input->Child(3);
        if (!EnsureAtom(*targetTypeNode, ctx.Expr) || !EnsureAtom(*memberNameNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!NYql::ValidateName(memberNameNode->Pos(), memberNameNode->Content(), "member", ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto targetType = targetTypeNode->Content();
        auto targetSlot = NKikimr::NUdf::FindDataSlot(targetType);
        if (!targetSlot) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(targetTypeNode->Pos()), TStringBuilder() << "Unknown datatype: " << targetType));
            return IGraphTransformer::TStatus::Error;
        }
        auto targetTypeExpr = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(*targetSlot));

        input->SetTypeAnn(targetTypeExpr);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus LambdaArgumentsCountWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!input->Head().IsLambda()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected lambda, but got: " << input->Head().Type()));
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("Uint32")
                .Atom(0, ToString(input->Head().Head().ChildrenSize()))
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus FromYsonSimpleType(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* sourceType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, sourceType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto sourceSlot = sourceType->GetSlot();
        if (sourceSlot != EDataSlot::String && sourceSlot != EDataSlot::Yson) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected string, yson or optional of it"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto targetType = input->Child(1)->Content();
        auto targetSlot = NKikimr::NUdf::FindDataSlot(targetType);
        if (!targetSlot) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Unknown datatype: " << targetType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(*targetSlot));
        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CurrentOperationIdWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("String")
                .Atom(0, ctx.Types.OperationOptions.Id.GetOrElse(""))
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus CurrentOperationSharedIdWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("String")
                .Atom(0, ctx.Types.OperationOptions.SharedId.GetOrElse(""))
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus CurrentAuthenticatedUserWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        if (!EnsureArgsCount(*input, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        output = ctx.Expr.Builder(input->Pos())
            .Callable("String")
                .Atom(0, ctx.Types.OperationOptions.AuthenticatedUser.GetOrElse(""))
            .Seal()
            .Build();

        return IGraphTransformer::TStatus::Repeat;
    }

    IGraphTransformer::TStatus SecureParamWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TExtContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto tokenName = input->Head().Content();
        auto separator = tokenName.find(":");

        if (separator == TString::npos || separator == tokenName.size()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "malformed secure param: " << tokenName));
            return IGraphTransformer::TStatus::Error;
        }

        const auto p0 = tokenName.substr(0, separator);
        if (p0 == "api") {
            const auto p1 = tokenName.substr(separator + 1);
            if (p1 != "oauth" && p1 != "cookie") {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "unknown token: " << p1 << ", prefix: " << p0));
                return IGraphTransformer::TStatus::Error;
            }
            if (p1 == "oauth" && ctx.Types.UserCredentials.OauthToken.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "got empty Oauth token string"));
                return IGraphTransformer::TStatus::Error;
            }
            if (p1 == "cookie" && ctx.Types.UserCredentials.BlackboxSessionIdCookie.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "got empty session cookie"));
                return IGraphTransformer::TStatus::Error;
            }
        } else if (p0 == "token" || p0 == "cluster") {
            const auto p1 = tokenName.substr(separator + 1);
            auto cred = ctx.Types.FindCredential(p1);
            TMaybe<TCredential> clusterCred;
            if (cred == nullptr && p0 == "cluster") {
                if (p1.StartsWith("default_")) {
                    TStringBuf clusterName = p1;
                    if (clusterName.SkipPrefix("default_")) {
                        for (auto& x : ctx.Types.DataSources) {
                            auto tokens = x->GetClusterTokens();
                            auto token = tokens ? tokens->FindPtr(clusterName) : nullptr;
                            if (token) {
                                clusterCred.ConstructInPlace(TString(x->GetName()), "", *token);
                                cred = clusterCred.Get();
                                break;
                            }
                        }
                        for (auto& x : ctx.Types.DataSinks) {
                            auto tokens = x->GetClusterTokens();
                            auto token = tokens ? tokens->FindPtr(clusterName) : nullptr;
                            if (token) {
                                clusterCred.ConstructInPlace(TString(x->GetName()), "", *token);
                                cred = clusterCred.Get();
                                break;
                            }
                        }
                    }
                }
            }

            if (cred == nullptr) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "unknown token id: " << p1 << ", prefix: " << p0));
                return IGraphTransformer::TStatus::Error;
            }
            if (cred->Content.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "got empty credential content for id: " << p1));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "unknown token prefix: " << p0));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String));

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MuxWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* inputType = input->Head().GetTypeAnn();
        const TTypeAnnotationNode* resultType = nullptr;
        if (inputType->GetKind() == ETypeAnnotationKind::Tuple) {
            const TTupleExprType* tupleType = inputType->Cast<TTupleExprType>();
            TTypeAnnotationNode::TListType itemTypes;
            TExprNode::TListType updatedChildren;
            for (size_t i = 0; i < tupleType->GetSize(); ++i) {
                if (!EnsureListType(input->Head().Pos(), *tupleType->GetItems()[i], ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                auto itemType = tupleType->GetItems()[i]->Cast<TListExprType>()->GetItemType();
                if (itemType->GetKind() == ETypeAnnotationKind::Struct
                    && AnyOf(itemType->Cast<TStructExprType>()->GetItems(), [](const TItemExprType* structItem) { return structItem->GetName().StartsWith("_yql_sys_"); })) {

                    if (updatedChildren.empty()) {
                        updatedChildren.resize(tupleType->GetSize());
                    }
                    updatedChildren[i] = ctx.Expr.Builder(input->Head().Pos())
                        .Callable("RemovePrefixMembers")
                            .Callable(0, "Nth")
                                .Add(0, input->HeadPtr())
                                .Atom(1, ToString(i), TNodeFlags::Default)
                            .Seal()
                            .List(1)
                                .Atom(0, "_yql_sys_", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .Build();
                }
                itemTypes.push_back(itemType);
            }
            if (!updatedChildren.empty()) {
                for (size_t i = 0; i < updatedChildren.size(); ++i) {
                    if (!updatedChildren[i]) {
                        updatedChildren[i] = ctx.Expr.Builder(input->Head().Pos())
                            .Callable("Nth")
                                .Add(0, input->HeadPtr())
                                .Atom(1, ToString(i), TNodeFlags::Default)
                            .Seal()
                            .Build();
                    }
                }
                output = ctx.Expr.ChangeChild(*input, 0, ctx.Expr.NewList(input->Head().Pos(), std::move(updatedChildren)));
                return IGraphTransformer::TStatus::Repeat;
            }
            resultType = ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TVariantExprType>(ctx.Expr.MakeType<TTupleExprType>(itemTypes)));
        }
        else if (inputType->GetKind() == ETypeAnnotationKind::Struct) {
            const TStructExprType* structType = inputType->Cast<TStructExprType>();
            TVector<const TItemExprType*> itemTypes;
            TExprNode::TListType updatedChildren;
            for (size_t i = 0; i < structType->GetSize(); ++i) {
                if (!EnsureListType(input->Head().Pos(), *structType->GetItems()[i]->GetItemType(), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
                auto itemType = structType->GetItems()[i]->GetItemType()->Cast<TListExprType>()->GetItemType();
                auto itemName = structType->GetItems()[i]->GetName();
                if (itemType->GetKind() == ETypeAnnotationKind::Struct
                    && AnyOf(itemType->Cast<TStructExprType>()->GetItems(), [](const TItemExprType* structItem) { return structItem->GetName().StartsWith("_yql_sys_"); })) {

                    if (updatedChildren.empty()) {
                        updatedChildren.resize(structType->GetSize());
                    }
                    updatedChildren[i] = ctx.Expr.Builder(input->Head().Pos())
                        .List()
                            .Atom(0, itemName)
                            .Callable(1, "RemovePrefixMembers")
                                .Callable(0, "Member")
                                    .Add(0, input->HeadPtr())
                                    .Atom(1, itemName)
                                .Seal()
                                .List(1)
                                    .Atom(0, "_yql_sys_", TNodeFlags::Default)
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                }

                itemTypes.push_back(ctx.Expr.MakeType<TItemExprType>(itemName, itemType));
            }
            if (!updatedChildren.empty()) {
                for (size_t i = 0; i < updatedChildren.size(); ++i) {
                    if (!updatedChildren[i]) {
                        auto itemName = structType->GetItems()[i]->GetName();
                        updatedChildren[i] = ctx.Expr.Builder(input->Head().Pos())
                            .List()
                                .Atom(0, itemName)
                                .Callable(1, "Member")
                                    .Add(0, input->HeadPtr())
                                    .Atom(1, itemName)
                                .Seal()
                            .Seal()
                            .Build();
                    }
                }
                output = ctx.Expr.ChangeChild(*input, 0, ctx.Expr.NewCallable(input->Head().Pos(), "AsStruct", std::move(updatedChildren)));
                return IGraphTransformer::TStatus::Repeat;
            }
            resultType = ctx.Expr.MakeType<TListExprType>(ctx.Expr.MakeType<TVariantExprType>(ctx.Expr.MakeType<TStructExprType>(itemTypes)));
        }
        else {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected Tuple or Struct type, but got: " << *inputType));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus DemuxWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listItemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!EnsureVariantType(input->Head().Pos(), *listItemType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto variantType = listItemType->Cast<TVariantExprType>();
        const TTypeAnnotationNode* resultType = nullptr;
        if (variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            const TTupleExprType* tupleType = variantType->GetUnderlyingType()->Cast<TTupleExprType>();
            TTypeAnnotationNode::TListType listTypes;
            for (size_t i = 0; i < tupleType->GetSize(); ++i) {
                listTypes.push_back(ctx.Expr.MakeType<TListExprType>(tupleType->GetItems()[i]));
            }
            resultType = ctx.Expr.MakeType<TTupleExprType>(listTypes);
        }
        else {
            const TStructExprType* structType = variantType->GetUnderlyingType()->Cast<TStructExprType>();
            TVector<const TItemExprType*> listTypes;
            for (size_t i = 0; i < structType->GetSize(); ++i) {
                listTypes.push_back(
                    ctx.Expr.MakeType<TItemExprType>(
                        structType->GetItems()[i]->GetName(),
                        ctx.Expr.MakeType<TListExprType>(structType->GetItems()[i]->GetItemType())
                    )
                );
            }
            resultType = ctx.Expr.MakeType<TStructExprType>(listTypes);
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TimezoneIdWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* ui16Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint16);
        const TTypeAnnotationNode* optUi16Type = ctx.Expr.MakeType<TOptionalExprType>(ui16Type);
        if (dataType->GetSlot() == EDataSlot::String) {
            // name
        } else {
            const TTypeAnnotationNode* expectedType = isOptional ? optUi16Type : ui16Type;
            auto convertStatus = TryConvertTo(input->ChildRef(0), *expectedType, ctx.Expr);
            if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Mismatch argument types"));
                return IGraphTransformer::TStatus::Error;
            }

            if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
                return convertStatus;
            }

            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(optUi16Type);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus TimezoneNameWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint16);
        if (isOptional) {
            expectedType = ctx.Expr.MakeType<TOptionalExprType>(expectedType);
        }

        auto convertStatus = TryConvertTo(input->ChildRef(0), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String)));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AddTimezoneWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional1;
        const TDataExprType* dataType1;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional1, dataType1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsDataTypeDate(dataType1->GetSlot())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected (optional) date type, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional2;
        const TDataExprType* dataType2;
        if (!EnsureDataOrOptionalOfData(*input->Child(1), isOptional2, dataType2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* ui16Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint16);
        const TTypeAnnotationNode* optUi16Type = ctx.Expr.MakeType<TOptionalExprType>(ui16Type);
        const TTypeAnnotationNode* expectedType = isOptional2 ? optUi16Type : ui16Type;
        auto convertStatus = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Mismatch argument types"));
            return IGraphTransformer::TStatus::Error;
        }

        if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
            return convertStatus;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(
            WithTzDate(dataType1->GetSlot()))));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RemoveTimezoneWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isOptional1;
        const TDataExprType* dataType1;
        if (!EnsureDataOrOptionalOfData(input->Head(), isOptional1, dataType1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsDataTypeTzDate(dataType1->GetSlot())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                TStringBuilder() << "Expected (optional) date with timezone type, but got: " << *input->Head().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TDataExprType>(WithoutTzDate(dataType1->GetSlot())));
        if (isOptional1) {
            input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->GetTypeAnn()));
        }

        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus JsonValueWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        using NNodes::TCoJsonValue;
        if (!EnsureMinArgsCount(*input, 7, ctx.Expr)
            || !EnsureMaxArgsCount(*input, 8, ctx.Expr)
            || !EnsureAtom(*input->Child(TCoJsonValue::idx_OnEmptyMode), ctx.Expr)
            || !EnsureAtom(*input->Child(TCoJsonValue::idx_OnErrorMode), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (TCoJsonValue::idx_ReturningType < input->ChildrenSize()) {
            auto status = EnsureTypeRewrite(input->ChildRef(TCoJsonValue::idx_ReturningType), ctx.Expr);
            if (status != IGraphTransformer::TStatus::Ok) {
                return status;
            }
        }

        TCoJsonValue jsonValue(input);

        // check first 3 common arguments
        if (!EnsureJsonQueryFunction(jsonValue, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        // default return value type is "Utf8?"
        EDataSlot resultSlot = EDataSlot::Utf8;

        // check if user provided custom return value type
        const auto& returningTypeArg = jsonValue.ReturningType();
        if (returningTypeArg) {
            const auto* returningTypeAnn = returningTypeArg.Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!EnsureDataType(returningTypeArg.Ref().Pos(), *returningTypeAnn, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            resultSlot = returningTypeAnn->Cast<TDataExprType>()->GetSlot();

            if (!IsDataTypeNumeric(resultSlot)
                && !IsDataTypeDate(resultSlot)
                && resultSlot != EDataSlot::Utf8
                && resultSlot != EDataSlot::String
                && resultSlot != EDataSlot::Bool) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Returning argument of JsonValue callable supports only Utf8, String, Bool, date and numeric types"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        // ON ERROR and ON EMPTY values must be castable to resultSlot or "Null"
        auto isValidCaseHandler = [&] (const TExprNode& node) {
            const auto* typeAnn = node.GetTypeAnn();
            if (!typeAnn) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node.Pos()), "Expected computable value, but got lambda"));
                return false;
            }

            if (IsNull(node)) {
                return true;
            }

            bool isOptional;
            const TDataExprType* dataType;
            if (!EnsureDataOrOptionalOfData(node, isOptional, dataType, ctx.Expr)) {
                return false;
            }

            const auto handlerSlot = dataType->GetSlot();
            const auto castResult = GetCastResult(handlerSlot, resultSlot);
            if (!castResult.Defined() || *castResult == NUdf::ECastOptions::Impossible) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(node.Pos()),
                    TStringBuilder() << "Cannot cast type of case handler " << handlerSlot << " to the returning type of JSON_VALUE " << resultSlot));
                return false;
            }

            return true;
        };

        if (!isValidCaseHandler(jsonValue.OnEmpty().Ref()) || !isValidCaseHandler(jsonValue.OnError().Ref())) {
            return IGraphTransformer::TStatus::Error;
        }

        // make returning type optional
        const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TDataExprType>(resultSlot);
        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(resultType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus JsonExistsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureMinArgsCount(*input, 3, ctx.Expr) || !EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        NNodes::TCoJsonExists jsonExists(input);

        // check first 3 common arguments
        if (!EnsureJsonQueryFunction(jsonExists, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        // onError argument if present must be "Bool?" type
        if (jsonExists.OnError()) {
            const auto& onErrorArg = jsonExists.OnError().Ref();

            if (!EnsureOptionalType(onErrorArg, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto optionalTypeAnn = onErrorArg.GetTypeAnn();
            if (!optionalTypeAnn) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(onErrorArg.Pos()), "Expected optional Bool, but got lambda"));
                return IGraphTransformer::TStatus::Error;
            }

            const auto underlyingType = optionalTypeAnn->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureSpecificDataType(onErrorArg.Pos(), *underlyingType, EDataSlot::Bool, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        // make returning type optional
        const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool);
        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(resultType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus JsonQueryWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 6, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        using NNodes::TCoJsonQuery;
        if (!EnsureAtom(*input->Child(TCoJsonQuery::idx_WrapMode), ctx.Expr)
            || !EnsureAtom(*input->Child(TCoJsonQuery::idx_OnEmpty), ctx.Expr)
            || !EnsureAtom(*input->Child(TCoJsonQuery::idx_OnError), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TCoJsonQuery jsonQuery(input);

        // check first 3 common arguments
        if (!EnsureJsonQueryFunction(jsonQuery, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& wrapModeArg = jsonQuery.WrapMode().Ref();
        EJsonQueryWrap wrapMode;
        if (!TryFromString(wrapModeArg.Content(), wrapMode)) {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Invalid value for WrapMode argument. Available options are: " << GetEnumAllNames<EJsonQueryWrap>()
            ));
            return IGraphTransformer::TStatus::Error;
        }

        const auto& onEmptyArg = jsonQuery.OnEmpty().Ref();
        EJsonQueryHandler onEmpty;
        if (!TryFromString(onEmptyArg.Content(), onEmpty)) {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Invalid value for OnEmpty argument. Available options are: " << GetEnumAllNames<EJsonQueryHandler>()
            ));
            return IGraphTransformer::TStatus::Error;
        }

        const auto& onErrorArg = jsonQuery.OnError().Ref();
        EJsonQueryHandler onError;
        if (!TryFromString(onErrorArg.Content(), onError)) {
            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Invalid value for OnError argument. Available options are: " << GetEnumAllNames<EJsonQueryHandler>()
            ));
            return IGraphTransformer::TStatus::Error;
        }

        // make returning type optional
        EDataSlot returnType = EDataSlot::JsonDocument;
        if (!ctx.Types.JsonQueryReturnsJsonDocument) {
            auto issue = TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                "JSON_QUERY returning Json type is deprecated. Please use PRAGMA JsonQueryReturnsJsonDocument; to "
                "make JSON_QUERY return JsonDocument type. It will be turned on by default soon"
            );
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_JSON_QUERY_RETURNING_JSON_IS_DEPRECATED, issue);
            if (!ctx.Expr.AddWarning(issue)) {
                return IGraphTransformer::TStatus::Error;
            }
            returnType = EDataSlot::Json;
        }
        const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TDataExprType>(returnType);
        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(resultType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus JsonVariablesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        for (size_t i = 0; i < input->ChildrenSize(); i++) {
            const auto& tuple = input->Child(i);

            using NNodes::TCoNameValueTuple;
            if (!EnsureTuple(*tuple, ctx.Expr) || !EnsureTupleSize(*tuple, 2, ctx.Expr) || !EnsureAtom(*tuple->Child(TCoNameValueTuple::idx_Name), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            TCoNameValueTuple nameValueTuple(tuple);
            const auto& variableValue = nameValueTuple.Value().Ref();
            if (IsNull(variableValue)) {
                continue;
            }

            bool isOptional;
            const TDataExprType* valueType;
            if (!EnsureDataOrOptionalOfData(variableValue, isOptional, valueType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto valueSlot = valueType->GetSlot();
            if (!IsDataTypeNumeric(valueSlot)
                && !IsDataTypeDate(valueSlot)
                && valueSlot != EDataSlot::Utf8
                && valueSlot != EDataSlot::Bool
                && valueSlot != EDataSlot::Json) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "You can pass only values of Utf8, Bool, Json, date and numeric types for jsonpath variables"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const auto* keyType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Utf8);
        const auto* payloadType = ctx.Expr.MakeType<TResourceExprType>("JsonNode");
        input->SetTypeAnn(ctx.Expr.MakeType<TDictExprType>(keyType, payloadType));
        return IGraphTransformer::TStatus::Ok;
    }

    bool IsValidTypeForRanges(const TTypeAnnotationNode* type) {
        YQL_ENSURE(type);
        if (type->GetKind() != ETypeAnnotationKind::Optional) {
            return false;
        }
        type = RemoveAllOptionals(type);
        YQL_ENSURE(type);
        return type->GetKind() == ETypeAnnotationKind::Data && type->IsComparable() && type->IsEquatable();
    }

    bool EnsureValidRangeBoundary(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx) {
        if (!type) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected tuple type, but got lambda"));
            return false;
        }

        if (type->GetKind() != ETypeAnnotationKind::Tuple) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected tuple type, but got: " << *type));
            return false;
        }

        const auto& components = type->Cast<TTupleExprType>()->GetItems();
        if (components.size() < 3 || components.size() % 2 == 0) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected tuple of minimal size 3 with odd number of components but got: " << *type));
            return false;
        }

        for (size_t i = 0; i < components.size(); ++i) {
            auto itemType = components[i];
            YQL_ENSURE(itemType);
            if (i % 2 == 0) {
                if (itemType->GetKind() != ETypeAnnotationKind::Data || itemType->Cast<TDataExprType>()->GetSlot() != EDataSlot::Int32) {
                    ctx.AddError(TIssue(ctx.GetPosition(pos),
                        TStringBuilder() << "Expected " << i <<
                            "th component of range boundary tuple to be Int32, but got: " << *itemType));
                    return false;
                }
            } else if (!IsValidTypeForRanges(itemType)) {
                ctx.AddError(TIssue(ctx.GetPosition(pos),
                    TStringBuilder() << "Expected " << i <<
                        "th component of range boundary tuple to be (multi) optional of "
                        "comparable and equatable Data type, but got: " << *itemType));
                return false;
            }
        }
        return true;
    }

    bool EnsureValidRange(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext& ctx) {
        if (!type) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected tuple type, but got lambda"));
            return false;
        }

        if (type->GetKind() != ETypeAnnotationKind::Tuple) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected tuple type, but got: " << *type));
            return false;
        }

        const auto& components = type->Cast<TTupleExprType>()->GetItems();
        if (components.size() != 2) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected tuple of size 2, but got: " << *type));
            return false;
        }

        if (!EnsureValidRangeBoundary(pos, components.front(), ctx)) {
            return false;
        }

        YQL_ENSURE(components.front() && components.back());
        if (!IsSameAnnotation(*components.front(), *components.back())) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Range begin/end type mismatch. Begin: " << *components.front()
                     << " End: " << *components.back()));
            return false;
        }
        return true;
    }

    bool EnsureValidUserRange(TPositionHandle pos, const TTypeAnnotationNode& range,
        const TTypeAnnotationNode*& resultRange, TExprContext& ctx)
    {
        resultRange = nullptr;
        if (range.GetKind() != ETypeAnnotationKind::Tuple) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected range to be tuple, but got: " << range));
            return false;
        }

        auto rangeTuple = range.Cast<TTupleExprType>();
        if (rangeTuple->GetSize() != 2) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected range tuple to be of size 2, but got: " << rangeTuple->GetSize()));
            return false;
        }

        if (!IsSameAnnotation(*rangeTuple->GetItems().front(), *rangeTuple->GetItems().back())) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected both component of range to be of same type, but got: "
                                 << *rangeTuple->GetItems().front() << " and " << *rangeTuple->GetItems().back()));
            return false;
        }

        auto boundaryType = rangeTuple->GetItems().front();
        if (boundaryType->GetKind() != ETypeAnnotationKind::Tuple) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected range boundary to be tuple, but got: " << *boundaryType));
            return false;
        }

        auto boundaryTuple = boundaryType->Cast<TTupleExprType>();
        if (boundaryTuple->GetSize() < 2) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected range boundary tuple to consist of at least 2 components, but got: " << boundaryTuple->GetSize()));
            return false;
        }

        // User range boundary encoding: AsTuple(x, ..., z, 0/1) - 0/1 means included/excluded.
        // For column type T, boundary value x should have type T?, top level NULL means infinity.
        // Infinity sign is implicit - infinity in left boundary always means minus infinity and plus infinity if in right boundary
        TTypeAnnotationNode::TListType resultBoundaryItems;
        auto int32Type = ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        const auto& items = boundaryTuple->GetItems();
        for (size_t i = 0; i < items.size() - 1; ++i) {
            if (!IsValidTypeForRanges(items[i])) {
                ctx.AddError(TIssue(ctx.GetPosition(pos),
                    TStringBuilder() << "Expected " << i << "th component of range boundary tuple to be (multi) optional of "
                                                            "comparable and equatable Data type, but got: " << *items[i]));
                return false;
            }
            resultBoundaryItems.push_back(int32Type);
            resultBoundaryItems.push_back(items[i]);
        }

        YQL_ENSURE(items.back());
        if (items.back()->GetKind() != ETypeAnnotationKind::Data || items.back()->Cast<TDataExprType>()->GetSlot() != EDataSlot::Int32) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected last component of range boundary tuple to be Int32, "
                                    "but got: " << *items.back()));
            return false;
        }

        resultBoundaryItems.push_back(int32Type);
        auto resultBoundary = ctx.MakeType<TTupleExprType>(resultBoundaryItems);
        resultRange = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{resultBoundary, resultBoundary});
        return true;
    }

    IGraphTransformer::TStatus AsRangeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (input->ChildrenSize() == 0) {
            output = ctx.Expr.RenameNode(*input, "RangeEmpty");
            return IGraphTransformer::TStatus::Repeat;
        }

        const TTypeAnnotationNode* rangeType = nullptr;
        const TTypeAnnotationNode* resultRangeType = nullptr;
        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            auto child = input->Child(i);
            TPositionHandle pos = child->Pos();
            auto argType = child->GetTypeAnn();
            if (!argType) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(pos),
                    TStringBuilder() << "Expected range tuple as " << i << "th argument, but got lambda"));
                return IGraphTransformer::TStatus::Error;
            }

            if (!rangeType) {
                rangeType = argType;
                if (!EnsureValidUserRange(pos, *rangeType, resultRangeType, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }
            }

            if (!IsSameAnnotation(*rangeType, *argType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(pos),
                    TStringBuilder() << "Expected all arguments to be of same type, but got: " << *rangeType
                                     << " as first argument and " << *argType << " as " << i << "th"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultRangeType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RangeCreateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto rangeType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        const TTypeAnnotationNode* resultRangeType = nullptr;
        if (!EnsureValidUserRange(input->Head().Pos(), *rangeType, resultRangeType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultRangeType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RangeEmptyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMaxArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (input->ChildrenSize() == 0) {
            input->SetTypeAnn(ctx.Expr.MakeType<TEmptyListExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        TTypeAnnotationNode::TListType optKeys;
        auto keyType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (keyType->GetKind() == ETypeAnnotationKind::Tuple) {
            for (auto& type : keyType->Cast<TTupleExprType>()->GetItems()) {
                optKeys.push_back(ctx.Expr.MakeType<TOptionalExprType>(type));
            }
        } else {
            optKeys.push_back(ctx.Expr.MakeType<TOptionalExprType>(keyType));
        }

        auto int32Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int32);
        TTypeAnnotationNode::TListType resultBoundaryItems;
        for (auto& optKeyType : optKeys) {
            if (!IsValidTypeForRanges(optKeyType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                    TStringBuilder() << "Expected (multi) optional of comparable and equatable Data type, but got: " << *optKeyType));
                return IGraphTransformer::TStatus::Error;
            }
            resultBoundaryItems.push_back(int32Type);
            resultBoundaryItems.push_back(optKeyType);
        }

        resultBoundaryItems.push_back(int32Type);

        const TTypeAnnotationNode* resultBoundaryType = ctx.Expr.MakeType<TTupleExprType>(resultBoundaryItems);
        const TTypeAnnotationNode* resultRangeType =
            ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{resultBoundaryType, resultBoundaryType});

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultRangeType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RangeForWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TStringBuf op = input->Head().Content();
        static const THashSet<TStringBuf> ops = {"==", "!=", "<=", "<", ">=", ">", "Exists", "NotExists", "===", "StartsWith", "NotStartsWith"};
        if (!ops.contains(op)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                TStringBuilder() << "Unknown operation: " << op));
            return IGraphTransformer::TStatus::Error;
        }

        auto valueType = input->Child(1)->GetTypeAnn();
        if (!valueType) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()),
                TStringBuilder() << "Expecting (optional) Data as second argument, but got lambda"));
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* valueBaseType = nullptr;
        if (op != "Exists" && op != "NotExists") {
            valueBaseType = RemoveAllOptionals(valueType);
            YQL_ENSURE(valueBaseType);
            if (valueBaseType->GetKind() != ETypeAnnotationKind::Data) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()),
                                         TStringBuilder() << "Expecting (optional) Data as second argument, but got: " << *valueType));
                return IGraphTransformer::TStatus::Error;
            }
        } else if (!EnsureVoidType(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->TailRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto keyType = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        YQL_ENSURE(keyType);
        auto optKeyType = ctx.Expr.MakeType<TOptionalExprType>(keyType);
        if (!IsValidTypeForRanges(optKeyType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                TStringBuilder() << "Expected (optional) of comparable and equatable Data type, but got: " << *keyType));
            return IGraphTransformer::TStatus::Error;
        }

        if (valueBaseType && CanCompare<false>(RemoveAllOptionals(keyType), valueBaseType) != ECompareOptions::Comparable) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Uncompatible key and value types: " << *keyType << " and " << *valueType));
            return IGraphTransformer::TStatus::Error;
        }

        auto int32Type = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int32);
        TTypeAnnotationNode::TListType resultBoundaryItems;
        resultBoundaryItems.push_back(int32Type);
        resultBoundaryItems.push_back(optKeyType);
        resultBoundaryItems.push_back(int32Type);

        const TTypeAnnotationNode* resultBoundaryType = ctx.Expr.MakeType<TTupleExprType>(resultBoundaryItems);
        const TTypeAnnotationNode* resultRangeType =
            ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{resultBoundaryType, resultBoundaryType});

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultRangeType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RangeUnionWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto argType = input->Head().GetTypeAnn();
        auto rangeType = argType->Cast<TListExprType>()->GetItemType();
        if (!EnsureValidRange(input->Head().Pos(), rangeType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            TPositionHandle pos = input->Child(i)->Pos();
            auto type = input->Child(i)->GetTypeAnn();
            if (!type) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(pos),
                    TStringBuilder() << "Expected " << *argType << " as argument #" << i << ", but got lambda"));
                return IGraphTransformer::TStatus::Error;
            }

            if (!IsSameAnnotation(*type, *argType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(pos),
                    TStringBuilder() << "Expected " << *argType << " as argument #" << i << ", but got: " << *type));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(argType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RangeMultiplyWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureMinArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Head(), EDataSlot::Uint64, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TTypeAnnotationNode::TListType resultComponents;
        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            if (!EnsureListType(*input->Child(i), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto rangeType = input->Child(i)->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (!EnsureValidRange(input->Child(i)->Pos(), rangeType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto& components = rangeType->Cast<TTupleExprType>()->GetItems().front()->Cast<TTupleExprType>()->GetItems();
            YQL_ENSURE(components.size() >= 3);
            resultComponents.insert(resultComponents.end(), components.begin(), components.end() - 1);
        }
        resultComponents.push_back(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int32));

        auto resultRangeBoundaryType = ctx.Expr.MakeType<TTupleExprType>(resultComponents);
        auto resultRangeType =
            ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{resultRangeBoundaryType, resultRangeBoundaryType});

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultRangeType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RangeFinalizeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto argType = input->Head().GetTypeAnn();
        auto rangeType = argType->Cast<TListExprType>()->GetItemType();
        if (!EnsureValidRange(input->Head().Pos(), rangeType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& components = rangeType->Cast<TTupleExprType>()->GetItems().front()->Cast<TTupleExprType>()->GetItems();
        TTypeAnnotationNode::TListType resultComponents;
        for (size_t i = 0; i < components.size(); ++i) {
            // take odd and last component
            if (i % 2 == 1 || i + 1 == components.size()) {
                resultComponents.push_back(components[i]);
            }
        }

        auto resultRangeBoundaryType = ctx.Expr.MakeType<TTupleExprType>(resultComponents);
        auto resultRangeType =
            ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{resultRangeBoundaryType, resultRangeBoundaryType});

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultRangeType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RangeComputeForWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 3, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto rowType = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        YQL_ENSURE(rowType);
        if (!EnsureStructType(input->Head().Pos(), *rowType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambdaNode = input->ChildRef(1);
        const auto status = ConvertToLambda(lambdaNode, ctx.Expr, 1);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!UpdateLambdaAllArgumentsTypes(lambdaNode, { rowType }, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambdaNode->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        // extract_predicate library supports TCoConditionalValueBase in lambda root or just plain predicate lambda
        using namespace NNodes;
        TCoLambda lambda(lambdaNode);
        if (!lambda.Body().Maybe<TCoConditionalValueBase>() && !EnsureSpecificDataType(lambda.Ref(), EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& tupleOfAtomsNode = input->Tail();
        if (!EnsureTupleMinSize(tupleOfAtomsNode, 1, ctx.Expr) || !EnsureTupleOfAtoms(tupleOfAtomsNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        THashSet<TStringBuf> indexKeys;
        const TStructExprType& structType = *rowType->Cast<TStructExprType>();
        TTypeAnnotationNode::TListType rangeBoundaryTypes;
        for (auto& keyNode : tupleOfAtomsNode.ChildrenList()) {
            TStringBuf key = keyNode->Content();
            if (!indexKeys.insert(key).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyNode->Pos()),
                    TStringBuilder() << "Duplicate index column '" << key << "'"));
                return IGraphTransformer::TStatus::Error;
            }

            auto pos = FindOrReportMissingMember(key, keyNode->Pos(), structType, ctx);
            if (!pos) {
                return IGraphTransformer::TStatus::Error;
            }

            auto keyType = structType.GetItems()[*pos]->GetItemType();
            auto optKeyType = ctx.Expr.MakeType<TOptionalExprType>(keyType);
            if (!IsValidTypeForRanges(optKeyType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyNode->Pos()),
                    TStringBuilder() << "Unsupported index column type: expecting Data or (multi) optional of Data, "
                                     << "got: " << *keyType << " for column '" << key << "'"));
                return IGraphTransformer::TStatus::Error;
            }

            rangeBoundaryTypes.push_back(optKeyType);
        }
        rangeBoundaryTypes.push_back(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int32));

        auto resultRangeBoundaryType = ctx.Expr.MakeType<TTupleExprType>(rangeBoundaryTypes);
        auto resultRangeType =
            ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{resultRangeBoundaryType, resultRangeBoundaryType});
        auto resultRangesType = ctx.Expr.MakeType<TListExprType>(resultRangeType);
        auto serializedLambdaType =
            ctx.Expr.MakeType<TTaggedExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::String), "AST");

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(
            ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{resultRangesType, serializedLambdaType})));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus RoundWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureDataType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (auto status = EnsureTypeRewrite(input->TailRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        auto dstType = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureDataType(input->Tail().Pos(), *dstType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto srcType = input->Head().GetTypeAnn();
        if (CanCompare<false>(srcType, dstType) != ECompareOptions::Comparable) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Uncompatible types in rounding: " << *srcType << " " << input->Content() << " to " << *dstType));
            return IGraphTransformer::TStatus::Error;
        }

        if (IsSameAnnotation(*srcType, *dstType)) {
            output = ctx.Expr.NewCallable(input->Pos(), "Just", { input->HeadPtr() });
            return IGraphTransformer::TStatus::Repeat;
        }

        auto sSlot = srcType->Cast<TDataExprType>()->GetSlot();
        auto tSlot = dstType->Cast<TDataExprType>()->GetSlot();
        auto resultType = ctx.Expr.MakeType<TOptionalExprType>(dstType);

        const auto cast = NUdf::GetCastResult(sSlot, tSlot);
        if (!cast) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Unsupported types in rounding: " << *srcType << " " << input->Content() << " to " << *dstType));
            return IGraphTransformer::TStatus::Error;
        }

        if (!(*cast & (NUdf::ECastOptions::MayFail | NUdf::ECastOptions::MayLoseData | NUdf::ECastOptions::AnywayLoseData))) {
            output = ctx.Expr.NewCallable(input->Pos(), "SafeCast",
                { input->HeadPtr(), ExpandType(input->Pos(), *resultType, ctx.Expr) });
            return IGraphTransformer::TStatus::Repeat;
        }

        input->SetTypeAnn(resultType);
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus NextValueWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStringOrUtf8Type(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TOptionalExprType>(input->Head().GetTypeAnn()));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus AssumeAllMembersNullableAtOnceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureStructOrOptionalStructType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = input->Head().GetTypeAnn();
        if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
            itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
        }

        for (const auto& x : itemType->Cast<TStructExprType>()->GetItems()) {
            if (x->GetItemType()->GetKind() != ETypeAnnotationKind::Optional) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected all columns to be optional. Non optional column: " << x->GetName()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        input->SetTypeAnn(input->Head().GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    TSyncFunctionsMap::TSyncFunctionsMap() {
        Functions["Data"] = &DataWrapper;
        Functions["DataSource"] = &DataSourceWrapper;
        Functions["Key"] = &KeyWrapper;
        Functions[LeftName] = &LeftWrapper;
        Functions[RightName] = &RightWrapper;
        Functions[ConsName] = &ConsWrapper;
        Functions["DataSink"] = &DataSinkWrapper;
        Functions["Filter"] = &FilterWrapper;
        Functions["OrderedFilter"] = &FilterWrapper;
        Functions["TakeWhile"] = &FilterWrapper;
        Functions["SkipWhile"] = &FilterWrapper;
        Functions["TakeWhileInclusive"] = &InclusiveFilterWrapper<false>;
        Functions["SkipWhileInclusive"] = &InclusiveFilterWrapper<true>;
        Functions["Member"] = &MemberWrapper;
        Functions["SingleMember"] = &SingleMemberWrapper;
        Functions["SqlColumn"] = &SqlColumnWrapper;
        Functions["SqlPlainColumn"] = &SqlColumnWrapper;
        Functions["SqlColumnOrType"] = &SqlColumnWrapper;
        Functions["SqlPlainColumnOrType"] = &SqlColumnWrapper;
        Functions["SqlColumnFromType"] = &SqlColumnFromTypeWrapper;
        Functions["Nth"] = &NthWrapper;
        Functions["FlattenMembers"] = &FlattenMembersWrapper;
        Functions["SelectMembers"] = &SelectMembersWrapper<true>;
        Functions["FilterMembers"] = &SelectMembersWrapper<false>;
        Functions["DivePrefixMembers"] = &DivePrefixMembersWrapper;
        Functions["FlattenByColumns"] = &FlattenByColumns;
        Functions["ExtractMembers"] = &ExtractMembersWrapper;
        Functions["FlattenStructs"] = &FlattenStructsWrapper;
        Functions["<"] = &CompareWrapper<false>;
        Functions["Less"] = &CompareWrapper<false>;
        Functions["<="] = &CompareWrapper<false>;
        Functions["LessOrEqual"] = &CompareWrapper<false>;
        Functions[">"] = &CompareWrapper<false>;
        Functions["Greater"] = &CompareWrapper<false>;
        Functions[">="] = &CompareWrapper<false>;
        Functions["GreaterOrEqual"] = &CompareWrapper<false>;
        Functions["=="] = &CompareWrapper<true>;
        Functions["Equal"] = &CompareWrapper<true>;
        Functions["!="] = &CompareWrapper<true>;
        Functions["NotEqual"] = &CompareWrapper<true>;
        Functions["Inc"] = &IncDecWrapper<true>;
        Functions["Dec"] = &IncDecWrapper<false>;
        Functions["BitNot"] = &BitOpsWrapper<1>;
        Functions["CountBits"] = &CountBitsWrapper;
        Functions["Plus"] = &PlusMinusWrapper;
        Functions["Minus"] = &PlusMinusWrapper;
        Functions["+"] = &AddWrapper;
        Functions["Add"] = &AddWrapper;
        Functions["AggrAdd"] = &AggrAddWrapper;
        Functions["-"] = &SubWrapper;
        Functions["Sub"] = &SubWrapper;
        Functions["*"] = &MulWrapper;
        Functions["Mul"] = &MulWrapper;
        Functions["/"] = &DivWrapper;
        Functions["Div"] = &DivWrapper;
        Functions["%"] = &ModWrapper;
        Functions["Mod"] = &ModWrapper;
        Functions["BitAnd"] = &BitOpsWrapper<2>;
        Functions["BitOr"] = &BitOpsWrapper<2>;
        Functions["BitXor"] = &BitOpsWrapper<2>;
        Functions["Min"] = &MinMaxWrapper;
        Functions["Max"] = &MinMaxWrapper;
        Functions["AggrEquals"] = &AggrCompareWrapper<true, false>;
        Functions["AggrNotEquals"] = &AggrCompareWrapper<false, false>;
        Functions["AggrLess"] = &AggrCompareWrapper<false, true>;
        Functions["AggrLessOrEqual"] = &AggrCompareWrapper<true, true>;
        Functions["AggrGreater"] = &AggrCompareWrapper<false, true>;
        Functions["AggrGreaterOrEqual"] = &AggrCompareWrapper<true, true>;
        Functions["AggrMin"] = &AggrMinMaxWrapper;
        Functions["AggrMax"] = &AggrMinMaxWrapper;
        Functions["IsNotDistinctFrom"] = &DistinctFromWrapper;
        Functions["IsDistinctFrom"] =  &DistinctFromWrapper;;
        Functions["Abs"] = &AbsWrapper;
        Functions["ShiftLeft"] = &ShiftWrapper;
        Functions["RotLeft"] = &ShiftWrapper;
        Functions["ShiftRight"] = &ShiftWrapper;
        Functions["RotRight"] = &ShiftWrapper;
        Functions[SyncName] = &SyncWrapper;
        Functions["Concat"] = &ConcatWrapper;
        Functions["AggrConcat"] = &AggrConcatWrapper;
        Functions["Substring"] = &SubstringWrapper;
        Functions["Find"] = &FindWrapper;
        Functions["RFind"] = &FindWrapper;
        Functions["StartsWith"] = &WithWrapper;
        Functions["EndsWith"] = &WithWrapper;
        Functions["ByteAt"] = &ByteAtWrapper;
        Functions["ListIf"] = &ListIfWrapper;
        Functions["AsList"] = &AsListWrapper<false>;
        Functions["AsListStrict"] = &AsListWrapper<true>;
        Functions["ToList"] = &ToListWrapper;
        Functions["ToOptional"] = &ToOptionalWrapper;
        Functions["Iterable"] = &IterableWrapper;
        Functions["Head"] = &HeadWrapper;
        Functions["Last"] = &ToOptionalWrapper;
        Functions["AsTagged"] = &AsTaggedWrapper;
        Functions["Untag"] = &UntagWrapper;
        Functions["And"] = &LogicalWrapper<false>;
        Functions["Or"] = &LogicalWrapper<false>;
        Functions["Xor"] = &LogicalWrapper<true>;
        Functions["Not"] = &BoolOpt1Wrapper;
        Functions["Likely"] = &BoolOpt1Wrapper;
        Functions["Map"] = &MapWrapper;
        Functions["OrderedMap"] = &MapWrapper;
        Functions["FoldMap"] = &FoldMapWrapper;
        Functions["Fold1Map"] = &Fold1MapWrapper;
        Functions["Chain1Map"] = &Chain1MapWrapper;
        Functions["LMap"] = &LMapWrapper;
        Functions["OrderedLMap"] = &LMapWrapper;
        Functions["Struct"] = &StructWrapper;
        Functions["AddMember"] = &AddMemberWrapper;
        Functions["RemoveMember"] = &RemoveMemberWrapper<false>;
        Functions["ForceRemoveMember"] = &RemoveMemberWrapper<true>;
        Functions["ReplaceMember"] = &ReplaceMemberWrapper;
        Functions["RemovePrefixMembers"] = &RemovePrefixMembersWrapper;
        Functions["RemoveSystemMembers"] = &RemoveSystemMembersWrapper;
        Functions["FlatMap"] = &FlatMapWrapper<false>;
        Functions["OrderedFlatMap"] = &FlatMapWrapper<false>;
        Functions["OrderedFlatMapWarn"] = &FlatMapWrapper<true>;
        Functions["MultiMap"] = &MultiMapWrapper<false>;
        Functions["OrderedMultiMap"] = &MultiMapWrapper<true>;
        Functions["FlatMapToEquiJoin"] = &FlatMapWrapper<false>;
        Functions["OrderedFlatMapToEquiJoin"] = &FlatMapWrapper<false>;
        Functions["FlatListIf"] = &FlatListIfWrapper;
        Functions["FlatOptionalIf"] = &FlatOptionalIfWrapper;
        Functions["Size"] = &SizeWrapper;
        Functions["Length"] = &LengthWrapper;
        Functions["Iterator"] = &IteratorWrapper;
        Functions["EmptyIterator"] = &EmptyIteratorWrapper;
        Functions["ForwardList"] = &ForwardListWrapper;
        Functions["ToStream"] = &ToStreamWrapper;
        Functions["ToSequence"] = &ToSequenceWrapper;
        Functions["Collect"] = &CollectWrapper;
        Functions["LazyList"] = &LazyListWrapper;
        Functions["ListFromRange"] = &ListFromRangeWrapper;
        Functions["Replicate"] = &ReplicateWrapper;
        Functions["Switch"] = &SwitchWrapper;
        Functions["Chopper"] = &ChopperWrapper;
        Functions["HasItems"] = &HasItemsWrapper;
        Functions["Append"] = &AppendWrapper;
        Functions["Insert"] = &AppendWrapper;
        Functions["Prepend"] = &PrependWrapper;
        Functions["Extend"] = &ExtendWrapper;
        Functions["OrderedExtend"] = &ExtendWrapper;
        Functions["Merge"] = &ExtendWrapper;
        Functions["Extract"] = &ExtractWrapper;
        Functions["OrderedExtract"] = &ExtractWrapper;
        Functions["UnionAll"] = &UnionAllWrapper;
        Functions["UnionMerge"] = &UnionAllWrapper;
        Functions["ListExtend"] = &ListExtendWrapper<false>;
        Functions["ListExtendStrict"] = &ListExtendWrapper<true>;
        Functions["ListUnionAll"] = &ListUnionAllWrapper;
        Functions["ListZip"] = &ListZipWrapper;
        Functions["ListZipAll"] = &ListZipAllWrapper;
        Functions["Sort"] = &SortWrapper;
        Functions["AssumeSorted"] = &SortWrapper;
        Functions["AssumeUnique"] = &AssumeUniqueWrapper;
        Functions["AssumeAllMembersNullableAtOnce"] = &AssumeAllMembersNullableAtOnceWrapper;
        Functions["Top"] = &TopWrapper;
        Functions["TopSort"] = &TopWrapper;
        Functions["KeepTop"] = &KeepTopWrapper;
        Functions["Unordered"] = &UnorderedWrapper;
        Functions["UnorderedSubquery"] = &UnorderedWrapper;
        Functions["SortTraits"] = &SortTraitsWrapper;
        Functions["SessionWindowTraits"] = &SessionWindowTraitsWrapper;
        Functions["FromString"] = &FromStringWrapper;
        Functions["StrictFromString"] = &StrictFromStringWrapper;
        Functions["FromBytes"] = &FromBytesWrapper;
        Functions["Convert"] = &ConvertWrapper;
        Functions["AlterTo"] = &AlterToWrapper;
        Functions["ToIntegral"] = &ToIntegralWrapper;
        Functions["Cast"] = &OldCastWrapper;
        Functions["SafeCast"] = &CastWrapper<false>;
        Functions["StrictCast"] = &CastWrapper<true>;
        Functions["BitCast"] = &BitCastWrapper;
        Functions["WidenIntegral"] = &WidenIntegralWrapper;
        Functions["Default"] = &DefaultWrapper;
        Functions["Pickle"] = &PickleWrapper;
        Functions["StablePickle"] = &PickleWrapper;
        Functions["Unpickle"] = &UnpickleWrapper;
        Functions["Coalesce"] = &CoalesceWrapper;
        Functions["CoalesceMembers"] = &CoalesceMembersWrapper;
        Functions["Nvl"] = &NvlWrapper;
        Functions["Nanvl"] = &NanvlWrapper;
        Functions["Unwrap"] = &UnwrapWrapper;
        Functions["Exists"] = &ExistsWrapper;
        Functions["Just"] = &JustWrapper;
        Functions["Optional"] = &OptionalWrapper;
        Functions["OptionalIf"] = &OptionalIfWrapper;
        Functions["ToString"] = &ToStringWrapper;
        Functions["ToBytes"] = &ToBytesWrapper;
        Functions["GroupByKey"] = &GroupByKeyWrapper;
        Functions["PartitionByKey"] = &PartitionByKeyWrapper;
        Functions["PartitionsByKeys"] = &PartitionsByKeysWrapper;
        Functions["Reverse"] = &ReverseWrapper;
        Functions["Skip"] = &TakeWrapper;
        Functions["Take"] = &TakeWrapper;
        Functions["Limit"] = &TakeWrapper;
        Functions["Fold"] = &FoldWrapper;
        Functions["Fold1"] = &Fold1Wrapper;
        Functions["Condense"] = &CondenseWrapper;
        Functions["Condense1"] = &Condense1Wrapper;
        Functions["Squeeze"] = &SqueezeWrapper;
        Functions["Squeeze1"] = &Squeeze1Wrapper;
        Functions["Discard"] = &DiscardWrapper;
        Functions["Zip"] = &ZipWrapper;
        Functions["ZipAll"] = &ZipAllWrapper;
        Functions["Enumerate"] = &EnumerateWrapper;
        Functions["GenericType"] = &TypeWrapper<ETypeAnnotationKind::Generic>;
        Functions["ResourceType"] = &TypeWrapper<ETypeAnnotationKind::Resource>;
        Functions["ErrorType"] = &TypeWrapper<ETypeAnnotationKind::Error>;
        Functions["DataType"] = &TypeWrapper<ETypeAnnotationKind::Data>;
        Functions["ListType"] = &TypeWrapper<ETypeAnnotationKind::List>;
        Functions["TupleType"] = &TypeWrapper<ETypeAnnotationKind::Tuple>;
        Functions["MultiType"] = &TypeWrapper<ETypeAnnotationKind::Multi>;
        Functions["StructType"] = &TypeWrapper<ETypeAnnotationKind::Struct>;
        Functions["OptionalType"] = &TypeWrapper<ETypeAnnotationKind::Optional>;
        Functions["TaggedType"] = &TypeWrapper<ETypeAnnotationKind::Tagged>;
        Functions["VariantType"] = &TypeWrapper<ETypeAnnotationKind::Variant>;
        Functions["StreamType"] = &TypeWrapper<ETypeAnnotationKind::Stream>;
        Functions["FlowType"] = &TypeWrapper<ETypeAnnotationKind::Flow>;
        Functions["Nothing"] = &NothingWrapper;
        Functions["List"] = &ListWrapper;
        Functions["DictType"] = &TypeWrapper<ETypeAnnotationKind::Dict>;
        Functions["Dict"] = &DictWrapper;
        Functions["Variant"] = &VariantWrapper;
        Functions["Enum"] = &EnumWrapper;
        Functions["AsVariant"] = &AsVariantWrapper;
        Functions["AsEnum"] = &AsEnumWrapper;
        Functions["Contains"] = &ContainsLookupWrapper<true>;
        Functions["SqlIn"] = &SqlInWrapper;
        Functions["Lookup"] = &ContainsLookupWrapper<false>;
        Functions["DictItems"] = &DictItemsWrapper<EDictItems::Both>;
        Functions["DictKeys"] = &DictItemsWrapper<EDictItems::Keys>;
        Functions["DictPayloads"] = &DictItemsWrapper<EDictItems::Payloads>;
        Functions["AsStruct"] = &AsStructWrapper;
        Functions["AsStructUnordered"] = &AsStructWrapper;
        Functions["AsDict"] = &AsDictWrapper<false, false>;
        Functions["AsDictStrict"] = &AsDictWrapper<true, false>;
        Functions["AsSet"] = &AsDictWrapper<false, true>;
        Functions["AsSetStrict"] = &AsDictWrapper<true, true>;
        Functions["DictFromKeys"] = &DictFromKeysWrapper;
        Functions["If"] = &IfWrapper<false>;
        Functions["IfStrict"] = &IfWrapper<true>;
        Functions[IfName] = &IfWorldWrapper;
        Functions[ForName] = &ForWorldWrapper;
        Functions["IfPresent"] = &IfPresentWrapper;
        Functions["StaticMap"] = &StaticMapWrapper;
        Functions["StaticZip"] = &StaticZipWrapper;
        Functions["TryRemoveAllOptionals"] = &TryRemoveAllOptionalsWrapper;
        Functions["HasNull"] = &HasNullWrapper;
        Functions["TypeOf"] = &TypeOfWrapper;
        Functions["ConstraintsOf"] = &ConstraintsOfWrapper;
        Functions["InstanceOf"] = &InstanceOfWrapper;
        Functions["SourceOf"] = &SourceOfWrapper;
        Functions["MatchType"] = &MatchTypeWrapper;
        Functions["IfType"] = &IfTypeWrapper;
        Functions["EnsureType"] = &TypeAssertWrapper<true>;
        Functions["EnsurePersistable"] = &PersistableAssertWrapper;
        Functions["PersistableRepr"] = &PersistableReprWrapper;
        Functions["EnsureConvertibleTo"] = &TypeAssertWrapper<false>;
        Functions["EnsureTupleSize"] = &TupleSizeAssertWrapper;
        Functions["Ensure"] = &EnsureWrapper;
        Functions["TryMember"] = &TryMemberWrapper;
        Functions["ToIndexDict"] = &ToIndexDictWrapper;
        Functions["ToDict"] = &ToDictWrapper;
        Functions["SqueezeToDict"] = &SqueezeToDictWrapper<false>;
        Functions["NarrowSqueezeToDict"] = &SqueezeToDictWrapper<true>;
        Functions["SqueezeToList"] = &SqueezeToListWrapper;
        Functions["Void"] = &VoidWrapper;
        Functions["Null"] = &NullWrapper;
        Functions["EmptyList"] = &EmptyListWrapper;
        Functions["EmptyDict"] = &EmptyDictWrapper;
        Functions["Error"] = &ErrorWrapper;
        Functions["VoidType"] = &TypeWrapper<ETypeAnnotationKind::Void>;
        Functions["UnitType"] = &TypeWrapper<ETypeAnnotationKind::Unit>;
        Functions["NullType"] = &TypeWrapper<ETypeAnnotationKind::Null>;
        Functions["EmptyListType"] = &TypeWrapper<ETypeAnnotationKind::EmptyList>;
        Functions["EmptyDictType"] = &TypeWrapper<ETypeAnnotationKind::EmptyDict>;
        Functions["Join"] = &JoinWrapper;
        Functions["JoinDict"] = &JoinDictWrapper;
        Functions["MapJoinCore"] = &MapJoinCoreWrapper;
        Functions["CommonJoinCore"] = &CommonJoinCoreWrapper;
        Functions["CombineCore"] = &CombineCoreWrapper;
        Functions["GroupingCore"] = &GroupingCoreWrapper;
        Functions["HoppingTraits"] = &HoppingTraitsWrapper;
        Functions["HoppingCore"] = &HoppingCoreWrapper;
        Functions["MultiHoppingCore"] = &MultiHoppingCoreWrapper;
        Functions["EquiJoin"] = &EquiJoinWrapper;
        Functions["OptionalReduce"] = &OptionalReduceWrapper;
        Functions["OptionalItemType"] = &TypeArgWrapper<ETypeArgument::OptionalItem>;
        Functions["ListItemType"] = &TypeArgWrapper<ETypeArgument::ListItem>;
        Functions["StreamItemType"] = &TypeArgWrapper<ETypeArgument::StreamItem>;
        Functions["TupleElementType"] = &TypeArgWrapper<ETypeArgument::TupleElement>;
        Functions["StructMemberType"] = &TypeArgWrapper<ETypeArgument::StructMember>;
        Functions["DictKeyType"] = &TypeArgWrapper<ETypeArgument::DictKey>;
        Functions["DictPayloadType"] = &TypeArgWrapper<ETypeArgument::DictPayload>;
        Functions["Apply"] = &ApplyWrapper;
        Functions["NamedApply"] = &NamedApplyWrapper;
        Functions["PositionalArgs"] = &PositionalArgsWrapper;
        Functions["SqlCall"] = &SqlCallWrapper;
        Functions["Callable"] = &CallableWrapper;
        Functions["CallableType"] = &TypeWrapper<ETypeAnnotationKind::Callable>;
        Functions["CallableResultType"] = &TypeArgWrapper<ETypeArgument::CallableResult>;
        Functions["CallableArgumentType"] = &TypeArgWrapper<ETypeArgument::CallableArgument>;
        Functions["CombineByKey"] = &CombineByKeyWrapper;
        Functions["NewMTRand"] = &NewMTRandWrapper;
        Functions["NextMTRand"] = &NextMTRandWrapper;
        Functions["FormatType"] = &FormatTypeWrapper;
        Functions["CastStruct"] = &CastStructWrapper;
        Functions["AggregationTraits"] = &AggregationTraitsWrapper;
        Functions["MultiAggregate"] = &MultiAggregateWrapper;
        Functions["Aggregate"] = &AggregateWrapper;
        Functions["SqlAggregateAll"] = &SqlAggregateAllWrapper;
        Functions["WinOnRows"] = &WinOnRowsWrapper;
        Functions["WinOnRange"] = &WinOnRangeWrapper;
        Functions["WindowTraits"] = &WindowTraitsWrapper;
        Functions["CalcOverWindow"] = &CalcOverWindowWrapper;
        Functions["CalcOverSessionWindow"] = &CalcOverWindowWrapper;
        Functions["CalcOverWindowGroup"] = &CalcOverWindowGroupWrapper;
        Functions["Lag"] = &WinLeadLagWrapper;
        Functions["Lead"] = &WinLeadLagWrapper;
        Functions["RowNumber"] = &WinRowNumberWrapper;
        Functions["Rank"] = &WinRankWrapper;
        Functions["DenseRank"] = &WinRankWrapper;
        Functions["Ascending"] = &PresortWrapper;
        Functions["Descending"] = &PresortWrapper;
        Functions["IsKeySwitch"] = &IsKeySwitchWrapper;
        Functions["TableName"] = &TableNameWrapper;
        Functions["FilterNullMembers"] = &FilterNullMembersWrapper;
        Functions["SkipNullMembers"] = &SkipNullMembersWrapper;
        Functions["FilterNullElements"] = &FilterNullElementsWrapper;
        Functions["SkipNullElements"] = &SkipNullElementsWrapper;
        Functions["AddMemberType"] = &TypeArgWrapper<ETypeArgument::AddMember>;
        Functions["RemoveMemberType"] = &TypeArgWrapper<ETypeArgument::RemoveMember>;
        Functions["ForceRemoveMemberType"] = &TypeArgWrapper<ETypeArgument::ForceRemoveMember>;
        Functions["FlattenMembersType"] = &TypeArgWrapper<ETypeArgument::FlattenMembers>;
        Functions["VariantUnderlyingType"] = &TypeArgWrapper<ETypeArgument::VariantUnderlying>;
        Functions["Guess"] = &GuessWrapper;
        Functions["VariantItem"] = &VariantItemWrapper;
        Functions["Visit"] = &VisitWrapper;
        Functions["Way"] = &WayWrapper;
        Functions["SqlAccess"] = &SqlAccessWrapper;
        Functions["SqlProcess"] = &SqlProcessWrapper;
        Functions["SqlReduce"] = &SqlReduceWrapper;
        Functions["SqlExternalFunction"] = &SqlExternalFunctionWrapper;
        Functions["SqlExtractKey"] = &SqlExtractKeyWrapper;
        Functions["SqlReduceUdf"] = &SqlReduceUdfWrapper;
        Functions["SqlProject"] = &SqlProjectWrapper;
        Functions["SqlTypeFromYson"] = &SqlTypeFromYsonWrapper;
        Functions["SqlColumnOrderFromYson"] = &SqlColumnOrderFromYsonWrapper;
        Functions["OrderedSqlProject"] = &SqlProjectWrapper;
        Functions["SqlProjectItem"] = &SqlProjectItemWrapper;
        Functions["SqlProjectStarItem"] = &SqlProjectItemWrapper;
        Functions["PgStar"] = &PgStarWrapper;
        Functions["PgCall"] = &PgCallWrapper;
        Functions["PgAgg"] = &PgAggWrapper;
        Functions["PgAggWindowCall"] = &PgAggWrapper;
        Functions["PgWindowCall"] = &PgWindowCallWrapper;
        Functions["PgQualifiedStar"] = &PgQualifiedStarWrapper;
        Functions["PgColumnRef"] = &PgColumnRefWrapper;
        Functions["PgResultItem"] = &PgResultItemWrapper;
        Functions["PgWhere"] = &PgWhereWrapper;
        Functions["PgSort"] = &PgSortWrapper;
        Functions["PgGroup"] = &PgWhereWrapper;
        Functions["PgWindow"] = &PgWindowWrapper;
        Functions["PgAnonWindow"] = &PgAnonWindowWrapper;
        Functions["AutoDemuxList"] = &AutoDemuxListWrapper;
        Functions["AggrCountInit"] = &AggrCountInitWrapper;
        Functions["AggrCountUpdate"] = &AggrCountUpdateWrapper;
        Functions["QueueCreate"] = &QueueCreateWrapper;
        Functions["QueuePop"] = &QueuePopWrapper;
        Functions["DependsOn"] = &DependsOnWrapper;
        Functions["Seq"] = &SeqWrapper;
        Functions["Parameter"] = &ParameterWrapper;
        Functions["WeakField"] = &WeakFieldWrapper;
        Functions["TryWeakMemberFromDict"] = &TryWeakMemberFromDictWrapper;
        Functions["ByteString"] = &ByteStringWrapper;
        Functions["Utf8String"] = &Utf8StringWrapper;
        Functions["FromYsonSimpleType"] = &FromYsonSimpleType;
        Functions["Mux"] = &MuxWrapper;
        Functions["Demux"] = &DemuxWrapper;
        Functions["TimezoneId"] = &TimezoneIdWrapper;
        Functions["TimezoneName"] = &TimezoneNameWrapper;
        Functions["AddTimezone"] = &AddTimezoneWrapper;
        Functions["RemoveTimezone"] = &RemoveTimezoneWrapper;
        Functions["TypeHandle"] = &TypeHandleWrapper;
        Functions["SerializeTypeHandle"] = &SerializeTypeHandleWrapper;
        Functions["ParseTypeHandle"] = &ParseTypeHandleWrapper;
        Functions["TypeKind"] = &TypeKindWrapper;
        Functions["DataTypeComponents"] = &SplitTypeHandleWrapper<ETypeAnnotationKind::Data>;
        Functions["DataTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Data>;
        Functions["OptionalTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Optional>;
        Functions["ListTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::List>;
        Functions["StreamTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Stream>;
        Functions["TupleTypeComponents"] = &SplitTypeHandleWrapper<ETypeAnnotationKind::Tuple>;
        Functions["TupleTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Tuple>;
        Functions["StructTypeComponents"] = &SplitTypeHandleWrapper<ETypeAnnotationKind::Struct>;
        Functions["StructTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Struct>;
        Functions["DictTypeComponents"] = &SplitTypeHandleWrapper<ETypeAnnotationKind::Dict>;
        Functions["DictTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Dict>;
        Functions["ResourceTypeTag"] = &SplitTypeHandleWrapper<ETypeAnnotationKind::Resource>;
        Functions["ResourceTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Resource>;
        Functions["TaggedTypeComponents"] = &SplitTypeHandleWrapper<ETypeAnnotationKind::Tagged>;
        Functions["TaggedTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Tagged>;
        Functions["VariantTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Variant>;
        Functions["VoidTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Void>;
        Functions["NullTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Null>;
        Functions["EmptyListTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::EmptyList>;
        Functions["EmptyDictTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::EmptyDict>;
        Functions["CallableTypeComponents"] = &SplitTypeHandleWrapper<ETypeAnnotationKind::Callable>;
        Functions["CallableArgument"] = &CallableArgumentWrapper;
        Functions["CallableTypeHandle"] = &MakeTypeHandleWrapper<ETypeAnnotationKind::Callable>;
        Functions["LambdaArgumentsCount"] = LambdaArgumentsCountWrapper;
        Functions["FormatCode"] = &FormatCodeWrapper;
        Functions["FormatCodeWithPositions"] = &FormatCodeWrapper;
        Functions["SerializeCode"] = &FormatCodeWrapper;
        Functions["WorldCode"] = &MakeCodeWrapper<TExprNode::World>;
        Functions["AtomCode"] = &MakeCodeWrapper<TExprNode::Atom>;
        Functions["ListCode"] = &MakeCodeWrapper<TExprNode::List>;
        Functions["FuncCode"] = &MakeCodeWrapper<TExprNode::Callable>;
        Functions["LambdaCode"] = &MakeCodeWrapper<TExprNode::Lambda>;
        Functions["ReprCode"] = &ReprCodeWrapper;
        Functions["EvaluateAtom"] = &RestartEvaluationWrapper;
        Functions["EvaluateExpr"] = &RestartEvaluationWrapper;
        Functions["EvaluateType"] = &RestartEvaluationWrapper;
        Functions["EvaluateCode"] = &RestartEvaluationWrapper;
        Functions["EvaluateExprIfPure"] = &EvaluateExprIfPureWrapper;
        Functions["ToFlow"] = &ToFlowWrapper;
        Functions["FromFlow"] = &FromFlowWrapper;
        Functions["BuildTablePath"] = &BuildTablePathWrapper;
        Functions["WithOptionalArgs"] = &WithOptionalArgsWrapper;

        Functions["DecimalDiv"] = &DecimalBinaryWrapper;
        Functions["DecimalMod"] = &DecimalBinaryWrapper;
        Functions["DecimalMul"] = &DecimalBinaryWrapper;

        Functions["ListFilter"] = &ListFilterWrapper;
        Functions["ListMap"] = &ListMapWrapper;
        Functions["ListFlatMap"] = &ListFlatMapWrapper;
        Functions["ListSkipWhile"] = &ListSkipWhileWrapper;
        Functions["ListTakeWhile"] = &ListTakeWhileWrapper;
        Functions["ListSkipWhileInclusive"] = &ListSkipWhileInclusiveWrapper;
        Functions["ListTakeWhileInclusive"] = &ListTakeWhileInclusiveWrapper;
        Functions["ListSkip"] = &ListSkipWrapper;
        Functions["ListTake"] = &ListTakeWrapper;
        Functions["ListHead"] = &ListHeadWrapper;
        Functions["ListLast"] = &ListLastWrapper;
        Functions["ListEnumerate"] = &ListEnumerateWrapper;
        Functions["ListReverse"] = &ListReverseWrapper;
        Functions["ListSort"] = &ListSortWrapper;
        Functions["ListExtract"] = &ListExtractWrapper;
        Functions["ListCollect"] = &ListCollectWrapper;
        Functions["ListMin"] = &ListMinWrapper;
        Functions["ListMax"] = &ListMaxWrapper;
        Functions["ListSum"] = &ListSumWrapper;
        Functions["ListConcat"] = &ListConcatWrapper;
        Functions["ListHas"] = &ContainsLookupWrapper<true, true>;
        Functions["ListAvg"] = &ListAvgWrapper;
        Functions["ListAll"] = &ListAllAnyWrapper<true>;
        Functions["ListAny"] = &ListAllAnyWrapper<false>;
        Functions["ListNotNull"] = &ListNotNullWrapper;
        Functions["ListFlatten"] = &ListFlattenWrapper;

        Functions["ExpandMap"] = &ExpandMapWrapper;
        Functions["WideMap"] = &WideMapWrapper;
        Functions["WideFilter"] = &WideFilterWrapper;
        Functions["WideTakeWhile"] = &WideWhileWrapper;
        Functions["WideSkipWhile"] = &WideWhileWrapper;
        Functions["WideTakeWhileInclusive"] = &WideWhileWrapper;
        Functions["WideSkipWhileInclusive"] = &WideWhileWrapper;
        Functions["WideCondense1"] = &WideCondense1Wrapper;
        Functions["WideCombiner"] = &WideCombinerWrapper;
        Functions["WideChopper"] = &WideChopperWrapper;
        Functions["WideChain1Map"] = &WideChain1MapWrapper;
        Functions["NarrowMap"] = &NarrowMapWrapper;
        Functions["NarrowFlatMap"] = &NarrowFlatMapWrapper;
        Functions["NarrowMultiMap"] = &NarrowMultiMapWrapper;

        Functions["AsRange"] = &AsRangeWrapper;
        Functions["RangeCreate"] = &RangeCreateWrapper;
        Functions["RangeEmpty"] = &RangeEmptyWrapper;
        Functions["RangeFor"] = &RangeForWrapper;
        Functions["RangeUnion"] = &RangeUnionWrapper;
        Functions["RangeIntersect"] = &RangeUnionWrapper;
        Functions["RangeMultiply"] = &RangeMultiplyWrapper;
        Functions["RangeFinalize"] = &RangeFinalizeWrapper;
        Functions["RangeComputeFor"] = &RangeComputeForWrapper;

        Functions["RoundUp"] = &RoundWrapper;
        Functions["RoundDown"] = &RoundWrapper;
        Functions["NextValue"] = &NextValueWrapper;

        ExtFunctions["PgSelect"] = &PgSelectWrapper;
        ExtFunctions["PgSetItem"] = &PgSetItemWrapper;
        ExtFunctions["TablePath"] = &TablePathWrapper;
        ExtFunctions["TableRecord"] = &TableRecordWrapper;
        ExtFunctions["Random"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::Double>;
        ExtFunctions["RandomNumber"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::Uint64>;
        ExtFunctions["RandomUuid"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::Uuid>;
        ExtFunctions["Now"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::Uint64>;
        ExtFunctions["CurrentUtcDate"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::Date>;
        ExtFunctions["CurrentUtcDatetime"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::Datetime>;
        ExtFunctions["CurrentUtcTimestamp"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::Timestamp>;
        ExtFunctions["CurrentTzDate"] = &CurrentTzWrapper<NKikimr::NUdf::EDataSlot::TzDate>;
        ExtFunctions["CurrentTzDatetime"] = &CurrentTzWrapper<NKikimr::NUdf::EDataSlot::TzDatetime>;
        ExtFunctions["CurrentTzTimestamp"] = &CurrentTzWrapper<NKikimr::NUdf::EDataSlot::TzTimestamp>;
        ExtFunctions["CurrentActorId"] = &DataGeneratorWrapper<NKikimr::NUdf::EDataSlot::String>;
        ExtFunctions["QueuePush"] = &QueuePushWrapper; ///< Ext for ParseTypeWrapper compatibility
        ExtFunctions["QueuePeek"] = &QueuePeekWrapper; ///< Ext for ParseTypeWrapper compatibility
        ExtFunctions["QueueRange"] = &QueueRangeWrapper; ///< Ext for ParseTypeWrapper compatibility
        ExtFunctions["PreserveStream"] = &PreserveStreamWrapper;
        ExtFunctions["FilePath"] = &FilePathWrapper;
        ExtFunctions["FileContent"] = &FileContentWrapper;
        ExtFunctions["FolderPath"] = &FolderPathWrapper;
        ExtFunctions["Files"] = &FilesWrapper;
        ExtFunctions["AuthTokens"] = &AuthTokensWrapper;
        ExtFunctions["Udf"] = &UdfWrapper;
        ExtFunctions["ScriptUdf"] = &ScriptUdfWrapper;
        ExtFunctions["ParseType"] = &ParseTypeWrapper;
        ExtFunctions["CurrentOperationId"] = &CurrentOperationIdWrapper;
        ExtFunctions["CurrentOperationSharedId"] = &CurrentOperationSharedIdWrapper;
        ExtFunctions["CurrentAuthenticatedUser"] = &CurrentAuthenticatedUserWrapper;
        ExtFunctions["SecureParam"] = &SecureParamWrapper;
        ExtFunctions["UnsafeTimestampCast"] = &UnsafeTimestampCastWrapper;
        ExtFunctions["JsonValue"] = &JsonValueWrapper;
        ExtFunctions["JsonExists"] = &JsonExistsWrapper;
        ExtFunctions["JsonQuery"] = &JsonQueryWrapper;
        ExtFunctions["JsonVariables"] = &JsonVariablesWrapper;
        ExtFunctions["AssumeColumnOrder"] = &AssumeColumnOrderWrapper;
        ExtFunctions["AssumeColumnOrderPartial"] = &AssumeColumnOrderWrapper;
        ExtFunctions["UnionAllPositional"] = &UnionAllPositionalWrapper;

        ColumnOrderFunctions["PgSetItem"] = &OrderForPgSetItem;
        ColumnOrderFunctions["AssumeColumnOrder"] = &OrderForAssumeColumnOrder;

        ColumnOrderFunctions["SqlProject"] = ColumnOrderFunctions["OrderedSqlProject"] = &OrderForSqlProject;
        ColumnOrderFunctions["SqlAggregateAll"] = &OrderFromFirst;

        ColumnOrderFunctions["Merge"] = ColumnOrderFunctions["Extend"] = &OrderForMergeExtend;
        ColumnOrderFunctions[RightName] = &OrderFromFirst;
        ColumnOrderFunctions["UnionAll"] = &OrderForUnionAll;
        ColumnOrderFunctions["EquiJoin"] = &OrderForEquiJoin;

        ColumnOrderFunctions["RemovePrefixMembers"] = &OrderFromFirstAndOutputType;
        ColumnOrderFunctions["Sort"] = ColumnOrderFunctions["Take"] = ColumnOrderFunctions["Skip"] =
            ColumnOrderFunctions["Filter"] = ColumnOrderFunctions["OrderedFilter"] = &OrderFromFirst;
        ColumnOrderFunctions["AssumeSorted"] = ColumnOrderFunctions["Unordered"] =
            ColumnOrderFunctions["UnorderedSubquery"] = ColumnOrderFunctions["AssumeUniq"] = &OrderFromFirst;

        for (ui32 i = 0; i < NKikimr::NUdf::DataSlotCount; ++i) {
            auto name = TString(NKikimr::NUdf::GetDataTypeInfo((EDataSlot)i).Name);
            Functions[name] = &DataConstructorWrapper;
        }

        for (ui32 k = (ui32)ETypeAnnotationKind::Unit; k < (ui32)ETypeAnnotationKind::LastType; ++k) {
            const ETypeAnnotationKind kind = (ETypeAnnotationKind)k;
            TypeKinds.insert(std::pair<TString, ETypeAnnotationKind>(TStringBuilder() << kind, kind));
        }

        for (auto& func : Functions) {
            AllNames.insert(func.first);
        }

        for (auto& func : ExtFunctions) {
            AllNames.insert(func.first);
        }

        AllNames.insert(TString(CommitName)); 
        AllNames.insert(TString(ReadName)); 
        AllNames.insert(TString(WriteName)); 
        AllNames.insert(TString(ConfigureName)); 
        AllNames.insert("Apply");
    }

    class TIntentDeterminationTransformer : public TSyncTransformerBase {
    public:
        TIntentDeterminationTransformer(const TTypeAnnotationContext& types)
            : Types(types)
        {}

        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;
            if (ctx.Step.IsDone(TExprStep::Intents)) {
                return TStatus::Ok;
            }

            TOptimizeExprSettings settings(nullptr);
            auto ret = OptimizeExpr(input, output, [this](const TExprNode::TPtr& input, TExprContext& ctx) {
                TStatus status = TStatus::Ok;
                auto output = input;

                bool foundFunc = false;

                for (auto& datasource : Types.DataSources) {
                    if (!datasource->CanParse(*input)) {
                        continue;
                    }
                    foundFunc = true;
                    status = DetermineIntents(*datasource, input, output, ctx);
                    break;
                }

                if (!foundFunc) {
                    for (auto& datasink : Types.DataSinks) {
                        if (!datasink->CanParse(*input)) {
                            continue;
                        }
                        status = DetermineIntents(*datasink, input, output, ctx);
                        break;
                    }
                }

                if (status == TStatus::Error) {
                    output = nullptr;
                }
                return output;
            }, ctx, settings);

            if (ret.Level == TStatus::Ok) {
                ctx.Step.Done(TExprStep::Intents);
            }

            return ret;
        }

    private:
        IGraphTransformer::TStatus DetermineIntents(IDataProvider& dataProvider,
            const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return dataProvider.GetIntentDeterminationTransformer().Transform(input, output, ctx);
        }

    private:
        const TTypeAnnotationContext& Types;
    };

    class TExtCallableTypeAnnotationTransformer : public TCallableTransformerBase<TExtCallableTypeAnnotationTransformer> {
    public:
        TExtCallableTypeAnnotationTransformer(TTypeAnnotationContext& types, bool instantOnly)
            : TCallableTransformerBase<TExtCallableTypeAnnotationTransformer>(types, instantOnly)
        {}

        TMaybe<IGraphTransformer::TStatus> ProcessCore(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            auto& functions = TSyncFunctionsMap::Instance().Functions;
            auto& extFunctions = TSyncFunctionsMap::Instance().ExtFunctions;
            auto& columnOrderFunctions = TSyncFunctionsMap::Instance().ColumnOrderFunctions;
            auto name = input->Content();
            IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
            if (auto func = functions.FindPtr(name)) {
                TContext funcCtx(ctx);
                status = (*func)(input, output, funcCtx);
            } else if (auto func = extFunctions.FindPtr(name)) {
                TExtContext funcCtx(ctx, Types);
                status = (*func)(input, output, funcCtx);
            } else {
                return Nothing();
            }

            if (status == IGraphTransformer::TStatus::Ok && Types.OrderedColumns && !Types.LookupColumnOrder(*input)) {
                if (auto func = columnOrderFunctions.FindPtr(name)) {
                    TExtContext funcCtx(ctx, Types);
                    status = (*func)(input, output, funcCtx);
                }
            }

            return status;
        }

        TMaybe<IGraphTransformer::TStatus> ProcessList(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&) {
            return {};
        }

        IGraphTransformer::TStatus ProcessUnknown(const TExprNode::TPtr& input, TExprContext& ctx) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << "(Core type annotation) Unsupported function: " << input->Content()));
            return TStatus::Error;
        }

        IGraphTransformer::TStatus ValidateProviderCommitResult(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!input->GetTypeAnn() || input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::World) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Bad datasink commit result"));
                return TStatus::Error;
            }
            return TStatus::Ok;
        }

        IGraphTransformer::TStatus ValidateProviderReadResult(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!input->GetTypeAnn() ||
                input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple ||
                input->GetTypeAnn()->Cast<TTupleExprType>()->GetSize() != 2 ||
                input->GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[0]->GetKind() != ETypeAnnotationKind::World) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Bad datasource read result"));
                return TStatus::Error;
            }
            return TStatus::Ok;
        }

        IGraphTransformer::TStatus ValidateProviderWriteResult(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!input->GetTypeAnn() || input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::World) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Bad datasink write result"));
                return TStatus::Error;
            }
            return TStatus::Ok;
        }

        IGraphTransformer::TStatus ValidateProviderConfigureResult(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!input->GetTypeAnn() || input->GetTypeAnn()->GetKind() != input->Head().GetTypeAnn()->GetKind()) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Bad provider configure result"));
                return TStatus::Error;
            }
            return TStatus::Ok;
        }

        IGraphTransformer& GetTransformer(IDataProvider& provider) const {
            return provider.GetTypeAnnotationTransformer(InstantOnly);
        }
    };
} // namespace NTypeAnnInpl

TAutoPtr<IGraphTransformer> CreateIntentDeterminationTransformer(const TTypeAnnotationContext& types) {
    return new NTypeAnnImpl::TIntentDeterminationTransformer(types);
}

TAutoPtr<IGraphTransformer> CreateExtCallableTypeAnnotationTransformer(TTypeAnnotationContext& types, bool instantOnly) {
    return new NTypeAnnImpl::TExtCallableTypeAnnotationTransformer(types, instantOnly);
}

const THashSet<TString>& GetBuiltinFunctions() {
    return NTypeAnnImpl::TSyncFunctionsMap::Instance().AllNames;
}

IGraphTransformer::TStatus ValidateDataSource(const TExprNode::TPtr& input, TExprContext& ctx, const TTypeAnnotationContext& types,
    TSet<std::pair<TString, TString>>& clusters) {
    if (!EnsureMinArgsCount(*input, 1, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto datasource = types.DataSourceMap.FindPtr(input->Head().Content());
    if (!datasource) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unsupported datasource: " << input->Head().Content()));
        return IGraphTransformer::TStatus::Error;
    }

    TMaybe<TString> cluster;
    if (!(*datasource)->ValidateParameters(*input, ctx, cluster)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (cluster) {
        clusters.insert(std::make_pair(TString(input->Head().Content()), *cluster));
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus ValidateDataSink(const TExprNode::TPtr& input, TExprContext& ctx, const TTypeAnnotationContext& types,
    TSet<std::pair<TString, TString>>& clusters) {
    if (!EnsureMinArgsCount(*input, 1, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto datasink = types.DataSinkMap.FindPtr(input->Head().Content());
    if (!datasink) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unsupported datasink: " << input->Head().Content()));
        return IGraphTransformer::TStatus::Error;
    }

    TMaybe<TString> cluster;
    if (!(*datasink)->ValidateParameters(*input, ctx, cluster)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (cluster) {
        clusters.insert(std::make_pair(TString(input->Head().Content()), *cluster));
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus ValidateProviders(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx, const TTypeAnnotationContext& types) {
    output = input;
    if (ctx.Step.IsDone(TExprStep::ValidateProviders)) {
        return IGraphTransformer::TStatus::Ok;
    }

    TSet<std::pair<TString, TString>> clusters;
    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    auto status = OptimizeExpr(output, output, [&](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
        if (input->Content() == "DataSource") {
            if (ValidateDataSource(input, ctx, types, clusters).Level == IGraphTransformer::TStatus::Error) {
                return nullptr;
            }
        }
        else if (input->Content() == "DataSink") {
            if (ValidateDataSink(input, ctx, types, clusters).Level == IGraphTransformer::TStatus::Error) {
                return nullptr;
            }
        }

        return input;
    }, ctx, settings);

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (ctx.Step.IsDone(TExprStep::ExprEval)) {
        status = OptimizeExpr(output, output, [&](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
            if (input->Content() == "CommitAll!") {
                if (!EnsureMinArgsCount(*input, 1, ctx)) {
                    return nullptr;
                }

                auto ret = input->HeadPtr();
                for (const auto& x : clusters) {
                    if (x.second == "$all") {
                        continue;
                    }

                    auto children = input->ChildrenList();
                    children[0] = ret;
                    auto sink = ctx.Builder(input->Pos())
                        .Callable("DataSink")
                            .Atom(0, x.first)
                            .Atom(1, x.second)
                        .Seal()
                        .Build();

                    children.insert(children.begin() + 1, sink);
                    ret = ctx.NewCallable(input->Pos(), CommitName, std::move(children));
                }

                return ret;
            }

            return input;
        }, ctx, settings);
    }

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    ctx.Step.Done(TExprStep::ValidateProviders);
    return status;
}

}
