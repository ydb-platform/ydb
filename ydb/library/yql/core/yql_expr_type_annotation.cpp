#include "yql_expr_type_annotation.h"
#include "ydb/library/yql/core/type_ann/type_ann_pg.h"
#include "yql_opt_proposed_by_data.h"
#include "yql_opt_rewrite_io.h"
#include "yql_opt_utils.h"
#include "yql_expr_optimize.h"
#include "yql_type_helpers.h"

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/minikql/jsonpath/jsonpath.h>
#include <ydb/library/yql/core/sql_types/simple_types.h>
#include "ydb/library/yql/parser/pg_catalog/catalog.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/utf8.h>

#include <util/generic/hash_set.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/ascii.h>

#include <unordered_set>

namespace NYql {

using namespace NNodes;
using namespace NKikimr;

namespace {

constexpr TStringBuf TypeResourceTag = "_Type";
constexpr TStringBuf CodeResourceTag = "_Expr";

TExprNode::TPtr RebuildDict(const TExprNode::TPtr& node, const TExprNode::TPtr& lambda, TExprContext& ctx) {
    auto ret = ctx.Builder(node->Pos())
        .Callable("ToDict")
            .Callable(0, "OrderedMap")
                .Callable(0, "DictItems")
                    .Add(0, node)
                .Seal()
                .Add(1, lambda)
            .Seal()
            .Lambda(1) // keyExtractor
                .Param("item")
                .Callable("Nth")
                    .Arg(0, "item")
                    .Atom(1, "0", TNodeFlags::Default)
                .Seal()
            .Seal()
            .Lambda(2) // payloadExtractor
                .Param("item")
                .Callable("Nth")
                    .Arg(0, "item")
                    .Atom(1, "1", TNodeFlags::Default)
                .Seal()
            .Seal()
            .List(3)
                .Atom(0, "Hashed", TNodeFlags::Default)
                .Atom(1, "One", TNodeFlags::Default)
            .Seal()
        .Seal()
        .Build();

    return ret;
}

TExprNode::TPtr RebuildVariant(const TExprNode::TPtr& node,
    const THashMap<TString, TExprNode::TPtr>& transforms, TExprContext& ctx) {
    auto ret = ctx.Builder(node->Pos())
        .Callable("Visit")
            .Add(0, node)
            .Do([&transforms](TExprNodeBuilder& builder) -> TExprNodeBuilder& {
                ui32 i = 1;
                for (const auto&[name, transform]: transforms) {
                    builder.Atom(i, name).Add(i + 1, transform);
                    i += 2;
                }
                return builder;
            })
        .Seal()
        .Build();

    return ret;
}

IGraphTransformer::TStatus TryConvertToImpl(TExprContext& ctx, TExprNode::TPtr& node,
    const TTypeAnnotationNode& sourceType, const TTypeAnnotationNode& expectedType, TConvertFlags flags, bool raiseIssues = false) {

    if (IsSameAnnotation(sourceType, expectedType)) {
        return IGraphTransformer::TStatus::Ok;
    }

    if (expectedType.GetKind() == ETypeAnnotationKind::Stream) {
        switch (sourceType.GetKind()) {
            case ETypeAnnotationKind::List:
            case ETypeAnnotationKind::Flow:
            if (const auto itemType = expectedType.Cast<TStreamExprType>()->GetItemType(); IsSameAnnotation(*itemType, *GetSeqItemType(&sourceType))) {
                auto pos = node->Pos();
                node = ctx.NewCallable(pos, "ToStream", {std::move(node)});
                return IGraphTransformer::TStatus::Repeat;
            }
            break;
            default: break;
        }
    }

    if (expectedType.GetKind() == ETypeAnnotationKind::Pg) {
        if (IsNull(sourceType)) {
            node = ctx.NewCallable(node->Pos(), "Nothing", { ExpandType(node->Pos(), expectedType, ctx) });
            return IGraphTransformer::TStatus::Repeat;
        }

        if (sourceType.GetKind() == ETypeAnnotationKind::Pg) {
            const auto fromTypeId = sourceType.Cast<TPgExprType>()->GetId();
            const auto toTypeId = expectedType.Cast<TPgExprType>()->GetId();

            // https://www.postgresql.org/docs/14/typeconv-query.html, step 2.
            if (fromTypeId == NPg::UnknownOid || NPg::IsCoercible(fromTypeId, toTypeId, NPg::ECoercionCode::Assignment)) {
                auto pos = node->Pos();
                node = ctx.NewCallable(pos, "PgCast", { std::move(node), ExpandType(pos, expectedType, ctx) });
                return IGraphTransformer::TStatus::Repeat;
            }
        }
    }

    if (expectedType.GetKind() == ETypeAnnotationKind::Optional) {
        auto nextType = expectedType.Cast<TOptionalExprType>()->GetItemType();
        auto originalNode = node;
        auto status1 = TryConvertToImpl(ctx, node, sourceType, *nextType, flags, raiseIssues);
        if (status1.Level != IGraphTransformer::TStatus::Error) {
            node = ctx.NewCallable(node->Pos(), "Just", { node });
            return IGraphTransformer::TStatus::Repeat;
        }

        node = originalNode;
        if (node->IsCallable("Just")) {
            auto sourceItemType = sourceType.Cast<TOptionalExprType>()->GetItemType();
            auto value = node->HeadRef();
            auto status = TryConvertToImpl(ctx, value, *sourceItemType, *nextType, flags, raiseIssues);
            if (status.Level != IGraphTransformer::TStatus::Error) {
                node = ctx.NewCallable(node->Pos(), "Just", { value });
                return IGraphTransformer::TStatus::Repeat;
            }
        } else if (sourceType.GetKind() == ETypeAnnotationKind::Optional) {
            auto sourceItemType = sourceType.Cast<TOptionalExprType>()->GetItemType();
            auto arg = ctx.NewArgument(node->Pos(), "item");
            auto originalArg = arg;
            auto status = TryConvertToImpl(ctx, arg, *sourceItemType, *nextType, flags, raiseIssues);
            if (status.Level != IGraphTransformer::TStatus::Error) {
                auto lambda = ctx.NewLambda(node->Pos(),
                    ctx.NewArguments(node->Pos(), { originalArg }),
                    std::move(arg));

                node = ctx.NewCallable(node->Pos(), "Map", { node, std::move(lambda) });
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        if (IsNull(sourceType)) {
            node = ctx.NewCallable(node->Pos(), "Nothing", { ExpandType(node->Pos(), expectedType, ctx) });
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Data && sourceType.GetKind() == ETypeAnnotationKind::Resource) {
        const auto to = expectedType.Cast<TDataExprType>()->GetSlot();
        const auto fromTag = sourceType.Cast<TResourceExprType>()->GetTag();
        if ((to == EDataSlot::Yson || to == EDataSlot::Json) && fromTag == "Yson2.Node") {
            node = ctx.Builder(node->Pos())
                .Callable("Apply")
                    .Callable(0, "Udf")
                        .Atom(0, to == EDataSlot::Yson ? "Yson2.Serialize" : "Yson2.SerializeJson", TNodeFlags::Default)
                    .Seal()
                    .Add(1, std::move(node))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        }
    } else if (expectedType.GetKind() == ETypeAnnotationKind::Resource && sourceType.GetKind() == ETypeAnnotationKind::Data) {
        const auto fromSlot = sourceType.Cast<TDataExprType>()->GetSlot();
        const auto to = expectedType.Cast<TResourceExprType>()->GetTag();
        if ((fromSlot == EDataSlot::Yson || fromSlot == EDataSlot::Json) && to == "Yson2.Node") {
            node = ctx.Builder(node->Pos())
                .Callable("Apply")
                    .Callable(0, "Udf")
                        .Atom(0, fromSlot == EDataSlot::Yson ? "Yson2.Parse" : "Yson2.ParseJson", TNodeFlags::Default)
                        .Callable(1, "Void")
                        .Seal()
                        .Callable(2, "TupleType")
                            .Callable(0, "TupleType")
                                .Callable(0, "DataType")
                                    .Atom(0, sourceType.Cast<TDataExprType>()->GetName(), TNodeFlags::Default)
                                .Seal()
                            .Seal()
                            .Callable(1, "StructType")
                            .Seal()
                            .Callable(2, "TupleType")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Add(1, std::move(node))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        } else if ((fromSlot == EDataSlot::Yson || fromSlot == EDataSlot::Json) && to == "Yson.Node") {
            node = ctx.Builder(node->Pos())
                .Callable("Apply")
                    .Callable(0, "Udf")
                        .Atom(0, fromSlot == EDataSlot::Yson ? "Yson.Parse" : "Yson.ParseJson", TNodeFlags::Default)
                        .Callable(1, "Void")
                        .Seal()
                        .Callable(2, "TupleType")
                            .Callable(0, "TupleType")
                                .Callable(0, "DataType")
                                    .Atom(0, sourceType.Cast<TDataExprType>()->GetName(), TNodeFlags::Default)
                                .Seal()
                            .Seal()
                            .Callable(1, "StructType")
                            .Seal()
                            .Callable(2, "TupleType")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Add(1, std::move(node))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;

        } else if ((GetDataTypeInfo(fromSlot).Features & (NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TzDateType)) && to == "DateTime2.TM") {
            node = ctx.Builder(node->Pos())
                .Callable("Apply")
                    .Callable(0, "Udf")
                        .Atom(0, "DateTime2.Split", TNodeFlags::Default)
                        .Callable(1, "Void")
                        .Seal()
                        .Callable(2, "TupleType")
                            .Callable(0, "TupleType")
                                .Callable(0, "DataType")
                                    .Atom(0, sourceType.Cast<TDataExprType>()->GetName(), TNodeFlags::Default)
                                .Seal()
                            .Seal()
                            .Callable(1, "StructType")
                            .Seal()
                            .Callable(2, "TupleType")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Add(1, std::move(node))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        } else if (fromSlot == EDataSlot::Json && to == "JsonNode") {
            node = ctx.Builder(node->Pos())
                .Callable("Apply")
                    .Callable(0, "Udf")
                        .Atom(0, "Json2.Parse", TNodeFlags::Default)
                        .Callable(1, "Void")
                        .Seal()
                        .Callable(2, "TupleType")
                            .Callable(0, "TupleType")
                                .Callable(0, "DataType")
                                    .Atom(0, sourceType.Cast<TDataExprType>()->GetName(), TNodeFlags::Default)
                                .Seal()
                            .Seal()
                            .Callable(1, "StructType")
                            .Seal()
                            .Callable(2, "TupleType")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Add(1, std::move(node))
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        }
    } else if (expectedType.GetKind() == ETypeAnnotationKind::Data && sourceType.GetKind() == ETypeAnnotationKind::Data) {
        const auto from = sourceType.Cast<TDataExprType>()->GetSlot();
        const auto to = expectedType.Cast<TDataExprType>()->GetSlot();
        if (from == EDataSlot::Utf8 && to == EDataSlot::String) {
            auto pos = node->Pos();
            node = ctx.NewCallable(pos, "ToString", { std::move(node) });
            return IGraphTransformer::TStatus::Repeat;
        }

        if (node->IsCallable("String") && to == EDataSlot::Utf8) {
            if (const  auto atom = node->Head().Content(); IsUtf8(atom)) {
                node = ctx.RenameNode(*node, "Utf8");
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        if (node->IsCallable({"String", "Utf8"}) && to == EDataSlot::Yson) {
            if (const auto atom = node->Head().Content(); NDom::IsValidYson(atom)) {
                node = ctx.RenameNode(*node, "Yson");
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        if (node->IsCallable({"String", "Utf8"}) && to == EDataSlot::Json) {
            if (const auto atom = node->Head().Content(); NDom::IsValidJson(atom)) {
                node = ctx.RenameNode(*node, "Json");
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        bool allow = false;
        bool isSafe = true;
        bool useCast = false;
        if (IsDataTypeNumeric(from) && IsDataTypeNumeric(to)) {
            allow = GetNumericDataTypeLevel(to) >= GetNumericDataTypeLevel(from);
            isSafe = false;
        } else if (from == EDataSlot::Date && (
                    to == EDataSlot::Date32 ||
                    to == EDataSlot::TzDate ||
                    to == EDataSlot::Datetime ||
                    to == EDataSlot::Timestamp ||
                    to == EDataSlot::TzDatetime ||
                    to == EDataSlot::TzTimestamp ||
                    to == EDataSlot::Datetime64 ||
                    to == EDataSlot::Timestamp64))
        {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::Datetime && (
                    to == EDataSlot::Datetime64 ||
                    to == EDataSlot::TzDatetime ||
                    to == EDataSlot::Timestamp ||
                    to == EDataSlot::TzTimestamp ||
                    to == EDataSlot::Timestamp64))
        {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::TzDate && (to == EDataSlot::TzDatetime || to == EDataSlot::TzTimestamp)) {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::TzDatetime && to == EDataSlot::TzTimestamp) {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::Timestamp && (to == EDataSlot::TzTimestamp || to == EDataSlot::Timestamp64)) {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::Date32 && (to == EDataSlot::Datetime64 || to == EDataSlot::Timestamp64)) {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::Datetime64 && (to == EDataSlot::Timestamp64)) {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::Interval && (to == EDataSlot::Interval64)) {
            allow = true;
            useCast = true;
        } else if (from == EDataSlot::Json && to == EDataSlot::Utf8) {
            allow = true;
            useCast = true;
        } else if ((from == EDataSlot::Yson || from == EDataSlot::Json) && to == EDataSlot::String) {
            node =  ctx.Builder(node->Pos())
                .Callable("ToBytes")
                    .Add(0, node)
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        } else if (IsDataTypeIntegral(from) && to == EDataSlot::Timestamp) {
            node =  ctx.Builder(node->Pos())
                .Callable("UnsafeTimestampCast")
                    .Callable(0, "BitCast")
                        .Add(0, node)
                        .Atom(1, "Uint64")
                    .Seal()
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
        }

        if (!allow || !isSafe) {
            auto current = node;
            bool negate = false;
            for (;;) {
                if (current->IsCallable("Plus")) {
                    current = current->HeadPtr();
                }
                else if (current->IsCallable("Minus")) {
                    current = current->HeadPtr();
                    negate = !negate;
                }
                else {
                    break;
                }
            }

            if (const auto maybeInt = TMaybeNode<TCoIntegralCtor>(current)) {
                auto allowIntergral = AllowIntegralConversion(maybeInt.Cast(), negate, to);
                allow = allow || allowIntergral;
                isSafe = allowIntergral;
            }
        }

        if (allow) {
            if (!isSafe && !flags.Test(NConvertFlags::AllowUnsafeConvert)) {
                auto castResult = NKikimr::NUdf::GetCastResult(from, to);
                if (*castResult & NKikimr::NUdf::Impossible) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (*castResult != NKikimr::NUdf::ECastOptions::Complete
                    && !(IsDataTypeIntegral(from) && IsDataTypeFloat(to))) {
                    auto issue = TIssue(node->Pos(ctx), TStringBuilder() <<
                        "Consider using explicit CAST or BITCAST to convert from " <<
                        NKikimr::NUdf::GetDataTypeInfo(from).Name << " to " << NKikimr::NUdf::GetDataTypeInfo(to).Name);
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_IMPLICIT_BITCAST, issue);
                    if (!ctx.AddWarning(issue)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }

            const auto pos = node->Pos();
            auto type = ExpandType(pos, expectedType, ctx);
            node = ctx.NewCallable(pos, useCast ? "SafeCast" : "Convert", {std::move(node), std::move(type)});
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Struct && sourceType.GetKind() == ETypeAnnotationKind::Struct) {
        auto from = sourceType.Cast<TStructExprType>();
        auto to = expectedType.Cast<TStructExprType>();
        const bool literalStruct = node->IsCallable({"Struct", "AsStruct"});
        THashMap<TString, TExprNode::TPtr> columnTransforms;
        ui32 usedFields = 0;
        for (auto newField : to->GetItems()) {
            auto pos = from->FindItem(newField->GetName());
            TExprNode::TPtr field;
            if (!pos) {
                switch (newField->GetItemType()->GetKind()) {
                case ETypeAnnotationKind::Null:
                    field = ctx.Builder(node->Pos())
                        .Callable(ToString(newField->GetItemType()->GetKind())).Seal()
                        .Build();
                    break;
                case ETypeAnnotationKind::Optional:
                case ETypeAnnotationKind::Pg:
                    field = ctx.Builder(node->Pos())
                        .Callable(GetEmptyCollectionName(newField->GetItemType()->GetKind()))
                            .Add(0, ExpandType(node->Pos(), *newField->GetItemType(), ctx))
                        .Seal()
                        .Build();
                    break;
                default:
                    if (raiseIssues) {
                        ctx.AddError(TIssue(node->Pos(ctx), TStringBuilder() <<
                                "Can't find  '" << newField->GetName() << ": " << *newField->GetItemType() << "' in " << sourceType));
                    }
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                ++usedFields;
                auto oldType = from->GetItems()[*pos];
                if (literalStruct) {
                    for (ui32 i = node->IsCallable("Struct") ? 1 : 0; i < node->ChildrenSize(); ++i) {
                        if (node->Child(i)->Head().Content() == newField->GetName()) {
                            field = node->Child(i)->ChildPtr(1);
                            break;
                        }
                    }
                    YQL_ENSURE(field);
                } else {
                    field = ctx.Builder(node->Pos())
                        .Callable("Member")
                            .Add(0, node)
                            .Atom(1, newField->GetName())
                            .Seal()
                        .Build();
                }

                auto status = TryConvertToImpl(ctx, field, *oldType->GetItemType(), *newField->GetItemType(), flags, raiseIssues);
                if (status.Level == IGraphTransformer::TStatus::Error) {
                    if (raiseIssues) {
                        ctx.AddError(TIssue(node->Pos(ctx), TStringBuilder() <<
                                "Failed to convert '" << newField->GetName() << "': " << *oldType->GetItemType() << " to " << *newField->GetItemType()));
                    }
                    return status;
                }
            }

            columnTransforms[newField->GetName()] = field;
        }

        if (flags.Test(NConvertFlags::DisableTruncation) && usedFields != from->GetSize()) {
            return IGraphTransformer::TStatus::Error;
        }

        TExprNode::TListType nodeChildren;
        for (auto& child : columnTransforms) {
            nodeChildren.push_back(ctx.NewList(node->Pos(), {
                ctx.NewAtom(node->Pos(), child.first), child.second }));
        }

        node = ctx.NewCallable(node->Pos(), "AsStruct", std::move(nodeChildren));
        return IGraphTransformer::TStatus::Repeat;
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Variant && node->IsCallable("Variant")) {
        auto from = sourceType.Cast<TVariantExprType>();
        auto to = expectedType.Cast<TVariantExprType>();

        if (from->GetUnderlyingType()->GetKind() != to->GetUnderlyingType()->GetKind()) {
            return IGraphTransformer::TStatus::Error;
        }

        switch (from->GetUnderlyingType()->GetKind()) {
            case ETypeAnnotationKind::Struct:
            {
                auto fromUnderlying = from->GetUnderlyingType()->Cast<TStructExprType>();
                auto toUnderlying = to->GetUnderlyingType()->Cast<TStructExprType>();

                if (fromUnderlying->GetSize() > toUnderlying->GetSize()) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto fromTargetIndex = fromUnderlying->FindItem(node->Child(1)->Content());
                if (fromTargetIndex.Empty()) {
                    return IGraphTransformer::TStatus::Error;
                }
                auto toTargetIndex = toUnderlying->FindItem(node->Child(1)->Content());
                if (toTargetIndex.Empty()) {
                    return IGraphTransformer::TStatus::Error;
                }
                auto targetItem = node->HeadPtr();
                auto status = TryConvertToImpl(
                    ctx, targetItem,
                    *fromUnderlying->GetItems()[*fromTargetIndex]->GetItemType(),
                    *toUnderlying->GetItems()[*toTargetIndex]->GetItemType(), flags, raiseIssues);
                if (status.Level == IGraphTransformer::TStatus::Error) {
                    return status;
                }

                for (auto item: fromUnderlying->GetItems()) {
                    if (item->GetName() == node->Child(1)->Content()) {
                        continue;  // Already checked when converting targetItem
                    }

                    auto toIndex = toUnderlying->FindItem(item->GetName());
                    if (toIndex.Empty()) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    auto fromElement = item->GetItemType();
                    auto toElement = toUnderlying->GetItems()[*toIndex]->GetItemType();
                    auto arg = ctx.NewArgument(TPositionHandle(), "arg");
                    auto status1 = TryConvertToImpl(ctx, arg, *fromElement, *toElement, flags, raiseIssues);
                    if (status1.Level == IGraphTransformer::TStatus::Error) {
                        return status1;
                    }
                }

                node = ctx.Builder(node->Pos())
                    .Callable("Variant")
                        .Add(0, std::move(targetItem))
                        .Add(1, node->ChildPtr(1))
                        .Add(2, ExpandType(node->Pos(), *to, ctx))
                    .Seal()
                    .Build();

                break;
            }
            case ETypeAnnotationKind::Tuple:
            {
                auto fromUnderlying = from->GetUnderlyingType()->Cast<TTupleExprType>();
                auto toUnderlying = to->GetUnderlyingType()->Cast<TTupleExprType>();

                if (fromUnderlying->GetSize() != toUnderlying->GetSize()) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto targetIndex = FromString<size_t>(node->Child(1)->Content());
                auto targetItem = node->HeadPtr();
                auto status = TryConvertToImpl(
                    ctx, targetItem,
                    *fromUnderlying->GetItems()[targetIndex],
                    *toUnderlying->GetItems()[targetIndex], flags, raiseIssues);
                if (status.Level == IGraphTransformer::TStatus::Error) {
                    return status;
                }

                for (size_t i = 0; i < fromUnderlying->GetSize(); i++) {
                    if (i == targetIndex) {
                        continue;  // Already checked when converting targetItem
                    }
                    auto arg = ctx.NewArgument(TPositionHandle(), "arg");
                    auto status1 = TryConvertToImpl(ctx, arg, *fromUnderlying->GetItems()[i],
                        *toUnderlying->GetItems()[i], flags, raiseIssues);
                    if (status1.Level == IGraphTransformer::TStatus::Error) {
                        return status1;
                    }
                }

                node = ctx.Builder(node->Pos())
                    .Callable("Variant")
                        .Add(0, std::move(targetItem))
                        .Add(1, node->ChildPtr(1))
                        .Add(2, ExpandType(node->Pos(), *to, ctx))
                    .Seal()
                    .Build();

                break;
            }
            default:
                Y_UNREACHABLE();
        }

        return IGraphTransformer::TStatus::Repeat;
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Variant && sourceType.GetKind() == ETypeAnnotationKind::Variant) {
        auto from = sourceType.Cast<TVariantExprType>();
        auto to = expectedType.Cast<TVariantExprType>();

        if (from->GetUnderlyingType()->GetKind() != to->GetUnderlyingType()->GetKind()) {
            return IGraphTransformer::TStatus::Error;
        }

        auto outTypeExpr = ExpandType(node->Pos(), *to, ctx);

        THashMap<TString, TExprNode::TPtr> transforms;

        switch (from->GetUnderlyingType()->GetKind()) {
            case ETypeAnnotationKind::Struct:
            {
                auto fromUnderlying = from->GetUnderlyingType()->Cast<TStructExprType>();
                auto toUnderlying = to->GetUnderlyingType()->Cast<TStructExprType>();

                if (fromUnderlying->GetSize() > toUnderlying->GetSize()) {
                    return IGraphTransformer::TStatus::Error;
                }

                for (auto item: fromUnderlying->GetItems()) {
                    auto toIndex = toUnderlying->FindItem(item->GetName());
                    if (toIndex.Empty()) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    auto fromElement = item->GetItemType();
                    auto toElement = toUnderlying->GetItems()[*toIndex]->GetItemType();

                    auto arg = ctx.NewArgument(node->Pos(), "item");
                    auto originalArg = arg;

                    auto status = TryConvertToImpl(ctx, arg, *fromElement, *toElement, flags, raiseIssues);
                    if (status.Level == IGraphTransformer::TStatus::Error) {
                        return status;
                    }

                    arg = ctx.Builder(node->Pos())
                        .Callable("Variant")
                            .Add(0, std::move(arg))
                            .Atom(1, item->GetName())
                            .Add(2, outTypeExpr)
                        .Seal()
                        .Build();

                    auto lambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), {originalArg}), std::move(arg));

                    transforms.emplace(item->GetName(), std::move(lambda));
                }
                break;
            }
            case ETypeAnnotationKind::Tuple:
            {
                auto fromUnderlying = from->GetUnderlyingType()->Cast<TTupleExprType>();
                auto toUnderlying = to->GetUnderlyingType()->Cast<TTupleExprType>();

                if (fromUnderlying->GetSize() != toUnderlying->GetSize()) {
                    return IGraphTransformer::TStatus::Error;
                }

                for (size_t i = 0; i < fromUnderlying->GetSize(); i++) {
                    auto fromElement = fromUnderlying->GetItems()[i];
                    auto toElement = toUnderlying->GetItems()[i];

                    auto arg = ctx.NewArgument(node->Pos(), "item");
                    auto originalArg = arg;

                    auto status = TryConvertToImpl(ctx, arg, *fromElement, *toElement, flags, raiseIssues);
                    if (status.Level == IGraphTransformer::TStatus::Error) {
                        return status;
                    }

                    arg = ctx.Builder(node->Pos())
                        .Callable("Variant")
                            .Add(0, std::move(arg))
                            .Atom(1, ToString(i), TNodeFlags::Default)
                            .Add(2, outTypeExpr)
                        .Seal()
                        .Build();

                    auto lambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), {originalArg}), std::move(arg));

                    transforms.emplace(ToString(i), std::move(lambda));
                }
                break;
            }
            default:
                Y_UNREACHABLE();
        }

        node = RebuildVariant(node, transforms, ctx);
        return IGraphTransformer::TStatus::Repeat;
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Tuple && node->IsList()) {
        auto from = sourceType.Cast<TTupleExprType>();
        auto to = expectedType.Cast<TTupleExprType>();
        if (from->GetSize() <= to->GetSize()) {
            TExprNode::TListType valueTransforms;
            valueTransforms.reserve(to->GetSize());
            for (ui32 i = 0; i < from->GetSize(); ++i) {
                const auto oldType = from->GetItems()[i];
                const auto newType = to->GetItems()[i];
                auto value = node->ChildPtr(i);
                if (const auto status = TryConvertToImpl(ctx, value, *oldType, *newType, flags, raiseIssues); status.Level == IGraphTransformer::TStatus::Error) {
                    return status;
                }

                valueTransforms.push_back(value);
            }

            for (auto i = from->GetSize(); i < to->GetSize(); ++i) {
                switch (const auto newType = to->GetItems()[i]; newType->GetKind()) {
                    case ETypeAnnotationKind::Optional:
                    case ETypeAnnotationKind::Pg:
                        valueTransforms.push_back(ctx.NewCallable(node->Pos(), "Nothing", {ExpandType(node->Pos(), *newType, ctx)}));
                        continue;
                    case ETypeAnnotationKind::Null:
                        valueTransforms.push_back(ctx.NewCallable(node->Pos(), "Null", {}));
                        continue;
                    default:
                        return IGraphTransformer::TStatus::Error;
                }
            }

            node = ctx.NewList(node->Pos(), std::move(valueTransforms));
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Tuple && sourceType.GetKind() == ETypeAnnotationKind::Tuple) {
        const auto from = sourceType.Cast<TTupleExprType>();
        const auto to = expectedType.Cast<TTupleExprType>();
        if (from->GetSize() <= to->GetSize()) {
            TExprNode::TListType valueTransforms;
            valueTransforms.reserve(to->GetSize());
            for (ui32 i = 0; i < from->GetSize(); ++i) {
                const auto oldType = from->GetItems()[i];
                const auto newType = to->GetItems()[i];
                auto value = ctx.Builder(node->Pos())
                    .Callable("Nth")
                    .Add(0, node)
                    .Atom(1, ToString(i), TNodeFlags::Default)
                    .Seal()
                    .Build();

                if (const auto status = TryConvertToImpl(ctx, value, *oldType, *newType, flags, raiseIssues); status.Level == IGraphTransformer::TStatus::Error) {
                    return status;
                }

                valueTransforms.push_back(value);
            }

            for (auto i = from->GetSize(); i < to->GetSize(); ++i) {
                switch (const auto newType = to->GetItems()[i]; newType->GetKind()) {
                    case ETypeAnnotationKind::Optional:
                    case ETypeAnnotationKind::Pg:
                        valueTransforms.push_back(ctx.NewCallable(node->Pos(), "Nothing", {ExpandType(node->Pos(), *newType, ctx)}));
                        continue;
                    case ETypeAnnotationKind::Null:
                        valueTransforms.push_back(ctx.NewCallable(node->Pos(), "Null", {}));
                        continue;
                    default:
                        return IGraphTransformer::TStatus::Error;
                }
            }

            node = ctx.NewList(node->Pos(), std::move(valueTransforms));
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::List && (node->IsCallable("List") || node->IsCallable("AsList"))) {
        auto from = sourceType.Cast<TListExprType>();
        auto to = expectedType.Cast<TListExprType>();

        auto oldItemType = from->GetItemType();
        auto newItemType = to->GetItemType();

        TExprNode::TListType valueTransforms;
        if (node->IsCallable("List")) {
            valueTransforms.push_back(ExpandType(node->Pos(), *to, ctx));
        }

        for (ui32 i = node->IsCallable("List") ? 1 : 0; i < node->ChildrenSize(); ++i) {
            auto value = node->ChildPtr(i);
            auto status = TryConvertToImpl(ctx, value, *oldItemType, *newItemType, flags, raiseIssues);
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return status;
            }

            valueTransforms.push_back(value);
        }

        node = ctx.NewCallable(node->Pos(), node->Content(), std::move(valueTransforms));
        return IGraphTransformer::TStatus::Repeat;
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::List && sourceType.GetKind() == ETypeAnnotationKind::List) {
        auto from = sourceType.Cast<TListExprType>();
        auto to = expectedType.Cast<TListExprType>();

        auto nextType = to->GetItemType();
        auto arg = ctx.NewArgument(node->Pos(), "item");
        auto originalArg = arg;
        auto status = TryConvertToImpl(ctx, arg, *from->GetItemType(), *nextType, flags, raiseIssues);
        if (status.Level != IGraphTransformer::TStatus::Error) {
            auto lambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { originalArg }), std::move(arg));

            node = ctx.NewCallable(node->Pos(), "OrderedMap", { node, lambda });

            return IGraphTransformer::TStatus::Repeat;
        }
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Stream && sourceType.GetKind() == ETypeAnnotationKind::Stream) {
        auto from = sourceType.Cast<TStreamExprType>();
        auto to = expectedType.Cast<TStreamExprType>();

        auto nextType = to->GetItemType();
        auto arg = ctx.NewArgument(node->Pos(), "item");
        auto originalArg = arg;
        auto status = TryConvertToImpl(ctx, arg, *from->GetItemType(), *nextType, flags, raiseIssues);
        if (status.Level != IGraphTransformer::TStatus::Error) {
            auto lambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { originalArg }), std::move(arg));

            node = ctx.NewCallable(node->Pos(), "OrderedMap", { node, lambda });

            return IGraphTransformer::TStatus::Repeat;
        }
    }
    else if (expectedType.GetKind() == ETypeAnnotationKind::Dict && (node->IsCallable("Dict") || node->IsCallable("AsDict"))) {
        auto from = sourceType.Cast<TDictExprType>();
        auto to = expectedType.Cast<TDictExprType>();

        auto oldKeyType = from->GetKeyType();
        auto oldPayloadType = from->GetPayloadType();
        auto newKeyType = to->GetKeyType();
        auto newPayloadType = to->GetPayloadType();

        TExprNode::TListType valueTransforms;
        if (node->IsCallable("Dict")) {
            valueTransforms.push_back(ExpandType(node->Pos(), *to, ctx));
        }

        for (ui32 i = node->IsCallable("Dict") ? 1 : 0; i < node->ChildrenSize(); ++i) {
            auto valueKey = node->Child(i)->ChildPtr(0);
            auto valuePayload = node->Child(i)->ChildPtr(1);
            auto status = TryConvertToImpl(ctx, valueKey, *oldKeyType, *newKeyType, flags, raiseIssues);
            status = status.Combine(TryConvertToImpl(ctx, valuePayload, *oldPayloadType, *newPayloadType, flags, raiseIssues));
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return status;
            }

            valueTransforms.push_back(ctx.NewList(node->Pos(), { valueKey, valuePayload }));
        }

        node = ctx.ChangeChildren(*node, std::move(valueTransforms));
        return IGraphTransformer::TStatus::Repeat;
    }  else if (expectedType.GetKind() == ETypeAnnotationKind::Dict && sourceType.GetKind() == ETypeAnnotationKind::Dict) {
        auto from = sourceType.Cast<TDictExprType>();
        auto to = expectedType.Cast<TDictExprType>();

        auto oldKeyType = from->GetKeyType();
        auto oldPayloadType = from->GetPayloadType();
        auto newKeyType = to->GetKeyType();
        auto newPayloadType = to->GetPayloadType();

        auto arg = ctx.NewArgument(node->Pos(), "item");
        auto key = ctx.NewCallable(node->Pos(), "Nth", { arg, ctx.NewAtom(node->Pos(), "0") });
        auto value = ctx.NewCallable(node->Pos(), "Nth", { arg, ctx.NewAtom(node->Pos(), "1") });
        auto status = TryConvertToImpl(ctx, key, *oldKeyType, *newKeyType, flags, raiseIssues);
        status = status.Combine(TryConvertToImpl(ctx, value, *oldPayloadType, *newPayloadType, flags, raiseIssues));
        if (status.Level != IGraphTransformer::TStatus::Error) {
            auto body = ctx.NewList(node->Pos(), { key, value });
            auto lambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { arg }), std::move(body));
            node = RebuildDict(node, lambda, ctx);
            return IGraphTransformer::TStatus::Repeat;
        }
    } else if (expectedType.GetKind() == ETypeAnnotationKind::Tagged && sourceType.GetKind() == ETypeAnnotationKind::Tagged) {
        auto from = sourceType.Cast<TTaggedExprType>();
        auto to = expectedType.Cast<TTaggedExprType>();
        if (from->GetTag() == to->GetTag()) {
            auto nextType = to->GetBaseType();
            auto arg = ctx.NewCallable(node->Pos(), "Untag", { node, ctx.NewAtom(node->Pos(), from->GetTag()) });
            auto status = TryConvertToImpl(ctx, arg, *from->GetBaseType(), *nextType, flags, raiseIssues);
            if (status.Level != IGraphTransformer::TStatus::Error) {
                node = ctx.NewCallable(node->Pos(), "AsTagged", { arg, ctx.NewAtom(node->Pos(), from->GetTag()) });
                return IGraphTransformer::TStatus::Repeat;
            }
        }
    } else if (expectedType.GetKind() == ETypeAnnotationKind::List && sourceType.GetKind() == ETypeAnnotationKind::EmptyList) {
        node = ctx.NewCallable(node->Pos(), "List", { ExpandType(node->Pos(), expectedType, ctx) });
        return IGraphTransformer::TStatus::Repeat;
    } else if (expectedType.GetKind() == ETypeAnnotationKind::Dict && sourceType.GetKind() == ETypeAnnotationKind::EmptyDict) {
        node = ctx.NewCallable(node->Pos(), "Dict", { ExpandType(node->Pos(), expectedType, ctx) });
        return IGraphTransformer::TStatus::Repeat;
    }

    return IGraphTransformer::TStatus::Error;
}

std::pair<ui8, ui8> GetDecimalParts(const TDataExprType& decimal) {
    const auto extra = static_cast<const TDataExprParamsType&>(decimal);
    const auto pr = FromString<ui8>(extra.GetParamOne());
    const auto sc = FromString<ui8>(extra.GetParamTwo());
    const auto dp = ui8(pr - sc);
    return {dp, sc};
}

using TFieldsOfStructs = std::unordered_map<std::string_view, std::array<const TTypeAnnotationNode*, 2U>>;

TFieldsOfStructs GetFieldsTypes(const TStructExprType& left, const TStructExprType& right) {
    TFieldsOfStructs fields(std::max(left.GetSize(), right.GetSize()));
    using TPairOfTypes = typename TFieldsOfStructs::mapped_type;
    for (const auto& item : right.GetItems()) {
        YQL_ENSURE(fields.emplace(item->GetName(), TPairOfTypes{{nullptr, item->GetItemType()}}).second);
    }
    for (const auto& item : left.GetItems()) {
        const auto ins = fields.emplace(item->GetName(), TPairOfTypes{{item->GetItemType(), nullptr}});
        if (!ins.second) {
            ins.first->second.front() = item->GetItemType();
        }
    }
    return fields;
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TDataExprType* source, const TDataExprType* target) {
    const auto sSlot = source->GetSlot();
    const auto tSlot = target->GetSlot();

    if (sSlot == tSlot) {
        if (EDataSlot::Decimal == sSlot) {
            if (*static_cast<const TDataExprParamsType*>(source) == *static_cast<const TDataExprParamsType*>(target)) {
                return NUdf::ECastOptions::Complete;
            }

            const auto sParts = GetDecimalParts(*source);
            const auto tParts = GetDecimalParts(*target);

            if (sParts.first <= tParts.first && sParts.second <= tParts.second) {
                return NUdf::ECastOptions::Complete;
            }
            if (std::min(sParts.first, tParts.first) + std::min(sParts.second, tParts.second)) {
                return Strong ? NUdf::ECastOptions::MayFail : NUdf::ECastOptions::MayLoseData;
            }
            return NUdf::ECastOptions::Impossible;
        }
        return NUdf::ECastOptions::Complete;
    }

    if (EDataSlot::Decimal == tSlot && IsDataTypeIntegral(sSlot)) {
        const auto tParts = GetDecimalParts(*target);
        const auto dSrc = NUdf::GetDataTypeInfo(sSlot).DecimalDigits;
        if (dSrc <= tParts.first) {
            return NUdf::ECastOptions::Complete;
        }
        return tParts.first ? Strong ? NUdf::ECastOptions::MayFail : NUdf::ECastOptions::MayLoseData : NUdf::ECastOptions::Impossible;
    }

    if (EDataSlot::Decimal == sSlot && IsDataTypeIntegral(tSlot)) {
        const auto sParts = GetDecimalParts(*source);
        const auto dDst = NUdf::GetDataTypeInfo(tSlot).DecimalDigits;
        if (!sParts.first) {
            return NUdf::ECastOptions::Impossible;
        }
        return Strong ? NUdf::ECastOptions::MayFail:
            NUdf::ECastOptions::MayFail |
            ((sParts.first >= dDst || sParts.second >0u) ? NUdf::ECastOptions::MayLoseData : NUdf::ECastOptions::Complete);
    }

    const auto option = *NUdf::GetCastResult(sSlot, tSlot);
    if (!(Strong && NUdf::ECastOptions::MayLoseData & option)) {
        return option;
    }
    return NUdf::IsComparable(sSlot, tSlot) ? NUdf::ECastOptions::MayFail : NUdf::ECastOptions::Impossible;
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TPgExprType* source, const TPgExprType* target) {
    if (source->GetId() != target->GetId()) {
        return NUdf::ECastOptions::Impossible;
    }

    return NUdf::ECastOptions::Complete;
}

template <bool Strong>
NUdf::TCastResultOptions ReduceCastResult(NUdf::TCastResultOptions result);

template <>
NUdf::TCastResultOptions ReduceCastResult<false>(NUdf::TCastResultOptions result) {
    constexpr auto fail = NUdf::ECastOptions::MayFail | NUdf::ECastOptions::AnywayLoseData;
    if (result & fail) {
        return result & ~fail | NUdf::ECastOptions::MayLoseData;
    }
    return result;
}

template <>
NUdf::TCastResultOptions ReduceCastResult<true>(NUdf::TCastResultOptions result) {
    if (result & NUdf::ECastOptions::AnywayLoseData) {
        return NUdf::ECastOptions::Impossible;
    }

    if (result & NUdf::ECastOptions::MayLoseData) {
        return result & ~NUdf::ECastOptions::MayLoseData | NUdf::ECastOptions::MayFail;
    }
    return result;
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TOptionalExprType* source, const TOptionalExprType* target) {
    return ReduceCastResult<Strong>(CastResult<Strong>(source->GetItemType(), target->GetItemType()));
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TListExprType* source, const TListExprType* target) {
    return ReduceCastResult<Strong>(CastResult<Strong>(source->GetItemType(), target->GetItemType()));
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TStreamExprType* source, const TStreamExprType* target) {
    return ReduceCastResult<Strong>(CastResult<Strong>(source->GetItemType(), target->GetItemType()));
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TFlowExprType* source, const TFlowExprType* target) {
    return ReduceCastResult<Strong>(CastResult<Strong>(source->GetItemType(), target->GetItemType()));
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TDictExprType* source, const TDictExprType* target) {
    return ReduceCastResult<Strong>(CastResult<Strong>(source->GetKeyType(), target->GetKeyType()) | CastResult<Strong>(source->GetPayloadType(), target->GetPayloadType()));
}

template <bool Strong, bool AllOrAnyElements = true>
NUdf::TCastResultOptions CastResult(const TTupleExprType* source, const TTupleExprType* target) {
    const auto& sItems = source->GetItems();
    const auto& tItems = target->GetItems();

    NUdf::TCastResultOptions result = NUdf::ECastOptions::Complete;
    for (size_t i = 0U; i < std::max(sItems.size(), tItems.size()); ++i) {
        if (i >= sItems.size()) {
            if constexpr (AllOrAnyElements) {
                if (!tItems[i]->IsOptionalOrNull())
                    return NUdf::ECastOptions::Impossible;
            }
        } else if (i >= tItems.size()) {
            if (sItems[i]->GetKind() != ETypeAnnotationKind::Null) {
                if (sItems[i]->IsOptionalOrNull()) {
                    result |= Strong ? NUdf::ECastOptions::MayFail : NUdf::ECastOptions::MayLoseData;
                } else {
                    if constexpr (Strong && AllOrAnyElements) {
                        return NUdf::ECastOptions::Impossible;
                    } else {
                        result |= AllOrAnyElements ? NUdf::ECastOptions::AnywayLoseData : NUdf::ECastOptions::MayFail;
                    }
                }
            }
        } else {
            result |= CastResult<Strong>(sItems[i], tItems[i]);
            if (result & NUdf::ECastOptions::Impossible) {
                return NUdf::ECastOptions::Impossible;
            }
        }
    }
    return result;
}

template <bool Strong, bool AllOrAnyMembers = true>
NUdf::TCastResultOptions CastResult(const TStructExprType* source, const TStructExprType* target) {
    const auto& fields = GetFieldsTypes(*source, *target);
    if (fields.empty()) {
        return NUdf::ECastOptions::Complete;
    }

    NUdf::TCastResultOptions result = NUdf::ECastOptions::Complete;
    for (const auto& field : fields) {
        if (!field.second.front()) {
            if constexpr (AllOrAnyMembers) {
                if (!field.second.back()->IsOptionalOrNull())
                    return NUdf::ECastOptions::Impossible;
            }
        } else if (!field.second.back()) {
            if (field.second.front()->GetKind() != ETypeAnnotationKind::Null) {
                if (field.second.front()->IsOptionalOrNull()) {
                    result |= Strong ? NUdf::ECastOptions::MayFail : NUdf::ECastOptions::MayLoseData;
                } else {
                    if constexpr (Strong && AllOrAnyMembers) {
                        return NUdf::ECastOptions::Impossible;
                    } else {
                        result |= AllOrAnyMembers ? NUdf::ECastOptions::AnywayLoseData : NUdf::ECastOptions::MayFail;
                    }
                }
            }
        } else {
            result |= CastResult<Strong>(field.second.front(), field.second.back());
        }

        if (result & NUdf::ECastOptions::Impossible) {
            return NUdf::ECastOptions::Impossible;
        }
    }
    return result;
}

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TVariantExprType* source, const TVariantExprType* target) {
    const auto sourceUnderType = source->GetUnderlyingType();
    const auto targetUnderType = target->GetUnderlyingType();
    if (sourceUnderType->GetKind() == ETypeAnnotationKind::Struct && targetUnderType->GetKind() == ETypeAnnotationKind::Struct) {
        return CastResult<Strong, false>(sourceUnderType->Cast<TStructExprType>(), targetUnderType->Cast<TStructExprType>());
    }
    if (sourceUnderType->GetKind() == ETypeAnnotationKind::Tuple && targetUnderType->GetKind() == ETypeAnnotationKind::Tuple) {
        return CastResult<Strong, false>(sourceUnderType->Cast<TTupleExprType>(), targetUnderType->Cast<TTupleExprType>());
    }
    return NUdf::ECastOptions::Impossible;
}

ECompareOptions AddOptional(ECompareOptions result) {
    return ECompareOptions::Comparable == result ? ECompareOptions::Optional : result;
}

template <bool Equality>
ECompareOptions Join(ECompareOptions state, ECompareOptions item) {
    if (ECompareOptions::Uncomparable == state || ECompareOptions::Uncomparable == item) {
        return ECompareOptions::Uncomparable;
    }

    if (ECompareOptions::Null == state) {
        return !Equality && ECompareOptions::Null == item ? ECompareOptions::Null : ECompareOptions::Optional;
    }

    if (ECompareOptions::Null == item || ECompareOptions::Optional == item) {
        return ECompareOptions::Optional;
    }

    return state;
}

ECompareOptions CanCompare(const TDataExprType* left, const TDataExprType* right) {
    return NUdf::IsComparable(left->GetSlot(), right->GetSlot()) ?
        ECompareOptions::Comparable : ECompareOptions::Uncomparable;
}

template <bool Equality>
ECompareOptions CanCompare(const TPgExprType* left, const TPgExprType* right) {
    if (left->GetId() != right->GetId()) {
        return ECompareOptions::Uncomparable;
    }

    if (Equality) {
        return left->IsEquatable() ? ECompareOptions::Optional : ECompareOptions::Uncomparable;
    } else {
        return left->IsComparable() ? ECompareOptions::Optional : ECompareOptions::Uncomparable;
    }
}

template <bool Equality>
ECompareOptions CanCompare(const TTaggedExprType* left, const TTaggedExprType* right) {
    if (left->GetTag() != right->GetTag())
        return ECompareOptions::Uncomparable;

    return CanCompare<Equality>(left->GetBaseType(), right->GetBaseType());
}

template <bool Equality>
ECompareOptions CanCompare(const TOptionalExprType* left, const TOptionalExprType* right) {
    return AddOptional(CanCompare<Equality>(left->GetItemType(), right->GetItemType()));
}

template <bool Equality>
ECompareOptions CanCompare(const TListExprType* left, const TListExprType* right) {
    return CanCompare<Equality>(left->GetItemType(), right->GetItemType());
}

template <bool Equality>
ECompareOptions CanCompare(const TDictExprType* left, const TDictExprType* right) {
   return Equality ? CanCompare<Equality>(left->GetPayloadType(), right->GetPayloadType()) : ECompareOptions::Uncomparable;
}

template <bool Equality>
ECompareOptions CanCompare(const TTupleExprType* left, const TTupleExprType* right) {
    const auto& lItems = left->GetItems();
    const auto& rItems = right->GetItems();

    ECompareOptions result = left->GetSize() == right->GetSize() ? ECompareOptions::Comparable : ECompareOptions::Optional;
    for (size_t i = 0U; i < std::min(lItems.size(), rItems.size()) && ECompareOptions::Uncomparable != result; ++i) {
        result = Join<Equality>(result, CanCompare<Equality>(lItems[i], rItems[i]));
    }
    return result;
}

template <bool Equality>
ECompareOptions CanCompare(const TStructExprType* left, const TStructExprType* right) {
    if (!Equality) {
        return ECompareOptions::Uncomparable;
    }

    if (!left->GetSize() && !right->GetSize()) {
        return ECompareOptions::Comparable;
    }

    const auto& fields = GetFieldsTypes(*left, *right);

    bool hasMissed = false, hasCommon = false;
    for (const auto& field : fields) {
        switch (CanCompare<Equality>(field.second.front(), field.second.back())) {
            case ECompareOptions::Null: hasMissed = true; continue;
            case ECompareOptions::Uncomparable: return ECompareOptions::Uncomparable;
            case ECompareOptions::Optional:     hasMissed = true; [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
            case ECompareOptions::Comparable:   hasCommon = true; break;
        }
    }

    return hasCommon ?
        hasMissed ? ECompareOptions::Optional : ECompareOptions::Comparable:
        ECompareOptions::Null;
}

template <bool Equality>
ECompareOptions CanCompare(const TVariantExprType* left, const TVariantExprType* right) {
    auto leftUnderlyingType = left->GetUnderlyingType();
    auto rightUnderlyingType = right->GetUnderlyingType();
    if (leftUnderlyingType->GetKind() == ETypeAnnotationKind::Tuple ||
        rightUnderlyingType->GetKind() == ETypeAnnotationKind::Tuple) {
        return CanCompare<Equality>(leftUnderlyingType, rightUnderlyingType);
    }

    bool hasMissed = false;
    auto leftStructType = leftUnderlyingType->Cast<TStructExprType>();
    auto rightStructType = rightUnderlyingType->Cast<TStructExprType>();
    for (ui32 i = 0; i < leftStructType->GetSize(); ++i) {
        auto pos = rightStructType->FindItem(leftStructType->GetItems()[i]->GetName());
        if (!pos) {
            continue;
        }

        auto leftItemType = leftStructType->GetItems()[i]->GetItemType();
        auto rightItemType = rightStructType->GetItems()[*pos]->GetItemType();
        switch (CanCompare<Equality>(leftItemType, rightItemType)) {
        case ECompareOptions::Null: hasMissed = true; continue;
        case ECompareOptions::Uncomparable: return ECompareOptions::Uncomparable;
        case ECompareOptions::Optional:     hasMissed = true; break;
        case ECompareOptions::Comparable:   break;
        }
    }

    return hasMissed ? ECompareOptions::Optional : ECompareOptions::Comparable;
}

const TTupleExprType* DryType(const TTupleExprType* type, bool& hasOptional, TExprContext& ctx) {
    auto items = type->GetItems();
    for (auto& item : items) {
        if (const auto dry = DryType(item, hasOptional, ctx))
            item = dry;
        else
            return nullptr;
    }
    return ctx.MakeType<TTupleExprType>(items);
}

template<bool Strict = true>
const TStructExprType* DryType(const TStructExprType* type, bool& hasOptional, TExprContext& ctx) {
    if (!type->GetSize())
        return type;

    auto items = type->GetItems();
    auto it = items.begin();
    for (const auto& item : items) {
        if (const auto dry = DryType(item->GetItemType(), hasOptional, ctx))
            *it++ = ctx.MakeType<TItemExprType>(item->GetName(), dry);
        else if constexpr (Strict)
            return nullptr;
    }
    items.erase(it, items.cend());
    return items.empty() ? nullptr : ctx.MakeType<TStructExprType>(items);
}

const TListExprType* DryType(const TListExprType* type, bool& hasOptional, TExprContext& ctx) {
    if (const auto itemType = DryType(type->GetItemType(), hasOptional, ctx))
        return ctx.MakeType<TListExprType>(itemType);
    return nullptr;
}

const TDictExprType* DryType(const TDictExprType* type, bool& hasOptional, TExprContext& ctx) {
    if (const auto dryKey = DryType(type->GetKeyType(), hasOptional, ctx))
        if (const auto dry = DryType(type->GetPayloadType(), hasOptional, ctx))
            return ctx.MakeType<TDictExprType>(dryKey, dry);
    return nullptr;
}

const TVariantExprType* DryType(const TVariantExprType* type, bool& hasOptional, TExprContext& ctx) {
    switch (const auto underType = type->GetUnderlyingType(); underType->GetKind()) {
        case ETypeAnnotationKind::Tuple:
            if (const auto dry = DryType(underType->Cast<TTupleExprType>(), hasOptional, ctx))
                return ctx.MakeType<TVariantExprType>(dry);
            break;
        case ETypeAnnotationKind::Struct:
            if (const auto dry = DryType<false>(underType->Cast<TStructExprType>(), hasOptional, ctx))
                return ctx.MakeType<TVariantExprType>(dry);
            break;
        default:
           break;

    }
    return nullptr;
}

const TTaggedExprType* DryType(const TTaggedExprType* type, bool& hasOptional, TExprContext& ctx) {
    if (const auto dry = DryType(type->GetBaseType(), hasOptional, ctx))
        return ctx.MakeType<TTaggedExprType>(dry, type->GetTag());
    return nullptr;
}

template<bool Silent>
const TDataExprType* CommonType(TPositionHandle pos, const TDataExprType* one, const TDataExprType* two, TExprContext& ctx, bool warn) {
    const auto slot1 = one->GetSlot();
    const auto slot2 = two->GetSlot();
    if (IsDataTypeDecimal(slot1) && IsDataTypeDecimal(slot2)) {
        const auto parts1 = GetDecimalParts(*one);
        const auto parts2 = GetDecimalParts(*two);
        const auto whole = std::min<ui8>(NDecimal::MaxPrecision, std::max<ui8>(parts1.first - parts1.second, parts2.first - parts2.second));
        const auto scale = std::min<ui8>(NDecimal::MaxPrecision - whole, std::max<ui8>(parts1.second, parts2.second));
        return ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, ToString(whole + scale), ToString(scale));
    } else if (!(IsDataTypeDecimal(slot1) || IsDataTypeDecimal(slot2))) {
        if (const auto super = GetSuperType(slot1, slot2, warn, &ctx, &pos))
            return ctx.MakeType<TDataExprType>(*super);
    }

    if constexpr (!Silent)
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Cannot infer common type for " << GetDataTypeInfo(slot1).Name << " and " << GetDataTypeInfo(slot2).Name));
    return nullptr;
}

template<bool Silent>
const TPgExprType* CommonType(TPositionHandle pos, const TPgExprType* one, const TPgExprType* two, TExprContext& ctx) {
    if (one->GetId() == two->GetId()) {
        return one;
    }
    const NPg::TTypeDesc* commonTypeDesc = nullptr;
    if (const auto issue = NPg::LookupCommonType({one->GetId(), two->GetId()},
        [&ctx, &pos](size_t i) {
            Y_UNUSED(i);

            return ctx.GetPosition(pos);
        }, commonTypeDesc))
    {
        if constexpr (!Silent) {
            ctx.AddError(*issue);
        }
        return nullptr;
    }
    return ctx.MakeType<TPgExprType>(commonTypeDesc->TypeId);
}

template<bool Silent>
const TResourceExprType* CommonType(TPositionHandle pos, const TResourceExprType* resource, const TDataExprType* data, TExprContext& ctx) {
    const auto slot = data->GetSlot();
    const auto& tag = resource->GetTag();
    if ((tag == "Yson2.Node" || tag == "Yson.Node") && (EDataSlot::Yson == slot || EDataSlot::Json == slot)) {
        return resource;
    }

    if (tag == "DateTime2.TM" &&
        (GetDataTypeInfo(slot).Features & (NUdf::EDataTypeFeatures::DateType | NUdf::EDataTypeFeatures::TzDateType)))
    {
        return resource;
    }

    if (tag == "JsonNode" && EDataSlot::Json == slot) {
        return resource;
    }

    if constexpr (!Silent) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Incompatible resource '" << tag << "' with " << GetDataTypeInfo(slot).Name));
    }
    return nullptr;
}

template<bool Strict, bool Silent, class SequenceType>
const SequenceType* CommonItemType(TPositionHandle pos, const SequenceType* one, const SequenceType* two, TExprContext& ctx) {
    if (const auto join = CommonType<Strict, Silent>(pos, one->GetItemType(), two->GetItemType(), ctx))
        return ctx.MakeType<SequenceType>(join);
    return nullptr;
}

template<bool Strict, bool Silent>
const TDictExprType* CommonType(TPositionHandle pos, const TDictExprType* one, const TDictExprType* two, TExprContext& ctx) {
    if (const auto joinKey = CommonType<Strict, Silent>(pos, one->GetKeyType(), two->GetKeyType(), ctx))
        if (const auto join = CommonType<Strict, Silent>(pos, one->GetPayloadType(), two->GetPayloadType(), ctx))
            return ctx.MakeType<TDictExprType>(joinKey, join);
    return nullptr;
}

template<bool Strict, bool Silent, bool Relaxed = false>
const TStructExprType* CommonType(TPositionHandle pos, const TStructExprType* one, const TStructExprType* two, TExprContext& ctx) {
    auto itemsOne = one->GetItems();
    auto itemsTwo = two->GetItems();

    if constexpr (Strict) {
        if constexpr (Relaxed) {
            auto it1 = itemsOne.cbegin();
            auto it2 = itemsTwo.cbegin();
            while (it1 < itemsOne.cend() && it2 < itemsTwo.cend()) {
                const auto& name1 = (*it1)->GetName();
                const auto& name2 = (*it2)->GetName();
                if (name1 < name2)
                    it1 = itemsOne.erase(it1);
                else if (name1 > name2)
                    it2 = itemsTwo.erase(it2);
                else {
                    ++it1;
                    ++it2;
                }
            }
            itemsOne.erase(it1, itemsOne.cend());
            itemsTwo.erase(it2, itemsTwo.cend());
        } else if (itemsOne.size() != itemsTwo.size())
            return nullptr;
    } else {
        auto it1 = itemsOne.cbegin();
        auto it2 = itemsTwo.cbegin();
        while (it1 < itemsOne.cend() || it2 < itemsTwo.cend()) {
            if (itemsTwo.cend() == it2 || (itemsOne.cend() > it1 && (*it1)->GetName() < (*it2)->GetName()))
                it2 = itemsTwo.emplace(it2, Relaxed ? *it1 : ctx.MakeType<TItemExprType>((*it1)->GetName(), ctx.MakeType<TNullExprType>()));
            else if (itemsOne.cend() == it1 || (itemsTwo.cend() > it2 && (*it1)->GetName() > (*it2)->GetName()))
                it1 = itemsOne.emplace(it1, Relaxed ? *it2 : ctx.MakeType<TItemExprType>((*it2)->GetName(), ctx.MakeType<TNullExprType>()));
            else {
                ++it1;
                ++it2;
            }
        }
    }

    for (auto i = 0U; i < itemsTwo.size(); ++i) {
        const auto& name = itemsOne[i]->GetName();
        if (name != itemsTwo[i]->GetName())
            return nullptr;

        if (const auto join = CommonType<Strict, Silent>(pos, itemsOne[i]->GetItemType(), itemsTwo[i]->GetItemType(), ctx))
            itemsOne[i] = ctx.MakeType<TItemExprType>(name, join);
        else
            return nullptr;
    }
    return ctx.MakeType<TStructExprType>(itemsOne);
}

template<bool Strict, bool Silent, bool Relaxed = false>
const TTupleExprType* CommonType(TPositionHandle pos, const TTupleExprType* one, const TTupleExprType* two, TExprContext& ctx) {
    auto itemsOne = one->GetItems();
    auto itemsTwo = two->GetItems();

    if constexpr (Strict && !Relaxed) {
        if (itemsOne.size() != itemsTwo.size())
            return nullptr;
    } else {
        const auto size = Strict ? std::min(itemsOne.size(), itemsTwo.size()) : std::max(itemsOne.size(), itemsTwo.size());
        itemsOne.resize(size);
        itemsTwo.resize(size);
    }

    for (auto i = 0U; i < itemsTwo.size(); ++i) {
        if (const auto join = CommonType<Strict, Silent>(pos,
            itemsOne[i] ? itemsOne[i] : Relaxed ? itemsTwo[i] : ctx.MakeType<TNullExprType>(),
            itemsTwo[i] ? itemsTwo[i] : Relaxed ? itemsOne[i] : ctx.MakeType<TNullExprType>(),
            ctx))
            itemsOne[i] = join;
        else
            return nullptr;
    }
    return ctx.MakeType<TTupleExprType>(itemsOne);
}

template<bool Strict, bool Silent>
const TVariantExprType* CommonType(TPositionHandle pos, const TVariantExprType* one, const TVariantExprType* two, TExprContext& ctx) {
    const auto underOne = one->GetUnderlyingType();
    const auto underTwo = two->GetUnderlyingType();
    const auto kind = underOne->GetKind();
    if (underTwo->GetKind() != kind)
        return nullptr;

    switch (kind) {
        case ETypeAnnotationKind::Tuple:
            if (const auto dry = CommonType<Strict, Silent, true>(pos, underOne->Cast<TTupleExprType>(), underTwo->Cast<TTupleExprType>(), ctx))
                return ctx.MakeType<TVariantExprType>(dry);
            break;
        case ETypeAnnotationKind::Struct:
            if (const auto dry = CommonType<Strict, Silent, true>(pos, underOne->Cast<TStructExprType>(), underTwo->Cast<TStructExprType>(), ctx))
                return ctx.MakeType<TVariantExprType>(dry);
            break;
        default:
           break;
    }
    return nullptr;
}

template<bool Strict, bool Silent>
const TTaggedExprType* CommonType(TPositionHandle pos, const TTaggedExprType* one, const TTaggedExprType* two, TExprContext& ctx) {
    const auto& tag = one->GetTag();
    if (two->GetTag() != tag) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Different tags '" << tag << "' and '" << two->GetTag() << "'."));
        return nullptr;
    }
    if (const auto join = CommonType<Strict, Silent>(pos, one->GetBaseType(), two->GetBaseType(), ctx))
        return ctx.MakeType<TTaggedExprType>(join, tag);
    return nullptr;
}

TExprNode::TPtr TryExpandSimpleType(TPositionHandle pos, const TStringBuf& type, TExprContext& ctx) {
    TExprNode::TPtr result;
    // backend is always ready for pragma FlexibleTypes;
    const bool flexibleTypes = true;
    if (auto found = LookupSimpleTypeBySqlAlias(type, flexibleTypes)) {
        auto typeName = ToString(*found);
        if (NKikimr::NUdf::FindDataSlot(typeName)) {
            result = ctx.NewCallable(pos, "DataType", { ctx.NewAtom(pos, typeName, TNodeFlags::Default )});
        } else {
            result = ctx.NewCallable(pos, typeName + ToString("Type"), {});
        }
    } else {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unknown type name: '" << type << "'"));
    }

    return result;
}

} // namespace

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TTypeAnnotationNode* source, const TTypeAnnotationNode* target) {
    const auto sKind = source->GetKind();
    const auto tKind = target->GetKind();

    if (sKind == tKind) {
        switch (sKind) {
            case ETypeAnnotationKind::Void:
            case ETypeAnnotationKind::Null:
            case ETypeAnnotationKind::EmptyList:
            case ETypeAnnotationKind::EmptyDict:
                return NUdf::ECastOptions::Complete;
            case ETypeAnnotationKind::Optional:
                return CastResult<Strong>(source->Cast<TOptionalExprType>(), target->Cast<TOptionalExprType>());
            case ETypeAnnotationKind::List:
                return CastResult<Strong>(source->Cast<TListExprType>(), target->Cast<TListExprType>());
            case ETypeAnnotationKind::Dict:
                return CastResult<Strong>(source->Cast<TDictExprType>(), target->Cast<TDictExprType>());
            case ETypeAnnotationKind::Tuple:
                return CastResult<Strong>(source->Cast<TTupleExprType>(), target->Cast<TTupleExprType>());
            case ETypeAnnotationKind::Struct:
                return CastResult<Strong>(source->Cast<TStructExprType>(), target->Cast<TStructExprType>());
            case ETypeAnnotationKind::Variant:
                return CastResult<Strong>(source->Cast<TVariantExprType>(), target->Cast<TVariantExprType>());
            case ETypeAnnotationKind::Data:
                return CastResult<Strong>(source->Cast<TDataExprType>(), target->Cast<TDataExprType>());
            case ETypeAnnotationKind::Pg:
                return CastResult<Strong>(source->Cast<TPgExprType>(), target->Cast<TPgExprType>());
            case ETypeAnnotationKind::Stream:
                return CastResult<Strong>(source->Cast<TStreamExprType>(), target->Cast<TStreamExprType>());
            case ETypeAnnotationKind::Flow:
                return CastResult<Strong>(source->Cast<TFlowExprType>(), target->Cast<TFlowExprType>());
            case ETypeAnnotationKind::Resource:
                return source->Cast<TResourceExprType>()->GetTag() == target->Cast<TResourceExprType>()->GetTag() ? NUdf::ECastOptions::Complete : NUdf::ECastOptions::Impossible;
            case ETypeAnnotationKind::Tagged:
                return source->Cast<TTaggedExprType>()->GetTag() == target->Cast<TTaggedExprType>()->GetTag() ?
                    CastResult<Strong>(source->Cast<TTaggedExprType>()->GetBaseType(), target->Cast<TTaggedExprType>()->GetBaseType()) :
                    NUdf::ECastOptions::Impossible;
            default: break;
        }
    } else if (sKind == ETypeAnnotationKind::Null) {
        return tKind == ETypeAnnotationKind::Optional ? NUdf::ECastOptions::Complete : Strong ? NUdf::ECastOptions::Impossible : NUdf::ECastOptions::MayFail;
    } else if (tKind == ETypeAnnotationKind::Null) {
        return sKind == ETypeAnnotationKind::Optional ?
            Strong ? NUdf::ECastOptions::MayFail : NUdf::ECastOptions::MayLoseData:
            Strong ? NUdf::ECastOptions::Impossible : NUdf::ECastOptions::AnywayLoseData;
    } else if (tKind == ETypeAnnotationKind::Optional) {
        return ReduceCastResult<Strong>(CastResult<Strong>(source, target->Cast<TOptionalExprType>()->GetItemType()));
    } else if (sKind == ETypeAnnotationKind::Optional) {
        return NUdf::ECastOptions::MayFail | CastResult<Strong>(source->Cast<TOptionalExprType>()->GetItemType(), target);
    } else if (tKind == ETypeAnnotationKind::List && sKind == ETypeAnnotationKind::EmptyList) {
        return NUdf::ECastOptions::Complete;
    } else if (tKind == ETypeAnnotationKind::Dict && sKind == ETypeAnnotationKind::EmptyDict) {
        return NUdf::ECastOptions::Complete;
    }

    return NUdf::ECastOptions::Impossible;
}

template NUdf::TCastResultOptions CastResult<true>(const TTypeAnnotationNode* source, const TTypeAnnotationNode* target);
template NUdf::TCastResultOptions CastResult<false>(const TTypeAnnotationNode* source, const TTypeAnnotationNode* target);

template <bool Equality>
ECompareOptions CanCompare(const TTypeAnnotationNode* left, const TTypeAnnotationNode* right) {
    if (!(left && right)) {
        return ECompareOptions::Null;
    }

    const auto lKind = left->GetKind();
    const auto rKind = right->GetKind();

    if (lKind == rKind) {
        switch (lKind) {
            case ETypeAnnotationKind::Void: return ECompareOptions::Comparable;
            case ETypeAnnotationKind::Null: return ECompareOptions::Null;
            case ETypeAnnotationKind::EmptyList: return ECompareOptions::Comparable;
            case ETypeAnnotationKind::EmptyDict: return Equality ? ECompareOptions::Comparable : ECompareOptions::Uncomparable;
            case ETypeAnnotationKind::Optional:
                return CanCompare<Equality>(left->Cast<TOptionalExprType>(), right->Cast<TOptionalExprType>());
            case ETypeAnnotationKind::List:
                return CanCompare<Equality>(left->Cast<TListExprType>(), right->Cast<TListExprType>());
            case ETypeAnnotationKind::Dict:
                return CanCompare<Equality>(left->Cast<TDictExprType>(), right->Cast<TDictExprType>());
            case ETypeAnnotationKind::Tuple:
                return CanCompare<Equality>(left->Cast<TTupleExprType>(), right->Cast<TTupleExprType>());
            case ETypeAnnotationKind::Struct:
                return CanCompare<Equality>(left->Cast<TStructExprType>(), right->Cast<TStructExprType>());
            case ETypeAnnotationKind::Variant:
                return CanCompare<Equality>(left->Cast<TVariantExprType>(), right->Cast<TVariantExprType>());
            case ETypeAnnotationKind::Tagged:
                return CanCompare<Equality>(left->Cast<TTaggedExprType>(), right->Cast<TTaggedExprType>());
            case ETypeAnnotationKind::Data:
                return CanCompare(left->Cast<TDataExprType>(), right->Cast<TDataExprType>());
            case ETypeAnnotationKind::Pg:
                return CanCompare<Equality>(left->Cast<TPgExprType>(), right->Cast<TPgExprType>());
            default: break;
        }
    } else if (lKind == ETypeAnnotationKind::Null || rKind == ETypeAnnotationKind::Null) {
        return ECompareOptions::Null;
    } else if (lKind == ETypeAnnotationKind::Optional) {
        return AddOptional(CanCompare<Equality>(left->Cast<TOptionalExprType>()->GetItemType(), right));
    } else if (rKind == ETypeAnnotationKind::Optional) {
        return AddOptional(CanCompare<Equality>(left, right->Cast<TOptionalExprType>()->GetItemType()));
    } else if (lKind == ETypeAnnotationKind::EmptyList && rKind == ETypeAnnotationKind::List) {
        return CanCompare<Equality>(right->Cast<TListExprType>(), right->Cast<TListExprType>());
    } else if (rKind == ETypeAnnotationKind::EmptyList && lKind == ETypeAnnotationKind::List) {
        return CanCompare<Equality>(left->Cast<TListExprType>(), left->Cast<TListExprType>());
    } else if (lKind == ETypeAnnotationKind::EmptyDict && rKind == ETypeAnnotationKind::Dict) {
        return CanCompare<Equality>(right->Cast<TDictExprType>(), right->Cast<TDictExprType>());
    } else if (rKind == ETypeAnnotationKind::EmptyDict && lKind == ETypeAnnotationKind::Dict) {
        return CanCompare<Equality>(left->Cast<TDictExprType>(), left->Cast<TDictExprType>());
    }

    return ECompareOptions::Uncomparable;
}

template ECompareOptions CanCompare<true>(const TTypeAnnotationNode* left, const TTypeAnnotationNode* right);
template ECompareOptions CanCompare<false>(const TTypeAnnotationNode* left, const TTypeAnnotationNode* right);

const TTypeAnnotationNode* DryType(const TTypeAnnotationNode* type, bool& hasOptional, TExprContext& ctx) {
    if (type) {
        switch (type->GetKind()) {
            case ETypeAnnotationKind::Optional:
                hasOptional = true;
                return DryType(type->Cast<TOptionalExprType>()->GetItemType(), hasOptional, ctx);
            case ETypeAnnotationKind::Pg:
            case ETypeAnnotationKind::Data:
            case ETypeAnnotationKind::Void:
            case ETypeAnnotationKind::EmptyList:
            case ETypeAnnotationKind::EmptyDict:
                return type;
            case ETypeAnnotationKind::Tuple:
                return DryType(type->Cast<TTupleExprType>(), hasOptional, ctx);
            case ETypeAnnotationKind::Struct:
                return DryType(type->Cast<TStructExprType>(), hasOptional, ctx);
            case ETypeAnnotationKind::List:
                return DryType(type->Cast<TListExprType>(), hasOptional, ctx);
            case ETypeAnnotationKind::Dict:
                return DryType(type->Cast<TDictExprType>(), hasOptional, ctx);
            case ETypeAnnotationKind::Variant:
                return DryType(type->Cast<TVariantExprType>(), hasOptional, ctx);
            case ETypeAnnotationKind::Tagged:
                return DryType(type->Cast<TTaggedExprType>(), hasOptional, ctx);
            default:
                break;
        }
    }

    return nullptr;
}

const TTypeAnnotationNode* DryType(const TTypeAnnotationNode* type, TExprContext& ctx) {
    if (bool optional = false; const auto dry = DryType(type, optional, ctx))
        return optional ? ctx.MakeType<TOptionalExprType>(dry) : dry;
    return nullptr;
}

const TTypeAnnotationNode* JoinDryKeyType(bool outer, const TTypeAnnotationNode* primary, const TTypeAnnotationNode* secondary, TExprContext& ctx) {
    bool hasOptional = false;
    if (const auto dry = DryType(primary, hasOptional, ctx))
        if (!((NUdf::ECastOptions::AnywayLoseData | NUdf::ECastOptions::Impossible) & CastResult<true>(secondary, dry)))
            return hasOptional && outer ? ctx.MakeType<TOptionalExprType>(dry) : dry;
    return nullptr;
}

const TTypeAnnotationNode* JoinDryKeyType(const TTypeAnnotationNode* primary, const TTypeAnnotationNode* secondary, bool& hasOptional, TExprContext& ctx) {
    if (const auto dry = DryType(primary, hasOptional, ctx))
        if (!((NUdf::ECastOptions::AnywayLoseData | NUdf::ECastOptions::Impossible) & CastResult<true>(secondary, dry)))
            return dry;
    return nullptr;
}

const TTypeAnnotationNode* JoinCommonDryKeyType(TPositionHandle position, bool outer, const TTypeAnnotationNode* one, const TTypeAnnotationNode* two, TExprContext& ctx) {
    bool optOne = false, optTwo = false;
    auto dryOne = DryType(one, optOne, ctx);
    auto dryTwo = DryType(two, optTwo, ctx);
    if (!(dryOne && dryTwo))
        return nullptr;

    if (outer && (optOne || optTwo)) {
        dryOne = ctx.MakeType<TOptionalExprType>(dryOne);
        dryTwo = ctx.MakeType<TOptionalExprType>(dryTwo);
    }

    return CommonType<true, false>(position, dryOne, dryTwo, ctx);
}

template<bool Strict, bool Silent>
const TTypeAnnotationNode* CommonType(TPositionHandle pos, const TTypeAnnotationNode* one, const TTypeAnnotationNode* two, TExprContext& ctx, bool warn) {
    if (!(one && two))
        return nullptr;

    if (HasError(one, ctx) || HasError(two, ctx)) {
        return nullptr;
    }

    if (IsSameAnnotation(*one, *two))
        return two;

    if (const auto kindOne = one->GetKind(), kindTwo = two->GetKind(); kindOne == kindTwo) {
        switch (kindOne) {
            case ETypeAnnotationKind::Data:
                return CommonType<Silent>(pos, one->Cast<TDataExprType>(), two->Cast<TDataExprType>(), ctx, warn);
            case ETypeAnnotationKind::Optional:
                return CommonItemType<Strict, Silent>(pos, one->Cast<TOptionalExprType>(), two->Cast<TOptionalExprType>(), ctx);
            case ETypeAnnotationKind::List:
                return CommonItemType<Strict, Silent>(pos, one->Cast<TListExprType>(), two->Cast<TListExprType>(), ctx);
            case ETypeAnnotationKind::Flow:
                return CommonItemType<Strict, Silent>(pos, one->Cast<TFlowExprType>(), two->Cast<TFlowExprType>(), ctx);
            case ETypeAnnotationKind::Stream:
                return CommonItemType<Strict, Silent>(pos, one->Cast<TStreamExprType>(), two->Cast<TStreamExprType>(), ctx);
            case ETypeAnnotationKind::Dict:
                return CommonType<Strict, Silent>(pos, one->Cast<TDictExprType>(), two->Cast<TDictExprType>(), ctx);
            case ETypeAnnotationKind::Tuple:
                return CommonType<Strict, Silent>(pos, one->Cast<TTupleExprType>(), two->Cast<TTupleExprType>(), ctx);
            case ETypeAnnotationKind::Struct:
                return CommonType<Strict, Silent>(pos, one->Cast<TStructExprType>(), two->Cast<TStructExprType>(), ctx);
            case ETypeAnnotationKind::Variant:
                return CommonType<Strict, Silent>(pos, one->Cast<TVariantExprType>(), two->Cast<TVariantExprType>(), ctx);
            case ETypeAnnotationKind::Tagged:
                return CommonType<Strict, Silent>(pos, one->Cast<TTaggedExprType>(), two->Cast<TTaggedExprType>(), ctx);
            case ETypeAnnotationKind::Pg:
                return CommonType<Silent>(pos, one->Cast<TPgExprType>(), two->Cast<TPgExprType>(), ctx);
            default:
                break;
        }

        if constexpr (!Silent)
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Cannot infer common type for " << kindOne));
    } else {
        if constexpr (!Strict) {
            if (ETypeAnnotationKind::Pg == kindOne) {
                if (ETypeAnnotationKind::Null == kindTwo)
                    return one;
            } else if (ETypeAnnotationKind::Pg == kindTwo) {
                if (ETypeAnnotationKind::Null == kindOne)
                    return two;
            } else if (ETypeAnnotationKind::Optional == kindOne) {
                if (ETypeAnnotationKind::Null  == kindTwo)
                    return one;
                else if (const auto itemType = CommonType<Strict, Silent>(pos, one->Cast<TOptionalExprType>()->GetItemType(), two, ctx))
                    return ctx.MakeType<TOptionalExprType>(itemType);
            } else if (ETypeAnnotationKind::Optional == kindTwo) {
                if (ETypeAnnotationKind::Null  == kindOne)
                    return two;
                else if (const auto itemType = CommonType<Strict, Silent>(pos, one, two->Cast<TOptionalExprType>()->GetItemType(), ctx))
                    return ctx.MakeType<TOptionalExprType>(itemType);
            } else if (ETypeAnnotationKind::Null == kindOne) {
                return ctx.MakeType<TOptionalExprType>(two);
            } else if (ETypeAnnotationKind::Null == kindTwo) {
                return ctx.MakeType<TOptionalExprType>(one);
            } else if (ETypeAnnotationKind::EmptyList == kindOne && ETypeAnnotationKind::List == kindTwo
                    || ETypeAnnotationKind::EmptyDict == kindOne && ETypeAnnotationKind::Dict == kindTwo) {
                return two;
            } else if (ETypeAnnotationKind::EmptyList == kindTwo && ETypeAnnotationKind::List == kindOne
                    || ETypeAnnotationKind::EmptyDict == kindTwo && ETypeAnnotationKind::Dict == kindOne) {
                return one;
            } else if (ETypeAnnotationKind::Resource == kindOne && ETypeAnnotationKind::Data == kindTwo) {
                if constexpr (!Strict)
                    return CommonType<Silent>(pos, one->Cast<TResourceExprType>(), two->Cast<TDataExprType>(), ctx);
            } else if (ETypeAnnotationKind::Resource == kindTwo && ETypeAnnotationKind::Data == kindOne) {
                if constexpr (!Strict)
                    return CommonType<Silent>(pos, two->Cast<TResourceExprType>(), one->Cast<TDataExprType>(), ctx);
            }
        }

        if constexpr (!Silent)
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Cannot infer common type for " << kindOne << " and " << kindTwo));
    }

    return nullptr;
}

template const TTypeAnnotationNode* CommonType<true, false>(TPositionHandle pos, const TTypeAnnotationNode* one, const TTypeAnnotationNode* two, TExprContext& ctx, bool warn);
template const TTypeAnnotationNode* CommonType<false, false>(TPositionHandle pos, const TTypeAnnotationNode* one, const TTypeAnnotationNode* two, TExprContext& ctx, bool warn);
template const TTypeAnnotationNode* CommonType<false, true>(TPositionHandle pos, const TTypeAnnotationNode* one, const TTypeAnnotationNode* two, TExprContext& ctx, bool warn);

const TTypeAnnotationNode* CommonType(TPositionHandle position, const TTypeAnnotationNode::TSpanType& types, TExprContext& ctx, bool warn) {
    switch (types.size()) {
        case 0U: return nullptr;
        case 1U: return types.front();
        case 2U: return CommonType<false, false>(position, types.front(), types.back(), ctx, warn);
        default: break;
    }

    const auto left = types.size() >> 1U, right = types.size() - left;
    return CommonType<false, false>(position, CommonType(position, types.first(left), ctx, warn), CommonType(position, types.last(right), ctx, warn), ctx, warn);
}

const TTypeAnnotationNode* CommonTypeForChildren(const TExprNode& node, TExprContext& ctx, bool warn) {
    TTypeAnnotationNode::TListType types(node.ChildrenSize());
    for (auto i = 0U; i < types.size(); ++i) {
        if (const auto item = node.Child(i); EnsureComputable(*item, ctx))
            types[i] = item->GetTypeAnn();
        else
            return nullptr;
    }
    return CommonType(node.Pos(), types, ctx, warn);
}

size_t GetOptionalLevel(const TTypeAnnotationNode* type) {
    size_t level = 0;
    YQL_ENSURE(type);
    while (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
        ++level;
    }
    return level;
}

void ClearExprTypeAnnotations(TExprNode& root) {
    for (auto& child : root.Children()) {
        ClearExprTypeAnnotations(*child);
    }

    root.SetTypeAnn(nullptr);
}

bool AreAllNodesTypeAnnotated(const TExprNode& root) {
    if (!root.GetTypeAnn())
        return false;

    for (auto& child : root.Children()) {
        if (!AreAllNodesTypeAnnotated(*child))
            return false;
    }

    return true;
}

void EnsureAllNodesTypeAnnotated(const TExprNode& root) {
    YQL_ENSURE(root.GetTypeAnn());
    for (auto& child : root.Children()) {
        EnsureAllNodesTypeAnnotated(*child);
    }
}

namespace {

bool IsPg(
    TPosition pos,
    const TTypeAnnotationNode* typeAnnotation,
    const TPgExprType*& pgType,
    TIssue& err,
    bool& hasErrorType)
{
    err = {};
    hasErrorType = false;

    if (!typeAnnotation) {
        err = TIssue(pos, TStringBuilder() << "Expected pg, but got lambda");
        return false;
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Pg) {
        pgType = typeAnnotation->Cast<TPgExprType>();
        return true;
    }

    if (!HasError(typeAnnotation, err)) {
        err = TIssue(pos, TStringBuilder() << "Expected pg, but got: " << *typeAnnotation);
    } else {
        hasErrorType = true;
    }
    return false;
}

bool IsDataOrOptionalOfData(TPosition pos, const TTypeAnnotationNode* typeAnnotation, bool& isOptional,
    const TDataExprType*& dataType, TIssue& err, bool& hasErrorType)
{
    err = {};
    hasErrorType = false;

    if (!typeAnnotation) {
        err = TIssue(pos, TStringBuilder() << "Expected data or optional of data, but got lambda");
        return false;
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Data) {
        isOptional = false;
        dataType = typeAnnotation->Cast<TDataExprType>();
        return true;
    }

    if (typeAnnotation->GetKind() != ETypeAnnotationKind::Optional) {
        if (!HasError(typeAnnotation, err)) {
            err = TIssue(pos, TStringBuilder() << "Expected data or optional of data, but got: " << *typeAnnotation);
        } else {
            hasErrorType = true;
        }
        return false;
    }

    auto itemType = typeAnnotation->Cast<TOptionalExprType>()->GetItemType();
    if (itemType->GetKind() != ETypeAnnotationKind::Data) {
        if (!HasError(itemType, err)) {
            err = TIssue(pos, TStringBuilder() << "Expected data or optional of data, but got optional of: " << *itemType);
        } else {
            hasErrorType = true;
        }
        return false;
    }

    isOptional = true;
    dataType = itemType->Cast<TDataExprType>();
    return true;
}

}

bool IsDataOrOptionalOfData(const TTypeAnnotationNode* typeAnnotation, bool& isOptional, const TDataExprType*& dataType)
{
    TIssue err;
    bool hasErrorType;
    return IsDataOrOptionalOfData({}, typeAnnotation, isOptional, dataType, err, hasErrorType);
}

bool IsDataOrOptionalOfData(const TTypeAnnotationNode* typeAnnotation) {
    bool isOptional;
    const TDataExprType* dataType;
    return IsDataOrOptionalOfData(typeAnnotation, isOptional, dataType);
}

bool IsPg(const TTypeAnnotationNode* typeAnnotation, const TPgExprType*& pgType) {
    TIssue err;
    bool hasErrorType;
    return IsPg({}, typeAnnotation, pgType, err, hasErrorType);
}

bool IsDataOrOptionalOfDataOrPg(const TTypeAnnotationNode* typeAnnotation) {
    bool isOptional;
    const TDataExprType* dataType;
    const TPgExprType* pg;
    return IsDataOrOptionalOfData(typeAnnotation, isOptional, dataType) || IsPg(typeAnnotation, pg);
}

bool EnsureArgsCount(const TExprNode& node, ui32 expectedArgs, TExprContext& ctx) {
    if (node.ChildrenSize() != expectedArgs) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << expectedArgs << " argument(s), but got " <<
            node.ChildrenSize()));
        return false;
    }

    return true;
}

bool EnsureMinArgsCount(const TExprNode& node, ui32 expectedArgs, TExprContext& ctx) {
    if (node.ChildrenSize() < expectedArgs) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected at least " << expectedArgs << " argument(s), but got " <<
            node.ChildrenSize()));
        return false;
    }

    return true;
}

bool EnsureMaxArgsCount(const TExprNode& node, ui32 expectedArgs, TExprContext& ctx) {
    if (node.ChildrenSize() > expectedArgs) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected at most " << expectedArgs << " argument(s), but got " <<
            node.ChildrenSize()));
        return false;
    }

    return true;
}

bool EnsureMinMaxArgsCount(const TExprNode& node, ui32 minArgs, ui32 maxArgs, TExprContext& ctx) {
    return EnsureMinArgsCount(node, minArgs, ctx) && EnsureMaxArgsCount(node, maxArgs, ctx);
}

bool EnsureCallableMinArgsCount(const TPositionHandle& pos, ui32 args, ui32 expectedArgs, TExprContext& ctx) {
    if (args < expectedArgs) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Callable expected at least " << expectedArgs << " argument(s), but got " << args));
        return false;
    }

    return true;
}

bool EnsureCallableMaxArgsCount(const TPositionHandle& pos, ui32 args, ui32 expectedArgs, TExprContext& ctx) {
    if (args > expectedArgs) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Callable expected at most " << expectedArgs << " argument(s), but got " << args));
        return false;
    }

    return true;
}

bool EnsureAtom(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (node.Type() != TExprNode::Atom) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected atom, but got: " << node.Type()));
        return false;
    }

    return true;
}

bool EnsureCallable(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (node.Type() != TExprNode::Callable) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected callable, but got: " << node.Type()));
        return false;
    }

    return true;
}

bool EnsureTuple(TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (node.Type() != TExprNode::List) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple, but got: " << node.Type()));
        return false;
    }

    node.SetLiteralList(true);
    return true;
}

bool EnsureTupleOfAtoms(TExprNode& node, TExprContext& ctx) {
    if (!EnsureTuple(node, ctx)) {
        return false;
    }

    for (const auto& atom : node.ChildrenList()) {
        if (!EnsureAtom(*atom, ctx)) {
            return false;
        }
    }
    return true;
}

bool EnsureValidSettings(TExprNode& node,
    const THashSet<TStringBuf>& supportedSettings,
    const TSettingNodeValidator& validator,
    TExprContext& ctx)
{
    if (!EnsureTuple(node, ctx)) {
        return false;
    }

    for (auto& settingNode : node.ChildrenList()) {
        if (!EnsureTupleMinSize(*settingNode, 1, ctx)) {
            return false;
        }

        if (!EnsureAtom(settingNode->Head(), ctx)) {
            return false;
        }

        const TStringBuf name = settingNode->Head().Content();
        if (!supportedSettings.contains(name)) {
            ctx.AddError(TIssue(ctx.GetPosition(settingNode->Head().Pos()), TStringBuilder() << "Unknown setting '" << name << "'"));
            return false;
        }

        if (!validator(name, *settingNode, ctx)) {
            return false;
        }
    }
    return true;
}


bool EnsureValidUserSchemaSetting(TExprNode& node, TExprContext& ctx) {
    if (!EnsureTupleMinSize(node, 2, ctx)) {
        return false;
    }

    if (!EnsureTupleMaxSize(node, 3, ctx)) {
        return false;
    }

    if (!EnsureAtom(node.Head(), ctx)) {
        return false;
    }

    if (node.Head().Content() != "userschema") {
        ctx.AddError(TIssue(ctx.GetPosition(node.Head().Pos()), TStringBuilder() << "Expecting userschema, but got '" << node.Head().Content() << "'"));
        return false;
    }

    if (!EnsureTypeWithStructType(*node.Child(1), ctx)) {
        return false;
    }

    if (node.ChildrenSize() == 3) {
        if (!EnsureTupleOfAtoms(*node.Child(2), ctx)) {
            return false;
        }
        const TStructExprType* s = node.Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        THashSet<TStringBuf> items;
        for (auto child : node.Child(2)->ChildrenList()) {
            if (!items.insert(child->Content()).second) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Head().Pos()),
                                    TStringBuilder() << "Invalid positional userschema: got duplicated field  '" << child->Content() << "'"));
                return false;

            }
            if (!s->FindItem(child->Content())) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Head().Pos()),
                                    TStringBuilder() << "Invalid positional userschema: field  '" << child->Content() << "'"
                                                     << " is not found in type " << *(const TTypeAnnotationNode*)s));
                return false;
            }
        }

        if (node.Child(2)->ChildrenSize() < s->GetSize()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Head().Pos()),
                                TStringBuilder() << "Invalid positional userschema of size " << node.Child(2)->ChildrenSize()
                                                 << " with struct type of size " << s->GetSize()));
            return false;
        }
    }

    return true;
}


TSettingNodeValidator RequireSingleValueSettings(const TSettingNodeValidator& validator) {
    return [validator](TStringBuf name, TExprNode& setting, TExprContext& ctx) {
        if (setting.ChildrenSize() != 2) {
            ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()),
                                TStringBuilder() << "Option '" << name << "' requires single argument"));
            return false;
        }
        return validator(name, setting, ctx);
    };
}

bool EnsureTupleSize(TExprNode& node, ui32 expectedSize, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (node.Type() != TExprNode::List) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple, but got: " << node.Type()));
        return false;
    }

    if (node.ChildrenSize() != expectedSize) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple size: " << expectedSize << ", but got: " <<
            node.ChildrenSize()));
        return false;
    }

    node.SetLiteralList(true);
    return true;
}

bool EnsureTupleMinSize(TExprNode& node, ui32 minSize, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (node.Type() != TExprNode::List) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple, but got: " << node.Type()));
        return false;
    }

    if (node.ChildrenSize() < minSize) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple of at least size: " << minSize << ", but got: " <<
            node.ChildrenSize()));
        return false;
    }

    node.SetLiteralList(true);
    return true;
}

bool EnsureTupleMaxSize(TExprNode& node, ui32 maxSize, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (node.Type() != TExprNode::List) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple, but got: " << node.Type()));
        return false;
    }

    if (node.ChildrenSize() > maxSize) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple of at most size: " << maxSize << ", but got: " <<
            node.ChildrenSize()));
        return false;
    }

    node.SetLiteralList(true);
    return true;
}

bool EnsureTupleType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tuple) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tuple type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureTupleType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Tuple) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected tuple type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureTupleTypeSize(const TExprNode& node, ui32 expectedSize, TExprContext& ctx) {
    return EnsureTupleTypeSize(node.Pos(), node.GetTypeAnn(), expectedSize, ctx);
}

bool EnsureTupleTypeSize(TPositionHandle position, const TTypeAnnotationNode* type, ui32 expectedSize, TExprContext& ctx) {
    if (HasError(type, ctx)) {
        return false;
    }

    if (!type) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected tuple type, but got lambda"));
        return false;
    }

    if (type->GetKind() != ETypeAnnotationKind::Tuple) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected tuple type, but got: " << *type));
        return false;
    }

    auto tupleSize = type->Cast<TTupleExprType>()->GetSize();
    if (tupleSize != expectedSize) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected tuple type of size: " << expectedSize << ", but got: "
            << tupleSize));
        return false;
    }

    return true;
}

bool EnsureMultiType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected multi type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Multi) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected multi type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureMultiType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Multi) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected multi type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureVariantType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected variant type, but got lambda"));
        return false;
    }

    return EnsureVariantType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureVariantType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Variant) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected variant type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureDataOrPgType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected data or pg type, but got lambda"));
        return false;
    }

    return EnsureDataOrPgType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureDataOrPgType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Data && type.GetKind() != ETypeAnnotationKind::Pg) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected data or pg type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsurePgType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected pg type, but got lambda"));
        return false;
    }

    return EnsurePgType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsurePgType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Pg) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected pg type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureDataType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected data type, but got lambda"));
        return false;
    }

    return EnsureDataType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureDataType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected data type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureLambda(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (node.Type() != TExprNode::Lambda) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected lambda, but got: " << node.Type()));
        return false;
    }

    return true;
}

IGraphTransformer::TStatus ConvertToLambda(TExprNode::TPtr& node, TExprContext& ctx, ui32 minArgumentsCount,
    ui32 maxArgumentsCount, bool withTypes) {
    if (node->Type() == TExprNode::Lambda || node->IsCallable("WithOptionalArgs")) {
        auto& actualLambda = node->IsCallable("WithOptionalArgs") ? *node->Child(0) : *node;
        const ui32 optionalArgsCount = node->IsCallable("WithOptionalArgs") ? FromString<ui32>(node->Child(1)->Content()) : 0;
        const ui32 maxLambdaArgs = actualLambda.Child(0)->ChildrenSize();
        const ui32 minLambdaArgs = maxLambdaArgs - optionalArgsCount;

        if (actualLambda.ChildrenSize() == 2U && actualLambda.Tail().IsLambda()) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Tail().Pos()), TStringBuilder() << "Free lambda is not expected here"));
            return IGraphTransformer::TStatus::Error;
        }

        const ui32 maxTargetArgs = maxArgumentsCount != Max<ui32>() ? maxArgumentsCount : minArgumentsCount;
        if (maxTargetArgs != Max<ui32>()) {
            if (maxTargetArgs < minLambdaArgs) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected at most "
                    << maxTargetArgs << " arguments, but lambda provided at least " << minLambdaArgs << " arguments"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (minArgumentsCount != Max<ui32>()) {
            if (minArgumentsCount > maxLambdaArgs) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected at least "
                    << minArgumentsCount << " arguments, but lambda provided at most " << maxLambdaArgs << " arguments"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (node->IsCallable("WithOptionalArgs")) {
            if (maxTargetArgs == Max<ui32>()) {
                node = node->ChildPtr(0);
            } else {
                // rebuild lambda with NULLs
                TExprNode::TListType args;
                for (ui32 index = 0; index < maxTargetArgs; ++index) {
                    args.push_back(ctx.NewArgument(node->Pos(), Sprintf("arg%" PRIu32, index)));
                }

                auto nullNode = ctx.NewCallable(node->Pos(), "Null", {});
                TNodeOnNodeOwnedMap replaces;
                replaces.reserve(maxLambdaArgs);
                ui32 i = 0U;
                actualLambda.Child(0)->ForEachChild([&](const TExprNode& arg) {
                    auto value = nullNode;
                    if (i < maxTargetArgs) {
                        value = args[i++];
                    }

                    replaces.emplace(&arg, value);
                });

                TExprNode::TListType body;
                if (actualLambda.ChildrenSize() > 2U) {
                    body = ctx.ReplaceNodes(GetLambdaBody(actualLambda), replaces);
                } else {
                    body = TExprNode::TListType({ ctx.ReplaceNodes(actualLambda.TailPtr(), replaces) });
                }

                auto arguments = ctx.NewArguments(node->Pos(), std::move(args));
                node = ctx.NewLambda(node->Pos(), std::move(arguments), std::move(body));
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        return IGraphTransformer::TStatus::Ok;
    }

    if (HasError(node->GetTypeAnn(), ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!withTypes || node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Callable) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected lambda, but got: " << node->Type()));
        return IGraphTransformer::TStatus::Error;
    }

    auto callableType = node->GetTypeAnn()->Cast<TCallableExprType>();
    if (minArgumentsCount != Max<ui32>() && (minArgumentsCount > callableType->GetArgumentsSize() ||
            minArgumentsCount < callableType->GetArgumentsSize() - callableType->GetOptionalArgumentsCount())) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to convert to lambda with "
            << minArgumentsCount << " arguments from callable type " << *node->GetTypeAnn()));

        return IGraphTransformer::TStatus::Error;
    }

    TExprNode::TListType args;
    args.push_back(node);
    for (ui32 index = 0; index < (minArgumentsCount != Max<ui32>() ? minArgumentsCount : callableType->GetArgumentsSize()); ++index) {
        args.push_back(ctx.NewArgument(node->Pos(), Sprintf("arg%" PRIu32, index)));
    }

    auto body = ctx.NewCallable(node->Pos(), "Apply", TExprNode::TListType(args));
    args.erase(args.begin());
    auto arguments = ctx.NewArguments(node->Pos(), std::move(args));
    node = ctx.NewLambda(node->Pos(), std::move(arguments), std::move(body));
    return IGraphTransformer::TStatus::Repeat;
}

bool EnsureSpecificDataType(const TExprNode& node, EDataSlot expectedDataSlot, TExprContext& ctx, bool allowOptional) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected data type, but got lambda"));
        return false;
    }

    if (allowOptional && node.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
        auto optionalType = node.GetTypeAnn()->Cast<TOptionalExprType>();
        return EnsureSpecificDataType(node.Pos(), *optionalType->GetItemType(), expectedDataSlot, ctx);
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected data type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    auto dataSlot = node.GetTypeAnn()->Cast<TDataExprType>()->GetSlot();
    if (dataSlot != expectedDataSlot) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected data type: " << NKikimr::NUdf::GetDataTypeInfo(expectedDataSlot).Name << ", but got: " <<
            *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureSpecificDataType(TPositionHandle position, const TTypeAnnotationNode& type, EDataSlot expectedDataSlot, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected data type, but got: " << type));
        return false;
    }

    auto dataSlot = type.Cast<TDataExprType>()->GetSlot();
    if (dataSlot != expectedDataSlot) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected data type: " << NKikimr::NUdf::GetDataTypeInfo(expectedDataSlot).Name << ", but got: " <<
            type));
        return false;
    }

    return true;
}

bool EnsureStringOrUtf8Type(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected String or Utf8, but got lambda."));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected String or Utf8, but got: " << *node.GetTypeAnn()));
        return false;
    }

    if (const auto dataSlot = node.GetTypeAnn()->Cast<TDataExprType>()->GetSlot(); dataSlot != EDataSlot::String && dataSlot != EDataSlot::Utf8) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected String or Utf8, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureStringOrUtf8Type(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected String or Utf8, but got: " << type));
        return false;
    }

    if (const auto dataSlot = type.Cast<TDataExprType>()->GetSlot(); dataSlot != EDataSlot::String && dataSlot != EDataSlot::Utf8) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected String or Utf8, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureStructType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected struct type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Struct) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected struct type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureStructType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Struct) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected struct type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureStaticContainerType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (!(type.GetKind() == ETypeAnnotationKind::Struct || type.GetKind() == ETypeAnnotationKind::Tuple || type.GetKind() == ETypeAnnotationKind::Multi)) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected struct, tuple or multi type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureTypeWithStructType(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureType(node, ctx)) {
        return false;
    }
    auto nodeType = node.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    YQL_ENSURE(nodeType);
    if (!EnsureStructType(node.Pos(), *nodeType, ctx)) {
        return false;
    }
    return true;
}

bool EnsureComposable(const TExprNode& node, TExprContext& ctx) {
    if (!node.IsComposable()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Composable required. World, datasink, datasource and lambda are not composable"));
        return false;
    }

    return true;
}

bool EnsureComposableType(const TExprNode& node, TExprContext& ctx) {
    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected composable type, but got lambda"));
        return false;
    }

    return EnsureComposableType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureComposableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (!type.IsComposable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), "Composable type required. World, datasink, datasource types are not composable"));
        return false;
    }

    return true;
}

bool EnsureWorldType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected world type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::World) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected world type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureDataSource(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasource callable, but got lambda"));
        return false;
    }

    if (!node.IsCallable("DataSource")) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasource callable"));
        return false;
    }

    return true;
}

bool EnsureDataSink(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasink callable, but got lambda"));
        return false;
    }

    if (!node.IsCallable("DataSink")) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasink callable"));
        return false;
    }

    return true;
}

bool EnsureDataProvider(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasource or datasink callable, but got lambda"));
        return false;
    }

    if (!node.IsCallable("DataSource") && !node.IsCallable("DataSink")) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasource or datasink callable"));
        return false;
    }

    return true;
}

bool EnsureSpecificDataSource(const TExprNode& node, TStringBuf expectedCategory, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasource callable, but got lambda"));
        return false;
    }

    if (!node.IsCallable("DataSource")) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasource callable"));
        return false;
    }

    auto category = node.Head().Content();
    if (category != expectedCategory) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasource category: " << expectedCategory <<
            ", but got: " << category));
        return false;
    }

    return true;
}

bool EnsureSpecificDataSink(const TExprNode& node, TStringBuf expectedCategory, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasink callable, but got lambda"));
        return false;
    }

    if (!node.IsCallable("DataSink")) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasink callable"));
        return false;
    }

    auto category = node.Head().Content();
    if (category != expectedCategory) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected datasink category: " << expectedCategory <<
            ", but got: " << category));
        return false;
    }

    return true;
}

bool EnsureListType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected list type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected list type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureListType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::List) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected list type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureListOrEmptyType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected (empty) list type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::List &&
        node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::EmptyList) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected (empty) list type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureListOrEmptyType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::List && type.GetKind() != ETypeAnnotationKind::EmptyList) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected (empty) list type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureListOfVoidType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected list of void type, but got lambda"));
        return false;
    }

    return EnsureListOfVoidType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureListOfVoidType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (!EnsureListType(position, type, ctx)) {
        return false;
    }

    auto listType = type.Cast<TListExprType>();
    YQL_ENSURE(listType);

    if (listType->GetItemType()->GetKind() != ETypeAnnotationKind::Void) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected list of void type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureStreamType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected stream type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Stream) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected stream type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureStreamType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Stream) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected stream type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureFlowType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected flow type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Flow) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected flow type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureFlowType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Flow) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected flow type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureWideFlowType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected wide flow type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Flow || node.GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Multi) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected wide flow type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureWideFlowType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Flow ||
        type.Cast<TFlowExprType>()->GetItemType()->GetKind() !=
            ETypeAnnotationKind::Multi) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected wide flow type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureWideStreamType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected wide stream type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Stream || node.GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Multi) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected wide stream type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureWideStreamType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Stream || type.Cast<TStreamExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Multi) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected wide stream type, but got: " << type));
        return false;
    }

    return true;
}

bool IsWideBlockType(const TTypeAnnotationNode& type) {
    if (type.GetKind() != ETypeAnnotationKind::Multi) {
        return false;
    }

    const auto& items = type.Cast<TMultiExprType>()->GetItems();
    if (items.empty()) {
        return false;
    }

    if (!AllOf(items, [](const auto& item){ return item->IsBlockOrScalar(); })) {
        return false;
    }

    if (items.back()->GetKind() != ETypeAnnotationKind::Scalar) {
        return false;
    }

    auto blockLenType = items.back()->Cast<TScalarExprType>()->GetItemType();
    if (blockLenType->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }

    return blockLenType->Cast<TDataExprType>()->GetSlot() == EDataSlot::Uint64;
}

bool IsWideSequenceBlockType(const TTypeAnnotationNode& type) {
    const TTypeAnnotationNode* itemType = nullptr;
    if (type.GetKind() == ETypeAnnotationKind::Stream) {
        itemType = type.Cast<TStreamExprType>()->GetItemType();
    } else if (type.GetKind() == ETypeAnnotationKind::Flow) {
        itemType = type.Cast<TFlowExprType>()->GetItemType();
    } else {
        return false;
    }
    return IsWideBlockType(*itemType);
}

bool IsSupportedAsBlockType(TPositionHandle pos, const TTypeAnnotationNode& type, TExprContext& ctx, TTypeAnnotationContext& types,
    bool reportUnspported)
{
    if (!types.ArrowResolver) {
        return false;
    }

    IArrowResolver::TUnsupportedTypeCallback onUnsupportedType;
    if (reportUnspported) {
        onUnsupportedType  = [&types](const auto& typeKindOrSlot) {
            std::visit([&types](const auto& value) { types.IncNoBlockType(value); }, typeKindOrSlot);
        };
    }
    auto resolveStatus = types.ArrowResolver->AreTypesSupported(ctx.GetPosition(pos), { &type }, ctx, onUnsupportedType);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    return resolveStatus == IArrowResolver::OK;
}

bool EnsureSupportedAsBlockType(TPositionHandle pos, const TTypeAnnotationNode& type, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (!types.ArrowResolver) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "Arrow resolver isn't available"));
        return false;
    }

    if (!IsSupportedAsBlockType(pos, type, ctx, types)) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Type " << type << " is not supported in Block mode"));
        return false;
    }

    return true;
}

bool EnsureWideBlockType(TPositionHandle position, const TTypeAnnotationNode& type, TTypeAnnotationNode::TListType& blockItemTypes, TExprContext& ctx, bool allowScalar) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Multi) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected wide type, but got: " << type));
        return false;
    }

    auto& items = type.Cast<TMultiExprType>()->GetItems();
    if (items.empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), "Expected at least one column"));
        return false;
    }

    bool isScalar;
    for (ui32 i = 0; i < items.size(); ++i) {
        const auto& itemType = items[i];
        if (!EnsureBlockOrScalarType(position, *itemType, ctx)) {
            return false;
        }

        blockItemTypes.push_back(GetBlockItemType(*itemType, isScalar));
        if (!allowScalar && isScalar && (i + 1 != items.size())) {
            ctx.AddError(TIssue(ctx.GetPosition(position), "Scalars are not allowed"));
            return false;
        }
    }

    if (!isScalar) {
        ctx.AddError(TIssue(ctx.GetPosition(position), "Last column should be a scalar"));
        return false;
    }

    if (!EnsureSpecificDataType(position, *blockItemTypes.back(), EDataSlot::Uint64, ctx)) {
        return false;
    }

    return true;
}

bool EnsureWideFlowBlockType(const TExprNode& node, TTypeAnnotationNode::TListType& blockItemTypes, TExprContext& ctx, bool allowScalar) {
    if (!EnsureWideFlowType(node, ctx)) {
        return false;
    }

    return EnsureWideBlockType(node.Pos(), *node.GetTypeAnn()->Cast<TFlowExprType>()->GetItemType(), blockItemTypes, ctx, allowScalar);
}

bool EnsureWideStreamBlockType(const TExprNode& node, TTypeAnnotationNode::TListType& blockItemTypes, TExprContext& ctx, bool allowScalar) {
    if (!EnsureWideStreamType(node, ctx)) {
        return false;
    }

    return EnsureWideBlockType(node.Pos(), *node.GetTypeAnn()->Cast<TStreamExprType>()->GetItemType(), blockItemTypes, ctx, allowScalar);
}

bool EnsureOptionalType(const TExprNode& node, TExprContext& ctx) {
    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected optional type, but got lambda"));
        return false;
    }

    return EnsureOptionalType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureOptionalType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Optional) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected optional type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureType(const TExprNode& node, TExprContext& ctx) {
    YQL_ENSURE(!node.IsCallable({"SqlColumnOrType", "SqlPlainColumnOrType", "SqlColumnFromType"}),
        "Unexpected " << node.Content() << " it should be processed earlier");
    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

IGraphTransformer::TStatus EnsureTypeRewrite(TExprNode::TPtr& node, TExprContext& ctx) {
    const TTypeAnnotationNode* type = node->GetTypeAnn();
    if (!type) {
        YQL_ENSURE(node->Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected type, but got lambda"));
        return IGraphTransformer::TStatus::Error;
    }

    if (node->IsCallable({"SqlColumnOrType", "SqlPlainColumnOrType", "SqlColumnFromType"})) {
        ui32 typeNameIdx = node->IsCallable("SqlColumnFromType") ? 2 : 1;
        auto typeNameNode = node->Child(typeNameIdx);
        YQL_ENSURE(typeNameNode->IsAtom());
        TStringBuf typeName = typeNameNode->Content();
        auto typeNode = TryExpandSimpleType(node->Pos(), typeName, ctx);
        if (!typeNode) {
            return IGraphTransformer::TStatus::Error;
        }
        node = typeNode;
        return IGraphTransformer::TStatus::Repeat;
    }

    if (type->GetKind() != ETypeAnnotationKind::Type) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected type, but got: " << *type));
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus EnsureTypeOrAtomRewrite(TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Type() != TExprNode::Atom) {
        return EnsureTypeRewrite(node, ctx);
    }

    return IGraphTransformer::TStatus::Ok;
}

bool EnsureTypePg(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureType(node, ctx)) {
        return false;
    }

    if (node.GetTypeAnn()->Cast<TTypeExprType>()->GetType()->GetKind() != ETypeAnnotationKind::Pg) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected pg type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureDryType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (const auto dry = DryType(&type, ctx); !(dry && IsSameAnnotation(*dry, type))) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected dry type, but got: " << type));
        return false;
    }
    return true;
}

bool EnsureDryType(const TExprNode& node, TExprContext& ctx) {
    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected dry type, but got lambda."));
        return false;
    }

    return EnsureDryType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureDictType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected dict type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Dict) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected dict type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureDictType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Dict) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected dict type, but got: " << type));
        return false;
    }

    return true;
}

bool IsVoidType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        return false;
    }
    return node.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Void;
}

bool EnsureVoidType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected void type, but got lambda"));
        return false;
    }

    if (!IsVoidType(node, ctx)) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected void type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureVoidLiteral(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected void literal, but got lambda"));
        return false;
    }

    if (!node.IsCallable("Void")) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected void literal, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureCallableType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected callable type, but got lambda"));
        return false;
    }

    return EnsureCallableType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureCallableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Callable) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected callable type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureResourceType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected resource type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Resource) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected resource type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureTaggedType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tagged type, but got lambda"));
        return false;
    }

    if (node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Tagged) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected tagged type, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureTaggedType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (type.GetKind() != ETypeAnnotationKind::Tagged) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected tagged type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureOneOrTupleOfDataOrOptionalOfData(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
            "Expected either data (optional of data) or non-empty tuple of data (optional of data), but got lambda"));
        return false;
    }

    return EnsureOneOrTupleOfDataOrOptionalOfData(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureOneOrTupleOfDataOrOptionalOfData(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    bool ok = false;
    bool isOptional = false;
    const TDataExprType* dataType = nullptr;
    TIssue err;
    bool hasErrorType = false;
    TPosition pos = ctx.GetPosition(position);
    if (type.GetKind() == ETypeAnnotationKind::Tuple) {
        for (auto& child: type.Cast<TTupleExprType>()->GetItems()) {
            ok = IsDataOrOptionalOfData(pos, child, isOptional, dataType, err, hasErrorType);
            if (!ok) {
                break;
            }
        }
    } else {
        ok = IsDataOrOptionalOfData(pos, &type, isOptional, dataType, err, hasErrorType);
    }

    if (!ok) {
        ctx.AddError(err);
        if (!hasErrorType) {
            ctx.AddError(TIssue(pos, TStringBuilder()
                << "Expected either data (optional of data) or non-empty tuple of data (optional of data), but got: " << type));
        }
    }

    return ok;
}

bool EnsureComparableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (!type.IsComparable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder()
            << "Expected comparable type, i.e. combination of Data, Optional, List or Tuple, but got:" << type));
        return false;
    }
    return true;
}

bool EnsureEquatableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (!type.IsEquatable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder()
            << "Expected equatable type, i.e. combination of Data, Optional, List, Dict, Tuple, Struct, or Variant, but got:" << type));
        return false;
    }
    return true;
}

bool IsInstantEqual(const TTypeAnnotationNode& type) {
    switch (type.GetKind()) {
    case ETypeAnnotationKind::Null: return true;
    case ETypeAnnotationKind::Void: return true;
    case ETypeAnnotationKind::Tuple: {
        const auto tupleType = type.Cast<TTupleExprType>();
        if (const auto size = tupleType->GetSize()) {
            for (const auto& item : tupleType->GetItems()) {
                if (!IsInstantEqual(*item)) {
                    return false;
                }
            }
        }
        break;
    }
    case ETypeAnnotationKind::Struct: {
        const auto structType = type.Cast<TStructExprType>();
        if (const auto size = structType->GetSize()) {
            for (const auto& item : structType->GetItems()) {
                if (!IsInstantEqual(*item)) {
                    return false;
                }
            }
        }
        break;
    }
    default: return false;
    }
    return true;
}

bool EnsureHashableKey(TPositionHandle position, const TTypeAnnotationNode* keyType, TExprContext& ctx) {
    if (HasError(keyType, ctx)) {
        return false;
    }

    if (!keyType->IsHashable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected hashable type, but got: " << *keyType));
        return false;
    }

    return true;
}

bool EnsureComparableKey(TPositionHandle position, const TTypeAnnotationNode* keyType, TExprContext& ctx) {
    if (HasError(keyType, ctx)) {
        return false;
    }

    if (!keyType->IsComparable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected comparable type, but got: " << *keyType));
        return false;
    }

    return true;
}

bool EnsureEquatableKey(TPositionHandle position, const TTypeAnnotationNode* keyType, TExprContext& ctx) {
    if (HasError(keyType, ctx)) {
        return false;
    }

    if (!keyType->IsEquatable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected equatable type, but got: " << *keyType));
        return false;
    }

    return true;
}

bool UpdateLambdaAllArgumentsTypes(TExprNode::TPtr& lambda, const std::vector<const TTypeAnnotationNode*>& argumentsAnnotations, TExprContext& ctx) {
    YQL_ENSURE(lambda->Type() == TExprNode::Lambda);

    const auto& args = lambda->Head();
    auto argsChildren = args.ChildrenList();
    YQL_ENSURE(argsChildren.size() == argumentsAnnotations.size());

    bool updateArgs = false;
    for (size_t i = 0U; i < argumentsAnnotations.size(); ++i) {
        const auto arg = args.Child(i);
        if (!arg->GetTypeAnn() || !IsSameAnnotation(*arg->GetTypeAnn(), *argumentsAnnotations[i])) {
            updateArgs = true;
            break;
        }
    }
    if (!updateArgs && args.GetTypeAnn()) {
        return true;
    }

    TNodeOnNodeOwnedMap replaces;
    replaces.reserve(argumentsAnnotations.size());

    for (size_t i = 0U; i < argumentsAnnotations.size(); ++i) {
        const auto arg = args.Child(i);
        auto newArg = ctx.ShallowCopy(*arg);
        newArg->SetTypeAnn(argumentsAnnotations[i]);
        YQL_ENSURE(replaces.emplace(arg, newArg).second);
        argsChildren[i] = std::move(newArg);
    }

    auto newArgs = ctx.NewArguments(args.Pos(), std::move(argsChildren));
    newArgs->SetTypeAnn(ctx.MakeType<TUnitExprType>());
    lambda = ctx.NewLambda(lambda->Pos(), std::move(newArgs), ctx.ReplaceNodes(GetLambdaBody(*lambda), replaces));
    return true;
}

bool UpdateLambdaArgumentsType(const TExprNode& lambda, TExprContext& ctx) {
    YQL_ENSURE(lambda.Type() == TExprNode::Lambda);
    const auto args = lambda.Child(0);
    YQL_ENSURE(0U == args->ChildrenSize());
    args->SetTypeAnn(ctx.MakeType<TUnitExprType>());
    return true;
}

bool EnsureDataOrOptionalOfData(const TExprNode& node, bool& isOptional, const TDataExprType*& dataType, TExprContext& ctx) {
    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
    }

    return EnsureDataOrOptionalOfData(node.Pos(), node.GetTypeAnn(), isOptional, dataType, ctx);
}

bool EnsureDataOrOptionalOfData(TPositionHandle position, const TTypeAnnotationNode* type,
    bool& isOptional, const TDataExprType*& dataType, TExprContext& ctx)
{
    TIssue err;
    bool hasErrorType;
    if (!IsDataOrOptionalOfData(ctx.GetPosition(position), type, isOptional, dataType, err, hasErrorType)) {
        ctx.AddError(err);
        return false;
    }
    return true;
}

bool EnsurePersistable(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.IsPersistable()) {
        if (node.GetTypeAnn()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
                "Expected persistable data, but got: " << *node.GetTypeAnn()));
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
                "Expected persistable data, but got lambda"));
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Persistable required. Atom, type, key, world, datasink, datasource, callable, resource, stream and lambda are not persistable"));
        return false;
    }

    return true;
}

bool EnsurePersistableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (!type.IsPersistable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() <<
            "Expected persistable data, but got: " << type));

        ctx.AddError(TIssue(ctx.GetPosition(position), "Persistable required. Atom, key, world, datasink, datasource, callable, resource, stream and lambda are not persistable"));
        return false;
    }

    return true;
}

bool EnsureComputable(const TExprNode& node, TExprContext& ctx) {
    if (!node.IsComputable()) {
        if (node.GetTypeAnn()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
                "Expected computable data, but got: " << *node.GetTypeAnn()));
        }
        else {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
                "Expected computable data, but got lambda"));
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Computable required. Atom, key, world, datasink, datasource, type, lambda are not computable"));
        return false;
    }

    return true;
}

bool EnsureInspectable(const TExprNode& node, TExprContext& ctx) {
    if (!node.IsInspectable()) {
        if (node.GetTypeAnn()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
                "Expected inspectable data, but got: " << *node.GetTypeAnn()));
        }
        else {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
                "Expected inspectable data, but got lambda"));
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Inspectable required. World, datasink, datasource, lambda are not inspectable"));
        return false;
    }

    return true;
}

bool EnsureInspectableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (!type.IsInspectable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() <<
            "Expected inspectable data, but got: " << type));

        ctx.AddError(TIssue(ctx.GetPosition(position), "Inspectable required. Atom, key, world, datasink, datasource, lambda are not inspectable"));
        return false;
    }

    return true;
}

bool EnsureComputableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (!type.IsComputable()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() <<
            "Expected computable data, but got: " << type));

        ctx.AddError(TIssue(ctx.GetPosition(position), "Computable required. Atom, key, world, datasink, datasource, type, lambda are not computable"));
        return false;
    }

    return true;
}

bool EnsureListOrOptionalType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either list or optional, but got lambda"));
        return false;
    }

    auto kind = node.GetTypeAnn()->GetKind();
    if (kind != ETypeAnnotationKind::List && kind != ETypeAnnotationKind::Optional) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either list or optional, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureListOrOptionalListType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    const auto type = node.GetTypeAnn();
    if (!type) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either list or optional of list, but got lambda"));
        return false;
    }

    const auto kind = type->GetKind();
    if (kind != ETypeAnnotationKind::List && kind != ETypeAnnotationKind::Optional) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either list or optional of list, but got: " << *type));
        return false;
    }

    if (kind == ETypeAnnotationKind::Optional) {
        const auto itemType = type->Cast<TOptionalExprType>()->GetItemType();
        if (itemType->GetKind() != ETypeAnnotationKind::List) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either list or optional of list, but got: " << *itemType));
            return false;
        }
    }

    return true;
}

bool EnsureSeqType(const TExprNode& node, TExprContext& ctx, bool* isStream) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either list or stream, but got lambda"));
        return false;
    }

    return EnsureSeqType(node.Pos(), *node.GetTypeAnn(), ctx, isStream);
}

bool EnsureSeqType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx, bool* isStream) {
    if (HasError(&type, ctx)) {
        return false;
    }

    switch (type.GetKind()) {
        case ETypeAnnotationKind::List:
        case ETypeAnnotationKind::Stream:
            if (isStream) {
                *isStream = (type.GetKind() == ETypeAnnotationKind::Stream);
            }
            return true;

        default:
            ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected list or stream, but got: " << type));
            return false;

    }
}

bool EnsureSeqOrOptionalType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either list, stream or optional, but got lambda"));
        return false;
    }

    auto kind = node.GetTypeAnn()->GetKind();
    if (kind != ETypeAnnotationKind::List && kind != ETypeAnnotationKind::Stream && kind != ETypeAnnotationKind::Optional) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected list, stream or optional, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

bool EnsureSeqOrOptionalType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    auto kind = type.GetKind();
    if (kind != ETypeAnnotationKind::List && kind != ETypeAnnotationKind::Stream && kind != ETypeAnnotationKind::Optional) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected list, stream or optional, but got: " << type));
        return false;
    }

    return true;
}

template <bool WithOptional, bool WithList, bool WithStream>
bool EnsureNewSeqType(const TExprNode& node, TExprContext& ctx, const TTypeAnnotationNode** itemType) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() <<
        (WithList ?
            (WithOptional ? "Expected flow, list, stream or optional, but got lambda." : "Expected flow, list or stream, but got lambda."):
            (WithOptional ? "Expected flow, stream or optional, but got lambda." : "Expected flow or stream, but got lambda.")
        )));
        return false;
    }

    return EnsureNewSeqType<WithOptional, WithList, WithStream>(node.Pos(), *node.GetTypeAnn(), ctx, itemType);
}

template <bool WithOptional, bool WithList, bool WithStream>
bool EnsureNewSeqType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx, const TTypeAnnotationNode** itemType) {
    if (HasError(&type, ctx)) {
        return false;
    }

    switch (type.GetKind()) {
        case ETypeAnnotationKind::Flow:
            if (itemType) {
                *itemType = type.Cast<TFlowExprType>()->GetItemType();
            }
            return true;
        case ETypeAnnotationKind::Stream:
            if constexpr (WithStream) {
                if (itemType) {
                    *itemType = type.Cast<TStreamExprType>()->GetItemType();
                }
            }
            return true;
        case ETypeAnnotationKind::List:
            if constexpr (WithList) {
                if (itemType) {
                    *itemType = type.Cast<TListExprType>()->GetItemType();
                }
                return true;
            }
            break;
        case ETypeAnnotationKind::Optional:
            if constexpr (WithOptional) {
                if (itemType) {
                    *itemType = type.Cast<TOptionalExprType>()->GetItemType();
                }
                return true;
            }
            break;
        default: break;
    }
    ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() <<
        (WithList ?
            (WithOptional ? "Expected flow, list, stream or optional, but got: " : "Expected flow, list or stream, but got: "):
            (WithOptional ? "Expected flow, stream or optional, but got: " : "Expected flow or stream, but got: ")
        ) << type));
    return false;
}

template bool EnsureNewSeqType<true, false, true>(const TExprNode& node, TExprContext& ctx, const TTypeAnnotationNode** itemType);
template bool EnsureNewSeqType<false, false, true>(const TExprNode& node, TExprContext& ctx, const TTypeAnnotationNode** itemType);
template bool EnsureNewSeqType<true, true, true>(const TExprNode& node, TExprContext& ctx, const TTypeAnnotationNode** itemType);
template bool EnsureNewSeqType<false, true, true>(const TExprNode& node, TExprContext& ctx, const TTypeAnnotationNode** itemType);
template bool EnsureNewSeqType<false, true, false>(const TExprNode& node, TExprContext& ctx, const TTypeAnnotationNode** itemType);

bool EnsureAnySeqType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Expected flow, list, stream or dict, but got lambda."));
        return false;
    }

    return EnsureAnySeqType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureAnySeqType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    switch (type.GetKind()) {
        case ETypeAnnotationKind::Flow:
        case ETypeAnnotationKind::Stream:
        case ETypeAnnotationKind::List:
        case ETypeAnnotationKind::Dict:
            return true;
        default: break;
    }
    ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected flow, list, stream or dict, but got: "  << type));
    return false;
}

bool EnsureStructOrOptionalStructType(const TExprNode& node, bool& isOptional, const TStructExprType*& structType, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either struct or optional of struct, but got lambda"));
        return false;
    }

    return EnsureStructOrOptionalStructType(node.Pos(), *node.GetTypeAnn(), isOptional, structType, ctx);
}
bool EnsureStructOrOptionalStructType(TPositionHandle position, const TTypeAnnotationNode& type, bool& isOptional,
    const TStructExprType*& structType, TExprContext& ctx)
{
    if (HasError(&type, ctx)) {
        return false;
    }

    auto kind = type.GetKind();
    if (kind != ETypeAnnotationKind::Struct && kind != ETypeAnnotationKind::Optional) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected either struct or optional of struct, but got: " << type));
        return false;
    }

    if (kind == ETypeAnnotationKind::Optional) {
        auto itemType = type.Cast<TOptionalExprType>()->GetItemType();
        kind = itemType->GetKind();
        if (kind != ETypeAnnotationKind::Struct) {
            ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected either struct or optional of struct, but got: " << type));
            return false;
        }
        isOptional = true;
        structType = itemType->Cast<TStructExprType>();
    } else {
        isOptional = false;
        structType = type.Cast<TStructExprType>();
    }

    return true;
}

bool EnsureDependsOn(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.IsCallable()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected DependsOn, but got node with type: " << node.Type()));
        return false;
    }

    if (!node.IsCallable("DependsOn")) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected DependsOn, but got callable: " << node.Content()));
        return false;
    }

    return true;
}

bool EnsureDependsOnTail(const TExprNode& node, TExprContext& ctx, unsigned requiredArgumentCount, unsigned requiredDependsOnCount) {
    if (!EnsureMinArgsCount(node, requiredArgumentCount+requiredDependsOnCount, ctx)) {
        return false;
    }
    for (unsigned i = requiredArgumentCount; i < node.ChildrenSize(); ++i) {
        if (!EnsureDependsOn(*node.Child(i), ctx)) {
            return false;
        }
    }
    return true;
}

const TTypeAnnotationNode* MakeTypeHandleResourceType(TExprContext& ctx) {
    return ctx.MakeType<TResourceExprType>(TypeResourceTag);
}

bool EnsureTypeHandleResourceType(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureResourceType(node, ctx)) {
        return false;
    }

    if (node.GetTypeAnn()->Cast<TResourceExprType>()->GetTag() != TypeResourceTag) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected type handle, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

const TTypeAnnotationNode* MakeCodeResourceType(TExprContext& ctx) {
    return ctx.MakeType<TResourceExprType>(CodeResourceTag);
}

bool EnsureCodeResourceType(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureResourceType(node, ctx)) {
        return false;
    }

    if (node.GetTypeAnn()->Cast<TResourceExprType>()->GetTag() != CodeResourceTag) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected type handle, but got: " << *node.GetTypeAnn()));
        return false;
    }

    return true;
}

const TTypeAnnotationNode* MakeSequenceType(ETypeAnnotationKind sequenceKind, const TTypeAnnotationNode& itemType, TExprContext& ctx) {
    switch (sequenceKind) {
        case ETypeAnnotationKind::Optional: return ctx.MakeType<TOptionalExprType>(&itemType);
        case ETypeAnnotationKind::Flow:     return ctx.MakeType<TFlowExprType>(&itemType);
        case ETypeAnnotationKind::List:     return ctx.MakeType<TListExprType>(&itemType);
        case ETypeAnnotationKind::Stream:   return ctx.MakeType<TStreamExprType>(&itemType);
        default: break;
    }

    ythrow yexception() << "Wrong sequence kind.";
}

IGraphTransformer::TStatus TryConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& expectedType,
    TExprContext& ctx, TConvertFlags flags) {
    if (HasError(node->GetTypeAnn(), ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (node->Type() == TExprNode::Lambda) {
        if (expectedType.GetKind() == ETypeAnnotationKind::Callable) {
            auto callableType = expectedType.Cast<TCallableExprType>();
            auto lambdaArgsCount = node->Head().ChildrenSize();
            if (lambdaArgsCount != callableType->GetArgumentsSize()) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Wrong number of lambda arguments: "
                    << lambdaArgsCount << ", failed to convert lambda to " << expectedType));
                return IGraphTransformer::TStatus::Error;
            }

            auto typeNode = ExpandType(node->Pos(), expectedType, ctx);
            node = ctx.NewCallable(node->Pos(), "Callable", { typeNode, node });
            return IGraphTransformer::TStatus::Repeat;
        }

        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to convert lambda to " << expectedType));
        return IGraphTransformer::TStatus::Error;
    }

    return TryConvertTo(node, *node->GetTypeAnn(), expectedType, ctx, flags);
}

IGraphTransformer::TStatus TryConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& sourceType,
    const TTypeAnnotationNode& expectedType, TExprContext& ctx, TConvertFlags flags) {
    if (HasError(node->GetTypeAnn(), ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    TIssueScopeGuard guard(ctx.IssueManager, [&] {
            return MakeIntrusive<TIssue>(ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Failed to convert type: " << sourceType << " to " << expectedType);
        });
    auto status = TryConvertToImpl(ctx, node, sourceType, expectedType, flags, /* raiseIssues */ true);
    if (status.Level  == IGraphTransformer::TStatus::Error) {
        guard.RaiseIssueForEmptyScope();
    }
    return status;
}

IGraphTransformer::TStatus TrySilentConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& expectedType,
    TExprContext& ctx, TConvertFlags flags) {
    if (node->Type() == TExprNode::Lambda) {
        auto currentType = &expectedType;
        ui32 optLevel = 0;
        while (currentType->GetKind() == ETypeAnnotationKind::Optional) {
            currentType = RemoveOptionalType(currentType);
            ++optLevel;
        }

        if (currentType->GetKind() == ETypeAnnotationKind::Callable) {
            auto callableType = currentType->Cast<TCallableExprType>();
            auto lambdaArgsCount = node->Head().ChildrenSize();
            if (lambdaArgsCount != callableType->GetArgumentsSize()) {
                return IGraphTransformer::TStatus::Error;
            }

            auto typeNode = ExpandType(node->Pos(), *currentType, ctx);
            node = ctx.NewCallable(node->Pos(), "Callable", { typeNode, node });
            for (ui32 i = 0; i < optLevel; ++i) {
                node = ctx.NewCallable(node->Pos(), "Just", { node });
            }

            return IGraphTransformer::TStatus::Repeat;
        }

        return IGraphTransformer::TStatus::Error;
    }

    return TrySilentConvertTo(node, *node->GetTypeAnn(), expectedType, ctx, flags);
}

IGraphTransformer::TStatus TrySilentConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& sourceType,
    const TTypeAnnotationNode& expectedType, TExprContext& ctx, TConvertFlags flags) {
    return TryConvertToImpl(ctx, node, sourceType, expectedType, flags);
}

bool IsDataTypeNumeric(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::NumericType;
}

bool IsDataTypeFloat(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::FloatType;
}

bool IsDataTypeIntegral(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::IntegralType;
}

bool IsDataTypeSigned(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::SignedIntegralType;
}

bool IsDataTypeUnsigned(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::UnsignedIntegralType;
}

bool IsDataTypeDateOrTzDateOrInterval(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & (NUdf::DateType | NUdf::TzDateType | NUdf::TimeIntervalType);
}

bool IsDataTypeDateOrTzDate(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & (NUdf::DateType | NUdf::TzDateType);
}

bool IsDataTypeInterval(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::TimeIntervalType;
}

bool IsDataTypeDate(EDataSlot dataSlot) {
    return (NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::DateType);
}

bool IsDataTypeTzDate(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::TzDateType;
}

bool IsDataTypeBigDate(EDataSlot dataSlot) {
    return (NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::BigDateType);
}

EDataSlot WithTzDate(EDataSlot dataSlot) {
    if (dataSlot == EDataSlot::Date) {
        return EDataSlot::TzDate;
    }

    if (dataSlot == EDataSlot::Datetime) {
        return EDataSlot::TzDatetime;
    }

    if (dataSlot == EDataSlot::Timestamp) {
        return EDataSlot::TzTimestamp;
    }

    if (dataSlot == EDataSlot::Date32) {
        return EDataSlot::TzDate32;
    }

    if (dataSlot == EDataSlot::Datetime64) {
        return EDataSlot::TzDatetime64;
    }

    if (dataSlot == EDataSlot::Timestamp64) {
        return EDataSlot::TzTimestamp64;
    }

    return dataSlot;
}

EDataSlot WithoutTzDate(EDataSlot dataSlot) {
    if (dataSlot == EDataSlot::TzDate) {
        return EDataSlot::Date;
    }

    if (dataSlot == EDataSlot::TzDatetime) {
        return EDataSlot::Datetime;
    }

    if (dataSlot == EDataSlot::TzTimestamp) {
        return EDataSlot::Timestamp;
    }

    if (dataSlot == EDataSlot::TzDate32) {
        return EDataSlot::Date32;
    }

    if (dataSlot == EDataSlot::TzDatetime64) {
        return EDataSlot::Datetime64;
    }

    if (dataSlot == EDataSlot::TzTimestamp64) {
        return EDataSlot::Timestamp64;
    }

    return dataSlot;
}

EDataSlot MakeSigned(EDataSlot dataSlot) {
    switch (dataSlot) {
        case EDataSlot::Uint8: return EDataSlot::Int8;
        case EDataSlot::Uint16: return EDataSlot::Int16;
        case EDataSlot::Uint32: return EDataSlot::Int32;
        case EDataSlot::Uint64: return EDataSlot::Int64;
        default: return dataSlot;
    }
}

EDataSlot MakeUnsigned(EDataSlot dataSlot) {
    switch (dataSlot) {
        case EDataSlot::Int8: return EDataSlot::Uint8;
        case EDataSlot::Int16: return EDataSlot::Uint16;
        case EDataSlot::Int32: return EDataSlot::Uint32;
        case EDataSlot::Int64: return EDataSlot::Uint64;
        default: return dataSlot;
    }
}

bool IsDataTypeDecimal(EDataSlot dataSlot) {
    return dataSlot == EDataSlot::Decimal;
}

ui8 GetDecimalWidthOfIntegral(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).DecimalDigits;
}

TMaybe<EDataSlot> GetSuperType(EDataSlot dataSlot1, EDataSlot dataSlot2, bool warn, TExprContext* ctx, TPositionHandle* pos) {
    if (dataSlot1 == dataSlot2) {
        return dataSlot1;
    }

    if (IsDataTypeNumeric(dataSlot1) && IsDataTypeNumeric(dataSlot2)) {
        auto lvl1 = GetNumericDataTypeLevel(dataSlot1);
        auto lvl2 = GetNumericDataTypeLevel(dataSlot2);
        if (lvl1 > lvl2) {
            std::swap(lvl1, lvl2);
        }
        bool isFromSignedToUnsigned = (lvl1 & 1) && !(lvl2 & 1);
        bool isFromUnsignedToSignedSameWidth = ((lvl1 == (lvl2 & ~1u)) && ((lvl1 ^ lvl2) & 1));
        if (warn && lvl2 < 8 && (isFromSignedToUnsigned || isFromUnsignedToSignedSameWidth)) {
            auto issue = TIssue(ctx->GetPosition(*pos), TStringBuilder() <<
                "Consider using explicit CAST or BITCAST to convert from " <<
                NKikimr::NUdf::GetDataTypeInfo(dataSlot1).Name << " to " << NKikimr::NUdf::GetDataTypeInfo(dataSlot2).Name);
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_IMPLICIT_BITCAST, issue);
            if (!ctx->AddWarning(issue)) {
                return {};
            }
        }
        return GetNumericDataTypeByLevel(lvl2);
    }

    if (IsDataTypeString(dataSlot1) && IsDataTypeString(dataSlot2)) {
        if (dataSlot1 == EDataSlot::Json && dataSlot2 == EDataSlot::Utf8 ||
            dataSlot1 == EDataSlot::Utf8 && dataSlot2 == EDataSlot::Json)
        {
            return EDataSlot::Utf8;
        }
        return EDataSlot::String;
    }

    if ((IsDataTypeDate(dataSlot1) || IsDataTypeTzDate(dataSlot1)) && (IsDataTypeDate(dataSlot2) || IsDataTypeTzDate(dataSlot2))) {
        // date < tzdate
        auto level1 = GetDateTypeLevel(WithoutTzDate(dataSlot1));
        auto level2 = GetDateTypeLevel(WithoutTzDate(dataSlot2));
        constexpr auto narrowDateMask = 3;
        auto level = Max(level1 & narrowDateMask, level2 & narrowDateMask);
        auto bigDateBit = (narrowDateMask + 1) & (level1 | level2);
        auto ret = GetDateTypeByLevel(level | bigDateBit);

        if (IsDataTypeTzDate(dataSlot1) || IsDataTypeTzDate(dataSlot2)) {
            ret = WithTzDate(ret);
        }

        return ret;
    }

    if (IsDataTypeInterval(dataSlot1) && IsDataTypeInterval(dataSlot2)) {
        return (dataSlot1 == EDataSlot::Interval64 || dataSlot2 == EDataSlot::Interval64) 
            ? EDataSlot::Interval64
            : EDataSlot::Interval;
    }

    return {};
}

IGraphTransformer::TStatus SilentInferCommonType(TExprNode::TPtr& node1, TExprNode::TPtr& node2, TExprContext& ctx,
    const TTypeAnnotationNode*& commonType, TConvertFlags flags) {
    if (!node1->GetTypeAnn() || !node2->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Error;
    }

    return SilentInferCommonType(node1, *node1->GetTypeAnn(), node2, *node2->GetTypeAnn(), ctx, commonType, flags);
}

IGraphTransformer::TStatus SilentInferCommonType(TExprNode::TPtr& node1, const TTypeAnnotationNode& type1,
    TExprNode::TPtr& node2, const TTypeAnnotationNode& type2, TExprContext& ctx,
    const TTypeAnnotationNode*& commonType, TConvertFlags flags) {
    if (IsSameAnnotation(type1, type2)) {
        commonType = &type1;
        return IGraphTransformer::TStatus::Ok;
    }

    auto newFlags = flags;
    newFlags.Set(NConvertFlags::DisableTruncation);
    if (const auto status = TrySilentConvertTo(node1, type1, type2, ctx, newFlags); status != IGraphTransformer::TStatus::Error) {
        commonType = &type2;
        return status;
    }

    if (const auto status = TrySilentConvertTo(node2, type2, type1, ctx, newFlags); status != IGraphTransformer::TStatus::Error) {
        commonType = &type1;
        return status;
    }

    if (type2.GetKind() == ETypeAnnotationKind::Optional && type1.GetKind() != ETypeAnnotationKind::Optional) {
        auto type1Opt = ctx.MakeType<TOptionalExprType>(&type1);
        auto prev = node1;
        node1 = ctx.NewCallable(node1->Pos(), "Just", { std::move(node1) });

        const TTypeAnnotationNode* commonItemType;
        if (SilentInferCommonType(node1, *type1Opt, node2, type2, ctx, commonItemType, flags) != IGraphTransformer::TStatus::Error) {
            commonType = commonItemType;
            return IGraphTransformer::TStatus::Repeat;
        }

        node1 = prev;
    }

    if (type1.GetKind() == ETypeAnnotationKind::Optional && type2.GetKind() != ETypeAnnotationKind::Optional) {
        auto type2Opt = ctx.MakeType<TOptionalExprType>(&type2);
        auto prev = node2;
        node2 = ctx.NewCallable(node2->Pos(), "Just", { std::move(node2) });

        const TTypeAnnotationNode* commonItemType;
        if (SilentInferCommonType(node1, type1, node2, *type2Opt, ctx, commonItemType, flags) != IGraphTransformer::TStatus::Error) {
            commonType = commonItemType;
            return IGraphTransformer::TStatus::Repeat;
        }

        node2 = prev;
    }

    if (IsNull(type1)) {
        if (type2.GetKind() == ETypeAnnotationKind::Optional || type2.GetKind() == ETypeAnnotationKind::Pg) {
            node1 = ctx.NewCallable(node1->Pos(), "Nothing", { ExpandType(node2->Pos(), type2, ctx) });
            commonType = &type2;
            return IGraphTransformer::TStatus::Repeat;
        } else {
            auto type2Opt = ctx.MakeType<TOptionalExprType>(&type2);
            node1 = ctx.NewCallable(node1->Pos(), "Nothing", { ExpandType(node2->Pos(), *type2Opt, ctx) });
            node2 = ctx.NewCallable(node2->Pos(), "Just", { std::move(node2) });
            commonType = type2Opt;
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    if (IsNull(type2)) {
        if (type1.GetKind() == ETypeAnnotationKind::Optional || type1.GetKind() == ETypeAnnotationKind::Pg) {
            node2 = ctx.NewCallable(node2->Pos(), "Nothing", { ExpandType(node1->Pos(), type1, ctx) });
            commonType = &type1;
            return IGraphTransformer::TStatus::Repeat;
        }
        else {
            auto type1Opt = ctx.MakeType<TOptionalExprType>(&type1);
            node2 = ctx.NewCallable(node2->Pos(), "Nothing", { ExpandType(node1->Pos(), *type1Opt, ctx) });
            node1 = ctx.NewCallable(node1->Pos(), "Just", { std::move(node1) });
            commonType = type1Opt;
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    if (type1.GetKind() == ETypeAnnotationKind::List && type2.GetKind() == ETypeAnnotationKind::List ||
        type1.GetKind() == ETypeAnnotationKind::Optional && type2.GetKind() == ETypeAnnotationKind::Optional) {
        const bool isList = type1.GetKind() == ETypeAnnotationKind::List;

        auto item1type = isList ?
            type1.Cast<TListExprType>()->GetItemType() :
            type1.Cast<TOptionalExprType>()->GetItemType();

        auto item2type = isList ?
            type2.Cast<TListExprType>()->GetItemType() :
            type2.Cast<TOptionalExprType>()->GetItemType();

        auto arg1 = ctx.NewArgument(node1->Pos(), "arg1");
        auto arg2 = ctx.NewArgument(node2->Pos(), "arg2");
        auto item1 = arg1;
        auto item2 = arg2;
        const TTypeAnnotationNode* commonItemType;
        if (SilentInferCommonType(item1, *item1type, item2, *item2type, ctx, commonItemType, flags) != IGraphTransformer::TStatus::Error) {
            if (item1 != arg1) {
                node1 = ctx.Builder(node1->Pos())
                    .Callable("OrderedMap")
                        .Add(0, node1)
                        .Add(1, ctx.NewLambda(node1->Pos(), ctx.NewArguments(node1->Pos(), { arg1 }), std::move(item1)))
                    .Seal()
                    .Build();
            }

            if (item2 != arg2) {
                node2 = ctx.Builder(node2->Pos())
                    .Callable("OrderedMap")
                        .Add(0, node2)
                        .Add(1, ctx.NewLambda(node2->Pos(), ctx.NewArguments(node2->Pos(), { arg2 }), std::move(item2)))
                    .Seal()
                    .Build();
            }

            if (isList) {
                commonType = ctx.MakeType<TListExprType>(commonItemType);
            } else {
                commonType = ctx.MakeType<TOptionalExprType>(commonItemType);
            }
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    if (type1.GetKind() == ETypeAnnotationKind::Tuple && type2.GetKind() == ETypeAnnotationKind::Tuple) {
        auto tupleType1 = type1.Cast<TTupleExprType>();
        auto tupleType2 = type2.Cast<TTupleExprType>();
        if (tupleType1->GetSize() == tupleType2->GetSize()) {
            TVector<const TTypeAnnotationNode*> commonItemTypes;
            TExprNode::TListType leftItems;
            TExprNode::TListType rightItems;
            bool hasError = false;
            for (ui32 i = 0; i < tupleType1->GetSize(); ++i) {
                auto item1type = tupleType1->GetItems()[i];
                auto item2type = tupleType2->GetItems()[i];
                auto atom = ctx.NewAtom(TPositionHandle(), ToString(i), TNodeFlags::Default);
                auto arg1 = ctx.NewCallable(node1->Pos(), "Nth", { node1, atom });
                auto arg2 = ctx.NewCallable(node2->Pos(), "Nth", { node2, atom });
                const TTypeAnnotationNode* commonItemType;
                if (SilentInferCommonType(arg1, *item1type, arg2, *item2type, ctx, commonItemType, flags) == IGraphTransformer::TStatus::Error) {
                    hasError = true;
                    break;
                }

                commonItemTypes.push_back(commonItemType);
                leftItems.push_back(arg1);
                rightItems.push_back(arg2);
            }

            if (!hasError) {
                commonType = ctx.MakeType<TTupleExprType>(commonItemTypes);
                node1 = ctx.NewList(node1->Pos(), std::move(leftItems));
                node2 = ctx.NewList(node2->Pos(), std::move(rightItems));
                return IGraphTransformer::TStatus::Repeat;
            }
        }
    }

    if (type1.GetKind() == ETypeAnnotationKind::Struct && type2.GetKind() == ETypeAnnotationKind::Struct) {
        auto structType1 = type1.Cast<TStructExprType>();
        auto structType2 = type2.Cast<TStructExprType>();
        TSet<TStringBuf> allFields;
        for (const auto& x : structType1->GetItems()) {
            allFields.emplace(x->GetName());
        }

        for (const auto& x : structType2->GetItems()) {
            allFields.emplace(x->GetName());
        }

        TVector<const TItemExprType*> commonItemTypes;
        TExprNode::TListType leftItems;
        TExprNode::TListType rightItems;
        bool hasError = false;
        for (const auto& x: allFields) {
            auto pos1 = structType1->FindItem(x);
            auto pos2 = structType2->FindItem(x);
            if (pos1 && pos2) {
                auto member1 = structType1->GetItems()[*pos1];
                auto member2 = structType2->GetItems()[*pos2];
                auto atom = ctx.NewAtom(TPositionHandle(), x);
                auto arg1 = ctx.NewCallable(node1->Pos(), "Member", { node1, atom });
                auto arg2 = ctx.NewCallable(node2->Pos(), "Member", { node2, atom });
                const TTypeAnnotationNode* commonItemType;
                if (SilentInferCommonType(arg1, *member1->GetItemType(), arg2, *member2->GetItemType(), ctx, commonItemType, flags) == IGraphTransformer::TStatus::Error) {
                    hasError = true;
                    break;
                }

                commonItemTypes.push_back(ctx.MakeType<TItemExprType>(x, commonItemType));
                leftItems.push_back(ctx.NewList(node1->Pos(), { atom, arg1 }));
                rightItems.push_back(ctx.NewList(node2->Pos(), { atom, arg2 }));
            } else if (pos1) {
                auto member1 = structType1->GetItems()[*pos1];
                auto commonItemType = member1->GetItemType();
                auto atom = ctx.NewAtom(TPositionHandle(), x);
                TExprNode::TPtr arg1, arg2;
                if (commonItemType->GetKind() != ETypeAnnotationKind::Null) {
                    bool addJust = false;
                    if (commonItemType->GetKind() != ETypeAnnotationKind::Optional) {
                        commonItemType = ctx.MakeType<TOptionalExprType>(commonItemType);
                        addJust = true;
                    }

                    arg1 = ctx.NewCallable(node1->Pos(), "Member", { node1, atom });
                    if (addJust) {
                        arg1 = ctx.NewCallable(node1->Pos(), "Just", { arg1 });
                    }

                    arg2 = ctx.NewCallable(node2->Pos(), "Nothing", { ExpandType(node2->Pos(), *commonItemType, ctx) });
                } else {
                    arg1 = arg2 = ctx.NewCallable(node1->Pos(), "Null", {});
                }

                commonItemTypes.push_back(ctx.MakeType<TItemExprType>(x, commonItemType));
                leftItems.push_back(ctx.NewList(node1->Pos(), { atom, arg1 }));
                rightItems.push_back(ctx.NewList(node2->Pos(), { atom, arg2 }));
            } else if (pos2) {
                auto member2 = structType2->GetItems()[*pos2];
                auto commonItemType = member2->GetItemType();
                auto atom = ctx.NewAtom(TPositionHandle(), x);
                TExprNode::TPtr arg1, arg2;
                if (commonItemType->GetKind() != ETypeAnnotationKind::Null) {
                    bool addJust = false;
                    if (commonItemType->GetKind() != ETypeAnnotationKind::Optional) {
                        commonItemType = ctx.MakeType<TOptionalExprType>(commonItemType);
                        addJust = true;
                    }

                    arg1 = ctx.NewCallable(node1->Pos(), "Nothing", { ExpandType(node1->Pos(), *commonItemType, ctx) });
                    arg2 = ctx.NewCallable(node2->Pos(), "Member", { node2, atom });
                    if (addJust) {
                        arg2 = ctx.NewCallable(node1->Pos(), "Just", { arg2 });
                    }
                } else {
                    arg1 = arg2 = ctx.NewCallable(node2->Pos(), "Null", {});
                }

                commonItemTypes.push_back(ctx.MakeType<TItemExprType>(x, commonItemType));
                leftItems.push_back(ctx.NewList(node1->Pos(), { atom, arg1 }));
                rightItems.push_back(ctx.NewList(node2->Pos(), { atom, arg2 }));
            } else {
                YQL_ENSURE(false, "Unexpected");
            }
        }

        if (!hasError) {
            commonType = ctx.MakeType<TStructExprType>(commonItemTypes);
            node1 = ctx.NewCallable(node1->Pos(), "AsStruct", std::move(leftItems));
            node2 = ctx.NewCallable(node2->Pos(), "AsStruct", std::move(rightItems));
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    if (type1.GetKind() == ETypeAnnotationKind::Dict && type2.GetKind() == ETypeAnnotationKind::Dict) {
        auto key1type = type1.Cast<TDictExprType>()->GetKeyType();
        auto key2type = type2.Cast<TDictExprType>()->GetKeyType();
        auto payload1type = type1.Cast<TDictExprType>()->GetPayloadType();
        auto payload2type = type2.Cast<TDictExprType>()->GetPayloadType();
        if (key1type == key2type) {
            auto arg1 = ctx.NewArgument(node1->Pos(), "arg1");
            auto arg2 = ctx.NewArgument(node2->Pos(), "arg2");
            auto key1 = ctx.NewCallable(node1->Pos(), "Nth", { arg1, ctx.NewAtom(node1->Pos(), "0", TNodeFlags::Default) });
            auto value1 = ctx.NewCallable(node1->Pos(), "Nth", { arg1, ctx.NewAtom(node1->Pos(), "1", TNodeFlags::Default) });
            auto key2 = ctx.NewCallable(node2->Pos(), "Nth", { arg2, ctx.NewAtom(node2->Pos(), "0", TNodeFlags::Default) });
            auto value2 = ctx.NewCallable(node2->Pos(), "Nth", { arg2, ctx.NewAtom(node2->Pos(), "1", TNodeFlags::Default) });

            auto oldValue1 = value1;
            auto oldValue2 = value2;
            const TTypeAnnotationNode* commonPayloadType;
            if (SilentInferCommonType(value1, *payload1type, value2, *payload2type, ctx, commonPayloadType, flags) != IGraphTransformer::TStatus::Error) {
                if (oldValue1 != value1) {
                    auto body1 = ctx.NewList(node1->Pos(), { key1, value1 });
                    auto lambda1 = ctx.NewLambda(node1->Pos(), ctx.NewArguments(node1->Pos(), { arg1 }), std::move(body1));
                    node1 = RebuildDict(node1, lambda1, ctx);
                }

                if (oldValue2 != value2) {
                    auto body2 = ctx.NewList(node2->Pos(), { key2, value2 });
                    auto lambda2 = ctx.NewLambda(node2->Pos(), ctx.NewArguments(node2->Pos(), { arg2 }), std::move(body2));
                    node2 = RebuildDict(node2, lambda2, ctx);
                }

                commonType = ctx.MakeType<TDictExprType>(key1type, commonPayloadType);
                return IGraphTransformer::TStatus::Repeat;
            }
        }
    }


    if (type1.GetKind() == ETypeAnnotationKind::Tagged && type2.GetKind() == ETypeAnnotationKind::Tagged) {
        auto taggedType1 = type1.Cast<TTaggedExprType>();
        auto taggedType2 = type2.Cast<TTaggedExprType>();
        if (taggedType1->GetTag() == taggedType2->GetTag()) {
            auto atom = ctx.NewAtom(node1->Pos(), taggedType1->GetTag());
            const TTypeAnnotationNode* commonBaseType;
            auto arg1 = ctx.NewCallable(node1->Pos(), "Untag", { node1, atom });
            auto arg2 = ctx.NewCallable(node2->Pos(), "Untag", { node2, atom });
            if (SilentInferCommonType(arg1, *taggedType1->GetBaseType(), arg2, *taggedType2->GetBaseType(), ctx, commonBaseType, flags) != IGraphTransformer::TStatus::Error) {
                commonType = ctx.MakeType<TTaggedExprType>(commonBaseType, taggedType1->GetTag());
                node1 = ctx.NewCallable(node1->Pos(), "AsTagged", { arg1, atom });
                node2 = ctx.NewCallable(node2->Pos(), "AsTagged", { arg2, atom });
                return IGraphTransformer::TStatus::Repeat;
            }
        }
    }

    if (type1.GetKind() == ETypeAnnotationKind::Variant && type2.GetKind() == ETypeAnnotationKind::Variant) {
        auto variantType1 = type1.Cast<TVariantExprType>();
        auto variantType2 = type2.Cast<TVariantExprType>();
        if (variantType1->GetUnderlyingType()->GetKind() != variantType2->GetUnderlyingType()->GetKind()) {
            return IGraphTransformer::TStatus::Error;
        }

        THashMap<TString, TExprNode::TPtr> transforms1;
        THashMap<TString, TExprNode::TPtr> transforms2;
        bool changed1 = false;
        bool changed2 = false;
        switch (variantType1->GetUnderlyingType()->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            auto underlying1 = variantType1->GetUnderlyingType()->Cast<TTupleExprType>();
            auto underlying2 = variantType2->GetUnderlyingType()->Cast<TTupleExprType>();

            if (underlying1->GetSize() != underlying2->GetSize()) {
                return IGraphTransformer::TStatus::Error;
            }

            TVector<const TTypeAnnotationNode*> commonItemTypes;
            TVector<TExprNode::TPtr> args1, args2, originalArgs1, originalArgs2;
            for (size_t i = 0; i < underlying1->GetSize(); i++) {
                auto elem1 = underlying1->GetItems()[i];
                auto elem2 = underlying2->GetItems()[i];

                auto arg1 = ctx.NewArgument(node1->Pos(), "item");
                auto arg2 = ctx.NewArgument(node2->Pos(), "item");
                auto originalArg1 = arg1;
                auto originalArg2 = arg2;

                const TTypeAnnotationNode* commonBaseType;
                if (SilentInferCommonType(arg1, *elem1, arg2, *elem2, ctx, commonBaseType, flags) == IGraphTransformer::TStatus::Error) {
                    return IGraphTransformer::TStatus::Error;
                }

                changed1 = changed1 || arg1 != originalArg1;
                changed2 = changed2 || arg2 != originalArg2;

                commonItemTypes.emplace_back(commonBaseType);
                args1.emplace_back(arg1);
                args2.emplace_back(arg2);
                originalArgs1.emplace_back(originalArg1);
                originalArgs2.emplace_back(originalArg2);
            }

            commonType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(commonItemTypes));
            auto commonTypeExpr = ExpandType(node1->Pos(), *commonType, ctx);
            for (size_t i = 0; i < underlying1->GetSize(); i++) {
                auto arg1 = ctx.Builder(node1->Pos())
                    .Callable("Variant")
                        .Add(0, std::move(args1[i]))
                        .Atom(1, ToString(i), TNodeFlags::Default)
                        .Add(2, commonTypeExpr)
                    .Seal()
                    .Build();

                auto lambda1 = ctx.NewLambda(node1->Pos(), ctx.NewArguments(node1->Pos(), {originalArgs1[i]}), std::move(arg1));
                transforms1.emplace(ToString(i), std::move(lambda1));

                auto arg2 = ctx.Builder(node2->Pos())
                    .Callable("Variant")
                        .Add(0, std::move(args2[i]))
                        .Atom(1, ToString(i), TNodeFlags::Default)
                        .Add(2, commonTypeExpr)
                    .Seal()
                    .Build();

                auto lambda2 = ctx.NewLambda(node2->Pos(), ctx.NewArguments(node2->Pos(), {originalArgs2[i]}), std::move(arg2));
                transforms2.emplace(ToString(i), std::move(lambda2));
            }
            break;
        }
        case ETypeAnnotationKind::Struct: {
            auto underlying1 = variantType1->GetUnderlyingType()->Cast<TStructExprType>();
            auto underlying2 = variantType2->GetUnderlyingType()->Cast<TStructExprType>();

            THashSet<TStringBuf> names;
            TVector<const TItemExprType*> commonItemTypes;
            for (const auto& x : underlying1->GetItems()) {
                names.emplace(x->GetName());
            }

            for (const auto& x : underlying2->GetItems()) {
                names.emplace(x->GetName());
            }

            THashMap<TStringBuf, TExprNode::TPtr> args1, args2, originalArgs1, originalArgs2;
            for (const auto& x : names) {
                auto pos1 = underlying1->FindItem(x);
                auto pos2 = underlying2->FindItem(x);
                if (pos1 && !pos2) {
                    commonItemTypes.emplace_back(underlying1->GetItems()[*pos1]);
                    changed2 = true;
                    auto arg1 = ctx.NewArgument(node1->Pos(), "item");
                    args1.emplace(x, arg1);
                    originalArgs1.emplace(x, arg1);
                } else if (!pos1 && pos2) {
                    commonItemTypes.emplace_back(underlying2->GetItems()[*pos2]);
                    changed1 = true;
                    auto arg2 = ctx.NewArgument(node2->Pos(), "item");
                    args2.emplace(x, arg2);
                    originalArgs2.emplace(x, arg2);
                } else {
                    auto elem1 = underlying1->GetItems()[*pos1];
                    auto elem2 = underlying2->GetItems()[*pos2];

                    auto arg1 = ctx.NewArgument(node1->Pos(), "item");
                    auto arg2 = ctx.NewArgument(node2->Pos(), "item");
                    auto originalArg1 = arg1;
                    auto originalArg2 = arg2;

                    const TTypeAnnotationNode* commonBaseType;
                    if (SilentInferCommonType(arg1, *elem1->GetItemType(), arg2, *elem2->GetItemType(), ctx, commonBaseType, flags) == IGraphTransformer::TStatus::Error) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    changed1 = changed1 || arg1 != originalArg1;
                    changed2 = changed2 || arg2 != originalArg2;

                    commonItemTypes.emplace_back(ctx.MakeType<TItemExprType>(x, commonBaseType));
                    args1.emplace(x, arg1);
                    args2.emplace(x, arg2);
                    originalArgs1.emplace(x, originalArg1);
                    originalArgs2.emplace(x, originalArg2);
                }
            }

            commonType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TStructExprType>(commonItemTypes));
            auto commonTypeExpr = ExpandType(node1->Pos(), *commonType, ctx);
            for (const auto& x : names) {
                auto pos1 = underlying1->FindItem(x);
                auto pos2 = underlying2->FindItem(x);
                if (pos1) {
                    auto arg1 = ctx.Builder(node1->Pos())
                        .Callable("Variant")
                            .Add(0, std::move(args1[x]))
                            .Atom(1, x)
                            .Add(2, commonTypeExpr)
                        .Seal()
                        .Build();

                    auto lambda1 = ctx.NewLambda(node1->Pos(), ctx.NewArguments(node1->Pos(), {originalArgs1[x]}), std::move(arg1));
                    transforms1.emplace(x, std::move(lambda1));
                }

                if (pos2) {
                    auto arg2 = ctx.Builder(node2->Pos())
                        .Callable("Variant")
                            .Add(0, std::move(args2[x]))
                            .Atom(1, x)
                            .Add(2, commonTypeExpr)
                        .Seal()
                        .Build();

                    auto lambda2 = ctx.NewLambda(node2->Pos(), ctx.NewArguments(node2->Pos(), {originalArgs2[x]}), std::move(arg2));
                    transforms2.emplace(x, std::move(lambda2));
                }
            }

            break;
        }
        default:
            Y_UNREACHABLE();
        }

        if (changed1) {
            node1 = RebuildVariant(node1, transforms1, ctx);
        }

        if (changed2) {
            node2 = RebuildVariant(node2, transforms2, ctx);
        }

        return changed1 || changed2 ?  IGraphTransformer::TStatus::Repeat : IGraphTransformer::TStatus::Ok;
    }

    return IGraphTransformer::TStatus::Error;
}

IGraphTransformer::TStatus ConvertChildrenToType(const TExprNode::TPtr& input, const TTypeAnnotationNode* targetType, TExprContext& ctx) {
    if (!input->ChildrenSize()) {
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
    for (auto i = 0U; i < input->ChildrenSize(); ++i) {
        const auto child = input->Child(i);
        if (!EnsureComputable(*child, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        status = status.Combine(TryConvertTo(input->ChildRef(i), *targetType, ctx));
        if (status == IGraphTransformer::TStatus::Error)
            break;
    }

    return status;
}

bool IsSqlInCollectionItemsNullable(const NNodes::TCoSqlIn& node) {
    auto collectionType = node.Collection().Ref().GetTypeAnn();
    if (collectionType->GetKind() == ETypeAnnotationKind::Optional) {
        collectionType = collectionType->Cast<TOptionalExprType>()->GetItemType();
    }

    const auto collectionKind = collectionType->GetKind();
    bool result = false;
    switch (collectionKind) {
        case ETypeAnnotationKind::Tuple:
        {
            const auto tupleType = collectionType->Cast<TTupleExprType>();
            result = AnyOf(tupleType->GetItems(), [](const auto& item) { return item->HasOptionalOrNull(); } );
            break;
        }
        case ETypeAnnotationKind::Dict:
            result = collectionType->Cast<TDictExprType>()->GetKeyType()->HasOptionalOrNull();
            break;
        case ETypeAnnotationKind::List:
            result = collectionType->Cast<TListExprType>()->GetItemType()->HasOptionalOrNull();
            break;
        case ETypeAnnotationKind::EmptyDict:
        case ETypeAnnotationKind::EmptyList:
        case ETypeAnnotationKind::Null:
            break;
        default:
            YQL_ENSURE(false, "Unexpected collection type: " << *collectionType);
    }

    return result;
}


ui32 GetNumericDataTypeLevel(EDataSlot dataSlot) {
    if (dataSlot == EDataSlot::Uint8)
        return 0;

    if (dataSlot == EDataSlot::Int8)
        return 1;

    if (dataSlot == EDataSlot::Uint16)
        return 2;

    if (dataSlot == EDataSlot::Int16)
        return 3;

    if (dataSlot == EDataSlot::Uint32)
        return 4;

    if (dataSlot == EDataSlot::Int32)
        return 5;

    if (dataSlot == EDataSlot::Uint64)
        return 6;

    if (dataSlot == EDataSlot::Int64)
        return 7;

    if (dataSlot == EDataSlot::Float)
        return 8;

    if (dataSlot == EDataSlot::Double)
        return 9;

    ythrow yexception() << "Unknown numeric type: " << NKikimr::NUdf::GetDataTypeInfo(dataSlot).Name;
}

EDataSlot GetNumericDataTypeByLevel(ui32 level) {
    switch (level) {
    case 0:
        return EDataSlot::Uint8;
    case 1:
        return EDataSlot::Int8;
    case 2:
        return EDataSlot::Uint16;
    case 3:
        return EDataSlot::Int16;
    case 4:
        return EDataSlot::Uint32;
    case 5:
        return EDataSlot::Int32;
    case 6:
        return EDataSlot::Uint64;
    case 7:
        return EDataSlot::Int64;
    case 8:
        return EDataSlot::Float;
    case 9:
        return EDataSlot::Double;
    default:
        ythrow yexception() << "Unknown numeric level: " << level;
    }
}

ui32 GetDateTypeLevel(EDataSlot dataSlot) {
    switch (dataSlot) {
    case EDataSlot::Date:
        return 0;
    case EDataSlot::Datetime:
        return 1;
    case EDataSlot::Timestamp:
        return 2;
    case EDataSlot::Date32:
        return 4;
    case EDataSlot::Datetime64:
        return 5;
    case EDataSlot::Timestamp64:
        return 6;
    default:
        ythrow yexception() << "Unknown date type: " << NKikimr::NUdf::GetDataTypeInfo(dataSlot).Name;
    }
}

EDataSlot GetDateTypeByLevel(ui32 level) {
    switch (level) {
    case 0:
        return EDataSlot::Date;
    case 1:
        return EDataSlot::Datetime;
    case 2:
        return EDataSlot::Timestamp;
    case 4:
        return EDataSlot::Date32;
    case 5:
        return EDataSlot::Datetime64;
    case 6:
        return EDataSlot::Timestamp64;
    default:
        ythrow yexception() << "Unknown date level: " << level;
    }
}

bool IsPureIsolatedLambdaImpl(const TExprNode& lambdaBody, TNodeSet& visited, TSyncMap* syncList) {
    if (!visited.emplace(&lambdaBody).second) {
        return true;
    }

    if (lambdaBody.IsCallable("TypeOf")) {
        return true;
    }

    if (syncList) {
        if (auto right = TMaybeNode<TCoRight>(&lambdaBody)) {
            if (auto cons = right.Cast().Input().Maybe<TCoCons>()) {
                syncList->emplace(cons.Cast().World().Ptr(), syncList->size());
                return IsPureIsolatedLambdaImpl(cons.Cast().Input().Ref(), visited, syncList);
            }

            if (right.Cast().Input().Ref().IsCallable("PgReadTable!")) {
                syncList->emplace(right.Cast().Input().Ref().HeadPtr(), syncList->size());
                return true;
            }

            return false;
        }
    }

    if (!lambdaBody.GetTypeAnn()->IsComposable()) {
        return false;
    }

    for (auto& child : lambdaBody.Children()) {
        if (!IsPureIsolatedLambdaImpl(*child, visited, syncList)) {
            return false;
        }
    }

    return true;
}

bool IsPureIsolatedLambda(const TExprNode& lambdaBody, TSyncMap* syncList) {
    TNodeSet visited;
    return IsPureIsolatedLambdaImpl(lambdaBody, visited, syncList);
}

TString GetIntegralAtomValue(ui64 value, bool hasSign) {
    return (hasSign) ? "-" + ToString(value) : ToString(value);
}

bool AllowIntegralConversion(TCoIntegralCtor node, bool negate, EDataSlot toType, TString* atomValue) {
    bool hasSign;
    bool isSigned;
    ui64 value;
    ExtractIntegralValue(node.Ref(), negate, hasSign, isSigned, value);

    bool allow = false;

    if (toType == EDataSlot::Uint8) {
        allow = !hasSign && value <= Max<ui8>();
    }
    else if (toType == EDataSlot::Uint16) {
        allow = !hasSign && value <= Max<ui16>();
    }
    else if (toType == EDataSlot::Uint32) {
        allow = !hasSign && value <= Max<ui32>();
    }
    else if (toType == EDataSlot::Int8) {
        allow = !hasSign && value <= (ui64)Max<i8>() || hasSign && value <= (ui64)Max<i8>() + 1;
    }
    else if (toType == EDataSlot::Int16) {
        allow = !hasSign && value <= (ui64)Max<i16>() || hasSign && value <= (ui64)Max<i16>() + 1;
    }
    else if (toType == EDataSlot::Int32) {
        allow = !hasSign && value <= (ui64)Max<i32>() || hasSign && value <= (ui64)Max<i32>() + 1;
    }
    else if (toType == EDataSlot::Uint64) {
        allow = !hasSign;
    }
    else if (toType == EDataSlot::Int64) {
        allow = !hasSign && value <= (ui64)Max<i64>() || hasSign && value <= (ui64)Max<i64>() + 1;
    }
    else if (toType == EDataSlot::Float) {
        allow = value <= Max<ui32>();
    }

    if (atomValue) {
        *atomValue = GetIntegralAtomValue(value, hasSign && isSigned);
    }

    return allow;
}

void ExtractIntegralValue(const TExprNode& constructor, bool negate, bool& hasSign, bool& isSigned, ui64& value) {
    hasSign = false;
    isSigned = false;
    value = 0;
    const auto& atom = constructor.Head();
    if (constructor.Content().StartsWith("Int")) {
        isSigned = true;
        if (atom.Flags() & TNodeFlags::BinaryContent) {
            if (constructor.Content().EndsWith("8")) {
                auto raw = i8(atom.Content().front());
                if (raw < 0) {
                    hasSign = true;
                    value = -raw;
                }
                else {
                    value = raw;
                }
            } else if (constructor.Content().EndsWith("16")) {
                auto raw = *reinterpret_cast<const i16*>(atom.Content().data());
                if (raw < 0) {
                    hasSign = true;
                    value = -raw;
                }
                else {
                    value = raw;
                }
            } else if (constructor.Content().EndsWith("32")) {
                auto raw = *reinterpret_cast<const i32*>(atom.Content().data());
                if (raw < 0) {
                    hasSign = true;
                    value = -raw;
                }
                else {
                    value = raw;
                }
            } else if (constructor.Content().EndsWith("64")) {
                auto raw = *reinterpret_cast<const i64*>(atom.Content().data());
                if (raw < 0) {
                    hasSign = true;
                    value = -raw;
                }
                else {
                    value = raw;
                }
            }
        }
        else {
            hasSign = atom.Content().StartsWith('-');
            auto strValue = hasSign
                ? atom.Content().Tail(1)
                : atom.Content();

            value = ::FromString<ui64>(strValue);
        }
    }
    else {
        if (atom.Flags() & TNodeFlags::BinaryContent) {
            memcpy(&value, atom.Content().data(), atom.Content().size());
        }
        else {
            value = FromString<ui64>(atom.Content());
        }
    }

    if (negate) {
        if (isSigned) {
            hasSign = !hasSign;
        }
        else {
            value = (~value + 1);
            if (constructor.IsCallable("Uint8")) {
                value = value & 0xFFu;
            }
            else if (constructor.IsCallable("Uint16")) {
                value = value & 0xFFFFu;
            }
            else if (constructor.IsCallable("Uint32")) {
                value = value & 0xFFFFFFFFu;
            }
        }
    }
}

TMaybe<ui32> GetDataFixedSize(const TTypeAnnotationNode* typeAnnotation) {
    if (!typeAnnotation) {
        return Nothing();
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Optional) {
        auto childSize = GetDataFixedSize(typeAnnotation->Cast<TOptionalExprType>()->GetItemType());
        if (childSize) {
            return 1 + *childSize;
        }

        return Nothing();
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Data) {
        const auto dataSlot = typeAnnotation->Cast<TDataExprType>()->GetSlot();
        if (EDataSlot::Bool == dataSlot || EDataSlot::Uint8 == dataSlot || EDataSlot::Int8 == dataSlot) {
            return 1;
        }

        if (EDataSlot::Date == dataSlot || EDataSlot::Uint16 == dataSlot || EDataSlot::Int16 == dataSlot) {
            return 2;
        }

        if (EDataSlot::Datetime == dataSlot || EDataSlot::Date32 == dataSlot
            || EDataSlot::Uint32 == dataSlot || EDataSlot::Int32 == dataSlot
            || EDataSlot::Float == dataSlot) {
            return 4;
        }

        if (EDataSlot::Timestamp == dataSlot || EDataSlot::Uint64 == dataSlot || EDataSlot::Int64 == dataSlot
            || EDataSlot::Datetime64 == dataSlot || EDataSlot::Timestamp64 == dataSlot || EDataSlot::Interval64 == dataSlot
            || EDataSlot::Double == dataSlot || EDataSlot::Interval == dataSlot) {
            return 8;
        }

        return Nothing();
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Tuple) {
        ui32 totalSize = 0;
        for (auto& child : typeAnnotation->Cast<TTupleExprType>()->GetItems()) {
            auto childSize = GetDataFixedSize(child);
            if (!childSize) {
                return Nothing();
            }

            totalSize += *childSize;
        }

        return totalSize;
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Struct) {
        ui32 totalSize = 0;
        for (auto& child : typeAnnotation->Cast<TStructExprType>()->GetItems()) {
            auto childSize = GetDataFixedSize(child->GetItemType());
            if (!childSize) {
                return Nothing();
            }

            totalSize += *childSize;
        }

        return totalSize;
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Void || typeAnnotation->GetKind() == ETypeAnnotationKind::Null) {
        return 0;
    }

    if (typeAnnotation->GetKind() == ETypeAnnotationKind::Variant) {
        ui32 maxSize = 0;
        auto underlyingType = typeAnnotation->Cast<TVariantExprType>()->GetUnderlyingType();
        if (underlyingType->GetKind() == ETypeAnnotationKind::Struct) {
            for (auto& child : underlyingType->Cast<TStructExprType>()->GetItems()) {
                auto childSize = GetDataFixedSize(child->GetItemType());
                if (!childSize) {
                    return Nothing();
                }

                maxSize = Max(maxSize, *childSize);
            }
        } else {
            for (auto& child : underlyingType->Cast<TTupleExprType>()->GetItems()) {
                auto childSize = GetDataFixedSize(child);
                if (!childSize) {
                    return Nothing();
                }

                maxSize = Max(maxSize, *childSize);
            }
        }

        return 1 + maxSize;
    }

    return Nothing();
}

bool IsFixedSizeData(const TTypeAnnotationNode* typeAnnotation) {
    return GetDataFixedSize(typeAnnotation).Defined();
}

bool IsDataTypeString(EDataSlot dataSlot) {
    return NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::StringType;
}

bool EnsureComparableDataType(TPositionHandle position, EDataSlot dataSlot, TExprContext& ctx) {
    if (0 == (NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::CanCompare)) {
        TStringBuilder sb;
        bool isYson = dataSlot == EDataSlot::Yson;
        bool isJson = dataSlot == EDataSlot::Json;
        if (isYson || isJson) {
            sb << NKikimr::NUdf::GetDataTypeInfo(dataSlot).Name << " is not a comparable data type." << Endl
               << "You can use ToBytes(...) to convert it to it's byte representation, which can be compared fast, but can lead to unexpected results in some cases," << Endl
               << "for example same dictionaries with different order of key/value pairs will be considered unequal or different ways of serialization of same data will also start to matter." << Endl
               << "Also there's a Yson::Equals(..., ...) function that can correctly determine if arbitrary Yson or Json values are equal, "
               << "but it's quite slow and CPU-intensive because it needs to parse data and build in-memory representation for each side.";
        } else {
            sb << "Expected comparable type, but got: " << NKikimr::NUdf::GetDataTypeInfo(dataSlot).Name;
        }
        ctx.AddError(TIssue(ctx.GetPosition(position), sb));
        return false;
    }

    return true;
}

bool EnsureEquatableDataType(TPositionHandle position, EDataSlot dataSlot, TExprContext& ctx) {
    if (0 == (NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::CanEquate)) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected equatable type, but got: " << NKikimr::NUdf::GetDataTypeInfo(dataSlot).Name));
        return false;
    }

    return true;
}

bool EnsureHashableDataType(TPositionHandle position, EDataSlot dataSlot, TExprContext& ctx) {
    if (0 == (NUdf::GetDataTypeInfo(dataSlot).Features & NUdf::CanHash)) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected hashable type, but got: " << NKikimr::NUdf::GetDataTypeInfo(dataSlot).Name));
        return false;
    }

    return true;
}

TMaybe<TIssue> NormalizeName(TPosition position, TString& name) {
    const ui32 inputLength = name.length();
    ui32 startCharPos = 0;
    ui32 totalSkipped = 0;
    bool atStart = true;
    bool justSkippedUnderscore = false;
    for (ui32 i = 0; i < inputLength; ++i) {
        const char c = name.at(i);
        if (c == '_') {
            if (!atStart) {
                if (justSkippedUnderscore) {
                    return TIssue(position, TStringBuilder() << "\"" << name << "\" looks weird, has multiple consecutive underscores");
                }
                justSkippedUnderscore = true;
                ++totalSkipped;
                continue;
            } else {
                ++startCharPos;
            }
        }
        else {
            atStart = false;
            justSkippedUnderscore = false;
        }
    }

    if (totalSkipped >= 5) {
        return TIssue(position, TStringBuilder() << "\"" << name << "\" looks weird, has multiple consecutive underscores");
    }

    ui32 outPos = startCharPos;
    for (ui32 i = startCharPos; i < inputLength; i++) {
        const char c = name.at(i);
        if (c == '_') {
            continue;
        } else {
            name[outPos] = AsciiToLower(c);
            ++outPos;
        }
    }

    name.resize(outPos);
    Y_ABORT_UNLESS(inputLength - outPos == totalSkipped);

    return Nothing();
}

TString NormalizeName(const TStringBuf& name) {
    TString result(name);
    TMaybe<TIssue> error = NormalizeName(TPosition(), result);
    YQL_ENSURE(error.Empty(), "" << error->GetMessage());
    return result;
}

bool HasError(const TTypeAnnotationNode* type, TExprContext& ctx) {
    TIssue errIssue;
    if (HasError(type, errIssue)) {
        ctx.AddError(errIssue);
        return true;
    }
    return false;
}

bool HasError(const TTypeAnnotationNode* type, TIssue& errIssue) {
    errIssue = {};

    if (!type) {
        return false;
    }

    if (type->GetKind() == ETypeAnnotationKind::Error) {
        errIssue = type->Cast<TErrorExprType>()->GetError();
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::Type) {
        return HasError(type->Cast<TTypeExprType>()->GetType(), errIssue);
    }

    return false;
}

bool IsNull(const TExprNode& node) {
    return node.GetTypeAnn() && IsNull(*node.GetTypeAnn());
}

bool IsNull(const TTypeAnnotationNode& type) {
    return type.GetKind() == ETypeAnnotationKind::Null;
}

bool IsEmptyList(const TExprNode& node) {
    return node.GetTypeAnn() && IsEmptyList(*node.GetTypeAnn());
}

bool IsEmptyList(const TTypeAnnotationNode& type) {
    return type.GetKind() == ETypeAnnotationKind::EmptyList;
}

bool IsFlowOrStream(const TTypeAnnotationNode& type) {
    const auto kind = type.GetKind();
    return kind == ETypeAnnotationKind::Stream || kind == ETypeAnnotationKind::Flow;
}

bool IsFlowOrStream(const TExprNode& node) {
    return IsFlowOrStream(*node.GetTypeAnn());
}

bool IsBoolLike(const TTypeAnnotationNode& type) {
    if (IsNull(type)) {
        return true;
    }
    const auto itemType = RemoveOptionalType(&type);
    return (itemType->GetKind() == ETypeAnnotationKind::Data) and (itemType->Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool);
}

bool IsBoolLike(const TExprNode& node) {
    return node.GetTypeAnn() && IsBoolLike(*node.GetTypeAnn());
}

namespace {

using TIndentPrinter = std::function<void(TStringBuilder& res, size_t)>;
void PrintTypeDiff(TStringBuilder& res, size_t level, const TIndentPrinter& indent, const TTypeAnnotationNode& left, const TTypeAnnotationNode& right);

void PrintStructDiff(TStringBuilder& res, size_t level, const TIndentPrinter& indent, const TStructExprType& left, const TStructExprType& right) {
    THashMap<TStringBuf, const TItemExprType*> rightItems;
    for (auto item: right.GetItems()) {
        rightItems.insert({item->GetName(), item});
    }
    bool diff = false;
    for (auto item: left.GetItems()) {
        if (auto rightItem = rightItems.Value(item->GetName(), nullptr)) {
            if (!IsSameAnnotation(*item, *rightItem)) {
                indent(res, level);
                res << item->GetName() << ':';
                PrintTypeDiff(res, level, indent, *item->GetItemType(), *rightItem->GetItemType());
                res << ',';
                diff = true;
            }
            rightItems.erase(item->GetName());
        } else {
            diff = true;
            indent(res, level);
            res << '-' << item->GetName() << ':' << *item->GetItemType() << ',';
        }
    }
    for (auto& item: rightItems) {
        diff = true;
        indent(res, level);
        res << '+' << item.first << ':' << *item.second->GetItemType() << ',';
    }
    if (diff) {
        res.pop_back(); // remove trailing comma
    } else {
        indent(res, level);
        res << "no diff";
    }
}

void PrintTupleDiff(TStringBuilder& res, size_t level, const TIndentPrinter& indent, const TTupleExprType& left, const TTupleExprType& right) {
    const size_t minSize = Min(left.GetSize(), right.GetSize());
    bool diff = false;
    for (size_t i = 0; i < minSize; ++i) {
        if (!IsSameAnnotation(*left.GetItems()[i], *right.GetItems()[i])) {
            indent(res, level);
            res << i << ':';
            PrintTypeDiff(res, level, indent, *left.GetItems()[i], *right.GetItems()[i]);
            res << ',';
            diff = true;
        }
    }
    if (left.GetSize() > minSize) {
        for (size_t i = minSize; i < left.GetSize(); ++i) {
            indent(res, level);
            res << '-' << *left.GetItems()[i] << ',';
        }
        diff = true;
    }
    if (right.GetSize() > minSize) {
        for (size_t i = minSize; i < right.GetSize(); ++i) {
            indent(res, level);
            res << '+' << *right.GetItems()[i] << ',';
        }
        diff = true;
    }
    if (diff) {
        res.pop_back();
    } else {
        indent(res, level);
        res << "no diff";
    }
}

static void PrintTypeDiff(TStringBuilder& res, size_t level, const TIndentPrinter& indent, const TTypeAnnotationNode& left, const TTypeAnnotationNode& right) {
    if (&left == &right) {
        res << "no diff";
        return;
    }
    if (left.GetKind() == right.GetKind()) {
        switch (left.GetKind()) {
        case ETypeAnnotationKind::List:
        case ETypeAnnotationKind::Optional:
        case ETypeAnnotationKind::Stream:
        case ETypeAnnotationKind::Flow:
            res << left.GetKind() << '<';
            indent(res, level + 1);
            PrintTypeDiff(res, level + 1, indent, GetSeqItemType(left), GetSeqItemType(right));
            indent(res, level);
            res << '>';
            break;
        case ETypeAnnotationKind::Struct:
            res << left.GetKind() << '<';
            PrintStructDiff(res, level + 1, indent, *left.Cast<TStructExprType>(), *right.Cast<TStructExprType>());
            indent(res, level);
            res << '>';
            break;
        case ETypeAnnotationKind::Variant:
            res << left.GetKind() << '<';
            if (left.Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == right.Cast<TVariantExprType>()->GetUnderlyingType()->GetKind()) {
                if (left.Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Struct) {
                    PrintStructDiff(res, level + 1, indent, *left.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TStructExprType>(), *right.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TStructExprType>());
                } else {
                    YQL_ENSURE(left.Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple);
                    PrintTupleDiff(res, level + 1, indent, *left.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>(), *right.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>());
                }
            } else {
                res << left.Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() << "!=" << right.Cast<TVariantExprType>()->GetUnderlyingType()->GetKind();
            }
            indent(res, level);
            res << '>';
            break;
        case ETypeAnnotationKind::Tagged:
            res << left.GetKind() << "<\"" << left.Cast<TTaggedExprType>()->GetTag() << '"';
            if (left.Cast<TTaggedExprType>()->GetTag() != right.Cast<TTaggedExprType>()->GetTag()) {
                res << "!=\"" << right.Cast<TTaggedExprType>()->GetTag() << '"';
            }
            res << ',';
            PrintTypeDiff(res, level + 1, indent, *left.Cast<TTaggedExprType>()->GetBaseType(), *right.Cast<TTaggedExprType>()->GetBaseType());
            indent(res, level);
            res << '>';
            break;
        case ETypeAnnotationKind::Dict: {
            res << left.GetKind() << '<';
            bool keyDiff = false;
            if (!IsSameAnnotation(*left.Cast<TDictExprType>()->GetKeyType(), *right.Cast<TDictExprType>()->GetKeyType())) {
                res << "key:";
                PrintTypeDiff(res, level + 1, indent, *left.Cast<TDictExprType>()->GetKeyType(), *right.Cast<TDictExprType>()->GetKeyType());
                keyDiff = true;
            }
            if (!IsSameAnnotation(*left.Cast<TDictExprType>()->GetPayloadType(), *right.Cast<TDictExprType>()->GetPayloadType())) {
                if (keyDiff) {
                    res << ',';
                }
                res << "payload:";
                PrintTypeDiff(res, level + 1, indent, *left.Cast<TDictExprType>()->GetPayloadType(), *right.Cast<TDictExprType>()->GetPayloadType());
            }
            indent(res, level);
            res << '>';
            break;
        }
        case ETypeAnnotationKind::Tuple:
            res << left.GetKind() << '<';
            PrintTupleDiff(res, level + 1, indent, *left.Cast<TTupleExprType>(), *right.Cast<TTupleExprType>());
            indent(res, level);
            res << '>';
            break;
        default:
            res << left << "!=" << right;
        }
    } else {
        res << left << "!=" << right;
    }
}

}

TString GetTypeDiff(const TTypeAnnotationNode& left, const TTypeAnnotationNode& right) {
    TStringBuilder res;
    PrintTypeDiff(res, 0, [](TStringBuilder&, size_t) {}, left, right);
    return res;
}

TString GetTypePrettyDiff(const TTypeAnnotationNode& left, const TTypeAnnotationNode& right) {
    TStringBuilder res;
    PrintTypeDiff(res, 0, [](TStringBuilder& res, size_t level) {
        res << '\n';
        for (size_t i = 0; i < level; ++i) {
            res << ' ';
        }
    }, left, right);
    return res;
}

TExprNode::TPtr ExpandTypeNoCache(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    switch (type.GetKind()) {
    case ETypeAnnotationKind::Unit:
        return ctx.NewCallable(position, "UnitType", {});
    case ETypeAnnotationKind::EmptyList:
        return ctx.NewCallable(position, "EmptyListType", {});
    case ETypeAnnotationKind::EmptyDict:
        return ctx.NewCallable(position, "EmptyDictType", {});
    case ETypeAnnotationKind::Multi:
    {
        TExprNode::TListType tupleItems;
        for (auto& child : type.Cast<TMultiExprType>()->GetItems()) {
            tupleItems.push_back(ExpandType(position, *child, ctx));
        }

        auto ret = ctx.NewCallable(position, "MultiType", std::move(tupleItems));
        return ret;
    }
    case ETypeAnnotationKind::Tuple:
    {
        TExprNode::TListType tupleItems;
        for (auto& child : type.Cast<TTupleExprType>()->GetItems()) {
            tupleItems.push_back(ExpandType(position, *child, ctx));
        }

        auto ret = ctx.NewCallable(position, "TupleType", std::move(tupleItems));
        return ret;
    }

    case ETypeAnnotationKind::Struct:
    {
        TExprNode::TListType structItems;
        for (auto& child : type.Cast<TStructExprType>()->GetItems()) {
            structItems.push_back(
                ctx.NewList(position, {
                    ctx.NewAtom(position, child->GetName()),
                    ExpandType(position, *child->GetItemType(), ctx)
                }));
        }

        auto ret = ctx.NewCallable(position, "StructType", std::move(structItems));
        return ret;
    }

    case ETypeAnnotationKind::List:
    {
        auto ret = ctx.NewCallable(position, "ListType",
            {ExpandType(position, *type.Cast<TListExprType>()->GetItemType(), ctx)});
        return ret;
    }

    case ETypeAnnotationKind::Data:
    {
        const auto data = type.Cast<TDataExprType>();
        if (const auto params = dynamic_cast<const TDataExprParamsType*>(data)) {
            return ctx.NewCallable(position, "DataType", {
                ctx.NewAtom(position, params->GetName(), TNodeFlags::Default),
                ctx.NewAtom(position, params->GetParamOne(), TNodeFlags::Default),
                ctx.NewAtom(position, params->GetParamTwo(), TNodeFlags::Default)
            });
        } else {
            return ctx.NewCallable(position, "DataType", {ctx.NewAtom(position, data->GetName(), TNodeFlags::Default)});
        }
    }

    case ETypeAnnotationKind::Pg:
    {
        const auto pgType = type.Cast<TPgExprType>();
        return ctx.NewCallable(position, "PgType", { ctx.NewAtom(position, pgType->GetName(), TNodeFlags::Default) });
    }

    case ETypeAnnotationKind::Optional:
    {
        auto ret = ctx.NewCallable(position, "OptionalType",
            {ExpandType(position, *type.Cast<TOptionalExprType>()->GetItemType(), ctx)});
        return ret;
    }

    case ETypeAnnotationKind::Generic:
    case ETypeAnnotationKind::Type:
    {
        return ctx.NewCallable(position, "GenericType", {});
    }

    case ETypeAnnotationKind::Dict:
    {
        auto dictType = type.Cast<TDictExprType>();
        auto ret = ctx.NewCallable(position, "DictType", {
            ExpandType(position, *dictType->GetKeyType(), ctx),
            ExpandType(position, *dictType->GetPayloadType(), ctx)
        });
        return ret;
    }

    case ETypeAnnotationKind::Void:
        return ctx.NewCallable(position, "VoidType", {});

    case ETypeAnnotationKind::Null:
        return ctx.NewCallable(position, "NullType", {});

    case ETypeAnnotationKind::Callable:
    {
        auto callableType = type.Cast<TCallableExprType>();

        TExprNode::TListType callableArgs;
        TExprNode::TListType mainSettings;
        if (callableType->GetOptionalArgumentsCount() != 0 || !callableType->GetPayload().empty()) {
            mainSettings.push_back(ctx.NewAtom(position, ToString(callableType->GetOptionalArgumentsCount())));
        }

        if (!callableType->GetPayload().empty()) {
            mainSettings.push_back(ctx.NewAtom(position, callableType->GetPayload()));
        }

        callableArgs.push_back(ctx.NewList(position, std::move(mainSettings)));

        TExprNode::TListType retSettings;
        retSettings.push_back(ExpandType(position, *callableType->GetReturnType(), ctx));
        callableArgs.push_back(ctx.NewList(position, std::move(retSettings)));

        for (const auto& child : callableType->GetArguments()) {
            TExprNode::TListType argSettings;
            argSettings.push_back(ExpandType(position, *child.Type, ctx));
            if (!child.Name.empty() || child.Flags != 0) {
                argSettings.push_back(ctx.NewAtom(position, child.Name));
            }

            if (child.Flags != 0) {
                argSettings.push_back(ctx.NewAtom(position, ToString(child.Flags)));
            }

            callableArgs.push_back(ctx.NewList(position, std::move(argSettings)));
        }

        auto ret = ctx.NewCallable(position, "CallableType", std::move(callableArgs));
        return ret;
    }

    case ETypeAnnotationKind::Resource:
        return ctx.NewCallable(position, "ResourceType",
            {ctx.NewAtom(position, type.Cast<TResourceExprType>()->GetTag())});

    case ETypeAnnotationKind::Error: {
        auto err = type.Cast<TErrorExprType>()->GetError();
        return ctx.NewCallable(position, "ErrorType", {
            ctx.NewAtom(position, ToString(err.Position.Row)),
            ctx.NewAtom(position, ToString(err.Position.Column)),
            ctx.NewAtom(position, err.Position.File),
            ctx.NewAtom(position, err.GetMessage())
        });
    }

    case ETypeAnnotationKind::Variant:
    {
        auto ret = ctx.NewCallable(position, "VariantType",
        { ExpandType(position, *type.Cast<TVariantExprType>()->GetUnderlyingType(), ctx) });
        return ret;
    }

    case ETypeAnnotationKind::Stream:
    {
        auto ret = ctx.NewCallable(position, "StreamType",
            {ExpandType(position, *type.Cast<TStreamExprType>()->GetItemType(), ctx)});
        return ret;
    }

    case ETypeAnnotationKind::Flow:
    {
        auto ret = ctx.NewCallable(position, "FlowType",
            {ExpandType(position, *type.Cast<TFlowExprType>()->GetItemType(), ctx)});
        return ret;
    }

    case ETypeAnnotationKind::Tagged:
    {
        auto taggedType = type.Cast<TTaggedExprType>();
        auto ret = ctx.NewCallable(position, "TaggedType",
            { ExpandType(position, *taggedType->GetBaseType(), ctx),
            ctx.NewAtom(position, taggedType->GetTag()) });
        return ret;
    }

    case ETypeAnnotationKind::Block:
    {
        auto ret = ctx.NewCallable(position, "BlockType",
            { ExpandType(position, *type.Cast<TBlockExprType>()->GetItemType(), ctx) });
        return ret;
    }

    case ETypeAnnotationKind::Scalar:
    {
        auto ret = ctx.NewCallable(position, "ScalarType",
            { ExpandType(position, *type.Cast<TScalarExprType>()->GetItemType(), ctx) });
        return ret;
    }

    default:
        YQL_ENSURE(false, "Unsupported kind: " << (ui32)type.GetKind());
    }
}

TExprNode::TPtr ExpandType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    TExprNode::TPtr& ret = ctx.TypeAsNodeCache[&type];
    if (!ret) {
        ret = ExpandTypeNoCache(position, type, ctx);
    }
    return ret;
}

bool IsSystemMember(const TStringBuf& memberName) {
    return memberName.StartsWith(TStringBuf("_yql_"));
}

template<bool Deduplicte, ui8 OrListsOfAtomsDepth>
IGraphTransformer::TStatus NormalizeTupleOfAtoms(const TExprNode::TPtr& input, ui32 index, TExprNode::TPtr& output, TExprContext& ctx)
{
    auto children = input->Child(index)->ChildrenList();
    bool needRestart = false;

    if constexpr (OrListsOfAtomsDepth == 2U) {
        if (!EnsureTuple(*input->Child(index), ctx))
            return IGraphTransformer::TStatus::Error;

        for (auto i = 0U; i < children.size(); ++i) {
            if (const auto item = input->Child(index)->Child(i); item->IsList()) {
                if (1U == item->ChildrenSize() && item->Head().IsAtom()) {
                    needRestart = true;
                    children[i] = item->HeadPtr();
                } else if (const auto status = NormalizeTupleOfAtoms<Deduplicte, 1U>(input->ChildPtr(index), i, children[i], ctx); status == IGraphTransformer::TStatus::Error)
                    return status;
                else
                    needRestart = needRestart || (status == IGraphTransformer::TStatus::Repeat);
            } else if (!EnsureAtom(*item, ctx))
                return IGraphTransformer::TStatus::Error;
        }
    } else if constexpr (OrListsOfAtomsDepth == 1U) {
        if (!EnsureTuple(*input->Child(index), ctx))
            return IGraphTransformer::TStatus::Error;

        for (auto i = 0U; i < children.size(); ++i) {
            if (const auto item = input->Child(index)->Child(i); item->IsList()) {
                if (1U == item->ChildrenSize() && item->Head().IsAtom()) {
                    needRestart = true;
                    children[i] = item->HeadPtr();
                } else if (!EnsureTupleOfAtoms(*item, ctx))
                    return IGraphTransformer::TStatus::Error;
            } else if (!EnsureAtom(*item, ctx))
                return IGraphTransformer::TStatus::Error;
        }
    } else if (!EnsureTupleOfAtoms(*input->Child(index), ctx))
        return IGraphTransformer::TStatus::Error;

    const auto getKey = [](const TExprNode::TPtr& node) {
        if constexpr (OrListsOfAtomsDepth == 2U) {
            using TItemType = TSmallVec<std::string_view>;
            using TKeyType = TSmallVec<TItemType>;

            if (node->IsAtom())
                return TKeyType(1U, TItemType(1U, node->Content()));

            TKeyType result(node->ChildrenSize());
            std::transform(node->Children().cbegin(), node->Children().cend(), result.begin(), [](const TExprNode::TPtr& item) {
                if (item->IsAtom())
                    return TItemType(1U, item->Content());

                TItemType part(item->ChildrenSize());
                std::transform(item->Children().cbegin(), item->Children().cend(), part.begin(), [](const TExprNode::TPtr& atom) { return atom->Content(); });
                return part;
            });
            return result;
        } else if constexpr (OrListsOfAtomsDepth == 1U) {
            using TKeyType = TSmallVec<std::string_view>;
            if (node->IsAtom())
                return TKeyType(1U, node->Content());

            TKeyType result(node->ChildrenSize());
            std::transform(node->Children().cbegin(), node->Children().cend(), result.begin(), [](const TExprNode::TPtr& atom) { return atom->Content(); });
            return result;
        } else
            return node->Content();
    };

    const auto cmp = [&getKey](const TExprNode::TPtr& a, const TExprNode::TPtr& b) { return getKey(a) < getKey(b); };
    if (std::is_sorted(children.cbegin(), children.cend(), cmp)) {
        if constexpr (Deduplicte) {
            if (const auto dups = UniqueBy(children.begin(), children.end(), getKey); children.cend() != dups) {
                needRestart = true;
                children.erase(dups, children.cend());
            }
        }
    } else {
        needRestart = true;
        if constexpr (Deduplicte) {
            SortUniqueBy(children, getKey);
        } else {
            SortBy(children, getKey);
        }
    }

    if (needRestart) {
        output = ctx.ChangeChild(*input, index, ctx.NewList(input->Child(index)->Pos(), std::move(children)));
        return IGraphTransformer::TStatus::Repeat;
    }

    return IGraphTransformer::TStatus::Ok;
}

template IGraphTransformer::TStatus NormalizeTupleOfAtoms<true, 2U>(const TExprNode::TPtr& input, ui32 index, TExprNode::TPtr& output, TExprContext& ctx);
template IGraphTransformer::TStatus NormalizeTupleOfAtoms<true, 1U>(const TExprNode::TPtr& input, ui32 index, TExprNode::TPtr& output, TExprContext& ctx);
template IGraphTransformer::TStatus NormalizeTupleOfAtoms<true, 0U>(const TExprNode::TPtr& input, ui32 index, TExprNode::TPtr& output, TExprContext& ctx);
template IGraphTransformer::TStatus NormalizeTupleOfAtoms<false, 0U>(const TExprNode::TPtr& input, ui32 index, TExprNode::TPtr& output, TExprContext& ctx);

IGraphTransformer::TStatus NormalizeKeyValueTuples(const TExprNode::TPtr& input, ui32 startIndex, TExprNode::TPtr& output,
    TExprContext &ctx, bool deduplicate)
{
    YQL_ENSURE(input->IsCallable() || input->IsList());
    YQL_ENSURE(startIndex <= input->ChildrenSize());

    auto children = input->ChildrenList();
    auto begin = children.begin() + startIndex;
    auto end = children.end();

    for (auto item = begin; item != end; ++item) {
        auto node = *item;
        if (!EnsureTupleMinSize(*node, 1, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureAtom(*node->Child(0), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    bool needRestart = false;
    auto getKey = [](const auto& node) { return node->Child(0)->Content(); };
    auto compare = [&getKey](const auto& a, const auto& b) { return getKey(a) < getKey(b); };
    if (!IsSorted(begin, end, compare)) {
        StableSort(begin, end, compare);
        needRestart = true;
    }

    if (deduplicate) {
        auto dups = UniqueBy(begin, end, getKey);
        if (dups != end) {
            needRestart = true;
            children.erase(dups, end);
        }
    }

    if (needRestart) {
        output = input->IsCallable() ? ctx.NewCallable(input->Pos(), input->Content(), std::move(children)) :
                                       ctx.NewList(input->Pos(), std::move(children));
        return IGraphTransformer::TStatus::Repeat;
    }

    return IGraphTransformer::TStatus::Ok;
}

std::optional<ui32> GetFieldPosition(const TMultiExprType& multiType, const TStringBuf& field) {
    if (ui32 pos; TryFromString(field, pos) && pos < multiType.GetSize())
        return {pos};
    return std::nullopt;
}

std::optional<ui32> GetFieldPosition(const TTupleExprType& tupleType, const TStringBuf& field) {
    if (ui32 pos; TryFromString(field, pos) && pos < tupleType.GetSize())
        return {pos};
    return std::nullopt;
}

std::optional<ui32> GetFieldPosition(const TStructExprType& structType, const TStringBuf& field) {
    if (const auto find = structType.FindItem(field))
        return {*find};
    return std::nullopt;
}

bool ExtractPgType(const TTypeAnnotationNode* type, ui32& pgType, bool& convertToPg, TPositionHandle pos, TExprContext& ctx) {
    pgType = 0;
    convertToPg = false;
    if (!type) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "Expected PG type, but got lambda"));
        return false;
    }

    if (type->GetKind() == ETypeAnnotationKind::Null) {
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::Data || type->GetKind() == ETypeAnnotationKind::Optional) {
        const TTypeAnnotationNode* unpacked = RemoveOptionalType(type);
        if (unpacked->GetKind() != ETypeAnnotationKind::Data) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                "Nested optional type is not compatible to PG"));
            return false;
        }

        auto slot = unpacked->Cast<TDataExprType>()->GetSlot();
        pgType = ConvertToPgType(slot);
        convertToPg = true;
        return true;
    } else if (type->GetKind() != ETypeAnnotationKind::Pg) {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Expected PG type, but got: " << type->GetKind()));
        return false;
    } else {
        pgType = type->Cast<TPgExprType>()->GetId();

        return true;
    }
}

bool HasContextFuncs(const TExprNode& input) {
    bool needCtx = false;
    VisitExpr(input, [&needCtx](const TExprNode& node) {
        if (needCtx || node.IsCallable("WithContext")) {
            return false;
        }
        if (node.IsCallable("PgResolvedCallCtx")) {
            needCtx = true;
            return false;
        }

        if (node.IsCallable({"AggApply","AggApplyState","AggApplyManyState","AggBlockApply","AggBlockApplyState"}) &&
            node.Head().Content().StartsWith("pg_")) {
            needCtx = true;
            return false;
        }

        return true;
    });

    return needCtx;
}

IGraphTransformer::TStatus TryConvertToPgOp(TStringBuf op, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    if (!EnsureMinArgsCount(*input, 1, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool hasPg = false;
    for (const auto& child : input->Children()) {
        if (!EnsureComputable(*child, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        hasPg = hasPg || child->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg;
    }

    if (!hasPg) {
        return IGraphTransformer::TStatus::Ok;
    }

    TExprNode::TListType args;
    args.push_back(ctx.NewAtom(input->Pos(), op));
    args.insert(args.end(), input->Children().begin(), input->Children().end());
    output = ctx.NewCallable(input->Pos(), "PgOp", std::move(args));
    return IGraphTransformer::TStatus::Repeat;
}

bool EnsureBlockOrScalarType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected block or scalar type, but got lambda"));
        return false;
    }

    return EnsureBlockOrScalarType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureBlockOrScalarType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (!type.IsBlockOrScalar()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected block or scalar type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureScalarType(const TExprNode& node, TExprContext& ctx) {
    if (HasError(node.GetTypeAnn(), ctx)) {
        return false;
    }

    if (!node.GetTypeAnn()) {
        YQL_ENSURE(node.Type() == TExprNode::Lambda);
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected scalar type, but got lambda"));
        return false;
    }

    return EnsureScalarType(node.Pos(), *node.GetTypeAnn(), ctx);
}

bool EnsureScalarType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx) {
    if (HasError(&type, ctx)) {
        return false;
    }

    if (!type.IsScalar()) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Expected scalar type, but got: " << type));
        return false;
    }

    return true;
}

bool EnsureValidJsonPath(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureSpecificDataType(node, EDataSlot::Utf8, ctx))
        return false;

    if (node.IsCallable("Utf8")) {
        if (TIssues issues; !NJsonPath::ParseJsonPath(node.Tail().Content(), issues, 7U)) {
            TIssue issue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Invalid json path: " << node.Tail().Content());
            if (bool(issues)) {
                for (const auto& i : issues) {
                    issue.AddSubIssue(new TIssue(i));
                }
            }
            ctx.AddError(issue);
            return false;
        }
    }
    return true;
}

const TTypeAnnotationNode* GetBlockItemType(const TTypeAnnotationNode& type, bool& isScalar) {
    YQL_ENSURE(type.IsBlockOrScalar());
    const auto kind = type.GetKind();
    if (kind == ETypeAnnotationKind::Block) {
        isScalar = false;
        return type.Cast<TBlockExprType>()->GetItemType();
    }
    isScalar = true;
    return type.Cast<TScalarExprType>()->GetItemType();
}

const TTypeAnnotationNode* AggApplySerializedStateType(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto name = input->Child(0)->Content();
    if (name == "count" || name == "count_all" || name == "min" || name == "max" || name == "some") {
        return input->GetTypeAnn();
    } else if (name == "avg" || name == "sum") {
        auto itemType = input->Content().StartsWith("AggBlock") ?
            input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType() :
            input->Child(2)->GetTypeAnn();
        if (input->Content().EndsWith("State")) {
            return itemType;
        }

        if (IsNull(*itemType)) {
            return itemType;
        }

        bool isOptional;
        const TDataExprType* lambdaType;
        YQL_ENSURE(IsDataOrOptionalOfData(itemType, isOptional, lambdaType));
        auto lambdaTypeSlot = lambdaType->GetSlot();
        const TTypeAnnotationNode* stateValueType;
        if (IsDataTypeDecimal(lambdaTypeSlot)) {
            if (name == "sum") {
                return input->GetTypeAnn();
            }

            const auto decimalType = lambdaType->Cast<TDataExprParamsType>();
            stateValueType = ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, "35", decimalType->GetParamTwo());
        } else if (IsDataTypeInterval(lambdaTypeSlot)) {
            stateValueType = ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, "35", "0");
            if (name == "sum") {
                if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
                    return ctx.MakeType<TOptionalExprType>(stateValueType);
                } else {
                    return stateValueType;
                }
            }
        } else {
            if (name == "sum") {
                return input->GetTypeAnn();
            }

            stateValueType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Double);
        }

        TVector<const TTypeAnnotationNode*> items = {
            stateValueType,
            ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)
        };

        const TTypeAnnotationNode* stateType = ctx.MakeType<TTupleExprType>(std::move(items));
        if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
            stateType = ctx.MakeType<TOptionalExprType>(stateType);
        }

        return stateType;
    } else if (name.StartsWith("pg_")) {
        auto func = name.SubStr(3);
        TVector<ui32> argTypes;
        if (input->Content().StartsWith("AggBlock")) {
            for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
                argTypes.push_back(input->Child(i)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TPgExprType>()->GetId());
            }
        } else {
            bool needRetype = false;
            auto status = ExtractPgTypesFromMultiLambda(input->ChildRef(2), argTypes, needRetype, ctx);
            YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
        }

        const NPg::TAggregateDesc& aggDesc = NPg::LookupAggregation(TString(func), argTypes);
        const auto& procDesc = NPg::LookupProc(aggDesc.SerializeFuncId ? aggDesc.SerializeFuncId : aggDesc.TransFuncId);
        return ctx.MakeType<TPgExprType>(procDesc.ResultType);
    } else {
        YQL_ENSURE(false, "Unknown AggApply: " << name);
    }
}

bool GetSumResultType(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx) {
    bool isOptional;
    const TDataExprType* lambdaType;
    if(IsDataOrOptionalOfData(&inputType, isOptional, lambdaType)) {
        auto lambdaTypeSlot = lambdaType->GetSlot();
        const TTypeAnnotationNode *sumResultType = nullptr;
        if (IsDataTypeSigned(lambdaTypeSlot)) {
            sumResultType = ctx.MakeType<TDataExprType>(EDataSlot::Int64);
        } else if (IsDataTypeUnsigned(lambdaTypeSlot)) {
            sumResultType = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        } else if (IsDataTypeDecimal(lambdaTypeSlot)) {
            const auto decimalType = lambdaType->Cast<TDataExprParamsType>();
            sumResultType = ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, "35", decimalType->GetParamTwo());
        } else if (IsDataTypeFloat(lambdaTypeSlot)) {
            sumResultType = ctx.MakeType<TDataExprType>(lambdaTypeSlot);
        } else if (IsDataTypeInterval(lambdaTypeSlot)) {
            sumResultType = ctx.MakeType<TDataExprType>(lambdaTypeSlot);
            isOptional = true;
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Unsupported column type: " << lambdaTypeSlot));
            return false;
        }

        if (isOptional) {
            sumResultType = ctx.MakeType<TOptionalExprType>(sumResultType);
        }

        retType = sumResultType;
        return true;
    } else if (IsNull(inputType)) {
        retType = ctx.MakeType<TNullExprType>();
        return true;
    } else {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Unsupported type: " << FormatType(&inputType) << ". Expected Data or Optional of Data."));
        return false;
    }
}

bool GetAvgResultType(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx) {
    bool isOptional;
    const TDataExprType* lambdaType;
    if(IsDataOrOptionalOfData(&inputType, isOptional, lambdaType)) {
        auto lambdaTypeSlot = lambdaType->GetSlot();
        const TTypeAnnotationNode *avgResultType = nullptr;
        if (IsDataTypeNumeric(lambdaTypeSlot) || lambdaTypeSlot == EDataSlot::Bool) {
            avgResultType = ctx.MakeType<TDataExprType>(EDataSlot::Double);
            if (isOptional) {
                avgResultType = ctx.MakeType<TOptionalExprType>(avgResultType);
            }
        } else if (IsDataTypeDecimal(lambdaTypeSlot)) {
            avgResultType = &inputType;
        } else if (IsDataTypeInterval(lambdaTypeSlot)) {
            avgResultType = &inputType;
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Unsupported column type: " << lambdaTypeSlot));
            return false;
        }

        retType = avgResultType;
        return true;
    } else if (IsNull(inputType)) {
        retType = ctx.MakeType<TNullExprType>();
        return true;
    } else {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Unsupported type: " << FormatType(&inputType) << ". Expected Data or Optional of Data."));
        return false;
    }
}

bool GetAvgResultTypeOverState(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx) {
    if (IsNull(inputType)) {
        retType = &inputType;
    } else {
        auto itemType = &inputType;
        bool isOptional = false;
        if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
            isOptional = true;
            itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
        }

        if (!EnsureTupleTypeSize(pos, itemType, 2, ctx)) {
            return false;
        }

        auto tupleType = itemType->Cast<TTupleExprType>();
        auto sumType = tupleType->GetItems()[0];
        const TTypeAnnotationNode* sumTypeOut;
        if (!GetSumResultType(pos, *sumType, sumTypeOut, ctx)) {
            return false;
        }

        if (!IsSameAnnotation(*sumType, *sumTypeOut)) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Mismatch sum type, expected: " << *sumType << ", but got: " << *sumTypeOut));
            return false;
        }

        auto countType = tupleType->GetItems()[1];
        if (!EnsureSpecificDataType(pos, *countType, EDataSlot::Uint64, ctx)) {
            return false;
        }

        retType = sumType;
        if (isOptional) {
            retType = ctx.MakeType<TOptionalExprType>(retType);
        }
    }

    return true;
}

bool GetMinMaxResultType(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx) {
    if (!inputType.IsComparable()) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected comparable type, but got: " << inputType));
        return false;
    }

    retType = &inputType;
    return true;
}

IGraphTransformer::TStatus ExtractPgTypesFromMultiLambda(TExprNode::TPtr& lambda, TVector<ui32>& argTypes,
    bool& needRetype, TExprContext& ctx) {
    for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
        auto type = lambda->Child(i)->GetTypeAnn();
        ui32 argType;
        bool convertToPg;
        if (!ExtractPgType(type, argType, convertToPg, lambda->Child(i)->Pos(), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (convertToPg) {
            needRetype = true;
        }

        argTypes.push_back(argType);
    }

    if (needRetype) {
        auto newLambda = ctx.DeepCopyLambda(*lambda);
        for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
            auto type = lambda->Child(i)->GetTypeAnn();
            ui32 argType;
            bool convertToPg;
            if (!ExtractPgType(type, argType, convertToPg, lambda->Child(i)->Pos(), ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (convertToPg) {
                newLambda->ChildRef(i) = ctx.NewCallable(newLambda->Child(i)->Pos(), "ToPg", { newLambda->ChildPtr(i) });
            }
        }

        lambda = newLambda;
        return IGraphTransformer::TStatus::Repeat;
    }

    return IGraphTransformer::TStatus::Ok;
}

TExprNode::TPtr ExpandPgAggregationTraits(TPositionHandle pos, const NPg::TAggregateDesc& aggDesc, bool onWindow,
    const TExprNode::TPtr& lambda, const TVector<ui32>& argTypes, const TTypeAnnotationNode* itemType, TExprContext& ctx) {
    auto idLambda = ctx.Builder(pos)
        .Lambda()
            .Param("state")
            .Arg("state")
        .Seal()
        .Build();

    auto saveLambda = idLambda;
    auto loadLambda = idLambda;
    auto finishLambda = idLambda;
    auto nullValue = ctx.NewCallable(pos, "Null", {});
    if (aggDesc.FinalFuncId) {
        const ui32 originalAggResultType = NPg::LookupProc(aggDesc.FinalFuncId).ResultType;
        ui32 aggResultType = originalAggResultType;
        AdjustReturnType(aggResultType, aggDesc.ArgTypes, 0, argTypes);
        finishLambda = ctx.Builder(pos)
            .Lambda()
            .Param("state")
            .Callable("PgResolvedCallCtx")
                .Atom(0, NPg::LookupProc(aggDesc.FinalFuncId).Name)
                .Atom(1, ToString(aggDesc.FinalFuncId))
                .List(2)
                    .Do([aggResultType, originalAggResultType](TExprNodeBuilder& builder) -> TExprNodeBuilder& {
                        if (aggResultType != originalAggResultType) { 
                            builder.List(0)
                                .Atom(0, "type")
                                .Atom(1, NPg::LookupType(aggResultType).Name)
                            .Seal();
                        }

                        return builder;
                    })
                .Seal()
                .Arg(3, "state")
                .Do([&aggDesc, nullValue](TExprNodeBuilder& builder) -> TExprNodeBuilder& {
                    if (aggDesc.FinalExtra) {
                        builder.Add(4, nullValue);
                    }

                    return builder;
                })
            .Seal()
            .Seal()
            .Build();
    }

    auto initValue = nullValue;
    if (aggDesc.InitValue) {
        initValue = ctx.Builder(pos)
            .Callable("PgCast")
                .Callable(0, "PgConst")
                    .Atom(0, aggDesc.InitValue)
                    .Callable(1, "PgType")
                        .Atom(0, "text")
                    .Seal()
                .Seal()
                .Callable(1, "PgType")
                    .Atom(0, NPg::LookupType(aggDesc.TransTypeId).Name)
                .Seal()
            .Seal()
            .Build();
    }

    const auto& transFuncDesc = NPg::LookupProc(aggDesc.TransFuncId);
    // use first non-null value as state if transFunc is strict
    bool searchNonNullForState = false;
    if (transFuncDesc.IsStrict && !aggDesc.InitValue) {
        Y_ENSURE(argTypes.size() == 1);
        searchNonNullForState = true;
    }

    TExprNode::TPtr initLambda, updateLambda;
    if (!searchNonNullForState) {
        initLambda = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("parent")
                .Callable("PgResolvedCallCtx")
                    .Atom(0, transFuncDesc.Name)
                    .Atom(1, ToString(aggDesc.TransFuncId))
                    .List(2)
                    .Seal()
                    .Callable(3, "PgClone")
                        .Add(0, initValue)
                        .Callable(1, "DependsOn")
                            .Arg(0, "row")
                        .Seal()
                        .Callable(2, "DependsOn")
                            .Arg(0, "parent")
                        .Seal()
                    .Seal()
                    .Apply(4, lambda)
                        .With(0, "row")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        updateLambda = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("state")
                .Param("parent")
                .Callable("Coalesce")
                    .Callable(0, "PgResolvedCallCtx")
                        .Atom(0, transFuncDesc.Name)
                        .Atom(1, ToString(aggDesc.TransFuncId))
                        .List(2)
                        .Seal()
                        .Callable(3, "Coalesce")
                            .Arg(0, "state")
                            .Callable(1, "PgClone")
                                .Add(0, initValue)
                                .Callable(1, "DependsOn")
                                    .Arg(0, "row")
                                .Seal()
                                .Callable(2, "DependsOn")
                                    .Arg(0, "parent")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Apply(4, lambda)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    } else {
        initLambda = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Apply(lambda)
                    .With(0, "row")
                .Seal()
            .Seal()
            .Build();

        if ((lambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null) ||
            (lambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg &&
            lambda->GetTypeAnn()->Cast<TPgExprType>()->GetName() == "unknown")) {
            initLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("row")
                    .Callable("PgCast")
                        .Apply(0, initLambda)
                            .With(0, "row")
                        .Seal()
                        .Callable(1, "PgType")
                            .Atom(0, NPg::LookupType(aggDesc.TransTypeId).Name)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        updateLambda = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("state")
                .Callable("If")
                    .Callable(0, "Exists")
                        .Arg(0, "state")
                    .Seal()
                    .Callable(1, "Coalesce")
                        .Callable(0, "PgResolvedCallCtx")
                            .Atom(0, transFuncDesc.Name)
                            .Atom(1, ToString(aggDesc.TransFuncId))
                            .List(2)
                            .Seal()
                            .Arg(3, "state")
                            .Apply(4, lambda)
                                .With(0, "row")
                            .Seal()
                        .Seal()
                        .Arg(1, "state")
                    .Seal()
                    .Apply(2, lambda)
                        .With(0, "row")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    auto mergeLambda = ctx.Builder(pos)
        .Lambda()
            .Param("state1")
            .Param("state2")
            .Callable("Void")
            .Seal()
        .Seal()
        .Build();

    auto zero = ctx.Builder(pos)
            .Callable("PgConst")
                .Atom(0, "0")
                .Callable(1, "PgType")
                    .Atom(0, "int8")
                .Seal()
            .Seal()
            .Build();

    auto defaultValue = (aggDesc.Name != "count") ? nullValue : zero;

    if (aggDesc.SerializeFuncId) {
        const auto& serializeFuncDesc = NPg::LookupProc(aggDesc.SerializeFuncId);
        saveLambda = ctx.Builder(pos)
            .Lambda()
                .Param("state")
                .Callable("PgResolvedCallCtx")
                    .Atom(0, serializeFuncDesc.Name)
                    .Atom(1, ToString(aggDesc.SerializeFuncId))
                    .List(2)
                    .Seal()
                    .Arg(3, "state")
                .Seal()
            .Seal()
            .Build();
    }

    if (aggDesc.DeserializeFuncId) {
        const auto& deserializeFuncDesc = NPg::LookupProc(aggDesc.DeserializeFuncId);
        loadLambda = ctx.Builder(pos)
            .Lambda()
                .Param("state")
                .Callable("PgResolvedCallCtx")
                    .Atom(0, deserializeFuncDesc.Name)
                    .Atom(1, ToString(aggDesc.DeserializeFuncId))
                    .List(2)
                    .Seal()
                    .Arg(3, "state")
                    .Callable(4, "PgInternal0")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    if (aggDesc.CombineFuncId) {
        const auto& combineFuncDesc = NPg::LookupProc(aggDesc.CombineFuncId);
        if (combineFuncDesc.IsStrict) {
            mergeLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("state1")
                    .Param("state2")
                    .Callable("If")
                        .Callable(0, "Exists")
                            .Arg(0, "state1")
                        .Seal()
                        .Callable(1, "Coalesce")
                            .Callable(0, "PgResolvedCallCtx")
                                .Atom(0, combineFuncDesc.Name)
                                .Atom(1, ToString(aggDesc.CombineFuncId))
                                .List(2)
                                .Seal()
                                .Arg(3, "state1")
                                .Arg(4, "state2")
                            .Seal()
                            .Arg(1, "state1")
                        .Seal()
                        .Arg(2, "state2")
                    .Seal()
                .Seal()
                .Build();
        } else {
            mergeLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("state1")
                    .Param("state2")
                    .Callable("PgResolvedCallCtx")
                        .Atom(0, combineFuncDesc.Name)
                        .Atom(1, ToString(aggDesc.CombineFuncId))
                        .List(2)
                        .Seal()
                        .Arg(3, "state1")
                        .Arg(4, "state2")
                    .Seal()
                .Seal()
                .Build();
        }
    }

    auto typeNode = ExpandType(pos, *itemType, ctx);
    if (onWindow) {
        return ctx.Builder(pos)
            .Callable("WindowTraits")
                .Add(0, typeNode)
                .Add(1, initLambda)
                .Add(2, updateLambda)
                .Lambda(3)
                    .Param("value")
                    .Param("state")
                    .Callable("Void")
                    .Seal()
                .Seal()
                .Add(4, finishLambda)
                .Add(5, defaultValue)
            .Seal()
            .Build();
    } else {
        return ctx.Builder(pos)
            .Callable("AggregationTraits")
                .Add(0, typeNode)
                .Add(1, initLambda)
                .Add(2, updateLambda)
                .Add(3, saveLambda)
                .Add(4, loadLambda)
                .Add(5, mergeLambda)
                .Add(6, finishLambda)
                .Add(7, defaultValue)
            .Seal()
            .Build();
    }
}

void AdjustReturnType(ui32& returnType, const TVector<ui32>& procArgTypes, ui32 procVariadicType, const TVector<ui32>& argTypes) {
    if (returnType == NPg::AnyArrayOid) {
        TMaybe<ui32> inputElementType;
        TMaybe<ui32> inputArrayType;
        for (ui32 i = 0; i < argTypes.size(); ++i) {
            if (!argTypes[i]) {
                continue;
            }

            auto targetType = i >= procArgTypes.size() ? procVariadicType : procArgTypes[i];
            if (targetType == NPg::AnyNonArrayOid) {
                if (!inputElementType) {
                   inputElementType = argTypes[i];
                } else {
                    if (*inputElementType != argTypes[i]) {
                        return;
                    }
                }
            }

            if (targetType == NPg::AnyArrayOid) {
                if (!inputArrayType) {
                   inputArrayType = argTypes[i];
                } else {
                    if (*inputArrayType != argTypes[i]) {
                        return;
                    }
                }
            }
        }

        if (inputElementType) {
            returnType = NPg::LookupType(*inputElementType).ArrayTypeId;
        } else if (inputArrayType) {
            returnType = *inputArrayType;
        }
    } else if (returnType == NPg::AnyElementOid) {
        for (ui32 i = 0; i < argTypes.size(); ++i) {
            if (!argTypes[i]) {
                continue;
            }

            const auto& typeDesc = NPg::LookupType(argTypes[i]);
            if (typeDesc.ArrayTypeId == typeDesc.TypeId) {
                returnType = typeDesc.ElementTypeId;
                return;
            }
        }
    }
}

const TTypeAnnotationNode* GetOriginalResultType(TPositionHandle pos, bool isMany, const TTypeAnnotationNode* originalExtractorType, TExprContext& ctx) {
    if (!EnsureStructType(pos, *originalExtractorType, ctx)) {
        return nullptr;
    }

    auto structType = originalExtractorType->Cast<TStructExprType>();
    if (structType->GetSize() != 1) {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Expected struct with one member"));
        return nullptr;
    }

    auto type = structType->GetItems()[0]->GetItemType();
    if (isMany) {
        if (type->GetKind() != ETypeAnnotationKind::Optional) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expected optional state"));
            return nullptr;
        }

        type = type->Cast<TOptionalExprType>()->GetItemType();
    }

    return type;
}

bool ApplyOriginalType(TExprNode::TPtr input, bool isMany, const TTypeAnnotationNode* originalExtractorType, TExprContext& ctx) {
    auto type = GetOriginalResultType(input->Pos(), isMany, originalExtractorType, ctx);
    if (!type) {
        return false;
    }

    input->SetTypeAnn(type);
    return true;
}

TExprNode::TPtr ConvertToMultiLambda(const TExprNode::TPtr& lambda, TExprContext& ctx) {
    Y_ENSURE(lambda->ChildrenSize() == 2);
    auto tupleTypeSize = lambda->GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    auto newArg = ctx.NewArgument(lambda->Pos(), "row");
    auto newBody = ctx.ReplaceNode(lambda->TailPtr(), lambda->Head().Head(), newArg);
    TExprNode::TListType bodies;
    for (ui32 i = 0; i < tupleTypeSize; ++i) {
        bodies.push_back(ctx.Builder(lambda->Pos())
            .Callable("Nth")
                .Add(0, newBody)
                .Atom(1, ToString(i))
            .Seal()
            .Build());
    }

    return ctx.NewLambda(lambda->Pos(), ctx.NewArguments(lambda->Pos(), { newArg }), std::move(bodies));
}

TStringBuf NormalizeCallableName(TStringBuf name) {
    name.ChopSuffix("MayWarn"sv);
    return name;
}

void CheckExpectedTypeAndColumnOrder(const TExprNode& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    auto it = typesCtx.ExpectedTypes.find(node.UniqueId());
    if (it != typesCtx.ExpectedTypes.end()) {
        YQL_ENSURE(IsSameAnnotation(*node.GetTypeAnn(), *it->second),
            "Rewrite error, type should be : " <<
            *it->second << ", but it is: " << *node.GetTypeAnn() << " for node " << node.Content());
    }

    auto coIt = typesCtx.ExpectedColumnOrders.find(node.UniqueId());
    if (coIt != typesCtx.ExpectedColumnOrders.end()) {
        TColumnOrder oldColumnOrder = coIt->second;
        TMaybe<TColumnOrder> newColumnOrder = typesCtx.LookupColumnOrder(node);
        if (!newColumnOrder) {
            // keep column order after rewrite
            // TODO: check if needed
            auto status = typesCtx.SetColumnOrder(node, oldColumnOrder, ctx);
            YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
        } else {
            YQL_ENSURE(newColumnOrder && *newColumnOrder == oldColumnOrder,
                "Rewrite error, column order should be: "
                << FormatColumnOrder(oldColumnOrder) << ", but it is: "
                << FormatColumnOrder(newColumnOrder) << " for node "
                << node.Content());
        }
    }
}

} // NYql
