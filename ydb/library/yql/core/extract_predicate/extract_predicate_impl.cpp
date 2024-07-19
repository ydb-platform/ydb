#include "extract_predicate_impl.h"

#include <ydb/library/yql/core/type_ann/type_ann_pg.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>

#include <ydb/library/yql/utils/utf8.h>

namespace NYql {
namespace NDetail {
namespace {

using namespace NNodes;
using NUdf::TCastResultOptions;

TExprNode::TPtr BuildMultiplyLimit(TMaybe<size_t> limit, TExprContext& ctx, TPositionHandle pos) {
    if (limit) {
        return ctx.Builder(pos)
            .Callable("Uint64")
                .Atom(0, ToString(*limit), TNodeFlags::Default)
            .Seal()
            .Build();
    } else {
        return ctx.Builder(pos)
            .Callable("Void")
            .Seal()
            .Build();
    }
}

const TTypeAnnotationNode* GetBasePgOrDataType(const TTypeAnnotationNode* type) {
    if (type && type->GetKind() == ETypeAnnotationKind::Pg) {
        return type;
    }
    type = RemoveAllOptionals(type);
    return (type && type->GetKind() == ETypeAnnotationKind::Data) ? type : nullptr;
}

bool IsPgOrDataOrMultiOptionalOfData(const TTypeAnnotationNode* type) {
    return GetBasePgOrDataType(type) != nullptr;
}

TMaybe<size_t> GetSqlInCollectionSize(const TExprNode::TPtr& collection) {
    TExprNode::TPtr curr = collection;
    if (curr->IsCallable("Just")) {
        curr = curr->HeadPtr();
    }

    if (curr->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple || curr->IsCallable({"AsList", "AsDict", "AsSet"})) {
        return std::max<size_t>(curr->ChildrenSize(), 1);
    }

    return {};
}

const TTypeAnnotationNode* GetSqlInCollectionItemType(const TTypeAnnotationNode* collectionType) {
    collectionType = RemoveOptionalType(collectionType);
    YQL_ENSURE(collectionType);
    switch (collectionType->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            const auto& items = collectionType->Cast<TTupleExprType>()->GetItems();
            YQL_ENSURE(items.size());
            YQL_ENSURE(AllOf(items, [&items](const TTypeAnnotationNode* type) { return type == items.front(); } ));
            return items.front();
        }
        case ETypeAnnotationKind::List: {
            const TTypeAnnotationNode* listItem = collectionType->Cast<TListExprType>()->GetItemType();
            if (listItem->GetKind() == ETypeAnnotationKind::Struct) {
                // single column table source
                const auto& items = listItem->Cast<TStructExprType>()->GetItems();
                YQL_ENSURE(items.size() == 1);
                return items.front()->GetItemType();
            }
            return listItem;
        }
        case ETypeAnnotationKind::Dict: {
            return collectionType->Cast<TDictExprType>()->GetKeyType();
        }
        default:
            YQL_ENSURE(false, "Unexpected IN collection type: " << *collectionType);
    }
}

TExprNode::TPtr GetSqlInCollectionAsOptList(const TExprNode::TPtr& collection, TExprContext& ctx) {
    auto collectionType = collection->GetTypeAnn();
    bool isOptionalCollection = false;
    if (collectionType->GetKind() == ETypeAnnotationKind::Optional) {
        isOptionalCollection = true;
        collectionType = collectionType->Cast<TOptionalExprType>()->GetItemType();
    }

    TPositionHandle pos = collection->Pos();
    TExprNode::TPtr body;
    TExprNode::TPtr collectionArg = ctx.NewArgument(pos, "input");
    switch (collectionType->GetKind()) {
        case ETypeAnnotationKind::List: {
            const TTypeAnnotationNode* listItem = collectionType->Cast<TListExprType>()->GetItemType();
            if (listItem->GetKind() == ETypeAnnotationKind::Struct) {
                // single column table source
                const auto& items = listItem->Cast<TStructExprType>()->GetItems();
                YQL_ENSURE(items.size() == 1);
                body = ctx.Builder(pos)
                    .Callable("Map")
                        .Add(0, collectionArg)
                        .Lambda(1)
                            .Param("item")
                            .Callable("Member")
                                .Arg(0, "item")
                                .Atom(1, items.front()->GetName())
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                body = collectionArg;
            }
            break;
        }
        case ETypeAnnotationKind::Dict: {
            body = ctx.NewCallable(pos, "DictKeys", { collectionArg });
            break;
        }
        case ETypeAnnotationKind::Tuple: {
            TExprNodeList items;
            for (size_t i = 0; i < collectionType->Cast<TTupleExprType>()->GetSize(); ++i) {
                items.push_back(
                    ctx.Builder(pos)
                        .Callable("Nth")
                            .Add(0, collectionArg)
                            .Atom(1, ToString(i), TNodeFlags::Default)
                        .Seal()
                        .Build()
                );
            }
            body = ctx.NewCallable(pos, "AsList", std::move(items));
            break;
        }
        default: YQL_ENSURE(false, "Unexpected type for IN collection: " << *collectionType);
    }

    TExprNode::TPtr lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { collectionArg }), std::move(body));
    auto optCollection = isOptionalCollection ? collection : ctx.NewCallable(pos, "Just", { collection });
    return ctx.NewCallable(pos, "Map", { optCollection, lambda });
}

bool IsRoundingSupported(const TExprNode& op, bool negated) {
    if (op.IsCallable({"Exists", "StartsWith"}) || (op.IsCallable("SqlIn") && !negated)) {
        return true;
    }

    if (op.IsCallable("==")) {
        YQL_ENSURE(!negated, "Not should be expanded earlier");
        return true;
    }

    struct TKeyValueTypes {
        const TTypeAnnotationNode* KeyBaseType = nullptr;
        const TTypeAnnotationNode* ValueBaseType = nullptr;

        static TVector<TKeyValueTypes> Extract(const TTypeAnnotationNode* keyType, const TTypeAnnotationNode* valueType) {
            TVector<TKeyValueTypes> result;
            if (keyType->GetKind() == ETypeAnnotationKind::Tuple) {
                const auto& keyTypes = keyType->Cast<TTupleExprType>()->GetItems();
                auto valueTypeStipped = RemoveAllOptionals(valueType);
                YQL_ENSURE(valueTypeStipped->GetKind() == ETypeAnnotationKind::Tuple);
                const auto& valueTypes = valueTypeStipped->Cast<TTupleExprType>()->GetItems();

                YQL_ENSURE(keyTypes.size() == valueTypes.size());
                YQL_ENSURE(!keyTypes.empty());

                for (size_t i = 0; i < keyTypes.size(); ++i) {
                    result.emplace_back();
                    result.back().KeyBaseType = GetBasePgOrDataType(keyTypes[i]);
                    result.back().ValueBaseType = GetBasePgOrDataType(valueTypes[i]);
                }
            } else {
                result.emplace_back();
                result.back().KeyBaseType = GetBasePgOrDataType(keyType);
                result.back().ValueBaseType = GetBasePgOrDataType(valueType);
            }

            return result;
        }
    };

    TVector<TKeyValueTypes> types;
    if (op.IsCallable("SqlIn")) {
        YQL_ENSURE(negated);
        TCoSqlIn in(&op);
        const auto& key = in.Lookup().Ref();
        const auto& collection = in.Collection().Ref();

        types = TKeyValueTypes::Extract(key.GetTypeAnn(), GetSqlInCollectionItemType(collection.GetTypeAnn()));
        // temporarily disable NOT IN
        return false;
    } else {
        YQL_ENSURE(op.IsCallable({"<", "<=", ">", ">=", "!="}));
        YQL_ENSURE(!negated, "Not should be expanded earlier");

        types = TKeyValueTypes::Extract(op.Head().GetTypeAnn(), op.Tail().GetTypeAnn());
    }

    for (auto& item : types) {
        YQL_ENSURE(item.KeyBaseType);
        YQL_ENSURE(item.ValueBaseType);
        const auto keyKind = item.KeyBaseType->GetKind();
        const auto valueKind = item.ValueBaseType->GetKind();
        if (keyKind != ETypeAnnotationKind::Data || valueKind != ETypeAnnotationKind::Data) {
            YQL_ENSURE(keyKind == valueKind);
            YQL_ENSURE(keyKind == ETypeAnnotationKind::Pg);
            if (!IsSameAnnotation(*item.KeyBaseType, *item.ValueBaseType)) {
                return false;
            }
            continue;
        }

        EDataSlot valueSlot = item.ValueBaseType->Cast<TDataExprType>()->GetSlot();
        EDataSlot keySlot = item.KeyBaseType->Cast<TDataExprType>()->GetSlot();

        auto maybeCastoptions = NUdf::GetCastResult(valueSlot, keySlot);
        if (*maybeCastoptions & (NUdf::ECastOptions::MayLoseData | NUdf::ECastOptions::MayFail)) {
            auto valueTypeInfo = NUdf::GetDataTypeInfo(valueSlot);
            auto keyTypeInfo = NUdf::GetDataTypeInfo(keySlot);
            auto supportedFeatures = NUdf::EDataTypeFeatures::IntegralType | NUdf::EDataTypeFeatures::DateType |
                NUdf::EDataTypeFeatures::StringType;
            return (valueTypeInfo.Features & supportedFeatures) &&
                   (keyTypeInfo.Features & supportedFeatures);
        }
    }
    return true;
}

bool IsSupportedMemberNode(const TExprNode& node, const TExprNode& row) {
    return node.IsCallable("Member") && node.Child(0) == &row && IsPgOrDataOrMultiOptionalOfData(node.GetTypeAnn());
}

bool IsValidForRange(const TExprNode& node, const TExprNode* otherNode, const TExprNode& row) {
    if (!IsSupportedMemberNode(node, row)) {
        return false;
    }

    if (otherNode && IsDepended(*otherNode, row)) {
        return false;
    }

    return true;
}

const THashMap<TStringBuf, TStringBuf> SupportedBinOps = {
    { "<", ">"},
    { "<=", ">="},
    { ">", "<"},
    { ">=", "<="},
    { "==", "=="},
    { "!=", "!="},
};

bool IsValidForRange(TExprNode::TPtr& node, const TExprNode& row, const TPredicateExtractorSettings& settings, TExprContext& ctx) {
    if (settings.IsValidForRange && !settings.IsValidForRange(node)) {
        return false;
    }
    auto it = SupportedBinOps.find(node->Content());
    if (it != SupportedBinOps.end()) {
        if (IsValidForRange(node->Head(), &node->Tail(), row)) {
            return true;
        }

        if (IsValidForRange(node->Tail(), &node->Head(), row)) {
            node = ctx.NewCallable(node->Pos(), it->second, { node->TailPtr(), node->HeadPtr() });
            return true;
        }

        return false;
    }

    if (node->IsCallable("Exists")) {
        return IsValidForRange(node->Head(), nullptr, row);
    }

    if (node->IsCallable("StartsWith") && settings.HaveNextValueCallable) {
        return IsValidForRange(node->Head(), node->Child(1), row);
    }

    if (node->IsCallable("SqlIn")) {
        TCoSqlIn sqlIn(node);

        const auto& collection = sqlIn.Collection().Ptr();
        const auto& lookup = sqlIn.Lookup().Ptr();

        if (IsDepended(*collection, row)) {
            return false;
        }

        TExprNodeList members;
        if (lookup->IsList()) {
            auto collectionItemBaseType = RemoveAllOptionals(GetSqlInCollectionItemType(collection->GetTypeAnn()));
            YQL_ENSURE(collectionItemBaseType);
            YQL_ENSURE(collectionItemBaseType->GetKind() == ETypeAnnotationKind::Tuple);
            if (collectionItemBaseType->Cast<TTupleExprType>()->GetSize() != lookup->ChildrenSize()) {
                return false;
            }
            members = lookup->ChildrenList();
        } else {
            members.push_back(lookup);
        }
        return AllOf(members, [&](const auto& child) { return IsSupportedMemberNode(*child, row); });
    }

    return false;
}

TExprNode::TPtr GetOpFromRange(const TExprNode& node, bool& negated) {
    YQL_ENSURE(node.IsCallable("Range"));
    negated = false;
    auto opNode = node.TailPtr();
    if (opNode->IsLambda()) {
        opNode = opNode->TailPtr();
    }
    if (opNode->IsCallable("Coalesce")) {
        opNode = opNode->HeadPtr();
    }
    if (opNode->IsCallable("Not")) {
        negated = true;
        opNode = opNode->HeadPtr();
    }

    return opNode;
}

TExprNode::TPtr GetOpFromRange(const TExprNode& node) {
    bool negated;
    return GetOpFromRange(node, negated);
}

TVector<TString> GetRawColumnsFromRange(const TExprNode& node) {
    auto opNode = GetOpFromRange(node);
    auto memberNode = opNode->IsCallable("SqlIn") ? opNode->ChildPtr(1) : opNode->ChildPtr(0);

    TExprNodeList members;
    if (memberNode->IsList()) {
        members = memberNode->ChildrenList();
    } else {
        members.push_back(std::move(memberNode));
    }

    TVector<TString> result;
    for (auto& member : members) {
        YQL_ENSURE(member->IsCallable("Member"));
        result.push_back(ToString(member->Tail().Content()));
    }
    YQL_ENSURE(!result.empty());
    return result;
}

TVector<TString> GetColumnsFromRange(const TExprNode& node, const THashMap<TString, size_t>& indexKeysOrder) {
    TVector<TString> result = GetRawColumnsFromRange(node);

    auto opNode = GetOpFromRange(node);
    YQL_ENSURE(opNode->IsCallable("SqlIn") || result.size() == 1);
    result.erase(
        std::remove_if(result.begin(), result.end(), [&](const TString& col) {
            return indexKeysOrder.find(col) == indexKeysOrder.end();
        }),
        result.end()
    );

    std::sort(result.begin(), result.end(), [&](const TString& a, const TString& b) {
        auto aIt = indexKeysOrder.find(a);
        auto bIt = indexKeysOrder.find(b);

        YQL_ENSURE(aIt != indexKeysOrder.end() && bIt != indexKeysOrder.end());
        return aIt->second < bIt->second;
    });

    if (!result.empty()) {
        size_t prevIdx = indexKeysOrder.find(result.front())->second;
        TVector<TString> resultNoGap;
        resultNoGap.push_back(result.front());
        for (size_t i = 1; i < result.size(); ++i) {
            size_t currIdx = indexKeysOrder.find(result[i])->second;
            YQL_ENSURE(currIdx >= prevIdx);
            if (currIdx == prevIdx) {
                continue;
            }
            if (currIdx != prevIdx + 1) {
                break;
            }
            prevIdx = currIdx;
            resultNoGap.push_back(result[i]);
        }
        result.swap(resultNoGap);
    }
    return result;
}

bool IsPointRange(const TExprNode& node) {
    bool negated;
    auto op = GetOpFromRange(node, negated);
    if (op->IsCallable("SqlIn")) {
        return !negated;
    }
    if (op->IsCallable("Exists")) {
        return negated;
    }
    if (op->IsCallable("StartsWith")) {
        return false;
    }
    YQL_ENSURE(!negated);
    return op->IsCallable("==");
}

bool IsCoalesceValidForRange(const TExprNode& node) {
    return node.IsCallable("Coalesce") && node.ChildrenSize() == 2 && node.Tail().IsCallable("Bool");
}

bool IsCoalesceValidForRange(const TExprNode& node, bool negated) {
    return IsCoalesceValidForRange(node) && FromString<bool>(node.Tail().Head().Content()) == negated;
}

TExprNode::TPtr ExpandTupleBinOp(const TExprNode& node, TExprContext& ctx) {
    YQL_ENSURE(node.Child(0)->IsList());
    YQL_ENSURE(node.Child(0)->ChildrenSize() > 0);
    if (node.IsCallable({"<=", ">="})) {
        return ctx.Builder(node.Pos())
            .Callable("Or")
                .Add(0, ctx.RenameNode(node, node.IsCallable("<=") ? "<" : ">"))
                .Add(1, ctx.RenameNode(node, "=="))
            .Seal()
            .Build();
    }

    if (node.IsCallable({"==", "!="})) {
        TExprNodeList predicates;
        for (ui32 i = 0; i < node.Child(0)->ChildrenSize(); ++i) {
            auto child = node.Child(0)->ChildPtr(i);
            predicates.push_back(
                ctx.Builder(child->Pos())
                    .Callable(node.Content())
                        .Add(0, child)
                        .Callable(1, "Nth")
                            .Add(0, node.ChildPtr(1))
                            .Atom(1, ToString(i), TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Build());
        }
        return ctx.NewCallable(node.Pos(), node.IsCallable("==") ? "And" : "Or", std::move(predicates));
    }

    YQL_ENSURE(node.IsCallable({"<", ">"}));
    TExprNodeList tupleItems = node.Child(0)->ChildrenList();
    TExprNode::TPtr firstPred = ctx.Builder(node.Pos())
        .Callable(node.Content())
            .Add(0, tupleItems.front())
            .Callable(1, "Nth")
                .Add(0, node.ChildPtr(1))
                .Atom(1, "0", TNodeFlags::Default)
            .Seal()
        .Seal()
        .Build();

    if (tupleItems.size() == 1) {
        return firstPred;
    }

    TExprNodeList otherTupleItems(tupleItems.begin() + 1, tupleItems.end());
    TExprNodeList otherValues;
    for (ui32 i = 1; i < tupleItems.size(); ++i) {
        otherValues.push_back(
            ctx.Builder(node.Child(1)->Pos())
                .Callable("Nth")
                    .Add(0, node.ChildPtr(1))
                    .Atom(1, ToString(i), TNodeFlags::Default)
                .Seal()
                .Build());
    }

    return ctx.Builder(node.Pos())
        .Callable("Or")
            .Add(0, firstPred)
            .Callable(1, "And")
                .Add(0, ctx.RenameNode(*firstPred, "=="))
                .Callable(1, node.Content())
                    .Add(0, ctx.NewList(node.Child(0)->Pos(), std::move(otherTupleItems)))
                    .Add(1, ctx.NewList(node.Child(1)->Pos(), std::move(otherValues)))
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

void DoBuildRanges(const TExprNode::TPtr& lambdaArg, const TExprNode::TPtr& currentNode, const TPredicateExtractorSettings& settings,
    TExprNode::TPtr& range, TSet<TString>& keysInScope, TExprContext& ctx, bool negated)
{
    keysInScope.clear();
    const TPositionHandle pos = currentNode->Pos();
    if (currentNode->IsCallable({"Or", "And"})) {
        YQL_ENSURE(currentNode->ChildrenSize() != 0);
        TExprNodeList children;
        const bool isUnion = currentNode->IsCallable("Or");
        TMaybe<TSet<TString>> commonKeysInScope;
        for (size_t i = 0; i < currentNode->ChildrenSize(); ++i) {
            const auto& child = currentNode->ChildPtr(i);
            children.emplace_back();
            TSet<TString> childKeys;
            DoBuildRanges(lambdaArg, child, settings, children.back(), childKeys, ctx, negated);
            if (!children.back()->IsCallable("RangeConst")) {
                if (!commonKeysInScope.Defined()) {
                    commonKeysInScope = childKeys;
                } else if (isUnion) {
                    TSet<TString> intersected;
                    std::set_intersection(commonKeysInScope->begin(), commonKeysInScope->end(),
                        childKeys.begin(), childKeys.end(), std::inserter(intersected, intersected.end()));

                    if (intersected.empty() || intersected.size() < std::min(childKeys.size(), commonKeysInScope->size())) {
                        *commonKeysInScope = std::move(intersected);
                    } else if (intersected.size() == commonKeysInScope->size()) {
                        *commonKeysInScope = std::move(childKeys);
                    }

                    if (commonKeysInScope->empty()) {
                        break;
                    }
                } else {
                    commonKeysInScope->insert(childKeys.begin(), childKeys.end());
                }
            }
        }

        if (!commonKeysInScope.Defined()) {
            range = ctx.NewCallable(pos, "RangeConst", { currentNode });
            return;
        }

        if (commonKeysInScope->empty()) {
            range = ctx.NewCallable(pos, "RangeRest", { currentNode });
            return;
        }

        keysInScope = std::move(*commonKeysInScope);
        range = ctx.NewCallable(pos, currentNode->IsCallable("Or") ? "RangeOr" : "RangeAnd", std::move(children));
        return;
    }

    if (IsCoalesceValidForRange(*currentNode, negated) || currentNode->IsCallable("Not")) {
        DoBuildRanges(lambdaArg, currentNode->HeadPtr(), settings, range, keysInScope, ctx, currentNode->IsCallable("Not") ? !negated : negated);
        YQL_ENSURE(range->IsCallable({"Range", "RangeRest", "RangeConst"})); // Coalesce/Not should be pushed down through Range{Unionn/Intersect}
        TExprNode::TPtr child;
        if (currentNode->IsCallable("Not")) {
            child = ctx.NewCallable(pos, "Not", { range->HeadPtr() });
        } else {
            child = ctx.NewCallable(pos, "Coalesce", { range->HeadPtr(), currentNode->TailPtr() });
        }

        range = ctx.NewCallable(pos, range->Content(), { child });
        return;
    }

    if (!IsDepended(*currentNode, *lambdaArg)) {
        range = ctx.NewCallable(pos, "RangeConst", { currentNode });
    } else {
        TExprNode::TPtr currentNodeNormalized = currentNode;
        bool isValid = IsValidForRange(currentNodeNormalized, *lambdaArg, settings, ctx);
        // TODO: all roundings should be supported
        isValid = isValid && IsRoundingSupported(*currentNodeNormalized, negated);
        range = ctx.NewCallable(pos, isValid ? "Range" : "RangeRest", { currentNodeNormalized });
        if (isValid) {
            keysInScope.clear();
            auto cols = GetRawColumnsFromRange(*range);
            keysInScope.insert(cols.begin(), cols.end());
        }
    }
}

bool IsListOfMembers(const TExprNode& node) {
    if (!node.IsList()) {
        return false;
    }

    return AllOf(node.ChildrenList(), [](const auto& child) {
        return child->IsCallable("Member") && IsPgOrDataOrMultiOptionalOfData(child->GetTypeAnn());
    });
}

bool IsMemberBinOpNode(const TExprNode& node) {
    YQL_ENSURE(node.ChildrenSize() == 2);
    return node.Head().IsCallable("Member") && IsPgOrDataOrMultiOptionalOfData(node.Head().GetTypeAnn()) ||
           node.Tail().IsCallable("Member") && IsPgOrDataOrMultiOptionalOfData(node.Tail().GetTypeAnn());
}

bool IsMemberListBinOpNode(const TExprNode& node) {
    YQL_ENSURE(node.ChildrenSize() == 2);
    return IsListOfMembers(node.Head()) || IsListOfMembers(node.Tail());
}

TExprNode::TPtr OptimizeNodeForRangeExtraction(const TExprNode::TPtr& node, const TExprNode::TPtr& parent, TExprContext& ctx) {
    auto it = node->IsCallable() ? SupportedBinOps.find(node->Content()) : SupportedBinOps.end();
    if (it != SupportedBinOps.end()) {
        TExprNode::TPtr toExpand;
        if (IsListOfMembers(node->Head())) {
            toExpand = node;
        } else if (IsListOfMembers(node->Tail())) {
            toExpand = ctx.NewCallable(node->Pos(), it->second, { node->TailPtr(), node->HeadPtr() });
        }

        if (toExpand) {
            auto baseType = RemoveAllOptionals(toExpand->Tail().GetTypeAnn());
            YQL_ENSURE(baseType);
            YQL_ENSURE(baseType->GetKind() == ETypeAnnotationKind::Tuple);
            if (baseType->Cast<TTupleExprType>()->GetSize() != node->Head().ChildrenSize()) {
                // comparison of tuples of different size
                return node;
            }
            YQL_CLOG(DEBUG, Core) << node->Content() << " over tuple";
            return ExpandTupleBinOp(*toExpand, ctx);
        }
    }

    if (node->IsCallable("Not")) {
        if (IsCoalesceValidForRange(node->Head())) {
            // The effect of this optimizer is the opposite of Coalesce optimizer in yql_co_simple1.cpp
            auto firstArg = node->Head().HeadPtr();
            auto secondArg = node->Head().TailPtr();
            YQL_CLOG(DEBUG, Core) << "Push down " << node->Content() << " over Coalesce";
            return ctx.Builder(node->Pos())
                .Callable("Coalesce")
                    .Callable(0, "Not")
                        .Add(0, firstArg)
                    .Seal()
                    .Callable(1, "Not")
                        .Add(0, secondArg)
                    .Seal()
                .Seal()
                .Build();
        }

        static const THashMap<TStringBuf, TStringBuf> binOpsWithNegations = {
            { "<", ">="},
            { "<=", ">"},
            { ">", "<="},
            { ">=", "<"},
            { "==", "!="},
            { "!=", "=="},
        };

        auto it = node->Head().IsCallable() ? binOpsWithNegations.find(node->Head().Content()) : binOpsWithNegations.end();
        if (it != binOpsWithNegations.end() && (IsMemberBinOpNode(node->Head()) || IsMemberListBinOpNode(node->Head()))) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return ctx.RenameNode(node->Head(), it->second);
        }

        if (node->Head().IsCallable({"And", "Or"})) {
            TExprNodeList children;
            for (auto& child : node->Head().ChildrenList()) {
                children.push_back(ctx.NewCallable(child->Pos(), "Not", { child }));
            }
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return ctx.NewCallable(node->Pos(), node->Head().IsCallable("Or") ? "And" : "Or", std::move(children));
        }

        if (node->Head().IsCallable("Not")) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Head().Content();
            return node->Head().HeadPtr();
        }

        if (node->Head().IsCallable("Bool")) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over literal Bool";
            bool value = FromString<bool>(node->Head().Head().Content());
            return MakeBool(node->Pos(), !value, ctx);
        }

        return node;
    }

    if (node->IsCallable("Nth")) {
        if (node->Head().Type() == TExprNode::List) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over tuple literal";
            const auto index = FromString<ui32>(node->Tail().Content());
            return node->Head().ChildPtr(index);
        }
    }

    if (IsCoalesceValidForRange(*node) && node->Head().IsCallable({"And", "Or"})) {
        YQL_CLOG(DEBUG, Core) << "PropagateCoalesceWithConst over " << node->Head().Content();
        auto children = node->Head().ChildrenList();
        for (auto& child : children) {
            child = ctx.NewCallable(node->Pos(), node->Content(), {std::move(child), node->TailPtr()});
        }
        return ctx.NewCallable(node->Head().Pos(), node->Head().Content(), std::move(children));
    }

    if (node->IsCallable({"And", "Or"})) {
        if (AnyOf(node->ChildrenList(), [&node](const auto& child) { return child->IsCallable(node->Content()); })) {
            YQL_CLOG(DEBUG, Core) << node->Content() << " over " << node->Content();
            TExprNodeList newChildren;
            for (auto& child : node->ChildrenList()) {
                if (child->IsCallable(node->Content())) {
                    auto chs = child->ChildrenList();
                    newChildren.insert(newChildren.end(), chs.begin(), chs.end());
                } else {
                    newChildren.emplace_back(std::move(child));
                }
            }
            return ctx.ChangeChildren(*node, std::move(newChildren));
        }
    }

    if (node->IsCallable("!=")) {
        TExprNode::TPtr litArg;
        TExprNode::TPtr anyArg;

        if (node->Child(1)->IsCallable("Bool")) {
            litArg = node->Child(1);
            anyArg = node->Child(0);
        }

        if (node->Child(0)->IsCallable("Bool")) {
            litArg = node->Child(0);
            anyArg = node->Child(1);
        }

        if (litArg && anyArg) {
            return ctx.Builder(node->Pos())
                .Callable("==")
                    .Add(0, anyArg)
                    .Add(1, MakeBool(node->Pos(), !FromString<bool>(litArg->Head().Content()), ctx))
                .Seal()
                .Build();
        }
    }

    if (node->IsCallable("Member") && (!parent || parent->IsCallable({"And", "Or", "Not", "Coalesce"}))) {
        auto* typeAnn = node->GetTypeAnn();
        if (typeAnn->GetKind() == ETypeAnnotationKind::Optional) {
            typeAnn = typeAnn->Cast<TOptionalExprType>()->GetItemType();
        }
        if (typeAnn->GetKind() == ETypeAnnotationKind::Data &&
            typeAnn->Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool)
        {
            YQL_CLOG(DEBUG, Core) << "Replace raw Member with explicit bool comparison";
            return ctx.Builder(node->Pos())
                .Callable("==")
                    .Add(0, node)
                    .Add(1, MakeBool(node->Pos(), true, ctx))
                .Seal()
                .Build();
        }
    }

    if (node->IsCallable("FromPg") && node->Head().IsCallable("PgResolvedOp")) {
        auto opNode = node->HeadPtr();
        TStringBuf op = opNode->Head().Content();
        if (SupportedBinOps.contains(op) || op == "<>" || op == "=") {
            auto left = opNode->ChildPtr(2);
            auto right = opNode->ChildPtr(3);
            if (IsSameAnnotation(*left->GetTypeAnn(), *right->GetTypeAnn())) {
                TStringBuf newOp;
                if (op == "=") {
                    newOp = "==";
                } else if (op == "<>") {
                    newOp = "!=";
                } else {
                    newOp = op;
                }
                YQL_ENSURE(opNode->ChildrenSize() == 4);
                YQL_CLOG(DEBUG, Core) << "Replace PgResolvedOp(" << op << ") with corresponding plain binary operation";
                return ctx.Builder(node->Pos())
                    .Callable(newOp)
                        .Add(0, opNode->ChildPtr(2))
                        .Add(1, opNode->ChildPtr(3))
                    .Seal()
                    .Build();
            }
        }
    }

    if (node->IsCallable("StartsWith")) {
        if (node->Head().IsCallable("FromPg")) {
            YQL_CLOG(DEBUG, Core) << "Get rid of FromPg() in " << node->Content() << " first argument";
            return ctx.ChangeChild(*node, 0, node->Head().HeadPtr());
        }
        if (node->Tail().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
            YQL_CLOG(DEBUG, Core) << "Convert second argument of " << node->Content() << " from PG type";
            return ctx.ChangeChild(*node, 1, ctx.NewCallable(node->Tail().Pos(), "FromPg", {node->TailPtr()}));
        }
    }

    return node;
}

void DoOptimizeForRangeExtraction(const TExprNode::TPtr& input, TExprNode::TPtr& output, bool topLevel, TExprContext& ctx, const TExprNode::TPtr& parent = nullptr) {
    output = OptimizeNodeForRangeExtraction(input, parent, ctx);
    if (output != input) {
        return;
    }

    auto children = input->ChildrenList();
    bool changed = false;
    for (auto& child : children) {
        if (!topLevel && !child->IsComputable()) {
            continue;
        }
        TExprNode::TPtr newChild = child;
        DoOptimizeForRangeExtraction(child, newChild, false, ctx, input);
        if (newChild != child) {
            changed = true;
            child = std::move(newChild);
        }
    }

    if (changed) {
        output = ctx.ChangeChildren(*input, std::move(children));
    }
}

THolder<IGraphTransformer> CreateRangeExtractionOptimizer(TTypeAnnotationContext& types) {
    TTransformationPipeline pipeline(&types);

    auto issueCode = TIssuesIds::CORE_EXEC;
    pipeline.AddServiceTransformers(issueCode);
    pipeline.AddTypeAnnotationTransformer(issueCode);
    // pipeline.Add(TExprLogTransformer::Sync("ExtractPredicateOpt", NLog::EComponent::Core, NLog::ELevel::TRACE),
    //     "ExtractPredicateOpt", issueCode, "ExtractPredicateOpt");
    pipeline.Add(CreateFunctorTransformer(
        [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            DoOptimizeForRangeExtraction(input, output, true, ctx);
            IGraphTransformer::TStatus result = IGraphTransformer::TStatus::Ok;
            if (input != output) {
                result = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
            }
            return result;
        }
    ), "ExtractPredicate", issueCode);

    return pipeline.BuildWithNoArgChecks(false);
}

TCoLambda OptimizeLambdaForRangeExtraction(const TExprNode::TPtr& filterLambdaNode, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    YQL_ENSURE(filterLambdaNode->IsLambda());
    TCoLambda filterLambda(filterLambdaNode);
    YQL_ENSURE(filterLambda.Args().Size() == 1);

    const TTypeAnnotationNode* argType = filterLambda.Args().Arg(0).Ref().GetTypeAnn();
    TConstraintNode::TListType argConstraints = filterLambda.Args().Arg(0).Ref().GetAllConstraints();

    YQL_ENSURE(argType && argType->GetKind() == ETypeAnnotationKind::Struct);

    auto transformer = CreateRangeExtractionOptimizer(typesCtx);
    TExprNode::TPtr output = filterLambdaNode;
    YQL_CLOG(DEBUG, Core) << "Begin optimizing lambda for range extraction";
    for (;;) {
        const auto status = InstantTransform(*transformer, output, ctx, true);
        if (status == IGraphTransformer::TStatus::Ok) {
            break;
        }
        YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
        YQL_ENSURE(status.HasRestart);
        YQL_ENSURE(UpdateLambdaAllArgumentsTypes(output, { argType }, ctx));
        YQL_ENSURE(UpdateLambdaConstraints(output, ctx, { argConstraints }) != IGraphTransformer::TStatus::Error);
    }
    YQL_CLOG(DEBUG, Core) << "Finish optimizing lambda for range extraction";

    return TCoLambda(output);
}

TExprNode::TPtr BuildFullRange(TPositionHandle pos, const TStructExprType& rowType, const TVector<TString>& indexKeys, TExprContext& ctx) {
    YQL_ENSURE(!indexKeys.empty());
    TExprNodeList components;
    for (auto key : indexKeys) {
        auto idx = rowType.FindItem(key);
        YQL_ENSURE(idx);
        const TTypeAnnotationNode* optKeyType = ctx.MakeType<TOptionalExprType>(rowType.GetItems()[*idx]->GetItemType());
        auto nullNode = ctx.NewCallable(pos, "Nothing", { ExpandType(pos, *optKeyType, ctx) });

        components.push_back(nullNode);
    }

    components.push_back(ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, "0", TNodeFlags::Default) }));
    auto boundary = ctx.NewList(pos, std::move(components));
    return ctx.Builder(pos)
        .Callable("AsRange")
            .List(0)
                .Add(0, boundary)
                .Add(1, boundary)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildSingleComputeRange(const TStructExprType& rowType,
    const TExprNode& range, const THashMap<TString, size_t>& indexKeysOrder,
    const TPredicateExtractorSettings& settings, const TString& lastKey, TExprContext& ctx)
{
    TVector<TString> keys = GetColumnsFromRange(range, indexKeysOrder);
    YQL_ENSURE(!keys.empty());

    auto idx = rowType.FindItem(keys.front());
    YQL_ENSURE(idx);
    const TTypeAnnotationNode* firstKeyType = rowType.GetItems()[*idx]->GetItemType();

    bool hasNot = false;
    auto opNode = GetOpFromRange(range, hasNot);
    TPositionHandle pos = opNode->Pos();
    if (opNode->IsCallable("Exists")) {
        YQL_ENSURE(keys.size() == 1);
        return ctx.Builder(pos)
            .Callable("RangeFor")
                .Atom(0, hasNot ? "NotExists" : "Exists", TNodeFlags::Default)
                .Callable(1, "Void")
                .Seal()
                .Add(2, ExpandType(pos, *firstKeyType, ctx))
            .Seal()
            .Build();
    }

    if (opNode->IsCallable("StartsWith")) {
        YQL_ENSURE(keys.size() == 1);
        const bool keyIsPg = firstKeyType->GetKind() == ETypeAnnotationKind::Pg;
        const TTypeAnnotationNode* rangeForType = firstKeyType;
        if (keyIsPg) {
            const TTypeAnnotationNode* yqlType = NTypeAnnImpl::FromPgImpl(pos, firstKeyType, ctx);
            YQL_ENSURE(yqlType);
            rangeForType = yqlType;
            YQL_ENSURE(opNode->Tail().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg);
        }
        auto rangeForNode = ctx.Builder(pos)
            .Callable("RangeFor")
                .Atom(0, hasNot ? "NotStartsWith" : "StartsWith", TNodeFlags::Default)
                .Add(1, opNode->TailPtr())
                .Add(2, ExpandType(pos, *rangeForType, ctx))
            .Seal()
            .Build();
        return ctx.WrapByCallableIf(keyIsPg, "RangeToPg", std::move(rangeForNode));
    }

    if (opNode->IsCallable("SqlIn")) {
        TCoSqlIn sqlIn(opNode);

        const auto inputCollection = ctx.NewArgument(pos, "collection");
        const bool haveTuples = sqlIn.Lookup().Ref().IsList();

        TTypeAnnotationNode::TListType keyTypes;
        for (auto& key : keys) {
            auto idx = rowType.FindItem(key);
            YQL_ENSURE(idx);
            keyTypes.push_back(rowType.GetItems()[*idx]->GetItemType());
        }

        const TTypeAnnotationNode* compositeKeyType = nullptr;
        if (haveTuples) {
            compositeKeyType = ctx.MakeType<TTupleExprType>(std::move(keyTypes));
        } else {
            YQL_ENSURE(keyTypes.size() == 1);
            compositeKeyType = keyTypes.front();
        }

        TExprNode::TPtr collection = inputCollection;
        if (haveTuples) {
            TVector<TString> rawKeys = GetRawColumnsFromRange(range);
            YQL_ENSURE(rawKeys.size() >= keys.size());
            THashMap<TString, size_t> rawKeysPos;
            for (size_t i = 0; i < rawKeys.size(); ++i) {
                // in case of duplicate members in tuple prefer the first one
                if (!rawKeysPos.contains(rawKeys[i])) {
                    rawKeysPos[rawKeys[i]] = i;
                }
            }

            // sourcePosition for given targetPosition
            TVector<size_t> sources;
            for (size_t target = 0; target < keys.size(); ++target) {
                TString key = keys[target];
                auto it = rawKeysPos.find(key);
                YQL_ENSURE(it != rawKeysPos.end());
                size_t source = it->second;
                sources.push_back(source);
            }

            // reorder and normalize collection
            auto collectionBaseType = RemoveAllOptionals(GetSqlInCollectionItemType(sqlIn.Collection().Ref().GetTypeAnn()));
            collection = ctx.Builder(pos)
                .Callable("Map")
                    .Callable(0, "FlatMap")
                        .Add(0, collection)
                        .Lambda(1)
                            .Param("item")
                            .Callable("StrictCast")
                                .Arg(0, "item")
                                .Add(1, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(collectionBaseType), ctx))
                            .Seal()
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Param("tuple")
                        .List()
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (size_t i = 0; i < sources.size(); ++i) {
                                parent
                                    .Callable(i, "Nth")
                                        .Arg(0, "tuple")
                                        .Atom(1, ToString(sources[i]), TNodeFlags::Default)
                                    .Seal();
                            }
                            return parent;
                        })
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        TExprNode::TPtr body;
        if (!hasNot) {
            // IN = collection of point intervals
            body = ctx.Builder(pos)
                    .Callable("FlatMap")
                        .Add(0, collection)
                        .Lambda(1)
                            .Param("item")
                            .Callable("RangeMultiply")
                                .Add(0, BuildMultiplyLimit(settings.MaxRanges, ctx, pos))
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    if (haveTuples) {
                                        const auto& types = compositeKeyType->Cast<TTupleExprType>()->GetItems();
                                        for (size_t i = 0; i < types.size(); ++i) {
                                            const bool needWidenKey = settings.MergeAdjacentPointRanges &&
                                                i + 1 == types.size() && lastKey == keys.back();
                                            parent
                                            .Callable(i + 1, "RangeFor")
                                                .Atom(0, needWidenKey ? "===" : "==", TNodeFlags::Default)
                                                .Callable(1, "Nth")
                                                    .Arg(0, "item")
                                                    .Atom(1, ToString(i), TNodeFlags::Default)
                                                .Seal()
                                                .Add(2, ExpandType(pos, *types[i], ctx))
                                            .Seal();
                                        }
                                    } else {
                                        const bool needWidenKey = settings.MergeAdjacentPointRanges &&
                                            lastKey == keys.back();
                                        parent
                                        .Callable(1, "RangeFor")
                                            .Atom(0, needWidenKey ? "===" : "==", TNodeFlags::Default)
                                            .Arg(1, "item")
                                            .Add(2, ExpandType(pos, *compositeKeyType, ctx))
                                        .Seal();
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                .Build();
            if (settings.MaxRanges) {
                YQL_ENSURE(*settings.MaxRanges < Max<size_t>());
                body = ctx.Builder(pos)
                    .Callable("Take")
                        .Add(0, body)
                        .Callable(1, "Uint64")
                            // +1 is essential here - RangeMultiply will detect overflow by this extra item
                            .Atom(0, ToString(*settings.MaxRanges + 1), TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Build();
            }
            body = ctx.NewCallable(pos, "Collect", { body });
            if (settings.MaxRanges) {
                body = ctx.Builder(pos)
                    .Callable("IfStrict")
                        .Callable(0, ">")
                            .Callable(0, "Length")
                                .Add(0, body)
                            .Seal()
                            .Callable(1, "Uint64")
                                .Atom(0, ToString(*settings.MaxRanges), TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .Add(1, BuildFullRange(pos, rowType, keys, ctx))
                        .Callable(2, "RangeUnion")
                            .Add(0, body)
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                body = ctx.Builder(pos)
                    .Callable("RangeUnion")
                        .Add(0, body)
                    .Seal()
                    .Build();
            }
        } else {
            YQL_ENSURE(false, "not supported yet, should be rejected earlier");
        }

        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { inputCollection }), std::move(body));

        auto optCollection = GetSqlInCollectionAsOptList(sqlIn.Collection().Ptr(), ctx);
        return ctx.Builder(pos)
            .Callable("IfPresent")
                .Add(0, optCollection)
                .Add(1, lambda)
                .Callable(2, "RangeEmpty")
                    .Add(0, ExpandType(pos, *compositeKeyType, ctx))
                .Seal()
            .Seal()
            .Build();
    }

    YQL_ENSURE(!hasNot);
    YQL_ENSURE(opNode->IsCallable({"<", "<=", ">", ">=", "==", "!="}));
    YQL_ENSURE(keys.size() == 1);
    TStringBuf op = opNode->Content();
    if (op == "==" && lastKey == keys.back() && settings.MergeAdjacentPointRanges) {
        op = "===";
    }
    return ctx.Builder(pos)
        .Callable("RangeFor")
            .Atom(0, op, TNodeFlags::Default)
            .Add(1, opNode->TailPtr())
            .Add(2, ExpandType(pos, *firstKeyType, ctx))
        .Seal()
        .Build();
}

TExprNode::TPtr RebuildAsRangeRest(const TStructExprType& rowType, const TExprNode& range, TExprContext& ctx) {
    if (range.IsCallable({"RangeRest", "Range"})) {
        return ctx.RenameNode(range, "RangeRest");
    }

    TPositionHandle pos = range.Pos();
    auto row = ctx.NewArgument(pos, "row");
    TExprNode::TPtr predicate;
    if (range.IsCallable("RangeConst")) {
        predicate = range.HeadPtr();
    } else {
        YQL_ENSURE(range.IsCallable({"RangeOr", "RangeAnd"}));

        TExprNodeList predicates;
        for (auto& child : range.ChildrenList()) {
            auto rebuilt = RebuildAsRangeRest(rowType, *child, ctx);
            predicates.push_back(ctx.Builder(pos).Apply(rebuilt->TailPtr()).With(0, row).Seal().Build());
        }
        predicate = ctx.NewCallable(pos, range.IsCallable("RangeOr") ? "Or" : "And", std::move(predicates));
    }

    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { row }), std::move(predicate));
    return ctx.NewCallable(pos, "RangeRest", { ExpandType(pos, rowType, ctx), lambda });
}

struct TIndexRange {
    size_t Begin = 0;
    size_t End = 0;
    size_t PointPrefixLen = 0;

    bool IsPoint() const {
        return !IsEmpty() && PointPrefixLen == (End - Begin);
    }

    bool IsEmpty() const {
        return Begin >= End;
    }

};

bool operator<(const TIndexRange& a, const TIndexRange& b) {
    if (a.IsEmpty()) {
        return false;
    }
    if (b.IsEmpty()) {
        return true;
    }

    if (a.Begin != b.Begin) {
        return a.Begin < b.Begin;
    }

    return a.End < b.End;
}

TIndexRange ExtractIndexRangeFromKeys(const TVector<TString>& keys, const THashMap<TString, size_t>& indexKeysOrder, bool isPoint) {
    YQL_ENSURE(!keys.empty());

    auto firstIt = indexKeysOrder.find(keys.front());
    auto lastIt = indexKeysOrder.find(keys.back());
    YQL_ENSURE(firstIt != indexKeysOrder.end() && lastIt != indexKeysOrder.end());

    TIndexRange result;
    result.Begin = firstIt->second;
    result.End = lastIt->second + 1;
    YQL_ENSURE(result.Begin < result.End);
    result.PointPrefixLen = isPoint ? (result.End - result.Begin) : 0;

    return result;
}

TExprNode::TPtr MakeRangeAnd(TPositionHandle pos, TExprNodeList&& children, TExprContext& ctx) {
    YQL_ENSURE(!children.empty());
    return children.size() == 1 ? children.front() : ctx.NewCallable(pos, "RangeAnd", std::move(children));
}

TExprNode::TPtr DoRebuildRangeForIndexKeys(const TStructExprType& rowType, const TExprNode::TPtr& range, const THashMap<TString, size_t>& indexKeysOrder,
    TIndexRange& resultIndexRange, TExprContext& ctx)
{
    resultIndexRange = {};
    if (range->IsCallable("RangeRest")) {
        return range;
    }

    if (range->IsCallable("RangeConst")) {
        resultIndexRange.Begin = 0;
        resultIndexRange.End = 1;
        return range;
    }

    if (range->IsCallable("Range")) {
        auto keys = GetColumnsFromRange(*range, indexKeysOrder);
        if (!keys.empty()) {
            resultIndexRange = ExtractIndexRangeFromKeys(keys, indexKeysOrder, false);
            return range;
        }
        return ctx.RenameNode(*range, "RangeRest");
    }

    TMaybe<TIndexRange> commonIndexRange;
    TExprNodeList rebuilt;
    if (range->IsCallable("RangeOr")) {
        rebuilt = range->ChildrenList();
        for (auto& child : rebuilt) {
            TIndexRange childIndexRange;
            child = DoRebuildRangeForIndexKeys(rowType, child, indexKeysOrder, childIndexRange, ctx);
            if (childIndexRange.IsEmpty()) {
                YQL_ENSURE(child->IsCallable("RangeRest"));
                return RebuildAsRangeRest(rowType, *range, ctx);
            }

            if (!commonIndexRange) {
                commonIndexRange = childIndexRange;
                continue;
            }

            if (commonIndexRange->Begin != childIndexRange.Begin) {
                return RebuildAsRangeRest(rowType, *range, ctx);
            }

            commonIndexRange->End = std::max(commonIndexRange->End, childIndexRange.End);
        }
    } else {
        YQL_ENSURE(range->IsCallable("RangeAnd"));
        struct TNodeAndIndexRange {
            TExprNode::TPtr Node;
            TIndexRange IndexRange;
            size_t OriginalPosition = 0;
        };

        TVector<TNodeAndIndexRange> toRebuild;
        size_t pos = 0;
        for (const auto& child : range->ChildrenList()) {
            toRebuild.emplace_back();
            TNodeAndIndexRange& curr = toRebuild.back();
            curr.Node = DoRebuildRangeForIndexKeys(rowType, child, indexKeysOrder, curr.IndexRange, ctx);
            curr.OriginalPosition = pos++;
        }

        TVector<TNodeAndIndexRange> rests;
        Sort(toRebuild.begin(), toRebuild.end(),
            [&](const auto& fs, const auto& sc) {
                return fs.IndexRange < sc.IndexRange;
            });
        TMap<size_t, TVector<TNodeAndIndexRange>> byBegin;
        if (!toRebuild.empty()) {
            byBegin[toRebuild[0].IndexRange.Begin] = {};
        }

        for (auto& current : toRebuild) {
            if (current.IndexRange.IsEmpty()) {
                YQL_ENSURE(current.Node->IsCallable("RangeRest"));
                rests.emplace_back(std::move(current));
            } else {
                if (byBegin.contains(current.IndexRange.Begin)) {
                    byBegin[current.IndexRange.Begin].push_back(current);
                    byBegin[current.IndexRange.End] = {};
                } else {
                    rests.push_back(std::move(current));
                }
            }
        }

        TMap<size_t, TNodeAndIndexRange> builtRange;
        for (auto it = byBegin.rbegin(); it != byBegin.rend(); ++it) {
            size_t end = 0;
            size_t indexRangeEnd = 0;
            TVector<TExprNode::TPtr> results;
            TVector<TExprNode::TPtr> currents;
            auto flush = [&] () {
                if (!currents) {
                    return;
                }

                TExprNode::TPtr toAdd = MakeRangeAnd(range->Pos(), std::move(currents), ctx);
                if (auto ptr = builtRange.FindPtr(end)) {
                    toAdd = MakeRangeAnd(range->Pos(), { toAdd, ptr->Node }, ctx);
                    indexRangeEnd = std::max(indexRangeEnd, ptr->IndexRange.End);
                }
                results.push_back(toAdd);

                indexRangeEnd = std::max(end, indexRangeEnd);
                currents = {};
            };
            for (auto node : it->second) {
                if (end != node.IndexRange.End) {
                    flush();
                    end = node.IndexRange.End;
                }
                currents.push_back(node.Node);
            }
            if (currents) {
                flush();
            }
            if (results) {
                auto& built = builtRange[it->first];
                built.Node = MakeRangeAnd(range->Pos(), std::move(results), ctx);
                built.IndexRange.Begin = it->first;
                built.IndexRange.End = indexRangeEnd;
            }
        }

        if (builtRange) {
            auto& first = *builtRange.begin();
            rebuilt.push_back(first.second.Node);
            commonIndexRange = first.second.IndexRange;
        }

        if (!rests.empty()) {
            // restore original order in rests
            std::sort(rests.begin(), rests.end(), [&](const TNodeAndIndexRange& a, const TNodeAndIndexRange& b) {
                return a.OriginalPosition < b.OriginalPosition;
            });
            TExprNodeList restsNodes;
            for (auto& item : rests) {
                restsNodes.push_back(RebuildAsRangeRest(rowType, *item.Node, ctx));
            }
            rebuilt.push_back(RebuildAsRangeRest(rowType, *MakeRangeAnd(range->Pos(), std::move(restsNodes), ctx), ctx));
        }
    }

    TExprNode::TPtr result = rebuilt.size() == 1 ? rebuilt[0] : ctx.ChangeChildren(*range, std::move(rebuilt));
    if (commonIndexRange) {
        resultIndexRange = *commonIndexRange;
    } else {
        result = RebuildAsRangeRest(rowType, *result, ctx);
    }

    return result;
}

TExprNode::TPtr RebuildRangeForIndexKeys(const TStructExprType& rowType, const TExprNode::TPtr& range, const THashMap<TString, size_t>& indexKeysOrder,
    size_t& usedPrefixLen, TExprContext& ctx)
{
    TIndexRange resultIndexRange;
    auto result = DoRebuildRangeForIndexKeys(rowType, range, indexKeysOrder, resultIndexRange, ctx);
    YQL_ENSURE(result);
    if (!resultIndexRange.IsEmpty() && resultIndexRange.Begin != 0) {
        resultIndexRange = {};
        result = RebuildAsRangeRest(rowType, *result, ctx);
    }
    usedPrefixLen = resultIndexRange.IsEmpty() ? 0 : resultIndexRange.End;
    return result;
}

TMaybe<size_t> CalcMaxRanges(const TExprNode::TPtr& range, const THashMap<TString, size_t>& indexKeysOrder) {
    if (range->IsCallable("RangeConst")) {
        return 1;
    }

    if (range->IsCallable("RangeRest")) {
        return 0;
    }

    if (range->IsCallable("Range")) {
        auto opNode = GetOpFromRange(*range);
        if (opNode->IsCallable("SqlIn")) {
            TCoSqlIn sqlIn(opNode);
            return GetSqlInCollectionSize(sqlIn.Collection().Ptr());
        }

        return opNode->IsCallable("!=") ? 2 : 1;
    }

    if (range->IsCallable("RangeOr")) {
        size_t result = 0;
        for (auto& child : range->ChildrenList()) {
            YQL_ENSURE(!child->IsCallable("RangeRest"));
            auto childRanges = CalcMaxRanges(child, indexKeysOrder);
            if (!childRanges) {
                return {};
            }
            result += *childRanges;
        }
        return result;
    }

    YQL_ENSURE(range->IsCallable("RangeAnd"));
    TMap<TVector<TString>, size_t> maxRangesByKey;
    TVector<size_t> toMultiply;
    for (auto& child : range->ChildrenList()) {
        TMaybe<size_t> childRanges;
        if (child->IsCallable("Range") || !child->IsCallable("RangeRest")) {
            childRanges = CalcMaxRanges(child, indexKeysOrder);
            if (!childRanges) {
                return {};
            }

            if (child->IsCallable("Range")) {
                auto key = GetColumnsFromRange(*child, indexKeysOrder);
                maxRangesByKey[key] = std::max(maxRangesByKey[key], *childRanges);
            } else {
                toMultiply.push_back(*childRanges);
            }
        }
    }

    for (const auto& [key, r] : maxRangesByKey) {
        toMultiply.push_back(r);
    }

    size_t result = 1;
    for (auto p : toMultiply) {
        YQL_ENSURE(p);
        if (result > std::numeric_limits<size_t>::max() / p) {
            // overflow
            return {};
        }
        result *= p;
    }

    return result;
}

TExprNode::TPtr MakePredicateFromPrunedRange(const TExprNode::TPtr& range, const TExprNode::TPtr& row, TExprContext& ctx) {
    TPositionHandle pos = range->Pos();
    if (range->IsCallable("RangeRest")) {
        return ctx.Builder(pos)
            .Apply(range->TailPtr())
                .With(0, row)
            .Seal()
            .Build();
    }

    YQL_ENSURE(range->IsCallable({"RangeOr", "RangeAnd"}));
    TExprNodeList children = range->ChildrenList();
    for (auto& child : children) {
        child = MakePredicateFromPrunedRange(child, row, ctx);
    }

    return ctx.NewCallable(pos, range->IsCallable("RangeOr") ? "Or" : "And", std::move(children));
}

TExprNode::TPtr BuildRestTrue(TPositionHandle pos, const TTypeAnnotationNode& rowType, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("RangeRest")
            .Add(0, ExpandType(pos, rowType, ctx))
            .Lambda(1)
                .Param("row")
                .Callable("Bool")
                    .Atom(0, "true", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

bool IsRestTrue(const TExprNode& node) {
    if (node.IsCallable("RangeRest")) {
        TCoLambda lambda(node.TailPtr());
        if (auto maybeBool = lambda.Body().Maybe<TCoBool>().Literal()) {
            if (maybeBool.Cast().Value() == "true") {
                return true;
            }
        }
    }
    return false;
}

TExprNode::TPtr BuildRangeMultiply(TPositionHandle pos, TMaybe<size_t> maxRanges, const TExprNodeList& toMultiply, TExprContext& ctx) {
    TExprNodeList args;
    args.reserve(toMultiply.size() + 1);
    args.push_back(BuildMultiplyLimit(maxRanges, ctx, pos));
    args.insert(args.end(), toMultiply.begin(), toMultiply.end());
    return ctx.NewCallable(pos, "RangeMultiply", std::move(args));
}

using TRangeHint = IPredicateRangeExtractor::TBuildResult::TLiteralRange;
using TRangeBoundHint = IPredicateRangeExtractor::TBuildResult::TLiteralRange::TLiteralRangeBound;

TMaybe<int> TryCompareColumns(const TExprNode::TPtr& fs, const TExprNode::TPtr& sc) {
    if (!fs || !sc) {
        return {};
    }
    if (fs == sc) {
        return 0;
    }

    auto isNull = [](const TExprNode::TPtr& ptr) {
        return ptr->IsCallable("Nothing") || (ptr->GetTypeAnn()
            && ptr->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null);
    };

    if (isNull(fs)) {
        if (isNull(sc)) {
            return 0;
        } else {
            return -1;
        }
    }
    if (isNull(sc)) {
        return 1;
    }

    return {};
}

TMaybe<TRangeBoundHint> CompareBounds(
    const TRangeBoundHint& hint1,
    const TRangeBoundHint& hint2,
    bool min, bool lefts)
{
    TRangeBoundHint hint;
    bool uniteAreas = min == lefts;
    for (size_t i = 0; ; i++) {
        if (i >= hint1.Columns.size()) {
            if (i >= hint2.Columns.size()) {
                hint = hint1;
                if (uniteAreas) {
                    hint.Inclusive = hint1.Inclusive || hint2.Inclusive;
                } else {
                    hint.Inclusive = hint1.Inclusive && hint2.Inclusive;
                }
            } else if (hint1.Inclusive != uniteAreas) {
                hint = hint2;
            } else {
                hint = hint1;
            }
            break;
        }
        if (i >= hint2.Columns.size()) {
            if (hint2.Inclusive != uniteAreas) {
                hint = hint1;
            } else {
                hint = hint2;
            }
            break;
        }

        if (!hint1.Columns[i] || !hint2.Columns[i]) {
            return Nothing();
        }
        if (auto cmp = TryCompareColumns(hint1.Columns[i], hint2.Columns[i])) {
            if (cmp == 0) {
                continue;
            } else if ((cmp < 0) == min) {
                hint = hint1;
            } else {
                hint = hint2;
            }

            break;
        } else {
            return Nothing();
        }
    }

    return hint;
}

TMaybe<TRangeHint> RangeHintIntersect(const TRangeHint& hint1, const TRangeHint& hint2) {
    auto left = CompareBounds(hint1.Left, hint2.Left, /* min */ false, true);
    auto right = CompareBounds(hint1.Right, hint2.Right, /* min */ true, false);
    if (left && right) {
        return TRangeHint{.Left = std::move(*left), .Right = std::move(*right)};
    } else {
        return Nothing();
    }
}

TMaybe<TRangeHint> RangeHintIntersect(const TMaybe<TRangeHint>& hint1, const TMaybe<TRangeHint>& hint2) {
    if (hint1 && hint2) {
        return RangeHintIntersect(*hint1, *hint2);
    } else {
        return {};
    }
}


TRangeHint RangeHintExtend(const TRangeHint& hint1, size_t hint1Len, const TRangeHint& hint2) {
    TRangeHint hint = hint1;
    if (hint.Left.Columns.size() == hint1Len && hint1.Left.Inclusive) {
        hint.Left.Columns.insert(hint.Left.Columns.end(), hint2.Left.Columns.begin(), hint2.Left.Columns.end());
        hint.Left.Inclusive = hint2.Left.Inclusive;
    }
    if (hint.Right.Columns.size() == hint1Len && hint1.Right.Inclusive) {
        hint.Right.Columns.insert(hint.Right.Columns.end(), hint2.Right.Columns.begin(), hint2.Right.Columns.end());
        hint.Right.Inclusive = hint2.Right.Inclusive;
    }
    return hint;
}

TMaybe<TRangeHint> RangeHintExtend(const TMaybe<TRangeHint>& hint1, size_t hint1Len, const TMaybe<TRangeHint>& hint2) {
    if (hint1 && hint2) {
        return RangeHintExtend(*hint1, hint1Len, *hint2);
    } else {
        return {};
    }
}

bool IsValid(const TRangeBoundHint& left, const TRangeBoundHint& right) {
    for (size_t i = 0; ; ++i) {
        if (i >= left.Columns.size() || i >= right.Columns.size()) {
            // ok, we have +-inf and sure that it's valid
            return true;
        }
        auto cmp = TryCompareColumns(left.Columns[i], right.Columns[i]);
        if (!cmp) {
            return false;
        } else if (*cmp < 0) {
            return true;
        } else if (*cmp > 0) {
            return false;
        }
    }
}

TMaybe<TRangeHint> RangeHintUnion(const TRangeHint& hint1, const TRangeHint& hint2) {
    if (!IsValid(hint1.Left, hint1.Right) || !IsValid(hint2.Left, hint2.Right)) {
        return Nothing();
    }

    auto left = CompareBounds(hint1.Left, hint2.Left, /* min */ true, true);
    auto right = CompareBounds(hint1.Right, hint2.Right, /* min */ false, false);
    auto intersection = RangeHintIntersect(hint1, hint2);
    if (!left || !right || !intersection) {
        return Nothing();
    }

    { // check if there is no gap between ranges
        for (size_t i = 0; ; ++i) {
            bool leftFinished = i >= intersection->Left.Columns.size();
            bool rightFinished = i >= intersection->Right.Columns.size();
            if (leftFinished || rightFinished) {
                if (leftFinished && intersection->Left.Inclusive) {
                    break;
                }
                if (rightFinished && intersection->Right.Inclusive) {
                    break;
                }
                return {};
            }

            auto cmp = TryCompareColumns(intersection->Left.Columns[i], intersection->Right.Columns[i]);
            if (!cmp) {
                return {};
            } else if (*cmp < 0) {
                break;
            } else if (*cmp > 0) {
                return {};
            }
        }
    }

    return TRangeHint{.Left = std::move(*left), .Right = std::move(*right)};
}

TMaybe<TRangeHint> RangeHintUnion(const TMaybe<TRangeHint>& hint1, const TMaybe<TRangeHint>& hint2) {
    if (hint1 && hint2) {
        return RangeHintUnion(*hint1, *hint2);
    } else {
        return {};
    }
}

void TryBuildSingleRangeHint(TExprNode::TPtr range, const TStructExprType& rowType, const TVector<TString>& indexKeys, TIndexRange indexRange, TMaybe<TRangeHint>& hint, TExprContext& ctx) {
    bool negated;
    auto op = GetOpFromRange(*range, negated);
    size_t rangeLen = indexRange.End - indexRange.Begin;

    auto idx = rowType.FindItem(indexKeys[indexRange.Begin]);
    YQL_ENSURE(idx);
    const TTypeAnnotationNode* firstKeyType = rowType.GetItems()[*idx]->GetItemType();

    auto isOptional = [&](const TExprNode::TPtr& node) {
        YQL_ENSURE(node->GetTypeAnn());
        return node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional || node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Null;
    };

    if (op->IsCallable("SqlIn") && !negated) {
        TCoSqlIn sqlIn(op);
        auto collection = sqlIn.Collection();
        if ((collection.Ptr()->IsCallable({"AsList", "AsSet", "Just"}) ||
                collection.Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) &&
            GetSqlInCollectionSize(collection.Ptr()) == TMaybe<size_t>(1))
        {
            auto item = sqlIn.Collection().Ptr()->Child(0);
            if (isOptional(item)) {
                return;
            }

            hint.ConstructInPlace();
            hint->Left.Inclusive = hint->Right.Inclusive = true;
            hint->Left.Columns = hint->Right.Columns = {item};
        }
    } else if (op->IsCallable(">") || op->IsCallable(">=")) {
        YQL_ENSURE(!negated);
        if (isOptional(op->ChildPtr(1))) {
            return;
        }

        hint.ConstructInPlace();
        hint->Left.Inclusive = op->IsCallable(">=");
        hint->Right.Inclusive = true;
        YQL_ENSURE(rangeLen == 1);
        hint->Left.Columns.push_back(op->ChildPtr(1));
    } else if (op->IsCallable("<") || op->IsCallable("<=")) {
        YQL_ENSURE(!negated);
        if (isOptional(op->ChildPtr(1))) {
            return;
        }

        hint.ConstructInPlace();
        hint->Right.Inclusive = op->IsCallable("<=");

        YQL_ENSURE(rangeLen == 1);
        hint->Right.Columns.push_back(op->ChildPtr(1));

        if (firstKeyType->GetKind() == ETypeAnnotationKind::Optional) {
            auto none = Build<TCoNothing>(ctx, op->Pos())
                    .OptionalType(ExpandType(op->Pos(), *firstKeyType, ctx))
                    .Done();
            hint->Left.Columns.push_back(none.Ptr());
            hint->Left.Inclusive = false;
        } else {
            hint->Left.Inclusive = true;
        }
    } else if (op->IsCallable("==")) {
        YQL_ENSURE(!negated);
        if (isOptional(op->ChildPtr(1))) {
            return;
        }

        hint.ConstructInPlace();
        hint->Left.Inclusive = hint->Right.Inclusive = true;
        hint->Left.Columns = hint->Right.Columns = {op->ChildPtr(1)};
    } else if (op->IsCallable("Exists")) {
        YQL_ENSURE(rangeLen == 1);
        hint.ConstructInPlace();
        auto none = Build<TCoNothing>(ctx, op->Pos())
                .OptionalType(ExpandType(op->Pos(), *firstKeyType, ctx))
                .Done();
        if (negated) {
            hint->Left.Inclusive = hint->Right.Inclusive = true;
            hint->Left.Columns.push_back(none.Ptr());
            hint->Right.Columns.push_back(none.Ptr());
        } else {
            hint->Left.Inclusive = false;
            hint->Left.Columns.push_back(none.Ptr());
            hint->Right.Inclusive = true;
        }
    }
}

TExprNode::TPtr DoBuildMultiColumnComputeNode(const TStructExprType& rowType, const TExprNode::TPtr& range,
    const TVector<TString>& indexKeys, const THashMap<TString, size_t>& indexKeysOrder,
    TExprNode::TPtr& prunedRange, TIndexRange& resultIndexRange, const TPredicateExtractorSettings& settings,
    size_t usedPrefixLen, TExprContext& ctx, TMaybe<TRangeHint>& hint)
{
    prunedRange = {};
    resultIndexRange = {};
    TPositionHandle pos = range->Pos();
    if (range->IsCallable("Range")) {
        // top-level node or child of RangeOr
        auto cols = GetColumnsFromRange(*range, indexKeysOrder);
        resultIndexRange = ExtractIndexRangeFromKeys(cols, indexKeysOrder, IsPointRange(*range));

        auto rawCols = GetRawColumnsFromRange(*range);
        prunedRange = (rawCols.size() == cols.size()) ?
            BuildRestTrue(pos, rowType, ctx) :
            RebuildAsRangeRest(rowType, *range, ctx);
        if (settings.BuildLiteralRange) {
            TryBuildSingleRangeHint(range, rowType, indexKeys, resultIndexRange, hint, ctx);
        }
        YQL_ENSURE(usedPrefixLen > 0 && usedPrefixLen <= indexKeys.size());
        return BuildSingleComputeRange(rowType, *range, indexKeysOrder, settings, indexKeys[usedPrefixLen - 1], ctx);
    }

    if (range->IsCallable("RangeRest")) {
        // top-level RangeRest
        prunedRange = range;
        return {};
    }

    if (range->IsCallable("RangeConst")) {
        prunedRange = BuildRestTrue(pos, rowType, ctx);
        resultIndexRange.Begin = 0;
        resultIndexRange.End = 1;
        resultIndexRange.PointPrefixLen = 0;
        return ctx.Builder(pos)
            .Callable("If")
                .Add(0, range->HeadPtr())
                .Add(1, BuildFullRange(range->Pos(), rowType, { indexKeys.front() }, ctx))
                .Callable(2, "RangeEmpty")
                .Seal()
            .Seal()
            .Build();
    }

    TExprNodeList output;
    TExprNodeList prunedOutput;
    if (range->IsCallable("RangeOr")) {
        TVector<TIndexRange> childIndexRanges;
        for (auto& child : range->ChildrenList()) {
            prunedOutput.emplace_back();
            TIndexRange childIndexRange;
            TMaybe<TRangeHint> childHint;
            output.push_back(DoBuildMultiColumnComputeNode(rowType, child, indexKeys, indexKeysOrder, prunedOutput.back(), childIndexRange, settings, usedPrefixLen, ctx, childHint));
            childIndexRanges.push_back(childIndexRange);
            YQL_ENSURE(!childIndexRange.IsEmpty());
            if (resultIndexRange.IsEmpty()) {
                resultIndexRange = childIndexRange;
                hint = childHint;
            } else {
                YQL_ENSURE(childIndexRange.Begin == resultIndexRange.Begin);
                resultIndexRange.End = std::max(resultIndexRange.End, childIndexRange.End);
                resultIndexRange.PointPrefixLen = std::min(resultIndexRange.PointPrefixLen, childIndexRange.PointPrefixLen);
                hint = RangeHintUnion(childHint, hint);
            }
        }

        for (size_t i = 0; i < output.size(); ++i) {
            if (childIndexRanges[i].End < resultIndexRange.End) {
                TVector<TString> alignKeys(indexKeys.begin() + childIndexRanges[i].End, indexKeys.begin() + resultIndexRange.End);
                output[i] = BuildRangeMultiply(pos, settings.MaxRanges, { output[i], BuildFullRange(pos, rowType, alignKeys, ctx) }, ctx);
            }
        }
    } else {
        YQL_ENSURE(range->IsCallable("RangeAnd"));
        TVector<TIndexRange> childIndexRanges;
        bool needAlign = true;
        for (const auto& child : range->ChildrenList()) {
            prunedOutput.emplace_back();
            TIndexRange childIndexRange;
            TMaybe<TRangeHint> childHint;
            auto compute = DoBuildMultiColumnComputeNode(rowType, child, indexKeys, indexKeysOrder, prunedOutput.back(), childIndexRange, settings, usedPrefixLen, ctx, childHint);
            if (!compute) {
                continue;
            }
            output.push_back(compute);
            childIndexRanges.push_back(childIndexRange);
            YQL_ENSURE(!childIndexRange.IsEmpty());
            if (resultIndexRange.IsEmpty()) {
                resultIndexRange = childIndexRange;
                hint = childHint;
            } else {
                if (childIndexRange.Begin != resultIndexRange.Begin)  {
                    YQL_ENSURE(childIndexRange.Begin == resultIndexRange.End);
                    hint = RangeHintExtend(hint, resultIndexRange.End - resultIndexRange.Begin, childHint);
                    needAlign = false;
                    if (!resultIndexRange.IsPoint()) {
                        prunedOutput.back() = RebuildAsRangeRest(rowType, *child, ctx);
                    } else {
                        resultIndexRange.PointPrefixLen += childIndexRange.PointPrefixLen;
                    }
                } else {
                    resultIndexRange.PointPrefixLen = std::max(resultIndexRange.PointPrefixLen, childIndexRange.PointPrefixLen);
                    hint = RangeHintIntersect(hint, childHint);
                }
                resultIndexRange.End = std::max(resultIndexRange.End, childIndexRange.End);
            }
        }

        if (needAlign) {
            for (size_t i = 0; i < output.size(); ++i) {
                if (childIndexRanges[i].End < resultIndexRange.End) {
                    TVector<TString> alignKeys(indexKeys.begin() + childIndexRanges[i].End, indexKeys.begin() + resultIndexRange.End);
                    output[i] = BuildRangeMultiply(pos, settings.MaxRanges, { output[i], BuildFullRange(pos, rowType, alignKeys, ctx) }, ctx);
                }
            }
        } else {
            output = { BuildRangeMultiply(pos, settings.MaxRanges, output, ctx) };
        }
    }
    YQL_ENSURE(!prunedOutput.empty());
    YQL_ENSURE(!output.empty());

    prunedOutput.erase(
        std::remove_if(prunedOutput.begin(), prunedOutput.end(), [](const auto& pruned) { return IsRestTrue(*pruned); }),
        prunedOutput.end()
        );

    if (prunedOutput.empty()) {
        prunedRange = BuildRestTrue(pos, rowType, ctx);
    } else if (range->IsCallable("RangeOr")) {
        prunedRange = RebuildAsRangeRest(rowType, *range, ctx);
    } else {
        prunedRange = ctx.NewCallable(pos, range->Content(), std::move(prunedOutput));
    }

    return ctx.NewCallable(pos, range->IsCallable("RangeOr") ? "RangeUnion" : "RangeIntersect", std::move(output));
}

IGraphTransformer::TStatus ConvertLiteral(TExprNode::TPtr& node, const NYql::TTypeAnnotationNode & sourceType, const NYql::TTypeAnnotationNode & expectedType, NYql::TExprContext& ctx) {
    if (IsSameAnnotation(sourceType, expectedType)) {
        return IGraphTransformer::TStatus::Ok;
    }

    if (expectedType.GetKind() == ETypeAnnotationKind::Optional) {
        auto nextType = expectedType.Cast<TOptionalExprType>()->GetItemType();
        auto originalNode = node;
        auto status1 = ConvertLiteral(node, sourceType, *nextType, ctx);
        if (status1.Level != IGraphTransformer::TStatus::Error) {
            node = ctx.NewCallable(node->Pos(), "Just", { node });
            return IGraphTransformer::TStatus::Repeat;
        }

        node = originalNode;
        if (node->IsCallable("Just")) {
            auto sourceItemType = sourceType.Cast<TOptionalExprType>()->GetItemType();
            auto value = node->HeadRef();
            auto status = ConvertLiteral(value, *sourceItemType, *nextType, ctx);
            if (status.Level != IGraphTransformer::TStatus::Error) {
                node = ctx.NewCallable(node->Pos(), "Just", { value });
                return IGraphTransformer::TStatus::Repeat;
            }
        } else if (sourceType.GetKind() == ETypeAnnotationKind::Optional) {
            return IGraphTransformer::TStatus::Error;
        }

        if (IsNull(sourceType)) {
            node = ctx.NewCallable(node->Pos(), "Nothing", { ExpandType(node->Pos(), expectedType, ctx) });
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    if (expectedType.GetKind() == ETypeAnnotationKind::Data && sourceType.GetKind() == ETypeAnnotationKind::Data) {
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

        if (IsDataTypeNumeric(from) && IsDataTypeNumeric(to)) {
            {
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
                    TString atomValue;
                    if (AllowIntegralConversion(maybeInt.Cast(), false, to, &atomValue)) {
                        node = ctx.NewCallable(node->Pos(), expectedType.Cast<TDataExprType>()->GetName(),
                            {ctx.NewAtom(node->Pos(), atomValue, TNodeFlags::Default)});
                        return IGraphTransformer::TStatus::Repeat;
                    }
                }
            }

            if (GetNumericDataTypeLevel(to) < GetNumericDataTypeLevel(from)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto castResult = NKikimr::NUdf::GetCastResult(from, to);
            if (*castResult & NKikimr::NUdf::Impossible) {
                return IGraphTransformer::TStatus::Error;
            }

            if (*castResult != NKikimr::NUdf::ECastOptions::Complete) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto pos = node->Pos();
            auto type = ExpandType(pos, expectedType, ctx);
            node = ctx.NewCallable(pos, "Convert", {std::move(node), std::move(type)});
            return IGraphTransformer::TStatus::Repeat;
        }

        auto fromFeatures = NUdf::GetDataTypeInfo(from).Features;
        auto toFeatures = NUdf::GetDataTypeInfo(to).Features;
        if ((fromFeatures & NUdf::TzDateType) && (toFeatures & (NUdf::DateType| NUdf::TzDateType)) ||
            (toFeatures & NUdf::TzDateType) && (fromFeatures & (NUdf::DateType | NUdf::TzDateType))) {
            const auto pos = node->Pos();
            auto type = ExpandType(pos, expectedType, ctx);
            node = ctx.NewCallable(pos, "SafeCast", {std::move(node), std::move(type)});
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    return IGraphTransformer::TStatus::Error;
}

void NormalizeRangeHint(TMaybe<TRangeHint>& hint, const TVector<TString>& indexKeys, const TStructExprType& rowType, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (!hint) {
        return;
    }

    auto normTypes = [&] (TRangeBoundHint& hint) {
        for (size_t i = 0; i < hint.Columns.size(); ++i) {
            auto idx = rowType.FindItem(indexKeys[i]);
            YQL_ENSURE(idx);
            const TTypeAnnotationNode* columnType = rowType.GetItems()[*idx]->GetItemType();
            const TTypeAnnotationNode* unwrapOptional = columnType;

            if (columnType->GetKind() == ETypeAnnotationKind::Optional) {
                unwrapOptional = columnType->Cast<TOptionalExprType>()->GetItemType();
            }

            TTransformationPipeline pipeline(&types);
            pipeline.AddServiceTransformers();
            pipeline.AddTypeAnnotationTransformer();
            pipeline.Add(CreateFunctorTransformer(
                [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
                    output = input;

                    auto status = ConvertLiteral(output, *output->GetTypeAnn(), *unwrapOptional, ctx);
                    if (status == IGraphTransformer::TStatus::Error) {
                        output = input;
                        status = ConvertLiteral(output, *output->GetTypeAnn(), *columnType, ctx);
                    }

                    if (status == IGraphTransformer::TStatus::Repeat) {
                        status.HasRestart = 1;
                    }
                    return status;
                }
            ), "ExtractPredicate", TIssuesIds::CORE_EXEC);

            auto transformer = pipeline.BuildWithNoArgChecks(true);

            for (;;) {
                auto status = InstantTransform(*transformer, hint.Columns[i], ctx, true);
                if (status == IGraphTransformer::TStatus::Ok) {
                    break;
                }
                if (status == IGraphTransformer::TStatus::Error) {
                    return false;
                }
            }
        }
        return true;
    };

    if (!normTypes(hint->Left) || !normTypes(hint->Right)) {
        hint.Clear();
    }
}

TExprNode::TPtr BuildMultiColumnComputeNode(const TStructExprType& rowType, const TExprNode::TPtr& range,
    const TVector<TString>& indexKeys, const THashMap<TString, size_t>& indexKeysOrder,
    TExprNode::TPtr& prunedRange, const TPredicateExtractorSettings& settings, size_t usedPrefixLen, size_t& pointPrefixLen,
    TExprContext& ctx, TTypeAnnotationContext& types, TMaybe<TRangeHint>& resultHint)
{
    TIndexRange resultIndexRange;
    auto result = DoBuildMultiColumnComputeNode(rowType, range, indexKeys, indexKeysOrder, prunedRange, resultIndexRange, settings, usedPrefixLen, ctx, resultHint);
    NormalizeRangeHint(resultHint, indexKeys, rowType, ctx, types);
    pointPrefixLen = resultIndexRange.PointPrefixLen;
    YQL_ENSURE(pointPrefixLen <= usedPrefixLen);
    YQL_ENSURE(prunedRange);
    if (result) {
        YQL_ENSURE(!resultIndexRange.IsEmpty());
        YQL_ENSURE(resultIndexRange.Begin == 0);
        YQL_ENSURE(usedPrefixLen == resultIndexRange.End);

        TPositionHandle pos = range->Pos();
        if (resultIndexRange.End < indexKeys.size()) {
            result = BuildRangeMultiply(pos, settings.MaxRanges,
                { result, BuildFullRange(pos, rowType, TVector<TString>(indexKeys.begin() + resultIndexRange.End, indexKeys.end()), ctx) }, ctx);
        }

        if (!result->IsCallable("RangeUnion")) {
            // normalize ranges
            result = ctx.NewCallable(pos, "RangeUnion", { result });
        }

        if (!result->IsCallable("RangeMultiply")) {
            // apply max_ranges limit
            result = BuildRangeMultiply(pos, settings.MaxRanges, { result }, ctx);
        }

        // convert to user format
        result = ctx.NewCallable(pos, "RangeFinalize", { result });
    }
    return result;
}

NYql::NNodes::TExprBase UnpackRangePoints(NYql::NNodes::TExprBase node, TConstArrayRef<TString> keyColumns, NYql::TExprContext& expCtx, NYql::TPositionHandle pos) {
    TCoArgument rangeArg = Build<TCoArgument>(expCtx, pos)
        .Name("rangeArg")
        .Done();

    TVector<TExprBase> structMembers;
    structMembers.reserve(keyColumns.size());
    for (size_t i = 0; i < keyColumns.size(); ++i) {
        auto kth = [&] (size_t k) {
            return Build<TCoUnwrap>(expCtx, pos)
                .Optional<TCoNth>()
                    .Tuple<TCoNth>()
                        .Tuple(rangeArg)
                        .Index().Build(k)
                        .Build()
                    .Index().Build(i)
                    .Build()
                .Done();
        };

        auto first = kth(0);
        auto second = kth(1);

        auto member = Build<TCoNameValueTuple>(expCtx, pos)
            .Name().Build(keyColumns[i])
            .Value<TCoEnsure>()
                .Value(first)
                .Message<TCoString>().Literal().Build("invalid range bounds").Build()
                .Predicate<TCoOr>()
                    .Add<TCoCmpEqual>()
                        .Left(first)
                        .Right(second)
                        .Build()
                    .Add<TCoAnd>()
                        .Add<TCoNot>().Value<TCoExists>().Optional(first).Build().Build()
                        .Add<TCoNot>().Value<TCoExists>().Optional(second).Build().Build()
                        .Build()
                    .Build()
                .Build()
            .Done();

        structMembers.push_back(member);
    }


    return Build<TCoMap>(expCtx, pos)
        .Input(node)
        .Lambda()
            .Args({rangeArg})
            .Body<TCoAsStruct>()
                .Add(structMembers)
                .Build()
            .Build()
        .Done();
}


} // namespace

bool TPredicateRangeExtractor::Prepare(const TExprNode::TPtr& filterLambdaNode, const TTypeAnnotationNode& rowType,
    THashSet<TString>& possibleIndexKeys, TExprContext& ctx, TTypeAnnotationContext& typesCtx)
{
    possibleIndexKeys.clear();
    YQL_ENSURE(!FilterLambda, "Prepare() should be called only once");
    FilterLambda = filterLambdaNode;
    YQL_ENSURE(rowType.GetKind() == ETypeAnnotationKind::Struct);
    RowType = rowType.Cast<TStructExprType>();

    TCoLambda filterLambda = OptimizeLambdaForRangeExtraction(filterLambdaNode, ctx, typesCtx);

    TExprNode::TPtr rowArg = filterLambda.Args().Arg(0).Ptr();
    TExprNode::TPtr pred;

    if (auto maybeCond = filterLambda.Body().Maybe<TCoConditionalValueBase>()) {
        pred = maybeCond.Cast().Predicate().Ptr();
    } else {
        pred = filterLambda.Body().Ptr();
    }

    const TTypeAnnotationNode* rowArgType = rowArg->GetTypeAnn();
    YQL_ENSURE(EnsureSpecificDataType(*pred, EDataSlot::Bool, ctx));
    YQL_ENSURE(rowArgType->GetKind() == ETypeAnnotationKind::Struct);
    for (auto& item : rowArgType->Cast<TStructExprType>()->GetItems()) {
        auto idx = RowType->FindItem(item->GetName());
        YQL_ENSURE(idx,
            "RowType/lambda arg mismatch: column " << item->GetName() << " is missing in original table");
        YQL_ENSURE(IsSameAnnotation(*RowType->GetItems()[*idx]->GetItemType(), *item->GetItemType()),
            "RowType/lambda arg mismatch: column " << item->GetName() << " has type: " << *item->GetItemType() <<
            " expecting: " << *RowType->GetItems()[*idx]->GetItemType());
    }

    TSet<TString> keysInScope;
    DoBuildRanges(rowArg, pred, Settings, Range, keysInScope, ctx, false);
    possibleIndexKeys.insert(keysInScope.begin(), keysInScope.end());

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    // replace plain predicate in Range/RangeRest with lambda
    auto status = OptimizeExpr(Range, Range, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (node->IsCallable({"Range", "RangeRest"}) && node->ChildrenSize() == 1) {
            auto pos = node->Pos();
            // use lambda in Range/RangeRest
            auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { rowArg }), node->HeadPtr());
            return ctx.Builder(pos)
                .Callable(node->Content())
                    .Add(0, ExpandType(pos, *rowArgType, ctx))
                    .Lambda(1)
                        .Param("row")
                        .Apply(lambda)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }
        return node;
    }, ctx, settings);

    return status == IGraphTransformer::TStatus::Ok;
}

TPredicateRangeExtractor::TBuildResult TPredicateRangeExtractor::BuildComputeNode(const TVector<TString>& indexKeys,
    TExprContext& ctx, TTypeAnnotationContext& typesCtx) const
{
    YQL_ENSURE(FilterLambda && Range && RowType, "Prepare() is not called");

    TBuildResult result;
    result.PrunedLambda = ctx.DeepCopyLambda(*FilterLambda);

    {
        THashSet<TString> uniqIndexKeys;
        for (auto& key : indexKeys) {
            YQL_ENSURE(uniqIndexKeys.insert(key).second, "Duplicate index column " << key);
        }
    }

    THashMap<TString, size_t> indexKeysOrder;
    TVector<TString> effectiveIndexKeys = indexKeys;
    for (size_t i = 0; i < effectiveIndexKeys.size(); ++i) {
        TMaybe<ui32> idx = RowType->FindItem(effectiveIndexKeys[i]);
        if (idx) {
            auto keyBaseType = GetBasePgOrDataType(RowType->GetItems()[*idx]->GetItemType());
            if (!(keyBaseType && keyBaseType->IsComparable() && keyBaseType->IsEquatable())) {
                idx = {};
            }
        }
        if (!idx) {
            effectiveIndexKeys.resize(i);
            break;
        }
        indexKeysOrder[effectiveIndexKeys[i]] = i;
    }

    if (effectiveIndexKeys.empty()) {
        return result;
    }

    TExprNode::TPtr rebuiltRange = RebuildRangeForIndexKeys(*RowType, Range, indexKeysOrder, result.UsedPrefixLen, ctx);
    TExprNode::TPtr prunedRange;
    result.ComputeNode = BuildMultiColumnComputeNode(*RowType, rebuiltRange, effectiveIndexKeys, indexKeysOrder,
        prunedRange, Settings, result.UsedPrefixLen, result.PointPrefixLen, ctx, typesCtx, result.LiteralRange);

    if (result.ComputeNode) {
        result.ExpectedMaxRanges = CalcMaxRanges(rebuiltRange, indexKeysOrder);
        if (!Settings.MaxRanges || (result.ExpectedMaxRanges && *result.ExpectedMaxRanges < *Settings.MaxRanges)) {
            TCoLambda lambda(result.PrunedLambda);
            auto newPred = MakePredicateFromPrunedRange(prunedRange, lambda.Args().Arg(0).Ptr(), ctx);

            TExprNode::TPtr newBody;
            if (auto maybeCond = lambda.Body().Maybe<TCoConditionalValueBase>()) {
                newBody = ctx.ChangeChild(lambda.Body().Ref(), TCoConditionalValueBase::idx_Predicate, std::move(newPred));
            } else {
                newBody = std::move(newPred);
            }

            result.PrunedLambda = ctx.ChangeChild(lambda.Ref(), TCoLambda::idx_Body, std::move(newBody));
        }
    }
    return result;
}

} // namespace NDetail

IPredicateRangeExtractor::TPtr MakePredicateRangeExtractor(const TPredicateExtractorSettings& settings) {
    return MakeHolder<NDetail::TPredicateRangeExtractor>(settings);
}


TExprNode::TPtr BuildPointsList(const IPredicateRangeExtractor::TBuildResult& result, TConstArrayRef<TString> keyColumns, NYql::TExprContext& expCtx) {
    return NDetail::UnpackRangePoints(NNodes::TExprBase(result.ComputeNode), keyColumns, expCtx, result.ComputeNode->Pos()).Ptr();
}

} // namespace NYql
