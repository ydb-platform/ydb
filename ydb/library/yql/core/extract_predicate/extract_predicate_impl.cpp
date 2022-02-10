#include "extract_predicate_impl.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>

namespace NYql {
namespace NDetail {
namespace {

using namespace NNodes;
using NUdf::TCastResultOptions;

const TTypeAnnotationNode* GetBaseDataType(const TTypeAnnotationNode *type) {
    type = RemoveAllOptionals(type);
    return (type && type->GetKind() == ETypeAnnotationKind::Data) ? type : nullptr;
}

bool IsDataOrMultiOptionalOfData(const TTypeAnnotationNode* type) {
    return GetBaseDataType(type) != nullptr;
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
                    result.back().KeyBaseType = GetBaseDataType(keyTypes[i]);
                    result.back().ValueBaseType = GetBaseDataType(valueTypes[i]);
                }
            } else {
                result.emplace_back();
                result.back().KeyBaseType = GetBaseDataType(keyType);
                result.back().ValueBaseType = GetBaseDataType(valueType);
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

        EDataSlot valueSlot = item.ValueBaseType->Cast<TDataExprType>()->GetSlot();
        EDataSlot keySlot = item.KeyBaseType->Cast<TDataExprType>()->GetSlot();

        auto maybeCastoptions = NUdf::GetCastResult(valueSlot, keySlot);
        if (!maybeCastoptions) {
            return IsSameAnnotation(*item.KeyBaseType, *item.ValueBaseType);
        }
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
    return node.IsCallable("Member") && node.Child(0) == &row && IsDataOrMultiOptionalOfData(node.GetTypeAnn());
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
                .Add(0, ctx.RenameNode(node, "=="))
                .Add(1, ctx.RenameNode(node, node.IsCallable("<=") ? "<" : ">"))
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
        return child->IsCallable("Member") && IsDataOrMultiOptionalOfData(child->GetTypeAnn());
    });
}

bool IsMemberBinOpNode(const TExprNode& node) {
    YQL_ENSURE(node.ChildrenSize() == 2);
    return node.Head().IsCallable("Member") && IsDataOrMultiOptionalOfData(node.Head().GetTypeAnn()) ||
           node.Tail().IsCallable("Member") && IsDataOrMultiOptionalOfData(node.Tail().GetTypeAnn());
}

bool IsMemberListBinOpNode(const TExprNode& node) {
    YQL_ENSURE(node.ChildrenSize() == 2);
    return IsListOfMembers(node.Head()) || IsListOfMembers(node.Tail());
}

TExprNode::TPtr OptimizeNodeForRangeExtraction(const TExprNode::TPtr& node, TExprContext& ctx) {
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

        return node;
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

    return node;
}

void DoOptimizeForRangeExtraction(const TExprNode::TPtr& input, TExprNode::TPtr& output, bool topLevel, TExprContext& ctx) {
    output = OptimizeNodeForRangeExtraction(input, ctx);
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
        DoOptimizeForRangeExtraction(child, newChild, false, ctx);
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
        return ctx.Builder(pos)
            .Callable("RangeFor")
                .Atom(0, hasNot ? "NotStartsWith" : "StartsWith", TNodeFlags::Default)
                .Add(1, opNode->TailPtr())
                .Add(2, ExpandType(pos, *firstKeyType, ctx))
            .Seal()
            .Build();
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
                .Callable("Take")
                    .Callable(0, "FlatMap")
                        .Add(0, collection)
                        .Lambda(1)
                            .Param("item")
                            .Callable("RangeMultiply")
                                .Callable(0, "Uint64")
                                    .Atom(0, ToString(settings.MaxRanges), TNodeFlags::Default)
                                .Seal()
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
                    .Callable(1, "Uint64")
                        // +1 is essential here - RangeMultiply will detect overflow by this extra item
                        .Atom(0, ToString(settings.MaxRanges + 1), TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Build();
            body = ctx.Builder(pos)
                .Callable("RangeUnion")
                    .Callable(0, "RangeMultiply")
                        .Callable(0, "Uint64")
                            .Atom(0, ToString(settings.MaxRanges), TNodeFlags::Default)
                        .Seal()
                        .Add(1, body)
                    .Seal()
                .Seal()
                .Build();
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
    bool IsPoint = false;

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
    result.IsPoint = isPoint;

    return result;
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
        };

        TVector<TNodeAndIndexRange> toRebuild;
        for (const auto& child : range->ChildrenList()) {
            toRebuild.emplace_back();
            TNodeAndIndexRange& curr = toRebuild.back();
            curr.Node = DoRebuildRangeForIndexKeys(rowType, child, indexKeysOrder, curr.IndexRange, ctx);
        }

        std::stable_sort(toRebuild.begin(), toRebuild.end(), [&](const TNodeAndIndexRange& a, const TNodeAndIndexRange& b) {
            // sort children by key order
            // move RangeRest/RangeConst to the end while preserving their relative order
            return a.IndexRange < b.IndexRange;
        });


        TExprNodeList rests;
        TVector<TExprNodeList> childrenChains;
        THashMap<size_t, TSet<size_t>> chainIdxByEndIdx;

        for (auto& current : toRebuild) {
            if (current.IndexRange.IsEmpty()) {
                YQL_ENSURE(current.Node->IsCallable("RangeRest"));
                rests.emplace_back(std::move(current.Node));
                continue;
            }
            const size_t beginIdx = current.IndexRange.Begin;
            const size_t endIdx = current.IndexRange.End;
            if (!commonIndexRange || beginIdx == commonIndexRange->Begin) {
                if (!commonIndexRange) {
                    commonIndexRange = current.IndexRange;
                } else {
                    commonIndexRange->End = std::max(commonIndexRange->End, endIdx);
                }
                chainIdxByEndIdx[endIdx].insert(childrenChains.size());
                childrenChains.emplace_back();
                childrenChains.back().push_back(current.Node);
            } else {
                auto it = chainIdxByEndIdx.find(beginIdx);
                if (it == chainIdxByEndIdx.end()) {
                    rests.emplace_back(RebuildAsRangeRest(rowType, *current.Node, ctx));
                    continue;
                }

                YQL_ENSURE(!it->second.empty());
                const size_t tgtChainIdx = *it->second.begin();
                it->second.erase(tgtChainIdx);
                if (it->second.empty()) {
                    chainIdxByEndIdx.erase(it);
                }

                childrenChains[tgtChainIdx].push_back(current.Node);
                chainIdxByEndIdx[endIdx].insert(tgtChainIdx);
                commonIndexRange->End = std::max(commonIndexRange->End, endIdx);
            }
        }

        for (auto& chain : childrenChains) {
            YQL_ENSURE(!chain.empty());
            rebuilt.push_back(ctx.NewCallable(range->Pos(), "RangeAnd", std::move(chain)));
        }

        if (!rests.empty()) {
            rebuilt.push_back(RebuildAsRangeRest(rowType, *ctx.NewCallable(range->Pos(), "RangeAnd", std::move(rests)), ctx));
        }
    }

    TExprNode::TPtr result = ctx.ChangeChildren(*range, std::move(rebuilt));
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

TExprNode::TPtr BuildRangeMultiply(TPositionHandle pos, size_t maxRanges, const TExprNodeList& toMultiply, TExprContext& ctx) {
    TExprNodeList args;
    args.reserve(toMultiply.size() + 1);
    args.push_back(ctx.NewCallable(pos, "Uint64", { ctx.NewAtom(pos, ToString(maxRanges), TNodeFlags::Default) }));
    args.insert(args.end(), toMultiply.begin(), toMultiply.end());
    return ctx.NewCallable(pos, "RangeMultiply", std::move(args));
}

TExprNode::TPtr DoBuildMultiColumnComputeNode(const TStructExprType& rowType, const TExprNode::TPtr& range,
    const TVector<TString>& indexKeys, const THashMap<TString, size_t>& indexKeysOrder,
    TExprNode::TPtr& prunedRange, TIndexRange& resultIndexRange, const TPredicateExtractorSettings& settings,
    size_t usedPrefixLen, TExprContext& ctx)
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
        resultIndexRange.IsPoint = false;
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
            output.push_back(DoBuildMultiColumnComputeNode(rowType, child, indexKeys, indexKeysOrder, prunedOutput.back(), childIndexRange, settings, usedPrefixLen, ctx));
            childIndexRanges.push_back(childIndexRange);
            YQL_ENSURE(!childIndexRange.IsEmpty());
            if (resultIndexRange.IsEmpty()) {
                resultIndexRange = childIndexRange;
            } else {
                YQL_ENSURE(childIndexRange.Begin == resultIndexRange.Begin);
                resultIndexRange.End = std::max(resultIndexRange.End, childIndexRange.End);
                resultIndexRange.IsPoint = resultIndexRange.IsPoint && childIndexRange.IsPoint;
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
            auto compute = DoBuildMultiColumnComputeNode(rowType, child, indexKeys, indexKeysOrder, prunedOutput.back(), childIndexRange, settings, usedPrefixLen, ctx);
            if (!compute) {
                continue;
            }
            output.push_back(compute);
            childIndexRanges.push_back(childIndexRange);
            YQL_ENSURE(!childIndexRange.IsEmpty());
            if (resultIndexRange.IsEmpty()) {
                resultIndexRange = childIndexRange;
            } else {
                if (childIndexRange.Begin != resultIndexRange.Begin)  {
                    needAlign = false;
                    if (!resultIndexRange.IsPoint) {
                        prunedOutput.back() = RebuildAsRangeRest(rowType, *child, ctx);
                    }
                    resultIndexRange.IsPoint = resultIndexRange.IsPoint && childIndexRange.IsPoint;
                } else {
                    resultIndexRange.IsPoint = resultIndexRange.IsPoint || childIndexRange.IsPoint;
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

TExprNode::TPtr BuildMultiColumnComputeNode(const TStructExprType& rowType, const TExprNode::TPtr& range,
    const TVector<TString>& indexKeys, const THashMap<TString, size_t>& indexKeysOrder,
    TExprNode::TPtr& prunedRange, const TPredicateExtractorSettings& settings, size_t usedPrefixLen, TExprContext& ctx)
{
    TIndexRange resultIndexRange;
    auto result = DoBuildMultiColumnComputeNode(rowType, range, indexKeys, indexKeysOrder, prunedRange, resultIndexRange, settings, usedPrefixLen, ctx);
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
    TExprContext& ctx) const
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
            auto keyBaseType = RemoveAllOptionals(RowType->GetItems()[*idx]->GetItemType());
            if (!(keyBaseType->GetKind() == ETypeAnnotationKind::Data && keyBaseType->IsComparable() && keyBaseType->IsEquatable())) {
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
        prunedRange, Settings, result.UsedPrefixLen, ctx);
    if (result.ComputeNode) {
        result.ExpectedMaxRanges = CalcMaxRanges(rebuiltRange, indexKeysOrder);
        if (result.ExpectedMaxRanges && *result.ExpectedMaxRanges < Settings.MaxRanges) {
            // rebuild filter lambda with prunedRange predicate
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

} // namespace NYql
