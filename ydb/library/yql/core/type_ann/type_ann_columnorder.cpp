#include "type_ann_columnorder.h"

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_join.h>

namespace NYql {
namespace NTypeAnnImpl {

namespace {
void FilterColumnOrderByType(TVector<TString>& columnOrder, const TTypeAnnotationNode& type) {
    TSet<TStringBuf> typeColumns = GetColumnsOfStructOrSequenceOfStruct(type);
    EraseIf(columnOrder, [&](const TString& col) { return !typeColumns.contains(col); });
}

void DivePrefixes(TVector<TString>& columnOrder, const TVector<TString>& prefixes) {
    TVector<TString> outputColumnOrder;
    THashSet<TString> outputSet;
    for (auto& col : columnOrder) {
        for (auto& prefix : prefixes) {
            if (col.StartsWith(prefix)) {
                TString outputColumn = col.substr(prefix.length());
                if (!outputSet.contains(outputColumn)) {
                    outputColumnOrder.push_back(outputColumn);
                    outputSet.insert(outputColumn);
                }
                break;
            }
        }
    }
    std::swap(columnOrder, outputColumnOrder);
}

void AddPrefix(TVector<TString>& columnOrder, const TString& prefix) {
    for (auto& col : columnOrder) {
        col = prefix + col;
    }
}

} // namespace

IGraphTransformer::TStatus OrderForPgSetItem(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Unit) {
        return IGraphTransformer::TStatus::Ok;
    }
    
    TVector<TString> columnOrder;
    auto result = GetSetting(node->Tail(), "result");
    auto emitPgStar = GetSetting(node->Tail(), "emit_pg_star");
    if (result) {
        for (size_t i = 0; i < result->Tail().ChildrenSize(); i++) {
            auto col = result->Tail().Child(i);
            if (col->Head().IsAtom()) {
                auto alias = TString(col->Head().Content());
                YQL_ENSURE(!alias.empty());
                if (!emitPgStar) {
                    columnOrder.push_back(alias);
                }
            }
            else {
                YQL_ENSURE(col->Head().IsList());
                for (const auto& x : col->Head().Children()) {
                    auto alias = TString(x->Content());
                    YQL_ENSURE(!alias.empty());
                    columnOrder.push_back(alias);
                }
            }
        }
    } else {
        auto values = GetSetting(node->Tail(), "values");
        YQL_ENSURE(values);
        TExprNode::TPtr valuesList = values->Child(1);
        for (size_t i = 0; i < valuesList->ChildrenSize(); i++) {
            auto alias = TString(valuesList->Child(i)->Content());
            YQL_ENSURE(!alias.empty());
            columnOrder.push_back(alias);
        }
    }

    return ctx.Types.SetColumnOrder(*node, columnOrder, ctx.Expr);
}

IGraphTransformer::TStatus OrderForAssumeColumnOrder(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    TVector<TString> columnOrder;
    for (auto& col : node->Tail().ChildrenList()) {
        columnOrder.push_back(TString(col->Content()));
    }

    return ctx.Types.SetColumnOrder(*node, columnOrder, ctx.Expr);
}

IGraphTransformer::TStatus OrderForSqlProject(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyList) {
        return IGraphTransformer::TStatus::Ok;
    }

    auto inputOrder = ctx.Types.LookupColumnOrder(node->Head());
    const bool hasStar = AnyOf(node->Child(1)->ChildrenList(),
        [](const TExprNode::TPtr& node) { return node->IsCallable("SqlProjectStarItem"); });

    if (hasStar && !inputOrder) {
        return IGraphTransformer::TStatus::Ok;
    }

    TVector<TString> resultColumnOrder;
    for (const auto& item : node->Child(1)->ChildrenList()) {
        TString name(item->Child(1)->Content());
        if (item->IsCallable("SqlProjectItem")) {
            resultColumnOrder.push_back(name);
            continue;
        }

        YQL_ENSURE(inputOrder);
        TVector<TString> starOutput = *inputOrder;

        if (item->ChildrenSize() < 4) {
            // legacy star without options - column order is not supported
            return IGraphTransformer::TStatus::Ok;
        }

        if (auto dive = GetSetting(*item->Child(3), "divePrefix")) {
            TVector<TString> divePrefixes;
            for (auto& prefix : dive->Child(1)->ChildrenList()) {
                YQL_ENSURE(prefix->IsAtom());
                divePrefixes.push_back(TString(prefix->Content()));
            }
            Sort(divePrefixes, [](const auto& left, const auto& right) { return right < left; });
            DivePrefixes(starOutput, divePrefixes);
        } else if (auto addPrefix = GetSetting(*item->Child(3), "addPrefix")) {
            YQL_ENSURE(addPrefix->Child(1)->IsAtom());
            AddPrefix(starOutput, TString(addPrefix->Child(1)->Content()));
        }

        FilterColumnOrderByType(starOutput, *item->GetTypeAnn());
        resultColumnOrder.insert(resultColumnOrder.end(), starOutput.begin(), starOutput.end());
    }
    return ctx.Types.SetColumnOrder(*node, resultColumnOrder, ctx.Expr);
}

IGraphTransformer::TStatus OrderForMergeExtend(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    YQL_ENSURE(node->ChildrenSize());
    auto common = ctx.Types.LookupColumnOrder(node->Head());
    if (!common) {
        return IGraphTransformer::TStatus::Ok;
    }

    for (ui32 i = 1; i < node->ChildrenSize(); i++) {
        auto current = ctx.Types.LookupColumnOrder(*node->Child(i));
        if (!current || current != common) {
            return IGraphTransformer::TStatus::Ok;
        }
    }

    return ctx.Types.SetColumnOrder(*node, *common, ctx.Expr);
}

IGraphTransformer::TStatus OrderForUnionAll(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    YQL_ENSURE(node->ChildrenSize());
    auto common = ctx.Types.LookupColumnOrder(node->Head());
    if (!common) {
        return IGraphTransformer::TStatus::Ok;
    }

    for (ui32 i = 1; i < node->ChildrenSize(); i++) {
        auto input = node->Child(i);
        auto current = ctx.Types.LookupColumnOrder(*input);
        if (!current) {
            return IGraphTransformer::TStatus::Ok;
        }

        bool truncated = false;
        for (size_t i = 0; i < Min(common->size(), current->size()); ++i) {
            if ((*current)[i] != (*common)[i]) {
                common->resize(i);
                truncated = true;
                break;
            }
        }
        if (!truncated && current->size() > common->size()) {
            common = current;
        }
    }

    if (common->size() > 0) {
        auto allColumns = GetColumnsOfStructOrSequenceOfStruct(*node->GetTypeAnn());
        for (auto& col : *common) {
            auto it = allColumns.find(col);
            YQL_ENSURE(it != allColumns.end());
            allColumns.erase(it);
        }

        for (auto& remain : allColumns) {
            common->push_back(TString(remain));
        }
        return ctx.Types.SetColumnOrder(*node, *common, ctx.Expr);
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus OrderForEquiJoin(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    const size_t numLists = node->ChildrenSize() - 2;
    const auto joinTree = node->Child(numLists);
    const auto optionsNode = node->Child(numLists + 1);;
    TVector<TMaybe<TColumnOrder>> inputColumnOrder;
    TJoinLabels labels;
    for (size_t i = 0; i < numLists; ++i) {
        auto& list = node->Child(i)->Head();
        inputColumnOrder.push_back(ctx.Types.LookupColumnOrder(list));
        if (auto err = labels.Add(ctx.Expr, *node->Child(i)->Child(1),
            list.GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()))
        {
            ctx.Expr.AddError(*err);
            return IGraphTransformer::TStatus::Error;
        }
    }

    TJoinOptions options;
    auto status = ValidateEquiJoinOptions(node->Pos(), *optionsNode, options, ctx.Expr);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }
    if (options.Flatten) {
        // will be done after ExpandFlattenEquiJoin
        return IGraphTransformer::TStatus::Ok;
    }

    auto columnTypes = GetJoinColumnTypes(*joinTree, labels, ctx.Expr);
    YQL_ENSURE(labels.Inputs.size() == inputColumnOrder.size());
    size_t idx = 0;
    TVector<TString> resultColumnOrder;
    for (const auto& label : labels.Inputs) {
        auto columnOrder = inputColumnOrder[idx++];
        if (!columnOrder) {
            bool survivesJoin = AnyOf(label.EnumerateAllColumns(), [&](const TString& outputColumn) {
                return columnTypes.contains(outputColumn);
            });
            if (survivesJoin) {
                // no column order on output
                return IGraphTransformer::TStatus::Ok;
            }
            continue;
        }

        for (auto col : *columnOrder) {
            TString fullName = label.FullName(col);
            if (columnTypes.contains(fullName)) {
                auto it = options.RenameMap.find(fullName);
                if (it != options.RenameMap.end()) {
                    for (auto target : it->second) {
                        resultColumnOrder.push_back(TString(target));
                    }
                } else {
                    resultColumnOrder.push_back(fullName);
                }
            }
        }
    }

    return ctx.Types.SetColumnOrder(*node, resultColumnOrder, ctx.Expr);
};

IGraphTransformer::TStatus OrderForCalcOverWindow(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);

    auto inputOrder = ctx.Types.LookupColumnOrder(node->Head());
    if (!inputOrder) {
        return IGraphTransformer::TStatus::Ok;
    }

    const TStructExprType* inputType = node->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const TStructExprType* outputType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    // we simply add new CalcOverWindow columns after original input columns
    TVector<TString> resultOrder = *inputOrder;
    for (auto& item : outputType->GetItems()) {
        auto col = item->GetName();
        if (!inputType->FindItem(col)) {
            resultOrder.emplace_back(col);
        }
    }

    return ctx.Types.SetColumnOrder(*node, resultOrder, ctx.Expr);
}

IGraphTransformer::TStatus OrderFromFirst(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (auto columnOrder = ctx.Types.LookupColumnOrder(node->Head())) {
        return ctx.Types.SetColumnOrder(*node, *columnOrder, ctx.Expr);
    }
    return IGraphTransformer::TStatus::Ok;
};

IGraphTransformer::TStatus OrderFromFirstAndOutputType(const TExprNode::TPtr& node, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (auto columnOrder = ctx.Types.LookupColumnOrder(node->Head())) {
        FilterColumnOrderByType(*columnOrder, *node->GetTypeAnn());
        return ctx.Types.SetColumnOrder(*node, *columnOrder, ctx.Expr);
    }
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NTypeAnnImpl
} // namespace NYql
