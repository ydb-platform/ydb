#include "kqp_olap_expr_inspection.h"

#include <ydb/library/yql/utils/plan/plan_utils.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <util/generic/hash.h>
#include <util/string/printf.h>
#include <util/string/vector.h>

#include <utility>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

const THashMap<TString, TString>& ComparisonSigns() {
    static const THashMap<TString, TString> signs = {
        {"eq", " == "},
        {"neq", " != "},
        {"lt", " < "},
        {"lte", " <= "},
        {"gt", " > "},
        {"gte", " >= "}
    };
    return signs;
}

const THashMap<TString, TString>& SubstringFormats() {
    static const THashMap<TString, TString> formats = {
        {"string_contains", "%s LIKE \"%%%s%%\""},
        {"starts_with", "%s LIKE \"%s%%\""},
        {"ends_with", "%s LIKE \"%%%s\""}
    };
    return formats;
}

bool IsAtomListComparison(const TCoAtomList& list, TString* columnName = nullptr) {
    const auto listPtr = list.Ptr();
    const size_t listSize = listPtr->Children().size();
    if (listSize != 3 && listSize != 4) {
        return false;
    }

    const auto op = TString(listPtr->Child(0)->Content());
    if (!ComparisonSigns().contains(op) && !SubstringFormats().contains(op)) {
        return false;
    }

    if (columnName) {
        *columnName = TString(listPtr->Child(1)->Content());
    }
    return true;
}

void AddColumn(TOlapFilterInspection& inspection, const TString& columnName) {
    if (!columnName.empty()) {
        inspection.Columns.insert(columnName);
    }
}

void InspectOlapExpressionImpl(const TExprNode::TPtr& node, TOlapFilterInspection& inspection) {
    if (!node) {
        return;
    }

    if (const auto maybeFilter = TMaybeNode<TKqpOlapFilter>(node)) {
        inspection.HasOlapFilter = true;
        const auto filter = maybeFilter.Cast();
        InspectOlapExpressionImpl(filter.Input().Ptr(), inspection);
        InspectOlapExpressionImpl(filter.Condition().Ptr(), inspection);
        return;
    }

    if (const auto maybeProjections = TMaybeNode<TKqpOlapProjections>(node)) {
        inspection.HasOlapProjections = true;
        const auto projections = maybeProjections.Cast();
        InspectOlapExpressionImpl(projections.Input().Ptr(), inspection);
        for (const auto& projectionNode : projections.Projections()) {
            const auto projection = TExprBase(projectionNode).Cast<TKqpOlapProjection>();
            InspectOlapExpressionImpl(projection.OlapOperation().Ptr(), inspection);
        }
        return;
    }

    if (const auto maybeColumnArg = TMaybeNode<TKqpOlapApplyColumnArg>(node)) {
        inspection.HasOlapApply = true;
        AddColumn(inspection, TString(maybeColumnArg.Cast().ColumnName().StringValue()));
        return;
    }

    if (node->IsCallable("Member")) {
        if (node->ChildrenSize() == 2 && node->Child(1)->IsAtom()) {
            AddColumn(inspection, TString(node->Child(1)->Content()));
        }
        return;
    }

    if (const auto maybeJsonValue = TMaybeNode<TKqpOlapJsonValue>(node)) {
        AddColumn(inspection, TString(maybeJsonValue.Cast().Column().StringValue()));
        return;
    }

    if (const auto maybeJsonExists = TMaybeNode<TKqpOlapJsonExists>(node)) {
        AddColumn(inspection, TString(maybeJsonExists.Cast().Column().StringValue()));
        return;
    }

    if (const auto maybeExists = TMaybeNode<TKqpOlapFilterExists>(node)) {
        AddColumn(inspection, TString(maybeExists.Cast().Column().StringValue()));
        return;
    }

    if (const auto maybeUnaryOp = TMaybeNode<TKqpOlapFilterUnaryOp>(node)) {
        InspectOlapExpressionImpl(maybeUnaryOp.Cast().Arg().Ptr(), inspection);
        return;
    }

    if (const auto maybeBinaryOp = TMaybeNode<TKqpOlapFilterBinaryOp>(node)) {
        const auto binaryOp = maybeBinaryOp.Cast();
        InspectOlapExpressionImpl(binaryOp.Left().Ptr(), inspection);
        InspectOlapExpressionImpl(binaryOp.Right().Ptr(), inspection);
        return;
    }

    if (const auto maybeTernaryOp = TMaybeNode<TKqpOlapFilterTernaryOp>(node)) {
        const auto ternaryOp = maybeTernaryOp.Cast();
        InspectOlapExpressionImpl(ternaryOp.First().Ptr(), inspection);
        InspectOlapExpressionImpl(ternaryOp.Second().Ptr(), inspection);
        InspectOlapExpressionImpl(ternaryOp.Third().Ptr(), inspection);
        return;
    }

    if (const auto maybeNot = TMaybeNode<TKqpOlapNot>(node)) {
        InspectOlapExpressionImpl(maybeNot.Cast().Value().Ptr(), inspection);
        return;
    }

    if (TKqpOlapAnd::Match(node.Get()) || TKqpOlapOr::Match(node.Get()) || TKqpOlapXor::Match(node.Get())) {
        for (const auto& child : node->ChildrenList()) {
            InspectOlapExpressionImpl(child, inspection);
        }
        return;
    }

    if (const auto maybeAtomList = TMaybeNode<TCoAtomList>(node)) {
        TString columnName;
        if (IsAtomListComparison(maybeAtomList.Cast(), &columnName)) {
            AddColumn(inspection, columnName);
            return;
        }

        for (const auto& child : node->ChildrenList()) {
            InspectOlapExpressionImpl(child, inspection);
        }
        return;
    }

    if (const auto maybeApply = TMaybeNode<TKqpOlapApply>(node)) {
        inspection.HasOlapApply = true;
        InspectOlapExpressionImpl(maybeApply.Cast().Lambda().Ptr(), inspection);
        return;
    }

    for (const auto& child : node->ChildrenList()) {
        InspectOlapExpressionImpl(child, inspection);
    }
}

TString AtomListValue(const TExprNode& node, bool quoteString) {
    if (node.ChildrenSize() >= 1) {
        auto value = TString(node.Child(0)->Content());
        if (quoteString && TString(node.Content()) == "String") {
            value = TStringBuilder() << '"' << value << '"';
        }
        return value;
    }

    if (node.ChildrenSize() == 0 && node.Content()) {
        return TString(node.Content());
    }

    return {};
}

TString FormatOlapUdfArgument(const TExprBase& argument) {
    if (const auto column = argument.Maybe<TKqpOlapApplyColumnArg>()) {
        return TStringBuilder() << '`' << column.Cast().ColumnName().StringValue() << '`';
    }
    return NPlanUtils::PrettyExprStr(argument);
}

TString FormatOlapUdf(const TKqpOlapUdf& udf) {
    TVector<TString> args;
    for (const auto& argument : udf.Args()) {
        args.emplace_back(FormatOlapUdfArgument(TExprBase(argument)));
    }
    return TStringBuilder() << "Udf(" << udf.KernelName().StringValue() << ")(" << JoinStrings(args, ", ") << ')';
}

TString FormatOlapFilterExpr(const TExprNode::TPtr& node) {
    if (!node) {
        return {};
    }

    TVector<TString> parts;
    if (TMaybeNode<TKqpOlapNot>(node)) {
        parts.emplace_back("NOT");
    } else if (auto maybeList = TMaybeNode<TCoAtomList>(node)) {
        auto listPtr = maybeList.Cast().Ptr();
        TString columnName;
        if (IsAtomListComparison(maybeList.Cast(), &columnName)) {
            const auto op = TString(listPtr->Child(0)->Content());

            if (ComparisonSigns().contains(op)) {
                const auto value = AtomListValue(*listPtr->Child(2), true);
                return TStringBuilder() << columnName << ComparisonSigns().at(op) << value;
            }
            if (SubstringFormats().contains(op)) {
                const auto value = AtomListValue(*listPtr->Child(2), false);
                return Sprintf(SubstringFormats().at(op).c_str(), columnName.c_str(), value.c_str());
            }
        }
    } else if (auto olapApply = TMaybeNode<TKqpOlapApply>(node)) {
        try {
            return NPlanUtils::ExtractPredicate(olapApply.Cast().Lambda()).Body;
        } catch (...) {
            return {};
        }
    } else if (auto olapUdf = TMaybeNode<TKqpOlapUdf>(node)) {
        return FormatOlapUdf(olapUdf.Cast());
    }

    for (const auto& child : node->Children()) {
        auto childStr = FormatOlapFilterExpr(child);
        if (!childStr.empty()) {
            parts.push_back(std::move(childStr));
        }
    }

    TString delim = " ";
    if (TMaybeNode<TKqpOlapAnd>(node)) {
        delim = " AND ";
    } else if (TMaybeNode<TKqpOlapOr>(node)) {
        delim = " OR ";
    }
    return JoinStrings(parts, delim);
}

bool IsOlapColumnAtomPosition(const TExprNode& node, ui32 index) {
    if (node.IsCallable("KqpOlapProjection")) {
        return index == TKqpOlapProjection::idx_ColumnName || index == TKqpOlapProjection::idx_OlapOperation;
    }
    if (node.IsCallable("KqpOlapFilter")) {
        return index == TKqpOlapFilter::idx_Condition;
    }
    if (node.IsCallable("KqpOlapNot") || node.IsCallable("KqpOlapAnd") ||
        node.IsCallable("KqpOlapOr") || node.IsCallable("KqpOlapXor")) {
        return true;
    }
    if (node.IsCallable("SafeCast")) {
        return index == TCoSafeCast::idx_Value;
    }
    if (node.IsCallable("KqpOlapApplyColumnArg")) {
        return index == TKqpOlapApplyColumnArg::idx_ColumnName;
    }

    if (node.IsCallable("KqpOlapJsonValue") || node.IsCallable("KqpOlapJsonExists")) {
        return index == TKqpOlapJsonValue::idx_Column;
    }

    if (!node.IsList()) {
        return false;
    }

    const auto size = node.ChildrenSize();
    if (size == 1) {
        return index == 0;
    }
    return size <= 5 && index > 0 && node.Child(0)->IsAtom();
}

TExprNode::TPtr RenameColumnAtom(const TExprNode::TPtr& atom, const THashMap<TString, TString>& renameMap, TExprContext& ctx) {
    const auto it = renameMap.find(TString(atom->Content()));
    return it == renameMap.end() ? atom : ctx.NewAtom(atom->Pos(), it->second);
}

TExprNode::TPtr RenameColumnsImpl(const TExprNode::TPtr& node, const THashMap<TString, TString>& renameMap,
    TExprContext& ctx, const TExprNode* rowArgument)
{
    if (!node) {
        return node;
    }

    if (node->IsLambda()) {
        // Kernel lambdas are handled separately below: their fields are user data.
        Y_ENSURE(node->Head().ChildrenSize() == 1);
        auto body = RenameColumnsImpl(node->TailPtr(), renameMap, ctx, &node->Head().Head());
        return body == node->TailPtr() ? node : ctx.ChangeChild(*node, 1, std::move(body));
    }

    if (node->IsCallable("Member")) {
        auto input = RenameColumnsImpl(node->HeadPtr(), renameMap, ctx, rowArgument);
        auto name = &node->Head() == rowArgument ? RenameColumnAtom(node->TailPtr(), renameMap, ctx) : node->TailPtr();
        return input == node->HeadPtr() && name == node->TailPtr()
            ? node : ctx.ChangeChildren(*node, {std::move(input), std::move(name)});
    }

    if (const auto column = TMaybeNode<TKqpOlapApplyColumnArg>(node)) {
        // Only the top-level fields describe bindings. Nested StructTypes are values.
        const auto rowType = column.Cast().TableRowType().Ptr();
        Y_ENSURE(rowType->IsCallable("StructType"), "Expected expanded OLAP input row type");
        auto items = rowType->ChildrenList();
        for (auto& item : items) {
            auto name = RenameColumnAtom(item->HeadPtr(), renameMap, ctx);
            if (name != item->HeadPtr()) {
                item = ctx.ChangeChild(*item, 0, std::move(name));
            }
        }
        auto type = items == rowType->ChildrenList() ? rowType : ctx.ChangeChildren(*rowType, std::move(items));
        auto name = RenameColumnAtom(column.Cast().ColumnName().Ptr(), renameMap, ctx);
        return type == rowType && name == column.Cast().ColumnName().Ptr()
            ? node : ctx.ChangeChildren(*node, {std::move(type), std::move(name)});
    }

    if (TCoDataCtor::Match(node.Get()) || TCoParameter::Match(node.Get()) || node->IsCallable("StructType")) {
        return node;
    }

    bool changed = false;
    auto children = node->ChildrenList();
    for (ui32 i = 0; i < children.size(); ++i) {
        // Apply's lambda operates on column values; its local fields and literal
        // lists are not table bindings. Only Args belongs to the OLAP namespace.
        if (TKqpOlapApply::Match(node.Get()) && i != TKqpOlapApply::idx_Args) {
            continue;
        }
        if (children[i]->IsAtom() && IsOlapColumnAtomPosition(*node, i)) {
            auto renamed = RenameColumnAtom(children[i], renameMap, ctx);
            if (renamed != children[i]) {
                children[i] = std::move(renamed);
                changed = true;
            }
            continue;
        }

        auto renamed = RenameColumnsImpl(children[i], renameMap, ctx, rowArgument);
        if (renamed != children[i]) {
            children[i] = std::move(renamed);
            changed = true;
        }
    }

    return changed ? ctx.ChangeChildren(*node, std::move(children)) : node;
}

} // anonymous namespace

TOlapFilterInspection TOlapFilterInspector::Inspect(const TExprNode::TPtr& node) {
    TOlapFilterInspection inspection;
    InspectOlapExpressionImpl(node, inspection);
    return inspection;
}

TOlapFilterInspection TOlapFilterInspector::InspectLambda(const TExprNode::TPtr& lambda) {
    TOlapFilterInspection inspection;
    if (!lambda) {
        return inspection;
    }

    try {
        InspectOlapExpressionImpl(lambda, inspection);
    } catch (...) {
        inspection.RequiresAllInputColumns = true;
    }
    return inspection;
}

TExprNode::TPtr TOlapFilterInspector::RenameColumns(
    const TExprNode::TPtr& node,
    const THashMap<TString, TString>& renameMap,
    TExprContext& ctx)
{
    if (renameMap.empty()) {
        return node;
    }
    return RenameColumnsImpl(node, renameMap, ctx, nullptr);
}

TString TOlapFilterInspector::Format(const TKqpOlapFilter& filter) {
    auto result = FormatOlapFilterExpr(filter.Condition().Ptr());
    if (auto maybeInnerFilter = TMaybeNode<TKqpOlapFilter>(filter.Input().Ptr())) {
        return TStringBuilder() << '(' << Format(maybeInnerFilter.Cast()) << ") AND (" << result << ')';
    }
    return result;
}

TOlapFilterInspection InspectOlapExpression(const TExprNode::TPtr& node) {
    return TOlapFilterInspector::Inspect(node);
}

TOlapFilterInspection InspectOlapProcessLambda(const TExprNode::TPtr& lambda) {
    return TOlapFilterInspector::InspectLambda(lambda);
}

TString FormatOlapFilter(const TKqpOlapFilter& filter) {
    return TOlapFilterInspector::Format(filter);
}

} // namespace NKikimr::NKqp::NOpt
