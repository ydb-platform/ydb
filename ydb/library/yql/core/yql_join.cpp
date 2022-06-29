#include "yql_join.h"
#include "yql_expr_type_annotation.h"
#include "yql_opt_utils.h"

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/type.h>

namespace NYql {

using namespace NNodes;

namespace {
    const TTypeAnnotationNode* AddOptionalType(const TTypeAnnotationNode* type, TExprContext& ctx) {
        if (type->IsOptionalOrNull()) {
            return type;
        }

        return ctx.MakeType<TOptionalExprType>(type);
    }

    struct TJoinState {
        bool Used = false;
    };

    IGraphTransformer::TStatus ParseJoinKeys(const TExprNode& side, TVector<std::pair<TStringBuf, TStringBuf>>& keys,
        TVector<const TTypeAnnotationNode*>& keyTypes, const TJoinLabels& labels,
        TExprContext& ctx, bool isCross) {
        if (!EnsureTuple(side, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (auto& child : side.Children()) {
            if (!EnsureAtom(*child, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (isCross) {
            if (side.ChildrenSize() != 0) {
                ctx.AddError(TIssue(ctx.GetPosition(side.Pos()),
                    TStringBuilder() << "Expected empty list"));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (side.ChildrenSize() < 2 || (side.ChildrenSize() % 2) != 0) {
                ctx.AddError(TIssue(ctx.GetPosition(side.Pos()),
                    TStringBuilder() << "Expected non-empty list of atoms with even length"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        keys.clear();
        for (ui32 i = 0; i < side.ChildrenSize(); i += 2) {
            auto table = side.Child(i)->Content();
            auto column = side.Child(i + 1)->Content();
            auto key = std::make_pair(table, column);
            keys.push_back(key);
        }

        for (auto& key : keys) {
            auto keyType = labels.FindColumn(key.first, key.second);
            if (!keyType) {
                ctx.AddError(TIssue(ctx.GetPosition(side.Pos()),
                    TStringBuilder() << "Unknown column: " << key.second << " in correlation name: " << key.first));
                return IGraphTransformer::TStatus::Error;
            }

            if (!(*keyType)->IsHashable() || !(*keyType)->IsEquatable()) {
                ctx.AddError(TIssue(ctx.GetPosition(side.Pos()),
                    TStringBuilder() << "Unsupported type of column: " << key.second << " in correlation name: " << key.first
                    << ", type: " << *(*keyType)));
                return IGraphTransformer::TStatus::Error;
            }

            keyTypes.push_back(*keyType);
        }

        return IGraphTransformer::TStatus::Ok;
    }

    struct TGLobalJoinState {
        ui32 NestedJoins = 0;
    };

    IGraphTransformer::TStatus ParseJoins(const TJoinLabels& labels,
        const TExprNode& joins, TVector<TJoinState>& joinsStates, THashSet<TStringBuf>& scope,
        TGLobalJoinState& globalState, bool strictKeys, TExprContext& ctx);

    bool AddEquiJoinLinkOptionHint(TStringBuf side, TSet<TStringBuf>& hints, const TExprNode& hintNode, TExprContext& ctx) {
        if (!EnsureAtom(hintNode, ctx)) {
            return false;
        }

        auto hint = hintNode.Content();
        auto pos = ctx.GetPosition(hintNode.Pos());
        if (hint == "unique" || hint == "small") {
            if (hints.contains(hint == "small" ? "unique" : "small")) {
                ctx.AddError(TIssue(pos, TStringBuilder() << "Hints 'unique' and 'small' are not compatible"));
                return false;
            }
            hints.insert(hint);
        } else if (hint == "any") {
            hints.insert(hint);
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Unknown hint: '" << hint << "' for " << side << " side"));
            return false;
        }

        return true;
    }

    IGraphTransformer::TStatus ParseJoinScope(const TJoinLabels& labels,
        const TExprNode& side, TVector<TJoinState>& joinsStates, THashSet<TStringBuf>& scope,
        TGLobalJoinState& globalState, bool strictKeys, TExprContext& ctx) {
        if (side.IsAtom()) {
            auto label = side.Content();
            auto input = labels.FindInput(label);
            if (!input) {
                ctx.AddError(TIssue(ctx.GetPosition(side.Pos()),
                    TStringBuilder() << "Unknown correlation name: " << label));
                return IGraphTransformer::TStatus::Error;
            }

            for (auto& x : (*input)->Tables) {
                scope.insert(x);
            }

            return IGraphTransformer::TStatus::Ok;
        }

        if (globalState.NestedJoins + 2 == labels.Inputs.size()) {
            ctx.AddError(TIssue(ctx.GetPosition(side.Pos()),
                TStringBuilder() << "Too many nested joins, expected exactly: " << (labels.Inputs.size() - 2)));
            return IGraphTransformer::TStatus::Error;
        }

        ++globalState.NestedJoins;
        return ParseJoins(labels, side, joinsStates, scope, globalState, strictKeys, ctx);
    }

    IGraphTransformer::TStatus ParseJoins(const TJoinLabels& labels,
        const TExprNode& joins, TVector<TJoinState>& joinsStates, THashSet<TStringBuf>& scope,
        TGLobalJoinState& globalState, bool strictKeys, TExprContext& ctx) {
        if (!EnsureTupleSize(joins, 6, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*joins.Child(0), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto joinType = joins.Child(0)->Content();
        if (joinType != "Inner" && joinType != "Left" && joinType != "Right" && joinType != "Full"
            && joinType != "LeftOnly" && joinType != "RightOnly" && joinType != "Exclusion"
            && joinType != "LeftSemi" && joinType != "RightSemi" && joinType != "Cross") {
            ctx.AddError(TIssue(ctx.GetPosition(joins.Child(0)->Pos()), TStringBuilder() << "Unsupported join type: " << joinType));
            return IGraphTransformer::TStatus::Error;
        }

        THashSet<TStringBuf> myLeftScope;
        THashSet<TStringBuf> myRightScope;
        auto status = ParseJoinScope(labels, *joins.Child(1), joinsStates, myLeftScope, globalState, strictKeys, ctx);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        status = ParseJoinScope(labels, *joins.Child(2), joinsStates, myRightScope, globalState, strictKeys, ctx);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        TVector<std::pair<TStringBuf, TStringBuf>> leftKeys;
        TVector<const TTypeAnnotationNode*> leftKeyTypes;
        status = ParseJoinKeys(*joins.Child(3), leftKeys, leftKeyTypes, labels, ctx, joinType == "Cross");
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        for (auto& x : leftKeys) {
            if (!myLeftScope.contains(x.first)) {
                ctx.AddError(TIssue(ctx.GetPosition(joins.Pos()),
                    TStringBuilder() << "Correlation name " << x.first << " is out of scope"));
                return IGraphTransformer::TStatus::Error;
            }

            auto leftIndex = *labels.FindInputIndex(x.first);
            joinsStates[leftIndex].Used = true;
        }

        TVector<std::pair<TStringBuf, TStringBuf>> rightKeys;
        TVector<const TTypeAnnotationNode*> rightKeyTypes;
        status = ParseJoinKeys(*joins.Child(4), rightKeys, rightKeyTypes, labels, ctx, joinType == "Cross");
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        for (auto& x : rightKeys) {
            if (!myRightScope.contains(x.first)) {
                ctx.AddError(TIssue(ctx.GetPosition(joins.Pos()),
                    TStringBuilder() << "Correlation name " << x.first << " is out of scope"));
                return IGraphTransformer::TStatus::Error;
            }

            auto rightIndex = *labels.FindInputIndex(x.first);
            joinsStates[rightIndex].Used = true;
        }

        if (leftKeys.size() != rightKeys.size()) {
            ctx.AddError(TIssue(ctx.GetPosition(joins.Pos()),
                TStringBuilder() << "Mismatch of key column count in equality between " << leftKeys[0].first
                << " and " << rightKeys[0].first));
            return IGraphTransformer::TStatus::Error;
        }

        for (ui32 i = 0; i < leftKeyTypes.size(); ++i) {
            if (strictKeys && leftKeyTypes[i] != rightKeyTypes[i]) {
                ctx.AddError(TIssue(ctx.GetPosition(joins.Pos()),
                    TStringBuilder() << "Strict key type match requested, but keys have different types: ("
                    << leftKeys[i].first << "." << leftKeys[i].second
                    << " has type: " << *leftKeyTypes[i] << ", " << rightKeys[i].first << "." << rightKeys[i].second
                    << " has type: " << *rightKeyTypes[i] << ")"));
                return IGraphTransformer::TStatus::Error;
            }
            if (ECompareOptions::Uncomparable == CanCompare<true>(leftKeyTypes[i], rightKeyTypes[i])) {
                ctx.AddError(TIssue(ctx.GetPosition(joins.Pos()),
                    TStringBuilder() << "Cannot compare key columns (" << leftKeys[i].first << "." << leftKeys[i].second
                    << " has type: " << *leftKeyTypes[i] << ", " << rightKeys[i].first << "." << rightKeys[i].second
                    << " has type: " << *rightKeyTypes[i] << ")"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (joinType == "Cross") {
            for (auto& x : myLeftScope) {
                auto leftIndex = *labels.FindInputIndex(x);
                joinsStates[leftIndex].Used = true;
            }

            for (auto& x : myRightScope) {
                auto rightIndex = *labels.FindInputIndex(x);
                joinsStates[rightIndex].Used = true;
            }
        }

        scope.clear();
        if (joinType != "RightOnly" && joinType != "RightSemi") {
            scope.insert(myLeftScope.begin(), myLeftScope.end());
        }

        if (joinType != "LeftOnly" && joinType != "LeftSemi") {
            scope.insert(myRightScope.begin(), myRightScope.end());
        }

        auto linkOptions = joins.Child(5);
        if (!EnsureTuple(*linkOptions, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        TMaybe<TSet<TStringBuf>> leftHints;
        TMaybe<TSet<TStringBuf>> rightHints;
        bool forceSortedMerge = false;
        for (auto child : linkOptions->Children()) {
            if (!EnsureTupleMinSize(*child, 1, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(*child->Child(0), ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto optionName = child->Child(0)->Content();
            if (optionName == "left" || optionName == "right") {
                if (!EnsureTupleSize(*child, 2, ctx)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto& hints = optionName == "left" ? leftHints : rightHints;
                if (hints) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Child(0)->Pos()), TStringBuilder() <<
                        "Duplication of hints for " << optionName << " side"));
                    return IGraphTransformer::TStatus::Error;
                }

                hints.ConstructInPlace();
                if (child->Child(1)->IsAtom()) {
                    if (!AddEquiJoinLinkOptionHint(optionName, *hints, *child->Child(1), ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                } else {
                    if (!EnsureTuple(*child->Child(1), ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    for (auto hint : child->Child(1)->Children()) {
                        if (!AddEquiJoinLinkOptionHint(optionName, *hints, *hint, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                }
            }
            else if (optionName == "forceSortedMerge") {
                if (!EnsureTupleSize(*child, 1, ctx)) {
                    return IGraphTransformer::TStatus::Error;
                }
                if (forceSortedMerge) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Child(0)->Pos()), TStringBuilder() <<
                        "Duplicate " << optionName << " link option"));
                    return IGraphTransformer::TStatus::Error;
                }
                forceSortedMerge = true;
            }
            else {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() <<
                    "Unknown option name: " << optionName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        return IGraphTransformer::TStatus::Ok;
    }

    struct TFlattenState {
        TString Table;
        TVector<const TTypeAnnotationNode*> AllTypes;
    };

    void CollectEquiJoinKeyColumnsFromLeaf(const TExprNode& columns, THashMap<TStringBuf, THashSet<TStringBuf>>& tableKeysMap) {
        YQL_ENSURE(columns.ChildrenSize() % 2 == 0);
        for (ui32 i = 0; i < columns.ChildrenSize(); i += 2) {
            auto table = columns.Child(i)->Content();
            auto column = columns.Child(i + 1)->Content();
            tableKeysMap[table].insert(column);
        }
    }

    void CollectEquiJoinKeyColumns(const TExprNode& joinTree, THashMap<TStringBuf, THashSet<TStringBuf>>& tableKeysMap) {
        auto& left = *joinTree.Child(1);
        if (!left.IsAtom()) {
            CollectEquiJoinKeyColumns(left, tableKeysMap);
        }

        auto& right = *joinTree.Child(2);
        if (!right.IsAtom()) {
            CollectEquiJoinKeyColumns(right, tableKeysMap);
        }

        CollectEquiJoinKeyColumnsFromLeaf(*joinTree.Child(3), tableKeysMap);
        CollectEquiJoinKeyColumnsFromLeaf(*joinTree.Child(4), tableKeysMap);
    }

    bool CollectEquiJoinOnlyParents(const TExprNode& current, const TExprNode* prev, ui32 depth,
                                    TVector<TEquiJoinParent>& results, const TExprNode* extractMembersInScope,
                                    const TParentsMap& parents)
    {
        if (depth == 0) {
            if (!prev || !TCoEquiJoin::Match(&current)) {
                return false;
            }

            TCoEquiJoin equiJoin(&current);
            for (ui32 i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
                auto joinInput = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
                auto list = joinInput.List();
                if (list.Raw() == prev) {
                    results.emplace_back(equiJoin.Raw(), i, extractMembersInScope);
                    return true;
                }
            }
            YQL_ENSURE(false, "Unable to locate FlatMap in EquiJoin");
        }

        auto it = parents.find(&current);
        if (it == parents.end() || it->second.empty()) {
            return false;
        }

        const TExprNode* extractMembers = extractMembersInScope;
        bool currentIsExtractMembers = TCoExtractMembers::Match(&current);
        if (currentIsExtractMembers) {
            if (extractMembers) {
                // repeatable extract members should not actually happen
                return false;
            }
            extractMembers = current.Child(1);
        }

        auto nextPrev = (TCoFlatMapBase::Match(&current) || currentIsExtractMembers) ? &current : prev;

        for (auto parent : it->second) {
            if (!CollectEquiJoinOnlyParents(*parent, nextPrev, currentIsExtractMembers ? depth : (depth - 1), results,
                                            extractMembers, parents))
            {
                return false;
            }
        }

        return true;
    }
}

TMaybe<TIssue> TJoinLabel::Parse(TExprContext& ctx, TExprNode& node, const TStructExprType* structType) {
    Tables.clear();
    InputType = structType;
    if (auto atom = TMaybeNode<TCoAtom>(&node)) {
        if (auto err = ValidateLabel(ctx, atom.Cast())) {
            return err;
        }

        AddLabel = true;
        Tables.push_back(atom.Cast().Value());
        return {};
    }
    else if (auto tuple = TMaybeNode<TCoAtomList>(&node)) {
        if (tuple.Cast().Size() == 0) {
            return TIssue(ctx.GetPosition(node.Pos()), "Empty list of correlation names are not allowed");
        }

        for (const auto& child : tuple.Cast()) {
            if (auto err = ValidateLabel(ctx, child)) {
                return err;
            }

            Tables.push_back(child.Value());
        }

        Sort(Tables);
        auto prevLabel = Tables[0];
        for (ui32 i = 1; i < Tables.size(); ++i) {
            if (Tables[i] == prevLabel) {
                return TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Duplication of correlation names: " << prevLabel);
            }

            prevLabel = Tables[i];
        }

        // all labels are unique, ensure that all columns are under one of label
        for (auto column : InputType->GetItems()) {
            auto name = column->GetName();
            auto pos = name.find('.');
            if (pos == TString::npos) {
                return TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected columns name as table.name, but got: " << name);
            }

            auto table = name.substr(0, pos);
            if (!BinarySearch(Tables.begin(), Tables.end(), table)) {
                return TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Unknown table name: " << table);
            }

            auto columnName = name.substr(pos + 1);
            if (columnName.empty()) {
                return TIssue(ctx.GetPosition(node.Pos()), "Empty correlation name is not allowed");
            }
        }

        return {};
    }
    else {
        return TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected either atom or list, but got" << node.Type());
    }
}

TMaybe<TIssue> TJoinLabel::ValidateLabel(TExprContext& ctx, const TCoAtom& label) {
    if (label.Value().empty()) {
        return TIssue(ctx.GetPosition(label.Pos()), "Empty correlation name is not allowed");
    }

    if (label.Value().Contains('.')) {
        return TIssue(ctx.GetPosition(label.Pos()), "Dot symbol is not allowed in the correlation name");
    }

    return {};
}

TString TJoinLabel::FullName(const TStringBuf& column) const {
    if (AddLabel) {
        return FullColumnName(Tables[0], column);
    } else {
        return TString(column);
    }
}

TStringBuf TJoinLabel::ColumnName(const TStringBuf& column) const {
    auto pos = column.find('.');
    if (pos == TString::npos) {
        return column;
    }

    return column.substr(pos + 1);
}

TStringBuf TJoinLabel::TableName(const TStringBuf& column) const {
    auto pos = column.find('.');
    if (pos == TString::npos) {
        YQL_ENSURE(AddLabel);
        return Tables[0];
    }

    return column.substr(0, pos);
}

bool TJoinLabel::HasTable(const TStringBuf& table) const {
    return BinarySearch(Tables.begin(), Tables.end(), table);
}

TMaybe<const TTypeAnnotationNode*> TJoinLabel::FindColumn(const TStringBuf& table, const TStringBuf& column) const {
    auto pos = InputType->FindItem(MemberName(table, column));
    if (!pos) {
        return TMaybe<const TTypeAnnotationNode*>();
    }

    return InputType->GetItems()[*pos]->GetItemType();
}

TString TJoinLabel::MemberName(const TStringBuf& table, const TStringBuf& column) const {
    return AddLabel ? TString(column) : FullColumnName(table, column);
}

TVector<TString> TJoinLabel::EnumerateAllColumns() const {
    TVector<TString> result;
    if (AddLabel) {
        // add label to all columns
        for (auto& x : InputType->GetItems()) {
            result.push_back(FullColumnName(Tables[0], x->GetName()));
        }
    } else {
        for (auto& x : InputType->GetItems()) {
            result.push_back(TString(x->GetName()));
        }
    }

    return result;
}

TVector<TString> TJoinLabel::EnumerateAllMembers() const {
    TVector<TString> result;
    for (auto& x : InputType->GetItems()) {
        result.push_back(TString(x->GetName()));
    }

    return result;
}

TMaybe<TIssue> TJoinLabels::Add(TExprContext& ctx, TExprNode& node, const TStructExprType* structType) {
    ui32 index = Inputs.size();
    Inputs.emplace_back();
    TJoinLabel& label = Inputs.back();
    if (auto err = label.Parse(ctx, node, structType)) {
        return err;
    }

    for (auto& table : label.Tables) {
        if (!InputByTable.insert({ table, index }).second) {
            return TIssue(
                ctx.GetPosition(node.Pos()),
                TStringBuilder() << "Duplication of table name " << table);
        }
    }

    return {};
}

TMaybe<const TJoinLabel*> TJoinLabels::FindInput(const TStringBuf& table) const {
    auto inputIndex = InputByTable.FindPtr(table);
    if (!inputIndex) {
        return {};
    }

    return &Inputs[*inputIndex];
}

TMaybe<ui32> TJoinLabels::FindInputIndex(const TStringBuf& table) const {
    auto inputIndex = InputByTable.FindPtr(table);
    if (!inputIndex) {
        return{};
    }

    return *inputIndex;
}

TMaybe<const TTypeAnnotationNode*> TJoinLabels::FindColumn(const TStringBuf& table, const TStringBuf& column) const {
    auto tableIndex = InputByTable.FindPtr(table);
    if (!tableIndex) {
        return TMaybe<const TTypeAnnotationNode*>();
    }

    return Inputs[*tableIndex].FindColumn(table, column);
}

TMaybe<const TTypeAnnotationNode*> TJoinLabels::FindColumn(const TStringBuf& fullName) const {
    TStringBuf part1;
    TStringBuf part2;
    SplitTableName(fullName, part1, part2);
    return FindColumn(part1, part2);
}

TVector<TString> TJoinLabels::EnumerateColumns(const TStringBuf& table) const {
    TVector<TString> result;
    auto tableIndex = InputByTable.FindPtr(table);
    Y_ENSURE(tableIndex, "Unknown table:" << table);
    auto& label = Inputs[*tableIndex];
    if (label.AddLabel) {
        // add label to all columns
        for (auto& x : label.InputType->GetItems()) {
            result.push_back(FullColumnName(table, x->GetName()));
        }
    }
    else {
        // filter out some columns
        for (auto& x : label.InputType->GetItems()) {
            TStringBuf part1;
            TStringBuf part2;
            SplitTableName(x->GetName(), part1, part2);
            if (part1 == table) {
                result.push_back(TString(x->GetName()));
            }
        }
    }

    return result;
}

IGraphTransformer::TStatus ValidateEquiJoinOptions(TPositionHandle positionHandle, const TExprNode& optionsNode,
    TJoinOptions& options, TExprContext& ctx)
{
    auto position = ctx.GetPosition(positionHandle);
    if (!EnsureTuple(optionsNode, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    options = TJoinOptions{};

    THashSet<TStringBuf> renameTargetSet;
    bool hasRename = false;
    for (auto child : optionsNode.Children()) {
        if (!EnsureTupleMinSize(*child, 1, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*child->Child(0), ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto optionName = child->Child(0)->Content();
        if (optionName == "rename") {
            hasRename = true;
            if (!EnsureTupleSize(*child, 3, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(*child->Child(1), ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(*child->Child(2), ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            auto& v = options.RenameMap[child->Child(1)->Content()];
            if (!child->Child(2)->Content().empty()) {
                if (!renameTargetSet.insert(child->Child(2)->Content()).second) {
                    ctx.AddError(TIssue(position, TStringBuilder() <<
                        "Duplicated target column: " << child->Child(2)->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                v.push_back(child->Child(2)->Content());
            }
        } else if (optionName == "flatten") {
            options.Flatten = true;
        } else if (optionName == "keep_sys") {
            options.KeepSysColumns = true;
        } else if (optionName == "strict_keys") {
            options.StrictKeys = true;
        } else if (optionName == "preferred_sort") {
            THashSet<TStringBuf> sortBySet;
            TVector<TStringBuf> sortBy;
            if (!EnsureTupleSize(*child, 2, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!EnsureTupleMinSize(*child->Child(1), 1, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            for (auto column : child->Child(1)->Children()) {
                if (!EnsureAtom(*column, ctx)) {
                    return IGraphTransformer::TStatus::Error;
                }
                if (!sortBySet.insert(column->Content()).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(column->Pos()), TStringBuilder() <<
                        "Duplicated preferred_sort column: " << column->Content()));
                    return IGraphTransformer::TStatus::Error;
                }
                sortBy.push_back(column->Content());
            }
            if (!options.PreferredSortSets.insert(sortBy).second) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Child(1)->Pos()), TStringBuilder() <<
                    "Duplicated preferred_sort set: " << JoinSeq(", ", sortBy)));
            }
        } else {
            ctx.AddError(TIssue(position, TStringBuilder() <<
                "Unknown option name: " << optionName));
            return IGraphTransformer::TStatus::Error;
        }

        if (hasRename && options.Flatten) {
            ctx.AddError(TIssue(position, TStringBuilder() <<
                "Options flatten and rename are incompatible with each other"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus EquiJoinAnnotation(
    TPositionHandle positionHandle,
    const TStructExprType*& resultType,
    const TJoinLabels& labels,
    const TExprNode& joins,
    const TJoinOptions& options,
    TExprContext& ctx
) {
    auto position = ctx.GetPosition(positionHandle);

    if (labels.InputByTable.size() < 2) {
        ctx.AddError(TIssue(position, TStringBuilder() << "Expected at least 2 table"));
        return IGraphTransformer::TStatus::Error;
    }

    TVector<TJoinState> joinsStates(labels.Inputs.size());
    TGLobalJoinState globalState;
    THashSet<TStringBuf> scope;
    auto parseStatus = ParseJoins(labels, joins, joinsStates, scope, globalState, options.StrictKeys, ctx);
    if (parseStatus.Level != IGraphTransformer::TStatus::Ok) {
        return parseStatus;
    }

    if (globalState.NestedJoins + 2 != labels.Inputs.size()) {
        ctx.AddError(TIssue(position,
            TStringBuilder() << "Too few nested joins, expected exactly: " << (labels.Inputs.size() - 2)));
        return IGraphTransformer::TStatus::Error;
    }

    for (ui32 i = 0; i < joinsStates.size(); ++i) {
        if (!joinsStates[i].Used) {
            ctx.AddError(TIssue(position, TStringBuilder() <<
                "Input with correlation name(s) " << JoinSeq(", ", labels.Inputs[i].Tables) << " was not used"));
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto columnTypes = GetJoinColumnTypes(joins, labels, ctx);
    TVector<const TItemExprType*> resultFields;
    TMap<TString, TFlattenState> flattenFields; // column -> table
    THashSet<TString> processedRenames;
    for (auto it: labels.Inputs) {
        for (auto item: it.InputType->GetItems()) {
            TString fullName = it.FullName(item->GetName());
            auto type = columnTypes.FindPtr(fullName);
            if (type) {
                TVector<TStringBuf> fullNames;
                fullNames.push_back(fullName);
                if (!processedRenames.contains(fullName)) {
                    auto renameIt = options.RenameMap.find(fullName);
                    if (renameIt != options.RenameMap.end()) {
                        fullNames = renameIt->second;
                        processedRenames.insert(fullName);
                    }
                }

                for (auto& fullName: fullNames) {
                    if (options.Flatten) {
                        auto tableName = it.TableName(fullName);
                        auto columnName = it.ColumnName(fullName);
                        auto iter = flattenFields.find(columnName);
                        if (iter != flattenFields.end()) {
                            if (AreSameJoinKeys(joins, tableName, columnName, iter->second.Table, columnName)) {
                                iter->second.AllTypes.push_back(*type);
                                continue;
                            }

                            ctx.AddError(TIssue(position, TStringBuilder() <<
                                "Conflict of flattening output on columns " << fullName << " and " << iter->second.Table
                                << "." << columnName));
                            return IGraphTransformer::TStatus::Error;
                        }

                        TFlattenState state;
                        state.AllTypes.push_back(*type);
                        state.Table = TString(tableName);
                        flattenFields.emplace(TString(columnName), state);
                    } else {
                        resultFields.push_back(ctx.MakeType<TItemExprType>(fullName, *type));
                    }
                }
            }
        }
    }

    if (options.Flatten) {
        for (auto& x : flattenFields) {
            bool isOptional = true;
            const TTypeAnnotationNode* commonType = nullptr;
            for (auto type : x.second.AllTypes) {
                if (type->GetKind() != ETypeAnnotationKind::Optional) {
                    isOptional = false;
                } else {
                    type = type->Cast<TOptionalExprType>()->GetItemType();
                }

                if (!commonType) {
                    commonType = type;
                } else {
                    auto arg1 = ctx.NewArgument(positionHandle, "a");
                    auto arg2 = ctx.NewArgument(positionHandle, "b");
                    if (SilentInferCommonType(arg1, *commonType, arg2, *type, ctx, commonType,
                        TConvertFlags().Set(NConvertFlags::AllowUnsafeConvert)) == IGraphTransformer::TStatus::Error) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }

            if (isOptional) {
                commonType = ctx.MakeType<TOptionalExprType>(commonType);
            }

            resultFields.push_back(ctx.MakeType<TItemExprType>(x.first, commonType));
        }
    }

    resultType = ctx.MakeType<TStructExprType>(resultFields);
    if (!resultType->Validate(position, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Ok;
}

THashMap<TStringBuf, THashSet<TStringBuf>> CollectEquiJoinKeyColumnsByLabel(const TExprNode& joinTree) {
    THashMap<TStringBuf, THashSet<TStringBuf>> result;
    CollectEquiJoinKeyColumns(joinTree, result);
    return result;
};

bool IsLeftJoinSideOptional(const TStringBuf& joinType) {
    if (joinType == "Right" || joinType == "Full" || joinType == "Exclusion") {
        return true;
    }

    return false;
}

bool IsRightJoinSideOptional(const TStringBuf& joinType) {
    if (joinType == "Left" || joinType == "Full" || joinType == "Exclusion") {
        return true;
    }

    return false;
}

TExprNode::TPtr FilterOutNullJoinColumns(TPositionHandle pos, const TExprNode::TPtr& input,
    const TJoinLabel& label, const TSet<TString>& optionalKeyColumns, TExprContext& ctx) {
    if (optionalKeyColumns.empty()) {
        return input;
    }

    TExprNode::TListType optColumns;
    for (auto fullColumnName : optionalKeyColumns) {
        TStringBuf table;
        TStringBuf column;
        SplitTableName(fullColumnName, table, column);
        auto memberName = label.MemberName(table, column);
        optColumns.push_back(ctx.NewAtom(pos, memberName));
    }

    auto optTuple = ctx.NewList(pos, std::move(optColumns));
    return ctx.Builder(pos)
        .Callable("SkipNullMembers")
            .Add(0, input)
            .Add(1, optTuple)
        .Seal()
        .Build();
}

TMap<TStringBuf, TVector<TStringBuf>> LoadJoinRenameMap(const TExprNode& settings) {
    TMap<TStringBuf, TVector<TStringBuf>> res;
    for (const auto& child : settings.Children()) {
        if (child->Child(0)->Content() == "rename") {
            auto& v = res[child->Child(1)->Content()];
            if (!child->Child(2)->Content().empty()) {
                v.push_back(child->Child(2)->Content());
            }
        }
    }

    return res;
}

TSet<TVector<TStringBuf>> LoadJoinSortSets(const TExprNode& settings) {
    TSet<TVector<TStringBuf>> res;
    for (const auto& child : settings.Children()) {
        if (child->Child(0)->Content() == "preferred_sort") {
            TVector<TStringBuf> sortBy;
            for (auto column : child->Child(1)->Children()) {
                sortBy.push_back(column->Content());
            }
            res.insert(sortBy);
        }
    }
    return res;
}

THashMap<TString, const TTypeAnnotationNode*> GetJoinColumnTypes(const TExprNode& joins,
    const TJoinLabels& labels, TExprContext& ctx) {
    return GetJoinColumnTypes(joins, labels, joins.Child(0)->Content(), ctx);
}

THashMap<TString, const TTypeAnnotationNode*> GetJoinColumnTypes(const TExprNode& joins,
    const TJoinLabels& labels, const TStringBuf& joinType, TExprContext& ctx) {
    THashMap<TString, const TTypeAnnotationNode*> finalType;
    THashMap<TString, const TTypeAnnotationNode*> leftType;
    THashMap<TString, const TTypeAnnotationNode*> rightType;
    bool isLeftOptional = IsLeftJoinSideOptional(joinType);
    bool isRightOptional = IsRightJoinSideOptional(joinType);
    if (joins.Child(1)->IsAtom()) {
        auto name = joins.Child(1)->Content();
        auto input = *labels.FindInput(name);
        for (auto& x : input->InputType->GetItems()) {
            leftType[input->FullName(x->GetName())] = x->GetItemType();
        }
    }
    else {
        leftType = GetJoinColumnTypes(*joins.Child(1), labels, ctx);
    }

    if (joins.Child(2)->IsAtom()) {
        auto name = joins.Child(2)->Content();
        auto input = *labels.FindInput(name);
        for (auto& x : input->InputType->GetItems()) {
            rightType[input->FullName(x->GetName())] = x->GetItemType();
        }
    }
    else {
        rightType = GetJoinColumnTypes(*joins.Child(2), labels, ctx);
    }

    if (isLeftOptional) {
        for (auto& x : leftType) {
            x.second = AddOptionalType(x.second, ctx);
        }
    }

    if (isRightOptional) {
        for (auto& x : rightType) {
            x.second = AddOptionalType(x.second, ctx);
        }
    }

    if (joinType != "RightOnly" && joinType != "RightSemi") {
        for (auto& x : leftType) {
            finalType.insert({ x.first, x.second });
        }
    }

    if (joinType != "LeftOnly" && joinType != "LeftSemi") {
        for (auto& x : rightType) {
            finalType.insert({ x.first, x.second });
        }
    }

    return finalType;
}

bool AreSameJoinKeys(const TExprNode& joins, const TStringBuf& table1, const TStringBuf& column1, const TStringBuf& table2, const TStringBuf& column2) {
    if (!joins.Child(1)->IsAtom()) {
        if (AreSameJoinKeys(*joins.Child(1), table1, column1, table2, column2)) {
            return true;
        }
    }

    if (!joins.Child(2)->IsAtom()) {
        if (AreSameJoinKeys(*joins.Child(2), table1, column1, table2, column2)) {
            return true;
        }
    }

    for (ui32 i = 0; i < joins.Child(3)->ChildrenSize(); i += 2) {
        if (joins.Child(3)->Child(i)->Content() == table1) {
            if (joins.Child(4)->Child(i)->Content() == table2 &&
                joins.Child(3)->Child(i + 1)->Content() == column1 &&
                joins.Child(4)->Child(i + 1)->Content() == column2) {
                return true;
            }
        }
        else if (joins.Child(3)->Child(i)->Content() == table2) {
            if (joins.Child(4)->Child(i)->Content() == table1 &&
                joins.Child(3)->Child(i + 1)->Content() == column2 &&
                joins.Child(4)->Child(i + 1)->Content() == column1) {
                return true;
            }
        }
    }

    return false;
}

std::pair<bool, bool> IsRequiredSide(const TExprNode::TPtr& joinTree, const TJoinLabels& labels, ui32 inputIndex) {
    auto joinType = joinTree->Child(0)->Content();
    auto left = joinTree->ChildPtr(1);
    auto right = joinTree->ChildPtr(2);
    if (joinType == "Inner" || joinType == "Left" || joinType == "LeftOnly" || joinType == "LeftSemi" || joinType == "RightSemi" || joinType == "Cross") {
        if (!left->IsAtom()) {
            auto x = IsRequiredSide(left, labels, inputIndex);
            if (x.first) {
                return x;
            }
        }
        else {
            auto table = left->Content();
            if (*labels.FindInputIndex(table) == inputIndex) {
                return { true, joinType == "Inner" || joinType == "LeftSemi" };
            }
        }
    }

    if (joinType == "Inner" || joinType == "Right" || joinType == "RightOnly" || joinType == "RightSemi" || joinType == "LeftSemi" || joinType == "Cross") {
        if (!right->IsAtom()) {
            auto x = IsRequiredSide(right, labels, inputIndex);
            if (x.first) {
                return x;
            }
        }
        else {
            auto table = right->Content();
            if (*labels.FindInputIndex(table) == inputIndex) {
                return{ true, joinType == "Inner" || joinType == "RightSemi" };
            }
        }
    }

    return{ false, false };
}

void AppendEquiJoinRenameMap(TPositionHandle pos, const TMap<TStringBuf, TVector<TStringBuf>>& newRenameMap,
    TExprNode::TListType& joinSettingNodes, TExprContext& ctx) {
    for (auto& x : newRenameMap) {
        if (x.second.empty()) {
            joinSettingNodes.push_back(ctx.Builder(pos)
                .List()
                    .Atom(0, "rename")
                    .Atom(1, x.first)
                    .Atom(2, "")
                .Seal()
                .Build());
            continue;
        }

        for (auto& y : x.second) {
            if (x.first == y && x.second.size() == 1) {
                continue;
            }

            joinSettingNodes.push_back(ctx.Builder(pos)
                .List()
                    .Atom(0, "rename")
                    .Atom(1, x.first)
                    .Atom(2, y)
                .Seal()
                .Build());
        }
    }
}

void AppendEquiJoinSortSets(TPositionHandle pos, const TSet<TVector<TStringBuf>>& newSortSets,
    TExprNode::TListType& joinSettingNodes, TExprContext& ctx)
{
    for (auto& ss : newSortSets) {
        YQL_ENSURE(!ss.empty());
        joinSettingNodes.push_back(ctx.Builder(pos)
            .List()
                .Atom(0, "preferred_sort")
                .List(1)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < ss.size(); ++i) {
                            parent.Atom(i, ss[i]);
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build());
    }
}

TMap<TStringBuf, TVector<TStringBuf>> UpdateUsedFieldsInRenameMap(
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    const TSet<TStringBuf>& usedFields,
    const TStructExprType* structType
) {
    TMap<TStringBuf, TStringBuf> reversedRenameMap;
    TMap<TStringBuf, TVector<TStringBuf>> newRenameMap;
    for (auto& x : renameMap) {
        if (!x.second.empty()) {
            for (auto& y : x.second) {
                reversedRenameMap[y] = x.first;
            }
        }
        else {
            // previous drops
            newRenameMap[x.first].clear();
        }
    }

    for (auto& item : structType->GetItems()) {
        bool needRemove = !usedFields.contains(item->GetName());
        if (auto renamed = reversedRenameMap.FindPtr(item->GetName())) {
            if (needRemove) {
                if (newRenameMap[*renamed].empty()) {
                    newRenameMap[*renamed].push_back("");
                }
            }
            else {
                if (!newRenameMap[*renamed].empty() && newRenameMap[*renamed][0].empty()) {
                    newRenameMap[*renamed].clear(); // Do not remove column because it will be renamed.
                }
                newRenameMap[*renamed].push_back(item->GetName());
            }
        }
        else {
            if (needRemove) {
                newRenameMap[item->GetName()].push_back("");
            }
        }
    }

    for (auto& x : newRenameMap) {
        if (AnyOf(x.second, [](const TStringBuf& value) { return !value.empty(); })) {
            continue;
        }

        x.second.clear();
    }

    return newRenameMap;
}

TVector<TEquiJoinParent> CollectEquiJoinOnlyParents(const TCoFlatMapBase& flatMap, const TParentsMap& parents)
{
    TVector<TEquiJoinParent> result;
    if (!CollectEquiJoinOnlyParents(flatMap.Ref(), nullptr, 2, result, nullptr, parents)) {
        result.clear();
    }

    return result;
}

TEquiJoinLinkSettings GetEquiJoinLinkSettings(const TExprNode& linkSettings) {
    TEquiJoinLinkSettings result;
    result.Pos = linkSettings.Pos();

    auto collectHints = [](TSet<TString>& hints, const TExprNode& hintsNode) {
        if (hintsNode.IsAtom()) {
            hints.insert(ToString(hintsNode.Content()));
        } else {
            for (auto h : hintsNode.Children()) {
                YQL_ENSURE(h->IsAtom());
                hints.insert(ToString(h->Content()));
            }
        }
    };

    if (auto left = GetSetting(linkSettings, "left")) {
        collectHints(result.LeftHints, *left->Child(1));
    }

    if (auto right = GetSetting(linkSettings, "right")) {
        collectHints(result.RightHints, *right->Child(1));
    }

    result.ForceSortedMerge = HasSetting(linkSettings, "forceSortedMerge");
    return result;
}

TExprNode::TPtr BuildEquiJoinLinkSettings(const TEquiJoinLinkSettings& linkSettings, TExprContext& ctx) {
    auto builder = [&](const TStringBuf& side) -> TExprNode::TPtr {
        return ctx.Builder(linkSettings.Pos)
            .List()
                .Atom(0, side)
                .List(1)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 i = 0;
                        for (auto h : (side == "left" ? linkSettings.LeftHints : linkSettings.RightHints)) {
                            parent.Atom(i++, h);
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();
    };

    TExprNode::TListType settings;
    if (linkSettings.ForceSortedMerge) {
        settings.push_back(ctx.NewList(linkSettings.Pos, { ctx.NewAtom(linkSettings.Pos, "forceSortedMerge", TNodeFlags::Default) }));
    }
    if (linkSettings.LeftHints) {
        settings.push_back(builder("left"));
    }

    if (linkSettings.RightHints) {
        settings.push_back(builder("right"));
    }

    return ctx.NewList(linkSettings.Pos, std::move(settings));
}

TExprNode::TPtr RemapNonConvertibleMemberForJoin(TPositionHandle pos, const TExprNode::TPtr& memberValue,
    const TTypeAnnotationNode& memberType, const TTypeAnnotationNode& unifiedType, TExprContext& ctx)
{
    TExprNode::TPtr result = memberValue;

    if (&memberType != &unifiedType) {
        result = ctx.Builder(pos)
            .Callable("StrictCast")
                .Add(0, std::move(result))
                .Add(1, ExpandType(pos, unifiedType, ctx))
            .Seal()
            .Build();
    }

    if (RemoveOptionalType(&unifiedType)->GetKind() != ETypeAnnotationKind::Data) {
        result = ctx.Builder(pos)
            .Callable("If")
                .Callable(0, "HasNull")
                    .Add(0, result)
                .Seal()
                .Callable(1, "Null")
                .Seal()
                .Callable(2, "StablePickle")
                    .Add(0, result)
                .Seal()
            .Seal()
            .Build();
    }

    return result;
}

TExprNode::TPtr PrepareListForJoin(TExprNode::TPtr list, const TTypeAnnotationNode::TListType& keyTypes, TExprNode::TListType& keys, TExprNode::TListType& payloads, bool payload, bool optional, bool filter, TExprContext& ctx) {
    const auto pos = list->Pos();

    if (keyTypes.empty() && 1U == keys.size()) {
        return payload ?
            ctx.Builder(pos)
                .Callable("Map")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .List()
                            .Add(0, std::move(keys.front()))
                            .Callable(1, "FilterMembers")
                                .Arg(0, "row")
                                .List(1)
                                    .Add(std::move(payloads))
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Build():
            ctx.Builder(pos)
                .Callable("List")
                    .Callable(0, "ListType")
                        .Callable(0, "DataType")
                            .Atom(0, "Bool", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal()
            .Build();
    }

    if (1U == keyTypes.size()) {
        const auto keyType = ctx.MakeType<TOptionalExprType>(keyTypes.front());
        list = payload ? optional ?
            ctx.Builder(pos)
                .Callable("Map")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .List()
                            .Callable(0, "StrictCast")
                                .Callable(0, "Member")
                                    .Arg(0, "row")
                                    .Add(1, std::move(keys.front()))
                                .Seal()
                                .Add(1, ExpandType(pos, *keyType, ctx))
                            .Seal()
                            .Callable(1, "FilterMembers")
                                .Arg(0, "row")
                                .List(1)
                                    .Add(std::move(payloads))
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Build():
            ctx.Builder(pos)
                .Callable("FlatMap")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .Callable("FlatMap")
                            .Callable(0, "StrictCast")
                                .Callable(0, "Member")
                                    .Arg(0, "row")
                                    .Add(1, std::move(keys.front()))
                                .Seal()
                                .Add(1, ExpandType(pos, *keyType, ctx))
                            .Seal()
                            .Lambda(1)
                                .Param("key")
                                .Callable("Just")
                                    .List(0)
                                        .Arg(0, "key")
                                        .Callable(1, "FilterMembers")
                                            .Arg(0, "row")
                                            .List(1)
                                                .Add(std::move(payloads))
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Build():
            ctx.Builder(pos)
                .Callable(optional ? "Map" : "FlatMap")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .Callable("StrictCast")
                            .Callable(0, "Member")
                                .Arg(0, "row")
                                .Add(1, std::move(keys.front()))
                            .Seal()
                            .Add(1, ExpandType(pos, *keyType, ctx))
                        .Seal()
                    .Seal()
                .Seal()
            .Build();
    } else {
        const auto keyType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTupleExprType>(keyTypes));
        list = payload ? optional ?
            ctx.Builder(pos)
                .Callable("Map")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .List()
                            .Callable(0, "StrictCast")
                                .List(0)
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        ui32 pos = 0;
                                        for (auto& key : keys) {
                                            parent.Callable(pos++, "Member")
                                                .Arg(0, "row")
                                                .Add(1, std::move(key))
                                            .Seal();
                                        }
                                        return parent;
                                    })
                                .Seal()
                                .Add(1, ExpandType(pos, *keyType, ctx))
                            .Seal()
                            .Callable(1, "FilterMembers")
                                .Arg(0, "row")
                                .List(1)
                                    .Add(std::move(payloads))
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Build():
            ctx.Builder(pos)
                .Callable("FlatMap")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .Callable("FlatMap")
                            .Callable(0, "StrictCast")
                                .List(0)
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        ui32 pos = 0;
                                        for (auto& key : keys) {
                                            parent.Callable(pos++, "Member")
                                                .Arg(0, "row")
                                                .Add(1, std::move(key))
                                            .Seal();
                                        }
                                        return parent;
                                    })
                                .Seal()
                                .Add(1, ExpandType(pos, *keyType, ctx))
                            .Seal()
                            .Lambda(1)
                                .Param("key")
                                .Callable("Just")
                                    .List(0)
                                        .Arg(0, "key")
                                        .Callable(1, "FilterMembers")
                                            .Arg(0, "row")
                                            .List(1)
                                                .Add(std::move(payloads))
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Build():
            ctx.Builder(pos)
                .Callable(optional ? "Map" : "FlatMap")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .Callable("StrictCast")
                            .List(0)
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 pos = 0;
                                    for (auto& key : keys) {
                                        parent.Callable(pos++, "Member")
                                            .Arg(0, "row")
                                            .Add(1, std::move(key))
                                        .Seal();
                                    }
                                    return parent;
                                })
                            .Seal()
                            .Add(1, ExpandType(pos, *keyType, ctx))
                        .Seal()
                    .Seal()
                .Seal()
            .Build();
    }

    if (optional && filter) {
        list = payload ?
            ctx.Builder(pos)
                .Callable("Filter")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .Callable(0, "Exists")
                            .Callable(0, "Nth")
                                .Arg(0, "row")
                                .Atom(1, "0", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Build():
            ctx.Builder(pos)
                .Callable("Filter")
                    .Add(0, std::move(list))
                    .Lambda(1)
                        .Param("row")
                        .Callable(0, "Exists")
                            .Arg(0, "row")
                        .Seal()
                    .Seal()
                .Seal()
            .Build();
    }

    return list;
}

template <bool Squeeze>
TExprNode::TPtr MakeDictForJoin(TExprNode::TPtr&& list, bool payload, bool multi, TExprContext& ctx) {
    return payload ?
        ctx.Builder(list->Pos())
            .Callable(Squeeze ? "SqueezeToDict" : "ToDict")
                .Add(0, std::move(list))
                .Lambda(1)
                    .Param("row")
                    .Callable("Nth")
                        .Arg(0, "row")
                        .Atom(1, "0", TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Lambda(2)
                    .Param("row")
                    .Callable("Nth")
                        .Arg(0, "row")
                        .Atom(1, "1", TNodeFlags::Default)
                    .Seal()
                .Seal()
                .List(3)
                    .Atom(0, multi ? "Many" : "One", TNodeFlags::Default)
                    .Atom(1, "Hashed", TNodeFlags::Default)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        if constexpr (Squeeze)
                            parent.Atom(2, "Compact", TNodeFlags::Default);
                        return parent;

                    })
                .Seal()
            .Seal()
        .Build():
        ctx.Builder(list->Pos())
            .Callable(Squeeze ? "SqueezeToDict" : "ToDict")
                .Add(0, std::move(list))
                .Lambda(1)
                    .Param("row")
                    .Arg("row")
                .Seal()
                .Lambda(2)
                    .Param("stub")
                    .Callable("Void").Seal()
                .Seal()
                .List(3)
                    .Atom(0, multi ? "Many" : "One", TNodeFlags::Default)
                    .Atom(1, "Hashed", TNodeFlags::Default)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        if constexpr (Squeeze)
                            parent.Atom(2, "Compact", TNodeFlags::Default);
                        return parent;

                    })
                .Seal()
            .Seal()
        .Build();
}

template TExprNode::TPtr MakeDictForJoin<true>(TExprNode::TPtr&& list, bool payload, bool multi, TExprContext& ctx);
template TExprNode::TPtr MakeDictForJoin<false>(TExprNode::TPtr&& list, bool payload, bool multi, TExprContext& ctx);

TExprNode::TPtr MakeCrossJoin(TPositionHandle pos, TExprNode::TPtr left, TExprNode::TPtr right, TExprContext& ctx) {
    return ctx.Builder(pos)
        .List()
            .Atom(0, "Cross")
            .Add(1, left)
            .Add(2, right)
            .List(3)
            .Seal()
            .List(4)
            .Seal()
            .List(5)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr PreparePredicate(TExprNode::TPtr predicate, TExprContext& ctx) {
    auto originalPredicate = predicate;
    bool isPg = false;
    if (predicate->IsCallable("ToPg")) {
        isPg = true;
        predicate = predicate->ChildPtr(0);
    }

    if (!predicate->IsCallable("Or")) {
        return originalPredicate;
    }

    if (predicate->ChildrenSize() == 1) {
        return originalPredicate;
    }

    // try to extract common And parts from Or
    TVector<TExprNode::TListType> andParts;
    for (ui32 i = 0; i < predicate->ChildrenSize(); ++i) {
        TExprNode::TListType res;
        bool isPg;
        GatherAndTerms(predicate->ChildPtr(i), res, isPg);
        YQL_ENSURE(!isPg); // direct child for Or
        andParts.emplace_back(std::move(res));
    }

    THashMap<const TExprNode*, ui32> commonParts;
    for (ui32 j = 0; j < andParts[0].size(); ++j) {
        commonParts[andParts[0][j].Get()] = j;
    }

    for (ui32 i = 1; i < andParts.size(); ++i) {
        THashSet<const TExprNode*> found;
        for (ui32 j = 0; j < andParts[i].size(); ++j) {
            found.insert(andParts[i][j].Get());
        }

        // remove
        for (auto it = commonParts.begin(); it != commonParts.end();) {
            if (found.contains(it->first)) {
                ++it;
            } else {
                commonParts.erase(it++);
            }
        }
    }

    if (commonParts.size() == 0) {
        return originalPredicate;
    }

    // rebuild commonParts in order of original And
    TVector<ui32> idx;
    for (const auto& x : commonParts) {
        idx.push_back(x.second);
    }

    Sort(idx);
    TExprNode::TListType andArgs;
    for (ui32 i : idx) {
        andArgs.push_back(andParts[0][i]);
    }

    TExprNode::TListType orArgs;
    for (ui32 i = 0; i < andParts.size(); ++i) {
        TExprNode::TListType restAndArgs;
        for (ui32 j = 0; j < andParts[i].size(); ++j) {
            if (commonParts.contains(andParts[i][j].Get())) {
                continue;
            }

            restAndArgs.push_back(andParts[i][j]);
        }

        if (restAndArgs.size() >= 1) {
            orArgs.push_back(ctx.NewCallable(predicate->Pos(), "And", std::move(restAndArgs)));
        }
    }

    if (orArgs.size() >= 1) {
        andArgs.push_back(ctx.NewCallable(predicate->Pos(), "Or", std::move(orArgs)));
    }

    auto ret = ctx.NewCallable(predicate->Pos(), "And", std::move(andArgs));
    if (isPg) {
        ret = ctx.NewCallable(predicate->Pos(), "ToPg", { ret });
    }

    return ret;
}

void GatherAndTermsImpl(const TExprNode::TPtr& predicate, TExprNode::TListType& andTerms) {
    if (!predicate->IsCallable("And")) {
        andTerms.emplace_back(predicate);
        return;
    }

    for (ui32 i = 0; i < predicate->ChildrenSize(); ++i) {
        GatherAndTermsImpl(predicate->ChildPtr(i), andTerms);
    }
}

void GatherAndTerms(const TExprNode::TPtr& predicate, TExprNode::TListType& andTerms, bool& isPg) {
    isPg = false;
    if (predicate->IsCallable("ToPg")) {
        isPg = true;
        GatherAndTermsImpl(predicate->HeadPtr(), andTerms);
    } else {
        GatherAndTermsImpl(predicate, andTerms);
    }
}

TExprNode::TPtr FuseAndTerms(TPositionHandle position, const TExprNode::TListType& andTerms, const TExprNode::TPtr& exclude, bool isPg, TExprContext& ctx) {
    TExprNode::TPtr prevAndNode = nullptr;
    TNodeSet added;
    for (const auto& otherAndTerm : andTerms) {
        if (otherAndTerm == exclude) {
            continue;
        }

        if (!added.insert(otherAndTerm.Get()).second) {
            continue;
        }

        if (!prevAndNode) {
            prevAndNode = otherAndTerm;
        } else {
            prevAndNode = ctx.NewCallable(position, "And", { prevAndNode, otherAndTerm });
        }
    }

    if (isPg) {
        return ctx.NewCallable(position, "ToPg", { prevAndNode });
    } else {
        return prevAndNode;
    }
}

bool IsEquality(TExprNode::TPtr predicate, TExprNode::TPtr& left, TExprNode::TPtr& right) {
    if (predicate->IsCallable("Coalesce")) {
        if (predicate->Tail().IsCallable("Bool") && IsFalse(predicate->Tail().Head().Content())) {
            predicate = predicate->HeadPtr();
        } else {
            return false;
        }
    }

    if (predicate->IsCallable("FromPg")) {
        predicate = predicate->HeadPtr();
    }

    if (predicate->IsCallable("==")) {
        left = predicate->ChildPtr(0);
        right = predicate->ChildPtr(1);
        return true;
    }

    if (predicate->IsCallable("PgResolvedOp") &&
        (predicate->Head().Content() == "=")) {
        left = predicate->ChildPtr(2);
        right = predicate->ChildPtr(3);
        return true;
    }

    return false;
}

} // namespace NYql
