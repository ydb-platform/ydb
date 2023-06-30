#include "yql_yt_join.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

TChoice Invert(TChoice choice) {
    switch (choice) {
    case TChoice::None:
    case TChoice::Both:
        return choice;
    case TChoice::Left:
        return TChoice::Right;
    case TChoice::Right:
        return TChoice::Left;
    }
}

bool IsLeftOrRight(TChoice choice) {
    return choice == TChoice::Left || choice == TChoice::Right;
}

TChoice Merge(TChoice a, TChoice b) {
    return TChoice(ui8(a) | ui8(b));
}

TMaybe<ui64> TMapJoinSettings::CalculatePartSize(ui64 rows) const {
    if (MapJoinShardCount > 1 && rows > MapJoinShardMinRows) {
        ui64 partSize = (rows + MapJoinShardCount - 1) / MapJoinShardCount;
        if (partSize < MapJoinShardMinRows) {
            // make less parts
            partSize = MapJoinShardMinRows;
            ui64 count = (rows + partSize - 1) / partSize;
            if (count > 1) {
                return partSize;
            }

            return {};
        }
        else {
            return partSize;
        }
    }

    return {};
}

THashSet<TString> BuildJoinKeys(const TJoinLabel& label, const TExprNode& keys) {
    THashSet<TString> result;
    for (ui32 i = 0; i < keys.ChildrenSize(); i += 2) {
        auto tableName = keys.Child(i)->Content();
        auto column = keys.Child(i + 1)->Content();
        result.insert(label.MemberName(tableName, column));
    }

    return result;
}

TVector<TString> BuildJoinKeyList(const TJoinLabel& label, const TExprNode& keys) {
    TVector<TString> result;
    for (ui32 i = 0; i < keys.ChildrenSize(); i += 2) {
        auto tableName = keys.Child(i)->Content();
        auto column = keys.Child(i + 1)->Content();
        result.push_back(label.MemberName(tableName, column));
    }

    return result;
}

TVector<const TTypeAnnotationNode*> BuildJoinKeyType(const TJoinLabel& label, const TExprNode& keys) {
    TVector<const TTypeAnnotationNode*> ret;
    for (ui32 i = 0; i < keys.ChildrenSize(); i += 2) {
        auto tableName = keys.Child(i)->Content();
        auto column = keys.Child(i + 1)->Content();
        auto type = label.FindColumn(tableName, column);
        ret.push_back(*type);
    }

    return ret;
}

TMap<TString, const TTypeAnnotationNode*> BuildJoinKeyTypeMap(const TJoinLabel& label, const TExprNode& keys) {
    auto names = BuildJoinKeyList(label, keys);
    auto types = BuildJoinKeyType(label, keys);

    YQL_ENSURE(names.size() == types.size());

    TMap<TString, const TTypeAnnotationNode*> ret;
    for (size_t i = 0; i < names.size(); ++i) {
        ret[names[i]] = types[i];
    }
    return ret;
}

TVector<const TTypeAnnotationNode*> RemoveNullsFromJoinKeyType(const TVector<const TTypeAnnotationNode*>& inputKeyType) {
    TVector<const TTypeAnnotationNode*> ret;
    for (const auto& x : inputKeyType) {
        ret.push_back(RemoveOptionalType(x));
    }

    return ret;
}

const TTypeAnnotationNode* AsDictKeyType(const TVector<const TTypeAnnotationNode*>& inputKeyType, TExprContext& ctx) {
    YQL_ENSURE(inputKeyType.size() > 0);
    if (inputKeyType.size() == 1) {
        return inputKeyType.front();
    } else {
        return ctx.MakeType<TTupleExprType>(inputKeyType);
    }
}

void SwapJoinType(TPositionHandle pos, TExprNode::TPtr& joinType, TExprContext& ctx) {
    if (joinType->Content() == "RightSemi") {
        joinType = ctx.NewAtom(pos, "LeftSemi");
    }
    else if (joinType->Content() == "RightOnly") {
        joinType = ctx.NewAtom(pos, "LeftOnly");
    }
    else if (joinType->Content() == "Right") {
        joinType = ctx.NewAtom(pos, "Left");
    }
    else if (joinType->Content() == "LeftSemi") {
        joinType = ctx.NewAtom(pos, "RightSemi");
    }
    else if (joinType->Content() == "LeftOnly") {
        joinType = ctx.NewAtom(pos, "RightOnly");
    }
    else if (joinType->Content() == "Left") {
        joinType = ctx.NewAtom(pos, "Right");
    }
}

const TStructExprType* MakeOutputJoinColumns(const THashMap<TString, const TTypeAnnotationNode*>& columnTypes,
    const TJoinLabel& label, TExprContext& ctx) {
    TVector<const TItemExprType*> resultFields;
    for (auto& x : label.InputType->GetItems()) {
        TString fullName = label.FullName(x->GetName());
        if (auto columnType = columnTypes.FindPtr(fullName)) {
            resultFields.push_back(ctx.MakeType<TItemExprType>(fullName, *columnType));
        }
    }

    return ctx.MakeType<TStructExprType>(resultFields);
}

const TTypeAnnotationNode* UnifyJoinKeyType(TPositionHandle pos, const TVector<const TTypeAnnotationNode*>& types, TExprContext& ctx) {
    TTypeAnnotationNode::TListType t = types;
    const TTypeAnnotationNode* commonType = CommonType(pos, t, ctx);
    if (commonType && !commonType->IsOptionalOrNull()) {
        NUdf::TCastResultOptions options = 0;
        for (auto type : types) {
            options |= CastResult<true>(type, commonType);
        }

        YQL_ENSURE(!(options & NKikimr::NUdf::ECastOptions::Impossible));
        if (options & NKikimr::NUdf::ECastOptions::MayFail) {
            commonType = ctx.MakeType<TOptionalExprType>(commonType);
        }
    }
    return commonType;
}

TVector<const TTypeAnnotationNode*> UnifyJoinKeyType(TPositionHandle pos, const TVector<const TTypeAnnotationNode*>& left,
    const TVector<const TTypeAnnotationNode*>& right, TExprContext& ctx)
{
    YQL_ENSURE(left.size() == right.size());
    TVector<const TTypeAnnotationNode*> ret;
    ret.reserve(left.size());
    for (size_t i = 0; i < left.size(); ++i) {
        ret.push_back(UnifyJoinKeyType(pos, { left[i], right[i] }, ctx));
    }

    return ret;
}

namespace  {
bool NeedSkipNulls(const TTypeAnnotationNode& keyType, const TTypeAnnotationNode& unifiedKeyType) {
    if (keyType.HasOptionalOrNull()) {
        return true;
    }

    auto options = CastResult<true>(&keyType, &unifiedKeyType);
    YQL_ENSURE(!(options & NKikimr::NUdf::ECastOptions::Impossible));
    return options & NKikimr::NUdf::ECastOptions::MayFail;
}

}

TExprNode::TPtr RemapNonConvertibleItems(const TExprNode::TPtr& input, const TJoinLabel& label,
    const TExprNode& keys, const TVector<const TTypeAnnotationNode*>& unifiedKeyTypes,
    TExprNode::TListType& columnNodes, TExprNode::TListType& columnNodesForSkipNull, TExprContext& ctx) {

    YQL_ENSURE(keys.ChildrenSize() % 2 == 0);
    YQL_ENSURE(keys.ChildrenSize() > 0);

    auto result = input;
    auto keysCount = keys.ChildrenSize() / 2;

    size_t unifiedKeysCount = unifiedKeyTypes.size();
    YQL_ENSURE(keysCount == unifiedKeysCount);

    columnNodes.clear();
    columnNodesForSkipNull.clear();
    columnNodes.reserve(keysCount);

    for (ui32 i = 0; i < keysCount; ++i) {
        const TTypeAnnotationNode* unifiedType = unifiedKeyTypes[i];

        auto tableName = keys.Child(2 * i)->Content();
        auto column = keys.Child(2 * i + 1)->Content();
        auto memberName = label.MemberName(tableName, column);
        const TTypeAnnotationNode* inputType = *label.FindColumn(tableName, column);

        auto arg = ctx.NewArgument(input->Pos(), "arg");
        auto columnValue = ctx.Builder(input->Pos())
            .Callable("Member")
                .Add(0, arg)
                .Atom(1, memberName)
            .Seal()
            .Build();

        auto remapped = RemapNonConvertibleMemberForJoin(input->Pos(), columnValue, *inputType, *unifiedType, ctx);
        if (remapped != columnValue) {
            auto newColumn = TStringBuilder() << "_yql_ej_convert_column_" << i;
            TString newMemberName = label.MemberName(tableName, newColumn);

            auto lambdaBody = ctx.Builder(input->Pos())
                .Callable("AddMember")
                    .Add(0, arg)
                    .Atom(1, newMemberName)
                    .Add(2, std::move(remapped))
                .Seal()
                .Build();

            auto lambda = ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), {std::move(arg)}), std::move(lambdaBody));

            result = ctx.Builder(input->Pos())
                .Callable("Map")
                    .Add(0, result)
                    .Add(1, lambda)
                .Seal()
                .Build();

            columnNodes.push_back(ctx.NewAtom(input->Pos(), newMemberName));
            if (NeedSkipNulls(*inputType, *unifiedType)) {
                columnNodesForSkipNull.push_back(columnNodes.back());
            }
        } else {
            columnNodes.push_back(ctx.NewAtom(input->Pos(), memberName));
        }
    }

    return result;
}

// generate Map/Reduce transformations which filter out null keys for each optional and non-convertible key
// TODO: merge common code with MakeCommonJoinCoreReduceLambda
TCommonJoinCoreLambdas MakeCommonJoinCoreLambdas(TPositionHandle pos, TExprContext& ctx, const TJoinLabel& label,
    const TJoinLabel& otherLabel, const TVector<const TTypeAnnotationNode*>& outputKeyType,
    const TExprNode& keyColumnsNode, TStringBuf joinType,
    const TStructExprType* myOutputSchemeType, const TStructExprType* otherOutputSchemeType,
    ui32 tableIndex, bool useSortedReduce, ui32 sortIndex,
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    bool myData, bool otherData, const TVector<TString>& ytReduceByColumns)
{
    TCommonJoinCoreLambdas result;
    auto arg = ctx.NewArgument(pos, "item");

    TVector<const TItemExprType*> items;
    TExprNode::TListType outStructItems;
    TVector<const TTypeAnnotationNode*> inputKeyType;

    if (joinType != "Cross") {
        YQL_ENSURE(ytReduceByColumns.size() == outputKeyType.size());
        for (ui32 i = 0; i < outputKeyType.size(); ++i) {
            const auto inputTable = keyColumnsNode.Child(2 * i)->Content();
            const auto columnName = keyColumnsNode.Child(2 * i + 1)->Content();

            const TString memberName = label.MemberName(inputTable, columnName);

            auto keyNode = ctx.NewCallable(pos, "Member", { arg, ctx.NewAtom(pos, memberName) });

            const auto fullName = FullColumnName(inputTable, columnName);
            const auto originalType = *label.FindColumn(inputTable, columnName);
            inputKeyType.push_back(originalType);

            auto targetType = outputKeyType[i];
            keyNode = RemapNonConvertibleMemberForJoin(pos, keyNode, *originalType, *targetType, ctx);

            auto keyItem = ctx.Builder(pos)
                .List()
                    .Atom(0, ytReduceByColumns[i])
                    .Add(1, keyNode)
                .Seal().Build();
            outStructItems.emplace_back(std::move(keyItem));
            items.emplace_back(ctx.MakeType<TItemExprType>(ytReduceByColumns[i], targetType));
        }
    } else {
        YQL_ENSURE(ytReduceByColumns.size() == 1);
        auto keyNode = ctx.Builder(pos)
            .List()
                .Atom(0, ytReduceByColumns.front())
                .Callable(1, "Uint32")
                    .Atom(0, "0", TNodeFlags::Default)
                .Seal()
            .Seal().Build();
        outStructItems.emplace_back(std::move(keyNode));
        items.emplace_back(ctx.MakeType<TItemExprType>(ytReduceByColumns.front(), ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    }

    if (useSortedReduce) {
        outStructItems.emplace_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, "_yql_sort")
                    .Callable(1, "Uint32")
                        .Atom(0, ToString(sortIndex), TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Build());
        items.emplace_back(ctx.MakeType<TItemExprType>("_yql_sort", ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    }

    // add payload
    {
        YQL_ENSURE(tableIndex <= 1);
        TTypeAnnotationNode::TListType variantItems(2);

        variantItems[tableIndex] = (const TTypeAnnotationNode*)label.InputType;
        variantItems[1 - tableIndex] = otherLabel.InputType;

        auto variantType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(std::move(variantItems)));

        outStructItems.emplace_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, "_yql_join_payload")
                    .Callable(1, "Variant")
                        .Add(0, arg)
                        .Atom(1, ToString(tableIndex), TNodeFlags::Default)
                        .Add(2, ExpandType(pos, *variantType, ctx))
                    .Seal()
                .Seal()
                .Build());
    }

    TExprNodeList filterKeyColumns;
    if (joinType == "Inner" || (joinType == (tableIndex == 0 ? "Right" : "Left")) || joinType == "LeftSemi"
        || (joinType == (tableIndex == 0 ? "RightOnly" : "LeftOnly")) || joinType == "RightSemi")
    {
        YQL_ENSURE(ytReduceByColumns.size() == outputKeyType.size());
        YQL_ENSURE(inputKeyType.size() == outputKeyType.size());
        for (size_t i = 0; i < ytReduceByColumns.size(); ++i) {
            if (NeedSkipNulls(*inputKeyType[i], *outputKeyType[i])) {
                filterKeyColumns.push_back(ctx.NewAtom(pos, ytReduceByColumns[i]));
            }
        }
    }

    auto body = ctx.NewCallable(pos, "Just", { ctx.NewCallable(pos, "AsStruct", std::move(outStructItems)) });
    if (!filterKeyColumns.empty()) {
        body = ctx.NewCallable(pos, "SkipNullMembers", { body, ctx.NewList(pos, std::move(filterKeyColumns)) });
    }

    result.MapLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { std::move(arg) }), std::move(body));

    arg = ctx.NewArgument(pos, "item");

    // fill myRow using arg
    TExprNode::TListType outValueItems;
    auto& myRowItems = myOutputSchemeType->GetItems();
    if (myData) {
        for (auto item : myRowItems) {
            if (auto renamed = renameMap.FindPtr(item->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto inputTable = label.TableName(item->GetName());
            auto columnName = label.ColumnName(item->GetName());
            TString memberName = label.AddLabel ? label.MemberName(inputTable, columnName) : TString(item->GetName());

            auto pair = ctx.Builder(pos)
                .List()
                    .Atom(0, item->GetName())
                    .Callable(1, "Member")
                        .Callable(0, "Just")
                            .Add(0, arg)
                        .Seal()
                        .Atom(1, memberName)
                    .Seal()
                .Seal()
                .Build();
            outValueItems.push_back(pair);
            items.emplace_back(ctx.MakeType<TItemExprType>(item->GetName(), item->GetItemType()->IsOptionalOrNull() ? item->GetItemType() : ctx.MakeType<TOptionalExprType>(item->GetItemType())));
        }
    }

    if (otherData) {
        auto& otherRowItems = otherOutputSchemeType->GetItems();
        for (auto item : otherRowItems) {
            if (auto renamed = renameMap.FindPtr(item->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto columnType = item->GetItemType();
            if (!columnType->IsOptionalOrNull()) {
                columnType = ctx.MakeType<TOptionalExprType>(columnType);
            }

            auto pair = ctx.Builder(pos)
                .List()
                    .Atom(0, item->GetName())
                    .Callable(1, "Nothing")
                        .Add(0, ExpandType(pos, *columnType, ctx))
                    .Seal()
                .Seal().Build();
            outValueItems.emplace_back(std::move(pair));
            items.emplace_back(ctx.MakeType<TItemExprType>(item->GetName(), item->GetItemType()->IsOptionalOrNull() ? item->GetItemType() : ctx.MakeType<TOptionalExprType>(item->GetItemType())));
        }
    }

    auto tableIndexNode = ctx.Builder(pos)
        .List()
            .Atom(0, "_yql_table_index")
            .Callable(1, "Uint32")
                .Atom(0, ToString(tableIndex), TNodeFlags::Default)
            .Seal()
        .Seal().Build();
    outValueItems.emplace_back(std::move(tableIndexNode));
    items.emplace_back(ctx.MakeType<TItemExprType>("_yql_table_index", ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));

    result.ReduceLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { std::move(arg) }),
        ctx.NewCallable(pos, "AsStruct", std::move(outValueItems)));
    result.CommonJoinCoreInputType = ctx.MakeType<TStructExprType>(items);
    return result;
}

TExprNode::TPtr PrepareForCommonJoinCore(TPositionHandle pos, TExprContext& ctx, const TExprNode::TPtr& input,
    const TExprNode::TPtr& reduceLambdaZero, const TExprNode::TPtr& reduceLambdaOne)
{
    return ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, input)
            .Lambda(1)
                .Param("item")
                .Callable("FlattenMembers")
                    .List(0)
                        .Atom(0, "")
                        .Callable(1, "RemoveMember")
                            .Arg(0, "item")
                            .Atom(1, "_yql_join_payload")
                        .Seal()
                    .Seal()
                    .List(1)
                        .Atom(0, "")
                        .Callable(1, "Visit")
                            .Callable(0, "Member")
                                .Arg(0, "item")
                                .Atom(1, "_yql_join_payload")
                            .Seal()
                            .Atom(1, "0", TNodeFlags::Default)
                            .Add(2, reduceLambdaZero)
                            .Atom(3, "1", TNodeFlags::Default)
                            .Add(4, reduceLambdaOne)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

// generate Reduce-only transformations which filter out null keys for each optional and non-convertible key
TCommonJoinCoreLambdas MakeCommonJoinCoreReduceLambda(TPositionHandle pos, TExprContext& ctx, const TJoinLabel& label,
    const TVector<const TTypeAnnotationNode*>& outputKeyType, const TExprNode& keyColumnsNode,
    TStringBuf joinType, const TStructExprType* myOutputSchemeType, const TStructExprType* otherOutputSchemeType,
    ui32 tableIndex, bool useSortedReduce, ui32 sortIndex,
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    bool myData, bool otherData, const TVector<TString>& ytReduceByColumns)
{

    TExprNode::TListType keyNodes;
    TVector<std::pair<TStringBuf, TStringBuf>> inputKeyColumns;
    TVector<const TItemExprType*> items;
    TVector<const TTypeAnnotationNode*> keyTypes;

    TTypeAnnotationNode::TListType outputKeyColumnsTypes;
    if (joinType != "Cross") {
        for (ui32 i = 0; i < outputKeyType.size(); ++i) {
            inputKeyColumns.push_back({ keyColumnsNode.Child(2 * i)->Content(),
                keyColumnsNode.Child(2 * i + 1)->Content() });

            outputKeyColumnsTypes.push_back(outputKeyType[i]);
        }
    }

    auto arg = ctx.NewArgument(pos, "item");
    for (ui32 i = 0; i < inputKeyColumns.size(); ++i) {
        const auto& inputTable = inputKeyColumns[i].first;
        const auto& columnName = inputKeyColumns[i].second;
        const TString memberName = label.MemberName(inputTable, columnName);

        keyNodes.emplace_back(ctx.NewCallable(pos, "Member", { arg, ctx.NewAtom(pos, memberName) }));

        auto& keyNode = keyNodes.back();
        const auto fullName = FullColumnName(inputTable, columnName);
        const auto originalType = *label.FindColumn(inputTable, columnName);

        auto targetType = outputKeyColumnsTypes[i];
        if (!targetType->IsOptionalOrNull()) {
            targetType = ctx.MakeType<TOptionalExprType>(targetType);
        }
        keyNode = RemapNonConvertibleMemberForJoin(pos, keyNode, *originalType, *targetType, ctx);
        keyTypes.emplace_back(targetType);
    }

    // fill myRow using arg
    TExprNode::TListType outValueItems;
    auto& myRowItems = myOutputSchemeType->GetItems();
    if (myData) {
        for (auto item : myRowItems) {
            if (auto renamed = renameMap.FindPtr(item->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto inputTable = label.TableName(item->GetName());
            auto columnName = label.ColumnName(item->GetName());
            TString memberName = label.AddLabel ? label.MemberName(inputTable, columnName) : TString(item->GetName());
            auto columnValue = ctx.Builder(pos)
                .Callable("Member")
                    .Add(0, arg)
                        .Atom(1, memberName)
                    .Seal()
                .Build();

            if (!item->GetItemType()->IsOptionalOrNull()) {
                columnValue = ctx.NewCallable(pos, "Just", { columnValue });
            }

            auto pair = ctx.Builder(pos)
                .List()
                    .Atom(0, item->GetName())
                    .Add(1, columnValue)
                .Seal()
                .Build();
            outValueItems.push_back(pair);
            items.emplace_back(ctx.MakeType<TItemExprType>(item->GetName(), item->GetItemType()->IsOptionalOrNull() ? item->GetItemType() : ctx.MakeType<TOptionalExprType>(item->GetItemType())));
        }
    }

    if (otherData) {
        auto& otherRowItems = otherOutputSchemeType->GetItems();
        for (auto item : otherRowItems) {
            if (auto renamed = renameMap.FindPtr(item->GetName())) {
                if (renamed->empty()) {
                    continue;
                }
            }

            auto columnType = item->GetItemType();
            if (!columnType->IsOptionalOrNull()) {
                columnType = ctx.MakeType<TOptionalExprType>(columnType);
            }

            auto pair = ctx.Builder(pos)
                .List()
                    .Atom(0, item->GetName())
                    .Callable(1, "Nothing")
                        .Add(0, ExpandType(pos, *columnType, ctx))
                    .Seal()
                .Seal().Build();
            outValueItems.emplace_back(std::move(pair));
            items.emplace_back(ctx.MakeType<TItemExprType>(item->GetName(), item->GetItemType()->IsOptionalOrNull() ? item->GetItemType() : ctx.MakeType<TOptionalExprType>(item->GetItemType())));
        }
    }

    auto tableIndexNode = ctx.Builder(pos)
        .List()
            .Atom(0, "_yql_table_index")
            .Callable(1, "Uint32")
                .Atom(0, ToString(tableIndex), TNodeFlags::Default)
            .Seal()
        .Seal().Build();
    outValueItems.emplace_back(std::move(tableIndexNode));
    items.emplace_back(ctx.MakeType<TItemExprType>("_yql_table_index", ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));

    TExprNode::TListType outStructItems;
    outStructItems = outValueItems;

    if (useSortedReduce) {
        outStructItems.emplace_back(ctx.Builder(pos)
            .List()
            .Atom(0, "_yql_sort")
                .Callable(1, "Uint32")
                    .Atom(0, ToString(sortIndex), TNodeFlags::Default)
                .Seal()
            .Seal().Build());
        items.emplace_back(ctx.MakeType<TItemExprType>("_yql_sort", ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    }

    // fill keys in out struct
    if (joinType == "Cross") {
        auto keyNode = ctx.Builder(pos)
            .List()
                .Atom(0, ytReduceByColumns.front())
                .Callable(1, "Uint32")
                    .Atom(0, "0", TNodeFlags::Default)
                .Seal()
            .Seal().Build();
        outStructItems.emplace_back(std::move(keyNode));
        items.emplace_back(ctx.MakeType<TItemExprType>(ytReduceByColumns.front(), ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    } else {
        for (ui32 i = 0; i < keyNodes.size(); ++i) {
            auto keyNode = ctx.Builder(pos)
                .List()
                    .Atom(0, ytReduceByColumns[i])
                    .Add(1, keyNodes[i])
                .Seal().Build();
            outStructItems.emplace_back(std::move(keyNode));
            items.emplace_back(ctx.MakeType<TItemExprType>(ytReduceByColumns[i], keyTypes[i]));
        }
    }

    TExprNodeList filterKeyColumns;

    if (joinType == "Inner" || (joinType == (tableIndex == 0 ? "Right" : "Left")) || joinType == "LeftSemi"
        || (joinType == (tableIndex == 0 ? "RightOnly" : "LeftOnly")) || joinType == "RightSemi")
    {
        for (const auto &keyCol : ytReduceByColumns) {
            filterKeyColumns.push_back(ctx.NewAtom(pos, keyCol));
        }
    }

    auto body = ctx.Builder(pos)
        .Callable("SkipNullMembers")
            .Callable(0, "Just")
                .Add(0, ctx.NewCallable(pos, "AsStruct", std::move(outStructItems)))
            .Seal()
            .Add(1, ctx.NewList(pos, std::move(filterKeyColumns)))
        .Seal()
        .Build();

    TCommonJoinCoreLambdas result;
    result.ReduceLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { std::move(arg) }), std::move(body));
    result.CommonJoinCoreInputType = ctx.MakeType<TStructExprType>(items);
    return result;
}

void AddJoinRemappedColumn(TPositionHandle pos, const TExprNode::TPtr& pairArg, TExprNode::TListType& joinedBodyChildren,
    TStringBuf name, TStringBuf newName, TExprContext& ctx)
{
    auto member = ctx.Builder(pos)
        .Callable("Member")
            .Add(0, pairArg)
            .Atom(1, name)
        .Seal()
        .Build();

    joinedBodyChildren.emplace_back(ctx.Builder(pos)
        .List()
            .Atom(0, newName)
            .Add(1, std::move(member))
        .Seal()
        .Build());
}

}
