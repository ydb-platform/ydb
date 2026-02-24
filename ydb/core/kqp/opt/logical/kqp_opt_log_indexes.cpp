#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <util/generic/hash.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TCoAtomList BuildKeyColumnsList(TPositionHandle pos, TExprContext& ctx, const auto& columnsToSelect) {
    TVector<TExprBase> columnsList;
    columnsList.reserve(columnsToSelect.size());
    for (auto column : columnsToSelect) {
        auto atom = Build<TCoAtom>(ctx, pos)
            .Value(column)
            .Done();

        columnsList.emplace_back(std::move(atom));
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(columnsList)
        .Done();
}

TCoAtomList BuildKeyColumnsList(const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx) {
    return BuildKeyColumnsList(pos, ctx, table.Metadata->KeyColumnNames);
}

TCoAtomList MergeColumns(const NNodes::TCoAtomList& col1, const TVector<TString>& col2, TExprContext& ctx) {
    TMap<TString, TCoAtom> columns;
    for (const auto& c : col1) {
        YQL_ENSURE(columns.insert({c.StringValue(), c}).second);
    }

    for (const auto& c : col2) {
        if (!columns.contains(c)) {
            auto atom = Build<TCoAtom>(ctx, col1.Pos())
                .Value(c)
                .Done();
            columns.insert({c, std::move(atom)});
        }
    }

    TVector<TCoAtom> columnsList;
    columnsList.reserve(columns.size());
    for (auto [_, column] : columns) {
        columnsList.emplace_back(std::move(column));
    }

    return Build<TCoAtomList>(ctx, col1.Pos())
        .Add(columnsList)
        .Done();
}

bool IsKeySelectorPkPrefix(NNodes::TCoLambda keySelector, const TKikimrTableDescription& tableDesc) {
    auto checkKey = [keySelector, &tableDesc] (const TExprBase& key, ui32 index) {
        if (!key.Maybe<TCoMember>()) {
            return false;
        }

        auto member = key.Cast<TCoMember>();
        if (member.Struct().Raw() != keySelector.Args().Arg(0).Raw()) {
            return false;
        }

        auto column = member.Name().StringValue();
        auto columnIndex = tableDesc.GetKeyColumnIndex(column);
        if (!columnIndex || *columnIndex != index) {
            return false;
        }

        return true;
    };

    auto lambdaBody = keySelector.Body();
    if (auto maybeTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (size_t i = 0; i < tuple.Size(); ++i) {
            if (!checkKey(tuple.Item(i), i)) {
                return false;
            }
        }
    } else {
        if (!checkKey(lambdaBody, 0)) {
            return false;
        }
    }

    return true;
}

bool IsTableExistsKeySelector(NNodes::TCoLambda keySelector, const TKikimrTableDescription& tableDesc, TVector<TString>* columns) {
    auto checkKey = [keySelector, &tableDesc, columns] (const TExprBase& key) {
        if (!key.Maybe<TCoMember>()) {
            return false;
        }

        auto member = key.Cast<TCoMember>();
        if (member.Struct().Raw() != keySelector.Args().Arg(0).Raw()) {
            return false;
        }

        auto column = member.Name().StringValue();
        if (!tableDesc.Metadata->Columns.contains(column)) {
            return false;
        }

        if (columns) {
            columns->emplace_back(std::move(column));
        }

        return true;
    };

    auto lambdaBody = keySelector.Body();
    if (auto maybeTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (size_t i = 0; i < tuple.Size(); ++i) {
            if (!checkKey(tuple.Item(i))) {
                return false;
            }
        }
    } else {
        if (!checkKey(lambdaBody)) {
            return false;
        }
    }

    return true;
}



bool CanPushTopSort(const TCoTopBase& node, const TKikimrTableDescription& indexDesc, TVector<TString>* columns) {
    return IsTableExistsKeySelector(node.KeySelectorLambda(), indexDesc, columns);
}

bool CanUseVectorIndex(const TIndexDescription& indexDesc, const TExprBase& lambdaBody, const TCoTopBase& top, TString& error) {
    Y_ASSERT(indexDesc.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree);
    // TODO(mbkkt) We need to account top.Count(), but not clear what to if it's value is runtime?
    const auto& col = indexDesc.KeyColumns.back();
    auto checkMember = [&] (const TExprBase& expr) {
        auto member = expr.Maybe<TCoMember>();
        return member && member.Cast().Name().Value() == col;
    };
    auto checkUdf = [&] (const TExprBase& expr, bool checkMembers) {
        auto apply = expr.Maybe<TCoApply>();
        if (!apply || apply.Cast().Args().Count() != 3) {
            return false;
        }
        if (checkMembers) {
            auto args = apply.Cast().Args();
            if (absl::c_none_of(args, [&] (const TExprBase& expr) { return checkMember(expr); })) {
                return false;
            }
        }
        auto udf = apply.Cast().Callable().Maybe<TCoUdf>();
        if (!udf) {
            return false;
        }
        auto directions = top.SortDirections().Maybe<TCoBool>();
        if (!directions) {
            return false;
        }
        const bool asc = directions.Cast().Literal().Value() == "true";
        const auto methodName = udf.Cast().MethodName().Value();
        auto& desc = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc.SpecializedIndexDescription);
        switch (desc.settings().settings().metric()) {
            case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
                if (!asc && methodName == "Knn.InnerProductSimilarity") {
                    return true;
                }
                error = TStringBuilder() << "Knn::InnerProductSimilarity(" << col << ", ...) DESC";
                return false;
            case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
            case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
                if (asc && methodName == "Knn.CosineDistance" ||
                    !asc && methodName == "Knn.CosineSimilarity") {
                    return true;
                }
                error = TStringBuilder() << "Knn::CosineSimilarity(" << col << ", ...) DESC or Knn::CosineDistance(" << col << ", ...) ASC";
                return false;
            case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
                if (asc && methodName == "Knn.ManhattanDistance") {
                    return true;
                }
                error = TStringBuilder() << "Knn::ManhattanDistance(" << col << ", ...) ASC";
                return false;
            case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
                if (asc && methodName == "Knn.EuclideanDistance") {
                    return true;
                }
                error = TStringBuilder() << "Knn::EuclideanDistance(" << col << ", ...) ASC";
                return false;
            default:
                Y_UNREACHABLE();
        }
    };
    // lambdaBody may be:
    // 1) Knn::Distance(Member(input row, 'embedding'), expression) where input row = freearg0.
    // 2) FlatMap(<freearg0>, lambda freearg0: Knn::Distance(Member(freearg0, 'embedding'), <expression>))
    // 3) FlatMap(<expression>, lambda expr: FlatMap(<freearg0>, lambda freearg0: Knn::Distance(Member(freearg0, 'embedding'), expr)))
    // In the latter case we should also check the inner lambda.
    if (lambdaBody.Maybe<TCoFlatMap>()) {
        auto flatMap = lambdaBody.Cast<TCoFlatMap>();
        auto flatMapInput = flatMap.Input();
        auto member = flatMapInput.Maybe<TCoMember>();
        if (member && member.Cast().Struct().Maybe<TCoArgument>()) {
            if (member.Cast().Name().Value() == col) {
                // First case
                return checkUdf(flatMap.Lambda().Body(), false);
            } else {
                // Other column
                return false;
            }
        }
        // Not a member of incoming argument - check inner lambda
        auto inner = flatMap.Lambda().Body();
        if (inner.Maybe<TCoFlatMap>()) {
            auto innerMap = inner.Cast<TCoFlatMap>();
            auto innerMapInput = innerMap.Input();
            auto member = innerMapInput.Maybe<TCoMember>();
            if (member && member.Cast().Struct().Maybe<TCoArgument>()) {
                if (member.Cast().Name().Value() == col) {
                    // Second case
                    return checkUdf(innerMap.Lambda().Body(), false);
                } else {
                    // Other column
                    return false;
                }
            }
        }
    }
    return checkUdf(lambdaBody, true);
}

struct TReadMatch {
    TMaybeNode<TKqlReadTableIndex> Read;
    TMaybeNode<TKqlReadTableIndexRanges> ReadRanges;


    static TReadMatch Match(TExprBase expr, const TKqpOptimizeContext&) {
        if (auto read = expr.Maybe<TKqlReadTableIndex>()) {
            return {read.Cast(), {}};
        }

        if (auto read = expr.Maybe<TKqlReadTableIndexRanges>()) {
            return {{}, read.Cast()};
        }

        return {};
    }

    static TReadMatch MatchIndexedRead(TExprBase expr, const TKqpOptimizeContext& kqpCtx) {
        if (auto read = expr.Maybe<TKqlReadTableIndex>()) {

            const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Cast().Table().Path());
            YQL_ENSURE(tableDesc.Metadata);
            auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(read.Cast().Index().Value());
            if (indexDesc->Type == TIndexDescription::EType::GlobalFulltextPlain
                || indexDesc->Type == TIndexDescription::EType::GlobalFulltextRelevance) {
                return {};
            }

            if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
                return {};
            }

            return {read.Cast(), {}};
        }
        if (auto read = expr.Maybe<TKqlReadTableIndexRanges>()) {

            const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Cast().Table().Path());
            YQL_ENSURE(tableDesc.Metadata);
            auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(read.Cast().Index().Value());
            if (indexDesc->Type == TIndexDescription::EType::GlobalFulltextPlain
                || indexDesc->Type == TIndexDescription::EType::GlobalFulltextRelevance) {
                return {};
            }

            if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
                return {};
            }

            return {{}, read.Cast()};
        }
        return {};
    }

    static TReadMatch MatchSyncVectorKMeansTreeRead(const TExprBase& node, const TKqpOptimizeContext& kqpCtx) {
        auto read = TReadMatch::Match(node, kqpCtx);
        if (!read || read.Index().Value().empty()) {
            return {};
        }

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
        YQL_ENSURE(tableDesc.Metadata);
        auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(read.Index().Value());
        if (indexDesc->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
            return {};
        }

        return read;
    }

    static TReadMatch MatchFullTextRead(const TExprBase& node, const TKqpOptimizeContext& kqpCtx) {
        auto read = TReadMatch::Match(node, kqpCtx);
        if (!read || read.Index().Value().empty()) {
            return {};
        }

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
        YQL_ENSURE(tableDesc.Metadata);
        auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(read.Index().Value());
        if (indexDesc->Type != TIndexDescription::EType::GlobalFulltextPlain
            && indexDesc->Type != TIndexDescription::EType::GlobalFulltextRelevance) {
            return {};
        }

        return read;
    }

    operator bool () const {
        return Read.IsValid() || ReadRanges.IsValid();
    }

    TKqpTable Table() const {
        if (Read) {
            return Read.Cast().Table();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Table();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    TCoAtom Index() const {
        if (Read) {
            return Read.Cast().Index();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Index();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    TCoAtomList Columns() const {
        if (Read) {
            return Read.Cast().Columns();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Columns();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    NYql::TPositionHandle Pos() const {
        if (Read) {
            return Read.Cast().Pos();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Pos();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    TCoNameValueTupleList Settings() const {
        if (Read) {
            return Read.Cast().Settings();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Settings();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    bool FullScan() const {
        if (Read) {
            return Read.Cast().Range().From().ArgCount() == 0 && Read.Cast().Range().To().ArgCount() == 0;
        }
        if (ReadRanges) {
            return TCoVoid::Match(ReadRanges.Cast().Ranges().Raw());
        }
        return false;
    }

    TExprBase BuildRead(TExprContext& ctx, TKqpTable tableMeta, TCoAtomList columns) const {
        if (Read) {
            return Build<TKqlReadTable>(ctx, Pos())
                .Table(tableMeta)
                .Range(Read.Range().Cast())
                .Columns(columns)
                .Settings(Settings())
                .Done();
        }

        if (ReadRanges) {
            return Build<TKqlReadTableRanges>(ctx, Pos())
                .Table(tableMeta)
                .Ranges(ReadRanges.Ranges().Cast())
                .Columns(columns)
                .Settings(Settings())
                .ExplainPrompt(ReadRanges.ExplainPrompt().Cast())
                .Done();
        }

        YQL_ENSURE(false);
    }
};

bool CheckIndexCovering(const TCoAtomList& readColumns, const TIntrusivePtr<TKikimrTableMetadata>& indexMeta) {
    for (const auto& col : readColumns) {
        if (!indexMeta->Columns.contains(col.StringValue())) {
            return false;
        }
    }
    return true;
}

TExprBase DoRewriteIndexRead(const TReadMatch& read, TExprContext& ctx,
    const TKikimrTableDescription& tableDesc, TIntrusivePtr<TKikimrTableMetadata> indexMeta,
    const TVector<TString>& extraColumns, const std::function<TExprBase(const TExprBase&)>& middleFilter = {})
{
    const bool isCovered = CheckIndexCovering(read.Columns(), indexMeta);

    if (read.FullScan()) {
        const auto indexName = read.Index().StringValue();
        auto issue = TIssue(ctx.GetPosition(read.Pos()), "Given predicate is not suitable for used index: " + indexName);
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, issue);
        ctx.AddWarning(issue);
    }

    if (isCovered) {
        // We can read all data from index table.
        auto ret = read.BuildRead(ctx, BuildTableMeta(*indexMeta, read.Pos(), ctx), read.Columns());

        if (middleFilter) {
            return middleFilter(ret);
        }
        return ret;
    }

    auto keyColumnsList = BuildKeyColumnsList(tableDesc, read.Pos(), ctx);
    auto columns = MergeColumns(keyColumnsList, extraColumns, ctx);

    TExprBase readIndexTable = read.BuildRead(ctx, BuildTableMeta(*indexMeta, read.Pos(), ctx), columns);

    if (middleFilter) {
        readIndexTable = middleFilter(readIndexTable);
    }

    if (extraColumns) {
        TCoArgument arg = Build<TCoArgument>(ctx, read.Pos())
            .Name("Arg")
            .Done();

        TVector<TExprBase> structMembers;
        structMembers.reserve(keyColumnsList.Size());

        for (const auto& keyColumn : keyColumnsList) {
            auto member = Build<TCoNameValueTuple>(ctx, read.Pos())
                .Name().Build(keyColumn.Value())
                .Value<TCoMember>()
                    .Struct(arg)
                    .Name().Build(keyColumn.Value())
                    .Build()
                .Done();

            structMembers.push_back(member);
        }

        // We need to save order for TopSort, otherwise TopSort will be replaced by Top during optimization (https://st.yandex-team.ru/YQL-15415)
        readIndexTable = Build<TCoMapBase>(ctx, read.Pos())
            .CallableName(readIndexTable.Maybe<TCoTopSort>() ? TCoOrderedMap::CallableName() : TCoMap::CallableName())
            .Input(readIndexTable)
            .Lambda()
                .Args({arg})
                .Body<TCoAsStruct>()
                    .Add(structMembers)
                    .Build()
                .Build()
            .Done();
    }

    TKqpStreamLookupSettings settings;
    settings.Strategy = EStreamLookupStrategyType::LookupRows;
    return Build<TKqlStreamLookupTable>(ctx, read.Pos())
        .Table(read.Table())
        .LookupKeys(readIndexTable.Ptr())
        .Columns(read.Columns())
        .Settings(settings.BuildNode(ctx, read.Pos()))
        .Done();
}

auto NewLambdaFrom(TExprContext& ctx, TPositionHandle pos, TNodeOnNodeOwnedMap& replaces, const TExprNode& args, const TExprBase& body) {
    const auto oldArgNodes = args.Children();
    replaces.clear();
    replaces.reserve(oldArgNodes.size());
    TExprNode::TListType newArgNodes;
    newArgNodes.reserve(oldArgNodes.size());
    for (const auto& arg : oldArgNodes) {
        auto newArg = ctx.ShallowCopy(*arg);
        YQL_ENSURE(replaces.emplace(arg.Get(), newArg).second);
        newArgNodes.emplace_back(std::move(newArg));
    }
    return TCoLambda{ctx.NewLambda(pos,
        ctx.NewArguments(pos, std::move(newArgNodes)),
        ctx.ReplaceNodes(TExprNode::TListType{body.Ptr()}, replaces))};
}

auto LevelLambdaFrom(
    const TIndexDescription& indexDesc, TExprContext& ctx, TPositionHandle pos, TNodeOnNodeOwnedMap& replaces,
    const TExprBase& fromArgs, const TExprBase& fromBody, TExprNode::TPtr& targetVectorExpr)
{
    auto newLambda = NewLambdaFrom(ctx, pos, replaces, *fromArgs.Raw(), fromBody);
    replaces.clear();
    auto args = newLambda.Args().Ptr();

    auto flatMap = newLambda.Body().Maybe<TCoFlatMap>();
    if (!flatMap) {
        auto apply = newLambda.Body().Cast<TCoApply>();
        for (auto arg : apply.Args()) {
            auto oldMember = arg.Maybe<TCoMember>();
            if (oldMember && oldMember.Cast().Name().Value() == indexDesc.KeyColumns.back()) {
                auto newMember = Build<TCoMember>(ctx, pos)
                    .Name().Build(NTableIndex::NKMeans::CentroidColumn)
                    .Struct(oldMember.Cast().Struct())
                .Done();
                replaces.emplace(oldMember.Raw(), newMember.Ptr());
            } else if (arg.Raw() != apply.Callable().Raw()) {
                targetVectorExpr = arg.Ptr();
            }
        }
        return ctx.NewLambda(pos,
            std::move(args),
            ctx.ReplaceNodes(TExprNode::TListType{apply.Ptr()}, replaces));
    }

    auto innerMap = flatMap.Cast().Lambda().Body().Maybe<TCoFlatMap>();
    if (innerMap) {
        // Handle the FlatMap(<expression>, lambda exprarg: FlatMap(<freearg0>, lambda freearg0: Knn::Distance(Member(freearg0, 'embedding'), exprarg))) case
        // See also CanUseVectorIndex()
        auto apply = innerMap.Cast().Lambda().Body().Cast<TCoApply>();
        TVector<TExprBase> newArgs;
        for (auto arg : apply.Args()) {
            if (arg.Raw() == apply.Callable().Raw()) {
                // Skip, callable is also returned in args for some reason
            } else if (arg.Raw() == innerMap.Cast().Lambda().Args().Arg(0).Raw()) {
                auto oldMember = innerMap.Cast().Input().Cast<TCoMember>();
                newArgs.push_back(Build<TCoMember>(ctx, pos)
                    .Name().Build(NTableIndex::NKMeans::CentroidColumn)
                    .Struct(oldMember.Struct())
                    .Done());
            } else {
                // Refer two levels up from this lambda... i.e. refer <expression> from <exprarg> in the line above
                if (arg.Raw() == flatMap.Cast().Lambda().Args().Arg(0).Raw()) {
                    targetVectorExpr = flatMap.Cast().Input().Ptr();
                } else {
                    targetVectorExpr = arg.Ptr();
                }
                newArgs.push_back(TExprBase(targetVectorExpr));
            }
        }
        auto newApply = Build<TCoApply>(ctx, pos)
            .Callable(apply.Callable())
            .FreeArgs()
                .Add(newArgs)
                .Build()
            .Done();
        replaces.emplace(innerMap.Raw(), newApply.Ptr());
        return ctx.NewLambda(pos, std::move(args), ctx.ReplaceNodes(newLambda.Body().Ptr(), replaces));
    }

    auto apply = flatMap.Cast().Lambda().Body().Cast<TCoApply>();
    for (auto arg : apply.Args()) {
        if (arg.Ref().Type() == NYql::TExprNode::Argument) {
            auto oldMember = flatMap.Cast().Input().Cast<TCoMember>();
            auto newMember = Build<TCoMember>(ctx, pos)
                .Name().Build(NTableIndex::NKMeans::CentroidColumn)
                .Struct(oldMember.Struct())
            .Done();
            replaces.emplace(arg.Raw(), newMember.Ptr());
        } else {
            targetVectorExpr = arg.Ptr();
        }
    }
    return ctx.NewLambda(pos,
        std::move(args),
        ctx.ReplaceNodes(TExprNode::TListType{apply.Ptr()}, replaces));
}

void RemapIdToParent(TExprContext& ctx, TPositionHandle pos, TExprNodePtr& read) {
    auto mapArg = Build<TCoArgument>(ctx, pos)
        .Name("mapArg")
    .Done();
    TVector<TExprBase> mapMembers{
        Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(NTableIndex::NKMeans::ParentColumn)
            .Value<TCoMember>().Struct(mapArg)
                .Name().Build(NTableIndex::NKMeans::IdColumn)
            .Build()
        .Done()
    };

    read = Build<TCoMap>(ctx, pos)
        .Input(read)
        .Lambda()
            .Args({mapArg})
            .template Body<TCoAsStruct>().Add(mapMembers).Build()
        .Build()
    .Done().Ptr();
}

void VectorReadLevel(
    const TIndexDescription& indexDesc, TExprContext& ctx, TPositionHandle pos,
    const TExprNodePtr& lambda, const TCoTopBase& top, const TKqpTable& levelTable, const TCoAtomList& levelColumns,
    const TExprNodePtr& levelTopCount, const TCoNameValueTupleList& streamLookupSettings, TExprNodePtr& read)
{
    const auto& settings = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc.SpecializedIndexDescription)
        .settings();
    const auto levels = std::max<ui32>(1, settings.levels());
    Y_ENSURE(levels >= 1);

    for (ui32 level = 1;; ++level) {
        read = Build<TCoTop>(ctx, pos)
            .Input(read)
            .KeySelectorLambda(lambda)
            .SortDirections(top.SortDirections())
            .Count(levelTopCount)
        .Done().Ptr();

        RemapIdToParent(ctx, pos, read);

        if (level == levels) {
            break;
        }

        read = Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(levelTable)
            .LookupKeys(read)
            .Columns(levelColumns)
            .Settings(streamLookupSettings)
            .Done().Ptr();
    }
}

void VectorReadWithPushdown(
    TExprContext& ctx, TPositionHandle pos,
    const TKqpTable& targetTable,
    const TIntrusivePtr<TKikimrTableMetadata> & mainTableMeta,
    const TCoAtomList& mainColumns,
    const TKqpStreamLookupSettings& pushdownSettings,
    TExprNodePtr& read)
{
    THashSet<TStringBuf> cols;
    for (const auto& col: mainColumns) {
        cols.insert(col.Value());
    }
    TVector<TCoAtom> columnsWithKey;
    if (pushdownSettings.VectorTopDistinct) {
        // stream lookup columns must contain primary key columns for DistinctColumns pushdown
        for (const auto& col: mainTableMeta->KeyColumnNames) {
            if (!cols.contains(col)) {
                columnsWithKey.push_back(Build<TCoAtom>(ctx, pos)
                    .Value(col)
                    .Done());
            }
        }
    }
    if (!cols.contains(pushdownSettings.VectorTopColumn)) {
        // stream lookup columns must contain vector column for VectorTop pushdown
        columnsWithKey.push_back(Build<TCoAtom>(ctx, pos)
            .Value(pushdownSettings.VectorTopColumn)
            .Done());
    }
    const auto settingsNode = pushdownSettings.BuildNode(ctx, pos);
    if (columnsWithKey.size()) {
        for (const auto& col: mainColumns) {
            columnsWithKey.push_back(col);
        }
        read = Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(targetTable)
            .LookupKeys(read)
            .Columns<TCoAtomList>().Add(columnsWithKey).Build()
            .Settings(settingsNode)
            .Done().Ptr();
        read = Build<TCoExtractMembers>(ctx, pos)
            .Input(read)
            .Members(mainColumns)
            .Done().Ptr();
    } else {
        read = Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(targetTable)
            .LookupKeys(read)
            .Columns(mainColumns)
            .Settings(settingsNode)
            .Done().Ptr();
    }
}

void VectorReadMain(
    TExprContext& ctx, TPositionHandle pos,
    const TKqpTable& postingTable,
    const TIntrusivePtr<TKikimrTableMetadata> & postingTableMeta,
    const TKqpTable& mainTable,
    const TIntrusivePtr<TKikimrTableMetadata> & mainTableMeta,
    const TCoAtomList& mainColumns,
    const TKqpStreamLookupSettings& pushdownSettings,
    TExprNodePtr& read)
{
    // vector is not covered => lookup posting + lookup main with pushdown
    // vector is covered but main columns are not => lookup posting with pushdown + lookup main
    // vector and main columns are covered => lookup posting with pushdown

    const bool isVectorCovered = postingTableMeta->Columns.contains(pushdownSettings.VectorTopColumn);
    const bool isCovered = CheckIndexCovering(mainColumns, postingTableMeta);

    if (isVectorCovered && isCovered) {
        VectorReadWithPushdown(ctx, pos, postingTable, mainTableMeta, mainColumns, pushdownSettings, read);
        return;
    }

    TKqpStreamLookupSettings settings;
    settings.Strategy = EStreamLookupStrategyType::LookupRows;

    const auto postingColumns = BuildKeyColumnsList(pos, ctx, mainTableMeta->KeyColumnNames);

    if (!isVectorCovered) {
        read = Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(postingTable)
            .LookupKeys(read)
            .Columns(postingColumns)
            .Settings(settings.BuildNode(ctx, pos))
        .Done().Ptr();

        VectorReadWithPushdown(ctx, pos, mainTable, mainTableMeta, mainColumns, pushdownSettings, read);
    } else {
        VectorReadWithPushdown(ctx, pos, postingTable, mainTableMeta, postingColumns, pushdownSettings, read);

        read = Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(mainTable)
            .LookupKeys(read)
            .Columns(mainColumns)
            .Settings(settings.BuildNode(ctx, pos))
        .Done().Ptr();
    }
}

void VectorTopMain(TExprContext& ctx, const TCoTopBase& top, TExprNodePtr& read) {
    read = Build<TCoTopBase>(ctx, top.Pos())
        .CallableName(top.Ref().Content())
        .Input(read)
        .KeySelectorLambda(ctx.DeepCopyLambda(top.KeySelectorLambda().Ref()))
        .SortDirections(top.SortDirections())
        .Count(top.Count())
    .Done().Ptr();
}

// FIXME Most of this rewriting should probably be handled in kqp/opt/physical
// Logical optimizer should only rewrite it to something like TKqlReadTableVectorIndex
// This would remove the need for skipping KqpApplyExtractMembersToReadTable based on settings.VectorTopDistinct

TExprBase DoRewriteTopSortOverKMeansTree(
    const TReadMatch& match, const TMaybeNode<TCoFlatMap>& flatMap, const TExprBase& lambdaArgs, const TExprBase& lambdaBody, const TCoTopBase& top,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TKikimrTableDescription& tableDesc, const TIndexDescription& indexDesc, const TKikimrTableMetadata& implTable)
{
    Y_ASSERT(indexDesc.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree);
    const auto* levelTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Name);
    const auto* postingTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Next->Name);
    YQL_ENSURE(!implTable.Next->Next);
    YQL_ENSURE(levelTableDesc->Metadata->Name.EndsWith(NTableIndex::NKMeans::LevelTable));
    YQL_ENSURE(postingTableDesc->Metadata->Name.EndsWith(NTableIndex::NKMeans::PostingTable));

    const auto pos = match.Pos();

    const auto levelTable = BuildTableMeta(*levelTableDesc->Metadata, pos, ctx);
    const auto postingTable = BuildTableMeta(*postingTableDesc->Metadata, pos, ctx);
    const auto mainTable = BuildTableMeta(*tableDesc.Metadata, pos, ctx);

    const auto levelColumns = BuildKeyColumnsList(pos, ctx,
            std::initializer_list<std::string_view>{NTableIndex::NKMeans::IdColumn, NTableIndex::NKMeans::CentroidColumn});
    const auto& mainColumns = match.Columns();

    TNodeOnNodeOwnedMap replaces;
    TExprNode::TPtr targetVector;
    const auto levelLambda = LevelLambdaFrom(indexDesc, ctx, pos, replaces, lambdaArgs, lambdaBody, targetVector);

    auto listType = Build<TCoListType>(ctx, pos)
        .ItemType<TCoStructType>()
            .Add<TExprList>()
                .Add<TCoAtom>()
                    .Value(NTableIndex::NKMeans::ParentColumn)
                .Build()
                .Add<TCoDataType>()
                .Type()
                    .Value("Uint64")
                .Build()
            .Build()
            .Build()
        .Build()
    .Done();

    // Is it best way to do `SELECT FROM levelTable WHERE first_pk_column = 0`?
    auto lookupKey = Build<TCoAsStruct>(ctx, pos)
        .Add()
            .Add<TCoAtom>()
                .Value(NTableIndex::NKMeans::ParentColumn)
            .Build()
            .Add<TCoUint64>()
                .Literal()
                    .Value("0")
                .Build()
            .Build()
        .Build()
        .Done();

    auto lookupKeys = Build<TCoList>(ctx, pos)
        .ListType(listType)
        .FreeArgs()
            .Add(lookupKey)
        .Build()
    .Done();

    const auto levelTop = kqpCtx.Config->KMeansTreeSearchTopSize.Get().GetOrElse(1);

    const auto& kmeansDesc = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc.SpecializedIndexDescription);
    const bool withOverlap = kmeansDesc.settings().overlap_clusters() > 1;

    TKqpStreamLookupSettings settings;
    settings.Strategy = EStreamLookupStrategyType::LookupRows;
    settings.VectorTopColumn = NTableIndex::NKMeans::CentroidColumn;
    settings.VectorTopIndex = indexDesc.Name;
    settings.VectorTopTarget = targetVector;
    settings.VectorTopLimit = ctx.Builder(pos).Callable("Uint64").Atom(0, std::to_string(levelTop), TNodeFlags::Default).Seal().Build();
    auto settingsNode = settings.BuildNode(ctx, pos);

    auto read = Build<TKqlStreamLookupTable>(ctx, pos)
        .Table(levelTable)
        .LookupKeys(lookupKeys)
        .Columns(levelColumns)
        .Settings(settingsNode)
        .Done().Ptr();

    VectorReadLevel(indexDesc, ctx, pos, levelLambda, top, levelTable, levelColumns, settings.VectorTopLimit, settingsNode, read);

    settings.VectorTopColumn = indexDesc.KeyColumns.back();
    settings.VectorTopLimit = top.Count().Ptr();
    settings.VectorTopDistinct = withOverlap;
    VectorReadMain(ctx, pos, postingTable, postingTableDesc->Metadata, mainTable, tableDesc.Metadata, mainColumns, settings, read);

    if (flatMap) {
        read = Build<TCoFlatMap>(ctx, flatMap.Cast().Pos())
            .Input(read)
            .Lambda(ctx.DeepCopyLambda(flatMap.Cast().Lambda().Ref()))
        .Done().Ptr();
    }

    VectorTopMain(ctx, top, read);

    return TExprBase{read};
}

template<typename T>
TExprBase FilterLeafRows(const TExprBase& read, TExprContext& ctx, TPositionHandle pos) {
    auto leafFlag = Build<TCoUint64>(ctx, pos)
        .Literal()
            .Value(std::to_string(NTableIndex::NKMeans::PostingParentFlag)) // "9223372036854775808"
            .Build()
        .Done();
    auto prefixRowArg = ctx.NewArgument(pos, "prefixRow");
    auto prefixCluster = Build<TCoMember>(ctx, pos)
        .Struct(prefixRowArg)
        .Name().Build(NTableIndex::NKMeans::ParentColumn)
        .Done();
    return Build<TCoFlatMap>(ctx, pos)
        .Input(read)
        .Lambda()
            .Args({prefixRowArg})
            .Body<TCoOptionalIf>()
                .Predicate<T>()
                    .Left(prefixCluster)
                    .Right(leafFlag)
                    .Build()
                .Value(prefixRowArg)
                .Build()
            .Build()
        .Done();
}

TExprBase DoRewriteTopSortOverPrefixedKMeansTree(
    const TReadMatch& match, const TCoFlatMap& flatMap, const TExprBase& lambdaArgs, const TExprBase& lambdaBody, const TCoTopBase& top,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TKikimrTableDescription& tableDesc, const TIndexDescription& indexDesc, const TKikimrTableMetadata& implTable)
{
    Y_ASSERT(indexDesc.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree);
    Y_ASSERT(indexDesc.KeyColumns.size() > 1);
    const auto* levelTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Name);
    const auto* postingTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Next->Name);
    const auto* prefixTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Next->Next->Name);
    YQL_ENSURE(!implTable.Next->Next->Next);
    YQL_ENSURE(levelTableDesc->Metadata->Name.EndsWith(NTableIndex::NKMeans::LevelTable));
    YQL_ENSURE(postingTableDesc->Metadata->Name.EndsWith(NTableIndex::NKMeans::PostingTable));
    YQL_ENSURE(prefixTableDesc->Metadata->Name.EndsWith(NTableIndex::NKMeans::PrefixTable));

    const auto pos = match.Pos();

    const auto levelTable = BuildTableMeta(*levelTableDesc->Metadata, pos, ctx);
    const auto postingTable = BuildTableMeta(*postingTableDesc->Metadata, pos, ctx);
    const auto prefixTable = BuildTableMeta(*prefixTableDesc->Metadata, pos, ctx);
    const auto mainTable = BuildTableMeta(*tableDesc.Metadata, pos, ctx);

    const auto levelColumns = BuildKeyColumnsList(pos, ctx,
        std::initializer_list<std::string_view>{NTableIndex::NKMeans::IdColumn, NTableIndex::NKMeans::CentroidColumn});
    const auto prefixColumns = [&] {
        auto columns = indexDesc.KeyColumns;
        columns.back().assign(NTableIndex::NKMeans::IdColumn);
        return BuildKeyColumnsList(pos, ctx, columns);
    }();
    auto mainColumns = match.Columns();

    THashSet<TStringBuf> prefixColumnSet;
    for (size_t i = 0; i < indexDesc.KeyColumns.size()-1; i++) {
        prefixColumnSet.insert(indexDesc.KeyColumns[i]);
    }
    bool prefixInResult = false;
    TNodeOnNodeOwnedMap replaces;
    TMaybeNode<TCoLambda> mainLambda;
    const auto prefixLambda = [&] {
        auto newLambda = NewLambdaFrom(ctx, pos, replaces, flatMap.Lambda().Args().Ref(), flatMap.Lambda().Body());
        auto optionalIf = newLambda.Body().Cast<TCoOptionalIf>();
        auto oldValue = optionalIf.Value().Maybe<TCoAsStruct>();
        if (!oldValue) {
            // SELECT *
            prefixInResult = true;
            return newLambda.Ptr();
        }
        // SELECT specific fields
        auto args = newLambda.Args();
        mainLambda = NewLambdaFrom(ctx, pos, replaces, args.Ref(), oldValue.Cast());
        auto arg0 = mainLambda.Cast().Args().Arg(0).Raw();
        VisitExpr(mainLambda.Cast().Ptr(), [&](const TExprNode::TPtr& node) {
            if (const auto maybeMember = TMaybeNode<TCoMember>(node)) {
                const auto member = maybeMember.Cast();
                if (member.Struct().Raw() == arg0 &&
                    prefixColumnSet.contains(member.Name().Value())) {
                    prefixInResult = true;
                    return false;
                }
            }
            return true;
        });

        replaces.clear();
        replaces.emplace(oldValue.Raw(), args.Arg(0).Ptr());
        return ctx.NewLambda(pos,
            args.Ptr(),
            ctx.ReplaceNodes(TExprNode::TListType{optionalIf.Ptr()}, replaces));
    }();
    TExprNode::TPtr targetVector;
    const auto levelLambda = LevelLambdaFrom(indexDesc, ctx, pos, replaces, lambdaArgs, lambdaBody, targetVector);
    if (!prefixInResult) {
        // Remove prefix columns from main table read if we don't need them
        TVector<TCoAtom> filteredColumns;
        for (const auto& col: mainColumns) {
            if (!prefixColumnSet.contains(col.Value())) {
                filteredColumns.push_back(col);
            }
        }
        mainColumns = Build<TCoAtomList>(ctx, pos)
            .Add(filteredColumns)
            .Done();
    }

    auto read = match.BuildRead(ctx, prefixTable, prefixColumns).Ptr();

    read = Build<TCoFlatMap>(ctx, pos)
        .Input(read)
        .Lambda(prefixLambda)
    .Done().Ptr();

    read = Build<TDqPrecompute>(ctx, pos)
        .Input(read)
    .Done().Ptr();

    RemapIdToParent(ctx, pos, read);

    auto prefixLeafRows = FilterLeafRows<TCoCmpGreaterOrEqual>(TExprBase(read), ctx, pos);
    auto prefixRootRows = FilterLeafRows<TCoCmpLess>(TExprBase(read), ctx, pos);

    const auto levelTop = kqpCtx.Config->KMeansTreeSearchTopSize.Get().GetOrElse(1);

    const auto& kmeansDesc = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc.SpecializedIndexDescription);
    const bool withOverlap = kmeansDesc.settings().overlap_clusters() > 1;

    TKqpStreamLookupSettings settings;
    settings.Strategy = EStreamLookupStrategyType::LookupRows;
    settings.VectorTopColumn = NTableIndex::NKMeans::CentroidColumn;
    settings.VectorTopIndex = indexDesc.Name;
    settings.VectorTopTarget = targetVector;
    settings.VectorTopLimit = ctx.Builder(pos).Callable("Uint64").Atom(0, std::to_string(levelTop), TNodeFlags::Default).Seal().Build();
    auto settingsNode = settings.BuildNode(ctx, pos);
    auto levelRows = Build<TKqlStreamLookupTable>(ctx, pos)
        .Table(levelTable)
        .LookupKeys(prefixRootRows)
        .Columns(levelColumns)
        .Settings(settingsNode)
        .Done().Ptr();
    VectorReadLevel(indexDesc, ctx, pos, levelLambda, top, levelTable, levelColumns, settings.VectorTopLimit, settingsNode, levelRows);

    read = Build<TCoUnionAll>(ctx, pos)
        .Add(levelRows)
        .Add(prefixLeafRows)
        .Done().Ptr();

    settings.VectorTopColumn = indexDesc.KeyColumns.back();
    settings.VectorTopLimit = top.Count().Ptr();
    settings.VectorTopDistinct = withOverlap;
    VectorReadMain(ctx, pos, postingTable, postingTableDesc->Metadata, mainTable, tableDesc.Metadata, mainColumns, settings, read);

    if (mainLambda) {
        read = Build<TCoMap>(ctx, flatMap.Pos())
            .Input(read)
            .Lambda(mainLambda.Cast())
        .Done().Ptr();
    }

    VectorTopMain(ctx, top, read);
    return TExprBase{read};
}

} // namespace

TExprBase KqpRewriteIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (auto indexRead = TReadMatch::MatchIndexedRead(node, kqpCtx)) {

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, indexRead.Table().Path());
        const auto indexName = indexRead.Index().Value();
        auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
        // TODO(mbkkt) instead of ensure should be warning and main table read?
        YQL_ENSURE(indexDesc->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree,
            "index read doesn't support vector index: " << indexName);

        return DoRewriteIndexRead(indexRead, ctx, tableDesc, implTable, {});
    }

    return node;
}


TExprBase KqpRewriteStreamLookupIndex(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlStreamLookupIndex>()) {
        return node;
    }

    auto streamLookupIndex = node.Maybe<TKqlStreamLookupIndex>().Cast();
    auto settings = TKqpStreamLookupSettings::Parse(streamLookupIndex);

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, streamLookupIndex.Table().Path());
    const auto indexName = streamLookupIndex.Index().Value();
    auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
    // TODO(mbkkt) instead of ensure should be warning and main table lookup?
    YQL_ENSURE(indexDesc->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree,
        "stream lookup doesn't support vector index: " << indexName);

    const bool isCovered = CheckIndexCovering(streamLookupIndex.Columns(), implTable);
    if (isCovered) {
        return Build<TKqlStreamLookupTable>(ctx, node.Pos())
            .Table(BuildTableMeta(*implTable, node.Pos(), ctx))
            .LookupKeys(streamLookupIndex.LookupKeys())
            .Columns(streamLookupIndex.Columns())
            .Settings(streamLookupIndex.Settings())
            .Done();
    }

    auto keyColumnsList = BuildKeyColumnsList(tableDesc, streamLookupIndex.Pos(), ctx);

    TExprBase lookupIndexTable = Build<TKqlStreamLookupTable>(ctx, node.Pos())
        .Table(BuildTableMeta(*implTable, node.Pos(), ctx))
        .LookupKeys(streamLookupIndex.LookupKeys())
        .Columns(keyColumnsList)
        .Settings(streamLookupIndex.Settings())
        .Done();

    // We should allow lookup by null keys here,
    // because main table pk can contain nulls and we don't want to lose these rows
    settings.AllowNullKeysPrefixSize = keyColumnsList.Size();
    return Build<TKqlStreamLookupTable>(ctx, node.Pos())
        .Table(streamLookupIndex.Table())
        .LookupKeys(lookupIndexTable)
        .Columns(streamLookupIndex.Columns())
        .Settings(settings.BuildNode(ctx, node.Pos()))
        .Done();
}

/// Can push flat map node to read from table using only columns available in table description
bool CanPushFlatMap(const TCoFlatMapBase& flatMap, const TKikimrTableDescription& tableDesc, const TParentsMap& parentsMap, TVector<TString> & extraColumns) {
    auto flatMapLambda = flatMap.Lambda();
    if (!IsFilterFlatMap(flatMapLambda)) {
        return false;
    }

    const auto & flatMapLambdaArgument = flatMapLambda.Args().Arg(0).Ref();
    auto flatMapLambdaConditional = flatMapLambda.Body().Cast<TCoConditionalValueBase>();

    TSet<TString> lambdaSubset;
    if (!HaveFieldsSubset(flatMapLambdaConditional.Predicate().Ptr(), flatMapLambdaArgument, lambdaSubset, parentsMap)) {
        return false;
    }

    for (auto & lambdaColumn : lambdaSubset) {
        auto columnIndex = tableDesc.GetKeyColumnIndex(lambdaColumn);
        if (!columnIndex) {
            return false;
        }
    }

    extraColumns.insert(extraColumns.end(), lambdaSubset.begin(), lambdaSubset.end());
    return true;
}

// Check that the key selector doesn't include any columns from the applyColumns or other
// complex expressions
bool KeySelectorAllMembers(const TCoLambda& lambda, const TSet<TString> & applyColumns) {
    if (auto body = lambda.Body().Maybe<TCoMember>()) {
        auto attrRef = body.Cast().Name().StringValue();
        if (applyColumns.contains(attrRef)){
            return false;
        }
    }
    else if (auto body = lambda.Body().Maybe<TExprList>()) {
        for (auto item : body.Cast()) {
            if (auto member = item.Maybe<TCoMember>()) {
                auto attrRef = member.Cast().Name().StringValue();
                if (applyColumns.contains(attrRef)) {
                    return false;
                }
            }
            else {
                return false;
            }
        }
    }
    else {
        return false;
    }
    return true;
}

// Construct a new lambda with renamed attributes based on the mapping
// If we see a complex expression in the key selector, we just pass it on into
// the new lambda
TCoLambda RenameKeySelector(const TCoLambda& lambda, TExprContext& ctx, const THashMap<TString,TString>& map) {
    // If its single member lambda body
    if (lambda.Body().Maybe<TCoMember>()) {
        auto attrRef = lambda.Body().Cast<TCoMember>().Name().StringValue();
        auto mapped = map.Value(attrRef,attrRef);

        return Build<TCoLambda>(ctx, lambda.Pos())
                .Args({"argument"})
                .Body<TCoMember>()
                    .Struct("argument")
                    .Name().Build(mapped)
                    .Build()
                .Done();
    }
    // Else its a list of members lambda body
    else {
        TCoArgument arg = Build<TCoArgument>(ctx, lambda.Pos())
            .Name("Arg")
            .Done();

        TVector<TExprBase> members;

        for (auto item : lambda.Body().Cast<TExprList>()) {
            auto attrRef = item.Cast<TCoMember>().Name().StringValue();
            auto mapped = map.Value(attrRef,attrRef);

            auto member = Build<TCoMember>(ctx, lambda.Pos())
                .Struct(arg)
                .Name().Build(mapped)
                .Done();
            members.push_back(member);
        }

        return Build<TCoLambda>(ctx, lambda.Pos())
                .Args({arg})
                .Body<TExprList>()
                    .Add(members)
                    .Build()
                .Done();
    }
}


// If we have a top-sort over flatmap, we can push it throught is, so that the
// RewriteTopSortOverIndexRead rule can fire next. If the flatmap renames some of the sort
// attributes, we need to use the original names in the top-sort. When pushing TopSort below
// FlatMap, we change FlatMap to OrderedFlatMap to preserve the order of its input.
TExprBase KqpRewriteTopSortOverFlatMap(const TExprBase& node, TExprContext& ctx) {

    // Check that we have a top-sort and a flat-map directly below it
    if(!node.Maybe<TCoTopBase>()) {
        return node;
    }

    const auto topBase = node.Maybe<TCoTopBase>().Cast();

    if (!topBase.Input().Maybe<TCoFlatMap>()) {
        return node;
    }

    auto flatMap = topBase.Input().Maybe<TCoFlatMap>().Cast();

    // Check that the flat-map is a rename or apply and compute the mapping
    // Also compute the apply mapping, if we have a key selector that mentions
    // apply columns, we cannot push the TopSort
    TExprNode::TPtr structNode;
    THashMap<TString, TString> renameMap;
    TSet<TString> applyColumns;
    if (!IsRenameOrApplyFlatMapWithMapping(flatMap, structNode, renameMap, applyColumns)) {
        return node;
    }

    // Check that the key selector doesn't contain apply columns or expressions
    if (!KeySelectorAllMembers(topBase.KeySelectorLambda(), applyColumns)) {
        return node;
    }

    // Rename the attributes in sort key selector of the sort
    TCoLambda newKeySelector = RenameKeySelector(topBase.KeySelectorLambda(), ctx, renameMap);

    // Swap top sort and rename operators
    auto flatMapInput = Build<TCoTopBase>(ctx, node.Pos())
        .CallableName(node.Ref().Content())
        .Input(flatMap.Input())
        .KeySelectorLambda(newKeySelector)
        .SortDirections(topBase.SortDirections())
        .Count(topBase.Count())
        .Done();

    return Build<TCoOrderedFlatMap>(ctx, node.Pos())
        .Input(flatMapInput)
        .Lambda(ctx.DeepCopyLambda(flatMap.Lambda().Ref()))
        .Done();
}


namespace {

bool StringOrAtomOrParameter(const TExprBase& exprBase) {
    auto expr = exprBase.Maybe<TCoJust>() ? exprBase.Maybe<TCoJust>().Cast().Input() : exprBase;
    return expr.Maybe<TCoString>() || expr.Maybe<TCoAtom>() || expr.Maybe<TCoParameter>();
}

bool DoubleOrParameter(const TExprBase& exprBase) {
    auto unwrapped = exprBase.Maybe<TCoJust>() ? exprBase.Maybe<TCoJust>().Cast().Input() : exprBase;
    if (!unwrapped.Maybe<TCoDouble>() && !unwrapped.Maybe<TCoParameter>() && !unwrapped.Maybe<TCoFloat>()) {
        return false;
    }
    return true;
}

}


TExprNode::TPtr BuildPostfiltersForMatch(TExprContext& ctx, TPositionHandle pos, TExprNode::TPtr pattern, TExprNode::TPtr column)  {
    auto patternExpr = ctx.Builder(pos)
        .Callable("Apply")
            .Callable(0, "Udf")
                .Atom(0, "Re2.PatternFromLike")
            .Seal()
            .Add(1, pattern)
        .Seal()
        .Build();

    auto optionsExpr = ctx.Builder(pos)
        .Callable("NamedApply")
            .Callable(0, "Udf")
                .Atom(0, "Re2.Options")
            .Seal()
            .List(1)
            .Seal()
            .Callable(2, "AsStruct")
                .List(0)
                    .Atom(0, "CaseSensitive")
                    .Callable(1, "Bool")
                        .Atom(0, "false", TNodeFlags::Default)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
    .Build();

    auto result = ctx.Builder(pos)
        .Callable("Apply")
            .Callable(0, "AssumeStrict")
                .Callable(0, "Udf")
                    .Atom(0, "Re2.Match")
                    .List(1)
                        .Add(0, patternExpr)
                        .Add(1, optionsExpr)
                    .Seal()
                .Seal()
            .Seal()
            .Add(1, column)
        .Seal()
    .Build();

    return result;
}

struct TFulltextQuery {
    TExprNode::TPtr Node;
    TExprNode::TPtr Column;
    TExprNode::TPtr Query;
    TExprNode::TPtr NamedOptions;
    THashMap<std::string_view, TExprNode::TPtr> Settings;
    bool StartsWithAny = false;
    bool EndsWithAny = false;

    bool IsScoreQuery() const {
        return Node->Content() == "FulltextScore";
    }

    bool IsMatchQuery() const {
        return Node->Content() == "FulltextMatch";
    }

    TString GetModeIfAny() const {
        if (!NamedOptions)
            return TString();

        for(auto& child : NamedOptions->Children()) {
            auto arg = TExprBase(child).Cast<TCoNameValueTuple>();
            if (arg.Name().StringValue() == "Mode") {
                return arg.Value().Cast<TCoString>().Literal().StringValue();
            }
        }

        return TString();
    }

    bool IsValid() {
        if (!Node || !Column || !Query || !StringOrAtomOrParameter(TExprBase(Query))) {
            return false;
        }

        if (TExprBase(Column).Maybe<TCoFlatMap>()) {
            auto lambda = TExprBase(Column).Cast<TCoFlatMap>();
            if (lambda.Lambda().Body().Maybe<TCoJust>() && lambda.Lambda().Body().Cast<TCoJust>().Ptr()->Head().Content() == "ToString") {
                Column = TExprBase(Column).Cast<TCoFlatMap>().Input().Ptr();
            }
        }

        if (!TExprBase(Column).Maybe<TCoMember>()) {
            return false;
        }

        if (!NamedOptions) {
            return true;
        }

        for(auto& arg : NamedOptions->Children()) {
            auto nameValueTuple = TExprBase(arg).Cast<TCoNameValueTuple>();
            TExprBase value = TExprBase(nameValueTuple.Value().Cast().Ptr());
            TString name = nameValueTuple.Name().StringValue();
            if (name == TKqpReadTableFullTextIndexSettings::BFactorSettingName && !DoubleOrParameter(value)) {
                return false;
            }

            if (name == TKqpReadTableFullTextIndexSettings::K1FactorSettingName  && !DoubleOrParameter(value)) {
                return false;
            }

            if (name == TKqpReadTableFullTextIndexSettings::DefaultOperatorSettingName && !StringOrAtomOrParameter(value)) {
                return false;
            }

            if (name == TKqpReadTableFullTextIndexSettings::MinimumShouldMatchSettingName && !StringOrAtomOrParameter(value)) {
                return false;
            }
        }

        return true;
    };

    operator bool() const {
        return Node != nullptr;
    }

    static TFulltextQuery Match(TExprNode::TPtr node, TExprContext& ctx) {

        if (node->Content() == "FulltextMatch" || node->Content() == "FulltextScore") {
            if (!EnsureArgsCount(*node, 2, ctx)) {
                return {};
            }

            TExprNode::TPtr posArgs = node;
            TExprNode::TPtr namedArg = nullptr;
            if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                posArgs = node->Child(0);
                namedArg = node->Child(1);
            }

            return TFulltextQuery{.Node=node, .Column=posArgs->Child(0), .Query=posArgs->Child(1), .NamedOptions=namedArg};
        } else if (node->Content() == "Apply") {
            const auto& udf = SkipCallables(node->Head(), {"AssumeStrict"});
            auto column = node->TailPtr();
            const auto& udfName = udf.Head();
            if (!udfName.Content().starts_with("Re2.Match")) {
                return {};
            }

            auto pattern = udf.Child(1);
            auto maybeLike = pattern->Child(0);

            if (maybeLike->Content() != "Apply") {
                return {};
            }

            const auto& patternUdf = SkipCallables(maybeLike->Head(), {"AssumeStrict"});
            if (!patternUdf.Head().Content().starts_with("Re2.PatternFromLike")) {
                return {};
            }

            TExprNode::TPtr query = maybeLike->TailPtr();
            if (!StringOrAtomOrParameter(TExprBase(query))) {
                return {};
            }

            return TFulltextQuery{.Node=node, .Column=column, .Query=query};
        } else if (node->Content() == "StartsWith") {
            auto query = TFulltextQuery{.Node=node, .Column=node->HeadPtr(), .Query=node->TailPtr()};
            query.EndsWithAny = true;
            return query;
        } else if (node->Content() == "EndsWith") {
            auto query = TFulltextQuery{.Node=node, .Column=node->HeadPtr(), .Query=node->TailPtr()};
            query.StartsWithAny = true;
            return query;
        } else if (node->Content() == "StringContains") {
            auto query = TFulltextQuery{.Node=node, .Column=node->HeadPtr(), .Query=node->TailPtr()};
            query.StartsWithAny = true;
            query.EndsWithAny = true;
            return query;
        } else if (node->Content() == "==") {
            auto query = TFulltextQuery{.Node=node, .Column=node->HeadPtr(), .Query=node->TailPtr()};
            query.StartsWithAny = false;
            query.EndsWithAny = false;
            return query;
        }

        return {};
    }
};

struct TFullTextApplyParseResult {
    TExprNode::TPtr BFactor;
    TExprNode::TPtr K1Factor;
    TExprNode::TPtr DefaultOperator;
    TExprNode::TPtr MinimumShouldMatch;
    TExprNode::TPtr ScoreRestriction;

    TNodeOnNodeOwnedMap Replaces;
    std::vector<TFulltextQuery> Queries;

    ui64 FulltextMatch = 0;
    ui64 FulltextScore = 0;

    bool IsScoreApply = false;
    bool HasErrors = false;

    TFullTextApplyParseResult()
    {}

    TVector<TCoNameValueTuple> Settings(TExprContext& ctx, TPositionHandle pos) {
        TVector<TCoNameValueTuple> settings;
        auto& query = Queries[0];
        if (!query.NamedOptions) return settings;
        for(auto& arg : query.NamedOptions->Children()) {
            auto nameValueTuple = TExprBase(arg).Cast<TCoNameValueTuple>();
            TExprBase value = TExprBase(nameValueTuple.Value().Cast().Ptr());
            TString name = nameValueTuple.Name().StringValue();
            settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name<TCoAtom>()
                    .Value(nameValueTuple.Name().StringValue())
                    .Build()
                .Value(value)
                .Done());
        }

        return settings;
    }
};

TExprNode::TPtr FindScoreOrScoreOverMemberOfStruct(const TExprNode::TPtr& node) {
    if (node->Content() == "FulltextScore") {
        return node;
    }

    auto exprBase = TExprBase(node);
    if (exprBase.Maybe<TCoMember>()) {
        auto member = exprBase.Cast<TCoMember>();
        for(const auto& child : member.Struct().Cast<TCoAsStruct>()) {
            if (child.Item(1).Ptr()->Content() == "FulltextScore") {
                return child.Item(1).Ptr();
            }
        }
    }

    return nullptr;
}

void VisitExprSkipOptionalIfValue(const TExprNode::TPtr& node, const TExprVisitPtrFunc& preFunc,
     TNodeSet& visitedNodes, TExprNode::TPtr& scoreRestriction)
{
    if (!visitedNodes.emplace(node.Get()).second) {
        return;
    }

    if (!preFunc || preFunc(node)) {
        if (node->Content() == "OptionalIf") {
            auto optionalIf = TExprBase(node).Maybe<TCoOptionalIf>().Cast();
            VisitExprSkipOptionalIfValue(optionalIf.Predicate().Ptr(), preFunc, visitedNodes, scoreRestriction);
            return;
        }

        if (node->Content() == "AsStruct") {
            auto asStruct = TExprBase(node).Maybe<TCoAsStruct>().Cast();
            for (auto child : asStruct) {
                VisitExprSkipOptionalIfValue(child.Item(1).Ptr(), preFunc, visitedNodes, scoreRestriction);
            }
            return;
        }

        if (node->Content() == ">" || node->Content() == "<") {
            auto compare = TExprBase(node).Maybe<TCoCompare>().Cast();
            if (compare.Left().Maybe<TCoIntegralCtor>() && compare.Left().Cast<TCoIntegralCtor>().Literal().Value() == "0" && node->Content() == "<") {
                auto score = FindScoreOrScoreOverMemberOfStruct(compare.Right().Ptr());
                if (score) {
                    scoreRestriction = node;
                    VisitExprSkipOptionalIfValue(score, preFunc, visitedNodes, scoreRestriction);
                }
            }

            if (compare.Right().Maybe<TCoIntegralCtor>() && compare.Right().Cast<TCoIntegralCtor>().Literal().Value() == "0" && node->Content() == ">") {
                auto score = FindScoreOrScoreOverMemberOfStruct(compare.Left().Ptr());
                if (score) {
                    scoreRestriction = node;
                    VisitExprSkipOptionalIfValue(score, preFunc, visitedNodes, scoreRestriction);
                }
            }

            return;
        }

        for (const auto& child : node->Children()) {
            VisitExprSkipOptionalIfValue(child, preFunc, visitedNodes, scoreRestriction);
        }
    }
}


TFullTextApplyParseResult FindMatchingApply(const TExprBase& node, TExprContext& ctx, std::string_view indexName, bool isNgram) {

    TFullTextApplyParseResult result;
    static const THashSet<TString> AllowedFulltextExprs = {
        "And",
        "Member",
        "OptionalIf",
        "Just",
        "AsStruct",
        ">",
        "<",
        "Apply",
        "AssumeStrict",
        "Coalesce",
    };

    TNodeSet visitedNodes;

    VisitExprSkipOptionalIfValue(node.Ptr(), [&] (const TExprNode::TPtr& expr) {
        bool isGreenNode = false;
        if (AllowedFulltextExprs.contains(expr->Content())) {
            isGreenNode = true;
        } else {
            isGreenNode = false;
        }

        if (auto match = TFulltextQuery::Match(expr, ctx) ; match.IsValid()) {
            if (match.IsScoreQuery()) {
                auto newMember = Build<TCoMember>(ctx, match.Query->Pos())
                    .Name().Build(NTableIndex::NFulltext::FullTextRelevanceColumn)
                        .Struct(TExprBase(match.Column).Cast<TCoMember>().Struct())
                    .Done();
                result.Replaces.emplace(match.Node.Get(), newMember.Ptr());
                result.IsScoreApply = true;
            }

            if (match.IsMatchQuery()) {
                auto mode = match.GetModeIfAny();
                mode.to_lower();
                if (isNgram && mode == "wildcard") {
                    auto postfilter = BuildPostfiltersForMatch(ctx, node.Pos(), match.Query, match.Column);
                    result.Replaces.emplace(match.Node.Get(), postfilter);
                } else {
                    auto newMember = Build<TCoBool>(ctx, node.Pos()).Literal().Build("true").Done();
                    result.Replaces.emplace(match.Node.Get(), newMember.Ptr());
                }
            }

            result.Queries.emplace_back(std::move(match));
            return false;
        }

        if (!isGreenNode) {
            return false;
        }


        return true;
    }, visitedNodes, result.ScoreRestriction);

    VisitExpr(node.Ptr(), [&] (const TExprNode::TPtr& expr) {
        if (auto match = TFulltextQuery::Match(expr, ctx) ; match.IsValid()) {
            if (match.IsScoreQuery()) {
                result.FulltextScore++;
            } else {
                result.FulltextMatch++;
            }
            return false;
        }

        return true;
    });

    bool scoreRestrictionFound = result.ScoreRestriction != nullptr;
    if (!result.IsScoreApply) {
        scoreRestrictionFound = true;
    }

    if (result.ScoreRestriction) {
        auto newMember = Build<TCoBool>(ctx, node.Pos()).Literal().Build("true").Done().Ptr();
        result.Replaces.emplace(result.ScoreRestriction.Get(), newMember);
    }

    TString explain = "";
    if (result.FulltextScore >= 1 && result.FulltextMatch >= 1) {
        result.HasErrors = true;
        explain = " Multiple fulltext predicates in a single read are not supported.";
    } else if (result.FulltextScore > 1) {
        result.HasErrors = true;
        explain = " Multiple fulltext score predicates in a single read are not supported.";
    } else if (result.Queries.empty()) {
        result.HasErrors = true;
        explain = " FulltextMatch/FulltextScore node is not reachable by conjunctions.";
    } else if (result.FulltextScore + result.FulltextMatch == 0) {
        result.HasErrors = true;
        explain = " FulltextMatch/FulltextScore predicate is not valid or not found.";
    } else if (result.FulltextScore > 0 && !scoreRestrictionFound) {
        result.HasErrors = true;
        explain = " Score restriction is not found in the predicate. It's required to put FulltextScore() > 0 constraint in the where clause.";
    }

    if (result.HasErrors) {
        auto message = TStringBuilder{} << "Unsupported index access, index name: " << indexName << ". " << explain;
        TIssue baseIssue{ctx.GetPosition(node.Pos()), message};
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_BAD_REQUEST, baseIssue);

        TIssue subIssue{ctx.GetPosition(node.Pos()), TStringBuilder{} << "Unsupported predicate is used to access index: " << indexName };
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, subIssue);
        baseIssue.AddSubIssue(MakeIntrusive<TIssue>(std::move(subIssue)));
        ctx.AddError(baseIssue);
    }

    return result;
}

TMaybeNode<TExprBase> KqpPushLimitOverFullText(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx)
{
    if (!node.Maybe<TCoTopBase>()) {
        return node;
    }

    auto topSort = node.Maybe<TCoTopBase>().Cast();
    auto read = topSort.Input().Maybe<TKqlReadTableFullTextIndex>();
    if (!read) {
        return node;
    }

    auto settings = TKqpReadTableFullTextIndexSettings::Parse(read.Cast().Settings());
    if (settings.ItemsLimit){
        return node;
    }

    auto directions = GetSortDirection(topSort.SortDirections());
    auto sortingKeys = ExtractSortingKeys(topSort.KeySelectorLambda());
    if (directions != ESortDirection::Reverse || sortingKeys.size() != 1) {
        return node;
    }

    if (sortingKeys.front() != NTableIndex::NFulltext::FullTextRelevanceColumn) {
        return node;
    }

    settings.SetItemsLimit(topSort.Count().Ptr());

    auto input = ctx.ChangeChild(
        read.Cast().Ref(), TKqlReadTableFullTextIndex::idx_Settings, settings.BuildNode(ctx, node.Pos()).Ptr());

    return ctx.ChangeChild(
        node.Ref(), TCoTopSort::idx_Input, std::move(input));
};

TMaybeNode<TExprBase> KqpRewriteFlatMapOverFullTextMatch(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }

    auto flatMap = node.Maybe<TCoFlatMap>().Cast();

    auto read = TReadMatch::MatchFullTextRead(flatMap.Input(), kqpCtx);
    if (!read) {
        return node;
    }

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
    YQL_ENSURE(tableDesc.Metadata);
    auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(read.Index().Value());
    if (indexDesc->Type != TIndexDescription::EType::GlobalFulltextPlain
        && indexDesc->Type != TIndexDescription::EType::GlobalFulltextRelevance) {
        return {};
    }

    const auto& fulltextMetadataInfo = std::get<NKikimrSchemeOp::TFulltextIndexDescription>(indexDesc->SpecializedIndexDescription);

    bool isNgram = false;
    for(const auto& analyzer : fulltextMetadataInfo.GetSettings().columns()) {
        if (analyzer.analyzers().use_filter_ngram() || analyzer.analyzers().use_filter_edge_ngram()) {
            isNgram = true;
        }
    }

    auto result = FindMatchingApply(flatMap.Lambda().Body(), ctx, read.Index().Value(), isNgram);
    if (result.HasErrors) {
        return {};
    }

    YQL_ENSURE(result.Queries.size() >= 1);

    TVector<TExprBase> queryData;
    for(const auto& query : result.Queries) {
        if (!queryData.empty()) {
            queryData.push_back(Build<TCoString>(ctx, node.Pos()).Literal().Build(" ").Done());
        }

        if (query.StartsWithAny) {
            queryData.push_back(Build<TCoString>(ctx, node.Pos()).Literal().Build("%").Done());
        }

        queryData.push_back(TExprBase(query.Query));

        if (query.EndsWithAny) {
            queryData.push_back(Build<TCoString>(ctx, node.Pos()).Literal().Build("%").Done());
        }
    }

    auto searchColumn = TExprBase(result.Queries[0].Column).Maybe<TCoMember>().Cast();

    auto searchColumns = Build<TCoAtomList>(ctx, node.Pos())
        .Add(Build<TCoAtom>(ctx, node.Pos())
            .Value(searchColumn.Name().StringValue())
            .Done())
        .Done();

    TVector<TCoAtom> resultColumnsVector;
    for(const auto& column: read.Columns()) {
        resultColumnsVector.push_back(column);
    }

    if (result.Queries[0].IsScoreQuery()) {
        resultColumnsVector.push_back(Build<TCoAtom>(ctx, node.Pos())
            .Value(NTableIndex::NFulltext::FullTextRelevanceColumn)
            .Done());
    }

    auto settings = result.Settings(ctx, node.Pos());

    auto resultColumns = Build<TCoAtomList>(ctx, node.Pos())
        .Add(resultColumnsVector)
        .Done();

    auto newInput = Build<TKqlReadTableFullTextIndex>(ctx, node.Pos())
        .Table(read.Table())
        .Index(read.Index())
        .Columns(resultColumns.Ptr())
        .Query<TExprList>().Add(queryData).Build()
        .QueryColumns(searchColumns.Ptr())
        .Settings<TCoNameValueTupleList>().Add(settings).Build()
        .Done();

    auto newLambdaBody = TCoLambda{ctx.NewLambda(
        flatMap.Lambda().Pos(),
        std::move(flatMap.Lambda().Args().Ptr()),
        ctx.ReplaceNodes(TExprNode::TListType{flatMap.Lambda().Body().Ptr()}, result.Replaces))};

    auto res = Build<TCoFlatMap>(ctx, read.Pos())
        .Input(newInput)
        .Lambda(NewLambdaFrom(ctx, flatMap.Lambda().Pos(), result.Replaces, flatMap.Lambda().Args().Ref(), newLambdaBody.Body()))
        .Done();
    return res;
}

// The index and main table have same number of rows, so we can push a copy of TCoTopSort or TCoTake
// through TKqlLookupTable.
// The simplest way is to match TopSort or Take over TKqlReadTableIndex.
// Additionally if there is TopSort or Take over filter, and filter depends only on columns available in index,
// we also push copy of filter through TKqlLookupTable.
TExprBase KqpRewriteTopSortOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
                                        const TParentsMap& parentsMap) {
    if (!node.Maybe<TCoTopBase>()) {
        return node;
    }

    const auto topBase = node.Maybe<TCoTopBase>().Cast();

    auto maybeFlatMap = topBase.Input().Maybe<TCoFlatMap>();
    TExprBase input = maybeFlatMap ? maybeFlatMap.Cast().Input() : topBase.Input();

    if (auto readTableIndex = TReadMatch::MatchSyncVectorKMeansTreeRead(input, kqpCtx)) {
        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
        const auto indexName = readTableIndex.Index().Value();
        auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
        YQL_ENSURE(indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree);

        auto reject = [&] (std::string_view because) {
            auto message = TStringBuilder{} << "Given predicate is not suitable for used index: "
                << indexName << ", because " << because << ", node dump:\n" << node.Ref().Dump();
            TIssue issue{ctx.GetPosition(readTableIndex.Pos()), message};
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, issue);
            ctx.AddWarning(issue);
            return node;
        };
        auto lambdaArgs = topBase.KeySelectorLambda().Args();
        auto lambdaBody = topBase.KeySelectorLambda().Body();
        TString error;
        bool canUseVectorIndex = CanUseVectorIndex(*indexDesc, lambdaBody, topBase, error);
        if (indexDesc->KeyColumns.size() > 1) {
            if (!canUseVectorIndex) {
                return reject(TStringBuilder() << "sorting must contain distance: "
                    << error << ", reference distance from projection not supported yet");
            }
            if (!maybeFlatMap.Lambda().Body().Maybe<TCoOptionalIf>()) {
                return reject("only simple conditions supported for now");
            }
            return DoRewriteTopSortOverPrefixedKMeansTree(readTableIndex, maybeFlatMap.Cast(), lambdaArgs, lambdaBody, topBase,
                                                          ctx, kqpCtx, tableDesc, *indexDesc, *implTable);
        }
        if (!canUseVectorIndex) {
            auto argument = lambdaBody.Maybe<TCoMember>().Struct().Maybe<TCoArgument>();
            if (!argument) {
                return reject(TStringBuilder() << "sorting must contain distance: " << error);
            }
            auto asStruct = maybeFlatMap.Lambda().Body().Maybe<TCoJust>().Input().Maybe<TCoAsStruct>();
            if (!asStruct) {
                return reject("only simple projection with distance referenced in sorting supported for now");
            }

            // TODO(mbkkt) I think variable name shouldn't matter, and I only need to check that result of FlatMap
            // used as argument for member access in top lambda. The name should be same, and it's same in the tests
            // and was same in real world, but for some reason recently it starts to fail in real-world, so I comment it out
            // const auto argumentName = argument.Cast().Name();
            // if (absl::c_none_of(maybeFlatMap.Cast().Lambda().Args(),
            //         [&](const TCoArgument& argument) { return argumentName == argument.Name(); })) {
            //     return reject("...");
            // }

            const auto memberName = lambdaBody.Cast<TCoMember>().Name().Value();
            for (const auto& arg : asStruct.Cast().Args()) {
                if (!arg->IsList()) {
                    continue;
                }
                auto argChildren = arg->Children();
                if (argChildren.size() != 2) {
                    continue;
                }
                auto atom = TExprBase{argChildren[0].Get()}.Maybe<TCoAtom>();
                if (!atom || atom.Cast().Value() != memberName) {
                    continue;
                }
                lambdaBody = TExprBase{argChildren[1]};
                canUseVectorIndex = CanUseVectorIndex(*indexDesc, lambdaBody, topBase, error);
                break;
            }
            if (!canUseVectorIndex) {
                return reject(TStringBuilder() << "projection or sorting must contain distance: " << error);
            }
            lambdaArgs = maybeFlatMap.Cast().Lambda().Args();
        }
        return DoRewriteTopSortOverKMeansTree(readTableIndex, maybeFlatMap, lambdaArgs, lambdaBody, topBase,
                                              ctx, kqpCtx, tableDesc, *indexDesc, *implTable);
    }

    auto readTableIndex = TReadMatch::MatchIndexedRead(input, kqpCtx);
    if (!readTableIndex)
        return node;

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
    const auto indexName = readTableIndex.Index().Value();
    auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);

    const auto& implTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable->Name);
    YQL_ENSURE(implTableDesc.Metadata->Name.EndsWith(NTableIndex::ImplTable));

    TVector<TString> extraColumns;

    if (maybeFlatMap && !CanPushFlatMap(maybeFlatMap.Cast(), implTableDesc, parentsMap, extraColumns))
        return node;

    if (!CanPushTopSort(topBase, implTableDesc, &extraColumns)) {
        return node;
    }

    bool needSort = node.Maybe<TCoTopSort>() && !IsKeySelectorPkPrefix(topBase.KeySelectorLambda(), implTableDesc);

    auto filter = [&](const TExprBase& in) mutable {
        auto sortInput = in;

        if (maybeFlatMap)
        {
            sortInput = Build<TCoFlatMap>(ctx, node.Pos())
                .Input(in)
                .Lambda(ctx.DeepCopyLambda(maybeFlatMap.Lambda().Ref()))
                .Done();
        }

        auto newTop = Build<TCoTopBase>(ctx, node.Pos())
            .CallableName(needSort ? TCoTopSort::CallableName() : TCoTop::CallableName())
            .Input(sortInput)
            .KeySelectorLambda(ctx.DeepCopyLambda(topBase.KeySelectorLambda().Ref()))
            .SortDirections(topBase.SortDirections())
            .Count(topBase.Count())
            .Done();

        return TExprBase(newTop);
    };

    auto lookup = DoRewriteIndexRead(readTableIndex, ctx, tableDesc, implTable,
        extraColumns, filter);

    if (!lookup.Maybe<TKqlStreamLookupTable>()) {
        return node;
    }

    return Build<TCoTopBase>(ctx, node.Pos())
        .CallableName(node.Ref().Content())
        .Input(lookup)
        .KeySelectorLambda(ctx.DeepCopyLambda(topBase.KeySelectorLambda().Ref()))
        .SortDirections(topBase.SortDirections())
        .Count(topBase.Count())
        .Done();
}

TExprBase KqpRewriteTakeOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
                                    const TParentsMap& parentsMap) {
    if (!node.Maybe<TCoTake>()) {
        return node;
    }

    auto take = node.Maybe<TCoTake>().Cast();

    auto maybeFlatMap = take.Input().Maybe<TCoFlatMap>();
    TExprBase input = maybeFlatMap ? maybeFlatMap.Cast().Input() : take.Input();

    auto readTableIndex = TReadMatch::MatchIndexedRead(input, kqpCtx);
    if (!readTableIndex)
        return node;

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
    const auto indexName = readTableIndex.Index().Value();
    auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
    if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
        // TODO(mbkkt) some warning?
        return node;
    }
    const auto& implTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable->Name);

    TVector<TString> extraColumns;
    if (maybeFlatMap && !CanPushFlatMap(maybeFlatMap.Cast(), implTableDesc, parentsMap, extraColumns))
        return node;

    auto filter = [&](const TExprBase& in) mutable {
        auto takeChild = in;

        if (maybeFlatMap)
        {
            takeChild = Build<TCoFlatMap>(ctx, node.Pos())
                .Input(in)
                .Lambda(ctx.DeepCopyLambda(maybeFlatMap.Lambda().Ref()))
                .Done();
        }

        // Change input for TCoTake. New input is result of TKqlReadTable.
        return TExprBase(ctx.ChangeChild(*node.Ptr(), 0, takeChild.Ptr()));
    };

    return DoRewriteIndexRead(readTableIndex, ctx, tableDesc, implTable, extraColumns, filter);
}

} // namespace NKikimr::NKqp::NOpt
