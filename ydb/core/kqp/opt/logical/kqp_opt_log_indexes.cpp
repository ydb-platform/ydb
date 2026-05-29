#include "kqp_opt_log_json_index.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
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
                || indexDesc->Type == TIndexDescription::EType::GlobalFulltextRelevance
                || indexDesc->Type == TIndexDescription::EType::GlobalJson) {
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
                || indexDesc->Type == TIndexDescription::EType::GlobalFulltextRelevance
                || indexDesc->Type == TIndexDescription::EType::GlobalJson) {
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

    static TReadMatch MatchJsonRead(const TExprBase& node, const TKqpOptimizeContext& kqpCtx) {
        auto read = TReadMatch::Match(node, kqpCtx);
        if (!read || read.Index().Value().empty()) {
            return {};
        }

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
        YQL_ENSURE(tableDesc.Metadata);
        auto [_, indexDesc] = tableDesc.Metadata->GetIndex(read.Index().Value());
        if (indexDesc->Type != TIndexDescription::EType::GlobalJson) {
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

ui32 GetKMeansTreeSearchTopSize(const TKqpOptimizeContext& kqpCtx, const bool withOverlap) {
    const ui32 defaultLevelTop = withOverlap ? 4 : 10;
    return kqpCtx.Config->KMeansTreeSearchTopSize.Get().GetOrElse(defaultLevelTop);
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

    const auto& kmeansDesc = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc.SpecializedIndexDescription);
    const bool withOverlap = kmeansDesc.settings().overlap_clusters() > 1;
    const auto levelTop = GetKMeansTreeSearchTopSize(kqpCtx, withOverlap);

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
    TExprContext& ctx, TTypeAnnotationContext& typesCtx, const TKqpOptimizeContext& kqpCtx,
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
        std::initializer_list<std::string_view>{
            NTableIndex::NKMeans::ParentColumn,
            NTableIndex::NKMeans::IdColumn,
            NTableIndex::NKMeans::CentroidColumn});
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

    TVector<TString> prefixKeys;
    for (size_t i = 0; i < indexDesc.KeyColumns.size() - 1; ++i) {
        prefixKeys.push_back(indexDesc.KeyColumns[i]);
    }

    size_t numPrefixGroups = 1;
    {
        THashSet<TString> possibleKeys;
        TPredicateExtractorSettings predSettings;
        predSettings.MergeAdjacentPointRanges = false;
        predSettings.HaveNextValueCallable = true;
        predSettings.BuildLiteralRange = true;
        if (kqpCtx.QueryCtx->RuntimeParameterSizeLimitSatisfied &&
            kqpCtx.QueryCtx->RuntimeParameterSizeLimit > 0)
        {
            predSettings.ExternalParameterMaxSize = kqpCtx.QueryCtx->RuntimeParameterSizeLimit;
        }
        auto extractor = MakePredicateRangeExtractor(predSettings);
        YQL_ENSURE(tableDesc.SchemeNode);
        if (extractor->Prepare(flatMap.Lambda().Ptr(), *tableDesc.SchemeNode, possibleKeys, ctx, typesCtx)) {
            auto buildResult = extractor->BuildComputeNode(prefixKeys, ctx, typesCtx);
            if (buildResult.PointPrefixLen >= prefixKeys.size() && buildResult.ExpectedMaxRanges.Defined()) {
                numPrefixGroups = std::max<size_t>(1, *buildResult.ExpectedMaxRanges);
            }
        }
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

    const auto& kmeansDesc = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc.SpecializedIndexDescription);
    const bool withOverlap = kmeansDesc.settings().overlap_clusters() > 1;

    const auto levelTop = GetKMeansTreeSearchTopSize(kqpCtx, withOverlap);
    const auto levelTopTotal = levelTop * numPrefixGroups;

    TKqpStreamLookupSettings firstLevelSettings;
    firstLevelSettings.Strategy = EStreamLookupStrategyType::LookupRows;
    firstLevelSettings.VectorTopColumn = NTableIndex::NKMeans::CentroidColumn;
    firstLevelSettings.VectorTopIndex = indexDesc.Name;
    firstLevelSettings.VectorTopTarget = targetVector;
    firstLevelSettings.VectorTopLimit = ctx.Builder(pos).Callable("Uint64").Atom(0, std::to_string(levelTopTotal), TNodeFlags::Default).Seal().Build();
    auto firstLevelSettingsNode = firstLevelSettings.BuildNode(ctx, pos);
    auto levelRows = Build<TKqlStreamLookupTable>(ctx, pos)
        .Table(levelTable)
        .LookupKeys(prefixRootRows)
        .Columns(levelColumns)
        .Settings(firstLevelSettingsNode)
        .Done().Ptr();

    {
        auto levelTopCount = ctx.Builder(pos)
            .Callable("Uint64")
            .Atom(0, std::to_string(levelTopTotal), TNodeFlags::Default)
            .Seal().Build();
        TKqpStreamLookupSettings levelSettings;
        levelSettings.Strategy = EStreamLookupStrategyType::LookupRows;
        auto levelSettingsNode = levelSettings.BuildNode(ctx, pos);
        VectorReadLevel(indexDesc, ctx, pos, levelLambda, top, levelTable, levelColumns, levelTopCount, levelSettingsNode, levelRows);
    }

    read = Build<TCoUnionAll>(ctx, pos)
        .Add(levelRows)
        .Add(prefixLeafRows)
        .Done().Ptr();

    TKqpStreamLookupSettings settings;
    settings.Strategy = EStreamLookupStrategyType::LookupRows;
    settings.VectorTopColumn = indexDesc.KeyColumns.back();
    settings.VectorTopIndex = indexDesc.Name;
    settings.VectorTopTarget = targetVector;
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

} // anonymous namespace

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
bool CanPushFlatMap(const TCoFlatMapBase& flatMap, const TKikimrTableDescription& tableDesc, const TParentsMap& parentsMap, TVector<TString> & extraColumns, bool& residual) {
    auto flatMapLambda = flatMap.Lambda();
    if (!flatMapLambda.Body().Maybe<TCoOptionalIf>() && !flatMapLambda.Body().Maybe<TCoListIf>()) {
        return false;
    }

    const auto & flatMapLambdaArgument = flatMapLambda.Args().Arg(0).Ref();
    auto flatMapLambdaConditional = flatMapLambda.Body().Cast<TCoConditionalValueBase>();

    TSet<TString> lambdaSubset;
    if (!HaveFieldsSubset(flatMapLambdaConditional.Predicate().Ptr(), flatMapLambdaArgument, lambdaSubset, parentsMap)) {
        return false;
    }

    for (auto & lambdaColumn : lambdaSubset) {
        auto columnType = tableDesc.GetColumnType(lambdaColumn);
        if (!columnType)
            return false;
    }

    if (flatMapLambdaConditional.Value().Raw() != flatMapLambda.Args().Arg(0).Raw()) {
        // doesn't support residial values
        if (!residual) {
            return false;
        }

        if (flatMapLambda.Body().Maybe<TCoListIf>()) {
            return false;
        }

    } else {
        residual = false;
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

} // anonymous namespace

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

    static bool ColumnIsIndexed(const TExprNode::TPtr& column, const THashSet<TString>& indexedColumns) {
        if (indexedColumns.empty()) {
            return true;
        }

        TExprNode::TPtr unwrapped = column;
        if (auto flatMap = TExprBase(unwrapped).Maybe<TCoFlatMap>()) {
            auto body = flatMap.Cast().Lambda().Body();
            if (body.Maybe<TCoJust>() && body.Cast<TCoJust>().Ptr()->Head().Content() == "ToString") {
                unwrapped = flatMap.Cast().Input().Ptr();
            }
        }
        auto member = TExprBase(unwrapped).Maybe<TCoMember>();
        if (!member) {
            return false;
        }
        return indexedColumns.contains(TString(member.Cast().Name().Value()));
    }

    static TFulltextQuery Match(TExprNode::TPtr node, TExprContext& ctx,
        const THashSet<TString>& indexedColumns = {})
    {
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

            if (!ColumnIsIndexed(posArgs->Child(0), indexedColumns)){
                return {};
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

            if (!ColumnIsIndexed(column, indexedColumns)) {
                return {};
            }
            return TFulltextQuery{.Node=node, .Column=column, .Query=query};
        } else if (node->Content() == "StartsWith") {
            if (!ColumnIsIndexed(node->HeadPtr(), indexedColumns)) {
                return {};
            }
            auto query = TFulltextQuery{.Node=node, .Column=node->HeadPtr(), .Query=node->TailPtr()};
            query.EndsWithAny = true;
            return query;
        } else if (node->Content() == "EndsWith") {
            if (!ColumnIsIndexed(node->HeadPtr(), indexedColumns)) {
                return {};
            }
            auto query = TFulltextQuery{.Node=node, .Column=node->HeadPtr(), .Query=node->TailPtr()};
            query.StartsWithAny = true;
            return query;
        } else if (node->Content() == "StringContains") {
            if (!ColumnIsIndexed(node->HeadPtr(), indexedColumns)) {
                return {};
            }
            auto query = TFulltextQuery{.Node=node, .Column=node->HeadPtr(), .Query=node->TailPtr()};
            query.StartsWithAny = true;
            query.EndsWithAny = true;
            return query;
        } else if (node->Content() == "==") {
            if (!ColumnIsIndexed(node->HeadPtr(), indexedColumns)) {
                return {};
            }
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

TFullTextApplyParseResult FindMatchingApply(const TExprBase& node, TExprContext& ctx, std::string_view indexName, bool isNgram,
    const THashSet<TString>& indexedColumns = {})
{
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

        if (auto match = TFulltextQuery::Match(expr, ctx, indexedColumns) ; match.IsValid()) {
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
        if (auto match = TFulltextQuery::Match(expr, ctx, indexedColumns) ; match.IsValid()) {
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
    auto maybeFlatMap = topSort.Input().Maybe<TCoFlatMapBase>();
    TExprNode::TPtr structNode;
    THashMap<TString, TString> renameMap;
    if (maybeFlatMap) {
        if (!maybeFlatMap.Cast().Input().Maybe<TKqlReadTableFullTextIndex>()) {
            return node;
        }

        if (!IsRenameFlatMapWithMapping(maybeFlatMap.Cast(), structNode, renameMap)) {
            return node;
        }
    }

    auto read = maybeFlatMap ? maybeFlatMap.Cast().Input().Maybe<TKqlReadTableFullTextIndex>() : topSort.Input().Maybe<TKqlReadTableFullTextIndex>();
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
        auto it = renameMap.find(sortingKeys.front());
        if (it == renameMap.end() || it->second != NTableIndex::NFulltext::FullTextRelevanceColumn) {
            return node;
        }
    }

    settings.SetItemsLimit(topSort.Count().Ptr());

    auto input = ctx.ChangeChild(
        read.Cast().Ref(), TKqlReadTableFullTextIndex::idx_Settings, settings.BuildNode(ctx, node.Pos()).Ptr());

    if (maybeFlatMap) {
        input = ctx.ChangeChild(topSort.Input().Ref(), TCoFlatMap::idx_Input, std::move(input));
    }

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
    THashSet<TString> indexedColumns;
    for(const auto& analyzer : fulltextMetadataInfo.GetSettings().columns()) {
        if (analyzer.analyzers().use_filter_ngram() || analyzer.analyzers().use_filter_edge_ngram()) {
            isNgram = true;
        }
        indexedColumns.insert(analyzer.column());
    }

    auto result = FindMatchingApply(flatMap.Lambda().Body(), ctx, read.Index().Value(), isNgram, indexedColumns);
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

TMaybeNode<TExprBase> KqpSelectJsonIndex(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!kqpCtx.Config->FeatureFlags.GetEnableJsonIndexAutoSelect()) {
        return node;
    }

    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }

    auto flatMap = node.Cast<TCoFlatMap>();
    if (!flatMap.Input().Maybe<TKqlReadTableRanges>()) {
        return node;
    }

    auto read = flatMap.Input().Cast<TKqlReadTableRanges>();
    auto readSettings = TKqpReadTableSettings::Parse(read.Settings());
    if (readSettings.ForcePrimary) {
        return node;
    }

    const auto& mainTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());
    if (mainTableDesc.Metadata->Kind == EKikimrTableKind::Olap) {
        return node;
    }

    if (!read.Ranges().Maybe<TCoVoid>()) {
        return node;
    }

    THashSet<TString> jsonIndexedColumns;
    for (const auto& indexInfo : mainTableDesc.Metadata->Indexes) {
        if (indexInfo.Type != TIndexDescription::EType::GlobalJson) {
            continue;
        }

        if (indexInfo.State != TIndexDescription::EIndexState::Ready) {
            continue;
        }

        YQL_ENSURE(indexInfo.KeyColumns.size() == 1, "Expected single key column in JSON index");
        jsonIndexedColumns.insert(indexInfo.KeyColumns.at(0));
    }

    auto expectedSettings = CollectJsonIndexPredicate(flatMap.Lambda().Body(), node, ctx, jsonIndexedColumns);
    if (!expectedSettings.has_value()) {
        return node;
    }

    const TString& columnName = expectedSettings->ColumnName;

    std::optional<TString> selectedIndex;
    for (const auto& indexInfo : mainTableDesc.Metadata->Indexes) {
        if (indexInfo.Type != TIndexDescription::EType::GlobalJson) {
            continue;
        }

        if (indexInfo.State != TIndexDescription::EIndexState::Ready) {
            continue;
        }

        YQL_ENSURE(indexInfo.KeyColumns.size() == 1, "Expected single key column in JSON index");
        const auto& keyCol = indexInfo.KeyColumns.at(0);

        if (keyCol == columnName) {
            selectedIndex = indexInfo.Name;
            break;
        }
    }

    if (!selectedIndex.has_value()) {
        return node;
    }

    const auto& jsonIndexSettings = expectedSettings.value();
    auto searchColumns = Build<TCoAtomList>(ctx, node.Pos())
        .Add(Build<TCoAtom>(ctx, node.Pos()).Value(jsonIndexSettings.ColumnName).Done())
        .Done();

    auto newInput = Build<TKqlReadTableFullTextIndex>(ctx, node.Pos())
        .Table(read.Table())
        .Index(Build<TCoAtom>(ctx, node.Pos()).Value(selectedIndex.value()).Done())
        .Columns(read.Columns())
        .Query<TExprList>().Build()
        .QueryColumns(searchColumns.Ptr())
        .Settings(jsonIndexSettings.Settings.BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TCoFlatMap>(ctx, node.Pos())
        .Input(newInput)
        .Lambda(flatMap.Lambda())
        .Done();
}

TMaybeNode<TExprBase> KqpRewriteFlatMapOverJsonRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }

    auto flatMap = node.Maybe<TCoFlatMap>().Cast();
    auto read = TReadMatch::MatchJsonRead(flatMap.Input(), kqpCtx);
    if (!read) {
        return node;
    }

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
    YQL_ENSURE(tableDesc.Metadata);

    auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(read.Index().Value());
    if (indexDesc->Type != TIndexDescription::EType::GlobalJson) {
        return {};
    }

    if (indexDesc->State != TIndexDescription::EIndexState::Ready) {
        return {};
    }

    THashSet<TString> jsonIndexedColumns;
    YQL_ENSURE(indexDesc->KeyColumns.size() == 1, "Expected single key column in JSON index");
    jsonIndexedColumns.insert(indexDesc->KeyColumns.at(0));

    auto expectedSettings = CollectJsonIndexPredicate(flatMap.Lambda().Body(), node, ctx, jsonIndexedColumns);
    if (!expectedSettings.has_value()) {
        ctx.AddError(std::move(expectedSettings.error()));
        return {};
    }

    const auto& jsonIndexSettings = expectedSettings.value();
    auto searchColumns = Build<TCoAtomList>(ctx, node.Pos())
        .Add(Build<TCoAtom>(ctx, node.Pos()).Value(jsonIndexSettings.ColumnName).Done())
        .Done();

    auto newInput = Build<TKqlReadTableFullTextIndex>(ctx, node.Pos())
        .Table(read.Table())
        .Index(read.Index())
        .Columns(read.Columns())
        .Query<TExprList>().Build()
        .QueryColumns(searchColumns.Ptr())
        .Settings(jsonIndexSettings.Settings.BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TCoFlatMap>(ctx, node.Pos())
        .Input(newInput)
        .Lambda(flatMap.Lambda())
        .Done();
}

// Rewrites a hybrid search query:
//
//   SELECT ... FROM t
//   ORDER BY HybridRank(FullTextScore(c1, $q), Knn::CosineDistance(c2, $v)) [DESC]
//   LIMIT k
//
// into two independent index sub-queries (a fulltext relevance scan and a vector kmeans-tree scan),
// each bounded to an internal limit, whose results are fused with Reciprocal Rank Fusion (RRF).
//
// Unlike the standalone fulltext/vector rewrites this query names no index via VIEW (it needs two),
// so the rule resolves the indexes from the table metadata by matching the scored columns: the
// FullTextScore column selects the GlobalFulltextRelevance index, the Knn column selects the
// GlobalSyncVectorKMeansTree index. (An explicit ("ft_idx","vec_idx") AS Indexes override is a
// follow-up.) On any misuse it raises a precise error; queries it cannot rewrite fall through to the
// peephole HybridRank stub, which fails with a clear message rather than returning wrong results.
TMaybeNode<TExprBase> KqpRewriteHybridRankTopSort(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (!node.Maybe<TCoTopBase>()) {
        return node;
    }
    auto top = node.Cast<TCoTopBase>();

    // The sort key of a hybrid query is HybridRank(...). Find that marker callable in the key selector.
    TExprNode::TPtr hybridRank;
    VisitExpr(top.KeySelectorLambda().Body().Ptr(), [&](const TExprNode::TPtr& n) {
        if (hybridRank) {
            return false;
        }
        if (n->IsCallable("HybridRank")) {
            hybridRank = n;
            return false;
        }
        return true;
    });
    if (!hybridRank) {
        return node;
    }

    auto addError = [&](const TString& message) -> TMaybeNode<TExprBase> {
        TIssue issue{ctx.GetPosition(hybridRank->Pos()), TStringBuilder() << "HybridRank: " << message};
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_BAD_REQUEST, issue);
        ctx.AddError(issue);
        return {};
    };

    if (top.KeySelectorLambda().Body().Ptr() != hybridRank) {
        return addError("must be the entire ORDER BY key; it cannot be negated or combined with other expressions");
    }

    if (hybridRank->ChildrenSize() < 2) {
        return addError("expects at least 2 arguments: a fulltext score and a vector distance");
    }

    auto findFulltextScore = [](const TExprNode::TPtr& root) -> TExprNode::TPtr {
        TExprNode::TPtr found;
        VisitExpr(root, [&](const TExprNode::TPtr& n) {
            if (found) {
                return false;
            }
            if (n->IsCallable("FulltextScore")) {
                found = n;
                return false;
            }
            return true;
        });
        return found;
    };
    // Match the Knn distance/similarity functions, not helpers like Knn::ToBinaryString* which may also
    // appear in the vector argument (e.g. when the search target is packed inline). Distances rank
    // smaller-is-better, similarities larger-is-better; the branch is sorted in the matching direction and
    // linear fusion normalizes accordingly (see vecIsSimilarity below).
    static const THashSet<TStringBuf> knnDistanceMethods = {
        "Knn.CosineDistance", "Knn.ManhattanDistance", "Knn.EuclideanDistance",
    };
    static const THashSet<TStringBuf> knnSimilarityMethods = {
        "Knn.CosineSimilarity", "Knn.InnerProductSimilarity",
    };
    auto findKnnApply = [](const TExprNode::TPtr& root) -> TMaybeNode<TCoApply> {
        TMaybeNode<TCoApply> found;
        VisitExpr(root, [&](const TExprNode::TPtr& n) {
            if (found.IsValid()) {
                return false;
            }
            if (auto apply = TExprBase(n).Maybe<TCoApply>()) {
                if (auto udf = apply.Cast().Callable().Maybe<TCoUdf>(); udf
                    && (knnDistanceMethods.contains(udf.Cast().MethodName().Value())
                        || knnSimilarityMethods.contains(udf.Cast().MethodName().Value()))) {
                    found = apply.Cast();
                    return false;
                }
            }
            return true;
        });
        return found;
    };

    // Positional args are HybridRank(ftScore, vecDistance). Optional named tuple arguments override
    // index selection and per-branch candidate limits: ("ft_idx","vec_idx") AS Indexes, (n,m) AS Limits.
    // With named args present the node becomes (HybridRank (List ft vec) (AsStruct '(name value) ...)).
    TExprNode::TPtr ftExpr;
    TExprNode::TPtr vecExpr;
    TMaybe<TString> ftIndexOverride;
    TMaybe<TString> vecIndexOverride;
    TMaybe<ui64> ftLimitOverride;
    TMaybe<ui64> vecLimitOverride;
    TMaybe<double> kOverride;
    TString fusionMode = "rrf";  // 'rrf' (Reciprocal Rank Fusion) or 'linear' (min-max normalized scores)
    double ftWeight = 1.0;       // per-direction weights, (w_ft, w_vec) AS Weights; default equal
    double vecWeight = 1.0;
    TMaybe<bool> normalizeOverride;  // linear-only; default true (min-max normalize before fusing)
    const bool hasNamedArgs = hybridRank->Head().GetTypeAnn()
        && hybridRank->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple;
    if (hasNamedArgs) {
        const auto positional = hybridRank->ChildPtr(0);
        if (positional->ChildrenSize() != 2) {
            return addError("expects exactly 2 positional arguments (a fulltext score and a vector distance)");
        }
        ftExpr = positional->ChildPtr(0);
        vecExpr = positional->ChildPtr(1);

        const auto getStringElem = [](const TExprNode::TPtr& n) -> TMaybe<TString> {
            if (n->IsCallable("String") && n->ChildrenSize() >= 1 && n->Head().IsAtom()) {
                return TString(n->Head().Content());
            }
            return {};
        };
        const auto getIntElem = [](const TExprNode::TPtr& n) -> TMaybe<ui64> {
            ui64 v = 0;
            if (n->ChildrenSize() >= 1 && n->Head().IsAtom() && TryFromString<ui64>(n->Head().Content(), v)) {
                return v;
            }
            return {};
        };
        const auto getDoubleElem = [](const TExprNode::TPtr& n) -> TMaybe<double> {
            double v = 0;
            if (n->ChildrenSize() >= 1 && n->Head().IsAtom() && TryFromString<double>(n->Head().Content(), v)) {
                return v;  // accepts both integer (2) and double (0.2) numeric literals
            }
            return {};
        };

        for (const auto& member : hybridRank->Child(1)->Children()) {
            if (member->ChildrenSize() != 2 || !member->Head().IsAtom()) {
                continue;
            }
            const auto name = member->Head().Content();
            const auto value = member->ChildPtr(1);
            if (name == "Indexes") {
                if (value->ChildrenSize() != 2) {
                    return addError("Indexes must be a tuple of two index names: (\"ft_idx\", \"vec_idx\")");
                }
                ftIndexOverride = getStringElem(value->ChildPtr(0));
                vecIndexOverride = getStringElem(value->ChildPtr(1));
                if (!ftIndexOverride || !vecIndexOverride) {
                    return addError("Indexes must be a tuple of two string literals: (\"ft_idx\", \"vec_idx\")");
                }
            } else if (name == "Limits") {
                if (value->ChildrenSize() != 2) {
                    return addError("Limits must be a tuple of two integers: (ft_limit, vec_limit)");
                }
                ftLimitOverride = getIntElem(value->ChildPtr(0));
                vecLimitOverride = getIntElem(value->ChildPtr(1));
                if (!ftLimitOverride || !vecLimitOverride) {
                    return addError("Limits must be a tuple of two positive integer literals: (ft_limit, vec_limit)");
                }
            } else if (name == "K" || name == "k") {
                double kv = 0;
                if (value->ChildrenSize() < 1 || !value->Head().IsAtom() || !TryFromString<double>(value->Head().Content(), kv)) {
                    return addError("K must be a numeric literal (the RRF constant), e.g. 60.0 AS K");
                }
                kOverride = kv;
            } else if (name == "Mode") {
                if (!value->IsCallable("String") || value->ChildrenSize() < 1 || !value->Head().IsAtom()) {
                    return addError("Mode must be a string literal: \"rrf\" or \"linear\"");
                }
                TString modeStr{value->Head().Content()};
                modeStr.to_lower();  // accept "RRF"/"Linear" too
                fusionMode = modeStr;
                if (fusionMode != "rrf" && fusionMode != "linear") {
                    return addError(TStringBuilder() << "unknown Mode '" << value->Head().Content() << "'; expected \"rrf\" or \"linear\"");
                }
            } else if (name == "Weights") {
                if (value->ChildrenSize() != 2) {
                    return addError("Weights must be a tuple of two numbers: (ft_weight, vec_weight)");
                }
                const auto w0 = getDoubleElem(value->ChildPtr(0));
                const auto w1 = getDoubleElem(value->ChildPtr(1));
                if (!w0 || !w1) {
                    return addError("Weights must be a tuple of two numeric literals: (ft_weight, vec_weight)");
                }
                ftWeight = *w0;
                vecWeight = *w1;
            } else if (name == "Normalize") {
                if (!value->IsCallable("Bool") || value->ChildrenSize() < 1 || !value->Head().IsAtom()) {
                    return addError("Normalize must be a boolean literal: true or false");
                }
                normalizeOverride = (value->Head().Content() == "true");
            } else {
                return addError(TStringBuilder() << "unknown named argument '" << name << "'; expected Indexes, Limits, K, Mode, Weights or Normalize");
            }
        }
    } else {
        ftExpr = hybridRank->ChildPtr(0);
        vecExpr = hybridRank->ChildPtr(1);
    }

    auto ftScore = findFulltextScore(ftExpr);
    auto knnApply = findKnnApply(vecExpr);
    if (!ftScore || !knnApply.IsValid()) {
        if (findFulltextScore(vecExpr) && findKnnApply(ftExpr).IsValid()) {
            return addError("arguments appear reversed; expected HybridRank(FullTextScore(...), Knn::Distance(...))");
        }
        return addError("expected the first argument to be FullTextScore(column, query) and the second to be a "
                        "Knn distance over a column, e.g. Knn::CosineDistance(embedding, $target)");
    }

    bool vecIsSimilarity = false;
    if (auto knnUdf = knnApply.Cast().Callable().Maybe<TCoUdf>()) {
        vecIsSimilarity = knnSimilarityMethods.contains(knnUdf.Cast().MethodName().Value());
    }

    // Extract the fulltext column from FullTextScore(<column>, <query>, ...).
    auto ftColumnMember = TExprBase(ftScore->ChildPtr(0)).Maybe<TCoMember>();
    if (!ftColumnMember) {
        return addError("the first argument of FullTextScore must reference a table column");
    }
    const TString ftColumn = ftColumnMember.Cast().Name().StringValue();

    // Extract the vector column: the table-column Member referenced anywhere in the vector expression.
    // A nullable column wraps the Knn call in an automap FlatMap, so the Member lives in the wider
    // vector expression (as the FlatMap input), not inside the Knn apply itself.
    TString vecColumn;
    VisitExpr(vecExpr, [&](const TExprNode::TPtr& n) {
        if (!vecColumn.empty()) {
            return false;
        }
        if (auto member = TExprBase(n).Maybe<TCoMember>()) {
            vecColumn = member.Cast().Name().StringValue();
            return false;
        }
        return true;
    });
    if (vecColumn.empty()) {
        return addError("the Knn distance argument must reference a table column, e.g. Knn::CosineDistance(embedding, $target)");
    }

    // The input under the TopSort must be a plain main-table read (optionally wrapped in a projection/filter FlatMap).
    auto input = top.Input();
    auto maybeFlatMap = input.Maybe<TCoFlatMap>();
    TExprBase readBase = maybeFlatMap ? maybeFlatMap.Cast().Input() : input;
    auto maybeRead = readBase.Maybe<TKqlReadTableRanges>();
    if (!maybeRead) {
        // Not a shape we can rewrite yet (e.g. already an index read); leave it for the peephole stub.
        return node;
    }
    auto read = maybeRead.Cast();

    const auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());
    YQL_ENSURE(tableDesc.Metadata);

    auto columnInList = [](const TVector<TString>& cols, const TString& name) {
        for (const auto& c : cols) {
            if (c == name) {
                return true;
            }
        }
        return false;
    };

    // Resolve the fulltext (relevance) and vector indexes: an explicit ("ft_idx","vec_idx") AS Indexes
    // override if given, otherwise auto-detect by matching the scored columns.
    TString ftIndexName;
    TString vecIndexName;
    if (ftIndexOverride) {
        const TIndexDescription* ftIdx = nullptr;
        const TIndexDescription* vecIdx = nullptr;
        for (const auto& idx : tableDesc.Metadata->Indexes) {
            if (idx.Name == *ftIndexOverride) {
                ftIdx = &idx;
            }
            if (idx.Name == *vecIndexOverride) {
                vecIdx = &idx;
            }
        }
        if (!ftIdx) {
            return addError(TStringBuilder() << "fulltext index '" << *ftIndexOverride << "' was not found");
        }
        if (ftIdx->State != TIndexDescription::EIndexState::Ready) {
            return addError(TStringBuilder() << "fulltext index '" << *ftIndexOverride << "' is not ready");
        }
        if (ftIdx->Type != TIndexDescription::EType::GlobalFulltextRelevance) {
            return addError(TStringBuilder() << "index '" << *ftIndexOverride << "' is not a fulltext relevance index");
        }
        if (!columnInList(ftIdx->KeyColumns, ftColumn)) {
            return addError(TStringBuilder() << "fulltext index '" << *ftIndexOverride
                << "' is not built on the FullTextScore column '" << ftColumn << "'");
        }
        if (!vecIdx) {
            return addError(TStringBuilder() << "vector index '" << *vecIndexOverride << "' was not found");
        }
        if (vecIdx->State != TIndexDescription::EIndexState::Ready) {
            return addError(TStringBuilder() << "vector index '" << *vecIndexOverride << "' is not ready");
        }
        if (vecIdx->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
            return addError(TStringBuilder() << "index '" << *vecIndexOverride << "' is not a vector kmeans-tree index");
        }
        if (vecIdx->KeyColumns.empty() || vecIdx->KeyColumns.back() != vecColumn) {
            return addError(TStringBuilder() << "vector index '" << *vecIndexOverride
                << "' is not built on the Knn distance column '" << vecColumn << "'");
        }
        if (vecIdx->KeyColumns.size() != 1) {
            return addError(TStringBuilder() << "vector index '" << *vecIndexOverride
                << "' is a prefixed vector index, which HybridRank does not support yet");
        }
        ftIndexName = *ftIndexOverride;
        vecIndexName = *vecIndexOverride;
    } else {
        ui32 ftMatches = 0;
        ui32 vecMatches = 0;
        for (const auto& idx : tableDesc.Metadata->Indexes) {
            if (idx.State != TIndexDescription::EIndexState::Ready) {
                continue;
            }
            if (idx.Type == TIndexDescription::EType::GlobalFulltextRelevance && columnInList(idx.KeyColumns, ftColumn)) {
                ftIndexName = idx.Name;
                ++ftMatches;
            }
            if (idx.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree
                && idx.KeyColumns.size() == 1 && idx.KeyColumns.back() == vecColumn) {
                vecIndexName = idx.Name;
                ++vecMatches;
            }
        }

        if (ftMatches == 0) {
            return addError(TStringBuilder() << "no ready fulltext relevance index found on column '" << ftColumn << "'");
        }
        if (ftMatches > 1) {
            return addError(TStringBuilder() << "multiple fulltext relevance indexes match column '" << ftColumn
                << "'; name it explicitly via ((\"ft_idx\", \"vec_idx\") AS Indexes)");
        }
        if (vecMatches == 0) {
            return addError(TStringBuilder() << "no ready vector (kmeans-tree) index found on column '" << vecColumn << "'");
        }
        if (vecMatches > 1) {
            return addError(TStringBuilder() << "multiple vector indexes match column '" << vecColumn
                << "'; name it explicitly via ((\"ft_idx\", \"vec_idx\") AS Indexes)");
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Build the two index branches and fuse them with Reciprocal Rank Fusion (RRF).
    // ---------------------------------------------------------------------------------------------
    const auto pos = node.Pos();

    const auto& pkColumns = tableDesc.Metadata->KeyColumnNames;
    if (pkColumns.size() != 1) {
        return addError("hybrid search currently supports only a single-column primary key");
    }
    const TString pkCol = pkColumns[0];

    // Reconstruct the WHERE predicate and projection to apply on top of the fused, looked-up rows.
    // A projecting FlatMap (from a WHERE clause or an explicit projection) provides them; without one
    // the TopSort reads the table directly and its output schema is simply the read's columns.
    TExprNode::TPtr origArg;
    TExprNode::TPtr origPred;   // over origArg; null => no WHERE filter
    TExprNode::TPtr origProj;   // AsStruct over origArg; null => emit the read columns as-is
    if (maybeFlatMap) {
        const auto origLambda = maybeFlatMap.Cast().Lambda();
        origArg = origLambda.Args().Arg(0).Ptr();
        const auto origBody = origLambda.Body();
        if (auto optIf = origBody.Maybe<TCoOptionalIf>()) {
            origPred = optIf.Cast().Predicate().Ptr();
            origProj = optIf.Cast().Value().Ptr();
        } else if (auto just = origBody.Maybe<TCoJust>()) {
            origProj = just.Cast().Input().Ptr();
        } else {
            return node;
        }
        if (!TExprBase(origProj).Maybe<TCoAsStruct>()) {
            return node;
        }
    }

    // The HybridRank sub-expressions reference the TopSort key-selector row argument; we rebind it to
    // each branch's own row when reusing those sub-expressions (which preserves the exact Knn shape,
    // automap wrapper and all, that the vector rewrite expects).
    const auto rowArg = top.KeySelectorLambda().Args().Arg(0).Ptr();
    const auto ftQuery = ftScore->ChildPtr(1);

    const TString relevanceCol{NTableIndex::NFulltext::FullTextRelevanceColumn};
    const TString distCol = "__ydb_hybrid_distance";
    const TString rrfCol = "__ydb_hybrid_rrf";

    auto uint64Node = [&](ui64 v) {
        return ctx.Builder(pos).Callable("Uint64").Atom(0, ToString(v), TNodeFlags::Default).Seal().Build();
    };
    // Per-branch internal candidate limit: an explicit (n, m) AS Limits override, otherwise
    // LIMIT * HybridSearchFactor (pragma, default 10), built as a runtime expression so it tracks a
    // parameterised LIMIT.
    TExprNode::TPtr ftN;
    TExprNode::TPtr vecN;
    if (ftLimitOverride) {
        ftN = uint64Node(*ftLimitOverride);
        vecN = uint64Node(*vecLimitOverride);
    } else {
        // The fulltext ItemsLimit and the vector TopLimit must be literals, so the per-branch candidate
        // count is LIMIT * HybridSearchFactor computed at optimize time. A non-literal (e.g. parameterised)
        // LIMIT cannot be sized safely -- a fixed fallback smaller than the runtime LIMIT would silently
        // drop rows from the final TopSort -- so reject it and ask for an explicit Limits override.
        const ui64 factor = kqpCtx.Config->HybridSearchFactor.Get().GetOrElse(10);
        ui64 limitVal = 0;
        bool literalLimit = false;
        const auto& countNode = top.Count().Ref();
        if (countNode.IsCallable() && countNode.ChildrenSize() == 1 && countNode.Head().IsAtom()) {
            literalLimit = TryFromString<ui64>(countNode.Head().Content(), limitVal);
        }
        if (!literalLimit) {
            return addError("requires a literal LIMIT; for a parameterised LIMIT pass explicit per-branch "
                            "candidate counts via ((n, m) AS Limits)");
        }
        ftN = vecN = uint64Node(limitVal * factor);
    }
    // Rank assigned to a document absent from one branch: a large sentinel, so its contribution from
    // that branch is ~0 (the intended "missing from this result set" behaviour).
    const auto penaltyRank = uint64Node(4000000000ull);
    const double kValue = kOverride.GetOrElse(kqpCtx.Config->HybridSearchK.Get().GetOrElse(60.0));
    const auto kConst = ctx.Builder(pos).Callable("Double").Atom(0, ToString(kValue), TNodeFlags::Default).Seal().Build();

    const auto mainTableMeta = BuildTableMeta(*tableDesc.Metadata, pos, ctx);

    // ---- Fulltext relevance branch: top-N {pk, relevance} ordered by relevance DESC ----
    // Emitted as the canonical FlatMap-over-index-read shape; the existing fulltext rules
    // (RewriteFlatMapOverFullTextMatch + PushLimitOverFullText) lower it.
    // Build the fulltext relevance read directly (as KqpSelectJsonIndex does for JSON) rather than emit a
    // synthetic FlatMap and rely on RewriteFlatMapOverFullTextMatch to fire on it. The read returns
    // {pk, __ydb_full_text_relevance} ranked by relevance, capped at N_internal via ItemsLimit.
    TKqpReadTableFullTextIndexSettings ftSettings;
    ftSettings.SetItemsLimit(ftN);
    const auto ftRead = Build<TKqlReadTableFullTextIndex>(ctx, pos)
        .Table(mainTableMeta)
        .Index().Build(ftIndexName)
        .Columns(BuildKeyColumnsList(pos, ctx, TVector<TString>{pkCol, relevanceCol}))
        .Query<TExprList>().Add(TExprBase(ftQuery)).Build()
        .QueryColumns(BuildKeyColumnsList(pos, ctx, TVector<TString>{ftColumn}))
        .Settings(ftSettings.BuildNode(ctx, pos))
        .Done().Ptr();
    const auto ftList = Build<TDqPrecompute>(ctx, pos).Input(TExprBase(ftRead)).Done().Ptr();

    // ---- Vector branch: top-N {pk, score} ordered best-first (distance ASC, similarity DESC) ----
    // Emitted as a synthetic TopSort over the vector index read; RewriteTopSortOverIndexRead
    // (DoRewriteTopSortOverKMeansTree) lowers it into the kmeans-tree lookup chain. CanUseVectorIndex
    // matches the function + this sort direction against the index metric (e.g. cosine accepts
    // CosineDistance ASC or CosineSimilarity DESC).
    const auto vecRead = Build<TKqlReadTableIndexRanges>(ctx, pos)
        .Table(mainTableMeta)
        .Ranges<TCoVoid>().Build()
        .ExplainPrompt().Build()
        .Columns(BuildKeyColumnsList(pos, ctx, TVector<TString>{pkCol, vecColumn}))
        .Settings().Build()
        .Index().Build(vecIndexName)
        .Done().Ptr();

    const auto vecRowArg = ctx.NewArgument(pos, "vecRow");
    const auto distShared = ctx.ReplaceNode(TExprNode::TPtr(vecExpr), *rowArg, vecRowArg);
    const auto vecFlatMapBody = ctx.Builder(pos)
        .Callable("Just")
            .Callable(0, "AsStruct")
                .List(0)
                    .Atom(0, pkCol)
                    .Callable(1, "Member").Add(0, vecRowArg).Atom(1, pkCol).Seal()
                .Seal()
                .List(1)
                    .Atom(0, distCol)
                    .Add(1, distShared)
                .Seal()
            .Seal()
        .Seal()
        .Build();
    const auto vecBranch = ctx.Builder(pos)
        .Callable("TopSort")
            .Callable(0, "FlatMap")
                .Add(0, vecRead)
                .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {vecRowArg}), TExprNode::TPtr(vecFlatMapBody)))
            .Seal()
            .Add(1, vecN)
            .Callable(2, "Bool").Atom(0, vecIsSimilarity ? "false" : "true", TNodeFlags::Default).Seal()
            .Lambda(3)
                .Param("r")
                .Callable("Member").Arg(0, "r").Atom(1, distCol).Seal()
            .Seal()
        .Seal()
        .Build();
    const auto vecList = Build<TDqPrecompute>(ctx, pos).Input(TExprBase(vecBranch)).Done().Ptr();

    // ---- Fuse: per-candidate hybrid score (RRF over ranks, or min-max linear over scores) ----
    const auto pkArg = ctx.NewArgument(pos, "pk");
    const auto emptyStruct = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{});
    const auto emptyTuple = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{});

    // Distinct candidate primary keys across both branches (independent of the payload dicts).
    auto pkListOf = [&](const TExprNode::TPtr& list) {
        return ctx.Builder(pos)
            .Callable("Map").Add(0, list)
                .Lambda(1).Param("r").Callable("Member").Arg(0, "r").Atom(1, pkCol).Seal().Seal()
            .Seal().Build();
    };
    const auto allKeys = ctx.Builder(pos)
        .Callable("DictKeys")
            .Callable(0, "ToDict")
                .Callable(0, "Extend").Add(0, pkListOf(ftList)).Add(1, pkListOf(vecList)).Seal()
                .Lambda(1).Param("k").Arg("k").Seal()
                .Lambda(2).Param("k").Callable("Void").Seal().Seal()
                .List(3).Atom(0, "One", TNodeFlags::Default).Atom(1, "Hashed", TNodeFlags::Default).Seal()
            .Seal()
        .Seal()
        .Build();

    // Per-direction weights and (linear-only) normalization flag, passed to the fusion UDF as constants.
    const bool normalize = normalizeOverride.GetOrElse(true);
    const auto wFtConst = ctx.Builder(pos).Callable("Double").Atom(0, ToString(ftWeight), TNodeFlags::Default).Seal().Build();
    const auto wVecConst = ctx.Builder(pos).Callable("Double").Atom(0, ToString(vecWeight), TNodeFlags::Default).Seal().Build();
    const auto normConst = ctx.Builder(pos).Callable("Bool").Atom(0, normalize ? "true" : "false", TNodeFlags::Default).Seal().Build();
    const auto vecSimConst = ctx.Builder(pos).Callable("Bool").Atom(0, vecIsSimilarity ? "true" : "false", TNodeFlags::Default).Seal().Build();

    TExprNode::TPtr scoreApply;  // the hybrid score for the candidate `pkArg`
    if (fusionMode == "linear") {
        // A score field of an item node, cast to a (non-optional) Double.
        auto castDouble = [&](const TExprNode::TPtr& itArg, const TString& col) {
            return ctx.Builder(pos)
                .Callable("Coalesce")
                    .Callable(0, "SafeCast")
                        .Callable(0, "Member").Add(0, itArg).Atom(1, col).Seal()
                        .Callable(1, "DataType").Atom(0, "Double", TNodeFlags::Default).Seal()
                    .Seal()
                    .Callable(1, "Double").Atom(0, "0", TNodeFlags::Default).Seal()
                .Seal().Build();
        };
        auto lambda1 = [&](const TExprNode::TPtr& argNode, const TExprNode::TPtr& body) {
            return ctx.NewLambda(pos, ctx.NewArguments(pos, {argNode}), TExprNode::TPtr(body));
        };
        auto scoreDictOf = [&](const TExprNode::TPtr& list, const TString& col) {
            const auto keyArg = ctx.NewArgument(pos, "it");
            const auto payArg = ctx.NewArgument(pos, "it");
            return ctx.Builder(pos).Callable("ToDict")
                .Add(0, list)
                .Add(1, lambda1(keyArg, ctx.Builder(pos).Callable("Member").Add(0, keyArg).Atom(1, pkCol).Seal().Build()))
                .Add(2, lambda1(payArg, castDouble(payArg, col)))
                .List(3).Atom(0, "One", TNodeFlags::Default).Atom(1, "Hashed", TNodeFlags::Default).Seal()
                .Seal().Build();
        };
        // min/max of a branch's scores across the candidate set (a scalar; loop-invariant).
        auto minMaxOf = [&](const TExprNode::TPtr& list, const TString& col, const char* op) {
            const auto mArg = ctx.NewArgument(pos, "it");
            const auto mapped = ctx.Builder(pos).Callable("Map").Add(0, list)
                .Add(1, lambda1(mArg, castDouble(mArg, col)))
                .Seal().Build();
            return ctx.Builder(pos)
                .Callable("Coalesce")
                    .Callable(0, op).Add(0, mapped).Seal()
                    .Callable(1, "Double").Atom(0, "0", TNodeFlags::Default).Seal()
                .Seal().Build();
        };
        const auto ftScoreDict = scoreDictOf(ftList, relevanceCol);
        const auto vecScoreDict = scoreDictOf(vecList, distCol);
        const auto ftMin = minMaxOf(ftList, relevanceCol, "ListMin");
        const auto ftMax = minMaxOf(ftList, relevanceCol, "ListMax");
        const auto vecMin = minMaxOf(vecList, distCol, "ListMin");
        const auto vecMax = minMaxOf(vecList, distCol, "ListMax");
        // Missing from a branch -> the value that maps to a 0 contribution: ftMin for fulltext; for the
        // vector branch the "worst" end, which is vecMax for a distance and vecMin for a similarity.
        auto scoreOrDefault = [&](const TExprNode::TPtr& dict, const TExprNode::TPtr& dflt) {
            return ctx.Builder(pos).Callable("Coalesce")
                .Callable(0, "Lookup").Add(0, dict).Add(1, pkArg).Seal()
                .Add(1, dflt)
            .Seal().Build();
        };
        // Arg types: 8 Doubles (ftScore, ftMin, ftMax, vecScore, vecMin, vecMax, wFt, wVec) + 2 Bools
        // (normalize, vecSimilarity).
        TTypeAnnotationNode::TListType linArgs(8, ctx.MakeType<TDataExprType>(EDataSlot::Double));
        linArgs.push_back(ctx.MakeType<TDataExprType>(EDataSlot::Bool));
        linArgs.push_back(ctx.MakeType<TDataExprType>(EDataSlot::Bool));
        const auto linUdfType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TTupleExprType>(linArgs),
            emptyStruct, emptyTuple,
        });
        const auto linUdf = Build<TCoUdf>(ctx, pos)
            .MethodName().Build("HybridSearch.LinearFuse")
            .RunConfigValue<TCoVoid>().Build()
            .UserType(ExpandType(pos, *linUdfType, ctx))
            .Done().Ptr();
        scoreApply = ctx.Builder(pos)
            .Callable("Apply")
                .Add(0, linUdf)
                .Add(1, scoreOrDefault(ftScoreDict, ftMin))
                .Add(2, ftMin)
                .Add(3, ftMax)
                .Add(4, scoreOrDefault(vecScoreDict, vecIsSimilarity ? vecMin : vecMax))
                .Add(5, vecMin)
                .Add(6, vecMax)
                .Add(7, wFtConst)
                .Add(8, wVecConst)
                .Add(9, normConst)
                .Add(10, vecSimConst)
            .Seal()
            .Build();
    } else {
        // RRF over 1-based ranks within each (already-sorted) branch.
        auto buildRankDict = [&](const TExprNode::TPtr& list) {
            return ctx.Builder(pos).Callable("ToDict")
                .Callable(0, "Enumerate").Add(0, list)
                    .Callable(1, "Uint64").Atom(0, "1", TNodeFlags::Default).Seal()
                    .Callable(2, "Uint64").Atom(0, "1", TNodeFlags::Default).Seal()
                .Seal()
                .Lambda(1).Param("p").Callable("Member").Callable(0, "Nth").Arg(0, "p").Atom(1, 1U).Seal().Atom(1, pkCol).Seal().Seal()
                .Lambda(2).Param("p").Callable("Nth").Arg(0, "p").Atom(1, 0U).Seal().Seal()
                .List(3).Atom(0, "One", TNodeFlags::Default).Atom(1, "Hashed", TNodeFlags::Default).Seal()
                .Seal().Build();
        };
        const auto ftDict = buildRankDict(ftList);
        const auto vecDict = buildRankDict(vecList);
        auto rankOrPenalty = [&](const TExprNode::TPtr& dict) {
            return ctx.Builder(pos).Callable("Coalesce")
                .Callable(0, "Lookup").Add(0, dict).Add(1, pkArg).Seal()
                .Add(1, penaltyRank)
            .Seal().Build();
        };
        const auto rrfUdfType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
                ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
                ctx.MakeType<TDataExprType>(EDataSlot::Double),
                ctx.MakeType<TDataExprType>(EDataSlot::Double),
                ctx.MakeType<TDataExprType>(EDataSlot::Double),
            }),
            emptyStruct, emptyTuple,
        });
        const auto rrfUdf = Build<TCoUdf>(ctx, pos)
            .MethodName().Build("HybridSearch.RRF")
            .RunConfigValue<TCoVoid>().Build()
            .UserType(ExpandType(pos, *rrfUdfType, ctx))
            .Done().Ptr();
        scoreApply = ctx.Builder(pos)
            .Callable("Apply").Add(0, rrfUdf)
                .Add(1, rankOrPenalty(ftDict)).Add(2, rankOrPenalty(vecDict)).Add(3, kConst)
                .Add(4, wFtConst).Add(5, wVecConst)
            .Seal().Build();
    }

    // rrfRows: { pk, __ydb_hybrid_rrf } for each candidate (the column holds whichever fused score).
    const auto rrfRowBody = ctx.Builder(pos)
        .Callable("AsStruct")
            .List(0).Atom(0, pkCol).Add(1, pkArg).Seal()
            .List(1).Atom(0, rrfCol).Add(1, scoreApply).Seal()
        .Seal()
        .Build();
    const auto rrfRows = ctx.Builder(pos)
        .Callable("Map")
            .Add(0, allKeys)
            .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {pkArg}), TExprNode::TPtr(rrfRowBody)))
        .Seal()
        .Build();

    // ---- Look up the main table by the fused candidate keys ----
    const auto lookupKeys = ctx.Builder(pos)
        .Callable("Map")
            .Add(0, rrfRows)
            .Lambda(1)
                .Param("r")
                .Callable("AsStruct")
                    .List(0).Atom(0, pkCol).Callable(1, "Member").Arg(0, "r").Atom(1, pkCol).Seal().Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    const auto mainColumns = MergeColumns(read.Columns(), TVector<TString>{pkCol}, ctx);

    TKqpStreamLookupSettings lookupSettings;
    lookupSettings.Strategy = EStreamLookupStrategyType::LookupRows;
    const auto mainRows = Build<TKqlStreamLookupTable>(ctx, pos)
        .Table(mainTableMeta)
        .LookupKeys(TExprBase(lookupKeys))
        .Columns(mainColumns)
        .Settings(lookupSettings.BuildNode(ctx, pos))
        .Done().Ptr();

    // ---- Re-apply WHERE, attach RRF, re-rank by RRF DESC, take LIMIT, project ----
    const auto rrfDict = ctx.Builder(pos)
        .Callable("ToDict")
            .Add(0, rrfRows)
            .Lambda(1).Param("r").Callable("Member").Arg(0, "r").Atom(1, pkCol).Seal().Seal()
            .Lambda(2).Param("r").Callable("Member").Arg(0, "r").Atom(1, rrfCol).Seal().Seal()
            .List(3).Atom(0, "One", TNodeFlags::Default).Atom(1, "Hashed", TNodeFlags::Default).Seal()
        .Seal()
        .Build();

    const auto mrowArg = ctx.NewArgument(pos, "mrow");
    const auto predOverMrow = origPred
        ? ctx.ReplaceNode(TExprNode::TPtr(origPred), *origArg, mrowArg)
        : ctx.Builder(pos).Callable("Bool").Atom(0, "true", TNodeFlags::Default).Seal().Build();
    const auto rrfForMrow = ctx.Builder(pos)
        .Callable("Coalesce")
            .Callable(0, "Lookup")
                .Add(0, rrfDict)
                .Callable(1, "Member").Add(0, mrowArg).Atom(1, pkCol).Seal()
            .Seal()
            .Callable(1, "Double").Atom(0, "0", TNodeFlags::Default).Seal()
        .Seal()
        .Build();
    const auto augBody = ctx.Builder(pos)
        .Callable("OptionalIf")
            .Add(0, predOverMrow)
            .Callable(1, "AddMember")
                .Add(0, mrowArg)
                .Atom(1, rrfCol)
                .Add(2, rrfForMrow)
            .Seal()
        .Seal()
        .Build();
    const auto augmented = ctx.Builder(pos)
        .Callable("FlatMap")
            .Add(0, mainRows)
            .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {mrowArg}), TExprNode::TPtr(augBody)))
        .Seal()
        .Build();

    const auto frArg = ctx.NewArgument(pos, "fr");
    TExprNode::TPtr projOverFr;
    if (origProj) {
        projOverFr = ctx.ReplaceNode(TExprNode::TPtr(origProj), *origArg, frArg);
    } else {
        // No explicit projection: emit the read's columns (dropping the synthetic RRF field).
        TExprNode::TListType members;
        for (const auto& col : read.Columns()) {
            members.push_back(ctx.Builder(pos)
                .List()
                    .Atom(0, col.Value())
                    .Callable(1, "Member").Add(0, frArg).Atom(1, col.Value()).Seal()
                .Seal()
                .Build());
        }
        projOverFr = ctx.NewCallable(pos, "AsStruct", std::move(members));
    }
    const auto result = ctx.Builder(pos)
        .Callable("Map")
            .Callable(0, "TopSort")
                .Add(0, augmented)
                .Add(1, top.Count().Ptr())
                .Callable(2, "Bool").Atom(0, "false", TNodeFlags::Default).Seal()
                .Lambda(3).Param("r").Callable("Member").Arg(0, "r").Atom(1, rrfCol).Seal().Seal()
            .Seal()
            .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {frArg}), TExprNode::TPtr(projOverFr)))
        .Seal()
        .Build();

    return TExprBase(result);
}

// The index and main table have same number of rows, so we can push a copy of TCoTopSort or TCoTake
// through TKqlLookupTable.
// The simplest way is to match TopSort or Take over TKqlReadTableIndex.
// Additionally if there is TopSort or Take over filter, and filter depends only on columns available in index,
// we also push copy of filter through TKqlLookupTable.
TExprBase KqpRewriteTopSortOverIndexRead(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
                                        const TKqpOptimizeContext& kqpCtx, const TParentsMap& parentsMap) {
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
                                                          ctx, typesCtx, kqpCtx, tableDesc, *indexDesc, *implTable);
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

    bool hasResidual = false;
    if (maybeFlatMap && !CanPushFlatMap(maybeFlatMap.Cast(), implTableDesc, parentsMap, extraColumns, hasResidual))
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

TExprBase KqpRewriteFlatMapOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TParentsMap& parentsMap)
{
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }

    auto flatMap = node.Cast<TCoFlatMap>();

    auto read = TReadMatch::MatchIndexedRead(flatMap.Input(), kqpCtx);
    if (!read) {
        return node;
    }

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
    const auto indexName = read.Index().Value();
    auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
    if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
        // TODO(mbkkt) some warning?
        return node;
    }
    const auto& implTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable->Name);

    TVector<TString> extraColumns;
    bool hasResidual = true;
    if (!CanPushFlatMap(flatMap, implTableDesc, parentsMap, extraColumns, hasResidual))
        return node;

    auto filter = [&](const TExprBase& in) mutable {
        if (!hasResidual) {
            return Build<TCoFlatMap>(ctx, node.Pos())
                .Input(in)
                .Lambda(ctx.DeepCopyLambda(flatMap.Lambda().Ref()))
                .Done();
        } else {
            TNodeOnNodeOwnedMap replaces;
            YQL_ENSURE(flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>());
            replaces.emplace(flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>().Value().Raw(), flatMap.Lambda().Args().Arg(0).Ptr());

            auto newLambdaBody = TCoLambda{ctx.NewLambda(
                flatMap.Lambda().Pos(),
                std::move(flatMap.Lambda().Args().Ptr()),
                ctx.ReplaceNodes(TExprNode::TListType{flatMap.Lambda().Body().Ptr()}, replaces))};

            return Build<TCoFlatMap>(ctx, node.Pos())
                .Input(in)
                .Lambda(NewLambdaFrom(ctx, flatMap.Lambda().Pos(), replaces, flatMap.Lambda().Args().Ref(), newLambdaBody.Body()))
                .Done();
        }
    };

    if (!hasResidual) {
        return DoRewriteIndexRead(read, ctx, tableDesc, implTable, extraColumns, filter);
    } else {
        auto newRead = DoRewriteIndexRead(read, ctx, tableDesc, implTable, extraColumns, filter);
        TNodeOnNodeOwnedMap replaces;
        YQL_ENSURE(flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>());
        replaces.emplace(flatMap.Lambda().Body().Raw(), flatMap.Lambda().Body().Cast<TCoConditionalValueBase>().Value().Ptr());

        auto newLambdaBody = TCoLambda{ctx.NewLambda(
            flatMap.Lambda().Pos(),
            std::move(flatMap.Lambda().Args().Ptr()),
            ctx.ReplaceNodes(TExprNode::TListType{flatMap.Lambda().Body().Ptr()}, replaces))};

        return Build<TCoMap>(ctx, node.Pos())
            .Input(newRead)
            .Lambda(NewLambdaFrom(ctx, flatMap.Lambda().Pos(), replaces, flatMap.Lambda().Args().Ref(), newLambdaBody.Body()))
            .Done();
    }
}

TExprBase KqpRewriteTakeOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
                                    const TParentsMap& parentsMap)
{
    if (!node.Maybe<TCoTake>()) {
        return node;
    }

    auto take = node.Maybe<TCoTake>().Cast();

    auto maybeSkip = take.Input().Maybe<TCoSkip>();

    auto maybeFlatMap = maybeSkip ? take.Input().Cast<TCoSkip>().Input().Maybe<TCoFlatMap>() : take.Input().Maybe<TCoFlatMap>();
    TExprBase input = maybeFlatMap ? maybeFlatMap.Cast().Input() : (maybeSkip ? maybeSkip.Cast().Input() : take.Input());

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
    bool hasResidual = true && maybeFlatMap;
    if (maybeFlatMap && !CanPushFlatMap(maybeFlatMap.Cast(), implTableDesc, parentsMap, extraColumns, hasResidual))
        return node;

    auto filter = [&](const TExprBase& in) mutable {
        auto takeChild = in;

        if (maybeFlatMap)
        {
            auto flatMap = maybeFlatMap.Cast();
            if (!hasResidual) {
                takeChild = Build<TCoFlatMap>(ctx, node.Pos())
                    .Input(in)
                    .Lambda(ctx.DeepCopyLambda(flatMap.Lambda().Ref()))
                    .Done();
            } else {
                TNodeOnNodeOwnedMap replaces;
                YQL_ENSURE(flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>());
                replaces.emplace(flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>().Value().Raw(), flatMap.Lambda().Args().Arg(0).Ptr());

                auto newLambdaBody = TCoLambda{ctx.NewLambda(
                    flatMap.Lambda().Pos(),
                    std::move(flatMap.Lambda().Args().Ptr()),
                    ctx.ReplaceNodes(TExprNode::TListType{flatMap.Lambda().Body().Ptr()}, replaces))};

                takeChild = Build<TCoFlatMap>(ctx, node.Pos())
                    .Input(in)
                    .Lambda(NewLambdaFrom(ctx, flatMap.Lambda().Pos(), replaces, flatMap.Lambda().Args().Ref(), newLambdaBody.Body()))
                    .Done();
            }
        }

        if (maybeSkip) {
            takeChild = TExprBase(ctx.ChangeChild(*maybeSkip.Cast().Ptr(), TCoSkip::idx_Input, takeChild.Ptr()));
        }
        // Change input for TCoTake. New input is result of TKqlReadTable.
        auto result = TExprBase(ctx.ChangeChild(*node.Ptr(), TCoTake::idx_Input, takeChild.Ptr()));
        return result;
    };

    if (!hasResidual) {
        return DoRewriteIndexRead(readTableIndex, ctx, tableDesc, implTable, extraColumns, filter);
    } else {
        auto flatMap = maybeFlatMap.Cast();
        auto newRead = DoRewriteIndexRead(readTableIndex, ctx, tableDesc, implTable, extraColumns, filter);
        TNodeOnNodeOwnedMap replaces;
        YQL_ENSURE(flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>());
        replaces.emplace(flatMap.Lambda().Body().Raw(), flatMap.Lambda().Body().Cast<TCoConditionalValueBase>().Value().Ptr());

        auto newLambdaBody = TCoLambda{ctx.NewLambda(
            flatMap.Lambda().Pos(),
            std::move(flatMap.Lambda().Args().Ptr()),
            ctx.ReplaceNodes(TExprNode::TListType{flatMap.Lambda().Body().Ptr()}, replaces))};

        return Build<TCoMap>(ctx, node.Pos())
            .Input(newRead)
            .Lambda(NewLambdaFrom(ctx, flatMap.Lambda().Pos(), replaces, flatMap.Lambda().Args().Ref(), newLambdaBody.Body()))
            .Done();

    }
}

} // namespace NKikimr::NKqp::NOpt
