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

// Parse-once accessor over a HybridRank node's arguments.
//
// HybridRank takes N >= 2 positional scoring expressions (each a FullTextScore or a Knn
// distance/similarity) followed by optional named tuple arguments. With named args present the node
// becomes (HybridRank (List pos...) (AsStruct '(name value) ...)); otherwise the children are the
// scoring expressions directly. THybridRankSettings::Parse reads both shapes once and exposes typed
// getters. Validation failures are reported via Error (the caller turns it into a BAD_REQUEST issue);
// the per-tuple overrides (Weights, Limits, Indexes) are positionally parallel to the scoring args.
struct THybridRankSettings {
    static THybridRankSettings Parse(const TExprNode::TPtr& hybridRank, TExprContext& ctx);

    const TVector<TExprNode::TPtr>& ScoringArgs() const { return ScoringArgs_; }
    const TString& Mode() const { return Mode_; }
    double K(const TKqpOptimizeContext& kqpCtx) const {
        return K_.GetOrElse(kqpCtx.Config->HybridSearchK.Get().GetOrElse(60.0));
    }
    bool Normalize() const { return Normalize_.GetOrElse(true); }
    double Weight(size_t i) const { return i < Weights_.size() ? Weights_[i] : 1.0; }
    TMaybe<ui64> Limit(size_t i) const { return i < Limits_.size() ? Limits_[i] : Nothing(); }
    TMaybe<TString> IndexOverride(size_t i) const {
        return i < IndexOverrides_.size() ? IndexOverrides_[i] : Nothing();
    }

    TMaybe<TString> Error;

private:
    static TMaybe<TString> GetStringElem(const TExprNode::TPtr& n) {
        if (n->IsCallable("String") && n->ChildrenSize() >= 1 && n->Head().IsAtom()) {
            return TString(n->Head().Content());
        }
        return {};
    }
    static TMaybe<ui64> GetIntElem(const TExprNode::TPtr& n) {
        ui64 v = 0;
        if (n->ChildrenSize() >= 1 && n->Head().IsAtom() && TryFromString<ui64>(n->Head().Content(), v)) {
            return v;
        }
        return {};
    }
    static TMaybe<double> GetDoubleElem(const TExprNode::TPtr& n) {
        double v = 0;
        if (n->ChildrenSize() >= 1 && n->Head().IsAtom() && TryFromString<double>(n->Head().Content(), v)) {
            return v;  // accepts both integer (2) and double (0.2) numeric literals
        }
        return {};
    }

    TVector<TExprNode::TPtr> ScoringArgs_;
    TString Mode_ = "rrf";              // 'rrf' (Reciprocal Rank Fusion) or 'linear' (min-max normalized scores)
    TMaybe<double> K_;                  // RRF constant override
    TMaybe<bool> Normalize_;            // linear-only; default true (min-max normalize before fusing)
    TVector<double> Weights_;           // parallel to ScoringArgs_, or empty => all 1.0
    TVector<TMaybe<ui64>> Limits_;      // parallel to ScoringArgs_, or empty => factor * LIMIT
    TVector<TMaybe<TString>> IndexOverrides_;  // parallel to ScoringArgs_, or empty => auto-detect
};

THybridRankSettings THybridRankSettings::Parse(const TExprNode::TPtr& hybridRank, TExprContext& ctx) {
    Y_UNUSED(ctx);
    THybridRankSettings s;
    auto fail = [&](const TString& msg) {
        s.Error = msg;
        return s;
    };

    // Named-arg form: head is a tuple (List of positional args), child 1 is the AsStruct of options.
    const bool hasNamedArgs = hybridRank->Head().GetTypeAnn()
        && hybridRank->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple;
    if (!hasNamedArgs) {
        s.ScoringArgs_.assign(hybridRank->Children().begin(), hybridRank->Children().end());
        return s;
    }

    const auto& positional = hybridRank->Head();
    s.ScoringArgs_.assign(positional.Children().begin(), positional.Children().end());
    const auto n = s.ScoringArgs_.size();

    // Validate one (Weights/Limits/Indexes) tuple: its arity must equal the scoring-arg count, and each
    // element must parse via `parseElem`. Returns the parsed vector or records a precise error.
    auto parseTuple = [&](const TStringBuf name, const TExprNode::TPtr& value, auto parseElem,
                          const TStringBuf elemKind) -> bool {
        if (value->ChildrenSize() != n) {
            s.Error = TStringBuilder() << name << " has " << value->ChildrenSize()
                << " entries but there are " << n << " scoring arguments";
            return false;
        }
        for (const auto& elem : value->Children()) {
            if (!parseElem(elem)) {
                s.Error = TStringBuilder() << name << " must be a tuple of " << elemKind
                    << ", one per scoring argument";
                return false;
            }
        }
        return true;
    };

    for (const auto& member : hybridRank->Child(1)->Children()) {
        if (member->ChildrenSize() != 2 || !member->Head().IsAtom()) {
            continue;
        }
        const auto name = member->Head().Content();
        const auto value = member->ChildPtr(1);
        if (name == "Indexes") {
            s.IndexOverrides_.clear();
            if (!parseTuple("Indexes", value, [&](const TExprNode::TPtr& e) {
                    auto v = GetStringElem(e);
                    if (v) { s.IndexOverrides_.push_back(v); }
                    return v.Defined();
                }, "string literals")) {
                return s;
            }
        } else if (name == "Limits") {
            s.Limits_.clear();
            if (!parseTuple("Limits", value, [&](const TExprNode::TPtr& e) {
                    auto v = GetIntElem(e);
                    if (v) { s.Limits_.push_back(v); }
                    return v.Defined();
                }, "positive integer literals")) {
                return s;
            }
        } else if (name == "Weights") {
            s.Weights_.clear();
            if (!parseTuple("Weights", value, [&](const TExprNode::TPtr& e) {
                    auto v = GetDoubleElem(e);
                    if (v) { s.Weights_.push_back(*v); }
                    return v.Defined();
                }, "numeric literals")) {
                return s;
            }
        } else if (name == "K" || name == "k") {
            double kv = 0;
            if (value->ChildrenSize() < 1 || !value->Head().IsAtom() || !TryFromString<double>(value->Head().Content(), kv)) {
                return fail("K must be a numeric literal (the RRF constant), e.g. 60.0 AS K");
            }
            s.K_ = kv;
        } else if (name == "Mode") {
            if (!value->IsCallable("String") || value->ChildrenSize() < 1 || !value->Head().IsAtom()) {
                return fail("Mode must be a string literal: \"rrf\" or \"linear\"");
            }
            TString modeStr{value->Head().Content()};
            modeStr.to_lower();  // accept "RRF"/"Linear" too
            s.Mode_ = modeStr;
            if (s.Mode_ != "rrf" && s.Mode_ != "linear") {
                return fail(TStringBuilder() << "unknown Mode '" << value->Head().Content() << "'; expected \"rrf\" or \"linear\"");
            }
        } else if (name == "Normalize") {
            if (!value->IsCallable("Bool") || value->ChildrenSize() < 1 || !value->Head().IsAtom()) {
                return fail("Normalize must be a boolean literal: true or false");
            }
            s.Normalize_ = (value->Head().Content() == "true");
        } else {
            return fail(TStringBuilder() << "unknown named argument '" << name << "'; expected Indexes, Limits, K, Mode, Weights or Normalize");
        }
    }
    return s;
}

// Rewrites a hybrid search query:
//
//   SELECT ... FROM t
//   ORDER BY HybridRank(FullTextScore(c1, $q), Knn::CosineDistance(c2, $v), ...) [DESC]
//   LIMIT k
//
// into N independent index sub-queries (fulltext relevance scans and vector kmeans-tree scans),
// each bounded to an internal limit, whose results are fused with Reciprocal Rank Fusion (RRF).
//
// The scoring expressions may appear in any order and any mix: each is classified by inspecting the
// expression (a FullTextScore is a fulltext branch; a Knn distance/similarity is a vector branch).
// Unlike the standalone fulltext/vector rewrites this query names no index via VIEW (it needs several),
// so the rule resolves each branch's index from the table metadata by matching the scored column: a
// FullTextScore column selects a GlobalFulltextRelevance index, a Knn column selects a
// GlobalSyncVectorKMeansTree index. An explicit (...) AS Indexes override (one name per scoring arg)
// disambiguates. On any misuse it raises a precise error; queries it cannot rewrite fall through to the
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

    // Feature kill-switch (TableServiceConfig.EnableHybridSearch, on by default). Checked only after a
    // HybridRank marker is confirmed, so non-hybrid TopSort/Top queries are untouched. Failing here with a
    // clear message is better than skipping the rewrite and letting the bare HybridRank reach the peephole
    // stub (ExpandHybridRankBuiltin), which fails at runtime with a cryptic "could not be rewritten".
    if (!kqpCtx.Config->GetEnableHybridSearch()) {
        return addError("hybrid search is disabled (set TableServiceConfig.EnableHybridSearch=true to enable)");
    }

    if (top.KeySelectorLambda().Body().Ptr() != hybridRank) {
        return addError("must be the entire ORDER BY key; it cannot be negated or combined with other expressions");
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
    // smaller-is-better, similarities larger-is-better; each branch is sorted in the matching direction and
    // linear fusion normalizes accordingly (see TBranch::IsSimilarity below).
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

    // Parse the positional scoring args plus the optional named tuple arguments (Mode, Weights, Limits,
    // Indexes, K, Normalize). The per-tuple overrides are positionally parallel to the scoring args.
    auto settings = THybridRankSettings::Parse(hybridRank, ctx);
    if (settings.Error) {
        return addError(*settings.Error);
    }
    const auto& scoringArgs = settings.ScoringArgs();
    if (scoringArgs.size() < 2) {
        return addError("expects at least 2 arguments: scoring expressions to fuse (a fulltext score and/or a vector distance)");
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

    const auto& pkColumns = tableDesc.Metadata->KeyColumnNames;
    if (pkColumns.size() != 1) {
        return addError("hybrid search currently supports only a single-column primary key");
    }
    const TString pkCol = pkColumns[0];

    auto columnInList = [](const TVector<TString>& cols, const TString& name) {
        for (const auto& c : cols) {
            if (c == name) {
                return true;
            }
        }
        return false;
    };

    // ---------------------------------------------------------------------------------------------
    // Classify each scoring argument into a branch and resolve its index, then fuse the branches.
    // ---------------------------------------------------------------------------------------------
    enum class EBranchKind { Fulltext, VectorDistance, VectorSimilarity };
    struct TBranch {
        EBranchKind Kind;
        TExprNode::TPtr ScoreExpr;     // the raw scoring expression node
        TString ScoredColumn;          // the FullTextScore / Knn column it references
        TString IndexName;             // resolved (override or auto-detected)
        double Weight;
        TMaybe<ui64> LimitOverride;
        size_t Index;                  // position; used for unique synthetic column names
        bool IsSimilarity;             // fulltext relevance and Knn similarities rank larger-is-better
        // Filled during build:
        TExprNode::TPtr List;          // ordered {pk, ScoreCol} candidate list
        TString ScoreCol;              // relevance column (fulltext) or per-branch distance column (vector)
    };

    TVector<TBranch> branches;
    branches.reserve(scoringArgs.size());
    for (size_t i = 0; i < scoringArgs.size(); ++i) {
        const auto& arg = scoringArgs[i];
        const auto ftScore = findFulltextScore(arg);
        const auto knnApply = findKnnApply(arg);

        TBranch b;
        b.Index = i;
        b.ScoreExpr = arg;
        b.Weight = settings.Weight(i);
        b.LimitOverride = settings.Limit(i);

        const TStringBuilder branchId = TStringBuilder() << "argument " << (i + 1);
        const auto indexOverride = settings.IndexOverride(i);

        if (ftScore && !knnApply.IsValid()) {
            // ---- Fulltext relevance branch ----
            b.Kind = EBranchKind::Fulltext;
            b.IsSimilarity = true;
            auto ftColumnMember = TExprBase(ftScore->ChildPtr(0)).Maybe<TCoMember>();
            if (!ftColumnMember) {
                return addError(TStringBuilder() << "the first argument of FullTextScore (" << branchId << ") must reference a table column");
            }
            b.ScoredColumn = ftColumnMember.Cast().Name().StringValue();

            if (indexOverride) {
                const TIndexDescription* idx = nullptr;
                for (const auto& cand : tableDesc.Metadata->Indexes) {
                    if (cand.Name == *indexOverride) {
                        idx = &cand;
                        break;
                    }
                }
                if (!idx) {
                    return addError(TStringBuilder() << "fulltext index '" << *indexOverride << "' was not found");
                }
                if (idx->State != TIndexDescription::EIndexState::Ready) {
                    return addError(TStringBuilder() << "fulltext index '" << *indexOverride << "' is not ready");
                }
                if (idx->Type != TIndexDescription::EType::GlobalFulltextRelevance) {
                    return addError(TStringBuilder() << "index '" << *indexOverride << "' is not a fulltext relevance index");
                }
                if (!columnInList(idx->KeyColumns, b.ScoredColumn)) {
                    return addError(TStringBuilder() << "fulltext index '" << *indexOverride
                        << "' is not built on the FullTextScore column '" << b.ScoredColumn << "'");
                }
                b.IndexName = *indexOverride;
            } else {
                ui32 matches = 0;
                for (const auto& idx : tableDesc.Metadata->Indexes) {
                    if (idx.State == TIndexDescription::EIndexState::Ready
                        && idx.Type == TIndexDescription::EType::GlobalFulltextRelevance
                        && columnInList(idx.KeyColumns, b.ScoredColumn)) {
                        b.IndexName = idx.Name;
                        ++matches;
                    }
                }
                if (matches == 0) {
                    return addError(TStringBuilder() << "no ready fulltext relevance index found on column '" << b.ScoredColumn << "'");
                }
                if (matches > 1) {
                    return addError(TStringBuilder() << "multiple fulltext relevance indexes match column '" << b.ScoredColumn
                        << "'; name it explicitly via an Indexes override");
                }
            }
        } else if (knnApply.IsValid() && !ftScore) {
            // ---- Vector (kmeans-tree) branch ----
            bool isSimilarity = false;
            if (auto knnUdf = knnApply.Cast().Callable().Maybe<TCoUdf>()) {
                isSimilarity = knnSimilarityMethods.contains(knnUdf.Cast().MethodName().Value());
            }
            b.Kind = isSimilarity ? EBranchKind::VectorSimilarity : EBranchKind::VectorDistance;
            b.IsSimilarity = isSimilarity;

            // The table column the Knn call ranks against: the Member referenced anywhere in the
            // expression. A nullable column wraps the Knn call in an automap FlatMap, so the Member can
            // live in the wider expression (as the FlatMap input), not inside the Knn apply itself.
            VisitExpr(arg, [&](const TExprNode::TPtr& n) {
                if (!b.ScoredColumn.empty()) {
                    return false;
                }
                if (auto member = TExprBase(n).Maybe<TCoMember>()) {
                    b.ScoredColumn = member.Cast().Name().StringValue();
                    return false;
                }
                return true;
            });
            if (b.ScoredColumn.empty()) {
                return addError(TStringBuilder() << "the Knn argument (" << branchId
                    << ") must reference a table column, e.g. Knn::CosineDistance(embedding, $target)");
            }

            auto checkVectorIndex = [&](const TIndexDescription& idx) -> TMaybe<TString> {
                if (idx.Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
                    return TStringBuilder() << "index '" << idx.Name << "' is not a vector kmeans-tree index";
                }
                if (idx.KeyColumns.size() != 1) {
                    return TStringBuilder() << "vector index '" << idx.Name
                        << "' is a prefixed vector index, which HybridRank does not support yet";
                }
                return Nothing();
            };

            if (indexOverride) {
                const TIndexDescription* idx = nullptr;
                for (const auto& cand : tableDesc.Metadata->Indexes) {
                    if (cand.Name == *indexOverride) {
                        idx = &cand;
                        break;
                    }
                }
                if (!idx) {
                    return addError(TStringBuilder() << "vector index '" << *indexOverride << "' was not found");
                }
                if (idx->State != TIndexDescription::EIndexState::Ready) {
                    return addError(TStringBuilder() << "vector index '" << *indexOverride << "' is not ready");
                }
                if (auto err = checkVectorIndex(*idx)) {
                    return addError(*err);
                }
                if (idx->KeyColumns.back() != b.ScoredColumn) {
                    return addError(TStringBuilder() << "vector index '" << *indexOverride
                        << "' is not built on the Knn distance column '" << b.ScoredColumn << "'");
                }
                b.IndexName = *indexOverride;
            } else {
                ui32 matches = 0;
                for (const auto& idx : tableDesc.Metadata->Indexes) {
                    if (idx.State == TIndexDescription::EIndexState::Ready
                        && idx.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree
                        && idx.KeyColumns.size() == 1 && idx.KeyColumns.back() == b.ScoredColumn) {
                        b.IndexName = idx.Name;
                        ++matches;
                    }
                }
                if (matches == 0) {
                    return addError(TStringBuilder() << "no ready vector (kmeans-tree) index found on column '" << b.ScoredColumn << "'");
                }
                if (matches > 1) {
                    return addError(TStringBuilder() << "multiple vector indexes match column '" << b.ScoredColumn
                        << "'; name it explicitly via an Indexes override");
                }
            }
        } else {
            return addError(TStringBuilder() << branchId << " is neither a FullTextScore nor a Knn distance/similarity over a column, "
                "e.g. FullTextScore(text, query) or Knn::CosineDistance(embedding, $target)");
        }

        branches.push_back(std::move(b));
    }

    // ---------------------------------------------------------------------------------------------
    // Build the N index branches and fuse their per-document contributions.
    // ---------------------------------------------------------------------------------------------
    const auto pos = node.Pos();

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

    const TString relevanceCol{NTableIndex::NFulltext::FullTextRelevanceColumn};
    const TString rrfCol = "__ydb_hybrid_rrf";

    auto uint64Node = [&](ui64 v) {
        return ctx.Builder(pos).Callable("Uint64").Atom(0, ToString(v), TNodeFlags::Default).Seal().Build();
    };
    // Per-branch internal candidate limit: an explicit AS Limits override, otherwise LIMIT *
    // HybridSearchFactor (pragma, default 10) computed at optimize time. The fulltext ItemsLimit and the
    // vector TopLimit must be literals, so a non-literal (e.g. parameterised) LIMIT cannot be sized safely
    // -- a fixed fallback smaller than the runtime LIMIT would silently drop rows from the final TopSort --
    // unless every branch carries an explicit Limits override. Compute the shared fallback once and only
    // require a literal LIMIT if some branch needs it.
    const ui64 factor = kqpCtx.Config->HybridSearchFactor.Get().GetOrElse(10);
    TMaybe<ui64> sharedFallback;  // limitVal * factor; computed lazily
    auto branchLimit = [&](const TBranch& b) -> TMaybeNode<TExprBase> {
        if (b.LimitOverride) {
            return TExprBase(uint64Node(*b.LimitOverride));
        }
        if (!sharedFallback) {
            ui64 limitVal = 0;
            bool literalLimit = false;
            const auto& countNode = top.Count().Ref();
            if (countNode.IsCallable() && countNode.ChildrenSize() == 1 && countNode.Head().IsAtom()) {
                literalLimit = TryFromString<ui64>(countNode.Head().Content(), limitVal);
            }
            if (!literalLimit) {
                return {};  // signal: literal LIMIT required
            }
            sharedFallback = limitVal * factor;
        }
        return TExprBase(uint64Node(*sharedFallback));
    };

    const auto kConst = ctx.Builder(pos).Callable("Double").Atom(0, ToString(settings.K(kqpCtx)), TNodeFlags::Default).Seal().Build();

    const auto mainTableMeta = BuildTableMeta(*tableDesc.Metadata, pos, ctx);

    // ---- Build each branch's ordered candidate list { pk, scoreCol } ----
    for (auto& b : branches) {
        auto limitNode = branchLimit(b);
        if (!limitNode) {
            return addError("requires a literal LIMIT; for a parameterised LIMIT pass explicit per-branch "
                            "candidate counts via (... AS Limits)");
        }
        const auto branchN = limitNode.Cast().Ptr();

        if (b.Kind == EBranchKind::Fulltext) {
            // Fulltext relevance branch: top-N {pk, relevance} ordered by relevance DESC. Built directly
            // (as KqpSelectJsonIndex does for JSON) as a TKqlReadTableFullTextIndex returning
            // {pk, __ydb_full_text_relevance} ranked by relevance, capped at branchN via ItemsLimit.
            b.ScoreCol = relevanceCol;
            const auto ftQuery = findFulltextScore(b.ScoreExpr)->ChildPtr(1);
            TKqpReadTableFullTextIndexSettings ftSettings;
            ftSettings.SetItemsLimit(branchN);
            b.List = Build<TKqlReadTableFullTextIndex>(ctx, pos)
                .Table(mainTableMeta)
                .Index().Build(b.IndexName)
                .Columns(BuildKeyColumnsList(pos, ctx, TVector<TString>{pkCol, relevanceCol}))
                .Query<TExprList>().Add(TExprBase(ftQuery)).Build()
                .QueryColumns(BuildKeyColumnsList(pos, ctx, TVector<TString>{b.ScoredColumn}))
                .Settings(ftSettings.BuildNode(ctx, pos))
                .Done().Ptr();
            // Raw ordered branch. RRF streams it through KqpStreamEnumerate (pushed into the branch's
            // single-partition stage by the PushStreamEnumerateToStage physical rule), so no TDqPrecompute
            // is needed; linear re-materializes it below (its ListMin/ListMax are whole-list aggregates).
        } else {
            // Vector branch: top-N {pk, score} ordered best-first (distance ASC, similarity DESC). Emitted
            // as a synthetic TopSort over the vector index read; RewriteTopSortOverIndexRead
            // (DoRewriteTopSortOverKMeansTree) lowers it into the kmeans-tree lookup chain. CanUseVectorIndex
            // matches the function + this sort direction against the index metric (e.g. cosine accepts
            // CosineDistance ASC or CosineSimilarity DESC). Each vector branch carries a unique synthetic
            // distance column so the branches never share schema (kept internal to this TopSort regardless).
            b.ScoreCol = TStringBuilder() << "__ydb_hybrid_distance_" << b.Index;
            const auto vecRead = Build<TKqlReadTableIndexRanges>(ctx, pos)
                .Table(mainTableMeta)
                .Ranges<TCoVoid>().Build()
                .ExplainPrompt().Build()
                .Columns(BuildKeyColumnsList(pos, ctx, TVector<TString>{pkCol, b.ScoredColumn}))
                .Settings().Build()
                .Index().Build(b.IndexName)
                .Done().Ptr();

            const auto vecRowArg = ctx.NewArgument(pos, "vecRow");
            const auto distShared = ctx.ReplaceNode(TExprNode::TPtr(b.ScoreExpr), *rowArg, vecRowArg);
            const auto vecFlatMapBody = ctx.Builder(pos)
                .Callable("Just")
                    .Callable(0, "AsStruct")
                        .List(0)
                            .Atom(0, pkCol)
                            .Callable(1, "Member").Add(0, vecRowArg).Atom(1, pkCol).Seal()
                        .Seal()
                        .List(1)
                            .Atom(0, b.ScoreCol)
                            .Add(1, distShared)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
            b.List = ctx.Builder(pos)
                .Callable("TopSort")
                    .Callable(0, "FlatMap")
                        .Add(0, vecRead)
                        .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {vecRowArg}), TExprNode::TPtr(vecFlatMapBody)))
                    .Seal()
                    .Add(1, branchN)
                    .Callable(2, "Bool").Atom(0, b.IsSimilarity ? "false" : "true", TNodeFlags::Default).Seal()
                    .Lambda(3)
                        .Param("r")
                        .Callable("Member").Arg(0, "r").Atom(1, b.ScoreCol).Seal()
                    .Seal()
                .Seal()
                .Build();
        }
    }

    // ---- Fuse: each branch emits a per-row weighted contribution; we then SUM the contributions
    //      grouped by primary key. A document absent from a branch simply has no contribution row
    //      there (so it contributes 0 -- no penalty-rank sentinel needed), and the cross-branch fusion
    //      is a single additive group-by-pk Aggregate rather than a per-candidate dict join. Each
    //      branch's contribution reuses the HybridSearch UDF with single-element lists:
    //        RRF:    w / (k + rank)         = HybridSearch.RRF([rank], [w], k)
    //        linear: w * normalize(score)   = HybridSearch.LinearFuse([score], [min], [max], [w], [isSim], norm)
    const auto emptyStruct = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{});
    const auto emptyTuple = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{});

    // Linear-only normalization flag, passed to the contribution UDF as a constant.
    const auto normConst = ctx.Builder(pos).Callable("Bool").Atom(0, settings.Normalize() ? "true" : "false", TNodeFlags::Default).Seal().Build();
    auto doubleConst = [&](double v) {
        return ctx.Builder(pos).Callable("Double").Atom(0, ToString(v), TNodeFlags::Default).Seal().Build();
    };
    auto boolConst = [&](bool v) {
        return ctx.Builder(pos).Callable("Bool").Atom(0, v ? "true" : "false", TNodeFlags::Default).Seal().Build();
    };

    const TString contribCol = "__ydb_hybrid_contrib";

    // One per-branch contribution list { pk, __ydb_hybrid_contrib }, in branch order.
    TVector<TExprNode::TPtr> branchContribs;
    branchContribs.reserve(branches.size());
    auto asList1 = [&](const TExprNode::TPtr& v) {
        return ctx.Builder(pos).Callable("AsList").Add(0, v).Seal().Build();
    };
    if (settings.Mode() == "linear") {
        // A score field of a row, cast to a (non-optional) Double.
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
        // min/max of a branch's scores across the candidate set (a scalar; loop-invariant per branch).
        auto minMaxOf = [&](const TExprNode::TPtr& list, const TString& col, const char* op) {
            const auto mArg = ctx.NewArgument(pos, "it");
            const auto mapped = ctx.Builder(pos).Callable("Map").Add(0, list)
                .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {mArg}), castDouble(mArg, col)))
                .Seal().Build();
            return ctx.Builder(pos)
                .Callable("Coalesce")
                    .Callable(0, op).Add(0, mapped).Seal()
                    .Callable(1, "Double").Atom(0, "0", TNodeFlags::Default).Seal()
                .Seal().Build();
        };
        // LinearFuse contribution UDF type: [List<Double> x4, List<Bool>, Bool] -> Double.
        const auto doubleListType = ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Double));
        const auto boolListType = ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Bool));
        const auto linUdfType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                doubleListType, doubleListType, doubleListType, doubleListType, boolListType,
                ctx.MakeType<TDataExprType>(EDataSlot::Bool),
            }),
            emptyStruct, emptyTuple,
        });
        const auto linUdf = Build<TCoUdf>(ctx, pos)
            .MethodName().Build("HybridSearch.LinearFuse")
            .RunConfigValue<TCoVoid>().Build()
            .UserType(ExpandType(pos, *linUdfType, ctx))
            .Done().Ptr();
        // { pk, w * normalize(score) } for one branch (the UDF does the min-max + distance/similarity math;
        // a single-element list per argument computes just this branch's contribution).
        auto buildContribs = [&](const TExprNode::TPtr& list, const TString& scoreCol,
                                 const TExprNode::TPtr& weight, const TExprNode::TPtr& simConst) {
            const auto branchMin = minMaxOf(list, scoreCol, "ListMin");
            const auto branchMax = minMaxOf(list, scoreCol, "ListMax");
            const auto contribRowArg = ctx.NewArgument(pos, "r");
            const auto contrib = ctx.Builder(pos)
                .Callable("Apply")
                    .Add(0, linUdf)
                    .Add(1, asList1(castDouble(contribRowArg, scoreCol)))
                    .Add(2, asList1(branchMin))
                    .Add(3, asList1(branchMax))
                    .Add(4, asList1(weight))
                    .Add(5, asList1(simConst))
                    .Add(6, normConst)
                .Seal().Build();
            const auto body = ctx.Builder(pos)
                .Callable("AsStruct")
                    .List(0).Atom(0, pkCol).Callable(1, "Member").Add(0, contribRowArg).Atom(1, pkCol).Seal().Seal()
                    .List(1).Atom(0, contribCol).Add(1, contrib).Seal()
                .Seal().Build();
            return ctx.Builder(pos).Callable("Map").Add(0, list)
                .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {contribRowArg}), TExprNode::TPtr(body)))
                .Seal().Build();
        };
        for (const auto& b : branches) {
            // Linear normalization needs per-branch min/max (whole-list aggregates), so materialize each branch.
            const auto mat = Build<TDqPrecompute>(ctx, pos).Input(TExprBase(b.List)).Done().Ptr();
            branchContribs.push_back(buildContribs(mat, b.ScoreCol, doubleConst(b.Weight), boolConst(b.IsSimilarity)));
        }
    } else {
        // RRF: rank = 1-based position in the (already-sorted) branch; contribution = w / (k + rank).
        const auto uint64ListType = ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
        const auto doubleListType = ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Double));
        const auto rrfUdfType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                uint64ListType,
                doubleListType,
                ctx.MakeType<TDataExprType>(EDataSlot::Double),
            }),
            emptyStruct, emptyTuple,
        });
        const auto rrfUdf = Build<TCoUdf>(ctx, pos)
            .MethodName().Build("HybridSearch.RRF")
            .RunConfigValue<TCoVoid>().Build()
            .UserType(ExpandType(pos, *rrfUdfType, ctx))
            .Done().Ptr();
        // { pk, w / (k + rank) } for one branch.
        auto buildContribs = [&](const TExprNode::TPtr& list, const TExprNode::TPtr& weight) {
            const auto pArg = ctx.NewArgument(pos, "p");
            const auto rankList = ctx.Builder(pos).Callable("AsList")
                .Callable(0, "Nth").Add(0, pArg).Atom(1, 0U).Seal()
                .Seal().Build();
            const auto pkMember = ctx.Builder(pos).Callable("Member")
                .Callable(0, "Nth").Add(0, pArg).Atom(1, 1U).Seal()
                .Atom(1, pkCol)
                .Seal().Build();
            const auto contrib = ctx.Builder(pos)
                .Callable("Apply")
                    .Add(0, rrfUdf)
                    .Add(1, rankList)
                    .Add(2, asList1(weight))
                    .Add(3, kConst)
                .Seal().Build();
            const auto body = ctx.Builder(pos)
                .Callable("AsStruct")
                    .List(0).Atom(0, pkCol).Add(1, pkMember).Seal()
                    .List(1).Atom(0, contribCol).Add(1, contrib).Seal()
                .Seal().Build();
            // KqpStreamEnumerate assigns the 1-based rank over the ordered branch *stream*; the
            // PushStreamEnumerateToStage physical rule later pushes it into the branch's single-partition
            // stage, so the branch need not be materialized via TDqPrecompute first.
            return ctx.Builder(pos)
                .Callable("Map")
                    .Callable(0, "KqpStreamEnumerate").Add(0, list).Seal()
                    .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {pArg}), TExprNode::TPtr(body)))
                .Seal().Build();
        };
        for (const auto& b : branches) {
            branchContribs.push_back(buildContribs(b.List, doubleConst(b.Weight)));
        }
    }

    // Cross-branch fusion: SUM the per-branch contributions grouped by primary key, giving
    // { pk, __ydb_hybrid_rrf } per distinct candidate. Sum is typed Optional<Double>; coalesce it to a
    // plain Double.
    // The per-branch {pk, contrib} streams are simply Extend-ed together and fed straight into the
    // Aggregate. The grouped SUM lowers to a distributed hash-combine pipeline
    // (DqPhyHashCombine -> DqCnHashShuffle by pk -> final DqPhyHashCombine): the shuffle routes every row
    // for a given pk to the same partition, so all of a candidate's contributions are summed together
    // regardless of which branch (or when) they arrive.
    const auto fusedInput = branchContribs.size() == 1
        ? branchContribs[0]
        : ctx.NewCallable(pos, "Extend", TExprNode::TListType(branchContribs.begin(), branchContribs.end()));
    const auto aggregated = ctx.Builder(pos)
        .Callable("Aggregate")
            .Add(0, fusedInput)
            .List(1).Atom(0, pkCol).Seal()                       // group keys
            .List(2)                                             // handlers
                .List(0)
                    .Atom(0, rrfCol)
                    .Callable(1, "AggApply")
                        .Atom(0, "sum")
                        .Callable(1, "ListItemType")
                            .Callable(0, "TypeOf").Add(0, fusedInput).Seal()
                        .Seal()
                        .Lambda(2).Param("row")
                            .Callable("Member").Arg(0, "row").Atom(1, contribCol).Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .List(3).Seal()                                      // settings
        .Seal()
        .Build();
    const auto fusedScored = ctx.Builder(pos)
        .Callable("Map").Add(0, aggregated)
            .Lambda(1).Param("r")
                .Callable("AsStruct")
                    .List(0).Atom(0, pkCol).Callable(1, "Member").Arg(0, "r").Atom(1, pkCol).Seal().Seal()
                    .List(1).Atom(0, rrfCol)
                        .Callable(1, "Coalesce")
                            .Callable(0, "Member").Arg(0, "r").Atom(1, rrfCol).Seal()
                            .Callable(1, "Double").Atom(0, "0", TNodeFlags::Default).Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // ---- Look up the main table by the fused candidate keys, carrying the RRF score through ----
    // A LookupJoinRows stream lookup joins the {pk, rrf} stream against the main table by pk and emits a
    // tuple (leftRow, Optional<mainRow>, _) per candidate. This carries the synthetic rrf score (a left
    // column) through the lookup, so the score does not have to be re-joined afterwards via a dict. It also
    // lets fusedScored feed the lookup directly -- it is consumed exactly once here, so no TDqPrecompute is
    // needed for it. (The left input here is the aggregate's already-merged one-row-per-pk output.)
    const auto lookupInput = ctx.Builder(pos)
        .Callable("Map")
            .Add(0, fusedScored)
            .Lambda(1)
                .Param("r")
                .List()
                    .Arg(0, "r")                                 // element 0: the {pk, rrf} left row
                    .Callable(1, "Just")                         // element 1: Just({pk}) -- the join key
                        .Callable(0, "AsStruct")
                            .List(0).Atom(0, pkCol).Callable(1, "Member").Arg(0, "r").Atom(1, pkCol).Seal().Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    const auto mainColumns = MergeColumns(read.Columns(), TVector<TString>{pkCol}, ctx);

    TKqpStreamLookupSettings lookupSettings;
    lookupSettings.Strategy = EStreamLookupStrategyType::LookupJoinRows;
    const auto joined = Build<TKqlStreamLookupTable>(ctx, pos)
        .Table(mainTableMeta)
        .LookupKeys(TExprBase(lookupInput))
        .Columns(mainColumns)
        .Settings(lookupSettings.BuildNode(ctx, pos))
        .Done().Ptr();

    // ---- Re-apply WHERE, attach the carried RRF, re-rank by RRF DESC, take LIMIT, project ----
    // joined emits (leftRow {pk, rrf}, Optional<mainRow>, _). Unwrap the main row, re-apply the original
    // WHERE on it, and graft the rrf score (read straight off the left row -- always present, no dict
    // lookup and no coalesce-to-0 needed).
    const auto joinArg = ctx.NewArgument(pos, "j");
    const auto mrowArg = ctx.NewArgument(pos, "mrow");
    const auto predOverMrow = origPred
        ? ctx.ReplaceNode(TExprNode::TPtr(origPred), *origArg, mrowArg)
        : ctx.Builder(pos).Callable("Bool").Atom(0, "true", TNodeFlags::Default).Seal().Build();
    const auto rrfForMrow = ctx.Builder(pos)
        .Callable("Member")
            .Callable(0, "Nth").Add(0, joinArg).Atom(1, 0U).Seal()    // leftRow {pk, rrf}
            .Atom(1, rrfCol)
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
    // For each join tuple, unwrap Optional<mainRow> (element 1) and apply the augment lambda to the present
    // main row.
    const auto perJoinBody = ctx.Builder(pos)
        .Callable("FlatMap")
            .Callable(0, "Nth").Add(0, joinArg).Atom(1, 1U).Seal()
            .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {mrowArg}), TExprNode::TPtr(augBody)))
        .Seal()
        .Build();
    const auto augmented = ctx.Builder(pos)
        .Callable("FlatMap")
            .Add(0, joined)
            .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {joinArg}), TExprNode::TPtr(perJoinBody)))
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
        .Callable("OrderedMap")
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
