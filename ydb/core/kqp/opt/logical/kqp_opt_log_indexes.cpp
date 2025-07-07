#include <ydb/core/base/table_index.h>
#include <ydb/core/base/table_vector_index.h>
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
    auto flatMap = lambdaBody.Maybe<TCoFlatMap>();
    if (!flatMap) {
        return checkUdf(lambdaBody, true);
    }
    auto flatMapInput = flatMap.Cast().Input();
    if (!checkMember(flatMapInput)) {
        return false;
    }
    auto flatMapLambdaBody = flatMap.Cast().Lambda().Body();
    return checkUdf(flatMapLambdaBody, false);
}

struct TReadMatch {
    TMaybeNode<TKqlReadTableIndex> Read;
    TMaybeNode<TKqlReadTableIndexRanges> ReadRanges;

    static TReadMatch Match(TExprBase expr) {
        if (auto read = expr.Maybe<TKqlReadTableIndex>()) {
            return {read.Cast(), {}};
        }
        if (auto read = expr.Maybe<TKqlReadTableIndexRanges>()) {
            return {{}, read.Cast()};
        }
        return {};
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
    const TKikimrTableDescription& tableDesc, TIntrusivePtr<TKikimrTableMetadata> indexMeta, bool useStreamLookup,
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

    if (useStreamLookup) {
        TKqpStreamLookupSettings settings;
        settings.Strategy = EStreamLookupStrategyType::LookupRows;
        return Build<TKqlStreamLookupTable>(ctx, read.Pos())
            .Table(read.Table())
            .LookupKeys(readIndexTable.Ptr())
            .Columns(read.Columns())
            .Settings(settings.BuildNode(ctx, read.Pos()))
            .Done();
    } else {
        return Build<TKqlLookupTable>(ctx, read.Pos())
            .Table(read.Table())
            .LookupKeys(readIndexTable.Ptr())
            .Columns(read.Columns())
            .Done();
    }
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
    const TExprNode& fromArgs, const TExprBase& fromBody)
{
    auto newLambda = NewLambdaFrom(ctx, pos, replaces, fromArgs, fromBody);
    replaces.clear();
    auto args = newLambda.Args().Ptr();

    auto flatMap = newLambda.Body().Maybe<TCoFlatMap>();
    if (!flatMap) {
        auto apply = newLambda.Body().Cast<TCoApply>();
        for (auto arg : apply.Args()) {
            auto oldMember = arg.Maybe<TCoMember>();
            if (oldMember && oldMember.Cast().Name().Value() == indexDesc.KeyColumns.back()) {
                auto newMember = Build<TCoMember>(ctx, pos)
                    .Name().Build(NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn)
                    .Struct(oldMember.Cast().Struct())
                .Done();
                replaces.emplace(oldMember.Raw(), newMember.Ptr());
                break;
            }
        }
        return ctx.NewLambda(pos,
            std::move(args),
            ctx.ReplaceNodes(TExprNode::TListType{apply.Ptr()}, replaces));
    }

    auto apply = flatMap.Cast().Lambda().Body().Cast<TCoApply>();
    for (auto arg : apply.Args()) {
        if (arg.Ref().Type() == NYql::TExprNode::Argument) {
            auto oldMember = flatMap.Cast().Input().Cast<TCoMember>();
            auto newMember = Build<TCoMember>(ctx, pos)
                .Name().Build(NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn)
                .Struct(oldMember.Struct())
            .Done();
            replaces.emplace(arg.Raw(), newMember.Ptr());
            break;
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
            .Name().Build(NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn)
            .Value<TCoMember>().Struct(mapArg)
                .Name().Build(NTableIndex::NTableVectorKmeansTreeIndex::IdColumn)
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
    const TIndexDescription& indexDesc, TExprContext& ctx, TPositionHandle pos, const TKqpOptimizeContext& kqpCtx, 
    const TExprNodePtr& lambda, const TCoTopBase& top,
    const TKqpTable& levelTable, const TCoAtomList& levelColumns, 
    TExprNodePtr& read)
{
    const auto& settings = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc.SpecializedIndexDescription)
        .settings();
    const auto clusters = std::max<ui32>(2, settings.clusters());
    const auto levels = std::max<ui32>(1, settings.levels());
    Y_ENSURE(levels >= 1);
    const auto levelTop = std::min<ui32>(kqpCtx.Config->KMeansTreeSearchTopSize.Get().GetOrElse(1), clusters);

    auto count = ctx.Builder(pos)
        .Callable("Uint64").Atom(0, std::to_string(levelTop), TNodeFlags::Default).Seal()
    .Build();

    for (ui32 level = 1;; ++level) {
        read = Build<TCoTop>(ctx, pos)
            .Input(read)
            .KeySelectorLambda(lambda)
            .SortDirections(top.SortDirections())
            .Count(count)
        .Done().Ptr();

        RemapIdToParent(ctx, pos, read);

        if (level == levels) {
            break;
        }

        read = Build<TKqlLookupTable>(ctx, pos)
            .Table(levelTable)
            .LookupKeys(read)
            .Columns(levelColumns)
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
    TExprNodePtr& read)
{
    const bool isCovered = CheckIndexCovering(mainColumns, postingTableMeta);

    if (!isCovered) {
        const auto postingColumns = BuildKeyColumnsList(pos, ctx, mainTableMeta->KeyColumnNames);

        read = Build<TKqlLookupTable>(ctx, pos)
            .Table(postingTable)
            .LookupKeys(read)
            .Columns(postingColumns)
            .Done().Ptr();

        read = Build<TKqlLookupTable>(ctx, pos)
            .Table(mainTable)
            .LookupKeys(read)
            .Columns(mainColumns)
            .Done().Ptr();
    } else {
        read = Build<TKqlLookupTable>(ctx, pos)
            .Table(postingTable)
            .LookupKeys(read)
            .Columns(mainColumns)
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

TExprBase DoRewriteTopSortOverKMeansTree(
    const TReadMatch& match, const TMaybeNode<TCoFlatMap>& flatMap, const TExprNode& lambdaArgs, const TExprBase& lambdaBody, const TCoTopBase& top,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TKikimrTableDescription& tableDesc, const TIndexDescription& indexDesc, const TKikimrTableMetadata& implTable)
{
    Y_ASSERT(indexDesc.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree);
    const auto* levelTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Name);
    const auto* postingTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Next->Name);
    YQL_ENSURE(!implTable.Next->Next);
    YQL_ENSURE(levelTableDesc->Metadata->Name.EndsWith(NTableIndex::NTableVectorKmeansTreeIndex::LevelTable));
    YQL_ENSURE(postingTableDesc->Metadata->Name.EndsWith(NTableIndex::NTableVectorKmeansTreeIndex::PostingTable));

    // TODO(mbkkt) It's kind of strange that almost everything here have same position
    const auto pos = match.Pos();

    const auto levelTable = BuildTableMeta(*levelTableDesc->Metadata, pos, ctx);
    const auto postingTable = BuildTableMeta(*postingTableDesc->Metadata, pos, ctx);
    const auto mainTable = BuildTableMeta(*tableDesc.Metadata, pos, ctx);

    const auto levelColumns = BuildKeyColumnsList(pos, ctx,
            std::initializer_list<std::string_view>{NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn});
    const auto& mainColumns = match.Columns();

    TNodeOnNodeOwnedMap replaces;
    const auto levelLambda = LevelLambdaFrom(indexDesc, ctx, pos, replaces, lambdaArgs, lambdaBody);

    // TODO(mbkkt) How to inline construction of these constants to construction of readLevel0?
    const auto fromValues = ctx.Builder(pos)
        .Callable(NTableIndex::ClusterIdTypeName).Atom(0, "0", TNodeFlags::Default).Seal()
    .Build();
    const auto toValues = ctx.Builder(pos)
        .Callable(NTableIndex::ClusterIdTypeName).Atom(0, "1", TNodeFlags::Default).Seal()
    .Build();

    // TODO(mbkkt) Is it best way to do `SELECT FROM levelTable WHERE first_pk_column = 0`?
    auto read = Build<TKqlReadTable>(ctx, pos)
        .Table(levelTable)
        .Range<TKqlKeyRange>()
            .From<TKqlKeyInc>()
                .Add(fromValues)
            .Build()
            .To<TKqlKeyExc>()
                .Add(toValues)
            .Build()
        .Build()
        .Columns(levelColumns)
        .Settings(match.Settings())
    .Done().Ptr();

    VectorReadLevel(indexDesc, ctx, pos, kqpCtx, levelLambda, top, levelTable, levelColumns, read);

    VectorReadMain(ctx, pos, postingTable, postingTableDesc->Metadata, mainTable, tableDesc.Metadata, mainColumns, read);

    if (flatMap) {
        read = Build<TCoFlatMap>(ctx, flatMap.Cast().Pos())
            .Input(read)
            .Lambda(ctx.DeepCopyLambda(flatMap.Cast().Lambda().Ref()))
        .Done().Ptr();
    }

    VectorTopMain(ctx, top, read);

    return TExprBase{read};
}

TExprBase DoRewriteTopSortOverPrefixedKMeansTree(
    const TReadMatch& match, const TCoFlatMap& flatMap, const TExprNode& lambdaArgs, const TExprBase& lambdaBody, const TCoTopBase& top,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TKikimrTableDescription& tableDesc, const TIndexDescription& indexDesc, const TKikimrTableMetadata& implTable)
{
    Y_ASSERT(indexDesc.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree);
    Y_ASSERT(indexDesc.KeyColumns.size() > 1);
    const auto* levelTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Name);
    const auto* postingTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Next->Name);
    const auto* prefixTableDesc = &kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, implTable.Next->Next->Name);
    YQL_ENSURE(!implTable.Next->Next->Next);
    YQL_ENSURE(levelTableDesc->Metadata->Name.EndsWith(NTableIndex::NTableVectorKmeansTreeIndex::LevelTable));
    YQL_ENSURE(postingTableDesc->Metadata->Name.EndsWith(NTableIndex::NTableVectorKmeansTreeIndex::PostingTable));
    YQL_ENSURE(prefixTableDesc->Metadata->Name.EndsWith(NTableIndex::NTableVectorKmeansTreeIndex::PrefixTable));

    // TODO(mbkkt) It's kind of strange that almost everything here have same position
    const auto pos = match.Pos();

    const auto levelTable = BuildTableMeta(*levelTableDesc->Metadata, pos, ctx);
    const auto postingTable = BuildTableMeta(*postingTableDesc->Metadata, pos, ctx);
    const auto prefixTable = BuildTableMeta(*prefixTableDesc->Metadata, pos, ctx);
    const auto mainTable = BuildTableMeta(*tableDesc.Metadata, pos, ctx);

    const auto levelColumns = BuildKeyColumnsList(pos, ctx,
            std::initializer_list<std::string_view>{NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn});
    const auto prefixColumns = [&] {
        auto columns = indexDesc.KeyColumns;
        columns.back().assign(NTableIndex::NTableVectorKmeansTreeIndex::IdColumn);
        return BuildKeyColumnsList(pos, ctx, columns);
    }();
    const auto& mainColumns = match.Columns();

    TNodeOnNodeOwnedMap replaces;
    TMaybeNode<TCoLambda> mainLambda;
    const auto prefixLambda = [&] {
        auto newLambda = NewLambdaFrom(ctx, pos, replaces, flatMap.Lambda().Args().Ref(), flatMap.Lambda().Body());
        auto optionalIf = newLambda.Body().Cast<TCoOptionalIf>();
        auto oldValue = optionalIf.Value().Maybe<TCoAsStruct>();
        if (!oldValue) {
            return newLambda.Ptr();
        }
        auto args = newLambda.Args();
        mainLambda = NewLambdaFrom(ctx, pos, replaces, args.Ref(), oldValue.Cast());

        replaces.clear();
        replaces.emplace(oldValue.Raw(), args.Arg(0).Ptr());
        return ctx.NewLambda(pos,
            args.Ptr(),
            ctx.ReplaceNodes(TExprNode::TListType{optionalIf.Ptr()}, replaces));
    }();
    const auto levelLambda = LevelLambdaFrom(indexDesc, ctx, pos, replaces, lambdaArgs, lambdaBody);

    auto read = match.BuildRead(ctx, prefixTable, prefixColumns).Ptr();

    read = Build<TCoFlatMap>(ctx, pos)
        .Input(read)
        .Lambda(prefixLambda)
    .Done().Ptr();

    RemapIdToParent(ctx, pos, read);

    read = Build<TKqlLookupTable>(ctx, pos)
        .Table(levelTable)
        .LookupKeys(read)
        .Columns(levelColumns)
    .Done().Ptr();

    VectorReadLevel(indexDesc, ctx, pos, kqpCtx, levelLambda, top, levelTable, levelColumns, read);

    VectorReadMain(ctx, pos, postingTable, postingTableDesc->Metadata, mainTable, tableDesc.Metadata, mainColumns, read);

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
    if (auto indexRead = TReadMatch::Match(node)) {

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, indexRead.Table().Path());
        const auto indexName = indexRead.Index().Value();
        auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
        // TODO(mbkkt) instead of ensure should be warning and main table read?
        YQL_ENSURE(indexDesc->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree,
            "index read doesn't support vector index: " << indexName);

        return DoRewriteIndexRead(indexRead, ctx, tableDesc, implTable, kqpCtx.IsScanQuery(), {});
    }

    return node;
}

TExprBase KqpRewriteLookupIndex(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (kqpCtx.IsScanQuery()) {
        // TODO: Enable index lookup for scan queries as we now support stream lookups.
        return node;
    }

    if (auto maybeLookupIndex = node.Maybe<TKqlLookupIndex>()) {
        auto lookupIndex = maybeLookupIndex.Cast();

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, lookupIndex.Table().Path());
        const auto indexName = lookupIndex.Index().Value();
        auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
        // TODO(mbkkt) instead of ensure should be warning and main table lookup?
        YQL_ENSURE(indexDesc->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree,
            "lookup doesn't support vector index: " << indexName);

        const bool isCovered = CheckIndexCovering(lookupIndex.Columns(), implTable);

        if (isCovered) {
            if (kqpCtx.Config->EnableKqpDataQueryStreamLookup) {
                TKqpStreamLookupSettings settings;
                settings.Strategy = EStreamLookupStrategyType::LookupRows;
                return Build<TKqlStreamLookupTable>(ctx, node.Pos())
                    .Table(BuildTableMeta(*implTable, node.Pos(), ctx))
                    .LookupKeys(lookupIndex.LookupKeys())
                    .Columns(lookupIndex.Columns())
                    .Settings(settings.BuildNode(ctx, node.Pos()))
                    .Done();
            }

            return Build<TKqlLookupTable>(ctx, node.Pos())
                .Table(BuildTableMeta(*implTable, node.Pos(), ctx))
                .LookupKeys(lookupIndex.LookupKeys())
                .Columns(lookupIndex.Columns())
                .Done();
        }

        auto keyColumnsList = BuildKeyColumnsList(tableDesc, node.Pos(), ctx);

        if (kqpCtx.Config->EnableKqpDataQueryStreamLookup) {
            TKqpStreamLookupSettings settings;
            settings.Strategy = EStreamLookupStrategyType::LookupRows;
            TExprBase lookupIndexTable = Build<TKqlStreamLookupTable>(ctx, node.Pos())
                .Table(BuildTableMeta(*implTable, node.Pos(), ctx))
                .LookupKeys(lookupIndex.LookupKeys())
                .Columns(keyColumnsList)
                .Settings(settings.BuildNode(ctx, node.Pos()))
                .Done();

            return Build<TKqlStreamLookupTable>(ctx, node.Pos())
                .Table(lookupIndex.Table())
                .LookupKeys(lookupIndexTable.Ptr())
                .Columns(lookupIndex.Columns())
                .Settings(settings.BuildNode(ctx, node.Pos()))
                .Done();
        }

        TExprBase lookupIndexTable = Build<TKqlLookupTable>(ctx, node.Pos())
            .Table(BuildTableMeta(*implTable, node.Pos(), ctx))
            .LookupKeys(lookupIndex.LookupKeys())
            .Columns(keyColumnsList)
            .Done();

        return Build<TKqlLookupTable>(ctx, node.Pos())
            .Table(lookupIndex.Table())
            .LookupKeys(lookupIndexTable.Ptr())
            .Columns(lookupIndex.Columns())
            .Done();
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

    TMaybeNode<TExprBase> lookupKeys;
    if (settings.Strategy == EStreamLookupStrategyType::LookupJoinRows || settings.Strategy == EStreamLookupStrategyType::LookupSemiJoinRows) {
        // Result type of lookupIndexTable: list<tuple<left_row, optional<main_table_pk>>>,
        // expected input type for main table stream join: list<tuple<optional<main_table_pk>, left_row>>,
        // so we should transform list<tuple<left_row, optional<main_table_pk>>> to list<tuple<optional<main_table_pk>, left_row>>
        lookupKeys = Build<TCoMap>(ctx, node.Pos())
            .Input(lookupIndexTable)
            .Lambda()
                .Args({"tuple"})
                .Body<TExprList>()
                    .Add<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("1").Build()
                        .Build()
                    .Add<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("0").Build()
                        .Build()
                    .Build()
                .Build()
            .Done();
    } else {
        lookupKeys = lookupIndexTable;
    }

    // We should allow lookup by null keys here,
    // because main table pk can contain nulls and we don't want to lose these rows
    settings.AllowNullKeysPrefixSize = keyColumnsList.Size();
    return Build<TKqlStreamLookupTable>(ctx, node.Pos())
        .Table(streamLookupIndex.Table())
        .LookupKeys(lookupKeys.Cast())
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

    auto readTableIndex = TReadMatch::Match(input);
    if (!readTableIndex)
        return node;

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
    const auto indexName = readTableIndex.Index().Value();
    auto [implTable, indexDesc] = tableDesc.Metadata->GetIndex(indexName);
    if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
        auto reject = [&] (std::string_view because) {
            auto message = TStringBuilder{} << "Given predicate is not suitable for used index: " 
                << indexName << ", because " << because << ", node dump:\n" << node.Ref().Dump();
            TIssue issue{ctx.GetPosition(readTableIndex.Pos()), message};
            SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, issue);
            ctx.AddWarning(issue);
            return node;
        };
        const auto* lambdaArgs = topBase.KeySelectorLambda().Args().Raw();
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
            return DoRewriteTopSortOverPrefixedKMeansTree(readTableIndex, maybeFlatMap.Cast(), *lambdaArgs, lambdaBody, topBase,
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
            lambdaArgs = maybeFlatMap.Cast().Lambda().Args().Raw();
        }
        return DoRewriteTopSortOverKMeansTree(readTableIndex, maybeFlatMap, *lambdaArgs, lambdaBody, topBase,
                                              ctx, kqpCtx, tableDesc, *indexDesc, *implTable);
    }
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
        kqpCtx.IsScanQuery(), extraColumns, filter);

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

    auto readTableIndex = TReadMatch::Match(input);
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

    return DoRewriteIndexRead(readTableIndex, ctx, tableDesc, implTable, kqpCtx.IsScanQuery(), extraColumns, filter);
}

} // namespace NKikimr::NKqp::NOpt
