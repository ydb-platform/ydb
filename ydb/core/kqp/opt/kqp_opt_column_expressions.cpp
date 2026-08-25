#include "kqp_opt_column_expressions.h"

#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <algorithm>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

TVector<TString> GetMissingStoredGeneratedDeps(const TVector<const TKikimrColumnMetadata*>& generatedColumns,
    const THashSet<TStringBuf>& inputColumnsSet)
{
    THashSet<TString> generatedNames;
    for (const auto* colMeta : generatedColumns) {
        generatedNames.insert(colMeta->Name);
    }

    THashSet<TString> missing;
    for (const auto* colMeta : generatedColumns) {
        for (const auto& dep : colMeta->DefaultExpression->Dependencies) {
            if (!inputColumnsSet.contains(dep) && !generatedNames.contains(dep)) {
                missing.insert(dep);
            }
        }
    }

    TVector<TString> result(missing.begin(), missing.end());
    std::ranges::sort(result);
    return result;
}

std::pair<TExprBase, TCoAtomList> BuildStoredGeneratedColumnsInline(const TExprBase& input, const TCoAtomList& inputColumns,
    const TVector<const TKikimrColumnMetadata*>& generatedColumns, const TKikimrTableDescription& table, TPositionHandle pos,
    TExprContext& ctx)
{
    THashSet<TString> generatedNames;
    THashSet<TStringBuf> inputColumnsSet;

    for (const auto* colMeta : generatedColumns) {
        generatedNames.insert(colMeta->Name);
    }
    for (const auto& col : inputColumns) {
        inputColumnsSet.insert(col.Value());
    }

    auto rowArg = Build<TCoArgument>(ctx, pos).Name("row").Done();
    TExprBase generatedInputRow = rowArg;

    auto absentDeps = GetMissingStoredGeneratedDeps(generatedColumns, inputColumnsSet);
    if (!absentDeps.empty()) {
        TVector<TExprBase> mergedMembers;
        for (const auto& col : inputColumns) {
            mergedMembers.push_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name(col)
                    .Value<TCoMember>()
                        .Struct(rowArg)
                        .Name().Build(col.Value())
                        .Build()
                    .Done());
        }

        for (const auto& dep : absentDeps) {
            const auto* columnType = table.GetColumnType(dep);
            YQL_ENSURE(columnType, "Unknown generated dependency column " << dep);

            const auto* optionalType = columnType->IsOptionalOrNull()
                ? columnType
                : ctx.MakeType<TOptionalExprType>(columnType);

            mergedMembers.push_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name().Build(dep)
                    .Value<TCoNothing>()
                        .OptionalType(NCommon::BuildTypeExpr(pos, *optionalType, ctx))
                        .Build()
                    .Done());
        }

        generatedInputRow = Build<TCoAsStruct>(ctx, pos).Add(mergedMembers).Done();
    }

    TVector<TCoAtom> allColumns;
    TVector<TExprBase> allMembers;

    for (const auto& col : inputColumns) {
        if (generatedNames.contains(TString(col.Value()))) {
            continue;
        }

        allColumns.push_back(col);
        allMembers.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(col)
                .Value<TCoMember>()
                    .Struct(rowArg)
                    .Name().Build(col.Value())
                    .Build()
                .Done());
    }

    auto [genColumnNames, genColumnMembers] = BuildGeneratedColumnMembers(generatedColumns, generatedInputRow, pos, ctx);
    for (auto& name : std::move(genColumnNames)) {
        allColumns.push_back(std::move(name));
    }
    for (auto& member : std::move(genColumnMembers)) {
        allMembers.push_back(std::move(member));
    }

    auto writeData = Build<TCoMap>(ctx, pos)
        .Input(input)
        .Lambda()
            .Args({rowArg})
            .Body<TCoAsStruct>()
                .Add(allMembers)
                .Build()
            .Build()
        .Done();

    auto columnList = Build<TCoAtomList>(ctx, pos)
        .Add(allColumns)
        .Done();

    return {writeData, columnList};
}

std::pair<TExprBase, TCoAtomList> BuildStoredGeneratedColumnsViaStreamLookup(const TExprBase& input, const TCoAtomList& inputColumns,
    const TVector<const TKikimrColumnMetadata*>& generatedColumns, const TVector<TString>& missingDeps, const TKikimrTableDescription& table,
    bool dropRowsAbsentFromTable, const THashSet<TStringBuf>& insertOnlyColumns, TPositionHandle pos, TExprContext& ctx)
{
    const auto& pk = table.Metadata->KeyColumnNames;

    THashSet<TString> generatedNames;
    for (const auto* colMeta : generatedColumns) {
        generatedNames.insert(colMeta->Name);
    }

    auto rowArg = Build<TCoArgument>(ctx, pos).Name("input_row").Done();

    TVector<TExprBase> keyMembers;
    keyMembers.reserve(pk.size());

    for (const auto& key : pk) {
        keyMembers.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(key)
                .Value<TCoMember>()
                    .Struct(rowArg)
                    .Name().Build(key)
                    .Build()
                .Done());
    }

    auto lookupKeys = Build<TCoMap>(ctx, pos)
        .Input(input)
        .Lambda()
            .Args({rowArg})
            .Body<TExprList>()
                .Add(rowArg)
                .Add<TCoJust>()
                    .Input<TCoAsStruct>()
                        .Add(keyMembers)
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();

    // Columns to fetch from the table: primary key + the missing dependency columns
    TVector<TCoAtom> lookupColumnNodes;
    lookupColumnNodes.reserve(pk.size() + missingDeps.size());

    THashSet<TStringBuf> seen;
    for (const auto& key : pk) {
        if (seen.insert(key).second) {
            lookupColumnNodes.push_back(TCoAtom(ctx.NewAtom(pos, key)));
        }
    }

    for (const auto& dep : missingDeps) {
        if (seen.insert(dep).second) {
            lookupColumnNodes.push_back(TCoAtom(ctx.NewAtom(pos, dep)));
        }
    }

    auto lookupColumns = Build<TCoAtomList>(ctx, pos).Add(lookupColumnNodes).Done();

    TKqpStreamLookupSettings lookupSettings;
    lookupSettings.Strategy = EStreamLookupStrategyType::LookupJoinRows;

    auto joined = Build<TKqlStreamLookupTable>(ctx, pos)
        .Table(BuildTableMeta(table, pos, ctx))
        .LookupKeys(lookupKeys)
        .Columns(lookupColumns)
        .Settings(lookupSettings.BuildNode(ctx, pos))
        .Done();

    auto joinArg = Build<TCoArgument>(ctx, pos).Name("joined_row").Done();
    auto leftRow = Build<TCoNth>(ctx, pos).Tuple(joinArg).Index().Build("0").Done();
    auto fetchedOpt = Build<TCoNth>(ctx, pos).Tuple(joinArg).Index().Build("1").Done();

    TVector<TExprBase> mergedMembers;
    THashSet<TStringBuf> missingDepsSet(missingDeps.begin(), missingDeps.end());
    for (const auto& col : inputColumns) {
        if (generatedNames.contains(TString(col.Value())) || missingDepsSet.contains(col.Value())) {
            continue;
        }

        mergedMembers.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(col)
                .Value<TCoMember>()
                    .Struct(leftRow)
                    .Name().Build(col.Value())
                    .Build()
                .Done());
    }

    auto presentRowArg = Build<TCoArgument>(ctx, pos).Name("present_row").Done();

    for (const auto& dep : missingDeps) {
        const auto* columnType = table.GetColumnType(dep);
        YQL_ENSURE(columnType, "Unknown generated dependency column " << dep);
        const bool hasInsertValue = insertOnlyColumns.contains(dep);

        TExprBase depValue = hasInsertValue
            ? TExprBase(Build<TCoMember>(ctx, pos)
                .Struct(leftRow)
                .Name().Build(dep)
                .Done())
            : TExprBase(Build<TCoMember>(ctx, pos)
                .Struct(presentRowArg)
                .Name().Build(dep)
                .Done());

        if (!dropRowsAbsentFromTable) {
            const auto* optionalType = columnType->IsOptionalOrNull()
                ? columnType
                : ctx.MakeType<TOptionalExprType>(columnType);

            auto fetchedArg = Build<TCoArgument>(ctx, pos).Name("fetched_row").Done();
            auto fetchedMember = Build<TCoMember>(ctx, pos)
                .Struct(fetchedArg)
                .Name().Build(dep)
                .Done();

            // If a missing table row has an insert-only value, both lookup branches have the
            // column's schema type. Do not make a NOT NULL dependency optional just to merge them
            TExprBase presentValue = columnType->IsOptionalOrNull() || hasInsertValue
                ? TExprBase(fetchedMember)
                : TExprBase(Build<TCoJust>(ctx, pos).Input(fetchedMember).Done());

            const TExprBase missingValue = [&]() -> TExprBase {
                if (hasInsertValue) {
                    return Build<TCoMember>(ctx, pos)
                        .Struct(leftRow)
                        .Name().Build(dep)
                        .Done();
                }
                return Build<TCoNothing>(ctx, pos)
                    .OptionalType(NCommon::BuildTypeExpr(pos, *optionalType, ctx))
                    .Done();
            }();

            depValue = Build<TCoIfPresent>(ctx, pos)
                .Optional(fetchedOpt)
                .PresentHandler<TCoLambda>()
                    .Args({fetchedArg})
                .Body(presentValue)
                .Build()
                .MissingValue(missingValue)
                    .Build()
                .Value();
        }

        mergedMembers.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(dep)
                .Value(depValue)
                .Done());
    }
    auto mergedRow = Build<TCoAsStruct>(ctx, pos).Add(mergedMembers).Done();

    TVector<TCoAtom> allColumns;
    TVector<TExprBase> allMembers;

    for (const auto& col : inputColumns) {
        if (generatedNames.contains(TString(col.Value()))) {
            continue;
        }

        allColumns.push_back(col);
        allMembers.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(col)
                .Value<TCoMember>()
                    .Struct(leftRow)
                    .Name().Build(col.Value())
                    .Build()
                .Done());
    }

    auto [genColumnNames, genColumnMembers] = BuildGeneratedColumnMembers(generatedColumns, mergedRow, pos, ctx);
    for (auto& name : std::move(genColumnNames)) {
        allColumns.push_back(std::move(name));
    }
    for (auto& member : std::move(genColumnMembers)) {
        allMembers.push_back(std::move(member));
    }

    auto outputRow = Build<TCoAsStruct>(ctx, pos).Add(allMembers).Done();

    const auto writeData = [&]() -> TExprBase {
        if (dropRowsAbsentFromTable) {
            return Build<TCoFlatMap>(ctx, pos)
                .Input(joined)
                .Lambda()
                    .Args({joinArg})
                    .Body<TCoFlatMap>()
                        .Input(fetchedOpt)
                        .Lambda()
                            .Args({presentRowArg})
                            .Body<TCoJust>()
                                .Input(outputRow)
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Done();
        }

        return Build<TCoMap>(ctx, pos)
            .Input(joined)
            .Lambda()
                .Args({joinArg})
                .Body(outputRow)
                .Build()
            .Done();
    }();

    auto columnList = Build<TCoAtomList>(ctx, pos)
        .Add(allColumns)
        .Done();

    return {writeData, columnList};
}

bool GeneratedDepsComeFromTable(TYdbOperation op) {
    return op == TYdbOperation::Upsert || op == TYdbOperation::UpdateOn;
}

bool WriteSkipsRowsAbsentFromTable(TYdbOperation op) {
    return op == TYdbOperation::UpdateOn;
}

}   // namespace

TVector<const TKikimrColumnMetadata*> CollectStoredGeneratedColumns(const TKikimrTableDescription& table) {
    TVector<const TKikimrColumnMetadata*> generatedColumns;
    for (const auto& name : table.Metadata->ColumnOrder) {
        const auto* colMeta = table.Metadata->Columns.FindPtr(name);
        if (colMeta && colMeta->IsDefaultFromExpression() && colMeta->DefaultExpression->IsGenerated()
            && colMeta->DefaultExpression->IsStored())
        {
            generatedColumns.push_back(colMeta);
        }
    }
    return generatedColumns;
}

TGeneratedColumnMembers BuildGeneratedColumnMembers(const TVector<const TKikimrColumnMetadata*>& generatedColumns,
    const TExprBase& depRow, TPositionHandle pos, TExprContext& ctx)
{
    TGeneratedColumnMembers result;
    result.Names.reserve(generatedColumns.size());
    result.Members.reserve(generatedColumns.size());

    for (const auto* colMeta : generatedColumns) {
        YQL_ENSURE(colMeta->DefaultExpression.Defined(), "STORED generated column " << colMeta->Name
            << " has no expression metadata");
        YQL_ENSURE(colMeta->DefaultExpression->Expr, "STORED generated column " << colMeta->Name
            << " has no compiled expression");

        auto value = Build<TExprApplier>(ctx, pos)
            .Apply(TCoLambda(colMeta->DefaultExpression->Expr))
            .With(0, depRow)
            .Done();

        auto nameAtom = TCoAtom(ctx.NewAtom(pos, colMeta->Name));
        result.Names.push_back(nameAtom);
        result.Members.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(nameAtom)
                .Value(value)
                .Done());
    }

    return result;
}

TExprBase BuildGeneratedDependencyRow(const TVector<const TKikimrColumnMetadata*>& generatedColumns,
    const TStructExprType& updateStructType, const TExprBase& updateStruct, const TExprBase& rowArg,
    const TPositionHandle pos, TExprContext& ctx)
{
    THashSet<TString> deps;
    for (const auto* colMeta : generatedColumns) {
        deps.insert(colMeta->DefaultExpression->Dependencies.begin(), colMeta->DefaultExpression->Dependencies.end());
    }

    TVector<TString> sortedDeps(deps.begin(), deps.end());
    std::ranges::sort(sortedDeps);

    TVector<TExprBase> members;
    members.reserve(sortedDeps.size());

    for (const auto& dep : sortedDeps) {
        TCoAtom depAtom(ctx.NewAtom(pos, dep));

        TExprBase valueSource = updateStructType.FindItem(dep)
            ? updateStruct
            : rowArg;

        members.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(depAtom)
                .Value<TCoMember>()
                    .Struct(valueSource)
                    .Name(depAtom)
                    .Build()
                .Done());
    }

    return Build<TCoAsStruct>(ctx, pos).Add(members).Done();
}

TBuildWriteInputResult ExtendInputRowsWithStoredGeneratedColumns(const TKiWriteTable& write, const TExprBase& input,
    const TCoAtomList& inputColumns, const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx, bool generatedLookup,
    const TCoAtomList* insertOnlyColumns)
{
    auto generatedColumns = CollectStoredGeneratedColumns(table);
    if (generatedColumns.empty()) {
        return { .Input=input, .Columns=inputColumns, .EmittedStreamLookup=false };
    }

    THashSet<TStringBuf> inputColumnsSet;
    for (const auto& col : inputColumns) {
        inputColumnsSet.insert(col.Value());
    }

    const auto tableOp = GetTableOp(write);
    const bool depsComeFromTable = GeneratedDepsComeFromTable(tableOp);

    if (tableOp == TYdbOperation::UpdateOn) {
        TVector<const TKikimrColumnMetadata*> touched;
        touched.reserve(generatedColumns.size());

        for (const auto* colMeta : generatedColumns) {
            const auto& deps = colMeta->DefaultExpression->Dependencies;
            if (std::ranges::any_of(deps, [&](const TString& dep) { return inputColumnsSet.contains(dep); })) {
                touched.push_back(colMeta);
            }
        }

        generatedColumns.swap(touched);
        if (generatedColumns.empty()) {
            return { .Input=input, .Columns=inputColumns, .EmittedStreamLookup=false };
        }
    }

    THashSet<TStringBuf> insertOnlyColumnsSet;
    if (insertOnlyColumns) {
        for (const auto& col : *insertOnlyColumns) {
            insertOnlyColumnsSet.insert(col.Value());
        }
    }

    if (generatedLookup && depsComeFromTable) {
        auto dependencyInputColumnsSet = inputColumnsSet;
        for (const auto& col : insertOnlyColumnsSet) {
            dependencyInputColumnsSet.erase(col);
        }
        auto missingDeps = GetMissingStoredGeneratedDeps(generatedColumns, dependencyInputColumnsSet);
        if (!missingDeps.empty()) {
            auto [rewritten, columns] = BuildStoredGeneratedColumnsViaStreamLookup(input, inputColumns, generatedColumns,
                missingDeps, table, WriteSkipsRowsAbsentFromTable(tableOp), insertOnlyColumnsSet, pos, ctx);
            return { .Input=rewritten, .Columns=columns, .EmittedStreamLookup=true };
        }
    }

    auto [rewritten, columns] = BuildStoredGeneratedColumnsInline(input, inputColumns, generatedColumns, table, pos, ctx);
    return { .Input=rewritten, .Columns=columns, .EmittedStreamLookup=false };
}

std::pair<TExprBase, TCoAtomList> ExtendInputRowsWithDefaultExpressionColumns(const TExprBase& input,
    const TCoAtomList& inputColumns, const TCoAtomList& defaultExprColumns, const TKikimrTableDescription& table,
    TPositionHandle pos, TExprContext& ctx)
{
    if (defaultExprColumns.Ref().ChildrenSize() == 0) {
        return {input, inputColumns};
    }

    auto rowArg = Build<TCoArgument>(ctx, pos).Name("row").Done();

    TVector<TCoAtom> allColumns;
    TVector<TExprBase> allMembers;

    for (const auto& col : inputColumns) {
        allColumns.push_back(col);
        allMembers.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(col)
                .Value<TCoMember>()
                    .Struct(rowArg)
                    .Name().Build(col.Value())
                    .Build()
                .Done());
    }

    // A DEFAULT expression cannot reference columns, so it is applied to an empty row
    auto emptyRow = Build<TCoAsStruct>(ctx, pos).Done();

    for (const auto& col : defaultExprColumns) {
        const TString colName(col.Value());
        const auto* colMeta = table.Metadata->Columns.FindPtr(colName);
        YQL_ENSURE(colMeta && colMeta->IsDefaultFromExpression(), "Column " << colName << " has no DEFAULT expression");
        YQL_ENSURE(colMeta->DefaultExpression->Expr, "DEFAULT expression of column " << colName << " is not compiled");

        allColumns.push_back(col);
        allMembers.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(col)
                .Value<TExprApplier>()
                    .Apply(TCoLambda(colMeta->DefaultExpression->Expr))
                    .With(0, emptyRow)
                    .Build()
                .Done());
    }

    auto writeData = Build<TCoMap>(ctx, pos)
        .Input(input)
        .Lambda()
            .Args({rowArg})
            .Body<TCoAsStruct>()
                .Add(allMembers)
                .Build()
            .Build()
        .Done();

    auto columnList = Build<TCoAtomList>(ctx, pos)
        .Add(allColumns)
        .Done();

    return {writeData, columnList};
}

}   // namespace NKikimr::NKqp::NOpt
