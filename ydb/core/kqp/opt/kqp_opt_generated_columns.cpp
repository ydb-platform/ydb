#include "kqp_opt_generated_columns.h"

#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/generated_column.h>
#include <ydb/library/generated_columns/generated_column_dependencies.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <algorithm>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

TVector<TString> GeneratedColumnNames(const TVector<const TKikimrColumnMetadata*>& columns) {
    TVector<TString> names;
    for (const auto* column : columns) {
        names.push_back(column->Name);
    }
    return names;
}

} // namespace

TMaybe<TGeneratedColumnEvaluationPlan> BuildGeneratedColumnEvaluationPlan(
    const TKikimrTableDescription& table, const TVector<TString>& requestedColumns,
    const THashSet<TString>& storedColumnsToRecompute, TPositionHandle pos, TExprContext& ctx)
{
    const auto* graph = table.GetGeneratedColumnGraph(pos, ctx);
    if (!graph) {
        return {};
    }

    THashSet<TString> visited;
    THashSet<TString> scheduled;
    THashSet<TString> physical;
    TVector<TString> pending = requestedColumns;
    while (!pending.empty()) {
        auto name = std::move(pending.back());
        pending.pop_back();
        if (!visited.insert(name).second) {
            continue;
        }
        const auto* column = table.Metadata->Columns.FindPtr(name);
        if (!column) {
            // System columns are present in the loaded row type rather than schema metadata.
            if (!table.GetColumnType(name)) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unknown generated input column " << name));
                return {};
            }
            physical.insert(name);
        } else if (!IsGeneratedColumn(column)
            || (column->DefaultExpression->Stored && !storedColumnsToRecompute.contains(name)))
        {
            physical.insert(name);
        } else {
            scheduled.insert(name);
            const auto& dependencies = graph->Dependencies.at(name);
            pending.insert(pending.end(), dependencies.begin(), dependencies.end());
        }
    }

    TGeneratedColumnEvaluationPlan result;
    for (const auto& name : graph->EvaluationOrder) {
        if (scheduled.contains(name)) {
            result.EvaluationOrder.push_back(table.Metadata->Columns.FindPtr(name));
        }
    }
    result.PhysicalColumns.assign(physical.begin(), physical.end());
    std::ranges::sort(result.PhysicalColumns);
    return result;
}

namespace {

TVector<TString> GetMissingStoredGeneratedDeps(const TGeneratedColumnEvaluationPlan& plan,
    const THashSet<TStringBuf>& inputColumnsSet, const THashSet<TString>& generateColumnsIfInsert)
{
    TVector<TString> missing;
    for (const auto& name : plan.PhysicalColumns) {
        if (!inputColumnsSet.contains(name) || generateColumnsIfInsert.contains(name)) {
            missing.push_back(name);
        }
    }
    return missing;
}

TMaybe<std::pair<TExprBase, TCoAtomList>> BuildStoredGeneratedColumnsInline(const TExprBase& input, const TCoAtomList& inputColumns,
    const TGeneratedColumnEvaluationPlan& plan, const TVector<TString>& outputColumns,
    const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx)
{
    THashSet<TString> generatedNames;
    THashSet<TStringBuf> inputColumnsSet;
    generatedNames.insert(outputColumns.begin(), outputColumns.end());
    for (const auto& col : inputColumns) {
        inputColumnsSet.insert(col.Value());
    }

    auto rowArg = Build<TCoArgument>(ctx, pos).Name("row").Done();
    TExprBase generatedInputRow = rowArg;

    const THashSet<TString> generateColumnsIfInsert;
    auto absentDeps = GetMissingStoredGeneratedDeps(plan, inputColumnsSet, generateColumnsIfInsert);
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
            if (!columnType) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unknown generated dependency column " << dep));
                return {};
            }

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

    auto generated = BuildGeneratedColumnMembers(plan, outputColumns, generatedInputRow, table, pos, ctx);
    if (!generated) {
        return {};
    }
    auto& [genColumnNames, genColumnMembers] = *generated;
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

    return std::pair{writeData, columnList};
}

TMaybe<std::pair<TExprBase, TCoAtomList>> BuildStoredGeneratedColumnsViaStreamLookup(const TExprBase& input, const TCoAtomList& inputColumns,
    const TGeneratedColumnEvaluationPlan& plan, const TVector<TString>& outputColumns, const TVector<TString>& missingDeps,
    const THashSet<TString>& generateColumnsIfInsert, const TKikimrTableDescription& table, bool dropRowsAbsentFromTable,
    TPositionHandle pos, TExprContext& ctx)
{
    const auto& pk = table.Metadata->KeyColumnNames;

    THashSet<TString> generatedNames;
    THashSet<TStringBuf> inputColumnsSet;

    generatedNames.insert(outputColumns.begin(), outputColumns.end());

    for (const auto& col : inputColumns) {
        inputColumnsSet.insert(col.Value());
    }

    THashSet<TStringBuf> missingDepsSet;
    for (const auto& dep : missingDeps) {
        missingDepsSet.insert(dep);
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
        if (!columnType) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unknown generated dependency column " << dep));
            return {};
        }

        TExprBase depValue = Build<TCoMember>(ctx, pos)
            .Struct(presentRowArg)
            .Name().Build(dep)
            .Done();

        if (!dropRowsAbsentFromTable) {
            auto fetchedArg = Build<TCoArgument>(ctx, pos).Name("fetched_row").Done();
            auto fetchedMember = Build<TCoMember>(ctx, pos)
                .Struct(fetchedArg)
                .Name().Build(dep)
                .Done();

            if (generateColumnsIfInsert.contains(dep) && inputColumnsSet.contains(dep)) {
                auto inputMember = Build<TCoMember>(ctx, pos)
                    .Struct(leftRow)
                    .Name().Build(dep)
                    .Done();

                depValue = Build<TCoIfPresent>(ctx, pos)
                    .Optional(fetchedOpt)
                    .PresentHandler<TCoLambda>()
                        .Args({fetchedArg})
                        .Body(fetchedMember)
                        .Build()
                    .MissingValue(inputMember)
                    .Done();
            } else {
                const auto* optionalType = columnType->IsOptionalOrNull() ? columnType : ctx.MakeType<TOptionalExprType>(columnType);
                TExprBase presentValue = columnType->IsOptionalOrNull()
                    ? TExprBase(fetchedMember)
                    : TExprBase(Build<TCoJust>(ctx, pos).Input(fetchedMember).Done());

                depValue = Build<TCoIfPresent>(ctx, pos)
                    .Optional(fetchedOpt)
                    .PresentHandler<TCoLambda>()
                        .Args({fetchedArg})
                        .Body(presentValue)
                        .Build()
                    .MissingValue<TCoNothing>()
                        .OptionalType(NCommon::BuildTypeExpr(pos, *optionalType, ctx))
                        .Build()
                    .Done();
            }
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

    auto generated = BuildGeneratedColumnMembers(plan, outputColumns, mergedRow, table, pos, ctx);
    if (!generated) {
        return {};
    }
    auto& [genColumnNames, genColumnMembers] = *generated;
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

    return std::pair{writeData, columnList};
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
        if (IsStoredGeneratedColumn(colMeta)) {
            generatedColumns.push_back(colMeta);
        }
    }
    return generatedColumns;
}

TMaybe<TVector<const TKikimrColumnMetadata*>> CollectAffectedStoredGeneratedColumns(
    const TKikimrTableDescription& table, const THashSet<TString>& changedColumns,
    TPositionHandle pos, TExprContext& ctx)
{
    const auto* graph = table.GetGeneratedColumnGraph(pos, ctx);
    if (!graph) {
        return {};
    }
    const auto affected = NGeneratedColumns::CollectAffectedGeneratedColumns(*graph, changedColumns);
    TVector<const TKikimrColumnMetadata*> result;
    for (const auto* column : CollectStoredGeneratedColumns(table)) {
        if (affected.contains(column->Name)) {
            result.push_back(column);
        }
    }
    return result;
}

TMaybe<TGeneratedColumnMembers> BuildGeneratedColumnMembers(const TGeneratedColumnEvaluationPlan& plan,
    const TVector<TString>& outputGeneratedColumns, const TExprBase& depRow,
    const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx)
{
    THashMap<TString, TExprNode::TPtr> values;
    for (const auto* column : plan.EvaluationOrder) {
        if (!column->DefaultExpression || !column->DefaultExpression->Expr) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Generated column " << column->Name << " has no compiled expression"));
            return {};
        }
        const auto* type = table.GetColumnType(column->Name);
        if (!type) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Unknown generated column " << column->Name));
            return {};
        }

        // Reuse the computed value nodes in every consumer, including the output.
        // Only direct dependencies are exposed, so old generated placeholders cannot shadow them.
        TVector<TExprBase> dependencies;
        THashSet<TString> seen;
        for (const auto& name : column->DefaultExpression->Dependencies) {
            if (!seen.insert(name).second) {
                continue;
            }
            const auto value = values.find(name);
            dependencies.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(name)
                .Value(value != values.end()
                    ? TExprBase(value->second)
                    : TExprBase(Build<TCoMember>(ctx, pos).Struct(depRow).Name().Build(name).Done()))
                .Done());
        }
        const auto dependencyRow = Build<TCoAsStruct>(ctx, pos).Add(dependencies).Done();
        const auto value = Build<TExprApplier>(ctx, pos)
            .Apply(TCoLambda(column->DefaultExpression->Expr))
            .With(0, dependencyRow)
            .Done();
        const auto normalized = Build<TCoStrictCast>(ctx, pos)
            .Value(value)
            .Type(NCommon::BuildTypeExpr(pos, *type, ctx))
            .Done();
        values.emplace(column->Name, normalized.Ptr());
    }

    TGeneratedColumnMembers result;
    for (const auto& name : outputGeneratedColumns) {
        const auto value = values.find(name);
        if (value == values.end()) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Generated column " << name << " is absent from the evaluation plan"));
            return {};
        }
        auto atom = TCoAtom(ctx.NewAtom(pos, name));
        result.Names.push_back(atom);
        result.Members.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name(atom).Value(TExprBase(value->second)).Done());
    }
    return result;
}

TMaybe<TCoAtomList> BuildPhysicalColumnsForVirtualGeneratedColumns(
    const TCoAtomList& logicalColumns, const TKikimrTableDescription& table,
    TPositionHandle pos, TExprContext& ctx)
{
    if (!table.GetGeneratedColumnGraph(pos, ctx)) {
        return {};
    }
    TVector<TString> requested;
    bool hasVirtual = false;
    for (const auto& column : logicalColumns) {
        requested.emplace_back(column.Value());
        hasVirtual |= IsVirtualGeneratedColumn(table.Metadata->Columns.FindPtr(column.Value()));
    }
    if (!hasVirtual) {
        return logicalColumns;
    }
    auto plan = BuildGeneratedColumnEvaluationPlan(table, requested, {}, pos, ctx);
    if (!plan) {
        return {};
    }
    if (plan->PhysicalColumns.empty()) {
        if (table.Metadata->KeyColumnNames.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "A constant generated projection requires a physical primary key"));
            return {};
        }
        plan->PhysicalColumns.push_back(table.Metadata->KeyColumnNames.front());
    }
    TVector<TCoAtom> physical;
    for (const auto& name : plan->PhysicalColumns) {
        physical.emplace_back(ctx.NewAtom(pos, name));
    }
    return Build<TCoAtomList>(ctx, pos).Add(physical).Done();
}

TMaybe<TExprBase> BuildVirtualGeneratedColumnProjection(
    const TExprBase& physicalRows, const TCoAtomList& logicalColumns,
    const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx)
{
    if (!table.GetGeneratedColumnGraph(pos, ctx)) {
        return {};
    }
    TVector<TString> requested;
    TVector<TString> virtualNames;
    for (const auto& column : logicalColumns) {
        requested.emplace_back(column.Value());
        if (IsVirtualGeneratedColumn(table.Metadata->Columns.FindPtr(column.Value()))) {
            virtualNames.emplace_back(column.Value());
        }
    }
    if (virtualNames.empty()) {
        return physicalRows;
    }
    const auto plan = BuildGeneratedColumnEvaluationPlan(table, requested, {}, pos, ctx);
    if (!plan) {
        return {};
    }
    auto row = Build<TCoArgument>(ctx, pos).Name("row").Done();
    const auto generated = BuildGeneratedColumnMembers(*plan, virtualNames, row, table, pos, ctx);
    if (!generated) {
        return {};
    }
    THashMap<TStringBuf, TExprBase> generatedMembers;
    for (const auto& member : generated->Members) {
        generatedMembers.emplace(member.Cast<TCoNameValueTuple>().Name().Value(), member);
    }
    TVector<TExprBase> members;
    for (const auto& column : logicalColumns) {
        const auto generatedMember = generatedMembers.find(column.Value());
        members.push_back(generatedMember != generatedMembers.end()
            ? generatedMember->second
            : TExprBase(Build<TCoNameValueTuple>(ctx, pos)
                .Name(column)
                .Value<TCoMember>().Struct(row).Name(column).Build()
                .Done()));
    }
    return Build<TCoMap>(ctx, pos)
        .Input(physicalRows)
        .Lambda().Args({row}).Body<TCoAsStruct>().Add(members).Build().Build()
        .Done();
}

TMaybe<TExprBase> BuildReadWithVirtualGeneratedColumns(
    const TCoAtomList& logicalColumns,
    const TKikimrTableDescription& table,
    TPositionHandle pos,
    TExprContext& ctx,
    const std::function<TExprBase(const TCoAtomList&)>& buildRead)
{
    const auto physicalColumns = BuildPhysicalColumnsForVirtualGeneratedColumns(logicalColumns, table, pos, ctx);
    if (!physicalColumns) {
        return {};
    }
    return BuildVirtualGeneratedColumnProjection(buildRead(*physicalColumns), logicalColumns, table, pos, ctx);
}

TExprBase BuildGeneratedDependencyRow(const TGeneratedColumnEvaluationPlan& plan,
    const TStructExprType& updateStructType, const TExprBase& updateStruct, const TExprBase& rowArg,
    const TPositionHandle pos, TExprContext& ctx)
{
    TVector<TExprBase> members;
    members.reserve(plan.PhysicalColumns.size());

    for (const auto& dep : plan.PhysicalColumns) {
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

TMaybe<TBuildWriteInputResult> ExtendInputRowsWithStoredGeneratedColumns(const TKiWriteTable& write, const TExprBase& input,
    const TCoAtomList& inputColumns, const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx, bool generatedLookup)
{
    if (!table.GetGeneratedColumnGraph(pos, ctx)) {
        return {};
    }
    auto generatedColumns = CollectStoredGeneratedColumns(table);
    if (generatedColumns.empty()) {
        return TBuildWriteInputResult{ .Input=input, .Columns=inputColumns, .EmittedStreamLookup=false };
    }

    THashSet<TStringBuf> inputColumnsSet;
    for (const auto& col : inputColumns) {
        inputColumnsSet.insert(col.Value());
    }

    const auto tableOp = GetTableOp(write);
    const bool depsComeFromTable = GeneratedDepsComeFromTable(tableOp);

    THashSet<TString> generateColumnsIfInsert;
    if (tableOp == TYdbOperation::Upsert) {
        if (const auto setting = GetSetting(write.Settings().Ref(), "generate_columns_if_insert")) {
            for (const auto& column : TCoNameValueTuple(setting).Value().Cast<TCoAtomList>()) {
                generateColumnsIfInsert.insert(TString(column.Value()));
            }
        }
    }

    if (tableOp == TYdbOperation::UpdateOn) {
        THashSet<TString> changedColumns;
        for (const auto& column : inputColumns) {
            if (!table.GetKeyColumnIndex(TString(column.Value()))) {
                changedColumns.emplace(column.Value());
            }
        }
        auto affected = CollectAffectedStoredGeneratedColumns(table, changedColumns, pos, ctx);
        if (!affected) {
            return {};
        }
        generatedColumns = std::move(*affected);
        if (generatedColumns.empty()) {
            return TBuildWriteInputResult{ .Input=input, .Columns=inputColumns, .EmittedStreamLookup=false };
        }
    }

    const auto outputColumns = GeneratedColumnNames(generatedColumns);
    const auto plan = BuildGeneratedColumnEvaluationPlan(table, outputColumns,
        THashSet<TString>(outputColumns.begin(), outputColumns.end()), pos, ctx);
    if (!plan) {
        return {};
    }

    if (generatedLookup && depsComeFromTable) {
        auto missingDeps = GetMissingStoredGeneratedDeps(*plan, inputColumnsSet, generateColumnsIfInsert);
        if (!missingDeps.empty()) {
            auto rewritten = BuildStoredGeneratedColumnsViaStreamLookup(input, inputColumns, *plan, outputColumns,
                missingDeps, generateColumnsIfInsert, table, WriteSkipsRowsAbsentFromTable(tableOp), pos, ctx);
            if (!rewritten) {
                return {};
            }
            return TBuildWriteInputResult{ .Input=rewritten->first, .Columns=rewritten->second, .EmittedStreamLookup=true };
        }
    }

    auto rewritten = BuildStoredGeneratedColumnsInline(input, inputColumns, *plan, outputColumns, table, pos, ctx);
    if (!rewritten) {
        return {};
    }
    return TBuildWriteInputResult{ .Input=rewritten->first, .Columns=rewritten->second, .EmittedStreamLookup=false };
}

}   // namespace NKikimr::NKqp::NOpt
