#include "kqp_rbo_physical_lookup_join_builder.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace {

template <class TTransform>
TExprNode::TPtr TransformInputStage(TExprNode::TPtr input, TTransform&& transform, const IOperator& producer, TExprContext& ctx) {
    // Special case for row tables.
    if (TDqPhyStage::Match(input.Get())) {
        const auto program = TDqPhyStage(input).Program().Ptr();
        return ctx.ChangeChild(*input, TDqPhyStage::idx_Program, ctx.ChangeChild(*program, 1,
            TransformInputStage(program->TailPtr(), transform, producer, ctx)));
    }
    if (producer.Kind == EOperator::Replicate && input->IsCallable("Switch")) {
        Y_ENSURE(producer.Props.StageOutputIndex);
        // Switch(input, buffer, [input indexes], lambda, ...).
        const auto index = 3 + 2 * *producer.Props.StageOutputIndex;
        const auto branch = input->ChildPtr(index);
        return ctx.ChangeChild(*input, index, ctx.ChangeChild(*branch, 1, transform(branch->TailPtr())));
    }
    return transform(input);
}

TCoNameValueTuple BuildMemberTuple(const TString& name, const TString& sourceName, const TExprBase& row, TExprContext& ctx,
                                   TPositionHandle pos) {
    // clang-format off
    return Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(name)
        .Value<TCoMember>()
            .Struct(row)
            .Name().Build(sourceName)
        .Build()
    .Done();
    // clang-format on
}

TExprBase BuildOptionalIf(const TExprBase& predicate, const TExprBase& value, TExprContext& ctx, TPositionHandle pos) {
    // clang-format off
    return Build<TCoOptionalIf>(ctx, pos)
        .Predicate<TCoCoalesce>()
            .Predicate(predicate)
            .Value<TCoBool>()
                .Literal().Build("false")
            .Build()
        .Build()
        .Value(value)
    .Done();
    // clang-format on
}

} // anonymous namespace

namespace NKikimr::NKqp::NLookupJoinBuilder {

// Translate an upstream IU row to the storage lookup-key contract. Join mode
// additionally preserves the left payload and optional-prefix behavior.
TLookupKeysResult BuildLookupKeys(TOpTableLookup& lookup, TExprNode::TPtr inputStage, TExprContext& ctx, const TPhysicalNames& names) {
    auto& input = *lookup.GetInput();
    const auto pos = lookup.Pos;
    if (!lookup.IsJoin()) {
        TVector<std::pair<TString, TString>> columns;
        TVector<const TItemExprType*> types;
        for (const auto& [id, column] : lookup.LookupKeys.Items()) {
            columns.emplace_back(names.Get(id), column);
            types.push_back(ctx.MakeType<TItemExprType>(column, input.GetIUType(id, ctx)));
        }
        auto stage = TransformInputStage(inputStage, [&](TExprNode::TPtr body) {
            return NPhysicalConvertionUtils::BuildRenameMap(body, columns, ctx);
        }, input, ctx);
        auto type = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(types));
        return {std::move(stage), NYql::ExpandType(pos, *type, ctx)};
    }

    const auto row = Build<TCoArgument>(ctx, pos).Name("lookup_join_left_row").Done();

    const auto& liveOut = GetLiveOut(&lookup);
    TVector<TExprBase> leftMembers;
    TVector<const TItemExprType*> leftItems;
    THashSet<TString> addedNames;
    auto addLeftMember = [&](TInfoUnitId iu) {
        const auto name = names.Get(iu);
        if (!addedNames.insert(name).second) {
            return;
        }

        auto type = input.GetIUType(iu, ctx);
        Y_ENSURE(type, "Type of the lookup join input column " << names.Get(iu) << " is not available");
        leftMembers.push_back(BuildMemberTuple(name, name, row, ctx, pos));
        leftItems.push_back(ctx.MakeType<TItemExprType>(name, type));
    };

    for (const auto& iu : input.GetOutputIUs()) {
        if (liveOut.Contains(iu)) {
            addLeftMember(iu);
        }
    }

    for (const auto& [leftKey, rightKey, equalNulls] : lookup.ResidualJoinKeys.Items()) {
        Y_UNUSED(rightKey);
        addLeftMember(leftKey);
    }

    const auto point = Build<TCoArgument>(ctx, pos).Name("lookup_join_key_point").Done();
    TVector<TExprBase> keyMembers;
    TVector<const TItemExprType*> keyItems;
    TVector<TExprBase> equalities;
    if (lookup.Prefix) {
        Y_ENSURE(lookup.Prefix->PointsItemType);
        for (const auto& column : lookup.Prefix->Columns) {
            auto type = lookup.Prefix->PointsItemType->FindItemType(column);
            Y_ENSURE(type, "Type of the lookup key prefix column " << column << " is not available");
            keyMembers.push_back(BuildMemberTuple(column, column, point, ctx, pos));
            keyItems.push_back(ctx.MakeType<TItemExprType>(column, type));
        }

        // This is copy behavior of old optimizer but it looks non optimal.
        // Does we need to filter every left side prefix column with constant point?
        // We can just lookup by this constant point instead.
        // Keeping this for now, but it looks like we can optimize it out.
        for (const auto& [key, column] : lookup.Prefix->Equalities.Items()) {
            // clang-format off
            equalities.push_back(Build<TCoCmpEqual>(ctx, pos)
                .Left<TCoMember>()
                    .Struct(point)
                    .Name().Build(column)
                .Build()
                .Right<TCoMember>()
                    .Struct(row)
                    .Name().Build(names.Get(key))
                .Build()
            .Done());
            // clang-format on
        }
    }

    for (const auto& [key, column] : lookup.LookupKeys.Items()) {
        auto type = input.GetIUType(key, ctx);
        Y_ENSURE(type, "Type of the lookup join key " << names.Get(key) << " is not available");
        keyMembers.push_back(BuildMemberTuple(column, names.Get(key), row, ctx, pos));
        keyItems.push_back(ctx.MakeType<TItemExprType>(column, type));
    }

    const auto leftStruct = Build<TCoAsStruct>(ctx, pos).Add(leftMembers).Done();
    const auto keyStruct = Build<TCoAsStruct>(ctx, pos).Add(keyMembers).Done();
    auto keyType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TStructExprType>(keyItems));

    TExprNode::TPtr lambdaBody;
    if (!lookup.Prefix) {
        // clang-format off
        lambdaBody = Build<TExprList>(ctx, pos)
            .Add(leftStruct)
            .Add<TCoJust>()
                .Input(keyStruct)
            .Build()
        .Done().Ptr();
        // clang-format on
    } else {
        TExprBase maybeKey = Build<TCoJust>(ctx, pos).Input(keyStruct).Done();
        if (!equalities.empty()) {
            const auto predicate = equalities.size() == 1
                ? equalities.front()
                : TExprBase(Build<TCoAnd>(ctx, pos).Add(equalities).Done());
            maybeKey = BuildOptionalIf(predicate, keyStruct, ctx, pos);
        }

        // We have to check that ranges are not null. For example `where a is null`, where a is a pk, is also valid point predicate for us.
        // clang-format off
        lambdaBody = Build<TCoIf>(ctx, pos)
            .Predicate<TCoHasItems>()
                .List(lookup.Prefix->Points)
            .Build()
            .ThenValue<TCoMap>()
                .Input(lookup.Prefix->Points)
                .Lambda()
                    .Args({point})
                    .Body<TExprList>()
                        .Add(leftStruct)
                        .Add(maybeKey)
                    .Build()
                .Build()
            .Build()
            .ElseValue<TCoAsList>()
                .Add<TExprList>()
                    .Add(leftStruct)
                    .Add<TCoNothing>()
                        .OptionalType(NYql::ExpandType(pos, *keyType, ctx))
                    .Build()
                .Build()
            .Build()
        .Done().Ptr();
        // clang-format on
    }

    // Here is a tuple for the left side.
    // clang-format off
    const auto lambda = Build<TCoLambda>(ctx, pos)
        .Args({row})
        .Body(lambdaBody)
    .Done();
    // clang-format on

    auto buildKeys = [&](TExprNode::TPtr body) {
        if (lookup.Prefix) {
            // clang-format off
            return Build<TCoFlatMap>(ctx, pos)
                .Input(body)
                .Lambda(lambda)
            .Done().Ptr();
            // clang-format on
        }
        // clang-format off
        return Build<TCoMap>(ctx, pos)
            .Input(body)
            .Lambda(lambda)
        .Done().Ptr();
        // clang-format on
    };

    const auto newInputStage = TransformInputStage(inputStage, buildKeys, input, ctx);

    // Tuple: (left row, lookup key).
    const TTypeAnnotationNode::TListType tupleItems{
        ctx.MakeType<TStructExprType>(leftItems),
        keyType,
    };
    const auto* keysType = ctx.MakeType<TListExprType>(ctx.MakeType<TTupleExprType>(tupleItems));

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical lookup join keys] " << KqpExprToPrettyString(TExprBase(newInputStage), ctx);

    return {newInputStage, NYql::ExpandType(pos, *keysType, ctx)};
}

} // namespace NKikimr::NKqp::NLookupJoinBuilder


TExprNode::TPtr TPhysicalIndexLookupJoinBuilder::BuildRenamedRow(const TExprBase& fetchedRow, const TOpTableLookup& lookup,
                                                                bool& needsRename) const {
    const auto row = Build<TCoArgument>(Ctx, Pos).Name("lookup_join_right_row").Done();
    TVector<TExprBase> members;
    needsRename = false;
    for (const auto id : lookup.GetColumns()) {
        const auto column = Registry.Get(id).GetColumnName();
        const auto& name = Names.Get(id);
        needsRename = needsRename || name != column;
        members.push_back(BuildMemberTuple(name, column, row, Ctx, Pos));
    }

    if (!needsRename) {
        return fetchedRow.Ptr();
    }

    // clang-format off
    return Build<TCoMap>(Ctx, Pos)
        .Input(fetchedRow)
        .Lambda()
            .Args({row})
            .Body<TCoAsStruct>()
                .Add(members)
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TPhysicalIndexLookupJoinBuilder::ProcessFetchedRows(TExprNode::TPtr input, const TOpTableLookup& lookup) const {
    const auto pair = Build<TCoArgument>(Ctx, Pos).Name("lookup_join_pair").Done();
    // clang-format off
    const auto fetchedRow = Build<TCoNth>(Ctx, Pos)
        .Tuple(pair)
        .Index().Value("1").Build()
    .Done();
    // clang-format on

    bool needsRename = false;
    auto processedRow = TExprBase(BuildRenamedRow(fetchedRow, lookup, needsRename));

    if (lookup.FetchedRowFilter) {
        const auto row = Build<TCoArgument>(Ctx, Pos).Name("fetched_row").Done();
        TMappedIUs<TExprNode::TPtr> fields;
        for (const auto id : lookup.FetchedRowFilter->GetRawInputIUs()) {
            fields.Add(id, Build<TCoMember>(Ctx, Pos).Struct(row).Name().Build(Names.Get(id)).Done().Ptr());
        }
        const auto predicate = TExprBase(NPhysicalConvertionUtils::LowerRowLambdaBody(lookup.FetchedRowFilter->Node, fields, Ctx));
        // clang-format off
        processedRow = Build<TCoFlatMap>(Ctx, Pos)
            .Input(processedRow)
            .Lambda()
                .Args({row})
                .Body(BuildOptionalIf(predicate, row, Ctx, Pos))
            .Build()
        .Done();
        // clang-format on
    }

    if (!lookup.ResidualJoinKeys.Items().empty()) {
        // clang-format off
        const auto leftRow = Build<TCoNth>(Ctx, Pos)
            .Tuple(pair)
            .Index().Value("0").Build()
        .Done();
        // clang-format on

        const auto rightArg = Build<TCoArgument>(Ctx, Pos).Name("lookup_join_residual_right").Done();
        TVector<TExprBase> equalities;

        // The join keys which are not present in the right side index.
        // We have to evaluate them before apply index lookup join.
        for (const auto& [leftKey, rightKey, equalNulls] : lookup.ResidualJoinKeys.Items()) {
            // clang-format off
            equalities.push_back(Build<TCoCmpEqual>(Ctx, Pos)
                .Left<TCoMember>()
                    .Struct(leftRow)
                    .Name<TCoAtom>().Build(Names.Get(leftKey))
                    .Build()
                .Right<TCoMember>()
                    .Struct(rightArg)
                    .Name<TCoAtom>().Build(Names.Get(rightKey))
                    .Build()
            .Done());
            // clang-format on
        }

        // clang-format off
        const TExprBase pred = equalities.size() == 1
            ? equalities.front()
            : TExprBase(Build<TCoAnd>(Ctx, Pos).Add(equalities).Done());
        // clang-format on

        // clang-format off
        processedRow = Build<TCoFlatMap>(Ctx, Pos)
            .Input(processedRow)
            .Lambda()
                .Args({rightArg})
                .Body(BuildOptionalIf(pred, rightArg, Ctx, Pos))
            .Build()
        .Done();
        // clang-format on
    }

    if (!lookup.FetchedRowFilter && lookup.ResidualJoinKeys.Items().empty() && !needsRename) {
        return input;
    }

    // This is a tuple which represents input for index lookup join.
    // clang-format off
    return Build<TCoMap>(Ctx, Pos)
        .Input(input)
        .Lambda()
            .Args({pair})
            .Body<TExprList>()
                .Add<TCoNth>()
                    .Tuple(pair)
                    .Index().Value("0").Build()
                .Build()
                .Add(processedRow)
                .Add<TCoNth>()
                    .Tuple(pair)
                    .Index().Value("2").Build()
                .Build()
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TPhysicalIndexLookupJoinBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    const auto& lookup = LookupJoin.GetTableLookup();

    input = Build<TCoToStream>(Ctx, Pos).Input(input).Done().Ptr();
    input = ProcessFetchedRows(input, lookup);

    // clang-format off
    input = Build<TKqpIndexLookupJoin>(Ctx, Pos)
        .Input(input)
        .JoinType().Build(LookupJoin.JoinKind)
        // TODO: If needed we can also propagate labels.
        .LeftLabel().Build("")
        .RightLabel().Build("")
    .Done().Ptr();
    // clang-format on

    const auto liveOutputs = NPhysicalConvertionUtils::GetLiveOutputIUs(LookupJoin);
    if (liveOutputs.size() != LookupJoin.GetOutputIUs().Size()) {
        input = NPhysicalConvertionUtils::ExtractMembers(input, Ctx, liveOutputs, Names);
    }

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical index lookup join] " << KqpExprToPrettyString(TExprBase(input), Ctx);

    return input;
}
