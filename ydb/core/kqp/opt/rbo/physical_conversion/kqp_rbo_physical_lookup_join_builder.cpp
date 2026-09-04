#include "kqp_rbo_physical_lookup_join_builder.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace {

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
    const auto item = ctx.NewCallable(pos, "Just", {value.Ptr()});
    return TExprBase(ctx.Builder(pos)
        .Callable("If")
            .Callable(0, "Coalesce")
                .Add(0, predicate.Ptr())
                .Callable(1, "Bool")
                    .Atom(0, "false")
                .Seal()
            .Seal()
            .Add(1, item)
            .Callable(2, "EmptyFrom")
                .Add(0, item)
            .Seal()
        .Seal().Build());
}

} // anonymous namespace

namespace NKikimr::NKqp::NLookupJoinBuilder {

// This function builds a lookup keys, to lookup to the right side of the join.
TLookupKeysResult BuildLookupKeys(TOpTableLookup& lookup, TExprNode::TPtr inputStage, TExprContext& ctx) {
    Y_ENSURE(lookup.IsJoin(), "Lookup keys are only built for a table lookup in join mode");
    Y_ENSURE(lookup.LookupKeys.size() == lookup.LookupKeyColumns.size());

    auto& input = *lookup.GetInput();
    const auto pos = lookup.Pos;
    const auto row = Build<TCoArgument>(ctx, pos).Name("lookup_join_left_row").Done();

    const auto& liveOut = GetLiveOut(&lookup);
    TVector<TExprBase> leftMembers;
    TVector<const TItemExprType*> leftItems;
    THashSet<TString> addedNames;
    auto addLeftMember = [&](const TInfoUnit& iu) {
        const auto name = iu.GetFullName();
        if (!addedNames.insert(name).second) {
            return;
        }

        auto type = input.GetIUType(iu);
        Y_ENSURE(type, "Type of the lookup join input column " << iu.GetFullName() << " is not available");
        leftMembers.push_back(BuildMemberTuple(name, name, row, ctx, pos));
        leftItems.push_back(ctx.MakeType<TItemExprType>(name, type));
    };

    for (const auto& iu : input.GetOutputIUs()) {
        if (liveOut.contains(iu)) {
            addLeftMember(iu);
        }
    }

    for (const auto& [leftKey, rightKey] : lookup.ResidualJoinKeys) {
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
        for (const auto& [column, key] : lookup.Prefix->Equalities) {
            // clang-format off
            equalities.push_back(Build<TCoCmpEqual>(ctx, pos)
                .Left<TCoMember>()
                    .Struct(point)
                    .Name().Build(column)
                .Build()
                .Right<TCoMember>()
                    .Struct(row)
                    .Name().Build(key.GetFullName())
                .Build()
            .Done());
            // clang-format on
        }
    }

    for (size_t i = 0; i < lookup.LookupKeys.size(); ++i) {
        const auto& key = lookup.LookupKeys[i];
        const auto& column = lookup.LookupKeyColumns[i];
        auto type = input.GetIUType(key);
        Y_ENSURE(type, "Type of the lookup join key " << key.GetFullName() << " is not available");
        keyMembers.push_back(BuildMemberTuple(column, key.GetFullName(), row, ctx, pos));
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

    TExprNode::TPtr newInputStage;
    // Special case for row tables.
    if (TDqPhyStage::Match(inputStage.Get())) {
        const auto stage = TDqPhyStage(inputStage);
        // clang-format off
        newInputStage = Build<TDqPhyStage>(ctx, inputStage->Pos())
            .InitFrom(stage)
            .Program<TCoLambda>()
                .Args(stage.Program().Args())
                .Body(buildKeys(stage.Program().Body().Ptr()))
            .Build()
        .Done().Ptr();
        // clang-format on
    } else {
        newInputStage = buildKeys(inputStage);
    }

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
    Y_ENSURE(lookup.FetchColumns.size() == lookup.OutputIUs.size());

    const auto row = Build<TCoArgument>(Ctx, Pos).Name("lookup_join_right_row").Done();
    TVector<TExprBase> members;
    needsRename = false;
    for (size_t i = 0; i < lookup.FetchColumns.size(); ++i) {
        const auto& column = lookup.FetchColumns[i];
        const auto name = lookup.OutputIUs[i].GetFullName();
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
        const auto lambda = TCoLambda(Ctx.DeepCopyLambda(*lookup.FetchedRowFilter->GetLambda()));
        const auto row = lambda.Args().Arg(0);
        // clang-format off
        processedRow = Build<TCoFlatMap>(Ctx, Pos)
            .Input(processedRow)
            .Lambda()
                .Args({row})
                .Body(BuildOptionalIf(lambda.Body(), row, Ctx, Pos))
            .Build()
        .Done();
        // clang-format on
    }

    if (!lookup.ResidualJoinKeys.empty()) {
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
        for (const auto& [leftKey, rightKey] : lookup.ResidualJoinKeys) {
            // clang-format off
            equalities.push_back(Build<TCoCmpEqual>(Ctx, Pos)
                .Left<TCoMember>()
                    .Struct(leftRow)
                    .Name<TCoAtom>().Build(leftKey.GetFullName())
                    .Build()
                .Right<TCoMember>()
                    .Struct(rightArg)
                    .Name<TCoAtom>().Build(rightKey.GetFullName())
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

    if (!lookup.FetchedRowFilter && lookup.ResidualJoinKeys.empty() && !needsRename) {
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
    const auto lookup = LookupJoin->GetTableLookup();

    input = Build<TCoToStream>(Ctx, Pos).Input(input).Done().Ptr();
    input = ProcessFetchedRows(input, *lookup);

    // clang-format off
    input = Build<TKqpIndexLookupJoin>(Ctx, Pos)
        .Input(input)
        .JoinType().Build(LookupJoin->JoinKind)
        // TODO: If needed we can also propagate labels.
        .LeftLabel().Build("")
        .RightLabel().Build("")
    .Done().Ptr();
    // clang-format on

    const auto liveOutputs = NPhysicalConvertionUtils::GetLiveOutputIUs(*LookupJoin);
    if (liveOutputs.size() != LookupJoin->GetOutputIUs().size()) {
        input = NPhysicalConvertionUtils::ExtractMembers(input, Ctx, liveOutputs);
    }

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical index lookup join] " << KqpExprToPrettyString(TExprBase(input), Ctx);

    return input;
}
