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

} // anonymous namespace

namespace NKikimr::NKqp::NLookupJoinBuilder {

TLookupKeysResult BuildLookupKeys(TOpTableLookup& lookup, TExprNode::TPtr inputStage, TExprContext& ctx) {
    Y_ENSURE(lookup.IsJoin(), "Lookup keys are only built for a table lookup in join mode");
    Y_ENSURE(lookup.LookupKeys.size() == lookup.LookupKeyColumns.size());

    auto& input = *lookup.GetInput();
    const auto pos = lookup.Pos;
    const auto row = Build<TCoArgument>(ctx, pos).Name("lookup_join_left_row").Done();

    // We want to split it into tuple(left row, lookup key).
    const auto& liveOut = GetLiveOut(&lookup);
    TVector<TExprBase> leftMembers;
    TVector<const TItemExprType*> leftItems;
    for (const auto& iu : input.GetOutputIUs()) {
        if (!liveOut.contains(iu)) {
            continue;
        }
        const auto* type = input.GetIUType(iu);
        Y_ENSURE(type, "Type of the lookup join input column " << iu.GetFullName() << " is not available");
        const auto name = iu.GetFullName();
        leftMembers.push_back(BuildMemberTuple(name, name, row, ctx, pos));
        leftItems.push_back(ctx.MakeType<TItemExprType>(name, type));
    }

    TVector<TExprBase> keyMembers;
    TVector<const TItemExprType*> keyItems;
    for (size_t i = 0; i < lookup.LookupKeys.size(); ++i) {
        const auto& key = lookup.LookupKeys[i];
        const auto& column = lookup.LookupKeyColumns[i];
        const auto* type = input.GetIUType(key);
        Y_ENSURE(type, "Type of the lookup join key " << key.GetFullName() << " is not available");
        keyMembers.push_back(BuildMemberTuple(column, key.GetFullName(), row, ctx, pos));
        keyItems.push_back(ctx.MakeType<TItemExprType>(column, type));
    }

    // Here is a tuple for the left side.
    // clang-format off
    const auto lambda = Build<TCoLambda>(ctx, pos)
        .Args({row})
        .Body<TExprList>()
            .Add<TCoAsStruct>()
                .Add(leftMembers)
            .Build()
            .Add<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(keyMembers)
                .Build()
            .Build()
        .Build()
    .Done();
    // clang-format on

    auto buildKeys = [&](TExprNode::TPtr body) {
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
        ctx.MakeType<TOptionalExprType>(ctx.MakeType<TStructExprType>(keyItems)),
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
                .Body<TCoOptionalIf>()
                    .Predicate<TCoCoalesce>()
                        .Predicate(lambda.Body())
                        .Value<TCoBool>()
                            .Literal().Build("false")
                        .Build()
                    .Build()
                    .Value(row)
                .Build()
            .Build()
        .Done();
        // clang-format on
    } else if (!needsRename) {
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
