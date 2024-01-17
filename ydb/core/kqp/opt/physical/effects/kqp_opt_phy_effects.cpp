#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TExprBase ExtractKeys(TCoArgument itemArg, const TKikimrTableDescription& tableDesc,
    TExprContext& ctx)
{
    TVector<TExprBase> keys;
    for (TString& keyColumnName : tableDesc.Metadata->KeyColumnNames) {
        auto key = Build<TCoMember>(ctx, itemArg.Pos())
            .Struct(itemArg)
            .Name().Build(keyColumnName)
            .Done();

        keys.emplace_back(std::move(key));
    }

    if (keys.size() == 1) {
        return keys[0];
    }

    return Build<TExprList>(ctx, itemArg.Pos())
        .Add(keys)
        .Done();
}

TExprBase RemoveDuplicateKeyFromInput(const TExprBase& input, const TKikimrTableDescription& tableDesc,
    TPositionHandle pos, TExprContext& ctx)
{
    const auto& keySelectorArg = Build<TCoArgument>(ctx, pos)
        .Name("item")
        .Done();

    const auto& streamArg = Build<TCoArgument>(ctx, pos)
        .Name("streamArg")
        .Done();

    return Build<TCoPartitionByKey>(ctx, pos)
        .Input(input)
        .KeySelectorLambda()
            .Args(keySelectorArg)
            .Body(ExtractKeys(keySelectorArg, tableDesc, ctx))
            .Build()
        .SortDirections<TCoVoid>().Build()
        .SortKeySelectorLambda<TCoVoid>().Build()
        .ListHandlerLambda()
            .Args({TStringBuf("stream")})
            .Body<TCoFlatMap>()
                .Input(TStringBuf("stream"))
                .Lambda<TCoLambda>()
                    .Args(streamArg)
                    .Body<TCoLast>()
                        .Input<TCoForwardList>()
                            .Stream<TCoNth>()
                                .Tuple(streamArg)
                                .Index().Value(ToString(1))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Build()
        .Done();
}

} // namespace

TMaybe<TCondenseInputResult> CondenseInput(const TExprBase& input, TExprContext& ctx) {
    TVector<TExprBase> stageInputs;
    TVector<TCoArgument> stageArguments;

    if (IsDqPureExpr(input)) {
        auto stream = Build<TCoToStream>(ctx, input.Pos())
            .Input<TCoJust>()
                .Input(input)
                .Build()
            .Done();

        return TCondenseInputResult { .Stream = stream };
    }

    if (!input.Maybe<TDqCnUnionAll>()) {
        return {};
    }

    auto arg = Build<TCoArgument>(ctx, input.Pos())
        .Name("input_rows")
        .Done();

    stageInputs.push_back(input);
    stageArguments.push_back(arg);

    auto squeeze = Build<TCoSqueezeToList>(ctx, input.Pos())
        .Stream(arg)
        .Done();

    return TCondenseInputResult {
        .Stream = squeeze,
        .StageInputs = stageInputs,
        .StageArgs = stageArguments
    };
}

TCondenseInputResult DeduplicateInput(const TCondenseInputResult& condenseResult, const TKikimrTableDescription& table,
    TExprContext& ctx)
{
    auto pos = condenseResult.Stream.Pos();
    auto listArg = TCoArgument(ctx.NewArgument(pos, "list_arg"));

    auto deduplicated = Build<TCoFlatMap>(ctx, pos)
        .Input(condenseResult.Stream)
        .Lambda()
            .Args({listArg})
            .Body<TCoJust>()
                .Input(RemoveDuplicateKeyFromInput(listArg, table, pos, ctx))
                .Build()
            .Build()
        .Done();

    return TCondenseInputResult {
        .Stream = deduplicated,
        .StageInputs = condenseResult.StageInputs,
        .StageArgs = condenseResult.StageArgs
    };
}

TMaybe<TCondenseInputResult> CondenseInputToDictByPk(const TExprBase& input, const TKikimrTableDescription& table,
    const TCoLambda& payloadSelector, TExprContext& ctx)
{
    auto condenseResult = CondenseInput(input, ctx);
    if (!condenseResult) {
        return {};
    }

    auto dictStream = Build<TCoMap>(ctx, input.Pos())
        .Input(condenseResult->Stream)
        .Lambda()
            .Args({"row_list"})
            .Body<TCoToDict>()
                .List("row_list")
                .KeySelector(MakeTableKeySelector(table.Metadata, input.Pos(), ctx))
                .PayloadSelector(payloadSelector)
                .Settings()
                    .Add().Build("One")
                    .Add().Build("Hashed")
                    .Build()
                .Build()
            .Build()
        .Done();

    return TCondenseInputResult {
        .Stream = dictStream,
        .StageInputs = condenseResult->StageInputs,
        .StageArgs = condenseResult->StageArgs
    };
}

TCoLambda MakeTableKeySelector(const TKikimrTableMetadataPtr meta, TPositionHandle pos, TExprContext& ctx, TMaybe<int> tupleId) {
    auto keySelectorArg = TCoArgument(ctx.NewArgument(pos, "key_selector"));

    TVector<TExprBase> keyTuples;
    keyTuples.reserve(meta->KeyColumnNames.size());

    TExprBase selector = keySelectorArg;
    if (tupleId) {
        selector = Build<TCoNth>(ctx, pos)
            .Tuple(keySelectorArg)
            .Index().Build(*tupleId)
            .Done();
    }

    for (const auto& key : meta->KeyColumnNames) {
        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(key)
            .Value<TCoMember>()
                .Struct(selector)
                .Name().Build(key)
                .Build()
            .Done();

        keyTuples.emplace_back(tuple);
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({keySelectorArg})
        .Body<TCoAsStruct>()
            .Add(keyTuples)
            .Build()
        .Done();
}

NYql::NNodes::TCoLambda MakeIndexPrefixKeySelector(const NYql::TIndexDescription& indexDesc, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx)
{
    auto keySelectorArg = TCoArgument(ctx.NewArgument(pos, "fk_selector"));

    TVector<TExprBase> keyTuples;
    keyTuples.reserve(indexDesc.KeyColumns.size());
    for (const auto& key : indexDesc.KeyColumns) {
        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(key)
            .Value<TCoMember>()
                .Struct(keySelectorArg)
                .Name().Build(key)
                .Build()
            .Done();

        keyTuples.emplace_back(tuple);
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({keySelectorArg})
        .Body<TCoAsStruct>()
            .Add(keyTuples)
            .Build()
        .Done();
}

TCoLambda MakeRowsPayloadSelector(const TCoAtomList& columns, const TKikimrTableDescription& table,
    TPositionHandle pos, TExprContext& ctx)
{
    for (const auto& key : table.Metadata->KeyColumnNames) {
        auto it = std::find_if(columns.begin(), columns.end(), [&key](const auto& x) { return x.Value() == key; });
        YQL_ENSURE(it != columns.end(), "Key column not found in columns list: " << key);
    }

    auto payloadSelectorArg = TCoArgument(ctx.NewArgument(pos, "payload_selector_row"));
    TVector<TExprBase> payloadTuples;
    payloadTuples.reserve(columns.Size() - table.Metadata->KeyColumnNames.size());
    for (const auto& column : columns) {
        if (table.GetKeyColumnIndex(TString(column))) {
            continue;
        }

        payloadTuples.emplace_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name(column)
                .Value<TCoMember>()
                    .Struct(payloadSelectorArg)
                    .Name(column)
                    .Build()
                .Done());
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({payloadSelectorArg})
        .Body<TCoAsStruct>()
            .Add(payloadTuples)
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
