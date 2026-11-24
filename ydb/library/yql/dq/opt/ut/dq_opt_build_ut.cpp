#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/core/yql_arrow_resolver.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {
namespace {

// Channel construction only needs to check whether the column types support blocks.
class TArrowResolver: public IArrowResolver {
public:
    EStatus LoadFunctionMetadata(const TPosition&, TStringBuf, const TVector<const TTypeAnnotationNode*>&,
        const TTypeAnnotationNode*, TExprContext&) const override {
        return ERROR;
    }

    EStatus HasCast(const TPosition&, const TTypeAnnotationNode*, const TTypeAnnotationNode*, TExprContext&) const override {
        return ERROR;
    }

    EStatus AreTypesSupported(const TPosition&, const TVector<const TTypeAnnotationNode*>&, TExprContext&,
        const TUnsupportedTypeCallback&) const override {
        return OK;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DqBuildBlockChannels) {
    Y_UNIT_TEST(SharedStageHasIndependentLambdaArguments) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto* itemType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
        const auto* narrowType = ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("value", itemType)});
        const auto* streamType = ctx.MakeType<TStreamExprType>(
            ctx.MakeType<TMultiExprType>(TVector<const TTypeAnnotationNode*>{itemType}));

        const auto settings = TDqStageSettings().SetWideChannels(*narrowType).BuildNode(ctx, pos);
        // The graph is already type annotated when the final channel transformer runs.
        for (const auto& setting : settings) {
            if (setting.Name().Value() == TDqStageSettings::WideChannelsSettingName) {
                setting.Value().Cast().Ptr()->SetTypeAnn(ctx.MakeType<TTypeExprType>(narrowType));
            }
        }

        const auto makeStage = [&](TExprNode::TPtr input) {
            auto arg = ctx.NewArgument(pos, "stream");
            arg->SetTypeAnn(streamType);
            auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {arg}), TExprNode::TListType{arg});
            program->SetTypeAnn(streamType);
            return NNodes::Build<NNodes::TDqPhyStage>(ctx, pos)
                .Inputs().Add(input).Build()
                .Program(program)
                .Settings(settings)
                .Done();
        };

        const auto source = ctx.NewCallable(pos, "DqSource", {
            ctx.NewCallable(pos, "DataSource", {ctx.NewAtom(pos, "test")}), ctx.NewList(pos, {})});
        const auto shared = makeStage(source);
        const auto connection = NNodes::Build<NNodes::TDqCnUnionAll>(ctx, pos)
            .Output()
                .Stage(shared)
                .Index().Build("0")
            .Build()
            .Done();
        const auto consumer = makeStage(connection.Ptr());
        // One consumer retains the scalar output; the other is rebuilt to use blocks.
        const auto input = ctx.NewList(pos, {shared.Ptr(), consumer.Ptr()});
        CheckArguments(*input);

        TTypeAnnotationContext typesCtx;
        typesCtx.ArrowResolver = IArrowResolver::TPtr(new TArrowResolver());
        auto transformer = CreateDqBuildWideBlockChannelsTransformer(typesCtx, CHANNEL_WIDE_FORCE_BLOCK);
        TExprNode::TPtr output;
        const auto status = transformer->Transform(input, output, ctx);
        UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(output);
        CheckArguments(*output);

        const auto rebuiltConsumer = NNodes::TDqPhyStage(output->ChildPtr(1));
        const auto rebuiltConnection = rebuiltConsumer.Inputs().Item(0).Cast<NNodes::TDqCnUnionAll>();
        const auto rebuiltShared = rebuiltConnection.Output().Stage().Cast<NNodes::TDqPhyStage>();
        UNIT_ASSERT(rebuiltShared.Program().Body().Maybe<NNodes::TCoWideToBlocks>());
        UNIT_ASSERT(rebuiltShared.Program().Args().Arg(0).Raw() != shared.Program().Args().Arg(0).Raw());
    }
}

} // namespace NYql::NDq
