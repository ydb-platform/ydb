#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_transformer.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboIdLowering) {
    Y_UNIT_TEST(UpsertKeepsDefaultColumnsThroughLowering) {
        NTests::TIdTestContext f;
        // Exercise RBO lowering regardless of the DML fallback default.
        f.Config->SetEnableFallbackOnDML(false);
        auto& tableDescription = f.Tables->GetOrAddTable(f.KqpCtx.Cluster, "/Root", "/Root/table");
        tableDescription.Metadata = MakeIntrusive<TKikimrTableMetadata>();
        tableDescription.Metadata->DoesExist = true;
        auto& ctx = f.ExprCtx;
        const auto pos = f.Pos;
        auto empty = ctx.NewCallable(pos, "KqpOpEmptySource", {});
        auto constant = MakeConstant("Uint64", "1", pos, &ctx).Node;
        const auto* type = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        constant->TailPtr()->SetTypeAnn(type);
        auto columns = ctx.NewList(pos, {ctx.NewAtom(pos, "value")});
        auto input = ctx.NewCallable(pos, "KqpOpMap", {empty, ctx.NewList(pos, {
            ctx.NewCallable(pos, "KqpOpMapElementLambda",
                {empty, ctx.NewAtom(pos, "value"), constant, ctx.NewAtom(pos, "false")})
        })});
        input->SetTypeAnn(ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("value", type)})));
        auto root = ctx.NewCallable(pos, "KqpOpRoot", {input, columns});
        const auto table = NNodes::Build<NNodes::TKqpTable>(ctx, pos)
            .Path().Build("/Root/table").PathId().Build("1:1")
            .SysView().Build("").Version().Build("1").Done();
        const auto upsert = NNodes::Build<NNodes::TKqlUpsertRows>(ctx, pos)
            .Table(table)
            .Input(root)
            .Columns(columns)
            .ReturningColumns().Build()
            .IsBatch().Build("false")
            .DefaultColumns(columns)
            .Settings().Build()
            .Done();

        ui64 uniqueSourceIdCounter = 0;
        const auto rewritten = RewriteTableEffect(upsert.Ptr(), ctx, f.KqpCtx, uniqueSourceIdCounter);
        auto plan = PlanConverter(f.TypeCtx, ctx).ConvertRoot(rewritten, nullptr);
        auto& effect = CastOperator<TOpTableEffect>(*plan->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(effect.GetExplainName(), "UpsertRows");
        const auto inputId = effect.GetColumns().Items().front().first;
        UNIT_ASSERT(effect.Options.DefaultColumns == TUnorderedIUs{inputId});
        // Defaults are input references: rebinding them must leave the sink's
        // destination column name unchanged.
        const auto replacement = plan->PlanProps.InfoUnitRegistry.AddCopy(inputId);
        TSubstitutions renames;
        renames.Add(inputId, replacement);
        effect.RenameUsedIUs(renames);
        UNIT_ASSERT(effect.Options.DefaultColumns == TUnorderedIUs{replacement});
        UNIT_ASSERT_VALUES_EQUAL(effect.GetColumns().Items().front().first, replacement);
        const NNodes::TKqpTableSinkSettings settings(effect.BuildSettings(ctx, 0));
        UNIT_ASSERT_VALUES_EQUAL(settings.DefaultColumns().Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.DefaultColumns().Item(0).StringValue(), "value");
    }
}

} // namespace NKikimr::NKqp
