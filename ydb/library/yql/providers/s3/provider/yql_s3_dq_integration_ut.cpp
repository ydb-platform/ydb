#include "yql_s3_dq_integration.h"
#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/proto/source.pb.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

using namespace NNodes;

bool ValidateWriteTarget(TStringBuf format, TStringBuf policy, bool hasPolicy, TExprContext& ctx) {
    const auto pos = ctx.AppendPosition({});
    TExprNode::TListType settings;
    if (hasPolicy) {
        settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "onmissingfile"), ctx.NewAtom(pos, policy)}));
    }
    auto target = ctx.NewCallable(pos, "S3Target", {
        ctx.NewAtom(pos, "folder/"), ctx.NewAtom(pos, format), ctx.NewList(pos, std::move(settings))});
    auto transformer = CreateS3DataSinkTypeAnnotationTransformer(MakeIntrusive<TS3State>());
    for (ui32 step = 0; step < 3; ++step) {
        TExprNode::TPtr output;
        const auto status = transformer->Transform(target, output, ctx);
        if (status.Level != IGraphTransformer::TStatus::Repeat) {
            return status.Level == IGraphTransformer::TStatus::Ok;
        }
        target = std::move(output);
    }
    UNIT_FAIL("write target did not finish type annotation");
    return false;
}

NS3::TSource SerializeRead(TStringBuf format, TStringBuf policy, TStringBuf literalType) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    auto state = MakeIntrusive<TS3State>();
    state->Configuration->Clusters["s3"].Url = "http://fake/";
    auto integration = CreateS3DqIntegration(state);
    auto world = ctx.NewWorld(pos);
    world->SetTypeAnn(ctx.MakeType<TWorldExprType>());
    auto extra = ctx.NewCallable(pos, "AsStruct", {});
    extra->SetTypeAnn(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}));
    auto paths = ctx.NewList(pos, {ctx.NewList(pos, {
        ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, R"(Paths { Name: "file" Size: 10 Read: true })")}),
        ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "true")}), extra})});
    TExprNode::TListType optionNodes;
    if (policy) {
        auto value = ctx.NewAtom(pos, policy);
        if (literalType) {
            value = ctx.NewCallable(pos, literalType, {value});
        }
        optionNodes.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "onmissingfile"), value}));
    }
    auto options = ctx.NewList(pos, std::move(optionNodes));
    auto read = ctx.NewCallable(pos, "S3ReadObject!", {
        world,
        ctx.NewCallable(pos, "DataSource", {ctx.NewAtom(pos, "s3"), ctx.NewAtom(pos, "s3")}),
        ctx.NewCallable(pos, "S3Object", {paths, ctx.NewAtom(pos, format), options}),
        ctx.NewCallable(pos, "Void", {}), ctx.NewAtom(pos, "file"),
        ctx.NewLambda(pos, ctx.NewArguments(pos, {}), ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "true")}))});
    const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{});
    read->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        world->GetTypeAnn(), ctx.MakeType<TListExprType>(rowType)}));
    auto wrap = TDqSourceWrap(integration->WrapRead(read, ctx, {}));
    if (auto parse = wrap.Input().Maybe<TS3ParseSettings>()) {
        parse.Cast().RowType().Ptr()->SetTypeAnn(ctx.MakeType<TTypeExprType>(rowType));
    }
    auto source = Build<TDqSource>(ctx, pos).DataSource(wrap.DataSource()).Settings(wrap.Input()).Done();
    google::protobuf::Any packed;
    TString sourceType;
    integration->FillSourceSettings(source.Ref(), packed, sourceType, 1, ctx);
    NS3::TSource result;
    UNIT_ASSERT(packed.UnpackTo(&result));
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TS3DqIntegration) {
    Y_UNIT_TEST(MissingFilePolicyDoesNotPreventWrites) {
        for (const auto format : {"raw"sv, "json_list"sv, "json_each_row"sv, "parquet"sv}) {
            for (const auto policy : {""sv, "fail"sv, "ignore"sv, "warn"sv, "WaRn"sv}) {
                TExprContext ctx;
                UNIT_ASSERT_C(ValidateWriteTarget(format, policy, !policy.empty(), ctx),
                    format << ": " << policy << " " << ctx.IssueManager.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST(InvalidMissingFilePolicyIsRejectedForWrites) {
        for (const auto policy : {""sv, "skip_all_errors"sv, "warn "sv, "1"sv}) {
            TExprContext ctx;
            UNIT_ASSERT(!ValidateWriteTarget("json_each_row", policy, true, ctx));
            UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "on_missing_file must be one of");
        }
    }

    Y_UNIT_TEST(MissingFilePolicyReachesTaskForAllReaderFormats) {
        for (const auto format : {"raw"sv, "json_list"sv, "json_each_row"sv, "parquet"sv}) {
            for (const auto policy : {""sv, "fail"sv, "ignore"sv, "warn"sv}) {
                for (const auto literalType : {""sv, "String"sv, "Utf8"sv}) {
                    const auto source = SerializeRead(format, policy, literalType);
                    if (!policy.empty()) {
                        UNIT_ASSERT_VALUES_EQUAL(source.GetSettings().at("onmissingfile"), policy);
                    } else {
                        UNIT_ASSERT(!source.GetSettings().contains("onmissingfile"));
                    }
                }
            }
        }
    }
}

} // namespace NYql
