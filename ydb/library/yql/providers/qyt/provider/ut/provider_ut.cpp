#include <ydb/library/yql/providers/qyt/expr_nodes/yql_qyt_expr_nodes.h>
#include <ydb/library/yql/providers/qyt/provider/yql_qyt_provider.h>
#include <ydb/library/yql/providers/qyt/provider/yql_qyt_provider_impl.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

namespace NYql {
namespace {
TExprNode::TPtr Read(
    TExprContext& ctx, TStringBuf format, bool consumer, TStringBuf cluster = "cluster", TStringBuf provider = QytProviderName) {
    const auto pos = TPositionHandle();
    TExprNode::TListType settings;
    if (consumer) {
        settings.push_back(ctx.NewList(pos, { ctx.NewAtom(pos, "consumer"), ctx.NewAtom(pos, "//consumer") }));
    }
    return ctx.NewCallable(pos, "Read!",
        { ctx.NewWorld(pos), ctx.NewCallable(pos, "DataSource", { ctx.NewAtom(pos, provider), ctx.NewAtom(pos, cluster) }),
            ctx.NewCallable(pos, "MrTableConcat",
                { ctx.NewCallable(pos, "MrObject", { ctx.NewAtom(pos, "//queue"), ctx.NewAtom(pos, format), ctx.NewAtom(pos, "") }) }),
            ctx.NewCallable(pos, "Void", {}), ctx.NewList(pos, std::move(settings)) });
}

IGraphTransformer::TStatus RunProviderPhase(IGraphTransformer& phase, TExprNode::TPtr& input, TExprContext& ctx) {
    auto status = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat);
    for (size_t step = 0; step < 20 && status.Level == IGraphTransformer::TStatus::Repeat; ++step) {
        TExprNode::TPtr output;
        status = phase.Transform(input, output, ctx);
        if (output) {
            input = output;
        }
    }
    return status;
}
}   // namespace

TEST(TQytProvider, PreserveFullDataSourcePathsAndCredentials) {
    auto source = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    source->AddCluster("/Root/a/source", { { "source_type", "QYT" }, { "location", "host:9013" }, { "token", "first" } });
    source->AddCluster("/Root/b/source", { { "source_type", "QYT" }, { "location", "host:9013" }, { "token", "second" } });
    EXPECT_EQ(source->GetValidClusters().size(), 2u);
    EXPECT_TRUE(source->GetValidClusters().contains("/Root/a/source"));
    EXPECT_TRUE(source->GetValidClusters().contains("/Root/b/source"));
    EXPECT_EQ(CreateStructuredTokenParser(source->GetClusterTokens()->at("/Root/a/source")).GetIAMToken(), "first");
    EXPECT_EQ(CreateStructuredTokenParser(source->GetClusterTokens()->at("/Root/b/source")).GetIAMToken(), "second");
}

TEST(TQytProvider, RejectOtherDatabaseType) {
    auto source = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    for (const auto* type : { "Ydb", "YT" }) {
        EXPECT_ANY_THROW(source->AddCluster("/Root/source", { { "source_type", type }, { "location", "host:2135" } }));
    }
    EXPECT_TRUE(source->GetValidClusters().empty());
}

TEST(TQytProvider, ParseKqpReadSettings) {
    TExprContext ctx;
    const auto read = ParseQytReadSettings(*Read(ctx, "", true));
    EXPECT_EQ(read.Path, "//queue");
    EXPECT_EQ(read.Consumer, "//consumer");
}

TEST(TQytProvider, RejectInvalidReadSettings) {
    auto source = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    source->AddCluster("/Root/eds", { { "source_type", "QYT" }, { "location", "localhost:1" } });
    {
        TExprContext ctx;
        EXPECT_ANY_THROW(ParseQytReadSettings(*Read(ctx, "", false, "/Root/eds")));
    }
    const auto checkFormat = [&](TStringBuf format, bool inSettings) {
        TExprContext ctx;
        auto read = Read(ctx, format, true, "/Root/eds");
        if (inSettings) {
            auto settings = read->Child(4)->ChildrenList();
            settings.push_back(
                ctx.NewList(TPositionHandle(), { ctx.NewAtom(TPositionHandle(), "format"), ctx.NewAtom(TPositionHandle(), "raw") }));
            read = ctx.ChangeChild(*read, 4, ctx.NewList(TPositionHandle(), std::move(settings)));
        }
        EXPECT_TRUE(source->CanParse(*read));
        EXPECT_FALSE(source->RewriteIO(ctx.NewCallable(TPositionHandle(), "Right!", { read }), ctx));
        EXPECT_NE(ctx.IssueManager.GetIssues().ToString().find("FORMAT is not supported for QYT reads"), TString::npos);
    };
    checkFormat("raw", false);
    checkFormat("json_each_row", false);
    checkFormat("", true);
}

TEST(TQytProvider, OwnsQueuesWithoutYtTableProvider) {
    TExprContext ctx;
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    auto provider = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    provider->AddCluster("cluster", { { "source_type", "QYT" }, { "location", "host:9013" } });
    types->AddDataSource(QytProviderName, provider);
    EXPECT_EQ(provider->GetName(), QytProviderName);
    EXPECT_FALSE(types->DataSourceMap.contains(TString(YtProviderName)));
    const auto queue = Read(ctx, "", true);
    EXPECT_TRUE(provider->CanParse(*queue));
    TMaybe<TString> cluster;
    EXPECT_TRUE(provider->ValidateParameters(*queue->Child(1), ctx, cluster));
    EXPECT_EQ(cluster.GetRef(), "cluster");

    const auto table = Read(ctx, "", true, "cluster", YtProviderName);
    EXPECT_FALSE(provider->CanParse(*table));
    EXPECT_FALSE(provider->ValidateParameters(*table->Child(1), ctx, cluster));
    EXPECT_FALSE(provider->GetDqIntegration()->CanRead(*table, ctx, true));
    const auto tableRight = ctx.NewCallable(TPositionHandle(), "Right!", { table });
    EXPECT_EQ(provider->RewriteIO(tableRight, ctx), tableRight);
}

TEST(TQytProvider, DiscoversOnlyQytReadsAndPreservesWorldDependency) {
    auto provider = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    auto& discovery = provider->GetIODiscoveryTransformer();
    for (size_t attempt = 0; attempt < 2; ++attempt) {
        TExprContext ctx;
        const auto pos = TPositionHandle();
        const auto stream = Read(ctx, "", true);
        const auto table = Read(ctx, "", true, "cluster", YtProviderName);
        auto input = ctx.NewList(pos, { ctx.NewCallable(pos, "Right!", { stream }), ctx.NewCallable(pos, "Right!", { table }) });
        ASSERT_EQ(RunProviderPhase(discovery, input, ctx).Level, IGraphTransformer::TStatus::Ok);
        EXPECT_TRUE(NNodes::TQytReadTable::Match(input->Child(0)->Child(0)));
        EXPECT_EQ(input->Child(1)->Child(0), table.Get());
        EXPECT_TRUE(provider->IsRead(*input->Child(0)->Child(0)));
        TExprNode::TListType dependencies;
        EXPECT_TRUE(provider->GetPlanFormatter().GetDependencies(*input->Child(0)->Child(0), dependencies, false));
        ASSERT_EQ(dependencies.size(), 1u);
        EXPECT_EQ(dependencies.front().Get(), stream->Child(0));
        discovery.Rewind();
    }
}

TEST(TQytProvider, AnnotatesSourceSettings) {
    auto provider = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    TExprContext ctx;
    const auto pos = TPositionHandle();
    auto world = ctx.NewWorld(pos);
    world->SetTypeAnn(ctx.MakeType<TWorldExprType>());
    auto input = ctx.NewCallable(pos, NNodes::TQytSourceSettings::CallableName(),
        { world, ctx.NewAtom(pos, "//queue"), ctx.NewCallable(pos, "SecureParam", { ctx.NewAtom(pos, "token") }),
            ctx.NewList(pos, { ctx.NewAtom(pos, "Data") }), ctx.NewAtom(pos, "//consumer"), ctx.NewAtom(pos, "1") });
    TExprNode::TPtr output;
    ASSERT_EQ(provider->GetTypeAnnotationTransformer(false).Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Ok);
    ASSERT_NE(input->GetTypeAnn(), nullptr);
    EXPECT_EQ(input->GetTypeAnn()->GetKind(), ETypeAnnotationKind::Stream);
}

TEST(TQytProvider, RejectsInvalidStreamDuringDiscovery) {
    auto provider = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    TExprContext ctx;
    auto input = ctx.NewCallable(TPositionHandle(), "Right!", { Read(ctx, "", false) });
    TExprNode::TPtr output;
    EXPECT_EQ(provider->GetIODiscoveryTransformer().Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Error);
}

TEST(TQytProvider, RediscoversStreamAfterInitialPassWithoutRewind) {
    auto provider = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    auto& discovery = provider->GetIODiscoveryTransformer();
    TExprContext ctx;
    auto input = ctx.NewWorld(TPositionHandle());
    ASSERT_EQ(RunProviderPhase(discovery, input, ctx).Level, IGraphTransformer::TStatus::Ok);
    input = ctx.NewCallable(TPositionHandle(), "Right!", { Read(ctx, "", true) });
    ASSERT_EQ(RunProviderPhase(discovery, input, ctx).Level, IGraphTransformer::TStatus::Ok);
    EXPECT_TRUE(NNodes::TQytReadTable::Match(input->Child(0)));
}

TEST(TQytProvider, ReloadsStreamMetadataAfterInitialPassWithoutRewind) {
    auto provider = CreateQytDataSource(CreateStructuredTokenCredentialsFactory());
    auto& metadata = provider->GetLoadTableMetadataTransformer();
    TExprContext ctx;
    auto input = ctx.NewWorld(TPositionHandle());
    ASSERT_EQ(RunProviderPhase(metadata, input, ctx).Level, IGraphTransformer::TStatus::Ok);
    input = ctx.NewCallable(TPositionHandle(), "Right!", { Read(ctx, "", false) });
    EXPECT_EQ(RunProviderPhase(metadata, input, ctx).Level, IGraphTransformer::TStatus::Error);
    EXPECT_FALSE(ctx.IssueManager.GetIssues().Empty());
}

TEST(TQytProvider, ResolvesNoAuthMessageStreamSecureParameter) {
    auto credentials = CreateStructuredTokenCredentialsFactory();
    auto provider = CreateQytDataSource(credentials);
    provider->AddCluster("/Root/eds", { { "source_type", "QYT" }, { "location", "host:9013" }, { "authMethod", "NONE" } });
    const auto token = provider->ResolveClusterToken("/Root/eds");
    ASSERT_TRUE(token.Defined());
    EXPECT_FALSE(token->empty());
    EXPECT_EQ(*token, provider->GetClusterTokens()->at("/Root/eds"));
    EXPECT_TRUE(credentials->Create(*token)->CreateProvider()->GetAuthInfo().empty());
    EXPECT_FALSE(provider->ResolveClusterToken("/Root/missing").Defined());
}
}   // namespace NYql
