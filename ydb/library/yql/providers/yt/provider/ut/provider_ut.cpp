#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_message_stream_impl.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_message_stream.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NYql {
TEST(TYtMessageStreamIntegration, PreserveFullDataSourcePaths) {
    auto source = CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory());
    source->AddCluster("/Root/a/source", {{"source_type", "YT"}, {"location", "yt-a:9013"}});
    source->AddCluster("/Root/b/source", {{"source_type", "YT"}, {"location", "yt-b:9013"}});
    EXPECT_EQ(source->GetValidClusters().size(), 2u);
    EXPECT_TRUE(source->GetValidClusters().contains("/Root/a/source"));
    EXPECT_TRUE(source->GetValidClusters().contains("/Root/b/source"));
}
TEST(TYtMessageStreamIntegration, RejectOtherDatabaseType) {
    auto source = CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory());
    EXPECT_ANY_THROW(source->AddCluster("/Root/source", {{"source_type", "Ydb"}, {"location", "host:2135"}}));
    EXPECT_TRUE(source->GetValidClusters().empty());
}
TEST(TYtMessageStreamIntegration, CredentialsRemainBoundToEdsPath) {
    auto source = CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory());
    source->AddCluster("/Root/a/source", {{"source_type", "YT"}, {"location", "host:9013"}, {"token", "first"}});
    source->AddCluster("/Root/b/source", {{"source_type", "YT"}, {"location", "host:9013"}, {"token", "second"}});
    EXPECT_EQ(CreateStructuredTokenParser(source->GetClusterTokens().at("/Root/a/source")).GetIAMToken(), "first");
    EXPECT_EQ(CreateStructuredTokenParser(source->GetClusterTokens().at("/Root/b/source")).GetIAMToken(), "second");
}
}

namespace NYql {
namespace {
TExprNode::TPtr Read(TExprContext& ctx, TStringBuf format, bool consumer) {
    const auto pos = TPositionHandle();
    TExprNode::TListType settings;
    if (consumer) {
        settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "consumer"), ctx.NewAtom(pos, "//consumer")}));
    }
    return ctx.NewCallable(pos, "Read!", {
        ctx.NewWorld(pos), ctx.NewCallable(pos, "DataSource", {ctx.NewAtom(pos, "yt"), ctx.NewAtom(pos, "cluster"), ctx.NewAtom(pos, "message_stream")}),
        ctx.NewCallable(pos, "MrTableConcat", {ctx.NewCallable(pos, "MrObject", {
            ctx.NewAtom(pos, "//queue"), ctx.NewAtom(pos, format), ctx.NewAtom(pos, "")})}),
        ctx.NewCallable(pos, "Void", {}), ctx.NewList(pos, std::move(settings))});
}
}
TEST(TYtMessageStreamIntegration, ParseKqpReadSettings) {
    TExprContext ctx;
    const auto read = ParseYtMessageStreamReadSettings(*Read(ctx, "raw", true));
    EXPECT_EQ(read.Path, "//queue");
    EXPECT_EQ(read.Consumer, "//consumer");
}
TEST(TYtMessageStreamIntegration, RejectMissingConsumerAndUnsupportedFormat) {
    TExprContext ctx;
    EXPECT_ANY_THROW(ParseYtMessageStreamReadSettings(*Read(ctx, "raw", false)));
    EXPECT_ANY_THROW(ParseYtMessageStreamReadSettings(*Read(ctx, "json_each_row", true)));
}
TEST(TYtMessageStreamIntegration, NoAuthProducesUsableSecureParameter) {
    auto credentials = CreateStructuredTokenCredentialsFactory();
    auto source = CreateYtMessageStreamIntegration(credentials);
    source->AddCluster("/Root/eds", {{"source_type", "YT"}, {"location", "host:9013"}, {"authMethod", "NONE"}});
    const auto& token = source->GetClusterTokens().at("/Root/eds");
    EXPECT_FALSE(token.empty());
    EXPECT_TRUE(credentials->Create(token)->CreateProvider()->GetAuthInfo().empty());
}
}

namespace NYql {
TEST(TYtMessageStreamIntegration, SharesYtProviderWithTables) {
    TExprContext ctx;
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    auto state = std::make_shared<TYtState>(types.Get());
    state->DqIntegration_ = MakeHolder<TDqIntegrationBase>();
    auto streams = CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory());
    streams->AddCluster("cluster", {{"source_type", "YT"}, {"location", "host:9013"}});
    auto provider = WrapYtDataSourceWithMessageStreams(CreateYtDataSource(state), streams);
    types->AddDataSource(YtProviderName, provider);

    const auto streamRead = Read(ctx, "raw", true);
    const auto tableSource = ctx.NewCallable(TPositionHandle(), "DataSource", {ctx.NewAtom(TPositionHandle(), "yt"), ctx.NewAtom(TPositionHandle(), "cluster")});
    const auto tableRead = ctx.ChangeChild(*streamRead, 1, TExprNode::TPtr(tableSource));
    EXPECT_EQ(types->DataSourceMap.size(), 1u);
    EXPECT_EQ(provider->GetName(), YtProviderName);
    EXPECT_TRUE(provider->CanParse(*tableRead));
    EXPECT_TRUE(provider->CanParse(*streamRead));
    EXPECT_TRUE(NNodes::TYtDSource::Match(tableSource.Get()));
    EXPECT_TRUE(streams->CanParse(*streamRead));
    EXPECT_FALSE(streams->CanParse(*tableRead));
    TMaybe<TString> cluster;
    EXPECT_TRUE(provider->ValidateParameters(*streamRead->Child(1), ctx, cluster));
    EXPECT_EQ(cluster.GetRef(), "cluster");
}
}

namespace NYql {
TEST(TYtMessageStreamIntegration, AnnotatesEveryNodeWhenMixingTablesAndStreams) {
    for (bool streamFirst : {false, true}) {
        TExprContext ctx;
        auto types = MakeIntrusive<TTypeAnnotationContext>();
        auto state = std::make_shared<TYtState>(types.Get());
        state->DqIntegration_ = MakeHolder<TDqIntegrationBase>();
        auto streams = CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory());
        auto provider = WrapYtDataSourceWithMessageStreams(CreateYtDataSource(state), streams);
        auto& transformer = provider->GetTypeAnnotationTransformer(false);
        const auto pos = TPositionHandle();
        for (size_t index = 0; index < 4; ++index) {
            const bool stream = (index % 2 == 0) == streamFirst;
            TExprNode::TPtr node;
            if (stream) {
                auto world = ctx.NewWorld(pos);
                world->SetTypeAnn(ctx.MakeType<TWorldExprType>());
                node = ctx.NewCallable(pos, "YtMessageStreamSourceSettings", {
                    world, ctx.NewAtom(pos, "//queue"),
                    ctx.NewCallable(pos, "SecureParam", {ctx.NewAtom(pos, "token")}),
                    ctx.NewList(pos, {ctx.NewAtom(pos, "Data")}),
                    ctx.NewAtom(pos, "//consumer"), ctx.NewAtom(pos, "1")});
            } else {
                node = ctx.NewCallable(pos, "YtRow", {
                    ctx.NewCallable(pos, "Uint64", {ctx.NewAtom(pos, "0")})});
            }
            TExprNode::TPtr output;
            EXPECT_EQ(transformer.Transform(node, output, ctx).Level, IGraphTransformer::TStatus::Ok);
            ASSERT_NE(node->GetTypeAnn(), nullptr);
            EXPECT_EQ(node->GetTypeAnn()->GetKind(), stream ? ETypeAnnotationKind::Stream : ETypeAnnotationKind::Unit);
        }
    }
}
}

namespace NYql {
namespace {
class TTableSourceForDiscovery final : public TDataProviderBase {
public:
    TTableSourceForDiscovery()
        : Discovery_(CreateFunctorTransformer([this](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) {
            ++DiscoveryCalls;
            VisitExpr(*input, [this](const TExprNode& node) {
                if (node.IsCallable("Read!")) {
                    ++TableReads;
                    EXPECT_FALSE(NNodes::TYtMessageStreamDataSource::Match(node.Child(1)));
                }
                if (NNodes::TYtMessageStreamReadTable::Match(&node)) {
                    ++StreamReads;
                }
                return true;
            });
            output = input;
            return IGraphTransformer::TStatus::Ok;
        }))
    {}
    TStringBuf GetName() const override { return YtProviderName; }
    IDqIntegration* GetDqIntegration() override { return &Dq_; }
    IGraphTransformer& GetIODiscoveryTransformer() override { return *Discovery_; }
    size_t DiscoveryCalls = 0;
    size_t TableReads = 0;
    size_t StreamReads = 0;
private:
    TDqIntegrationBase Dq_;
    THolder<IGraphTransformer> Discovery_;
};
}

TEST(TYtMessageStreamIntegration, LowersStreamsBeforeTableDiscoveryAndAfterRewind) {
    auto tables = MakeIntrusive<TTableSourceForDiscovery>();
    auto streams = CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory());
    auto provider = WrapYtDataSourceWithMessageStreams(tables, streams);
    auto& discovery = provider->GetIODiscoveryTransformer();
    for (size_t attempt = 0; attempt < 2; ++attempt) {
        TExprContext ctx;
        const auto pos = TPositionHandle();
        const auto stream = Read(ctx, "raw", true);
        const auto table = ctx.ChangeChild(*stream, 1, ctx.NewCallable(pos, "DataSource", {
            ctx.NewAtom(pos, "yt"), ctx.NewAtom(pos, "cluster")}));
        auto input = ctx.NewList(pos, {
            ctx.NewCallable(pos, "Right!", {stream}), ctx.NewCallable(pos, "Right!", {table})});
        auto status = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat);
        for (size_t step = 0; step < 10 && status.Level == IGraphTransformer::TStatus::Repeat; ++step) {
            TExprNode::TPtr output;
            status = discovery.Transform(input, output, ctx);
            if (output) {
                input = output;
            }
        }
        ASSERT_EQ(status.Level, IGraphTransformer::TStatus::Ok);
        EXPECT_TRUE(NNodes::TYtMessageStreamReadTable::Match(input->Child(0)->Child(0)));
        EXPECT_EQ(input->Child(1)->Child(0), table.Get());
        EXPECT_TRUE(provider->IsRead(*input->Child(0)->Child(0)));
        TExprNode::TListType dependencies;
        EXPECT_TRUE(provider->GetPlanFormatter().GetDependencies(*input->Child(0)->Child(0), dependencies, false));
        ASSERT_EQ(dependencies.size(), 1u);
        EXPECT_EQ(dependencies.front().Get(), stream->Child(0));
        discovery.Rewind();
    }
    EXPECT_EQ(tables->DiscoveryCalls, 2u);
    EXPECT_EQ(tables->TableReads, 2u);
    EXPECT_EQ(tables->StreamReads, 2u);
}

TEST(TYtMessageStreamIntegration, RejectsInvalidStreamBeforeTableDiscovery) {
    auto tables = MakeIntrusive<TTableSourceForDiscovery>();
    auto provider = WrapYtDataSourceWithMessageStreams(tables,
        CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory()));
    TExprContext ctx;
    auto input = ctx.NewCallable(TPositionHandle(), "Right!", {Read(ctx, "raw", false)});
    TExprNode::TPtr output;
    EXPECT_EQ(provider->GetIODiscoveryTransformer().Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Error);
    EXPECT_EQ(tables->DiscoveryCalls, 0u);
}
}

namespace NYql {
TEST(TYtMessageStreamIntegration, RoutesIntentsForStreamsAndTables) {
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    auto state = std::make_shared<TYtState>(types.Get());
    state->DqIntegration_ = MakeHolder<TDqIntegrationBase>();
    auto provider = WrapYtDataSourceWithMessageStreams(CreateYtDataSource(state),
        CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory()));
    auto& intent = provider->GetIntentDeterminationTransformer();
    for (size_t attempt = 0; attempt < 2; ++attempt) {
        TExprContext ctx;
        auto stream = Read(ctx, "raw", true);
        TExprNode::TPtr output;
        EXPECT_EQ(intent.Transform(stream, output, ctx).Level, IGraphTransformer::TStatus::Ok);
        EXPECT_EQ(output, stream);

        const auto pos = TPositionHandle();
        auto table = ctx.ChangeChild(*stream, 1, ctx.NewCallable(pos, "DataSource", {
            ctx.NewAtom(pos, "yt"), ctx.NewAtom(pos, "cluster")}));
        table = ctx.ChangeChild(*table, 2, ctx.NewList(pos, {}));
        EXPECT_EQ(intent.Transform(table, output, ctx).Level, IGraphTransformer::TStatus::Ok);

        auto invalidTable = ctx.NewCallable(pos, "Read!", {table->ChildPtr(0), table->ChildPtr(1)});
        EXPECT_EQ(intent.Transform(invalidTable, output, ctx).Level, IGraphTransformer::TStatus::Error);
        intent.Rewind();
    }
}
}

namespace NYql {
TEST(TYtMessageStreamIntegration, ValidatesRawReadsBeforeMetadataLoading) {
    auto streams = CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory());
    TExprContext ctx;
    auto input = ctx.NewCallable(TPositionHandle(), "Right!", {Read(ctx, "raw", false)});
    TExprNode::TPtr output;
    EXPECT_EQ(streams->GetLoadTableMetadataTransformer().Transform(input, output, ctx).Level,
        IGraphTransformer::TStatus::Error);
    EXPECT_FALSE(ctx.IssueManager.GetIssues().Empty());
}
}

namespace NYql {
namespace {
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
}

TEST(TYtMessageStreamIntegration, RediscoversStreamAfterInitialPassWithoutRewind) {
    auto tables = MakeIntrusive<TTableSourceForDiscovery>();
    auto provider = WrapYtDataSourceWithMessageStreams(tables,
        CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory()));
    auto& discovery = provider->GetIODiscoveryTransformer();
    TExprContext ctx;
    auto input = ctx.NewWorld(TPositionHandle());
    ASSERT_EQ(RunProviderPhase(discovery, input, ctx).Level, IGraphTransformer::TStatus::Ok);
    input = ctx.NewCallable(TPositionHandle(), "Right!", {Read(ctx, "raw", true)});
    ASSERT_EQ(RunProviderPhase(discovery, input, ctx).Level, IGraphTransformer::TStatus::Ok);
    EXPECT_TRUE(NNodes::TYtMessageStreamReadTable::Match(input->Child(0)));
    EXPECT_EQ(tables->StreamReads, 1u);
}

TEST(TYtMessageStreamIntegration, ReloadsStreamMetadataAfterInitialPassWithoutRewind) {
    auto provider = WrapYtDataSourceWithMessageStreams(MakeIntrusive<TTableSourceForDiscovery>(),
        CreateYtMessageStreamIntegration(CreateStructuredTokenCredentialsFactory()));
    auto& metadata = provider->GetLoadTableMetadataTransformer();
    TExprContext ctx;
    auto input = ctx.NewWorld(TPositionHandle());
    ASSERT_EQ(RunProviderPhase(metadata, input, ctx).Level, IGraphTransformer::TStatus::Ok);
    input = ctx.NewCallable(TPositionHandle(), "Right!", {Read(ctx, "raw", false)});
    EXPECT_EQ(RunProviderPhase(metadata, input, ctx).Level, IGraphTransformer::TStatus::Error);
    EXPECT_FALSE(ctx.IssueManager.GetIssues().Empty());
}
}

namespace NYql {
TEST(TYtMessageStreamIntegration, ResolvesNoAuthMessageStreamSecureParameter) {
    auto credentials = CreateStructuredTokenCredentialsFactory();
    auto streams = CreateYtMessageStreamIntegration(credentials);
    streams->AddCluster("/Root/eds", {{"source_type", "YT"}, {"location", "host:9013"}, {"authMethod", "NONE"}});
    auto provider = WrapYtDataSourceWithMessageStreams(MakeIntrusive<TTableSourceForDiscovery>(), streams);
    const auto token = provider->ResolveClusterToken("/Root/eds");
    ASSERT_TRUE(token.Defined());
    EXPECT_FALSE(token->empty());
    EXPECT_EQ(*token, streams->GetClusterTokens().at("/Root/eds"));
    EXPECT_TRUE(credentials->Create(*token)->CreateProvider()->GetAuthInfo().empty());
    EXPECT_FALSE(provider->ResolveClusterToken("/Root/missing").Defined());
}
}
