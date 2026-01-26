#include "yql_pq_provider_impl.h"
#include "yql_pq_topic_key_parser.h"
#include "yql_pq_helpers.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <yql/essentials/providers/common/config/yql_configuration_transformer.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_lazy_init.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqDataSourceProvider : public TDataProviderBase {
public:
    TPqDataSourceProvider(TPqState::TPtr state, IPqGateway::TPtr gateway)
        : State_(state)
        , Gateway_(gateway)
        , ConfigurationTransformer_([this]() {
        return MakeHolder<NCommon::TProviderConfigurationTransformer>(State_->Configuration, *State_->Types, TString{ PqProviderName });
    })
        , LoadMetaDataTransformer_(CreatePqLoadTopicMetadataTransformer(State_))
        , TypeAnnotationTransformer_(CreatePqDataSourceTypeAnnotationTransformer(State_))
        , IODiscoveryTransformer_(CreatePqIODiscoveryTransformer(State_)) {
    }

    TStringBuf GetName() const override {
        return PqProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
            if (node.Child(0)->Content() == PqProviderName) {
                auto clusterName = node.Child(1)->Content();
                const auto& clusterSettings = State_->Configuration->ClustersConfigurationSettings;
                if (clusterName != NCommon::ALL_CLUSTERS && !clusterSettings.FindPtr(clusterName)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() <<
                        "Unknown cluster name: " << clusterName));
                    return false;
                }
                cluster = clusterName;
                return true;
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Pq DataSource parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoRead::CallableName())) {
            return TPqDataSource::Match(node.Child(1));
        }
        return TypeAnnotationTransformer_->CanParse(node);
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        return *IODiscoveryTransformer_;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return *ConfigurationTransformer_;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *LoadMetaDataTransformer_;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    bool EnableDqSource() const {
        return !State_->IsRtmrMode();
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderPq) << "RewriteIO";
        if (auto left = TMaybeNode<TCoLeft>(node)) {
            return left.Input().Maybe<TPqRead>().World().Cast().Ptr();
        }

        auto read = TCoRight(node).Input().Cast<TPqRead>();
        TIssueScopeGuard issueScopeRead(ctx.IssueManager, [&]() {
            return MakeIntrusive<TIssue>(ctx.GetPosition(read.Pos()), TStringBuilder() << "At function: " << TCoRead::CallableName());
        });

        TTopicKeyParser topicKeyParser(read.FreeArgs().Get(2).Ref(), read.Ref().Child(4), ctx);
        const TString cluster(read.DataSource().Cluster().Value());
        const auto* topicMeta = State_->FindTopicMeta(cluster, topicKeyParser.GetTopicPath());
        if (!topicMeta) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Unknown topic `" << cluster << "`.`" << topicKeyParser.GetTopicPath() << "`"));
            return nullptr;
        }

        TVector<TCoNameValueTuple> sourceMetadata;
        for (auto sysColumn : AllowedPqMetaSysColumns(State_->AllowTransparentSystemColumns)) {
            sourceMetadata.push_back(Build<TCoNameValueTuple>(ctx, read.Pos())
                .Name().Build("system")
                .Value<TCoAtom>().Build(sysColumn)
                .Done());
        }

        auto topicNode = Build<TPqTopic>(ctx, read.Pos())
            .Cluster().Value(cluster).Build()
            .Database().Value(State_->Configuration->GetDatabaseForTopic(cluster)).Build()
            .Path().Value(topicKeyParser.GetTopicPath()).Build()
            .RowSpec(topicMeta->RowSpec)
            .Props(BuildTopicPropsList(*topicMeta, read.Pos(), ctx))
            .Metadata().Add(sourceMetadata).Build()
            .Done();

        TExprNode::TPtr columns;
        if (auto columnOrder = topicKeyParser.GetColumnOrder()) {
            columns = std::move(columnOrder);
        } else {
            columns = Build<TCoVoid>(ctx, read.Pos()).Done().Ptr();
        }

        auto format = topicKeyParser.GetFormat();
        if (format.empty()) {
            format = "raw";
        }

        auto settings = Build<TCoNameValueTupleList>(ctx, read.Pos());

        bool hasDateTimeFormat = false;
        bool hasDateTimeFormatName = false;
        if (auto dateTimeFormatName = topicKeyParser.GetDateTimeFormatName()) {
            if (!NCommon::ValidateDateTimeFormatName(dateTimeFormatName->Child(1)->Content(), ctx)) {
                return nullptr;
            }
            settings.Add(std::move(dateTimeFormatName));
            hasDateTimeFormatName = true;
        }

        if (auto dateTimeFormat = topicKeyParser.GetDateTimeFormat()) {
            settings.Add(std::move(dateTimeFormat));
            hasDateTimeFormat = true;
        }

        if (hasDateTimeFormat && hasDateTimeFormatName) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Don't use data.datetime.format_name and data.datetime.format together"));
            return nullptr;
        }

        if (!hasDateTimeFormat && !hasDateTimeFormatName) {
            settings.Add<TExprList>()
                .Add<TCoAtom>().Build("data.datetime.formatname")
                .Add<TCoAtom>().Build("POSIX")
                .Build();
        }

        bool hasTimestampFormat = false;
        bool hasTimestampFormatName = false;
        if (auto timestampFormatName = topicKeyParser.GetTimestampFormatName()) {
            if (!NCommon::ValidateTimestampFormatName(timestampFormatName->Child(1)->Content(), ctx)) {
                return nullptr;
            }
            settings.Add(std::move(timestampFormatName));
            hasTimestampFormatName = true;
        }

        if (auto timestampFormat = topicKeyParser.GetTimestampFormat()) {
            settings.Add(std::move(timestampFormat));
            hasTimestampFormat = true;
        }

        if (hasTimestampFormat && hasTimestampFormatName) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Don't use data.timestamp.format_name and data.timestamp.format together"));
            return nullptr;
        }

        if (!hasTimestampFormat && !hasTimestampFormatName) {
            settings.Add<TExprList>()
                .Add<TCoAtom>().Build("data.timestamp.formatname")
                .Add<TCoAtom>().Build("POSIX")
                .Build();
        }

        if (auto dateFormat = topicKeyParser.GetDateFormat()) {
            settings.Add(std::move(dateFormat));
        }

        if (auto watermarkAdjustLateEvents = topicKeyParser.GetWatermarkAdjustLateEvents()) {
            settings.Add(std::move(watermarkAdjustLateEvents));
        }

        if (auto watermarkDropLateEvents = topicKeyParser.GetWatermarkDropLateEvents()) {
            settings.Add(std::move(watermarkDropLateEvents));
        }

        if (auto watermarkGranularity = topicKeyParser.GetWatermarkGranularity()) {
            settings.Add(std::move(watermarkGranularity));
        }

        if (auto watermarkIdleTimeout = topicKeyParser.GetWatermarkIdleTimeout()) {
            settings.Add(std::move(watermarkIdleTimeout));
        }

        if (auto skipJsonErrors = topicKeyParser.GetSkipJsonErrors()) {
            settings.Add(std::move(skipJsonErrors));
        }

        if (auto streamingTopicRead = topicKeyParser.GetStreamingTopicRead()) {
            if (!topicKeyParser.ParseStreamingTopicRead(*streamingTopicRead, ctx)) {
                return nullptr;
            }
            settings.Add(std::move(streamingTopicRead));
        }

        auto builder = Build<TPqReadTopic>(ctx, read.Pos())
            .World(read.World())
            .DataSource(read.DataSource())
            .Topic(std::move(topicNode))
            .Columns(std::move(columns))
            .Format().Value(format).Build()
            .Compression().Value(topicKeyParser.GetCompression()).Build()
            .LimitHint<TCoVoid>().Build()
            .Settings(settings.Done());

        if (auto watermark = topicKeyParser.GetWatermark()) {
            builder.Watermark(std::move(watermark));
        }

        return Build<TCoRight>(ctx, read.Pos())
            .Input(builder.Done())
            .Done().Ptr();
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return &State_->Configuration->Tokens;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);

        for (auto& child : node.Children()) {
            children.push_back(child.Get());
        }

        if (TMaybeNode<TPqReadTopic>(&node)) {
            return true;
        }
        return false;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
        Y_UNUSED(withLimits);
        if (auto maybeRead = TMaybeNode<TPqReadTopic>(&node)) {
            if (auto maybeTopic = maybeRead.Topic()) {
                TStringBuf cluster;
                if (auto dataSource = maybeRead.DataSource().Maybe<TPqDataSource>()) {
                    cluster = dataSource.Cast().Cluster().Value();
                }
                auto topicDisplayName = MakeTopicDisplayName(cluster, maybeTopic.Cast().Path().Value());
                inputs.push_back(TPinInfo(maybeRead.DataSource().Raw(), nullptr, maybeTopic.Cast().Raw(), topicDisplayName, false));
                return 1;
            }
        }
        return 0;
    }

    void AddCluster(const TString& clusterName, const THashMap<TString, TString>& properties) override {
        NYql::TPqClusterConfig cluster;
        cluster.SetName(clusterName);
        cluster.SetClusterType(NYql::TPqClusterConfig::CT_DATA_STREAMS);
        const TString& location = properties.Value("location", "");
        cluster.SetEndpoint(location);
        cluster.SetToken(properties.Value("token", ""));
        cluster.SetDatabase(properties.Value("database_name", ""));
        TString useTls = properties.Value("use_tls", "false");
        useTls.to_lower();
        cluster.SetUseSsl(useTls == "true"sv);
        TString sharedReading = properties.Value("shared_reading", "false");
        sharedReading.to_lower();
        cluster.SetSharedReading(sharedReading == "true"sv);

        State_->Configuration->AddCluster(cluster, State_->DatabaseIds, State_->Types->Credentials, State_->DbResolver, properties);
        Gateway_->AddCluster(cluster);
    }

    IDqIntegration* GetDqIntegration() override {
        return State_->DqIntegration.Get();
    }

private:
    TPqState::TPtr State_;
    IPqGateway::TPtr Gateway_;
    TLazyInitHolder<IGraphTransformer> ConfigurationTransformer_;
    THolder<IGraphTransformer> LoadMetaDataTransformer_;
    THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    THolder<IGraphTransformer> IODiscoveryTransformer_;
};

} // anonymous namespace

TIntrusivePtr<IDataProvider> CreatePqDataSource(TPqState::TPtr state, IPqGateway::TPtr gateway) {
    return new TPqDataSourceProvider(state, gateway);
}

} // namespace NYql
