#include "yql_pq_ytflow_integration.h"

#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>

namespace NYql {

using namespace NNodes;

class TPqYtflowIntegration : public TEmptyYtflowIntegration {
public:
    TPqYtflowIntegration(const TPqState::TPtr& state)
        : State_(state.Get())
    {
    }

    TMaybe<bool> CanRead(const TExprNode& node, TExprContext& /*ctx*/) override {
        auto maybeReadTopic = TMaybeNode<TPqReadTopic>(&node);
        if (!maybeReadTopic) {
            return Nothing();
        }

        return true;
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx) override {
        auto maybeReadTopic = TMaybeNode<TPqReadTopic>(read);
        YQL_ENSURE(maybeReadTopic);

        auto readTopic = maybeReadTopic.Cast();
        const auto tokenName = "cluster:default_" + readTopic.DataSource().Cluster().StringValue();

        return Build<TYtflowReadWrap>(ctx, read->Pos())
            .Input(readTopic)
            .Token<TCoSecureParam>()
                .Name().Build(tokenName)
                .Build()
            .Done().Ptr();
    }

    TMaybe<bool> CanWrite(const TExprNode& node, TExprContext& /*ctx*/) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(&node);
        if (!maybeWriteTopic) {
            return Nothing();
        }
        return true;
    }

    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(write);
        YQL_ENSURE(maybeWriteTopic);

        auto writeTopic = maybeWriteTopic.Cast();
        const auto tokenName = "cluster:default_" + writeTopic.DataSink().Cluster().StringValue();

        return Build<TYtflowWriteWrap>(ctx, write->Pos())
            .Input(writeTopic)
            .Token<TCoSecureParam>()
                .Name().Build(tokenName)
                .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr GetReadWorld(const TExprNode& read, TExprContext& /*ctx*/) override {
        auto maybeReadTopic = TMaybeNode<TPqReadTopic>(&read);
        YQL_ENSURE(maybeReadTopic);
        return maybeReadTopic.Cast().World().Ptr();
    }

    TExprNode::TPtr GetWriteWorld(const TExprNode& write, TExprContext& /*ctx*/) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(&write);
        YQL_ENSURE(maybeWriteTopic);
        return maybeWriteTopic.Cast().World().Ptr();
    }

    TExprNode::TPtr UpdateWriteWorld(const TExprNode::TPtr& write, const TExprNode::TPtr& world, TExprContext& ctx) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(write);
        YQL_ENSURE(maybeWriteTopic);
        return Build<TPqWriteTopic>(ctx, write->Pos())
            .InitFrom(maybeWriteTopic.Cast())
            .World(world)
            .Done().Ptr();
    }

    TExprNode::TPtr GetWriteContent(const TExprNode& write, TExprContext& /*ctx*/) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(&write);
        YQL_ENSURE(maybeWriteTopic);
        return maybeWriteTopic.Cast().Input().Ptr();
    }

    TExprNode::TPtr UpdateWriteContent(
        const TExprNode::TPtr& write,
        const TExprNode::TPtr& content,
        TExprContext& ctx
    ) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(write);
        YQL_ENSURE(maybeWriteTopic);
        return Build<TPqWriteTopic>(ctx, write->Pos())
            .InitFrom(maybeWriteTopic.Cast())
            .Input(content)
            .Done().Ptr();
    }

    void FillSourceSettings(
        const TExprNode& source, ::google::protobuf::Any& settings, TExprContext& /*ctx*/
    ) override {
        auto maybeReadTopic = TMaybeNode<TPqReadTopic>(&source);
        YQL_ENSURE(maybeReadTopic);

        auto topic = maybeReadTopic.Cast().Topic();
        auto federatedClustersIterator = FindIf(topic.Props(), [](TCoNameValueTuple property) {
            return property.Name() == "FederatedClusters";
        });

        auto clusterInfoIterator = State_->Configuration->ClustersConfigurationSettings.find(topic.Cluster());
        YQL_ENSURE(clusterInfoIterator != State_->Configuration->ClustersConfigurationSettings.end());
        NYtflow::NProto::TPQSourceMessage sourceSettings;
        if (federatedClustersIterator != topic.Props().end()) {
            auto clusterList = (*federatedClustersIterator).Value().Cast<TDqPqFederatedClusterList>();
            for (auto cluster : clusterList) {
                sourceSettings.AddEndpoints(cluster.Endpoint().StringValue());
            }
        } else {
            sourceSettings.AddEndpoints(clusterInfoIterator->second.Endpoint);
        }
        sourceSettings.SetClusterType(clusterInfoIterator->second.ClusterType);
        sourceSettings.SetDatabase(topic.Database().StringValue());
        sourceSettings.SetTopicPath(topic.Path().StringValue());

        settings.PackFrom(sourceSettings);
    }

    void FillSinkSettings(
        const TExprNode& sink, ::google::protobuf::Any& settings, TExprContext& /*ctx*/
    ) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(&sink);
        YQL_ENSURE(maybeWriteTopic);

        auto topic = maybeWriteTopic.Cast().Topic();
        NYtflow::NProto::TPQSinkMessage sinkSettings;
        auto clusterInfoIterator = State_->Configuration->ClustersConfigurationSettings.find(topic.Cluster());
        YQL_ENSURE(clusterInfoIterator != State_->Configuration->ClustersConfigurationSettings.end());
        sinkSettings.SetCluster(clusterInfoIterator->second.Endpoint);
        sinkSettings.SetClusterType(clusterInfoIterator->second.ClusterType);
        sinkSettings.SetDatabase(topic.Database().StringValue());
        sinkSettings.SetTopicPath(topic.Path().StringValue());

        settings.PackFrom(sinkSettings);
    }

private:
    TPqState* State_;
};

THolder<IYtflowIntegration> CreatePqYtflowIntegration(const TPqState::TPtr& state) {
    YQL_ENSURE(state);
    return MakeHolder<TPqYtflowIntegration>(state);
}

} // namespace NYql
