#include "yql_solomon_ytflow_integration.h"

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/proto/solomon.pb.h>

#include <library/cpp/monlib/metrics/metric_type.h>

#include <google/protobuf/any.pb.h>

#include <util/string/split.h>

namespace NYql {

using namespace NNodes;

class TSolomonYtflowIntegration : public TEmptyYtflowIntegration {
public:
    TSolomonYtflowIntegration(const TSolomonState::TPtr& state)
        : State_(state.Get())
    {
    }

    TMaybe<bool> CanRead(const TExprNode& /*node*/, TExprContext& /*ctx*/) override {
        return Nothing();
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& /*read*/, TExprContext& /*ctx*/) override {
        YQL_ENSURE(false, "Method " << __FUNCTION__ << " is not implemented");
    }

    TMaybe<bool> CanWrite(const TExprNode& node, TExprContext& /*ctx*/) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(&node);
        if (!maybeWriteToShard) {
            return Nothing();
        }
        return true;
    }

    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(write);
        YQL_ENSURE(maybeWriteToShard);

        auto writeToShard = maybeWriteToShard.Cast();
        const auto tokenName = "cluster:default_" + writeToShard.DataSink().Cluster().StringValue();

        return Build<TYtflowWriteWrap>(ctx, write->Pos())
            .Input(writeToShard)
            .Token<TCoSecureParam>()
                .Name().Build(tokenName)
                .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr GetReadWorld(const TExprNode& /*read*/, TExprContext& /*ctx*/) override {
        YQL_ENSURE(false, "Method " << __FUNCTION__ << " is not implemented");
    }

    TExprNode::TPtr GetWriteWorld(const TExprNode& write, TExprContext& /*ctx*/) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(&write);
        YQL_ENSURE(maybeWriteToShard);
        return maybeWriteToShard.Cast().World().Ptr();
    }

    TExprNode::TPtr UpdateWriteWorld(const TExprNode::TPtr& write, const TExprNode::TPtr& world, TExprContext& ctx) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(write);
        YQL_ENSURE(maybeWriteToShard);
        return Build<TSoWriteToShard>(ctx, write->Pos())
            .InitFrom(maybeWriteToShard.Cast())
            .World(world)
            .Done().Ptr();
    }

    TExprNode::TPtr GetWriteContent(const TExprNode& write, TExprContext& /*ctx*/) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(&write);
        YQL_ENSURE(maybeWriteToShard);
        return maybeWriteToShard.Cast().Input().Ptr();
    }

    TExprNode::TPtr UpdateWriteContent(
        const TExprNode::TPtr& write,
        const TExprNode::TPtr& content,
        TExprContext& ctx
    ) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(write);
        YQL_ENSURE(maybeWriteToShard);
        return Build<TSoWriteToShard>(ctx, write->Pos())
            .InitFrom(maybeWriteToShard.Cast())
            .Input(content)
            .Done().Ptr();
    }

    void FillSourceSettings(
        const TExprNode& /*source*/, ::google::protobuf::Any& /*settings*/, TExprContext& /*ctx*/
    ) override {
        YQL_ENSURE(false, "Method " << __FUNCTION__ << " is not implemented");
    }

    void FillSinkSettings(
        const TExprNode& sink, ::google::protobuf::Any& settings, TExprContext& /*ctx*/
    ) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(&sink);
        YQL_ENSURE(maybeWriteToShard);

        auto writeToShard = maybeWriteToShard.Cast();
        auto* listType = writeToShard.Input().Ref().GetTypeAnn();
        auto* itemType = listType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        NYtflow::NProto::TSolomonSinkMessage sinkSettings;
        for (auto* structItem : itemType->GetItems()) {
            TString itemName(structItem->GetName());
            const TDataExprType* itemType = nullptr;

            bool isOptional = false;
            if (!IsDataOrOptionalOfData(structItem->GetItemType(), isOptional, itemType)) {
                continue;
            }

            const auto dataType = NUdf::GetDataTypeInfo(itemType->GetSlot());

            switch (dataType.TypeId) {
                case NUdf::TDataType<NUdf::TTimestamp>::Id:
                    sinkSettings.SetMetricTimestampColumn(itemName);
                    break;
                case NUdf::TDataType<char*>::Id:
                    sinkSettings.AddLabelColumns(itemName);
                    break;
                case NUdf::TDataType<ui8>::Id:
                case NUdf::TDataType<ui16>::Id:
                case NUdf::TDataType<ui32>::Id:
                case NUdf::TDataType<ui64>::Id: {
                    auto* metric = sinkSettings.AddMetrics();
                    metric->SetMetricValueColumn(itemName);
                    metric->SetMetricType(TString(
                        NMonitoring::MetricTypeToStr(NMonitoring::EMetricType::COUNTER)));
                    break;
                }
                case NUdf::TDataType<i8>::Id:
                case NUdf::TDataType<i16>::Id:
                case NUdf::TDataType<i32>::Id:
                case NUdf::TDataType<i64>::Id: {
                    auto* metric = sinkSettings.AddMetrics();
                    metric->SetMetricValueColumn(itemName);
                    metric->SetMetricType(TString(
                        NMonitoring::MetricTypeToStr(NMonitoring::EMetricType::IGAUGE)));
                    break;
                }
                case NUdf::TDataType<float>::Id:
                case NUdf::TDataType<double>::Id: {
                    auto* metric = sinkSettings.AddMetrics();
                    metric->SetMetricValueColumn(itemName);
                    metric->SetMetricType(TString(
                        NMonitoring::MetricTypeToStr(NMonitoring::EMetricType::GAUGE)));
                    break;
                }
            }
        }

        auto dataSink = writeToShard.DataSink();
        const auto* clusterSettings = State_->Configuration->ClusterConfigs.FindPtr(dataSink.Cluster());
        YQL_ENSURE(clusterSettings, "Unknown cluster name: " << dataSink.Cluster().StringValue());
        TString endpoint = (clusterSettings->GetUseSsl() ? "https://" : "http://") + clusterSettings->GetCluster();
        sinkSettings.SetEndpoint(std::move(endpoint));
        auto shardPath = StringSplitter(writeToShard.Shard().StringValue())
            .Split('/')
            .SkipEmpty()
            .ToList<TString>();
        YQL_ENSURE(shardPath.size() == 3);
        sinkSettings.SetProject(shardPath[0]);
        sinkSettings.SetCluster(shardPath[1]);
        sinkSettings.SetService(shardPath[2]);

        settings.PackFrom(sinkSettings);
    }

private:
    TSolomonState* State_;
};

THolder<IYtflowIntegration> CreateSolomonYtflowIntegration(const TSolomonState::TPtr& state) {
    YQL_ENSURE(state);
    return MakeHolder<TSolomonYtflowIntegration>(state);
}

} // namespace NYql
