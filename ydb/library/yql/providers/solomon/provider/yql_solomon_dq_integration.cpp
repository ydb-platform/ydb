#include "yql_solomon_dq_integration.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NNodes;

namespace {

NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType) {
    switch (clusterType) {
        case TSolomonClusterConfig::SCT_SOLOMON:
            return NSo::NProto::ESolomonClusterType::CT_SOLOMON;
        case TSolomonClusterConfig::SCT_MONITORING:
            return NSo::NProto::ESolomonClusterType::CT_MONITORING;
        default:
            YQL_ENSURE(false, "Invalid cluster type " << ToString<ui32>(clusterType));
    }
}

const TTypeAnnotationNode* GetItemType(const TExprNode& node) {
    const TTypeAnnotationNode* typeAnn = node.GetTypeAnn();
    switch (typeAnn->GetKind()) {
        case ETypeAnnotationKind::Flow:
            return typeAnn->Cast<TFlowExprType>()->GetItemType();
        case ETypeAnnotationKind::Stream:
            return typeAnn->Cast<TStreamExprType>()->GetItemType();
        default: break;
    }
    YQL_ENSURE(false, "Invalid solomon sink type " << typeAnn->GetKind());
    return nullptr;
}

void FillScheme(const TTypeAnnotationNode& itemType, NSo::NProto::TDqSolomonShardScheme& scheme) {
    int index = 0;
    for (const TItemExprType* structItem : itemType.Cast<TStructExprType>()->GetItems()) {
        const auto itemName = structItem->GetName();
        const TDataExprType* itemType = nullptr;

        bool isOptionalUnused = false;
        YQL_ENSURE(IsDataOrOptionalOfData(structItem->GetItemType(), isOptionalUnused, itemType), "Failed to unwrap optional type");

        const auto dataType = NUdf::GetDataTypeInfo(itemType->GetSlot());

        NSo::NProto::TDqSolomonSchemeItem schemeItem;
        schemeItem.SetKey(TString(itemName));
        schemeItem.SetIndex(index++);
        schemeItem.SetDataTypeId(dataType.TypeId);

        if (dataType.Features & NUdf::DateType || dataType.Features & NUdf::TzDateType) {
            *scheme.MutableTimestamp() = schemeItem;
        } else if (dataType.Features & NUdf::NumericType) {
            scheme.MutableSensors()->Add(std::move(schemeItem));
        } else if (dataType.Features & NUdf::StringType) {
            scheme.MutableLabels()->Add(std::move(schemeItem));
        } else {
            YQL_ENSURE(false, "Invalid data type for monitoring sink: " << dataType.Name);
        }
    }
}

class TSolomonDqIntegration: public TDqIntegrationBase {
public:
    TSolomonDqIntegration(const TSolomonState::TPtr& state)
        : State_(state.Get())
    {
    }

    bool CanRead(const TExprNode&, TExprContext&, bool) override {
        YQL_ENSURE(false, "Unimplemented");
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>&, TExprContext&) override {
        YQL_ENSURE(false, "Unimplemented");
    }


    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr&, TExprContext&) override {
        YQL_ENSURE(false, "Unimplemented");
    }

    TMaybe<bool> CanWrite(const TExprNode&, TExprContext&) override {
        YQL_ENSURE(false, "Unimplemented");
    }

    void FillSourceSettings(const TExprNode&, ::google::protobuf::Any&, TString&, size_t) override {
        YQL_ENSURE(false, "Unimplemented");
    }

    void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sinkType) override {
        const auto maybeDqSink = TMaybeNode<TDqSink>(&node);
        if (!maybeDqSink) {
            return;
        }
        const auto dqSink = maybeDqSink.Cast();

        const auto settings = dqSink.Settings();
        const auto maybeShard = TMaybeNode<TSoShard>(settings.Raw());
        if (!maybeShard) {
            return;
        }

        const TSoShard shard = maybeShard.Cast();

        const auto solomonCluster = shard.SolomonCluster().StringValue();
        const auto* clusterDesc = State_->Configuration->ClusterConfigs.FindPtr(solomonCluster);
        YQL_ENSURE(clusterDesc, "Unknown cluster " << solomonCluster);

        NSo::NProto::TDqSolomonShard shardDesc;
        shardDesc.SetEndpoint(clusterDesc->GetCluster());
        shardDesc.SetProject(shard.Project().StringValue());
        shardDesc.SetCluster(shard.Cluster().StringValue());
        shardDesc.SetService(shard.Service().StringValue());

        shardDesc.SetClusterType(MapClusterType(clusterDesc->GetClusterType()));
        shardDesc.SetUseSsl(clusterDesc->GetUseSsl());

        const TTypeAnnotationNode* itemType = GetItemType(node);
        FillScheme(*itemType, *shardDesc.MutableScheme());

        if (auto maybeToken = shard.Token()) {
            shardDesc.MutableToken()->SetName(TString(maybeToken.Cast().Name().Value()));
        }

        protoSettings.PackFrom(shardDesc);
        sinkType = "SolomonSink";
    }

private:
    TSolomonState* State_; // State owns dq integration, so back reference must be not smart.
};

}

THolder<IDqIntegration> CreateSolomonDqIntegration(const TSolomonState::TPtr& state) {
    return MakeHolder<TSolomonDqIntegration>(state);
}

}
