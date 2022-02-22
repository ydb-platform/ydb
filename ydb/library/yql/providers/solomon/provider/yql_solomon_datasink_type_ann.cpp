#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql {

using namespace NNodes;

class TSolomonDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TSolomonDataSinkTypeAnnotationTransformer(TSolomonState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TSolomonDataSinkTypeAnnotationTransformer;
        AddHandler({TSoWriteToShard::CallableName()}, Hndl(&TSelf::HandleWriteToShard));
        AddHandler({TSoShard::CallableName()}, Hndl(&TSelf::HandleSoShard));
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
    }

private:
    TStatus HandleWriteToShard(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 4, ctx)) {
            return TStatus::Error;
        }
        TSoWriteToShard write = input.Cast<TSoWriteToShard>();
        if (!EnsureWorldType(write.World().Ref(), ctx)) {
            return TStatus::Error;
        }
        if (!EnsureSpecificDataSink(write.DataSink().Ref(), SolomonProviderName, ctx)) {
            return TStatus::Error;
        }
        if (!EnsureAtom(write.Shard().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!State_->IsRtmrMode()) {
            const TTypeAnnotationNode* inputItemType;
            if (!EnsureNewSeqType<true, true, true>(write.Input().Pos(), *write.Input().Ref().GetTypeAnn(), ctx, &inputItemType)) {
                return TStatus::Error;
            }

            if (!EnsureStructType(write.Input().Pos(), *inputItemType, ctx)) {
                return TStatus::Error;
            }

            auto structType = inputItemType->Cast<TStructExprType>();

            bool hasTimestampMember = false;
            ui32 labelMembers = 0;
            ui32 sensorMembers = 0;

            for (auto* structItem : structType->GetItems()) {
                const auto itemName = structItem->GetName();
                const auto* itemType = structItem->GetItemType();
                YQL_ENSURE(
                    itemType->GetKind() != ETypeAnnotationKind::Optional,
                    "Optional types are not supported in monitoring sink. FieldName: " << itemName);

                const auto dataType = NUdf::GetDataTypeInfo(itemType->Cast<TDataExprType>()->GetSlot());

                if (dataType.Features & NUdf::DateType || dataType.Features & NUdf::TzDateType) {
                    YQL_ENSURE(!hasTimestampMember, "Multiple timestamps were provided for monitoing sink");
                    hasTimestampMember = true;
                } else if (dataType.Features & NUdf::StringType) {
                    labelMembers++;
                } else if (dataType.Features & NUdf::NumericType) {
                    sensorMembers++;
                } else {
                    YQL_ENSURE(false, "Ivalid data type for monitoing sink: " << dataType.Name);
                }
            }

            YQL_ENSURE(hasTimestampMember, "Timestamp wasn't provided for monitoing sink");
            YQL_ENSURE(sensorMembers != 0, "No sensors were provided for monitoing sink");

            YQL_ENSURE(labelMembers <= SolomonMaxLabelsCount,
                "Max labels count is " << SolomonMaxLabelsCount << " but " << labelMembers << " were provided");
            YQL_ENSURE(sensorMembers <= SolomonMaxSensorsCount,
                "Max sensors count is " << SolomonMaxSensorsCount << " but " << sensorMembers << " were provided");
        }

        input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleSoShard(TExprBase input, TExprContext& ctx) {
        YQL_ENSURE(!State_->IsRtmrMode(), "SoShard can't be used in rtmr mode");

        if (!EnsureMinArgsCount(input.Ref(), 4, ctx) || !EnsureMaxArgsCount(input.Ref(), 5, ctx)) {
            return TStatus::Error;
        }

        const TSoShard shard = input.Cast<TSoShard>();

        if (!EnsureAtom(shard.SolomonCluster().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(shard.Project().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(shard.Cluster().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(shard.Service().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (shard.Token() && !EnsureCallable(shard.Token().Ref(), ctx)) {
            return TStatus::Error;
        }

        auto clusterType = shard.SolomonCluster().StringValue();
        if (State_->Configuration->ClusterConfigs.at(clusterType).GetClusterType() == TSolomonClusterConfig::SCT_MONITORING) {
            YQL_ENSURE(shard.Service().StringValue() ==  "custom", "Monitoring allows writing only to 'custom' service");
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
        Y_UNUSED(ctx);
        auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TSolomonState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateSolomonDataSinkTypeAnnotationTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonDataSinkTypeAnnotationTransformer(state));
}

} // namespace NYql
