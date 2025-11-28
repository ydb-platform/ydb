#include "yql_solomon_provider_impl.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
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
        AddHandler({TSoInsert::CallableName()}, Hndl(&TSelf::HandleInsert));
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
            const auto& writeInput = write.Input().Ref();
            const auto inputPos = writeInput.Pos();
            const TTypeAnnotationNode* inputItemType = nullptr;
            if (!EnsureNewSeqType<true, true, true>(inputPos, *writeInput.GetTypeAnn(), ctx, &inputItemType)) {
                return TStatus::Error;
            }

            if (!ValidateWriteTypeAnnotation(inputPos, inputItemType, ctx)) {
                return TStatus::Error;
            }
        }

        input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleSoShard(TExprBase input, TExprContext& ctx) {
        YQL_ENSURE(!State_->IsRtmrMode(), "SoShard can't be used in rtmr mode");

        if (!EnsureMinMaxArgsCount(input.Ref(), 5, 6, ctx)) {
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

        if (!EnsureType(shard.RowType().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (shard.Token() && !EnsureCallable(shard.Token().Ref(), ctx)) {
            return TStatus::Error;
        }

        auto clusterType = shard.SolomonCluster().StringValue();
        if (State_->Configuration->ClusterConfigs.at(clusterType).GetClusterType() == TSolomonClusterConfig::SCT_MONITORING) {
            if (shard.Service().StringValue() != "custom") {
                ctx.AddError(TIssue(ctx.GetPosition(shard.SolomonCluster().Pos()), TStringBuilder() << "It is not allowed to write into Monitoring service '" << shard.Service().StringValue() << "'. Use service 'custom' instead"));
                return TStatus::Error;
            }
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

    static TStatus HandleInsert(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 4U, ctx)) {
            return TStatus::Error;
        }

        const auto insert = input.Cast<TSoInsert>();
        if (!EnsureWorldType(insert.World().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSink(insert.DataSink().Ref(), SolomonProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(insert.Shard().Ref(), ctx)) {
            return TStatus::Error;
        }

        const auto& insertInput = insert.Input().Ref();
        const auto inputPos = insertInput.Pos();
        const TTypeAnnotationNode* inputItemType = nullptr;
        if (!EnsureNewSeqType<true, true, true>(inputPos, *insertInput.GetTypeAnn(), ctx, &inputItemType)) {
            return TStatus::Error;
        }

        if (!ValidateWriteTypeAnnotation(inputPos, inputItemType, ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TListExprType>(inputItemType)
        }));
        return TStatus::Ok;
    }

    static bool ValidateWriteTypeAnnotation(TPositionHandle position, const TTypeAnnotationNode* inputItemType, TExprContext& ctx) {
        if (!EnsureStructType(position, *inputItemType, ctx)) {
            return false;
        }

        auto structType = inputItemType->Cast<TStructExprType>();

        bool hasTimestampMember = false;
        ui32 labelMembers = 0;
        ui32 sensorMembers = 0;

        for (auto* structItem : structType->GetItems()) {
            const auto itemName = structItem->GetName();
            const TDataExprType* itemType = nullptr;

            bool isOptional = false;
            if (!IsDataOrOptionalOfData(structItem->GetItemType(), isOptional, itemType)) {
                return false;
            }

            const auto dataType = NUdf::GetDataTypeInfo(itemType->GetSlot());

            if (dataType.Features & NUdf::DateType || dataType.Features & NUdf::TzDateType) {
                if (hasTimestampMember) {
                    ctx.AddError(TIssue(ctx.GetPosition(position), "Multiple timestamps should not be used when writing into Monitoring"));
                    return false;
                }
                hasTimestampMember = true;
                continue;
            }

            if (isOptional) {
                ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Optional types for labels and metric values are not supported in writing into Monitoring. FieldName: " << itemName));
                return false;
            }
            
            if (dataType.Features & NUdf::StringType) {
                labelMembers++;
            } else if (dataType.Features & NUdf::NumericType) {
                sensorMembers++;
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Field " << itemName << " of type " << dataType.Name << " could not be written into Monitoring"));
                return false;
            }
        }

        if (!hasTimestampMember) {
            ctx.AddError(TIssue(ctx.GetPosition(position), "Timestamp wasn't provided for Monitoring"));
            return false;
        }

        if (!sensorMembers) {
            ctx.AddError(TIssue(ctx.GetPosition(position), "No sensors were provided for Monitoring"));
            return false;
        }

        if (labelMembers > SolomonMaxLabelsCount) {
            ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Max labels count is " << SolomonMaxLabelsCount << " but " << labelMembers << " were provided"));
            return false;
        }

        if (sensorMembers > SolomonMaxSensorsCount) {
            ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Max sensors count is " << SolomonMaxSensorsCount << " but " << sensorMembers << " were provided"));
            return false;
        }

        return true;
    }

    TSolomonState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateSolomonDataSinkTypeAnnotationTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonDataSinkTypeAnnotationTransformer(state));
}

} // namespace NYql
