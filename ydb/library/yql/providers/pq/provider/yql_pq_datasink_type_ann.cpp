#include "yql_pq_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {
bool EnsureStructTypeWithSingleStringMember(const TTypeAnnotationNode* input, TPositionHandle pos, TExprContext& ctx) {
    YQL_ENSURE(input);
    auto itemSchema = input->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    if (itemSchema->GetSize() != 1) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "only struct with single string, yson or json field is accepted, but has struct with " << itemSchema->GetSize() << " members"));
        return false;
    }

    auto column = itemSchema->GetItems()[0];
    auto columnType = column->GetItemType();
    if (columnType->GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Column " << column->GetName() << " must have a data type, but has " << columnType->GetKind()));
        return false;
    }

    auto columnDataType = columnType->Cast<TDataExprType>();
    auto dataSlot = columnDataType->GetSlot();

    if (dataSlot != NUdf::EDataSlot::String &&
        dataSlot != NUdf::EDataSlot::Yson &&
        dataSlot != NUdf::EDataSlot::Json) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Column " << column->GetName() << " is not a string, yson or json, but " << NUdf::GetDataTypeInfo(dataSlot).Name));
        return false;
    }
    return true;
}

class TPqDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TPqDataSinkTypeAnnotationTransformer(TPqState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TPqDataSinkTypeAnnotationTransformer;
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
        AddHandler({TPqWriteTopic::CallableName() }, Hndl(&TSelf::HandleWriteTopic));
        AddHandler({NNodes::TPqClusterConfig::CallableName() }, Hndl(&TSelf::HandleClusterConfig));
        AddHandler({TDqPqTopicSink::CallableName()}, Hndl(&TSelf::HandleDqPqTopicSink));
    }

    TStatus HandleCommit(TExprBase input, TExprContext&) {
        const auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleWriteTopic(TExprBase input, TExprContext& ctx) {
        const auto write = input.Cast<TPqWriteTopic>();
        const auto& writeInput = write.Input().Ref();
        if (!EnsureStructTypeWithSingleStringMember(writeInput.GetTypeAnn(), writeInput.Pos(), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleClusterConfig(TExprBase input, TExprContext& ctx) {
        const auto config = input.Cast<NNodes::TPqClusterConfig>();
        if (!EnsureAtom(config.Endpoint().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(config.TvmId().Ref(), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleDqPqTopicSink(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

private:
    TPqState::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreatePqDataSinkTypeAnnotationTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqDataSinkTypeAnnotationTransformer>(state);
}

} // namespace NYql
