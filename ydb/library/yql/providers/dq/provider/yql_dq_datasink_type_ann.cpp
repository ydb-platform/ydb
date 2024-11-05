#include "yql_dq_datasink_type_ann.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

namespace NYql {

using namespace NNodes;

namespace {

class TDqsDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TDqsDataSinkTypeAnnotationTransformer(TTypeAnnotationContext* typeCtx, bool enableDqReplicate)
        : TVisitorTransformerBase(true), TypeCtx(typeCtx)
    {
        AddHandler({TDqStage::CallableName()}, Hndl(&NDq::AnnotateDqStage));
        AddHandler({TDqPhyStage::CallableName()}, Hndl(&NDq::AnnotateDqPhyStage));
        AddHandler({TDqOutput::CallableName()}, Hndl(&NDq::AnnotateDqOutput));
        AddHandler({TDqCnUnionAll::CallableName()}, Hndl(&NDq::AnnotateDqConnection));
        AddHandler({TDqCnStreamLookup::CallableName()}, Hndl(&NDq::AnnotateDqCnStreamLookup));
        AddHandler({TDqCnHashShuffle::CallableName()}, Hndl(&NDq::AnnotateDqCnHashShuffle));
        AddHandler({TDqCnResult::CallableName()}, Hndl(&NDq::AnnotateDqCnResult));
        AddHandler({TDqCnMap::CallableName()}, Hndl(&NDq::AnnotateDqConnection));
        AddHandler({TDqCnBroadcast::CallableName()}, Hndl(&NDq::AnnotateDqConnection));
        AddHandler({TDqCnValue::CallableName()}, Hndl(&NDq::AnnotateDqCnValue));
        AddHandler({TDqCnMerge::CallableName()}, Hndl(&NDq::AnnotateDqCnMerge));
        if (enableDqReplicate) {
            AddHandler({TDqReplicate::CallableName()}, Hndl(&NDq::AnnotateDqReplicate));
        } else {
            AddHandler({TDqReplicate::CallableName()}, Hndl(&TDqsDataSinkTypeAnnotationTransformer::AnnotateDqReplicateAlwaysError));
        }
        AddHandler({TDqJoin::CallableName()}, Hndl(&NDq::AnnotateDqJoin));
        AddHandler({TDqPhyGraceJoin::CallableName()}, Hndl(&NDq::AnnotateDqMapOrDictJoin));
        AddHandler({TDqPhyMapJoin::CallableName()}, Hndl(&NDq::AnnotateDqMapOrDictJoin));
        AddHandler({TDqPhyCrossJoin::CallableName()}, Hndl(&NDq::AnnotateDqCrossJoin));
        AddHandler({TDqPhyJoinDict::CallableName()}, Hndl(&NDq::AnnotateDqMapOrDictJoin));
        AddHandler({TDqSink::CallableName()}, Hndl(&NDq::AnnotateDqSink));
        AddHandler({TDqWrite::CallableName()}, Hndl(&TDqsDataSinkTypeAnnotationTransformer::AnnotateDqWrite));
        AddHandler({TDqQuery::CallableName()}, Hndl(&NDq::AnnotateDqQuery));
        AddHandler({TDqPrecompute::CallableName()}, Hndl(&NDq::AnnotateDqPrecompute));
        AddHandler({TDqPhyPrecompute::CallableName()}, Hndl(&NDq::AnnotateDqPhyPrecompute));
        AddHandler({TDqTransform::CallableName()}, Hndl(&NDq::AnnotateDqTransform));
    }

private:
    TStatus AnnotateDqReplicateAlwaysError(const TExprNode::TPtr& input, TExprContext& ctx) {
        ctx.AddError(YqlIssue(ctx.GetPosition(input->Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, "Reading multiple times from the same source is not supported"));
        return TStatus::Error;
    }

    TStatus AnnotateDqWrite(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureMaxArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureNewSeqType<false, false, true>(input->Head(), ctx)){
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx)) {
            return TStatus::Error;
        }

        auto providerName = TString(input->Child(1)->Content());

        if (!TypeCtx->DataSinkMap.FindPtr(providerName)) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "No datasink defined for provider name " << providerName));
            return TStatus::Error;
        }

        providerName.front() = std::toupper(providerName.front());
        output = ctx.NewCallable(input->Pos(), providerName += TDqWrite::CallableName(), {input->HeadPtr(), input->TailPtr()});
        return TStatus::Repeat;
    }

    TTypeAnnotationContext* TypeCtx;
};

} // unnamed

THolder<TVisitorTransformerBase> CreateDqsDataSinkTypeAnnotationTransformer(TTypeAnnotationContext* typeCtx, bool enableDqReplicate) {
    return THolder(new TDqsDataSinkTypeAnnotationTransformer(typeCtx, enableDqReplicate));
}

} // NYql
