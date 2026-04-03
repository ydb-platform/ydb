#include "execution.h"
#include "hierarchical_index.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> THierarchicalIndexCheckerProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto scalarConst = context.GetResources().GetConstantScalarVerified(GetInput().front().GetColumnId());

    auto source = context.GetDataSource().lock();
    if (!source) {
        return TConclusionStatus::Fail("source was destroyed before (hierarchical index check start)");
    }
    auto conclusion = source->CheckHierarchicalIndex(context, IndexContext, scalarConst);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    if (*conclusion) {
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
            NAccessor::TSparsedArray::BuildTrueArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
    } else {
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
            NAccessor::TSparsedArray::BuildFalseArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
    }
    return IResourceProcessor::EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
