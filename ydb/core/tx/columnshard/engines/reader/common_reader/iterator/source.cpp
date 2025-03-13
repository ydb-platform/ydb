#include "source.h"

namespace NKikimr::NOlap::NReader::NCommon {

void TExecutionContext::Start(const std::shared_ptr<IDataSource>& source,
    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program, const TFetchingScriptCursor& step) {
    auto readMeta = source->GetContext()->GetCommonContext()->GetReadMetadata();
    NArrow::NSSA::TProcessorContext context(
        source, source->GetStageData().GetTable(), readMeta->GetLimitRobustOptional(), readMeta->IsDescSorted());
    auto visitor = std::make_shared<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>(context);
    SetProgramIterator(program->BuildIterator(visitor), visitor);
    SetCursorStep(step);
}

}   // namespace NKikimr::NOlap::NReader::NCommon
