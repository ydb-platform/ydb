#include "constructor.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NCommon {

void TExecutionContext::Stop() {
    ProgramIterator.reset();
    ExecutionVisitor.reset();
}

void TExecutionContext::Start(const std::shared_ptr<IDataSource>& source,
    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program, const TFetchingScriptCursor& step) {
    auto readMeta = source->GetContext()->GetCommonContext()->GetReadMetadata();
    NArrow::NSSA::TProcessorContext context(
        source, source->MutableStageData().ExtractTable(), readMeta->GetLimitRobustOptional(), readMeta->IsDescSorted());
    auto visitor = std::make_shared<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>(std::move(context));
    SetProgramIterator(program->BuildIterator(visitor), visitor);
    SetCursorStep(step);
}

TConclusion<bool> IDataSource::DoStartFetch(
    const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& fetchersExt) {
    std::vector<std::shared_ptr<IKernelFetchLogic>> fetchers;
    for (auto&& i : fetchersExt) {
        fetchers.emplace_back(std::static_pointer_cast<IKernelFetchLogic>(i));
    }
    if (fetchers.empty()) {
        return false;
    }
    return DoStartFetchImpl(context, fetchers);
}

}   // namespace NKikimr::NOlap::NReader::NCommon
