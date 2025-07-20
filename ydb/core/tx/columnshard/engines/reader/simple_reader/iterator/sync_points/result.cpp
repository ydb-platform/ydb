#include "result.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NSimple {

ISyncPoint::ESourceAction TSyncPointResult::OnSourceReady(const std::shared_ptr<IDataSource>& source, TPlainReadData& reader) {
    if (Next) {
        AFL_VERIFY(source->GetStageData().GetTable()->HasData() && !source->GetStageData().GetTable()->HasDataAndResultIsEmpty());
        return ESourceAction::ProvideNext;
    } else {
        if (source->GetStageResult().IsEmpty()) {
            return ESourceAction::Finish;
        }
        auto resultChunk = source->MutableStageResult().ExtractResultChunk();
        const bool isFinished = source->GetStageResult().IsFinished();
        if (resultChunk && resultChunk->HasData()) {
            std::optional<TPartialSourceAddress> partialSourceAddress;
            if (!isFinished) {
                partialSourceAddress = TPartialSourceAddress(source->GetSourceId(), source->GetSourceIdx(), GetPointIndex());
            }
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_id", source->GetSourceId())(
                "source_idx", source->GetSourceIdx())("table", resultChunk->GetTable()->num_rows())("is_finished", isFinished);
            auto cursor = Collection->BuildCursor(source, resultChunk->GetStartIndex() + resultChunk->GetRecordsCount(),
                Context->GetCommonContext()->GetReadMetadata()->GetTabletId());
            reader.OnIntervalResult(std::make_unique<TPartialReadResult>(source->ExtractResourceGuards(), source->ExtractGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), partialSourceAddress));
        } else if (!isFinished) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "continue_source")("source_id", source->GetSourceId())(
                "source_idx", source->GetSourceIdx());
            source->ContinueCursor(source);
        }
        if (!isFinished) {
            return ESourceAction::Wait;
        }
        source->ClearResult();
        return ESourceAction::ProvideNext;
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
