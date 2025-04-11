#include "result.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NSimple {

ISyncPoint::ESourceAction TSyncPointResult::OnSourceReady(const std::shared_ptr<IDataSource>& source, TPlainReadData& reader) {
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
        auto cursor = Collection->BuildCursor(source, resultChunk->GetStartIndex() + resultChunk->GetRecordsCount());
        reader.OnIntervalResult(std::make_shared<TPartialReadResult>(source->GetResourceGuards(), source->GetGroupGuard(),
            resultChunk->GetTable(), cursor, Context->GetCommonContext(), partialSourceAddress));
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

}   // namespace NKikimr::NOlap::NReader::NSimple
