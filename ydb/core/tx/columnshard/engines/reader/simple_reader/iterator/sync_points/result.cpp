#include "result.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>

namespace NKikimr::NOlap::NReader::NSimple {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

bool TSyncPointResult::IsSourcePrepared(const std::shared_ptr<NCommon::IDataSource>& source) const {
    if (!Next) {
        return source->IsSyncSection() && source->HasStageResult() &&
               (source->GetStageResult().HasResultChunk() || source->GetStageResult().IsEmpty());
    } else if (source->IsSyncSection()) {
        AFL_VERIFY(source->HasStageData() || (source->HasStageResult() && source->GetStageResult().IsEmpty()));
        return true;
    } else {
        return false;
    }
}

ISyncPoint::ESourceAction TSyncPointResult::OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& reader) {
    const ui32 resultChunkRowsCount = (source->HasStageResult() && !source->GetStageResult().IsEmpty())
        ? source->GetStageResult().GetResultChunkRowsCount()
        : 0;
    LWTRACK(ResultSyncPoint, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), GetPointName(), source->GetFilteredRowsCount(), resultChunkRowsCount,
            source->GetReservedMemory(), source->GetSourcesAheadQueueWaitDuration(), source->GetSourcesAhead(), DebugString());
    if (Next) {
        if (source->HasStageResult() && source->GetStageResult().IsEmpty()) {
            return ESourceAction::Finish;
        }
        if (source->HasStageData() && !source->GetStageData().GetTable().HasSomeUsefulInfo()) {
            return ESourceAction::Finish;
        }
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
                partialSourceAddress = TPartialSourceAddress(source->GetSourceIdx(), GetPointIndex());
            }
            
            // Track page creation for backpressure
            const bool isStreamingMode = source->GetAs<IDataSource>()->IsStreamingMode();
            if (isStreamingMode) {
                Collection->OnPageCreated();
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "page_created")
                    ("source_idx", source->GetSourceIdx())("pages_in_flight", Collection->GetPagesInFlightCount());
            }
            
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_idx", source->GetSourceIdx())
                ("table", resultChunk->GetTable()->num_rows())("is_finished", isFinished)("streaming", isStreamingMode);
            auto cursor = Collection->BuildCursor(source, resultChunk->GetStartIndex() + resultChunk->GetRecordsCount(),
                Context->GetCommonContext()->GetReadMetadata()->GetTabletId());
            reader.OnIntervalResult(
                std::make_unique<TPartialReadResult>(source->GetResourceGuards(), source->MutableAs<IDataSource>()->GetGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), partialSourceAddress, source->GetDeprecatedPortionId()));
        } else if (!isFinished) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "continue_source")
                ("source_idx", source->GetSourceIdx())("is_finished", isFinished);
            source->MutableAs<IDataSource>()->ContinueCursor(source);
        }
        if (!isFinished) {
            return ESourceAction::Wait;
        }
        source->MutableAs<IDataSource>()->ClearResult();
        return ESourceAction::ProvideNext;
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
