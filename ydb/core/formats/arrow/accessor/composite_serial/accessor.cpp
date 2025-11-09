#include "accessor.h"

#include <ydb/core/formats/arrow/accessor/sparsed/constructor.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/prof/tag.h>

namespace NKikimr::NArrow::NAccessor {

IChunkedArray::TLocalChunkedArrayAddress TDeserializeChunkedArray::DoGetLocalChunkedArray(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    if (PredefinedArray) {
        return TLocalChunkedArrayAddress(PredefinedArray, 0, 0);
    }
    if (Counter.Inc() > 1) {
        AFL_WARN(NKikimrServices::ARROW_HELPER)("event", "many_deserializations")("counter", Counter.Val())("size", Data.size())(
            "buffer", DataBuffer.size());
    }
    if (!!Data) {
        auto result = Loader->ApplyConclusion(Data, GetRecordsCount());
        Y_ABORT_UNLESS(result, "Incorrect object for result request. internal path id: %s portion id: %ull error: %s ", InternalPathId.DebugString().data(), PortionId, result.GetErrorMessage().data());
        return TLocalChunkedArrayAddress(result.DetachResult(), 0, 0);
    } else {
        AFL_VERIFY(!!DataBuffer);
        auto result = Loader->ApplyConclusion(TString(DataBuffer.data(), DataBuffer.size()), GetRecordsCount());
        Y_ABORT_UNLESS(result, "Incorrect object for result request. internal path id: %s portion id: %ull error: %s ", InternalPathId.DebugString().data(), PortionId, result.GetErrorMessage().data());
        return TLocalChunkedArrayAddress(result.DetachResult(), 0, 0);
    }
}

}   // namespace NKikimr::NArrow::NAccessor
