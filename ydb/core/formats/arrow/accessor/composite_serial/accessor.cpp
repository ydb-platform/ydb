#include "accessor.h"

#include <ydb/core/formats/arrow/accessor/sparsed/constructor.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/prof/tag.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::ARROW_HELPER

namespace NKikimr::NArrow::NAccessor {

IChunkedArray::TLocalChunkedArrayAddress TDeserializeChunkedArray::DoGetLocalChunkedArray(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    if (PredefinedArray) {
        return TLocalChunkedArrayAddress(PredefinedArray, 0, 0);
    }
    if (Counter.Inc() > 1) {
        YDB_LOG_WARN("",
            {"event", "many_deserializations"},
            {"counter", Counter.Val()},
            {"size", Data.size()},
            {"buffer", DataBuffer.size()});
    }
    if (!!Data) {
        auto result = Loader->ApplyConclusion(Data, GetRecordsCount(), std::nullopt, AdditionalAccessorData);
        AFL_VERIFY(result.IsSuccess())("event", "deserialization_error")("error", result.GetErrorMessage());
        return TLocalChunkedArrayAddress(result.DetachResult(), 0, 0);
    } else {
        AFL_VERIFY(!!DataBuffer);
        auto result = Loader->ApplyConclusion(TString(DataBuffer.data(), DataBuffer.size()), GetRecordsCount(), std::nullopt, AdditionalAccessorData);
        AFL_VERIFY(result.IsSuccess())("event", "deserialization_error")("error", result.GetErrorMessage());
        return TLocalChunkedArrayAddress(result.DetachResult(), 0, 0);
    }
}

}   // namespace NKikimr::NArrow::NAccessor
