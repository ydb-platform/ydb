#include "chunked_array_serialized.h"

namespace NKikimr::NOlap::NChunks {

TChunkedArraySerialized::TChunkedArraySerialized(
    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& array, NArrow::NAccessor::TBlobWithAdditionalAccessorData&& blobAndMeta)
    : Array(array)
    , SerializedData(std::move(blobAndMeta.Blob))
    , Meta(std::move(blobAndMeta.Meta))
{
    YDB_LOG_VERIFY(SerializedData);
    YDB_LOG_VERIFY(Array);
    YDB_LOG_VERIFY(Array->GetRecordsCount());
    YDB_LOG_VERIFY(Meta);
}

}   // namespace NKikimr::NOlap::NChunks
