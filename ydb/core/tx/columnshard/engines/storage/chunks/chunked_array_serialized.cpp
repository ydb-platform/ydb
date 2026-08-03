#include "chunked_array_serialized.h"

namespace NKikimr::NOlap::NChunks {

TChunkedArraySerialized::TChunkedArraySerialized(
    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& array, NArrow::NAccessor::TBlobWithAdditionalAccessorData&& blobAndMeta)
    : Array(array)
    , SerializedData(std::move(blobAndMeta.Blob))
    , Meta(std::move(blobAndMeta.Meta))
{
    AFL_VERIFY(SerializedData);
    AFL_VERIFY(Array);
    AFL_VERIFY(Array->GetRecordsCount());
    AFL_VERIFY(Meta);
}

}   // namespace NKikimr::NOlap::NChunks
