#include "blob_info.h"

namespace NKikimr::NOlap {

void TSplittedBlob::Take(const IPortionColumnChunk::TPtr& chunk) {
    Chunks.emplace_back(chunk);
    Size += chunk->GetPackedSize();
}

}
