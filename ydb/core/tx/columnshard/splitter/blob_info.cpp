#include "blob_info.h"

namespace NKikimr::NOlap {

void TSplittedBlob::Take(const std::shared_ptr<IPortionDataChunk>& chunk) {
    Chunks.emplace_back(chunk);
    Size += chunk->GetPackedSize();
}
}
