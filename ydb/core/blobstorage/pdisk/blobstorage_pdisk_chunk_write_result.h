#pragma once
#include "blobstorage_pdisk_writer.h"

namespace NKikimr {
namespace NPDisk {

class TChunkWriteResult {
public:
    THolder<TChunkWriter> ChunkWriter;
    bool LastPart;
    TChunkWriteResult(THolder<TChunkWriter>&& chunkWriter, bool lastPart) : ChunkWriter(std::move(chunkWriter)), LastPart(lastPart) {}
};

} // NPDisk
} // NKikimr
