#pragma once

#include "output_queue.h"

namespace NDB {

class ReadBuffer;

} // namespace NDB

namespace NYql::NBzip2 {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& source);

IOutputQueue::TPtr MakeCompressor(std::optional<int> blockSize100k = {});

} // namespace NYql::NBzip2
