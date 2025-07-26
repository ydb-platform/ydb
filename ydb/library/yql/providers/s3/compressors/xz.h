#pragma once

#include "output_queue.h"

namespace NDB {

class ReadBuffer;

} // namespace NDB

namespace NYql::NXz {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& source);

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel = {});

} // namespace NYql::NXz
