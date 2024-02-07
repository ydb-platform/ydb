#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
namespace NDB
{

std::pair<bool, size_t> fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, Memory<> & memory, size_t min_chunk_size);

bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf);

}
