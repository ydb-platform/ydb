#pragma once
#include <ydb/services/deprecated/persqueue_v0/api/protos/persqueue.pb.h>

namespace NPersQueue {

// Converts responses with BatchedData field to responses with Data field.
// Other responses will be leaved unchanged.
void ConvertToOldBatch(ReadResponse& response);

} // namespace NPersQueue
