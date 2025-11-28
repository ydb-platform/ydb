#pragma once

#include "output_queue.h"

namespace NDB {

// forward declaration for <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
class ReadBuffer;

} // namespace NDB

namespace NYql::NLz4 {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& source);

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel = {});

} // namespace NYql::NLz4
