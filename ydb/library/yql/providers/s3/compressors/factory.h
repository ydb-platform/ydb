#pragma once

#include "output_queue.h"

#if defined(_win_)

namespace NYql {

IOutputQueue::TPtr MakeCompressorQueue(const std::string_view& compression);

} // namespace NYql

#else

namespace NDB {

// forward declaration for <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
class ReadBuffer;

} // namespace NDB

namespace NYql {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& input, const std::string_view& compression);

IOutputQueue::TPtr MakeCompressorQueue(const std::string_view& compression);

} // namespace NYql

#endif
