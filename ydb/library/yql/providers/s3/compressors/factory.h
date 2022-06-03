#pragma once

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>

namespace NYql {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& input, const std::string_view& compression);

}
