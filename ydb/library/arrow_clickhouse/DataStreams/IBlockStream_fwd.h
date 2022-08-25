// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once

#include <memory>
#include <vector>

namespace CH
{

class IBlockInputStream;
class IBlockOutputStream;

using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;
using BlockOutputStreams = std::vector<BlockOutputStreamPtr>;

}
