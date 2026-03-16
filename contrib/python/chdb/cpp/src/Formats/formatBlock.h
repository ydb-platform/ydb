#pragma once
#include <memory>

namespace DB_CHDB
{

class Block;

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

void formatBlock(OutputFormatPtr out, const Block & block);

}
