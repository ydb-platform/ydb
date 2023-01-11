// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"
#include <DataStreams/IBlockInputStream.h>

namespace CH
{

/** A stream of blocks from which you can read one block.
  * Also see BlocksListBlockInputStream.
  */
class OneBlockInputStream : public IBlockInputStream
{
public:
    explicit OneBlockInputStream(Block block_)
        : block(std::move(block_))
    {
        auto status = block->Validate();
        if (!status.ok())
            throw Exception(std::string("Bad batch in OneBlockInputStream: ") + status.ToString());
    }

    String getName() const override { return "One"; }

    Header getHeader() const override
    {
        if (!block)
            return {};
        return block->schema();
    }

protected:
    Block readImpl() override
    {
        if (has_been_read)
            return {};

        has_been_read = true;
        return block;
    }

private:
    Block block{};
    bool has_been_read = false;
};

}
