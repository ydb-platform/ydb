// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"
#include "Aggregator.h"
#include <DataStreams/IBlockInputStream.h>


namespace CH
{

/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedBlockInputStream : public IBlockInputStream
{
public:
    MergingAggregatedBlockInputStream(const BlockInputStreamPtr & input, const Aggregator::Params & params, bool final_)
        : aggregator(params), final(final_)
    {
        children.push_back(input);
    }

    String getName() const override { return "MergingAggregated"; }

    Header getHeader() const override;

protected:
    Block readImpl() override;

private:
    Aggregator aggregator;
    bool final;

    bool executed = false;
    BlocksList blocks;
    BlocksList::iterator it;
};

}
