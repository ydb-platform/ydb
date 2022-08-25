// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include "Aggregator.h"
#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/IBlockInputStream.h>

namespace CH
{

/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
  * Columns with aggregate functions adds to the end of the block.
  * If final = false, the aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  */
class AggregatingBlockInputStream : public IBlockInputStream
{
public:
    /** keys are taken from the GROUP BY part of the query
      * Aggregate functions are searched everywhere in the expression.
      * Columns corresponding to keys and arguments of aggregate functions must already be computed.
      */
    AggregatingBlockInputStream(const BlockInputStreamPtr & input, const Aggregator::Params & params_, bool final_)
        : params(params_), aggregator(params), final(final_)
    {
        children.push_back(input);
    }

    String getName() const override { return "Aggregating"; }
    Header getHeader() const override;

protected:
    Block readImpl() override;

    Aggregator::Params params;
    Aggregator aggregator;
    bool final;

    bool executed = false;

     /** From here we will get the completed blocks after the aggregation. */
    std::unique_ptr<IBlockInputStream> impl;
};

}
