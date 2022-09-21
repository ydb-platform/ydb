// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <DataStreams/MergingAggregatedBlockInputStream.h>


namespace CH
{

Header MergingAggregatedBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


Block MergingAggregatedBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        AggregatedDataVariants data_variants;
#if 0
        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);
#endif
        aggregator.mergeStream(children.back(), data_variants);
        blocks = aggregator.convertToBlocks(data_variants, final);
        it = blocks.begin();
    }

    Block res;
    if (isCancelledOrThrowIfKilled() || it == blocks.end())
        return res;

    res = std::move(*it);
    ++it;

    return res;
}


}
