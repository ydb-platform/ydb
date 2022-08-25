// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>

namespace CH
{

/** Combines aggregation states together, turns them into blocks, and outputs streams.
  * If the aggregation states are two-level, then it produces blocks strictly in order of 'bucket_num'.
  * (This is important for distributed processing.)
  * In doing so, it can handle different buckets in parallel, using up to `threads` threads.
  */
class MergingAndConvertingBlockInputStream : public IBlockInputStream
{
public:
    /** The input is a set of non-empty sets of partially aggregated data,
      *  which are all either single-level, or are two-level.
      */
    MergingAndConvertingBlockInputStream(const Aggregator & aggregator_, ManyAggregatedDataVariants & data_, bool final_)
        : aggregator(aggregator_), data(data_), final(final_), threads(1)
    {
        /// At least we need one arena in first data item per thread
        if (!data.empty() && threads > data[0]->aggregates_pools.size())
        {
            Arenas & first_pool = data[0]->aggregates_pools;
            for (size_t j = first_pool.size(); j < threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }
    }

    String getName() const override { return "MergingAndConverting"; }

    Header getHeader() const override { return aggregator.getHeader(final); }

protected:
    Block readImpl() override
    {
        if (data.empty())
            return {};

        if (current_bucket_num >= NUM_BUCKETS)
            return {};

        AggregatedDataVariantsPtr & first = data[0];

        if (current_bucket_num == -1)
        {
            ++current_bucket_num;

            if (first->type == AggregatedDataVariants::Type::without_key || aggregator.params.overflow_row)
            {
                aggregator.mergeWithoutKeyDataImpl(data);
                return aggregator.prepareBlockAndFillWithoutKey(
                    *first, final, first->type != AggregatedDataVariants::Type::without_key);
            }
        }

        {
            if (current_bucket_num > 0)
                return {};

            if (first->type == AggregatedDataVariants::Type::without_key)
                return {};

            ++current_bucket_num;

        #define M(NAME) \
            else if (first->type == AggregatedDataVariants::Type::NAME) \
                aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(data);
            if (false) {} // NOLINT
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
            else
                throw Exception("Unknown aggregated data variant.");

            return aggregator.prepareBlockAndFillSingleLevel(*first, final);
        }
    }

private:
    const Aggregator & aggregator;
    ManyAggregatedDataVariants data;
    bool final;
    size_t threads;

    Int32 current_bucket_num = -1;
    static constexpr Int32 NUM_BUCKETS = 256;
};

static std::unique_ptr<IBlockInputStream> mergeAndConvertToBlocks(Aggregator & aggregator,
                                                                  ManyAggregatedDataVariants & data_variants,
                                                                  bool final)
{
    ManyAggregatedDataVariants non_empty_data = aggregator.prepareVariantsToMerge(data_variants);
    if (non_empty_data.empty())
        return std::make_unique<OneBlockInputStream>(blockFromHeader(aggregator.getHeader(final)));
    return std::make_unique<MergingAndConvertingBlockInputStream>(aggregator, non_empty_data, final);
}

Header AggregatingBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}

Block AggregatingBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();

        aggregator.execute(children.back(), *data_variants);

        ManyAggregatedDataVariants many_data { data_variants };
        impl = mergeAndConvertToBlocks(aggregator, many_data, final);
    }

    if (isCancelledOrThrowIfKilled() || !impl)
        return {};

    return impl->read();
}

}
