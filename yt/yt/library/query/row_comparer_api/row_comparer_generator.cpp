#include "row_comparer_generator.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TCGKeyComparers GenerateComparers(TRange<EValueType> /*keyColumnTypes*/)
{
    // Proper implementation resides in yt/yt/library/query/row_comparer/row_comparer_generator.cpp.
    YT_ABORT();
}

Y_WEAK IRowComparerProviderPtr CreateRowComparerProvider(TSlruCacheConfigPtr /*config*/)
{
    // Proper implementation resides in yt/yt/library/query/row_comparer/row_comparer_generator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
