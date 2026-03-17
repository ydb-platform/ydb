#include <Access/QuotaUsage.h>
#include <boost/range/algorithm/fill.hpp>


namespace DB_CHDB
{
QuotaUsage::QuotaUsage() : quota_id(UUID(UInt128(0)))
{
}


QuotaUsage::Interval::Interval()
{
    boost::range::fill(used, 0);
}
}
