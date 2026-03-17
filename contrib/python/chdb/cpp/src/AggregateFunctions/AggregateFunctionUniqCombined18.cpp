#include <AggregateFunctions/AggregateFunctionUniqCombined.h>

namespace DB_CHDB
{
template AggregateFunctionPtr createAggregateFunctionWithHashType<18>(bool use_64_bit_hash, const DataTypes & argument_types, const Array & params);
}
