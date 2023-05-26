#include "mkql_block_agg_factory.h"
#include "mkql_block_agg_count.h"
#include "mkql_block_agg_sum.h"
#include "mkql_block_agg_minmax.h"
#include "mkql_block_agg_some.h"

#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

namespace NKikimr {
namespace NMiniKQL {

struct TAggregatorFactories {
    THashMap<TString, std::unique_ptr<IBlockAggregatorFactory>> Factories;

    TAggregatorFactories()
    {
        Factories["count_all"] = MakeBlockCountAllFactory();
        Factories["count"] = MakeBlockCountFactory();
        Factories["sum"] = MakeBlockSumFactory();
        Factories["avg"] = MakeBlockAvgFactory();
        Factories["min"] = MakeBlockMinFactory();
        Factories["max"] = MakeBlockMaxFactory();
        Factories["some"] = MakeBlockSomeFactory();
        RegisterPgBlockAggs(Factories);
    }
};

const IBlockAggregatorFactory& GetBlockAggregatorFactory(TStringBuf name) {
    const auto& f = Singleton<TAggregatorFactories>()->Factories;
    auto it = f.find(name);
    if (it == f.end()) {
        throw yexception() << "Unsupported block aggregation function: " << name;
    }

    return *it->second;
}

}
}
