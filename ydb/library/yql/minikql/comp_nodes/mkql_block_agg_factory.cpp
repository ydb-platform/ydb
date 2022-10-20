#include "mkql_block_agg_factory.h"
#include "mkql_block_agg_count.h"

namespace NKikimr {
namespace NMiniKQL {

struct TAggregatorFactories {
    THashMap<TString, std::unique_ptr<IBlockAggregatorFactory>> Factories;

    TAggregatorFactories()
    {
        Factories["count_all"] = MakeBlockCountAllFactory();
        Factories["count"] = MakeBlockCountFactory();
    }
};

std::unique_ptr<IBlockAggregator> MakeBlockAggregator(
    TStringBuf name,
    TTupleType* tupleType,
    ui32 countColumn,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns) {
    const auto& f = Singleton<TAggregatorFactories>()->Factories;
    auto it = f.find(name);
    if (it == f.end()) {
        throw yexception() << "Unsupported block aggregation function: " << name;
    }

    return it->second->Make(tupleType, countColumn, filterColumn, argsColumns);
}

}
}
