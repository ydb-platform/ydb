#include "flat_comp_create.h"
#include "flat_comp_gen.h"
#include "flat_comp_shard.h"

namespace NKikimr {
namespace NTable{

    THolder<ICompactionStrategy> CreateGenCompactionStrategy(
            ui32 table,
            ICompactionBackend* backend,
            IResourceBroker* broker,
            ITimeProvider* time,
            TString taskNameSuffix)
    {
        return MakeHolder<NCompGen::TGenCompactionStrategy>(
                table, backend, broker, time, std::move(taskNameSuffix));
    }

    THolder<ICompactionStrategy> CreateShardedCompactionStrategy(
            ui32 table,
            ICompactionBackend* backend,
            IResourceBroker* broker,
            NUtil::ILogger* logger,
            ITimeProvider* time,
            TString taskNameSuffix)
    {
        return MakeHolder<NCompShard::TShardedCompactionStrategy>(
                table, backend, broker, logger, time, std::move(taskNameSuffix));
    }

}
}
