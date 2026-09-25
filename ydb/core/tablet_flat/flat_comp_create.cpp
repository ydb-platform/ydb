#include "flat_comp_create.h"
#include "flat_comp_gen.h"

namespace NKikimr {
namespace NTable{

    std::unique_ptr<ICompactionStrategy> CreateGenCompactionStrategy(
            ui32 table,
            ICompactionBackend* backend,
            IResourceBroker* broker,
            ITimeProvider* time,
            NUtil::ILogger* logger,
            TString taskNameSuffix)
    {
        return std::make_unique<NCompGen::TGenCompactionStrategy>(
                table, backend, broker, time, logger, std::move(taskNameSuffix));
    }

}
}
