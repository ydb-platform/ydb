#include "flat_comp_create.h"
#include "flat_comp_gen.h"

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

}
}
