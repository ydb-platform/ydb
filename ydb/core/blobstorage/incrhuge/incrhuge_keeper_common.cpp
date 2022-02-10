#include "incrhuge_keeper_common.h"
#include "incrhuge_keeper.h"

namespace NKikimr {
    namespace NIncrHuge {

        TKeeperComponentBase::TKeeperComponentBase(TKeeper& keeper, const char *name)
            : Keeper(keeper)
            , LogPrefix(Sprintf("[PDisk# %09" PRIu32 " %s] ", Keeper.State.Settings.PDiskId, name))
        {}

    } // NIncrHuge
} // NKikimr
