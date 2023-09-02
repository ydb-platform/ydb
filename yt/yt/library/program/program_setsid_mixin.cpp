#include "program_setsid_mixin.h"

#ifdef _linux_
#include <unistd.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TProgramSetsidMixin::TProgramSetsidMixin(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("setsid", "create a new session")
        .StoreTrue(&Setsid_)
        .Optional();
}

bool TProgramSetsidMixin::HandleSetsidOptions()
{
    if (Setsid_) {
#ifdef _linux_
        setsid();
#endif
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
