#include "program_pdeathsig_mixin.h"

#ifdef _linux_
#include <sys/prctl.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TProgramPdeathsigMixin::TProgramPdeathsigMixin(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("pdeathsig", "parent death signal")
        .StoreResult(&ParentDeathSignal_)
        .RequiredArgument("PDEATHSIG");
}

bool TProgramPdeathsigMixin::HandlePdeathsigOptions()
{
    if (ParentDeathSignal_ > 0) {
#ifdef _linux_
        // Parent death signal is set by testing framework to avoid dangling processes when test runner crashes.
        // Unfortunately, setting pdeathsig in preexec_fn in subprocess call in test runner is not working
        // when the program has suid bit (pdeath_sig is reset after exec call in this case)
        // More details can be found in
        // http://linux.die.net/man/2/prctl
        // http://www.isec.pl/vulnerabilities/isec-0024-death-signal.txt
        YT_VERIFY(prctl(PR_SET_PDEATHSIG, ParentDeathSignal_) == 0);
#endif
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
