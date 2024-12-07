#pragma once

#include "program_mixin.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProgramPdeathsigMixin
    : public virtual TProgramMixinBase
{
protected:
    explicit TProgramPdeathsigMixin(NLastGetopt::TOpts& opts);

private:
    int ParentDeathSignal_ = -1;

    void Handle();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
