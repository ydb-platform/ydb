#pragma once

#include "program_mixin.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProgramSetsidMixin
    : public virtual TProgramMixinBase
{
protected:
    explicit TProgramSetsidMixin(NLastGetopt::TOpts& opts);

private:
    bool Setsid_ = false;

    void Handle();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
