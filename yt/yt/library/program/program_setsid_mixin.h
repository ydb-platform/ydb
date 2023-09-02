#pragma once

#include "program.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProgramSetsidMixin
{
protected:
    explicit TProgramSetsidMixin(NLastGetopt::TOpts& opts);

    bool HandleSetsidOptions();

private:
    bool Setsid_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
