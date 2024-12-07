#pragma once

#include "program.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProgramMixinBase
    : public virtual TProgram
{
protected:
    void RegisterMixinCallback(std::function<void()> callback);
    void RunMixinCallbacks();

private:
    std::vector<std::function<void()>> MixinCallbacks_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
