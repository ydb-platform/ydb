#include "program_mixin.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TProgramMixinBase::RegisterMixinCallback(std::function<void()> callback)
{
    MixinCallbacks_.push_back(std::move(callback));
}

void TProgramMixinBase::RunMixinCallbacks()
{
    for (const auto& callback : MixinCallbacks_) {
        callback();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
