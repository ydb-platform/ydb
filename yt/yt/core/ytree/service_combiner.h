#pragma once

#include "ypath_service.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IServiceCombiner
    : public virtual IYPathService
{
    virtual void SetUpdatePeriod(std::optional<TDuration> period) = 0;
};

DEFINE_REFCOUNTED_TYPE(IServiceCombiner)

IServiceCombinerPtr CreateServiceCombiner(
    std::vector<IYPathServicePtr> services,
    std::optional<TDuration> keysUpdatePeriod = std::nullopt,
    bool updateKeysOnMissingKey = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

