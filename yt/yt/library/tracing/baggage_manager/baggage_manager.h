#pragma once

#include "public.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TBaggageManager
{
public:
    static bool IsBaggageAdditionEnabled();

    //! Configures the singleton.
    static void Configure(const TBaggageManagerConfigPtr& config);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
