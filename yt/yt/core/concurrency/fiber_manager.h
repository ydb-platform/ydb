#pragma once

#include "public.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFiberManager
{
public:
    //! Returns the current global limit for the number of pooled fiber stacks of a given size.
    static int GetFiberStackPoolSize(EExecutionStackKind stackKind);

    //! Sets the global limit for the number of pooled fiber stacks of a given size.
    static void SetFiberStackPoolSize(EExecutionStackKind stackKind, int poolSize);

    //! Returns the current global limit for the number of idle fibers.
    static int GetMaxIdleFibers();

    //! Sets the global limit for the number of idle fibers.
    static void SetMaxIdleFibers(int maxIdleFibers);

    //! Configures the singleton.
    static void Configure(const TFiberManagerConfigPtr& config);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
