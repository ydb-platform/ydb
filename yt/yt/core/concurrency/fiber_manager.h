#pragma once

#include "public.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFiberManager
{
public:
    //! Returns the configured size of a fiber stack of a given kind.
    static size_t GetFiberStackSize(EExecutionStackKind stackKind);

    //! Sets the global size of fiber stacks of a given kind.
    //! Existing stacks keep their size and are discarded instead of being reused.
    static void SetFiberStackSize(EExecutionStackKind stackKind, size_t stackSize);

    //! Throws if #stackSize is not a valid stack size for #stackKind.
    static void ValidateFiberStackSize(EExecutionStackKind stackKind, size_t stackSize);

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
