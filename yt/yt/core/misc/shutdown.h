#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An opaque ref-counted entity representing a registered shutdown callback.
using TShutdownCookie = TIntrusivePtr<TRefCounted>;

//! Registers a new callback to be called at global shutdown.
/*!
 *  If null is returned then the shutdown has already been started
 *  and #callback is not registered.
 *
 *  When the returned cookie is lost, the callback is automatically
 *  unregistered.
 */
[[nodiscard]]
TShutdownCookie RegisterShutdownCallback(
    TString name,
    TClosure callback,
    int priority = 0);


struct TShutdownOptions
{
    //! The amount of time to wait for all background threads to finish gracefully.
    TDuration GraceTimeout = TDuration::Seconds(60);

    //! Controls shutdown behavior when #GraceTimeout expires but some background
    //! threads are still runnining.
    //! If true then aborts the program (typically producing a core dump).
    //! If false then |_exit|s the program with #HungExitCode.
    bool AbortOnHang = true;

    //! Exit code to use in case on nongraceful exit.
    int HungExitCode = 0;
};

//! Starts the global shutdown.
/*!
 *  Invokes all registered shutdown callbacks in the order of
 *  decreasing priority.
 *
 *  Safe to call multiple times. All calls after the first one are,
 *  however, no ops.
 *
 *  This call happens automatically on program exit but on some legacy
 *  systems (e.g. Ubuntu 12) it may be sequenced too late (i.e. when the
 *  destructors of global static objects already started running).
 *  Hence calling it manually at a proper place is always a viable option.
 */
void Shutdown(const TShutdownOptions& options = {});

//! Returns true if the global shutdown has already been started
//! (and is possibly already completed).
bool IsShutdownStarted();

//! Controls if shutdown must be invoked automatically on process teardown.
void SetAutoShutdownEnabled(bool enabled);

//! Enables logging shutdown messages to stderr.
void EnableShutdownLoggingToStderr();

//! Enables logging shutdown messages to the given file.
void EnableShutdownLoggingToFile(const TString& fileName);

//! Returns the pointer to the log file if shutdown logging has been enabled or nullptr otherwise.
FILE* TryGetShutdownLogFile();

//! In case the global shutdown has been started, returns
//! the id of the thread invoking shutdown callbacks.
size_t GetShutdownThreadId();

//! Some actions that are required for proper shutdown
//! may only happen during the shutdown when they are
//! no longer safe to be executed, e.g. system invokers
//! creation.
//! Call this method before |Shutdown| to make sure everything is going to work safely.
//! This method can be called multiple times. No-op after the first call.
//! If you encounter build timeout during codegen phase or something similar,
//! try calling any other method from this header prior as it would break
//! any possible recursive behaviors of static variables.
void EnsureSafeShutdown();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
