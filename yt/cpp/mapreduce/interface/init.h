#pragma once

///
/// @file yt/cpp/mapreduce/interface/init.h
///
/// Initialization functions of YT Wrapper.

#include <yt/cpp/mapreduce/interface/wait_proxy.h>

#include <util/generic/fwd.h>

#include <functional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// Options for @ref NYT::Initialize() and @ref NYT::JoblessInitialize() functions
struct TInitializeOptions
{
    using TSelf = TInitializeOptions;

    ///
    /// @brief Override waiting functions for YT Wrapper.
    ///
    /// This options allows to override functions used by this library to wait something.
    FLUENT_FIELD_DEFAULT(::TIntrusivePtr<IWaitProxy>, WaitProxy, nullptr);

    ///
    /// @brief Enable/disable cleanup when program execution terminates abnormally.
    ///
    /// When set to true, library will abort all active transactions and running operations when program
    /// terminates on error or signal.
    FLUENT_FIELD_DEFAULT(bool, CleanupOnTermination, false);

    ///
    /// @brief Set callback to be called before exit() in job mode.
    ///
    /// Provided function will be called just before exit() when program is started in job mode.
    /// This might be useful for shutting down libraries that are used inside operations.
    ///
    /// NOTE: Keep in mind that inside job execution environment differs from client execution environment.
    /// So JobOnExitFunction should not depend on argc/argv environment variables etc.
    FLUENT_FIELD_OPTION(std::function<void()>, JobOnExitFunction);
};

///
/// @brief Performs basic initialization (logging, termination handlers, etc).
///
/// This function never switches to job mode.
void JoblessInitialize(const TInitializeOptions& options = TInitializeOptions());

///
/// @brief Performs basic initialization and switches to a job mode if required.
///
/// This function performs basic initialization (it sets up logging reads the config, etc) and checks if binary is launched
/// on YT machine inside a job. If latter is true this function launches proper job and after job is done it calls exit().
///
/// This function must be called if application starts any operation.
/// This function must be called immediately after entering main() function before any argument parsing is done.
void Initialize(int argc, const char **argv, const TInitializeOptions &options = TInitializeOptions());

/// Similar to @ref NYT::Initialize(int, const char**, const TInitializeOptions&)
void Initialize(int argc, char **argv, const TInitializeOptions &options = TInitializeOptions());

/// Similar to @ref NYT::Initialize(int, const char**, const TInitializeOptions&)
void Initialize(const TInitializeOptions &options = TInitializeOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
