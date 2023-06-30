#pragma once

///
/// @file yt/cpp/mapreduce/interface/serialize.h
///
/// Header containing interface to enable customizable waiting.

#include <yt/cpp/mapreduce/interface/common.h>

#include <util/datetime/base.h>

namespace NThreading {
template <typename T>
class TFuture;
}

class TSystemEvent;
class TCondVar;
class TMutex;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Interface to facilitate customizable waiting.
///
/// All the waiting functions in the library are obliged to use the methods of a wait proxy instead of direct function calls.
class IWaitProxy
    : public TThrRefBase
{
public:
    virtual ~IWaitProxy() = default;

    ///
    /// @brief Wait for the future setting with timeout.
    virtual bool WaitFuture(const ::NThreading::TFuture<void>& future, TDuration timeout) = 0;

    ///
    /// @brief Wait for a system event with timeout.
    virtual bool WaitEvent(TSystemEvent& event, TDuration timeout) = 0;

    ///
    /// @brief Wait for the notification on the condition variable with timeout.
    virtual bool WaitCondVar(TCondVar& condVar, TMutex& mutex, TDuration timeout) = 0;

    ///
    /// @brief Sleep in the current thread for (approximately) specified amount of time.
    virtual void Sleep(TDuration timeout) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
