#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/datetime/base.h>

#include <atomic>

namespace NYdb::NConsoleClient {

class TInterruptableCommand {
protected:
    static void OnTerminate(int);
    static void ResetInterrupted();
    static void SetInterruptHandlers();
    static bool IsInterrupted();

    template <typename T>
    static bool WaitInterruptable(NThreading::TFuture<T>& future, TDuration interval = TDuration::MilliSeconds(50)) {
        if (SignalHandlers.empty()) {
            SetInterruptHandlers();
        }

        while (!future.Wait(interval)) {
            if (IsInterrupted()) {
                Cerr << "<INTERRUPTED>" << Endl;
                return false;
            }
        }
        return true;
    }

private:
    static std::atomic<bool> Interrupted;
    inline static std::unordered_map<int, void (*)(int)> SignalHandlers;
};

} // namespace NYdb::NConsoleClient
