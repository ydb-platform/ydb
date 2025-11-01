#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/datetime/base.h>

namespace NYdb {
namespace NConsoleClient {

class TInterruptibleCommand {
protected:
    static void OnTerminate(int);
    static void SetInterruptHandlers();
    static bool IsInterrupted();

    template <typename T>
    static bool WaitInterruptible(NThreading::TFuture<T>& future, TDuration interval = TDuration::MilliSeconds(50)) {
        while (!future.Wait(interval)) {
            if (IsInterrupted()) {
                Cerr << "<INTERRUPTED>" << Endl;
                return false;
            }
        }
        return true;
    }

private:
    static TAtomic Interrupted;
};

}
}
