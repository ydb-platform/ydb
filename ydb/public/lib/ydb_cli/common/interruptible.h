#pragma once

#include "command.h"
#include "formats.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb {
namespace NConsoleClient {

class TInterruptibleCommand {
protected:
    static void OnTerminate(int);
    static void SetInterruptHandlers();
    static bool IsInterrupted();

private:
    static TAtomic Interrupted;
};

}
}
