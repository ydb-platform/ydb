#pragma once

#include <cstddef>
#include <util/datetime/base.h>

namespace NYdb {
namespace NConsoleClient {

class TWaitingBar {
public:
    TWaitingBar(TString&& message = "Waiting... ");
    void Render();
    void Finish(bool cleanup);

private:
    void ClearLine();

    TInstant StartTime;
    TString WaitingMessage;
    bool Started = false;
    size_t LastMessageLength = 0;
};

} // namespace NConsoleClient
} // namespace NYdb
