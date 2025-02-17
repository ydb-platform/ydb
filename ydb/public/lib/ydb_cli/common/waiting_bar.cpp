#include "common.h"
#include "waiting_bar.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <util/string/cast.h>

namespace NYdb {
namespace NConsoleClient {

TWaitingBar::TWaitingBar(TString&& message)
    : StartTime(TInstant::Now())
    , WaitingMessage(std::move(message))
{}

void TWaitingBar::Render() {
    Started = true;
    TString newMessage = WaitingMessage;
    size_t secondsElapsed = (TInstant::Now() - StartTime).Seconds();
    if (secondsElapsed > 0) {
        newMessage += " " + ToString(secondsElapsed) + "s";
    }
    ClearLine();
    LastMessageLength = newMessage.length();
    Cerr << newMessage;
    Cerr.Flush();
}

// Set cleanup to true to remove waiting message + elapsed time
void TWaitingBar::Finish(bool cleanup) {
    if (!Started) {
        return;
    }
    
    if (cleanup) {
        ClearLine();
    } else {
        Cerr << Endl;
    }
    Cerr.Flush();
    Started = false;
}

void TWaitingBar::ClearLine() {
    if (LastMessageLength > 0) {
        Cerr << "\r" << (TString(" ") * LastMessageLength) << "\r";
    }
}

} // namespace NConsoleClient
} // namespace NYdb
