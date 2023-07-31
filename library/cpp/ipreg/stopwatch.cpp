#include "stopwatch.h"

#include <util/stream/str.h>

namespace NIPREG {

TStopWatch::TStopWatch() {
    Start = TInstant::Now();
}

TStopWatch::~TStopWatch() {
    try {
        if (TaskRunning)
            StopTask();

        Cerr << "Everything done in " << FormatTime(TInstant::Now() - Start) << Endl;
    } catch (...) {
        // not much problem if we can't write the summary
    }
}

void TStopWatch::StartTask(const TString& message) {
    StopTask();

    ++TaskOrdNum;
    TaskStart = TInstant::Now();
    TaskRunning = true;
    Cerr << TaskOrdNum << ". " << message << "...\n";
}

void TStopWatch::StopTask() {
    if (TaskRunning) {
        Cerr << "Done in " << FormatTime(TInstant::Now() - TaskStart) << Endl;
        TaskRunning = false;
    }
}

TString TStopWatch::FormatTime(const TDuration& dur) {
    auto sec = dur.Seconds();

    TStringStream ss;

    if (sec < 60)
        ss << sec << "s";
    else if (sec < 3600)
        ss << sec / 60 << "m " << sec % 60 << "s";
    else
        ss << sec / 3600 << "h " << (sec / 60) % 60 << "m";

    return ss.Str();
}

}
