#pragma once

#include <util/datetime/base.h>

namespace NIPREG {

class TStopWatch {
private:
    TInstant Start;
    TInstant TaskStart;
    bool TaskRunning = false;
    ui32 TaskOrdNum = 0;

private:
    TString FormatTime(const TDuration& dur);

public:
    TStopWatch();
    ~TStopWatch();

    void StartTask(const TString& message);
    void StopTask();
};

}
