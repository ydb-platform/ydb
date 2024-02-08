#pragma once
#include "defs.h"

class TProgramShouldContinue {
public:
    enum EState {
        Continue,
        Stop,
        Restart,
    };

    void ShouldRestart();
    void ShouldStop(int returnCode = 0);

    EState PollState();
    int GetReturnCode();

    void Reset();
private:
    std::atomic<int> ReturnCode = 0;
    std::atomic<EState> State = Continue;
};
