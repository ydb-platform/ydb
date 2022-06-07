#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

class TLocalTasks {
private:
    TAtomic GotTasks;

public:
    TLocalTasks()
        : GotTasks(0)
    {
    }

    void AddTask() {
        AtomicSet(GotTasks, 1);
    }

    bool FetchTask() {
        bool gotTasks = AtomicCas(&GotTasks, 0, 1);
        return gotTasks;
    }
};
