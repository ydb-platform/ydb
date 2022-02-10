#pragma once

#include <util/datetime/base.h>
#include <util/generic/singleton.h>
#include <util/string/cast.h>
#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/system/env.h>

#include <cstdlib>

/* Keeps current time accurate up to 1/10 second */

class TTimeKeeper {
public:
    static TInstant GetNow(void) {
        return TInstant::MicroSeconds(GetTime());
    }

    static time_t GetTime(void) {
        return Singleton<TTimeKeeper>()->CurrentTime.tv_sec;
    }

    static const struct timeval& GetTimeval(void) {
        return Singleton<TTimeKeeper>()->CurrentTime;
    }

    TTimeKeeper()
        : Thread(&TTimeKeeper::Worker, this)
    {
        ConstTime = !!GetEnv("TEST_TIME");
        if (ConstTime) {
            try {
                CurrentTime.tv_sec = FromString<ui32>(GetEnv("TEST_TIME"));
            } catch (TFromStringException exc) {
                ConstTime = false;
            }
        }
        if (!ConstTime) {
            gettimeofday(&CurrentTime, nullptr);
            Thread.Start();
        }
    }

    ~TTimeKeeper() {
        if (!ConstTime) {
            Exit.Signal();
            Thread.Join();
        }
    }

private:
    static const ui32 UpdateInterval = 100000;
    struct timeval CurrentTime;
    bool ConstTime;
    TSystemEvent Exit;
    TThread Thread;

    static void* Worker(void* arg) {
        TTimeKeeper* owner = static_cast<TTimeKeeper*>(arg);

        do {
            /* Race condition may occur here but locking looks too expensive */

            gettimeofday(&owner->CurrentTime, nullptr);
        } while (!owner->Exit.WaitT(TDuration::MicroSeconds(UpdateInterval)));

        return nullptr;
    }
};
