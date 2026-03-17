#pragma once

extern "C" {
#include <contrib/libs/vlfeat/vl/generic.h>
}

#include <util/system/guard.h>
#include <util/system/mutex.h>

class TVlFeatToken {
private:
    static TMutex Mutex;
    static int Count;

public:
    TVlFeatToken() {
        TGuard<TMutex> guard(Mutex);
        if (!Count++)
            vl_constructor();
    }

    ~TVlFeatToken() {
        TGuard<TMutex> guard(Mutex);
        if (!--Count)
            vl_destructor();
    }
};
