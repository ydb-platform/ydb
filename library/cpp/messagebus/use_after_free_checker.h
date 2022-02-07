#pragma once

#include <util/system/platform.h>
#include <util/system/types.h>

class TUseAfterFreeChecker {
private:
    ui64 Magic;

public:
    TUseAfterFreeChecker();
    ~TUseAfterFreeChecker();
    void CheckNotFreed() const;
};

// check twice: in constructor and in destructor
class TUseAfterFreeCheckerGuard {
private:
    const TUseAfterFreeChecker& Check;

public:
    TUseAfterFreeCheckerGuard(const TUseAfterFreeChecker& check)
        : Check(check)
    {
        Check.CheckNotFreed();
    }

    ~TUseAfterFreeCheckerGuard() {
        Check.CheckNotFreed();
    }
};
