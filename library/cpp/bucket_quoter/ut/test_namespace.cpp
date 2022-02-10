#include "test_namespace.h"

namespace NBucketQuoterTest {

    TMockTimer::TTime TMockTimer::CurrentTime = 0;

    template <>
    void Sleep<TMockTimer>(TDuration duration) {
        TMockTimer::Sleep(duration);
    }

}

