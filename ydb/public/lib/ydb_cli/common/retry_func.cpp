#include "retry_func.h"

namespace NYdb::NConsoleClient {

void ExponentialBackoff(TDuration& sleep, TDuration max) {
    Sleep(sleep);
    sleep = Min(sleep * 2, max);
}

}
