#include "random.h"

TDuration RandomDuration(TDuration max) {
    return TDuration::MicroSeconds(RandomNumber((max + TDuration::MicroSeconds(1)).MicroSeconds()));
}
