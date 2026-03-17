#include "git2_features-linux.h"

#undef GIT_RAND_GETENTROPY

#undef GIT_QSORT_GNU
#define GIT_QSORT_MSC

#undef GIT_IO_POLL
#define GIT_IO_WSAPOLL 1
