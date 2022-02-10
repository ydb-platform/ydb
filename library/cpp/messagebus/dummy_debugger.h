#pragma once

#include <util/datetime/base.h>
#include <util/stream/output.h>

#define MB_TRACE()                                                                                    \ 
    do {                                                                                              \ 
        Cerr << TInstant::Now() << " " << __FILE__ << ":" << __LINE__ << " " << __FUNCTION__ << Endl; \
    } while (false)
