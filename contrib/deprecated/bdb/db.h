#ifndef db_sdf75s67df5
#define db_sdf75s67df5

#include <util/system/platform.h>

#if defined(_unix_)
    #include "db_unix.h"
#elif defined(_win_)
    #include "db_win.h"
#else
    #error todo
#endif

#endif
