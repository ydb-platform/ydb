#ifndef db_int_h_sdf75s67df5
#define db_int_h_sdf75s67df5

#include <util/system/platform.h>

#if defined(_unix_)
    #include "db_int_unix.h"
#elif defined(_win_)
    #include "db_int_win.h"
#else
    #error todo
#endif

#endif
