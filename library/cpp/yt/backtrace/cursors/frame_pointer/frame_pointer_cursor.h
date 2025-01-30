#pragma once

#ifdef __x86_64__
    #include "frame_pointer_cursor_x86_64.h"
#else
    #include "frame_pointer_cursor_dummy.h"
#endif
