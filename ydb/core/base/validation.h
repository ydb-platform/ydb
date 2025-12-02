#pragma once

#ifndef NDEBUG
    #define Y_DEBUG_VERIFY Y_ABORT_UNLESS
#else
    #define Y_DEBUG_VERIFY Y_VERIFY
#endif
