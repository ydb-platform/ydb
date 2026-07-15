#pragma once

#ifdef LOG_LAZY
#error log macro redefinition
#endif

#define LOG_LAZY(log, priority, message)                     \
    if (log.IsOpen() && log.FiltrationLevel() >= priority) { \
        log.Write(priority, message);                        \
    }
