// breakpad_init.cc: A shared library to initialize breakpad signal handler via LD_PRELOAD.

#include "client/linux/handler/exception_handler.h"

using google_breakpad::MinidumpDescriptor;
using google_breakpad::ExceptionHandler;

// create signal handlers on shared library init
__attribute__((constructor))
static void breakpad_init() {

    const char * path = ::getenv("BREAKPAD_MINIDUMPS_PATH");

    static MinidumpDescriptor descriptor((path) ? path : "/tmp");
    static ExceptionHandler handler(
            descriptor,  // minidump descriptor
            NULL,        // callback filter
            NULL,        // callback function
            NULL,        // callback context
            true,        // do install handler
            -1           // server descriptor
    );
}