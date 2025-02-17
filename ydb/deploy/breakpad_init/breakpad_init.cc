// breakpad_init.cc: A shared library to initialize breakpad signal handler via LD_PRELOAD.

#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include "client/linux/handler/exception_handler.h"

using google_breakpad::MinidumpDescriptor;
using google_breakpad::ExceptionHandler;

// callback function, called after minidump was created
static bool dumpCallback(const MinidumpDescriptor& descriptor, void* context, bool succeeded) {
    char *script = getenv("BREAKPAD_MINIDUMPS_SCRIPT");
    if (script != NULL) {
        pid_t pid=fork();
        if (pid == 0) {
            char* dumpSucceded = succeeded ? (char *)"true" : (char *)"false";
            char* descriptorPath = succeeded ? (char *)descriptor.path() : (char *)"\0";
            char* cmd[] = {script, dumpSucceded, descriptorPath, NULL};
            execve(cmd[0], &cmd[0], NULL);
        } else {
            waitpid(pid, 0, 0);
        }
    }
    return succeeded;
}

// create signal handlers on shared library init
__attribute__((constructor))
static void breakpad_init() {

    const char * path = ::getenv("BREAKPAD_MINIDUMPS_PATH");

    static MinidumpDescriptor descriptor((path) ? path : "/tmp");
    static ExceptionHandler handler(
            descriptor,         // minidump descriptor
            NULL,               // callback filter
            dumpCallback,       // callback function
            NULL,               // callback context
            true,               // do install handler
            -1                  // server descriptor
    );
}
