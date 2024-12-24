#include <util/generic/ptr.h>
#include <contrib/libs/breakpad/src/client/linux/handler/exception_handler.h>
#include <unistd.h>
#include <sys/wait.h>


class TMinidumper {
public:
    TMinidumper() {
        if(const char* path = getenv("BREAKPAD_MINIDUMPS_PATH")) {
            using namespace google_breakpad;
            Handler = MakeHolder<ExceptionHandler>(MinidumpDescriptor(path), nullptr, DumpCallback, nullptr, true, -1, true);
        }
    }

private:
    static bool DumpCallback(const google_breakpad::MinidumpDescriptor& descriptor, void* context, bool succeeded) {
        if (char* script = getenv("BREAKPAD_MINIDUMPS_SCRIPT")) {
            if (auto pid = fork()) {
                waitpid(pid, 0, 0);
            } else {
                char* dumpSucceded = succeeded ? (char *)"true" : (char *)"false";  
                char* descriptorPath = succeeded ? (char *)descriptor.path() : (char *)"\0";  
                char* cmd[] = {script, dumpSucceded, descriptorPath, NULL};  
                execve(cmd[0], &cmd[0], NULL);
            }
        }
        return succeeded;
    }

    THolder<google_breakpad::ExceptionHandler> Handler;
};

TMinidumper Minidumper;
