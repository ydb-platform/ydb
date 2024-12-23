#include <contrib/libs/breakpad/src/client/linux/handler/exception_handler.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <util/system/shellcommand.h>
#include <util/system/env.h>

class TMinidumper {
public:
    TMinidumper() {
        if(const auto path = GetEnv("BREAKPAD_MINIDUMPS_PATH")) {
            using namespace google_breakpad;
            Handler = MakeHolder<ExceptionHandler>(MinidumpDescriptor(path.c_str()), nullptr, DumpCallback, nullptr, true, -1, true);
        }
    }

private:
    static bool DumpCallback(const google_breakpad::MinidumpDescriptor& descriptor, void* context, bool succeeded) {
        if (const auto script = GetEnv("BREAKPAD_MINIDUMPS_SCRIPT")) {
            TShellCommand cmd(script, succeeded ? TList<TString>{"true", descriptor.path()} : TList<TString>{"false", ""});
            cmd.Run().Wait();
        }
        return succeeded;
    }

    THolder<google_breakpad::ExceptionHandler> Handler;
};

TMinidumper Minidumper;