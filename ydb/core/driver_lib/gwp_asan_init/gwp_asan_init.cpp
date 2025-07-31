#include "gwp_asan_init.h"

#if defined(__clang__) && !defined(_win32_)
// GWP-ASan is only available on non-Windows platforms with Clang
#define ENABLE_GWP_ASAN 1
#include <gwp_asan/guarded_pool_allocator.h>
#include <gwp_asan/crash_handler.h>
#include <cstdlib>
#include <cstring>
#include <cstdarg>
#include <cstdio>
#include <execinfo.h>
#include <unistd.h>
#endif

namespace NKikimr {

#if ENABLE_GWP_ASAN
static gwp_asan::GuardedPoolAllocator GPA;
static bool GwpAsanInitialized = false;

// Simple backtrace implementation
static size_t BacktraceImpl(uintptr_t *TraceBuffer, size_t Size) {
    return backtrace(reinterpret_cast<void **>(TraceBuffer), Size);
}

// Simple printf implementation for error reporting
static void PrintfImpl(const char *Format, ...) {
    va_list Args;
    va_start(Args, Format);
    vfprintf(stderr, Format, Args);
    va_end(Args);
}

// Parse simple GWP_ASAN_OPTIONS environment variable
static void ParseEnvironmentOptions(gwp_asan::options::Options &Opts) {
    const char *OptionsStr = getenv("GWP_ASAN_OPTIONS");
    if (!OptionsStr) {
        return;
    }
    
    // Simple parsing for key=value pairs separated by colons
    char *Options = strdup(OptionsStr);
    char *Token = strtok(Options, ":");
    
    while (Token) {
        if (strncmp(Token, "Enabled=", 8) == 0) {
            Opts.Enabled = (strcmp(Token + 8, "1") == 0 || strcmp(Token + 8, "true") == 0);
        } else if (strncmp(Token, "SampleRate=", 11) == 0) {
            Opts.SampleRate = atoi(Token + 11);
        } else if (strncmp(Token, "MaxSimultaneousAllocations=", 27) == 0) {
            Opts.MaxSimultaneousAllocations = atoi(Token + 27);
        } else if (strncmp(Token, "InstallSignalHandlers=", 22) == 0) {
            Opts.InstallSignalHandlers = (strcmp(Token + 22, "1") == 0 || strcmp(Token + 22, "true") == 0);
        } else if (strncmp(Token, "Recoverable=", 12) == 0) {
            Opts.Recoverable = (strcmp(Token + 12, "1") == 0 || strcmp(Token + 12, "true") == 0);
        }
        Token = strtok(nullptr, ":");
    }
    
    free(Options);
}

// Set default options for production use
static void SetDefaultOptions(gwp_asan::options::Options &Opts) {
    Opts.setDefaults();
    
    // Production-friendly defaults:
    // - Enabled by default but can be disabled via environment
    // - Conservative sample rate (1/10000 instead of default 1/5000)
    // - Smaller pool size (8 instead of default 16) to reduce memory overhead
    // - Install signal handlers for better error reporting
    // - Enable fork handlers for multi-process safety
    Opts.Enabled = true;
    Opts.SampleRate = 10000;
    Opts.MaxSimultaneousAllocations = 8;
    Opts.InstallSignalHandlers = true;  
    Opts.InstallForkHandlers = true;
    Opts.Recoverable = false;
    Opts.Backtrace = BacktraceImpl;
}
#endif

void InitializeGwpAsan() {
#if ENABLE_GWP_ASAN
    if (GwpAsanInitialized) {
        return;
    }
    
    gwp_asan::options::Options Opts;
    SetDefaultOptions(Opts);
    
    // Override with environment variables
    ParseEnvironmentOptions(Opts);
    
    // Only proceed if enabled
    if (!Opts.Enabled) {
        GwpAsanInitialized = true;
        return;
    }
    
    // Print configuration information in debug builds or when explicitly requested
    const char* DebugEnv = getenv("GWP_ASAN_DEBUG");
    if (DebugEnv && strcmp(DebugEnv, "1") == 0) {
        fprintf(stderr, "GWP-ASan: Enabled with SampleRate=%d, MaxSimultaneousAllocations=%d\n", 
                Opts.SampleRate, Opts.MaxSimultaneousAllocations);
    }
    
    // Initialize the guarded pool allocator
    GPA.init(Opts);
    
    // Install signal handlers for better error reporting
    if (Opts.InstallSignalHandlers) {
        gwp_asan::crash_handler::installSignalHandlers(&GPA, PrintfImpl);
    }
    
    GwpAsanInitialized = true;
#else
    // No-op when GWP-ASan is not available
    static bool WarningShown = false;
    if (!WarningShown) {
        const char* DebugEnv = getenv("GWP_ASAN_DEBUG");
        if (DebugEnv && strcmp(DebugEnv, "1") == 0) {
            fprintf(stderr, "GWP-ASan: Not available on this platform/compiler\n");
        }
        WarningShown = true;
    }
#endif
}

} // namespace NKikimr