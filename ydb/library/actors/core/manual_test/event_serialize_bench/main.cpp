#include "bench_runtime.h"

int main(int argc, char** argv) {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    TBenchConfig config;
    if (!ParseArgs(argc, argv, config)) {
        PrintUsage(argv[0]);
        return 1;
    }

    RunBenchmarks(config);
    return 0;
}
