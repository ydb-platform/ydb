#include "args.h"

#include <library/cpp/getopt/last_getopt.h>

namespace NLsp::NYql {

TArgs TArgs::Parse(int argc, char** argv) {
    TArgs args;

    NLastGetopt::TOpts opts;

    opts.AddLongOption("stdio", "use stdio as the communication channel")
        .NoArgument()
        .DefaultValue(true)
        .StoreResult(&args.IsStdIO);

    opts.AddLongOption('j', "threads", "readonly concurrency")
        .DefaultValue(1)
        .StoreResult(&args.Threads);

    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    Y_UNUSED(res);

    if (!args.IsStdIO) {
        throw TBadArgumentException() << "only stdio communication is supported";
    }

    if (const size_t t = args.Threads, min = 1, max = 128; t < min || max < t) {
        throw TBadArgumentException()
            << "threads must be in range "
            << "[" << min << ", " << max << "], "
            << "but got " << t;
    }

    return args;
}

} // namespace NLsp::NYql
