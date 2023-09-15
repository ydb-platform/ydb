#include "options.h"
#include <library/cpp/getopt/last_getopt.h>

TOptions::TOptions(int argc, const char* argv[])
{
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddHelpOption('h');
    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&Endpoint);
    opts.AddLongOption('d', "database", "YDB database name").DefaultValue("/Root").RequiredArgument("PATH")
        .StoreResult(&Database);
    opts.AddLongOption('t', "topic-path", "Topic path for reading").Required().RequiredArgument("PATH")
        .StoreResult(&TopicPath);
    opts.AddLongOption('c', "consumer", "Consumer name").Required().RequiredArgument("CONSUMER")
        .StoreResult(&ConsumerName);
    opts.AddLongOption("secure-connection", "Use secure connection")
        .SetFlag(&UseSecureConnection).NoArgument();
    opts.AddLongOption("table-path", "Table path for writing").Required().RequiredArgument("PATH")
        .StoreResult(&TablePath);
    opts.AddLongOption('v', "verbose", "Verbosity").NoArgument()
        .Handler0([this](){ ++reinterpret_cast<unsigned&>(LogPriority); });
    opts.SetFreeArgsNum(0);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
}
