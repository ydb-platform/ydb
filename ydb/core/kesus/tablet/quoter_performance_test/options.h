#pragma once
#include <library/cpp/getopt/opt.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

struct TOptions {
    size_t ResourcesCount = 0;
    size_t SessionsCountPerResource = 0;
    TDuration TestTime;

    double MaxUnitsPerSecond;

public:
    TOptions(int argc, const char** argv) {
        try {
            ParseOptions(argc, argv);
            ValidateOptions();
        } catch (const std::exception&) {
            Cerr << "Failed to get options: " << CurrentExceptionMessage() << Endl;
            exit(1);
        }
    }

private:
    void ParseOptions(int argc, const char** argv) {
        NLastGetopt::TOpts opts;
        opts.SetTitle("Quoter performance test program");
        opts.SetFreeArgsNum(0);
        opts.AddHelpOption('h');
        opts.AddVersionOption();

        opts.AddLongOption('r', "resources-count", "resources count in test")
            .DefaultValue(100)
            .RequiredArgument("COUNT")
            .StoreResult(&ResourcesCount);
        opts.AddLongOption('s', "sessions-count", "sessions count per resource")
            .DefaultValue(100)
            .RequiredArgument("COUNT")
            .StoreResult(&SessionsCountPerResource);
        opts.AddLongOption('t', "test-time", "test time")
            .DefaultValue(TDuration::Seconds(10))
            .RequiredArgument("TIME")
            .StoreResult(&TestTime);
        opts.AddLongOption('u', "max-units-per-second", "quota value")
            .DefaultValue(100.0)
            .StoreResult(&MaxUnitsPerSecond);

        NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    }

    void ValidateOptions() {
        Y_ENSURE(ResourcesCount > 0);
        Y_ENSURE(ResourcesCount < 10000000);
        Y_ENSURE(SessionsCountPerResource > 0);
        Y_ENSURE(SessionsCountPerResource < 10000000);
    }
};
