#pragma once
#include <library/cpp/getopt/opt.h>
#include <library/cpp/colorizer/colors.h>

#include <util/datetime/base.h>

#include <random>

namespace NKikimr {

struct TOptions {
    size_t KesusCount = 0;
    size_t ResourcePerKesus = 0;
    size_t RootResourceSpeedLimit = 0;

    size_t LocalResourceCount = 0;
    ui32 LocalResourceQuotaRate = 0;

    TDuration TestTime;
    size_t RequestRate = 0;
    TDuration QuotaRequestDeadline;

    mutable std::seed_seq SeedSeq;

    TOptions(int argc, const char *argv[])
        : SeedSeq({std::random_device()(), std::random_device()(), std::random_device()()})
    {
        ParseOptions(argc, argv);
    }

    void ParseOptions(int argc, const char *argv[]) {
        NLastGetopt::TOpts opts;
        opts.SetTitle("Quoter service bandwidth test program");
        opts.SetFreeArgsNum(0);
        opts.AddHelpOption('h');
        opts.AddVersionOption();

        opts.AddLongOption('k', "kesus-count", "kesus count in test")
                .DefaultValue(100)
                .RequiredArgument("COUNT")
                .StoreResult(&KesusCount);
        opts.AddLongOption('p', "resource-per-kesus", "resource count per each kesus")
                .DefaultValue(100)
                .RequiredArgument("COUNT")
                .StoreResult(&ResourcePerKesus);
        opts.AddLongOption('s', "root-resource-speed-limit", "kesus root resource speed limit")
                .DefaultValue(100000)
                .RequiredArgument("RATE")
                .StoreResult(&RootResourceSpeedLimit);
        opts.AddLongOption('l', "local-resource-count", "local resource count")
                .DefaultValue(100)
                .RequiredArgument("COUNT")
                .StoreResult(&LocalResourceCount);
        opts.AddLongOption('q', "local-resource-speed-limit", "local resource speed limit")
                .DefaultValue(100000)
                .RequiredArgument("RATE")
                .StoreResult(&LocalResourceQuotaRate);
        opts.AddLongOption('t', "test-time", "test time")
                .DefaultValue(TDuration::Seconds(10))
                .RequiredArgument("DURATION")
                .StoreResult(&TestTime);
        opts.AddLongOption('r', "request-rate", "request rate")
                .DefaultValue(100)
                .RequiredArgument("RATE")
                .StoreResult(&RequestRate);
        opts.AddLongOption('d', "quota-request-deadline", "quota request deadline")
                .DefaultValue(TDuration::MilliSeconds(150))
                .RequiredArgument("DURATION")
                .StoreResult(&QuotaRequestDeadline);

        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        CheckOptions();

        //PrintOpts();
    }

    void CheckOptions() {
        Y_ABORT_UNLESS(RequestRate > 0, "Zero request rate");
    }

    void PrintOpts() const {
        auto PrintOption = [](const char* name, const auto& value) {
            Cerr << NColorizer::StdErr().Red() << name << NColorizer::StdErr().Default() << ": "
                 << NColorizer::StdErr().Green() << value << NColorizer::StdErr().Default() << Endl;
        };
        PrintOption("Kesus count", KesusCount);
        PrintOption("Resources per kesus", ResourcePerKesus);
        PrintOption("Root kesus resource speed limit", RootResourceSpeedLimit);
        PrintOption("Local resource count", LocalResourceCount);
        PrintOption("Local resource quota rate", LocalResourceQuotaRate);
        PrintOption("Quota request rate per resource", RequestRate);
        PrintOption("Quota request deadline", QuotaRequestDeadline);
        PrintOption("Test time", TestTime);
    }
};

} // namespace NKikimr
