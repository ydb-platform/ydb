#include <ydb/core/blobstorage/ut_pdiskfit/lib/fail_injection_test.h>
#include <ydb/core/blobstorage/ut_pdiskfit/lib/basic_test.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/string_utils/parse_size/parse_size.h>

using namespace NLastGetopt;

int main(int argc, char *argv[]) {
    TString device;
    NSize::TSize size;
    ui32 numIterations = 100;
    ui32 numFailsPerIteration = 100;
    NSize::TSize chunkSize = 16 << 20;
    NSize::TSize sectorSize = 4 << 10;
    bool erasureEncode = false;
    TString test;
    ui32 numVDisks = 4;

    TOpts opts = TOpts::Default();

    opts.AddHelpOption();

    opts.AddLongOption("device")
        .StoreResult(&device)
        .Help("path to device/regular file to store PDisk in")
        .RequiredArgument("DEVICE")
        .Required();

    opts.AddLongOption("size")
        .StoreResult(&size)
        .Help("size of PDisk in case when DEVICE is a regular file")
        .RequiredArgument("SIZE")
        .Optional();

    opts.AddLongOption("num-iterations")
        .StoreResult(&numIterations)
        .Help("number of iterations to perform (0 = unlimited)")
        .RequiredArgument("NUM")
        .Optional();

    opts.AddLongOption("num-fails-per-iteration")
        .StoreResult(&numFailsPerIteration)
        .Help("number of fails per single iteration to simulate (0 = unlimited)")
        .RequiredArgument("NUM")
        .Optional();

    opts.AddLongOption("chunk-size")
        .StoreResult(&chunkSize)
        .Help("chunk size")
        .RequiredArgument("SIZE")
        .Optional();

    opts.AddLongOption("sector-size")
        .StoreResult(&sectorSize)
        .Help("sector size")
        .RequiredArgument("SIZE")
        .Optional();

    opts.AddLongOption("test")
        .StoreResult(&test)
        .Help("type of test to run")
        .RequiredArgument("TEST")
        .Optional()
        .DefaultValue("basic");

    opts.AddLongOption("num-vdisks")
        .StoreResult(&numVDisks)
        .Help("number of VDisk to spawn on PDisk")
        .RequiredArgument("NUM")
        .Optional();

    opts.AddLongOption("erasure-encode")
        .SetFlag(&erasureEncode)
        .Help("enable erasure encoding of pdisk");

    opts.SetFreeArgsMax(0);

    TOptsParseResult result(&opts, argc, argv);

    TPDiskFailureInjectionTest fit;
    fit.NumIterations = numIterations;
    fit.NumFailsInIteration = numFailsPerIteration;
    fit.PDiskFilePath = device;
    fit.DiskSize = size;
    fit.ChunkSize = chunkSize;
    fit.SectorSize = sectorSize;
    fit.ErasureEncode = erasureEncode;

    if (test == "basic") {
        fit.RunCycle<TBasicTest>(false, numVDisks, false);
    } else {
        Cerr << "unknown test type " << test << Endl;
        return 1;
    }

    return 0;
}
