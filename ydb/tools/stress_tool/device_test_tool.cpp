#include "defs.h"

#include <library/cpp/getopt/last_getopt.h>
#include <util/string/printf.h>

#include "device_test_tool.h"
#include "device_test_tool_aio_test.h"
#include "device_test_tool_ddisk_test.h"
#include "device_test_tool_driveestimator.h"
#include "device_test_tool_pdisk_test.h"
#include "device_test_tool_trim_test.h"

namespace NKikimr {
namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

static const char* ydb_logo =
R"__(

==:.     .==      ==:.     .-=
==.      .==      ==:       -==      @@@@       @@@@ @@@@@@@        @@@@@@@@
============+==================       @@@@     @@@@ @@@@@@@@@@@@   @@@@@@@@@@@@
===============================        @@@@   @@@@  @@@@     @@@@@ @@@@    @@@@
======-  === =+=+ ===. .=======         @@@@ @@@@   @@@@       @@@ @@@@    @@@@
======== .=.      .=: -========           @@@@@@    @@@@       @@@ @@@@@@@@@@@@
 ==========.      .==========              @@@@     @@@@       @@@ @@@@     @@@@
         ============                      @@@@     @@@@     @@@@@ @@@@      @@@
         ============                      @@@@     @@@@@@@@@@@@   @@@@@@@@@@@@@
         ============                      @@@       @@@@@@@@       @@@@@@@@@
         ============
          ==========

--------------------------------------------------------------------------------

)__";

int main(int argc, char **argv) {
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();
    opts.AddLongOption("path", "path to device").RequiredArgument("FILE");
    opts.AddLongOption("cfg", "path to config file").RequiredArgument().DefaultValue("cfg.txt");
    opts.AddLongOption("name", "device name").DefaultValue("Name");
    opts.AddLongOption("type", "device type  - ROT|SSD|NVME").DefaultValue("ROT");
    opts.AddLongOption("output-format", "wiki|human|json").DefaultValue("wiki");
    opts.AddLongOption("mon-port", "port for monitoring http page").DefaultValue("0");
    opts.AddLongOption("run-count", "number of times to run each test").DefaultValue("1");
    opts.AddLongOption("no-logo", "disable logo printing on start").NoArgument();
    opts.AddLongOption("disable-file-lock", "disable file locking before test").NoArgument().DefaultValue("0");
    TOptsParseResult res(&opts, argc, argv);

    if (!res.Has("no-logo") && res.Get("output-format") != TString("json")) {
        Cout << ydb_logo << Flush;
    }

    NKikimr::TPerfTestConfig config(res.Get("path"), res.Get("name"), res.Get("type"),
            res.Get("output-format"), res.Get("mon-port"), !res.Has("disable-file-lock"),
            res.Get("run-count"));
    NDevicePerfTest::TPerfTests protoTests;
    NKikimr::ParsePBFromFile(res.Get("cfg"), &protoTests);

    // When run-count > 1, only one test is allowed in the config
    if (config.RunCount > 1) {
        ui32 totalTests = 0;
        totalTests += protoTests.AioTestListSize();
        totalTests += protoTests.TrimTestListSize();
        // For PDiskTest, count the inner PDiskTestList items
        for (ui32 i = 0; i < protoTests.PDiskTestListSize(); ++i) {
            totalTests += protoTests.GetPDiskTestList(i).PDiskTestListSize();
        }
        // For DDiskTest, count the inner DDiskTestList items
        for (ui32 i = 0; i < protoTests.DDiskTestListSize(); ++i) {
            totalTests += protoTests.GetDDiskTestList(i).DDiskTestListSize();
        }
        if (protoTests.HasDriveEstimatorTest()) {
            totalTests += 1;
        }

        if (totalTests > 1) {
            Cerr << "Error: run-count > 1 requires exactly one test in the config file, but found " << totalTests << " tests" << Endl;
            return 1;
        }
    }

    auto printer = MakeIntrusive<NKikimr::TResultPrinter>(config.OutputFormat, config.RunCount);

    for (ui32 i = 0; i < protoTests.AioTestListSize(); ++i) {
        NDevicePerfTest::TAioTest testProto = protoTests.GetAioTestList(i);
        for (ui32 run = 0; run < config.RunCount; ++run) {
            THolder<NKikimr::TPerfTest> test(new NKikimr::TAioTest(config, testProto));
            test->SetPrinter(printer);
            test->RunTest();
        }
    }
    printer->EndTest();
    for (ui32 i = 0; i < protoTests.TrimTestListSize(); ++i) {
        NDevicePerfTest::TTrimTest testProto = protoTests.GetTrimTestList(i);
        for (ui32 run = 0; run < config.RunCount; ++run) {
            THolder<NKikimr::TPerfTest> test(new NKikimr::TTrimTest(config, testProto));
            test->SetPrinter(printer);
            test->RunTest();
        }
    }
    printer->EndTest();
    for (ui32 i = 0; i < protoTests.PDiskTestListSize(); ++i) {
        NDevicePerfTest::TPDiskTest testProto = protoTests.GetPDiskTestList(i);
        for (ui32 run = 0; run < config.RunCount; ++run) {
            THolder<NKikimr::TPerfTest> test(new NKikimr::TPDiskTest(config, testProto));
            test->SetPrinter(printer);
            test->RunTest();
        }
    }
    printer->EndTest();
    for (ui32 i = 0; i < protoTests.DDiskTestListSize(); ++i) {
        NDevicePerfTest::TDDiskTest testProto = protoTests.GetDDiskTestList(i);
        for (ui32 run = 0; run < config.RunCount; ++run) {
            THolder<NKikimr::TPerfTest> test(new NKikimr::TDDiskTest(config, testProto));
            test->SetPrinter(printer);
            test->RunTest();
        }
    }
    printer->EndTest();
    if (protoTests.HasDriveEstimatorTest()) {
        NDevicePerfTest::TDriveEstimatorTest testProto = protoTests.GetDriveEstimatorTest();
        for (ui32 run = 0; run < config.RunCount; ++run) {
            THolder<NKikimr::TPerfTest> test(new NKikimr::TDriveEstimatorTest(config, testProto));
            test->SetPrinter(printer);
            test->RunTest();
        }
    }
    printer->EndTest();
    return 0;
}
