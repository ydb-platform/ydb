#include "defs.h"

#include <library/cpp/getopt/last_getopt.h>
#include <util/string/printf.h>

#include "device_test_tool.h"
#include "device_test_tool_aio_test.h"
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
    opts.AddLongOption("no-logo", "disable logo printing on start").NoArgument();
    opts.AddLongOption("disable-file-lock", "disable file locking before test").NoArgument().DefaultValue("0");
    TOptsParseResult res(&opts, argc, argv);

    if (!res.Has("no-logo") && res.Get("output-format") != TString("json")) {
        Cout << ydb_logo << Flush;
    }

    NKikimr::TPerfTestConfig config(res.Get("path"), res.Get("name"), res.Get("type"),
            res.Get("output-format"), res.Get("mon-port"), !res.Has("disable-file-lock"));
    NDevicePerfTest::TPerfTests protoTests;
    NKikimr::ParsePBFromFile(res.Get("cfg"), &protoTests);
    auto printer = MakeIntrusive<NKikimr::TResultPrinter>(config.OutputFormat);

    for (ui32 i = 0; i < protoTests.AioTestListSize(); ++i) {
        NDevicePerfTest::TAioTest testProto = protoTests.GetAioTestList(i);
        THolder<NKikimr::TPerfTest> test(new NKikimr::TAioTest(config, testProto));
        test->SetPrinter(printer);
        test->RunTest();
    }
    printer->EndTest();
    for (ui32 i = 0; i < protoTests.TrimTestListSize(); ++i) {
        NDevicePerfTest::TTrimTest testProto = protoTests.GetTrimTestList(i);
        THolder<NKikimr::TPerfTest> test(new NKikimr::TTrimTest(config, testProto));
        test->SetPrinter(printer);
        test->RunTest();
    }
    printer->EndTest();
    for (ui32 i = 0; i < protoTests.PDiskTestListSize(); ++i) {
        NDevicePerfTest::TPDiskTest testProto = protoTests.GetPDiskTestList(i);
        THolder<NKikimr::TPerfTest> test(new NKikimr::TPDiskTest(config, testProto));
        test->SetPrinter(printer);
        test->RunTest();
    }
    printer->EndTest();
    if (protoTests.HasDriveEstimatorTest()) {
        NDevicePerfTest::TDriveEstimatorTest testProto = protoTests.GetDriveEstimatorTest();
        THolder<NKikimr::TPerfTest> test(new NKikimr::TDriveEstimatorTest(config, testProto));
        test->SetPrinter(printer);
        test->RunTest();
    }
    printer->EndTest();
    return 0;
}
