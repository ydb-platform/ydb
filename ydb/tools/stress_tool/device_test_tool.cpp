#include "defs.h"

#include <library/cpp/getopt/last_getopt.h>
#include <util/string/printf.h>

#include "device_test_tool.h"
#include "device_test_tool_aio_test.h"
#include "device_test_tool_ddisk_test.h"
#include "device_test_tool_driveestimator.h"
#include "device_test_tool_pdisk_test.h"
#include "device_test_tool_trim_test.h"
#include "device_test_tool_uring_router_test.h"

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
    opts.AddLongOption("inflight-from", "override InFlight starting value (PDisk/DDisk/UringRouter tests)").DefaultValue("0");
    opts.AddLongOption("inflight-to", "override InFlight ending value (PDisk/DDisk/UringRouter tests)").DefaultValue("0");
    opts.AddLongOption("no-logo", "disable logo printing on start").NoArgument();
    opts.AddLongOption("disable-file-lock", "disable file locking before test").NoArgument().DefaultValue("0");
    TOptsParseResult res(&opts, argc, argv);

    if (!res.Has("no-logo") && res.Get("output-format") != TString("json")) {
        Cout << ydb_logo << Flush;
    }

    NKikimr::TPerfTestConfig config(res.Get("path"), res.Get("name"), res.Get("type"),
            res.Get("output-format"), res.Get("mon-port"), !res.Has("disable-file-lock"),
            res.Get("run-count"), res.Get("inflight-from"), res.Get("inflight-to"));
    NDevicePerfTest::TPerfTests protoTests;
    NKikimr::ParsePBFromFile(res.Get("cfg"), &protoTests);

    // When run-count > 1, only one test is allowed in the config
    if (config.RunCount > 1) {
        ui32 totalTests = 0;
        totalTests += protoTests.AioTestListSize();
        totalTests += protoTests.UringRouterTestListSize();
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

    // Check if inflight override is specified for unsupported tests
    if (config.HasInFlightOverride()) {
        if (protoTests.AioTestListSize() > 0) {
            Cerr << "Error: --inflight-from/--inflight-to are not supported for AioTest" << Endl;
            return 1;
        }
        if (protoTests.TrimTestListSize() > 0) {
            Cerr << "Error: --inflight-from/--inflight-to are not supported for TrimTest" << Endl;
            return 1;
        }
        if (protoTests.HasDriveEstimatorTest()) {
            Cerr << "Error: --inflight-from/--inflight-to are not supported for DriveEstimatorTest" << Endl;
            return 1;
        }
        // Check for unsupported PDiskLogLoad
        for (ui32 i = 0; i < protoTests.PDiskTestListSize(); ++i) {
            const auto& pdiskTest = protoTests.GetPDiskTestList(i);
            for (ui32 j = 0; j < pdiskTest.PDiskTestListSize(); ++j) {
                const auto& record = pdiskTest.GetPDiskTestList(j);
                if (record.Command_case() == NKikimr::TEvLoadTestRequest::CommandCase::kPDiskLogLoad) {
                    Cerr << "Error: --inflight-from/--inflight-to are not supported for PDiskLogLoad" << Endl;
                    return 1;
                }
            }
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

    for (ui32 i = 0; i < protoTests.UringRouterTestListSize(); ++i) {
        NDevicePerfTest::TUringRouterTest testProto = protoTests.GetUringRouterTestList(i);
        if (config.HasInFlightOverride()) {
            for (ui32 inFlight = config.InFlightFrom; inFlight <= config.InFlightTo; inFlight *= 2) {
                testProto.SetQueueDepth(inFlight);
                for (ui32 run = 0; run < config.RunCount; ++run) {
                    THolder<NKikimr::TPerfTest> test(new NKikimr::TUringRouterTest(config, testProto));
                    test->SetPrinter(printer);
                    test->RunTest();
                }
            }
        } else {
            for (ui32 run = 0; run < config.RunCount; ++run) {
                THolder<NKikimr::TPerfTest> test(new NKikimr::TUringRouterTest(config, testProto));
                test->SetPrinter(printer);
                test->RunTest();
            }
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

    // Helper lambda to override InFlight in PDiskTest proto
    auto overridePDiskInFlight = [](NDevicePerfTest::TPDiskTest& testProto, ui32 inFlight) {
        for (size_t j = 0; j < testProto.PDiskTestListSize(); ++j) {
            auto* record = testProto.MutablePDiskTestList(j);
            switch (record->Command_case()) {
            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskWriteLoad:
                record->MutablePDiskWriteLoad()->SetInFlightWrites(inFlight);
                break;
            case NKikimr::TEvLoadTestRequest::CommandCase::kPDiskReadLoad:
                record->MutablePDiskReadLoad()->SetInFlightReads(inFlight);
                break;
            default:
                break;
            }
        }
    };

    for (ui32 i = 0; i < protoTests.PDiskTestListSize(); ++i) {
        NDevicePerfTest::TPDiskTest testProto = protoTests.GetPDiskTestList(i);
        if (config.HasInFlightOverride()) {
            for (ui32 inFlight = config.InFlightFrom; inFlight <= config.InFlightTo; inFlight *= 2) {
                overridePDiskInFlight(testProto, inFlight);
                for (ui32 run = 0; run < config.RunCount; ++run) {
                    THolder<NKikimr::TPerfTest> test(new NKikimr::TPDiskTest(config, testProto));
                    test->SetPrinter(printer);
                    test->RunTest();
                }
            }
        } else {
            for (ui32 run = 0; run < config.RunCount; ++run) {
                THolder<NKikimr::TPerfTest> test(new NKikimr::TPDiskTest(config, testProto));
                test->SetPrinter(printer);
                test->RunTest();
            }
        }
    }
    printer->EndTest();

    // Helper lambda to override InFlight in DDiskTest proto
    auto overrideDDiskInFlight = [](NDevicePerfTest::TDDiskTest& testProto, ui32 inFlight) {
        for (size_t j = 0; j < testProto.DDiskTestListSize(); ++j) {
            auto* record = testProto.MutableDDiskTestList(j);
            if (record->Command_case() == NKikimr::TEvLoadTestRequest::CommandCase::kDDiskWriteLoad) {
                record->MutableDDiskWriteLoad()->SetInFlightWrites(inFlight);
            }
        }
    };

    for (ui32 i = 0; i < protoTests.DDiskTestListSize(); ++i) {
        NDevicePerfTest::TDDiskTest testProto = protoTests.GetDDiskTestList(i);
        if (config.HasInFlightOverride()) {
            for (ui32 inFlight = config.InFlightFrom; inFlight <= config.InFlightTo; inFlight *= 2) {
                overrideDDiskInFlight(testProto, inFlight);
                for (ui32 run = 0; run < config.RunCount; ++run) {
                    THolder<NKikimr::TPerfTest> test(new NKikimr::TDDiskTest(config, testProto));
                    test->SetPrinter(printer);
                    test->RunTest();
                }
            }
        } else {
            for (ui32 run = 0; run < config.RunCount; ++run) {
                THolder<NKikimr::TPerfTest> test(new NKikimr::TDDiskTest(config, testProto));
                test->SetPrinter(printer);
                test->RunTest();
            }
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
