#include "defs.h"

#include <library/cpp/getopt/last_getopt.h>
#include <util/generic/bitops.h>
#include <util/generic/strbuf.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/info.h>

#ifdef _linux_
#include <sched.h>
#endif

#include "device_test_tool.h"
#include "device_test_tool_aio_test.h"
#include "device_test_tool_ddisk_test.h"
#include "device_test_tool_ddisk_client_server.h"
#include "device_test_tool_driveestimator.h"
#include "device_test_tool_pb_test.h"
#include "device_test_tool_pdisk_test.h"
#include "device_test_tool_trim_test.h"
#ifdef _linux_
#include "device_test_tool_uring_router_test.h"
#endif

#include <csignal>

namespace NKikimr {
namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

// Map a --log-level string to NLog::EPriority. Accepted: warn, info, debug, trace.
static NActors::NLog::EPriority ParseLogLevel(const TString& s) {
    if (s == "warn") {
        return NActors::NLog::PRI_WARN;
    } else if (s == "info") {
        return NActors::NLog::PRI_INFO;
    } else if (s == "debug") {
        return NActors::NLog::PRI_DEBUG;
    } else if (s == "trace") {
        return NActors::NLog::PRI_TRACE;
    }
    ythrow yexception() << "invalid --log-level '" << s << "', expected one of: warn, info, debug, trace";
}

// Parse "host:port" or "[host]:port" (the latter for IPv6 literals).
// Splits on the LAST ':' for the non-bracket form. Throws on malformed input.
static std::pair<TString, ui16> ParseHostPort(const TString& s) {
    if (s.empty()) {
        ythrow yexception() << "endpoint is empty";
    }
    TString host;
    TStringBuf portBuf;
    if (s.front() == '[') {
        size_t close = s.find(']');
        if (close == TString::npos || close + 1 >= s.size() || s[close + 1] != ':') {
            ythrow yexception() << "malformed bracketed endpoint '" << s << "', expected [host]:port";
        }
        host = s.substr(1, close - 1);
        portBuf = TStringBuf(s).SubStr(close + 2);
    } else {
        size_t colon = s.rfind(':');
        if (colon == TString::npos) {
            ythrow yexception() << "endpoint '" << s << "' missing ':port'";
        }
        host = s.substr(0, colon);
        portBuf = TStringBuf(s).SubStr(colon + 1);
    }
    ui16 port = 0;
    if (!TryFromString<ui16>(portBuf, port) || port == 0) {
        ythrow yexception() << "endpoint '" << s << "' has invalid port";
    }
    if (host.empty()) {
        ythrow yexception() << "endpoint '" << s << "' has empty host";
    }
    return {host, port};
}

static void ServerSignalHandler(int) {
    NKikimr::DDiskServerStopEvent.Signal();
}

static void InstallServerSignalHandler() {
    signal(SIGINT, ServerSignalHandler);
    signal(SIGTERM, ServerSignalHandler);
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

#ifdef _linux_
static size_t NumberOfMyCpus() {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == -1) {
        return NSystemInfo::CachedNumberOfCpus();
    }

    int count = 0;
    for (int i = 0; i < CPU_SETSIZE; i++) {
        if (CPU_ISSET(i, &set))
            count++;
    }

    return count;
}
#else
static size_t NumberOfMyCpus() {
    return NSystemInfo::CachedNumberOfCpus();
}
#endif

int main(int argc, char **argv) {
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();
    bool disablePDiskDataEncryption = false;
    TVector<TString> paths;
    opts.AddLongOption("path", "path to device (can be specified multiple times for multi-device tests)")
        .RequiredArgument("FILE")
        .AppendTo(&paths);
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
    opts.AddLongOption("disable-pdisk-encryption", "disable PDisk data encryption").StoreTrue(&disablePDiskDataEncryption);
    opts.AddLongOption("log-level", "log level for BS_LOAD_TEST/BS_DDISK: warn|info|debug|trace (default warn). INTERCONNECT is floored at INFO; BS_DEVICE/BS_PDISK at WARN")
        .RequiredArgument("LEVEL").DefaultValue("warn");
    ui32 serverNodeId = 0;
    ui32 clientNodeId = 0;
    TVector<TString> clientEndpoints;
    ui16 icPort = 0;
    // DDisk client/server options are hidden from the auto-generated --help list and
    // re-rendered below as a dedicated "DDisk client/server options" section. Keep the
    // section text in sync with the option definitions here.
    opts.AddLongOption("server", "run as DDisk server with the given node ID (sets up PDisks/DDisks, listens via interconnect)")
        .RequiredArgument("NODE_ID").StoreResult(&serverNodeId).Hidden();
    opts.AddLongOption("client", "client node ID; in server mode this is the expected client's ID (used in the nameserver), in client mode this is the client's own ID")
        .RequiredArgument("NODE_ID").StoreResult(&clientNodeId).Hidden();
    opts.AddLongOption("endpoint", "server endpoint in 'host:port' or '[host]:port' form; repeat once per server (client only)")
        .RequiredArgument("HOST:PORT").AppendTo(&clientEndpoints).Hidden();
    opts.AddLongOption("ic-port", "interconnect port for server to listen on (server only)")
        .RequiredArgument("PORT").StoreResult(&icPort).Hidden();
    opts.AddLongOption("num-server-devices", "number of devices per server (default 1)")
        .RequiredArgument("N").DefaultValue("1").Hidden();

    {
        const size_t kCol = 26;
        auto row = [&](TStringBuf flag, TStringBuf help) {
            TString left = TString::Join("--", flag);
            if (left.size() < kCol) {
                return TString::Join(left, TString(kCol - left.size(), ' '), help, "\n");
            }
            return TString::Join(left, "\n", TString(kCol, ' '), help, "\n");
        };

        TStringBuilder ddiskHelp;
        ddiskHelp
            << row("server NODE_ID",
                   "run as DDisk server with the given node ID (sets up PDisks/DDisks, listens via interconnect)")
            << row("client NODE_ID",
                   "client node ID; in server mode this is the expected client's ID (used in the nameserver), "
                   "in client mode this is the client's own ID")
            << row("endpoint HOST:PORT",
                   "server endpoint in 'host:port' or '[host]:port' form; repeat once per server (client only)")
            << row("ic-port PORT",
                   "interconnect port for server to listen on (server only)")
            << row("num-server-devices N",
                   "number of devices per server (default 1)");

        opts.AddSection("DDisk client/server options", ddiskHelp);
    }

    TOptsParseResult res(&opts, argc, argv);

    // Server mode is selected by --server. Client mode by --client without --server.
    // In server mode, --client is also required and provides the expected client's NodeID
    // so the interconnect handshake can resolve the peer in the static nameserver table.
    const bool serverMode = res.Has("server");
    const bool clientMode = !serverMode && res.Has("client");
    if (serverMode) {
        if (serverNodeId == 0) {
            Cerr << "Error: --server requires a non-zero NODE_ID" << Endl;
            return 1;
        }
        if (!res.Has("client") || clientNodeId == 0) {
            Cerr << "Error: --server requires --client NODE_ID (expected client node ID, e.g. server_count + 1)" << Endl;
            return 1;
        }
        if (clientNodeId == serverNodeId) {
            Cerr << "Error: --server and --client NODE_IDs must differ" << Endl;
            return 1;
        }
        if (paths.empty()) {
            Cerr << "Error: --server requires at least one --path" << Endl;
            return 1;
        }
        if (!icPort) {
            Cerr << "Error: --server requires --ic-port" << Endl;
            return 1;
        }
    }
    if (clientMode) {
        if (clientNodeId == 0) {
            Cerr << "Error: --client requires a non-zero NODE_ID" << Endl;
            return 1;
        }
        if (clientEndpoints.empty()) {
            Cerr << "Error: --client requires at least one --endpoint" << Endl;
            return 1;
        }
    }

    if (!clientMode && paths.empty()) {
        Cerr << "Error: at least one --path must be specified" << Endl;
        return 1;
    }

    if (!res.Has("no-logo") && res.Get("output-format") != TString("json")) {
        Cout << ydb_logo << Flush;
    }

    // For client mode, paths can be empty; use a dummy for TPerfTestConfig
    if (clientMode && paths.empty()) {
        paths.push_back("unused");
    }

    NActors::NLog::EPriority logLevel = NActors::NLog::PRI_WARN;
    try {
        logLevel = ParseLogLevel(res.Get("log-level"));
    } catch (const yexception& ex) {
        Cerr << "Error: " << ex.what() << Endl;
        return 1;
    }

    NKikimr::TPerfTestConfig config(paths, res.Get("name"), res.Get("type"),
            res.Get("output-format"), res.Get("mon-port"), !res.Has("disable-file-lock"),
            res.Get("run-count"), res.Get("inflight-from"), res.Get("inflight-to"), disablePDiskDataEncryption,
            logLevel);
    NDevicePerfTest::TPerfTests protoTests;
    NKikimr::ParsePBFromFile(res.Get("cfg"), &protoTests);

    for (ui32 i = 0; i < protoTests.DDiskTestListSize(); ++i) {
        const auto& ddiskTest = protoTests.GetDDiskTestList(i);
        for (ui32 j = 0; j < ddiskTest.DDiskTestListSize(); ++j) {
            const auto& record = ddiskTest.GetDDiskTestList(j);
            if (record.Command_case() != NKikimr::TEvLoadTestRequest::CommandCase::kDDiskLoad) {
                continue;
            }

            const ui32 ioSizeBytes = record.GetDDiskLoad().GetIoSizeBytes();
            if (ioSizeBytes < 4096 || !IsPowerOf2(ioSizeBytes)) {
                Cerr << "Error: invalid DDiskLoad.IoSizeBytes in DDiskTestList[" << i
                    << "].DDiskTestList[" << j << "]: " << ioSizeBytes
                    << " (must be power of two and >= 4096)" << Endl;
                return 1;
            }
        }
    }

    // Client-server mode dispatch
    if (serverMode || clientMode) {
        if (protoTests.DDiskTestListSize() == 0) {
            Cerr << "Error: --server/--client mode requires DDiskTestList in config" << Endl;
            return 1;
        }
        NDevicePerfTest::TDDiskTest testProto = protoTests.GetDDiskTestList(0);

        if (serverMode) {
            InstallServerSignalHandler();
            auto printer = MakeIntrusive<NKikimr::TResultPrinter>(config.OutputFormat, config.RunCount);
            THolder<NKikimr::TPerfTest> test(new NKikimr::TDDiskServer<>(config, testProto, serverNodeId, clientNodeId, icPort));
            test->SetPrinter(printer);
            test->RunTest();
            return 0;
        }

        if (clientMode) {
            ui32 numServerDevices = FromString<ui32>(res.Get("num-server-devices"));

            // Build server peer list. Server with index i (0-based) gets NodeId = i + 1.
            TVector<NKikimr::TInterconnectPeer> serverPeers;
            serverPeers.reserve(clientEndpoints.size());
            for (size_t i = 0; i < clientEndpoints.size(); ++i) {
                try {
                    auto [host, port] = ParseHostPort(clientEndpoints[i]);
                    serverPeers.push_back({static_cast<ui32>(i + 1), host, port});
                } catch (const yexception& ex) {
                    Cerr << "Error: --endpoint #" << (i + 1) << ": " << ex.what() << Endl;
                    return 1;
                }
            }

            auto overrideDDiskInFlight = [](NDevicePerfTest::TDDiskTest& tp, ui32 inFlight) {
                for (size_t j = 0; j < tp.DDiskTestListSize(); ++j) {
                    auto* record = tp.MutableDDiskTestList(j);
                    if (record->Command_case() == NKikimr::TEvLoadTestRequest::CommandCase::kDDiskLoad) {
                        record->MutableDDiskLoad()->SetInFlight(inFlight);
                    }
                }
            };

            auto printer = MakeIntrusive<NKikimr::TResultPrinter>(config.OutputFormat, config.RunCount);
            if (config.HasInFlightOverride()) {
                for (ui32 inFlight = config.InFlightFrom; inFlight <= config.InFlightTo; inFlight *= 2) {
                    overrideDDiskInFlight(testProto, inFlight);
                    for (ui32 run = 0; run < config.RunCount; ++run) {
                        THolder<NKikimr::TPerfTest> test(
                            new NKikimr::TDDiskClient(config, testProto, clientNodeId, serverPeers, numServerDevices));
                        test->SetPrinter(printer);
                        test->RunTest();
                    }
                }
            } else {
                for (ui32 run = 0; run < config.RunCount; ++run) {
                    THolder<NKikimr::TPerfTest> test(
                        new NKikimr::TDDiskClient(config, testProto, clientNodeId, serverPeers, numServerDevices));
                    test->SetPrinter(printer);
                    test->RunTest();
                }
            }
            printer->EndTest();
            return 0;
        }
    }

#ifndef NDEBUG
    Cerr << "Warning: you're running stress tool built without NDEBUG defined, results will be much worse than expected"
        << Endl;
#endif

    // When run-count > 1, only one test is allowed in the config
    if (config.RunCount > 1) {
        ui32 totalTests = 0;
        totalTests += protoTests.AioTestListSize();
#ifdef _linux_
        totalTests += protoTests.UringRouterTestListSize();
#endif
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

    // Multi-device is only supported for PDisk/DDisk/UringRouter/PersistentBuffer tests
    if (config.NumDevices() > 1) {
        if (protoTests.AioTestListSize() > 0) {
            Cerr << "Error: multiple --path is not supported for AioTest" << Endl;
            return 1;
        }
        if (protoTests.TrimTestListSize() > 0) {
            Cerr << "Error: multiple --path is not supported for TrimTest" << Endl;
            return 1;
        }
        if (protoTests.HasDriveEstimatorTest()) {
            Cerr << "Error: multiple --path is not supported for DriveEstimatorTest" << Endl;
            return 1;
        }

        // 1 pdisk actor thread + 1 load actor thread per device, plus ~4 for nameservice/coordinator/IO/scheduler
        static constexpr size_t CpuCoresPerDevice = 2;
        static constexpr size_t CpuCoresOverhead = 4;

        const size_t cpus = NumberOfMyCpus();
        const size_t recommended = config.NumDevices() * CpuCoresPerDevice + CpuCoresOverhead;
        if (cpus < recommended) {
            Cerr << "Warning: " << config.NumDevices() << " devices but only " << cpus
                 << " CPUs available (recommended at least " << recommended
                 << "), performance results may be unreliable" << Endl;
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

#ifdef _linux_
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
#endif

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
            if (record->Command_case() == NKikimr::TEvLoadTestRequest::CommandCase::kDDiskLoad) {
                record->MutableDDiskLoad()->SetInFlight(inFlight);
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

    auto overridePBufferInFlight = [](NDevicePerfTest::TPersistentBufferTest& testProto, ui32 inFlight) {
        for (size_t j = 0; j < testProto.PersistentBufferTestListSize(); ++j) {
            auto* record = testProto.MutablePersistentBufferTestList(j);
            if (record->Command_case() == NKikimr::TEvLoadTestRequest::CommandCase::kPersistentBufferWriteLoad) {
                record->MutablePersistentBufferWriteLoad()->SetInFlightWrites(inFlight);
            }
        }
    };
    auto overridePBufferMeasureType = [](NDevicePerfTest::TPersistentBufferTest& testProto, ui32 measure) {
        for (size_t j = 0; j < testProto.PersistentBufferTestListSize(); ++j) {
            auto* record = testProto.MutablePersistentBufferTestList(j);
            if (record->Command_case() == NKikimr::TEvLoadTestRequest::CommandCase::kPersistentBufferWriteLoad) {
                switch (measure) {
                    case 0:
                        record->MutablePersistentBufferWriteLoad()->SetMeasureType(NKikimr::TEvLoadTestRequest::TPersistentBufferWriteLoad::WRITE);
                        break;
                    case 1:
                        record->MutablePersistentBufferWriteLoad()->SetMeasureType(NKikimr::TEvLoadTestRequest::TPersistentBufferWriteLoad::READ);
                        break;
                    case 2:
                        record->MutablePersistentBufferWriteLoad()->SetMeasureType(NKikimr::TEvLoadTestRequest::TPersistentBufferWriteLoad::ERASE);
                        break;
                }
            }
        }
    };
    for (ui32 i = 0; i < protoTests.PersistentBufferTestListSize(); ++i) {
        NDevicePerfTest::TPersistentBufferTest testProto = protoTests.GetPersistentBufferTestList(i);
        if (config.HasInFlightOverride()) {
            for (ui32 inFlight = config.InFlightFrom; inFlight <= config.InFlightTo; inFlight *= 2) {
                overridePBufferInFlight(testProto, inFlight);
                for (ui32 run = 0; run < config.RunCount; ++run) {
                    for (ui32 measureType : xrange(3)) {
                        overridePBufferMeasureType(testProto, measureType);
                        THolder<NKikimr::TPerfTest> test(new NKikimr::TPersistentBufferTest(config, testProto));
                        test->SetPrinter(printer);
                        test->RunTest();
                    }
                }
            }
        } else {
            for (ui32 run = 0; run < config.RunCount; ++run) {
                for (ui32 measureType : xrange(3)) {
                    overridePBufferMeasureType(testProto, measureType);
                    THolder<NKikimr::TPerfTest> test(new NKikimr::TPersistentBufferTest(config, testProto));
                    test->SetPrinter(printer);
                    test->RunTest();
                }
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
