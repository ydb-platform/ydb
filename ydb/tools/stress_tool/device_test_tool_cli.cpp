#include "device_test_tool_cli.h"

#include <ydb/core/util/pb.h>

#include <util/generic/bitops.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <cmath>
#include <initializer_list>

namespace NKikimr::NStressTool {
namespace {

const std::initializer_list<const char*> WorkloadOptions = {
    "areas", "duration", "read-only", "io-size", "sequential",
    "background-write-ratio", "background-write-size-kib",
};

void AddOptionSection(NLastGetopt::TOpts& opts, const TString& title,
        std::initializer_list<const char*> names) {
    TStringBuilder help;
    for (const char* name : names) {
        auto& opt = opts.GetLongOption(name);
        opt.Hidden();
        help << "  --" << name;
        if (opt.GetHasArg() != NLastGetopt::NO_ARGUMENT) {
            help << " " << opt.GetArgTitle();
        }
        help << "\n      " << opt.GetHelp();
        if (opt.HasDefaultValue()) {
            help << " (default: " << opt.GetDefaultValue() << ")";
        }
        help << "\n";
    }
    opts.AddSection(title, help);
}

ui32 PositiveValue(const NLastGetopt::TOptsParseResult& res, const TString& name) {
    const TString value = res.Get(name);
    ui32 result = 0;
    for (char c : value) {
        Y_ENSURE(c >= '0' && c <= '9', "--" << name << " requires a positive uint32 value");
    }
    Y_ENSURE(TryFromString(value, result) && result, "--" << name << " requires a positive uint32 value");
    return result;
}

} // namespace

TCommandLine::TCommandLine(bool ddisk)
    : DDisk(ddisk)
    , Opts(NLastGetopt::TOpts::Default())
{
    using namespace NLastGetopt;
    auto& opts = Opts;
    opts.AddLongOption("path", "path to device (can be specified multiple times for multi-device tests)")
        .RequiredArgument("FILE")
        .AppendTo(&Paths);
    auto& cfg = opts.AddLongOption("cfg", "path to config file (cannot combine with DDisk workload options)").RequiredArgument("FILE");
    if (!DDisk) {
        cfg.DefaultValue("cfg.txt");
        opts.AddSection("Commands", "  ddisk <options>  Run DDisk tests without a config file; see ddisk --help.\n");
    } else {
        opts.SetFreeArgsNum(0);
    }
    opts.AddLongOption("name", "device name").DefaultValue("Name");
    opts.AddLongOption("type", "device type  - ROT|SSD|NVME").DefaultValue("ROT");
    opts.AddLongOption("output-format", "wiki|human|json").DefaultValue("wiki");
    opts.AddLongOption("mon-port", "port for monitoring http page").DefaultValue("0");
    opts.AddLongOption("run-count", "number of times to run each test").DefaultValue("1");
    opts.AddLongOption("inflight", "override InFlight with one positive value; mutually exclusive with an inflight range (config-free DDisk default: 128)").RequiredArgument("N");
    opts.AddLongOption("inflight-from", "override InFlight starting value (PDisk/DDisk/UringRouter tests)").DefaultValue("0");
    opts.AddLongOption("inflight-to", "override InFlight ending value (PDisk/DDisk/UringRouter tests)").DefaultValue("0");
    opts.AddLongOption("no-logo", "disable logo printing on start").NoArgument();
    opts.AddLongOption("disable-file-lock", "disable file locking before test").NoArgument().DefaultValue("0");
    opts.AddLongOption("disable-pdisk-encryption", "disable PDisk data encryption").StoreTrue(&DisablePDiskDataEncryption);
    opts.AddLongOption("disable-ddisk-checksums",
            "disable DDisk and Persistent Buffer checksums (use on both client and server)")
        .StoreTrue(&DisableDDiskChecksums);
    opts.AddLongOption("ddisk-checksums-cache-size",
            "DDisk checksum cache size per DDisk in MiB (default 64; 0 disables caching; set on server for client/server tests)")
        .RequiredArgument("MiB");
    opts.AddLongOption("force-ddisk-pdisk-fallback",
            "force DDisk direct I/O through the PDisk actor instead of io_uring")
        .StoreTrue(&ForcePDiskFallback);
    opts.AddLongOption("log-level", "log level for BS_LOAD_TEST/BS_DDISK: warn|info|debug|trace (default warn). INTERCONNECT is floored at INFO; BS_DEVICE/BS_PDISK at WARN")
        .RequiredArgument("LEVEL").DefaultValue("warn");
    opts.AddLongOption("server", DDisk
        ? "run as DDisk server with the given node ID (sets up PDisks/DDisks, listens via interconnect)"
        : "run as DDisk server with the given node ID (sets up PDisks/DDisks, listens via interconnect); "
            "if the config has no DDiskTestList but has InterconnectTestList, runs as an interconnect load responder instead "
            "(for testing interconnect over a real network between two ydb_stress_tool instances)")
        .RequiredArgument("NODE_ID").StoreResult(&ServerNodeId).Hidden();
    opts.AddLongOption("client", "client node ID; in server mode this is the expected client's ID (used in the nameserver), in client mode this is the client's own ID")
        .RequiredArgument("NODE_ID").StoreResult(&ClientNodeId).Hidden();
    opts.AddLongOption("endpoint", "server endpoint in 'host:port' or '[host]:port' form; repeat once per server (client only)")
        .RequiredArgument("HOST:PORT").AppendTo(&ClientEndpoints).Hidden();
    opts.AddLongOption("ic-port", "interconnect port for server to listen on (server only)")
        .RequiredArgument("PORT").StoreResult(&IcPort).Hidden();
    opts.AddLongOption("num-server-devices", "number of devices per server (default 1); ignored in interconnect server/client mode")
        .RequiredArgument("N").DefaultValue("1").Hidden();

    if (DDisk) {
        opts.AddLongOption("areas", "number of equally weighted 128 MiB areas per device").RequiredArgument("N").DefaultValue("32");
        opts.AddLongOption("duration", "load duration in seconds (initialization is additional)").RequiredArgument("SECONDS").DefaultValue("10");
        opts.AddLongOption("read-only", "measure reads instead of writes; initialization writes still occur, and explicit background writes are allowed").NoArgument();
        opts.AddLongOption("io-size", "read/write request size in bytes (power of two, at least 4096)").RequiredArgument("BYTES").DefaultValue("4096");
        opts.AddLongOption("sequential", "access each area sequentially instead of randomly").NoArgument();
        opts.AddLongOption("background-write-ratio", "unmeasured writes per measured read, in [0, 1]; nonzero requires --read-only").RequiredArgument("R").DefaultValue("0");
        opts.AddLongOption("background-write-size-kib", "background write size in KiB (power of two, at least 4)").RequiredArgument("N").DefaultValue("4");
    }

    AddOptionSection(opts, "DDisk options", {
        "disable-ddisk-checksums", "ddisk-checksums-cache-size", "force-ddisk-pdisk-fallback",
    });
    if (DDisk) {
        AddOptionSection(opts, "DDisk workload options (without --cfg)", WorkloadOptions);
    }
    AddOptionSection(opts, DDisk ? "DDisk client/server options" : "DDisk / Interconnect client/server options", {
        "server", "client", "endpoint", "ic-port", "num-server-devices",
    });
}

TInFlightOptions ResolveInFlight(const NLastGetopt::TOptsParseResult& res, bool ddisk) {
    if (res.Has("inflight")) {
        Y_ENSURE(!res.Has("inflight-from") && !res.Has("inflight-to"),
            "--inflight cannot be combined with --inflight-from/--inflight-to");
        const auto value = ToString(PositiveValue(res, "inflight"));
        return {value, value};
    }
    if (ddisk && (res.Has("inflight-from") || res.Has("inflight-to"))) {
        Y_ENSURE(res.Has("inflight-from") && res.Has("inflight-to"),
            "--inflight-from and --inflight-to must be specified together");
        const ui32 from = PositiveValue(res, "inflight-from");
        const ui32 to = PositiveValue(res, "inflight-to");
        Y_ENSURE(from <= to, "--inflight-from must not exceed --inflight-to");
        return {ToString(from), ToString(to)};
    }
    // Preserve legacy parsing and its treatment of zero/incomplete ranges.
    return {res.Get("inflight-from"), res.Get("inflight-to")};
}

TVector<ui32> InFlightValues(ui32 from, ui32 to) {
    TVector<ui32> values;
    for (ui64 value = from; value && value <= to; value *= 2) {
        values.push_back(static_cast<ui32>(value));
    }
    return values;
}

NDevicePerfTest::TPerfTests MakeDDiskTests(const NLastGetopt::TOptsParseResult& res) {
    const ui32 areas = PositiveValue(res, "areas");
    const ui32 duration = PositiveValue(res, "duration");
    const ui32 ioSize = PositiveValue(res, "io-size");
    Y_ENSURE(ioSize >= 4096 && IsPowerOf2(ioSize) && DDiskChunkSize % ioSize == 0,
        "--io-size must be a power of two between 4096 and 134217728 bytes");
    const float ratio = res.Get<float>("background-write-ratio");
    Y_ENSURE(std::isfinite(ratio) && ratio >= 0 && ratio <= 1,
        "--background-write-ratio must be finite and in [0, 1]");
    Y_ENSURE(ratio == 0 || res.Has("read-only"),
        "nonzero --background-write-ratio requires --read-only");
    const ui32 writeSize = PositiveValue(res, "background-write-size-kib");
    Y_ENSURE(writeSize >= 4 && IsPowerOf2(writeSize) && writeSize <= DDiskChunkSize / 1024,
        "--background-write-size-kib must be a power of two between 4 and 131072 KiB");

    NDevicePerfTest::TPerfTests tests;
    auto* load = tests.AddDDiskTestList()->AddDDiskTestList()->MutableDDiskLoad();
    load->SetTag(1);
    auto* id = load->MutableDDiskId();
    id->SetNodeId(1);
    id->SetPDiskId(1);
    id->SetDDiskSlotId(1);
    load->SetDurationSeconds(duration);
    load->SetDelayBeforeMeasurementsSeconds(0);
    load->SetIntervalMsMin(0);
    load->SetIntervalMsMax(0);
    load->SetInFlight(128);
    load->SetExpectedChunkSize(DDiskChunkSize);
    load->SetIoSizeBytes(ioSize);
    load->SetIsReadLoad(res.Has("read-only"));
    load->SetBackgroundWriteRatio(ratio);
    load->SetBackgroundWriteSizeKiB(writeSize);
    for (ui32 i = 0; i < areas; ++i) {
        auto* area = load->AddAreas();
        area->SetAreaSize(DDiskChunkSize);
        area->SetWeight(1);
        area->SetSequential(res.Has("sequential"));
    }
    return tests;
}

void ValidateDDiskTests(const NDevicePerfTest::TPerfTests& tests) {
    Y_ENSURE(tests.DDiskTestListSize() && !tests.AioTestListSize() && !tests.TrimTestListSize()
        && !tests.PDiskTestListSize() && !tests.HasDriveEstimatorTest()
        && !tests.UringRouterTestListSize() && !tests.PersistentBufferTestListSize()
        && !tests.InterconnectTestListSize(), "ddisk --cfg requires DDisk-only workloads");
    for (const auto& test : tests.GetDDiskTestList()) {
        Y_ENSURE(test.DDiskTestListSize(), "DDiskTestList must contain at least one DDiskLoad");
        for (const auto& record : test.GetDDiskTestList()) {
            Y_ENSURE(record.HasDDiskLoad(), "ddisk --cfg requires DDiskLoad records");
        }
    }
}

NDevicePerfTest::TPerfTests LoadTests(const NLastGetopt::TOptsParseResult& res, bool ddisk) {
    if (ddisk && !res.Has("cfg")) {
        return MakeDDiskTests(res);
    }
    if (ddisk) {
        for (const char* name : WorkloadOptions) {
            Y_ENSURE(!res.Has(name), "--cfg cannot be combined with --" << name);
        }
    }
    NDevicePerfTest::TPerfTests tests;
    const bool parsed = ParsePBFromFile(res.Get("cfg"), &tests);
    if (ddisk) {
        Y_ENSURE(parsed, "cannot parse DDisk config: " << res.Get("cfg"));
        ValidateDDiskTests(tests);
    }
    return tests;
}

} // namespace NKikimr::NStressTool
