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

const std::initializer_list<const char*> DDiskOptions = {
    "disable-ddisk-checksums", "ddisk-checksums-cache-size", "force-ddisk-pdisk-fallback", "ddisk-devnull",
};

const std::initializer_list<const char*> NetworkOptions = {
    "server", "client", "endpoint", "ic-port", "num-server-devices",
};

#ifndef _darwin_
void AddOptionSection(
    NLastGetopt::TOpts& opts,
    const TString& title,
    std::initializer_list<const char*> names)
{
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
#endif

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
#ifdef _darwin_
    opts.AddLongOption("path", "sector map: SectorMap:<name>[:<GiB>[:<mode>]] (repeat for multiple maps)")
#else
    opts.AddLongOption("path", "path to device (can be specified multiple times for multi-device tests)")
#endif
        .RequiredArgument("FILE")
        .AppendTo(&Paths);
#ifdef _darwin_
    auto& cfg = opts.AddLongOption("cfg", "path to config containing PDisk read, write, or log workloads").RequiredArgument("FILE");
#else
    auto& cfg = opts.AddLongOption("cfg", "path to config file (cannot combine with DDisk workload options)").RequiredArgument("FILE");
#endif
    if (!DDisk) {
        cfg.DefaultValue("cfg.txt");
#ifndef _darwin_
        opts.AddSection("Commands", "  ddisk <options>  Run DDisk tests without a config file; see ddisk --help.\n");
#endif
    } else {
        opts.SetFreeArgsNum(0);
    }
    opts.AddLongOption("name", "device name").DefaultValue("Name");
    opts.AddLongOption("type", "device type  - ROT|SSD|NVME").DefaultValue("ROT");
    opts.AddLongOption("output-format", "wiki|human|json").DefaultValue("wiki");
    opts.AddLongOption("mon-port", "port for monitoring http page").DefaultValue("0");
    opts.AddLongOption("run-count", "number of times to run each test").DefaultValue("1");
#ifdef _darwin_
    opts.AddLongOption("inflight", "override PDisk read/write InFlight with one positive value; mutually exclusive with an inflight range").RequiredArgument("N");
    opts.AddLongOption("inflight-from", "override PDisk read/write InFlight starting value").DefaultValue("0");
    opts.AddLongOption("inflight-to", "override PDisk read/write InFlight ending value").DefaultValue("0");
#else
    opts.AddLongOption("inflight", "override InFlight with one positive value; mutually exclusive with an inflight range (config-free DDisk default: 128)").RequiredArgument("N");
    opts.AddLongOption("inflight-from", "override InFlight starting value (PDisk/DDisk/UringRouter tests)").DefaultValue("0");
    opts.AddLongOption("inflight-to", "override InFlight ending value (PDisk/DDisk/UringRouter tests)").DefaultValue("0");
#endif
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
    opts.AddLongOption("ddisk-devnull",
            "acknowledge DDisk data I/O without device reads/writes (zero-filled reads; router-attaching slots on a PDisk must agree)")
        .StoreTrue(&DDiskDevNullMode);
#ifdef _darwin_
    opts.AddLongOption("log-level", "log level for BS_LOAD_TEST: warn|info|debug|trace (default warn). BS_DEVICE/BS_PDISK are floored at WARN")
#else
    opts.AddLongOption("log-level", "log level for BS_LOAD_TEST/BS_DDISK: warn|info|debug|trace (default warn). INTERCONNECT is floored at INFO; BS_DEVICE/BS_PDISK at WARN")
#endif
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

#ifdef _darwin_
    for (const char* name : DDiskOptions) {
        opts.GetLongOption(name).Hidden();
    }
    if (DDisk) {
        for (const char* name : WorkloadOptions) {
            opts.GetLongOption(name).Hidden();
        }
    }
    opts.GetLongOption("disable-file-lock").Hidden();
    opts.AddSection("Supported workloads", "  macOS supports local PDisk read, write, and log workloads on sector maps.\n");
#else
    AddOptionSection(opts, "DDisk options", DDiskOptions);
    if (DDisk) {
        AddOptionSection(opts, "DDisk workload options (without --cfg)", WorkloadOptions);
    }
    AddOptionSection(opts, DDisk ? "DDisk client/server options" : "DDisk / Interconnect client/server options", NetworkOptions);
#endif
}

TInFlightOptions ResolveInFlight(const NLastGetopt::TOptsParseResult& res) {
    if (res.Has("inflight")) {
        Y_ENSURE(!res.Has("inflight-from") && !res.Has("inflight-to"),
            "--inflight cannot be combined with --inflight-from/--inflight-to");
        const auto value = ToString(PositiveValue(res, "inflight"));
        return {value, value};
    }
    if (res.Has("inflight-from") || res.Has("inflight-to")) {
        Y_ENSURE(res.Has("inflight-from") && res.Has("inflight-to"),
            "--inflight-from and --inflight-to must be specified together");
        const ui32 from = PositiveValue(res, "inflight-from");
        const ui32 to = PositiveValue(res, "inflight-to");
        Y_ENSURE(from <= to, "--inflight-from must not exceed --inflight-to");
        return {ToString(from), ToString(to)};
    }
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

void ValidateDDiskOptions(const NLastGetopt::TOptsParseResult& res) {
    if (!res.Has("ddisk-devnull")) {
        return;
    }
    Y_ENSURE(!res.Has("force-ddisk-pdisk-fallback"),
        "--ddisk-devnull cannot be combined with --force-ddisk-pdisk-fallback");
    // Only the process owning the DDisks can select the router's mode.
    Y_ENSURE(!res.Has("client") || res.Has("server"),
        "--ddisk-devnull has no effect on the client; pass it to the server process");
}

void ValidatePDiskSectorMapOptions(const NLastGetopt::TOptsParseResult& res, bool ddisk) {
    Y_ENSURE(!ddisk, "macOS supports only PDisk sector-map workloads; the ddisk command is not supported");
    for (const auto& names : {DDiskOptions, NetworkOptions}) {
        for (const char* name : names) {
            Y_ENSURE(!res.Has(name), "macOS supports only PDisk sector-map workloads; --" << name << " is not supported");
        }
    }
}

void ValidatePDiskSectorMapTests(const NDevicePerfTest::TPerfTests& tests) {
    Y_ENSURE(tests.PDiskTestListSize() && !tests.AioTestListSize() && !tests.TrimTestListSize()
        && !tests.HasDriveEstimatorTest() && !tests.DDiskTestListSize()
        && !tests.UringRouterTestListSize() && !tests.PersistentBufferTestListSize()
        && !tests.InterconnectTestListSize(),
        "macOS requires a nonempty PDiskTestList containing only PDisk sector-map workloads");
    for (const auto& test : tests.GetPDiskTestList()) {
        Y_ENSURE(test.PDiskTestListSize(), "PDiskTestList must contain at least one PDisk read, write, or log load");
        for (const auto& record : test.GetPDiskTestList()) {
            Y_ENSURE(record.HasPDiskReadLoad() || record.HasPDiskWriteLoad() || record.HasPDiskLogLoad(),
                "macOS PDiskTestList supports only PDiskReadLoad, PDiskWriteLoad, and PDiskLogLoad records");
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
#ifdef _darwin_
    if (!ddisk) {
        Y_ENSURE(parsed, "cannot parse PDisk config: " << res.Get("cfg"));
    }
#endif
    return tests;
}

} // namespace NKikimr::NStressTool
