#include "../device_test_tool_cli.h"

#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/system/tempfile.h>

using namespace NKikimr::NStressTool;

namespace {

struct TParsed {
    TCommandLine Cli;
    TVector<const char*> Args;
    NLastGetopt::TOptsParseResultException Result;

    TParsed(std::initializer_list<const char*> args, bool ddisk = true)
        : Cli(ddisk)
        , Args(args)
        , Result(&Cli.Opts, Args.size(), Args.data())
    {}
};

} // namespace

Y_UNIT_TEST_SUITE(TStressToolCli) {
    Y_UNIT_TEST(DevNullOptionAndFallbackConflict) {
        TParsed defaults({"tool ddisk"});
        UNIT_ASSERT(!defaults.Cli.DDiskDevNullMode);
        ValidateDDiskOptions(defaults.Result);
        TParsed enabled({"tool ddisk", "--ddisk-devnull"});
        UNIT_ASSERT(enabled.Cli.DDiskDevNullMode);
        ValidateDDiskOptions(enabled.Result);
        UNIT_ASSERT_VALUES_EQUAL(LoadTests(enabled.Result, true).DDiskTestListSize(), 1);
        for (bool ddisk : {false, true}) {
            TParsed conflict({"tool", "--ddisk-devnull", "--force-ddisk-pdisk-fallback"}, ddisk);
            UNIT_ASSERT_EXCEPTION_CONTAINS(ValidateDDiskOptions(conflict.Result), yexception,
                "--ddisk-devnull cannot be combined with --force-ddisk-pdisk-fallback");
        }
    }

    Y_UNIT_TEST(DevNullOptionIsRejectedOnTheClient) {
        TParsed client({"tool ddisk", "--ddisk-devnull", "--client", "2", "--endpoint", "localhost:1"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(ValidateDDiskOptions(client.Result), yexception,
            "pass it to the server process");
        TParsed server({"tool ddisk", "--ddisk-devnull", "--server", "1", "--client", "2", "--ic-port", "1"});
        ValidateDDiskOptions(server.Result);
        UNIT_ASSERT(server.Cli.DDiskDevNullMode);
    }

    Y_UNIT_TEST(GeneratedDefaults) {
        TParsed parsed({"tool ddisk"});
        UNIT_ASSERT(!parsed.Result.Has("cfg", true));
        const auto tests = LoadTests(parsed.Result, true);
        UNIT_ASSERT_VALUES_EQUAL(tests.DDiskTestListSize(), 1);
        const auto& test = tests.GetDDiskTestList(0);
        UNIT_ASSERT_VALUES_EQUAL(test.DDiskTestListSize(), 1);
        const auto& load = test.GetDDiskTestList(0).GetDDiskLoad();
        UNIT_ASSERT_VALUES_EQUAL(load.GetTag(), 1);
        UNIT_ASSERT_VALUES_EQUAL(load.GetDDiskId().GetNodeId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(load.GetDDiskId().GetPDiskId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(load.GetDDiskId().GetDDiskSlotId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(load.GetDurationSeconds(), 10);
        UNIT_ASSERT_VALUES_EQUAL(load.GetDelayBeforeMeasurementsSeconds(), 0);
        UNIT_ASSERT_VALUES_EQUAL(load.GetIntervalMsMin(), 0);
        UNIT_ASSERT_VALUES_EQUAL(load.GetIntervalMsMax(), 0);
        UNIT_ASSERT_VALUES_EQUAL(load.GetInFlight(), 128);
        UNIT_ASSERT_VALUES_EQUAL(load.GetIoSizeBytes(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(load.GetExpectedChunkSize(), 128 << 20);
        UNIT_ASSERT_VALUES_EQUAL(load.GetBackgroundWriteRatio(), 0);
        UNIT_ASSERT_VALUES_EQUAL(load.GetBackgroundWriteSizeKiB(), 4);
        UNIT_ASSERT(!load.GetIsReadLoad());
        UNIT_ASSERT_VALUES_EQUAL(load.AreasSize(), 32);
        for (const auto& area : load.GetAreas()) {
            UNIT_ASSERT_VALUES_EQUAL(area.GetAreaSize(), 128 << 20);
            UNIT_ASSERT_VALUES_EQUAL(area.GetWeight(), 1);
            UNIT_ASSERT(!area.GetSequential());
            UNIT_ASSERT(!area.HasInitType());
        }
    }

    Y_UNIT_TEST(GeneratedOverrides) {
        TParsed parsed({"tool ddisk", "--areas", "2", "--duration", "3", "--read-only",
            "--sequential", "--io-size", "65536", "--background-write-ratio", "0.5",
            "--background-write-size-kib", "8"});
        const auto tests = LoadTests(parsed.Result, true);
        const auto& load = tests.GetDDiskTestList(0).GetDDiskTestList(0).GetDDiskLoad();
        UNIT_ASSERT_VALUES_EQUAL(load.AreasSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(load.GetDurationSeconds(), 3);
        UNIT_ASSERT_VALUES_EQUAL(load.GetIoSizeBytes(), 65536);
        UNIT_ASSERT_VALUES_EQUAL(load.GetBackgroundWriteRatio(), 0.5);
        UNIT_ASSERT_VALUES_EQUAL(load.GetBackgroundWriteSizeKiB(), 8);
        UNIT_ASSERT(load.GetIsReadLoad());
        UNIT_ASSERT(load.GetAreas(0).GetSequential());
        UNIT_ASSERT(!load.GetAreas(0).HasInitType());
    }

    Y_UNIT_TEST(InvalidWorkloadOptions) {
        for (const auto& [name, value] : TVector<std::pair<const char*, const char*>>{
                {"--areas", "0"}, {"--areas", "-1"}, {"--areas", "4294967296"},
                {"--duration", "0"}, {"--duration", "no"}, {"--io-size", "2048"},
                {"--io-size", "12288"}, {"--io-size", "268435456"},
                {"--background-write-ratio", "-0.1"}, {"--background-write-ratio", "1.1"},
                {"--background-write-ratio", "nan"}, {"--background-write-ratio", "inf"},
                {"--background-write-size-kib", "2"}, {"--background-write-size-kib", "12"},
                {"--background-write-size-kib", "262144"}}) {
            TParsed parsed({"tool ddisk", "--read-only", name, value});
            UNIT_ASSERT_EXCEPTION(LoadTests(parsed.Result, true), yexception);
        }
        TParsed write({"tool ddisk", "--background-write-ratio", "0.5"});
        UNIT_ASSERT_EXCEPTION(LoadTests(write.Result, true), yexception);
    }

    Y_UNIT_TEST(InFlightOptions) {
        for (bool ddisk : {false, true}) {
            TParsed defaults({"tool"}, ddisk);
            const auto unset = ResolveInFlight(defaults.Result);
            UNIT_ASSERT_VALUES_EQUAL(unset.From, "0");
            UNIT_ASSERT_VALUES_EQUAL(unset.To, "0");
            TParsed single({"tool", "--inflight", "17"}, ddisk);
            const auto one = ResolveInFlight(single.Result);
            UNIT_ASSERT_VALUES_EQUAL(one.From, "17");
            UNIT_ASSERT_VALUES_EQUAL(one.To, "17");
            TParsed conflict({"tool", "--inflight", "17", "--inflight-from", "1"}, ddisk);
            UNIT_ASSERT_EXCEPTION(ResolveInFlight(conflict.Result), yexception);
            TParsed range({"tool", "--inflight-from", "3", "--inflight-to", "12"}, ddisk);
            const auto values = ResolveInFlight(range.Result);
            UNIT_ASSERT_VALUES_EQUAL(values.From, "3");
            UNIT_ASSERT_VALUES_EQUAL(values.To, "12");
        }
        UNIT_ASSERT_VALUES_EQUAL(InFlightValues(3, 12), (TVector<ui32>{3, 6, 12}));
        UNIT_ASSERT_VALUES_EQUAL(InFlightValues(3, 11), (TVector<ui32>{3, 6}));
        UNIT_ASSERT_VALUES_EQUAL(InFlightValues(1u << 31, Max<ui32>()), (TVector<ui32>{1u << 31}));
        UNIT_ASSERT_VALUES_EQUAL(InFlightValues(Max<ui32>(), Max<ui32>()), (TVector<ui32>{Max<ui32>()}));
        UNIT_ASSERT(InFlightValues(0, 10).empty());
    }

    Y_UNIT_TEST(InvalidInFlightOptions) {
        for (bool ddisk : {false, true}) {
            for (const char* name : {"--inflight-from", "--inflight-to"}) {
                TParsed incomplete({"tool", name, "8"}, ddisk);
                UNIT_ASSERT_EXCEPTION_CONTAINS(ResolveInFlight(incomplete.Result), yexception,
                    "--inflight-from and --inflight-to must be specified together");
            }
            TParsed inverted({"tool", "--inflight-from", "8", "--inflight-to", "4"}, ddisk);
            UNIT_ASSERT_EXCEPTION_CONTAINS(ResolveInFlight(inverted.Result), yexception,
                "--inflight-from must not exceed --inflight-to");
            for (const char* value : {"0", "-1", "bad", "", "4294967296"}) {
                TParsed single({"tool", "--inflight", value}, ddisk);
                UNIT_ASSERT_EXCEPTION_CONTAINS(ResolveInFlight(single.Result), yexception,
                    "--inflight requires a positive uint32 value");
                TParsed from({"tool", "--inflight-from", value, "--inflight-to", "8"}, ddisk);
                UNIT_ASSERT_EXCEPTION_CONTAINS(ResolveInFlight(from.Result), yexception,
                    "--inflight-from requires a positive uint32 value");
                TParsed to({"tool", "--inflight-from", "1", "--inflight-to", value}, ddisk);
                UNIT_ASSERT_EXCEPTION_CONTAINS(ResolveInFlight(to.Result), yexception,
                    "--inflight-to requires a positive uint32 value");
            }
        }
    }

    Y_UNIT_TEST(LegacyDefaults) {
        TParsed defaults({"tool"}, false);
        UNIT_ASSERT_VALUES_EQUAL(defaults.Result.Get("cfg"), "cfg.txt");
        UNIT_ASSERT_EXCEPTION((TParsed({"tool", "--areas", "2"}, false)), yexception);
    }

    Y_UNIT_TEST(ConfigCompatibilityAndConflicts) {
        TTempFileHandle file;
        {
            TFileOutput out(file.Name());
            out << "DDiskTestList { DDiskTestList { DDiskLoad { DurationSeconds: 37 "
                "DelayBeforeMeasurementsSeconds: 5 InFlight: 7 Areas { AreaSize: 10485760 } } } }";
        }
        TParsed parsed({"tool ddisk", "--cfg", file.Name().c_str(), "--inflight", "8"});
        const auto tests = LoadTests(parsed.Result, true);
        const auto& load = tests.GetDDiskTestList(0).GetDDiskTestList(0).GetDDiskLoad();
        UNIT_ASSERT_VALUES_EQUAL(load.GetDurationSeconds(), 37);
        UNIT_ASSERT_VALUES_EQUAL(load.GetDelayBeforeMeasurementsSeconds(), 5);
        UNIT_ASSERT_VALUES_EQUAL(load.GetInFlight(), 7);
        UNIT_ASSERT_VALUES_EQUAL(load.GetAreas(0).GetAreaSize(), 10485760);
        UNIT_ASSERT_VALUES_EQUAL(ResolveInFlight(parsed.Result).From, "8");
        for (const auto& [name, value] : TVector<std::pair<const char*, const char*>>{
                {"--areas", "32"}, {"--duration", "10"}, {"--io-size", "4096"},
                {"--background-write-ratio", "0"}, {"--background-write-size-kib", "4"}}) {
            TParsed conflict({"tool ddisk", "--cfg", file.Name().c_str(), name, value});
            UNIT_ASSERT_EXCEPTION(LoadTests(conflict.Result, true), yexception);
        }
        for (const char* flag : {"--read-only", "--sequential"}) {
            TParsed conflict({"tool ddisk", "--cfg", file.Name().c_str(), flag});
            UNIT_ASSERT_EXCEPTION(LoadTests(conflict.Result, true), yexception);
        }
        auto mixed = tests;
        mixed.AddInterconnectTestList();
        UNIT_ASSERT_EXCEPTION(ValidateDDiskTests(mixed), yexception);
        mixed = tests;
        mixed.MutableDDiskTestList(0)->MutableDDiskTestList(0)->MutablePDiskReadLoad();
        UNIT_ASSERT_EXCEPTION(ValidateDDiskTests(mixed), yexception);
        UNIT_ASSERT_EXCEPTION(ValidateDDiskTests({}), yexception);
    }

    Y_UNIT_TEST(ConfigFreeNetworkOptionsAndLegacyFlags) {
        TParsed server({"tool ddisk", "--path", "one", "--path", "two", "--server", "1",
            "--client", "2", "--ic-port", "19001", "--disable-ddisk-checksums",
            "--disable-pdisk-encryption", "--force-ddisk-pdisk-fallback"});
        UNIT_ASSERT_VALUES_EQUAL(server.Cli.Paths, (TVector<TString>{"one", "two"}));
        UNIT_ASSERT_VALUES_EQUAL(server.Cli.ServerNodeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(server.Cli.ClientNodeId, 2);
        UNIT_ASSERT_VALUES_EQUAL(server.Cli.IcPort, 19001);
        UNIT_ASSERT(server.Cli.DisableDDiskChecksums);
        UNIT_ASSERT(server.Cli.DisablePDiskDataEncryption);
        UNIT_ASSERT(server.Cli.ForcePDiskFallback);
        UNIT_ASSERT_VALUES_EQUAL(LoadTests(server.Result, true).DDiskTestListSize(), 1);
        TParsed client({"tool ddisk", "--client", "3", "--endpoint", "one:19001",
            "--endpoint", "two:19002", "--num-server-devices", "2"});
        UNIT_ASSERT(client.Cli.Paths.empty());
        UNIT_ASSERT_VALUES_EQUAL(client.Cli.ClientEndpoints.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(LoadTests(client.Result, true).DDiskTestListSize(), 1);
    }

    Y_UNIT_TEST(SectorMapOptionsAcceptDefaultsAndPDiskFlags) {
        TParsed defaults({"tool"}, false);
        UNIT_ASSERT(!defaults.Result.Has("num-server-devices"));
        UNIT_ASSERT(defaults.Result.Has("num-server-devices", true));
        ValidatePDiskSectorMapOptions(defaults.Result, false);

        TParsed parsed({"tool", "--path", "SectorMap:one:64", "--path", "SectorMap:two:64",
            "--cfg", "pdisk.txt", "--inflight", "4", "--run-count", "2", "--output-format", "human",
            "--mon-port", "8080", "--disable-pdisk-encryption", "--log-level", "debug"}, false);
        ValidatePDiskSectorMapOptions(parsed.Result, false);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Cli.Paths, (TVector<TString>{"SectorMap:one:64", "SectorMap:two:64"}));
    }

    Y_UNIT_TEST(SectorMapOptionsRejectDDiskAndNetworkFlags) {
        TParsed ddisk({"tool ddisk"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(ValidatePDiskSectorMapOptions(ddisk.Result, true), yexception,
            "the ddisk command is not supported");
        for (const char* flag : {"--disable-ddisk-checksums", "--force-ddisk-pdisk-fallback", "--ddisk-devnull"}) {
            TParsed parsed({"tool", flag}, false);
            UNIT_ASSERT_EXCEPTION_CONTAINS(ValidatePDiskSectorMapOptions(parsed.Result, false), yexception, flag);
        }
        for (const auto& [name, value] : TVector<std::pair<const char*, const char*>>{
                {"--ddisk-checksums-cache-size", "0"}, {"--server", "0"}, {"--client", "0"},
                {"--endpoint", "localhost:1"}, {"--ic-port", "0"}, {"--num-server-devices", "1"}}) {
            TParsed parsed({"tool", name, value}, false);
            UNIT_ASSERT_EXCEPTION_CONTAINS(ValidatePDiskSectorMapOptions(parsed.Result, false), yexception, name);
        }
    }

    Y_UNIT_TEST(SectorMapConfigsAcceptOnlyPDiskLoads) {
        NDevicePerfTest::TPerfTests tests;
        auto* test = tests.AddPDiskTestList();
        test->AddPDiskTestList()->MutablePDiskReadLoad();
        test->AddPDiskTestList()->MutablePDiskWriteLoad();
        test->AddPDiskTestList()->MutablePDiskLogLoad();
        tests.AddPDiskTestList()->AddPDiskTestList()->MutablePDiskReadLoad();
        ValidatePDiskSectorMapTests(tests);

        for (const char* unsupported : {
                "AioTestList {}", "TrimTestList {}", "DriveEstimatorTest {}", "DDiskTestList {}",
                "UringRouterTestList {}", "PersistentBufferTestList {}", "InterconnectTestList {}"}) {
            NDevicePerfTest::TPerfTests mixed;
            UNIT_ASSERT(NProtoBuf::TextFormat::ParseFromString(unsupported, &mixed));
            mixed.MergeFrom(tests);
            UNIT_ASSERT_EXCEPTION_CONTAINS(ValidatePDiskSectorMapTests(mixed), yexception,
                "only PDisk sector-map workloads");
        }
    }

    Y_UNIT_TEST(SectorMapConfigsRejectEmptyAndUnrelatedCommands) {
        for (const char* config : {
                "", "PDiskTestList {}", "PDiskTestList { PDiskTestList {} }",
                "PDiskTestList { PDiskTestList { Stop {} } }",
                "PDiskTestList { PDiskTestList { DDiskLoad {} } }",
                "PDiskTestList { PDiskTestList { PDiskReadLoad {} } PDiskTestList { StorageLoad {} } }",
                "PDiskTestList { PDiskTestList { PDiskReadLoad {} } } PDiskTestList {}"}) {
            NDevicePerfTest::TPerfTests tests;
            UNIT_ASSERT(NProtoBuf::TextFormat::ParseFromString(config, &tests));
            UNIT_ASSERT_EXCEPTION(ValidatePDiskSectorMapTests(tests), yexception);
        }
    }

#ifdef _darwin_
    Y_UNIT_TEST(SectorMapConfigFileErrors) {
        TTempFileHandle file;
        {
            TFileOutput out(file.Name());
            out << "PDiskTestList { unknown_field: 1 }";
        }
        TParsed malformed({"tool", "--cfg", file.Name().c_str()}, false);
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadTests(malformed.Result, false), yexception, "cannot parse PDisk config");
        const TString missingName = file.Name() + ".missing";
        TParsed missing({"tool", "--cfg", missingName.c_str()}, false);
        UNIT_ASSERT_EXCEPTION(LoadTests(missing.Result, false), yexception);
    }
#endif

    Y_UNIT_TEST(HelpSectionsAndUnsupportedOptions) {
        for (bool ddisk : {false, true}) {
            TCommandLine cli(ddisk);
            TStringStream help;
            cli.Opts.PrintUsage("tool", help);
            const TString text = help.Str();
#ifdef _darwin_
            UNIT_ASSERT(text.Contains("--inflight"));
            UNIT_ASSERT(text.Contains("SectorMap:<name>"));
            UNIT_ASSERT(text.Contains("PDisk read, write, and log"));
            for (const char* unsupported : {"DDisk", "ddisk", "UringRouter", "--areas", "--server", "--client",
                    "--endpoint", "--ic-port", "--num-server-devices"}) {
                UNIT_ASSERT_C(!text.Contains(unsupported), unsupported);
            }
#else
            const auto common = text.find("--inflight");
            const auto section = text.find("DDisk options");
            const auto checksum = text.find("--disable-ddisk-checksums");
            UNIT_ASSERT(common != TString::npos && common < section && section < checksum);
            UNIT_ASSERT_VALUES_EQUAL(text.Contains("--areas"), ddisk);
            UNIT_ASSERT_VALUES_EQUAL(text.Contains("ddisk <options>"), !ddisk);
            UNIT_ASSERT(text.Contains("--ddisk-devnull"));
#endif
            UNIT_ASSERT(!text.Contains("DelayBeforeMeasurementsSeconds"));
        }
        UNIT_ASSERT_EXCEPTION((TParsed({"tool ddisk", "--delay-before-measurements", "1"})), yexception);
        UNIT_ASSERT_EXCEPTION((TParsed({"tool ddisk", "unexpected"})), yexception);
    }
}
