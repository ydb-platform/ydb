#pragma once

#include <library/cpp/getopt/last_getopt.h>
#include <ydb/core/protos/blobstorage_ddisk.pb.h>
#include <ydb/tools/stress_tool/proto/device_perf_test.pb.h>

namespace NKikimr::NStressTool {

inline constexpr ui32 DDiskChunkSize = 128 << 20;

// Owns storage referenced by getopt handlers; it must outlive the parse result.
struct TCommandLine {
    const bool DDisk;
    NLastGetopt::TOpts Opts;
    TVector<TString> Paths;
    bool DisablePDiskDataEncryption = false;
    bool DisableDDiskChecksums = false;
    bool ForcePDiskFallback = false;
    ui32 ServerNodeId = 0;
    ui32 ClientNodeId = 0;
    TVector<TString> ClientEndpoints;
    ui16 IcPort = 0;

    explicit TCommandLine(bool ddisk);
    TCommandLine(const TCommandLine&) = delete;
    TCommandLine& operator=(const TCommandLine&) = delete;
};

struct TInFlightOptions {
    TString From;
    TString To;
};

TInFlightOptions ResolveInFlight(const NLastGetopt::TOptsParseResult& res, bool ddisk);
TVector<ui32> InFlightValues(ui32 from, ui32 to);
NDevicePerfTest::TPerfTests MakeDDiskTests(const NLastGetopt::TOptsParseResult& res);
void ValidateDDiskTests(const NDevicePerfTest::TPerfTests& tests);
NDevicePerfTest::TPerfTests LoadTests(const NLastGetopt::TOptsParseResult& res, bool ddisk);

} // namespace NKikimr::NStressTool
