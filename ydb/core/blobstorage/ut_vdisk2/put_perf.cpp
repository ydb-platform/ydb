#include <ydb/core/blobstorage/ut_vdisk2/env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>
#include <util/stream/output.h>

#include <chrono>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TVDiskPutPerfTest) {

    Y_UNIT_TEST(PutOneMillionAndReportSpeed) {
        constexpr ui32 NumPuts = 100'000;
        constexpr ui32 ValueSize = 1024;
        constexpr double BytesInMB = 1024.0 * 1024.0;

        std::optional<TTestEnv> env(std::in_place);
        const TString value(ValueSize, 'x');

        constexpr ui64 TabletId = 1;
        constexpr ui32 Generation = 1;
        constexpr ui8 Channel = 0;

        const auto start = std::chrono::steady_clock::now();

        for (ui32 step = 1; step <= NumPuts; ++step) {
            const TLogoBlobID id(TabletId, Generation, step, Channel, ValueSize, 0, 1);
            auto res = env->Put(id, value, NKikimrBlobStorage::EPutHandleClass::TabletLog); // Sends TEvVPut.
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), NKikimrProto::OK, "step# " << step);
        }

        const auto finish = std::chrono::steady_clock::now();
        const std::chrono::duration<double> elapsed = finish - start;
        const double elapsedSec = elapsed.count();

        UNIT_ASSERT_C(elapsedSec > 0.0, "non-positive elapsed time");

        const double opsPerSec = static_cast<double>(NumPuts) / elapsedSec;
        const double mbPerSec = static_cast<double>(NumPuts) * static_cast<double>(ValueSize) / BytesInMB / elapsedSec;

        Cerr << "VDisk TEvVPut throughput"
            << " keys# " << NumPuts
            << " value_bytes# " << ValueSize
            << " elapsed_sec# " << Sprintf("%.3f", elapsedSec)
            << " ops_per_sec# " << Sprintf("%.2f", opsPerSec)
            << " mb_per_sec# " << Sprintf("%.2f", mbPerSec)
            << Endl;
    }
}

} // namespace NKikimr
