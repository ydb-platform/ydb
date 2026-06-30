#include <ydb/core/blobstorage/rocksdisk/rocksdisk.h>
#include <ydb/core/blobstorage/rocksdisk/rocksdisk_actor.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>
#include <util/folder/tempdir.h>
#include <util/string/builder.h>
#include <util/stream/output.h>

#include <chrono>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TRocksDiskTest) {
    Y_UNIT_TEST(PutAndGetOneKey) {
        TTempDir tempDir;
        const TString dbPath = TStringBuilder() << tempDir() << "/rocksdb_test";

        TRocksDisk disk;
        TString error;

        UNIT_ASSERT_C(disk.Open(dbPath, &error), error);

        UNIT_ASSERT_C(disk.PutAsync("key1", "value1", &error), error);

        TString value;
        UNIT_ASSERT_C(disk.Get("key1", &value, &error), error);
        UNIT_ASSERT_VALUES_EQUAL(value, "value1");
    }

    Y_UNIT_TEST(PutSyncAndReadAfterReopen) {
        TTempDir tempDir;
        const TString dbPath = TStringBuilder() << tempDir() << "/rocksdb_sync_test";
        TString error;

        {
            TRocksDisk disk;
            UNIT_ASSERT_C(disk.Open(dbPath, &error), error);
            UNIT_ASSERT_C(disk.PutSync("sync_key", "sync_value", &error), error);
        }

        {
            TRocksDisk disk;
            UNIT_ASSERT_C(disk.Open(dbPath, &error), error);

            TString value;
            UNIT_ASSERT_C(disk.Get("sync_key", &value, &error), error);
            UNIT_ASSERT_VALUES_EQUAL(value, "sync_value");
        }
    }

    Y_UNIT_TEST(PutOneMillionAndReportSpeed) {
        constexpr ui64 NumKeys = 10'000;
        constexpr ui64 ValueSize = 1024;
        constexpr double BytesInMB = 1024.0 * 1024.0;

        TTempDir tempDir;
        const TString dbPath = TStringBuilder() << tempDir() << "/rocksdb_perf_test";

        TRocksDisk disk;
        TString error;

        UNIT_ASSERT_C(disk.Open(dbPath, &error), error);

        const TString value(ValueSize, 'x');
        const auto start = std::chrono::steady_clock::now();

        for (ui64 i = 0; i < NumKeys; ++i) {
            const TString key = TStringBuilder() << "key" << i;
            UNIT_ASSERT_C(disk.PutAsync(key, value, &error), TStringBuilder()
                << "index# " << i << " error# " << error);
        }

        const auto finish = std::chrono::steady_clock::now();
        const std::chrono::duration<double> elapsed = finish - start;
        const double elapsedSec = elapsed.count();

        UNIT_ASSERT_C(elapsedSec > 0.0, "non-positive elapsed time");

        const double opsPerSec = static_cast<double>(NumKeys) / elapsedSec;
        const double mbPerSec = (static_cast<double>(NumKeys) * static_cast<double>(ValueSize)) / BytesInMB / elapsedSec;

        Cerr << "TRocksDisk throughput"
            << " keys# " << NumKeys
            << " value_bytes# " << ValueSize
            << " elapsed_sec# " << Sprintf("%.3f", elapsedSec)
            << " ops_per_sec# " << Sprintf("%.2f", opsPerSec)
            << " mb_per_sec# " << Sprintf("%.2f", mbPerSec)
            << Endl;
    }

    Y_UNIT_TEST(PutOneMillionAndReportSpeedActor) {
        constexpr ui64 NumKeys = 10'000;
        constexpr ui64 ValueSize = 1024;
        constexpr double BytesInMB = 1024.0 * 1024.0;
        constexpr ui32 NodeId = 1;

        TTempDir tempDir;
        const TString dbPath = TStringBuilder() << tempDir() << "/rocksdb_actor_perf_test";

        const TVDiskID vdiskId(0, 1, 0, 0, 0);
        TTestActorSystem runtime(1);
        runtime.Start();

        const TActorId actorId = runtime.Register(CreateRocksDiskActor(dbPath, vdiskId), NodeId);
        const TActorId edge = runtime.AllocateEdgeActor(NodeId);

        constexpr ui64 TabletId = 1;
        constexpr ui32 Generation = 1;
        constexpr ui8 Channel = 0;

        const TString value(ValueSize, 'x');
        const auto start = std::chrono::steady_clock::now();

        for (ui64 step = 1; step <= NumKeys; ++step) {
            const TLogoBlobID id(TabletId, Generation, step, Channel, ValueSize, 0, 1);
            auto request = std::make_unique<TEvBlobStorage::TEvVPut>(
                id,
                TRope(value),
                vdiskId,
                false,
                nullptr,
                TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog,
                false);
            runtime.Send(new IEventHandle(actorId, edge, request.release()), NodeId);

            auto response = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrProto::OK, "step# " << step);
        }

        const auto finish = std::chrono::steady_clock::now();
        runtime.Stop();

        const std::chrono::duration<double> elapsed = finish - start;
        const double elapsedSec = elapsed.count();

        UNIT_ASSERT_C(elapsedSec > 0.0, "non-positive elapsed time");

        const double opsPerSec = static_cast<double>(NumKeys) / elapsedSec;
        const double mbPerSec = (static_cast<double>(NumKeys) * static_cast<double>(ValueSize)) / BytesInMB / elapsedSec;

        Cerr << "TRocksDiskActor throughput"
            << " keys# " << NumKeys
            << " value_bytes# " << ValueSize
            << " elapsed_sec# " << Sprintf("%.3f", elapsedSec)
            << " ops_per_sec# " << Sprintf("%.2f", opsPerSec)
            << " mb_per_sec# " << Sprintf("%.2f", mbPerSec)
            << Endl;
    }
}

} // namespace NKikimr
