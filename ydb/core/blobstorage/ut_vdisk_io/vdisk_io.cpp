#include "env.h"

using namespace NKikimr;

Y_UNIT_TEST_SUITE(VDiskIOTest) {

    Y_UNIT_TEST(HugeBlobIOCount) {
        SetRandomSeed(FromString<int>(GetEnv("SEED", "1")));
        std::optional<TTestEnv> env(std::in_place);

        char value = 1;
        TString blob = TString::Uninitialized(500_KB);
        memset(blob.Detach(), value, blob.size());

        std::vector<ui64> tabletIds;
        for (ui32 i = 0; i < 100; ++i) {
            tabletIds.push_back(i + 1);
        }

        struct TTabletContext {
            ui32 Gen = 1, Step = 1;
        };
        std::unordered_map<ui64, TTabletContext> tablets;

        ui8 channel = 0;

        ui64 totalWrites = 0;
        ui64 totalLogWrites = 0;
        ui64 totalChunkWrites = 0;

        ui64 totalPuts = 0;

        auto pdiskStats = [&env]() {
            TAppData* appData = (TAppData*) env->GetRuntime()->AppData(1);
            auto pDisks = appData->Counters->GetSubgroup("counters", "pdisks");
            auto rot = pDisks->GetSubgroup("pdisk", "000000001")->GetSubgroup("media", "rot");
            auto device = rot->GetSubgroup("subsystem", "device");
            auto pDisk = rot->GetSubgroup("subsystem", "pdisk");

            ui64 deviceWrites = device->FindCounter("DeviceWrites")->GetAtomic();
            ui64 writesLog = pDisk->GetSubgroup("req", "WriteLog")->FindCounter("Requests")->GetAtomic();
            ui64 writesHugeUser = pDisk->GetSubgroup("req", "WriteHugeUser")->FindCounter("Requests")->GetAtomic();

            return std::make_tuple(deviceWrites, writesLog, writesHugeUser);
        };

        ui64 totalDeviceWrites = 0;
        ui64 totalPDiskReqWritesLog = 0;
        ui64 totalPDiskReqWritesHugeUser = 0;

        const ui16 putCount = 5000;

        for (ui16 i = 0; i < putCount; ++i) {
            const ui64 tabletId = tabletIds[RandomNumber(tabletIds.size())];
            TTabletContext& tablet = tablets[tabletId];

            TLogoBlobID id(tabletId, tablet.Gen, tablet.Step++, channel, blob.size(), 0, 1);

            ui64 writeCount = 0;
            ui64 logWrite = 0;
            ui64 chunkWrite = 0;

            TTestActorRuntimeBase* r = env->GetRuntime();
            auto holder = r->AddObserver([&](auto&& ev) {
                if (ev->Recipient == env->GetPDiskServiceId()) {
                    if (ev->Type == NPDisk::TEvLog::EventType) {
                        ++logWrite;
                    } else if (ev->Type == NPDisk::TEvChunkWrite::EventType) {
                        ++chunkWrite;
                    }
                }
            });

            auto sPre = pdiskStats();

            auto res = env->Put(id, blob);

            auto sAfter = pdiskStats();

            ui64 deviceWrites = std::get<0>(sAfter) - std::get<0>(sPre);
            ui64 reqWritesLog = std::get<1>(sAfter) - std::get<1>(sPre);
            ui64 reqWritesHugeUser = std::get<2>(sAfter) - std::get<2>(sPre);

            totalDeviceWrites += deviceWrites;
            totalPDiskReqWritesLog += reqWritesLog;
            totalPDiskReqWritesHugeUser += reqWritesHugeUser;

            writeCount = logWrite + chunkWrite;

            totalLogWrites += logWrite;
            totalChunkWrites += chunkWrite;
            totalWrites += writeCount;

            totalPuts++;

            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
        }

        Cerr << "Total puts# " << totalPuts << Endl;

        if (totalWrites < 10'000 || totalWrites > 12'000) {
            UNIT_ASSERT_C(false, "Total vdisk writes " << totalWrites);
        }

        if (totalLogWrites < 5'000 || totalLogWrites > 5'200) {
            UNIT_ASSERT_C(false, "VDisk log writes " << totalLogWrites);
        }

        if (totalChunkWrites < 5'000 || totalChunkWrites > 5'200) {
            UNIT_ASSERT_C(false, "VDisk chunk writes " << totalChunkWrites);
        }

        double logWritesPerPut = (double)totalLogWrites / totalPuts;
        double chunkWritesPerPut = (double)totalChunkWrites / totalPuts;
        double writesPerPut = (double)totalWrites / totalPuts;

        UNIT_ASSERT_VALUES_EQUAL(1, (ui32)logWritesPerPut);
        UNIT_ASSERT_VALUES_EQUAL(1, (ui32)chunkWritesPerPut);
        UNIT_ASSERT_VALUES_EQUAL(2, (ui32)writesPerPut);

        if (totalDeviceWrites < 10'000 || totalDeviceWrites > 12'000) {
            UNIT_ASSERT_C(false, "DeviceWrites " << totalDeviceWrites);
        }

        if (totalPDiskReqWritesLog < 5'000 || totalPDiskReqWritesLog > 5'200) {
            UNIT_ASSERT_C(false, "WriteLog requests " << totalPDiskReqWritesLog);
        }

        if (totalPDiskReqWritesHugeUser < 5'000 || totalPDiskReqWritesHugeUser > 5'200) {
            UNIT_ASSERT_C(false, "WriteHugeUser requests " << totalPDiskReqWritesHugeUser);
        }

        Cerr << "Total vdisk writes# " << totalWrites << Endl;
        Cerr << "Total log vdisk writes# " << totalLogWrites << Endl;
        Cerr << "Total chunk vdisk writes# " << totalChunkWrites << Endl;
        Cerr << "Log vdisk writes per put# " << logWritesPerPut << Endl;
        Cerr << "Chunk vdisk writes per put# " << chunkWritesPerPut << Endl;
        Cerr << "VDisk writes per put# " << writesPerPut << Endl;

        Cerr << "DeviceWrites " << totalDeviceWrites << Endl;
        Cerr << "WriteLog requests " << totalPDiskReqWritesLog << Endl;
        Cerr << "WriteHugeUser requests " << totalPDiskReqWritesHugeUser << Endl;
    }

}
