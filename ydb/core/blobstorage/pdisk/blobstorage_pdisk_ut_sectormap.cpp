#include "blobstorage_pdisk_ut.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_ut_env.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/system/hp_timer.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TSectorMapPerformance) {

    enum class ESectorPosition : ui8 {
        SectorFirst = 0,
        SectorLast,
    };

    enum class EOperationType : ui8 {
        OperationRead = 0,
        OperationWrite,
    };

    using EDiskMode = NPDisk::NSectorMap::EDiskMode;

    bool TestSectorMapPerformance(EDiskMode diskMode, ui64 diskSizeGb, ui64 dataSizeMb, ESectorPosition sectorPosition,
            EOperationType operationType, std::pair<double, double> deviationRange = {0.05, 0.5},
            std::pair<double, double>* time = nullptr) {
        static TString data = PrepareData(1024 * 1024 * 1024);
        ui64 dataSize = dataSizeMb * 1024 * 1024;
        ui64 deviceSize = diskSizeGb * 1024 * 1024 * 1024;

        auto deviceType = NPDisk::NSectorMap::DiskModeToDeviceType(diskMode);
        ui64 diskRate;
        const auto& performanceParams = NPDisk::DevicePerformance.at(deviceType);
        if (operationType == EOperationType::OperationRead) {
            diskRate = (sectorPosition == ESectorPosition::SectorFirst)
                    ? performanceParams.FirstSectorReadBytesPerSec
                    : performanceParams.LastSectorReadBytesPerSec;
        } else {
            diskRate = (sectorPosition == ESectorPosition::SectorFirst)
                    ? performanceParams.FirstSectorWriteBytesPerSec
                    : performanceParams.LastSectorWriteBytesPerSec;
        }

        ui64 sectorsNum = deviceSize / NPDisk::NSectorMap::SECTOR_SIZE;
        ui64 sectorPos = (sectorPosition == ESectorPosition::SectorFirst)
                ? 0
                : sectorsNum - dataSize / NPDisk::NSectorMap::SECTOR_SIZE - 2;

        double timeExpected = (double)dataSize / diskRate + 1e-9 * performanceParams.SeekTimeNs;

        NPDisk::TSectorMap sectorMap(deviceSize, diskMode);
        sectorMap.ZeroInit(2);

        double timeElapsed = 0;
        if (operationType == EOperationType::OperationRead) {
            sectorMap.Write((ui8*)data.data(), dataSize, sectorPos * NPDisk::NSectorMap::SECTOR_SIZE);
            THPTimer timer;
            sectorMap.Read((ui8*)data.data(), dataSize, sectorPos * NPDisk::NSectorMap::SECTOR_SIZE);
            timeElapsed = timer.Passed();
        } else {
            THPTimer timer;
            sectorMap.Write((ui8*)data.data(), dataSize, sectorPos * NPDisk::NSectorMap::SECTOR_SIZE);
            timeElapsed = timer.Passed();
        }

        double relativeDeviation = (timeElapsed - timeExpected) / timeExpected;
        if (time) {
            *time = { timeExpected, timeElapsed };
        }

        return relativeDeviation >= -deviationRange.first && relativeDeviation <= deviationRange.second;
    }


#define MAKE_TEST(diskMode, diskSizeGb, dataSizeMb, operationType, position)                                    \
    Y_UNIT_TEST(Test##diskMode##diskSizeGb##GB##operationType##dataSizeMb##MB##On##position##Sector) {          \
        std::pair<double, double> time;                                                                         \
        UNIT_ASSERT_C(TestSectorMapPerformance(EDiskMode::DM_##diskMode, diskSizeGb,  dataSizeMb,               \
                ESectorPosition::Sector##position, EOperationType::Operation##operationType, { 0.05, 2.0 },     \
                &time), "Time expected# " << time.first << " time elapsed#" << time.second);                    \
    }

    MAKE_TEST(HDD, 1960, 100, Read, First);
    MAKE_TEST(HDD, 1960, 100, Read, Last);
    MAKE_TEST(HDD, 1960, 100, Write, First);
    MAKE_TEST(HDD, 1960, 100, Write, Last);
    MAKE_TEST(HDD, 1960, 1000, Read, First);
    MAKE_TEST(HDD, 1960, 1000, Read, Last);
    MAKE_TEST(HDD, 1960, 1000, Write, First);
    MAKE_TEST(HDD, 1960, 1000, Write, Last);

    MAKE_TEST(SSD, 1960, 100, Read, First);
    MAKE_TEST(SSD, 1960, 100, Write, First);
    MAKE_TEST(SSD, 1960, 1000, Read, First);
    MAKE_TEST(SSD, 1960, 1000, Write, First);

#undef MAKE_TEST
}
}
