#include "blobstorage_pdisk_ut.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_ut_env.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/pdisk_io/aio.h>

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
        const auto& performanceParams = NPDisk::TDevicePerformanceParams::Get(deviceType);
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

        if (operationType == EOperationType::OperationRead) {
            sectorMap.Write((ui8*)data.data(), dataSize, sectorPos * NPDisk::NSectorMap::SECTOR_SIZE);
        }
        double timeElapsed = 0;
        THPTimer timer;
        if (operationType == EOperationType::OperationRead) {
            sectorMap.Read((ui8*)data.data(), dataSize, sectorPos * NPDisk::NSectorMap::SECTOR_SIZE);
        } else {
            sectorMap.Write((ui8*)data.data(), dataSize, sectorPos * NPDisk::NSectorMap::SECTOR_SIZE);
        }
        timeElapsed = timer.Passed();

        double relativeDeviation = (timeElapsed - timeExpected) / timeExpected;
        if (time) {
            *time = { timeExpected, timeElapsed };
        }

        bool ok = relativeDeviation >= -deviationRange.first && relativeDeviation <= deviationRange.second;
        return NSan::PlainOrUnderSanitizer(ok, true);
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

    MAKE_TEST(SSD, 1960, 100, Read, First);
    MAKE_TEST(SSD, 1960, 100, Write, First);
    MAKE_TEST(SSD, 1960, 1000, Read, First);
    MAKE_TEST(SSD, 1960, 1000, Write, First);

#undef MAKE_TEST

    Y_UNIT_TEST(TestAsyncReadQueueDepthAndJitter) {
        constexpr ui32 QueueDepth = 8;
        constexpr ui32 OperationCount = 100;
        constexpr ui64 SeekSleepUs = 10'000;
        constexpr ui64 SeekSleepJitterUs = 10'000;
        constexpr ui64 SectorSize = NPDisk::NSectorMap::SECTOR_SIZE;

        struct TNoopCallback : NPDisk::ICallback {
            void Exec(NPDisk::TAsyncIoOperationResult*) override {
            }
        };

        struct TOperationState {
            TInstant SubmittedAt;
            TDuration Latency;
        };

        auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(SectorSize * 16, EDiskMode::DM_HDD);
        auto diskModeParams = sectorMap->GetDiskModeParams();
        UNIT_ASSERT(diskModeParams);
        diskModeParams->SeekSleepMicroSeconds.store(SeekSleepUs);
        diskModeParams->SeekSleepJitterMicroSeconds.store(SeekSleepJitterUs);
        diskModeParams->FirstSectorReadRate.store(1ull << 40);
        diskModeParams->LastSectorReadRate.store(1ull << 40);

        auto io = NPDisk::CreateAsyncIoContextMap("SectorMapAsyncReadQueueDepthAndJitter", 1, sectorMap);
        UNIT_ASSERT(io->Setup(QueueDepth, true) == NPDisk::EIoResult::Ok);

        TNoopCallback callback;
        TVector<TOperationState> states(OperationCount);
        TVector<TString> buffers;
        TVector<NPDisk::IAsyncIoOperation*> operations;
        buffers.reserve(OperationCount);
        operations.reserve(OperationCount);

        for (ui32 idx = 0; idx < OperationCount; ++idx) {
            buffers.emplace_back(TString::Uninitialized(SectorSize));
            auto *op = io->CreateAsyncIoOperation(&states[idx], NPDisk::TReqId(NPDisk::TReqId::Test0, idx), nullptr);
            io->PreparePRead(op, buffers.back().Detach(), SectorSize, 0);
            operations.push_back(op);
        }

        auto submit = [&](ui32 idx) {
            const TInstant submittedAt = TInstant::Now();
            const auto result = io->Submit(operations[idx], &callback);
            if (result == NPDisk::EIoResult::Ok) {
                states[idx].SubmittedAt = submittedAt;
            }
            return result;
        };

        ui32 nextToSubmit = 0;
        ui32 completed = 0;
        ui32 inFlight = 0;
        ui32 maxInFlight = 0;
        TVector<TDuration> latencies;
        latencies.reserve(OperationCount);

        const TInstant startedAt = TInstant::Now();
        for (; nextToSubmit < QueueDepth; ++nextToSubmit) {
            UNIT_ASSERT(submit(nextToSubmit) == NPDisk::EIoResult::Ok);
            maxInFlight = Max(maxInFlight, ++inFlight);
        }
        UNIT_ASSERT(submit(nextToSubmit) == NPDisk::EIoResult::TryAgain);

        while (completed < OperationCount) {
            NPDisk::TAsyncIoOperationResult events[QueueDepth];
            const i64 eventCount = io->GetEvents(1, QueueDepth, events, TDuration::Seconds(5));
            UNIT_ASSERT_C(eventCount > 0, "No SectorMap read completions received");

            for (i64 idx = 0; idx < eventCount; ++idx) {
                UNIT_ASSERT(events[idx].Result == NPDisk::EIoResult::Ok);
                auto *state = static_cast<TOperationState*>(events[idx].Operation->GetCookie());
                state->Latency = TInstant::Now() - state->SubmittedAt;
                latencies.push_back(state->Latency);
                io->DestroyAsyncIoOperation(events[idx].Operation);
                --inFlight;
                ++completed;
            }

            while (nextToSubmit < OperationCount && inFlight < QueueDepth) {
                const auto result = submit(nextToSubmit);
                if (result == NPDisk::EIoResult::TryAgain) {
                    break;
                }
                UNIT_ASSERT(result == NPDisk::EIoResult::Ok);
                ++nextToSubmit;
                maxInFlight = Max(maxInFlight, ++inFlight);
            }
        }

        const TDuration elapsed = TInstant::Now() - startedAt;
        UNIT_ASSERT(io->Destroy() == NPDisk::EIoResult::Ok);

        TDuration minLatency = TDuration::Max();
        TDuration maxLatency = TDuration::Zero();
        ui64 firstLatencyMs = 0;
        bool hasFirstLatencyMs = false;
        bool hasDifferentLatencyMs = false;
        for (const TDuration latency : latencies) {
            minLatency = Min(minLatency, latency);
            maxLatency = Max(maxLatency, latency);
            const ui64 latencyMs = latency.MilliSeconds();
            if (!hasFirstLatencyMs) {
                firstLatencyMs = latencyMs;
                hasFirstLatencyMs = true;
            } else if (firstLatencyMs != latencyMs) {
                hasDifferentLatencyMs = true;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(latencies.size(), OperationCount);
        UNIT_ASSERT_VALUES_EQUAL(maxInFlight, QueueDepth);
        UNIT_ASSERT_C(minLatency >= TDuration::MilliSeconds(8), "Expected base SectorMap read delay"
                << " minLatency# " << minLatency << " maxLatency# " << maxLatency);
        UNIT_ASSERT_C(hasDifferentLatencyMs, "Expected jitter to produce different read latencies"
                << " minLatency# " << minLatency << " maxLatency# " << maxLatency);
        UNIT_ASSERT_C(NSan::PlainOrUnderSanitizer(elapsed < TDuration::MilliSeconds(900), true),
                "Expected 100 reads with QD# " << QueueDepth << " to finish faster than serial execution"
                << " elapsed# " << elapsed << " minLatency# " << minLatency << " maxLatency# " << maxLatency);
    }
}
}
