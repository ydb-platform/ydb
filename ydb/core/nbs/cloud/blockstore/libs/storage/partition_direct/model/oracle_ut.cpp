#include "oracle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THostStateControllerMock: public IHostStateController
{
    TMap<THostIndex, EHostState> States;
    ui64 HostPBufferUsedSize = 1_MB;

    // Records every host index passed to QueryAddHost, in call order.
    TVector<THostIndex> AddHostQueries;

    THostStateControllerMock() = default;

    void SetHostState(
        THostIndex hostIndex,
        EHostState oldState,
        EHostState newState) override
    {
        Y_UNUSED(oldState);

        States[hostIndex] = newState;
    }

    ui64 GetHostPBufferUsedSize(THostIndex hostIndex) const override
    {
        Y_UNUSED(hostIndex);

        return HostPBufferUsedSize;
    }

    void QueryAddHost(THostIndex hostIndex) override
    {
        AddHostQueries.push_back(hostIndex);
    }

    void QueryRemoveHost(THostIndex hostIndex) override
    {
        Y_UNUSED(hostIndex);
    }
};

TStorageConfigPtr MakeStorageConfig()
{
    NProto::TStorageServiceConfig rawConfig;
    rawConfig.MutableOracleConfig()->SetTimePredictionHistorySize(10);
    rawConfig.MutableOracleConfig()->SetTimePredictionNthFromEnd(1);
    return std::make_shared<TStorageConfig>(rawConfig);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOracle)
{
    Y_UNIT_TEST(SelectBestPBufferHostShouldPickHostWithLowestInflight)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        const auto hosts = THostMask::MakeAll(5);

        TOracle oracle(storageConfig, nullptr);

        // Host 2 has the lowest inflight count (zero), all others are higher.
        const auto now = TInstant::Now();
        for (THostIndex hostIndex: {0, 1, 3, 4}) {
            oracle.OnRequestStarted(
                hostIndex,
                EOperation::WriteToManyPBuffers,
                now);
        }

        // Run multiple times to ensure deterministic selection (no ties at
        // the minimum).
        for (size_t iter = 0; iter < 100; ++iter) {
            const THostIndex selected = oracle.SelectBestPBufferHost(
                hosts,
                EOperation::WriteToManyPBuffers);

            UNIT_ASSERT_VALUES_EQUAL(2u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldHandleSingleHost)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        const auto hosts = THostMask::MakeOne(3);

        TOracle oracle(storageConfig, nullptr);

        // Host 2 has the lowest inflight count (zero), all others are higher.
        const auto now = TInstant::Now();
        for (THostIndex hostIndex: {0, 1, 3, 4}) {
            oracle.OnRequestStarted(
                hostIndex,
                EOperation::WriteToManyPBuffers,
                now);
        }

        // Run multiple times to ensure deterministic selection (no ties at
        // the minimum).
        for (size_t iter = 0; iter < 100; ++iter) {
            const THostIndex selected = oracle.SelectBestPBufferHost(
                hosts,
                EOperation::WriteToManyPBuffers);

            UNIT_ASSERT_VALUES_EQUAL(3u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldRespectSubsetOfHosts)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        // Even if some non-listed host has a lower inflight count, the
        // selection must be limited to the supplied hostIndexes.
        THostMask hosts;
        hosts.Set(0);
        hosts.Set(3);

        TOracle oracle(storageConfig, nullptr);

        const auto now = TInstant::Now();
        for (THostIndex hostIndex: {0, 0, 3}) {
            oracle.OnRequestStarted(
                hostIndex,
                EOperation::WriteToManyPBuffers,
                now);
        }

        // Run multiple times to ensure deterministic selection (no ties at
        // the minimum).
        for (size_t iter = 0; iter < 100; ++iter) {
            const THostIndex selected = oracle.SelectBestPBufferHost(
                hosts,
                EOperation::WriteToManyPBuffers);

            UNIT_ASSERT_VALUES_EQUAL(3u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldDistributeTiesAcrossAllCandidates)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        // All three hosts have the same (zero) inflight count. With reservoir
        // sampling, every tied host must have a roughly equal probability of
        // being selected. We verify this by sampling a large number of times
        // and checking that every candidate appears at least once.
        THostMask hosts;
        hosts.Set(1);
        hosts.Set(2);
        hosts.Set(4);

        TOracle oracle(storageConfig, nullptr);

        std::map<THostIndex, size_t> counts;
        const size_t iterations = 3000;
        for (size_t iter = 0; iter < iterations; ++iter) {
            const THostIndex selected = oracle.SelectBestPBufferHost(
                hosts,
                EOperation::WriteToManyPBuffers);
            ++counts[selected];
        }

        UNIT_ASSERT_VALUES_EQUAL(3u, counts.size());

        // Each tied host must be picked a meaningful share of the time.
        // Expected ~1/3 of iterations each, allow generous tolerance.
        const size_t expected = iterations / 3;
        const size_t tolerance = expected / 2;   // 50% tolerance
        for (auto hostIndex: hosts) {
            const size_t count = counts[hostIndex];
            UNIT_ASSERT_C(
                count + tolerance >= expected && count <= expected + tolerance,
                TStringBuilder()
                    << "host " << PrintHostIndex(hostIndex) << " was picked "
                    << count << " times, expected ~" << expected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldIgnoreTiesAboveMinimum)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        // Hosts with inflight equal to the (non-best) tie value must never be
        // picked - only ties at the global minimum are randomized.
        const auto hosts = THostMask::MakeAll(5);

        TOracle oracle(storageConfig, nullptr);

        // Host 2 has the lowest inflight count (zero), all others are higher.
        const auto now = TInstant::Now();
        for (THostIndex hostIndex: {0, 1, 3, 4}) {
            oracle.OnRequestStarted(hostIndex, EOperation::Flush, now);
        }

        for (size_t iter = 0; iter < 200; ++iter) {
            const THostIndex selected =
                oracle.SelectBestPBufferHost(hosts, EOperation::Flush);

            UNIT_ASSERT_VALUES_EQUAL(2u, selected);
        }
    }

    Y_UNIT_TEST(DisableHostOnErrorAndTime)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(1);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate error
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // One second later
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Two seconds later
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Three seconds later. Switching to the temporaryOffline state.
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::TemporaryOffline,
            hostStateController.States[0]);

        // Six seconds later. Switching to the Offline state.
        now += TDuration::Seconds(3);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::Offline,
            hostStateController.States[0]);

        // Generate success. Switching to the enabled state.
        now += TDuration::Seconds(1);
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestSucceeded(
            0,
            EOperation::WriteToPBuffer,
            now,
            TDuration());

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::Online,
            hostStateController.States[0]);
    }

    Y_UNIT_TEST(DoNotDisableHostOnFewErrorAndTime)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(5);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate error
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // One second later
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Two seconds later
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Three seconds later.
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());
    }

    Y_UNIT_TEST(DisableHostOnLotOfErrors)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(5);
        oracleConfig.SetErrorsCountForGoingOffline(500);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate a lot of errors. Switching to the temporaryOffline state.
        for (size_t i = 0; i < 500; ++i) {
            oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
            oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);
        }

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::TemporaryOffline,
            hostStateController.States[0]);

        // Generate success. Switching to the enabled state.
        now += TDuration::Seconds(1);
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestSucceeded(
            0,
            EOperation::WriteToPBuffer,
            now,
            TDuration());

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::Online,
            hostStateController.States[0]);
    }

    Y_UNIT_TEST(DisableHostOnTotalSizeOfErrors)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetErrorsTotalSizeForGoingOffline(1_MB);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate a error with large total size. Switching to the
        // temporaryOffline state.
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::TemporaryOffline,
            hostStateController.States[0]);

        // Generate success. Switching to the enabled state.
        now += TDuration::Seconds(1);
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestSucceeded(
            0,
            EOperation::WriteToPBuffer,
            now,
            TDuration());

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::Online,
            hostStateController.States[0]);
    }

    Y_UNIT_TEST(GetReadHedgingDelayReturnsDefaultWhenNoHistory)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        TOracle oracle(storageConfig, nullptr);

        // No read requests recorded -> predictor returns zero -> fallback to
        // default. Default ReadHedgingDelay is 1ms when not set in config.
        const auto defaultDelay = storageConfig->GetReadHedgingDelay();

        UNIT_ASSERT_VALUES_EQUAL(
            defaultDelay,
            oracle.GetReadHedgingDelay(0, EDataLocation::DDisk));
        UNIT_ASSERT_VALUES_EQUAL(
            defaultDelay,
            oracle.GetReadHedgingDelay(0, EDataLocation::PBuffer));
    }

    Y_UNIT_TEST(GetReadHedgingDelayDDiskAndPBufferAreIndependent)
    {
        auto storageConfig = MakeStorageConfig();

        TOracle oracle(storageConfig, nullptr);
        auto now = TInstant::Now();

        // Feed DDisk reads with 100ms, 200ms on host 0.
        for (auto duration: {100, 200}) {
            oracle.OnRequestStarted(0, EOperation::ReadFromDDisk, now);
            oracle.OnRequestSucceeded(
                0,
                EOperation::ReadFromDDisk,
                now,
                TDuration::MilliSeconds(duration));
        }

        // Feed PBuffer reads with 300ms, 400ms on host 0.
        for (auto duration: {300, 400}) {
            oracle.OnRequestStarted(0, EOperation::ReadFromPBuffer, now);
            oracle.OnRequestSucceeded(
                0,
                EOperation::ReadFromPBuffer,
                now,
                TDuration::MilliSeconds(duration));
        }

        // Feed PBuffer reads with 400ms, 500ms on host 1.
        for (auto duration: {400, 500}) {
            oracle.OnRequestStarted(1, EOperation::ReadFromPBuffer, now);
            oracle.OnRequestSucceeded(
                1,
                EOperation::ReadFromPBuffer,
                now,
                TDuration::MilliSeconds(duration));
        }

        // H0 DDisk predictor: [100, 200] -> predict 100.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            oracle.GetReadHedgingDelay(0, EDataLocation::DDisk));

        // H0: PBuffer predictor: [300, 400] -> predict 300.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(300),
            oracle.GetReadHedgingDelay(0, EDataLocation::PBuffer));

        // H1: PBuffer predictor: [400, 500] -> predict 400.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(400),
            oracle.GetReadHedgingDelay(1, EDataLocation::PBuffer));
    }

    Y_UNIT_TEST(GetWriteHedgingDelayReturnsDefaultWhenNoHistory)
    {
        auto storageConfig = MakeStorageConfig();

        TOracle oracle(storageConfig, nullptr);

        // No write requests recorded -> predictor returns zero -> fallback to
        // default. Default WriteHedgingDelay is 1ms when not set in config.
        const auto defaultDelay = storageConfig->GetWriteHedgingDelay();

        UNIT_ASSERT_VALUES_EQUAL(
            defaultDelay,
            oracle.GetWriteHedgingDelay(THostMask::MakeOne(0), false));
        UNIT_ASSERT_VALUES_EQUAL(
            defaultDelay,
            oracle.GetWriteHedgingDelay(THostMask::MakeOne(0), true));
    }

    Y_UNIT_TEST(GetWriteHedgingDelayDirectAndIndirectAreIndependent)
    {
        auto storageConfig = MakeStorageConfig();

        TOracle oracle(storageConfig, nullptr);
        auto now = TInstant::Now();

        // WriteToPBuffer (direct): [100, 200] -> predict 100ms.
        for (auto duration: {100, 200}) {
            oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
            oracle.OnRequestSucceeded(
                0,
                EOperation::WriteToPBuffer,
                now,
                TDuration::MilliSeconds(duration));
        }

        // WriteToManyPBuffers (indirect): [300, 400] -> predict 300ms.
        for (auto duration: {300, 400}) {
            oracle.OnRequestStarted(0, EOperation::WriteToManyPBuffers, now);
            oracle.OnRequestSucceeded(
                0,
                EOperation::WriteToManyPBuffers,
                now,
                TDuration::MilliSeconds(duration));
        }

        // The direct mode reads only the WriteToPBuffer predictor.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            oracle.GetWriteHedgingDelay(THostMask::MakeOne(0), false));
        // The indirect mode reads only the WriteToManyPBuffers predictor.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(300),
            oracle.GetWriteHedgingDelay(THostMask::MakeOne(0), true));
    }

    Y_UNIT_TEST(GetFlushRequestCooldownReturnsZeroWithoutErrors)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        TOracle oracle(storageConfig, nullptr);

        // No errors recorded on any host -> no cooldown.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            oracle.GetFlushRequestCooldown(THostMask::MakeAll(5)));
    }

    Y_UNIT_TEST(GetFlushRequestCooldownScalesWithConsecutiveErrors)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        TOracle oracle(storageConfig, nullptr);
        const auto now = TInstant::Now();

        // Each consecutive error adds a 10ms penalty on host 0.
        for (size_t errorCount = 1; errorCount <= 3; ++errorCount) {
            oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
            oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);

            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MilliSeconds(10 * errorCount),
                oracle.GetFlushRequestCooldown(THostMask::MakeOne(0)));
        }

        // A single success resets the consecutive error count back to zero.
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestSucceeded(
            0,
            EOperation::WriteToPBuffer,
            now,
            TDuration());

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            oracle.GetFlushRequestCooldown(THostMask::MakeOne(0)));
    }

    Y_UNIT_TEST(GetFlushRequestCooldownReturnsMaxAcrossHosts)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        TOracle oracle(storageConfig, nullptr);
        const auto now = TInstant::Now();

        // Host 1: two consecutive errors -> 20ms.
        for (size_t i = 0; i < 2; ++i) {
            oracle.OnRequestStarted(1, EOperation::WriteToPBuffer, now);
            oracle.OnRequestFailed(1, EOperation::WriteToPBuffer, now);
        }

        // Host 3: four consecutive errors -> 40ms.
        for (size_t i = 0; i < 4; ++i) {
            oracle.OnRequestStarted(3, EOperation::WriteToPBuffer, now);
            oracle.OnRequestFailed(3, EOperation::WriteToPBuffer, now);
        }

        // A mask covering both hosts must return the maximum cooldown.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(40),
            oracle.GetFlushRequestCooldown(
                THostMask::MakeOne(1).Include(THostMask::MakeOne(3))));

        // A mask restricted to the less-penalized host returns its own value.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(20),
            oracle.GetFlushRequestCooldown(THostMask::MakeOne(1)));

        // A mask covering only error-free hosts returns zero.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            oracle.GetFlushRequestCooldown(
                THostMask::MakeOne(0).Include(THostMask::MakeOne(4))));
    }

    Y_UNIT_TEST(GetFlushRequestCooldownIsClampedToMaxReconnectDelay)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        TOracle oracle(storageConfig, nullptr);
        const auto now = TInstant::Now();

        // The cooldown grows by 10ms per consecutive error and is capped at
        // MaxReconnectDelay (10s). 10s / 10ms == 1000 errors reach the cap
        // exactly; any additional error must not exceed it.
        const auto maxReconnectDelay = TDuration::Seconds(10);
        for (size_t i = 0; i < 1000; ++i) {
            oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
            oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);
        }

        // Exactly at the cap.
        UNIT_ASSERT_VALUES_EQUAL(
            maxReconnectDelay,
            oracle.GetFlushRequestCooldown(THostMask::MakeOne(0)));

        // Far beyond the cap - still clamped.
        for (size_t i = 0; i < 500; ++i) {
            oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
            oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            maxReconnectDelay,
            oracle.GetFlushRequestCooldown(THostMask::MakeOne(0)));
    }

    Y_UNIT_TEST(ThinkDoesNotQueryAddHostWhenAllHostsOnline)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto config = std::make_shared<TStorageConfig>(rawConfig);

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);

        // With the default set of DirectBlockGroupHostCount online hosts, the
        // alive count equals the required count, so no new host is requested.
        oracle.Think(TInstant::Now());
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.AddHostQueries.size());
    }

    Y_UNIT_TEST(ThinkDoesNotQueryAddHostForTemporaryOfflineHost)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(1);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        // Push host 0 into the TemporaryOffline state.
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);

        now += TDuration::Seconds(3);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::TemporaryOffline,
            hostStateController.States[0]);

        // A TemporaryOffline host is still considered alive, so the alive
        // count is unchanged and no new host is requested.
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.AddHostQueries.size());
    }

    Y_UNIT_TEST(ThinkQueriesAddHostWhenHostGoesOffline)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(1);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        // Generate a single error on host 0.
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);

        // Still TemporaryOffline three seconds later - no add-host request.
        now += TDuration::Seconds(3);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::TemporaryOffline,
            hostStateController.States[0]);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.AddHostQueries.size());

        // Six seconds later host 0 goes Offline, dropping the alive count
        // below DirectBlockGroupHostCount. The oracle must request a new host
        // with the next free index (equal to the current host count).
        now += TDuration::Seconds(3);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::Offline,
            hostStateController.States[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.AddHostQueries.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            hostStateController.AddHostQueries[0]);
    }

    Y_UNIT_TEST(ThinkKeepsQueryingAddHostWhileHostRemainsOffline)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(1);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        // Drive host 0 to Offline.
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);
        now += TDuration::Seconds(6);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(
            EHostState::Offline,
            hostStateController.States[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.AddHostQueries.size());

        // As long as no new host is actually added, the alive count stays
        // below the required count and every Think re-issues the request with
        // the same next-free index.
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(2, hostStateController.AddHostQueries.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            hostStateController.AddHostQueries[1]);
    }

    Y_UNIT_TEST(ThinkStopsQueryingAddHostAfterNewHostIsAdded)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(2000);
        oracleConfig.SetMaxDurationBeforeGoingOffline(4000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(1);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        auto now = TInstant::Now();

        // Drive host 0 to Offline so a new host is requested.
        oracle.OnRequestStarted(0, EOperation::WriteToPBuffer, now);
        oracle.OnRequestFailed(0, EOperation::WriteToPBuffer, now);
        now += TDuration::Seconds(6);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.AddHostQueries.size());

        // Satisfy the request: add the newly-requested host. It comes up
        // Online, restoring the alive count to DirectBlockGroupHostCount.
        oracle.AddHostIfNeeded(hostStateController.AddHostQueries.back());

        // No further add-host requests are issued.
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.AddHostQueries.size());
    }

    Y_UNIT_TEST(AddHostIfNeededGrowsHostCount)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto config = std::make_shared<TStorageConfig>(rawConfig);

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        const auto now = TInstant::Now();

        // Initially there are DirectBlockGroupHostCount hosts.
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            oracle.BuildHostStats(now).size());

        // Requesting a host with a higher index grows the internal storage so
        // that index becomes valid. Index 6 requires 7 hosts total.
        oracle.AddHostIfNeeded(6);
        UNIT_ASSERT_VALUES_EQUAL(7u, oracle.BuildHostStats(now).size());

        // The freshly added hosts start in the Online state.
        const auto stats = oracle.BuildHostStats(now);
        for (size_t i = DirectBlockGroupHostCount; i < stats.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(EHostState::Online, stats[i].State);
        }
    }

    Y_UNIT_TEST(AddHostIfNeededIsNoOpForExistingIndex)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto config = std::make_shared<TStorageConfig>(rawConfig);

        THostStateControllerMock hostStateController;
        TOracle oracle(config, &hostStateController);
        const auto now = TInstant::Now();

        // An index that already exists must not change the host count.
        oracle.AddHostIfNeeded(DirectBlockGroupHostCount - 1);
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            oracle.BuildHostStats(now).size());

        // Calling it again with the same index is idempotent.
        oracle.AddHostIfNeeded(0);
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            oracle.BuildHostStats(now).size());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
