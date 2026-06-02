#include "oracle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <map>
#include <vector>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THostStateControllerMock: public IHostStateController
{
    TMap<THostIndex, EHostState> States;
    ui64 HostPBufferUsedSize = 1_MB;

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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TOracle)
{
    Y_UNIT_TEST(SelectBestPBufferHostShouldPickHostWithLowestInflight)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};

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
                hostIndexes,
                EOperation::WriteToManyPBuffers);

            UNIT_ASSERT_VALUES_EQUAL(2u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldHandleSingleHost)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto storageConfig = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<THostIndex> hostIndexes = {3};

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
                hostIndexes,
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
        const std::vector<THostIndex> hostIndexes = {0, 3};

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
                hostIndexes,
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
        const std::vector<THostIndex> hostIndexes = {1, 2, 4};

        TOracle oracle(storageConfig, nullptr);

        std::map<THostIndex, size_t> counts;
        const size_t iterations = 3000;
        for (size_t iter = 0; iter < iterations; ++iter) {
            const THostIndex selected = oracle.SelectBestPBufferHost(
                hostIndexes,
                EOperation::WriteToManyPBuffers);
            ++counts[selected];
        }

        UNIT_ASSERT_VALUES_EQUAL(3u, counts.size());

        // Each tied host must be picked a meaningful share of the time.
        // Expected ~1/3 of iterations each, allow generous tolerance.
        const size_t expected = iterations / 3;
        const size_t tolerance = expected / 2;   // 50% tolerance
        for (auto hostIndex: hostIndexes) {
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
        const std::vector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};

        TOracle oracle(storageConfig, nullptr);

        // Host 2 has the lowest inflight count (zero), all others are higher.
        const auto now = TInstant::Now();
        for (THostIndex hostIndex: {0, 1, 3, 4}) {
            oracle.OnRequestStarted(hostIndex, EOperation::Flush, now);
        }

        for (size_t iter = 0; iter < 200; ++iter) {
            const THostIndex selected =
                oracle.SelectBestPBufferHost(hostIndexes, EOperation::Flush);

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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
