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
    TVector<THostState>* StatesPtr;
    TMap<ui8, THostState::EState> States;

    explicit THostStateControllerMock(TVector<THostState>* statesPtr)
        : StatesPtr(statesPtr)
    {}

    void SetHostState(ui8 hostIndex, THostState::EState state) override
    {
        (*StatesPtr)[hostIndex].State = state;
        States[hostIndex] = state;
    }

    ui64 GetHostPBufferUsedSize(ui8 hostIndex) const override
    {
        Y_UNUSED(hostIndex);

        return 1_MB;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TOracle)
{
    Y_UNIT_TEST(SelectBestPBufferHostShouldPickHostWithLowestInflight)
    {
        const std::vector<ui8> hostIndexes = {0, 1, 2, 3, 4};

        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        // Host 2 has the lowest inflight count (zero), all others are higher.
        for (ui8 hostIndex: {0, 1, 3, 4}) {
            stats[hostIndex].OnRequest(EOperation::WriteToManyPBuffers);
        }

        TOracle oracle(nullptr, nullptr, stats, states);

        // Run multiple times to ensure deterministic selection (no ties at
        // the minimum).
        for (size_t iter = 0; iter < 100; ++iter) {
            const ui8 selected = oracle.SelectBestPBufferHost(
                hostIndexes,
                EOperation::WriteToManyPBuffers);

            UNIT_ASSERT_VALUES_EQUAL(2u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldHandleSingleHost)
    {
        const std::vector<ui8> hostIndexes = {3};

        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        // Host 2 has the lowest inflight count (zero), all others are higher.
        for (ui8 hostIndex: {0, 1, 3, 4}) {
            stats[hostIndex].OnRequest(EOperation::WriteToManyPBuffers);
        }

        TOracle oracle(nullptr, nullptr, stats, states);

        // Run multiple times to ensure deterministic selection (no ties at
        // the minimum).
        for (size_t iter = 0; iter < 100; ++iter) {
            const ui8 selected = oracle.SelectBestPBufferHost(
                hostIndexes,
                EOperation::WriteToManyPBuffers);

            UNIT_ASSERT_VALUES_EQUAL(3u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldRespectSubsetOfHosts)
    {
        // Even if some non-listed host has a lower inflight count, the
        // selection must be limited to the supplied hostIndexes.
        const std::vector<ui8> hostIndexes = {0, 3};

        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        stats[0].OnRequest(EOperation::WriteToManyPBuffers);
        stats[0].OnRequest(EOperation::WriteToManyPBuffers);
        stats[3].OnRequest(EOperation::WriteToManyPBuffers);

        TOracle oracle(nullptr, nullptr, stats, states);

        // Run multiple times to ensure deterministic selection (no ties at
        // the minimum).
        for (size_t iter = 0; iter < 100; ++iter) {
            const ui8 selected = oracle.SelectBestPBufferHost(
                hostIndexes,
                EOperation::WriteToManyPBuffers);

            UNIT_ASSERT_VALUES_EQUAL(3u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldDistributeTiesAcrossAllCandidates)
    {
        // All three hosts have the same (zero) inflight count. With reservoir
        // sampling, every tied host must have a roughly equal probability of
        // being selected. We verify this by sampling a large number of times
        // and checking that every candidate appears at least once.
        const std::vector<ui8> hostIndexes = {1, 2, 4};

        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        TOracle oracle(nullptr, nullptr, stats, states);

        std::map<ui8, size_t> counts;
        const size_t iterations = 3000;
        for (size_t iter = 0; iter < iterations; ++iter) {
            const ui8 selected = oracle.SelectBestPBufferHost(
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
                    << "host " << static_cast<ui32>(hostIndex) << " was picked "
                    << count << " times, expected ~" << expected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldIgnoreTiesAboveMinimum)
    {
        // Hosts with inflight equal to the (non-best) tie value must never be
        // picked - only ties at the global minimum are randomized.
        const std::vector<ui8> hostIndexes = {0, 1, 2, 3, 4};
        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        TOracle oracle(nullptr, nullptr, stats, states);

        // Host 2 has the lowest inflight count (zero), all others are higher.
        for (ui8 hostIndex: {0, 1, 3, 4}) {
            stats[hostIndex].OnRequest(EOperation::Flush);
        }

        for (size_t iter = 0; iter < 200; ++iter) {
            const ui8 selected =
                oracle.SelectBestPBufferHost(hostIndexes, EOperation::Flush);

            UNIT_ASSERT_VALUES_EQUAL(2u, selected);
        }
    }

    Y_UNIT_TEST(DisableHostOnErrorAndTime)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingOffline(2000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(1);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<ui8> hostIndexes = {0, 1, 2, 3, 4};
        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        THostStateControllerMock hostStateController(&states);
        TOracle oracle(config, &hostStateController, stats, states);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate error
        stats[0].OnRequest(EOperation::WriteToPBuffer);
        stats[0].OnError(now, EOperation::WriteToPBuffer);

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

        // Three seconds later. Switching to the disabled state.
        now += TDuration::Seconds(1);
        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            THostState::EState::Disabled,
            hostStateController.States[0]);

        // Generate success. Switching to the enabled state.
        now += TDuration::Seconds(1);
        stats[0].OnRequest(EOperation::WriteToPBuffer);
        stats[0].OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            THostState::EState::Enabled,
            hostStateController.States[0]);
    }

    Y_UNIT_TEST(DoNotDisableHostOnFewErrorAndTime)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingOffline(2000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(5);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<ui8> hostIndexes = {0, 1, 2, 3, 4};
        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        THostStateControllerMock hostStateController(&states);
        TOracle oracle(config, &hostStateController, stats, states);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate error
        stats[0].OnRequest(EOperation::WriteToPBuffer);
        stats[0].OnError(now, EOperation::WriteToPBuffer);

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
        oracleConfig.SetMaxDurationBeforeGoingOffline(2000);
        oracleConfig.SetMinErrorsCountBeforeGoingOffline(5);
        oracleConfig.SetErrorsCountForGoingOffline(500);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<ui8> hostIndexes = {0, 1, 2, 3, 4};
        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        THostStateControllerMock hostStateController(&states);
        TOracle oracle(config, &hostStateController, stats, states);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate a lot of errors. Switching to the disabled state.
        for (size_t i = 0; i < 500; ++i) {
            stats[0].OnRequest(EOperation::WriteToPBuffer);
            stats[0].OnError(now, EOperation::WriteToPBuffer);
        }

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            THostState::EState::Disabled,
            hostStateController.States[0]);

        // Generate success. Switching to the enabled state.
        now += TDuration::Seconds(1);
        stats[0].OnRequest(EOperation::WriteToPBuffer);
        stats[0].OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            THostState::EState::Enabled,
            hostStateController.States[0]);
    }

    Y_UNIT_TEST(DisableHostOnTotalSizeOfErrors)
    {
        NProto::TStorageServiceConfig rawConfig;
        auto& oracleConfig = *rawConfig.MutableOracleConfig();
        oracleConfig.SetMaxDurationBeforeGoingOffline(2000);
        oracleConfig.SetErrorsTotalSizeForGoingOffline(1_MB);

        auto config = std::make_shared<TStorageConfig>(rawConfig);

        const std::vector<ui8> hostIndexes = {0, 1, 2, 3, 4};
        TVector<THostStat> stats{{}, {}, {}, {}, {}};
        TVector<THostState> states{{}, {}, {}, {}, {}};

        THostStateControllerMock hostStateController(&states);
        TOracle oracle(config, &hostStateController, stats, states);
        auto now = TInstant::Now();

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(0, hostStateController.States.size());

        // Generate a error with large total size. Switching to the disabled
        // state.
        stats[0].OnRequest(EOperation::WriteToPBuffer);
        stats[0].OnError(now, EOperation::WriteToPBuffer);

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            THostState::EState::Disabled,
            hostStateController.States[0]);

        // Generate success. Switching to the enabled state.
        now += TDuration::Seconds(1);
        stats[0].OnRequest(EOperation::WriteToPBuffer);
        stats[0].OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);

        oracle.Think(now);
        UNIT_ASSERT_VALUES_EQUAL(1, hostStateController.States.size());
        UNIT_ASSERT_VALUES_EQUAL(
            THostState::EState::Enabled,
            hostStateController.States[0]);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
