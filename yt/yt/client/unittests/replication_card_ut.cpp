#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChaosClient {
namespace {

using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardFetchOptionsContainsTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        TReplicationCardFetchOptions,
        TReplicationCardFetchOptions,
        bool>>
{ };

TEST_P(TReplicationCardFetchOptionsContainsTest, Contains)
{
    const auto& params = GetParam();
    auto self = std::get<0>(params);
    auto& other = std::get<1>(params);
    auto expected = std::get<2>(params);


    EXPECT_EQ(self.Contains(other), expected)
        << "progress: " << std::get<0>(params) << std::endl
        << "update: " << std::get<1>(params) << std::endl
        << "expected: " << std::get<2>(params) << std::endl
        << "actual: " << self.Contains(other) << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TReplicationCardFetchOptionsContainsTest,
    TReplicationCardFetchOptionsContainsTest,
    ::testing::Values(
        std::tuple(
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = false,
                .IncludeProgress = false,
                .IncludeHistory = false,
                .IncludeReplicatedTableOptions = false,
            },
            true),
        std::tuple(
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            true),
        std::tuple(
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = false,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = false,
            },
            true),
        std::tuple(
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = false,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = false,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = false,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = false,
            },
            false)));


class TReplicationCardFetchOptionsOrTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        TReplicationCardFetchOptions,
        TReplicationCardFetchOptions,
        TReplicationCardFetchOptions>>
{ };

TEST_P(TReplicationCardFetchOptionsOrTest, Or)
{
    const auto& params = GetParam();
    auto self = std::get<0>(params);
    auto& other = std::get<1>(params);
    auto expected = std::get<2>(params);


    EXPECT_EQ(self |= other, expected)
        << "progress: " << std::get<0>(params) << std::endl
        << "update: " << std::get<1>(params) << std::endl
        << "expected: " << std::get<2>(params) << std::endl
        << "actual: " << self.Contains(other) << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TReplicationCardFetchOptionsOrTest,
    TReplicationCardFetchOptionsOrTest,
    ::testing::Values(
        std::tuple(
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = false,
                .IncludeProgress = false,
                .IncludeHistory = false,
                .IncludeReplicatedTableOptions = false,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            }),
        std::tuple(
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = true,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            }),
        std::tuple(
            TReplicationCardFetchOptions {
                .IncludeCoordinators = false,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = false,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = false,
                .IncludeProgress = false,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            },
            TReplicationCardFetchOptions {
                .IncludeCoordinators = false,
                .IncludeProgress = true,
                .IncludeHistory = true,
                .IncludeReplicatedTableOptions = true,
            })));

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardIsReplicaReallySyncTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        ETableReplicaMode,
        ETableReplicaState,
        const std::vector<TReplicaHistoryItem>,
        bool>>
{ };

TEST_P(TReplicationCardIsReplicaReallySyncTest, IsReplicaReallySync)
{
    const auto& params = GetParam();
    auto mode = std::get<0>(params);
    auto state = std::get<1>(params);
    const auto& history = std::get<2>(params);
    auto expected = std::get<3>(params);

    auto actual = IsReplicaReallySync(mode, state, history);

    EXPECT_EQ(actual, expected)
        << "progress: " << ToString(mode) << std::endl
        << "update: " << ToString(state) << std::endl
        << "history: " << ToString(history) << std::endl
        << "actual: " << actual << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TReplicationCardIsReplicaReallySyncTest,
    TReplicationCardIsReplicaReallySyncTest,
    ::testing::Values(
        std::tuple(
            ETableReplicaMode::Sync,
            ETableReplicaState::Enabled,
            std::vector<TReplicaHistoryItem>(),
            true),
        std::tuple(
            ETableReplicaMode::SyncToAsync,
            ETableReplicaState::Enabled,
            std::vector<TReplicaHistoryItem>{
                TReplicaHistoryItem{
                    .Era = 0,
                    .Timestamp = 0,
                    .Mode = ETableReplicaMode::Sync,
                    .State = ETableReplicaState::Enabled,
                }
            },
            true),
        std::tuple(
            ETableReplicaMode::SyncToAsync,
            ETableReplicaState::Enabled,
            std::vector<TReplicaHistoryItem>{
                TReplicaHistoryItem{
                    .Era = 0,
                    .Timestamp = 0,
                    .Mode = ETableReplicaMode::Sync,
                    .State = ETableReplicaState::Disabled,
                },
                TReplicaHistoryItem{
                    .Era = 1,
                    .Timestamp = 1,
                    .Mode = ETableReplicaMode::Sync,
                    .State = ETableReplicaState::Enabled,
                }
            },
            true),
        std::tuple(
            ETableReplicaMode::SyncToAsync,
            ETableReplicaState::Enabled,
            std::vector<TReplicaHistoryItem>{
                TReplicaHistoryItem{
                    .Era = 0,
                    .Timestamp = 0,
                    .Mode = ETableReplicaMode::Async,
                    .State = ETableReplicaState::Enabled,
                }
            },
            false),
        std::tuple(
            ETableReplicaMode::SyncToAsync,
            ETableReplicaState::Enabled,
            std::vector<TReplicaHistoryItem>{
                TReplicaHistoryItem{
                    .Era = 0,
                    .Timestamp = 0,
                    .Mode = ETableReplicaMode::Sync,
                    .State = ETableReplicaState::Disabled,
                }
            },
            false),
        std::tuple(
            ETableReplicaMode::Async,
            ETableReplicaState::Enabled,
            std::vector<TReplicaHistoryItem>(),
            false)));

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardComputeReplicasLagTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        THashMap<TReplicaId, TReplicaInfo>,
        THashMap<TReplicaId, TDuration>>>
{
public:

    using TTestData = std::tuple<THashMap<TReplicaId, TReplicaInfo>, THashMap<TReplicaId, TDuration>>;

    static TTestData CreateTestDataNormal1()
    {
        auto syncReplicaId = TReplicaId::Create();
        auto syncReplicaInfo = TReplicaInfo{
            .Mode = ETableReplicaMode::Sync,
            .State = ETableReplicaState::Enabled,
            .ReplicationProgress = ConvertTo<TReplicationProgress>(
                // 1073741824
                TYsonStringBuf("{segments=[{lower_key=[];timestamp=21474836480}];upper_key=[<type=max>#]}")),
            .History = {
                TReplicaHistoryItem{
                    .Era = 1,
                    .Timestamp = 10ull << 30,
                    .Mode = ETableReplicaMode::Sync,
                    .State = ETableReplicaState::Enabled,
                }
            },
        };

        auto asyncReplicaId = TReplicaId::Create();
        auto asyncReplicaInfo = TReplicaInfo{
            .Mode = ETableReplicaMode::Async,
            .State = ETableReplicaState::Enabled,
            .ReplicationProgress = ConvertTo<TReplicationProgress>(
                TYsonStringBuf("{segments=[{lower_key=[];timestamp=16106127360}];upper_key=[<type=max>#]}")),
            .History = {
                TReplicaHistoryItem{
                    .Era = 1,
                    .Timestamp = 10ull << 30,
                    .Mode = ETableReplicaMode::Async,
                    .State = ETableReplicaState::Enabled,
                }
            },
        };

        return {
            THashMap{
                std::pair(syncReplicaId, syncReplicaInfo),
                std::pair(asyncReplicaId, asyncReplicaInfo)
            },
            THashMap{
                std::pair(syncReplicaId, TDuration::Zero()),
                std::pair(asyncReplicaId, TDuration::Seconds(4))
            }
        };
    }

    static TTestData CreateTestDataLaggingSyncReplica()
    {
        auto syncReplicaId = TReplicaId::Create();
        auto syncReplicaInfo = TReplicaInfo{
            .Mode = ETableReplicaMode::Sync,
            .State = ETableReplicaState::Enabled,
            .ReplicationProgress = ConvertTo<TReplicationProgress>(
                // 1073741824
                TYsonStringBuf("{segments=[{lower_key=[];timestamp=21474836480}];upper_key=[<type=max>#]}")),
            .History = {
                TReplicaHistoryItem{
                    .Era = 1,
                    .Timestamp = 10ull << 30,
                    .Mode = ETableReplicaMode::Async,
                    .State = ETableReplicaState::Enabled,
                },
                TReplicaHistoryItem{
                    .Era = 2,
                    .Timestamp = 30ull << 30,
                    .Mode = ETableReplicaMode::Async,
                    .State = ETableReplicaState::Enabled,
                }
            },
        };

        auto asyncReplicaId = TReplicaId::Create();
        auto asyncReplicaInfo = TReplicaInfo{
            .Mode = ETableReplicaMode::Async,
            .State = ETableReplicaState::Enabled,
            .ReplicationProgress = ConvertTo<TReplicationProgress>(
                TYsonStringBuf("{segments=[{lower_key=[];timestamp=16106127360}];upper_key=[<type=max>#]}")),
            .History = {
                TReplicaHistoryItem{
                    .Era = 1,
                    .Timestamp = 10ull << 30,
                    .Mode = ETableReplicaMode::Async,
                    .State = ETableReplicaState::Enabled,
                }
            },
        };

        return {
            THashMap{
                std::pair(syncReplicaId, syncReplicaInfo),
                std::pair(asyncReplicaId, asyncReplicaInfo)
            },
            THashMap{
                std::pair(syncReplicaId, TDuration::Seconds(9)),
                std::pair(asyncReplicaId, TDuration::Seconds(14))
            }
        };
    }
};

TEST_P(TReplicationCardComputeReplicasLagTest, ComputeReplicasLag)
{
    const auto& params = GetParam();
    const auto& replicas = std::get<0>(params);
    const auto& expected = std::get<1>(params);

    auto actual = ComputeReplicasLag(replicas);

    EXPECT_EQ(actual, expected);
}

INSTANTIATE_TEST_SUITE_P(
    TReplicationCardComputeReplicasLagTest,
    TReplicationCardComputeReplicasLagTest,
    ::testing::Values(
        TReplicationCardComputeReplicasLagTest::CreateTestDataNormal1(),
        TReplicationCardComputeReplicasLagTest::CreateTestDataLaggingSyncReplica()));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosClient
