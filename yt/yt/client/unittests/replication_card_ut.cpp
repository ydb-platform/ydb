#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChaosClient {
namespace {

using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardFetchOptionsTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        TReplicationCardFetchOptions,
        TReplicationCardFetchOptions,
        bool>>
{ };

TEST_P(TReplicationCardFetchOptionsTest, Contains)
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
    TReplicationCardFetchOptionsTest,
    TReplicationCardFetchOptionsTest,
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
            false)
));

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
            false)
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosClient
