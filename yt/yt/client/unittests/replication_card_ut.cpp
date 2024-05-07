#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChaosClient {
namespace {

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

} // namespace
} // namespace NYT::NChaosClient
