#include "host_health_policy.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <public.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

constexpr TDuration DefaultMaxDurationBeforeGoingTemporaryOffline =
    TDuration::Seconds(3);
constexpr TDuration DefaultMaxDurationBeforeGoingOffline =
    TDuration::Seconds(4);
constexpr ui32 DefaultMinErrorsCountBeforeGoingOffline = 2;
constexpr ui32 DefaultErrorsCountForGoingOffline = 3;
constexpr ui64 DefaultErrorsTotalSizeForGoingOffline = 1_MB;
constexpr TDuration DefaultMaxDurationBeforeReturningOnline =
    TDuration::Seconds(2);
constexpr ui32 DefaultMinSuccessesCountBeforeReturningOnline = 2;

struct TConfigPreset
{
    TDuration MaxDurationBeforeGoingTemporaryOffline =
        DefaultMaxDurationBeforeGoingTemporaryOffline;
    TDuration MaxDurationBeforeGoingOffline =
        DefaultMaxDurationBeforeGoingOffline;

    ui32 MinErrorsCountBeforeGoingOffline =
        DefaultMinErrorsCountBeforeGoingOffline;
    ui32 ErrorsCountForGoingOffline = DefaultErrorsCountForGoingOffline;
    ui64 ErrorsTotalSizeForGoingOffline = DefaultErrorsTotalSizeForGoingOffline;

    TDuration MaxDurationBeforeReturningOnline =
        DefaultMaxDurationBeforeReturningOnline;

    ui32 MinSuccessesCountBeforeReturningOnline =
        DefaultMinSuccessesCountBeforeReturningOnline;
};

struct TPolicyTest
{
    [[maybe_unused]] TStorageConfigPtr StorageConfig;
    [[maybe_unused]] TOracleConfigPtr OracleConfig;
    std::unique_ptr<IHostHealthPolicy> Policy;
};

TStorageConfigPtr CreateStorageConfig(const TConfigPreset& preset)
{
    NProto::TStorageServiceConfig rawConfig;
    auto& oracleConfig = *rawConfig.MutableOracleConfig();
    oracleConfig.SetMaxDurationBeforeGoingTemporaryOffline(
        preset.MaxDurationBeforeGoingTemporaryOffline.MilliSeconds());
    oracleConfig.SetMaxDurationBeforeGoingOffline(
        preset.MaxDurationBeforeGoingOffline.MilliSeconds());
    oracleConfig.SetMinErrorsCountBeforeGoingOffline(
        preset.MinErrorsCountBeforeGoingOffline);
    oracleConfig.SetErrorsCountForGoingOffline(
        preset.ErrorsCountForGoingOffline);
    oracleConfig.SetErrorsTotalSizeForGoingOffline(
        preset.ErrorsTotalSizeForGoingOffline);
    oracleConfig.SetMaxDurationBeforeReturningOnline(
        preset.MaxDurationBeforeReturningOnline.MilliSeconds());
    oracleConfig.SetMinSuccessesCountBeforeReturningOnline(
        preset.MinSuccessesCountBeforeReturningOnline);

    return std::make_shared<TStorageConfig>(rawConfig);
}

TPolicyTest CreatePolicyTest(const TConfigPreset& preset = TConfigPreset{})
{
    auto storageConfig = CreateStorageConfig(preset);
    auto oracleConfig = std::make_shared<TOracleConfig>(storageConfig);
    auto policy = CreateDefaultHostHealthPolicy(oracleConfig);
    return {
        .StorageConfig = std::move(storageConfig),
        .OracleConfig = std::move(oracleConfig),
        .Policy = std::move(policy),
    };
}

// constexpr size_t OverwhelmingRequestsCount = 10;

}   // namespace

Y_UNIT_TEST_SUITE(TDefaultHostHealthPolicyTest)
{
    // Empty stats behavior

    Y_UNIT_TEST(StaysOnlineWithEmptyStats)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{};

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Online,
            policy.Policy->GetNewHealth(EHostHealth::Online, stats, 0));
    }

    Y_UNIT_TEST(GoesFromSufferrerToOnlineWithEmptyStats)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{};

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Online,
            policy.Policy->GetNewHealth(EHostHealth::Sufferer, stats, 0));
    }

    Y_UNIT_TEST(StaysTemporaryOfflineWithEmptyStats)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{};

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));
    }

    Y_UNIT_TEST(StaysOfflineWithEmptyStats)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{};

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Offline, stats, 0));
    }

    // Idle behavior (health's specific conditions are preserved)

    Y_UNIT_TEST(StaysOnlineOnIdle)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstSuccess = TDuration::Seconds(1),
            .FromLastSuccess = TDuration::Seconds(1),
            .ConsecutiveSuccessCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Online,
            policy.Policy->GetNewHealth(EHostHealth::Online, stats, 0));
    }

    Y_UNIT_TEST(StaysSuffererOnIdle)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError = TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(1),
            .ConsecutiveErrorCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Sufferer,
            policy.Policy->GetNewHealth(EHostHealth::Sufferer, stats, 0));
    }

    Y_UNIT_TEST(StaysTemporaryOfflineOnShortIdle)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError = TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = DefaultErrorsCountForGoingOffline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));
    }

    Y_UNIT_TEST(GoesFromTemporaryOfflineToOfflineOnLongIdle)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError =
                DefaultMaxDurationBeforeGoingOffline + TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = DefaultErrorsCountForGoingOffline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));
    }

    Y_UNIT_TEST(StaysOfflineOnIdle)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError =
                DefaultMaxDurationBeforeGoingOffline + TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = DefaultErrorsCountForGoingOffline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Offline, stats, 0));
    }

    // Online<->Sufferer transitions

    Y_UNIT_TEST(GoesOnlineToSuffererOnFewErrors)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError = TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Sufferer,
            policy.Policy->GetNewHealth(EHostHealth::Online, stats, 0));
    }

    Y_UNIT_TEST(GoesSuffererToOnlineOnNoErrors)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstSuccess = TDuration::Seconds(1),
            .FromLastSuccess = TDuration::Seconds(1),
            .ConsecutiveSuccessCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Online,
            policy.Policy->GetNewHealth(EHostHealth::Sufferer, stats, 0));
    }

    // Online/Sufferer->TemporaryOffline

    Y_UNIT_TEST(GoesToTemporaryOfflineOnTooManyErrors)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError = TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = DefaultErrorsCountForGoingOffline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy->GetNewHealth(EHostHealth::Online, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy->GetNewHealth(EHostHealth::Sufferer, stats, 0));
    }

    Y_UNIT_TEST(GoesToTemporaryOfflineOnManyErrorsAfterDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError = DefaultMaxDurationBeforeGoingTemporaryOffline +
                              TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = DefaultMinErrorsCountBeforeGoingOffline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy->GetNewHealth(EHostHealth::Online, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy->GetNewHealth(EHostHealth::Sufferer, stats, 0));
    }

    Y_UNIT_TEST(GoesToTemporaryOfflineOnErrorsTotalSizeExceeded)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError = TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy->GetNewHealth(
                EHostHealth::Online,
                stats,
                DefaultErrorsTotalSizeForGoingOffline));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy->GetNewHealth(
                EHostHealth::Sufferer,
                stats,
                DefaultErrorsTotalSizeForGoingOffline));
    }

    Y_UNIT_TEST(DoesntGoToTemporaryOfflineOnTotalSizeExceededWithoutErrors)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstSuccess = TDuration::Seconds(1),
            .FromLastSuccess = TDuration::Seconds(0),
            .ConsecutiveSuccessCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Online,
            policy.Policy->GetNewHealth(
                EHostHealth::Online,
                stats,
                DefaultErrorsTotalSizeForGoingOffline));
    }

    // Online/Sufferer/TemporaryOffline->Offline

    Y_UNIT_TEST(GoesToOfflineOnTooManyErrorsAfterOfflineDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError =
                DefaultMaxDurationBeforeGoingOffline + TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = DefaultErrorsCountForGoingOffline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Online, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Sufferer, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));
    }

    Y_UNIT_TEST(GoesToOfflineOnManyErrorsAfterOfflineDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError =
                DefaultMaxDurationBeforeGoingOffline + TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = DefaultMinErrorsCountBeforeGoingOffline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Online, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Sufferer, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));
    }

    Y_UNIT_TEST(GoesToOfflineOnErrorsTotalSizeExceededAfterOfflineDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstError =
                DefaultMaxDurationBeforeGoingOffline + TDuration::Seconds(1),
            .FromLastError = TDuration::Seconds(0),
            .ConsecutiveErrorCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(
                EHostHealth::Online,
                stats,
                DefaultErrorsTotalSizeForGoingOffline));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(
                EHostHealth::Sufferer,
                stats,
                DefaultErrorsTotalSizeForGoingOffline));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(
                EHostHealth::TemporaryOffline,
                stats,
                DefaultErrorsTotalSizeForGoingOffline));
    }

    // TemporaryOffline/Offline->Online

    Y_UNIT_TEST(GoesToOnlineOnEnoughSuccessesAfterDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstSuccess =
                DefaultMaxDurationBeforeReturningOnline + TDuration::Seconds(1),
            .FromLastSuccess = TDuration::Seconds(0),
            .ConsecutiveSuccessCount =
                DefaultMinSuccessesCountBeforeReturningOnline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Online,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Online,
            policy.Policy->GetNewHealth(EHostHealth::Offline, stats, 0));
    }

    Y_UNIT_TEST(DoesntGoToOnlineOnNotEnoughSuccessesAfterDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstSuccess =
                DefaultMaxDurationBeforeReturningOnline + TDuration::Seconds(1),
            .FromLastSuccess = TDuration::Seconds(0),
            .ConsecutiveSuccessCount = 1,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Offline, stats, 0));
    }

    Y_UNIT_TEST(DoesntGoToOnlineOnEnoughSuccessesBeforeDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstSuccess = TDuration::Seconds(1),
            .FromLastSuccess = TDuration::Seconds(0),
            .ConsecutiveSuccessCount =
                DefaultMinSuccessesCountBeforeReturningOnline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::TemporaryOffline,
            policy.Policy
                ->GetNewHealth(EHostHealth::TemporaryOffline, stats, 0));

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Offline,
            policy.Policy->GetNewHealth(EHostHealth::Offline, stats, 0));
    }

    // Broken->Online

    Y_UNIT_TEST(DoesntGoFromBrokenToOnlineOnEnoughSuccessesAfterDelay)
    {
        auto policy = CreatePolicyTest();

        THostErrorsInfo stats{
            .FromFirstSuccess =
                DefaultMaxDurationBeforeReturningOnline + TDuration::Seconds(1),
            .FromLastSuccess = TDuration::Seconds(0),
            .ConsecutiveSuccessCount =
                DefaultMinSuccessesCountBeforeReturningOnline,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            EHostHealth::Broken,
            policy.Policy->GetNewHealth(EHostHealth::Broken, stats, 0));
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
