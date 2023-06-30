#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/hedging_manager.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveHedgingManagerTest
    : public ::testing::Test
{
protected:
    const TAdaptiveHedgingManagerConfigPtr Config_ = New<TAdaptiveHedgingManagerConfig>();

    IHedgingManagerPtr HedgingManager_;


    void CreateHedgingManager()
    {
        Config_->Postprocess();
        HedgingManager_ = CreateAdaptiveHedgingManager(Config_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAdaptiveHedgingManagerTest, Simple)
{
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.1;
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, RatioGreaterThanOne)
{
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 2.0;
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, RestrainHedging)
{
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.5;
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
    EXPECT_FALSE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, ApproveHedging)
{
    Config_->MinHedgingDelay = TDuration::Seconds(1);
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.5;
    Config_->TickPeriod = TDuration::Seconds(1);
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    Sleep(TDuration::Seconds(1));

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, TuneHedgingDelay)
{
    Config_->MinHedgingDelay = TDuration::MilliSeconds(1);
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.1;
    Config_->HedgingDelayTuneFactor = 1e9;
    Config_->TickPeriod = TDuration::Seconds(1);
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    Sleep(TDuration::Seconds(1));

    EXPECT_EQ(TDuration::MilliSeconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));

    Sleep(TDuration::Seconds(1));

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
