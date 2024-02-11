#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/digest.h>
#include <yt/yt/core/misc/config.h>

#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TLogDigestTest
    : public ::testing::Test
{
protected:
    void CreateStandardLogDigest()
    {
        auto config = New<TLogDigestConfig>();
        config->LowerBound = 0.5;
        config->UpperBound = 1.0;
        config->RelativePrecision = Epsilon;
        LogDigest_ = CreateLogDigest(config);
    }

    bool LogNear(double a, double b)
    {
        return a < b * (1 + Epsilon) * (1 + Epsilon) && b < a * (1 + Epsilon) * (1 + Epsilon);
    }

    static constexpr double Epsilon = 0.01;
    static constexpr int SampleCount = 10000;

    IDigestPtr LogDigest_;
};

TEST_F(TLogDigestTest, TestStrictFixtureInRange)
{
    CreateStandardLogDigest();

    for (int i = 0; i < SampleCount; ++i) {
        LogDigest_->AddSample(0.77);
    }

    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.0), 0.5));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.5), 0.77));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(1.0), 0.77));
}

TEST_F(TLogDigestTest, TestStrictFixtureBelowRange)
{
    CreateStandardLogDigest();

    for (int i = 0; i < SampleCount; ++i) {
        LogDigest_->AddSample(0.17);
    }

    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.0), 0.5));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.5), 0.5));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(1.0), 0.5));
}

TEST_F(TLogDigestTest, TestStrictFixtureAboveRange)
{
    CreateStandardLogDigest();

    for (int i = 0; i < SampleCount; ++i) {
        LogDigest_->AddSample(1.17);
    }

    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.0), 0.5));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.5), 1.0));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(1.0), 1.0));
}

TEST_F(TLogDigestTest, TestNormalDistributionFixture)
{
    CreateStandardLogDigest();

    std::mt19937 generator(42 /*seed*/);
    std::normal_distribution<double> distribution(0.77, 0.05);

    for (int i = 0; i < SampleCount; ++i) {
        LogDigest_->AddSample(distribution(generator));
    }

    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.5), 0.77));
    // Theoretical 95% quantile.
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.95), 0.852));
}

TEST_F(TLogDigestTest, TestUniformRandomFixture)
{
    CreateStandardLogDigest();

    std::mt19937 generator(42 /*seed*/);
    std::uniform_real_distribution<double> distribution(0.25, 1.25);

    for (int i = 0; i < SampleCount; ++i) {
        LogDigest_->AddSample(distribution(generator));
    }

    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(1.0), 1.0));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.75), 1.0));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.5), 0.75));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.25), 0.5));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.0), 0.5));
}

TEST_F(TLogDigestTest, TestCoincidingBounds)
{
    auto config = New<TLogDigestConfig>();
    config->LowerBound = 1.0;
    config->UpperBound = 1.0;
    config->RelativePrecision = Epsilon;
    LogDigest_ = CreateLogDigest(config);

    std::mt19937 generator(42 /*seed*/);
    std::uniform_real_distribution<double> distribution(0.5, 1.5);

    for (int i = 0; i < SampleCount; ++i) {
        LogDigest_->AddSample(distribution(generator));
    }

    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(1.0), 1.0));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.75), 1.0));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.5), 1.0));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.25), 1.0));
    EXPECT_TRUE(LogNear(LogDigest_->GetQuantile(0.0), 1.0));
}

////////////////////////////////////////////////////////////////////////////////

class THistogramDigestTest
    : public ::testing::Test
{
protected:
    void CreateStandardHistogramDigest()
    {
        auto config = New<THistogramDigestConfig>();
        config->LowerBound = 0.0;
        config->UpperBound = 1.0;
        config->AbsolutePrecision = Epsilon;
        Digest_ = CreateHistogramDigest(config);
    }

    static constexpr double Epsilon = 0.01;
    static constexpr int SampleCount = 10000;
    static constexpr int Seed = 225;

    IDigestPtr Digest_;
};

TEST_F(THistogramDigestTest, TestStrictFixtureInRange)
{
    CreateStandardHistogramDigest();

    for (int i = 0; i < SampleCount; ++i) {
        Digest_->AddSample(0.77);
    }

    EXPECT_NEAR(Digest_->GetQuantile(0.0), 0.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.5), 0.77, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(1.0), 0.77, Epsilon);
}

TEST_F(THistogramDigestTest, TestStrictFixtureBelowRange)
{
    CreateStandardHistogramDigest();

    for (int i = 0; i < SampleCount; ++i) {
        Digest_->AddSample(-0.117);
    }

    EXPECT_NEAR(Digest_->GetQuantile(0.0), 0.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.5), 0.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(1.0), 0.0, Epsilon);
}

TEST_F(THistogramDigestTest, TestStrictFixtureAboveRange)
{
    CreateStandardHistogramDigest();

    for (int i = 0; i < SampleCount; ++i) {
        Digest_->AddSample(1.17);
    }

    EXPECT_NEAR(Digest_->GetQuantile(0.0), 0.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.5), 1.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(1.0), 1.0, Epsilon);
}

TEST_F(THistogramDigestTest, TestNormalDistributionFixture)
{
    CreateStandardHistogramDigest();

    std::mt19937 generator(Seed);
    std::normal_distribution<double> distribution(0.77, 0.05);

    for (int i = 0; i < SampleCount; ++i) {
        Digest_->AddSample(distribution(generator));
    }

    EXPECT_NEAR(Digest_->GetQuantile(0.5), 0.77, Epsilon);
    // Theoretical 95% quantile.
    EXPECT_NEAR(Digest_->GetQuantile(0.95), 0.852, Epsilon);
}

TEST_F(THistogramDigestTest, TestUniformRandomFixture)
{
    CreateStandardHistogramDigest();

    std::mt19937 generator(Seed);
    std::uniform_real_distribution<double> distribution(-0.5, 1.5);

    for (int i = 0; i < SampleCount; ++i) {
        Digest_->AddSample(distribution(generator));
    }

    EXPECT_NEAR(Digest_->GetQuantile(1.0), 1.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.75), 1.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.5), 0.5, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.25), 0.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.0), 0.0, Epsilon);
}

TEST_F(THistogramDigestTest, TestCoincidingBounds)
{
    auto config = New<THistogramDigestConfig>();
    config->LowerBound = 1.0;
    config->UpperBound = 1.0;
    config->AbsolutePrecision = Epsilon;
    Digest_ = CreateHistogramDigest(std::move(config));

    std::mt19937 generator(Seed);
    std::uniform_real_distribution<double> distribution(0.5, 1.5);

    for (int i = 0; i < SampleCount; ++i) {
        Digest_->AddSample(distribution(generator));
    }

    EXPECT_NEAR(Digest_->GetQuantile(1.0), 1.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.75), 1.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.5), 1.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.25), 1.0, Epsilon);
    EXPECT_NEAR(Digest_->GetQuantile(0.0), 1.0, Epsilon);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
