#include "digest.h"
#include "config.h"

#include <yt/yt/core/misc/phoenix.h>

namespace NYT {

using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

class TLogDigest
    : public IPersistentDigest
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TLogDigest(TLogDigestConfigPtr config)
        : Step_(1 + config->RelativePrecision)
        , LogStep_(log(Step_))
        , LowerBound_(config->LowerBound)
        , UpperBound_(config->UpperBound)
        , DefaultValue_(config->DefaultValue ? *config->DefaultValue : config->LowerBound)
        , BucketCount_(std::max(1, static_cast<int>(ceil(log(UpperBound_ / LowerBound_) / LogStep_))))
        , Buckets_(BucketCount_)
    { }

    TLogDigest() = default;

    void AddSample(double value) override
    {
        double bucketId = log(value / LowerBound_) / LogStep_;
        if (std::isnan(bucketId) || bucketId < std::numeric_limits<i32>::min() || bucketId > std::numeric_limits<i32>::max()) {
            // Discard all incorrect values (those that are non-positive, too small or too large).
            return;
        }
        ++Buckets_[std::clamp(static_cast<int>(bucketId), 0, BucketCount_ - 1)];
        ++SampleCount_;
    }

    double GetQuantile(double alpha) const override
    {
        if (SampleCount_ == 0) {
            return DefaultValue_;
        }
        double value = LowerBound_;
        i64 sum = 0;
        for (int index = 0; index < BucketCount_; ++index) {
            if (sum >= alpha * SampleCount_) {
                return value;
            }
            sum += Buckets_[index];
            value *= Step_;
        }
        return UpperBound_;
    }

    void Reset() override
    {
        SampleCount_ = 0;
        std::fill(Buckets_.begin(), Buckets_.end(), 0);
    }

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, Step_);
        Persist(context, LogStep_);
        Persist(context, LowerBound_);
        Persist(context, UpperBound_);
        Persist(context, DefaultValue_);
        Persist(context, BucketCount_);
        Persist(context, SampleCount_);
        Persist(context, Buckets_);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TLogDigest, 0x42424243);

private:
    double Step_;
    double LogStep_;

    double LowerBound_;
    double UpperBound_;
    double DefaultValue_;

    int BucketCount_;

    i64 SampleCount_ = 0;

    std::vector<i64> Buckets_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TLogDigest);

////////////////////////////////////////////////////////////////////////////////

IPersistentDigestPtr CreateLogDigest(TLogDigestConfigPtr config)
{
    return New<TLogDigest>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

class THistogramDigest
    : public IDigest
{
public:
    explicit THistogramDigest(THistogramDigestConfigPtr config)
        : Config_(std::move(config))
        , Step_(Config_->AbsolutePrecision)
        , BucketCount_(static_cast<int>(std::round((Config_->UpperBound - Config_->LowerBound) / Step_)) + 1)
        , Buckets_(BucketCount_)
    { }

    void AddSample(double value) override
    {
        if (!std::isfinite(value)) {
            return;
        }

        value = std::clamp(value, Config_->LowerBound, Config_->UpperBound);
        int bucketId = static_cast<int>(std::round((value - Config_->LowerBound) / Step_));
        YT_ASSERT(bucketId < BucketCount_);

        // Note that due to round, i-th bucket corresponds to range [LowerBound + i*Step - Step/2; LowerBound + i*Step + Step/2).
        ++Buckets_[bucketId];
        ++SampleCount_;
    }

    double GetQuantile(double alpha) const override
    {
        if (SampleCount_ == 0) {
            return Config_->DefaultValue.value_or(Config_->LowerBound);
        }

        double value = Config_->LowerBound;
        i64 sum = 0;
        for (int index = 0; index < BucketCount_; ++index) {
            sum += Buckets_[index];
            if (sum >= alpha * SampleCount_) {
                return value;
            }
            value += Step_;
        }

        return Config_->UpperBound;
    }

    void Reset() override
    {
        SampleCount_ = 0;
        std::fill(Buckets_.begin(), Buckets_.end(), 0);
    }

private:
    THistogramDigestConfigPtr Config_;
    const double Step_;
    const int BucketCount_;

    i64 SampleCount_ = 0;
    std::vector<i64> Buckets_;
};

////////////////////////////////////////////////////////////////////////////////

IDigestPtr CreateHistogramDigest(THistogramDigestConfigPtr config)
{
    return New<THistogramDigest>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
