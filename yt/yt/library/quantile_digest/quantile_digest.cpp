#include "quantile_digest.h"
#include "config.h"

#include <yt/yt/library/quantile_digest/proto/quantile_digest.pb.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/tdigest/tdigest.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TTDigestAdaptor
    : public IQuantileDigest
{
public:
    TTDigestAdaptor(double delta, double compressionFrequency)
        : UnderlyingDigest_(delta, compressionFrequency)
    { }

    explicit TTDigestAdaptor(const NProto::TTDigest& serialized)
        : UnderlyingDigest_(serialized.data())
    { }

    void AddValue(double value) override
    {
        UnderlyingDigest_.AddValue(value);
    }

    i64 GetCount() const override
    {
        return UnderlyingDigest_.GetCount();
    }

    double GetQuantile(double quantile) override
    {
        return UnderlyingDigest_.GetPercentile(quantile);
    }

    double GetRank(double value) override
    {
        return UnderlyingDigest_.GetRank(value);
    }

    TString Serialize() override
    {
        NProto::TQuantileDigest quantileDigest;

        auto* tDigest = quantileDigest.MutableExtension(NProto::TTDigest::t_digest);
        *tDigest->mutable_data() = UnderlyingDigest_.Serialize();

        return quantileDigest.SerializeAsString();
    }

private:
    TDigest UnderlyingDigest_;
};

////////////////////////////////////////////////////////////////////////////////

IQuantileDigestPtr CreateTDigest(const TTDigestConfigPtr& config)
{
    return New<TTDigestAdaptor>(config->Delta, config->CompressionFrequency);
}

////////////////////////////////////////////////////////////////////////////////

IQuantileDigestPtr LoadQuantileDigest(TStringBuf serialized)
{
    NProto::TQuantileDigest quantileDigest;
    if (!quantileDigest.ParseFromArray(serialized.begin(), serialized.Size())) {
        THROW_ERROR_EXCEPTION("Failed to parse quantile digest from proto");
    }

    if (quantileDigest.HasExtension(NProto::TTDigest::t_digest)) {
        const auto& protoTDigest = quantileDigest.GetExtension(NProto::TTDigest::t_digest);
        return New<TTDigestAdaptor>(protoTDigest);
    }

    THROW_ERROR_EXCEPTION("No suitable quantile digest extensions found");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
