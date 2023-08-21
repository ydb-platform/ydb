#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IQuantileDigest
    : public TRefCounted
{
    virtual void AddValue(double value) = 0;

    virtual i64 GetCount() const = 0;

    virtual double GetQuantile(double quantile) = 0;

    virtual double GetRank(double value) = 0;

    virtual TString Serialize() = 0;
};

DEFINE_REFCOUNTED_TYPE(IQuantileDigest)

////////////////////////////////////////////////////////////////////////////////

IQuantileDigestPtr CreateTDigest(const TTDigestConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

IQuantileDigestPtr LoadQuantileDigest(TStringBuf serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
