#pragma once

#include "public.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct ILogWriter
    : public virtual TRefCounted
{
    virtual void Write(const TLogEvent& event) = 0;
    virtual void Flush() = 0;

    virtual void Reload() = 0;

    virtual void SetRateLimit(std::optional<i64> limit) = 0;
    virtual void SetCategoryRateLimits(const THashMap<TString, i64>& categoryRateLimits) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILogWriter)

////////////////////////////////////////////////////////////////////////////////

struct IFileLogWriter
    : public virtual ILogWriter
{
    virtual const TString& GetFileName() const = 0;
    virtual void CheckSpace(i64 minSpace) = 0;
    virtual void MaybeRotate() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileLogWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
