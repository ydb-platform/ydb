#pragma once

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/public.h>

#include "public.h"

namespace NYT::NCoreDump {

////////////////////////////////////////////////////////////////////////////////

struct TCoreDump
{
    TString Path;
    TFuture<void> WrittenEvent;
};

////////////////////////////////////////////////////////////////////////////////

struct ICoreDumper
    : public virtual TRefCounted
{
    virtual TCoreDump WriteCoreDump(const std::vector<TString>& notes, const TString& reason) = 0;

    virtual const NYTree::IYPathServicePtr& CreateOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICoreDumper)
YT_DEFINE_TYPEID(ICoreDumper)

////////////////////////////////////////////////////////////////////////////////

ICoreDumperPtr CreateCoreDumper(TCoreDumperConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
