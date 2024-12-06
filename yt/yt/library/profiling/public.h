#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/datetime/base.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct ISummaryBase;

using ISummary = ISummaryBase<double>;
using ITimer = ISummaryBase<TDuration>;

DECLARE_REFCOUNTED_TYPE(ISummary)
DECLARE_REFCOUNTED_TYPE(ITimer)

DECLARE_REFCOUNTED_STRUCT(ICounter)
DECLARE_REFCOUNTED_STRUCT(ITimeCounter)
DECLARE_REFCOUNTED_STRUCT(IGauge)
DECLARE_REFCOUNTED_STRUCT(ITimeGauge)
DECLARE_REFCOUNTED_STRUCT(IHistogram)

DECLARE_REFCOUNTED_STRUCT(IRegistry)
DECLARE_REFCOUNTED_STRUCT(ISensorProducer)

DECLARE_REFCOUNTED_CLASS(TBufferedProducer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
