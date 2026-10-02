#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/fwd.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRequestRetryPolicy;
using IRequestRetryPolicyPtr = ::TIntrusivePtr<IRequestRetryPolicy>;

class IClientRetryPolicy;
using IClientRetryPolicyPtr = ::TIntrusivePtr<IClientRetryPolicy>;

////////////////////////////////////////////////////////////////////////////////

namespace NTracing {

struct TTraceContextWrapper;
using TTraceContextWrapperPtr = std::shared_ptr<TTraceContextWrapper>;

TTraceContextWrapperPtr CreateTraceContext(const std::string& name, const TConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing

} // namespace NYT
