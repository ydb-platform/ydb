#pragma once

#include <yt/yt/core/http/public.h>

#include <yt/yt/library/ytprof/build_info.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

//! Register profiling handlers.
void Register(
    const NHttp::IServerPtr& server,
    const TString& prefix,
    const TBuildInfo& buildInfo = TBuildInfo::GetDefault());

void Register(
    const NHttp::IRequestPathMatcherPtr& handlers,
    const TString& prefix,
    const TBuildInfo& buildInfo = TBuildInfo::GetDefault());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
