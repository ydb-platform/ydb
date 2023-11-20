#pragma once

#include <memory>
#include <optional>

#include <util/generic/string.h>

#include <yt/yt/library/ytprof/proto/profile.pb.h>

#include "build_info.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void Symbolize(NProto::Profile* profile, bool filesOnly = false);

void AddBuildInfo(NProto::Profile* profile, const TBuildInfo& buildInfo);

std::pair<void*, void*> GetVdsoRange();

// Returns current binary build id as binary string.
std::optional<TString> GetBuildId();

// Returns version of profiler library.
TString GetVersion();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
