#pragma once

#include <optional>

#include <string>

#include <yt/yt/library/ytprof/proto/profile.pb.h>

#include "build_info.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TSymbolizeOptions
{
    //! Fill function names based on their addresses.
    //! Function's id is assumed to be an address somewhere inside the function.
    bool SymbolizeExistingFunctions = true;

    //! Add function definitions for all location addresses.
    bool SymbolizeLocations = false;
};

void Symbolize(NProto::Profile* profile, const TSymbolizeOptions& options = {});

void AddBuildInfo(NProto::Profile* profile, const TBuildInfo& buildInfo);

std::pair<void*, void*> GetVdsoRange();

// Returns current binary build id as binary string.
std::optional<std::string> GetBuildId();

// Returns version of profiler library.
std::string GetVersion();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
