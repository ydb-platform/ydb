#pragma once

#include <string>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TBuildInfo
{
    // Semantic version of current binary.
    //
    // Meaning of this field is application specific. Empty by default.
    std::string BinaryVersion;

    // ArcRevision this binary was built from.
    std::string ArcRevision;
    int ArcLastChangeNum;
    bool ArcDirty;

    // BuildType this binary was built with.
    std::string BuildType;

    static TBuildInfo GetDefault();
};

bool IsProfileBuild();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
