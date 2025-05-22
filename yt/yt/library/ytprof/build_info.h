#pragma once

#include <util/generic/string.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TBuildInfo
{
    // Semantic version of current binary.
    //
    // Meaning of this field is application specific. Empty by default.
    TString BinaryVersion;

    // ArcRevision this binary was built from.
    TString ArcRevision;
    bool ArcDirty;

    // BuildType this binary was built with.
    TString BuildType;

    static TBuildInfo GetDefault();
};

bool IsProfileBuild();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
