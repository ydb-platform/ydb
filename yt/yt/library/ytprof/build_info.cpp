#include "build_info.h"

#include <library/cpp/svnversion/svnversion.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TBuildInfo TBuildInfo::GetDefault()
{
    TBuildInfo buildInfo;

    buildInfo.BuildType = YTPROF_BUILD_TYPE;
    buildInfo.BuildType.to_lower(); // no shouting

    if (GetVCS() == TString{"arc"}) {
        buildInfo.ArcRevision = GetProgramCommitId();
    }

    return buildInfo;
}

bool IsProfileBuild()
{
#ifdef YTPROF_PROFILE_BUILD
    return true;
#else
    return false;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
