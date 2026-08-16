#include "build_info.h"

#include <library/cpp/svnversion/svnversion.h>

#include <algorithm>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TBuildInfo TBuildInfo::GetDefault()
{
    TBuildInfo buildInfo;

    buildInfo.BuildType = YTPROF_BUILD_TYPE;
    std::transform(buildInfo.BuildType.begin(), buildInfo.BuildType.end(), buildInfo.BuildType.begin(), ::tolower);

    if (GetVCS() == std::string_view{"arc"}) {
        buildInfo.ArcRevision = GetProgramCommitId();
        buildInfo.ArcLastChangeNum = GetArcadiaLastChangeNum();
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
