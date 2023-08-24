#include "symbolize.h"
#include "util/system/compiler.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void Symbolize(NProto::Profile* profile, bool filesOnly)
{
    Y_UNUSED(profile, filesOnly);
}

std::pair<void*, void*> GetVdsoRange()
{
    return {nullptr, nullptr};
}

TString GetVersion()
{
    return "0.2";
}

void AddBuildInfo(NProto::Profile* profile, const TBuildInfo& buildInfo)
{
    Y_UNUSED(profile, buildInfo);
}

std::optional<TString> GetBuildId()
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
