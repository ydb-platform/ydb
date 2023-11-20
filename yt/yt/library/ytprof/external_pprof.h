#pragma once

#include <yt/yt/library/ytprof/proto/profile.pb.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TSymbolizationOptions
{
    TString TmpDir = "/tmp";

    bool KeepTmpDir = false;

    std::function<void(const std::vector<TString>&)> RunTool;
};

void SymbolizeByExternalPProf(
    NProto::Profile* profile,
    const TSymbolizationOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
