#include "external_pprof.h"

#include "profile.h"

#include <library/cpp/resource/resource.h>

#include <util/folder/tempdir.h>
#include <util/system/file.h>
#include <util/stream/file.h>

#include <util/generic/string.h>

#include <vector>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void SymbolizeByExternalPProf(NProto::Profile* profile, const TSymbolizationOptions& options)
{
    TTempDir tmpDir = TTempDir::NewTempDir(options.TmpDir);
    if (options.KeepTmpDir) {
        tmpDir.DoNotRemove();
    }

    auto pprofBinary = NResource::Find("/ytprof/pprof");
    auto llvmSymbolizerBinary = NResource::Find("/ytprof/llvm-symbolizer");

    auto writeFile = [&] (const TString& name) {
        TFile file{tmpDir.Path() / name, EOpenModeFlag::CreateAlways|EOpenModeFlag::WrOnly|EOpenModeFlag::AX|EOpenModeFlag::ARW};
        auto binary = NResource::Find("/ytprof/" + name);
        file.Write(binary.data(), binary.size());
    };

    writeFile("pprof");
    writeFile("llvm-symbolizer");

    TFileOutput output(tmpDir.Path() / "in.pb.gz");
    WriteProfile(&output, *profile);
    output.Finish();

    options.RunTool(std::vector<TString>{
        tmpDir.Path() / "pprof",
        "-proto",
        "-output=" + (tmpDir.Path() / "out.pb.gz").GetPath(),
        "-tools=" + tmpDir.Path().GetPath(),
        (tmpDir.Path() / "in.pb.gz").GetPath()
    });

    TFileInput input(tmpDir.Path() / "out.pb.gz");
    ReadProfile(&input, profile);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
