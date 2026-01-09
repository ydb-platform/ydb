#include "external_pprof.h"

#include "profile.h"

#include <yt/yt/core/misc/fs.h>

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
    TTempDir tmpDir = TTempDir::NewTempDir(std::string(options.TmpDir));
    if (options.KeepTmpDir) {
        tmpDir.DoNotRemove();
    }

    auto writeFile = [&] (const std::string& name) {
        auto path = tmpDir.Path() / name;
        TFile file{path, EOpenModeFlag::CreateAlways|EOpenModeFlag::WrOnly|EOpenModeFlag::AX|EOpenModeFlag::ARW};
        auto binary = NResource::Find("/ytprof/" + name);
        file.Write(binary.data(), binary.size());
        return TString(path);
    };

    std::string pprofPath;
    std::optional<std::string> llvmSymbolyzerPath;

    if (NResource::Has("/ytprof/pprof")) {
        pprofPath = writeFile("pprof");
    } else {
        pprofPath = "pprof";
    }

    if (NResource::Has("/ytprof/llvm-symbolizer")) {
        llvmSymbolyzerPath = writeFile("llvm-symbolizer");
    } else {
        llvmSymbolyzerPath = NFS::FindBinaryPath("llvm-symbolizer");
    }

    TFileOutput output(tmpDir.Path() / "in.pb.gz");
    WriteCompressedProfile(&output, *profile);
    output.Finish();

    std::vector<std::string> arguments{
        pprofPath,
        "-proto",
        "-output=" + (tmpDir.Path() / "out.pb.gz").GetPath(),
    };
    if (llvmSymbolyzerPath) {
        arguments.push_back("-tools=" + NFS::GetDirectoryName(*llvmSymbolyzerPath));
    }
    arguments.push_back((tmpDir.Path() / "in.pb.gz").GetPath());

    options.RunTool(arguments);

    TFileInput input(tmpDir.Path() / "out.pb.gz");
    ReadCompressedProfile(&input, profile);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
