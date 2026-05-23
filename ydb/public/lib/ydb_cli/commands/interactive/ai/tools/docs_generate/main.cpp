#include <library/cpp/archive/yarchive.h>
#include <library/cpp/getopt/modchooser.h>

#include <util/generic/size_literals.h>
#include <util/folder/filelist.h>
#include <util/stream/buffered.h>
#include <util/stream/file.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TGenerateDocsMain final : public TMainClassArgs {
    static constexpr ui64 BUFFER_SIZE = 8_KB;

    void RegisterOptions(NLastGetopt::TOpts& options) final {
        options.SetTitle("Tool for generating ydb docs archive with *.md and toc*.yaml files");
        options.AddHelpOption('h');
        options.SetFreeArgsNum(2);

        options.AddFreeArgBinding("DOCS_PATH", DocsPath);
        options.AddFreeArgBinding("OUTPUT_PATH", OutputPath);
    }

    int DoRun(NLastGetopt::TOptsParseResult&&) final {
        TBuffered<TUnbufferedFileOutput> out(BUFFER_SIZE, OutputPath);
        TArchiveWriter archiveWriter(&out, /* compress */ true);
        CollectFiles(DocsPath, archiveWriter);
        archiveWriter.Finish();
        out.Finish();
        return 0;
    }

    static void CollectFiles(const TString& dir, TArchiveWriter& w, const TString& prefix = "") {
        {
            TFileList fl;
            fl.Fill(dir);

            const char* name = nullptr;
            while (name = fl.Next()) {
                TMappedFileInput in(TStringBuilder() << dir << name);
                w.Add(TStringBuilder() << prefix << name, &in);
            }
        }

        {
            TDirsList dl;
            dl.Fill(dir);

            const char* name = nullptr;
            while (name = dl.Next()) {
                if (strcmp(name, ".") && strcmp(name, "..")) {
                    CollectFiles(TStringBuilder() << dir << name << "/", w, TStringBuilder() << prefix << name << "/");
                }
            }
        }
    }

    TString DocsPath;
    TString OutputPath;
};

} // anonymous namespace

} // namespace NYdb::NConsoleClient::NAi

int main(int argc, const char* argv[]) {
    NYdb::NConsoleClient::NAi::TGenerateDocsMain main;
    return main.Run(argc, argv);
}
