#include <ydb/library/yql/core/file_storage/file_exporter.h>
#include <ydb/library/yql/core/file_storage/download_stream.h>
#include <ydb/library/yql/core/file_storage/storage.h>
#include <ydb/library/yql/utils/fetch/fetch.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/utils/multi_resource_lock.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <grpc++/grpc++.h>

#include <arc/api/public/repo.grpc.pb.h>

#include <util/system/shellcommand.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/string/strip.h>
#include <util/folder/dirut.h>

namespace NYql {

class TClient {
public:
    TClient(
        std::shared_ptr<grpc::Channel> channel)
        : Stub(NVcs::NPublic::FileService::NewStub(channel))
    {
    }

    TString ReadFile(const NVcs::NPublic::ReadFileRequest& request, const TString& path) {
        grpc::ClientContext context;
        auto reader = Stub->ReadFile(&context, request);
        NVcs::NPublic::ReadFileResponse response;
        TString ret;
        for (;;) {
            if (!reader->Read(&response)) {
                grpc::Status status = reader->Finish();
                if (!status.ok()) {
                    ythrow yexception() << "Failed to download url: " << path << ", status: " << status.error_message();
                }

                break;
            }

            if (response.HasData()) {
                ret += response.GetData();
            }
        }

        return ret;
    }
private:
    std::unique_ptr<NVcs::NPublic::FileService::Stub> Stub;
};

class TFileExporter : public IFileExporter {
public:
    std::pair<ui64, TString> ExportToFile(const TFileStorageConfig& config, const TString& convertedUrl, const THttpURL& url, const TString& dstFile) override {
        Y_UNUSED(convertedUrl);
        TCgiParameters params(url.GetField(NUri::TField::FieldQuery));
        auto hash = params.Get("hash");
        if (hash) {
            if (hash.length() != 40) {
                throw yexception() << "Only commit hash is expected, but got: " << hash;
            }

            if (!config.HasArcTokenPath()) {
                throw yexception() << "Missing Arc token";
            }

            auto path = config.GetArcTokenPath();
            if (path.StartsWith("~")) {
                path = GetHomeDir() + path.substr(1, path.Size() - 1);
            }

            TString authToken = Strip(TFileInput(path).ReadAll());

            std::shared_ptr<grpc::ChannelCredentials> credentials = grpc::CompositeChannelCredentials(
                grpc::SslCredentials(grpc::SslCredentialsOptions()),
                grpc::AccessTokenCredentials(authToken));
            TClient client(grpc::CreateChannel("api.arc-vcs.yandex-team.ru:6734", credentials));

            TString remoteFilePath = TStringBuilder() << url.GetField(NUri::TField::FieldHost) << url.GetField(NUri::TField::FieldPath);
            NVcs::NPublic::ReadFileRequest request;
            request.SetPath(remoteFilePath);
            request.SetRevision(hash);
            const auto content = client.ReadFile(request, remoteFilePath);
            TFileOutput dst(dstFile);
            dst.Write(content.Data(), content.Size());
            dst.Finish();

            i64 dstFileLen = GetFileLength(dstFile.c_str());
            if (dstFileLen == -1) {
                ythrow TSystemError() << "cannot get file length: " << dstFile;
            }

            YQL_ENSURE(static_cast<ui64>(dstFileLen) == content.Size());
        }
        else {
            auto branch = params.Get("branch");
            // support both operative and peg revision (see http://svnbook.red-bean.com/en/1.6/svn.advanced.pegrevs.html )
            // peg rev is enough usually
            auto pegRevStr = params.Get("rev");
            unsigned int pegRev = 0;
            if (!TryFromString(pegRevStr, pegRev) || pegRev == 0) {
                throw yexception() << "Revision for Arcadia file must be specified";
            }

            auto opRev = params.Get("op_rev");
            if (!branch) {
                branch = "trunk";
            }

            // in order to support @ in file names we have to add @ and optional peg revision
            // see https://stackoverflow.com/questions/757435/how-to-escape-characters-in-subversion-managed-file-names
            TStringBuilder fullUrlBuilder = TStringBuilder() << "svn+ssh://arcadia-ro.yandex.ru/arc/" << branch << "/arcadia/" <<
                url.GetField(NUri::TField::FieldHost) << url.GetField(NUri::TField::FieldPath) << "@" << pegRev;

            TStringBuilder configOptionBuilder = TStringBuilder() << "config:tunnels:ssh=ssh -F /dev/null -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ForwardAgent=true";

            if (config.GetArcSshPkPath()) {
                configOptionBuilder << " -i " << config.GetArcSshPkPath();
            }

            if (config.GetArcSshUser()) {
                configOptionBuilder << " -l " << config.GetArcSshUser();
            }

            TList<TString> args = { "export", fullUrlBuilder, dstFile, "--non-interactive", "--depth=empty", "--config-option", configOptionBuilder };
            if (opRev) {
                // equals to peg rev if not specified
                args.push_back("-r");
                args.push_back(opRev);
            }

            TShellCommandOptions shellOptions;
            shellOptions
                .SetUseShell(false) // disable shell for security reasons due to possible injections!
                .SetDetachSession(false);

            TShellCommand shell("svn", args, shellOptions);
            switch (shell.Run().GetStatus()) {
            case TShellCommand::SHELL_INTERNAL_ERROR:
                ythrow yexception() << "Export url internal error: "
                    << shell.GetInternalError();
            case TShellCommand::SHELL_ERROR:
                ythrow TDownloadError() << "Downloading url " << convertedUrl << " failed, reason: " << shell.GetError();
            case TShellCommand::SHELL_FINISHED:
                break;
            default:
                ythrow yexception() << "Unexpected state: " << int(shell.GetStatus());
            }
        }

        auto stat = TFileStat(dstFile, true); // do not follow symlinks
        if (stat.IsDir()) {
            RemoveDirWithContents(dstFile);
            ythrow yexception() << "Folders are not allowed";
        }

        if (stat.IsSymlink()) {
            NFs::Remove(dstFile);
            ythrow yexception() << "Symlinks are not allowed";
        }

        SetCacheFilePermissions(dstFile);

        const auto md5 = MD5::File(dstFile);
        return std::make_pair(stat.Size, md5);
    }
};

std::unique_ptr<IFileExporter> CreateFileExporter() {
    return std::make_unique<TFileExporter>();
}

}

