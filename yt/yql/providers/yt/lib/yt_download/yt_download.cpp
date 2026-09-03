#include "yt_download.h"

#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yt/yql/providers/yt/lib/init_yt_api/init.h>
#include <yql/essentials/core/file_storage/defs/provider.h>

#include <yql/essentials/utils/md5_stream.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/string/cast.h>
#include <util/stream/input.h>
#include <util/stream/file.h>
#include <util/system/fstat.h>

namespace NYql {

class TYtDownloader: public NYql::NFS::IDownloader {
public:
    TYtDownloader(const TFileStorageConfig&, TConfigClusters::TPtr clusters)
        : Clusters_(std::move(clusters))
    {}

    TYtDownloader(const TFileStorageConfig&, const TString& defaultCluster)
        : DefaultCluster_(defaultCluster)
    {}

    ~TYtDownloader() = default;

    bool Accept(const THttpURL& url) final {
        const auto rawScheme = url.GetField(NUri::TField::FieldScheme);
        return NUri::EqualNoCase(rawScheme, "yt");
    }

    std::tuple<NYql::NFS::TDataProvider, TString, TString> Download(const THttpURL& url, const TString& oauthToken, const TString& oldEtag, const TString& /*oldLastModified*/) final {
        InitYtApiOnce();

        TCgiParameters params(url.GetField(NUri::TField::FieldQuery));
        NYT::TCreateClientOptions createOpts;
        if (oauthToken) {
            createOpts.Token(oauthToken);
        }
        auto clusterYtName = url.PrintS(NUri::TField::FlagHostPort);

        TString clusterServer = Clusters_ ? Clusters_->TryResolveServerByYtName(clusterYtName) : TString{};
        if (!clusterServer) {
            if (clusterYtName == "current") {
                if (!DefaultCluster_) {
                    throw yexception() << "Cannot download url: " << url.PrintS() << ", default cluster is not defined";
                }
                clusterServer = DefaultCluster_;
            } else {
                clusterServer = clusterYtName;
            }
        }
        auto path = params.Has("path") ? params.Get("path") : TString{TStringBuf(url.GetField(NUri::TField::FieldPath)).Skip(1)};

        auto client = NYT::CreateClient(clusterServer, createOpts);
        NYT::IClientBasePtr tx = client;
        TString txId = params.Get("transaction_id");
        if (!txId) {
            txId = params.Get("t");
        }
        YQL_LOG(INFO) << "YtDownload: clusterYtName=" << clusterYtName << ", clusterServer=" << clusterServer << ", path='" << path << "', tx=" << txId;

        if (txId) {
            TGUID g;
            if (!GetGuid(txId, g)) {
                throw yexception() << "Bad transaction ID: " << txId;
            }
            if (g) {
                tx = client->AttachTransaction(g);
            }
        }

        const NYT::TNode attrs = tx->Get(path + "/@", NYT::TGetOptions().AttributeFilter(
            NYT::TAttributeFilter()
                .AddAttribute(TString("revision"))
                .AddAttribute(TString("content_revision"))
        ));
        auto rev = ToString(GetContentRevision(attrs));
        if (oldEtag == rev) {
            YQL_LOG(INFO) << "YtDownload: clusterYtName=" << clusterYtName << ", clusterServer=" << clusterServer << ", path='" << path << "', tx=" << txId << " - same revision";
            return std::make_tuple(NYql::NFS::TDataProvider{}, TString{}, TString{});
        }

        auto puller = [tx = std::move(tx), path = std::move(path)](const TFsPath& dstFile) -> std::pair<ui64, TString> {
            auto reader = tx->CreateFileReader(path);

            TFile outFile(dstFile, CreateAlways | ARW | AX);
            TUnbufferedFileOutput out(outFile);
            TMd5OutputStream md5Out(out);

            const ui64 size = TransferData(reader.Get(), &md5Out);
            auto result = std::make_pair(size, md5Out.Finalize());
            out.Finish();
            outFile.Close();

            i64 dstFileLen = GetFileLength(dstFile.c_str());
            if (dstFileLen == -1) {
                ythrow TSystemError() << "cannot get file length: " << dstFile;
            }

            YQL_ENSURE(static_cast<ui64>(dstFileLen) == size);
            return result;
        };
        return std::make_tuple(puller, rev, TString{});
    }

private:
    const TConfigClusters::TPtr Clusters_;
    const TString DefaultCluster_;
};

NYql::NFS::IDownloaderPtr MakeYtDownloader(const TFileStorageConfig& config) {
    return MakeIntrusive<TYtDownloader>(config, TString{});
}

NYql::NFS::IDownloaderPtr MakeYtDownloader(const TFileStorageConfig& config, const TString& defaultCluster) {
    return MakeIntrusive<TYtDownloader>(config, defaultCluster);
}

NYql::NFS::IDownloaderPtr MakeYtDownloader(const TFileStorageConfig& config, TConfigClusters::TPtr clusters) {
    return MakeIntrusive<TYtDownloader>(config, std::move(clusters));
}

} // NYql
