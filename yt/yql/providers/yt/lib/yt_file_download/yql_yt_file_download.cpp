#include "yql_yt_file_download.h"

#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/streams/brotli/brotli.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/stream/fwd.h>

namespace NYql {

ITableDownloaderFunc MakeYtNativeFileDownloader(
    IYtGateway::TPtr gateway,
    const TString& sessionId,
    const TString& cluster,
    TYtSettings::TConstPtr settings,
    NYT::IClientPtr client,
    TTempFiles::TPtr tmpFiles
) {
    return [gateway, sessionId, cluster, settings, client, tmpFiles] (const TTableDownloaderOptions& options) {
        YQL_ENSURE(client, "YT client should be set before attemping to download table");
        IYtGateway::TDownloadTableOptions downloadTableOptions(sessionId);
        downloadTableOptions.Cluster(cluster);
        downloadTableOptions.Config(settings);
        downloadTableOptions.ForceLocalTableContent(options.ForceLocalTableContent);
        downloadTableOptions.PublicId(options.PublicId);
        downloadTableOptions.StructColumns(options.StructColumns);
        downloadTableOptions.TmpFiles(tmpFiles);
        downloadTableOptions.DeliveryMode(options.DeliveryMode);

        TMaybe<IYtGateway::TDownloadTableOptions::TSamplingConfig> samplingConfig = Nothing();
        if (options.SamplingConfig) {
            samplingConfig = IYtGateway::TDownloadTableOptions::TSamplingConfig {
                .SamplingPercent = options.SamplingConfig->SamplingPercent,
                .SamplingSeed = options.SamplingConfig->SamplingSeed,
                .IsSystemSampling = options.SamplingConfig->IsSystemSampling
            };
        }
        downloadTableOptions.SamplingConfig(samplingConfig);

        TVector<IYtGateway::TDownloadTableOptions::TRemoteYtTable> tables;
        for (auto& table: options.Tables) {
            tables.emplace_back(
                IYtGateway::TDownloadTableOptions::TRemoteYtTable{
                    .RichPath = table.RichPath,
                    .TableOptions = IYtGateway::TDownloadTableOptions::TYtTableOptions{
                        .IsTemporary = table.TableOptions.IsTemporary,
                        .IsAnonymous = table.TableOptions.IsAnonymous,
                        .Epoch = table.TableOptions.Epoch
                    },
                    .Format = table.Format
                }
            );
        }
        downloadTableOptions.UniqueId(options.UniqueId);
        downloadTableOptions.Tables(tables);

        return gateway->DownloadTable(std::move(downloadTableOptions)).Apply([] (const auto& f) {
            auto downloadTableResult = f.GetValue();
            return NThreading::MakeFuture<TTableDownloaderResult>(TTableDownloaderResult{
                .RemoteFiles = downloadTableResult.RemoteFiles,
                .LocalFiles = downloadTableResult.LocalFiles,
            });
        });
    };
}

} // namespace NYql
