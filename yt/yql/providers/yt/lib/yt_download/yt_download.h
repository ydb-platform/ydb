#pragma once

#include <yt/yql/providers/yt/lib/config_clusters/config_clusters.h>

#include <yql/essentials/core/file_storage/defs/downloader.h>

namespace NYql {

class TFileStorageConfig;

NYql::NFS::IDownloaderPtr MakeYtDownloader(const TFileStorageConfig& config);
NYql::NFS::IDownloaderPtr MakeYtDownloader(const TFileStorageConfig& config, TConfigClusters::TPtr clusters);
NYql::NFS::IDownloaderPtr MakeYtDownloader(const TFileStorageConfig& config, const TString& defaultCluster);

} // NYql
