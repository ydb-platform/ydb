#pragma once

#include <ydb/library/yql/core/file_storage/defs/downloader.h>

#include <vector>

namespace NYql {

class TFileStorageConfig;

NYql::NFS::IDownloaderPtr MakeHttpDownloader(bool restictedUser, const TFileStorageConfig& config, const std::vector<TString>& extraAllowedUrls);

} // NYql
