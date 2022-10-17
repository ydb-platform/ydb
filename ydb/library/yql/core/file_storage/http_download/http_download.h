#pragma once

#include <ydb/library/yql/core/file_storage/defs/downloader.h>

#include <vector>

namespace NYql {

class TFileStorageConfig;

NYql::NFS::IDownloaderPtr MakeHttpDownloader(const TFileStorageConfig& config);

} // NYql
