#pragma once

#include <ydb/library/yql/core/file_storage/defs/downloader.h>

namespace NYql {

class TFileStorageConfig;

NYql::NFS::IDownloaderPtr MakeYtDownloader(const TFileStorageConfig& config, const TString& defaultServer = {});

} // NYql
