#pragma once 
 
#include <ydb/library/yql/core/file_storage/file_storage.h> 
 
#include <vector> 
 
namespace NYql { 
 
class TFileStorageConfig; 
 
IFileStorage::IDownloaderPtr MakeHttpDownloader(bool restictedUser, const TFileStorageConfig& config, const std::vector<TString>& extraAllowedUrls); 
 
} // NYql 
