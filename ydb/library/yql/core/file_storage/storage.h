#pragma once

#include <ydb/library/yql/core/file_storage/defs/provider.h>

#include <util/folder/path.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>

#include <functional>

namespace NYql {

class TFileLink: public TThrRefBase {
public:
    TFileLink(const TFsPath& path, const TString& storageFileName, ui64 size, const TString& md5, bool deleteOnDestroy = true);
    ~TFileLink();

    const TString& GetStorageFileName() const { return StorageFileName; }
    const TFsPath& GetPath() const { return Path; }
    ui64 GetSize() const { return Size; }
    const TString& GetMd5() const { return Md5; }

private:
    const TFsPath Path;
    const TString StorageFileName;
    const ui64 Size;
    const TString Md5;
    const bool DeleteOnDestroy;
};

using TFileLinkPtr = TIntrusivePtr<TFileLink>;

TFileLinkPtr CreateFakeFileLink(const TFsPath& path, const TString& md5 = "", bool deleteOnDestroy = false);

/* File cache with limits by count and by occupied size.
   The same file can be added multiple times.
   It is safe to add the same file from multiple threads/processes simultaneously.
   Only first thread will do the actual work. Other threads will wait and reuse the result.
*/
class TStorage {
public:
    /* Constructs the storage with the specified limits.
       storagePath can be empty - a temporary directory will be used in this case.
    */
    TStorage(size_t maxFiles, ui64 maxSize, const TString& storagePath = {});
    ~TStorage();

    // Returns root storage directory
    TFsPath GetRoot() const;
    // Returns temp storage directory
    TFsPath GetTemp() const;
    // Puts the passed data to the storage with the specified storage file name.
    // The second argument outFileName specifies a name of temporary link returned from the Put(). If empty, then random guid is used.
    // Provide valid md5 if it is known in advance, otherwise pass "". It will be overridden by puller result
    // The provided puller does the actual transfer of the data to a storage file. It can take a long time.
    TFileLinkPtr Put(const TString& storageFileName, const TString& outFileName, const TString& md5, const NYql::NFS::TDataProvider& puller);
    // Returns nullptr on error
    TFileLinkPtr HardlinkFromStorage(const TString& existingStorageFileName, const TString& storageFileMd5, const TString& outFileName);
    void MoveToStorage(const TFsPath& src, const TString& dstStorageFileName);
    bool RemoveFromStorage(const TString& existingStorageFileName);
    // Total size of all files in the cache
    ui64 GetOccupiedSize() const;
    // Total count of files in the cache
    size_t GetCount() const;
    TString GetTempName();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

constexpr int MODE0711 = S_IRWXU | S_IXGRP | S_IXOTH;
constexpr int MODE0744 = S_IRWXU | S_IRGRP | S_IROTH;

void SetCacheFilePermissions(const TString& path);
bool SetCacheFilePermissionsNoThrow(const TString& path);
void SetFilePermissions(const TString& path, int mode);
} // NYql
