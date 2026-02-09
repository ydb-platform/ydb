#include "fair_throttler_ipc.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/crypto/crypto.h>

#include <util/folder/path.h>

#include <util/system/file_lock.h>
#include <util/system/filemap.h>
#include <util/system/mlock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr TStringBuf LockFileName = "lock";
constexpr TStringBuf BucketLockFileNameSuffix = ".lock";
constexpr TStringBuf SharedFileName = "shared.v1";
constexpr TStringBuf BucketsDirName = "buckets.v1";

////////////////////////////////////////////////////////////////////////////////

std::string PathToShmPath(const std::string& path)
{
    return std::string("/dev/shm/") +  NCrypto::GetSha256HexDigestLowerCase(path);
}

std::unique_ptr<TFile> TryOpenFile(const std::string& path)
{
    // TODO(babenko): migrate to std::string
    TFileHandle handle(TString(path), OpenExisting | RdWr);
    if (!handle.IsOpen()) {
        if (LastSystemError() == ENOENT) {
            return nullptr;
        }
        THROW_ERROR_EXCEPTION("Error opening %v",
            path)
            << TError::FromSystem();
    }
    // TODO(babenko): migrate to std::string
    return std::make_unique<TFile>(handle.Release(), TString(path));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TSharedMemoryRegion
{
public:
    TSharedMemoryRegion(
        std::string path,
        bool useShmem)
        : Path_(std::move(path))
    {
        // Just create but don't lock yet.
        // TODO(babenko): migrate to std::string
        TFile lockFile(TString(GetLockPath()), OpenAlways | RdWr);

        if (useShmem) {
            auto shmPath = PathToShmPath(Path_);
            while (true) {
                DataFile_ = TryOpenFile(Path_);
                if (DataFile_) {
                    break;
                }
                ShmDataFile_ = std::make_unique<TFile>(shmPath, OpenAlways | RdWr);
                try {
                    NFS::MakeSymbolicLink(shmPath, Path_);
                } catch (...) {
                    if (LastSystemError() != EEXIST) {
                        throw;
                    }
                    // This could be a race; retry.
                    ShmDataFile_.reset();
                }
            }
        } else {
            // TODO(babenko): migrate to std::string
            DataFile_ = std::make_unique<TFile>(TString(Path_), OpenAlways | RdWr);
        }

        DataFile_->Resize(sizeof(T));
        Map_ = std::make_unique<TFileMap>(*DataFile_, TMemoryMapCommon::oRdWr);
        Map_->Map(0, Map_->Length());
        LockMemory(Map_->Ptr(), Map_->Length());
    }

    void Lock()
    {
        try {
            // TODO(babenko): migrate to std::string
            LockedDataFile_ = std::make_unique<TFile>(TString(Path_), OpenExisting | RdWr);
            LockedDataFile_->Flock(LOCK_EX | LOCK_NB);

            LockedLockFile_ = TryOpenFile(GetLockPath());
            if (LockedLockFile_) {
                LockedLockFile_->Flock(LOCK_EX | LOCK_NB);
            }
        } catch (...) {
            LockedDataFile_.reset();
            LockedLockFile_.reset();
            throw;
        }
    }

    bool TryLock()
    {
        try {
            Lock();
            return true;
        } catch (const TSystemError& ex) {
            if (ex.Status() != EWOULDBLOCK) {
                throw;
            }
            return false;
        }
    }

    T* Get()
    {
        return reinterpret_cast<T*>(Map_->Ptr());
    }

private:
    const std::string Path_;

    std::unique_ptr<TFile> DataFile_;
    std::unique_ptr<TFile> ShmDataFile_;
    std::unique_ptr<TFileMap> Map_;

    std::unique_ptr<TFile> LockedDataFile_;
    std::unique_ptr<TFile> LockedLockFile_;

    std::string GetLockPath() const
    {
        return Path_ + std::string(BucketLockFileNameSuffix);
    }
};

////////////////////////////////////////////////////////////////////////////////

bool TryRemoveOrphanedMemoryRegion(const std::string& path)
{
    enum EResult
    {
        Missing,
        StillLocked,
        Removed,
    };

    auto tryLockAndRemove = [&] (const std::string& path) {
        auto file = TryOpenFile(path);
        if (!file) {
            return EResult::Missing;
        }

        if (Flock(file->GetHandle(), LOCK_EX | LOCK_NB) != 0) {
            return EResult::StillLocked;
        }

        NFS::Remove(path);
        return EResult::Removed;
    };

    auto lockRemovalResult = tryLockAndRemove(path + std::string(BucketLockFileNameSuffix));
    if (lockRemovalResult == EResult::StillLocked) {
        return false;
    }

    auto dataRemovalResult = tryLockAndRemove(path);

#ifdef __linux__
    if (lockRemovalResult == EResult::Removed) {
        auto shmPath = PathToShmPath(path);
        try {
            NFS::Remove(shmPath);
        } catch (...) {
            // Ignore.
        }
    }
#endif

    return
        lockRemovalResult == EResult::Removed ||
        dataRemovalResult == EResult::Removed;
}

////////////////////////////////////////////////////////////////////////////////

class TFairThrottlerFileIpcBucket
    : public IFairThrottlerIpcBucket
{
public:
    TFairThrottlerFileIpcBucket(
        const std::string& path,
        bool create,
        bool useShmem)
        : Region_(path, useShmem)
    {
        if (create) {
            Region_.Lock();
        }
    }

    TState* GetState() override
    {
        return Region_.Get();
    }

private:
    TSharedMemoryRegion<TState> Region_;
};

////////////////////////////////////////////////////////////////////////////////

class TFairThrottlerFileIpc
    : public IFairThrottlerIpc
{
public:
    TFairThrottlerFileIpc(
        const std::string& rootPath,
        bool useShmem,
        NLogging::TLogger logger)
        : RootPath_(rootPath)
        , UseShmem_(useShmem)
        , Logger(std::move(logger))
    {
        NFS::MakeDirRecursive(RootPath_);
        NFS::MakeDirRecursive(RootPath_ + "/" + BucketsDirName);

        Lock_ = std::make_unique<TFileLock>(RootPath_ + "/" + LockFileName);

        Region_ = std::make_unique<TSharedMemoryRegion<TSharedState>>(
            RootPath_ + "/" + SharedFileName,
            UseShmem_);
    }

    bool TryLock() override
    {
        return Region_->TryLock();
    }

    TSharedState* GetState() override
    {
        return Region_->Get();
    }

    std::vector<IFairThrottlerIpcBucketPtr> ListBuckets() override
    {
        Lock_->Acquire();
        auto release = Finally([this] {
            Lock_->Release();
        });

        Reload();

        return GetValues(OpenBuckets_);
    }

    IFairThrottlerIpcBucketPtr CreateBucket() override
    {
        Lock_->Acquire();
        auto release = Finally([this] {
            Lock_->Release();
        });

        auto id = TGuid::Create();
        auto fileName = ToString(id);
        auto bucketPath = GetBucketPath(fileName);
        OwnedBucketNames_.insert(fileName);

        YT_LOG_DEBUG("Bucket created (Name: %v)", fileName);

        return New<TFairThrottlerFileIpcBucket>(
            bucketPath,
            /*create*/ true,
            UseShmem_);
    }

private:
    // TODO(babenko): migrate to std::string
    const TString RootPath_;
    const bool UseShmem_;
    const NLogging::TLogger Logger;

    std::unique_ptr<TFileLock> Lock_;
    std::unique_ptr<TSharedMemoryRegion<TSharedState>> Region_;

    THashMap<std::string, IFairThrottlerIpcBucketPtr> OpenBuckets_;
    THashSet<std::string> OwnedBucketNames_;

    void Reload()
    {
        auto openBuckets = std::exchange(OpenBuckets_, {});

        TVector<TString> currentBucketPaths;
        TFsPath{RootPath_ + "/" + BucketsDirName}.ListNames(currentBucketPaths);
        for (const auto& fileName : currentBucketPaths) {
            if (fileName.EndsWith(BucketLockFileNameSuffix)) {
                continue;
            }
            if (OwnedBucketNames_.contains(fileName)) {
                continue;
            }

            try {
                auto bucketPath = GetBucketPath(fileName);
                if (TryRemoveOrphanedMemoryRegion(bucketPath)) {
                    YT_LOG_DEBUG("Orphaned bucket removed (Name: %v)", fileName);
                    continue;
                }

                if (auto it = openBuckets.find(fileName); it != openBuckets.end()) {
                    EmplaceOrCrash(OpenBuckets_, fileName, it->second);
                    continue;
                }

                YT_LOG_DEBUG("Bucket found (Name: %v)", fileName);

                auto bucket = New<TFairThrottlerFileIpcBucket>(
                    bucketPath,
                    /*create*/ false,
                    UseShmem_);
                EmplaceOrCrash(OpenBuckets_, fileName, bucket);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error reloading throttler IPC; ignored");
                continue;
            }
        }
    }

    TString GetBucketPath(const std::string& fileName) const
    {
        return RootPath_ + "/" + BucketsDirName + "/" + fileName;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFairThrottlerIpcPtr CreateFairThrottlerFileIpc(
    const std::string& rootPath,
    bool useShmem,
    NLogging::TLogger logger)
{
    return New<TFairThrottlerFileIpc>(
        rootPath,
        useShmem,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
