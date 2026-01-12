#include "fair_throttler_ipc.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/fs.h>

#include <util/folder/path.h>

#include <util/system/file_lock.h>
#include <util/system/filemap.h>
#include <util/system/mlock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr TStringBuf LockFileName = "lock";
constexpr TStringBuf SharedFileName = "shared.v1";
constexpr TStringBuf BucketsFileName = "buckets.v1";

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TFairThrottlerFileIpcBucket
    : public IFairThrottlerIpcBucket
{
public:
    TFairThrottlerFileIpcBucket(const std::string& path, bool create)
        // TODO(babenko): migrate to std::string
        : File_(TString(path), OpenAlways | RdWr)
    {
        if (create) {
            File_.Flock(LOCK_EX | LOCK_NB);
        }

        File_.Resize(sizeof(TState));

        Map_ = std::make_unique<TFileMap>(File_, TMemoryMapCommon::oRdWr);
        Map_->Map(0, Map_->Length());
        LockMemory(Map_->Ptr(), Map_->Length());
    }

    TState* GetState() override
    {
        return reinterpret_cast<TState*>(Map_->Ptr());
    }

private:
    TFile File_;
    std::unique_ptr<TFileMap> Map_;
};

////////////////////////////////////////////////////////////////////////////////

class TFairThrottlerFileIpc
    : public IFairThrottlerIpc
{
public:
    explicit TFairThrottlerFileIpc(const std::string& path)
        : Path_(path)
    {
        NFS::MakeDirRecursive(Path_);
        NFS::MakeDirRecursive(Path_ + "/" + BucketsFileName);

        Lock_ = std::make_unique<TFileLock>(Path_ + "/" + LockFileName);

        SharedBucketFile_ = TFile(Path_ + "/" + SharedFileName, OpenAlways | RdWr);
        SharedBucketFile_.Resize(sizeof(TSharedState));

        SharedBucketMap_ = std::make_unique<TFileMap>(SharedBucketFile_, TMemoryMapCommon::oRdWr);
        SharedBucketMap_->Map(0, SharedBucketMap_->Length());
        LockMemory(SharedBucketMap_->Ptr(), SharedBucketMap_->Length());
    }

    bool TryLock() override
    {
        try {
            SharedBucketFile_.Flock(LOCK_EX | LOCK_NB);
            return true;
        } catch (const TSystemError& ex) {
            if (ex.Status() != EWOULDBLOCK) {
                throw;
            }

            return false;
        }
    }

    TSharedState* GetState() override
    {
        return reinterpret_cast<TSharedState*>(SharedBucketMap_->Ptr());
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
        return New<TFairThrottlerFileIpcBucket>(bucketPath, /*create*/ true);
    }

private:
    // TODO(babenko): migrate to std::string
    const TString Path_;

    std::unique_ptr<TFileLock> Lock_;

    TFile SharedBucketFile_;
    std::unique_ptr<TFileMap> SharedBucketMap_;

    THashMap<std::string, IFairThrottlerIpcBucketPtr> OpenBuckets_;
    THashSet<std::string> OwnedBucketNames_;

    void Reload()
    {
        auto openBuckets = std::exchange(OpenBuckets_, {});

        TVector<TString> currentBucketPaths;
        TFsPath{Path_ + "/" + BucketsFileName}.ListNames(currentBucketPaths);
        for (const auto& fileName : currentBucketPaths) {
            if (OwnedBucketNames_.contains(fileName)) {
                continue;
            }

            try {
                auto bucketPath = GetBucketPath(fileName);
                TFileLock bucketLock(bucketPath);
                if (bucketLock.TryAcquire()) {
                    NFS::Remove(bucketPath);
                    continue;
                }

                if (auto it = openBuckets.find(fileName); it != openBuckets.end()) {
                    EmplaceOrCrash(OpenBuckets_, fileName, it->second);
                    continue;
                }

                auto bucket = New<TFairThrottlerFileIpcBucket>(bucketPath, /*create*/ false);
                EmplaceOrCrash(OpenBuckets_, fileName, bucket);
            } catch (const TSystemError& ex) {
                continue;
            }
        }
    }

    TString GetBucketPath(const std::string& fileName) const
    {
        return Path_ + "/" + BucketsFileName + "/" + fileName;
    }
};

////////////////////////////////////////////////////////////////////////////////

IFairThrottlerIpcPtr CreateFairThrottlerFileIpc(const std::string& path)
{
    return New<TFairThrottlerFileIpc>(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
