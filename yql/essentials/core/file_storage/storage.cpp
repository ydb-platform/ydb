#include "storage.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/rand_guid.h>
#include <yql/essentials/utils/proc_alive.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/folder/dirut.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/generic/ptr.h>
#include <util/generic/utility.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>
#include <util/system/fs.h>
#include <util/system/maxlen.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/system/utime.h>

#include <functional>
#include <atomic>

#if defined(_unix_)
    #include <pthread.h>
#endif

#include <cerrno>

namespace NYql {

namespace {

constexpr const char CleanupLockFilename[] = ".cleanup_lock";

struct TFileObject {
    TString Name;
    time_t MTime;
    ui64 Size;
};

TFsPath ToFilePath(const TString& path)
{
    if (path.empty()) {
        char tempDir[MAX_PATH];
        if (MakeTempDir(tempDir, nullptr) != 0) {
            ythrow yexception() << "FileStorage: Can't create temporary directory " << tempDir;
        }
        return tempDir;
    }
    return path;
}

constexpr char FileLocksDir[] = "locks";

constexpr size_t MaxLockPathInStorage = 4096;
} // namespace

TFileLink::TFileLink(const TFsPath& path, const TString& storageFileName, ui64 size, const TString& md5, bool deleteOnDestroy)
    : Path_(path)
    , StorageFileName_(storageFileName)
    , Size_(size)
    , Md5_(md5)
    , DeleteOnDestroy_(deleteOnDestroy)
{
}

TFileLink::~TFileLink() {
    if (!DeleteOnDestroy_) {
        return;
    }

    YQL_LOG(INFO) << "Destroying TFileLink for " << Path_.GetPath().Quote();
    try {
        Path_.ForceDelete();
    } catch (...) {
        YQL_LOG(ERROR) << CurrentExceptionMessage();
    }
}

TFileLinkPtr CreateFakeFileLink(const TFsPath& path, const TString& md5, bool deleteOnDestroy) {
    if (!path.Exists()) {
        ythrow yexception() << "Unable to create file link for non-existent file " << path.GetPath().Quote();
    }

    TString effectiveMd5 = md5;
    if (!effectiveMd5) {
        effectiveMd5 = MD5::File(path);
    }
    const i64 size = GetFileLength(path);
    if (size == -1) {
        ythrow yexception() << "Unable to get size for file " << path.GetPath().Quote();
    }

    return new TFileLink(path, effectiveMd5, size, effectiveMd5, deleteOnDestroy);
}

bool SetCacheFilePermissionsNoThrow(const TString& path) {
    return Chmod(path.data(), MODE0755) == 0;
}

void SetCacheFilePermissions(const TString& path) {
    SetFilePermissions(path, MODE0755);
}

void SetFilePermissions(const TString& path, int mode) {
    if (Chmod(path.data(), mode)) {
        TFileStat fstat(path.data());
        ythrow TSystemError() << "Failed to chmod file " << path.Quote() << ", uid = " << fstat.Uid << ", mode = " << fstat.Mode << ", new mode = " << mode;
    }
}

class TStorage::TImpl: public TIntrusiveListItem<TImpl> {
public:
    class TAtforkReinit {
    public:
        inline TAtforkReinit() {
#if defined(_bionic_)
// no pthread_atfork on android libc
#elif defined(_unix_)
            pthread_atfork(nullptr, nullptr, ProcessReinit);
#endif
        }

        inline void Register(TImpl* obj) {
            auto guard = Guard(Mutex_);
            Registered_.PushBack(obj);
        }

        inline void Unregister(TImpl* obj) {
            auto guard = Guard(Mutex_);
            obj->Unlink();
        }

        static TAtforkReinit& Get() {
            return *SingletonWithPriority<TAtforkReinit, 256>();
        }

    private:
        void Reinit() {
            for (auto& v : Registered_) {
                v.ResetAtFork();
            }
        }

        static void ProcessReinit() {
            Get().Reinit();
        }

        TIntrusiveList<TImpl> Registered_;
        TMutex Mutex_;
    };

    TImpl(size_t maxFiles, ui64 maxSize, const TString& storagePath)
        : StorageDir_(ToFilePath(storagePath))
        , ProcessTempDir_(StorageDir_ / ToString(GetPID())) // must be subfolder for fast hardlinking
        , FileLocksDir_(StorageDir_ / ToString(FileLocksDir))
        , IsTemp_(storagePath.empty())
        , MaxFiles_(maxFiles)
        , MaxSize_(maxSize)
        , CurrentFiles_(0)
        , CurrentSize_(0)
        , Dirty_(false)
    {
        // TFsPath is not thread safe. It can initialize internal Split at any time. Force do it right now
        StorageDir_.PathSplit();
        ProcessTempDir_.PathSplit();
        FileLocksDir_.PathSplit();

        StorageDir_.MkDirs(MODE0711);
        ProcessTempDir_.MkDirs(MODE0711);
        FileLocksDir_.MkDirs(MODE0711);

        CleanupLock_ = THolder<TFileLock>(new TFileLock(FileLocksDir_ / CleanupLockFilename));

#ifdef _linux_
        ProcessTempDirLock_.Reset(new TFileLock(ProcessTempDir_ / ".lockfile"));
        ProcessTempDirLock_->Acquire();
        // We never explicitly release this lock. It will be released when all file handles (including those in child processes) will be closed
#endif

        if (!IsTemp_) {
            LoadStats();
        }
        TAtforkReinit::Get().Register(this);
        YQL_LOG(INFO) << "FileStorage initialized in " << StorageDir_.GetPath().Quote()
                      << ", temporary dir: " << ProcessTempDir_.GetPath().Quote()
                      << ", locks dir:" << FileLocksDir_.GetPath().Quote()
                      << ", files: " << CurrentFiles_.load()
                      << ", total size: " << CurrentSize_.load();
    }

    ~TImpl() {
        TAtforkReinit::Get().Unregister(this);
        try {
            ProcessTempDir_.ForceDelete();
            if (IsTemp_) {
                FileLocksDir_.ForceDelete();
                StorageDir_.ForceDelete();
            }
        } catch (...) {
            YQL_LOG(ERROR) << CurrentExceptionMessage();
        }
    }

    const TFsPath& GetRoot() const {
        return StorageDir_;
    }

    const TFsPath& GetTemp() const {
        return ProcessTempDir_;
    }

    TFsPath GetLockFilePath(const TString& componentName, const TString& lockName) const {
        return FileLocksDir_ / (componentName + "_" + ToString(THash<TString>{}(lockName) % MaxLockPathInStorage) + ".lockfile");
    }

    TFileLinkPtr Put(const TString& storageFileName, const TString& outFileName, const TString& md5, const NYql::NFS::TDataProvider& puller) {
        bool newFileAdded = false;
        TFileLinkPtr result = HardlinkFromStorage(storageFileName, md5, outFileName);
        if (!result) {
            TFsPath storageFile = StorageDir_ / storageFileName;
            TFsPath hardlinkFile = ProcessTempDir_ / (outFileName ? outFileName : GetTempName());
            Y_ENSURE(!hardlinkFile.Exists(), "FileStorage: temporary file " << hardlinkFile.GetPath().Quote() << " already exists");

            ui64 fileSize = 0;
            TString pullerMd5; // overrides input arg 'md5' when puller returns non-empty result
            try {
                std::tie(fileSize, pullerMd5) = puller(hardlinkFile);
            } catch (...) {
                YQL_LOG(ERROR) << CurrentExceptionMessage();
                NFs::Remove(hardlinkFile);
                throw;
            }
            Y_ENSURE(hardlinkFile.Exists(), "FileStorage: cannot put not existing temporary path");
            Y_ENSURE(hardlinkFile.IsFile(), "FileStorage: cannot put non-file temporary path");

            SetCacheFilePermissionsNoThrow(hardlinkFile);

            if (NFs::HardLink(hardlinkFile, storageFile)) {
                ++CurrentFiles_;
                CurrentSize_ += fileSize;
            }
            // Ignore HardLink fail. Another process managed to download before us
            TouchFile(storageFile.c_str());

            newFileAdded = true;
            if (pullerMd5.empty()) {
                pullerMd5 = md5;
            }
            result = MakeIntrusive<TFileLink>(hardlinkFile, storageFileName, fileSize, pullerMd5);
        }

        YQL_LOG(INFO) << "Using " << (newFileAdded ? "new" : "existing") << " storage file " << result->GetStorageFileName().Quote()
                      << ", temp path: " << result->GetPath().GetPath().Quote()
                      << ", size: " << result->GetSize();

        if (newFileAdded) {
            Cleanup();
        }
        return result;
    }

    TFileLinkPtr HardlinkFromStorage(const TString& existingStorageFileName, const TString& storageFileMd5, const TString& outFileName) {
        TFsPath storageFile = StorageDir_ / existingStorageFileName;
        TFsPath hardlinkFile = ProcessTempDir_ / (outFileName ? outFileName : GetTempName());
        Y_ENSURE(!hardlinkFile.Exists(), "FileStorage: temporary file " << hardlinkFile.GetPath().Quote() << " already exists");

        if (!NFs::HardLink(storageFile, hardlinkFile)) {
            return nullptr;
        }

        TouchFile(storageFile.c_str());
        SetCacheFilePermissionsNoThrow(hardlinkFile);

        const i64 fileSize = GetFileLength(hardlinkFile);
        if (fileSize < 0) {
            ythrow yexception() << "Unable to get size for file " << hardlinkFile.GetPath().Quote();
        }
        TString md5 = storageFileMd5;
        if (!md5) {
            // could happen rarely
            YQL_LOG(WARN) << "Rebuilding MD5 for file " << hardlinkFile.GetPath().Quote() << ", storage file " << existingStorageFileName << ". Usually it means file was downloaded via HTTP by another process and we just hardlinked it";
            md5 = MD5::File(hardlinkFile);
        }

        return new TFileLink(hardlinkFile, existingStorageFileName, fileSize, md5);
    }

    void MoveToStorage(const TFsPath& src, const TString& dstStorageFileName) {
        TFsPath dstStorageFile = StorageDir_ / dstStorageFileName;
        const bool prevFileExisted = dstStorageFile.Exists();
        const i64 prevFileSize = Max<i64>(0, GetFileLength(dstStorageFile.c_str()));

        if (!NFs::Rename(src, dstStorageFile)) {
            ythrow TSystemError() << "Failed to rename file from " << src << " to " << dstStorageFile;
        }
        SetCacheFilePermissionsNoThrow(dstStorageFile);

        const i64 newFileSize = Max<i64>(0, GetFileLength(dstStorageFile.c_str()));

        if (!prevFileExisted) {
            ++CurrentFiles_;
        }

        CurrentSize_ += newFileSize - prevFileSize;
    }

    bool RemoveFromStorage(const TString& existingStorageFileName) {
        TFsPath storageFile = StorageDir_ / existingStorageFileName;
        if (!storageFile.Exists()) {
            // can't update statistics
            // not sure we had this file at all
            return false;
        }

        // file could be removed by another process, handle this situation
        const i64 prevFileSize = Max<i64>(0, GetFileLength(storageFile.c_str()));
        const bool result = NFs::Remove(storageFile);

        if (result || !storageFile.Exists()) {
            --CurrentFiles_;
            CurrentSize_ -= prevFileSize;
        }

        return result;
    }

    ui64 GetOccupiedSize() const {
        return CurrentSize_.load();
    }

    size_t GetCount() const {
        return CurrentFiles_.load();
    }

    TString GetTempName() {
        with_lock (RndLock_) {
            return Rnd_.GenGuid();
        }
    }

private:
    void LoadStats() {
        TVector<TString> names;
        StorageDir_.ListNames(names);

        ui64 actualFiles = 0;
        ui64 actualSize = 0;

        ui32 oldPid;

        for (const TString& name : names) {
            TFsPath childPath(StorageDir_ / name);
            TFileStat stat(childPath, true);
            if (stat.IsFile()) {
                ++actualFiles;
                actualSize += stat.Size;
            } else if (stat.IsDir() && TryFromString(name, oldPid)) {
                if (!IsProcessAlive(oldPid)) {
                    // cleanup of previously not cleaned hardlinks directory
                    try {
#ifdef _linux_
                        TFileLock childLock(childPath / ".lockfile");
                        TTryGuard guard(childLock);
#else
                        bool guard = true;
#endif
                        if (guard) {
                            childPath.ForceDelete();
                        } else {
                            YQL_LOG(WARN) << "Not cleaning dead process dir " << childPath
                                          << ": " << "directory is still locked, skipping";
                        }
                    } catch (...) {
                        YQL_LOG(WARN) << "Error cleaning dead process dir " << childPath
                                      << ": " << CurrentExceptionMessage();
                    }
                }
            }
        }

        CurrentFiles_ = actualFiles;
        CurrentSize_ = actualSize;
    }

    bool NeedToCleanup() const {
        return Dirty_.load() || static_cast<ui64>(CurrentFiles_.load()) > MaxFiles_ || static_cast<ui64>(CurrentSize_.load()) > MaxSize_;
    }

    void Cleanup() {
        if (!NeedToCleanup()) {
            return;
        }

        TTryGuard guard(*CleanupLock_);

        if (!guard.WasAcquired()) {
            Dirty_.store(true);
            return;
        }

        Dirty_.store(false);

        TVector<TString> names;
        StorageDir_.ListNames(names);

        TVector<TFileObject> files;
        files.reserve(names.size());

        ui64 actualFiles = 0;
        ui64 actualSize = 0;

        for (const TString& name : names) {
            TFsPath childPath(StorageDir_ / name);
            TFileStat stat(childPath, true);
            if (stat.IsFile()) {
                files.push_back(TFileObject{name, stat.MTime, stat.Size});
                ++actualFiles;
                actualSize += stat.Size;
            }
        }

        if (actualFiles > MaxFiles_ || actualSize > MaxSize_) {
            // sort files to get older files first
            Sort(files, [](const TFileObject& f1, const TFileObject& f2) {
                if (f1.MTime == f2.MTime) {
                    return f1.Name.compare(f2.Name) < 0;
                }
                return f1.MTime < f2.MTime;
            });

            for (const TFileObject& f : files) {
                if (actualFiles <= MaxFiles_ && actualSize <= MaxSize_) {
                    break;
                }

                YQL_LOG(INFO) << "Removing file from cache (name: " << f.Name
                              << ", size: " << f.Size
                              << ", mtime: " << f.MTime << ")";
                if (!NFs::Remove(StorageDir_ / f.Name)) {
                    YQL_LOG(WARN) << "Failed to remove file " << f.Name.Quote() << ": " << LastSystemErrorText();
                } else {
                    --actualFiles;
                    Y_ENSURE(actualSize >= f.Size);
                    actualSize -= f.Size;
                }
            }
        }

        CurrentFiles_.store(actualFiles);
        CurrentSize_.store(actualSize);
    }

    void ResetAtFork() {
        RndLock_.Release();
        with_lock (RndLock_) {
            Rnd_.ResetSeed();
        }
        // Force cleanup on next file add, because other processes may change the state
        Dirty_.store(true);
    }

private:
    const TFsPath StorageDir_;
    const TFsPath ProcessTempDir_;
    const TFsPath FileLocksDir_;
    THolder<TFileLock> CleanupLock_;

    THolder<TFileLock> ProcessTempDirLock_;
    const bool IsTemp_;
    const ui64 MaxFiles_;
    const ui64 MaxSize_;
    std::atomic<i64> CurrentFiles_ = 0;
    std::atomic<i64> CurrentSize_ = 0;
    std::atomic_bool Dirty_;
    TSpinLock RndLock_;
    TRandGuid Rnd_;
};

TStorage::TStorage(size_t maxFiles, ui64 maxSize, const TString& storagePath)
    : Impl_(new TImpl(maxFiles, maxSize, storagePath))
{
}

TStorage::~TStorage()
{
}

TFsPath TStorage::GetRoot() const {
    return Impl_->GetRoot();
}

TFsPath TStorage::GetTemp() const {
    return Impl_->GetTemp();
}

TFsPath TStorage::GetLockFilePath(const TString& componentName, const TString& lockName) const {
    return Impl_->GetLockFilePath(componentName, lockName);
}

TFileLinkPtr TStorage::Put(const TString& storageFileName, const TString& outFileName, const TString& md5, const NFS::TDataProvider& puller)
{
    return Impl_->Put(storageFileName, outFileName, md5, puller);
}

TFileLinkPtr TStorage::HardlinkFromStorage(const TString& existingStorageFileName, const TString& storageFileMd5, const TString& outFileName)
{
    return Impl_->HardlinkFromStorage(existingStorageFileName, storageFileMd5, outFileName);
}

void TStorage::MoveToStorage(const TFsPath& src, const TString& dstStorageFileName)
{
    return Impl_->MoveToStorage(src, dstStorageFileName);
}

bool TStorage::RemoveFromStorage(const TString& existingStorageFileName)
{
    return Impl_->RemoveFromStorage(existingStorageFileName);
}

ui64 TStorage::GetOccupiedSize() const {
    return Impl_->GetOccupiedSize();
}

size_t TStorage::GetCount() const {
    return Impl_->GetCount();
}

TString TStorage::GetTempName()
{
    return Impl_->GetTempName();
}
} // namespace NYql
