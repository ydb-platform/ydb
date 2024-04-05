#include "file_cache.h"

#include <ydb/library/yql/utils/log/log.h>

#include <util/system/fs.h>
#include <util/system/file.h>
#include <util/folder/path.h>
#include <util/folder/iterator.h>

namespace NYql {

TFileCache::TFileCache(const TString& baseDir, i64 size)
    : BaseDir(baseDir)
    , TotalSize(size)
{
    NFs::MakeDirectoryRecursive(BaseDir, NFs::FP_NONSECRET_FILE, false);

    BaseDir = TFsPath(BaseDir).RealPath().GetPath();

    Scan();
}

void TFileCache::Scan()
{
    TDirIterator it(
        BaseDir,
        TDirIterator::TOptions()
            .SetMaxLevel(2));

    TVector<TFileObject> allFiles;
    for (const auto& item : it) {
        if (item.fts_level != 2) {
            continue;
        }
        TFsPath path = item.fts_path;
        if (!path.IsFile()) {
            continue;
        }
        auto split = path.PathSplit();

        TFileObject file;
        file.ObjectId = TString(split[split.size() - 2]) + TString(split[split.size() - 1]);
        file.Name = TString(split[split.size() - 1]);
#ifdef _linux_
        file.LastAccess = item.fts_statp->st_atim.tv_sec;
#endif
        file.Size = item.fts_statp->st_size;

        UsedSize += file.Size;

        YQL_CLOG(DEBUG, ProviderDq) << file.Name << "|" << file.ObjectId;

        allFiles.emplace_back(std::move(file));
    }

    std::sort(allFiles.begin(), allFiles.end(), [](const auto&a, const auto& b) {
        return a.LastAccess < b.LastAccess;
    });

    TGuard<TMutex> guard(Mutex);
    for (auto& file : allFiles) {
        LRU.push_back(file.ObjectId);
        file.Position = --LRU.end();
        Files.insert(std::make_pair(file.ObjectId, file));
    }

    Clean();
}

i64 TFileCache::FreeDiskSize() {
    return TotalSize - UsedSize;
}

ui64 TFileCache::UsedDiskSize() {
    return UsedSize;
}

void TFileCache::Clean() {
    while (UsedSize > TotalSize && LRU.size() > 2) {
        auto objectId = LRU.front();
        LRU.pop_front();

        auto maybeFile = Files.find(objectId);
        if (maybeFile != Files.end()) {
            auto path = GetDir(objectId) + "/" + maybeFile->second.Name;

            YQL_CLOG(DEBUG, ProviderDq) << "Remove File " << path << " UsedSize " << ToString(UsedSize) << " FileSize " << ToString(maybeFile->second.Size);

            if (maybeFile->second.UseCount == 0) {
                UsedSize -= maybeFile->second.Size;
                Files.erase(maybeFile);
            } else {
                maybeFile->second.Position = LRU.end();
            }
            NFs::Remove(path);
        }
    }
}

TString TFileCache::GetDir(const TString& md5) const
{
    return BaseDir + "/" + md5.substr(0, md5.length() / 2);
}

void TFileCache::AddFile(const TString& path, const TString& objectId)
{
    auto dir = GetDir(objectId);
    NFs::MakeDirectoryRecursive(dir, NFs::FP_NONSECRET_FILE, false);

    auto newBaseName = objectId.substr(objectId.length() / 2);
    auto newName = dir + "/" + newBaseName;
#ifndef _win_
    chmod(path.c_str(), 0755);
#endif
    TFileObject file;
    file.LastAccess = 0; // unused
    file.Size = TFile(path, RdOnly).GetLength();
    file.Name = newBaseName;
    file.ObjectId = objectId;

    {
        TGuard<TMutex> guard(Mutex);
        LRU.push_back(objectId);
        file.Position = --LRU.end();
        if (!Files.contains(objectId)) {
            UsedSize += file.Size;
        }
        Files[objectId] = file;
    }

    // don't lock on fs
    NFs::Rename(path, newName);

    TGuard<TMutex> guard(Mutex);
    Clean();

    YQL_CLOG(DEBUG, ProviderDq) << "FreeDiskSize/UsedDiskSize " << FreeDiskSize() << "/" << UsedDiskSize();
}

THashMap<TString, TFileCache::TFileObject>::iterator TFileCache::FindFileObject(const TString& objectId)
{
    auto maybeFile = Files.find(objectId);
    if (maybeFile == Files.end() || maybeFile->second.Position == LRU.end()) {
        return Files.end();
    }
    LRU.erase(maybeFile->second.Position);
    LRU.push_back(objectId);
    maybeFile->second.Position = --LRU.end();
    return maybeFile;
}

TMaybe<TString> TFileCache::FindFile(const TString& objectId)
{
    TString fileName;
    {
        TGuard<TMutex> guard(Mutex);
        auto maybeFile = FindFileObject(objectId);
        if (maybeFile == Files.end()) {
            return { };
        }
        fileName = maybeFile->second.Name;
    }

    return {GetDir(objectId) + "/" + fileName};
}

TMaybe<TString> TFileCache::AcquireFile(const TString& objectId)
{
    TString fileName;
    {
        TGuard<TMutex> guard(Mutex);
        auto maybeFile = FindFileObject(objectId);
        if (maybeFile == Files.end()) {
            return { };
        }
        maybeFile->second.UseCount ++;
        fileName = maybeFile->second.Name;
    }

    return {GetDir(objectId) + "/" + fileName};
}

void TFileCache::ReleaseFile(const TString& objectId) {
    TGuard<TMutex> guard(Mutex);
    auto maybeFile = Files.find(objectId);
    if (maybeFile != Files.end()) {
        maybeFile->second.UseCount = std::max((i64)0, maybeFile->second.UseCount-1);
        if (maybeFile->second.UseCount == 0 && maybeFile->second.Position == LRU.end()) {
            UsedSize -= maybeFile->second.Size;
            Files.erase(maybeFile);
        }
    }
}

bool TFileCache::Contains(const TString& objectId)
{
    TGuard<TMutex> guard(Mutex);
    return FindFileObject(objectId) != Files.end();
}

void TFileCache::Walk(const std::function<void(const TString& objectId)>& f)
{
    TGuard<TMutex> guard(Mutex);
    for (const auto& objectId : LRU) {
        f(objectId);
    }
}

} // namespace NYql

