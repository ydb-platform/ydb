#pragma once

#include <library/cpp/threading/future/async.h>
#include <util/folder/path.h>
#include <filesystem>
#include <iostream>
#include <limits>
#include <unordered_map>
#include <chrono>

#include <util/string/split.h>

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
#include <util/system/utime.h>
#include <util/system/thread.h>

#include <ydb/library/yql/core/spilling/interface/spilling.h>
#include <ydb/library/yql/core/spilling/storage/storage.h>


#include <ydb/library/yql/utils/log/log.h>


namespace NYql {
namespace NSpilling {

// Class to implement ISpillStorage interface based on file system storage
class FileSpillStorage: public ISpillStorage {
public:

    ui64 GetCurrSize() {return 0;} // Returns current size of spill storage
    // Returns full list of namespaces for current spill storage. Number of namespaces should be reasonable (no more than 1000)
    TVector<TString> GetNamespaces();

    // Returns list of file names for namespace
    TVector<TString> GetNamespaceFiles(const TString& ns);

    THolder<ISpillFile> CreateSpillFile(const TString& ns, const TString& fn, ui32 reserveStep);

    TOperationResults LastOperationResults();
    FileSpillStorage (const TFileStorageConfig& config);

private:
    TFsPath RootPath_;  // Root path to storage directory
    TOperationResults OperationResults_; // Last operation results

    bool RootPathExists(); // Returns true if root path exists for spill storage

};


// Class to implement ISpillFile interface based on file system files.
class FsSpillFile: public ISpillFile {
public:
    TString GetName() {return Name_;};
    bool IsLocked();
    ui64 Reserve(ui32 size);
    void Write(ui32 offset, const char * data, ui32 bytes);
    void Seek(ui32 offset);
    i32 Read(ui32 offset, char* buf, ui32 len);
    void Delete();
    FsSpillFile(const TString& name, EOpenMode oMode, ui32 reserveStep);
private:
    TString Name_;
    TFile File_;
    TFileLock FileLock_;
    bool Locked_ = false;
    std::atomic<ui64> TotalSpace_ = 0;
    std::atomic<ui64> ReservedSpace_ = 0;
    ui32 ReserveStep_ = 10000000; // File is incremented by 10 MB chunks 
};


TVector<TString> FileSpillStorage::GetNamespaces() {
    TVector<TString> res;
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    if (!RootPathExists()) {
        OperationResults_.Status = EOperationStatus::CannotOpenStorageProxy;
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        OperationResults_.ErrorString =  "Root directory for temp storage path: " + TString(RootPath_) + " does not exist";
        return res;
    }


    TVector<TFsPath> dirEntries;
    RootPath_.List(dirEntries);
    for (const auto & p: dirEntries) {
        if (p.IsDirectory() ) {
            res.push_back(p.GetName());
        }
    }

    OperationResults_.Status = EOperationStatus::Success;
    OperationResults_.ErrorString.clear();

    return res;
        
}

TVector<TString> FileSpillStorage::GetNamespaceFiles(const TString& ns) {
    TVector<TString> res;

    if (!RootPathExists()) {
        OperationResults_.Status = EOperationStatus::CannotOpenStorageProxy;
        OperationResults_.ErrorString =  "Root directory for temp storage path: " + TString(RootPath_) + " does not exist";
        return res;
    }

    TFsPath child = RootPath_.Child(ns);
    if ( !child.Exists()) {
        return res;
    }
    TVector<TFsPath> dirEntries;
    child.List(dirEntries);
    for (const auto & p: dirEntries) {
        TVector<TString> splitted;
        size_t tokenCount = Split(p.GetName(), ".", splitted);
        if (tokenCount >= 3 && tokenCount <= 4 && splitted[0] == "ydbspl" ) {
            ui64 ind = 0;
            if (TryFromString(splitted[1], ind)) {
            }
            res.push_back(p.GetName());
        }

    }

    OperationResults_.Status = EOperationStatus::Success;
    OperationResults_.ErrorString.clear();

    return res;
        
}

THolder<ISpillFile> FileSpillStorage::CreateSpillFile(const TString& ns, const TString& fn, ui32 reserveStep) {
    TFsPath nsdir = RootPath_.Child(ns);
    if (!nsdir.Exists()) {
        nsdir.MkDir();
    }
    TFsPath filePath = nsdir.Child(fn);
    return MakeHolder<FsSpillFile>(filePath.GetPath(), 
        EOpenModeFlag::OpenAlways | EOpenModeFlag::RdWr , reserveStep);
}


bool FileSpillStorage::RootPathExists() {
    if (RootPath_.IsDirectory()) {
        return true;
    } else {
        return false;
    }
}

TOperationResults FileSpillStorage::LastOperationResults() {
    return OperationResults_;
}

FileSpillStorage::FileSpillStorage (const TFileStorageConfig& config) {

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    RootPath_ = TFsPath(config.Path);

    if (!RootPathExists()) {
        OperationResults_.Status = EOperationStatus::CannotOpenStorageProxy;
        OperationResults_.ErrorString =  "Temp storage path: " + config.Path + " does not exist";
        return;
    }


    TVector<TFsPath> dirEntries;
    RootPath_.List(dirEntries);
    for (const auto & p: dirEntries) {
        TVector<TString> splitted;
        size_t tokenCount = Split(p.GetName(), ".", splitted);
        if (tokenCount == 3 && splitted[0] == "ydbspl" ) {
            ui64 ind = 0;
            if (TryFromString(splitted[1], ind)) {
            }
        }
    }

    OperationResults_.Status = EOperationStatus::Success;
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    OperationResults_.ErrorString.clear();



}


std::pair< THolder<ISpillStorage>, TOperationResults >  OpenFileStorageForSpilling(const TFileStorageConfig & config ) {
    THolder<FileSpillStorage> sp = MakeHolder<FileSpillStorage>(config);
    TOperationResults res = sp->LastOperationResults();
    return std::make_pair< THolder<ISpillStorage>, TOperationResults >( std::move(sp), std::move(res) );
}


bool FsSpillFile::IsLocked() {
    return Locked_;
}

ui64 FsSpillFile::Reserve(ui32 size) {
    ui64 offset = ReservedSpace_.fetch_add(size);
    while ( (offset + size) >= TotalSpace_ ) {
        File_.Resize(offset + size + ReserveStep_);
        TotalSpace_ = File_.GetLength();
    }

    return offset;
}

void FsSpillFile::Write(ui32 offset, const char * data, ui32 bytes) {
    File_.Pwrite(data, bytes, offset);
}

void FsSpillFile::Seek(ui32 offset) {
    File_.Seek(offset, SeekDir::sSet);
}

i32 FsSpillFile::Read(ui32 offset, char* buf, ui32 len) {
    return File_.Pread(buf, len, offset);
}

void FsSpillFile::Delete() {
    TFsPath path(Name_);
    YQL_LOG(INFO) << "Deleting: " << Name_ << Endl;
    path.ForceDelete();
}

FsSpillFile::FsSpillFile(const TString& name, EOpenMode oMode, ui32 reserveStep) :
    Name_(name),
    File_(name, oMode),
    FileLock_(name),
    ReserveStep_(reserveStep)
{
    Locked_ = FileLock_.TryAcquire();
    if (Locked_) {
        ui64 fsize = File_.GetLength();
        if (fsize >= std::numeric_limits<ui32>::max()) {

        }
        TotalSpace_ = (ui32) fsize;
        ReservedSpace_ = fsize;
    }
}


}
}