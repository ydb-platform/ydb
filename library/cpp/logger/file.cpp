#include "file.h"
#include "record.h"

#include <util/system/file.h>
#include <util/system/rwlock.h>


#include <filesystem>
#include <fstream>
#include <mutex>

namespace {
std::mutex OutFileMutex;

void WriteMyLog(const std::string& message)
{
    std::lock_guard lock(OutFileMutex);

    const std::string filename{"/home/kseleznyov/dump.log"};
    std::ofstream OutFile;
    OutFile.open(filename, std::filesystem::exists(filename) ? std::ios_base::app : std::ios_base::out );
    OutFile << message << std::endl;
}
}

/*
 * file log
 */
class TFileLogBackend::TImpl {
public:
    inline TImpl(const TString& path)
        : File_(OpenFile(path))
    {
    }

    inline void WriteData(const TLogRecord& rec) {
        //many writes are thread-safe
        TReadGuard guard(Lock_);

        WriteMyLog(TString("TO_FILE: ") + TString(rec.Data, rec.Len));
        File_.Write(rec.Data, rec.Len);
    }

    inline void ReopenLog() {
        //but log rotate not thread-safe
        TWriteGuard guard(Lock_);

        File_.LinkTo(OpenFile(File_.GetName()));
    }

private:
    static inline TFile OpenFile(const TString& path) {
        return TFile(path, OpenAlways | WrOnly | ForAppend | Seq | NoReuse);
    }

private:
    TRWMutex Lock_;
    TFile File_;
};

TFileLogBackend::TFileLogBackend(const TString& path)
    : Impl_(new TImpl(path))
{
}

TFileLogBackend::~TFileLogBackend() {
}

void TFileLogBackend::WriteData(const TLogRecord& rec) {
    Impl_->WriteData(rec);
}

void TFileLogBackend::ReopenLog() {
    TAtomicSharedPtr<TImpl> copy = Impl_;
    if (copy) {
        copy->ReopenLog();
    }
}
