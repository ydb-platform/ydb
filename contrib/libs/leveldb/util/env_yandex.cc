#include "env.h"
#include "logging.h"
#include "slice.h"

#include <library/cpp/deprecated/mapped_file/mapped_file.h>
#include <library/cpp/logger/log.h>

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>
#include <util/folder/path.h>
#include <util/generic/deque.h>
#include <util/stream/str.h>
#include <util/string/printf.h>
#include <util/system/condvar.h>
#include <util/system/datetime.h>
#include <util/system/file.h>
#include <util/system/filemap.h>
#include <util/system/fs.h>
#include <util/system/info.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>

using std::string;
using std::vector;
using std::exception;

namespace leveldb {

namespace {

static Status IOError(const string& context) {
  return Status::IOError(context, LastSystemErrorText());
}

static Status IOError(const string& context, int err) {
  return Status::IOError(context, LastSystemErrorText(err));
}

static Status IOError(const string& context, const exception& e) {
  return Status::IOError(context, e.what());
}

// simple read() based sequential-access
// NOTE: file access is NOT buffered as LevelDB sources already contains enough read buffers
class YSequentialFile : public SequentialFile {
 public:
  YSequentialFile(const string& name)
    : name_(name)
    , handle_(name.c_str(), OpenExisting | RdOnly | Seq)
  {}

  bool IsOpen() const {
    return handle_.IsOpen();
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    i32 r = handle_.Read(scratch, n);
    if (r < 0) {
      return IOError("Unable to read " + name_);
    }
    *result = Slice(scratch, r);
    return Status::OK();
  }

  virtual Status Skip(uint64_t n) {
    i64 r = handle_.Seek(n, sCur);
    if (r < 0) {
      return IOError("Unable to seek " + name_);
    }
    return Status::OK();
  }

 private:
  string name_;
  TFileHandle handle_;
};

// simple write() based sequential-access
class YWritableFile : public WritableFile {
 public:
  YWritableFile(const string& name)
      : name_(name)
      , handle_(name.c_str(), CreateAlways | RdWr | Seq)
  {}

  bool IsOpen() const {
    return handle_.IsOpen();
  }

  virtual Status Append(const Slice& data) {
    i32 r = handle_.Write(data.data(), data.size());
    if (r < 0) {
      return IOError("Unable to write " + name_);
    }
    return Status::OK();
  }

  virtual Status Close() {
    if (!handle_.Close()) {
      return IOError("Error on close " + name_);
    }
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    if (!handle_.Flush()) {
      return IOError("Unable to fsync " + name_);
    }
    return Status::OK();
  }

 private:
  string name_;
  TFileHandle handle_;
};

// pread() based random-access
class YRandomAccessFile : public RandomAccessFile {
 public:
  YRandomAccessFile(const string& name)
    : name_(name)
    , handle_(name.c_str(), OpenExisting | RdOnly)
  {}

  bool IsOpen() const {
    return handle_.IsOpen();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
    i32 r = handle_.Pread(scratch, n, offset);
    if (r < 0) {
      return IOError("Unable to read " + name_);
    }
    *result = Slice(scratch, r);
    return Status::OK();
  }

 private:
  string name_;
  TFileHandle handle_;
};

// mmap() based random-access
class YMmapReadableFile : public RandomAccessFile {
 public:
  YMmapReadableFile(const string& name) // throws
    : name_(name)
    , map_(TFile(name.c_str(), OpenExisting | RdOnly), TFileMap::oRdOnly)
  {}

  virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
    Y_UNUSED(scratch);
    if (offset + n > map_.getSize()) {
      return IOError("Unable to read " + name_, EINVAL);
    }
    *result = Slice(reinterpret_cast<char*>(map_.getData(offset)), n);
    return Status::OK();
  }

 private:
  string name_;
  TMappedFile map_;
};

static const TFileMap::EOpenMode kMmapOpenMode = TFileMap::oRdWr | TFileMap::oNotGreedy;

// mmap() based for append-only access. See env_posix for comments.
class YMmapWritableFile : public WritableFile {
 public:
  YMmapWritableFile(const string& name, size_t page_size) // throws
    : name_(name)
    , file_(name.c_str(), CreateAlways | RdWr | Seq)
    , map_(new TFileMap(file_, kMmapOpenMode))
    , page_size_(page_size)
    , map_size_(Roundup(65536, page_size))
    , base_(NULL)
    , limit_(NULL)
    , dst_(NULL)
    , last_sync_(NULL)
    , file_offset_(0)
    , pending_sync_(false)
  {
    Y_ASSERT((page_size & (page_size - 1)) == 0);
  }

  ~YMmapWritableFile() {
    if (file_.IsOpen()) {
      YMmapWritableFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t left = data.size();
    while (left > 0) {
      Y_ASSERT(base_ <= dst_);
      Y_ASSERT(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
        Status s = UnmapCurrentRegion();
        if (s.ok())
          s = MapNewRegion();
        if (!s.ok())
          return s;
      }

      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
    }

    return Status::OK();
  }

  virtual Status Close() {
    size_t unused = limit_ - dst_;
    Status s = UnmapCurrentRegion();
    if (s.ok() && unused > 0) {
      // Trim the extra space at the end of the file
      try {
        file_.Resize(file_offset_ - unused);
      } catch (const exception& e) {
        s = IOError("Unable to resize " + name_, e);
      }
    }

    try {
      file_.Close();
    } catch (const exception& e) {
      s = IOError("Unable to close " + name_, e);
    }

    base_ = NULL;
    limit_ = NULL;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;

    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
      try {
        file_.FlushData();
      } catch (const exception& e) {
        s = IOError("Unable to fsync " + name_, e);
      }
    }

    if (dst_ > last_sync_) {
      // TODO: Find the beginnings of the pages that contain the first and last
      // bytes to be synced.
      //size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
      //size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
      last_sync_ = dst_;
      // if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
      //   s = IOError(name_);
      // }
      try {
        map_->Flush();
      } catch (const exception& e) {
        s = IOError("Unable to msync " + name_, e);
      }
    }

    return s;
  }

 private:
  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  Status MapNewRegion() {
    Y_ASSERT(base_ == NULL);

    try {
      file_.Resize(file_offset_ + map_size_);
    } catch (const exception& e) {
      return IOError("Unable to resize " + name_, e);
    }

    try {
      map_.Reset(new TFileMap(file_, kMmapOpenMode)); // TODO: need to reset TFileMap::Length
      map_->Map(file_offset_, map_size_);
    } catch (const exception& e) {
      return IOError("Unable to map " + name_, e);
    }

    base_ = reinterpret_cast<char*>(map_->Ptr());
    limit_ = base_ + map_size_;
    dst_ = base_;
    last_sync_ = base_;

    return Status::OK();
  }

  Status UnmapCurrentRegion() {
    if (base_ != NULL) {
      // Defer syncing this data until next Sync() call, if any
      if (last_sync_ < limit_) {
        pending_sync_ = true;
      }

      map_.Destroy();
      file_offset_ += limit_ - base_;
      base_ = NULL;
      limit_ = NULL;
      last_sync_ = NULL;
      dst_ = NULL;

      // Increase the amount we map the next time, but capped at 1MB
      if (map_size_ < (1<<20)) {
        map_size_ *= 2;
      }
    }

    return Status::OK();
  }

  string name_;           // File name
  TFile file_;            // File
  TAutoPtr<TFileMap> map_;// File mapping
  size_t page_size_;      // Size of the system page
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
  bool pending_sync_;     // Have we done an munmap of unsynced data?
};

class YLogger : public Logger {
 public:
  YLogger(const string& name)
    : log_(name.c_str())
  {}

  virtual void Logv(const char* format, va_list ap) {
    TString entry;
    vsprintf(entry, format, ap);
    if (!entry || entry.back() != '\n') {
        entry += '\n';
    }
    log_.Write(entry.data(), entry.size());
  }

 private:
  TLog log_;
};

class YFileLock : public FileLock {
 public:
  YFileLock(const string& name)
    : name_(name)
    , file_(name.c_str(), OpenAlways | RdWr)
  {}

  const string& Name() const {
    return name_;
  }

  void Lock() {
    file_.Flock(LOCK_EX | LOCK_NB);
  }

  void Unlock() {
   file_.Flock(LOCK_UN);
  }

 private:
  string name_;
  TFile file_;
};

class YEnv : public Env {
public:
  YEnv(bool mmap_disabled = false)
    : mmap_disabled_(mmap_disabled)
    , page_size_(NSystemInfo::GetPageSize())
    , scheduler_thread_(SchedulerProc, this)
    , stopped_(false)
  {}

  ~YEnv() {
    {
        TGuard<TMutex> guard(&scheduler_lock_);
        stopped_ = true;
        scheduler_signal_.Signal();
    }
    scheduler_thread_.Join();
  }

  /// File accessors

  virtual Status NewSequentialFile(const string& name, SequentialFile** result) {
    YSequentialFile* file = new YSequentialFile(name);
    if (!file->IsOpen()) {
      delete file;
      return IOError("Unable to open " + name);
    }
    *result = file;
    return Status::OK();
  }

  bool UseMmap() const {
    return !mmap_disabled_ && sizeof(void*) >= 8;
  }

  virtual Status NewRandomAccessFile(const string& name, RandomAccessFile** result) {
    if (UseMmap()) {
      try {
        *result = new YMmapReadableFile(name);
        return Status::OK();
      } catch (const exception& e) {
        return IOError("Unable to open " + name, e);
      }
    } else {
      YRandomAccessFile* file = new YRandomAccessFile(name);
      if (!file->IsOpen()) {
        delete file;
        return IOError("Unable to open " + name);
      }
      *result = file;
      return Status::OK();
    }
  }

  virtual Status NewWritableFile(const string& name, WritableFile** result) {
    if (UseMmap()) {
      try {
        *result = new YMmapWritableFile(name, page_size_);
        return Status::OK();
      } catch (const exception& e) {
          return IOError("Unable to open " + name, e);
      }
    } else {
      YWritableFile* file = new YWritableFile(name);
      if (!file->IsOpen()) {
        delete file;
        return IOError("Unable to open " + name);
      }
      *result = file;
      return Status::OK();
    }
  }

  /// FS operations

  virtual bool FileExists(const string& name) {
    try {
      TFsPath path(name.c_str());
      return path.Exists();
    } catch (const exception& e) {
      return false;
    }
  }
#ifdef DeleteFile
#undef DeleteFile
#endif
  virtual Status DeleteFile(const string& name) {
    if (!NFs::Remove(TString(name))) {
      return IOError("Unable to delete " + name);
    }
    return Status::OK();
  }

  virtual Status GetChildren(const string& dir, vector<string>* result) {
    try {
      result->clear();

      TFileList fileList;
      fileList.Fill(dir.c_str());

      const char* name;
      while ((name = fileList.Next()) != NULL) {
        result->push_back(name);
      }
    } catch (const exception& e) {
      return IOError("Unable to enumerate content " + dir, e);
    }
    return Status::OK();
  }

  virtual Status CreateDir(const string& dir) {
    try {
      TFsPath path(dir.c_str());
      path.MkDir();
    } catch (const exception& e) {
      return IOError("Unable to create " + dir, e);
    }
    return Status::OK();
  }

  virtual Status DeleteDir(const string& dir) {
    try {
      TFsPath path(dir.c_str());
      path.DeleteIfExists();
    } catch (const exception& e) {
      return IOError("Unable to delete " + dir, e);
    }
    return Status::OK();
  }

  virtual Status GetFileSize(const string& name, uint64_t* file_size) {
    try {
      TFileStat stat(name.c_str());
      *file_size = stat.Size;
    } catch (const exception& e) {
      return IOError("Unable to get stat " + name, e);
    }
    return Status::OK();
  }

  virtual Status RenameFile(const string& src, const string& target) {
    try {
      TFsPath path(src.c_str());
      path.RenameTo(target.c_str());
    } catch (const exception& e) {
      return IOError("Unable to rename " + src, e);
    }
    return Status::OK();
  }

  virtual Status LockFile(const string& name, FileLock** lock) {
    try {
      YFileLock* l = new YFileLock(name);
      l->Lock();
      *lock = l;
    } catch (const exception& e) {
      return IOError("Unable to lock " + name, e);
    }
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    THolder<YFileLock> l(reinterpret_cast<YFileLock*>(lock));
    try {
      l->Unlock();
    } catch (const exception& e) {
      return IOError("Unable to unlock " + l->Name(), e);
    }
    return Status::OK();
  }

  virtual Status GetTestDirectory(string* path) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *path = env;
    } else {
      TFsPath temp_path(GetSystemTempDir());
      TFsPath test_path(temp_path.Child("leveldbtest"));
      *path = test_path.GetPath();
    }
    // Directory may already exist
    CreateDir(*path);
    return Status::OK();
  }

  /// Support for logging

  virtual Status NewLogger(const string& name, Logger** result) {
    *result = new YLogger(name);
    return Status::OK();
  }

  /// Support for threads

  virtual void StartThread(void (*function)(void* arg), void* arg) {
    TThread thread(TaskProc, new ScheduledTask(function, arg));
    thread.Start();
    thread.Detach();
  }

  virtual void Schedule(void (*function)(void* arg), void* arg) {
    {
      TGuard<TMutex> guard(&scheduler_lock_);
      if (!scheduler_thread_.Running()) {
        scheduler_thread_.Start();
      }
      if (scheduled_tasks_.empty()) {
        scheduler_signal_.Signal();
      }
      scheduled_tasks_.push_back(ScheduledTask(function, arg));
    }
  }

  virtual uint64_t NowMicros() {
    return MicroSeconds();
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

 private:
  typedef void (*TFunction)(void* arg);

  struct ScheduledTask {
    inline ScheduledTask(TFunction function, void* arg)
      : Function(function)
      , Arg(arg)
    {
    }

    TFunction Function;
    void* Arg;
  };

  static void* TaskProc(void* arg) {
    ScheduledTask* task = reinterpret_cast<ScheduledTask*>(arg);
    task->Function(task->Arg);
    delete task;
    return NULL;
  }

  static void* SchedulerProc(void* arg) {
    reinterpret_cast<YEnv*>(arg)->SchedulerLoop();
    return NULL;
  }

  void SchedulerLoop() {
    while (true) {
      void (*function)(void*);
      void* arg;
      {
        TGuard<TMutex> guard(&scheduler_lock_);
        while (scheduled_tasks_.empty() && !stopped_) {
          scheduler_signal_.Wait(scheduler_lock_);
        }
        if (stopped_)
            return; // abort thread execution
        function = scheduled_tasks_.front().Function;
        arg = scheduled_tasks_.front().Arg;
        scheduled_tasks_.pop_front();
      }
      function(arg);
    }
  }

  bool mmap_disabled_;
  size_t page_size_;
  TDeque<ScheduledTask> scheduled_tasks_;
  TThread scheduler_thread_;
  TMutex scheduler_lock_;
  TCondVar scheduler_signal_;
  volatile bool stopped_;
};

}  // namespace

Env* Env::Default() {
  return Singleton<YEnv>();
}

Env* Env::New(bool mmap_disabled) {
  return new YEnv(mmap_disabled);
}

}  // namespace leveldb
