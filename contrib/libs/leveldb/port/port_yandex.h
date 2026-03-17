#ifndef STORAGE_LEVELDB_PORT_PORT_YANDEX_H_
#define STORAGE_LEVELDB_PORT_PORT_YANDEX_H_

#include <string>

#include <util/system/defaults.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/generic/singleton.h>

#include <contrib/libs/snappy/snappy.h>

namespace leveldb {
namespace port {

#ifdef _little_endian_
static const bool kLittleEndian = true;
#else
static const bool kLittleEndian = false;
#endif

class Mutex {
 public:
  Mutex() {}
  void Lock() { mu_.Acquire(); }
  void Unlock() { mu_.Release(); };
  void AssertHeld() { }

 private:
  friend class CondVar;
  TMutex mu_;

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu)
    : mu_(mu) {}
  void Wait() { cv_.Wait(mu_->mu_); }
  void Signal() { cv_.Signal(); }
  void SignalAll() { cv_.BroadCast(); }

 private:
  TCondVar cv_;
  Mutex* mu_;
};

class AtomicPointer {
public:
  AtomicPointer() {
  }

  explicit AtomicPointer(void* p) {
    NoBarrier_Store(p);
  }

  inline void* NoBarrier_Load() const {
    return (void*)AtomicGet(rep_);
  }

  inline void NoBarrier_Store(void* v) {
    AtomicSet(rep_, (TAtomicBase)v);
  }

  inline void* Acquire_Load() const {
    void* result = NoBarrier_Load();
    ATOMIC_COMPILER_BARRIER();
    return result;
  }

  inline void Release_Store(void* v) {
    ATOMIC_COMPILER_BARRIER();
    NoBarrier_Store(v);
  }

private:
  TAtomic rep_;
};

inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
  return snappy::GetUncompressedLength(input, length, result);
}

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
  return snappy::RawUncompress(input, length, output);
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  Y_UNUSED(func);
  Y_UNUSED(arg);

  return false;
}

} // namespace port
} // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_YANDEX_H_
