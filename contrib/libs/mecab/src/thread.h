//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_THREAD_H
#define MECAB_THREAD_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_PTHREAD_H
#include <pthread.h>
#else
#ifdef _WIN32
#include <windows.h>
#include <process.h>
#endif
#endif

#if defined HAVE_GCC_ATOMIC_OPS || defined HAVE_OSX_ATOMIC_OPS
#include <sched.h>
#endif

#if defined HAVE_OSX_ATOMIC_OPS
#include <libkern/OSAtomic.h>
#endif

#if defined HAVE_PTHREAD_H
#define MECAB_USE_THREAD 1
#endif

#if (defined(_WIN32) && !defined(__CYGWIN__))
#define MECAB_USE_THREAD 1
#define BEGINTHREAD(src, stack, func, arg, flag, id)                    \
  (HANDLE)_beginthreadex((void *)(src), (unsigned)(stack),              \
                         (unsigned(_stdcall *)(void *))(func), (void *)(arg), \
                         (unsigned)(flag), (unsigned *)(id))
#endif

namespace MeCab {

#if (defined(_WIN32) && !defined(__CYGWIN__))
#undef atomic_add
#undef compare_and_swap
#undef yield_processor
#define atomic_add(a, b) ::InterlockedExchangeAdd(a, b)
#define compare_and_swap(a, b, c)  ::InterlockedCompareExchange(a, c, b)
#define yield_processor() YieldProcessor()
#define HAVE_ATOMIC_OPS 1
#endif

#ifdef HAVE_GCC_ATOMIC_OPS
#undef atomic_add
#undef compare_and_swap
#undef yield_processor
#define atomic_add(a, b) __sync_add_and_fetch(a, b)
#define compare_and_swap(a, b, c)  __sync_val_compare_and_swap(a, b, c)
#define yield_processor() sched_yield()
#define HAVE_ATOMIC_OPS 1
#endif

#ifdef HAVE_OSX_ATOMIC_OPS
#undef atomic_add
#undef compare_and_swap
#undef yield_processor
#define atomic_add(a, b) OSAtomicAdd32(b, a)
#define compare_and_swap(a, b, c) OSAtomicCompareAndSwapInt(b, c, a)
#define yield_processor() sched_yield()
#define HAVE_ATOMIC_OPS 1
#endif

#ifdef HAVE_ATOMIC_OPS
// This is a simple non-scalable writer-preference lock.
// Slightly modified the following paper.
// "Scalable Reader-Writer Synchronization for Shared-Memory Multiprocessors"
// PPoPP '91. John M. Mellor-Crummey and Michael L. Scott. T
class read_write_mutex {
 public:
  inline void write_lock() {
    atomic_add(&write_pending_, 1);
    while (compare_and_swap(&l_, 0, kWaFlag)) {
      yield_processor();
    }
  }
  inline void read_lock() {
    while (write_pending_ > 0) {
      yield_processor();
    }
    atomic_add(&l_, kRcIncr);
    while ((l_ & kWaFlag) != 0) {
      yield_processor();
    }
  }
  inline void write_unlock() {
    atomic_add(&l_, -kWaFlag);
    atomic_add(&write_pending_, -1);
  }
  inline void read_unlock() {
    atomic_add(&l_, -kRcIncr);
  }

  read_write_mutex(): l_(0), write_pending_(0) {}

 private:
  static const int kWaFlag = 0x1;
  static const int kRcIncr = 0x2;
#ifdef HAVE_OSX_ATOMIC_OPS
  volatile int l_;
  volatile int write_pending_;
#else
  long l_;
  long write_pending_;
#endif
};

class scoped_writer_lock {
 public:
  scoped_writer_lock(read_write_mutex *mutex) : mutex_(mutex) {
    mutex_->write_lock();
  }
  ~scoped_writer_lock() {
    mutex_->write_unlock();
  }
 private:
  read_write_mutex *mutex_;
};

class scoped_reader_lock {
 public:
  scoped_reader_lock(read_write_mutex *mutex) : mutex_(mutex) {
    mutex_->read_lock();
  }
  ~scoped_reader_lock() {
    mutex_->read_unlock();
  }
 private:
  read_write_mutex *mutex_;
};
#endif  // HAVE_ATOMIC_OPS

class thread {
 private:
#ifdef HAVE_PTHREAD_H
  pthread_t hnd;
#else
#ifdef _WIN32
  HANDLE  hnd;
#endif
#endif

 public:
  static void* wrapper(void *ptr) {
    thread *p = static_cast<thread *>(ptr);
    p->run();
    return 0;
  }

  virtual void run() {}

  void start() {
#ifdef HAVE_PTHREAD_H
    pthread_create(&hnd, 0, &thread::wrapper,
                   static_cast<void *>(this));

#else
#ifdef _WIN32
    DWORD id;
    hnd = BEGINTHREAD(0, 0, &thread::wrapper, this, 0, &id);
#endif
#endif
  }

  void join() {
#ifdef HAVE_PTHREAD_H
    pthread_join(hnd, 0);
#else
#ifdef _WIN32
    WaitForSingleObject(hnd, INFINITE);
    CloseHandle(hnd);
#endif
#endif
  }

  virtual ~thread() {}
};
}
#endif
