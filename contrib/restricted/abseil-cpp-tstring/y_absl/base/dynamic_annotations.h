// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file defines dynamic annotations for use with dynamic analysis tool
// such as valgrind, PIN, etc.
//
// Dynamic annotation is a source code annotation that affects the generated
// code (that is, the annotation is not a comment). Each such annotation is
// attached to a particular instruction and/or to a particular object (address)
// in the program.
//
// The annotations that should be used by users are macros in all upper-case
// (e.g., Y_ABSL_ANNOTATE_THREAD_NAME).
//
// Actual implementation of these macros may differ depending on the dynamic
// analysis tool being used.
//
// This file supports the following configurations:
// - Dynamic Annotations enabled (with static thread-safety warnings disabled).
//   In this case, macros expand to functions implemented by Thread Sanitizer,
//   when building with TSan. When not provided an external implementation,
//   dynamic_annotations.cc provides no-op implementations.
//
// - Static Clang thread-safety warnings enabled.
//   When building with a Clang compiler that supports thread-safety warnings,
//   a subset of annotations can be statically-checked at compile-time. We
//   expand these macros to static-inline functions that can be analyzed for
//   thread-safety, but afterwards elided when building the final binary.
//
// - All annotations are disabled.
//   If neither Dynamic Annotations nor Clang thread-safety warnings are
//   enabled, then all annotation-macros expand to empty.

#ifndef Y_ABSL_BASE_DYNAMIC_ANNOTATIONS_H_
#define Y_ABSL_BASE_DYNAMIC_ANNOTATIONS_H_

#include <stddef.h>
#include <stdint.h>

#include "y_absl/base/attributes.h"
#include "y_absl/base/config.h"
#ifdef __cplusplus
#include "y_absl/base/macros.h"
#endif

#ifdef Y_ABSL_HAVE_HWADDRESS_SANITIZER
#include <sanitizer/hwasan_interface.h>
#endif

// TODO(rogeeff): Remove after the backward compatibility period.
#include "y_absl/base/internal/dynamic_annotations.h"  // IWYU pragma: export

// -------------------------------------------------------------------------
// Decide which features are enabled.

#ifdef Y_ABSL_HAVE_THREAD_SANITIZER

#define Y_ABSL_INTERNAL_RACE_ANNOTATIONS_ENABLED 1
#define Y_ABSL_INTERNAL_READS_ANNOTATIONS_ENABLED 1
#define Y_ABSL_INTERNAL_WRITES_ANNOTATIONS_ENABLED 1
#define Y_ABSL_INTERNAL_ANNOTALYSIS_ENABLED 0
#define Y_ABSL_INTERNAL_READS_WRITES_ANNOTATIONS_ENABLED 1

#else

#define Y_ABSL_INTERNAL_RACE_ANNOTATIONS_ENABLED 0
#define Y_ABSL_INTERNAL_READS_ANNOTATIONS_ENABLED 0
#define Y_ABSL_INTERNAL_WRITES_ANNOTATIONS_ENABLED 0

// Clang provides limited support for static thread-safety analysis through a
// feature called Annotalysis. We configure macro-definitions according to
// whether Annotalysis support is available. When running in opt-mode, GCC
// will issue a warning, if these attributes are compiled. Only include them
// when compiling using Clang.

#if defined(__clang__)
#define Y_ABSL_INTERNAL_ANNOTALYSIS_ENABLED 1
#if !defined(SWIG)
#define Y_ABSL_INTERNAL_IGNORE_READS_ATTRIBUTE_ENABLED 1
#endif
#else
#define Y_ABSL_INTERNAL_ANNOTALYSIS_ENABLED 0
#endif

// Read/write annotations are enabled in Annotalysis mode; disabled otherwise.
#define Y_ABSL_INTERNAL_READS_WRITES_ANNOTATIONS_ENABLED \
  Y_ABSL_INTERNAL_ANNOTALYSIS_ENABLED

#endif  // Y_ABSL_HAVE_THREAD_SANITIZER

#ifdef __cplusplus
#define Y_ABSL_INTERNAL_BEGIN_EXTERN_C extern "C" {
#define Y_ABSL_INTERNAL_END_EXTERN_C }  // extern "C"
#define Y_ABSL_INTERNAL_GLOBAL_SCOPED(F) ::F
#define Y_ABSL_INTERNAL_STATIC_INLINE inline
#else
#define Y_ABSL_INTERNAL_BEGIN_EXTERN_C  // empty
#define Y_ABSL_INTERNAL_END_EXTERN_C    // empty
#define Y_ABSL_INTERNAL_GLOBAL_SCOPED(F) F
#define Y_ABSL_INTERNAL_STATIC_INLINE static inline
#endif

// -------------------------------------------------------------------------
// Define race annotations.

#if Y_ABSL_INTERNAL_RACE_ANNOTATIONS_ENABLED == 1
// Some of the symbols used in this section (e.g. AnnotateBenignRaceSized) are
// defined by the compiler-based sanitizer implementation, not by the Abseil
// library. Therefore they do not use Y_ABSL_INTERNAL_C_SYMBOL.

// -------------------------------------------------------------
// Annotations that suppress errors. It is usually better to express the
// program's synchronization using the other annotations, but these can be used
// when all else fails.

// Report that we may have a benign race at `pointer`, with size
// "sizeof(*(pointer))". `pointer` must be a non-void* pointer. Insert at the
// point where `pointer` has been allocated, preferably close to the point
// where the race happens. See also Y_ABSL_ANNOTATE_BENIGN_RACE_STATIC.
#define Y_ABSL_ANNOTATE_BENIGN_RACE(pointer, description) \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateBenignRaceSized)  \
  (__FILE__, __LINE__, pointer, sizeof(*(pointer)), description)

// Same as Y_ABSL_ANNOTATE_BENIGN_RACE(`address`, `description`), but applies to
// the memory range [`address`, `address`+`size`).
#define Y_ABSL_ANNOTATE_BENIGN_RACE_SIZED(address, size, description) \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateBenignRaceSized)              \
  (__FILE__, __LINE__, address, size, description)

// Enable (`enable`!=0) or disable (`enable`==0) race detection for all threads.
// This annotation could be useful if you want to skip expensive race analysis
// during some period of program execution, e.g. during initialization.
#define Y_ABSL_ANNOTATE_ENABLE_RACE_DETECTION(enable)        \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateEnableRaceDetection) \
  (__FILE__, __LINE__, enable)

// -------------------------------------------------------------
// Annotations useful for debugging.

// Report the current thread `name` to a race detector.
#define Y_ABSL_ANNOTATE_THREAD_NAME(name) \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateThreadName)(__FILE__, __LINE__, name)

// -------------------------------------------------------------
// Annotations useful when implementing locks. They are not normally needed by
// modules that merely use locks. The `lock` argument is a pointer to the lock
// object.

// Report that a lock has been created at address `lock`.
#define Y_ABSL_ANNOTATE_RWLOCK_CREATE(lock) \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateRWLockCreate)(__FILE__, __LINE__, lock)

// Report that a linker initialized lock has been created at address `lock`.
#ifdef Y_ABSL_HAVE_THREAD_SANITIZER
#define Y_ABSL_ANNOTATE_RWLOCK_CREATE_STATIC(lock)          \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateRWLockCreateStatic) \
  (__FILE__, __LINE__, lock)
#else
#define Y_ABSL_ANNOTATE_RWLOCK_CREATE_STATIC(lock) \
  Y_ABSL_ANNOTATE_RWLOCK_CREATE(lock)
#endif

// Report that the lock at address `lock` is about to be destroyed.
#define Y_ABSL_ANNOTATE_RWLOCK_DESTROY(lock) \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateRWLockDestroy)(__FILE__, __LINE__, lock)

// Report that the lock at address `lock` has been acquired.
// `is_w`=1 for writer lock, `is_w`=0 for reader lock.
#define Y_ABSL_ANNOTATE_RWLOCK_ACQUIRED(lock, is_w)     \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateRWLockAcquired) \
  (__FILE__, __LINE__, lock, is_w)

// Report that the lock at address `lock` is about to be released.
// `is_w`=1 for writer lock, `is_w`=0 for reader lock.
#define Y_ABSL_ANNOTATE_RWLOCK_RELEASED(lock, is_w)     \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateRWLockReleased) \
  (__FILE__, __LINE__, lock, is_w)

// Apply Y_ABSL_ANNOTATE_BENIGN_RACE_SIZED to a static variable `static_var`.
#define Y_ABSL_ANNOTATE_BENIGN_RACE_STATIC(static_var, description)      \
  namespace {                                                          \
  class static_var##_annotator {                                       \
   public:                                                             \
    static_var##_annotator() {                                         \
      Y_ABSL_ANNOTATE_BENIGN_RACE_SIZED(&static_var, sizeof(static_var), \
                                      #static_var ": " description);   \
    }                                                                  \
  };                                                                   \
  static static_var##_annotator the##static_var##_annotator;           \
  }  // namespace

// Function prototypes of annotations provided by the compiler-based sanitizer
// implementation.
Y_ABSL_INTERNAL_BEGIN_EXTERN_C
void AnnotateRWLockCreate(const char* file, int line,
                          const volatile void* lock);
void AnnotateRWLockCreateStatic(const char* file, int line,
                                const volatile void* lock);
void AnnotateRWLockDestroy(const char* file, int line,
                           const volatile void* lock);
void AnnotateRWLockAcquired(const char* file, int line,
                            const volatile void* lock, long is_w);  // NOLINT
void AnnotateRWLockReleased(const char* file, int line,
                            const volatile void* lock, long is_w);  // NOLINT
void AnnotateBenignRace(const char* file, int line,
                        const volatile void* address, const char* description);
void AnnotateBenignRaceSized(const char* file, int line,
                             const volatile void* address, size_t size,
                             const char* description);
void AnnotateThreadName(const char* file, int line, const char* name);
void AnnotateEnableRaceDetection(const char* file, int line, int enable);
Y_ABSL_INTERNAL_END_EXTERN_C

#else  // Y_ABSL_INTERNAL_RACE_ANNOTATIONS_ENABLED == 0

#define Y_ABSL_ANNOTATE_RWLOCK_CREATE(lock)                            // empty
#define Y_ABSL_ANNOTATE_RWLOCK_CREATE_STATIC(lock)                     // empty
#define Y_ABSL_ANNOTATE_RWLOCK_DESTROY(lock)                           // empty
#define Y_ABSL_ANNOTATE_RWLOCK_ACQUIRED(lock, is_w)                    // empty
#define Y_ABSL_ANNOTATE_RWLOCK_RELEASED(lock, is_w)                    // empty
#define Y_ABSL_ANNOTATE_BENIGN_RACE(address, description)              // empty
#define Y_ABSL_ANNOTATE_BENIGN_RACE_SIZED(address, size, description)  // empty
#define Y_ABSL_ANNOTATE_THREAD_NAME(name)                              // empty
#define Y_ABSL_ANNOTATE_ENABLE_RACE_DETECTION(enable)                  // empty
#define Y_ABSL_ANNOTATE_BENIGN_RACE_STATIC(static_var, description)    // empty

#endif  // Y_ABSL_INTERNAL_RACE_ANNOTATIONS_ENABLED

// -------------------------------------------------------------------------
// Define memory annotations.

#ifdef Y_ABSL_HAVE_MEMORY_SANITIZER

#include <sanitizer/msan_interface.h>

#define Y_ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(address, size) \
  __msan_unpoison(address, size)

#define Y_ABSL_ANNOTATE_MEMORY_IS_UNINITIALIZED(address, size) \
  __msan_allocated_memory(address, size)

#else  // !defined(Y_ABSL_HAVE_MEMORY_SANITIZER)

#define Y_ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(address, size)    // empty
#define Y_ABSL_ANNOTATE_MEMORY_IS_UNINITIALIZED(address, size)  // empty

#endif  // Y_ABSL_HAVE_MEMORY_SANITIZER

// -------------------------------------------------------------------------
// Define IGNORE_READS_BEGIN/_END attributes.

#if defined(Y_ABSL_INTERNAL_IGNORE_READS_ATTRIBUTE_ENABLED)

#define Y_ABSL_INTERNAL_IGNORE_READS_BEGIN_ATTRIBUTE \
  __attribute((exclusive_lock_function("*")))
#define Y_ABSL_INTERNAL_IGNORE_READS_END_ATTRIBUTE \
  __attribute((unlock_function("*")))

#else  // !defined(Y_ABSL_INTERNAL_IGNORE_READS_ATTRIBUTE_ENABLED)

#define Y_ABSL_INTERNAL_IGNORE_READS_BEGIN_ATTRIBUTE  // empty
#define Y_ABSL_INTERNAL_IGNORE_READS_END_ATTRIBUTE    // empty

#endif  // defined(Y_ABSL_INTERNAL_IGNORE_READS_ATTRIBUTE_ENABLED)

// -------------------------------------------------------------------------
// Define IGNORE_READS_BEGIN/_END annotations.

#if Y_ABSL_INTERNAL_READS_ANNOTATIONS_ENABLED == 1
// Some of the symbols used in this section (e.g. AnnotateIgnoreReadsBegin) are
// defined by the compiler-based implementation, not by the Abseil
// library. Therefore they do not use Y_ABSL_INTERNAL_C_SYMBOL.

// Request the analysis tool to ignore all reads in the current thread until
// Y_ABSL_ANNOTATE_IGNORE_READS_END is called. Useful to ignore intentional racey
// reads, while still checking other reads and all writes.
// See also Y_ABSL_ANNOTATE_UNPROTECTED_READ.
#define Y_ABSL_ANNOTATE_IGNORE_READS_BEGIN()              \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateIgnoreReadsBegin) \
  (__FILE__, __LINE__)

// Stop ignoring reads.
#define Y_ABSL_ANNOTATE_IGNORE_READS_END()              \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateIgnoreReadsEnd) \
  (__FILE__, __LINE__)

// Function prototypes of annotations provided by the compiler-based sanitizer
// implementation.
Y_ABSL_INTERNAL_BEGIN_EXTERN_C
void AnnotateIgnoreReadsBegin(const char* file, int line)
    Y_ABSL_INTERNAL_IGNORE_READS_BEGIN_ATTRIBUTE;
void AnnotateIgnoreReadsEnd(const char* file,
                            int line) Y_ABSL_INTERNAL_IGNORE_READS_END_ATTRIBUTE;
Y_ABSL_INTERNAL_END_EXTERN_C

#elif defined(Y_ABSL_INTERNAL_ANNOTALYSIS_ENABLED)

// When Annotalysis is enabled without Dynamic Annotations, the use of
// static-inline functions allows the annotations to be read at compile-time,
// while still letting the compiler elide the functions from the final build.
//
// TODO(delesley) -- The exclusive lock here ignores writes as well, but
// allows IGNORE_READS_AND_WRITES to work properly.

#define Y_ABSL_ANNOTATE_IGNORE_READS_BEGIN()                          \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(                                      \
      Y_ABSL_INTERNAL_C_SYMBOL(AbslInternalAnnotateIgnoreReadsBegin)) \
  ()

#define Y_ABSL_ANNOTATE_IGNORE_READS_END()                          \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(                                    \
      Y_ABSL_INTERNAL_C_SYMBOL(AbslInternalAnnotateIgnoreReadsEnd)) \
  ()

Y_ABSL_INTERNAL_STATIC_INLINE void Y_ABSL_INTERNAL_C_SYMBOL(
    AbslInternalAnnotateIgnoreReadsBegin)()
    Y_ABSL_INTERNAL_IGNORE_READS_BEGIN_ATTRIBUTE {}

Y_ABSL_INTERNAL_STATIC_INLINE void Y_ABSL_INTERNAL_C_SYMBOL(
    AbslInternalAnnotateIgnoreReadsEnd)()
    Y_ABSL_INTERNAL_IGNORE_READS_END_ATTRIBUTE {}

#else

#define Y_ABSL_ANNOTATE_IGNORE_READS_BEGIN()  // empty
#define Y_ABSL_ANNOTATE_IGNORE_READS_END()    // empty

#endif

// -------------------------------------------------------------------------
// Define IGNORE_WRITES_BEGIN/_END annotations.

#if Y_ABSL_INTERNAL_WRITES_ANNOTATIONS_ENABLED == 1

// Similar to Y_ABSL_ANNOTATE_IGNORE_READS_BEGIN, but ignore writes instead.
#define Y_ABSL_ANNOTATE_IGNORE_WRITES_BEGIN() \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateIgnoreWritesBegin)(__FILE__, __LINE__)

// Stop ignoring writes.
#define Y_ABSL_ANNOTATE_IGNORE_WRITES_END() \
  Y_ABSL_INTERNAL_GLOBAL_SCOPED(AnnotateIgnoreWritesEnd)(__FILE__, __LINE__)

// Function prototypes of annotations provided by the compiler-based sanitizer
// implementation.
Y_ABSL_INTERNAL_BEGIN_EXTERN_C
void AnnotateIgnoreWritesBegin(const char* file, int line);
void AnnotateIgnoreWritesEnd(const char* file, int line);
Y_ABSL_INTERNAL_END_EXTERN_C

#else

#define Y_ABSL_ANNOTATE_IGNORE_WRITES_BEGIN()  // empty
#define Y_ABSL_ANNOTATE_IGNORE_WRITES_END()    // empty

#endif

// -------------------------------------------------------------------------
// Define the Y_ABSL_ANNOTATE_IGNORE_READS_AND_WRITES_* annotations using the more
// primitive annotations defined above.
//
//     Instead of doing
//        Y_ABSL_ANNOTATE_IGNORE_READS_BEGIN();
//        ... = x;
//        Y_ABSL_ANNOTATE_IGNORE_READS_END();
//     one can use
//        ... = Y_ABSL_ANNOTATE_UNPROTECTED_READ(x);

#if defined(Y_ABSL_INTERNAL_READS_WRITES_ANNOTATIONS_ENABLED)

// Start ignoring all memory accesses (both reads and writes).
#define Y_ABSL_ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN() \
  do {                                                \
    Y_ABSL_ANNOTATE_IGNORE_READS_BEGIN();               \
    Y_ABSL_ANNOTATE_IGNORE_WRITES_BEGIN();              \
  } while (0)

// Stop ignoring both reads and writes.
#define Y_ABSL_ANNOTATE_IGNORE_READS_AND_WRITES_END() \
  do {                                              \
    Y_ABSL_ANNOTATE_IGNORE_WRITES_END();              \
    Y_ABSL_ANNOTATE_IGNORE_READS_END();               \
  } while (0)

#ifdef __cplusplus
// Y_ABSL_ANNOTATE_UNPROTECTED_READ is the preferred way to annotate racey reads.
#define Y_ABSL_ANNOTATE_UNPROTECTED_READ(x) \
  y_absl::base_internal::AnnotateUnprotectedRead(x)

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace base_internal {

template <typename T>
inline T AnnotateUnprotectedRead(const volatile T& x) {  // NOLINT
  Y_ABSL_ANNOTATE_IGNORE_READS_BEGIN();
  T res = x;
  Y_ABSL_ANNOTATE_IGNORE_READS_END();
  return res;
}

}  // namespace base_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl
#endif

#else

#define Y_ABSL_ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN()  // empty
#define Y_ABSL_ANNOTATE_IGNORE_READS_AND_WRITES_END()    // empty
#define Y_ABSL_ANNOTATE_UNPROTECTED_READ(x) (x)

#endif

// -------------------------------------------------------------------------
// Address sanitizer annotations

#ifdef Y_ABSL_HAVE_ADDRESS_SANITIZER
// Describe the current state of a contiguous container such as e.g.
// std::vector or TString. For more details see
// sanitizer/common_interface_defs.h, which is provided by the compiler.
#include <sanitizer/common_interface_defs.h>

#define Y_ABSL_ANNOTATE_CONTIGUOUS_CONTAINER(beg, end, old_mid, new_mid) \
  __sanitizer_annotate_contiguous_container(beg, end, old_mid, new_mid)
#define Y_ABSL_ADDRESS_SANITIZER_REDZONE(name) \
  struct {                                   \
    alignas(8) char x[8];                    \
  } name

#else

#define Y_ABSL_ANNOTATE_CONTIGUOUS_CONTAINER(beg, end, old_mid, new_mid)  // empty
#define Y_ABSL_ADDRESS_SANITIZER_REDZONE(name) static_assert(true, "")

#endif  // Y_ABSL_HAVE_ADDRESS_SANITIZER

// -------------------------------------------------------------------------
// HWAddress sanitizer annotations

#ifdef __cplusplus
namespace y_absl {
#ifdef Y_ABSL_HAVE_HWADDRESS_SANITIZER
// Under HWASAN changes the tag of the pointer.
template <typename T>
T* HwasanTagPointer(T* ptr, uintptr_t tag) {
  return reinterpret_cast<T*>(__hwasan_tag_pointer(ptr, tag));
}
#else
template <typename T>
T* HwasanTagPointer(T* ptr, uintptr_t) {
  return ptr;
}
#endif
}  // namespace y_absl
#endif

// -------------------------------------------------------------------------
// Undefine the macros intended only for this file.

#undef Y_ABSL_INTERNAL_RACE_ANNOTATIONS_ENABLED
#undef Y_ABSL_INTERNAL_READS_ANNOTATIONS_ENABLED
#undef Y_ABSL_INTERNAL_WRITES_ANNOTATIONS_ENABLED
#undef Y_ABSL_INTERNAL_ANNOTALYSIS_ENABLED
#undef Y_ABSL_INTERNAL_READS_WRITES_ANNOTATIONS_ENABLED
#undef Y_ABSL_INTERNAL_BEGIN_EXTERN_C
#undef Y_ABSL_INTERNAL_END_EXTERN_C
#undef Y_ABSL_INTERNAL_STATIC_INLINE

#endif  // Y_ABSL_BASE_DYNAMIC_ANNOTATIONS_H_
