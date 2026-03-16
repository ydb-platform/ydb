// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_RETAIN_PTR_H_
#define CORE_FXCRT_RETAIN_PTR_H_

#include <stdint.h>

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"

namespace fxcrt {

// Used with std::unique_ptr to Release() objects that can't be deleted.
template <class T>
struct ReleaseDeleter {
  inline void operator()(T* ptr) const { ptr->Release(); }
};

// Analogous to base's scoped_refptr.
template <class T>
class TRIVIAL_ABI RetainPtr {
 public:
  RetainPtr() noexcept = default;

  // Deliberately implicit to allow returning nullptrs.
  // NOLINTNEXTLINE(runtime/explicit)
  RetainPtr(std::nullptr_t ptr) {}

  explicit RetainPtr(T* pObj) noexcept : m_pObj(pObj) {
    if (m_pObj)
      m_pObj->Retain();
  }

  // Copy-construct a RetainPtr.
  // Required in addition to copy conversion constructor below.
  RetainPtr(const RetainPtr& that) noexcept : RetainPtr(that.Get()) {}

  // Move-construct a RetainPtr. After construction, |that| will be NULL.
  // Required in addition to move conversion constructor below.
  RetainPtr(RetainPtr&& that) noexcept { Unleak(that.Leak()); }

  // Copy conversion constructor.
  template <class U,
            typename = typename std::enable_if<
                std::is_convertible<U*, T*>::value>::type>
  RetainPtr(const RetainPtr<U>& that) : RetainPtr(that.Get()) {}

  // Move-conversion constructor.
  template <class U,
            typename = typename std::enable_if<
                std::is_convertible<U*, T*>::value>::type>
  RetainPtr(RetainPtr<U>&& that) noexcept {
    Unleak(that.Leak());
  }

  // Assign a RetainPtr from nullptr;
  RetainPtr& operator=(std::nullptr_t) noexcept {
    Reset();
    return *this;
  }

  // Copy-assign a RetainPtr.
  // Required in addition to copy conversion assignment below.
  RetainPtr& operator=(const RetainPtr& that) {
    if (*this != that)
      Reset(that.Get());
    return *this;
  }

  // Move-assign a RetainPtr. After assignment, |that| will be NULL.
  // Required in addition to move conversion assignment below.
  RetainPtr& operator=(RetainPtr&& that) noexcept {
    Unleak(that.Leak());
    return *this;
  }

  // Copy-convert assign a RetainPtr.
  template <class U,
            typename = typename std::enable_if<
                std::is_convertible<U*, T*>::value>::type>
  RetainPtr& operator=(const RetainPtr<U>& that) {
    if (*this != that)
      Reset(that.Get());
    return *this;
  }

  // Move-convert assign a RetainPtr. After assignment, |that| will be NULL.
  template <class U,
            typename = typename std::enable_if<
                std::is_convertible<U*, T*>::value>::type>
  RetainPtr& operator=(RetainPtr<U>&& that) noexcept {
    Unleak(that.Leak());
    return *this;
  }

  ~RetainPtr() = default;

  template <class U>
  U* AsRaw() const {
    return static_cast<U*>(Get());
  }

  template <class U>
  RetainPtr<U> As() const {
    return RetainPtr<U>(AsRaw<U>());
  }

  void Reset(T* obj = nullptr) {
    if (obj)
      obj->Retain();
    m_pObj.reset(obj);
  }

  operator T*() const noexcept { return Get(); }
  T* Get() const noexcept { return m_pObj.get(); }

  void Swap(RetainPtr& that) { m_pObj.swap(that.m_pObj); }

  // Useful for passing notion of object ownership across a C API.
  T* Leak() { return m_pObj.release(); }
  void Unleak(T* ptr) { m_pObj.reset(ptr); }

  bool operator==(const RetainPtr& that) const { return Get() == that.Get(); }
  bool operator!=(const RetainPtr& that) const { return !(*this == that); }

  template <typename U>
  bool operator==(const U& that) const {
    return Get() == that;
  }

  template <typename U>
  bool operator!=(const U& that) const {
    return !(*this == that);
  }

  bool operator<(const RetainPtr& that) const {
    return std::less<T*>()(Get(), that.Get());
  }

  explicit operator bool() const { return !!m_pObj; }
  T& operator*() const { return *m_pObj; }
  T* operator->() const { return m_pObj.get(); }

 private:
  std::unique_ptr<T, ReleaseDeleter<T>> m_pObj;
};

// Trivial implementation - internal ref count with virtual destructor.
class Retainable {
 public:
  Retainable() = default;

  bool HasOneRef() const { return m_nRefCount == 1; }

 protected:
  virtual ~Retainable() = default;

 private:
  template <typename U>
  friend struct ReleaseDeleter;

  template <typename U>
  friend class RetainPtr;

  Retainable(const Retainable& that) = delete;
  Retainable& operator=(const Retainable& that) = delete;

  // These need to be const methods operating on a mutable member so that
  // RetainPtr<const T> can be used for an object that is otherwise const
  // apart from the internal ref-counting.
  void Retain() const {
    ++m_nRefCount;
    CHECK(m_nRefCount > 0);
  }
  void Release() const {
    CHECK(m_nRefCount > 0);
    if (--m_nRefCount == 0)
      delete this;
  }

  mutable uintptr_t m_nRefCount = 0;
  static_assert(std::is_unsigned<decltype(m_nRefCount)>::value,
                "m_nRefCount must be an unsigned type for overflow check"
                "to work properly in Retain()");
};

}  // namespace fxcrt

using fxcrt::ReleaseDeleter;
using fxcrt::Retainable;
using fxcrt::RetainPtr;

namespace pdfium {

// Helper to make a RetainPtr along the lines of std::make_unique<>().
// Arguments are forwarded to T's constructor. Classes managed by RetainPtr
// should have protected (or private) constructors, and should friend this
// function.
template <typename T, typename... Args>
RetainPtr<T> MakeRetain(Args&&... args) {
  return RetainPtr<T>(new T(std::forward<Args>(args)...));
}

// Type-deducing wrapper to make a RetainPtr from an ordinary pointer,
// since equivalent constructor is explicit.
template <typename T>
RetainPtr<T> WrapRetain(T* that) {
  return RetainPtr<T>(that);
}

}  // namespace pdfium

// Macro to allow construction via MakeRetain<>() only, when used
// with a private constructor in a class.
#define CONSTRUCT_VIA_MAKE_RETAIN         \
  template <typename T, typename... Args> \
  friend RetainPtr<T> pdfium::MakeRetain(Args&&... args)

#endif  // CORE_FXCRT_RETAIN_PTR_H_
