// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_STRING_TEMPLATE_H_
#define CORE_FXCRT_STRING_TEMPLATE_H_

#include <stddef.h>

#include <type_traits>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/string_data_template.h"
#include "core/fxcrt/string_view_template.h"

namespace fxcrt {

inline constexpr const char* EmptyString(char*) {
  return "";
}
inline constexpr const wchar_t* EmptyString(wchar_t*) {
  return L"";
}

// Base class for a  mutable string with shared buffers using copy-on-write
// semantics that avoids std::string's iterator stability guarantees.
template <typename T>
class StringTemplate {
 public:
  using CharType = T;
  using UnsignedType = typename std::make_unsigned<CharType>::type;
  using StringView = StringViewTemplate<T>;
  using const_iterator = T*;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  bool IsEmpty() const { return !GetLength(); }
  size_t GetLength() const { return m_pData ? m_pData->m_nDataLength : 0; }

  // Return length as determined by the position of the first NUL.
  size_t GetStringLength() const {
    return m_pData ? m_pData->GetStringLength() : 0;
  }

  // Explicit conversion to UnsignedType*. May return nullptr.
  // Note: Any subsequent modification of |this| will invalidate the result.
  const UnsignedType* unsigned_str() const {
    return m_pData ? reinterpret_cast<const UnsignedType*>(m_pData->m_String)
                   : nullptr;
  }

  // Explicit conversion to StringView.
  // Note: Any subsequent modification of |this| will invalidate the result.
  StringView AsStringView() const { return StringView(unsigned_span()); }

  // Explicit conversion to C-style string. The result is never nullptr,
  // and is always NUL terminated.
  // Note: Any subsequent modification of |this| will invalidate the result.
  const CharType* c_str() const {
    return m_pData ? m_pData->m_String
                   : EmptyString(static_cast<CharType*>(nullptr));
  }

  // Explicit conversion to span.
  // Note: Any subsequent modification of |this| will invalidate the result.
  pdfium::span<const CharType> span() const {
    return m_pData ? m_pData->span() : pdfium::span<const CharType>();
  }

  // Explicit conversion to spans of unsigned types.
  // Note: Any subsequent modification of |this| will invalidate the result.
  pdfium::span<const UnsignedType> unsigned_span() const {
    return reinterpret_span<const UnsignedType>(span());
  }

  // Explicit conversion to spans including the NUL terminator. The result is
  // never an empty span. Only a const-form is provided to preclude modifying
  // the terminator. Usage should be rare and carefully considered.
  // Note: Any subsequent modification of |this| will invalidate the result.
  pdfium::span<const CharType> span_with_terminator() const {
    // SAFETY: EmptyString() returns one NUL byte.
    return m_pData ? m_pData->span_with_terminator()
                   : UNSAFE_BUFFERS(pdfium::make_span(
                         EmptyString(static_cast<CharType*>(nullptr)), 1u));
  }

  // Explicit conversion to spans of unsigned types including the NUL
  // terminator. The result is never an empty span. Only a const-form is
  // provided to preclude modifying the terminator. Usage should be rare
  // and carefully considered.
  // Note: Any subsequent modification of |this| will invalidate the result.
  pdfium::span<const UnsignedType> unsigned_span_with_terminator() const {
    return reinterpret_span<const UnsignedType>(span_with_terminator());
  }

  // Note: Any subsequent modification of |this| will invalidate iterators.
  const_iterator begin() const {
    return m_pData ? m_pData->span().begin() : nullptr;
  }
  const_iterator end() const {
    return m_pData ? m_pData->span().end() : nullptr;
  }

  // Note: Any subsequent modification of |this| will invalidate iterators.
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }
  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }

  bool IsValidIndex(size_t index) const { return index < GetLength(); }
  bool IsValidLength(size_t length) const { return length <= GetLength(); }

  // CHECK() if index is out of range (via span's operator[]).
  CharType operator[](const size_t index) const {
    CHECK(m_pData);
    return m_pData->span()[index];
  }

  // Unlike std::wstring::front(), this is always safe and returns a
  // NUL char when the string is empty.
  CharType Front() const { return m_pData ? m_pData->Front() : 0; }

  // Unlike std::wstring::back(), this is always safe and returns a
  // NUL char when the string is empty.
  CharType Back() const { return m_pData ? m_pData->Back() : 0; }

  // Holds on to buffer if possible for later re-use. Use assignment
  // to force immediate release if desired.
  void clear();

  // Increase the backing store of the string so that it is capable of storing
  // at least `nMinBufLength` chars. Returns a span to the entire buffer,
  // which may be larger than `nMinBufLength` due to rounding by allocators.
  // Note: any modification of the string (including ReleaseBuffer()) may
  // invalidate the span, which must not outlive its buffer.
  pdfium::span<T> GetBuffer(size_t nMinBufLength);

  // Sets the size of the string to `nNewLength` chars. Call this after a call
  // to GetBuffer(), to indicate how much of the buffer was actually used.
  void ReleaseBuffer(size_t nNewLength);

  // Returns size of string following insertion.
  size_t Insert(size_t index, T ch);
  size_t InsertAtFront(T ch) { return Insert(0, ch); }
  size_t InsertAtBack(T ch) { return Insert(GetLength(), ch); }

  // Returns number of instances of `ch` removed.
  size_t Remove(T ch);

  // Returns size of the string following deletion.
  size_t Delete(size_t index, size_t count = 1);

  // Returns the index within the string when found.
  std::optional<size_t> Find(StringView str, size_t start = 0) const;
  std::optional<size_t> Find(T ch, size_t start = 0) const;
  std::optional<size_t> ReverseFind(T ch) const;

  bool Contains(StringView str, size_t start = 0) const {
    return Find(str, start).has_value();
  }
  bool Contains(T ch, size_t start = 0) const {
    return Find(ch, start).has_value();
  }

  // Replace all occurences of `oldstr' with `newstr'.
  size_t Replace(StringView oldstr, StringView newstr);

  // Overwrite character at `index`.
  void SetAt(size_t index, T ch);

  void Reserve(size_t len) { GetBuffer(len); }

  // Remove character `ch` from  both/front/back of string.
  void Trim(T ch);
  void TrimFront(T ch);
  void TrimBack(T ch);

  // Remove all characters in `targets` from both/front/back of string.
  void Trim(StringView targets);
  void TrimFront(StringView targets);
  void TrimBack(StringView targets);

 protected:
  using StringData = StringDataTemplate<T>;

  StringTemplate() = default;
  StringTemplate(const StringTemplate& other) = default;

  // Move-construct a StringTemplate. After construction, |other| is empty.
  StringTemplate(StringTemplate&& other) noexcept = default;

  ~StringTemplate() = default;

  void ReallocBeforeWrite(size_t nNewLen);
  void AllocBeforeWrite(size_t nNewLen);
  void AssignCopy(const T* pSrcData, size_t nSrcLen);
  void Concat(const T* pSrcData, size_t nSrcLen);

  RetainPtr<StringData> m_pData;
};

extern template class StringTemplate<char>;
extern template class StringTemplate<wchar_t>;

}  // namespace fxcrt

#endif  // CORE_FXCRT_STRING_TEMPLATE_H_
