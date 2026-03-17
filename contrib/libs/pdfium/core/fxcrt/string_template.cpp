// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/string_template.h"

#include <algorithm>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/span_util.h"

namespace fxcrt {

template <typename T>
pdfium::span<T> StringTemplate<T>::GetBuffer(size_t nMinBufLength) {
  if (!m_pData) {
    if (nMinBufLength == 0) {
      return pdfium::span<T>();
    }
    m_pData = StringData::Create(nMinBufLength);
    m_pData->m_nDataLength = 0;
    m_pData->m_String[0] = 0;
    return m_pData->alloc_span();
  }
  if (m_pData->CanOperateInPlace(nMinBufLength)) {
    return m_pData->alloc_span();
  }
  nMinBufLength = std::max(nMinBufLength, m_pData->m_nDataLength);
  if (nMinBufLength == 0) {
    return pdfium::span<T>();
  }
  RetainPtr<StringData> pNewData = StringData::Create(nMinBufLength);
  pNewData->CopyContents(*m_pData);
  pNewData->m_nDataLength = m_pData->m_nDataLength;
  m_pData = std::move(pNewData);
  return m_pData->alloc_span();
}

template <typename T>
void StringTemplate<T>::ReleaseBuffer(size_t nNewLength) {
  if (!m_pData) {
    return;
  }
  nNewLength = std::min(nNewLength, m_pData->m_nAllocLength);
  if (nNewLength == 0) {
    clear();
    return;
  }
  DCHECK_EQ(m_pData->m_nRefs, 1);
  m_pData->m_nDataLength = nNewLength;
  m_pData->capacity_span()[nNewLength] = 0;
  if (m_pData->m_nAllocLength - nNewLength >= 32) {
    // Over arbitrary threshold, so pay the price to relocate.  Force copy to
    // always occur by holding a second reference to the string.
    StringTemplate preserve(*this);
    ReallocBeforeWrite(nNewLength);
  }
}

template <typename T>
size_t StringTemplate<T>::Remove(T chRemove) {
  size_t count = std::count(span().begin(), span().end(), chRemove);
  if (count == 0) {
    return 0;
  }
  ReallocBeforeWrite(m_pData->m_nDataLength);
  auto src_span = m_pData->span();
  auto dst_span = m_pData->span();
  // Perform self-intersecting copy in forwards order.
  while (!src_span.empty()) {
    if (src_span[0] != chRemove) {
      dst_span[0] = src_span[0];
      dst_span = dst_span.subspan(1);
    }
    src_span = src_span.subspan(1);
  }
  m_pData->m_nDataLength -= count;
  m_pData->capacity_span()[m_pData->m_nDataLength] = 0;
  return count;
}

template <typename T>
size_t StringTemplate<T>::Insert(size_t index, T ch) {
  const size_t cur_length = GetLength();
  if (!IsValidLength(index)) {
    return cur_length;
  }
  const size_t new_length = cur_length + 1;
  ReallocBeforeWrite(new_length);
  fxcrt::spanmove(m_pData->capacity_span().subspan(index + 1),
                  m_pData->capacity_span().subspan(index, new_length - index));
  m_pData->capacity_span()[index] = ch;
  m_pData->m_nDataLength = new_length;
  return new_length;
}

template <typename T>
size_t StringTemplate<T>::Delete(size_t index, size_t count) {
  if (!m_pData) {
    return 0;
  }
  size_t old_length = m_pData->m_nDataLength;
  if (count == 0 || index != std::clamp<size_t>(index, 0, old_length)) {
    return old_length;
  }
  size_t removal_length = index + count;
  if (removal_length > old_length) {
    return old_length;
  }
  ReallocBeforeWrite(old_length);
  // Include the NUL char not accounted for in lengths.
  size_t chars_to_copy = old_length - removal_length + 1;
  fxcrt::spanmove(
      m_pData->capacity_span().subspan(index),
      m_pData->capacity_span().subspan(removal_length, chars_to_copy));
  m_pData->m_nDataLength = old_length - count;
  return m_pData->m_nDataLength;
}

template <typename T>
void StringTemplate<T>::SetAt(size_t index, T ch) {
  DCHECK(IsValidIndex(index));
  ReallocBeforeWrite(m_pData->m_nDataLength);
  m_pData->span()[index] = ch;
}

template <typename T>
std::optional<size_t> StringTemplate<T>::Find(T ch, size_t start) const {
  return Find(StringView(ch), start);
}

template <typename T>
std::optional<size_t> StringTemplate<T>::Find(StringView str,
                                              size_t start) const {
  if (!m_pData) {
    return std::nullopt;
  }
  if (!IsValidIndex(start)) {
    return std::nullopt;
  }
  std::optional<size_t> result =
      spanpos(m_pData->span().subspan(start), str.span());
  if (!result.has_value()) {
    return std::nullopt;
  }
  return start + result.value();
}

template <typename T>
std::optional<size_t> StringTemplate<T>::ReverseFind(T ch) const {
  if (!m_pData) {
    return std::nullopt;
  }
  size_t nLength = m_pData->m_nDataLength;
  while (nLength--) {
    if (m_pData->span()[nLength] == ch) {
      return nLength;
    }
  }
  return std::nullopt;
}

template <typename T>
size_t StringTemplate<T>::Replace(StringView oldstr, StringView newstr) {
  if (!m_pData || oldstr.IsEmpty()) {
    return 0;
  }
  size_t count = 0;
  {
    // Limit span lifetime.
    pdfium::span<const T> search_span = m_pData->span();
    while (true) {
      std::optional<size_t> found = spanpos(search_span, oldstr.span());
      if (!found.has_value()) {
        break;
      }
      ++count;
      search_span = search_span.subspan(found.value() + oldstr.GetLength());
    }
  }
  if (count == 0) {
    return 0;
  }
  size_t nNewLength = m_pData->m_nDataLength +
                      count * (newstr.GetLength() - oldstr.GetLength());
  if (nNewLength == 0) {
    clear();
    return count;
  }
  RetainPtr<StringData> newstr_data = StringData::Create(nNewLength);
  {
    // Spans can't outlive StringData buffers.
    pdfium::span<const T> search_span = m_pData->span();
    pdfium::span<T> dest_span = newstr_data->span();
    for (size_t i = 0; i < count; i++) {
      size_t found = spanpos(search_span, oldstr.span()).value();
      dest_span = spancpy(dest_span, search_span.first(found));
      dest_span = spancpy(dest_span, newstr.span());
      search_span = search_span.subspan(found + oldstr.GetLength());
    }
    dest_span = spancpy(dest_span, search_span);
    CHECK(dest_span.empty());
  }
  m_pData = std::move(newstr_data);
  return count;
}

template <typename T>
void StringTemplate<T>::Trim(T ch) {
  TrimFront(ch);
  TrimBack(ch);
}

template <typename T>
void StringTemplate<T>::TrimFront(T ch) {
  TrimFront(StringView(ch));
}

template <typename T>
void StringTemplate<T>::TrimBack(T ch) {
  TrimBack(StringView(ch));
}

template <typename T>
void StringTemplate<T>::Trim(StringView targets) {
  TrimFront(targets);
  TrimBack(targets);
}

template <typename T>
void StringTemplate<T>::TrimFront(StringView targets) {
  if (!m_pData || targets.IsEmpty()) {
    return;
  }

  size_t len = GetLength();
  if (len == 0) {
    return;
  }

  size_t pos = 0;
  while (pos < len) {
    size_t i = 0;
    while (i < targets.GetLength() &&
           targets.CharAt(i) != m_pData->span()[pos]) {
      i++;
    }
    if (i == targets.GetLength()) {
      break;
    }
    pos++;
  }
  if (!pos) {
    return;
  }

  ReallocBeforeWrite(len);
  size_t nDataLength = len - pos;
  // Move the terminating NUL as well.
  fxcrt::spanmove(m_pData->capacity_span(),
                  m_pData->capacity_span().subspan(pos, nDataLength + 1));
  m_pData->m_nDataLength = nDataLength;
}

template <typename T>
void StringTemplate<T>::TrimBack(StringView targets) {
  if (!m_pData || targets.IsEmpty()) {
    return;
  }

  size_t pos = GetLength();
  if (pos == 0) {
    return;
  }

  while (pos) {
    size_t i = 0;
    while (i < targets.GetLength() &&
           targets.CharAt(i) != m_pData->span()[pos - 1]) {
      i++;
    }
    if (i == targets.GetLength()) {
      break;
    }
    pos--;
  }
  if (pos < m_pData->m_nDataLength) {
    ReallocBeforeWrite(m_pData->m_nDataLength);
    m_pData->m_nDataLength = pos;
    m_pData->capacity_span()[m_pData->m_nDataLength] = 0;
  }
}

template <typename T>
void StringTemplate<T>::ReallocBeforeWrite(size_t nNewLength) {
  if (m_pData && m_pData->CanOperateInPlace(nNewLength)) {
    return;
  }
  if (nNewLength == 0) {
    clear();
    return;
  }
  RetainPtr<StringData> pNewData = StringData::Create(nNewLength);
  if (m_pData) {
    size_t nCopyLength = std::min(m_pData->m_nDataLength, nNewLength);
    // SAFETY: copy of no more than m_nDataLength bytes.
    pNewData->CopyContents(
        UNSAFE_BUFFERS(pdfium::make_span(m_pData->m_String, nCopyLength)));
    pNewData->m_nDataLength = nCopyLength;
  } else {
    pNewData->m_nDataLength = 0;
  }
  pNewData->capacity_span()[pNewData->m_nDataLength] = 0;
  m_pData = std::move(pNewData);
}

template <typename T>
void StringTemplate<T>::AllocBeforeWrite(size_t nNewLength) {
  if (m_pData && m_pData->CanOperateInPlace(nNewLength)) {
    return;
  }
  if (nNewLength == 0) {
    clear();
    return;
  }
  m_pData = StringData::Create(nNewLength);
}

template <typename T>
void StringTemplate<T>::AssignCopy(const T* pSrcData, size_t nSrcLen) {
  AllocBeforeWrite(nSrcLen);
  // SAFETY: AllocBeforeWrite() ensures `nSrcLen` bytes available.
  m_pData->CopyContents(UNSAFE_BUFFERS(pdfium::make_span(pSrcData, nSrcLen)));
  m_pData->m_nDataLength = nSrcLen;
}

template <typename T>
void StringTemplate<T>::Concat(const T* pSrcData, size_t nSrcLen) {
  if (!pSrcData || nSrcLen == 0) {
    return;
  }
  // SAFETY: required from caller.
  // TODO(tsepez): should be UNSAFE_BUFFER_USAGE or pass span.
  auto src_span = UNSAFE_BUFFERS(pdfium::make_span(pSrcData, nSrcLen));
  if (!m_pData) {
    m_pData = StringData::Create(src_span);
    return;
  }
  if (m_pData->CanOperateInPlace(m_pData->m_nDataLength + nSrcLen)) {
    m_pData->CopyContentsAt(m_pData->m_nDataLength, src_span);
    m_pData->m_nDataLength += nSrcLen;
    return;
  }
  // Increase size by at least 50% to amortize repeated concatenations.
  size_t nGrowLen = std::max(m_pData->m_nDataLength / 2, nSrcLen);
  RetainPtr<StringData> pNewData =
      StringData::Create(m_pData->m_nDataLength + nGrowLen);
  pNewData->CopyContents(*m_pData);
  pNewData->CopyContentsAt(m_pData->m_nDataLength, src_span);
  pNewData->m_nDataLength = m_pData->m_nDataLength + nSrcLen;
  m_pData = std::move(pNewData);
}

template <typename T>
void StringTemplate<T>::clear() {
  if (m_pData && m_pData->CanOperateInPlace(0)) {
    m_pData->m_nDataLength = 0;
    return;
  }
  m_pData.Reset();
}

// Instantiate.
template class StringTemplate<char>;
template class StringTemplate<wchar_t>;

}  // namespace fxcrt
