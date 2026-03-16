// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/widetext_buffer.h"

#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/span_util.h"

namespace fxcrt {

size_t WideTextBuffer::GetLength() const {
  return GetSize() / sizeof(wchar_t);
}

pdfium::span<wchar_t> WideTextBuffer::GetWideSpan() {
  return reinterpret_span<wchar_t>(GetMutableSpan());
}

pdfium::span<const wchar_t> WideTextBuffer::GetWideSpan() const {
  return reinterpret_span<const wchar_t>(GetSpan());
}

WideStringView WideTextBuffer::AsStringView() const {
  return WideStringView(GetWideSpan());
}

WideString WideTextBuffer::MakeString() const {
  return WideString(AsStringView());
}

void WideTextBuffer::AppendChar(wchar_t ch) {
  pdfium::span<wchar_t> new_span = ExpandWideBuf(1);
  new_span[0] = ch;
}

void WideTextBuffer::Delete(size_t start_index, size_t count) {
  DeleteBuf(start_index * sizeof(wchar_t), count * sizeof(wchar_t));
}

void WideTextBuffer::AppendWideString(WideStringView str) {
  AppendSpan(pdfium::as_bytes(str.span()));
}

WideTextBuffer& WideTextBuffer::operator<<(ByteStringView ascii) {
  pdfium::span<wchar_t> new_span = ExpandWideBuf(ascii.GetLength());
  for (size_t i = 0; i < ascii.GetLength(); ++i)
    new_span[i] = ascii[i];
  return *this;
}

WideTextBuffer& WideTextBuffer::operator<<(WideStringView str) {
  AppendWideString(str);
  return *this;
}

WideTextBuffer& WideTextBuffer::operator<<(const WideString& str) {
  AppendWideString(str.AsStringView());
  return *this;
}

WideTextBuffer& WideTextBuffer::operator<<(const wchar_t* lpsz) {
  AppendWideString(WideStringView(lpsz));
  return *this;
}

WideTextBuffer& WideTextBuffer::operator<<(const WideTextBuffer& buf) {
  AppendWideString(buf.AsStringView());
  return *this;
}

pdfium::span<wchar_t> WideTextBuffer::ExpandWideBuf(size_t char_count) {
  size_t original_count = GetLength();
  FX_SAFE_SIZE_T safe_bytes = char_count;
  safe_bytes *= sizeof(wchar_t);
  size_t bytes = safe_bytes.ValueOrDie();
  ExpandBuf(bytes);
  m_DataSize += bytes;
  return GetWideSpan().subspan(original_count);
}

}  // namespace fxcrt
