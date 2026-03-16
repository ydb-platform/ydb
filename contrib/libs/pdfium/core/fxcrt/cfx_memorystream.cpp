// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/cfx_memorystream.h"

#include <algorithm>
#include <utility>

#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/stl_util.h"

CFX_MemoryStream::CFX_MemoryStream() = default;

CFX_MemoryStream::~CFX_MemoryStream() = default;

FX_FILESIZE CFX_MemoryStream::GetSize() {
  return static_cast<FX_FILESIZE>(m_nCurSize);
}

bool CFX_MemoryStream::IsEOF() {
  return m_nCurPos >= static_cast<size_t>(GetSize());
}

FX_FILESIZE CFX_MemoryStream::GetPosition() {
  return static_cast<FX_FILESIZE>(m_nCurPos);
}

bool CFX_MemoryStream::Flush() {
  return true;
}

pdfium::span<const uint8_t> CFX_MemoryStream::GetSpan() const {
  return pdfium::make_span(m_data).first(m_nCurSize);
}

bool CFX_MemoryStream::ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                                         FX_FILESIZE offset) {
  if (buffer.empty() || offset < 0)
    return false;

  FX_SAFE_SIZE_T new_pos = buffer.size();
  new_pos += offset;
  if (!new_pos.IsValid() || new_pos.ValueOrDefault(0) == 0 ||
      new_pos.ValueOrDie() > m_nCurSize) {
    return false;
  }

  m_nCurPos = new_pos.ValueOrDie();
  // Safe to cast `offset` because it was used to calculate `new_pos` above, and
  // `new_pos` is valid.
  fxcrt::Copy(GetSpan().subspan(static_cast<size_t>(offset), buffer.size()),
              buffer);
  return true;
}

bool CFX_MemoryStream::WriteBlock(pdfium::span<const uint8_t> buffer) {
  if (buffer.empty())
    return true;

  FX_SAFE_SIZE_T safe_new_pos = buffer.size();
  safe_new_pos += m_nCurSize;
  if (!safe_new_pos.IsValid()) {
    return false;
  }

  size_t new_pos = safe_new_pos.ValueOrDie();
  if (new_pos > m_data.size()) {
    static constexpr size_t kBlockSize = 64 * 1024;
    FX_SAFE_SIZE_T new_size = new_pos;
    new_size *= 2;
    new_size += (kBlockSize - 1);
    new_size /= kBlockSize;
    new_size *= kBlockSize;
    if (!new_size.IsValid())
      return false;

    m_data.resize(new_size.ValueOrDie());
  }
  m_nCurPos = new_pos;

  fxcrt::Copy(buffer, pdfium::make_span(m_data).subspan(m_nCurSize));
  m_nCurSize = std::max(m_nCurSize, m_nCurPos);

  return true;
}
