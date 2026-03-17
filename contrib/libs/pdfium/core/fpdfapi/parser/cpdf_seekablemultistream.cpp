// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_seekablemultistream.h"

#include <algorithm>
#include <utility>

#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"

CPDF_SeekableMultiStream::CPDF_SeekableMultiStream(
    std::vector<RetainPtr<const CPDF_Stream>> streams) {
  for (auto& pStream : streams) {
    m_Data.push_back(pdfium::MakeRetain<CPDF_StreamAcc>(std::move(pStream)));
    m_Data.back()->LoadAllDataFiltered();
  }
}

CPDF_SeekableMultiStream::~CPDF_SeekableMultiStream() = default;

FX_FILESIZE CPDF_SeekableMultiStream::GetSize() {
  FX_SAFE_FILESIZE dwSize = 0;
  for (const auto& acc : m_Data)
    dwSize += acc->GetSize();
  return dwSize.ValueOrDie();
}

bool CPDF_SeekableMultiStream::ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                                                 FX_FILESIZE offset) {
  int32_t iCount = fxcrt::CollectionSize<int32_t>(m_Data);
  int32_t index = 0;
  while (index < iCount) {
    const auto& acc = m_Data[index];
    FX_FILESIZE dwSize = acc->GetSize();
    if (offset < dwSize)
      break;

    offset -= dwSize;
    index++;
  }
  while (index < iCount) {
    auto acc_span = m_Data[index]->GetSpan();
    size_t dwRead = std::min<size_t>(buffer.size(), acc_span.size() - offset);
    buffer = fxcrt::spancpy(buffer, acc_span.subspan(offset, dwRead));
    if (buffer.empty())
      return true;

    offset = 0;
    index++;
  }
  return false;
}

FX_FILESIZE CPDF_SeekableMultiStream::GetPosition() {
  return 0;
}

bool CPDF_SeekableMultiStream::IsEOF() {
  return false;
}

bool CPDF_SeekableMultiStream::Flush() {
  NOTREACHED_NORETURN();
}

bool CPDF_SeekableMultiStream::WriteBlock(pdfium::span<const uint8_t> buffer) {
  NOTREACHED_NORETURN();
}
