// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxcodec/basic/basicmodule.h"

#include <stdint.h>

#include <algorithm>
#include <utility>

#include "core/fxcodec/scanlinedecoder.h"
#include "core/fxcrt/byteorder.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/stl_util.h"

namespace fxcodec {

namespace {

class RLScanlineDecoder final : public ScanlineDecoder {
 public:
  RLScanlineDecoder();
  ~RLScanlineDecoder() override;

  bool Create(pdfium::span<const uint8_t> src_buf,
              int width,
              int height,
              int nComps,
              int bpc);

  // ScanlineDecoder:
  bool Rewind() override;
  pdfium::span<uint8_t> GetNextLine() override;
  uint32_t GetSrcOffset() override;

 private:
  bool CheckDestSize();
  void GetNextOperator();
  void UpdateOperator(uint8_t used_bytes);

  DataVector<uint8_t> m_Scanline;
  pdfium::raw_span<const uint8_t> m_SrcBuf;
  size_t m_dwLineBytes = 0;
  size_t m_SrcOffset = 0;
  bool m_bEOD = false;
  uint8_t m_Operator = 0;
};

RLScanlineDecoder::RLScanlineDecoder() = default;

RLScanlineDecoder::~RLScanlineDecoder() {
  // Span in superclass can't outlive our buffer.
  m_pLastScanline = pdfium::span<uint8_t>();
}

bool RLScanlineDecoder::CheckDestSize() {
  size_t i = 0;
  uint32_t old_size = 0;
  uint32_t dest_size = 0;
  while (i < m_SrcBuf.size()) {
    if (m_SrcBuf[i] < 128) {
      old_size = dest_size;
      dest_size += m_SrcBuf[i] + 1;
      if (dest_size < old_size) {
        return false;
      }
      i += m_SrcBuf[i] + 2;
    } else if (m_SrcBuf[i] > 128) {
      old_size = dest_size;
      dest_size += 257 - m_SrcBuf[i];
      if (dest_size < old_size) {
        return false;
      }
      i += 2;
    } else {
      break;
    }
  }
  if (((uint32_t)m_OrigWidth * m_nComps * m_bpc * m_OrigHeight + 7) / 8 >
      dest_size) {
    return false;
  }
  return true;
}

bool RLScanlineDecoder::Create(pdfium::span<const uint8_t> src_buf,
                               int width,
                               int height,
                               int nComps,
                               int bpc) {
  m_SrcBuf = src_buf;
  m_OutputWidth = m_OrigWidth = width;
  m_OutputHeight = m_OrigHeight = height;
  m_nComps = nComps;
  m_bpc = bpc;
  // Aligning the pitch to 4 bytes requires an integer overflow check.
  FX_SAFE_UINT32 pitch = width;
  pitch *= nComps;
  pitch *= bpc;
  pitch += 31;
  pitch /= 32;
  pitch *= 4;
  if (!pitch.IsValid()) {
    return false;
  }
  m_Pitch = pitch.ValueOrDie();
  // Overflow should already have been checked before this is called.
  m_dwLineBytes = (static_cast<uint32_t>(width) * nComps * bpc + 7) / 8;
  m_Scanline.resize(m_Pitch);
  return CheckDestSize();
}

bool RLScanlineDecoder::Rewind() {
  fxcrt::Fill(m_Scanline, 0);
  m_SrcOffset = 0;
  m_bEOD = false;
  m_Operator = 0;
  return true;
}

pdfium::span<uint8_t> RLScanlineDecoder::GetNextLine() {
  if (m_SrcOffset == 0) {
    GetNextOperator();
  } else if (m_bEOD) {
    return pdfium::span<uint8_t>();
  }
  fxcrt::Fill(m_Scanline, 0);
  uint32_t col_pos = 0;
  bool eol = false;
  auto scan_span = pdfium::make_span(m_Scanline);
  while (m_SrcOffset < m_SrcBuf.size() && !eol) {
    if (m_Operator < 128) {
      uint32_t copy_len = m_Operator + 1;
      if (col_pos + copy_len >= m_dwLineBytes) {
        copy_len = pdfium::checked_cast<uint32_t>(m_dwLineBytes - col_pos);
        eol = true;
      }
      if (copy_len >= m_SrcBuf.size() - m_SrcOffset) {
        copy_len =
            pdfium::checked_cast<uint32_t>(m_SrcBuf.size() - m_SrcOffset);
        m_bEOD = true;
      }
      fxcrt::Copy(m_SrcBuf.subspan(m_SrcOffset, copy_len),
                  scan_span.subspan(col_pos));
      col_pos += copy_len;
      UpdateOperator((uint8_t)copy_len);
    } else if (m_Operator > 128) {
      int fill = 0;
      if (m_SrcOffset < m_SrcBuf.size()) {
        fill = m_SrcBuf[m_SrcOffset];
      }
      uint32_t duplicate_len = 257 - m_Operator;
      if (col_pos + duplicate_len >= m_dwLineBytes) {
        duplicate_len = pdfium::checked_cast<uint32_t>(m_dwLineBytes - col_pos);
        eol = true;
      }
      fxcrt::Fill(scan_span.subspan(col_pos, duplicate_len), fill);
      col_pos += duplicate_len;
      UpdateOperator((uint8_t)duplicate_len);
    } else {
      m_bEOD = true;
      break;
    }
  }
  return m_Scanline;
}

uint32_t RLScanlineDecoder::GetSrcOffset() {
  return pdfium::checked_cast<uint32_t>(m_SrcOffset);
}

void RLScanlineDecoder::GetNextOperator() {
  if (m_SrcOffset >= m_SrcBuf.size()) {
    m_Operator = 128;
    return;
  }
  m_Operator = m_SrcBuf[m_SrcOffset];
  m_SrcOffset++;
}
void RLScanlineDecoder::UpdateOperator(uint8_t used_bytes) {
  if (used_bytes == 0) {
    return;
  }
  if (m_Operator < 128) {
    DCHECK((uint32_t)m_Operator + 1 >= used_bytes);
    if (used_bytes == m_Operator + 1) {
      m_SrcOffset += used_bytes;
      GetNextOperator();
      return;
    }
    m_Operator -= used_bytes;
    m_SrcOffset += used_bytes;
    if (m_SrcOffset >= m_SrcBuf.size()) {
      m_Operator = 128;
    }
    return;
  }
  uint8_t count = 257 - m_Operator;
  DCHECK((uint32_t)count >= used_bytes);
  if (used_bytes == count) {
    m_SrcOffset++;
    GetNextOperator();
    return;
  }
  count -= used_bytes;
  m_Operator = 257 - count;
}

}  // namespace

// static
std::unique_ptr<ScanlineDecoder> BasicModule::CreateRunLengthDecoder(
    pdfium::span<const uint8_t> src_buf,
    int width,
    int height,
    int nComps,
    int bpc) {
  auto pDecoder = std::make_unique<RLScanlineDecoder>();
  if (!pDecoder->Create(src_buf, width, height, nComps, bpc))
    return nullptr;

  return pDecoder;
}

// static
DataVector<uint8_t> BasicModule::RunLengthEncode(
    pdfium::span<const uint8_t> src_span) {
  if (src_span.empty())
    return {};

  // Handle edge case.
  if (src_span.size() == 1)
    return {0, src_span[0], 128};

  // Worst case: 1 nonmatch, 2 match, 1 nonmatch, 2 match, etc. This becomes
  // 4 output chars for every 3 input, plus up to 4 more for the 1-2 chars
  // rounded off plus the terminating character.
  FX_SAFE_SIZE_T estimated_size = src_span.size();
  estimated_size += 2;
  estimated_size /= 3;
  estimated_size *= 4;
  estimated_size += 1;
  DataVector<uint8_t> result(estimated_size.ValueOrDie());

  // Set up span and counts.
  auto result_span = pdfium::make_span(result);
  uint32_t run_start = 0;
  uint32_t run_end = 1;
  uint8_t x = src_span[run_start];
  uint8_t y = src_span[run_end];
  while (run_end < src_span.size()) {
    size_t max_len = std::min<size_t>(128, src_span.size() - run_start);
    while (x == y && (run_end - run_start < max_len - 1))
      y = src_span[++run_end];

    // Reached end with matched run. Update variables to expected values.
    if (x == y) {
      run_end++;
      if (run_end < src_span.size())
        y = src_span[run_end];
    }
    if (run_end - run_start > 1) {  // Matched run but not at end of input.
      result_span[0] = 257 - (run_end - run_start);
      result_span[1] = x;
      x = y;
      run_start = run_end;
      run_end++;
      if (run_end < src_span.size())
        y = src_span[run_end];
      result_span = result_span.subspan(2);
      continue;
    }
    // Mismatched run
    while (x != y && run_end <= run_start + max_len) {
      result_span[run_end - run_start] = x;
      x = y;
      run_end++;
      if (run_end == src_span.size()) {
        if (run_end <= run_start + max_len) {
          result_span[run_end - run_start] = x;
          run_end++;
        }
        break;
      }
      y = src_span[run_end];
    }
    result_span[0] = run_end - run_start - 2;
    result_span = result_span.subspan(run_end - run_start);
    run_start = run_end - 1;
  }
  if (run_start < src_span.size()) {  // 1 leftover character
    result_span[0] = 0;
    result_span[1] = x;
    result_span = result_span.subspan(2);
  }
  result_span[0] = 128;
  size_t new_size = 1 + result.size() - result_span.size();
  CHECK_LE(new_size, result.size());
  result.resize(new_size);
  return result;
}

// static
DataVector<uint8_t> BasicModule::A85Encode(
    pdfium::span<const uint8_t> src_span) {
  DataVector<uint8_t> result;
  if (src_span.empty())
    return result;

  // Worst case: 5 output for each 4 input (plus up to 4 from leftover), plus
  // 2 character new lines each 75 output chars plus 2 termination chars. May
  // have fewer if there are special "z" chars.
  FX_SAFE_SIZE_T estimated_size = src_span.size();
  estimated_size /= 4;
  estimated_size *= 5;
  estimated_size += 4;
  estimated_size += src_span.size() / 30;
  estimated_size += 2;
  result.resize(estimated_size.ValueOrDie());

  // Set up span and counts.
  auto result_span = pdfium::make_span(result);
  uint32_t pos = 0;
  uint32_t line_length = 0;
  while (src_span.size() >= 4 && pos < src_span.size() - 3) {
    auto val_span = src_span.subspan(pos, 4);
    uint32_t val = fxcrt::GetUInt32MSBFirst(val_span);
    pos += 4;
    if (val == 0) {  // All zero special case
      result_span[0] = 'z';
      result_span = result_span.subspan(1);
      line_length++;
    } else {  // Compute base 85 characters and add 33.
      for (int i = 4; i >= 0; i--) {
        result_span[i] = (val % 85) + 33;
        val /= 85;
      }
      result_span = result_span.subspan(5);
      line_length += 5;
    }
    if (line_length >= 75) {  // Add a return.
      result_span[0] = '\r';
      result_span[1] = '\n';
      result_span = result_span.subspan(2);
      line_length = 0;
    }
  }
  if (pos < src_span.size()) {  // Leftover bytes
    uint32_t val = 0;
    int count = 0;
    while (pos < src_span.size()) {
      val += (uint32_t)(src_span[pos]) << (8 * (3 - count));
      count++;
      pos++;
    }
    for (int i = 4; i >= 0; i--) {
      if (i <= count)
        result_span[i] = (val % 85) + 33;
      val /= 85;
    }
    result_span = result_span.subspan(count + 1);
  }

  // Terminating characters.
  result_span[0] = '~';
  result_span[1] = '>';
  size_t new_size = 2 + result.size() - result_span.size();
  CHECK_LE(new_size, result.size());
  result.resize(new_size);
  return result;
}

}  // namespace fxcodec
