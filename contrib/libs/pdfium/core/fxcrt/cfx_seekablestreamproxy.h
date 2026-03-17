// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CFX_SEEKABLESTREAMPROXY_H_
#define CORE_FXCRT_CFX_SEEKABLESTREAMPROXY_H_

#include <stddef.h>
#include <stdint.h>

#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/fx_types.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class CFX_SeekableStreamProxy final : public Retainable {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  FX_FILESIZE GetSize() const;  // Estimate under worst possible expansion.
  bool IsEOF() const;

  // Returns number of wchar_t elements placed into `buffer`.
  size_t ReadBlock(pdfium::span<wchar_t> buffer);

  FX_CodePage GetCodePage() const { return m_wCodePage; }
  void SetCodePage(FX_CodePage wCodePage);

 private:
  enum class From {
    Begin = 0,
    Current,
  };

  explicit CFX_SeekableStreamProxy(
      const RetainPtr<IFX_SeekableReadStream>& stream);
  ~CFX_SeekableStreamProxy() override;

  FX_FILESIZE GetPosition() const;
  void Seek(From eSeek, FX_FILESIZE iOffset);
  size_t ReadData(pdfium::span<uint8_t> buffer);

  FX_CodePage m_wCodePage = FX_CodePage::kDefANSI;
  size_t m_wBOMLength = 0;
  FX_FILESIZE m_iPosition = 0;
  RetainPtr<IFX_SeekableReadStream> const m_pStream;
};

#endif  // CORE_FXCRT_CFX_SEEKABLESTREAMPROXY_H_
