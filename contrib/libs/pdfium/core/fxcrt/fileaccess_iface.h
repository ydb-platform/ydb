// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FILEACCESS_IFACE_H_
#define CORE_FXCRT_FILEACCESS_IFACE_H_

#include <memory>

#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/span.h"

class FileAccessIface {
 public:
  static std::unique_ptr<FileAccessIface> Create();
  virtual ~FileAccessIface() = default;

  // Opens in read-only mode. `fileName` is UTF-8 on all platforms.
  virtual bool Open(ByteStringView fileName) = 0;
  virtual void Close() = 0;
  virtual FX_FILESIZE GetSize() const = 0;
  virtual FX_FILESIZE GetPosition() const = 0;
  virtual FX_FILESIZE SetPosition(FX_FILESIZE pos) = 0;
  virtual size_t Read(pdfium::span<uint8_t> buffer) = 0;
  virtual size_t Write(pdfium::span<const uint8_t> buffer) = 0;
  virtual size_t ReadPos(pdfium::span<uint8_t> buffer, FX_FILESIZE pos) = 0;
  virtual bool Flush() = 0;
  virtual bool Truncate(FX_FILESIZE szFile) = 0;
};

#endif  // CORE_FXCRT_FILEACCESS_IFACE_H_
