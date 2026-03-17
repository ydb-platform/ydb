// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_STREAM_H_
#define CORE_FXCRT_FX_STREAM_H_

#include <stddef.h>
#include <stdint.h>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_types.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class IFX_WriteStream {
 public:
  // When `size` is 0, treat it as a no-op and return true.
  virtual bool WriteBlock(pdfium::span<const uint8_t> data) = 0;

  bool WriteString(ByteStringView str);
  bool WriteByte(uint8_t byte);
  bool WriteDWord(uint32_t i);
  bool WriteFilesize(FX_FILESIZE size);

 protected:
  virtual ~IFX_WriteStream() = default;
};

class IFX_ArchiveStream : public IFX_WriteStream {
 public:
  virtual FX_FILESIZE CurrentOffset() const = 0;
};

class IFX_StreamWithSize {
 public:
  virtual FX_FILESIZE GetSize() = 0;
};

class IFX_RetainableWriteStream : virtual public Retainable,
                                  public IFX_WriteStream {};

class IFX_SeekableWriteStream : virtual public IFX_StreamWithSize,
                                public IFX_RetainableWriteStream {
 public:
  virtual bool Flush() = 0;
};

class IFX_SeekableReadStream : virtual public Retainable,
                               virtual public IFX_StreamWithSize {
 public:
  static RetainPtr<IFX_SeekableReadStream> CreateFromFilename(
      const char* filename);

  virtual bool IsEOF();
  virtual FX_FILESIZE GetPosition();
  [[nodiscard]] virtual bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                                               FX_FILESIZE offset) = 0;
};

class IFX_SeekableStream : public IFX_SeekableReadStream,
                           public IFX_SeekableWriteStream {
};

#endif  // CORE_FXCRT_FX_STREAM_H_
