// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_stream.h"

#include <memory>
#include <utility>

#include "core/fxcrt/fileaccess_iface.h"

namespace {

class CFX_CRTFileStream final : public IFX_SeekableStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_SeekableStream:
  FX_FILESIZE GetSize() override { return m_pFile->GetSize(); }
  bool IsEOF() override { return GetPosition() >= GetSize(); }
  FX_FILESIZE GetPosition() override { return m_pFile->GetPosition(); }
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override {
    return m_pFile->ReadPos(buffer, offset) > 0;
  }
  bool WriteBlock(pdfium::span<const uint8_t> buffer) override {
    if (m_pFile->SetPosition(GetSize()) == static_cast<FX_FILESIZE>(-1)) {
      return false;
    }
    return !!m_pFile->Write(buffer);
  }
  bool Flush() override { return m_pFile->Flush(); }

 private:
  explicit CFX_CRTFileStream(std::unique_ptr<FileAccessIface> pFA)
      : m_pFile(std::move(pFA)) {}
  ~CFX_CRTFileStream() override = default;

  std::unique_ptr<FileAccessIface> m_pFile;
};

}  // namespace

bool IFX_WriteStream::WriteString(ByteStringView str) {
  return WriteBlock(str.unsigned_span());
}

bool IFX_WriteStream::WriteByte(uint8_t byte) {
  return WriteBlock(pdfium::byte_span_from_ref(byte));
}

bool IFX_WriteStream::WriteDWord(uint32_t i) {
  char buf[20] = {};
  FXSYS_itoa(i, buf, 10);
  auto buf_span = pdfium::as_byte_span(buf);
  return WriteBlock(buf_span.first(strlen(buf)));
}

bool IFX_WriteStream::WriteFilesize(FX_FILESIZE size) {
  char buf[20] = {};
  FXSYS_i64toa(size, buf, 10);
  return WriteBlock(pdfium::as_writable_byte_span(buf).first(strlen(buf)));
}

// static
RetainPtr<IFX_SeekableReadStream> IFX_SeekableReadStream::CreateFromFilename(
    const char* filename) {
  std::unique_ptr<FileAccessIface> pFA = FileAccessIface::Create();
  if (!pFA->Open(filename))
    return nullptr;
  return pdfium::MakeRetain<CFX_CRTFileStream>(std::move(pFA));
}

bool IFX_SeekableReadStream::IsEOF() {
  return false;
}

FX_FILESIZE IFX_SeekableReadStream::GetPosition() {
  return 0;
}
