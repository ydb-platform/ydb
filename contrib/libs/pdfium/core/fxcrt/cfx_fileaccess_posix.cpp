// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/cfx_fileaccess_posix.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <memory>

#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/numerics/safe_conversions.h"

#ifndef O_BINARY
#define O_BINARY 0
#endif  // O_BINARY

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif  // O_LARGEFILE

// static
std::unique_ptr<FileAccessIface> FileAccessIface::Create() {
  return std::make_unique<CFX_FileAccess_Posix>();
}

CFX_FileAccess_Posix::CFX_FileAccess_Posix() : m_nFD(-1) {}

CFX_FileAccess_Posix::~CFX_FileAccess_Posix() {
  Close();
}

bool CFX_FileAccess_Posix::Open(ByteStringView fileName) {
  if (m_nFD > -1)
    return false;

  // TODO(tsepez): check usage of c_str() below.
  m_nFD =
      open(fileName.unterminated_c_str(), O_BINARY | O_LARGEFILE | O_RDONLY);
  return m_nFD > -1;
}

void CFX_FileAccess_Posix::Close() {
  if (m_nFD < 0) {
    return;
  }
  close(m_nFD);
  m_nFD = -1;
}

FX_FILESIZE CFX_FileAccess_Posix::GetSize() const {
  if (m_nFD < 0) {
    return 0;
  }
  struct stat s = {};
  fstat(m_nFD, &s);
  return pdfium::checked_cast<FX_FILESIZE>(s.st_size);
}

FX_FILESIZE CFX_FileAccess_Posix::GetPosition() const {
  if (m_nFD < 0) {
    return (FX_FILESIZE)-1;
  }
  return lseek(m_nFD, 0, SEEK_CUR);
}

FX_FILESIZE CFX_FileAccess_Posix::SetPosition(FX_FILESIZE pos) {
  if (m_nFD < 0) {
    return (FX_FILESIZE)-1;
  }
  return lseek(m_nFD, pos, SEEK_SET);
}

size_t CFX_FileAccess_Posix::Read(pdfium::span<uint8_t> buffer) {
  if (m_nFD < 0) {
    return 0;
  }
  return read(m_nFD, buffer.data(), buffer.size());
}

size_t CFX_FileAccess_Posix::Write(pdfium::span<const uint8_t> buffer) {
  if (m_nFD < 0) {
    return 0;
  }
  return write(m_nFD, buffer.data(), buffer.size());
}

size_t CFX_FileAccess_Posix::ReadPos(pdfium::span<uint8_t> buffer,
                                     FX_FILESIZE pos) {
  if (m_nFD < 0) {
    return 0;
  }
  if (pos >= GetSize()) {
    return 0;
  }
  if (SetPosition(pos) == (FX_FILESIZE)-1) {
    return 0;
  }
  return Read(buffer);
}

bool CFX_FileAccess_Posix::Flush() {
  if (m_nFD < 0)
    return false;

  return fsync(m_nFD) > -1;
}

bool CFX_FileAccess_Posix::Truncate(FX_FILESIZE szFile) {
  if (m_nFD < 0)
    return false;

  return !ftruncate(m_nFD, szFile);
}
