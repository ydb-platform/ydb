// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_folder.h"

#include <memory>

#include "build/build_config.h"
#include "core/fxcrt/ptr_util.h"

#if !BUILDFLAG(IS_WIN)
#error "built on wrong platform"
#endif

#include <direct.h>

class FX_WindowsFolder : public FX_Folder {
 public:
  ~FX_WindowsFolder() override;
  bool GetNextFile(ByteString* filename, bool* bFolder) override;

 private:
  friend class FX_Folder;
  FX_WindowsFolder();

  HANDLE m_Handle = INVALID_HANDLE_VALUE;
  bool m_bReachedEnd = false;
  WIN32_FIND_DATAA m_FindData;
};

std::unique_ptr<FX_Folder> FX_Folder::OpenFolder(const ByteString& path) {
  // Private ctor.
  auto handle = pdfium::WrapUnique(new FX_WindowsFolder());
  ByteString search_path = path + "/*.*";
  handle->m_Handle =
      FindFirstFileExA(search_path.c_str(), FindExInfoStandard,
                       &handle->m_FindData, FindExSearchNameMatch, nullptr, 0);
  if (handle->m_Handle == INVALID_HANDLE_VALUE)
    return nullptr;

  return handle;
}

FX_WindowsFolder::FX_WindowsFolder() = default;

FX_WindowsFolder::~FX_WindowsFolder() {
  if (m_Handle != INVALID_HANDLE_VALUE)
    FindClose(m_Handle);
}

bool FX_WindowsFolder::GetNextFile(ByteString* filename, bool* bFolder) {
  if (m_bReachedEnd)
    return false;

  *filename = m_FindData.cFileName;
  *bFolder = !!(m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
  if (!FindNextFileA(m_Handle, &m_FindData))
    m_bReachedEnd = true;
  return true;
}
