// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_folder.h"

#include <memory>

#include "build/build_config.h"
#include "core/fxcrt/ptr_util.h"
#include "core/fxcrt/unowned_ptr.h"

#if BUILDFLAG(IS_WIN)
#error "built on wrong platform"
#endif

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

class FX_PosixFolder : public FX_Folder {
 public:
  ~FX_PosixFolder() override;

  bool GetNextFile(ByteString* filename, bool* bFolder) override;

 private:
  friend class FX_Folder;
  FX_PosixFolder(const ByteString& path, DIR* dir);

  const ByteString m_Path;
  UnownedPtr<DIR> m_Dir;
};

std::unique_ptr<FX_Folder> FX_Folder::OpenFolder(const ByteString& path) {
  DIR* dir = opendir(path.c_str());
  if (!dir)
    return nullptr;

  // Private ctor.
  return pdfium::WrapUnique(new FX_PosixFolder(path, dir));
}

FX_PosixFolder::FX_PosixFolder(const ByteString& path, DIR* dir)
    : m_Path(path), m_Dir(dir) {}

FX_PosixFolder::~FX_PosixFolder() {
  closedir(m_Dir.ExtractAsDangling());
}

bool FX_PosixFolder::GetNextFile(ByteString* filename, bool* bFolder) {
  struct dirent* de = readdir(m_Dir);
  if (!de)
    return false;

  ByteString fullpath = m_Path + "/" + de->d_name;
  struct stat deStat;
  if (stat(fullpath.c_str(), &deStat) < 0)
    return false;

  *filename = de->d_name;
  *bFolder = S_ISDIR(deStat.st_mode);
  return true;
}
