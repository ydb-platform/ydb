// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_FOLDER_H_
#define CORE_FXCRT_FX_FOLDER_H_

#include <memory>

#include "core/fxcrt/bytestring.h"

class FX_Folder {
 public:
  static std::unique_ptr<FX_Folder> OpenFolder(const ByteString& path);

  virtual ~FX_Folder() = default;

  // `filename` and `folder` are required out-parameters.
  virtual bool GetNextFile(ByteString* filename, bool* bFolder) = 0;
};

#endif  // CORE_FXCRT_FX_FOLDER_H_
