// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXGE_WIN32_CFX_PSFONTTRACKER_H_
#define CORE_FXGE_WIN32_CFX_PSFONTTRACKER_H_

#include <stdint.h>

#include <functional>
#include <set>

#include "core/fxcrt/unowned_ptr.h"

class CFX_Font;

class CFX_PSFontTracker {
 public:
  CFX_PSFontTracker();
  ~CFX_PSFontTracker();

  void AddFontObject(const CFX_Font* font);
  bool SeenFontObject(const CFX_Font* font) const;

 private:
  // Tracks font objects via tags, so if two CFX_Font instances are for the same
  // PDF object, then they are deduplicated.
  std::set<uint64_t> seen_font_tags_;

  // For fonts without valid tags, e.g. ones created in-memory, track them by
  // pointer.
  std::set<UnownedPtr<const CFX_Font>, std::less<>> seen_font_ptrs_;
};

#endif  // CORE_FXGE_WIN32_CFX_PSFONTTRACKER_H_
