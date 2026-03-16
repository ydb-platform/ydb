// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxge/win32/cfx_psfonttracker.h"

#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxge/cfx_font.h"

CFX_PSFontTracker::CFX_PSFontTracker() = default;

CFX_PSFontTracker::~CFX_PSFontTracker() = default;

void CFX_PSFontTracker::AddFontObject(const CFX_Font* font) {
  uint64_t tag = font->GetObjectTag();
  [[maybe_unused]] bool inserted;
  if (tag != 0) {
    inserted = seen_font_tags_.insert(tag).second;
  } else {
    inserted = seen_font_ptrs_.insert(UnownedPtr<const CFX_Font>(font)).second;
  }
  DCHECK(inserted);
}

bool CFX_PSFontTracker::SeenFontObject(const CFX_Font* font) const {
  uint64_t tag = font->GetObjectTag();
  if (tag != 0)
    return pdfium::Contains(seen_font_tags_, tag);
  return pdfium::Contains(seen_font_ptrs_, font);
}
