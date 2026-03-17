// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_pagemodule.h"

#include "core/fpdfapi/font/cpdf_fontglobals.h"
#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fpdfapi/page/cpdf_streamcontentparser.h"

// static
void CPDF_PageModule::Create() {
  CPDF_ColorSpace::InitializeGlobals();
  CPDF_FontGlobals::Create();
  CPDF_FontGlobals::GetInstance()->LoadEmbeddedMaps();
  CPDF_StreamContentParser::InitializeGlobals();
}

// static
void CPDF_PageModule::Destroy() {
  CPDF_StreamContentParser::DestroyGlobals();
  CPDF_FontGlobals::Destroy();
  CPDF_ColorSpace::DestroyGlobals();
}
