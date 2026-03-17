// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_PAGEMODULE_H_
#define CORE_FPDFAPI_PAGE_CPDF_PAGEMODULE_H_

// TODO(thestig): Replace with class with standalone functions without polluting
// the global namespace.
class CPDF_PageModule {
 public:
  static void Create();
  static void Destroy();
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_PAGEMODULE_H_
