// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JBIG2_JBIG2_SYMBOLDICT_H_
#define CORE_FXCODEC_JBIG2_JBIG2_SYMBOLDICT_H_

#include <memory>
#include <utility>
#include <vector>

#include "core/fxcodec/jbig2/JBig2_ArithDecoder.h"

class CJBig2_Image;

class CJBig2_SymbolDict {
 public:
  CJBig2_SymbolDict();
  ~CJBig2_SymbolDict();

  std::unique_ptr<CJBig2_SymbolDict> DeepCopy() const;

  void AddImage(std::unique_ptr<CJBig2_Image> image) {
    m_SDEXSYMS.push_back(std::move(image));
  }

  size_t NumImages() const { return m_SDEXSYMS.size(); }
  CJBig2_Image* GetImage(size_t index) const { return m_SDEXSYMS[index].get(); }

  const std::vector<JBig2ArithCtx>& GbContexts() const { return m_gbContexts; }
  const std::vector<JBig2ArithCtx>& GrContexts() const { return m_grContexts; }

  void SetGbContexts(std::vector<JBig2ArithCtx> gbContexts) {
    m_gbContexts = std::move(gbContexts);
  }
  void SetGrContexts(std::vector<JBig2ArithCtx> grContexts) {
    m_grContexts = std::move(grContexts);
  }

 private:
  std::vector<JBig2ArithCtx> m_gbContexts;
  std::vector<JBig2ArithCtx> m_grContexts;
  std::vector<std::unique_ptr<CJBig2_Image>> m_SDEXSYMS;
};

#endif  // CORE_FXCODEC_JBIG2_JBIG2_SYMBOLDICT_H_
