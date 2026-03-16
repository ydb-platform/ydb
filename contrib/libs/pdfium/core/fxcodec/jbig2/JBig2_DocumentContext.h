// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JBIG2_JBIG2_DOCUMENTCONTEXT_H_
#define CORE_FXCODEC_JBIG2_JBIG2_DOCUMENTCONTEXT_H_

#include <list>
#include <memory>
#include <utility>

class CJBig2_SymbolDict;

// Cache is keyed by both the key of a stream and an index within the stream.
using CJBig2_CompoundKey = std::pair<uint64_t, uint32_t>;
using CJBig2_CachePair =
    std::pair<CJBig2_CompoundKey, std::unique_ptr<CJBig2_SymbolDict>>;

// Holds per-document JBig2 related data.
class JBig2_DocumentContext {
 public:
  JBig2_DocumentContext();
  ~JBig2_DocumentContext();

  std::list<CJBig2_CachePair>* GetSymbolDictCache() {
    return &m_SymbolDictCache;
  }

 private:
  std::list<CJBig2_CachePair> m_SymbolDictCache;
};

#endif  // CORE_FXCODEC_JBIG2_JBIG2_DOCUMENTCONTEXT_H_
