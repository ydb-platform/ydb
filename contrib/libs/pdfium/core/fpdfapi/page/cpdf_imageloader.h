// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_IMAGELOADER_H_
#define CORE_FPDFAPI_PAGE_CPDF_IMAGELOADER_H_

#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CFX_DIBBase;
class CPDF_Dictionary;
class CPDF_ImageObject;
class CPDF_PageImageCache;
class CPDF_TransferFunc;
class PauseIndicatorIface;

class CPDF_ImageLoader {
 public:
  CPDF_ImageLoader();
  ~CPDF_ImageLoader();

  bool Start(const CPDF_ImageObject* pImage,
             CPDF_PageImageCache* pPageImageCache,
             const CPDF_Dictionary* pFormResource,
             const CPDF_Dictionary* pPageResource,
             bool bStdCS,
             CPDF_ColorSpace::Family eFamily,
             bool bLoadMask,
             const CFX_Size& max_size_required);
  bool Continue(PauseIndicatorIface* pPause);

  RetainPtr<CFX_DIBBase> TranslateImage(
      RetainPtr<CPDF_TransferFunc> pTransferFunc);

  const RetainPtr<CFX_DIBBase>& GetBitmap() const { return m_pBitmap; }
  const RetainPtr<CFX_DIBBase>& GetMask() const { return m_pMask; }
  uint32_t MatteColor() const { return m_MatteColor; }

 private:
  void Finish();

  uint32_t m_MatteColor = 0;
  bool m_bCached = false;
  RetainPtr<CFX_DIBBase> m_pBitmap;
  RetainPtr<CFX_DIBBase> m_pMask;
  UnownedPtr<CPDF_PageImageCache> m_pCache;
  UnownedPtr<const CPDF_ImageObject> m_pImageObject;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_IMAGELOADER_H_
