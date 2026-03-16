// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_imageloader.h"

#include <utility>

#include "core/fpdfapi/page/cpdf_dib.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/page/cpdf_imageobject.h"
#include "core/fpdfapi/page/cpdf_pageimagecache.h"
#include "core/fpdfapi/page/cpdf_transferfunc.h"
#include "core/fxcrt/check.h"
#include "core/fxge/dib/cfx_dibitmap.h"

CPDF_ImageLoader::CPDF_ImageLoader() = default;

CPDF_ImageLoader::~CPDF_ImageLoader() = default;

bool CPDF_ImageLoader::Start(const CPDF_ImageObject* pImage,
                             CPDF_PageImageCache* pPageImageCache,
                             const CPDF_Dictionary* pFormResource,
                             const CPDF_Dictionary* pPageResource,
                             bool bStdCS,
                             CPDF_ColorSpace::Family eFamily,
                             bool bLoadMask,
                             const CFX_Size& max_size_required) {
  m_pCache = pPageImageCache;
  m_pImageObject = pImage;
  bool should_continue;
  if (m_pCache) {
    should_continue = m_pCache->StartGetCachedBitmap(
        m_pImageObject->GetImage(), pFormResource, pPageResource, bStdCS,
        eFamily, bLoadMask, max_size_required);
  } else {
    should_continue = m_pImageObject->GetImage()->StartLoadDIBBase(
        pFormResource, pPageResource, bStdCS, eFamily, bLoadMask,
        max_size_required);
  }
  if (!should_continue) {
    Finish();
  }
  return should_continue;
}

bool CPDF_ImageLoader::Continue(PauseIndicatorIface* pPause) {
  bool should_continue = m_pCache
                             ? m_pCache->Continue(pPause)
                             : m_pImageObject->GetImage()->Continue(pPause);
  if (!should_continue) {
    Finish();
  }
  return should_continue;
}

RetainPtr<CFX_DIBBase> CPDF_ImageLoader::TranslateImage(
    RetainPtr<CPDF_TransferFunc> pTransferFunc) {
  DCHECK(pTransferFunc);
  DCHECK(!pTransferFunc->GetIdentity());
  m_pBitmap = pTransferFunc->TranslateImage(std::move(m_pBitmap));
  if (m_bCached && m_pMask)
    m_pMask = m_pMask->Realize();
  m_bCached = false;
  return m_pBitmap;
}

void CPDF_ImageLoader::Finish() {
  if (m_pCache) {
    m_bCached = true;
    m_pBitmap = m_pCache->DetachCurBitmap();
    m_pMask = m_pCache->DetachCurMask();
    m_MatteColor = m_pCache->GetCurMatteColor();
    return;
  }
  RetainPtr<CPDF_Image> pImage = m_pImageObject->GetImage();
  m_bCached = false;
  m_pBitmap = pImage->DetachBitmap();
  m_pMask = pImage->DetachMask();
  m_MatteColor = pImage->GetMatteColor();
}
