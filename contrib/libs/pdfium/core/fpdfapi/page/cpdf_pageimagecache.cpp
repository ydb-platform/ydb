// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_pageimagecache.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <utility>
#include <vector>

#include "core/fpdfapi/page/cpdf_dib.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"

#if defined(PDF_USE_SKIA)
#include "core/fxcrt/data_vector.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#error #include "third_party/skia/include/core/SkImage.h"
#error #include "third_party/skia/include/core/SkRefCnt.h"
#endif

namespace {

struct CacheInfo {
  CacheInfo(uint32_t t, RetainPtr<const CPDF_Stream> stream)
      : time(t), pStream(std::move(stream)) {}

  uint32_t time;
  RetainPtr<const CPDF_Stream> pStream;

  bool operator<(const CacheInfo& other) const { return time < other.time; }
};

#if defined(PDF_USE_SKIA)
// Wrapper around a `CFX_DIBBase` that memoizes `RealizeSkImage()`. This is only
// safe if the underlying `CFX_DIBBase` is not mutable.
class CachedImage final : public CFX_DIBBase {
 public:
  explicit CachedImage(RetainPtr<CFX_DIBBase> image)
      : image_(std::move(image)) {
    SetFormat(image_->GetFormat());
    SetWidth(image_->GetWidth());
    SetHeight(image_->GetHeight());
    SetPitch(image_->GetPitch());

    if (image_->HasPalette()) {
      pdfium::span<const uint32_t> palette = image_->GetPaletteSpan();
      palette_ = DataVector<uint32_t>(palette.begin(), palette.end());
    }
  }

  pdfium::span<const uint8_t> GetScanline(int line) const override {
    // TODO(crbug.com/pdfium/2050): Still needed for `Realize()` call in
    // `CPDF_ImageRenderer`.
    return image_->GetScanline(line);
  }

  bool SkipToScanline(int line, PauseIndicatorIface* pause) const override {
    return image_->SkipToScanline(line, pause);
  }

  size_t GetEstimatedImageMemoryBurden() const override {
    // A better estimate would account for realizing the `SkImage`.
    return image_->GetEstimatedImageMemoryBurden();
  }

#if BUILDFLAG(IS_WIN) || defined(PDF_USE_SKIA)
  RetainPtr<const CFX_DIBitmap> RealizeIfNeeded() const override {
    return image_->RealizeIfNeeded();
  }
#endif

  sk_sp<SkImage> RealizeSkImage() const override {
    if (!cached_skia_image_) {
      cached_skia_image_ = image_->RealizeSkImage();
    }
    return cached_skia_image_;
  }

 private:
  RetainPtr<CFX_DIBBase> image_;
  mutable sk_sp<SkImage> cached_skia_image_;
};
#endif  // defined(PDF_USE_SKIA)

// Makes a `CachedImage` backed by `image` if Skia is the default renderer,
// otherwise return the image itself. `realize_hint` indicates whether it would
// be beneficial to realize `image` before caching.
RetainPtr<CFX_DIBBase> MakeCachedImage(RetainPtr<CFX_DIBBase> image,
                                       bool realize_hint) {
#if defined(PDF_USE_SKIA)
  if (CFX_DefaultRenderDevice::UseSkiaRenderer()) {
    // Ignore `realize_hint`, as `RealizeSkImage()` doesn't benefit from it.
    return pdfium::MakeRetain<CachedImage>(std::move(image));
  }
#endif  // defined(PDF_USE_SKIA)
  return realize_hint ? image->Realize() : image;
}

}  // namespace

CPDF_PageImageCache::CPDF_PageImageCache(CPDF_Page* pPage) : m_pPage(pPage) {}

CPDF_PageImageCache::~CPDF_PageImageCache() = default;

void CPDF_PageImageCache::CacheOptimization(int32_t dwLimitCacheSize) {
  if (m_nCacheSize <= (uint32_t)dwLimitCacheSize)
    return;

  uint32_t nCount = fxcrt::CollectionSize<uint32_t>(m_ImageCache);
  std::vector<CacheInfo> cache_info;
  cache_info.reserve(nCount);
  for (const auto& it : m_ImageCache) {
    cache_info.emplace_back(it.second->GetTimeCount(),
                            it.second->GetImage()->GetStream());
  }
  std::sort(cache_info.begin(), cache_info.end());

  // Check if time value is about to roll over and reset all entries.
  // The comparison is legal because uint32_t is an unsigned type.
  uint32_t nTimeCount = m_nTimeCount;
  if (nTimeCount + 1 < nTimeCount) {
    for (uint32_t i = 0; i < nCount; i++)
      m_ImageCache[cache_info[i].pStream]->SetTimeCount(i);
    m_nTimeCount = nCount;
  }

  size_t i = 0;
  while (i + 15 < nCount)
    ClearImageCacheEntry(cache_info[i++].pStream);

  while (i < nCount && m_nCacheSize > (uint32_t)dwLimitCacheSize)
    ClearImageCacheEntry(cache_info[i++].pStream);
}

void CPDF_PageImageCache::ClearImageCacheEntry(const CPDF_Stream* pStream) {
  auto it = m_ImageCache.find(pStream);
  if (it == m_ImageCache.end())
    return;

  m_nCacheSize -= it->second->EstimateSize();

  // Avoid leaving `m_pCurImageCacheEntry` as a dangling pointer when `it` is
  // about to be deleted.
  if (m_pCurImageCacheEntry.Get() == it->second.get()) {
    DCHECK(!m_pCurImageCacheEntry.IsOwned());
    m_pCurImageCacheEntry.Reset();
  }
  m_ImageCache.erase(it);
}

bool CPDF_PageImageCache::StartGetCachedBitmap(
    RetainPtr<CPDF_Image> pImage,
    const CPDF_Dictionary* pFormResources,
    const CPDF_Dictionary* pPageResources,
    bool bStdCS,
    CPDF_ColorSpace::Family eFamily,
    bool bLoadMask,
    const CFX_Size& max_size_required) {
  // A cross-document image may have come from the embedder.
  if (m_pPage->GetDocument() != pImage->GetDocument())
    return false;

  RetainPtr<const CPDF_Stream> pStream = pImage->GetStream();
  const auto it = m_ImageCache.find(pStream);
  m_bCurFindCache = it != m_ImageCache.end();
  if (m_bCurFindCache) {
    m_pCurImageCacheEntry = it->second.get();
  } else {
    m_pCurImageCacheEntry = std::make_unique<Entry>(std::move(pImage));
  }
  CPDF_DIB::LoadState ret = m_pCurImageCacheEntry->StartGetCachedBitmap(
      this, pFormResources, pPageResources, bStdCS, eFamily, bLoadMask,
      max_size_required);
  if (ret == CPDF_DIB::LoadState::kContinue)
    return true;

  m_nTimeCount++;
  if (!m_bCurFindCache)
    m_ImageCache[pStream] = m_pCurImageCacheEntry.Release();

  if (ret == CPDF_DIB::LoadState::kFail)
    m_nCacheSize += m_pCurImageCacheEntry->EstimateSize();

  return false;
}

bool CPDF_PageImageCache::Continue(PauseIndicatorIface* pPause) {
  bool ret = m_pCurImageCacheEntry->Continue(pPause, this);
  if (ret)
    return true;

  m_nTimeCount++;
  if (!m_bCurFindCache) {
    m_ImageCache[m_pCurImageCacheEntry->GetImage()->GetStream()] =
        m_pCurImageCacheEntry.Release();
  }
  m_nCacheSize += m_pCurImageCacheEntry->EstimateSize();
  return false;
}

void CPDF_PageImageCache::ResetBitmapForImage(RetainPtr<CPDF_Image> pImage) {
  RetainPtr<const CPDF_Stream> pStream = pImage->GetStream();
  const auto it = m_ImageCache.find(pStream);
  if (it == m_ImageCache.end())
    return;

  Entry* pEntry = it->second.get();
  m_nCacheSize -= pEntry->EstimateSize();
  pEntry->Reset();
  m_nCacheSize += pEntry->EstimateSize();
}

uint32_t CPDF_PageImageCache::GetCurMatteColor() const {
  return m_pCurImageCacheEntry->GetMatteColor();
}

RetainPtr<CFX_DIBBase> CPDF_PageImageCache::DetachCurBitmap() {
  return m_pCurImageCacheEntry->DetachBitmap();
}

RetainPtr<CFX_DIBBase> CPDF_PageImageCache::DetachCurMask() {
  return m_pCurImageCacheEntry->DetachMask();
}

CPDF_PageImageCache::Entry::Entry(RetainPtr<CPDF_Image> pImage)
    : m_pImage(std::move(pImage)) {}

CPDF_PageImageCache::Entry::~Entry() = default;

void CPDF_PageImageCache::Entry::Reset() {
  m_pCachedBitmap.Reset();
  CalcSize();
}

RetainPtr<CFX_DIBBase> CPDF_PageImageCache::Entry::DetachBitmap() {
  return std::move(m_pCurBitmap);
}

RetainPtr<CFX_DIBBase> CPDF_PageImageCache::Entry::DetachMask() {
  return std::move(m_pCurMask);
}

CPDF_DIB::LoadState CPDF_PageImageCache::Entry::StartGetCachedBitmap(
    CPDF_PageImageCache* pPageImageCache,
    const CPDF_Dictionary* pFormResources,
    const CPDF_Dictionary* pPageResources,
    bool bStdCS,
    CPDF_ColorSpace::Family eFamily,
    bool bLoadMask,
    const CFX_Size& max_size_required) {
  if (m_pCachedBitmap && IsCacheValid(max_size_required)) {
    m_pCurBitmap = m_pCachedBitmap;
    m_pCurMask = m_pCachedMask;
    return CPDF_DIB::LoadState::kSuccess;
  }

  m_pCurBitmap = m_pImage->CreateNewDIB();
  CPDF_DIB::LoadState ret = m_pCurBitmap.AsRaw<CPDF_DIB>()->StartLoadDIBBase(
      true, pFormResources, pPageResources, bStdCS, eFamily, bLoadMask,
      max_size_required);
  m_bCachedSetMaxSizeRequired =
      (max_size_required.width != 0 && max_size_required.height != 0);
  if (ret == CPDF_DIB::LoadState::kContinue)
    return CPDF_DIB::LoadState::kContinue;

  if (ret == CPDF_DIB::LoadState::kSuccess)
    ContinueGetCachedBitmap(pPageImageCache);
  else
    m_pCurBitmap.Reset();
  return CPDF_DIB::LoadState::kFail;
}

bool CPDF_PageImageCache::Entry::Continue(
    PauseIndicatorIface* pPause,
    CPDF_PageImageCache* pPageImageCache) {
  CPDF_DIB::LoadState ret =
      m_pCurBitmap.AsRaw<CPDF_DIB>()->ContinueLoadDIBBase(pPause);
  if (ret == CPDF_DIB::LoadState::kContinue)
    return true;

  if (ret == CPDF_DIB::LoadState::kSuccess)
    ContinueGetCachedBitmap(pPageImageCache);
  else
    m_pCurBitmap.Reset();
  return false;
}

void CPDF_PageImageCache::Entry::ContinueGetCachedBitmap(
    CPDF_PageImageCache* pPageImageCache) {
  m_MatteColor = m_pCurBitmap.AsRaw<CPDF_DIB>()->GetMatteColor();
  m_pCurMask = m_pCurBitmap.AsRaw<CPDF_DIB>()->DetachMask();
  m_dwTimeCount = pPageImageCache->GetTimeCount();
  if (m_pCurBitmap->GetPitch() * m_pCurBitmap->GetHeight() < kHugeImageSize) {
    m_pCachedBitmap = MakeCachedImage(m_pCurBitmap, /*realize_hint=*/true);
    m_pCurBitmap.Reset();
  } else {
    m_pCachedBitmap = MakeCachedImage(m_pCurBitmap, /*realize_hint=*/false);
  }
  if (m_pCurMask) {
    m_pCachedMask = MakeCachedImage(m_pCurMask, /*realize_hint=*/true);
    m_pCurMask.Reset();
  }
  m_pCurBitmap = m_pCachedBitmap;
  m_pCurMask = m_pCachedMask;
  CalcSize();
}

void CPDF_PageImageCache::Entry::CalcSize() {
  m_dwCacheSize = 0;
  if (m_pCachedBitmap)
    m_dwCacheSize += m_pCachedBitmap->GetEstimatedImageMemoryBurden();
  if (m_pCachedMask)
    m_dwCacheSize += m_pCachedMask->GetEstimatedImageMemoryBurden();
}

bool CPDF_PageImageCache::Entry::IsCacheValid(
    const CFX_Size& max_size_required) const {
  if (!m_bCachedSetMaxSizeRequired) {
    return true;
  }
  if (max_size_required.width == 0 && max_size_required.height == 0) {
    return false;
  }

  return (m_pCachedBitmap->GetWidth() >= max_size_required.width) &&
         (m_pCachedBitmap->GetHeight() >= max_size_required.height);
}
