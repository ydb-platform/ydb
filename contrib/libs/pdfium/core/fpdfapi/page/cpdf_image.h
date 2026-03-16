// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_IMAGE_H_
#define CORE_FPDFAPI_PAGE_CPDF_IMAGE_H_

#include <stdint.h>

#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CFX_DIBBase;
class CFX_DIBitmap;
class CPDF_DIB;
class CPDF_Dictionary;
class CPDF_Document;
class CPDF_Page;
class CPDF_Stream;
class PauseIndicatorIface;
class IFX_SeekableReadStream;

class CPDF_Image final : public Retainable {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  static bool IsValidJpegComponent(int32_t comps);
  static bool IsValidJpegBitsPerComponent(int32_t bpc);

  // Can only be called when `IsInline()` returns true.
  void ConvertStreamToIndirectObject();

  RetainPtr<const CPDF_Dictionary> GetDict() const;
  RetainPtr<const CPDF_Stream> GetStream() const;
  RetainPtr<const CPDF_Dictionary> GetOC() const;

  // Never returns nullptr.
  CPDF_Document* GetDocument() const { return m_pDocument; }

  int32_t GetPixelHeight() const { return m_Height; }
  int32_t GetPixelWidth() const { return m_Width; }
  uint32_t GetMatteColor() const { return m_MatteColor; }
  bool IsInline() const { return m_bIsInline; }
  bool IsMask() const { return m_bIsMask; }
  bool IsInterpol() const { return m_bInterpolate; }

  RetainPtr<CPDF_DIB> CreateNewDIB() const;
  RetainPtr<CFX_DIBBase> LoadDIBBase() const;

  void SetImage(const RetainPtr<CFX_DIBitmap>& pBitmap);
  void SetJpegImage(RetainPtr<IFX_SeekableReadStream> pFile);
  void SetJpegImageInline(RetainPtr<IFX_SeekableReadStream> pFile);

  void ResetCache(CPDF_Page* pPage);

  void WillBeDestroyed();
  bool IsGoingToBeDestroyed() const { return m_bWillBeDestroyed; }

  // Returns whether to Continue() or not.
  bool StartLoadDIBBase(const CPDF_Dictionary* pFormResource,
                        const CPDF_Dictionary* pPageResource,
                        bool bStdCS,
                        CPDF_ColorSpace::Family GroupFamily,
                        bool bLoadMask,
                        const CFX_Size& max_size_required);

  // Returns whether to Continue() or not.
  bool Continue(PauseIndicatorIface* pPause);

  RetainPtr<CFX_DIBBase> DetachBitmap();
  RetainPtr<CFX_DIBBase> DetachMask();

 private:
  explicit CPDF_Image(CPDF_Document* pDoc);
  CPDF_Image(CPDF_Document* pDoc, RetainPtr<CPDF_Stream> pStream);
  CPDF_Image(CPDF_Document* pDoc, uint32_t dwStreamObjNum);
  ~CPDF_Image() override;

  void FinishInitialization();
  RetainPtr<CPDF_Dictionary> InitJPEG(pdfium::span<uint8_t> src_span);
  RetainPtr<CPDF_Dictionary> CreateXObjectImageDict(int width, int height);

  int32_t m_Height = 0;
  int32_t m_Width = 0;
  uint32_t m_MatteColor = 0;
  bool m_bIsInline = false;
  bool m_bIsMask = false;
  bool m_bInterpolate = false;
  bool m_bWillBeDestroyed = false;
  UnownedPtr<CPDF_Document> const m_pDocument;
  RetainPtr<CFX_DIBBase> m_pDIBBase;
  RetainPtr<CFX_DIBBase> m_pMask;
  RetainPtr<CPDF_Stream> m_pStream;
  RetainPtr<const CPDF_Dictionary> m_pOC;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_IMAGE_H_
