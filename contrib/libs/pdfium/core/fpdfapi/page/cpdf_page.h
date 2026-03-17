// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_PAGE_H_
#define CORE_FPDFAPI_PAGE_CPDF_PAGE_H_

#include <memory>
#include <optional>
#include <utility>

#include "core/fpdfapi/page/cpdf_pageobjectholder.h"
#include "core/fpdfapi/page/ipdf_page.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Array;
class CPDF_Dictionary;
class CPDF_Document;
class CPDF_Object;
class CPDF_PageImageCache;

class CPDF_Page final : public IPDF_Page, public CPDF_PageObjectHolder {
 public:
  // Caller implements as desired, exists here due to layering.
  class View : public Observable {
   public:
    virtual void ClearPage(CPDF_Page* pPage) = 0;
  };

  // Data for the render layer to attach to this page.
  class RenderContextIface {
   public:
    virtual ~RenderContextIface() = default;
  };

  class RenderContextClearer {
   public:
    FX_STACK_ALLOCATED();
    explicit RenderContextClearer(CPDF_Page* pPage);
    ~RenderContextClearer();

   private:
    UnownedPtr<CPDF_Page> const m_pPage;
  };

  CONSTRUCT_VIA_MAKE_RETAIN;

  // IPDF_Page:
  CPDF_Page* AsPDFPage() override;
  CPDFXFA_Page* AsXFAPage() override;
  CPDF_Document* GetDocument() const override;
  float GetPageWidth() const override;
  float GetPageHeight() const override;
  CFX_Matrix GetDisplayMatrix(const FX_RECT& rect, int iRotate) const override;
  std::optional<CFX_PointF> DeviceToPage(
      const FX_RECT& rect,
      int rotate,
      const CFX_PointF& device_point) const override;
  std::optional<CFX_PointF> PageToDevice(
      const FX_RECT& rect,
      int rotate,
      const CFX_PointF& page_point) const override;

  // CPDF_PageObjectHolder:
  bool IsPage() const override;

  void ParseContent();
  const CFX_SizeF& GetPageSize() const { return m_PageSize; }
  const CFX_Matrix& GetPageMatrix() const { return m_PageMatrix; }
  int GetPageRotation() const;

  RetainPtr<CPDF_Array> GetOrCreateAnnotsArray();
  RetainPtr<CPDF_Array> GetMutableAnnotsArray();
  RetainPtr<const CPDF_Array> GetAnnotsArray() const;

  void AddPageImageCache();
  CPDF_PageImageCache* GetPageImageCache() { return m_pPageImageCache.get(); }
  RenderContextIface* GetRenderContext() { return m_pRenderContext.get(); }

  // `pContext` cannot be null. `SetRenderContext()` cannot be called if the
  // page already has a render context. Use `ClearRenderContext()` to reset the
  // render context.
  void SetRenderContext(std::unique_ptr<RenderContextIface> pContext);
  void ClearRenderContext();

  void SetView(View* pView) { m_pView.Reset(pView); }
  void ClearView();
  void UpdateDimensions();

 private:
  CPDF_Page(CPDF_Document* pDocument, RetainPtr<CPDF_Dictionary> pPageDict);
  ~CPDF_Page() override;

  RetainPtr<CPDF_Object> GetMutablePageAttr(const ByteString& name);
  RetainPtr<const CPDF_Object> GetPageAttr(const ByteString& name) const;
  CFX_FloatRect GetBox(const ByteString& name) const;

  CFX_SizeF m_PageSize;
  CFX_Matrix m_PageMatrix;
  UnownedPtr<CPDF_Document> const m_pPDFDocument;
  std::unique_ptr<CPDF_PageImageCache> m_pPageImageCache;
  std::unique_ptr<RenderContextIface> m_pRenderContext;
  ObservedPtr<View> m_pView;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_PAGE_H_
