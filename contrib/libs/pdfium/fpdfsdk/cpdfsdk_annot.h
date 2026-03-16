// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_ANNOT_H_
#define FPDFSDK_CPDFSDK_ANNOT_H_

#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/mask.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"
#include "public/fpdf_fwlevent.h"

class CPDF_Page;
class CPDFSDK_BAAnnot;
class CPDFSDK_PageView;
class CPDFXFA_Widget;
class IPDF_Page;

class CPDFSDK_Annot : public Observable {
 public:
  // These methods may destroy the class that implements them when called.
  // Access through the static methods below of the same name.
  class UnsafeInputHandlers {
   public:
    virtual void OnMouseEnter(Mask<FWL_EVENTFLAG> nFlags) = 0;
    virtual void OnMouseExit(Mask<FWL_EVENTFLAG> nFlags) = 0;
    virtual bool OnLButtonDown(Mask<FWL_EVENTFLAG> nFlags,
                               const CFX_PointF& point) = 0;
    virtual bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlags,
                             const CFX_PointF& point) = 0;
    virtual bool OnLButtonDblClk(Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point) = 0;
    virtual bool OnMouseMove(Mask<FWL_EVENTFLAG> nFlags,
                             const CFX_PointF& point) = 0;
    virtual bool OnMouseWheel(Mask<FWL_EVENTFLAG> nFlags,
                              const CFX_PointF& point,
                              const CFX_Vector& delta) = 0;
    virtual bool OnRButtonDown(Mask<FWL_EVENTFLAG> nFlags,
                               const CFX_PointF& point) = 0;
    virtual bool OnRButtonUp(Mask<FWL_EVENTFLAG> nFlags,
                             const CFX_PointF& point) = 0;
    virtual bool OnChar(uint32_t nChar, Mask<FWL_EVENTFLAG> nFlags) = 0;
    virtual bool OnKeyDown(FWL_VKEYCODE nKeyCode,
                           Mask<FWL_EVENTFLAG> nFlags) = 0;
    virtual bool OnSetFocus(Mask<FWL_EVENTFLAG> nFlags) = 0;
    virtual bool OnKillFocus(Mask<FWL_EVENTFLAG> nFlags) = 0;
  };

  virtual ~CPDFSDK_Annot();

  virtual CPDFSDK_BAAnnot* AsBAAnnot();
  virtual CPDFXFA_Widget* AsXFAWidget();

  // Never returns nullptr.
  virtual UnsafeInputHandlers* GetUnsafeInputHandlers() = 0;

  virtual void OnLoad() {}
  virtual int GetLayoutOrder() const;
  virtual CPDF_Annot* GetPDFAnnot() const;
  virtual CPDF_Annot::Subtype GetAnnotSubtype() const = 0;
  virtual CFX_FloatRect GetRect() const = 0;
  virtual void OnDraw(CFX_RenderDevice* pDevice,
                      const CFX_Matrix& mtUser2Device,
                      bool bDrawAnnots) = 0;
  virtual bool DoHitTest(const CFX_PointF& point) = 0;
  virtual CFX_FloatRect GetViewBBox() = 0;
  virtual bool CanUndo() = 0;
  virtual bool CanRedo() = 0;
  virtual bool Undo() = 0;
  virtual bool Redo() = 0;
  virtual WideString GetText() = 0;
  virtual WideString GetSelectedText() = 0;
  virtual void ReplaceAndKeepSelection(const WideString& text) = 0;
  virtual void ReplaceSelection(const WideString& text) = 0;
  virtual bool SelectAllText() = 0;
  virtual bool SetIndexSelected(int index, bool selected) = 0;
  virtual bool IsIndexSelected(int index) = 0;

  // Callers must check if `pAnnot` is still valid after calling these methods,
  // before accessing them again.
  static void OnMouseEnter(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                           Mask<FWL_EVENTFLAG> nFlags);
  static void OnMouseExit(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                          Mask<FWL_EVENTFLAG> nFlags);
  static bool OnLButtonDown(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                            Mask<FWL_EVENTFLAG> nFlags,
                            const CFX_PointF& point);
  static bool OnLButtonUp(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                          Mask<FWL_EVENTFLAG> nFlags,
                          const CFX_PointF& point);
  static bool OnLButtonDblClk(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                              Mask<FWL_EVENTFLAG> nFlags,
                              const CFX_PointF& point);
  static bool OnMouseMove(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                          Mask<FWL_EVENTFLAG> nFlags,
                          const CFX_PointF& point);
  static bool OnMouseWheel(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                           Mask<FWL_EVENTFLAG> nFlags,
                           const CFX_PointF& point,
                           const CFX_Vector& delta);
  static bool OnRButtonDown(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                            Mask<FWL_EVENTFLAG> nFlags,
                            const CFX_PointF& point);
  static bool OnRButtonUp(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                          Mask<FWL_EVENTFLAG> nFlags,
                          const CFX_PointF& point);
  static bool OnChar(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                     uint32_t nChar,
                     Mask<FWL_EVENTFLAG> nFlags);
  static bool OnKeyDown(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                        FWL_VKEYCODE nKeyCode,
                        Mask<FWL_EVENTFLAG> nFlags);
  static bool OnSetFocus(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                         Mask<FWL_EVENTFLAG> nFlags);
  static bool OnKillFocus(ObservedPtr<CPDFSDK_Annot>& pAnnot,
                          Mask<FWL_EVENTFLAG> nFlags);

  // Three cases: PDF page only, XFA page only, or XFA page backed by PDF page.
  IPDF_Page* GetPage();     // Returns XFA Page if possible, else PDF page.
  CPDF_Page* GetPDFPage();  // Returns PDF page or nullptr.
  IPDF_Page* GetXFAPage();  // Returns XFA page or nullptr.

  // Never returns nullptr.
  CPDFSDK_PageView* GetPageView() const { return m_pPageView; }

 protected:
  explicit CPDFSDK_Annot(CPDFSDK_PageView* pPageView);

 private:
  UnownedPtr<CPDFSDK_PageView> const m_pPageView;
};

inline CPDFXFA_Widget* ToXFAWidget(CPDFSDK_Annot* pAnnot) {
  return pAnnot ? pAnnot->AsXFAWidget() : nullptr;
}

#endif  // FPDFSDK_CPDFSDK_ANNOT_H_
