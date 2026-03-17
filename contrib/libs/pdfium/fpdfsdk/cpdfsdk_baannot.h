// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_BAANNOT_H_
#define FPDFSDK_CPDFSDK_BAANNOT_H_

#include "core/fpdfdoc/cpdf_aaction.h"
#include "core/fpdfdoc/cpdf_action.h"
#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_renderdevice.h"
#include "fpdfsdk/cpdfsdk_annot.h"

class CFX_Matrix;
class CPDF_Dictionary;
class CPDFSDK_PageView;

class CPDFSDK_BAAnnot : public CPDFSDK_Annot,
                        CPDFSDK_Annot::UnsafeInputHandlers {
 public:
  CPDFSDK_BAAnnot(CPDF_Annot* pAnnot, CPDFSDK_PageView* pPageView);
  ~CPDFSDK_BAAnnot() override;

  // CPDFSDK_Annot:
  CPDFSDK_BAAnnot* AsBAAnnot() override;
  CPDFSDK_Annot::UnsafeInputHandlers* GetUnsafeInputHandlers() override;
  CPDF_Annot::Subtype GetAnnotSubtype() const override;
  CFX_FloatRect GetRect() const override;
  CPDF_Annot* GetPDFAnnot() const override;
  int GetLayoutOrder() const override;
  void OnDraw(CFX_RenderDevice* pDevice,
              const CFX_Matrix& mtUser2Device,
              bool bDrawAnnots) override;
  bool DoHitTest(const CFX_PointF& point) override;
  CFX_FloatRect GetViewBBox() override;
  bool CanUndo() override;
  bool CanRedo() override;
  bool Undo() override;
  bool Redo() override;
  WideString GetText() override;
  WideString GetSelectedText() override;
  void ReplaceAndKeepSelection(const WideString& text) override;
  void ReplaceSelection(const WideString& text) override;
  bool SelectAllText() override;
  bool SetIndexSelected(int index, bool selected) override;
  bool IsIndexSelected(int index) override;

  virtual CPDF_Action GetAAction(CPDF_AAction::AActionType eAAT);
  virtual bool IsAppearanceValid();
  virtual void DrawAppearance(CFX_RenderDevice* pDevice,
                              const CFX_Matrix& mtUser2Device,
                              CPDF_Annot::AppearanceMode mode);

  void SetAnnotName(const WideString& sName);
  WideString GetAnnotName() const;

  void SetFlags(uint32_t nFlags);
  uint32_t GetFlags() const;

  void SetAppStateOff();
  ByteString GetAppState() const;

  void SetBorderWidth(int nWidth);
  int GetBorderWidth() const;

  void SetBorderStyle(BorderStyle nStyle);
  BorderStyle GetBorderStyle() const;

  bool IsVisible() const;

  CPDF_Action GetAction() const;
  CPDF_AAction GetAAction() const;
  CPDF_Dest GetDestination() const;

 protected:
  const CPDF_Dictionary* GetAnnotDict() const;
  RetainPtr<CPDF_Dictionary> GetMutableAnnotDict();
  RetainPtr<CPDF_Dictionary> GetAPDict();
  void ClearCachedAnnotAP();
  bool IsFocusableAnnot(const CPDF_Annot::Subtype& annot_type) const;

 private:
  // CPDFSDK_Annot::UnsafeInputHandlers:
  void OnMouseEnter(Mask<FWL_EVENTFLAG> nFlags) override;
  void OnMouseExit(Mask<FWL_EVENTFLAG> nFlags) override;
  bool OnLButtonDown(Mask<FWL_EVENTFLAG> nFlags,
                     const CFX_PointF& point) override;
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlags,
                   const CFX_PointF& point) override;
  bool OnLButtonDblClk(Mask<FWL_EVENTFLAG> nFlags,
                       const CFX_PointF& point) override;
  bool OnMouseMove(Mask<FWL_EVENTFLAG> nFlags,
                   const CFX_PointF& point) override;
  bool OnMouseWheel(Mask<FWL_EVENTFLAG> nFlags,
                    const CFX_PointF& point,
                    const CFX_Vector& delta) override;
  bool OnRButtonDown(Mask<FWL_EVENTFLAG> nFlags,
                     const CFX_PointF& point) override;
  bool OnRButtonUp(Mask<FWL_EVENTFLAG> nFlags,
                   const CFX_PointF& point) override;
  bool OnChar(uint32_t nChar, Mask<FWL_EVENTFLAG> nFlags) override;
  bool OnKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlags) override;
  bool OnSetFocus(Mask<FWL_EVENTFLAG> nFlags) override;
  bool OnKillFocus(Mask<FWL_EVENTFLAG> nFlags) override;

  void SetOpenState(bool bOpenState);
  void UpdateAnnotRects();
  void InvalidateRect();

  bool is_focused_ = false;
  UnownedPtr<CPDF_Annot> const m_pAnnot;
};

#endif  // FPDFSDK_CPDFSDK_BAANNOT_H_
