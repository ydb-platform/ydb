// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_WIDGET_H_
#define FPDFSDK_CPDFSDK_WIDGET_H_

#include <optional>

#include "core/fpdfdoc/cpdf_aaction.h"
#include "core/fpdfdoc/cpdf_action.h"
#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fpdfdoc/cpdf_formfield.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"
#include "core/fxge/cfx_color.h"
#include "fpdfsdk/cpdfsdk_baannot.h"

class CFFL_InteractiveFormFiller;
class CFX_RenderDevice;
class CPDF_Annot;
class CPDF_FormControl;
class CPDF_FormField;
class CPDFSDK_FormFillEnvironment;
class CPDFSDK_InteractiveForm;
class CPDFSDK_PageView;
struct CFFL_FieldAction;

#ifdef PDF_ENABLE_XFA
class CXFA_FFWidget;
class CXFA_FFWidgetHandler;

enum PDFSDK_XFAAActionType {
  PDFSDK_XFA_Click = 0,
  PDFSDK_XFA_Full,
  PDFSDK_XFA_PreOpen,
  PDFSDK_XFA_PostOpen
};
#endif  // PDF_ENABLE_XFA

class CPDFSDK_Widget final : public CPDFSDK_BAAnnot {
 public:
  enum ValueChanged : bool { kValueUnchanged = false, kValueChanged = true };

  CPDFSDK_Widget(CPDF_Annot* pAnnot,
                 CPDFSDK_PageView* pPageView,
                 CPDFSDK_InteractiveForm* pInteractiveForm);
  ~CPDFSDK_Widget() override;

  // CPDFSDK_BAAnnot:
  void OnLoad() override;
  CPDF_Action GetAAction(CPDF_AAction::AActionType eAAT) override;
  bool IsAppearanceValid() override;
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
  void DrawAppearance(CFX_RenderDevice* pDevice,
                      const CFX_Matrix& mtUser2Device,
                      CPDF_Annot::AppearanceMode mode) override;

  bool IsSignatureWidget() const;
  void SetRect(const CFX_FloatRect& rect);
  FormFieldType GetFieldType() const;
  int GetFieldFlags() const;
  int GetRotate() const;

  std::optional<FX_COLORREF> GetFillColor() const;
  std::optional<FX_COLORREF> GetBorderColor() const;
  std::optional<FX_COLORREF> GetTextColor() const;
  float GetFontSize() const;

  int GetSelectedIndex(int nIndex) const;
  WideString GetValue() const;
  WideString GetExportValue() const;
  WideString GetOptionLabel(int nIndex) const;
  WideString GetSelectExportText(int nIndex) const;

  int CountOptions() const;
  bool IsOptionSelected(int nIndex) const;
  int GetTopVisibleIndex() const;
  bool IsChecked() const;
  int GetAlignment() const;
  int GetMaxLen() const;

  void SetCheck(bool bChecked);
  void SetValue(const WideString& sValue);
  void SetOptionSelection(int index);
  void ClearSelection();
  void SetTopVisibleIndex(int index);

#ifdef PDF_ENABLE_XFA
  CXFA_FFWidget* GetMixXFAWidget() const;
  bool HasXFAAAction(PDFSDK_XFAAActionType eXFAAAT) const;
  bool OnXFAAAction(PDFSDK_XFAAActionType eXFAAAT,
                    CFFL_FieldAction* data,
                    const CPDFSDK_PageView* pPageView);
  void Synchronize(bool bSynchronizeElse);
  // TODO(thestig): Figure out if the parameter should be used or removed.
  void ResetXFAAppearance(ValueChanged bValueChanged);
#endif  // PDF_ENABLE_XFA

  void ResetAppearance(std::optional<WideString> sValue,
                       ValueChanged bValueChanged);
  void ResetFieldAppearance();
  void UpdateField();
  std::optional<WideString> OnFormat();

  bool OnAAction(CPDF_AAction::AActionType type,
                 CFFL_FieldAction* data,
                 const CPDFSDK_PageView* pPageView);

  CPDF_FormField* GetFormField() const;
  CPDF_FormControl* GetFormControl() const;

  void DrawShadow(CFX_RenderDevice* pDevice, CPDFSDK_PageView* pPageView);

  void SetAppModified();
  void ClearAppModified();
  bool IsAppModified() const;

  uint32_t GetAppearanceAge() const { return m_nAppearanceAge; }
  uint32_t GetValueAge() const { return m_nValueAge; }

  bool IsWidgetAppearanceValid(CPDF_Annot::AppearanceMode mode) const;
  bool IsPushHighlighted() const;
  CFX_Matrix GetMatrix() const;
  CFX_FloatRect GetClientRect() const;
  CFX_FloatRect GetRotatedRect() const;
  CFX_Color GetTextPWLColor() const;
  CFX_Color GetBorderPWLColor() const;
  CFX_Color GetFillPWLColor() const;

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

  CFFL_InteractiveFormFiller* GetInteractiveFormFiller();

#ifdef PDF_ENABLE_XFA
  CXFA_FFWidgetHandler* GetXFAWidgetHandler() const;
  CXFA_FFWidget* GetGroupMixXFAWidget() const;
  WideString GetName() const;
  bool HandleXFAAAction(CPDF_AAction::AActionType type,
                        CFFL_FieldAction* data,
                        CPDFSDK_FormFillEnvironment* pFormFillEnv);
#endif  // PDF_ENABLE_XFA

  UnownedPtr<CPDFSDK_InteractiveForm> const m_pInteractiveForm;
  bool m_bAppModified = false;
  uint32_t m_nAppearanceAge = 0;
  uint32_t m_nValueAge = 0;
};

inline CPDFSDK_Widget* ToCPDFSDKWidget(CPDFSDK_Annot* pAnnot) {
  return pAnnot && pAnnot->GetAnnotSubtype() == CPDF_Annot::Subtype::WIDGET
             ? static_cast<CPDFSDK_Widget*>(pAnnot)
             : nullptr;
}

#endif  // FPDFSDK_CPDFSDK_WIDGET_H_
