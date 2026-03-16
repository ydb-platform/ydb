// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/cpdfsdk_widget.h"

#include "constants/access_permissions.h"
#include "constants/annotation_common.h"
#include "constants/appearance.h"
#include "constants/form_flags.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfdoc/cpdf_bafontmap.h"
#include "core/fpdfdoc/cpdf_defaultappearance.h"
#include "core/fpdfdoc/cpdf_formcontrol.h"
#include "core/fpdfdoc/cpdf_formfield.h"
#include "core/fpdfdoc/cpdf_iconfit.h"
#include "core/fpdfdoc/cpdf_interactiveform.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/notreached.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"
#include "fpdfsdk/cpdfsdk_appstream.h"
#include "fpdfsdk/cpdfsdk_formfillenvironment.h"
#include "fpdfsdk/cpdfsdk_interactiveform.h"
#include "fpdfsdk/cpdfsdk_pageview.h"
#include "fpdfsdk/formfiller/cffl_fieldaction.h"
#include "fpdfsdk/pwl/cpwl_edit.h"

#ifdef PDF_ENABLE_XFA
#include "fpdfsdk/fpdfxfa/cpdfxfa_context.h"
#include "xfa/fxfa/cxfa_eventparam.h"
#include "xfa/fxfa/cxfa_ffdocview.h"
#include "xfa/fxfa/cxfa_ffwidget.h"
#include "xfa/fxfa/cxfa_ffwidgethandler.h"
#include "xfa/fxfa/parser/cxfa_node.h"
#endif  // PDF_ENABLE_XFA

CPDFSDK_Widget::CPDFSDK_Widget(CPDF_Annot* pAnnot,
                               CPDFSDK_PageView* pPageView,
                               CPDFSDK_InteractiveForm* pInteractiveForm)
    : CPDFSDK_BAAnnot(pAnnot, pPageView),
      m_pInteractiveForm(pInteractiveForm) {}

CPDFSDK_Widget::~CPDFSDK_Widget() {
  GetInteractiveFormFiller()->OnDelete(this);
  m_pInteractiveForm->RemoveMap(GetFormControl());
}

#ifdef PDF_ENABLE_XFA
CXFA_FFWidget* CPDFSDK_Widget::GetMixXFAWidget() const {
  CPDF_Document::Extension* pContext =
      GetPageView()->GetFormFillEnv()->GetDocExtension();
  if (!pContext || !pContext->ContainsExtensionForegroundForm())
    return nullptr;

  CXFA_FFDocView* pDocView =
      static_cast<CPDFXFA_Context*>(pContext)->GetXFADocView();
  if (!pDocView)
    return nullptr;

  WideString sName;
  if (GetFieldType() == FormFieldType::kRadioButton) {
    sName = GetAnnotName();
    if (sName.IsEmpty())
      sName = GetName();
  } else {
    sName = GetName();
  }

  if (sName.IsEmpty())
    return nullptr;

  return pDocView->GetWidgetByName(sName, nullptr);
}

CXFA_FFWidget* CPDFSDK_Widget::GetGroupMixXFAWidget() const {
  CPDF_Document::Extension* pContext =
      GetPageView()->GetFormFillEnv()->GetDocExtension();
  if (!pContext || !pContext->ContainsExtensionForegroundForm())
    return nullptr;

  CXFA_FFDocView* pDocView =
      static_cast<CPDFXFA_Context*>(pContext)->GetXFADocView();
  if (!pDocView)
    return nullptr;

  WideString sName = GetName();
  return !sName.IsEmpty() ? pDocView->GetWidgetByName(sName, nullptr) : nullptr;
}

CXFA_FFWidgetHandler* CPDFSDK_Widget::GetXFAWidgetHandler() const {
  CPDF_Document::Extension* pContext =
      GetPageView()->GetFormFillEnv()->GetDocExtension();
  if (!pContext || !pContext->ContainsExtensionForegroundForm())
    return nullptr;

  CXFA_FFDocView* pDocView =
      static_cast<CPDFXFA_Context*>(pContext)->GetXFADocView();
  return pDocView ? pDocView->GetWidgetHandler() : nullptr;
}

static XFA_EVENTTYPE GetXFAEventType(PDFSDK_XFAAActionType eXFAAAT) {
  XFA_EVENTTYPE eEventType = XFA_EVENT_Unknown;

  switch (eXFAAAT) {
    case PDFSDK_XFA_Click:
      eEventType = XFA_EVENT_Click;
      break;
    case PDFSDK_XFA_Full:
      eEventType = XFA_EVENT_Full;
      break;
    case PDFSDK_XFA_PreOpen:
      eEventType = XFA_EVENT_PreOpen;
      break;
    case PDFSDK_XFA_PostOpen:
      eEventType = XFA_EVENT_PostOpen;
      break;
  }

  return eEventType;
}

static XFA_EVENTTYPE GetXFAEventType(CPDF_AAction::AActionType eAAT,
                                     bool bWillCommit) {
  XFA_EVENTTYPE eEventType = XFA_EVENT_Unknown;

  switch (eAAT) {
    case CPDF_AAction::kCursorEnter:
      eEventType = XFA_EVENT_MouseEnter;
      break;
    case CPDF_AAction::kCursorExit:
      eEventType = XFA_EVENT_MouseExit;
      break;
    case CPDF_AAction::kButtonDown:
      eEventType = XFA_EVENT_MouseDown;
      break;
    case CPDF_AAction::kButtonUp:
      eEventType = XFA_EVENT_MouseUp;
      break;
    case CPDF_AAction::kGetFocus:
      eEventType = XFA_EVENT_Enter;
      break;
    case CPDF_AAction::kLoseFocus:
      eEventType = XFA_EVENT_Exit;
      break;
    case CPDF_AAction::kPageOpen:
    case CPDF_AAction::kPageClose:
    case CPDF_AAction::kPageVisible:
    case CPDF_AAction::kPageInvisible:
      break;
    case CPDF_AAction::kKeyStroke:
      if (!bWillCommit)
        eEventType = XFA_EVENT_Change;
      break;
    case CPDF_AAction::kValidate:
      eEventType = XFA_EVENT_Validate;
      break;
    case CPDF_AAction::kOpenPage:
    case CPDF_AAction::kClosePage:
    case CPDF_AAction::kFormat:
    case CPDF_AAction::kCalculate:
    case CPDF_AAction::kCloseDocument:
    case CPDF_AAction::kSaveDocument:
    case CPDF_AAction::kDocumentSaved:
    case CPDF_AAction::kPrintDocument:
    case CPDF_AAction::kDocumentPrinted:
      break;
    case CPDF_AAction::kDocumentOpen:
    case CPDF_AAction::kNumberOfActions:
      NOTREACHED_NORETURN();
  }

  return eEventType;
}

bool CPDFSDK_Widget::HasXFAAAction(PDFSDK_XFAAActionType eXFAAAT) const {
  CXFA_FFWidget* pWidget = GetMixXFAWidget();
  if (!pWidget)
    return false;

  CXFA_FFWidgetHandler* pXFAWidgetHandler = GetXFAWidgetHandler();
  if (!pXFAWidgetHandler)
    return false;

  XFA_EVENTTYPE eEventType = GetXFAEventType(eXFAAAT);
  if ((eEventType == XFA_EVENT_Click || eEventType == XFA_EVENT_Change) &&
      GetFieldType() == FormFieldType::kRadioButton) {
    CXFA_FFWidget* hGroupWidget = GetGroupMixXFAWidget();
    if (hGroupWidget &&
        hGroupWidget->HasEventUnderHandler(eEventType, pXFAWidgetHandler)) {
      return true;
    }
  }

  return pWidget->HasEventUnderHandler(eEventType, pXFAWidgetHandler);
}

bool CPDFSDK_Widget::OnXFAAAction(PDFSDK_XFAAActionType eXFAAAT,
                                  CFFL_FieldAction* data,
                                  const CPDFSDK_PageView* pPageView) {
  auto* pContext = static_cast<CPDFXFA_Context*>(
      GetPageView()->GetFormFillEnv()->GetDocExtension());
  if (!pContext)
    return false;

  CXFA_FFWidget* pWidget = GetMixXFAWidget();
  if (!pWidget)
    return false;

  XFA_EVENTTYPE eEventType = GetXFAEventType(eXFAAAT);
  if (eEventType == XFA_EVENT_Unknown)
    return false;

  CXFA_FFWidgetHandler* pXFAWidgetHandler = GetXFAWidgetHandler();
  if (!pXFAWidgetHandler)
    return false;

  CXFA_EventParam param(eEventType);
  param.m_wsChange = data->sChange;
  param.m_iCommitKey = 0;
  param.m_bShift = data->bShift;
  param.m_iSelStart = data->nSelStart;
  param.m_iSelEnd = data->nSelEnd;
  param.m_wsFullText = data->sValue;
  param.m_bKeyDown = data->bKeyDown;
  param.m_bModifier = data->bModifier;
  param.m_wsPrevText = data->sValue;
  if ((eEventType == XFA_EVENT_Click || eEventType == XFA_EVENT_Change) &&
      GetFieldType() == FormFieldType::kRadioButton) {
    CXFA_FFWidget* hGroupWidget = GetGroupMixXFAWidget();
    if (hGroupWidget &&
        !hGroupWidget->ProcessEventUnderHandler(&param, pXFAWidgetHandler)) {
      return false;
    }
  }

  bool ret = pWidget->ProcessEventUnderHandler(&param, pXFAWidgetHandler);
  CXFA_FFDocView* pDocView = pContext->GetXFADocView();
  if (pDocView)
    pDocView->UpdateDocView();

  return ret;
}

void CPDFSDK_Widget::Synchronize(bool bSynchronizeElse) {
  CXFA_FFWidget* hWidget = GetMixXFAWidget();
  if (!hWidget)
    return;

  CXFA_Node* node = hWidget->GetNode();
  if (!node->IsWidgetReady())
    return;

  CPDF_FormField* pFormField = GetFormField();
  switch (GetFieldType()) {
    case FormFieldType::kCheckBox:
    case FormFieldType::kRadioButton: {
      CPDF_FormControl* pFormCtrl = GetFormControl();
      XFA_CheckState eCheckState =
          pFormCtrl->IsChecked() ? XFA_CheckState::kOn : XFA_CheckState::kOff;
      node->SetCheckState(eCheckState);
      break;
    }
    case FormFieldType::kTextField:
      node->SetValue(XFA_ValuePicture::kEdit, pFormField->GetValue());
      break;
    case FormFieldType::kComboBox:
    case FormFieldType::kListBox: {
      node->ClearAllSelections();
      for (int i = 0; i < pFormField->CountSelectedItems(); ++i) {
        int nIndex = pFormField->GetSelectedIndex(i);
        if (nIndex > -1 &&
            static_cast<size_t>(nIndex) < node->CountChoiceListItems(false)) {
          node->SetItemState(nIndex, true, false, false);
        }
      }
      if (GetFieldType() == FormFieldType::kComboBox)
        node->SetValue(XFA_ValuePicture::kEdit, pFormField->GetValue());
      break;
    }
    default:
      break;
  }

  if (bSynchronizeElse) {
    auto* context = static_cast<CPDFXFA_Context*>(
        GetPageView()->GetFormFillEnv()->GetDocExtension());
    context->GetXFADocView()->ProcessValueChanged(node);
  }
}

bool CPDFSDK_Widget::HandleXFAAAction(
    CPDF_AAction::AActionType type,
    CFFL_FieldAction* data,
    CPDFSDK_FormFillEnvironment* pFormFillEnv) {
  auto* pContext =
      static_cast<CPDFXFA_Context*>(pFormFillEnv->GetDocExtension());
  if (!pContext)
    return false;

  CXFA_FFWidget* hWidget = GetMixXFAWidget();
  if (!hWidget)
    return false;

  XFA_EVENTTYPE eEventType = GetXFAEventType(type, data->bWillCommit);
  if (eEventType == XFA_EVENT_Unknown)
    return false;

  CXFA_FFWidgetHandler* pXFAWidgetHandler = GetXFAWidgetHandler();
  if (!pXFAWidgetHandler)
    return false;

  CXFA_EventParam param(eEventType);
  param.m_wsChange = data->sChange;
  param.m_iCommitKey = 0;
  param.m_bShift = data->bShift;
  param.m_iSelStart = data->nSelStart;
  param.m_iSelEnd = data->nSelEnd;
  param.m_wsFullText = data->sValue;
  param.m_bKeyDown = data->bKeyDown;
  param.m_bModifier = data->bModifier;
  param.m_wsPrevText = data->sValue;
  bool ret = hWidget->ProcessEventUnderHandler(&param, pXFAWidgetHandler);
  CXFA_FFDocView* pDocView = pContext->GetXFADocView();
  if (pDocView)
    pDocView->UpdateDocView();

  return ret;
}
#endif  // PDF_ENABLE_XFA

bool CPDFSDK_Widget::IsWidgetAppearanceValid(
    CPDF_Annot::AppearanceMode mode) const {
  RetainPtr<const CPDF_Dictionary> pAP =
      GetAnnotDict()->GetDictFor(pdfium::annotation::kAP);
  if (!pAP)
    return false;

  // Choose the right sub-ap
  const char* ap_entry = "N";
  if (mode == CPDF_Annot::AppearanceMode::kDown)
    ap_entry = "D";
  else if (mode == CPDF_Annot::AppearanceMode::kRollover)
    ap_entry = "R";
  if (!pAP->KeyExist(ap_entry))
    ap_entry = "N";

  // Get the AP stream or subdirectory
  RetainPtr<const CPDF_Object> pSub = pAP->GetDirectObjectFor(ap_entry);
  if (!pSub)
    return false;

  FormFieldType fieldType = GetFieldType();
  switch (fieldType) {
    case FormFieldType::kPushButton:
    case FormFieldType::kComboBox:
    case FormFieldType::kListBox:
    case FormFieldType::kTextField:
    case FormFieldType::kSignature:
      return pSub->IsStream();
    case FormFieldType::kCheckBox:
    case FormFieldType::kRadioButton:
      if (const CPDF_Dictionary* pSubDict = pSub->AsDictionary()) {
        return !!pSubDict->GetStreamFor(GetAppState());
      }
      return false;
    default:
      return true;
  }
}

bool CPDFSDK_Widget::IsPushHighlighted() const {
  return GetFormControl()->GetHighlightingMode() == CPDF_FormControl::kPush;
}

FormFieldType CPDFSDK_Widget::GetFieldType() const {
  CPDF_FormField* pField = GetFormField();
  return pField ? pField->GetFieldType() : FormFieldType::kUnknown;
}

void CPDFSDK_Widget::SetRect(const CFX_FloatRect& rect) {
  DCHECK(rect.right - rect.left >= 1.0f);
  DCHECK(rect.top - rect.bottom >= 1.0f);
  GetMutableAnnotDict()->SetRectFor(pdfium::annotation::kRect, rect);
}

bool CPDFSDK_Widget::IsAppearanceValid() {
#ifdef PDF_ENABLE_XFA
  CPDF_Document::Extension* pContext =
      GetPageView()->GetFormFillEnv()->GetDocExtension();
  if (pContext && pContext->ContainsExtensionFullForm())
    return true;
#endif  // PDF_ENABLE_XFA
  return CPDFSDK_BAAnnot::IsAppearanceValid();
}

int CPDFSDK_Widget::GetLayoutOrder() const {
  return 2;
}

int CPDFSDK_Widget::GetFieldFlags() const {
  return GetFormField()->GetFieldFlags();
}

bool CPDFSDK_Widget::IsSignatureWidget() const {
  return GetFieldType() == FormFieldType::kSignature;
}

CPDF_FormField* CPDFSDK_Widget::GetFormField() const {
  CPDF_FormControl* pControl = GetFormControl();
  return pControl ? pControl->GetField() : nullptr;
}

CPDF_FormControl* CPDFSDK_Widget::GetFormControl() const {
  CPDF_InteractiveForm* pPDFInteractiveForm =
      m_pInteractiveForm->GetInteractiveForm();
  return pPDFInteractiveForm->GetControlByDict(GetAnnotDict());
}

int CPDFSDK_Widget::GetRotate() const {
  CPDF_FormControl* pCtrl = GetFormControl();
  return pCtrl->GetRotation() % 360;
}

#ifdef PDF_ENABLE_XFA
WideString CPDFSDK_Widget::GetName() const {
  return GetFormField()->GetFullName();
}
#endif  // PDF_ENABLE_XFA

std::optional<FX_COLORREF> CPDFSDK_Widget::GetFillColor() const {
  CFX_Color::TypeAndARGB type_argb_pair =
      GetFormControl()->GetColorARGB(pdfium::appearance::kBG);

  if (type_argb_pair.color_type == CFX_Color::Type::kTransparent)
    return std::nullopt;

  return ArgbToColorRef(type_argb_pair.argb);
}

std::optional<FX_COLORREF> CPDFSDK_Widget::GetBorderColor() const {
  CFX_Color::TypeAndARGB type_argb_pair =
      GetFormControl()->GetColorARGB(pdfium::appearance::kBC);
  if (type_argb_pair.color_type == CFX_Color::Type::kTransparent)
    return std::nullopt;

  return ArgbToColorRef(type_argb_pair.argb);
}

std::optional<FX_COLORREF> CPDFSDK_Widget::GetTextColor() const {
  CPDF_DefaultAppearance da = GetFormControl()->GetDefaultAppearance();
  std::optional<CFX_Color::TypeAndARGB> maybe_type_argb_pair =
      da.GetColorARGB();

  if (!maybe_type_argb_pair.has_value())
    return std::nullopt;

  if (maybe_type_argb_pair.value().color_type == CFX_Color::Type::kTransparent)
    return std::nullopt;

  return ArgbToColorRef(maybe_type_argb_pair.value().argb);
}

float CPDFSDK_Widget::GetFontSize() const {
  CPDF_FormControl* pFormCtrl = GetFormControl();
  CPDF_DefaultAppearance pDa = pFormCtrl->GetDefaultAppearance();
  float fFontSize;
  pDa.GetFont(&fFontSize);
  return fFontSize;
}

int CPDFSDK_Widget::GetSelectedIndex(int nIndex) const {
#ifdef PDF_ENABLE_XFA
  if (CXFA_FFWidget* hWidget = GetMixXFAWidget()) {
    CXFA_Node* node = hWidget->GetNode();
    if (node->IsWidgetReady()) {
      if (nIndex < node->CountSelectedItems())
        return node->GetSelectedItem(nIndex);
    }
  }
#endif  // PDF_ENABLE_XFA
  CPDF_FormField* pFormField = GetFormField();
  return pFormField->GetSelectedIndex(nIndex);
}

WideString CPDFSDK_Widget::GetValue() const {
#ifdef PDF_ENABLE_XFA
  if (CXFA_FFWidget* hWidget = GetMixXFAWidget()) {
    CXFA_Node* node = hWidget->GetNode();
    if (node->IsWidgetReady())
      return node->GetValue(XFA_ValuePicture::kDisplay);
  }
#endif  // PDF_ENABLE_XFA
  CPDF_FormField* pFormField = GetFormField();
  return pFormField->GetValue();
}

WideString CPDFSDK_Widget::GetExportValue() const {
  CPDF_FormControl* pFormCtrl = GetFormControl();
  return pFormCtrl->GetExportValue();
}

WideString CPDFSDK_Widget::GetOptionLabel(int nIndex) const {
  CPDF_FormField* pFormField = GetFormField();
  return pFormField->GetOptionLabel(nIndex);
}

WideString CPDFSDK_Widget::GetSelectExportText(int nIndex) const {
  if (nIndex < 0)
    return WideString();

  CPDF_FormField* pFormField = GetFormField();
  if (!pFormField)
    return WideString();

  WideString swRet = pFormField->GetOptionValue(nIndex);
  if (!swRet.IsEmpty())
    return swRet;

  return pFormField->GetOptionLabel(nIndex);
}

int CPDFSDK_Widget::CountOptions() const {
  CPDF_FormField* pFormField = GetFormField();
  return pFormField->CountOptions();
}

bool CPDFSDK_Widget::IsOptionSelected(int nIndex) const {
#ifdef PDF_ENABLE_XFA
  if (CXFA_FFWidget* hWidget = GetMixXFAWidget()) {
    CXFA_Node* node = hWidget->GetNode();
    if (node->IsWidgetReady()) {
      if (nIndex > -1 &&
          static_cast<size_t>(nIndex) < node->CountChoiceListItems(false)) {
        return node->GetItemState(nIndex);
      }
      return false;
    }
  }
#endif  // PDF_ENABLE_XFA
  CPDF_FormField* pFormField = GetFormField();
  return pFormField->IsItemSelected(nIndex);
}

int CPDFSDK_Widget::GetTopVisibleIndex() const {
  CPDF_FormField* pFormField = GetFormField();
  return pFormField->GetTopVisibleIndex();
}

bool CPDFSDK_Widget::IsChecked() const {
#ifdef PDF_ENABLE_XFA
  if (CXFA_FFWidget* hWidget = GetMixXFAWidget()) {
    CXFA_Node* node = hWidget->GetNode();
    if (node->IsWidgetReady())
      return node->GetCheckState() == XFA_CheckState::kOn;
  }
#endif  // PDF_ENABLE_XFA
  CPDF_FormControl* pFormCtrl = GetFormControl();
  return pFormCtrl->IsChecked();
}

int CPDFSDK_Widget::GetAlignment() const {
  CPDF_FormControl* pFormCtrl = GetFormControl();
  return pFormCtrl->GetControlAlignment();
}

int CPDFSDK_Widget::GetMaxLen() const {
  CPDF_FormField* pFormField = GetFormField();
  return pFormField->GetMaxLen();
}

void CPDFSDK_Widget::SetCheck(bool bChecked) {
  CPDF_FormControl* pFormCtrl = GetFormControl();
  CPDF_FormField* pFormField = pFormCtrl->GetField();
  pFormField->CheckControl(pFormField->GetControlIndex(pFormCtrl), bChecked,
                           NotificationOption::kDoNotNotify);
#ifdef PDF_ENABLE_XFA
  if (!IsWidgetAppearanceValid(CPDF_Annot::AppearanceMode::kNormal))
    ResetXFAAppearance(CPDFSDK_Widget::kValueChanged);
  Synchronize(true);
#endif  // PDF_ENABLE_XFA
}

void CPDFSDK_Widget::SetValue(const WideString& sValue) {
  CPDF_FormField* pFormField = GetFormField();
  pFormField->SetValue(sValue, NotificationOption::kDoNotNotify);
#ifdef PDF_ENABLE_XFA
  Synchronize(true);
#endif  // PDF_ENABLE_XFA
}

void CPDFSDK_Widget::SetOptionSelection(int index) {
  CPDF_FormField* pFormField = GetFormField();
  pFormField->SetItemSelection(index, NotificationOption::kDoNotNotify);
#ifdef PDF_ENABLE_XFA
  Synchronize(true);
#endif  // PDF_ENABLE_XFA
}

void CPDFSDK_Widget::ClearSelection() {
  CPDF_FormField* pFormField = GetFormField();
  pFormField->ClearSelection(NotificationOption::kDoNotNotify);
#ifdef PDF_ENABLE_XFA
  Synchronize(true);
#endif  // PDF_ENABLE_XFA
}

void CPDFSDK_Widget::SetTopVisibleIndex(int index) {}

void CPDFSDK_Widget::SetAppModified() {
  m_bAppModified = true;
}

void CPDFSDK_Widget::ClearAppModified() {
  m_bAppModified = false;
}

bool CPDFSDK_Widget::IsAppModified() const {
  return m_bAppModified;
}

#ifdef PDF_ENABLE_XFA
void CPDFSDK_Widget::ResetXFAAppearance(ValueChanged bValueChanged) {
  switch (GetFieldType()) {
    case FormFieldType::kTextField:
    case FormFieldType::kComboBox: {
      ResetAppearance(OnFormat(), kValueChanged);
      break;
    }
    default:
      ResetAppearance(std::nullopt, kValueUnchanged);
      break;
  }
}
#endif  // PDF_ENABLE_XFA

void CPDFSDK_Widget::ResetAppearance(std::optional<WideString> sValue,
                                     ValueChanged bValueChanged) {
  SetAppModified();

  m_nAppearanceAge++;
  if (bValueChanged == kValueChanged)
    m_nValueAge++;

  CPDFSDK_AppStream appStream(this, GetAPDict().Get());
  switch (GetFieldType()) {
    case FormFieldType::kPushButton:
      appStream.SetAsPushButton();
      break;
    case FormFieldType::kCheckBox:
      appStream.SetAsCheckBox();
      break;
    case FormFieldType::kRadioButton:
      appStream.SetAsRadioButton();
      break;
    case FormFieldType::kComboBox:
      appStream.SetAsComboBox(sValue);
      break;
    case FormFieldType::kListBox:
      appStream.SetAsListBox();
      break;
    case FormFieldType::kTextField:
      appStream.SetAsTextField(sValue);
      break;
    default:
      break;
  }

  ClearCachedAnnotAP();
}

std::optional<WideString> CPDFSDK_Widget::OnFormat() {
  CPDF_FormField* pFormField = GetFormField();
  DCHECK(pFormField);
  return m_pInteractiveForm->OnFormat(pFormField);
}

void CPDFSDK_Widget::ResetFieldAppearance() {
  CPDF_FormField* pFormField = GetFormField();
  DCHECK(pFormField);
  m_pInteractiveForm->ResetFieldAppearance(pFormField, std::nullopt);
}

void CPDFSDK_Widget::OnDraw(CFX_RenderDevice* pDevice,
                            const CFX_Matrix& mtUser2Device,
                            bool bDrawAnnots) {
  if (IsSignatureWidget()) {
    DrawAppearance(pDevice, mtUser2Device, CPDF_Annot::AppearanceMode::kNormal);
    return;
  }

  GetInteractiveFormFiller()->OnDraw(GetPageView(), this, pDevice,
                                     mtUser2Device);
}

bool CPDFSDK_Widget::DoHitTest(const CFX_PointF& point) {
  if (IsSignatureWidget() || !IsVisible())
    return false;

  if (GetFieldFlags() & pdfium::form_flags::kReadOnly)
    return false;

  bool do_hit_test = GetFieldType() == FormFieldType::kPushButton;
  if (!do_hit_test) {
    uint32_t perms = GetPDFPage()->GetDocument()->GetUserPermissions(
        /*get_owner_perms=*/true);
    do_hit_test = (perms & pdfium::access_permissions::kFillForm) ||
                  (perms & pdfium::access_permissions::kModifyAnnotation);
  }
  return do_hit_test && GetViewBBox().Contains(point);
}

CFX_FloatRect CPDFSDK_Widget::GetViewBBox() {
  if (IsSignatureWidget())
    return CFX_FloatRect();

  auto* form_filler = GetInteractiveFormFiller();
  return CFX_FloatRect(form_filler->GetViewBBox(GetPageView(), this));
}

void CPDFSDK_Widget::OnMouseEnter(Mask<FWL_EVENTFLAG> nFlags) {
  if (IsSignatureWidget())
    return;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  GetInteractiveFormFiller()->OnMouseEnter(GetPageView(), observer, nFlags);
}

void CPDFSDK_Widget::OnMouseExit(Mask<FWL_EVENTFLAG> nFlags) {
  if (IsSignatureWidget())
    return;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  GetInteractiveFormFiller()->OnMouseExit(GetPageView(), observer, nFlags);
}

bool CPDFSDK_Widget::OnLButtonDown(Mask<FWL_EVENTFLAG> nFlags,
                                   const CFX_PointF& point) {
  if (IsSignatureWidget())
    return false;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnLButtonDown(GetPageView(), observer,
                                                   nFlags, point);
}

bool CPDFSDK_Widget::OnLButtonUp(Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point) {
  if (IsSignatureWidget())
    return false;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnLButtonUp(GetPageView(), observer,
                                                 nFlags, point);
}

bool CPDFSDK_Widget::OnLButtonDblClk(Mask<FWL_EVENTFLAG> nFlags,
                                     const CFX_PointF& point) {
  if (IsSignatureWidget())
    return false;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnLButtonDblClk(GetPageView(), observer,
                                                     nFlags, point);
}

bool CPDFSDK_Widget::OnMouseMove(Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point) {
  if (IsSignatureWidget())
    return false;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnMouseMove(GetPageView(), observer,
                                                 nFlags, point);
}

bool CPDFSDK_Widget::OnMouseWheel(Mask<FWL_EVENTFLAG> nFlags,
                                  const CFX_PointF& point,
                                  const CFX_Vector& delta) {
  if (IsSignatureWidget())
    return false;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnMouseWheel(GetPageView(), observer,
                                                  nFlags, point, delta);
}

bool CPDFSDK_Widget::OnRButtonDown(Mask<FWL_EVENTFLAG> nFlags,
                                   const CFX_PointF& point) {
  if (IsSignatureWidget())
    return false;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnRButtonDown(GetPageView(), observer,
                                                   nFlags, point);
}

bool CPDFSDK_Widget::OnRButtonUp(Mask<FWL_EVENTFLAG> nFlags,
                                 const CFX_PointF& point) {
  if (IsSignatureWidget())
    return false;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnRButtonUp(GetPageView(), observer,
                                                 nFlags, point);
}

bool CPDFSDK_Widget::OnChar(uint32_t nChar, Mask<FWL_EVENTFLAG> nFlags) {
  return !IsSignatureWidget() &&
         GetInteractiveFormFiller()->OnChar(this, nChar, nFlags);
}

bool CPDFSDK_Widget::OnKeyDown(FWL_VKEYCODE nKeyCode,
                               Mask<FWL_EVENTFLAG> nFlags) {
  return !IsSignatureWidget() &&
         GetInteractiveFormFiller()->OnKeyDown(this, nKeyCode, nFlags);
}

bool CPDFSDK_Widget::OnSetFocus(Mask<FWL_EVENTFLAG> nFlags) {
  if (!IsFocusableAnnot(GetPDFAnnot()->GetSubtype()))
    return false;

  if (IsSignatureWidget())
    return true;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnSetFocus(observer, nFlags);
}

bool CPDFSDK_Widget::OnKillFocus(Mask<FWL_EVENTFLAG> nFlags) {
  if (!IsFocusableAnnot(GetPDFAnnot()->GetSubtype()))
    return false;

  if (IsSignatureWidget())
    return true;

  ObservedPtr<CPDFSDK_Widget> observer(this);
  return GetInteractiveFormFiller()->OnKillFocus(observer, nFlags);
}

bool CPDFSDK_Widget::CanUndo() {
  return !IsSignatureWidget() && GetInteractiveFormFiller()->CanUndo(this);
}

bool CPDFSDK_Widget::CanRedo() {
  return !IsSignatureWidget() && GetInteractiveFormFiller()->CanRedo(this);
}

bool CPDFSDK_Widget::Undo() {
  return !IsSignatureWidget() && GetInteractiveFormFiller()->Undo(this);
}

bool CPDFSDK_Widget::Redo() {
  return !IsSignatureWidget() && GetInteractiveFormFiller()->Redo(this);
}

WideString CPDFSDK_Widget::GetText() {
  if (IsSignatureWidget())
    return WideString();
  return GetInteractiveFormFiller()->GetText(this);
}

WideString CPDFSDK_Widget::GetSelectedText() {
  if (IsSignatureWidget())
    return WideString();
  return GetInteractiveFormFiller()->GetSelectedText(this);
}

void CPDFSDK_Widget::ReplaceAndKeepSelection(const WideString& text) {
  if (IsSignatureWidget())
    return;

  GetInteractiveFormFiller()->ReplaceAndKeepSelection(this, text);
}

void CPDFSDK_Widget::ReplaceSelection(const WideString& text) {
  if (IsSignatureWidget())
    return;

  GetInteractiveFormFiller()->ReplaceSelection(this, text);
}

bool CPDFSDK_Widget::SelectAllText() {
  return !IsSignatureWidget() &&
         GetInteractiveFormFiller()->SelectAllText(this);
}

bool CPDFSDK_Widget::SetIndexSelected(int index, bool selected) {
  ObservedPtr<CPDFSDK_Widget> observer(this);
  return !IsSignatureWidget() && GetInteractiveFormFiller()->SetIndexSelected(
                                     observer, index, selected);
}

bool CPDFSDK_Widget::IsIndexSelected(int index) {
  ObservedPtr<CPDFSDK_Widget> observer(this);
  return !IsSignatureWidget() &&
         GetInteractiveFormFiller()->IsIndexSelected(observer, index);
}

void CPDFSDK_Widget::DrawAppearance(CFX_RenderDevice* pDevice,
                                    const CFX_Matrix& mtUser2Device,
                                    CPDF_Annot::AppearanceMode mode) {
  FormFieldType fieldType = GetFieldType();

  if ((fieldType == FormFieldType::kCheckBox ||
       fieldType == FormFieldType::kRadioButton) &&
      mode == CPDF_Annot::AppearanceMode::kNormal &&
      !IsWidgetAppearanceValid(CPDF_Annot::AppearanceMode::kNormal)) {
    CFX_GraphStateData gsd;
    gsd.m_LineWidth = 0.0f;

    CFX_Path path;
    path.AppendFloatRect(GetRect());
    pDevice->DrawPath(path, &mtUser2Device, &gsd, 0, 0xFFAAAAAA,
                      CFX_FillRenderOptions::EvenOddOptions());
  } else {
    CPDFSDK_BAAnnot::DrawAppearance(pDevice, mtUser2Device, mode);
  }
}

void CPDFSDK_Widget::UpdateField() {
  CPDF_FormField* pFormField = GetFormField();
  DCHECK(pFormField);
  m_pInteractiveForm->UpdateField(pFormField);
}

void CPDFSDK_Widget::DrawShadow(CFX_RenderDevice* pDevice,
                                CPDFSDK_PageView* pPageView) {
  FormFieldType fieldType = GetFieldType();
  if (!m_pInteractiveForm->IsNeedHighLight(fieldType))
    return;

  CFX_Matrix page2device = pPageView->GetCurrentMatrix();
  CFX_FloatRect rcDevice = GetRect();
  CFX_PointF tmp =
      page2device.Transform(CFX_PointF(rcDevice.left, rcDevice.bottom));
  rcDevice.left = tmp.x;
  rcDevice.bottom = tmp.y;

  tmp = page2device.Transform(CFX_PointF(rcDevice.right, rcDevice.top));
  rcDevice.right = tmp.x;
  rcDevice.top = tmp.y;
  rcDevice.Normalize();

  pDevice->FillRect(
      rcDevice.ToFxRect(),
      AlphaAndColorRefToArgb(
          static_cast<int>(m_pInteractiveForm->GetHighlightAlpha()),
          m_pInteractiveForm->GetHighlightColor(fieldType)));
}

CFX_FloatRect CPDFSDK_Widget::GetClientRect() const {
  CFX_FloatRect rcWindow = GetRotatedRect();
  float fBorderWidth = GetBorderWidth();
  switch (GetBorderStyle()) {
    case BorderStyle::kBeveled:
    case BorderStyle::kInset:
      fBorderWidth *= 2.0f;
      break;
    default:
      break;
  }
  return rcWindow.GetDeflated(fBorderWidth, fBorderWidth);
}

CFX_FloatRect CPDFSDK_Widget::GetRotatedRect() const {
  CFX_FloatRect rectAnnot = GetRect();
  float fWidth = rectAnnot.Width();
  float fHeight = rectAnnot.Height();

  CPDF_FormControl* pControl = GetFormControl();
  CFX_FloatRect rcPWLWindow;
  switch (abs(pControl->GetRotation() % 360)) {
    case 0:
    case 180:
    default:
      rcPWLWindow = CFX_FloatRect(0, 0, fWidth, fHeight);
      break;
    case 90:
    case 270:
      rcPWLWindow = CFX_FloatRect(0, 0, fHeight, fWidth);
      break;
  }

  return rcPWLWindow;
}

CFX_Matrix CPDFSDK_Widget::GetMatrix() const {
  CFX_Matrix mt;
  CPDF_FormControl* pControl = GetFormControl();
  CFX_FloatRect rcAnnot = GetRect();
  float fWidth = rcAnnot.Width();
  float fHeight = rcAnnot.Height();

  switch (abs(pControl->GetRotation() % 360)) {
    default:
    case 0:
      break;
    case 90:
      mt = CFX_Matrix(0, 1, -1, 0, fWidth, 0);
      break;
    case 180:
      mt = CFX_Matrix(-1, 0, 0, -1, fWidth, fHeight);
      break;
    case 270:
      mt = CFX_Matrix(0, -1, 1, 0, 0, fHeight);
      break;
  }

  return mt;
}

CFX_Color CPDFSDK_Widget::GetTextPWLColor() const {
  CPDF_FormControl* pFormCtrl = GetFormControl();
  std::optional<CFX_Color> crText =
      pFormCtrl->GetDefaultAppearance().GetColor();
  return crText.value_or(CFX_Color(CFX_Color::Type::kGray, 0));
}

CFX_Color CPDFSDK_Widget::GetBorderPWLColor() const {
  CPDF_FormControl* pFormCtrl = GetFormControl();
  return pFormCtrl->GetOriginalBorderColor();
}

CFX_Color CPDFSDK_Widget::GetFillPWLColor() const {
  CPDF_FormControl* pFormCtrl = GetFormControl();
  return pFormCtrl->GetOriginalBackgroundColor();
}

bool CPDFSDK_Widget::OnAAction(CPDF_AAction::AActionType type,
                               CFFL_FieldAction* data,
                               const CPDFSDK_PageView* pPageView) {
  CPDFSDK_FormFillEnvironment* pFormFillEnv = pPageView->GetFormFillEnv();

#ifdef PDF_ENABLE_XFA
  if (HandleXFAAAction(type, data, pFormFillEnv))
    return true;
#endif  // PDF_ENABLE_XFA

  CPDF_Action action = GetAAction(type);
  if (action.GetType() != CPDF_Action::Type::kUnknown) {
    pFormFillEnv->DoActionField(action, type, GetFormField(), data);
  }
  return false;
}

void CPDFSDK_Widget::OnLoad() {
  ObservedPtr<CPDFSDK_Widget> pObserved(this);
  if (pObserved->IsSignatureWidget()) {
    return;
  }
  if (!pObserved->IsAppearanceValid()) {
    pObserved->ResetAppearance(std::nullopt, CPDFSDK_Widget::kValueUnchanged);
  }
  FormFieldType field_type = pObserved->GetFieldType();
  if (field_type == FormFieldType::kTextField ||
      field_type == FormFieldType::kComboBox) {
    std::optional<WideString> sValue = pObserved->OnFormat();
    if (!pObserved) {
      return;
    }
    if (sValue.has_value() && field_type == FormFieldType::kComboBox) {
      pObserved->ResetAppearance(sValue, CPDFSDK_Widget::kValueUnchanged);
    }
  }
#ifdef PDF_ENABLE_XFA
  auto* pContext =
      pObserved->GetPageView()->GetFormFillEnv()->GetDocExtension();
  if (pContext && pContext->ContainsExtensionForegroundForm()) {
    if (!pObserved->IsAppearanceValid() && !pObserved->GetValue().IsEmpty()) {
      pObserved->ResetXFAAppearance(CPDFSDK_Widget::kValueUnchanged);
    }
  }
#endif  // PDF_ENABLE_XFA
}

CPDF_Action CPDFSDK_Widget::GetAAction(CPDF_AAction::AActionType eAAT) {
  switch (eAAT) {
    case CPDF_AAction::kCursorEnter:
    case CPDF_AAction::kCursorExit:
    case CPDF_AAction::kButtonDown:
    case CPDF_AAction::kButtonUp:
    case CPDF_AAction::kGetFocus:
    case CPDF_AAction::kLoseFocus:
    case CPDF_AAction::kPageOpen:
    case CPDF_AAction::kPageClose:
    case CPDF_AAction::kPageVisible:
    case CPDF_AAction::kPageInvisible:
      return CPDFSDK_BAAnnot::GetAAction(eAAT);

    case CPDF_AAction::kKeyStroke:
    case CPDF_AAction::kFormat:
    case CPDF_AAction::kValidate:
    case CPDF_AAction::kCalculate: {
      CPDF_FormField* pField = GetFormField();
      if (pField->GetAdditionalAction().HasDict())
        return pField->GetAdditionalAction().GetAction(eAAT);
      return CPDFSDK_BAAnnot::GetAAction(eAAT);
    }
    default:
      break;
  }

  return CPDF_Action(nullptr);
}

CFFL_InteractiveFormFiller* CPDFSDK_Widget::GetInteractiveFormFiller() {
  return GetPageView()->GetFormFillEnv()->GetInteractiveFormFiller();
}
