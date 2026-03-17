// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_INTERACTIVEFORM_H_
#define FPDFSDK_CPDFSDK_INTERACTIVEFORM_H_

#include <array>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "core/fpdfdoc/cpdf_action.h"
#include "core/fpdfdoc/cpdf_interactiveform.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/dib/fx_dib.h"
#include "fpdfsdk/cpdfsdk_widget.h"

class CPDF_Dictionary;
class CPDF_FormControl;
class CPDF_FormField;
class CPDF_Object;
class CPDFSDK_FormFillEnvironment;

class CPDFSDK_InteractiveForm final
    : public CPDF_InteractiveForm::NotifierIface {
 public:
  explicit CPDFSDK_InteractiveForm(CPDFSDK_FormFillEnvironment* pFormFillEnv);
  ~CPDFSDK_InteractiveForm() override;

  CPDF_InteractiveForm* GetInteractiveForm() const {
    return m_pInteractiveForm.get();
  }

  CPDFSDK_Widget* GetWidget(CPDF_FormControl* pControl) const;
  void GetWidgets(const WideString& sFieldName,
                  std::vector<ObservedPtr<CPDFSDK_Widget>>* widgets) const;
  void GetWidgets(CPDF_FormField* pField,
                  std::vector<ObservedPtr<CPDFSDK_Widget>>* widgets) const;

  void AddMap(CPDF_FormControl* pControl, CPDFSDK_Widget* pWidget);
  void RemoveMap(CPDF_FormControl* pControl);

  void EnableCalculate(bool bEnabled);
  bool IsCalculateEnabled() const;

#ifdef PDF_ENABLE_XFA
  void XfaEnableCalculate(bool bEnabled);
  bool IsXfaCalculateEnabled() const;
  bool IsXfaValidationsEnabled();
  void XfaSetValidationsEnabled(bool bEnabled);
  void SynchronizeField(CPDF_FormField* pFormField);
#endif  // PDF_ENABLE_XFA

  bool OnKeyStrokeCommit(CPDF_FormField* pFormField, const WideString& csValue);
  bool OnValidate(CPDF_FormField* pFormField, const WideString& csValue);
  void OnCalculate(CPDF_FormField* pFormField);
  std::optional<WideString> OnFormat(CPDF_FormField* pFormField);

  void ResetFieldAppearance(CPDF_FormField* pFormField,
                            std::optional<WideString> sValue);
  void UpdateField(CPDF_FormField* pFormField);

  bool DoAction_Hide(const CPDF_Action& action);
  bool DoAction_SubmitForm(const CPDF_Action& action);
  void DoAction_ResetForm(const CPDF_Action& action);

  std::vector<CPDF_FormField*> GetFieldFromObjects(
      const std::vector<RetainPtr<const CPDF_Object>>& objects) const;
  bool SubmitFields(const WideString& csDestination,
                    const std::vector<CPDF_FormField*>& fields,
                    bool bIncludeOrExclude,
                    bool bUrlEncoded);
  bool SubmitForm(const WideString& sDestination);
  ByteString ExportFormToFDFTextBuf();
  ByteString ExportFieldsToFDFTextBuf(
      const std::vector<CPDF_FormField*>& fields,
      bool bIncludeOrExclude);

  bool IsNeedHighLight(FormFieldType fieldType) const;
  void RemoveAllHighLights();
  void SetHighlightAlpha(uint8_t alpha) { m_HighlightAlpha = alpha; }
  uint8_t GetHighlightAlpha() { return m_HighlightAlpha; }
  void SetHighlightColor(FX_COLORREF clr, FormFieldType fieldType);
  void SetAllHighlightColors(FX_COLORREF clr);
  FX_COLORREF GetHighlightColor(FormFieldType fieldType);

 private:
  // CPDF_InteractiveForm::NotifierIface:
  bool BeforeValueChange(CPDF_FormField* pField,
                         const WideString& csValue) override;
  void AfterValueChange(CPDF_FormField* pField) override;
  bool BeforeSelectionChange(CPDF_FormField* pField,
                             const WideString& csValue) override;
  void AfterSelectionChange(CPDF_FormField* pField) override;
  void AfterCheckedStatusChange(CPDF_FormField* pField) override;
  void AfterFormReset(CPDF_InteractiveForm* pForm) override;

  int GetPageIndexByAnnotDict(CPDF_Document* pDocument,
                              const CPDF_Dictionary* pAnnotDict) const;

  UnownedPtr<CPDFSDK_FormFillEnvironment> const m_pFormFillEnv;
  std::unique_ptr<CPDF_InteractiveForm> const m_pInteractiveForm;
  std::map<UnownedPtr<const CPDF_FormControl>,
           UnownedPtr<CPDFSDK_Widget>,
           std::less<>>
      m_Map;
#ifdef PDF_ENABLE_XFA
  bool m_bXfaCalculate = true;
  bool m_bXfaValidationsEnabled = true;
#endif  // PDF_ENABLE_XFA
  bool m_bCalculate = true;
  bool m_bBusy = false;
  uint8_t m_HighlightAlpha = 0;
  std::array<FX_COLORREF, kFormFieldTypeCount> m_HighlightColor;
  std::array<bool, kFormFieldTypeCount> m_NeedsHighlight;
};

#endif  // FPDFSDK_CPDFSDK_INTERACTIVEFORM_H_
