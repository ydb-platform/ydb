// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_INTERACTIVEFORM_H_
#define CORE_FPDFDOC_CPDF_INTERACTIVEFORM_H_

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfdoc/cpdf_defaultappearance.h"
#include "core/fpdfdoc/cpdf_formfield.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CFieldTree;
class CFDF_Document;
class CPDF_Document;
class CPDF_Font;
class CPDF_FormControl;
class CPDF_Page;

class CPDF_InteractiveForm {
 public:
  class NotifierIface {
   public:
    virtual ~NotifierIface() = default;

    virtual bool BeforeValueChange(CPDF_FormField* pField,
                                   const WideString& csValue) = 0;
    virtual void AfterValueChange(CPDF_FormField* pField) = 0;
    virtual bool BeforeSelectionChange(CPDF_FormField* pField,
                                       const WideString& csValue) = 0;
    virtual void AfterSelectionChange(CPDF_FormField* pField) = 0;
    virtual void AfterCheckedStatusChange(CPDF_FormField* pField) = 0;
    virtual void AfterFormReset(CPDF_InteractiveForm* pForm) = 0;
  };

  explicit CPDF_InteractiveForm(CPDF_Document* pDocument);
  ~CPDF_InteractiveForm();

  static bool IsUpdateAPEnabled();
  static void SetUpdateAP(bool bUpdateAP);
  static RetainPtr<CPDF_Font> AddNativeInteractiveFormFont(
      CPDF_Document* pDocument,
      ByteString* csNameTag);

  size_t CountFields(const WideString& csFieldName) const;
  CPDF_FormField* GetField(size_t index, const WideString& csFieldName) const;
  CPDF_FormField* GetFieldByDict(const CPDF_Dictionary* pFieldDict) const;

  const CPDF_FormControl* GetControlAtPoint(const CPDF_Page* pPage,
                                            const CFX_PointF& point,
                                            int* z_order) const;
  CPDF_FormControl* GetControlByDict(const CPDF_Dictionary* pWidgetDict) const;

  bool NeedConstructAP() const;
  int CountFieldsInCalculationOrder();
  CPDF_FormField* GetFieldInCalculationOrder(int index);
  int FindFieldInCalculationOrder(const CPDF_FormField* pField);

  RetainPtr<CPDF_Font> GetFormFont(ByteString csNameTag) const;
  RetainPtr<CPDF_Font> GetFontForElement(
      RetainPtr<CPDF_Dictionary> pElement) const;
  CPDF_DefaultAppearance GetDefaultAppearance() const;
  int GetFormAlignment() const;
  bool CheckRequiredFields(const std::vector<CPDF_FormField*>* fields,
                           bool bIncludeOrExclude) const;

  std::unique_ptr<CFDF_Document> ExportToFDF(const WideString& pdf_path) const;
  std::unique_ptr<CFDF_Document> ExportToFDF(
      const WideString& pdf_path,
      const std::vector<CPDF_FormField*>& fields,
      bool bIncludeOrExclude) const;

  void ResetForm();
  void ResetForm(pdfium::span<CPDF_FormField*> fields, bool bIncludeOrExclude);

  void SetNotifierIface(NotifierIface* pNotify);
  void FixPageFields(CPDF_Page* pPage);

  // Wrap callbacks thru NotifierIface.
  bool NotifyBeforeValueChange(CPDF_FormField* pField,
                               const WideString& csValue);
  void NotifyAfterValueChange(CPDF_FormField* pField);
  bool NotifyBeforeSelectionChange(CPDF_FormField* pField,
                                   const WideString& csValue);
  void NotifyAfterSelectionChange(CPDF_FormField* pField);
  void NotifyAfterCheckedStatusChange(CPDF_FormField* pField);

  const std::vector<UnownedPtr<CPDF_FormControl>>& GetControlsForField(
      const CPDF_FormField* pField);

 private:
  void LoadField(RetainPtr<CPDF_Dictionary> pFieldDict, int nLevel);
  void AddTerminalField(RetainPtr<CPDF_Dictionary> pFieldDict);
  CPDF_FormControl* AddControl(CPDF_FormField* pField,
                               RetainPtr<CPDF_Dictionary> pWidgetDict);

  static bool s_bUpdateAP;

  ByteString m_bsEncoding;
  UnownedPtr<CPDF_Document> const m_pDocument;
  RetainPtr<CPDF_Dictionary> m_pFormDict;
  std::unique_ptr<CFieldTree> m_pFieldTree;
  std::map<RetainPtr<const CPDF_Dictionary>,
           std::unique_ptr<CPDF_FormControl>,
           std::less<>>
      m_ControlMap;
  // Points into |m_ControlMap|.
  std::map<UnownedPtr<const CPDF_FormField>,
           std::vector<UnownedPtr<CPDF_FormControl>>,
           std::less<>>
      m_ControlLists;
  UnownedPtr<NotifierIface> m_pFormNotify;
};

#endif  // CORE_FPDFDOC_CPDF_INTERACTIVEFORM_H_
