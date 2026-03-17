// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_formfield.h"

#include <map>
#include <set>
#include <utility>

#include "constants/form_fields.h"
#include "constants/form_flags.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/parser/cfdf_document.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfdoc/cpdf_defaultappearance.h"
#include "core/fpdfdoc/cpdf_formcontrol.h"
#include "core/fpdfdoc/cpdf_generateap.h"
#include "core/fpdfdoc/cpdf_interactiveform.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/stl_util.h"

namespace {

RetainPtr<const CPDF_Object> GetFieldAttrRecursive(
    const CPDF_Dictionary* pFieldDict,
    const ByteString& name,
    int nLevel) {
  static constexpr int kGetFieldMaxRecursion = 32;
  if (!pFieldDict || nLevel > kGetFieldMaxRecursion)
    return nullptr;

  RetainPtr<const CPDF_Object> pAttr = pFieldDict->GetDirectObjectFor(name);
  if (pAttr)
    return pAttr;

  return GetFieldAttrRecursive(
      pFieldDict->GetDictFor(pdfium::form_fields::kParent).Get(), name,
      nLevel + 1);
}

bool IsComboOrListField(CPDF_FormField::Type type) {
  switch (type) {
    case CPDF_FormField::kComboBox:
    case CPDF_FormField::kListBox:
      return true;
    default:
      return false;
  }
}

bool HasOptField(CPDF_FormField::Type type) {
  switch (type) {
    case CPDF_FormField::kCheckBox:
    case CPDF_FormField::kRadioButton:
    case CPDF_FormField::kComboBox:
    case CPDF_FormField::kListBox:
      return true;
    default:
      return false;
  }
}

}  // namespace

// static
std::optional<FormFieldType> CPDF_FormField::IntToFormFieldType(int value) {
  if (value >= static_cast<int>(FormFieldType::kUnknown) &&
      value < static_cast<int>(kFormFieldTypeCount)) {
    return static_cast<FormFieldType>(value);
  }
  return std::nullopt;
}

// static
RetainPtr<const CPDF_Object> CPDF_FormField::GetFieldAttrForDict(
    const CPDF_Dictionary* pFieldDict,
    const ByteString& name) {
  return GetFieldAttrRecursive(pFieldDict, name, 0);
}

// static
RetainPtr<CPDF_Object> CPDF_FormField::GetMutableFieldAttrForDict(
    CPDF_Dictionary* pFieldDict,
    const ByteString& name) {
  return pdfium::WrapRetain(const_cast<CPDF_Object*>(
      GetFieldAttrRecursive(pFieldDict, name, 0).Get()));
}

// static
WideString CPDF_FormField::GetFullNameForDict(
    const CPDF_Dictionary* pFieldDict) {
  WideString full_name;
  std::set<const CPDF_Dictionary*> visited;
  const CPDF_Dictionary* pLevel = pFieldDict;
  while (pLevel) {
    visited.insert(pLevel);
    WideString short_name = pLevel->GetUnicodeTextFor(pdfium::form_fields::kT);
    if (!short_name.IsEmpty()) {
      if (full_name.IsEmpty())
        full_name = std::move(short_name);
      else
        full_name = short_name + L'.' + full_name;
    }
    pLevel = pLevel->GetDictFor(pdfium::form_fields::kParent).Get();
    if (pdfium::Contains(visited, pLevel))
      break;
  }
  return full_name;
}

CPDF_FormField::CPDF_FormField(CPDF_InteractiveForm* pForm,
                               RetainPtr<CPDF_Dictionary> pDict)
    : m_pForm(pForm), m_pDict(std::move(pDict)) {
  InitFieldFlags();
}

CPDF_FormField::~CPDF_FormField() = default;

void CPDF_FormField::InitFieldFlags() {
  RetainPtr<const CPDF_Object> ft_attr =
      GetFieldAttrInternal(pdfium::form_fields::kFT);
  ByteString type_name = ft_attr ? ft_attr->GetString() : ByteString();
  uint32_t flags = GetFieldFlags();
  m_bRequired = flags & pdfium::form_flags::kRequired;
  m_bNoExport = flags & pdfium::form_flags::kNoExport;

  if (type_name == pdfium::form_fields::kBtn) {
    if (flags & pdfium::form_flags::kButtonRadio) {
      m_Type = kRadioButton;
      m_bIsUnison = flags & pdfium::form_flags::kButtonRadiosInUnison;
    } else if (flags & pdfium::form_flags::kButtonPushbutton) {
      m_Type = kPushButton;
    } else {
      m_Type = kCheckBox;
      m_bIsUnison = true;
    }
  } else if (type_name == pdfium::form_fields::kTx) {
    if (flags & pdfium::form_flags::kTextFileSelect)
      m_Type = kFile;
    else if (flags & pdfium::form_flags::kTextRichText)
      m_Type = kRichText;
    else
      m_Type = kText;
  } else if (type_name == pdfium::form_fields::kCh) {
    if (flags & pdfium::form_flags::kChoiceCombo) {
      m_Type = kComboBox;
    } else {
      m_Type = kListBox;
      m_bIsMultiSelectListBox = flags & pdfium::form_flags::kChoiceMultiSelect;
    }
    m_bUseSelectedIndices = UseSelectedIndicesObject();
  } else if (type_name == pdfium::form_fields::kSig) {
    m_Type = kSign;
  }
}

WideString CPDF_FormField::GetFullName() const {
  return GetFullNameForDict(m_pDict.Get());
}

RetainPtr<const CPDF_Object> CPDF_FormField::GetFieldAttr(
    const ByteString& name) const {
  return GetFieldAttrInternal(name);
}

RetainPtr<const CPDF_Dictionary> CPDF_FormField::GetFieldDict() const {
  return pdfium::WrapRetain(GetFieldDictInternal());
}

bool CPDF_FormField::ResetField() {
  switch (m_Type) {
    case kCheckBox:
    case kRadioButton: {
      int iCount = CountControls();
      // TODO(weili): Check whether anything special needs to be done for
      // |m_bIsUnison|.
      for (int i = 0; i < iCount; i++) {
        CheckControl(i, GetControl(i)->IsDefaultChecked(),
                     NotificationOption::kDoNotNotify);
      }
      m_pForm->NotifyAfterCheckedStatusChange(this);
      break;
    }
    case kComboBox:
    case kListBox: {
      ClearSelection(NotificationOption::kDoNotNotify);
      WideString csValue;
      int iIndex = GetDefaultSelectedItem();
      if (iIndex >= 0)
        csValue = GetOptionLabel(iIndex);
      if (!NotifyListOrComboBoxBeforeChange(csValue)) {
        return false;
      }
      SetItemSelection(iIndex, NotificationOption::kDoNotNotify);
      NotifyListOrComboBoxAfterChange();
      break;
    }
    case kText:
    case kRichText:
    case kFile:
    default: {
      WideString csDValue;
      WideString csValue;
      {
        // Limit scope of |pDV| and |pV| because they may get invalidated
        // during notification below.
        RetainPtr<const CPDF_Object> pDV = GetDefaultValueObject();
        if (pDV)
          csDValue = pDV->GetUnicodeText();

        RetainPtr<const CPDF_Object> pV = GetValueObject();
        if (pV)
          csValue = pV->GetUnicodeText();
      }

      bool bHasRV = !!GetFieldAttrInternal(pdfium::form_fields::kRV);
      if (!bHasRV && (csDValue == csValue))
        return false;

      if (!m_pForm->NotifyBeforeValueChange(this, csDValue))
        return false;

      {
        // Limit scope of |pDV| because it may get invalidated during
        // notification below.
        RetainPtr<const CPDF_Object> pDV = GetDefaultValueObject();
        if (pDV) {
          RetainPtr<CPDF_Object> pClone = pDV->Clone();
          if (!pClone)
            return false;

          m_pDict->SetFor(pdfium::form_fields::kV, std::move(pClone));
          if (bHasRV) {
            m_pDict->SetFor(pdfium::form_fields::kRV, pDV->Clone());
          }
        } else {
          m_pDict->RemoveFor(pdfium::form_fields::kV);
          m_pDict->RemoveFor(pdfium::form_fields::kRV);
        }
      }
      m_pForm->NotifyAfterValueChange(this);
      break;
    }
  }
  return true;
}

int CPDF_FormField::CountControls() const {
  return fxcrt::CollectionSize<int>(GetControls());
}

CPDF_FormControl* CPDF_FormField::GetControl(int index) const {
  return GetControls()[index];
}

int CPDF_FormField::GetControlIndex(const CPDF_FormControl* pControl) const {
  if (!pControl)
    return -1;

  const auto& controls = GetControls();
  auto it = std::find(controls.begin(), controls.end(), pControl);
  if (it == controls.end())
    return -1;

  return pdfium::checked_cast<int>(it - controls.begin());
}

FormFieldType CPDF_FormField::GetFieldType() const {
  switch (m_Type) {
    case kPushButton:
      return FormFieldType::kPushButton;
    case kCheckBox:
      return FormFieldType::kCheckBox;
    case kRadioButton:
      return FormFieldType::kRadioButton;
    case kComboBox:
      return FormFieldType::kComboBox;
    case kListBox:
      return FormFieldType::kListBox;
    case kText:
    case kRichText:
    case kFile:
      return FormFieldType::kTextField;
    case kSign:
      return FormFieldType::kSignature;
    default:
      return FormFieldType::kUnknown;
  }
}

CPDF_AAction CPDF_FormField::GetAdditionalAction() const {
  RetainPtr<const CPDF_Object> pObj =
      GetFieldAttrInternal(pdfium::form_fields::kAA);
  return CPDF_AAction(pObj ? pObj->GetDict() : nullptr);
}

WideString CPDF_FormField::GetAlternateName() const {
  RetainPtr<const CPDF_Object> pObj =
      GetFieldAttrInternal(pdfium::form_fields::kTU);
  return pObj ? pObj->GetUnicodeText() : WideString();
}

WideString CPDF_FormField::GetMappingName() const {
  RetainPtr<const CPDF_Object> pObj =
      GetFieldAttrInternal(pdfium::form_fields::kTM);
  return pObj ? pObj->GetUnicodeText() : WideString();
}

uint32_t CPDF_FormField::GetFieldFlags() const {
  RetainPtr<const CPDF_Object> pObj =
      GetFieldAttrInternal(pdfium::form_fields::kFf);
  return pObj ? pObj->GetInteger() : 0;
}

void CPDF_FormField::SetFieldFlags(uint32_t dwFlags) {
  m_pDict->SetNewFor<CPDF_Number>(pdfium::form_fields::kFf,
                                  static_cast<int>(dwFlags));
}

WideString CPDF_FormField::GetValue(bool bDefault) const {
  if (GetType() == kCheckBox || GetType() == kRadioButton)
    return GetCheckValue(bDefault);

  RetainPtr<const CPDF_Object> pValue =
      bDefault ? GetDefaultValueObject() : GetValueObject();
  if (!pValue) {
    if (!bDefault && m_Type != kText)
      pValue = GetDefaultValueObject();
    if (!pValue)
      return WideString();
  }

  switch (pValue->GetType()) {
    case CPDF_Object::kString:
    case CPDF_Object::kStream:
      return pValue->GetUnicodeText();
    case CPDF_Object::kArray: {
      RetainPtr<const CPDF_Object> pNewValue =
          pValue->AsArray()->GetDirectObjectAt(0);
      if (pNewValue)
        return pNewValue->GetUnicodeText();
      break;
    }
    default:
      break;
  }
  return WideString();
}

WideString CPDF_FormField::GetValue() const {
  return GetValue(false);
}

WideString CPDF_FormField::GetDefaultValue() const {
  return GetValue(true);
}

bool CPDF_FormField::SetValue(const WideString& value,
                              bool bDefault,
                              NotificationOption notify) {
  switch (GetType()) {
    case kCheckBox:
    case kRadioButton: {
      SetCheckValue(value, bDefault, notify);
      return true;
    }
    case kFile:
    case kRichText:
    case kText:
    case kComboBox: {
      WideString csValue = value;
      if (notify == NotificationOption::kNotify &&
          !m_pForm->NotifyBeforeValueChange(this, csValue)) {
        return false;
      }
      ByteString key(bDefault ? pdfium::form_fields::kDV
                              : pdfium::form_fields::kV);
      m_pDict->SetNewFor<CPDF_String>(key, csValue.AsStringView());

      int iIndex;
      if (GetType() == kComboBox) {
        iIndex = FindOption(csValue);
      } else {
        iIndex = -1;
      }
      if (iIndex < 0) {
        if (m_Type == kRichText && !bDefault) {
          m_pDict->SetFor(pdfium::form_fields::kRV,
                          m_pDict->GetObjectFor(key)->Clone());
        }
        m_pDict->RemoveFor("I");
      } else {
        if (!bDefault) {
          ClearSelection(NotificationOption::kDoNotNotify);
          SetItemSelection(iIndex, NotificationOption::kDoNotNotify);
        }
      }
      if (notify == NotificationOption::kNotify)
        m_pForm->NotifyAfterValueChange(this);
      break;
    }
    case kListBox: {
      int iIndex = FindOption(value);
      if (iIndex < 0)
        return false;

      if (bDefault && iIndex == GetDefaultSelectedItem())
        return false;

      if (notify == NotificationOption::kNotify &&
          !m_pForm->NotifyBeforeSelectionChange(this, value)) {
        return false;
      }
      if (!bDefault) {
        ClearSelection(NotificationOption::kDoNotNotify);
        SetItemSelection(iIndex, NotificationOption::kDoNotNotify);
      }
      if (notify == NotificationOption::kNotify)
        m_pForm->NotifyAfterSelectionChange(this);
      break;
    }
    default:
      break;
  }
  return true;
}

bool CPDF_FormField::SetValue(const WideString& value,
                              NotificationOption notify) {
  return SetValue(value, false, notify);
}

int CPDF_FormField::GetMaxLen() const {
  RetainPtr<const CPDF_Object> pObj = GetFieldAttrInternal("MaxLen");
  if (pObj)
    return pObj->GetInteger();

  for (auto& pControl : GetControls()) {
    if (!pControl)
      continue;

    RetainPtr<const CPDF_Dictionary> pWidgetDict = pControl->GetWidgetDict();
    if (pWidgetDict->KeyExist("MaxLen"))
      return pWidgetDict->GetIntegerFor("MaxLen");
  }
  return 0;
}

int CPDF_FormField::CountSelectedItems() const {
  const CPDF_Object* pValue = GetValueOrSelectedIndicesObject();
  if (!pValue)
    return 0;

  if (pValue->IsString() || pValue->IsNumber())
    return pValue->GetString().IsEmpty() ? 0 : 1;
  const CPDF_Array* pArray = pValue->AsArray();
  return pArray ? fxcrt::CollectionSize<int>(*pArray) : 0;
}

int CPDF_FormField::GetSelectedIndex(int index) const {
  const CPDF_Object* pValue = GetValueOrSelectedIndicesObject();
  if (!pValue)
    return -1;

  if (pValue->IsNumber())
    return pValue->GetInteger();

  WideString sel_value;
  if (pValue->IsString()) {
    if (index != 0)
      return -1;
    sel_value = pValue->GetUnicodeText();
  } else {
    const CPDF_Array* pArray = pValue->AsArray();
    if (!pArray || index < 0)
      return -1;

    RetainPtr<const CPDF_Object> elementValue =
        pArray->GetDirectObjectAt(index);
    sel_value = elementValue ? elementValue->GetUnicodeText() : WideString();
  }
  if (index < CountSelectedOptions()) {
    int iOptIndex = GetSelectedOptionIndex(index);
    WideString csOpt = GetOptionValue(iOptIndex);
    if (csOpt == sel_value)
      return iOptIndex;
  }
  for (int i = 0; i < CountOptions(); i++) {
    if (sel_value == GetOptionValue(i))
      return i;
  }
  return -1;
}

bool CPDF_FormField::ClearSelection(NotificationOption notify) {
  if (notify == NotificationOption::kNotify) {
    WideString csValue;
    int iIndex = GetSelectedIndex(0);
    if (iIndex >= 0)
      csValue = GetOptionLabel(iIndex);
    if (!NotifyListOrComboBoxBeforeChange(csValue))
      return false;
  }
  m_pDict->RemoveFor(pdfium::form_fields::kV);
  m_pDict->RemoveFor("I");
  if (notify == NotificationOption::kNotify)
    NotifyListOrComboBoxAfterChange();
  return true;
}

bool CPDF_FormField::IsItemSelected(int index) const {
  CHECK(IsComboOrListField(GetType()));
  if (index < 0 || index >= CountOptions()) {
    return false;
  }
  // First consider the /I entry if it is valid, then fall back to the /V entry.
  return m_bUseSelectedIndices ? IsSelectedIndex(index)
                               : IsSelectedOption(GetOptionValue(index));
}

bool CPDF_FormField::SetItemSelection(int index, NotificationOption notify) {
  CHECK(IsComboOrListField(GetType()));
  if (index < 0 || index >= CountOptions()) {
    return false;
  }
  WideString opt_value = GetOptionValue(index);
  if (notify == NotificationOption::kNotify &&
      !NotifyListOrComboBoxBeforeChange(opt_value)) {
    return false;
  }

  SetItemSelectionSelected(index, opt_value);

  // UseSelectedIndicesObject() has a non-trivial linearithmic run-time, so run
  // only if necessary.
  if (!m_bUseSelectedIndices)
    m_bUseSelectedIndices = UseSelectedIndicesObject();

  if (notify == NotificationOption::kNotify)
    NotifyListOrComboBoxAfterChange();
  return true;
}

void CPDF_FormField::SetItemSelectionSelected(int index,
                                              const WideString& opt_value) {
  if (GetType() != kListBox) {
    m_pDict->SetNewFor<CPDF_String>(pdfium::form_fields::kV,
                                    opt_value.AsStringView());
    auto pI = m_pDict->SetNewFor<CPDF_Array>("I");
    pI->AppendNew<CPDF_Number>(index);
    return;
  }

  SelectOption(index);
  if (!m_bIsMultiSelectListBox) {
    m_pDict->SetNewFor<CPDF_String>(pdfium::form_fields::kV,
                                    opt_value.AsStringView());
    return;
  }

  auto pArray = m_pDict->SetNewFor<CPDF_Array>(pdfium::form_fields::kV);
  for (int i = 0; i < CountOptions(); i++) {
    if (i == index || IsItemSelected(i))
      pArray->AppendNew<CPDF_String>(GetOptionValue(i).AsStringView());
  }
}

int CPDF_FormField::GetDefaultSelectedItem() const {
  CHECK(IsComboOrListField(GetType()));
  RetainPtr<const CPDF_Object> pValue = GetDefaultValueObject();
  if (!pValue)
    return -1;
  WideString csDV = pValue->GetUnicodeText();
  if (csDV.IsEmpty())
    return -1;
  for (int i = 0; i < CountOptions(); i++) {
    if (csDV == GetOptionValue(i))
      return i;
  }
  return -1;
}

int CPDF_FormField::CountOptions() const {
  CHECK(HasOptField(GetType()));
  RetainPtr<const CPDF_Array> pArray = ToArray(GetFieldAttrInternal("Opt"));
  return pArray ? fxcrt::CollectionSize<int>(*pArray) : 0;
}

WideString CPDF_FormField::GetOptionText(int index, int sub_index) const {
  CHECK(HasOptField(GetType()));
  RetainPtr<const CPDF_Array> pArray = ToArray(GetFieldAttrInternal("Opt"));
  if (!pArray)
    return WideString();

  RetainPtr<const CPDF_Object> pOption = pArray->GetDirectObjectAt(index);
  if (!pOption)
    return WideString();

  const CPDF_Array* pOptionArray = pOption->AsArray();
  if (pOptionArray)
    pOption = pOptionArray->GetDirectObjectAt(sub_index);

  if (!pOption)
    return WideString();

  const CPDF_String* pString = pOption->AsString();
  return pString ? pString->GetUnicodeText() : WideString();
}

WideString CPDF_FormField::GetOptionLabel(int index) const {
  return GetOptionText(index, 1);
}

WideString CPDF_FormField::GetOptionValue(int index) const {
  return GetOptionText(index, 0);
}

int CPDF_FormField::FindOption(const WideString& csOptValue) const {
  for (int i = 0; i < CountOptions(); i++) {
    if (GetOptionValue(i) == csOptValue)
      return i;
  }
  return -1;
}

bool CPDF_FormField::CheckControl(int iControlIndex,
                                  bool bChecked,
                                  NotificationOption notify) {
  DCHECK(GetType() == kCheckBox || GetType() == kRadioButton);
  CPDF_FormControl* pControl = GetControl(iControlIndex);
  if (!pControl)
    return false;
  if (!bChecked && pControl->IsChecked() == bChecked)
    return false;

  const WideString csWExport = pControl->GetExportValue();
  int iCount = CountControls();
  for (int i = 0; i < iCount; i++) {
    CPDF_FormControl* pCtrl = GetControl(i);
    if (m_bIsUnison) {
      WideString csEValue = pCtrl->GetExportValue();
      if (csEValue == csWExport) {
        if (pCtrl->GetOnStateName() == pControl->GetOnStateName())
          pCtrl->CheckControl(bChecked);
        else if (bChecked)
          pCtrl->CheckControl(false);
      } else if (bChecked) {
        pCtrl->CheckControl(false);
      }
    } else {
      if (i == iControlIndex)
        pCtrl->CheckControl(bChecked);
      else if (bChecked)
        pCtrl->CheckControl(false);
    }
  }

  RetainPtr<const CPDF_Object> pOpt = GetFieldAttrInternal("Opt");
  if (!ToArray(pOpt)) {
    ByteString csBExport = PDF_EncodeText(csWExport.AsStringView());
    if (bChecked) {
      m_pDict->SetNewFor<CPDF_Name>(pdfium::form_fields::kV, csBExport);
    } else {
      ByteString csV;
      const CPDF_Object* pV = GetValueObject();
      if (pV)
        csV = pV->GetString();
      if (csV == csBExport)
        m_pDict->SetNewFor<CPDF_Name>(pdfium::form_fields::kV, "Off");
    }
  } else if (bChecked) {
    m_pDict->SetNewFor<CPDF_Name>(pdfium::form_fields::kV,
                                  ByteString::FormatInteger(iControlIndex));
  }
  if (notify == NotificationOption::kNotify)
    m_pForm->NotifyAfterCheckedStatusChange(this);
  return true;
}

WideString CPDF_FormField::GetCheckValue(bool bDefault) const {
  DCHECK(GetType() == kCheckBox || GetType() == kRadioButton);
  auto csExport = WideString::FromASCII("Off");
  int iCount = CountControls();
  for (int i = 0; i < iCount; i++) {
    CPDF_FormControl* pControl = GetControl(i);
    bool bChecked =
        bDefault ? pControl->IsDefaultChecked() : pControl->IsChecked();
    if (bChecked) {
      csExport = pControl->GetExportValue();
      break;
    }
  }
  return csExport;
}

bool CPDF_FormField::SetCheckValue(const WideString& value,
                                   bool bDefault,
                                   NotificationOption notify) {
  DCHECK(GetType() == kCheckBox || GetType() == kRadioButton);
  int iCount = CountControls();
  for (int i = 0; i < iCount; i++) {
    CPDF_FormControl* pControl = GetControl(i);
    WideString csExport = pControl->GetExportValue();
    bool val = csExport == value;
    if (!bDefault) {
      CheckControl(GetControlIndex(pControl), val,
                   NotificationOption::kDoNotNotify);
    }
    if (val)
      break;
  }
  if (notify == NotificationOption::kNotify)
    m_pForm->NotifyAfterCheckedStatusChange(this);
  return true;
}

int CPDF_FormField::GetTopVisibleIndex() const {
  RetainPtr<const CPDF_Object> pObj = GetFieldAttrInternal("TI");
  return pObj ? pObj->GetInteger() : 0;
}

int CPDF_FormField::CountSelectedOptions() const {
  RetainPtr<const CPDF_Array> pArray = ToArray(GetSelectedIndicesObject());
  return pArray ? fxcrt::CollectionSize<int>(*pArray) : 0;
}

int CPDF_FormField::GetSelectedOptionIndex(int index) const {
  if (index < 0)
    return 0;

  RetainPtr<const CPDF_Array> pArray = ToArray(GetSelectedIndicesObject());
  if (!pArray)
    return -1;

  return index < fxcrt::CollectionSize<int>(*pArray)
             ? pArray->GetIntegerAt(index)
             : -1;
}

bool CPDF_FormField::IsSelectedOption(const WideString& wsOptValue) const {
  RetainPtr<const CPDF_Object> pValueObject = GetValueObject();
  if (!pValueObject)
    return false;

  const CPDF_Array* pValueArray = pValueObject->AsArray();
  if (pValueArray) {
    CPDF_ArrayLocker locker(pValueArray);
    for (const auto& pObj : locker) {
      if (pObj->IsString() && pObj->GetUnicodeText() == wsOptValue)
        return true;
    }
  }

  return pValueObject->IsString() &&
         pValueObject->GetUnicodeText() == wsOptValue;
}

bool CPDF_FormField::IsSelectedIndex(int iOptIndex) const {
  RetainPtr<const CPDF_Object> pSelectedIndicesObject =
      GetSelectedIndicesObject();
  if (!pSelectedIndicesObject)
    return false;

  const CPDF_Array* pSelectedIndicesArray = pSelectedIndicesObject->AsArray();
  if (pSelectedIndicesArray) {
    CPDF_ArrayLocker locker(pSelectedIndicesArray);
    for (const auto& pObj : locker) {
      if (pObj->IsNumber() && pObj->GetInteger() == iOptIndex)
        return true;
    }
  }

  return pSelectedIndicesObject->IsNumber() &&
         pSelectedIndicesObject->GetInteger() == iOptIndex;
}

void CPDF_FormField::SelectOption(int iOptIndex) {
  RetainPtr<CPDF_Array> pArray = m_pDict->GetOrCreateArrayFor("I");
  for (size_t i = 0; i < pArray->size(); i++) {
    int iFind = pArray->GetIntegerAt(i);
    if (iFind == iOptIndex)
      return;

    if (iFind > iOptIndex) {
      pArray->InsertNewAt<CPDF_Number>(i, iOptIndex);
      return;
    }
  }
  pArray->AppendNew<CPDF_Number>(iOptIndex);
}

bool CPDF_FormField::UseSelectedIndicesObject() const {
  CHECK(IsComboOrListField(GetType()));

  RetainPtr<const CPDF_Object> pSelectedIndicesObject =
      GetSelectedIndicesObject();
  if (!pSelectedIndicesObject)
    return false;

  // If there's not value object, then just use the indices object.
  RetainPtr<const CPDF_Object> pValueObject = GetValueObject();
  if (!pValueObject)
    return true;

  // Verify that the selected indices object is either an array or a number and
  // count the number of indices.
  size_t selected_indices_size;
  const CPDF_Array* pSelectedIndicesArray = pSelectedIndicesObject->AsArray();
  if (pSelectedIndicesArray)
    selected_indices_size = pSelectedIndicesArray->size();
  else if (pSelectedIndicesObject->IsNumber())
    selected_indices_size = 1;
  else
    return false;

  // Verify that the number of values is equal to |selected_indices_size|. Then,
  // count the number of occurrences of each of the distinct values in the
  // values object.
  std::map<WideString, size_t> values;
  const CPDF_Array* pValueArray = pValueObject->AsArray();
  if (pValueArray) {
    if (pValueArray->size() != selected_indices_size)
      return false;
    CPDF_ArrayLocker locker(pValueArray);
    for (const auto& pObj : locker) {
      if (pObj->IsString())
        values[pObj->GetUnicodeText()]++;
    }
  } else if (pValueObject->IsString()) {
    if (selected_indices_size != 1)
      return false;
    values[pValueObject->GetUnicodeText()]++;
  }

  // Validate each index in the selected indices object. Then, verify that items
  // identified by selected indices entry do not differ from those in the values
  // entry of the field dictionary.
  const int num_options = CountOptions();
  if (pSelectedIndicesArray) {
    CPDF_ArrayLocker locker(pSelectedIndicesArray);
    for (const auto& pObj : locker) {
      if (!pObj->IsNumber())
        return false;

      int index = pObj->GetInteger();
      if (index < 0 || index >= num_options)
        return false;

      WideString wsOptValue = GetOptionValue(index);
      auto it = values.find(wsOptValue);
      if (it == values.end())
        return false;

      it->second--;
      if (it->second == 0)
        values.erase(it);
    }

    return values.empty();
  }

  DCHECK(pSelectedIndicesObject->IsNumber());
  int index = pSelectedIndicesObject->GetInteger();
  if (index < 0 || index >= num_options)
    return false;

  return pdfium::Contains(values, GetOptionValue(index));
}

bool CPDF_FormField::NotifyListOrComboBoxBeforeChange(const WideString& value) {
  switch (GetType()) {
    case kListBox:
      return m_pForm->NotifyBeforeSelectionChange(this, value);
    case kComboBox:
      return m_pForm->NotifyBeforeValueChange(this, value);
    default:
      return true;
  }
}

void CPDF_FormField::NotifyListOrComboBoxAfterChange() {
  switch (GetType()) {
    case kListBox:
      m_pForm->NotifyAfterSelectionChange(this);
      break;
    case kComboBox:
      m_pForm->NotifyAfterValueChange(this);
      break;
    default:
      break;
  }
}

RetainPtr<const CPDF_Object> CPDF_FormField::GetFieldAttrInternal(
    const ByteString& name) const {
  return GetFieldAttrRecursive(m_pDict.Get(), name, 0);
}

const CPDF_Dictionary* CPDF_FormField::GetFieldDictInternal() const {
  return m_pDict.Get();
}

RetainPtr<const CPDF_Object> CPDF_FormField::GetDefaultValueObject() const {
  return GetFieldAttrInternal(pdfium::form_fields::kDV);
}

RetainPtr<const CPDF_Object> CPDF_FormField::GetValueObject() const {
  return GetFieldAttrInternal(pdfium::form_fields::kV);
}

RetainPtr<const CPDF_Object> CPDF_FormField::GetSelectedIndicesObject() const {
  CHECK(IsComboOrListField(GetType()));
  return GetFieldAttrInternal("I");
}

RetainPtr<const CPDF_Object> CPDF_FormField::GetValueOrSelectedIndicesObject()
    const {
  CHECK(IsComboOrListField(GetType()));
  RetainPtr<const CPDF_Object> pValue = GetValueObject();
  return pValue ? pValue : GetSelectedIndicesObject();
}

const std::vector<UnownedPtr<CPDF_FormControl>>& CPDF_FormField::GetControls()
    const {
  return m_pForm->GetControlsForField(this);
}
