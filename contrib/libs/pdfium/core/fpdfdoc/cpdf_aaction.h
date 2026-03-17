// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_AACTION_H_
#define CORE_FPDFDOC_CPDF_AACTION_H_

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfdoc/cpdf_action.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_AAction {
 public:
  enum AActionType {
    kCursorEnter = 0,
    kCursorExit,
    kButtonDown,
    kButtonUp,
    kGetFocus,
    kLoseFocus,
    kPageOpen,
    kPageClose,
    kPageVisible,
    kPageInvisible,
    kOpenPage,
    kClosePage,
    kKeyStroke,
    kFormat,
    kValidate,
    kCalculate,
    kCloseDocument,
    kSaveDocument,
    kDocumentSaved,
    kPrintDocument,
    kDocumentPrinted,
    kDocumentOpen,
    kNumberOfActions  // Must be last.
  };

  explicit CPDF_AAction(RetainPtr<const CPDF_Dictionary> pDict);
  CPDF_AAction(const CPDF_AAction& that);
  ~CPDF_AAction();

  bool ActionExist(AActionType eType) const;
  CPDF_Action GetAction(AActionType eType) const;
  bool HasDict() const { return !!m_pDict; }

  static bool IsUserInput(AActionType type);

 private:
  RetainPtr<const CPDF_Dictionary> const m_pDict;
};

#endif  // CORE_FPDFDOC_CPDF_AACTION_H_
