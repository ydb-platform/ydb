// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FXJS_CJS_EVENT_CONTEXT_STUB_H_
#define FXJS_CJS_EVENT_CONTEXT_STUB_H_

#include "fxjs/ijs_event_context.h"

class CJS_EventContextStub final : public IJS_EventContext {
 public:
  CJS_EventContextStub();
  ~CJS_EventContextStub() override;

  // IJS_EventContext:
  std::optional<IJS_Runtime::JS_Error> RunScript(
      const WideString& script) override;

  void OnDoc_Open(const WideString& strTargetName) override {}
  void OnDoc_WillPrint() override {}
  void OnDoc_DidPrint() override {}
  void OnDoc_WillSave() override {}
  void OnDoc_DidSave() override {}
  void OnDoc_WillClose() override {}
  void OnPage_Open() override {}
  void OnPage_Close() override {}
  void OnPage_InView() override {}
  void OnPage_OutView() override {}
  void OnField_MouseDown(bool bModifier,
                         bool bShift,
                         CPDF_FormField* pTarget) override {}
  void OnField_MouseEnter(bool bModifier,
                          bool bShift,
                          CPDF_FormField* pTarget) override {}
  void OnField_MouseExit(bool bModifier,
                         bool bShift,
                         CPDF_FormField* pTarget) override {}
  void OnField_MouseUp(bool bModifier,
                       bool bShift,
                       CPDF_FormField* pTarget) override {}
  void OnField_Focus(bool bModifier,
                     bool bShift,
                     CPDF_FormField* pTarget,
                     WideString* Value) override {}
  void OnField_Blur(bool bModifier,
                    bool bShift,
                    CPDF_FormField* pTarget,
                    WideString* Value) override {}
  void OnField_Calculate(CPDF_FormField* pSource,
                         CPDF_FormField* pTarget,
                         WideString* pValue,
                         bool* pRc) override {}
  void OnField_Format(CPDF_FormField* pTarget, WideString* Value) override {}
  void OnField_Keystroke(WideString* strChange,
                         const WideString& strChangeEx,
                         bool KeyDown,
                         bool bModifier,
                         int* nSelEnd,
                         int* nSelStart,
                         bool bShift,
                         CPDF_FormField* pTarget,
                         WideString* Value,
                         bool bWillCommit,
                         bool bFieldFull,
                         bool* bRc) override {}
  void OnField_Validate(WideString* strChange,
                        const WideString& strChangeEx,
                        bool bKeyDown,
                        bool bModifier,
                        bool bShift,
                        CPDF_FormField* pTarget,
                        WideString* Value,
                        bool* bRc) override {}
  void OnExternal_Exec() override {}
};

#endif  // FXJS_CJS_EVENT_CONTEXT_STUB_H_
