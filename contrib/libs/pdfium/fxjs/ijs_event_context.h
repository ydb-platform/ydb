// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FXJS_IJS_EVENT_CONTEXT_H_
#define FXJS_IJS_EVENT_CONTEXT_H_

#include <optional>

#include "core/fxcrt/widestring.h"
#include "fxjs/ijs_runtime.h"

class CPDF_FormField;

// Records the details of an event and triggers JS execution for it. There
// can be more than one of these at any given time, as JS callbacks to C++
// may trigger new events on top of one another.
class IJS_EventContext {
 public:
  virtual ~IJS_EventContext() = default;

  virtual std::optional<IJS_Runtime::JS_Error> RunScript(
      const WideString& script) = 0;

  virtual void OnDoc_Open(const WideString& strTargetName) = 0;
  virtual void OnDoc_WillPrint() = 0;
  virtual void OnDoc_DidPrint() = 0;
  virtual void OnDoc_WillSave() = 0;
  virtual void OnDoc_DidSave() = 0;
  virtual void OnDoc_WillClose() = 0;

  virtual void OnPage_Open() = 0;
  virtual void OnPage_Close() = 0;
  virtual void OnPage_InView() = 0;
  virtual void OnPage_OutView() = 0;

  virtual void OnField_MouseDown(bool bModifier,
                                 bool bShift,
                                 CPDF_FormField* pTarget) = 0;
  virtual void OnField_MouseEnter(bool bModifier,
                                  bool bShift,
                                  CPDF_FormField* pTarget) = 0;
  virtual void OnField_MouseExit(bool bModifier,
                                 bool bShift,
                                 CPDF_FormField* pTarget) = 0;
  virtual void OnField_MouseUp(bool bModifier,
                               bool bShift,
                               CPDF_FormField* pTarget) = 0;
  virtual void OnField_Focus(bool bModifier,
                             bool bShift,
                             CPDF_FormField* pTarget,
                             WideString* Value) = 0;
  virtual void OnField_Blur(bool bModifier,
                            bool bShift,
                            CPDF_FormField* pTarget,
                            WideString* Value) = 0;
  virtual void OnField_Calculate(CPDF_FormField* pSource,
                                 CPDF_FormField* pTarget,
                                 WideString* Value,
                                 bool* bRc) = 0;
  virtual void OnField_Format(CPDF_FormField* pTarget, WideString* Value) = 0;
  virtual void OnField_Keystroke(WideString* strChange,
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
                                 bool* bRc) = 0;
  virtual void OnField_Validate(WideString* strChange,
                                const WideString& strChangeEx,
                                bool bKeyDown,
                                bool bModifier,
                                bool bShift,
                                CPDF_FormField* pTarget,
                                WideString* Value,
                                bool* bRc) = 0;

  virtual void OnExternal_Exec() = 0;
};

#endif  // FXJS_IJS_EVENT_CONTEXT_H_
