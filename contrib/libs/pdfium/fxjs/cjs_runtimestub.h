// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FXJS_CJS_RUNTIMESTUB_H_
#define FXJS_CJS_RUNTIMESTUB_H_

#include <memory>

#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"
#include "fxjs/ijs_runtime.h"

class CPDFSDK_FormFillEnvironment;
class IJS_EventContext;

class CJS_RuntimeStub final : public IJS_Runtime {
 public:
  explicit CJS_RuntimeStub(CPDFSDK_FormFillEnvironment* pFormFillEnv);
  ~CJS_RuntimeStub() override;

  // IJS_Runtime:
  CJS_Runtime* AsCJSRuntime() override;
  IJS_EventContext* NewEventContext() override;
  void ReleaseEventContext(IJS_EventContext* pContext) override;
  CPDFSDK_FormFillEnvironment* GetFormFillEnv() const override;

  std::optional<IJS_Runtime::JS_Error> ExecuteScript(
      const WideString& script) override;

 private:
  UnownedPtr<CPDFSDK_FormFillEnvironment> const m_pFormFillEnv;
  std::unique_ptr<IJS_EventContext> m_pContext;
};

#endif  // FXJS_CJS_RUNTIMESTUB_H_
