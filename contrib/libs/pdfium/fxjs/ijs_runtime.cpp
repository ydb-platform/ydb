// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "fxjs/ijs_runtime.h"

#include "fxjs/cjs_runtimestub.h"

#ifdef PDF_ENABLE_V8
#include "fpdfsdk/cpdfsdk_formfillenvironment.h"
#include "fxjs/cfxjs_engine.h"
#include "fxjs/cjs_runtime.h"
#include "fxjs/global_timer.h"
#ifdef PDF_ENABLE_XFA
#include "fxjs/gc/heap.h"
#endif  // PDF_ENABLE_XFA
#endif  // PDF_ENABLE_V8

IJS_Runtime::ScopedEventContext::ScopedEventContext(IJS_Runtime* pRuntime)
    : m_pRuntime(pRuntime), m_pContext(pRuntime->NewEventContext()) {}

IJS_Runtime::ScopedEventContext::~ScopedEventContext() {
  m_pRuntime->ReleaseEventContext(m_pContext.ExtractAsDangling());
}

// static
void IJS_Runtime::Initialize(unsigned int slot, void* isolate, void* platform) {
#ifdef PDF_ENABLE_V8
  GlobalTimer::InitializeGlobals();
  FXJS_Initialize(slot, static_cast<v8::Isolate*>(isolate));
#ifdef PDF_ENABLE_XFA
  FXGC_Initialize(static_cast<v8::Platform*>(platform),
                  static_cast<v8::Isolate*>(isolate));
#endif  // PDF_ENABLE_XFA
#endif  // PDF_ENABLE_V8
}

// static
void IJS_Runtime::Destroy() {
#ifdef PDF_ENABLE_V8
#ifdef PDF_ENABLE_XFA
  FXGC_Release();
#endif  // PDF_ENABLE_XFA
  FXJS_Release();
  GlobalTimer::DestroyGlobals();
#endif  // PDF_ENABLE_V8
}

// static
std::unique_ptr<IJS_Runtime> IJS_Runtime::Create(
    CPDFSDK_FormFillEnvironment* pFormFillEnv) {
#ifdef PDF_ENABLE_V8
  if (pFormFillEnv->IsJSPlatformPresent())
    return std::make_unique<CJS_Runtime>(pFormFillEnv);
#endif
  return std::make_unique<CJS_RuntimeStub>(pFormFillEnv);
}

IJS_Runtime::~IJS_Runtime() = default;

IJS_Runtime::JS_Error::JS_Error(int line,
                                int column,
                                const WideString& exception)
    : line(line), column(column), exception(exception) {}
