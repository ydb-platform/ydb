// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FXJS_IJS_RUNTIME_H_
#define FXJS_IJS_RUNTIME_H_

#include <memory>
#include <optional>

#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"

class CJS_Runtime;
class CPDFSDK_FormFillEnvironment;
class IJS_EventContext;

// Owns the FJXS objects needed to actually execute JS, if possible. This
// virtual interface is backed by either an actual JS runtime, or a stub,
// when JS is not present.
class IJS_Runtime {
 public:
  struct JS_Error {
    int line;
    int column;
    WideString exception;

    JS_Error(int line, int column, const WideString& exception);
  };

  class ScopedEventContext {
   public:
    FX_STACK_ALLOCATED();

    explicit ScopedEventContext(IJS_Runtime* pRuntime);
    ~ScopedEventContext();

    IJS_EventContext* Get() const { return m_pContext; }
    IJS_EventContext* operator->() const { return m_pContext; }

   private:
    UnownedPtr<IJS_Runtime> const m_pRuntime;
    UnownedPtr<IJS_EventContext> m_pContext;
  };

  static void Initialize(unsigned int slot, void* isolate, void* platform);
  static void Destroy();
  static std::unique_ptr<IJS_Runtime> Create(
      CPDFSDK_FormFillEnvironment* pFormFillEnv);

  virtual ~IJS_Runtime();

  virtual CJS_Runtime* AsCJSRuntime() = 0;
  virtual IJS_EventContext* NewEventContext() = 0;
  virtual void ReleaseEventContext(IJS_EventContext* pContext) = 0;
  virtual CPDFSDK_FormFillEnvironment* GetFormFillEnv() const = 0;
  virtual std::optional<JS_Error> ExecuteScript(const WideString& script) = 0;

 protected:
  IJS_Runtime() = default;
};

#endif  // FXJS_IJS_RUNTIME_H_
