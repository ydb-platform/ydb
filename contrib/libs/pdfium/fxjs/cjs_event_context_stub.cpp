// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fxjs/cjs_event_context_stub.h"

CJS_EventContextStub::CJS_EventContextStub() = default;

CJS_EventContextStub::~CJS_EventContextStub() = default;

std::optional<IJS_Runtime::JS_Error> CJS_EventContextStub::RunScript(
    const WideString& script) {
  return IJS_Runtime::JS_Error(1, 1, L"JavaScript support not present");
}
