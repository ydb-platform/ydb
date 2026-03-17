// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fxjs/js_define.h"

#include <math.h>
#include <stdarg.h>

#include <algorithm>
#include <limits>

#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/span.h"
#include "fxjs/cjs_document.h"
#include "fxjs/cjs_object.h"
#include "fxjs/fx_date_helpers.h"
#include "fxjs/fxv8.h"
#include "v8/include/v8-context.h"
#include "v8/include/v8-function.h"
#include "v8/include/v8-isolate.h"

void JSDestructor(v8::Local<v8::Object> obj) {
  CFXJS_Engine::SetBinding(obj, nullptr);
}

double JS_DateParse(v8::Isolate* pIsolate, const WideString& str) {
  v8::Isolate::Scope isolate_scope(pIsolate);
  v8::HandleScope scope(pIsolate);

  v8::Local<v8::Context> context = pIsolate->GetCurrentContext();

  // Use the built-in object method.
  v8::MaybeLocal<v8::Value> maybe_value =
      context->Global()->Get(context, fxv8::NewStringHelper(pIsolate, "Date"));

  v8::Local<v8::Value> value;
  if (!maybe_value.ToLocal(&value) || !value->IsObject())
    return 0;

  v8::Local<v8::Object> obj = value.As<v8::Object>();
  maybe_value = obj->Get(context, fxv8::NewStringHelper(pIsolate, "parse"));
  if (!maybe_value.ToLocal(&value) || !value->IsFunction())
    return 0;

  v8::Local<v8::Function> func = value.As<v8::Function>();
  static constexpr int argc = 1;
  v8::Local<v8::Value> argv[argc] = {
      fxv8::NewStringHelper(pIsolate, str.AsStringView()),
  };
  maybe_value = func->Call(context, context->Global(), argc, argv);
  if (!maybe_value.ToLocal(&value) || !value->IsNumber())
    return 0;

  double date = value.As<v8::Number>()->Value();
  return isfinite(date) ? FX_LocalTime(date) : date;
}

v8::LocalVector<v8::Value> ExpandKeywordParams(
    CJS_Runtime* pRuntime,
    pdfium::span<v8::Local<v8::Value>> originals,
    size_t nKeywords,
    ...) {
  DCHECK(nKeywords);

  v8::LocalVector<v8::Value> result(pRuntime->GetIsolate(), nKeywords);
  size_t size = std::min(originals.size(), nKeywords);
  for (size_t i = 0; i < size; ++i)
    result[i] = originals[i];

  if (originals.size() != 1 || !originals[0]->IsObject() ||
      originals[0]->IsArray()) {
    return result;
  }
  result[0] = v8::Local<v8::Value>();  // Make unknown.

  v8::Local<v8::Object> pObj = pRuntime->ToObject(originals[0]);
  va_list ap;
  va_start(ap, nKeywords);
  for (size_t i = 0; i < nKeywords; ++i) {
    const char* property = va_arg(ap, const char*);
    v8::Local<v8::Value> v8Value = pRuntime->GetObjectProperty(pObj, property);
    if (!v8Value->IsUndefined())
      result[i] = v8Value;
  }
  va_end(ap);

  return result;
}

bool IsExpandedParamKnown(v8::Local<v8::Value> value) {
  return !value.IsEmpty() &&
         (value->IsString() || value->IsNumber() || value->IsBoolean() ||
          value->IsDate() || value->IsObject() || value->IsNull() ||
          value->IsUndefined());
}
