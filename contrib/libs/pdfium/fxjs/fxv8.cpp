// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fxjs/fxv8.h"

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "v8/include/v8-container.h"
#include "v8/include/v8-date.h"
#include "v8/include/v8-exception.h"
#include "v8/include/v8-isolate.h"
#include "v8/include/v8-primitive.h"
#include "v8/include/v8-value.h"

namespace fxv8 {

bool IsUndefined(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsUndefined();
}

bool IsNull(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsNull();
}

bool IsBoolean(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsBoolean();
}

bool IsString(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsString();
}

bool IsNumber(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsNumber();
}

bool IsInteger(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsInt32();
}

bool IsObject(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsObject();
}

bool IsArray(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsArray();
}

bool IsDate(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsDate();
}

bool IsFunction(v8::Local<v8::Value> value) {
  return !value.IsEmpty() && value->IsFunction();
}

v8::Local<v8::Value> NewNullHelper(v8::Isolate* pIsolate) {
  return v8::Null(pIsolate);
}

v8::Local<v8::Value> NewUndefinedHelper(v8::Isolate* pIsolate) {
  return v8::Undefined(pIsolate);
}

v8::Local<v8::Number> NewNumberHelper(v8::Isolate* pIsolate, int number) {
  return v8::Int32::New(pIsolate, number);
}

v8::Local<v8::Number> NewNumberHelper(v8::Isolate* pIsolate, double number) {
  return v8::Number::New(pIsolate, number);
}

v8::Local<v8::Number> NewNumberHelper(v8::Isolate* pIsolate, float number) {
  return v8::Number::New(pIsolate, number);
}

v8::Local<v8::Boolean> NewBooleanHelper(v8::Isolate* pIsolate, bool b) {
  return v8::Boolean::New(pIsolate, b);
}

v8::Local<v8::String> NewStringHelper(v8::Isolate* pIsolate,
                                      ByteStringView str) {
  return v8::String::NewFromUtf8(pIsolate, str.unterminated_c_str(),
                                 v8::NewStringType::kNormal,
                                 pdfium::checked_cast<int>(str.GetLength()))
      .ToLocalChecked();
}

v8::Local<v8::String> NewStringHelper(v8::Isolate* pIsolate,
                                      WideStringView str) {
  return NewStringHelper(pIsolate, FX_UTF8Encode(str).AsStringView());
}

v8::Local<v8::Array> NewArrayHelper(v8::Isolate* pIsolate) {
  return v8::Array::New(pIsolate);
}

v8::Local<v8::Array> NewArrayHelper(v8::Isolate* pIsolate,
                                    pdfium::span<v8::Local<v8::Value>> values) {
  v8::Local<v8::Array> result = NewArrayHelper(pIsolate);
  for (size_t i = 0; i < values.size(); ++i) {
    fxv8::ReentrantPutArrayElementHelper(
        pIsolate, result, i,
        values[i].IsEmpty() ? fxv8::NewUndefinedHelper(pIsolate) : values[i]);
  }
  return result;
}

v8::Local<v8::Object> NewObjectHelper(v8::Isolate* pIsolate) {
  return v8::Object::New(pIsolate);
}

v8::Local<v8::Date> NewDateHelper(v8::Isolate* pIsolate, double d) {
  return v8::Date::New(pIsolate->GetCurrentContext(), d)
      .ToLocalChecked()
      .As<v8::Date>();
}

WideString ToWideString(v8::Isolate* pIsolate, v8::Local<v8::String> pValue) {
  v8::String::Utf8Value s(pIsolate, pValue);
  // SAFETY: required from V8.
  return WideString::FromUTF8(UNSAFE_BUFFERS(ByteStringView(*s, s.length())));
}

ByteString ToByteString(v8::Isolate* pIsolate, v8::Local<v8::String> pValue) {
  v8::String::Utf8Value s(pIsolate, pValue);
  // SAFETY: required from V8.
  return UNSAFE_BUFFERS(ByteString(*s, s.length()));
}

int ReentrantToInt32Helper(v8::Isolate* pIsolate, v8::Local<v8::Value> pValue) {
  if (pValue.IsEmpty())
    return 0;
  v8::TryCatch squash_exceptions(pIsolate);
  return pValue->Int32Value(pIsolate->GetCurrentContext()).FromMaybe(0);
}

bool ReentrantToBooleanHelper(v8::Isolate* pIsolate,
                              v8::Local<v8::Value> pValue) {
  if (pValue.IsEmpty())
    return false;
  v8::TryCatch squash_exceptions(pIsolate);
  return pValue->BooleanValue(pIsolate);
}

float ReentrantToFloatHelper(v8::Isolate* pIsolate,
                             v8::Local<v8::Value> pValue) {
  return static_cast<float>(ReentrantToDoubleHelper(pIsolate, pValue));
}

double ReentrantToDoubleHelper(v8::Isolate* pIsolate,
                               v8::Local<v8::Value> pValue) {
  if (pValue.IsEmpty())
    return 0.0;
  v8::TryCatch squash_exceptions(pIsolate);
  return pValue->NumberValue(pIsolate->GetCurrentContext()).FromMaybe(0.0);
}

WideString ReentrantToWideStringHelper(v8::Isolate* pIsolate,
                                       v8::Local<v8::Value> pValue) {
  if (pValue.IsEmpty())
    return WideString();

  v8::TryCatch squash_exceptions(pIsolate);
  v8::MaybeLocal<v8::String> maybe_string =
      pValue->ToString(pIsolate->GetCurrentContext());
  if (maybe_string.IsEmpty())
    return WideString();

  return ToWideString(pIsolate, maybe_string.ToLocalChecked());
}

ByteString ReentrantToByteStringHelper(v8::Isolate* pIsolate,
                                       v8::Local<v8::Value> pValue) {
  if (pValue.IsEmpty())
    return ByteString();

  v8::TryCatch squash_exceptions(pIsolate);
  v8::MaybeLocal<v8::String> maybe_string =
      pValue->ToString(pIsolate->GetCurrentContext());
  if (maybe_string.IsEmpty())
    return ByteString();

  return ToByteString(pIsolate, maybe_string.ToLocalChecked());
}

v8::Local<v8::Object> ReentrantToObjectHelper(v8::Isolate* pIsolate,
                                              v8::Local<v8::Value> pValue) {
  if (!fxv8::IsObject(pValue))
    return v8::Local<v8::Object>();

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::Context> context = pIsolate->GetCurrentContext();
  return pValue->ToObject(context).ToLocalChecked();
}

v8::Local<v8::Array> ReentrantToArrayHelper(v8::Isolate* pIsolate,
                                            v8::Local<v8::Value> pValue) {
  if (!fxv8::IsArray(pValue))
    return v8::Local<v8::Array>();

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::Context> context = pIsolate->GetCurrentContext();
  return v8::Local<v8::Array>::Cast(pValue->ToObject(context).ToLocalChecked());
}

v8::Local<v8::Value> ReentrantGetObjectPropertyHelper(
    v8::Isolate* pIsolate,
    v8::Local<v8::Object> pObj,
    ByteStringView bsUTF8PropertyName) {
  if (pObj.IsEmpty())
    return v8::Local<v8::Value>();

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::Value> val;
  if (!pObj->Get(pIsolate->GetCurrentContext(),
                 NewStringHelper(pIsolate, bsUTF8PropertyName))
           .ToLocal(&val)) {
    return v8::Local<v8::Value>();
  }
  return val;
}

std::vector<WideString> ReentrantGetObjectPropertyNamesHelper(
    v8::Isolate* pIsolate,
    v8::Local<v8::Object> pObj) {
  if (pObj.IsEmpty())
    return std::vector<WideString>();

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::Array> val;
  v8::Local<v8::Context> context = pIsolate->GetCurrentContext();
  if (!pObj->GetPropertyNames(context).ToLocal(&val))
    return std::vector<WideString>();

  std::vector<WideString> result;
  for (uint32_t i = 0; i < val->Length(); ++i) {
    result.push_back(ReentrantToWideStringHelper(
        pIsolate, val->Get(context, i).ToLocalChecked()));
  }
  return result;
}

bool ReentrantHasObjectOwnPropertyHelper(v8::Isolate* pIsolate,
                                         v8::Local<v8::Object> pObj,
                                         ByteStringView bsUTF8PropertyName) {
  if (pObj.IsEmpty())
    return false;

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::Context> pContext = pIsolate->GetCurrentContext();
  v8::Local<v8::String> hKey =
      fxv8::NewStringHelper(pIsolate, bsUTF8PropertyName);
  return pObj->HasRealNamedProperty(pContext, hKey).FromJust();
}

bool ReentrantSetObjectOwnPropertyHelper(v8::Isolate* pIsolate,
                                         v8::Local<v8::Object> pObj,
                                         ByteStringView bsUTF8PropertyName,
                                         v8::Local<v8::Value> pValue) {
  if (pObj.IsEmpty() || pValue.IsEmpty())
    return false;

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::String> name = NewStringHelper(pIsolate, bsUTF8PropertyName);
  return pObj->DefineOwnProperty(pIsolate->GetCurrentContext(), name, pValue)
      .FromMaybe(false);
}

bool ReentrantPutObjectPropertyHelper(v8::Isolate* pIsolate,
                                      v8::Local<v8::Object> pObj,
                                      ByteStringView bsUTF8PropertyName,
                                      v8::Local<v8::Value> pPut) {
  if (pObj.IsEmpty() || pPut.IsEmpty())
    return false;

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::String> name = NewStringHelper(pIsolate, bsUTF8PropertyName);
  v8::Maybe<bool> result = pObj->Set(pIsolate->GetCurrentContext(), name, pPut);
  return result.IsJust() && result.FromJust();
}

void ReentrantDeleteObjectPropertyHelper(v8::Isolate* pIsolate,
                                         v8::Local<v8::Object> pObj,
                                         ByteStringView bsUTF8PropertyName) {
  v8::TryCatch squash_exceptions(pIsolate);
  pObj->Delete(pIsolate->GetCurrentContext(),
               fxv8::NewStringHelper(pIsolate, bsUTF8PropertyName))
      .FromJust();
}

bool ReentrantPutArrayElementHelper(v8::Isolate* pIsolate,
                                    v8::Local<v8::Array> pArray,
                                    size_t index,
                                    v8::Local<v8::Value> pValue) {
  if (pArray.IsEmpty())
    return false;

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Maybe<bool> result =
      pArray->Set(pIsolate->GetCurrentContext(),
                  pdfium::checked_cast<uint32_t>(index), pValue);
  return result.IsJust() && result.FromJust();
}

v8::Local<v8::Value> ReentrantGetArrayElementHelper(v8::Isolate* pIsolate,
                                                    v8::Local<v8::Array> pArray,
                                                    size_t index) {
  if (pArray.IsEmpty())
    return v8::Local<v8::Value>();

  v8::TryCatch squash_exceptions(pIsolate);
  v8::Local<v8::Value> val;
  if (!pArray
           ->Get(pIsolate->GetCurrentContext(),
                 pdfium::checked_cast<uint32_t>(index))
           .ToLocal(&val)) {
    return v8::Local<v8::Value>();
  }
  return val;
}

size_t GetArrayLengthHelper(v8::Local<v8::Array> pArray) {
  if (pArray.IsEmpty())
    return 0;
  return pArray->Length();
}

void ThrowExceptionHelper(v8::Isolate* pIsolate, ByteStringView str) {
  pIsolate->ThrowException(NewStringHelper(pIsolate, str));
}

void ThrowExceptionHelper(v8::Isolate* pIsolate, WideStringView str) {
  pIsolate->ThrowException(NewStringHelper(pIsolate, str));
}

}  // namespace fxv8
