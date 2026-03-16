// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_pagelabel.h"

#include <array>
#include <utility>

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfdoc/cpdf_numbertree.h"
#include "core/fxcrt/stl_util.h"

namespace {

WideString MakeRoman(int num) {
  constexpr auto kArabic = fxcrt::ToArray<const int>(
      {1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1});
  const auto kRoman = fxcrt::ToArray<const WideStringView>(
      {L"m", L"cm", L"d", L"cd", L"c", L"xc", L"l", L"xl", L"x", L"ix", L"v",
       L"iv", L"i"});
  constexpr int kMaxNum = 1000000;

  num %= kMaxNum;
  int i = 0;
  WideString result;
  result.Reserve(10);  // Should cover most use cases.
  while (num > 0) {
    while (num >= kArabic[i]) {
      num -= kArabic[i];
      result += kRoman[i];
    }
    ++i;
  }
  return result;
}

WideString MakeLetters(int num) {
  if (num == 0) {
    return WideString();
  }

  constexpr int kMaxCount = 1000;
  constexpr int kLetterCount = 26;

  --num;
  const int count = (num / kLetterCount + 1) % kMaxCount;
  const wchar_t ch = L'a' + num % kLetterCount;

  WideString result;
  {
    auto result_span = result.GetBuffer(count);
    fxcrt::Fill(result_span, ch);
    result.ReleaseBuffer(count);
  }
  return result;
}

WideString GetLabelNumPortion(int num, const ByteString& style) {
  if (style.IsEmpty()) {
    return WideString();
  }
  if (style == "D") {
    return WideString::FormatInteger(num);
  }
  if (style == "R") {
    WideString result = MakeRoman(num);
    result.MakeUpper();
    return result;
  }
  if (style == "r") {
    return MakeRoman(num);
  }
  if (style == "A") {
    WideString result = MakeLetters(num);
    result.MakeUpper();
    return result;
  }
  if (style == "a") {
    return MakeLetters(num);
  }
  return WideString();
}

}  // namespace

CPDF_PageLabel::CPDF_PageLabel(CPDF_Document* doc) : doc_(doc) {}

CPDF_PageLabel::~CPDF_PageLabel() = default;

std::optional<WideString> CPDF_PageLabel::GetLabel(int page_index) const {
  if (!doc_) {
    return std::nullopt;
  }

  if (page_index < 0 || page_index >= doc_->GetPageCount()) {
    return std::nullopt;
  }

  const CPDF_Dictionary* root_dict = doc_->GetRoot();
  if (!root_dict) {
    return std::nullopt;
  }

  RetainPtr<const CPDF_Dictionary> labels_dict =
      root_dict->GetDictFor("PageLabels");
  if (!labels_dict) {
    return std::nullopt;
  }

  CPDF_NumberTree number_tree(std::move(labels_dict));
  RetainPtr<const CPDF_Object> label_value;
  std::optional<CPDF_NumberTree::KeyValue> lower_bound =
      number_tree.GetLowerBound(page_index);
  if (lower_bound.has_value()) {
    label_value = lower_bound.value().value;
  }

  const CPDF_Dictionary* label_dict =
      label_value ? label_value->GetDirect()->AsDictionary() : nullptr;
  if (!label_dict) {
    return WideString::FormatInteger(page_index + 1);
  }

  WideString label;
  if (label_dict->KeyExist("P")) {
    label = label_dict->GetUnicodeTextFor("P");
  }

  ByteString style = label_dict->GetByteStringFor("S", ByteString());
  int label_number =
      page_index - lower_bound.value().key + label_dict->GetIntegerFor("St", 1);
  label += GetLabelNumPortion(label_number, style);
  return label;
}
