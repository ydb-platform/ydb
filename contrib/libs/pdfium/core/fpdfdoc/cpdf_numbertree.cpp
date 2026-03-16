// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_numbertree.h"

#include <optional>
#include <utility>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"

namespace {

RetainPtr<const CPDF_Object> FindNumberNode(const CPDF_Dictionary* node_dict,
                                            int num) {
  RetainPtr<const CPDF_Array> limits_array = node_dict->GetArrayFor("Limits");
  if (limits_array && (num < limits_array->GetIntegerAt(0) ||
                       num > limits_array->GetIntegerAt(1))) {
    return nullptr;
  }
  RetainPtr<const CPDF_Array> numbers_array = node_dict->GetArrayFor("Nums");
  if (numbers_array) {
    for (size_t i = 0; i < numbers_array->size() / 2; i++) {
      int index = numbers_array->GetIntegerAt(i * 2);
      if (num == index) {
        return numbers_array->GetDirectObjectAt(i * 2 + 1);
      }
      if (index > num) {
        break;
      }
    }
    return nullptr;
  }

  RetainPtr<const CPDF_Array> kids_array = node_dict->GetArrayFor("Kids");
  if (!kids_array) {
    return nullptr;
  }

  for (size_t i = 0; i < kids_array->size(); i++) {
    RetainPtr<const CPDF_Dictionary> kid_dict = kids_array->GetDictAt(i);
    if (!kid_dict) {
      continue;
    }

    RetainPtr<const CPDF_Object> result = FindNumberNode(kid_dict.Get(), num);
    if (result) {
      return result;
    }
  }
  return nullptr;
}

std::optional<CPDF_NumberTree::KeyValue> FindLowerBound(
    const CPDF_Dictionary* node_dict,
    int num) {
  RetainPtr<const CPDF_Array> limits_array = node_dict->GetArrayFor("Limits");
  if (limits_array) {
    if (num < limits_array->GetIntegerAt(0)) {
      return std::nullopt;
    }
    const int max_value = limits_array->GetIntegerAt(1);
    if (num >= max_value) {
      return CPDF_NumberTree::KeyValue(max_value,
                                       FindNumberNode(node_dict, max_value));
    }
  }

  RetainPtr<const CPDF_Array> numbers_array = node_dict->GetArrayFor("Nums");
  if (numbers_array) {
    for (size_t i = numbers_array->size() / 2; i > 0; --i) {
      const size_t key_index = (i - 1) * 2;
      const int key = numbers_array->GetIntegerAt(key_index);
      if (num >= key) {
        const size_t value_index = key_index + 1;
        return CPDF_NumberTree::KeyValue(
            key, numbers_array->GetDirectObjectAt(value_index));
      }
    }
    return std::nullopt;
  }

  RetainPtr<const CPDF_Array> kids_array = node_dict->GetArrayFor("Kids");
  if (!kids_array) {
    return std::nullopt;
  }

  for (size_t i = kids_array->size(); i > 0; --i) {
    RetainPtr<const CPDF_Dictionary> kid_dict = kids_array->GetDictAt(i - 1);
    if (!kid_dict) {
      continue;
    }

    std::optional<CPDF_NumberTree::KeyValue> result =
        FindLowerBound(kid_dict.Get(), num);
    if (result.has_value()) {
      return result;
    }
  }
  return std::nullopt;
}

}  // namespace

CPDF_NumberTree::CPDF_NumberTree(RetainPtr<const CPDF_Dictionary> root)
    : root_(std::move(root)) {}

CPDF_NumberTree::~CPDF_NumberTree() = default;

RetainPtr<const CPDF_Object> CPDF_NumberTree::LookupValue(int num) const {
  return FindNumberNode(root_.Get(), num);
}

std::optional<CPDF_NumberTree::KeyValue> CPDF_NumberTree::GetLowerBound(
    int num) const {
  return FindLowerBound(root_.Get(), num);
}

CPDF_NumberTree::KeyValue::KeyValue(int key, RetainPtr<const CPDF_Object> value)
    : key(key), value(std::move(value)) {}

CPDF_NumberTree::KeyValue::KeyValue(CPDF_NumberTree::KeyValue&&) noexcept =
    default;

CPDF_NumberTree::KeyValue& CPDF_NumberTree::KeyValue::operator=(
    CPDF_NumberTree::KeyValue&&) noexcept = default;

CPDF_NumberTree::KeyValue::~KeyValue() = default;
