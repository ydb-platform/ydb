// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssselector.h"

#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_extension.h"

namespace {

size_t GetCSSNameLen(WideStringView str) {
  for (size_t i = 0; i < str.GetLength(); ++i) {
    wchar_t wch = str[i];
    if (!isascii(wch) || (!isalnum(wch) && wch != '_' && wch != '-'))
      return i;
  }
  return str.GetLength();
}

}  // namespace

CFX_CSSSelector::CFX_CSSSelector(WideStringView str,
                                 std::unique_ptr<CFX_CSSSelector> next)
    : name_hash_(FX_HashCode_GetLoweredW(str)), next_(std::move(next)) {}

CFX_CSSSelector::~CFX_CSSSelector() = default;

// static.
std::unique_ptr<CFX_CSSSelector> CFX_CSSSelector::FromString(
    WideStringView str) {
  DCHECK(!str.IsEmpty());

  for (wchar_t wch : str) {
    switch (wch) {
      case '>':
      case '[':
      case '+':
        return nullptr;
    }
  }

  std::unique_ptr<CFX_CSSSelector> head;
  for (size_t i = 0; i < str.GetLength();) {
    wchar_t wch = str[i];
    if (wch == ' ') {
      ++i;
      continue;
    }

    const bool is_star = wch == '*';
    const bool is_valid_char = is_star || (isascii(wch) && isalpha(wch));
    if (!is_valid_char)
      return nullptr;

    if (head)
      head->set_is_descendant();
    size_t len = is_star ? 1 : GetCSSNameLen(str.Last(str.GetLength() - i));
    auto new_head =
        std::make_unique<CFX_CSSSelector>(str.Substr(i, len), std::move(head));
    head = std::move(new_head);
    i += len;
  }
  return head;
}
