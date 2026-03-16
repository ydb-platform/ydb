// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CONSTANTS_FORM_FLAGS_H_
#define CONSTANTS_FORM_FLAGS_H_

namespace pdfium {
namespace form_flags {

// PDF 1.7 spec, table 8.70.
// Field flags common to all field types.
constexpr uint32_t kReadOnly = 1 << 0;
constexpr uint32_t kRequired = 1 << 1;
constexpr uint32_t kNoExport = 1 << 2;

// PDF 1.7 spec, table 8.75.
// Field flags specific to button fields.
constexpr uint32_t kButtonNoToggleToOff = 1 << 14;
constexpr uint32_t kButtonRadio = 1 << 15;
constexpr uint32_t kButtonPushbutton = 1 << 16;
constexpr uint32_t kButtonRadiosInUnison = 1 << 25;

// PDF 1.7 spec, table 8.77.
// Field flags specific to text fields.
constexpr uint32_t kTextMultiline = 1 << 12;
constexpr uint32_t kTextPassword = 1 << 13;
constexpr uint32_t kTextFileSelect = 1 << 20;
constexpr uint32_t kTextDoNotSpellCheck = 1 << 22;
constexpr uint32_t kTextDoNotScroll = 1 << 23;
constexpr uint32_t kTextComb = 1 << 24;
constexpr uint32_t kTextRichText = 1 << 25;

// PDF 1.7 spec, table 8.79.
// Field flags specific to choice fields.
constexpr uint32_t kChoiceCombo = 1 << 17;
constexpr uint32_t kChoiceEdit = 1 << 18;
constexpr uint32_t kChoiceSort = 1 << 19;
constexpr uint32_t kChoiceMultiSelect = 1 << 21;
constexpr uint32_t kChoiceDoNotSpellCheck = 1 << 22;
constexpr uint32_t kChoiceCommitOnSelChange = 1 << 26;

}  // namespace form_flags
}  // namespace pdfium

#endif  // CONSTANTS_FORM_FLAGS_H_
