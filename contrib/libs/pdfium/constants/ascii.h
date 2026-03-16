// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CONSTANTS_ASCII_H_
#define CONSTANTS_ASCII_H_

#include <stdint.h>

namespace pdfium {
namespace ascii {

constexpr uint8_t kNul = 0x00;
constexpr uint8_t kControlA = 0x01;
constexpr uint8_t kControlB = 0x02;
constexpr uint8_t kControlC = 0x03;
constexpr uint8_t kBackspace = 0x08;
constexpr uint8_t kTab = 0x09;
constexpr uint8_t kNewline = 0x0a;
constexpr uint8_t kReturn = 0x0d;
constexpr uint8_t kControlV = 0x16;
constexpr uint8_t kControlX = 0x18;
constexpr uint8_t kControlZ = 0x1a;
constexpr uint8_t kEscape = 0x1b;
constexpr uint8_t kSpace = 0x20;

}  // namespace ascii
}  // namespace pdfium

#endif  // CONSTANTS_ASCII_H_
