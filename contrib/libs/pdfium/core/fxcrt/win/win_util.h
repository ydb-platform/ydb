// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_WIN_WIN_UTIL_H_
#define CORE_FXCRT_WIN_WIN_UTIL_H_

namespace pdfium {

// Returns true if the current process can make USER32 or GDI32 calls such as
// CreateWindow and CreateDC. Windows 8 and above allow the kernel component
// of these calls to be disabled which can cause undefined behaviour such as
// crashes. This function can be used to guard areas of code using these calls
// and provide a fallback path if necessary.
bool IsUser32AndGdi32Available();

}  // namespace pdfium

#endif  // CORE_FXCRT_WIN_WIN_UTIL_H_
