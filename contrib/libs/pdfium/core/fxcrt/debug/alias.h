// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_DEBUG_ALIAS_H_
#define CORE_FXCRT_DEBUG_ALIAS_H_

namespace pdfium {

// Make the optimizer think that var is aliased. This is to prevent it from
// optimizing out local variables that would not otherwise be live at the point
// of a potential crash.
// pdfium::Alias should only be used for local variables, not globals,
// object members, or function return values - these must be copied to locals if
// you want to ensure they are recorded in crash dumps.
// Note that if the local variable is a pointer then its value will be retained
// but the memory that it points to will probably not be saved in the crash
// dump - by default only stack memory is saved. Therefore the aliasing
// technique is usually only worthwhile with non-pointer variables. If you have
// a pointer to an object and you want to retain the object's state you need to
// copy the object or its fields to local variables. Example usage:
//   int last_error = err_;
//   pdfium::Alias(&last_error);
//   DEBUG_ALIAS_FOR_CSTR(name_copy, p->name, 16);
//   CHECK(false);
void Alias(const void* var);

}  // namespace pdfium

#endif  // CORE_FXCRT_DEBUG_ALIAS_H_
