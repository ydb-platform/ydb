// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_FX_STRING_WRAPPERS_H_
#define CORE_FXCRT_FX_STRING_WRAPPERS_H_

#include <iosfwd>
#include <string>

#include "core/fxcrt/fx_memory_wrappers.h"

namespace fxcrt {

// String that uses partition alloc for backing store.
using string =
    std::basic_string<char, std::char_traits<char>, FxStringAllocator<char>>;

// String stream that uses PartitionAlloc for backing store.
using ostringstream = std::
    basic_ostringstream<char, std::char_traits<char>, FxStringAllocator<char>>;

}  // namespace fxcrt

#endif  // CORE_FXCRT_FX_STRING_WRAPPERS_H_
