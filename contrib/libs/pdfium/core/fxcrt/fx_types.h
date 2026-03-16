// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_TYPES_H_
#define CORE_FXCRT_FX_TYPES_H_

#include "build/build_config.h"

// PDFium file sizes match the platform. The value must be signed to support -1
// error returns.
#if BUILDFLAG(IS_WIN)
#include <stdint.h>
#define FX_FILESIZE int64_t
#else
#include <sys/types.h>  // For off_t.
#define FX_FILESIZE off_t
#endif  // BUILDFLAG(IS_WIN)

#endif  // CORE_FXCRT_FX_TYPES_H_
