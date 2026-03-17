// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_CHECK_OP_H_
#define CORE_FXCRT_CHECK_OP_H_

#include "core/fxcrt/check.h"

#define CHECK_EQ(x, y) CHECK((x) == (y))
#define CHECK_NE(x, y) CHECK((x) != (y))
#define CHECK_LT(x, y) CHECK((x) < (y))
#define CHECK_GT(x, y) CHECK((x) > (y))
#define CHECK_LE(x, y) CHECK((x) <= (y))
#define CHECK_GE(x, y) CHECK((x) >= (y))

#define DCHECK_EQ(x, y) DCHECK((x) == (y))
#define DCHECK_NE(x, y) DCHECK((x) != (y))
#define DCHECK_LT(x, y) DCHECK((x) < (y))
#define DCHECK_GT(x, y) DCHECK((x) > (y))
#define DCHECK_LE(x, y) DCHECK((x) <= (y))
#define DCHECK_GE(x, y) DCHECK((x) >= (y))

#endif  // CORE_FXCRT_CHECK_OP_H_
