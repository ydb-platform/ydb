// Copyright 2023 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_DIB_BLEND_H_
#define CORE_FXGE_DIB_BLEND_H_

enum class BlendMode;

namespace fxge {

// Note that Blend() only handles separable blend modes.
int Blend(BlendMode blend_mode, int back_color, int src_color);

}  // namespace fxge

#endif  // CORE_FXGE_DIB_BLEND_H_
