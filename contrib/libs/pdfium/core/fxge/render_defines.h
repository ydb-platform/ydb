// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_RENDER_DEFINES_H_
#define CORE_FXGE_RENDER_DEFINES_H_

#define FXDC_PIXEL_WIDTH 1
#define FXDC_PIXEL_HEIGHT 2
#define FXDC_BITS_PIXEL 3
#define FXDC_HORZ_SIZE 4
#define FXDC_VERT_SIZE 5
#define FXDC_RENDER_CAPS 6

#define FXRC_GET_BITS 0x01
#define FXRC_ALPHA_PATH 0x02
#define FXRC_ALPHA_IMAGE 0x04
#define FXRC_ALPHA_OUTPUT 0x08
#define FXRC_BLEND_MODE 0x10
#define FXRC_SOFT_CLIP 0x20
#define FXRC_BYTEMASK_OUTPUT 0x40
// Assuming these are Skia-only for now. If this assumption changes, update both
// the #if logic here, as well as the callsites that check these capabilities.
#if defined(PDF_USE_SKIA)
#define FXRC_FILLSTROKE_PATH 0x80
#define FXRC_SHADING 0x100
#define FXRC_PREMULTIPLIED_ALPHA 0x200
#endif

#endif  // CORE_FXGE_RENDER_DEFINES_H_
