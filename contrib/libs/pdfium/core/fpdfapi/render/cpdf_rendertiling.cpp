// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/render/cpdf_rendertiling.h"

#include <limits>
#include <memory>
#include <utility>

#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_pageimagecache.h"
#include "core/fpdfapi/page/cpdf_tilingpattern.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/render/cpdf_rendercontext.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fpdfapi/render/cpdf_renderstatus.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/span_util.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/dib/cfx_dibitmap.h"

namespace {

RetainPtr<CFX_DIBitmap> DrawPatternBitmap(
    CPDF_Document* pDoc,
    CPDF_PageImageCache* pCache,
    CPDF_TilingPattern* pPattern,
    CPDF_Form* pPatternForm,
    const CFX_Matrix& mtObject2Device,
    int width,
    int height,
    const CPDF_RenderOptions::Options& draw_options) {
  auto pBitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  // TODO(crbug.com/42271020): Consider adding support for
  // `FXDIB_Format::kBgraPremul`
  if (!pBitmap->Create(width, height,
                       pPattern->colored() ? FXDIB_Format::kBgra
                                           : FXDIB_Format::k8bppMask)) {
    return nullptr;
  }
  CFX_DefaultRenderDevice bitmap_device;
  bitmap_device.AttachWithBackdropAndGroupKnockout(
      pBitmap, /*pBackdropBitmap=*/nullptr, /*bGroupKnockout=*/true);
  CFX_FloatRect cell_bbox =
      pPattern->pattern_to_form().TransformRect(pPattern->bbox());
  cell_bbox = mtObject2Device.TransformRect(cell_bbox);
  CFX_FloatRect bitmap_rect(0.0f, 0.0f, width, height);
  CFX_Matrix mtAdjust;
  mtAdjust.MatchRect(bitmap_rect, cell_bbox);

  CFX_Matrix mtPattern2Bitmap = mtObject2Device * mtAdjust;
  CPDF_RenderOptions options;
  if (!pPattern->colored())
    options.SetColorMode(CPDF_RenderOptions::kAlpha);

  options.GetOptions() = draw_options;
  options.GetOptions().bForceHalftone = true;

  CPDF_RenderContext context(pDoc, nullptr, pCache);
  context.AppendLayer(pPatternForm, mtPattern2Bitmap);
  context.Render(&bitmap_device, nullptr, &options, nullptr);

  return pBitmap;
}

}  // namespace

// static
RetainPtr<CFX_DIBitmap> CPDF_RenderTiling::Draw(
    CPDF_RenderStatus* pRenderStatus,
    CPDF_PageObject* pPageObj,
    CPDF_TilingPattern* pPattern,
    CPDF_Form* pPatternForm,
    const CFX_Matrix& mtObj2Device,
    const FX_RECT& clip_box,
    bool bStroke) {
  const CFX_Matrix mtPattern2Device =
      pPattern->pattern_to_form() * mtObj2Device;

  CFX_FloatRect cell_bbox = mtPattern2Device.TransformRect(pPattern->bbox());

  float ceil_height = std::ceil(cell_bbox.Height());
  float ceil_width = std::ceil(cell_bbox.Width());

  // Validate the float will fit into the int when the conversion is done.
  if (!pdfium::IsValueInRangeForNumericType<int>(ceil_height) ||
      !pdfium::IsValueInRangeForNumericType<int>(ceil_width)) {
    return nullptr;
  }

  int width = static_cast<int>(ceil_width);
  int height = static_cast<int>(ceil_height);
  if (width <= 0)
    width = 1;
  if (height <= 0)
    height = 1;

  CFX_FloatRect clip_box_p =
      mtPattern2Device.GetInverse().TransformRect(CFX_FloatRect(clip_box));
  int min_col = static_cast<int>(
      ceil((clip_box_p.left - pPattern->bbox().right) / pPattern->x_step()));
  int max_col = static_cast<int>(
      floor((clip_box_p.right - pPattern->bbox().left) / pPattern->x_step()));
  int min_row = static_cast<int>(
      ceil((clip_box_p.bottom - pPattern->bbox().top) / pPattern->y_step()));
  int max_row = static_cast<int>(
      floor((clip_box_p.top - pPattern->bbox().bottom) / pPattern->y_step()));

  // Make sure we can fit the needed width * height into an int.
  if (height > std::numeric_limits<int>::max() / width)
    return nullptr;

  CFX_RenderDevice* pDevice = pRenderStatus->GetRenderDevice();
  CPDF_RenderContext* pContext = pRenderStatus->GetContext();
  const CPDF_RenderOptions& options = pRenderStatus->GetRenderOptions();
  if (width > clip_box.Width() || height > clip_box.Height() ||
      width * height > clip_box.Width() * clip_box.Height()) {
    std::unique_ptr<CPDF_GraphicStates> pStates;
    if (!pPattern->colored()) {
      pStates = CPDF_RenderStatus::CloneObjStates(&pPageObj->graphic_states(),
                                                  bStroke);
    } else if (pPageObj->AsPath()) {
      pStates = std::make_unique<CPDF_GraphicStates>();
      pStates->SetDefaultStates();
      pStates->mutable_general_state().SetFillAlpha(pPageObj->general_state().GetFillAlpha());
    }

    RetainPtr<const CPDF_Dictionary> pFormResource =
        pPatternForm->GetDict()->GetDictFor("Resources");
    for (int col = min_col; col <= max_col; col++) {
      for (int row = min_row; row <= max_row; row++) {
        CFX_PointF original = mtPattern2Device.Transform(
            CFX_PointF(col * pPattern->x_step(), row * pPattern->y_step()));
        CFX_Matrix matrix = mtObj2Device;
        matrix.Translate(original.x - mtPattern2Device.e,
                         original.y - mtPattern2Device.f);
        CFX_RenderDevice::StateRestorer restorer2(pDevice);
        CPDF_RenderStatus status(pContext, pDevice);
        status.SetOptions(options);
        status.SetTransparency(pPatternForm->GetTransparency());
        status.SetFormResource(pFormResource);
        status.SetDropObjects(pRenderStatus->GetDropObjects());
        status.Initialize(pRenderStatus, pStates.get());
        status.RenderObjectList(pPatternForm, matrix);
      }
    }
    return nullptr;
  }

  bool bAligned =
      pPattern->bbox().left == 0 && pPattern->bbox().bottom == 0 &&
      pPattern->bbox().right == pPattern->x_step() &&
      pPattern->bbox().top == pPattern->y_step() &&
      (mtPattern2Device.IsScaled() || mtPattern2Device.Is90Rotated());
  if (bAligned) {
    int orig_x = FXSYS_roundf(mtPattern2Device.e);
    int orig_y = FXSYS_roundf(mtPattern2Device.f);
    min_col = (clip_box.left - orig_x) / width;
    if (clip_box.left < orig_x)
      min_col--;

    max_col = (clip_box.right - orig_x) / width;
    if (clip_box.right <= orig_x)
      max_col--;

    min_row = (clip_box.top - orig_y) / height;
    if (clip_box.top < orig_y)
      min_row--;

    max_row = (clip_box.bottom - orig_y) / height;
    if (clip_box.bottom <= orig_y)
      max_row--;
  }
  float left_offset = cell_bbox.left - mtPattern2Device.e;
  float top_offset = cell_bbox.bottom - mtPattern2Device.f;
  RetainPtr<CFX_DIBitmap> pPatternBitmap;
  if (width * height < 16) {
    RetainPtr<CFX_DIBitmap> pEnlargedBitmap = DrawPatternBitmap(
        pContext->GetDocument(), pContext->GetPageCache(), pPattern,
        pPatternForm, mtObj2Device, 8, 8, options.GetOptions());
    pPatternBitmap = pEnlargedBitmap->StretchTo(
        width, height, FXDIB_ResampleOptions(), nullptr);
  } else {
    pPatternBitmap = DrawPatternBitmap(
        pContext->GetDocument(), pContext->GetPageCache(), pPattern,
        pPatternForm, mtObj2Device, width, height, options.GetOptions());
  }
  if (!pPatternBitmap)
    return nullptr;

  if (options.ColorModeIs(CPDF_RenderOptions::kGray))
    pPatternBitmap->ConvertColorScale(0, 0xffffff);

  FX_ARGB fill_argb = pRenderStatus->GetFillArgb(pPageObj);
  int clip_width = clip_box.right - clip_box.left;
  int clip_height = clip_box.bottom - clip_box.top;
  auto pScreen = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!pScreen->Create(clip_width, clip_height, FXDIB_Format::kBgra)) {
    return nullptr;
  }

  pdfium::span<const uint8_t> src_buf = pPatternBitmap->GetBuffer();
  for (int col = min_col; col <= max_col; col++) {
    for (int row = min_row; row <= max_row; row++) {
      int start_x;
      int start_y;
      if (bAligned) {
        start_x =
            FXSYS_roundf(mtPattern2Device.e) + col * width - clip_box.left;
        start_y =
            FXSYS_roundf(mtPattern2Device.f) + row * height - clip_box.top;
      } else {
        CFX_PointF original = mtPattern2Device.Transform(
            CFX_PointF(col * pPattern->x_step(), row * pPattern->y_step()));

        FX_SAFE_INT32 safeStartX = FXSYS_roundf(original.x + left_offset);
        FX_SAFE_INT32 safeStartY = FXSYS_roundf(original.y + top_offset);

        safeStartX -= clip_box.left;
        safeStartY -= clip_box.top;
        if (!safeStartX.IsValid() || !safeStartY.IsValid())
          return nullptr;

        start_x = safeStartX.ValueOrDie();
        start_y = safeStartY.ValueOrDie();
      }
      if (width == 1 && height == 1) {
        if (start_x < 0 || start_x >= clip_box.Width() || start_y < 0 ||
            start_y >= clip_box.Height()) {
          continue;
        }
        uint32_t* dest_buf = fxcrt::reinterpret_span<uint32_t>(
                                 pScreen->GetWritableScanline(start_y))
                                 .subspan(start_x)
                                 .data();
        if (pPattern->colored()) {
          const uint32_t* src_buf32 =
              fxcrt::reinterpret_span<const uint32_t>(src_buf).data();
          *dest_buf = *src_buf32;
        } else {
          *dest_buf = (*(src_buf.data()) << 24) | (fill_argb & 0xffffff);
        }
      } else {
        if (pPattern->colored()) {
          pScreen->CompositeBitmap(start_x, start_y, width, height,
                                   pPatternBitmap, 0, 0, BlendMode::kNormal,
                                   nullptr, false);
        } else {
          pScreen->CompositeMask(start_x, start_y, width, height,
                                 pPatternBitmap, fill_argb, 0, 0,
                                 BlendMode::kNormal, nullptr, false);
        }
      }
    }
  }
  return pScreen;
}
