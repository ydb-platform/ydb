// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/apple/fx_quartz_device.h"

#include <CoreGraphics/CoreGraphics.h>

#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_memory_wrappers.h"
#include "core/fxcrt/zip.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"
#include "core/fxge/dib/cfx_dibitmap.h"

#ifndef CGFLOAT_IS_DOUBLE
#error Expected CGFLOAT_IS_DOUBLE to be defined by CoreGraphics headers
#endif

FX_DATA_PARTITION_EXCEPTION(CGPoint);

void* CQuartz2D::CreateGraphics(const RetainPtr<CFX_DIBitmap>& pBitmap) {
  if (!pBitmap)
    return nullptr;
  CGBitmapInfo bmpInfo = kCGBitmapByteOrder32Little;
  switch (pBitmap->GetFormat()) {
    case FXDIB_Format::kBgrx:
      bmpInfo |= kCGImageAlphaNoneSkipFirst;
      break;
    case FXDIB_Format::kBgra:
    default:
      return nullptr;
  }
  CGColorSpaceRef colorSpace = CGColorSpaceCreateDeviceRGB();
  CGContextRef context = CGBitmapContextCreate(
      pBitmap->GetWritableBuffer().data(), pBitmap->GetWidth(),
      pBitmap->GetHeight(), 8, pBitmap->GetPitch(), colorSpace, bmpInfo);
  CGColorSpaceRelease(colorSpace);
  return context;
}

void CQuartz2D::DestroyGraphics(void* graphics) {
  if (graphics)
    CGContextRelease((CGContextRef)graphics);
}

void* CQuartz2D::CreateFont(pdfium::span<const uint8_t> font_data) {
  CGDataProviderRef data_provider = CGDataProviderCreateWithData(
      nullptr, font_data.data(), font_data.size(), nullptr);
  if (!data_provider) {
    return nullptr;
  }

  CGFontRef cg_font = CGFontCreateWithDataProvider(data_provider);
  CGDataProviderRelease(data_provider);
  return cg_font;
}

void CQuartz2D::DestroyFont(void* font) {
  CGFontRelease((CGFontRef)font);
}

void CQuartz2D::SetGraphicsTextMatrix(void* graphics,
                                      const CFX_Matrix& matrix) {
  if (!graphics)
    return;
  CGContextRef context = reinterpret_cast<CGContextRef>(graphics);
  CGFloat ty = CGBitmapContextGetHeight(context) - matrix.f;
  CGContextSetTextMatrix(
      context, CGAffineTransformMake(matrix.a, matrix.b, matrix.c, matrix.d,
                                     matrix.e, ty));
}

bool CQuartz2D::DrawGraphicsString(void* graphics,
                                   void* font,
                                   float font_size,
                                   pdfium::span<uint16_t> glyph_indices,
                                   pdfium::span<CGPoint> glyph_positions,
                                   FX_ARGB argb) {
  if (!graphics)
    return false;

  CGContextRef context = (CGContextRef)graphics;
  CGContextSetFont(context, (CGFontRef)font);
  CGContextSetFontSize(context, font_size);

  const FX_BGRA_STRUCT<uint8_t> bgra = ArgbToBGRAStruct(argb);
  CGContextSetRGBFillColor(context, bgra.red / 255.f, bgra.green / 255.f,
                           bgra.blue / 255.f, bgra.alpha / 255.f);
  CGContextSaveGState(context);
#if CGFLOAT_IS_DOUBLE
  auto glyph_positions_cg =
      FixedSizeDataVector<CGPoint>::Uninit(glyph_positions.size());
  for (auto [input, output] :
       fxcrt::Zip(glyph_positions, glyph_positions_cg.span())) {
    output.x = input.x;
    output.y = input.y;
  }
  const CGPoint* glyph_positions_cg_ptr = glyph_positions_cg.span().data();
#else
  const CGPoint* glyph_positions_cg_ptr = glyph_positions.data();
#endif
  CGContextShowGlyphsAtPositions(
      context, reinterpret_cast<CGGlyph*>(glyph_indices.data()),
      glyph_positions_cg_ptr, glyph_positions.size());
  CGContextRestoreGState(context);
  return true;
}
