// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_COLORSTATE_H_
#define CORE_FPDFAPI_PAGE_CPDF_COLORSTATE_H_

#include <optional>
#include <vector>

#include "core/fpdfapi/page/cpdf_color.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/shared_copy_on_write.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class CPDF_ColorSpace;
class CPDF_Pattern;

class CPDF_ColorState {
 public:
  CPDF_ColorState();
  CPDF_ColorState(const CPDF_ColorState& that);
  ~CPDF_ColorState();

  void Emplace();
  void SetDefault();

  FX_COLORREF GetFillColorRef() const;
  void SetFillColorRef(FX_COLORREF colorref);

  FX_COLORREF GetStrokeColorRef() const;
  void SetStrokeColorRef(FX_COLORREF colorref);

  const CPDF_Color* GetFillColor() const;
  CPDF_Color* GetMutableFillColor();
  bool HasFillColor() const;

  const CPDF_Color* GetStrokeColor() const;
  CPDF_Color* GetMutableStrokeColor();
  bool HasStrokeColor() const;

  void SetFillColor(RetainPtr<CPDF_ColorSpace> colorspace,
                    std::vector<float> values);
  void SetStrokeColor(RetainPtr<CPDF_ColorSpace> colorspace,
                      std::vector<float> values);
  void SetFillPattern(RetainPtr<CPDF_Pattern> pattern,
                      pdfium::span<float> values);
  void SetStrokePattern(RetainPtr<CPDF_Pattern> pattern,
                        pdfium::span<float> values);

  bool HasRef() const { return !!m_Ref; }

 private:
  class ColorData final : public Retainable {
   public:
    CONSTRUCT_VIA_MAKE_RETAIN;

    RetainPtr<ColorData> Clone() const;

    void SetDefault();

    FX_COLORREF m_FillColorRef = 0;
    FX_COLORREF m_StrokeColorRef = 0;
    CPDF_Color m_FillColor;
    CPDF_Color m_StrokeColor;

   private:
    ColorData();
    ColorData(const ColorData& src);
    ~ColorData() override;
  };

  std::optional<FX_COLORREF> SetColor(RetainPtr<CPDF_ColorSpace> colorspace,
                                      std::vector<float> values,
                                      CPDF_Color& color);
  FX_COLORREF SetPattern(RetainPtr<CPDF_Pattern> pattern,
                         pdfium::span<float> values,
                         CPDF_Color& color);

  SharedCopyOnWrite<ColorData> m_Ref;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_COLORSTATE_H_
