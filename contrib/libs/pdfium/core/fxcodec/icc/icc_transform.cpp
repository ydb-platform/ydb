// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/icc/icc_transform.h"

#include <stdint.h>

#include <algorithm>
#include <memory>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/ptr_util.h"

namespace fxcodec {

namespace {

// For use with std::unique_ptr<cmsHPROFILE>.
struct CmsProfileDeleter {
  inline void operator()(cmsHPROFILE p) { cmsCloseProfile(p); }
};

using ScopedCmsProfile = std::unique_ptr<void, CmsProfileDeleter>;

bool Check3Components(cmsColorSpaceSignature cs) {
  switch (cs) {
    case cmsSigGrayData:
    case cmsSigCmykData:
      return false;
    default:
      return true;
  }
}

}  // namespace

IccTransform::IccTransform(cmsHTRANSFORM hTransform,
                           int srcComponents,
                           bool bIsLab,
                           bool bNormal)
    : m_hTransform(hTransform),
      m_nSrcComponents(srcComponents),
      m_bLab(bIsLab),
      m_bNormal(bNormal) {}

IccTransform::~IccTransform() {
  cmsDeleteTransform(m_hTransform);
}

// static
std::unique_ptr<IccTransform> IccTransform::CreateTransformSRGB(
    pdfium::span<const uint8_t> span) {
  ScopedCmsProfile srcProfile(cmsOpenProfileFromMem(
      span.data(), pdfium::checked_cast<cmsUInt32Number>(span.size())));
  if (!srcProfile)
    return nullptr;

  ScopedCmsProfile dstProfile(cmsCreate_sRGBProfile());
  if (!dstProfile)
    return nullptr;

  cmsColorSpaceSignature srcCS = cmsGetColorSpace(srcProfile.get());
  uint32_t nSrcComponents = cmsChannelsOf(srcCS);

  if (!IsValidIccComponents(nSrcComponents)) {
    return nullptr;
  }

  int srcFormat;
  bool bLab = false;
  bool bNormal = false;
  if (srcCS == cmsSigLabData) {
    srcFormat =
        COLORSPACE_SH(PT_Lab) | CHANNELS_SH(nSrcComponents) | BYTES_SH(0);
    bLab = true;
  } else {
    srcFormat =
        COLORSPACE_SH(PT_ANY) | CHANNELS_SH(nSrcComponents) | BYTES_SH(1);
    // TODO(thestig): Check to see if lcms2 supports more colorspaces that can
    // be considered normal.
    bNormal = srcCS == cmsSigGrayData || srcCS == cmsSigRgbData ||
              srcCS == cmsSigCmykData;
  }
  cmsColorSpaceSignature dstCS = cmsGetColorSpace(dstProfile.get());
  if (!Check3Components(dstCS))
    return nullptr;

  cmsHTRANSFORM hTransform = nullptr;
  switch (dstCS) {
    case cmsSigRgbData:
      hTransform =
          cmsCreateTransform(srcProfile.get(), srcFormat, dstProfile.get(),
                             TYPE_BGR_8, INTENT_PERCEPTUAL, /*dwFlags=*/0);
      break;
    case cmsSigGrayData:
    case cmsSigCmykData:
      // Check3Components() already filtered these types.
      NOTREACHED_NORETURN();
    default:
      break;
  }
  if (!hTransform)
    return nullptr;

  // Private ctor.
  return pdfium::WrapUnique(
      new IccTransform(hTransform, nSrcComponents, bLab, bNormal));
}

void IccTransform::Translate(pdfium::span<const float> pSrcValues,
                             pdfium::span<float> pDestValues) {
  uint8_t output[4];
  // TODO(npm): Currently the CmsDoTransform method is part of LCMS and it will
  // apply some member of m_hTransform to the input. We need to go over all the
  // places which set transform to verify that only `pSrcValues.size()`
  // components are used.
  if (m_bLab) {
    DataVector<double> inputs(std::max<size_t>(pSrcValues.size(), 16));
    for (uint32_t i = 0; i < pSrcValues.size(); ++i)
      inputs[i] = pSrcValues[i];
    cmsDoTransform(m_hTransform, inputs.data(), output, 1);
  } else {
    DataVector<uint8_t> inputs(std::max<size_t>(pSrcValues.size(), 16));
    for (size_t i = 0; i < pSrcValues.size(); ++i) {
      inputs[i] = static_cast<int>(std::clamp(pSrcValues[i] * 255.0f, 0.0f, 255.0f));
    }
    cmsDoTransform(m_hTransform, inputs.data(), output, 1);
  }
  pDestValues[0] = output[2] / 255.0f;
  pDestValues[1] = output[1] / 255.0f;
  pDestValues[2] = output[0] / 255.0f;
}

void IccTransform::TranslateScanline(pdfium::span<uint8_t> pDest,
                                     pdfium::span<const uint8_t> pSrc,
                                     int32_t pixels) {
  cmsDoTransform(m_hTransform, pSrc.data(), pDest.data(), pixels);
}

// static
bool IccTransform::IsValidIccComponents(int components) {
  // According to PDF spec, number of components must be 1, 3, or 4.
  return components == 1 || components == 3 || components == 4;
}

}  // namespace fxcodec
