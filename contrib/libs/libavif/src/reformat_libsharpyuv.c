// Copyright 2022 Google LLC
// SPDX-License-Identifier: BSD-2-Clause

#include "avif/internal.h"

#if defined(AVIF_LIBSHARPYUV_ENABLED)
#include <limits.h>
#include <sharpyuv/sharpyuv.h>
#include <sharpyuv/sharpyuv_csp.h>

avifResult avifImageRGBToYUVLibSharpYUV(avifImage * image, const avifRGBImage * rgb, const avifReformatState * state)
{
    // The width, height, and stride parameters of SharpYuvConvertWithOptions()
    // and SharpYuvConvert() are all of the int type.
    if (rgb->width > INT_MAX || rgb->height > INT_MAX || rgb->rowBytes > INT_MAX || image->yuvRowBytes[AVIF_CHAN_Y] > INT_MAX) {
        return AVIF_RESULT_NOT_IMPLEMENTED;
    }
    const SharpYuvColorSpace colorSpace = {
        state->yuv.kr, state->yuv.kb, image->depth, (state->yuv.range == AVIF_RANGE_LIMITED) ? kSharpYuvRangeLimited : kSharpYuvRangeFull
    };

    SharpYuvConversionMatrix matrix;
    // Fills in 'matrix' for the given YUVColorSpace.
    SharpYuvComputeConversionMatrix(&colorSpace, &matrix);
#if SHARPYUV_VERSION >= SHARPYUV_MAKE_VERSION(0, 4, 0)
    SharpYuvOptions options;
    SharpYuvOptionsInit(&matrix, &options);
    if (image->transferCharacteristics == AVIF_TRANSFER_CHARACTERISTICS_UNSPECIFIED) {
        // Set to sRGB for backward compatibility.
        options.transfer_type = kSharpYuvTransferFunctionSrgb;
    } else {
        options.transfer_type = (SharpYuvTransferFunctionType)image->transferCharacteristics;
    }
    const int sharpyuvRes = SharpYuvConvertWithOptions(&rgb->pixels[state->rgb.offsetBytesR],
                                                       &rgb->pixels[state->rgb.offsetBytesG],
                                                       &rgb->pixels[state->rgb.offsetBytesB],
                                                       state->rgb.pixelBytes,
                                                       rgb->rowBytes,
                                                       rgb->depth,
                                                       image->yuvPlanes[AVIF_CHAN_Y],
                                                       image->yuvRowBytes[AVIF_CHAN_Y],
                                                       image->yuvPlanes[AVIF_CHAN_U],
                                                       image->yuvRowBytes[AVIF_CHAN_U],
                                                       image->yuvPlanes[AVIF_CHAN_V],
                                                       image->yuvRowBytes[AVIF_CHAN_V],
                                                       image->depth,
                                                       rgb->width,
                                                       rgb->height,
                                                       &options);
#else
    const int sharpyuvRes = SharpYuvConvert(&rgb->pixels[state->rgb.offsetBytesR],
                                            &rgb->pixels[state->rgb.offsetBytesG],
                                            &rgb->pixels[state->rgb.offsetBytesB],
                                            state->rgb.pixelBytes,
                                            rgb->rowBytes,
                                            rgb->depth,
                                            image->yuvPlanes[AVIF_CHAN_Y],
                                            image->yuvRowBytes[AVIF_CHAN_Y],
                                            image->yuvPlanes[AVIF_CHAN_U],
                                            image->yuvRowBytes[AVIF_CHAN_U],
                                            image->yuvPlanes[AVIF_CHAN_V],
                                            image->yuvRowBytes[AVIF_CHAN_V],
                                            image->depth,
                                            rgb->width,
                                            rgb->height,
                                            &matrix);
#endif // SHARPYUV_VERSION >= SHARPYUV_MAKE_VERSION(0, 4, 0)
    if (!sharpyuvRes) {
        return AVIF_RESULT_REFORMAT_FAILED;
    }

    return AVIF_RESULT_OK;
}

#else

avifResult avifImageRGBToYUVLibSharpYUV(avifImage * image, const avifRGBImage * rgb, const avifReformatState * state)
{
    (void)image;
    (void)rgb;
    (void)state;
    return AVIF_RESULT_NOT_IMPLEMENTED;
}
#endif // defined(AVIF_LIBSHARPYUV_ENABLED)
