// Copyright 2020 Joe Drago. All rights reserved.
// SPDX-License-Identifier: BSD-2-Clause

#include "avif/internal.h"

#include <assert.h>
#include <string.h>

void avifFillAlpha(const avifAlphaParams * params)
{
    if (params->dstDepth > 8) {
        const uint16_t maxChannel = (uint16_t)((1 << params->dstDepth) - 1);
        uint8_t * dstRow = &params->dstPlane[params->dstOffsetBytes];
        for (uint32_t j = 0; j < params->height; ++j) {
            uint8_t * dstPixel = dstRow;
            for (uint32_t i = 0; i < params->width; ++i) {
                *((uint16_t *)dstPixel) = maxChannel;
                dstPixel += params->dstPixelBytes;
            }
            dstRow += params->dstRowBytes;
        }
    } else {
        // In this case, (1 << params->dstDepth) - 1 is always equal to 255.
        const uint8_t maxChannel = 255;
        uint8_t * dstRow = &params->dstPlane[params->dstOffsetBytes];
        for (uint32_t j = 0; j < params->height; ++j) {
            uint8_t * dstPixel = dstRow;
            for (uint32_t i = 0; i < params->width; ++i) {
                *dstPixel = maxChannel;
                dstPixel += params->dstPixelBytes;
            }
            dstRow += params->dstRowBytes;
        }
    }
}

void avifReformatAlpha(const avifAlphaParams * params)
{
    const int srcMaxChannel = (1 << params->srcDepth) - 1;
    const int dstMaxChannel = (1 << params->dstDepth) - 1;
    const float srcMaxChannelF = (float)srcMaxChannel;
    const float dstMaxChannelF = (float)dstMaxChannel;

    if (params->srcDepth == params->dstDepth) {
        // no depth rescale

        if (params->srcDepth > 8) {
            // no depth rescale, uint16_t -> uint16_t

            const uint8_t * srcRow = &params->srcPlane[params->srcOffsetBytes];
            uint8_t * dstRow = &params->dstPlane[params->dstOffsetBytes];
            for (uint32_t j = 0; j < params->height; ++j) {
                const uint8_t * srcPixel = srcRow;
                uint8_t * dstPixel = dstRow;
                for (uint32_t i = 0; i < params->width; ++i) {
                    *((uint16_t *)dstPixel) = *((const uint16_t *)srcPixel);
                    srcPixel += params->srcPixelBytes;
                    dstPixel += params->dstPixelBytes;
                }
                srcRow += params->srcRowBytes;
                dstRow += params->dstRowBytes;
            }
        } else {
            // no depth rescale, uint8_t -> uint8_t

            const uint8_t * srcRow = &params->srcPlane[params->srcOffsetBytes];
            uint8_t * dstRow = &params->dstPlane[params->dstOffsetBytes];
            for (uint32_t j = 0; j < params->height; ++j) {
                const uint8_t * srcPixel = srcRow;
                uint8_t * dstPixel = dstRow;
                for (uint32_t i = 0; i < params->width; ++i) {
                    *dstPixel = *srcPixel;
                    srcPixel += params->srcPixelBytes;
                    dstPixel += params->dstPixelBytes;
                }
                srcRow += params->srcRowBytes;
                dstRow += params->dstRowBytes;
            }
        }
    } else {
        // depth rescale

        if (params->srcDepth > 8) {
            if (params->dstDepth > 8) {
                // depth rescale, uint16_t -> uint16_t

                const uint8_t * srcRow = &params->srcPlane[params->srcOffsetBytes];
                uint8_t * dstRow = &params->dstPlane[params->dstOffsetBytes];
                for (uint32_t j = 0; j < params->height; ++j) {
                    const uint8_t * srcPixel = srcRow;
                    uint8_t * dstPixel = dstRow;
                    for (uint32_t i = 0; i < params->width; ++i) {
                        int srcAlpha = *((const uint16_t *)srcPixel);
                        float alphaF = (float)srcAlpha / srcMaxChannelF;
                        int dstAlpha = (int)(0.5f + (alphaF * dstMaxChannelF));
                        dstAlpha = AVIF_CLAMP(dstAlpha, 0, dstMaxChannel);
                        *((uint16_t *)dstPixel) = (uint16_t)dstAlpha;
                        srcPixel += params->srcPixelBytes;
                        dstPixel += params->dstPixelBytes;
                    }
                    srcRow += params->srcRowBytes;
                    dstRow += params->dstRowBytes;
                }
            } else {
                // depth rescale, uint16_t -> uint8_t

                const uint8_t * srcRow = &params->srcPlane[params->srcOffsetBytes];
                uint8_t * dstRow = &params->dstPlane[params->dstOffsetBytes];
                for (uint32_t j = 0; j < params->height; ++j) {
                    const uint8_t * srcPixel = srcRow;
                    uint8_t * dstPixel = dstRow;
                    for (uint32_t i = 0; i < params->width; ++i) {
                        int srcAlpha = *((const uint16_t *)srcPixel);
                        float alphaF = (float)srcAlpha / srcMaxChannelF;
                        int dstAlpha = (int)(0.5f + (alphaF * dstMaxChannelF));
                        dstAlpha = AVIF_CLAMP(dstAlpha, 0, dstMaxChannel);
                        *dstPixel = (uint8_t)dstAlpha;
                        srcPixel += params->srcPixelBytes;
                        dstPixel += params->dstPixelBytes;
                    }
                    srcRow += params->srcRowBytes;
                    dstRow += params->dstRowBytes;
                }
            }
        } else {
            // If (srcDepth == 8), dstDepth must be >8 otherwise we'd be in the (params->srcDepth == params->dstDepth) block above.
            assert(params->dstDepth > 8);

            // depth rescale, uint8_t -> uint16_t
            const uint8_t * srcRow = &params->srcPlane[params->srcOffsetBytes];
            uint8_t * dstRow = &params->dstPlane[params->dstOffsetBytes];
            for (uint32_t j = 0; j < params->height; ++j) {
                const uint8_t * srcPixel = srcRow;
                uint8_t * dstPixel = dstRow;
                for (uint32_t i = 0; i < params->width; ++i) {
                    int srcAlpha = *srcPixel;
                    float alphaF = (float)srcAlpha / srcMaxChannelF;
                    int dstAlpha = (int)(0.5f + (alphaF * dstMaxChannelF));
                    dstAlpha = AVIF_CLAMP(dstAlpha, 0, dstMaxChannel);
                    *((uint16_t *)dstPixel) = (uint16_t)dstAlpha;
                    srcPixel += params->srcPixelBytes;
                    dstPixel += params->dstPixelBytes;
                }
                srcRow += params->srcRowBytes;
                dstRow += params->dstRowBytes;
            }
        }
    }
}

avifResult avifRGBImagePremultiplyAlpha(avifRGBImage * rgb)
{
    // no data
    if (!rgb->pixels || !rgb->rowBytes) {
        return AVIF_RESULT_REFORMAT_FAILED;
    }

    // no alpha.
    if (!avifRGBFormatHasAlpha(rgb->format)) {
        return AVIF_RESULT_INVALID_ARGUMENT;
    }

    avifResult libyuvResult = avifRGBImagePremultiplyAlphaLibYUV(rgb);
    if (libyuvResult != AVIF_RESULT_NOT_IMPLEMENTED) {
        return libyuvResult;
    }

    assert(rgb->depth >= 8 && rgb->depth <= 16);

    uint32_t max = (1 << rgb->depth) - 1;
    float maxF = (float)max;

    if (rgb->depth > 8) {
        if (rgb->format == AVIF_RGB_FORMAT_RGBA || rgb->format == AVIF_RGB_FORMAT_BGRA) {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint16_t * pixel = (uint16_t *)row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint16_t a = pixel[3];
                    if (a >= max) {
                        // opaque is no-op
                    } else if (a == 0) {
                        // result must be zero
                        pixel[0] = 0;
                        pixel[1] = 0;
                        pixel[2] = 0;
                    } else {
                        // a < maxF is always true now, so we don't need clamp here
                        pixel[0] = (uint16_t)avifRoundf((float)pixel[0] * (float)a / maxF);
                        pixel[1] = (uint16_t)avifRoundf((float)pixel[1] * (float)a / maxF);
                        pixel[2] = (uint16_t)avifRoundf((float)pixel[2] * (float)a / maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        } else {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint16_t * pixel = (uint16_t *)row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint16_t a = pixel[0];
                    if (a >= max) {
                    } else if (a == 0) {
                        pixel[1] = 0;
                        pixel[2] = 0;
                        pixel[3] = 0;
                    } else {
                        pixel[1] = (uint16_t)avifRoundf((float)pixel[1] * (float)a / maxF);
                        pixel[2] = (uint16_t)avifRoundf((float)pixel[2] * (float)a / maxF);
                        pixel[3] = (uint16_t)avifRoundf((float)pixel[3] * (float)a / maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        }
    } else {
        if (rgb->format == AVIF_RGB_FORMAT_RGBA || rgb->format == AVIF_RGB_FORMAT_BGRA) {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint8_t * pixel = row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint8_t a = pixel[3];
                    // uint8_t can't exceed 255
                    if (a == max) {
                    } else if (a == 0) {
                        pixel[0] = 0;
                        pixel[1] = 0;
                        pixel[2] = 0;
                    } else {
                        pixel[0] = (uint8_t)avifRoundf((float)pixel[0] * (float)a / maxF);
                        pixel[1] = (uint8_t)avifRoundf((float)pixel[1] * (float)a / maxF);
                        pixel[2] = (uint8_t)avifRoundf((float)pixel[2] * (float)a / maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        } else {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint8_t * pixel = row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint8_t a = pixel[0];
                    if (a == max) {
                    } else if (a == 0) {
                        pixel[1] = 0;
                        pixel[2] = 0;
                        pixel[3] = 0;
                    } else {
                        pixel[1] = (uint8_t)avifRoundf((float)pixel[1] * (float)a / maxF);
                        pixel[2] = (uint8_t)avifRoundf((float)pixel[2] * (float)a / maxF);
                        pixel[3] = (uint8_t)avifRoundf((float)pixel[3] * (float)a / maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        }
    }

    return AVIF_RESULT_OK;
}

avifResult avifRGBImageUnpremultiplyAlpha(avifRGBImage * rgb)
{
    // no data
    if (!rgb->pixels || !rgb->rowBytes) {
        return AVIF_RESULT_REFORMAT_FAILED;
    }

    // no alpha.
    if (!avifRGBFormatHasAlpha(rgb->format)) {
        return AVIF_RESULT_REFORMAT_FAILED;
    }

    avifResult libyuvResult = avifRGBImageUnpremultiplyAlphaLibYUV(rgb);
    if (libyuvResult != AVIF_RESULT_NOT_IMPLEMENTED) {
        return libyuvResult;
    }

    assert(rgb->depth >= 8 && rgb->depth <= 16);

    uint32_t max = (1 << rgb->depth) - 1;
    float maxF = (float)max;

    if (rgb->depth > 8) {
        if (rgb->format == AVIF_RGB_FORMAT_RGBA || rgb->format == AVIF_RGB_FORMAT_BGRA) {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint16_t * pixel = (uint16_t *)row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint16_t a = pixel[3];
                    if (a >= max) {
                        // opaque is no-op
                    } else if (a == 0) {
                        // prevent division by zero
                        pixel[0] = 0;
                        pixel[1] = 0;
                        pixel[2] = 0;
                    } else {
                        float c1 = avifRoundf((float)pixel[0] * maxF / (float)a);
                        float c2 = avifRoundf((float)pixel[1] * maxF / (float)a);
                        float c3 = avifRoundf((float)pixel[2] * maxF / (float)a);
                        pixel[0] = (uint16_t)AVIF_MIN(c1, maxF);
                        pixel[1] = (uint16_t)AVIF_MIN(c2, maxF);
                        pixel[2] = (uint16_t)AVIF_MIN(c3, maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        } else {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint16_t * pixel = (uint16_t *)row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint16_t a = pixel[0];
                    if (a >= max) {
                    } else if (a == 0) {
                        pixel[1] = 0;
                        pixel[2] = 0;
                        pixel[3] = 0;
                    } else {
                        float c1 = avifRoundf((float)pixel[1] * maxF / (float)a);
                        float c2 = avifRoundf((float)pixel[2] * maxF / (float)a);
                        float c3 = avifRoundf((float)pixel[3] * maxF / (float)a);
                        pixel[1] = (uint16_t)AVIF_MIN(c1, maxF);
                        pixel[2] = (uint16_t)AVIF_MIN(c2, maxF);
                        pixel[3] = (uint16_t)AVIF_MIN(c3, maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        }
    } else {
        if (rgb->format == AVIF_RGB_FORMAT_RGBA || rgb->format == AVIF_RGB_FORMAT_BGRA) {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint8_t * pixel = row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint8_t a = pixel[3];
                    if (a == max) {
                    } else if (a == 0) {
                        pixel[0] = 0;
                        pixel[1] = 0;
                        pixel[2] = 0;
                    } else {
                        float c1 = avifRoundf((float)pixel[0] * maxF / (float)a);
                        float c2 = avifRoundf((float)pixel[1] * maxF / (float)a);
                        float c3 = avifRoundf((float)pixel[2] * maxF / (float)a);
                        pixel[0] = (uint8_t)AVIF_MIN(c1, maxF);
                        pixel[1] = (uint8_t)AVIF_MIN(c2, maxF);
                        pixel[2] = (uint8_t)AVIF_MIN(c3, maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        } else {
            uint8_t * row = rgb->pixels;
            for (uint32_t j = 0; j < rgb->height; ++j) {
                uint8_t * pixel = row;
                for (uint32_t i = 0; i < rgb->width; ++i) {
                    uint8_t a = pixel[0];
                    if (a == max) {
                    } else if (a == 0) {
                        pixel[1] = 0;
                        pixel[2] = 0;
                        pixel[3] = 0;
                    } else {
                        float c1 = avifRoundf((float)pixel[1] * maxF / (float)a);
                        float c2 = avifRoundf((float)pixel[2] * maxF / (float)a);
                        float c3 = avifRoundf((float)pixel[3] * maxF / (float)a);
                        pixel[1] = (uint8_t)AVIF_MIN(c1, maxF);
                        pixel[2] = (uint8_t)AVIF_MIN(c2, maxF);
                        pixel[3] = (uint8_t)AVIF_MIN(c3, maxF);
                    }
                    pixel += 4;
                }
                row += rgb->rowBytes;
            }
        }
    }

    return AVIF_RESULT_OK;
}
