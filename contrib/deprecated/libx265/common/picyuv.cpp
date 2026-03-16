/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Min Chen <chenm003@163.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#include "common.h"
#include "picyuv.h"
#include "slice.h"
#include "primitives.h"

using namespace X265_NS;

PicYuv::PicYuv()
{
    m_picBuf[0] = NULL;
    m_picBuf[1] = NULL;
    m_picBuf[2] = NULL;

    m_picOrg[0] = NULL;
    m_picOrg[1] = NULL;
    m_picOrg[2] = NULL;

    m_cuOffsetY = NULL;
    m_cuOffsetC = NULL;
    m_buOffsetY = NULL;
    m_buOffsetC = NULL;

    m_maxLumaLevel = 0;
    m_avgLumaLevel = 0;

    m_maxChromaULevel = 0;
    m_avgChromaULevel = 0;

    m_maxChromaVLevel = 0;
    m_avgChromaVLevel = 0;

#if (X265_DEPTH > 8)
    m_minLumaLevel = 0xFFFF;
    m_minChromaULevel = 0xFFFF;
    m_minChromaVLevel = 0xFFFF;
#else
    m_minLumaLevel = 0xFF;
    m_minChromaULevel = 0xFF;
    m_minChromaVLevel = 0xFF;
#endif

    m_stride = 0;
    m_strideC = 0;
    m_hChromaShift = 0;
    m_vChromaShift = 0;
}

bool PicYuv::create(x265_param* param, bool picAlloc, pixel *pixelbuf)
{
    m_param = param;
    uint32_t picWidth = m_param->sourceWidth;
    uint32_t picHeight = m_param->sourceHeight;
    uint32_t picCsp = m_param->internalCsp;
    m_picWidth  = picWidth;
    m_picHeight = picHeight;
    m_hChromaShift = CHROMA_H_SHIFT(picCsp);
    m_vChromaShift = CHROMA_V_SHIFT(picCsp);
    m_picCsp = picCsp;

    uint32_t numCuInWidth = (m_picWidth + param->maxCUSize - 1)  / param->maxCUSize;
    uint32_t numCuInHeight = (m_picHeight + param->maxCUSize - 1) / param->maxCUSize;

    m_lumaMarginX = param->maxCUSize + 32; // search margin and 8-tap filter half-length, padded for 32-byte alignment
    m_lumaMarginY = param->maxCUSize + 16; // margin for 8-tap filter and infinite padding
    m_stride = (numCuInWidth * param->maxCUSize) + (m_lumaMarginX << 1);

    int maxHeight = numCuInHeight * param->maxCUSize;
    if (pixelbuf)
        m_picOrg[0] = pixelbuf;
    else
    {
        if (picAlloc)
        {
            CHECKED_MALLOC(m_picBuf[0], pixel, m_stride * (maxHeight + (m_lumaMarginY * 2)));
            m_picOrg[0] = m_picBuf[0] + m_lumaMarginY * m_stride + m_lumaMarginX;
        }
    }

    if (picCsp != X265_CSP_I400)
    {
        m_chromaMarginX = m_lumaMarginX;  // keep 16-byte alignment for chroma CTUs
        m_chromaMarginY = m_lumaMarginY >> m_vChromaShift;
        m_strideC = ((numCuInWidth * m_param->maxCUSize) >> m_hChromaShift) + (m_chromaMarginX * 2);
        if (picAlloc)
        {
            CHECKED_MALLOC(m_picBuf[1], pixel, m_strideC * ((maxHeight >> m_vChromaShift) + (m_chromaMarginY * 2)));
            CHECKED_MALLOC(m_picBuf[2], pixel, m_strideC * ((maxHeight >> m_vChromaShift) + (m_chromaMarginY * 2)));

            m_picOrg[1] = m_picBuf[1] + m_chromaMarginY * m_strideC + m_chromaMarginX;
            m_picOrg[2] = m_picBuf[2] + m_chromaMarginY * m_strideC + m_chromaMarginX;
        }
    }
    else
    {
        m_picBuf[1] = m_picBuf[2] = NULL;
        m_picOrg[1] = m_picOrg[2] = NULL;
    }
    return true;

fail:
    return false;
}

int PicYuv::getLumaBufLen(uint32_t picWidth, uint32_t picHeight, uint32_t picCsp)
{
    m_picWidth = picWidth;
    m_picHeight = picHeight;
    m_hChromaShift = CHROMA_H_SHIFT(picCsp);
    m_vChromaShift = CHROMA_V_SHIFT(picCsp);
    m_picCsp = picCsp;

    uint32_t numCuInWidth = (m_picWidth + m_param->maxCUSize - 1) / m_param->maxCUSize;
    uint32_t numCuInHeight = (m_picHeight + m_param->maxCUSize - 1) / m_param->maxCUSize;

    m_lumaMarginX = m_param->maxCUSize + 32; // search margin and 8-tap filter half-length, padded for 32-byte alignment
    m_lumaMarginY = m_param->maxCUSize + 16; // margin for 8-tap filter and infinite padding
    m_stride = (numCuInWidth * m_param->maxCUSize) + (m_lumaMarginX << 1);

    int maxHeight = numCuInHeight * m_param->maxCUSize;
    int bufLen = (int)(m_stride * (maxHeight + (m_lumaMarginY * 2)));

    return bufLen;
}

/* the first picture allocated by the encoder will be asked to generate these
 * offset arrays. Once generated, they will be provided to all future PicYuv
 * allocated by the same encoder. */
bool PicYuv::createOffsets(const SPS& sps)
{
    uint32_t numPartitions = 1 << (m_param->unitSizeDepth * 2);

    if (m_picCsp != X265_CSP_I400)
    {
        CHECKED_MALLOC(m_cuOffsetY, intptr_t, sps.numCuInWidth * sps.numCuInHeight);
        CHECKED_MALLOC(m_cuOffsetC, intptr_t, sps.numCuInWidth * sps.numCuInHeight);
        for (uint32_t cuRow = 0; cuRow < sps.numCuInHeight; cuRow++)
        {
            for (uint32_t cuCol = 0; cuCol < sps.numCuInWidth; cuCol++)
            {
                m_cuOffsetY[cuRow * sps.numCuInWidth + cuCol] = m_stride * cuRow * m_param->maxCUSize + cuCol * m_param->maxCUSize;
                m_cuOffsetC[cuRow * sps.numCuInWidth + cuCol] = m_strideC * cuRow * (m_param->maxCUSize >> m_vChromaShift) + cuCol * (m_param->maxCUSize >> m_hChromaShift);
            }
        }

        CHECKED_MALLOC(m_buOffsetY, intptr_t, (size_t)numPartitions);
        CHECKED_MALLOC(m_buOffsetC, intptr_t, (size_t)numPartitions);
        for (uint32_t idx = 0; idx < numPartitions; ++idx)
        {
            intptr_t x = g_zscanToPelX[idx];
            intptr_t y = g_zscanToPelY[idx];
            m_buOffsetY[idx] = m_stride * y + x;
            m_buOffsetC[idx] = m_strideC * (y >> m_vChromaShift) + (x >> m_hChromaShift);
        }
    }
    else
    {
        CHECKED_MALLOC(m_cuOffsetY, intptr_t, sps.numCuInWidth * sps.numCuInHeight);
        for (uint32_t cuRow = 0; cuRow < sps.numCuInHeight; cuRow++)
        for (uint32_t cuCol = 0; cuCol < sps.numCuInWidth; cuCol++)
            m_cuOffsetY[cuRow * sps.numCuInWidth + cuCol] = m_stride * cuRow * m_param->maxCUSize + cuCol * m_param->maxCUSize;

        CHECKED_MALLOC(m_buOffsetY, intptr_t, (size_t)numPartitions);
        for (uint32_t idx = 0; idx < numPartitions; ++idx)
        {
            intptr_t x = g_zscanToPelX[idx];
            intptr_t y = g_zscanToPelY[idx];
            m_buOffsetY[idx] = m_stride * y + x;
        }
    }
    return true;

fail:
    return false;
}

void PicYuv::destroy()
{
    X265_FREE(m_picBuf[0]);
    X265_FREE(m_picBuf[1]);
    X265_FREE(m_picBuf[2]);
}

/* Copy pixels from an x265_picture into internal PicYuv instance.
 * Shift pixels as necessary, mask off bits above X265_DEPTH for safety. */
void PicYuv::copyFromPicture(const x265_picture& pic, const x265_param& param, int padx, int pady)
{
    /* m_picWidth is the width that is being encoded, padx indicates how many
     * of those pixels are padding to reach multiple of MinCU(4) size.
     *
     * Internally, we need to extend rows out to a multiple of 16 for lowres
     * downscale and other operations. But those padding pixels are never
     * encoded.
     *
     * The same applies to m_picHeight and pady */

    /* width and height - without padsize (input picture raw width and height) */
    int width = m_picWidth - padx;
    int height = m_picHeight - pady;

    /* internal pad to multiple of 16x16 blocks */
    uint8_t rem = width & 15;

    padx = rem ? 16 - rem : padx;
    rem = height & 15;
    pady = rem ? 16 - rem : pady;

    /* add one more row and col of pad for downscale interpolation, fixes
     * warnings from valgrind about using uninitialized pixels */
    padx++;
    pady++;
    m_picCsp = pic.colorSpace;

    X265_CHECK(pic.bitDepth >= 8, "pic.bitDepth check failure");

    uint64_t lumaSum;
    uint64_t cbSum;
    uint64_t crSum;
    lumaSum = cbSum = crSum = 0;

    if (m_param->bCopyPicToFrame)
    {
        if (pic.bitDepth == 8)
        {
#if (X265_DEPTH > 8)
        {
            pixel *yPixel = m_picOrg[0];

            uint8_t *yChar = (uint8_t*)pic.planes[0];
            int shift = (X265_DEPTH - 8);

            primitives.planecopy_cp(yChar, pic.stride[0] / sizeof(*yChar), yPixel, m_stride, width, height, shift);

            if (param.internalCsp != X265_CSP_I400)
            {
                pixel *uPixel = m_picOrg[1];
                pixel *vPixel = m_picOrg[2];

                uint8_t *uChar = (uint8_t*)pic.planes[1];
                uint8_t *vChar = (uint8_t*)pic.planes[2];

                primitives.planecopy_cp(uChar, pic.stride[1] / sizeof(*uChar), uPixel, m_strideC, width >> m_hChromaShift, height >> m_vChromaShift, shift);
                primitives.planecopy_cp(vChar, pic.stride[2] / sizeof(*vChar), vPixel, m_strideC, width >> m_hChromaShift, height >> m_vChromaShift, shift);
            }
        }
#else /* Case for (X265_DEPTH == 8) */
            // TODO: Does we need this path? may merge into above in future
        {
            pixel *yPixel = m_picOrg[0];
            uint8_t *yChar = (uint8_t*)pic.planes[0];

            for (int r = 0; r < height; r++)
            {
                memcpy(yPixel, yChar, width * sizeof(pixel));

                yPixel += m_stride;
                yChar += pic.stride[0] / sizeof(*yChar);
            }

            if (param.internalCsp != X265_CSP_I400)
            {
                pixel *uPixel = m_picOrg[1];
                pixel *vPixel = m_picOrg[2];

                uint8_t *uChar = (uint8_t*)pic.planes[1];
                uint8_t *vChar = (uint8_t*)pic.planes[2];

                for (int r = 0; r < height >> m_vChromaShift; r++)
                {
                    memcpy(uPixel, uChar, (width >> m_hChromaShift) * sizeof(pixel));
                    memcpy(vPixel, vChar, (width >> m_hChromaShift) * sizeof(pixel));

                    uPixel += m_strideC;
                    vPixel += m_strideC;
                    uChar += pic.stride[1] / sizeof(*uChar);
                    vChar += pic.stride[2] / sizeof(*vChar);
                }
            }
        }
#endif /* (X265_DEPTH > 8) */
        }
        else /* pic.bitDepth > 8 */
        {
            /* defensive programming, mask off bits that are supposed to be zero */
            uint16_t mask = (1 << X265_DEPTH) - 1;
            int shift = abs(pic.bitDepth - X265_DEPTH);
            pixel *yPixel = m_picOrg[0];

            uint16_t *yShort = (uint16_t*)pic.planes[0];

            if (pic.bitDepth > X265_DEPTH)
            {
                /* shift right and mask pixels to final size */
                primitives.planecopy_sp(yShort, pic.stride[0] / sizeof(*yShort), yPixel, m_stride, width, height, shift, mask);
            }
            else /* Case for (pic.bitDepth <= X265_DEPTH) */
            {
                /* shift left and mask pixels to final size */
                primitives.planecopy_sp_shl(yShort, pic.stride[0] / sizeof(*yShort), yPixel, m_stride, width, height, shift, mask);
            }

            if (param.internalCsp != X265_CSP_I400)
            {
                pixel *uPixel = m_picOrg[1];
                pixel *vPixel = m_picOrg[2];

                uint16_t *uShort = (uint16_t*)pic.planes[1];
                uint16_t *vShort = (uint16_t*)pic.planes[2];

                if (pic.bitDepth > X265_DEPTH)
                {
                    primitives.planecopy_sp(uShort, pic.stride[1] / sizeof(*uShort), uPixel, m_strideC, width >> m_hChromaShift, height >> m_vChromaShift, shift, mask);
                    primitives.planecopy_sp(vShort, pic.stride[2] / sizeof(*vShort), vPixel, m_strideC, width >> m_hChromaShift, height >> m_vChromaShift, shift, mask);
                }
                else /* Case for (pic.bitDepth <= X265_DEPTH) */
                {
                    primitives.planecopy_sp_shl(uShort, pic.stride[1] / sizeof(*uShort), uPixel, m_strideC, width >> m_hChromaShift, height >> m_vChromaShift, shift, mask);
                    primitives.planecopy_sp_shl(vShort, pic.stride[2] / sizeof(*vShort), vPixel, m_strideC, width >> m_hChromaShift, height >> m_vChromaShift, shift, mask);
                }
            }
        }
    }
    else
    {
        m_picOrg[0] = (pixel*)pic.planes[0];
        m_picOrg[1] = (pixel*)pic.planes[1];
        m_picOrg[2] = (pixel*)pic.planes[2];
    }

    pixel *Y = m_picOrg[0];
    pixel *U = m_picOrg[1];
    pixel *V = m_picOrg[2];

    pixel *yPic = m_picOrg[0];
    pixel *uPic = m_picOrg[1];
    pixel *vPic = m_picOrg[2];

    for (int r = 0; r < height; r++)
    {
        for (int c = 0; c < width; c++)
        {
            m_maxLumaLevel = X265_MAX(yPic[c], m_maxLumaLevel);
            m_minLumaLevel = X265_MIN(yPic[c], m_minLumaLevel);
            lumaSum += yPic[c];
        }
        yPic += m_stride;
    }
    m_avgLumaLevel = (double)lumaSum / (m_picHeight * m_picWidth);

    if (param.csvLogLevel >= 2)
    {
        if (param.internalCsp != X265_CSP_I400)
        {
            for (int r = 0; r < height >> m_vChromaShift; r++)
            {
                for (int c = 0; c < width >> m_hChromaShift; c++)
                {
                    m_maxChromaULevel = X265_MAX(uPic[c], m_maxChromaULevel);
                    m_minChromaULevel = X265_MIN(uPic[c], m_minChromaULevel);
                    cbSum += uPic[c];

                    m_maxChromaVLevel = X265_MAX(vPic[c], m_maxChromaVLevel);
                    m_minChromaVLevel = X265_MIN(vPic[c], m_minChromaVLevel);
                    crSum += vPic[c];
                }

                uPic += m_strideC;
                vPic += m_strideC;
            }
            m_avgChromaULevel = (double)cbSum / ((height >> m_vChromaShift) * (width >> m_hChromaShift));
            m_avgChromaVLevel = (double)crSum / ((height >> m_vChromaShift) * (width >> m_hChromaShift));
        }
    }

#if HIGH_BIT_DEPTH
    bool calcHDRParams = !!param.minLuma || (param.maxLuma != PIXEL_MAX);
    /* Apply min/max luma bounds for HDR pixel manipulations */
    if (calcHDRParams)
    {
        X265_CHECK(pic.bitDepth == 10, "HDR stats can be applied/calculated only for 10bpp content");
        uint64_t sumLuma;
        m_maxLumaLevel = primitives.planeClipAndMax(Y, m_stride, width, height, &sumLuma, (pixel)param.minLuma, (pixel)param.maxLuma);
        m_avgLumaLevel = (double) sumLuma / (m_picHeight * m_picWidth);
    }
#else
    (void) param;
#endif

    /* extend the right edge if width was not multiple of the minimum CU size */
    for (int r = 0; r < height; r++)
    {
        for (int x = 0; x < padx; x++)
            Y[width + x] = Y[width - 1];
        Y += m_stride;
    }

    /* extend the bottom if height was not multiple of the minimum CU size */
    Y = m_picOrg[0] + (height - 1) * m_stride;
    for (int i = 1; i <= pady; i++)
        memcpy(Y + i * m_stride, Y, (width + padx) * sizeof(pixel));

    if (param.internalCsp != X265_CSP_I400)
    {
        for (int r = 0; r < height >> m_vChromaShift; r++)
        {
            for (int x = 0; x < padx >> m_hChromaShift; x++)
            {
                U[(width >> m_hChromaShift) + x] = U[(width >> m_hChromaShift) - 1];
                V[(width >> m_hChromaShift) + x] = V[(width >> m_hChromaShift) - 1];
            }

            U += m_strideC;
            V += m_strideC;
        }

        U = m_picOrg[1] + ((height >> m_vChromaShift) - 1) * m_strideC;
        V = m_picOrg[2] + ((height >> m_vChromaShift) - 1) * m_strideC;

        for (int j = 1; j <= pady >> m_vChromaShift; j++)
        {
            memcpy(U + j * m_strideC, U, ((width + padx) >> m_hChromaShift) * sizeof(pixel));
            memcpy(V + j * m_strideC, V, ((width + padx) >> m_hChromaShift) * sizeof(pixel));
        }
    }
}

namespace X265_NS {

template<uint32_t OUTPUT_BITDEPTH_DIV8>
static void md5_block(MD5Context& md5, const pixel* plane, uint32_t n)
{
    /* create a 64 byte buffer for packing pixel's into */
    uint8_t buf[64 / OUTPUT_BITDEPTH_DIV8][OUTPUT_BITDEPTH_DIV8];

    for (uint32_t i = 0; i < n; i++)
    {
        pixel pel = plane[i];
        /* perform bitdepth and endian conversion */
        for (uint32_t d = 0; d < OUTPUT_BITDEPTH_DIV8; d++)
            buf[i][d] = (uint8_t)(pel >> (d * 8));
    }

    MD5Update(&md5, (uint8_t*)buf, n * OUTPUT_BITDEPTH_DIV8);
}

/* Update md5 with all samples in plane in raster order, each sample
 * is adjusted to OUTBIT_BITDEPTH_DIV8 */
template<uint32_t OUTPUT_BITDEPTH_DIV8>
static void md5_plane(MD5Context& md5, const pixel* plane, uint32_t width, uint32_t height, intptr_t stride)
{
    /* N is the number of samples to process per md5 update.
     * All N samples must fit in buf */
    uint32_t N = 32;
    uint32_t width_modN = width % N;
    uint32_t width_less_modN = width - width_modN;

    for (uint32_t y = 0; y < height; y++)
    {
        /* convert pel's into uint32_t chars in little endian byte order.
         * NB, for 8bit data, data is truncated to 8bits. */
        for (uint32_t x = 0; x < width_less_modN; x += N)
            md5_block<OUTPUT_BITDEPTH_DIV8>(md5, &plane[y * stride + x], N);

        /* mop up any of the remaining line */
        md5_block<OUTPUT_BITDEPTH_DIV8>(md5, &plane[y * stride + width_less_modN], width_modN);
    }
}

void updateCRC(const pixel* plane, uint32_t& crcVal, uint32_t height, uint32_t width, intptr_t stride)
{
    uint32_t crcMsb;
    uint32_t bitVal;
    uint32_t bitIdx;

    for (uint32_t y = 0; y < height; y++)
    {
        for (uint32_t x = 0; x < width; x++)
        {
            // take CRC of first pictureData byte
            for (bitIdx = 0; bitIdx < 8; bitIdx++)
            {
                crcMsb = (crcVal >> 15) & 1;
                bitVal = (plane[y * stride + x] >> (7 - bitIdx)) & 1;
                crcVal = (((crcVal << 1) + bitVal) & 0xffff) ^ (crcMsb * 0x1021);
            }

#if _MSC_VER
#pragma warning(disable: 4127) // conditional expression is constant
#endif
            // take CRC of second pictureData byte if bit depth is greater than 8-bits
            if (X265_DEPTH > 8)
            {
                for (bitIdx = 0; bitIdx < 8; bitIdx++)
                {
                    crcMsb = (crcVal >> 15) & 1;
                    bitVal = (plane[y * stride + x] >> (15 - bitIdx)) & 1;
                    crcVal = (((crcVal << 1) + bitVal) & 0xffff) ^ (crcMsb * 0x1021);
                }
            }
        }
    }
}

void crcFinish(uint32_t& crcVal, uint8_t digest[16])
{
    uint32_t crcMsb;

    for (int bitIdx = 0; bitIdx < 16; bitIdx++)
    {
        crcMsb = (crcVal >> 15) & 1;
        crcVal = ((crcVal << 1) & 0xffff) ^ (crcMsb * 0x1021);
    }

    digest[0] = (crcVal >> 8)  & 0xff;
    digest[1] =  crcVal        & 0xff;
}

void updateChecksum(const pixel* plane, uint32_t& checksumVal, uint32_t height, uint32_t width, intptr_t stride, int row, uint32_t cuHeight)
{
    uint8_t xor_mask;

    for (uint32_t y = row * cuHeight; y < ((row * cuHeight) + height); y++)
    {
        for (uint32_t x = 0; x < width; x++)
        {
            xor_mask = (uint8_t)((x & 0xff) ^ (y & 0xff) ^ (x >> 8) ^ (y >> 8));
            checksumVal = (checksumVal + ((plane[y * stride + x] & 0xff) ^ xor_mask)) & 0xffffffff;

            if (X265_DEPTH > 8)
                checksumVal = (checksumVal + ((plane[y * stride + x] >> 7 >> 1) ^ xor_mask)) & 0xffffffff;
        }
    }
}

void checksumFinish(uint32_t checksum, uint8_t digest[16])
{
    digest[0] = (checksum >> 24) & 0xff;
    digest[1] = (checksum >> 16) & 0xff;
    digest[2] = (checksum >> 8)  & 0xff;
    digest[3] =  checksum        & 0xff;
}

void updateMD5Plane(MD5Context& md5, const pixel* plane, uint32_t width, uint32_t height, intptr_t stride)
{
    /* choose an md5_plane packing function based on the system bitdepth */
    typedef void(*MD5PlaneFunc)(MD5Context&, const pixel*, uint32_t, uint32_t, intptr_t);
    MD5PlaneFunc md5_plane_func;
    md5_plane_func = X265_DEPTH <= 8 ? (MD5PlaneFunc)md5_plane<1> : (MD5PlaneFunc)md5_plane<2>;

    md5_plane_func(md5, plane, width, height, stride);
}
}
