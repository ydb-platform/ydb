/*------------------------------------------------------------------------
 *  Copyright 2007-2009 (c) Jeff Brown <spadix@users.sourceforge.net>
 *
 *  This file is part of the ZBar Bar Code Reader.
 *
 *  The ZBar Bar Code Reader is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU Lesser Public License as
 *  published by the Free Software Foundation; either version 2.1 of
 *  the License, or (at your option) any later version.
 *
 *  The ZBar Bar Code Reader is distributed in the hope that it will be
 *  useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 *  of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser Public License
 *  along with the ZBar Bar Code Reader; if not, write to the Free
 *  Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 *  Boston, MA  02110-1301  USA
 *
 *  http://sourceforge.net/projects/zbar
 *------------------------------------------------------------------------*/

#include "image.h"
#include "video.h"
#include "window.h"

/* pack bit size and location offset of a component into one byte
 */
#define RGB_BITS(off, size) ((((8 - (size)) & 0x7) << 5) | ((off) & 0x1f))

typedef void (conversion_handler_t)(zbar_image_t*,
                                    const zbar_format_def_t*,
                                    const zbar_image_t*,
                                    const zbar_format_def_t*);

typedef struct conversion_def_s {
    int cost;                           /* conversion "badness" */
    conversion_handler_t *func;         /* function that accomplishes it */
} conversion_def_t;


/* NULL terminated list of known formats, in order of preference
 * (NB Cr=V Cb=U)
 */
const uint32_t _zbar_formats[] = {

    /* planar YUV formats */
    fourcc('4','2','2','P'), /* FIXME also YV16? */
    fourcc('I','4','2','0'),
    fourcc('Y','U','1','2'), /* FIXME also IYUV? */
    fourcc('Y','V','1','2'),
    fourcc('4','1','1','P'),

    /* planar Y + packed UV plane */
    fourcc('N','V','1','2'),
    fourcc('N','V','2','1'),

    /* packed YUV formats */
    fourcc('Y','U','Y','V'),
    fourcc('U','Y','V','Y'),
    fourcc('Y','U','Y','2'), /* FIXME add YVYU */
    fourcc('Y','U','V','4'), /* FIXME where is this from? */

    /* packed rgb formats */
    fourcc('R','G','B','3'),
    fourcc( 3 , 0 , 0 , 0 ),
    fourcc('B','G','R','3'),
    fourcc('R','G','B','4'),
    fourcc('B','G','R','4'),

    fourcc('R','G','B','P'),
    fourcc('R','G','B','O'),
    fourcc('R','G','B','R'),
    fourcc('R','G','B','Q'),

    fourcc('Y','U','V','9'),
    fourcc('Y','V','U','9'),

    /* basic grayscale format */
    fourcc('G','R','E','Y'),
    fourcc('Y','8','0','0'),
    fourcc('Y','8',' ',' '),
    fourcc('Y','8', 0 , 0 ),

    /* low quality RGB formats */
    fourcc('R','G','B','1'),
    fourcc('R','4','4','4'),
    fourcc('B','A','8','1'),

    /* unsupported packed YUV formats */
    fourcc('Y','4','1','P'),
    fourcc('Y','4','4','4'),
    fourcc('Y','U','V','O'),
    fourcc('H','M','1','2'),

    /* unsupported packed RGB format */
    fourcc('H','I','2','4'),

    /* unsupported compressed formats */
    fourcc('J','P','E','G'),
    fourcc('M','J','P','G'),
    fourcc('M','P','E','G'),

    /* terminator */
    0
};

const int _zbar_num_formats = sizeof(_zbar_formats) / sizeof(uint32_t);

/* format definitions */
static const zbar_format_def_t format_defs[] = {

    { fourcc('R','G','B','4'), ZBAR_FMT_RGB_PACKED,
        { { 4, RGB_BITS(8, 8), RGB_BITS(16, 8), RGB_BITS(24, 8) } } },
    { fourcc('B','G','R','1'), ZBAR_FMT_RGB_PACKED,
        { { 1, RGB_BITS(0, 3), RGB_BITS(3, 3), RGB_BITS(6, 2) } } },
    { fourcc('4','2','2','P'), ZBAR_FMT_YUV_PLANAR, { { 1, 0, 0 /*UV*/ } } },
    { fourcc('Y','8','0','0'), ZBAR_FMT_GRAY, },
    { fourcc('Y','U','Y','2'), ZBAR_FMT_YUV_PACKED,
        { { 1, 0, 0, /*YUYV*/ } } },
    { fourcc('J','P','E','G'), ZBAR_FMT_JPEG, },
    { fourcc('Y','V','Y','U'), ZBAR_FMT_YUV_PACKED,
        { { 1, 0, 1, /*YVYU*/ } } },
    { fourcc('Y','8', 0 , 0 ), ZBAR_FMT_GRAY, },
    { fourcc('N','V','2','1'), ZBAR_FMT_YUV_NV,     { { 1, 1, 1 /*VU*/ } } },
    { fourcc('N','V','1','2'), ZBAR_FMT_YUV_NV,     { { 1, 1, 0 /*UV*/ } } },
    { fourcc('B','G','R','3'), ZBAR_FMT_RGB_PACKED,
        { { 3, RGB_BITS(16, 8), RGB_BITS(8, 8), RGB_BITS(0, 8) } } },
    { fourcc('Y','V','U','9'), ZBAR_FMT_YUV_PLANAR, { { 2, 2, 1 /*VU*/ } } },
    { fourcc('R','G','B','O'), ZBAR_FMT_RGB_PACKED,
        { { 2, RGB_BITS(10, 5), RGB_BITS(5, 5), RGB_BITS(0, 5) } } },
    { fourcc('R','G','B','Q'), ZBAR_FMT_RGB_PACKED,
        { { 2, RGB_BITS(2, 5), RGB_BITS(13, 5), RGB_BITS(8, 5) } } },
    { fourcc('G','R','E','Y'), ZBAR_FMT_GRAY, },
    { fourcc( 3 , 0 , 0 , 0 ), ZBAR_FMT_RGB_PACKED,
        { { 4, RGB_BITS(16, 8), RGB_BITS(8, 8), RGB_BITS(0, 8) } } },
    { fourcc('Y','8',' ',' '), ZBAR_FMT_GRAY, },
    { fourcc('I','4','2','0'), ZBAR_FMT_YUV_PLANAR, { { 1, 1, 0 /*UV*/ } } },
    { fourcc('R','G','B','1'), ZBAR_FMT_RGB_PACKED,
        { { 1, RGB_BITS(5, 3), RGB_BITS(2, 3), RGB_BITS(0, 2) } } },
    { fourcc('Y','U','1','2'), ZBAR_FMT_YUV_PLANAR, { { 1, 1, 0 /*UV*/ } } },
    { fourcc('Y','V','1','2'), ZBAR_FMT_YUV_PLANAR, { { 1, 1, 1 /*VU*/ } } },
    { fourcc('R','G','B','3'), ZBAR_FMT_RGB_PACKED,
        { { 3, RGB_BITS(0, 8), RGB_BITS(8, 8), RGB_BITS(16, 8) } } },
    { fourcc('R','4','4','4'), ZBAR_FMT_RGB_PACKED,
        { { 2, RGB_BITS(8, 4), RGB_BITS(4, 4), RGB_BITS(0, 4) } } },
    { fourcc('B','G','R','4'), ZBAR_FMT_RGB_PACKED,
        { { 4, RGB_BITS(16, 8), RGB_BITS(8, 8), RGB_BITS(0, 8) } } },
    { fourcc('Y','U','V','9'), ZBAR_FMT_YUV_PLANAR, { { 2, 2, 0 /*UV*/ } } },
    { fourcc('M','J','P','G'), ZBAR_FMT_JPEG, },
    { fourcc('4','1','1','P'), ZBAR_FMT_YUV_PLANAR, { { 2, 0, 0 /*UV*/ } } },
    { fourcc('R','G','B','P'), ZBAR_FMT_RGB_PACKED,
        { { 2, RGB_BITS(11, 5), RGB_BITS(5, 6), RGB_BITS(0, 5) } } },
    { fourcc('R','G','B','R'), ZBAR_FMT_RGB_PACKED,
        { { 2, RGB_BITS(3, 5), RGB_BITS(13, 6), RGB_BITS(8, 5) } } },
    { fourcc('Y','U','Y','V'), ZBAR_FMT_YUV_PACKED,
        { { 1, 0, 0, /*YUYV*/ } } },
    { fourcc('U','Y','V','Y'), ZBAR_FMT_YUV_PACKED,
        { { 1, 0, 2, /*UYVY*/ } } },
};

static const int num_format_defs =
    sizeof(format_defs) / sizeof(zbar_format_def_t);

#ifdef DEBUG_CONVERT
static int intsort (const void *a,
                    const void *b)
{
    return(*(uint32_t*)a - *(uint32_t*)b);
}
#endif

/* verify that format list is in required sort order */
static inline int verify_format_sort (void)
{
    int i;
    for(i = 0; i < num_format_defs; i++) {
        int j = i * 2 + 1;
        if((j < num_format_defs &&
            format_defs[i].format < format_defs[j].format) ||
           (j + 1 < num_format_defs &&
            format_defs[j + 1].format < format_defs[i].format))
            break;
    }
    if(i == num_format_defs)
        return(0);

    /* spew correct order for fix */
    fprintf(stderr, "ERROR: image format list is not sorted!?\n");

#ifdef DEBUG_CONVERT
    assert(num_format_defs);
    uint32_t sorted[num_format_defs];
    uint32_t ordered[num_format_defs];
    for(i = 0; i < num_format_defs; i++)
        sorted[i] = format_defs[i].format;
    qsort(sorted, num_format_defs, sizeof(uint32_t), intsort);
    for(i = 0; i < num_format_defs; i = i << 1 | 1);
    i = (i - 1) / 2;
    ordered[i] = sorted[0];
    int j, k;
    for(j = 1; j < num_format_defs; j++) {
        k = i * 2 + 2;
        if(k < num_format_defs) {
            i = k;
            for(k = k * 2 + 1; k < num_format_defs; k = k * 2 + 1)
                i = k;
        }
        else {
            for(k = (i - 1) / 2; i != k * 2 + 1; k = (i - 1) / 2) {
                assert(i);
                i = k;
            }
            i = k;
        }
        ordered[i] = sorted[j];
    }
    fprintf(stderr, "correct sort order is:");
    for(i = 0; i < num_format_defs; i++)
        fprintf(stderr, " %4.4s", (char*)&ordered[i]);
    fprintf(stderr, "\n");
#endif
    return(-1);
}

static inline void uv_round (zbar_image_t *img,
                             const zbar_format_def_t *fmt)
{
    img->width >>= fmt->p.yuv.xsub2;
    img->width <<= fmt->p.yuv.xsub2;
    img->height >>= fmt->p.yuv.ysub2;
    img->height <<= fmt->p.yuv.ysub2;
}

static inline void uv_roundup (zbar_image_t *img,
                               const zbar_format_def_t *fmt)
{
    if(fmt->group == ZBAR_FMT_GRAY)
        return;
    unsigned xmask = (1 << fmt->p.yuv.xsub2) - 1;
    if(img->width & xmask)
        img->width = (img->width + xmask) & ~xmask;
    unsigned ymask = (1 << fmt->p.yuv.ysub2) - 1;
    if(img->height & ymask)
        img->height = (img->height + ymask) & ~ymask;
}

static inline unsigned long uvp_size (const zbar_image_t *img,
                                      const zbar_format_def_t *fmt)
{
    if(fmt->group == ZBAR_FMT_GRAY)
        return(0);
    return((img->width >> fmt->p.yuv.xsub2) *
           (img->height >> fmt->p.yuv.ysub2));
}

static inline uint32_t convert_read_rgb (const uint8_t *srcp,
                                         int bpp)
{
    uint32_t p;
    if(bpp == 3) {
        p = *srcp;
        p |= *(srcp + 1) << 8;
        p |= *(srcp + 2) << 16;
    }
    else if(bpp == 4)
        p = *((uint32_t*)(srcp));
    else if(bpp == 2)
        p = *((uint16_t*)(srcp));
    else
        p = *srcp;
    return(p);
}

static inline void convert_write_rgb (uint8_t *dstp,
                                      uint32_t p,
                                      int bpp)
{
    if(bpp == 3) {
        *dstp = p & 0xff;
        *(dstp + 1) = (p >> 8) & 0xff;
        *(dstp + 2) = (p >> 16) & 0xff;
    }
    else if(bpp == 4)
        *((uint32_t*)dstp) = p;
    else if(bpp == 2)
        *((uint16_t*)dstp) = p;
    else
        *dstp = p;
}

/* cleanup linked image by unrefing */
static void cleanup_ref (zbar_image_t *img)
{
    if(img->next)
        _zbar_image_refcnt(img->next, -1);
}

/* resize y plane, drop extra columns/rows from the right/bottom,
 * or duplicate last column/row to pad missing data
 */
static inline void convert_y_resize (zbar_image_t *dst,
                                     const zbar_format_def_t *dstfmt,
                                     const zbar_image_t *src,
                                     const zbar_format_def_t *srcfmt,
                                     size_t n)
{
    if(dst->width == src->width && dst->height == src->height) {
        memcpy((void*)dst->data, src->data, n);
        return;
    }
    uint8_t *psrc = (void*)src->data;
    uint8_t *pdst = (void*)dst->data;
    unsigned width = (dst->width > src->width) ? src->width : dst->width;
    unsigned xpad = (dst->width > src->width) ? dst->width - src->width : 0;
    unsigned height = (dst->height > src->height) ? src->height : dst->height;
    unsigned y;
    for(y = 0; y < height; y++) {
        memcpy(pdst, psrc, width);
        pdst += width;
        psrc += src->width;
        if(xpad) {
            memset(pdst, *(psrc - 1), xpad);
            pdst += xpad;
        }
    }
    psrc -= src->width;
    for(; y < dst->height; y++) {
        memcpy(pdst, psrc, width);
        pdst += width;
        if(xpad) {
            memset(pdst, *(psrc - 1), xpad);
            pdst += xpad;
        }
    }
}

/* make new image w/reference to the same image data */
static void convert_copy (zbar_image_t *dst,
                          const zbar_format_def_t *dstfmt,
                          const zbar_image_t *src,
                          const zbar_format_def_t *srcfmt)
{
    if(src->width == dst->width &&
       src->height == dst->height) {
        dst->data = src->data;
        dst->datalen = src->datalen;
        dst->cleanup = cleanup_ref;
        zbar_image_t *s = (zbar_image_t*)src;
        dst->next = s;
        _zbar_image_refcnt(s, 1);
    }
    else
        /* NB only for GRAY/YUV_PLANAR formats */
        convert_y_resize(dst, dstfmt, src, srcfmt, dst->width * dst->height);
}

/* append neutral UV plane to grayscale image */
static void convert_uvp_append (zbar_image_t *dst,
                                const zbar_format_def_t *dstfmt,
                                const zbar_image_t *src,
                                const zbar_format_def_t *srcfmt)
{
    uv_roundup(dst, dstfmt);
    dst->datalen = uvp_size(dst, dstfmt) * 2;
    unsigned long n = dst->width * dst->height;
    dst->datalen += n;
    assert(src->datalen >= src->width * src->height);
    zprintf(24, "dst=%dx%d (%lx) %lx src=%dx%d %lx\n",
            dst->width, dst->height, n, dst->datalen,
            src->width, src->height, src->datalen);
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    convert_y_resize(dst, dstfmt, src, srcfmt, n);
    memset((void*)dst->data + n, 0x80, dst->datalen - n);
}

/* interleave YUV planes into packed YUV */
static void convert_yuv_pack (zbar_image_t *dst,
                              const zbar_format_def_t *dstfmt,
                              const zbar_image_t *src,
                              const zbar_format_def_t *srcfmt)
{
    uv_roundup(dst, dstfmt);
    dst->datalen = dst->width * dst->height + uvp_size(dst, dstfmt) * 2;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    uint8_t *dstp = (void*)dst->data;

    unsigned long srcm = uvp_size(src, srcfmt);
    unsigned long srcn = src->width * src->height;
    assert(src->datalen >= srcn + 2 * srcn);
    uint8_t flags = dstfmt->p.yuv.packorder ^ srcfmt->p.yuv.packorder;
    uint8_t *srcy = (void*)src->data;
    const uint8_t *srcu, *srcv;
    if(flags & 1) {
        srcv = src->data + srcn;
        srcu = srcv + srcm;
    } else {
        srcu = src->data + srcn;
        srcv = srcu + srcm;
    }
    flags = dstfmt->p.yuv.packorder & 2;

    unsigned srcl = src->width >> srcfmt->p.yuv.xsub2;
    unsigned xmask = (1 << srcfmt->p.yuv.xsub2) - 1;
    unsigned ymask = (1 << srcfmt->p.yuv.ysub2) - 1;
    unsigned x, y;
    uint8_t y0 = 0, y1 = 0, u = 0x80, v = 0x80;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height) {
            srcy -= src->width;
            srcu -= srcl;  srcv -= srcl;
        }
        else if(y & ymask) {
            srcu -= srcl;  srcv -= srcl;
        }
        for(x = 0; x < dst->width; x += 2) {
            if(x < src->width) {
                y0 = *(srcy++);  y1 = *(srcy++);
                if(!(x & xmask)) {
                    u = *(srcu++);  v = *(srcv++);
                }
            }
            if(flags) {
                *(dstp++) = u;  *(dstp++) = y0;
                *(dstp++) = v;  *(dstp++) = y1;
            } else {
                *(dstp++) = y0;  *(dstp++) = u;
                *(dstp++) = y1;  *(dstp++) = v;
            }
        }
        for(; x < src->width; x += 2) {
            srcy += 2;
            if(!(x & xmask)) {
                srcu++;  srcv++;
            }
        }
    }
}

/* split packed YUV samples and join into YUV planes
 * FIXME currently ignores color and grayscales the image
 */
static void convert_yuv_unpack (zbar_image_t *dst,
                                const zbar_format_def_t *dstfmt,
                                const zbar_image_t *src,
                                const zbar_format_def_t *srcfmt)
{
    uv_roundup(dst, dstfmt);
    unsigned long dstn = dst->width * dst->height;
    unsigned long dstm2 = uvp_size(dst, dstfmt) * 2;
    dst->datalen = dstn + dstm2;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    if(dstm2)
        memset((void*)dst->data + dstn, 0x80, dstm2);
    uint8_t *dsty = (void*)dst->data;

    uint8_t flags = srcfmt->p.yuv.packorder ^ dstfmt->p.yuv.packorder;
    flags &= 2;
    const uint8_t *srcp = src->data;
    if(flags)
        srcp++;

    unsigned srcl = src->width + (src->width >> srcfmt->p.yuv.xsub2);
    unsigned x, y;
    uint8_t y0 = 0, y1 = 0;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height)
            srcp -= srcl;
        for(x = 0; x < dst->width; x += 2) {
            if(x < src->width) {
                y0 = *(srcp++);  srcp++;
                y1 = *(srcp++);  srcp++;
            }
            *(dsty++) = y0;
            *(dsty++) = y1;
        }
        if(x < src->width)
            srcp += (src->width - x) * 2;
    }
}

/* resample and resize UV plane(s)
 * FIXME currently ignores color and grayscales the image
 */
static void convert_uvp_resample (zbar_image_t *dst,
                                  const zbar_format_def_t *dstfmt,
                                  const zbar_image_t *src,
                                  const zbar_format_def_t *srcfmt)
{
    uv_roundup(dst, dstfmt);
    unsigned long dstn = dst->width * dst->height;
    unsigned long dstm2 = uvp_size(dst, dstfmt) * 2;
    dst->datalen = dstn + dstm2;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    convert_y_resize(dst, dstfmt, src, srcfmt, dstn);
    if(dstm2)
        memset((void*)dst->data + dstn, 0x80, dstm2);
}

/* rearrange interleaved UV componets */
static void convert_uv_resample (zbar_image_t *dst,
                                 const zbar_format_def_t *dstfmt,
                                 const zbar_image_t *src,
                                 const zbar_format_def_t *srcfmt)
{
    uv_roundup(dst, dstfmt);
    unsigned long dstn = dst->width * dst->height;
    dst->datalen = dstn + uvp_size(dst, dstfmt) * 2;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    uint8_t *dstp = (void*)dst->data;

    uint8_t flags = (srcfmt->p.yuv.packorder ^ dstfmt->p.yuv.packorder) & 1;
    const uint8_t *srcp = src->data;

    unsigned srcl = src->width + (src->width >> srcfmt->p.yuv.xsub2);
    unsigned x, y;
    uint8_t y0 = 0, y1 = 0, u = 0x80, v = 0x80;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height)
            srcp -= srcl;
        for(x = 0; x < dst->width; x += 2) {
            if(x < src->width) {
                if(!(srcfmt->p.yuv.packorder & 2)) {
                    y0 = *(srcp++);  u = *(srcp++);
                    y1 = *(srcp++);  v = *(srcp++);
                }
                else {
                    u = *(srcp++);  y0 = *(srcp++);
                    v = *(srcp++);  y1 = *(srcp++);
                }
                if(flags) {
                    uint8_t tmp = u;  u = v;  v = tmp;
                }
            }
            if(!(dstfmt->p.yuv.packorder & 2)) {
                *(dstp++) = y0;  *(dstp++) = u;
                *(dstp++) = y1;  *(dstp++) = v;
            }
            else {
                *(dstp++) = u;  *(dstp++) = y0;
                *(dstp++) = v;  *(dstp++) = y1;
            }
        }
        if(x < src->width)
            srcp += (src->width - x) * 2;
    }
}

/* YUV planes to packed RGB
 * FIXME currently ignores color and grayscales the image
 */
static void convert_yuvp_to_rgb (zbar_image_t *dst,
                                 const zbar_format_def_t *dstfmt,
                                 const zbar_image_t *src,
                                 const zbar_format_def_t *srcfmt)
{
    dst->datalen = dst->width * dst->height * dstfmt->p.rgb.bpp;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    uint8_t *dstp = (void*)dst->data;

    int drbits = RGB_SIZE(dstfmt->p.rgb.red);
    int drbit0 = RGB_OFFSET(dstfmt->p.rgb.red);
    int dgbits = RGB_SIZE(dstfmt->p.rgb.green);
    int dgbit0 = RGB_OFFSET(dstfmt->p.rgb.green);
    int dbbits = RGB_SIZE(dstfmt->p.rgb.blue);
    int dbbit0 = RGB_OFFSET(dstfmt->p.rgb.blue);

    unsigned long srcm = uvp_size(src, srcfmt);
    unsigned long srcn = src->width * src->height;
    assert(src->datalen >= srcn + 2 * srcm);
    uint8_t *srcy = (void*)src->data;

    unsigned x, y;
    uint32_t p = 0;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height)
            srcy -= src->width;
        for(x = 0; x < dst->width; x++) {
            if(x < src->width) {
                /* FIXME color space? */
                unsigned y0 = *(srcy++);
                p = (((y0 >> drbits) << drbit0) |
                     ((y0 >> dgbits) << dgbit0) |
                     ((y0 >> dbbits) << dbbit0));
            }
            convert_write_rgb(dstp, p, dstfmt->p.rgb.bpp);
            dstp += dstfmt->p.rgb.bpp;
        }
        if(x < src->width)
            srcy += (src->width - x);
    }
}

/* packed RGB to YUV planes
 * FIXME currently ignores color and grayscales the image
 */
static void convert_rgb_to_yuvp (zbar_image_t *dst,
                                 const zbar_format_def_t *dstfmt,
                                 const zbar_image_t *src,
                                 const zbar_format_def_t *srcfmt)
{
    uv_roundup(dst, dstfmt);
    unsigned long dstn = dst->width * dst->height;
    unsigned long dstm2 = uvp_size(dst, dstfmt) * 2;
    dst->datalen = dstn + dstm2;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    if(dstm2)
        memset((void*)dst->data + dstn, 0x80, dstm2);
    uint8_t *dsty = (void*)dst->data;

    assert(src->datalen >= (src->width * src->height * srcfmt->p.rgb.bpp));
    const uint8_t *srcp = src->data;

    int rbits = RGB_SIZE(srcfmt->p.rgb.red);
    int rbit0 = RGB_OFFSET(srcfmt->p.rgb.red);
    int gbits = RGB_SIZE(srcfmt->p.rgb.green);
    int gbit0 = RGB_OFFSET(srcfmt->p.rgb.green);
    int bbits = RGB_SIZE(srcfmt->p.rgb.blue);
    int bbit0 = RGB_OFFSET(srcfmt->p.rgb.blue);

    unsigned srcl = src->width * srcfmt->p.rgb.bpp;
    unsigned x, y;
    uint16_t y0 = 0;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height)
            srcp -= srcl;
        for(x = 0; x < dst->width; x++) {
            if(x < src->width) {
                uint8_t r, g, b;
                uint32_t p = convert_read_rgb(srcp, srcfmt->p.rgb.bpp);
                srcp += srcfmt->p.rgb.bpp;

                /* FIXME endianness? */
                r = ((p >> rbit0) << rbits) & 0xff;
                g = ((p >> gbit0) << gbits) & 0xff;
                b = ((p >> bbit0) << bbits) & 0xff;

                /* FIXME color space? */
                y0 = ((77 * r + 150 * g + 29 * b) + 0x80) >> 8;
            }
            *(dsty++) = y0;
        }
        if(x < src->width)
            srcp += (src->width - x) * srcfmt->p.rgb.bpp;
    }
}

/* packed YUV to packed RGB */
static void convert_yuv_to_rgb (zbar_image_t *dst,
                                const zbar_format_def_t *dstfmt,
                                const zbar_image_t *src,
                                const zbar_format_def_t *srcfmt)
{
    unsigned long dstn = dst->width * dst->height;
    dst->datalen = dstn * dstfmt->p.rgb.bpp;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    uint8_t *dstp = (void*)dst->data;

    int drbits = RGB_SIZE(dstfmt->p.rgb.red);
    int drbit0 = RGB_OFFSET(dstfmt->p.rgb.red);
    int dgbits = RGB_SIZE(dstfmt->p.rgb.green);
    int dgbit0 = RGB_OFFSET(dstfmt->p.rgb.green);
    int dbbits = RGB_SIZE(dstfmt->p.rgb.blue);
    int dbbit0 = RGB_OFFSET(dstfmt->p.rgb.blue);

    assert(src->datalen >= (src->width * src->height +
                            uvp_size(src, srcfmt) * 2));
    const uint8_t *srcp = src->data;
    if(srcfmt->p.yuv.packorder & 2)
        srcp++;

    assert(srcfmt->p.yuv.xsub2 == 1);
    unsigned srcl = src->width + (src->width >> 1);
    unsigned x, y;
    uint32_t p = 0;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height)
            srcp -= srcl;
        for(x = 0; x < dst->width; x++) {
            if(x < src->width) {
                uint8_t y0 = *(srcp++);
                srcp++;

                if(y0 <= 16)
                    y0 = 0;
                else if(y0 >= 235)
                    y0 = 255;
                else
                    y0 = (uint16_t)(y0 - 16) * 255 / 219;

                p = (((y0 >> drbits) << drbit0) |
                     ((y0 >> dgbits) << dgbit0) |
                     ((y0 >> dbbits) << dbbit0));
            }
            convert_write_rgb(dstp, p, dstfmt->p.rgb.bpp);
            dstp += dstfmt->p.rgb.bpp;
        }
        if(x < src->width)
            srcp += (src->width - x) * 2;
    }
}

/* packed RGB to packed YUV
 * FIXME currently ignores color and grayscales the image
 */
static void convert_rgb_to_yuv (zbar_image_t *dst,
                                const zbar_format_def_t *dstfmt,
                                const zbar_image_t *src,
                                const zbar_format_def_t *srcfmt)
{
    uv_roundup(dst, dstfmt);
    dst->datalen = dst->width * dst->height + uvp_size(dst, dstfmt) * 2;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    uint8_t *dstp = (void*)dst->data;
    uint8_t flags = dstfmt->p.yuv.packorder & 2;

    assert(src->datalen >= (src->width * src->height * srcfmt->p.rgb.bpp));
    const uint8_t *srcp = src->data;

    int rbits = RGB_SIZE(srcfmt->p.rgb.red);
    int rbit0 = RGB_OFFSET(srcfmt->p.rgb.red);
    int gbits = RGB_SIZE(srcfmt->p.rgb.green);
    int gbit0 = RGB_OFFSET(srcfmt->p.rgb.green);
    int bbits = RGB_SIZE(srcfmt->p.rgb.blue);
    int bbit0 = RGB_OFFSET(srcfmt->p.rgb.blue);

    unsigned srcl = src->width * srcfmt->p.rgb.bpp;
    unsigned x, y;
    uint16_t y0 = 0;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height)
            srcp -= srcl;
        for(x = 0; x < dst->width; x++) {
            if(x < src->width) {
                uint8_t r, g, b;
                uint32_t p = convert_read_rgb(srcp, srcfmt->p.rgb.bpp);
                srcp += srcfmt->p.rgb.bpp;

                /* FIXME endianness? */
                r = ((p >> rbit0) << rbits) & 0xff;
                g = ((p >> gbit0) << gbits) & 0xff;
                b = ((p >> bbit0) << bbits) & 0xff;

                /* FIXME color space? */
                y0 = ((77 * r + 150 * g + 29 * b) + 0x80) >> 8;
            }
            if(flags) {
                *(dstp++) = 0x80;  *(dstp++) = y0;
            }
            else {
                *(dstp++) = y0;  *(dstp++) = 0x80;
            }
        }
        if(x < src->width)
            srcp += (src->width - x) * srcfmt->p.rgb.bpp;
    }
}

/* resample and resize packed RGB components */
static void convert_rgb_resample (zbar_image_t *dst,
                                  const zbar_format_def_t *dstfmt,
                                  const zbar_image_t *src,
                                  const zbar_format_def_t *srcfmt)
{
    unsigned long dstn = dst->width * dst->height;
    dst->datalen = dstn * dstfmt->p.rgb.bpp;
    dst->data = malloc(dst->datalen);
    if(!dst->data) return;
    uint8_t *dstp = (void*)dst->data;

    int drbits = RGB_SIZE(dstfmt->p.rgb.red);
    int drbit0 = RGB_OFFSET(dstfmt->p.rgb.red);
    int dgbits = RGB_SIZE(dstfmt->p.rgb.green);
    int dgbit0 = RGB_OFFSET(dstfmt->p.rgb.green);
    int dbbits = RGB_SIZE(dstfmt->p.rgb.blue);
    int dbbit0 = RGB_OFFSET(dstfmt->p.rgb.blue);

    assert(src->datalen >= (src->width * src->height * srcfmt->p.rgb.bpp));
    const uint8_t *srcp = src->data;

    int srbits = RGB_SIZE(srcfmt->p.rgb.red);
    int srbit0 = RGB_OFFSET(srcfmt->p.rgb.red);
    int sgbits = RGB_SIZE(srcfmt->p.rgb.green);
    int sgbit0 = RGB_OFFSET(srcfmt->p.rgb.green);
    int sbbits = RGB_SIZE(srcfmt->p.rgb.blue);
    int sbbit0 = RGB_OFFSET(srcfmt->p.rgb.blue);

    unsigned srcl = src->width * srcfmt->p.rgb.bpp;
    unsigned x, y;
    uint32_t p = 0;
    for(y = 0; y < dst->height; y++) {
        if(y >= src->height)
            y -= srcl;
        for(x = 0; x < dst->width; x++) {
            if(x < src->width) {
                uint8_t r, g, b;
                p = convert_read_rgb(srcp, srcfmt->p.rgb.bpp);
                srcp += srcfmt->p.rgb.bpp;

                /* FIXME endianness? */
                r = (p >> srbit0) << srbits;
                g = (p >> sgbit0) << sgbits;
                b = (p >> sbbit0) << sbbits;

                p = (((r >> drbits) << drbit0) |
                     ((g >> dgbits) << dgbit0) |
                     ((b >> dbbits) << dbbit0));
            }
            convert_write_rgb(dstp, p, dstfmt->p.rgb.bpp);
            dstp += dstfmt->p.rgb.bpp;
        }
        if(x < src->width)
            srcp += (src->width - x) * srcfmt->p.rgb.bpp;
    }
}

#ifdef HAVE_LIBJPEG
void _zbar_convert_jpeg_to_y(zbar_image_t *dst,
                              const zbar_format_def_t *dstfmt,
                              const zbar_image_t *src,
                              const zbar_format_def_t *srcfmt);

static void convert_jpeg(zbar_image_t *dst,
                         const zbar_format_def_t *dstfmt,
                         const zbar_image_t *src,
                         const zbar_format_def_t *srcfmt);
#endif

/* group conversion matrix */
static conversion_def_t conversions[][ZBAR_FMT_NUM] = {
    { /* *from* GRAY */
        {   0, convert_copy },           /* to GRAY */
        {   8, convert_uvp_append },     /* to YUV_PLANAR */
        {  24, convert_yuv_pack },       /* to YUV_PACKED */
        {  32, convert_yuvp_to_rgb },    /* to RGB_PACKED */
        {   8, convert_uvp_append },     /* to YUV_NV */
        {  -1, NULL },                   /* to JPEG */
    },
    { /* from YUV_PLANAR */
        {   1, convert_copy },           /* to GRAY */
        {  48, convert_uvp_resample },   /* to YUV_PLANAR */
        {  64, convert_yuv_pack },       /* to YUV_PACKED */
        { 128, convert_yuvp_to_rgb },    /* to RGB_PACKED */
        {  40, convert_uvp_append },     /* to YUV_NV */
        {  -1, NULL },                   /* to JPEG */
    },
    { /* from YUV_PACKED */
        {  24, convert_yuv_unpack },     /* to GRAY */
        {  52, convert_yuv_unpack },     /* to YUV_PLANAR */
        {  20, convert_uv_resample },    /* to YUV_PACKED */
        { 144, convert_yuv_to_rgb },     /* to RGB_PACKED */
        {  18, convert_yuv_unpack },     /* to YUV_NV */
        {  -1, NULL },                   /* to JPEG */
    },
    { /* from RGB_PACKED */
        { 112, convert_rgb_to_yuvp },    /* to GRAY */
        { 160, convert_rgb_to_yuvp },    /* to YUV_PLANAR */
        { 144, convert_rgb_to_yuv },     /* to YUV_PACKED */
        { 120, convert_rgb_resample },   /* to RGB_PACKED */
        { 152, convert_rgb_to_yuvp },    /* to YUV_NV */
        {  -1, NULL },                   /* to JPEG */
    },
    { /* from YUV_NV (FIXME treated as GRAY) */
        {   1, convert_copy },           /* to GRAY */
        {   8, convert_uvp_append },     /* to YUV_PLANAR */
        {  24, convert_yuv_pack },       /* to YUV_PACKED */
        {  32, convert_yuvp_to_rgb },    /* to RGB_PACKED */
        {   8, convert_uvp_append },     /* to YUV_NV */
        {  -1, NULL },                   /* to JPEG */
    },
#ifdef HAVE_LIBJPEG
    { /* from JPEG */
        {  96, _zbar_convert_jpeg_to_y }, /* to GRAY */
        { 104, convert_jpeg },           /* to YUV_PLANAR */
        { 116, convert_jpeg },           /* to YUV_PACKED */
        { 256, convert_jpeg },           /* to RGB_PACKED */
        { 104, convert_jpeg },           /* to YUV_NV */
        {  -1, NULL },                   /* to JPEG */
    },
#else
    { /* from JPEG */
        {  -1, NULL },                   /* to GRAY */
        {  -1, NULL },                   /* to YUV_PLANAR */
        {  -1, NULL },                   /* to YUV_PACKED */
        {  -1, NULL },                   /* to RGB_PACKED */
        {  -1, NULL },                   /* to YUV_NV */
        {  -1, NULL },                   /* to JPEG */
    },
#endif
};

const zbar_format_def_t *_zbar_format_lookup (uint32_t fmt)
{
    const zbar_format_def_t *def = NULL;
    int i = 0;
    while(i < num_format_defs) {
        def = &format_defs[i];
        if(fmt == def->format)
            return(def);
        i = i * 2 + 1;
        if(fmt > def->format)
            i++;
    }
    return(NULL);
}

#ifdef HAVE_LIBJPEG
/* convert JPEG data via an intermediate format supported by libjpeg */
static void convert_jpeg (zbar_image_t *dst,
                          const zbar_format_def_t *dstfmt,
                          const zbar_image_t *src,
                          const zbar_format_def_t *srcfmt)
{
    /* define intermediate image in a format supported by libjpeg
     * (currently only grayscale)
     */
    zbar_image_t *tmp;
    if(!src->src) {
        tmp = zbar_image_create();
        tmp->format = fourcc('Y','8','0','0');
        tmp->width = dst->width;
        tmp->height = dst->height;
    }
    else {
        tmp = src->src->jpeg_img;
        assert(tmp);
        dst->width = tmp->width;
        dst->height = tmp->height;
    }

    const zbar_format_def_t *tmpfmt = _zbar_format_lookup(tmp->format);
    assert(tmpfmt);

    /* convert to intermediate format */
    _zbar_convert_jpeg_to_y(tmp, tmpfmt, src, srcfmt);

    /* now convert to dst */
    dst->width = tmp->width;
    dst->height = tmp->height;

    conversion_handler_t *func =
        conversions[tmpfmt->group][dstfmt->group].func;

    func(dst, dstfmt, tmp, tmpfmt);

    if(!src->src)
        zbar_image_destroy(tmp);
}
#endif

zbar_image_t *zbar_image_convert_resize (const zbar_image_t *src,
                                         unsigned long fmt,
                                         unsigned width,
                                         unsigned height)
{
    zbar_image_t *dst = zbar_image_create();
    dst->format = fmt;
    dst->width = width;
    dst->height = height;
    if(src->format == fmt &&
       src->width == width &&
       src->height == height) {
        convert_copy(dst, NULL, src, NULL);
        return(dst);
    }

    const zbar_format_def_t *srcfmt = _zbar_format_lookup(src->format);
    const zbar_format_def_t *dstfmt = _zbar_format_lookup(dst->format);
    if(!srcfmt || !dstfmt)
        /* FIXME free dst */
        return(NULL);

    if(srcfmt->group == dstfmt->group &&
       srcfmt->p.cmp == dstfmt->p.cmp &&
       src->width == width &&
       src->height == height) {
        convert_copy(dst, NULL, src, NULL);
        return(dst);
    }

    conversion_handler_t *func =
        conversions[srcfmt->group][dstfmt->group].func;

    dst->cleanup = zbar_image_free_data;
    func(dst, dstfmt, src, srcfmt);
    if(!dst->data) {
        /* conversion failed */
        zbar_image_destroy(dst);
        return(NULL);
    }
    return(dst);
}

zbar_image_t *zbar_image_convert (const zbar_image_t *src,
                                  unsigned long fmt)
{
    return(zbar_image_convert_resize(src, fmt, src->width, src->height));
}

static inline int has_format (uint32_t fmt,
                              const uint32_t *fmts)
{
    for(; *fmts; fmts++)
        if(*fmts == fmt)
            return(1);
    return(0);
}

/* select least cost conversion from src format to available dsts */
int _zbar_best_format (uint32_t src,
                       uint32_t *dst,
                       const uint32_t *dsts)
{
    if(dst)
        *dst = 0;
    if(!dsts)
        return(-1);
    if(has_format(src, dsts)) {
        zprintf(8, "shared format: %4.4s\n", (char*)&src);
        if(dst)
            *dst = src;
        return(0);
    }
    const zbar_format_def_t *srcfmt = _zbar_format_lookup(src);
    if(!srcfmt)
        return(-1);

    zprintf(8, "from %.4s(%08" PRIx32 ") to", (char*)&src, src);
    unsigned min_cost = -1;
    for(; *dsts; dsts++) {
        const zbar_format_def_t *dstfmt = _zbar_format_lookup(*dsts);
        if(!dstfmt)
            continue;
        int cost;
        if(srcfmt->group == dstfmt->group &&
           srcfmt->p.cmp == dstfmt->p.cmp)
            cost = 0;
        else
            cost = conversions[srcfmt->group][dstfmt->group].cost;

        if(_zbar_verbosity >= 8)
            fprintf(stderr, " %.4s(%08" PRIx32 ")=%d",
                    (char*)dsts, *dsts, cost);
        if(cost >= 0 && min_cost > cost) {
            min_cost = cost;
            if(dst)
                *dst = *dsts;
        }
    }
    if(_zbar_verbosity >= 8)
        fprintf(stderr, "\n");
    return(min_cost);
}

int zbar_negotiate_format (zbar_video_t *vdo,
                           zbar_window_t *win)
{
    if(!vdo && !win)
        return(0);

    if(win)
        (void)window_lock(win);

    errinfo_t *errdst = (vdo) ? &vdo->err : &win->err;
    if(verify_format_sort()) {
        if(win)
            (void)window_unlock(win);
        return(err_capture(errdst, SEV_FATAL, ZBAR_ERR_INTERNAL, __func__,
                           "image format list is not sorted!?"));
    }

    if((vdo && !vdo->formats) || (win && !win->formats)) {
        if(win)
            (void)window_unlock(win);
        return(err_capture(errdst, SEV_ERROR, ZBAR_ERR_UNSUPPORTED, __func__,
                           "no input or output formats available"));
    }

    static const uint32_t y800[2] = { fourcc('Y','8','0','0'), 0 };
    const uint32_t *srcs = (vdo) ? vdo->formats : y800;
    const uint32_t *dsts = (win) ? win->formats : y800;

    unsigned min_cost = -1;
    uint32_t min_fmt = 0;
    const uint32_t *fmt;
    for(fmt = _zbar_formats; *fmt; fmt++) {
        /* only consider formats supported by video device */
        if(!has_format(*fmt, srcs))
            continue;
        uint32_t win_fmt = 0;
        int cost = _zbar_best_format(*fmt, &win_fmt, dsts);
        if(cost < 0) {
            zprintf(4, "%.4s(%08" PRIx32 ") -> ? (unsupported)\n",
                    (char*)fmt, *fmt);
            continue;
        }
        zprintf(4, "%.4s(%08" PRIx32 ") -> %.4s(%08" PRIx32 ") (%d)\n",
                (char*)fmt, *fmt, (char*)&win_fmt, win_fmt, cost);
        if(min_cost > cost) {
            min_cost = cost;
            min_fmt = *fmt;
            if(!cost)
                break;
        }
    }
    if(win)
        (void)window_unlock(win);

    if(!min_fmt)
        return(err_capture(errdst, SEV_ERROR, ZBAR_ERR_UNSUPPORTED, __func__,
                           "no supported image formats available"));
    if(!vdo)
        return(0);

    zprintf(2, "setting best format %.4s(%08" PRIx32 ") (%d)\n",
            (char*)&min_fmt, min_fmt, min_cost);
    return(zbar_video_init(vdo, min_fmt));
}
