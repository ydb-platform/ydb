/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
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
#include "bitstream.h"
#include "param.h"

#include "encoder.h"
#include "entropy.h"
#include "level.h"
#include "nal.h"
#include "bitcost.h"

/* multilib namespace reflectors */
#if LINKED_8BIT
namespace x265_8bit {
const x265_api* x265_api_get(int bitDepth);
const x265_api* x265_api_query(int bitDepth, int apiVersion, int* err);
}
#endif

#if LINKED_10BIT
namespace x265_10bit {
const x265_api* x265_api_get(int bitDepth);
const x265_api* x265_api_query(int bitDepth, int apiVersion, int* err);
}
#endif

#if LINKED_12BIT
namespace x265_12bit {
const x265_api* x265_api_get(int bitDepth);
const x265_api* x265_api_query(int bitDepth, int apiVersion, int* err);
}
#endif

#if EXPORT_C_API
/* these functions are exported as C functions (default) */
using namespace X265_NS;
extern "C" {
#else
/* these functions exist within private namespace (multilib) */
namespace X265_NS {
#endif

static const char* summaryCSVHeader =
    "Command, Date/Time, Elapsed Time, FPS, Bitrate, "
    "Y PSNR, U PSNR, V PSNR, Global PSNR, SSIM, SSIM (dB), "
    "I count, I ave-QP, I kbps, I-PSNR Y, I-PSNR U, I-PSNR V, I-SSIM (dB), "
    "P count, P ave-QP, P kbps, P-PSNR Y, P-PSNR U, P-PSNR V, P-SSIM (dB), "
    "B count, B ave-QP, B kbps, B-PSNR Y, B-PSNR U, B-PSNR V, B-SSIM (dB), "
    "MaxCLL, MaxFALL, Version\n";

x265_encoder *x265_encoder_open(x265_param *p)
{
    if (!p)
        return NULL;

#if _MSC_VER
#pragma warning(disable: 4127) // conditional expression is constant, yes I know
#endif

#if HIGH_BIT_DEPTH
    if (X265_DEPTH != 10 && X265_DEPTH != 12)
#else
    if (X265_DEPTH != 8)
#endif
    {
        x265_log(p, X265_LOG_ERROR, "Build error, internal bit depth mismatch\n");
        return NULL;
    }

    Encoder* encoder = NULL;
    x265_param* param = PARAM_NS::x265_param_alloc();
    x265_param* latestParam = PARAM_NS::x265_param_alloc();
    if (!param || !latestParam)
        goto fail;

    memcpy(param, p, sizeof(x265_param));
    x265_log(param, X265_LOG_INFO, "HEVC encoder version %s\n", PFX(version_str));
    x265_log(param, X265_LOG_INFO, "build info %s\n", PFX(build_info_str));

    x265_setup_primitives(param);

    if (x265_check_params(param))
        goto fail;

    encoder = new Encoder;
    if (!param->rc.bEnableSlowFirstPass)
        PARAM_NS::x265_param_apply_fastfirstpass(param);

    // may change params for auto-detect, etc
    encoder->configure(param);
    // may change rate control and CPB params
    if (!enforceLevel(*param, encoder->m_vps))
        goto fail;

    // will detect and set profile/tier/level in VPS
    determineLevel(*param, encoder->m_vps);

    if (!param->bAllowNonConformance && encoder->m_vps.ptl.profileIdc == Profile::NONE)
    {
        x265_log(param, X265_LOG_INFO, "non-conformant bitstreams not allowed (--allow-non-conformance)\n");
        goto fail;
    }

    encoder->create();
    /* Try to open CSV file handle */
    if (encoder->m_param->csvfn)
    {
        encoder->m_param->csvfpt = x265_csvlog_open(encoder->m_param);
        if (!encoder->m_param->csvfpt)
        {
            x265_log(encoder->m_param, X265_LOG_ERROR, "Unable to open CSV log file <%s>, aborting\n", encoder->m_param->csvfn);
            encoder->m_aborted = true;
        }
    }

    encoder->m_latestParam = latestParam;
    memcpy(latestParam, param, sizeof(x265_param));
    if (encoder->m_aborted)
        goto fail;

    x265_print_params(param);
    return encoder;

fail:
    delete encoder;
    PARAM_NS::x265_param_free(param);
    PARAM_NS::x265_param_free(latestParam);
    return NULL;
}

int x265_encoder_headers(x265_encoder *enc, x265_nal **pp_nal, uint32_t *pi_nal)
{
    if (pp_nal && enc)
    {
        Encoder *encoder = static_cast<Encoder*>(enc);
        Entropy sbacCoder;
        Bitstream bs;
        if (encoder->m_param->rc.bStatRead && encoder->m_param->bMultiPassOptRPS)
        {
            if (!encoder->computeSPSRPSIndex())
            {
                encoder->m_aborted = true;
                return -1;
            }
        }
        encoder->getStreamHeaders(encoder->m_nalList, sbacCoder, bs);
        *pp_nal = &encoder->m_nalList.m_nal[0];
        if (pi_nal) *pi_nal = encoder->m_nalList.m_numNal;
        return encoder->m_nalList.m_occupancy;
    }

    if (enc)
    {
        Encoder *encoder = static_cast<Encoder*>(enc);
        encoder->m_aborted = true;
    }
    return -1;
}

void x265_encoder_parameters(x265_encoder *enc, x265_param *out)
{
    if (enc && out)
    {
        Encoder *encoder = static_cast<Encoder*>(enc);
        memcpy(out, encoder->m_param, sizeof(x265_param));
    }
}

int x265_encoder_reconfig(x265_encoder* enc, x265_param* param_in)
{
    if (!enc || !param_in)
        return -1;

    x265_param save;
    Encoder* encoder = static_cast<Encoder*>(enc);
    if (encoder->m_latestParam->forceFlush != param_in->forceFlush)
        return encoder->reconfigureParam(encoder->m_latestParam, param_in);
    bool isReconfigureRc = encoder->isReconfigureRc(encoder->m_latestParam, param_in);
    if ((encoder->m_reconfigure && !isReconfigureRc) || (encoder->m_reconfigureRc && isReconfigureRc)) /* Reconfigure in progress */
        return 1;
    memcpy(&save, encoder->m_latestParam, sizeof(x265_param));
    int ret = encoder->reconfigureParam(encoder->m_latestParam, param_in);
    if (ret)
    {
        /* reconfigure failed, recover saved param set */
        memcpy(encoder->m_latestParam, &save, sizeof(x265_param));
        ret = -1;
    }
    else
    {
        if (encoder->m_latestParam->scalingLists && encoder->m_latestParam->scalingLists != encoder->m_param->scalingLists)
        {
            if (encoder->m_param->bRepeatHeaders)
            {
                if (encoder->m_scalingList.parseScalingList(encoder->m_latestParam->scalingLists))
                {
                    memcpy(encoder->m_latestParam, &save, sizeof(x265_param));
                    return -1;
                }
                encoder->m_scalingList.setupQuantMatrices(encoder->m_param->internalCsp);
            }
            else
            {
                x265_log(encoder->m_param, X265_LOG_ERROR, "Repeat headers is turned OFF, cannot reconfigure scalinglists\n");
                memcpy(encoder->m_latestParam, &save, sizeof(x265_param));
                return -1;
            }
        }
        if (!isReconfigureRc)
            encoder->m_reconfigure = true;
        else if (encoder->m_reconfigureRc)
        {
            VPS saveVPS;
            memcpy(&saveVPS.ptl, &encoder->m_vps.ptl, sizeof(saveVPS.ptl));
            determineLevel(*encoder->m_latestParam, encoder->m_vps);
            if (saveVPS.ptl.profileIdc != encoder->m_vps.ptl.profileIdc || saveVPS.ptl.levelIdc != encoder->m_vps.ptl.levelIdc
                || saveVPS.ptl.tierFlag != encoder->m_vps.ptl.tierFlag)
            {
                x265_log(encoder->m_param, X265_LOG_WARNING, "Profile/Level/Tier has changed from %d/%d/%s to %d/%d/%s.Cannot reconfigure rate-control.\n",
                         saveVPS.ptl.profileIdc, saveVPS.ptl.levelIdc, saveVPS.ptl.tierFlag ? "High" : "Main", encoder->m_vps.ptl.profileIdc,
                         encoder->m_vps.ptl.levelIdc, encoder->m_vps.ptl.tierFlag ? "High" : "Main");
                memcpy(encoder->m_latestParam, &save, sizeof(x265_param));
                memcpy(&encoder->m_vps.ptl, &saveVPS.ptl, sizeof(saveVPS.ptl));
                encoder->m_reconfigureRc = false;
            }
        }
        encoder->printReconfigureParams();
    }
    return ret;
}

int x265_encoder_encode(x265_encoder *enc, x265_nal **pp_nal, uint32_t *pi_nal, x265_picture *pic_in, x265_picture *pic_out)
{
    if (!enc)
        return -1;

    Encoder *encoder = static_cast<Encoder*>(enc);
    int numEncoded;

    // While flushing, we cannot return 0 until the entire stream is flushed
    do
    {
        numEncoded = encoder->encode(pic_in, pic_out);
    }
    while ((numEncoded == 0 && !pic_in && encoder->m_numDelayedPic && !encoder->m_latestParam->forceFlush) && !encoder->m_externalFlush);
    if (numEncoded)
        encoder->m_externalFlush = false;

    // do not allow reuse of these buffers for more than one picture. The
    // encoder now owns these analysisData buffers.
    if (pic_in)
    {
        pic_in->analysisData.wt = NULL;
        pic_in->analysisData.intraData = NULL;
        pic_in->analysisData.interData = NULL;
        pic_in->analysis2Pass.analysisFramedata = NULL;
    }

    if (pp_nal && numEncoded > 0)
    {
        *pp_nal = &encoder->m_nalList.m_nal[0];
        if (pi_nal) *pi_nal = encoder->m_nalList.m_numNal;
    }
    else if (pi_nal)
        *pi_nal = 0;

    if (numEncoded && encoder->m_param->csvLogLevel)
        x265_csvlog_frame(encoder->m_param, pic_out);

    if (numEncoded < 0)
        encoder->m_aborted = true;

    return numEncoded;
}

void x265_encoder_get_stats(x265_encoder *enc, x265_stats *outputStats, uint32_t statsSizeBytes)
{
    if (enc && outputStats)
    {
        Encoder *encoder = static_cast<Encoder*>(enc);
        encoder->fetchStats(outputStats, statsSizeBytes);
    }
}

void x265_encoder_log(x265_encoder* enc, int argc, char **argv)
{
    if (enc)
    {
        Encoder *encoder = static_cast<Encoder*>(enc);
        x265_stats stats;
        encoder->fetchStats(&stats, sizeof(stats));
        x265_csvlog_encode(enc, &stats, argc, argv);
    }
}

void x265_encoder_close(x265_encoder *enc)
{
    if (enc)
    {
        Encoder *encoder = static_cast<Encoder*>(enc);

        encoder->stopJobs();
        encoder->printSummary();
        encoder->destroy();
        delete encoder;
    }
}

int x265_encoder_intra_refresh(x265_encoder *enc)
{
    if (!enc)
        return -1;

    Encoder *encoder = static_cast<Encoder*>(enc);
    encoder->m_bQueuedIntraRefresh = 1;
    return 0;
}
int x265_encoder_ctu_info(x265_encoder *enc, int poc, x265_ctu_info_t** ctu)
{
    if (!ctu || !enc)
        return -1;
    Encoder* encoder = static_cast<Encoder*>(enc);
    encoder->copyCtuInfo(ctu, poc);
    return 0;
}

int x265_get_slicetype_poc_and_scenecut(x265_encoder *enc, int *slicetype, int *poc, int *sceneCut)
{
    if (!enc)
        return -1;
    Encoder *encoder = static_cast<Encoder*>(enc);
    if (!encoder->copySlicetypePocAndSceneCut(slicetype, poc, sceneCut))
        return 0;
    return -1;
}

int x265_get_ref_frame_list(x265_encoder *enc, x265_picyuv** l0, x265_picyuv** l1, int sliceType, int poc)
{
    if (!enc)
        return -1;

    Encoder *encoder = static_cast<Encoder*>(enc);
    return encoder->getRefFrameList((PicYuv**)l0, (PicYuv**)l1, sliceType, poc);
}

int x265_set_analysis_data(x265_encoder *enc, x265_analysis_data *analysis_data, int poc, uint32_t cuBytes)
{
    if (!enc)
        return -1;

    Encoder *encoder = static_cast<Encoder*>(enc);
    if (!encoder->setAnalysisData(analysis_data, poc, cuBytes))
        return 0;

    return -1;
}

void x265_cleanup(void)
{
    BitCost::destroy();
}

x265_picture *x265_picture_alloc()
{
    return (x265_picture*)x265_malloc(sizeof(x265_picture));
}

void x265_picture_init(x265_param *param, x265_picture *pic)
{
    memset(pic, 0, sizeof(x265_picture));

    pic->bitDepth = param->internalBitDepth;
    pic->colorSpace = param->internalCsp;
    pic->forceqp = X265_QP_AUTO;
    pic->quantOffsets = NULL;
    pic->userSEI.payloads = NULL;
    pic->userSEI.numPayloads = 0;

    if (param->analysisReuseMode || (param->bMVType == AVC_INFO))
    {
        uint32_t widthInCU = (param->sourceWidth + param->maxCUSize - 1) >> param->maxLog2CUSize;
        uint32_t heightInCU = (param->sourceHeight + param->maxCUSize - 1) >> param->maxLog2CUSize;

        uint32_t numCUsInFrame   = widthInCU * heightInCU;
        pic->analysisData.numCUsInFrame = numCUsInFrame;
        pic->analysisData.numPartitions = param->num4x4Partitions;
    }
}

void x265_picture_free(x265_picture *p)
{
    return x265_free(p);
}

static const x265_api libapi =
{
    X265_MAJOR_VERSION,
    X265_BUILD,
    sizeof(x265_param),
    sizeof(x265_picture),
    sizeof(x265_analysis_data),
    sizeof(x265_zone),
    sizeof(x265_stats),

    PFX(max_bit_depth),
    PFX(version_str),
    PFX(build_info_str),

    &PARAM_NS::x265_param_alloc,
    &PARAM_NS::x265_param_free,
    &PARAM_NS::x265_param_default,
    &PARAM_NS::x265_param_parse,
    &PARAM_NS::x265_param_apply_profile,
    &PARAM_NS::x265_param_default_preset,
    &x265_picture_alloc,
    &x265_picture_free,
    &x265_picture_init,
    &x265_encoder_open,
    &x265_encoder_parameters,
    &x265_encoder_reconfig,
    &x265_encoder_headers,
    &x265_encoder_encode,
    &x265_encoder_get_stats,
    &x265_encoder_log,
    &x265_encoder_close,
    &x265_cleanup,

    sizeof(x265_frame_stats),
    &x265_encoder_intra_refresh,
    &x265_encoder_ctu_info,
    &x265_get_slicetype_poc_and_scenecut,
    &x265_get_ref_frame_list,
    &x265_csvlog_open,
    &x265_csvlog_frame,
    &x265_csvlog_encode,
    &x265_dither_image,
    &x265_set_analysis_data
};

typedef const x265_api* (*api_get_func)(int bitDepth);
typedef const x265_api* (*api_query_func)(int bitDepth, int apiVersion, int* err);

#define xstr(s) str(s)
#define str(s) #s

#if _WIN32
#define ext ".dll"
#elif MACOS
#include <dlfcn.h>
#define ext ".dylib"
#else
#include <dlfcn.h>
#define ext ".so"
#endif

static int g_recursion /* = 0 */;

const x265_api* x265_api_get(int bitDepth)
{
    if (bitDepth && bitDepth != X265_DEPTH)
    {
#if LINKED_8BIT
        if (bitDepth == 8) return x265_8bit::x265_api_get(0);
#endif
#if LINKED_10BIT
        if (bitDepth == 10) return x265_10bit::x265_api_get(0);
#endif
#if LINKED_12BIT
        if (bitDepth == 12) return x265_12bit::x265_api_get(0);
#endif

        const char* libname = NULL;
        const char* method = "x265_api_get_" xstr(X265_BUILD);
        const char* multilibname = "libx265" ext;

        if (bitDepth == 12)
            libname = "libx265_main12" ext;
        else if (bitDepth == 10)
            libname = "libx265_main10" ext;
        else if (bitDepth == 8)
            libname = "libx265_main" ext;
        else
            return NULL;

        const x265_api* api = NULL;
        int reqDepth = 0;

        if (g_recursion > 1)
            return NULL;
        else
            g_recursion++;

#if _WIN32
        HMODULE h = LoadLibraryA(libname);
        if (!h)
        {
            h = LoadLibraryA(multilibname);
            reqDepth = bitDepth;
        }
        if (h)
        {
            api_get_func get = (api_get_func)GetProcAddress(h, method);
            if (get)
                api = get(reqDepth);
        }
#else
        void* h = dlopen(libname, RTLD_LAZY | RTLD_LOCAL);
        if (!h)
        {
            h = dlopen(multilibname, RTLD_LAZY | RTLD_LOCAL);
            reqDepth = bitDepth;
        }
        if (h)
        {
            api_get_func get = (api_get_func)dlsym(h, method);
            if (get)
                api = get(reqDepth);
        }
#endif

        g_recursion--;

        if (api && bitDepth != api->bit_depth)
        {
            x265_log(NULL, X265_LOG_WARNING, "%s does not support requested bitDepth %d\n", libname, bitDepth);
            return NULL;
        }

        return api;
    }

    return &libapi;
}

const x265_api* x265_api_query(int bitDepth, int apiVersion, int* err)
{
    if (apiVersion < 51)
    {
        /* builds before 1.6 had re-ordered public structs */
        if (err) *err = X265_API_QUERY_ERR_VER_REFUSED;
        return NULL;
    }

    if (err) *err = X265_API_QUERY_ERR_NONE;

    if (bitDepth && bitDepth != X265_DEPTH)
    {
#if LINKED_8BIT
        if (bitDepth == 8) return x265_8bit::x265_api_query(0, apiVersion, err);
#endif
#if LINKED_10BIT
        if (bitDepth == 10) return x265_10bit::x265_api_query(0, apiVersion, err);
#endif
#if LINKED_12BIT
        if (bitDepth == 12) return x265_12bit::x265_api_query(0, apiVersion, err);
#endif

        const char* libname = NULL;
        const char* method = "x265_api_query";
        const char* multilibname = "libx265" ext;

        if (bitDepth == 12)
            libname = "libx265_main12" ext;
        else if (bitDepth == 10)
            libname = "libx265_main10" ext;
        else if (bitDepth == 8)
            libname = "libx265_main" ext;
        else
        {
            if (err) *err = X265_API_QUERY_ERR_LIB_NOT_FOUND;
            return NULL;
        }

        const x265_api* api = NULL;
        int reqDepth = 0;
        int e = X265_API_QUERY_ERR_LIB_NOT_FOUND;

        if (g_recursion > 1)
        {
            if (err) *err = X265_API_QUERY_ERR_LIB_NOT_FOUND;
            return NULL;
        }
        else
            g_recursion++;

#if _WIN32
        HMODULE h = LoadLibraryA(libname);
        if (!h)
        {
            h = LoadLibraryA(multilibname);
            reqDepth = bitDepth;
        }
        if (h)
        {
            e = X265_API_QUERY_ERR_FUNC_NOT_FOUND;
            api_query_func query = (api_query_func)GetProcAddress(h, method);
            if (query)
                api = query(reqDepth, apiVersion, err);
        }
#else
        void* h = dlopen(libname, RTLD_LAZY | RTLD_LOCAL);
        if (!h)
        {
            h = dlopen(multilibname, RTLD_LAZY | RTLD_LOCAL);
            reqDepth = bitDepth;
        }
        if (h)
        {
            e = X265_API_QUERY_ERR_FUNC_NOT_FOUND;
            api_query_func query = (api_query_func)dlsym(h, method);
            if (query)
                api = query(reqDepth, apiVersion, err);
        }
#endif

        g_recursion--;

        if (api && bitDepth != api->bit_depth)
        {
            x265_log(NULL, X265_LOG_WARNING, "%s does not support requested bitDepth %d\n", libname, bitDepth);
            if (err) *err = X265_API_QUERY_ERR_WRONG_BITDEPTH;
            return NULL;
        }

        if (err) *err = api ? X265_API_QUERY_ERR_NONE : e;
        return api;
    }

    return &libapi;
}

FILE* x265_csvlog_open(const x265_param* param)
{
    FILE *csvfp = x265_fopen(param->csvfn, "r");
    if (csvfp)
    {
        /* file already exists, re-open for append */
        fclose(csvfp);
        return x265_fopen(param->csvfn, "ab");
    }
    else
    {
        /* new CSV file, write header */
        csvfp = x265_fopen(param->csvfn, "wb");
        if (csvfp)
        {
            if (param->csvLogLevel)
            {
                fprintf(csvfp, "Encode Order, Type, POC, QP, Bits, Scenecut, ");
                if (param->csvLogLevel >= 2)
                    fprintf(csvfp, "I/P cost ratio, ");
                if (param->rc.rateControlMode == X265_RC_CRF)
                    fprintf(csvfp, "RateFactor, ");
                if (param->rc.vbvBufferSize)
                    fprintf(csvfp, "BufferFill, ");
                if (param->bEnablePsnr)
                    fprintf(csvfp, "Y PSNR, U PSNR, V PSNR, YUV PSNR, ");
                if (param->bEnableSsim)
                    fprintf(csvfp, "SSIM, SSIM(dB), ");
                fprintf(csvfp, "Latency, ");
                fprintf(csvfp, "List 0, List 1");
                uint32_t size = param->maxCUSize;
                for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
                {
                    fprintf(csvfp, ", Intra %dx%d DC, Intra %dx%d Planar, Intra %dx%d Ang", size, size, size, size, size, size);
                    size /= 2;
                }
                fprintf(csvfp, ", 4x4");
                size = param->maxCUSize;
                if (param->bEnableRectInter)
                {
                    for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
                    {
                        fprintf(csvfp, ", Inter %dx%d, Inter %dx%d (Rect)", size, size, size, size);
                        if (param->bEnableAMP)
                            fprintf(csvfp, ", Inter %dx%d (Amp)", size, size);
                        size /= 2;
                    }
                }
                else
                {
                    for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
                    {
                        fprintf(csvfp, ", Inter %dx%d", size, size);
                        size /= 2;
                    }
                }
                size = param->maxCUSize;
                for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
                {
                    fprintf(csvfp, ", Skip %dx%d", size, size);
                    size /= 2;
                }
                size = param->maxCUSize;
                for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
                {
                    fprintf(csvfp, ", Merge %dx%d", size, size);
                    size /= 2;
                }

                if (param->csvLogLevel >= 2)
                {
                    fprintf(csvfp, ", Avg Luma Distortion, Avg Chroma Distortion, Avg psyEnergy, Avg Residual Energy,"
                        " Min Luma Level, Max Luma Level, Avg Luma Level");

                    if (param->internalCsp != X265_CSP_I400)
                        fprintf(csvfp, ", Min Cb Level, Max Cb Level, Avg Cb Level, Min Cr Level, Max Cr Level, Avg Cr Level");

                    /* PU statistics */
                    size = param->maxCUSize;
                    for (uint32_t i = 0; i< param->maxLog2CUSize - (uint32_t)g_log2Size[param->minCUSize] + 1; i++)
                    {
                        fprintf(csvfp, ", Intra %dx%d", size, size);
                        fprintf(csvfp, ", Skip %dx%d", size, size);
                        fprintf(csvfp, ", AMP %d", size);
                        fprintf(csvfp, ", Inter %dx%d", size, size);
                        fprintf(csvfp, ", Merge %dx%d", size, size);
                        fprintf(csvfp, ", Inter %dx%d", size, size / 2);
                        fprintf(csvfp, ", Merge %dx%d", size, size / 2);
                        fprintf(csvfp, ", Inter %dx%d", size / 2, size);
                        fprintf(csvfp, ", Merge %dx%d", size / 2, size);
                        size /= 2;
                    }

                    if ((uint32_t)g_log2Size[param->minCUSize] == 3)
                        fprintf(csvfp, ", 4x4");

                    /* detailed performance statistics */
                    fprintf(csvfp, ", DecideWait (ms), Row0Wait (ms), Wall time (ms), Ref Wait Wall (ms), Total CTU time (ms),"
                        "Stall Time (ms), Total frame time (ms), Avg WPP, Row Blocks");
                }
                fprintf(csvfp, "\n");
            }
            else
                fputs(summaryCSVHeader, csvfp);
        }
        return csvfp;
    }
}

// per frame CSV logging
void x265_csvlog_frame(const x265_param* param, const x265_picture* pic)
{
    if (!param->csvfpt)
        return;

    const x265_frame_stats* frameStats = &pic->frameData;
    fprintf(param->csvfpt, "%d, %c-SLICE, %4d, %2.2lf, %10d, %d,", frameStats->encoderOrder, frameStats->sliceType, frameStats->poc,
                                                                   frameStats->qp, (int)frameStats->bits, frameStats->bScenecut);
    if (param->csvLogLevel >= 2)
        fprintf(param->csvfpt, "%.2f,", frameStats->ipCostRatio);
    if (param->rc.rateControlMode == X265_RC_CRF)
        fprintf(param->csvfpt, "%.3lf,", frameStats->rateFactor);
    if (param->rc.vbvBufferSize)
        fprintf(param->csvfpt, "%.3lf,", frameStats->bufferFill);
    if (param->bEnablePsnr)
        fprintf(param->csvfpt, "%.3lf, %.3lf, %.3lf, %.3lf,", frameStats->psnrY, frameStats->psnrU, frameStats->psnrV, frameStats->psnr);
    if (param->bEnableSsim)
        fprintf(param->csvfpt, " %.6f, %6.3f,", frameStats->ssim, x265_ssim2dB(frameStats->ssim));
    fprintf(param->csvfpt, "%d, ", frameStats->frameLatency);
    if (frameStats->sliceType == 'I' || frameStats->sliceType == 'i')
        fputs(" -, -,", param->csvfpt);
    else
    {
        int i = 0;
        while (frameStats->list0POC[i] != -1)
            fprintf(param->csvfpt, "%d ", frameStats->list0POC[i++]);
        fprintf(param->csvfpt, ",");
        if (frameStats->sliceType != 'P')
        {
            i = 0;
            while (frameStats->list1POC[i] != -1)
                fprintf(param->csvfpt, "%d ", frameStats->list1POC[i++]);
            fprintf(param->csvfpt, ",");
        }
        else
            fputs(" -,", param->csvfpt);
    }

    if (param->csvLogLevel)
    {
        for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
            fprintf(param->csvfpt, "%5.2lf%%, %5.2lf%%, %5.2lf%%,", frameStats->cuStats.percentIntraDistribution[depth][0],
                                                                    frameStats->cuStats.percentIntraDistribution[depth][1],
                                                                    frameStats->cuStats.percentIntraDistribution[depth][2]);
        fprintf(param->csvfpt, "%5.2lf%%", frameStats->cuStats.percentIntraNxN);
        if (param->bEnableRectInter)
        {
            for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
            {
                fprintf(param->csvfpt, ", %5.2lf%%, %5.2lf%%", frameStats->cuStats.percentInterDistribution[depth][0],
                                                               frameStats->cuStats.percentInterDistribution[depth][1]);
                if (param->bEnableAMP)
                    fprintf(param->csvfpt, ", %5.2lf%%", frameStats->cuStats.percentInterDistribution[depth][2]);
            }
        }
        else
        {
            for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
                fprintf(param->csvfpt, ", %5.2lf%%", frameStats->cuStats.percentInterDistribution[depth][0]);
        }
        for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
            fprintf(param->csvfpt, ", %5.2lf%%", frameStats->cuStats.percentSkipCu[depth]);
        for (uint32_t depth = 0; depth <= param->maxCUDepth; depth++)
            fprintf(param->csvfpt, ", %5.2lf%%", frameStats->cuStats.percentMergeCu[depth]);
    }

    if (param->csvLogLevel >= 2)
    {
        fprintf(param->csvfpt, ", %.2lf, %.2lf, %.2lf, %.2lf ", frameStats->avgLumaDistortion,
                                                                frameStats->avgChromaDistortion,
                                                                frameStats->avgPsyEnergy,
                                                                frameStats->avgResEnergy);

        fprintf(param->csvfpt, ", %d, %d, %.2lf", frameStats->minLumaLevel, frameStats->maxLumaLevel, frameStats->avgLumaLevel);

        if (param->internalCsp != X265_CSP_I400)
        {
            fprintf(param->csvfpt, ", %d, %d, %.2lf", frameStats->minChromaULevel, frameStats->maxChromaULevel, frameStats->avgChromaULevel);
            fprintf(param->csvfpt, ", %d, %d, %.2lf", frameStats->minChromaVLevel, frameStats->maxChromaVLevel, frameStats->avgChromaVLevel);
        }

        for (uint32_t i = 0; i < param->maxLog2CUSize - (uint32_t)g_log2Size[param->minCUSize] + 1; i++)
        {
            fprintf(param->csvfpt, ", %.2lf%%", frameStats->puStats.percentIntraPu[i]);
            fprintf(param->csvfpt, ", %.2lf%%", frameStats->puStats.percentSkipPu[i]);
            fprintf(param->csvfpt, ",%.2lf%%", frameStats->puStats.percentAmpPu[i]);
            for (uint32_t j = 0; j < 3; j++)
            {
                fprintf(param->csvfpt, ", %.2lf%%", frameStats->puStats.percentInterPu[i][j]);
                fprintf(param->csvfpt, ", %.2lf%%", frameStats->puStats.percentMergePu[i][j]);
            }
        }
        if ((uint32_t)g_log2Size[param->minCUSize] == 3)
            fprintf(param->csvfpt, ",%.2lf%%", frameStats->puStats.percentNxN);

        fprintf(param->csvfpt, ", %.1lf, %.1lf, %.1lf, %.1lf, %.1lf, %.1lf, %.1lf,", frameStats->decideWaitTime, frameStats->row0WaitTime,
                                                                                     frameStats->wallTime, frameStats->refWaitWallTime,
                                                                                     frameStats->totalCTUTime, frameStats->stallTime,
                                                                                     frameStats->totalFrameTime);

        fprintf(param->csvfpt, " %.3lf, %d", frameStats->avgWPP, frameStats->countRowBlocks);
    }
    fprintf(param->csvfpt, "\n");
    fflush(stderr);
}

void x265_csvlog_encode(x265_encoder *enc, const x265_stats* stats, int argc, char** argv)
{
    if (enc)
    {
        Encoder *encoder = static_cast<Encoder*>(enc);
        int padx = encoder->m_sps.conformanceWindow.rightOffset;
        int pady = encoder->m_sps.conformanceWindow.bottomOffset;
        const x265_api * api = x265_api_get(0);

        if (!encoder->m_param->csvfpt)
            return;

        if (encoder->m_param->csvLogLevel)
        {
            // adding summary to a per-frame csv log file, so it needs a summary header
            fprintf(encoder->m_param->csvfpt, "\nSummary\n");
            fputs(summaryCSVHeader, encoder->m_param->csvfpt);
        }

        // CLI arguments or other
        if (argc)
        {
            fputc('"', encoder->m_param->csvfpt);
            for (int i = 1; i < argc; i++)
            {
                fputc(' ', encoder->m_param->csvfpt);
                fputs(argv[i], encoder->m_param->csvfpt);
            }
            fputc('"', encoder->m_param->csvfpt);
        }
        else
        {
            const x265_param* paramTemp = encoder->m_param;
            char *opts = x265_param2string((x265_param*)paramTemp, padx, pady);
            if (opts)
            {
                fputc('"', encoder->m_param->csvfpt);
                fputs(opts, encoder->m_param->csvfpt);
                fputc('"', encoder->m_param->csvfpt);
            }
        }

        // current date and time
        time_t now;
        struct tm* timeinfo;
        time(&now);
        timeinfo = localtime(&now);
        char buffer[200];
        strftime(buffer, 128, "%c", timeinfo);
        fprintf(encoder->m_param->csvfpt, ", %s, ", buffer);

        // elapsed time, fps, bitrate
        fprintf(encoder->m_param->csvfpt, "%.2f, %.2f, %.2f,",
            stats->elapsedEncodeTime, stats->encodedPictureCount / stats->elapsedEncodeTime, stats->bitrate);

        if (encoder->m_param->bEnablePsnr)
            fprintf(encoder->m_param->csvfpt, " %.3lf, %.3lf, %.3lf, %.3lf,",
            stats->globalPsnrY / stats->encodedPictureCount, stats->globalPsnrU / stats->encodedPictureCount,
            stats->globalPsnrV / stats->encodedPictureCount, stats->globalPsnr);
        else
            fprintf(encoder->m_param->csvfpt, " -, -, -, -,");
        if (encoder->m_param->bEnableSsim)
            fprintf(encoder->m_param->csvfpt, " %.6f, %6.3f,", stats->globalSsim, x265_ssim2dB(stats->globalSsim));
        else
            fprintf(encoder->m_param->csvfpt, " -, -,");

        if (stats->statsI.numPics)
        {
            fprintf(encoder->m_param->csvfpt, " %-6u, %2.2lf, %-8.2lf,", stats->statsI.numPics, stats->statsI.avgQp, stats->statsI.bitrate);
            if (encoder->m_param->bEnablePsnr)
                fprintf(encoder->m_param->csvfpt, " %.3lf, %.3lf, %.3lf,", stats->statsI.psnrY, stats->statsI.psnrU, stats->statsI.psnrV);
            else
                fprintf(encoder->m_param->csvfpt, " -, -, -,");
            if (encoder->m_param->bEnableSsim)
                fprintf(encoder->m_param->csvfpt, " %.3lf,", stats->statsI.ssim);
            else
                fprintf(encoder->m_param->csvfpt, " -,");
        }
        else
            fprintf(encoder->m_param->csvfpt, " -, -, -, -, -, -, -,");

        if (stats->statsP.numPics)
        {
            fprintf(encoder->m_param->csvfpt, " %-6u, %2.2lf, %-8.2lf,", stats->statsP.numPics, stats->statsP.avgQp, stats->statsP.bitrate);
            if (encoder->m_param->bEnablePsnr)
                fprintf(encoder->m_param->csvfpt, " %.3lf, %.3lf, %.3lf,", stats->statsP.psnrY, stats->statsP.psnrU, stats->statsP.psnrV);
            else
                fprintf(encoder->m_param->csvfpt, " -, -, -,");
            if (encoder->m_param->bEnableSsim)
                fprintf(encoder->m_param->csvfpt, " %.3lf,", stats->statsP.ssim);
            else
                fprintf(encoder->m_param->csvfpt, " -,");
        }
        else
            fprintf(encoder->m_param->csvfpt, " -, -, -, -, -, -, -,");

        if (stats->statsB.numPics)
        {
            fprintf(encoder->m_param->csvfpt, " %-6u, %2.2lf, %-8.2lf,", stats->statsB.numPics, stats->statsB.avgQp, stats->statsB.bitrate);
            if (encoder->m_param->bEnablePsnr)
                fprintf(encoder->m_param->csvfpt, " %.3lf, %.3lf, %.3lf,", stats->statsB.psnrY, stats->statsB.psnrU, stats->statsB.psnrV);
            else
                fprintf(encoder->m_param->csvfpt, " -, -, -,");
            if (encoder->m_param->bEnableSsim)
                fprintf(encoder->m_param->csvfpt, " %.3lf,", stats->statsB.ssim);
            else
                fprintf(encoder->m_param->csvfpt, " -,");
        }
        else
            fprintf(encoder->m_param->csvfpt, " -, -, -, -, -, -, -,");

        fprintf(encoder->m_param->csvfpt, " %-6u, %-6u, %s\n", stats->maxCLL, stats->maxFALL, api->version_str);
    }
}

/* The dithering algorithm is based on Sierra-2-4A error diffusion.
 * We convert planes in place (without allocating a new buffer). */
static void ditherPlane(uint16_t *src, int srcStride, int width, int height, int16_t *errors, int bitDepth)
{
    const int lShift = 16 - bitDepth;
    const int rShift = 16 - bitDepth + 2;
    const int half = (1 << (16 - bitDepth + 1));
    const int pixelMax = (1 << bitDepth) - 1;

    memset(errors, 0, (width + 1) * sizeof(int16_t));

    if (bitDepth == 8)
    {
        for (int y = 0; y < height; y++, src += srcStride)
        {
            uint8_t* dst = (uint8_t *)src;
            int16_t err = 0;
            for (int x = 0; x < width; x++)
            {
                err = err * 2 + errors[x] + errors[x + 1];
                int tmpDst = x265_clip3(0, pixelMax, ((src[x] << 2) + err + half) >> rShift);
                errors[x] = err = (int16_t)(src[x] - (tmpDst << lShift));
                dst[x] = (uint8_t)tmpDst;
            }
        }
    }
    else
    {
        for (int y = 0; y < height; y++, src += srcStride)
        {
            int16_t err = 0;
            for (int x = 0; x < width; x++)
            {
                err = err * 2 + errors[x] + errors[x + 1];
                int tmpDst = x265_clip3(0, pixelMax, ((src[x] << 2) + err + half) >> rShift);
                errors[x] = err = (int16_t)(src[x] - (tmpDst << lShift));
                src[x] = (uint16_t)tmpDst;
            }
        }
    }
}

void x265_dither_image(x265_picture* picIn, int picWidth, int picHeight, int16_t *errorBuf, int bitDepth)
{
    const x265_api* api = x265_api_get(0);

    if (sizeof(x265_picture) != api->sizeof_picture)
    {
        fprintf(stderr, "extras [error]: structure size skew, unable to dither\n");
        return;
    }

    if (picIn->bitDepth <= 8)
    {
        fprintf(stderr, "extras [error]: dither support enabled only for input bitdepth > 8\n");
        return;
    }

    if (picIn->bitDepth == bitDepth)
    {
        fprintf(stderr, "extras[error]: dither support enabled only if encoder depth is different from picture depth\n");
        return;
    }

    /* This portion of code is from readFrame in x264. */
    for (int i = 0; i < x265_cli_csps[picIn->colorSpace].planes; i++)
    {
        if (picIn->bitDepth < 16)
        {
            /* upconvert non 16bit high depth planes to 16bit */
            uint16_t *plane = (uint16_t*)picIn->planes[i];
            uint32_t pixelCount = x265_picturePlaneSize(picIn->colorSpace, picWidth, picHeight, i);
            int lShift = 16 - picIn->bitDepth;

            /* This loop assumes width is equal to stride which
             * happens to be true for file reader outputs */
            for (uint32_t j = 0; j < pixelCount; j++)
                plane[j] = plane[j] << lShift;
        }

        int height = (int)(picHeight >> x265_cli_csps[picIn->colorSpace].height[i]);
        int width = (int)(picWidth >> x265_cli_csps[picIn->colorSpace].width[i]);

        ditherPlane(((uint16_t*)picIn->planes[i]), picIn->stride[i] / 2, width, height, errorBuf, bitDepth);
    }
}

} /* end namespace or extern "C" */
