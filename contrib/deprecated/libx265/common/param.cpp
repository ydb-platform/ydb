/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Deepthi Nandakumar <deepthi@multicorewareinc.com>
 *          Min Chen <min.chen@multicorewareinc.com>
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
#include "slice.h"
#include "threading.h"
#include "param.h"
#include "cpu.h"
#include "x265.h"

#if _MSC_VER
#pragma warning(disable: 4996) // POSIX functions are just fine, thanks
#pragma warning(disable: 4706) // assignment within conditional
#pragma warning(disable: 4127) // conditional expression is constant
#endif

#if _WIN32
#define strcasecmp _stricmp
#endif

#if !defined(HAVE_STRTOK_R)

/*
 * adapted from public domain strtok_r() by Charlie Gordon
 *
 *   from comp.lang.c  9/14/2007
 *
 *      http://groups.google.com/group/comp.lang.c/msg/2ab1ecbb86646684
 *
 *     (Declaration that it's public domain):
 *      http://groups.google.com/group/comp.lang.c/msg/7c7b39328fefab9c
 */

#undef strtok_r
static char* strtok_r(char* str, const char* delim, char** nextp)
{
    if (!str)
        str = *nextp;

    str += strspn(str, delim);

    if (!*str)
        return NULL;

    char *ret = str;

    str += strcspn(str, delim);

    if (*str)
        *str++ = '\0';

    *nextp = str;

    return ret;
}

#endif // if !defined(HAVE_STRTOK_R)

#if EXPORT_C_API

/* these functions are exported as C functions (default) */
using namespace X265_NS;
extern "C" {

#else

/* these functions exist within private namespace (multilib) */
namespace X265_NS {

#endif

x265_param *x265_param_alloc()
{
    return (x265_param*)x265_malloc(sizeof(x265_param));
}

void x265_param_free(x265_param* p)
{
    x265_free(p);
}

void x265_param_default(x265_param* param)
{
    memset(param, 0, sizeof(x265_param));

    /* Applying default values to all elements in the param structure */
    param->cpuid = X265_NS::cpu_detect();
    param->bEnableWavefront = 1;
    param->frameNumThreads = 0;

    param->logLevel = X265_LOG_INFO;
    param->csvLogLevel = 0;
    param->csvfn = NULL;
    param->rc.lambdaFileName = NULL;
    param->bLogCuStats = 0;
    param->decodedPictureHashSEI = 0;

    /* Quality Measurement Metrics */
    param->bEnablePsnr = 0;
    param->bEnableSsim = 0;

    /* Source specifications */
    param->internalBitDepth = X265_DEPTH;
    param->internalCsp = X265_CSP_I420;
    param->levelIdc = 0; //Auto-detect level
    param->uhdBluray = 0;
    param->bHighTier = 1; //Allow high tier by default
    param->interlaceMode = 0;
    param->bAnnexB = 1;
    param->bRepeatHeaders = 0;
    param->bEnableAccessUnitDelimiters = 0;
    param->bEmitHRDSEI = 0;
    param->bEmitInfoSEI = 1;
    param->bEmitHDRSEI = 0;

    /* CU definitions */
    param->maxCUSize = 64;
    param->minCUSize = 8;
    param->tuQTMaxInterDepth = 1;
    param->tuQTMaxIntraDepth = 1;
    param->maxTUSize = 32;

    /* Coding Structure */
    param->keyframeMin = 0;
    param->keyframeMax = 250;
    param->bOpenGOP = 1;
    param->bframes = 4;
    param->lookaheadDepth = 20;
    param->bFrameAdaptive = X265_B_ADAPT_TRELLIS;
    param->bBPyramid = 1;
    param->scenecutThreshold = 40; /* Magic number pulled in from x264 */
    param->lookaheadSlices = 8;
    param->lookaheadThreads = 0;
    param->scenecutBias = 5.0;
    /* Intra Coding Tools */
    param->bEnableConstrainedIntra = 0;
    param->bEnableStrongIntraSmoothing = 1;
    param->bEnableFastIntra = 0;
    param->bEnableSplitRdSkip = 0;

    /* Inter Coding tools */
    param->searchMethod = X265_HEX_SEARCH;
    param->subpelRefine = 2;
    param->searchRange = 57;
    param->maxNumMergeCand = 2;
    param->limitReferences = 3;
    param->limitModes = 0;
    param->bEnableWeightedPred = 1;
    param->bEnableWeightedBiPred = 0;
    param->bEnableEarlySkip = 0;
    param->bEnableRecursionSkip = 1;
    param->bEnableAMP = 0;
    param->bEnableRectInter = 0;
    param->rdLevel = 3;
    param->rdoqLevel = 0;
    param->bEnableSignHiding = 1;
    param->bEnableTransformSkip = 0;
    param->bEnableTSkipFast = 0;
    param->maxNumReferences = 3;
    param->bEnableTemporalMvp = 1;
    param->bSourceReferenceEstimation = 0;
    param->limitTU = 0;
    param->dynamicRd = 0;

    /* Loop Filter */
    param->bEnableLoopFilter = 1;

    /* SAO Loop Filter */
    param->bEnableSAO = 1;
    param->bSaoNonDeblocked = 0;
    param->bLimitSAO = 0;
    /* Coding Quality */
    param->cbQpOffset = 0;
    param->crQpOffset = 0;
    param->rdPenalty = 0;
    param->psyRd = 2.0;
    param->psyRdoq = 0.0;
    param->analysisReuseMode = 0;
    param->analysisMultiPassRefine = 0;
    param->analysisMultiPassDistortion = 0;
    param->analysisReuseFileName = NULL;
    param->bIntraInBFrames = 0;
    param->bLossless = 0;
    param->bCULossless = 0;
    param->bEnableTemporalSubLayers = 0;
    param->bEnableRdRefine = 0;
    param->bMultiPassOptRPS = 0;
    param->bSsimRd = 0;

    /* Rate control options */
    param->rc.vbvMaxBitrate = 0;
    param->rc.vbvBufferSize = 0;
    param->rc.vbvBufferInit = 0.9;
    param->vbvBufferEnd = 0;
    param->vbvEndFrameAdjust = 0;
    param->rc.rfConstant = 28;
    param->rc.bitrate = 0;
    param->rc.qCompress = 0.6;
    param->rc.ipFactor = 1.4f;
    param->rc.pbFactor = 1.3f;
    param->rc.qpStep = 4;
    param->rc.rateControlMode = X265_RC_CRF;
    param->rc.qp = 32;
    param->rc.aqMode = X265_AQ_VARIANCE;
    param->rc.qgSize = 32;
    param->rc.aqStrength = 1.0;
    param->rc.cuTree = 1;
    param->rc.rfConstantMax = 0;
    param->rc.rfConstantMin = 0;
    param->rc.bStatRead = 0;
    param->rc.bStatWrite = 0;
    param->rc.statFileName = NULL;
    param->rc.complexityBlur = 20;
    param->rc.qblur = 0.5;
    param->rc.zoneCount = 0;
    param->rc.zones = NULL;
    param->rc.bEnableSlowFirstPass = 1;
    param->rc.bStrictCbr = 0;
    param->rc.bEnableGrain = 0;
    param->rc.qpMin = 0;
    param->rc.qpMax = QP_MAX_MAX;
    param->rc.bEnableConstVbv = 0;

    /* Video Usability Information (VUI) */
    param->vui.aspectRatioIdc = 0;
    param->vui.sarWidth = 0;
    param->vui.sarHeight = 0;
    param->vui.bEnableOverscanAppropriateFlag = 0;
    param->vui.bEnableVideoSignalTypePresentFlag = 0;
    param->vui.videoFormat = 5;
    param->vui.bEnableVideoFullRangeFlag = 0;
    param->vui.bEnableColorDescriptionPresentFlag = 0;
    param->vui.colorPrimaries = 2;
    param->vui.transferCharacteristics = 2;
    param->vui.matrixCoeffs = 2;
    param->vui.bEnableChromaLocInfoPresentFlag = 0;
    param->vui.chromaSampleLocTypeTopField = 0;
    param->vui.chromaSampleLocTypeBottomField = 0;
    param->vui.bEnableDefaultDisplayWindowFlag = 0;
    param->vui.defDispWinLeftOffset = 0;
    param->vui.defDispWinRightOffset = 0;
    param->vui.defDispWinTopOffset = 0;
    param->vui.defDispWinBottomOffset = 0;
    param->maxCLL = 0;
    param->maxFALL = 0;
    param->minLuma = 0;
    param->maxLuma = PIXEL_MAX;
    param->log2MaxPocLsb = 8;
    param->maxSlices = 1;

    param->bEmitVUITimingInfo   = 1;
    param->bEmitVUIHRDInfo      = 1;
    param->bOptQpPPS            = 0;
    param->bOptRefListLengthPPS = 0;
    param->bOptCUDeltaQP        = 0;
    param->bAQMotion = 0;
    param->bHDROpt = 0;
    param->analysisReuseLevel = 5;

    param->toneMapFile = NULL;
    param->bDhdr10opt = 0;
    param->bCTUInfo = 0;
    param->bUseRcStats = 0;
    param->scaleFactor = 0;
    param->intraRefine = 0;
    param->interRefine = 0;
    param->mvRefine = 0;
    param->bUseAnalysisFile = 1;
    param->csvfpt = NULL;
    param->forceFlush = 0;
    param->bDisableLookahead = 0;
    param->bCopyPicToFrame = 1;

    /* DCT Approximations */
    param->bLowPassDct = 0;
    param->bMVType = 0;
}

int x265_param_default_preset(x265_param* param, const char* preset, const char* tune)
{
#if EXPORT_C_API
    ::x265_param_default(param);
#else
    X265_NS::x265_param_default(param);
#endif

    if (preset)
    {
        char *end;
        int i = strtol(preset, &end, 10);
        if (*end == 0 && i >= 0 && i < (int)(sizeof(x265_preset_names) / sizeof(*x265_preset_names) - 1))
            preset = x265_preset_names[i];

        if (!strcmp(preset, "ultrafast"))
        {
            param->lookaheadDepth = 5;
            param->scenecutThreshold = 0; // disable lookahead
            param->maxCUSize = 32;
            param->minCUSize = 16;
            param->bframes = 3;
            param->bFrameAdaptive = 0;
            param->subpelRefine = 0;
            param->searchMethod = X265_DIA_SEARCH;
            param->bEnableEarlySkip = 1;
            param->bEnableSAO = 0;
            param->bEnableSignHiding = 0;
            param->bEnableWeightedPred = 0;
            param->rdLevel = 2;
            param->maxNumReferences = 1;
            param->limitReferences = 0;
            param->rc.aqStrength = 0.0;
            param->rc.aqMode = X265_AQ_NONE;
            param->rc.qgSize = 32;
            param->bEnableFastIntra = 1;
        }
        else if (!strcmp(preset, "superfast"))
        {
            param->lookaheadDepth = 10;
            param->maxCUSize = 32;
            param->bframes = 3;
            param->bFrameAdaptive = 0;
            param->subpelRefine = 1;
            param->bEnableEarlySkip = 1;
            param->bEnableWeightedPred = 0;
            param->rdLevel = 2;
            param->maxNumReferences = 1;
            param->limitReferences = 0;
            param->rc.aqStrength = 0.0;
            param->rc.aqMode = X265_AQ_NONE;
            param->rc.qgSize = 32;
            param->bEnableSAO = 0;
            param->bEnableFastIntra = 1;
        }
        else if (!strcmp(preset, "veryfast"))
        {
            param->lookaheadDepth = 15;
            param->bFrameAdaptive = 0;
            param->subpelRefine = 1;
            param->bEnableEarlySkip = 1;
            param->rdLevel = 2;
            param->maxNumReferences = 2;
            param->rc.qgSize = 32;
            param->bEnableFastIntra = 1;
        }
        else if (!strcmp(preset, "faster"))
        {
            param->lookaheadDepth = 15;
            param->bFrameAdaptive = 0;
            param->bEnableEarlySkip = 1;
            param->rdLevel = 2;
            param->maxNumReferences = 2;
            param->bEnableFastIntra = 1;
        }
        else if (!strcmp(preset, "fast"))
        {
            param->lookaheadDepth = 15;
            param->bFrameAdaptive = 0;
            param->rdLevel = 2;
            param->maxNumReferences = 3;
            param->bEnableFastIntra = 1;
        }
        else if (!strcmp(preset, "medium"))
        {
            /* defaults */
        }
        else if (!strcmp(preset, "slow"))
        {
            param->bEnableRectInter = 1;
            param->lookaheadDepth = 25;
            param->rdLevel = 4;
            param->rdoqLevel = 2;
            param->psyRdoq = 1.0;
            param->subpelRefine = 3;
            param->maxNumMergeCand = 3;
            param->searchMethod = X265_STAR_SEARCH;
            param->maxNumReferences = 4;
            param->limitModes = 1;
            param->lookaheadSlices = 4; // limit parallelism as already enough work exists
        }
        else if (!strcmp(preset, "slower"))
        {
            param->bEnableWeightedBiPred = 1;
            param->bEnableAMP = 1;
            param->bEnableRectInter = 1;
            param->lookaheadDepth = 30;
            param->bframes = 8;
            param->tuQTMaxInterDepth = 2;
            param->tuQTMaxIntraDepth = 2;
            param->rdLevel = 6;
            param->rdoqLevel = 2;
            param->psyRdoq = 1.0;
            param->subpelRefine = 3;
            param->maxNumMergeCand = 3;
            param->searchMethod = X265_STAR_SEARCH;
            param->maxNumReferences = 4;
            param->limitReferences = 2;
            param->limitModes = 1;
            param->bIntraInBFrames = 1;
            param->lookaheadSlices = 4; // limit parallelism as already enough work exists
            param->limitTU = 4;
        }
        else if (!strcmp(preset, "veryslow"))
        {
            param->bEnableWeightedBiPred = 1;
            param->bEnableAMP = 1;
            param->bEnableRectInter = 1;
            param->lookaheadDepth = 40;
            param->bframes = 8;
            param->tuQTMaxInterDepth = 3;
            param->tuQTMaxIntraDepth = 3;
            param->rdLevel = 6;
            param->rdoqLevel = 2;
            param->psyRdoq = 1.0;
            param->subpelRefine = 4;
            param->maxNumMergeCand = 4;
            param->searchMethod = X265_STAR_SEARCH;
            param->maxNumReferences = 5;
            param->limitReferences = 1;
            param->limitModes = 1;
            param->bIntraInBFrames = 1;
            param->lookaheadSlices = 0; // disabled for best quality
            param->limitTU = 4;
        }
        else if (!strcmp(preset, "placebo"))
        {
            param->bEnableWeightedBiPred = 1;
            param->bEnableAMP = 1;
            param->bEnableRectInter = 1;
            param->lookaheadDepth = 60;
            param->searchRange = 92;
            param->bframes = 8;
            param->tuQTMaxInterDepth = 4;
            param->tuQTMaxIntraDepth = 4;
            param->rdLevel = 6;
            param->rdoqLevel = 2;
            param->psyRdoq = 1.0;
            param->subpelRefine = 5;
            param->maxNumMergeCand = 5;
            param->searchMethod = X265_STAR_SEARCH;
            param->bEnableTransformSkip = 1;
            param->bEnableRecursionSkip = 0;
            param->maxNumReferences = 5;
            param->limitReferences = 0;
            param->bIntraInBFrames = 1;
            param->lookaheadSlices = 0; // disabled for best quality
            // TODO: optimized esa
        }
        else
            return -1;
    }
    if (tune)
    {
        if (!strcmp(tune, "psnr"))
        {
            param->rc.aqStrength = 0.0;
            param->psyRd = 0.0;
            param->psyRdoq = 0.0;
        }
        else if (!strcmp(tune, "ssim"))
        {
            param->rc.aqMode = X265_AQ_AUTO_VARIANCE;
            param->psyRd = 0.0;
            param->psyRdoq = 0.0;
        }
        else if (!strcmp(tune, "fastdecode") ||
                 !strcmp(tune, "fast-decode"))
        {
            param->bEnableLoopFilter = 0;
            param->bEnableSAO = 0;
            param->bEnableWeightedPred = 0;
            param->bEnableWeightedBiPred = 0;
            param->bIntraInBFrames = 0;
        }
        else if (!strcmp(tune, "zerolatency") ||
                 !strcmp(tune, "zero-latency"))
        {
            param->bFrameAdaptive = 0;
            param->bframes = 0;
            param->lookaheadDepth = 0;
            param->scenecutThreshold = 0;
            param->rc.cuTree = 0;
            param->frameNumThreads = 1;
        }
        else if (!strcmp(tune, "grain"))
        {
            param->rc.ipFactor = 1.1;
            param->rc.pbFactor = 1.0;
            param->rc.cuTree = 0;
            param->rc.aqMode = 0;
            param->rc.qpStep = 1;
            param->rc.bEnableGrain = 1;
            param->bEnableRecursionSkip = 0;
            param->psyRd = 4.0;
            param->psyRdoq = 10.0;
            param->bEnableSAO = 0;
            param->rc.bEnableConstVbv = 1;
        }
        else
            return -1;
    }

    return 0;
}

static int x265_atobool(const char* str, bool& bError)
{
    if (!strcmp(str, "1") ||
        !strcmp(str, "true") ||
        !strcmp(str, "yes"))
        return 1;
    if (!strcmp(str, "0") ||
        !strcmp(str, "false") ||
        !strcmp(str, "no"))
        return 0;
    bError = true;
    return 0;
}

static int parseName(const char* arg, const char* const* names, bool& bError)
{
    for (int i = 0; names[i]; i++)
        if (!strcmp(arg, names[i]))
            return i;

    return x265_atoi(arg, bError);
}

/* internal versions of string-to-int with additional error checking */
#undef atoi
#undef atof
#define atoi(str) x265_atoi(str, bError)
#define atof(str) x265_atof(str, bError)
#define atobool(str) (bNameWasBool = true, x265_atobool(str, bError))

int x265_param_parse(x265_param* p, const char* name, const char* value)
{
    bool bError = false;
    bool bNameWasBool = false;
    bool bValueWasNull = !value;
    bool bExtraParams = false;
    char nameBuf[64];

    if (!name)
        return X265_PARAM_BAD_NAME;

    // skip -- prefix if provided
    if (name[0] == '-' && name[1] == '-')
        name += 2;

    // s/_/-/g
    if (strlen(name) + 1 < sizeof(nameBuf) && strchr(name, '_'))
    {
        char *c;
        strcpy(nameBuf, name);
        while ((c = strchr(nameBuf, '_')) != 0)
            *c = '-';

        name = nameBuf;
    }

    if (!strncmp(name, "no-", 3))
    {
        name += 3;
        value = !value || x265_atobool(value, bError) ? "false" : "true";
    }
    else if (!strncmp(name, "no", 2))
    {
        name += 2;
        value = !value || x265_atobool(value, bError) ? "false" : "true";
    }
    else if (!value)
        value = "true";
    else if (value[0] == '=')
        value++;

#if defined(_MSC_VER)
#pragma warning(disable: 4127) // conditional expression is constant
#endif
#define OPT(STR) else if (!strcmp(name, STR))
#define OPT2(STR1, STR2) else if (!strcmp(name, STR1) || !strcmp(name, STR2))
    if (0) ;
    OPT("asm")
    {
        if (bValueWasNull)
            p->cpuid = atobool(value);
        else
            p->cpuid = parseCpuName(value, bError);
    }
    OPT("fps")
    {
        if (sscanf(value, "%u/%u", &p->fpsNum, &p->fpsDenom) == 2)
            ;
        else
        {
            float fps = (float)atof(value);
            if (fps > 0 && fps <= INT_MAX / 1000)
            {
                p->fpsNum = (int)(fps * 1000 + .5);
                p->fpsDenom = 1000;
            }
            else
            {
                p->fpsNum = atoi(value);
                p->fpsDenom = 1;
            }
        }
    }
    OPT("frame-threads") p->frameNumThreads = atoi(value);
    OPT("pmode") p->bDistributeModeAnalysis = atobool(value);
    OPT("pme") p->bDistributeMotionEstimation = atobool(value);
    OPT2("level-idc", "level")
    {
        /* allow "5.1" or "51", both converted to integer 51 */
        /* if level-idc specifies an obviously wrong value in either float or int, 
        throw error consistently. Stronger level checking will be done in encoder_open() */
        if (atof(value) < 10)
            p->levelIdc = (int)(10 * atof(value) + .5);
        else if (atoi(value) < 100)
            p->levelIdc = atoi(value);
        else 
            bError = true;
    }
    OPT("high-tier") p->bHighTier = atobool(value);
    OPT("allow-non-conformance") p->bAllowNonConformance = atobool(value);
    OPT2("log-level", "log")
    {
        p->logLevel = atoi(value);
        if (bError)
        {
            bError = false;
            p->logLevel = parseName(value, logLevelNames, bError) - 1;
        }
    }
    OPT("cu-stats") p->bLogCuStats = atobool(value);
    OPT("total-frames") p->totalFrames = atoi(value);
    OPT("annexb") p->bAnnexB = atobool(value);
    OPT("repeat-headers") p->bRepeatHeaders = atobool(value);
    OPT("wpp") p->bEnableWavefront = atobool(value);
    OPT("ctu") p->maxCUSize = (uint32_t)atoi(value);
    OPT("min-cu-size") p->minCUSize = (uint32_t)atoi(value);
    OPT("tu-intra-depth") p->tuQTMaxIntraDepth = (uint32_t)atoi(value);
    OPT("tu-inter-depth") p->tuQTMaxInterDepth = (uint32_t)atoi(value);
    OPT("max-tu-size") p->maxTUSize = (uint32_t)atoi(value);
    OPT("subme") p->subpelRefine = atoi(value);
    OPT("merange") p->searchRange = atoi(value);
    OPT("rect") p->bEnableRectInter = atobool(value);
    OPT("amp") p->bEnableAMP = atobool(value);
    OPT("max-merge") p->maxNumMergeCand = (uint32_t)atoi(value);
    OPT("temporal-mvp") p->bEnableTemporalMvp = atobool(value);
    OPT("early-skip") p->bEnableEarlySkip = atobool(value);
    OPT("rskip") p->bEnableRecursionSkip = atobool(value);
    OPT("rdpenalty") p->rdPenalty = atoi(value);
    OPT("tskip") p->bEnableTransformSkip = atobool(value);
    OPT("no-tskip-fast") p->bEnableTSkipFast = atobool(value);
    OPT("tskip-fast") p->bEnableTSkipFast = atobool(value);
    OPT("strong-intra-smoothing") p->bEnableStrongIntraSmoothing = atobool(value);
    OPT("lossless") p->bLossless = atobool(value);
    OPT("cu-lossless") p->bCULossless = atobool(value);
    OPT2("constrained-intra", "cip") p->bEnableConstrainedIntra = atobool(value);
    OPT("fast-intra") p->bEnableFastIntra = atobool(value);
    OPT("open-gop") p->bOpenGOP = atobool(value);
    OPT("intra-refresh") p->bIntraRefresh = atobool(value);
    OPT("lookahead-slices") p->lookaheadSlices = atoi(value);
    OPT("scenecut")
    {
        p->scenecutThreshold = atobool(value);
        if (bError || p->scenecutThreshold)
        {
            bError = false;
            p->scenecutThreshold = atoi(value);
        }
    }
    OPT("temporal-layers") p->bEnableTemporalSubLayers = atobool(value);
    OPT("keyint") p->keyframeMax = atoi(value);
    OPT("min-keyint") p->keyframeMin = atoi(value);
    OPT("rc-lookahead") p->lookaheadDepth = atoi(value);
    OPT("bframes") p->bframes = atoi(value);
    OPT("bframe-bias") p->bFrameBias = atoi(value);
    OPT("b-adapt")
    {
        p->bFrameAdaptive = atobool(value);
        if (bError || p->bFrameAdaptive)
        {
            bError = false;
            p->bFrameAdaptive = atoi(value);
        }
    }
    OPT("interlace")
    {
        p->interlaceMode = atobool(value);
        if (bError || p->interlaceMode)
        {
            bError = false;
            p->interlaceMode = parseName(value, x265_interlace_names, bError);
        }
    }
    OPT("ref") p->maxNumReferences = atoi(value);
    OPT("limit-refs") p->limitReferences = atoi(value);
    OPT("limit-modes") p->limitModes = atobool(value);
    OPT("weightp") p->bEnableWeightedPred = atobool(value);
    OPT("weightb") p->bEnableWeightedBiPred = atobool(value);
    OPT("cbqpoffs") p->cbQpOffset = atoi(value);
    OPT("crqpoffs") p->crQpOffset = atoi(value);
    OPT("rd") p->rdLevel = atoi(value);
    OPT2("rdoq", "rdoq-level")
    {
        int bval = atobool(value);
        if (bError || bval)
        {
            bError = false;
            p->rdoqLevel = atoi(value);
        }
        else
            p->rdoqLevel = 0;
    }
    OPT("psy-rd")
    {
        int bval = atobool(value);
        if (bError || bval)
        {
            bError = false;
            p->psyRd = atof(value);
        }
        else
            p->psyRd = 0.0;
    }
    OPT("psy-rdoq")
    {
        int bval = atobool(value);
        if (bError || bval)
        {
            bError = false;
            p->psyRdoq = atof(value);
        }
        else
            p->psyRdoq = 0.0;
    }
    OPT("rd-refine") p->bEnableRdRefine = atobool(value);
    OPT("signhide") p->bEnableSignHiding = atobool(value);
    OPT("b-intra") p->bIntraInBFrames = atobool(value);
    OPT("lft") p->bEnableLoopFilter = atobool(value); /* DEPRECATED */
    OPT("deblock")
    {
        if (2 == sscanf(value, "%d:%d", &p->deblockingFilterTCOffset, &p->deblockingFilterBetaOffset) ||
            2 == sscanf(value, "%d,%d", &p->deblockingFilterTCOffset, &p->deblockingFilterBetaOffset))
        {
            p->bEnableLoopFilter = true;
        }
        else if (sscanf(value, "%d", &p->deblockingFilterTCOffset))
        {
            p->bEnableLoopFilter = 1;
            p->deblockingFilterBetaOffset = p->deblockingFilterTCOffset;
        }
        else
            p->bEnableLoopFilter = atobool(value);
    }
    OPT("sao") p->bEnableSAO = atobool(value);
    OPT("sao-non-deblock") p->bSaoNonDeblocked = atobool(value);
    OPT("ssim") p->bEnableSsim = atobool(value);
    OPT("psnr") p->bEnablePsnr = atobool(value);
    OPT("hash") p->decodedPictureHashSEI = atoi(value);
    OPT("aud") p->bEnableAccessUnitDelimiters = atobool(value);
    OPT("info") p->bEmitInfoSEI = atobool(value);
    OPT("b-pyramid") p->bBPyramid = atobool(value);
    OPT("hrd") p->bEmitHRDSEI = atobool(value);
    OPT2("ipratio", "ip-factor") p->rc.ipFactor = atof(value);
    OPT2("pbratio", "pb-factor") p->rc.pbFactor = atof(value);
    OPT("qcomp") p->rc.qCompress = atof(value);
    OPT("qpstep") p->rc.qpStep = atoi(value);
    OPT("cplxblur") p->rc.complexityBlur = atof(value);
    OPT("qblur") p->rc.qblur = atof(value);
    OPT("aq-mode") p->rc.aqMode = atoi(value);
    OPT("aq-strength") p->rc.aqStrength = atof(value);
    OPT("vbv-maxrate") p->rc.vbvMaxBitrate = atoi(value);
    OPT("vbv-bufsize") p->rc.vbvBufferSize = atoi(value);
    OPT("vbv-init")    p->rc.vbvBufferInit = atof(value);
    OPT("crf-max")     p->rc.rfConstantMax = atof(value);
    OPT("crf-min")     p->rc.rfConstantMin = atof(value);
    OPT("qpmax")       p->rc.qpMax = atoi(value);
    OPT("crf")
    {
        p->rc.rfConstant = atof(value);
        p->rc.rateControlMode = X265_RC_CRF;
    }
    OPT("bitrate")
    {
        p->rc.bitrate = atoi(value);
        p->rc.rateControlMode = X265_RC_ABR;
    }
    OPT("qp")
    {
        p->rc.qp = atoi(value);
        p->rc.rateControlMode = X265_RC_CQP;
    }
    OPT("rc-grain") p->rc.bEnableGrain = atobool(value);
    OPT("zones")
    {
        p->rc.zoneCount = 1;
        const char* c;

        for (c = value; *c; c++)
            p->rc.zoneCount += (*c == '/');

        p->rc.zones = X265_MALLOC(x265_zone, p->rc.zoneCount);
        c = value;
        for (int i = 0; i < p->rc.zoneCount; i++ )
        {
            int len;
            if (3 == sscanf(c, "%d,%d,q=%d%n", &p->rc.zones[i].startFrame, &p->rc.zones[i].endFrame, &p->rc.zones[i].qp, &len))
                p->rc.zones[i].bForceQp = 1;
            else if (3 == sscanf(c, "%d,%d,b=%f%n", &p->rc.zones[i].startFrame, &p->rc.zones[i].endFrame, &p->rc.zones[i].bitrateFactor, &len))
                p->rc.zones[i].bForceQp = 0;
            else
            {
                bError = true;
                break;
            }
            c += len + 1;
        }
    }
    OPT("input-res") bError |= sscanf(value, "%dx%d", &p->sourceWidth, &p->sourceHeight) != 2;
    OPT("input-csp") p->internalCsp = parseName(value, x265_source_csp_names, bError);
    OPT("me")        p->searchMethod = parseName(value, x265_motion_est_names, bError);
    OPT("cutree")    p->rc.cuTree = atobool(value);
    OPT("slow-firstpass") p->rc.bEnableSlowFirstPass = atobool(value);
    OPT("strict-cbr")
    {
        p->rc.bStrictCbr = atobool(value);
        p->rc.pbFactor = 1.0;
    }
    OPT("analysis-reuse-mode") p->analysisReuseMode = parseName(value, x265_analysis_names, bError);
    OPT("sar")
    {
        p->vui.aspectRatioIdc = parseName(value, x265_sar_names, bError);
        if (bError)
        {
            p->vui.aspectRatioIdc = X265_EXTENDED_SAR;
            bError = sscanf(value, "%d:%d", &p->vui.sarWidth, &p->vui.sarHeight) != 2;
        }
    }
    OPT("overscan")
    {
        if (!strcmp(value, "show"))
            p->vui.bEnableOverscanInfoPresentFlag = 1;
        else if (!strcmp(value, "crop"))
        {
            p->vui.bEnableOverscanInfoPresentFlag = 1;
            p->vui.bEnableOverscanAppropriateFlag = 1;
        }
        else if (!strcmp(value, "undef"))
            p->vui.bEnableOverscanInfoPresentFlag = 0;
        else
            bError = true;
    }
    OPT("videoformat")
    {
        p->vui.bEnableVideoSignalTypePresentFlag = 1;
        p->vui.videoFormat = parseName(value, x265_video_format_names, bError);
    }
    OPT("range")
    {
        p->vui.bEnableVideoSignalTypePresentFlag = 1;
        p->vui.bEnableVideoFullRangeFlag = parseName(value, x265_fullrange_names, bError);
    }
    OPT("colorprim")
    {
        p->vui.bEnableVideoSignalTypePresentFlag = 1;
        p->vui.bEnableColorDescriptionPresentFlag = 1;
        p->vui.colorPrimaries = parseName(value, x265_colorprim_names, bError);
    }
    OPT("transfer")
    {
        p->vui.bEnableVideoSignalTypePresentFlag = 1;
        p->vui.bEnableColorDescriptionPresentFlag = 1;
        p->vui.transferCharacteristics = parseName(value, x265_transfer_names, bError);
    }
    OPT("colormatrix")
    {
        p->vui.bEnableVideoSignalTypePresentFlag = 1;
        p->vui.bEnableColorDescriptionPresentFlag = 1;
        p->vui.matrixCoeffs = parseName(value, x265_colmatrix_names, bError);
    }
    OPT("chromaloc")
    {
        p->vui.bEnableChromaLocInfoPresentFlag = 1;
        p->vui.chromaSampleLocTypeTopField = atoi(value);
        p->vui.chromaSampleLocTypeBottomField = p->vui.chromaSampleLocTypeTopField;
    }
    OPT2("display-window", "crop-rect")
    {
        p->vui.bEnableDefaultDisplayWindowFlag = 1;
        bError |= sscanf(value, "%d,%d,%d,%d",
                         &p->vui.defDispWinLeftOffset,
                         &p->vui.defDispWinTopOffset,
                         &p->vui.defDispWinRightOffset,
                         &p->vui.defDispWinBottomOffset) != 4;
    }
    OPT("nr-intra") p->noiseReductionIntra = atoi(value);
    OPT("nr-inter") p->noiseReductionInter = atoi(value);
    OPT("pass")
    {
        int pass = x265_clip3(0, 3, atoi(value));
        p->rc.bStatWrite = pass & 1;
        p->rc.bStatRead = pass & 2;
    }
    OPT("stats") p->rc.statFileName = strdup(value);
    OPT("scaling-list") p->scalingLists = strdup(value);
    OPT2("pools", "numa-pools") p->numaPools = strdup(value);
    OPT("lambda-file") p->rc.lambdaFileName = strdup(value);
    OPT("analysis-reuse-file") p->analysisReuseFileName = strdup(value);
    OPT("qg-size") p->rc.qgSize = atoi(value);
    OPT("master-display") p->masteringDisplayColorVolume = strdup(value);
    OPT("max-cll") bError |= sscanf(value, "%hu,%hu", &p->maxCLL, &p->maxFALL) != 2;
    OPT("min-luma") p->minLuma = (uint16_t)atoi(value);
    OPT("max-luma") p->maxLuma = (uint16_t)atoi(value);
    OPT("uhd-bd") p->uhdBluray = atobool(value);
    else
        bExtraParams = true;

    // solve "fatal error C1061: compiler limit : blocks nested too deeply"
    if (bExtraParams)
    {
        if (0) ;
        OPT("csv") p->csvfn = strdup(value);
        OPT("csv-log-level") p->csvLogLevel = atoi(value);
        OPT("qpmin") p->rc.qpMin = atoi(value);
        OPT("analyze-src-pics") p->bSourceReferenceEstimation = atobool(value);
        OPT("log2-max-poc-lsb") p->log2MaxPocLsb = atoi(value);
        OPT("vui-timing-info") p->bEmitVUITimingInfo = atobool(value);
        OPT("vui-hrd-info") p->bEmitVUIHRDInfo = atobool(value);
        OPT("slices") p->maxSlices = atoi(value);
        OPT("limit-tu") p->limitTU = atoi(value);
        OPT("opt-qp-pps") p->bOptQpPPS = atobool(value);
        OPT("opt-ref-list-length-pps") p->bOptRefListLengthPPS = atobool(value);
        OPT("multi-pass-opt-rps") p->bMultiPassOptRPS = atobool(value);
        OPT("scenecut-bias") p->scenecutBias = atof(value);
        OPT("lookahead-threads") p->lookaheadThreads = atoi(value);
        OPT("opt-cu-delta-qp") p->bOptCUDeltaQP = atobool(value);
        OPT("multi-pass-opt-analysis") p->analysisMultiPassRefine = atobool(value);
        OPT("multi-pass-opt-distortion") p->analysisMultiPassDistortion = atobool(value);
        OPT("aq-motion") p->bAQMotion = atobool(value);
        OPT("dynamic-rd") p->dynamicRd = atof(value);
        OPT("analysis-reuse-level") p->analysisReuseLevel = atoi(value);
        OPT("ssim-rd")
        {
            int bval = atobool(value);
            if (bError || bval)
            {
                bError = false;
                p->psyRd = 0.0;
                p->bSsimRd = atobool(value);
            }
        }
        OPT("hdr") p->bEmitHDRSEI = atobool(value);
        OPT("hdr-opt") p->bHDROpt = atobool(value);
        OPT("limit-sao") p->bLimitSAO = atobool(value);
        OPT("dhdr10-info") p->toneMapFile = strdup(value);
        OPT("dhdr10-opt") p->bDhdr10opt = atobool(value);
        OPT("const-vbv") p->rc.bEnableConstVbv = atobool(value);
        OPT("ctu-info") p->bCTUInfo = atoi(value);
        OPT("scale-factor") p->scaleFactor = atoi(value);
        OPT("refine-intra")p->intraRefine = atoi(value);
        OPT("refine-inter")p->interRefine = atoi(value);
        OPT("refine-mv")p->mvRefine = atobool(value);
        OPT("force-flush")p->forceFlush = atoi(value);
        OPT("splitrd-skip") p->bEnableSplitRdSkip = atobool(value);
		OPT("lowpass-dct") p->bLowPassDct = atobool(value);
        OPT("vbv-end") p->vbvBufferEnd = atof(value);
        OPT("vbv-end-fr-adj") p->vbvEndFrameAdjust = atof(value);
        OPT("copy-pic") p->bCopyPicToFrame = atobool(value);
        OPT("refine-mv-type")
        {
            if (strcmp(strdup(value), "avc") == 0)
            {
                p->bMVType = AVC_INFO;
            }
            else if (strcmp(strdup(value), "off") == 0)
            {
                p->bMVType = NO_INFO;
            }
            else
            {
                bError = true;
            }
         }
        else
            return X265_PARAM_BAD_NAME;
    }
#undef OPT
#undef atobool
#undef atoi
#undef atof

    bError |= bValueWasNull && !bNameWasBool;
    return bError ? X265_PARAM_BAD_VALUE : 0;
}

} /* end extern "C" or namespace */

namespace X265_NS {
// internal encoder functions

int x265_atoi(const char* str, bool& bError)
{
    char *end;
    int v = strtol(str, &end, 0);

    if (end == str || *end != '\0')
        bError = true;
    return v;
}

double x265_atof(const char* str, bool& bError)
{
    char *end;
    double v = strtod(str, &end);

    if (end == str || *end != '\0')
        bError = true;
    return v;
}

/* cpu name can be:
 *   auto || true - x265::cpu_detect()
 *   false || no  - disabled
 *   integer bitmap value
 *   comma separated list of SIMD names, eg: SSE4.1,XOP */
int parseCpuName(const char* value, bool& bError)
{
    if (!value)
    {
        bError = 1;
        return 0;
    }
    int cpu;
    if (isdigit(value[0]))
        cpu = x265_atoi(value, bError);
    else
        cpu = !strcmp(value, "auto") || x265_atobool(value, bError) ? X265_NS::cpu_detect() : 0;

    if (bError)
    {
        char *buf = strdup(value);
        char *tok, *saveptr = NULL, *init;
        bError = 0;
        cpu = 0;
        for (init = buf; (tok = strtok_r(init, ",", &saveptr)); init = NULL)
        {
            int i;
            for (i = 0; X265_NS::cpu_names[i].flags && strcasecmp(tok, X265_NS::cpu_names[i].name); i++)
            {
            }

            cpu |= X265_NS::cpu_names[i].flags;
            if (!X265_NS::cpu_names[i].flags)
                bError = 1;
        }

        free(buf);
        if ((cpu & X265_CPU_SSSE3) && !(cpu & X265_CPU_SSE2_IS_SLOW))
            cpu |= X265_CPU_SSE2_IS_FAST;
    }

    return cpu;
}

static const int fixedRatios[][2] =
{
    { 1,  1 },
    { 12, 11 },
    { 10, 11 },
    { 16, 11 },
    { 40, 33 },
    { 24, 11 },
    { 20, 11 },
    { 32, 11 },
    { 80, 33 },
    { 18, 11 },
    { 15, 11 },
    { 64, 33 },
    { 160, 99 },
    { 4, 3 },
    { 3, 2 },
    { 2, 1 },
};

void setParamAspectRatio(x265_param* p, int width, int height)
{
    p->vui.aspectRatioIdc = X265_EXTENDED_SAR;
    p->vui.sarWidth = width;
    p->vui.sarHeight = height;
    for (size_t i = 0; i < sizeof(fixedRatios) / sizeof(fixedRatios[0]); i++)
    {
        if (width == fixedRatios[i][0] && height == fixedRatios[i][1])
        {
            p->vui.aspectRatioIdc = (int)i + 1;
            return;
        }
    }
}

void getParamAspectRatio(x265_param* p, int& width, int& height)
{
    if (!p->vui.aspectRatioIdc)
        width = height = 0;
    else if ((size_t)p->vui.aspectRatioIdc <= sizeof(fixedRatios) / sizeof(fixedRatios[0]))
    {
        width  = fixedRatios[p->vui.aspectRatioIdc - 1][0];
        height = fixedRatios[p->vui.aspectRatioIdc - 1][1];
    }
    else if (p->vui.aspectRatioIdc == X265_EXTENDED_SAR)
    {
        width  = p->vui.sarWidth;
        height = p->vui.sarHeight;
    }
    else
        width = height = 0;
}

static inline int _confirm(x265_param* param, bool bflag, const char* message)
{
    if (!bflag)
        return 0;

    x265_log(param, X265_LOG_ERROR, "%s\n", message);
    return 1;
}

int x265_check_params(x265_param* param)
{
#define CHECK(expr, msg) check_failed |= _confirm(param, expr, msg)
    int check_failed = 0; /* abort if there is a fatal configuration problem */
    CHECK(param->uhdBluray == 1 && (X265_DEPTH != 10 || param->internalCsp != 1 || param->interlaceMode != 0),
        "uhd-bd: bit depth, chroma subsample, source picture type must be 10, 4:2:0, progressive");
    CHECK(param->maxCUSize != 64 && param->maxCUSize != 32 && param->maxCUSize != 16,
          "max cu size must be 16, 32, or 64");
    if (check_failed == 1)
        return check_failed;

    uint32_t maxLog2CUSize = (uint32_t)g_log2Size[param->maxCUSize];
    uint32_t tuQTMaxLog2Size = X265_MIN(maxLog2CUSize, 5);
    uint32_t tuQTMinLog2Size = 2; //log2(4)

    CHECK((param->maxSlices > 1) && !param->bEnableWavefront,
        "Multiple-Slices mode must be enable Wavefront Parallel Processing (--wpp)");
    CHECK(param->internalBitDepth != X265_DEPTH,
          "internalBitDepth must match compiled bit depth");
    CHECK(param->minCUSize != 32 && param->minCUSize != 16 && param->minCUSize != 8,
          "minimim CU size must be 8, 16 or 32");
    CHECK(param->minCUSize > param->maxCUSize,
          "min CU size must be less than or equal to max CU size");
    CHECK(param->rc.qp < -6 * (param->internalBitDepth - 8) || param->rc.qp > QP_MAX_SPEC,
          "QP exceeds supported range (-QpBDOffsety to 51)");
    CHECK(param->fpsNum == 0 || param->fpsDenom == 0,
          "Frame rate numerator and denominator must be specified");
    CHECK(param->interlaceMode < 0 || param->interlaceMode > 2,
          "Interlace mode must be 0 (progressive) 1 (top-field first) or 2 (bottom field first)");
    CHECK(param->searchMethod < 0 || param->searchMethod > X265_FULL_SEARCH,
          "Search method is not supported value (0:DIA 1:HEX 2:UMH 3:HM 4:SEA 5:FULL)");
    CHECK(param->searchRange < 0,
          "Search Range must be more than 0");
    CHECK(param->searchRange >= 32768,
          "Search Range must be less than 32768");
    CHECK(param->subpelRefine > X265_MAX_SUBPEL_LEVEL,
          "subme must be less than or equal to X265_MAX_SUBPEL_LEVEL (7)");
    CHECK(param->subpelRefine < 0,
          "subme must be greater than or equal to 0");
    CHECK(param->limitReferences > 3,
          "limitReferences must be 0, 1, 2 or 3");
    CHECK(param->limitModes > 1,
          "limitRectAmp must be 0, 1");
    CHECK(param->frameNumThreads < 0 || param->frameNumThreads > X265_MAX_FRAME_THREADS,
          "frameNumThreads (--frame-threads) must be [0 .. X265_MAX_FRAME_THREADS)");
    CHECK(param->cbQpOffset < -12, "Min. Chroma Cb QP Offset is -12");
    CHECK(param->cbQpOffset >  12, "Max. Chroma Cb QP Offset is  12");
    CHECK(param->crQpOffset < -12, "Min. Chroma Cr QP Offset is -12");
    CHECK(param->crQpOffset >  12, "Max. Chroma Cr QP Offset is  12");

    CHECK(tuQTMaxLog2Size > maxLog2CUSize,
          "QuadtreeTULog2MaxSize must be log2(maxCUSize) or smaller.");

    CHECK(param->tuQTMaxInterDepth < 1 || param->tuQTMaxInterDepth > 4,
          "QuadtreeTUMaxDepthInter must be greater than 0 and less than 5");
    CHECK(maxLog2CUSize < tuQTMinLog2Size + param->tuQTMaxInterDepth - 1,
          "QuadtreeTUMaxDepthInter must be less than or equal to the difference between log2(maxCUSize) and QuadtreeTULog2MinSize plus 1");
    CHECK(param->tuQTMaxIntraDepth < 1 || param->tuQTMaxIntraDepth > 4,
          "QuadtreeTUMaxDepthIntra must be greater 0 and less than 5");
    CHECK(maxLog2CUSize < tuQTMinLog2Size + param->tuQTMaxIntraDepth - 1,
          "QuadtreeTUMaxDepthInter must be less than or equal to the difference between log2(maxCUSize) and QuadtreeTULog2MinSize plus 1");
    CHECK((param->maxTUSize != 32 && param->maxTUSize != 16 && param->maxTUSize != 8 && param->maxTUSize != 4),
          "max TU size must be 4, 8, 16, or 32");
    CHECK(param->limitTU > 4, "Invalid limit-tu option, limit-TU must be between 0 and 4");
    CHECK(param->maxNumMergeCand < 1, "MaxNumMergeCand must be 1 or greater.");
    CHECK(param->maxNumMergeCand > 5, "MaxNumMergeCand must be 5 or smaller.");

    CHECK(param->maxNumReferences < 1, "maxNumReferences must be 1 or greater.");
    CHECK(param->maxNumReferences > MAX_NUM_REF, "maxNumReferences must be 16 or smaller.");

    CHECK(param->sourceWidth < (int)param->maxCUSize || param->sourceHeight < (int)param->maxCUSize,
          "Picture size must be at least one CTU");
    CHECK(param->internalCsp < X265_CSP_I400 || X265_CSP_I444 < param->internalCsp,
          "chroma subsampling must be i400 (4:0:0 monochrome), i420 (4:2:0 default), i422 (4:2:0), i444 (4:4:4)");
    CHECK(param->sourceWidth & !!CHROMA_H_SHIFT(param->internalCsp),
          "Picture width must be an integer multiple of the specified chroma subsampling");
    CHECK(param->sourceHeight & !!CHROMA_V_SHIFT(param->internalCsp),
          "Picture height must be an integer multiple of the specified chroma subsampling");

    CHECK(param->rc.rateControlMode > X265_RC_CRF || param->rc.rateControlMode < X265_RC_ABR,
          "Rate control mode is out of range");
    CHECK(param->rdLevel < 1 || param->rdLevel > 6,
          "RD Level is out of range");
    CHECK(param->rdoqLevel < 0 || param->rdoqLevel > 2,
        "RDOQ Level is out of range");
    CHECK(param->dynamicRd < 0 || param->dynamicRd > x265_ADAPT_RD_STRENGTH,
        "Dynamic RD strength must be between 0 and 4");
    CHECK(param->bframes && param->bframes >= param->lookaheadDepth && !param->rc.bStatRead,
          "Lookahead depth must be greater than the max consecutive bframe count");
    CHECK(param->bframes < 0,
          "bframe count should be greater than zero");
    CHECK(param->bframes > X265_BFRAME_MAX,
          "max consecutive bframe count must be 16 or smaller");
    CHECK(param->lookaheadDepth > X265_LOOKAHEAD_MAX,
          "Lookahead depth must be less than 256");
    CHECK(param->lookaheadSlices > 16 || param->lookaheadSlices < 0,
          "Lookahead slices must between 0 and 16");
    CHECK(param->rc.aqMode < X265_AQ_NONE || X265_AQ_AUTO_VARIANCE_BIASED < param->rc.aqMode,
          "Aq-Mode is out of range");
    CHECK(param->rc.aqStrength < 0 || param->rc.aqStrength > 3,
          "Aq-Strength is out of range");
    CHECK(param->deblockingFilterTCOffset < -6 || param->deblockingFilterTCOffset > 6,
          "deblocking filter tC offset must be in the range of -6 to +6");
    CHECK(param->deblockingFilterBetaOffset < -6 || param->deblockingFilterBetaOffset > 6,
          "deblocking filter Beta offset must be in the range of -6 to +6");
    CHECK(param->psyRd < 0 || 5.0 < param->psyRd, "Psy-rd strength must be between 0 and 5.0");
    CHECK(param->psyRdoq < 0 || 50.0 < param->psyRdoq, "Psy-rdoq strength must be between 0 and 50.0");
    CHECK(param->bEnableWavefront < 0, "WaveFrontSynchro cannot be negative");
    CHECK((param->vui.aspectRatioIdc < 0
           || param->vui.aspectRatioIdc > 16)
          && param->vui.aspectRatioIdc != X265_EXTENDED_SAR,
          "Sample Aspect Ratio must be 0-16 or 255");
    CHECK(param->vui.aspectRatioIdc == X265_EXTENDED_SAR && param->vui.sarWidth <= 0,
          "Sample Aspect Ratio width must be greater than 0");
    CHECK(param->vui.aspectRatioIdc == X265_EXTENDED_SAR && param->vui.sarHeight <= 0,
          "Sample Aspect Ratio height must be greater than 0");
    CHECK(param->vui.videoFormat < 0 || param->vui.videoFormat > 5,
          "Video Format must be component,"
          " pal, ntsc, secam, mac or undef");
    CHECK(param->vui.colorPrimaries < 0
          || param->vui.colorPrimaries > 12
          || param->vui.colorPrimaries == 3,
          "Color Primaries must be undef, bt709, bt470m,"
          " bt470bg, smpte170m, smpte240m, film, bt2020, smpte-st-428, smpte-rp-431 or smpte-eg-432");
    CHECK(param->vui.transferCharacteristics < 0
          || param->vui.transferCharacteristics > 18
          || param->vui.transferCharacteristics == 3,
          "Transfer Characteristics must be undef, bt709, bt470m, bt470bg,"
          " smpte170m, smpte240m, linear, log100, log316, iec61966-2-4, bt1361e,"
          " iec61966-2-1, bt2020-10, bt2020-12, smpte-st-2084, smpte-st-428 or arib-std-b67");
    CHECK(param->vui.matrixCoeffs < 0
          || param->vui.matrixCoeffs > 14
          || param->vui.matrixCoeffs == 3,
          "Matrix Coefficients must be undef, bt709, fcc, bt470bg, smpte170m,"
          " smpte240m, GBR, YCgCo, bt2020nc, bt2020c, smpte-st-2085, chroma-nc, chroma-c or ictcp");
    CHECK(param->vui.chromaSampleLocTypeTopField < 0
          || param->vui.chromaSampleLocTypeTopField > 5,
          "Chroma Sample Location Type Top Field must be 0-5");
    CHECK(param->vui.chromaSampleLocTypeBottomField < 0
          || param->vui.chromaSampleLocTypeBottomField > 5,
          "Chroma Sample Location Type Bottom Field must be 0-5");
    CHECK(param->vui.defDispWinLeftOffset < 0,
          "Default Display Window Left Offset must be 0 or greater");
    CHECK(param->vui.defDispWinRightOffset < 0,
          "Default Display Window Right Offset must be 0 or greater");
    CHECK(param->vui.defDispWinTopOffset < 0,
          "Default Display Window Top Offset must be 0 or greater");
    CHECK(param->vui.defDispWinBottomOffset < 0,
          "Default Display Window Bottom Offset must be 0 or greater");
    CHECK(param->rc.rfConstant < -6 * (param->internalBitDepth - 8) || param->rc.rfConstant > 51,
          "Valid quality based range: -qpBDOffsetY to 51");
    CHECK(param->rc.rfConstantMax < -6 * (param->internalBitDepth - 8) || param->rc.rfConstantMax > 51,
          "Valid quality based range: -qpBDOffsetY to 51");
    CHECK(param->rc.rfConstantMin < -6 * (param->internalBitDepth - 8) || param->rc.rfConstantMin > 51,
          "Valid quality based range: -qpBDOffsetY to 51");
    CHECK(param->bFrameAdaptive < 0 || param->bFrameAdaptive > 2,
          "Valid adaptive b scheduling values 0 - none, 1 - fast, 2 - full");
    CHECK(param->logLevel<-1 || param->logLevel> X265_LOG_FULL,
          "Valid Logging level -1:none 0:error 1:warning 2:info 3:debug 4:full");
    CHECK(param->scenecutThreshold < 0,
          "scenecutThreshold must be greater than 0");
    CHECK(param->scenecutBias < 0 || 100 < param->scenecutBias,
           "scenecut-bias must be between 0 and 100");
    CHECK(param->rdPenalty < 0 || param->rdPenalty > 2,
          "Valid penalty for 32x32 intra TU in non-I slices. 0:disabled 1:RD-penalty 2:maximum");
    CHECK(param->keyframeMax < -1,
          "Invalid max IDR period in frames. value should be greater than -1");
    CHECK(param->decodedPictureHashSEI < 0 || param->decodedPictureHashSEI > 3,
          "Invalid hash option. Decoded Picture Hash SEI 0: disabled, 1: MD5, 2: CRC, 3: Checksum");
    CHECK(param->rc.vbvBufferSize < 0,
          "Size of the vbv buffer can not be less than zero");
    CHECK(param->rc.vbvMaxBitrate < 0,
          "Maximum local bit rate can not be less than zero");
    CHECK(param->rc.vbvBufferInit < 0,
          "Valid initial VBV buffer occupancy must be a fraction 0 - 1, or size in kbits");
    CHECK(param->vbvBufferEnd < 0,
        "Valid final VBV buffer emptiness must be a fraction 0 - 1, or size in kbits");
    CHECK(param->vbvEndFrameAdjust < 0,
        "Valid vbv-end-fr-adj must be a fraction 0 - 1");
    CHECK(!param->totalFrames && param->vbvEndFrameAdjust,
        "vbv-end-fr-adj cannot be enabled when total number of frames is unknown");
    CHECK(param->rc.bitrate < 0,
          "Target bitrate can not be less than zero");
    CHECK(param->rc.qCompress < 0.5 || param->rc.qCompress > 1.0,
          "qCompress must be between 0.5 and 1.0");
    if (param->noiseReductionIntra)
        CHECK(0 > param->noiseReductionIntra || param->noiseReductionIntra > 2000, "Valid noise reduction range 0 - 2000");
    if (param->noiseReductionInter)
        CHECK(0 > param->noiseReductionInter || param->noiseReductionInter > 2000, "Valid noise reduction range 0 - 2000");
    CHECK(param->rc.rateControlMode == X265_RC_CQP && param->rc.bStatRead,
          "Constant QP is incompatible with 2pass");
    CHECK(param->rc.bStrictCbr && (param->rc.bitrate <= 0 || param->rc.vbvBufferSize <=0),
          "Strict-cbr cannot be applied without specifying target bitrate or vbv bufsize");
    CHECK(param->analysisReuseMode && (param->analysisReuseMode < X265_ANALYSIS_OFF || param->analysisReuseMode > X265_ANALYSIS_LOAD),
        "Invalid analysis mode. Analysis mode 0: OFF 1: SAVE : 2 LOAD");
    CHECK(param->analysisReuseMode && (param->analysisReuseLevel < 1 || param->analysisReuseLevel > 10),
        "Invalid analysis refine level. Value must be between 1 and 10 (inclusive)");
    CHECK(param->scaleFactor > 2, "Invalid scale-factor. Supports factor <= 2");
    CHECK(param->rc.qpMax < QP_MIN || param->rc.qpMax > QP_MAX_MAX,
        "qpmax exceeds supported range (0 to 69)");
    CHECK(param->rc.qpMin < QP_MIN || param->rc.qpMin > QP_MAX_MAX,
        "qpmin exceeds supported range (0 to 69)");
    CHECK(param->log2MaxPocLsb < 4 || param->log2MaxPocLsb > 16,
        "Supported range for log2MaxPocLsb is 4 to 16");
    CHECK(param->bCTUInfo < 0 || (param->bCTUInfo != 0 && param->bCTUInfo != 1 && param->bCTUInfo != 2 && param->bCTUInfo != 4 && param->bCTUInfo != 6) || param->bCTUInfo > 6,
        "Supported values for bCTUInfo are 0, 1, 2, 4, 6");
    CHECK(param->interRefine > 3 || param->interRefine < 0,
        "Invalid refine-inter value, refine-inter levels 0 to 3 supported");
    CHECK(param->intraRefine > 3 || param->intraRefine < 0,
        "Invalid refine-intra value, refine-intra levels 0 to 3 supported");
#if !X86_64
    CHECK(param->searchMethod == X265_SEA && (param->sourceWidth > 840 || param->sourceHeight > 480),
        "SEA motion search does not support resolutions greater than 480p in 32 bit build");
#endif

    if (param->masteringDisplayColorVolume || param->maxFALL || param->maxCLL)
        param->bEmitHDRSEI = 1;

    return check_failed;
}

void x265_param_apply_fastfirstpass(x265_param* param)
{
    /* Set faster options in case of turbo firstpass */
    if (param->rc.bStatWrite && !param->rc.bStatRead)
    {
        param->maxNumReferences = 1;
        param->maxNumMergeCand = 1;
        param->bEnableRectInter = 0;
        param->bEnableFastIntra = 1;
        param->bEnableAMP = 0;
        param->searchMethod = X265_DIA_SEARCH;
        param->subpelRefine = X265_MIN(2, param->subpelRefine);
        param->bEnableEarlySkip = 1;
        param->rdLevel = X265_MIN(2, param->rdLevel);
    }
}

static void appendtool(x265_param* param, char* buf, size_t size, const char* toolstr)
{
    static const int overhead = (int)strlen("x265 [info]: tools: ");

    if (strlen(buf) + strlen(toolstr) + overhead >= size)
    {
        x265_log(param, X265_LOG_INFO, "tools:%s\n", buf);
        sprintf(buf, " %s", toolstr);
    }
    else
    {
        strcat(buf, " ");
        strcat(buf, toolstr);
    }
}

void x265_print_params(x265_param* param)
{
    if (param->logLevel < X265_LOG_INFO)
        return;

    if (param->interlaceMode)
        x265_log(param, X265_LOG_INFO, "Interlaced field inputs             : %s\n", x265_interlace_names[param->interlaceMode]);

    x265_log(param, X265_LOG_INFO, "Coding QT: max CU size, min CU size : %d / %d\n", param->maxCUSize, param->minCUSize);

    x265_log(param, X265_LOG_INFO, "Residual QT: max TU size, max depth : %d / %d inter / %d intra\n",
             param->maxTUSize, param->tuQTMaxInterDepth, param->tuQTMaxIntraDepth);

    x265_log(param, X265_LOG_INFO, "ME / range / subpel / merge         : %s / %d / %d / %d\n",
             x265_motion_est_names[param->searchMethod], param->searchRange, param->subpelRefine, param->maxNumMergeCand);
    if (param->keyframeMax != INT_MAX || param->scenecutThreshold)
        x265_log(param, X265_LOG_INFO, "Keyframe min / max / scenecut / bias: %d / %d / %d / %.2lf\n", param->keyframeMin, param->keyframeMax, param->scenecutThreshold, param->scenecutBias * 100);
    else
        x265_log(param, X265_LOG_INFO, "Keyframe min / max / scenecut       : disabled\n");

    if (param->cbQpOffset || param->crQpOffset)
        x265_log(param, X265_LOG_INFO, "Cb/Cr QP Offset                     : %d / %d\n", param->cbQpOffset, param->crQpOffset);

    if (param->rdPenalty)
        x265_log(param, X265_LOG_INFO, "Intra 32x32 TU penalty type         : %d\n", param->rdPenalty);

    x265_log(param, X265_LOG_INFO, "Lookahead / bframes / badapt        : %d / %d / %d\n", param->lookaheadDepth, param->bframes, param->bFrameAdaptive);
    x265_log(param, X265_LOG_INFO, "b-pyramid / weightp / weightb       : %d / %d / %d\n",
             param->bBPyramid, param->bEnableWeightedPred, param->bEnableWeightedBiPred);
    x265_log(param, X265_LOG_INFO, "References / ref-limit  cu / depth  : %d / %s / %s\n",
             param->maxNumReferences, (param->limitReferences & X265_REF_LIMIT_CU) ? "on" : "off",
             (param->limitReferences & X265_REF_LIMIT_DEPTH) ? "on" : "off");

    if (param->rc.aqMode)
        x265_log(param, X265_LOG_INFO, "AQ: mode / str / qg-size / cu-tree  : %d / %0.1f / %d / %d\n", param->rc.aqMode,
                 param->rc.aqStrength, param->rc.qgSize, param->rc.cuTree);

    if (param->bLossless)
        x265_log(param, X265_LOG_INFO, "Rate Control                        : Lossless\n");
    else switch (param->rc.rateControlMode)
    {
    case X265_RC_ABR:
        x265_log(param, X265_LOG_INFO, "Rate Control / qCompress            : ABR-%d kbps / %0.2f\n", param->rc.bitrate, param->rc.qCompress); break;
    case X265_RC_CQP:
        x265_log(param, X265_LOG_INFO, "Rate Control                        : CQP-%d\n", param->rc.qp); break;
    case X265_RC_CRF:
        x265_log(param, X265_LOG_INFO, "Rate Control / qCompress            : CRF-%0.1f / %0.2f\n", param->rc.rfConstant, param->rc.qCompress); break;
    }

    if (param->rc.vbvBufferSize)
    {
        if (param->vbvBufferEnd)
            x265_log(param, X265_LOG_INFO, "VBV/HRD buffer / max-rate / init / end / fr-adj: %d / %d / %.3f / %.3f / %.3f\n",
            param->rc.vbvBufferSize, param->rc.vbvMaxBitrate, param->rc.vbvBufferInit, param->vbvBufferEnd, param->vbvEndFrameAdjust);
        else
            x265_log(param, X265_LOG_INFO, "VBV/HRD buffer / max-rate / init    : %d / %d / %.3f\n",
            param->rc.vbvBufferSize, param->rc.vbvMaxBitrate, param->rc.vbvBufferInit);
    }
    
    char buf[80] = { 0 };
    char tmp[40];
#define TOOLOPT(FLAG, STR) if (FLAG) appendtool(param, buf, sizeof(buf), STR);
#define TOOLVAL(VAL, STR)  if (VAL) { sprintf(tmp, STR, VAL); appendtool(param, buf, sizeof(buf), tmp); }
    TOOLOPT(param->bEnableRectInter, "rect");
    TOOLOPT(param->bEnableAMP, "amp");
    TOOLOPT(param->limitModes, "limit-modes");
    TOOLVAL(param->rdLevel, "rd=%d");
    TOOLVAL(param->dynamicRd, "dynamic-rd=%.2f");
    TOOLOPT(param->bSsimRd, "ssim-rd");
    TOOLVAL(param->psyRd, "psy-rd=%.2lf");
    TOOLVAL(param->rdoqLevel, "rdoq=%d");
    TOOLVAL(param->psyRdoq, "psy-rdoq=%.2lf");
    TOOLOPT(param->bEnableRdRefine, "rd-refine");
    TOOLOPT(param->bEnableEarlySkip, "early-skip");
    TOOLOPT(param->bEnableRecursionSkip, "rskip");
    TOOLOPT(param->bEnableSplitRdSkip, "splitrd-skip");
    TOOLVAL(param->noiseReductionIntra, "nr-intra=%d");
    TOOLVAL(param->noiseReductionInter, "nr-inter=%d");
    TOOLOPT(param->bEnableTSkipFast, "tskip-fast");
    TOOLOPT(!param->bEnableTSkipFast && param->bEnableTransformSkip, "tskip");
    TOOLVAL(param->limitTU , "limit-tu=%d");
    TOOLOPT(param->bCULossless, "cu-lossless");
    TOOLOPT(param->bEnableSignHiding, "signhide");
    TOOLOPT(param->bEnableTemporalMvp, "tmvp");
    TOOLOPT(param->bEnableConstrainedIntra, "cip");
    TOOLOPT(param->bIntraInBFrames, "b-intra");
    TOOLOPT(param->bEnableFastIntra, "fast-intra");
    TOOLOPT(param->bEnableStrongIntraSmoothing, "strong-intra-smoothing");
    TOOLVAL(param->lookaheadSlices, "lslices=%d");
    TOOLVAL(param->lookaheadThreads, "lthreads=%d")
    TOOLVAL(param->bCTUInfo, "ctu-info=%d");
    if (param->bMVType == AVC_INFO)
        TOOLOPT(param->bMVType, "refine-mv-type=avc");
    if (param->maxSlices > 1)
        TOOLVAL(param->maxSlices, "slices=%d");
    if (param->bEnableLoopFilter)
    {
        if (param->deblockingFilterBetaOffset || param->deblockingFilterTCOffset)
        {
            sprintf(tmp, "deblock(tC=%d:B=%d)", param->deblockingFilterTCOffset, param->deblockingFilterBetaOffset);
            appendtool(param, buf, sizeof(buf), tmp);
        }
        else
            TOOLOPT(param->bEnableLoopFilter, "deblock");
    }
    TOOLOPT(param->bSaoNonDeblocked, "sao-non-deblock");
    TOOLOPT(!param->bSaoNonDeblocked && param->bEnableSAO, "sao");
    TOOLOPT(param->rc.bStatWrite, "stats-write");
    TOOLOPT(param->rc.bStatRead,  "stats-read");
#if ENABLE_HDR10_PLUS
    TOOLOPT(param->toneMapFile != NULL, "dhdr10-info");
#endif
    x265_log(param, X265_LOG_INFO, "tools:%s\n", buf);
    fflush(stderr);
}

char *x265_param2string(x265_param* p, int padx, int pady)
{
    char *buf, *s;

    buf = s = X265_MALLOC(char, MAXPARAMSIZE);
    if (!buf)
        return NULL;

#define BOOL(param, cliopt) \
    s += sprintf(s, " %s", (param) ? cliopt : "no-" cliopt);

    s += sprintf(s, "cpuid=%d", p->cpuid);
    s += sprintf(s, " frame-threads=%d", p->frameNumThreads);
    if (p->numaPools)
        s += sprintf(s, " numa-pools=%s", p->numaPools);
    BOOL(p->bEnableWavefront, "wpp");
    BOOL(p->bDistributeModeAnalysis, "pmode");
    BOOL(p->bDistributeMotionEstimation, "pme");
    BOOL(p->bEnablePsnr, "psnr");
    BOOL(p->bEnableSsim, "ssim");
    s += sprintf(s, " log-level=%d", p->logLevel);
    if (p->csvfn)
        s += sprintf(s, " csvfn=%s csv-log-level=%d", p->csvfn, p->csvLogLevel);
    s += sprintf(s, " bitdepth=%d", p->internalBitDepth);
    s += sprintf(s, " input-csp=%d", p->internalCsp);
    s += sprintf(s, " fps=%u/%u", p->fpsNum, p->fpsDenom);
    s += sprintf(s, " input-res=%dx%d", p->sourceWidth - padx, p->sourceHeight - pady);
    s += sprintf(s, " interlace=%d", p->interlaceMode);
    s += sprintf(s, " total-frames=%d", p->totalFrames);
    s += sprintf(s, " level-idc=%d", p->levelIdc);
    s += sprintf(s, " high-tier=%d", p->bHighTier);
    s += sprintf(s, " uhd-bd=%d", p->uhdBluray);
    s += sprintf(s, " ref=%d", p->maxNumReferences);
    BOOL(p->bAllowNonConformance, "allow-non-conformance");
    BOOL(p->bRepeatHeaders, "repeat-headers");
    BOOL(p->bAnnexB, "annexb");
    BOOL(p->bEnableAccessUnitDelimiters, "aud");
    BOOL(p->bEmitHRDSEI, "hrd");
    BOOL(p->bEmitInfoSEI, "info");
    s += sprintf(s, " hash=%d", p->decodedPictureHashSEI);
    BOOL(p->bEnableTemporalSubLayers, "temporal-layers");
    BOOL(p->bOpenGOP, "open-gop");
    s += sprintf(s, " min-keyint=%d", p->keyframeMin);
    s += sprintf(s, " keyint=%d", p->keyframeMax);
    s += sprintf(s, " bframes=%d", p->bframes);
    s += sprintf(s, " b-adapt=%d", p->bFrameAdaptive);
    BOOL(p->bBPyramid, "b-pyramid");
    s += sprintf(s, " bframe-bias=%d", p->bFrameBias);
    s += sprintf(s, " rc-lookahead=%d", p->lookaheadDepth);
    s += sprintf(s, " lookahead-slices=%d", p->lookaheadSlices);
    s += sprintf(s, " scenecut=%d", p->scenecutThreshold);
    BOOL(p->bIntraRefresh, "intra-refresh");
    s += sprintf(s, " ctu=%d", p->maxCUSize);
    s += sprintf(s, " min-cu-size=%d", p->minCUSize);
    BOOL(p->bEnableRectInter, "rect");
    BOOL(p->bEnableAMP, "amp");
    s += sprintf(s, " max-tu-size=%d", p->maxTUSize);
    s += sprintf(s, " tu-inter-depth=%d", p->tuQTMaxInterDepth);
    s += sprintf(s, " tu-intra-depth=%d", p->tuQTMaxIntraDepth);
    s += sprintf(s, " limit-tu=%d", p->limitTU);
    s += sprintf(s, " rdoq-level=%d", p->rdoqLevel);
    s += sprintf(s, " dynamic-rd=%.2f", p->dynamicRd);
    BOOL(p->bSsimRd, "ssim-rd");
    BOOL(p->bEnableSignHiding, "signhide");
    BOOL(p->bEnableTransformSkip, "tskip");
    s += sprintf(s, " nr-intra=%d", p->noiseReductionIntra);
    s += sprintf(s, " nr-inter=%d", p->noiseReductionInter);
    BOOL(p->bEnableConstrainedIntra, "constrained-intra");
    BOOL(p->bEnableStrongIntraSmoothing, "strong-intra-smoothing");
    s += sprintf(s, " max-merge=%d", p->maxNumMergeCand);
    s += sprintf(s, " limit-refs=%d", p->limitReferences);
    BOOL(p->limitModes, "limit-modes");
    s += sprintf(s, " me=%d", p->searchMethod);
    s += sprintf(s, " subme=%d", p->subpelRefine);
    s += sprintf(s, " merange=%d", p->searchRange);
    BOOL(p->bEnableTemporalMvp, "temporal-mvp");
    BOOL(p->bEnableWeightedPred, "weightp");
    BOOL(p->bEnableWeightedBiPred, "weightb");
    BOOL(p->bSourceReferenceEstimation, "analyze-src-pics");
    BOOL(p->bEnableLoopFilter, "deblock");
    if (p->bEnableLoopFilter)
        s += sprintf(s, "=%d:%d", p->deblockingFilterTCOffset, p->deblockingFilterBetaOffset);
    BOOL(p->bEnableSAO, "sao");
    BOOL(p->bSaoNonDeblocked, "sao-non-deblock");
    s += sprintf(s, " rd=%d", p->rdLevel);
    BOOL(p->bEnableEarlySkip, "early-skip");
    BOOL(p->bEnableRecursionSkip, "rskip");
    BOOL(p->bEnableFastIntra, "fast-intra");
    BOOL(p->bEnableTSkipFast, "tskip-fast");
    BOOL(p->bCULossless, "cu-lossless");
    BOOL(p->bIntraInBFrames, "b-intra");
    BOOL(p->bEnableSplitRdSkip, "splitrd-skip");
    s += sprintf(s, " rdpenalty=%d", p->rdPenalty);
    s += sprintf(s, " psy-rd=%.2f", p->psyRd);
    s += sprintf(s, " psy-rdoq=%.2f", p->psyRdoq);
    BOOL(p->bEnableRdRefine, "rd-refine");
    s += sprintf(s, " analysis-reuse-mode=%d", p->analysisReuseMode);
    BOOL(p->bLossless, "lossless");
    s += sprintf(s, " cbqpoffs=%d", p->cbQpOffset);
    s += sprintf(s, " crqpoffs=%d", p->crQpOffset);
    s += sprintf(s, " rc=%s", p->rc.rateControlMode == X265_RC_ABR ? (
         p->rc.bitrate == p->rc.vbvMaxBitrate ? "cbr" : "abr")
         : p->rc.rateControlMode == X265_RC_CRF ? "crf" : "cqp");
    if (p->rc.rateControlMode == X265_RC_ABR || p->rc.rateControlMode == X265_RC_CRF)
    {
        if (p->rc.rateControlMode == X265_RC_CRF)
            s += sprintf(s, " crf=%.1f", p->rc.rfConstant);
        else
            s += sprintf(s, " bitrate=%d", p->rc.bitrate);
        s += sprintf(s, " qcomp=%.2f qpstep=%d", p->rc.qCompress, p->rc.qpStep);
        s += sprintf(s, " stats-write=%d", p->rc.bStatWrite);
        s += sprintf(s, " stats-read=%d", p->rc.bStatRead);
        if (p->rc.bStatRead)
            s += sprintf(s, " cplxblur=%.1f qblur=%.1f",
            p->rc.complexityBlur, p->rc.qblur);
        if (p->rc.bStatWrite && !p->rc.bStatRead)
            BOOL(p->rc.bEnableSlowFirstPass, "slow-firstpass");
        if (p->rc.vbvBufferSize)
        {
            s += sprintf(s, " vbv-maxrate=%d vbv-bufsize=%d vbv-init=%.1f",
                 p->rc.vbvMaxBitrate, p->rc.vbvBufferSize, p->rc.vbvBufferInit);
            if (p->vbvBufferEnd)
                s += sprintf(s, " vbv-end=%.1f vbv-end-fr-adj=%.1f", p->vbvBufferEnd, p->vbvEndFrameAdjust);
            if (p->rc.rateControlMode == X265_RC_CRF)
                s += sprintf(s, " crf-max=%.1f crf-min=%.1f", p->rc.rfConstantMax, p->rc.rfConstantMin);   
        }
    }
    else if (p->rc.rateControlMode == X265_RC_CQP)
        s += sprintf(s, " qp=%d", p->rc.qp);
    if (!(p->rc.rateControlMode == X265_RC_CQP && p->rc.qp == 0))
    {
        s += sprintf(s, " ipratio=%.2f", p->rc.ipFactor);
        if (p->bframes)
            s += sprintf(s, " pbratio=%.2f", p->rc.pbFactor);
    }
    s += sprintf(s, " aq-mode=%d", p->rc.aqMode);
    s += sprintf(s, " aq-strength=%.2f", p->rc.aqStrength);
    BOOL(p->rc.cuTree, "cutree");
    s += sprintf(s, " zone-count=%d", p->rc.zoneCount);
    if (p->rc.zoneCount)
    {
        for (int i = 0; i < p->rc.zoneCount; ++i)
        {
            s += sprintf(s, " zones: start-frame=%d end-frame=%d",
                 p->rc.zones[i].startFrame, p->rc.zones[i].endFrame);
            if (p->rc.zones[i].bForceQp)
                s += sprintf(s, " qp=%d", p->rc.zones[i].qp);
            else
                s += sprintf(s, " bitrate-factor=%f", p->rc.zones[i].bitrateFactor);
        }
    }
    BOOL(p->rc.bStrictCbr, "strict-cbr");
    s += sprintf(s, " qg-size=%d", p->rc.qgSize);
    BOOL(p->rc.bEnableGrain, "rc-grain");
    s += sprintf(s, " qpmax=%d qpmin=%d", p->rc.qpMax, p->rc.qpMin);
    BOOL(p->rc.bEnableConstVbv, "const-vbv");
    s += sprintf(s, " sar=%d", p->vui.aspectRatioIdc);
    if (p->vui.aspectRatioIdc == X265_EXTENDED_SAR)
        s += sprintf(s, " sar-width : sar-height=%d:%d", p->vui.sarWidth, p->vui.sarHeight);
    s += sprintf(s, " overscan=%d", p->vui.bEnableOverscanInfoPresentFlag);
    if (p->vui.bEnableOverscanInfoPresentFlag)
        s += sprintf(s, " overscan-crop=%d", p->vui.bEnableOverscanAppropriateFlag);
    s += sprintf(s, " videoformat=%d", p->vui.videoFormat);
    s += sprintf(s, " range=%d", p->vui.bEnableVideoFullRangeFlag);
    s += sprintf(s, " colorprim=%d", p->vui.colorPrimaries);
    s += sprintf(s, " transfer=%d", p->vui.transferCharacteristics);
    s += sprintf(s, " colormatrix=%d", p->vui.matrixCoeffs);
    s += sprintf(s, " chromaloc=%d", p->vui.bEnableChromaLocInfoPresentFlag);
    if (p->vui.bEnableChromaLocInfoPresentFlag)
        s += sprintf(s, " chromaloc-top=%d chromaloc-bottom=%d",
        p->vui.chromaSampleLocTypeTopField, p->vui.chromaSampleLocTypeBottomField);
    s += sprintf(s, " display-window=%d", p->vui.bEnableDefaultDisplayWindowFlag);
    if (p->vui.bEnableDefaultDisplayWindowFlag)
        s += sprintf(s, " left=%d top=%d right=%d bottom=%d",
        p->vui.defDispWinLeftOffset, p->vui.defDispWinTopOffset,
        p->vui.defDispWinRightOffset, p->vui.defDispWinBottomOffset);
    if (p->masteringDisplayColorVolume)
        s += sprintf(s, " master-display=%s", p->masteringDisplayColorVolume);
    s += sprintf(s, " max-cll=%hu,%hu", p->maxCLL, p->maxFALL);
    s += sprintf(s, " min-luma=%hu", p->minLuma);
    s += sprintf(s, " max-luma=%hu", p->maxLuma);
    s += sprintf(s, " log2-max-poc-lsb=%d", p->log2MaxPocLsb);
    BOOL(p->bEmitVUITimingInfo, "vui-timing-info");
    BOOL(p->bEmitVUIHRDInfo, "vui-hrd-info");
    s += sprintf(s, " slices=%d", p->maxSlices);
    BOOL(p->bOptQpPPS, "opt-qp-pps");
    BOOL(p->bOptRefListLengthPPS, "opt-ref-list-length-pps");
    BOOL(p->bMultiPassOptRPS, "multi-pass-opt-rps");
    s += sprintf(s, " scenecut-bias=%.2f", p->scenecutBias);
    BOOL(p->bOptCUDeltaQP, "opt-cu-delta-qp");
    BOOL(p->bAQMotion, "aq-motion");
    BOOL(p->bEmitHDRSEI, "hdr");
    BOOL(p->bHDROpt, "hdr-opt");
    BOOL(p->bDhdr10opt, "dhdr10-opt");
    s += sprintf(s, " analysis-reuse-level=%d", p->analysisReuseLevel);
    s += sprintf(s, " scale-factor=%d", p->scaleFactor);
    s += sprintf(s, " refine-intra=%d", p->intraRefine);
    s += sprintf(s, " refine-inter=%d", p->interRefine);
    s += sprintf(s, " refine-mv=%d", p->mvRefine);
    BOOL(p->bLimitSAO, "limit-sao");
    s += sprintf(s, " ctu-info=%d", p->bCTUInfo);
    BOOL(p->bLowPassDct, "lowpass-dct");
    s += sprintf(s, " refine-mv-type=%d", p->bMVType);
    s += sprintf(s, " copy-pic=%d", p->bCopyPicToFrame);
#undef BOOL
    return buf;
}

bool parseLambdaFile(x265_param* param)
{
    if (!param->rc.lambdaFileName)
        return false;

    FILE *lfn = x265_fopen(param->rc.lambdaFileName, "r");
    if (!lfn)
    {
        x265_log_file(param, X265_LOG_ERROR, "unable to read lambda file <%s>\n", param->rc.lambdaFileName);
        return true;
    }

    char line[2048];
    char *toksave = NULL, *tok = NULL, *buf = NULL;

    for (int t = 0; t < 3; t++)
    {
        double *table = t ? x265_lambda2_tab : x265_lambda_tab;

        for (int i = 0; i < QP_MAX_MAX + 1; i++)
        {
            double value;

            do
            {
                if (!tok)
                {
                    /* consume a line of text file */
                    if (!fgets(line, sizeof(line), lfn))
                    {
                        fclose(lfn);

                        if (t < 2)
                        {
                            x265_log(param, X265_LOG_ERROR, "lambda file is incomplete\n");
                            return true;
                        }
                        else
                            return false;
                    }

                    /* truncate at first hash */
                    char *hash = strchr(line, '#');
                    if (hash) *hash = 0;
                    buf = line;
                }

                tok = strtok_r(buf, " ,", &toksave);
                buf = NULL;
                if (tok && sscanf(tok, "%lf", &value) == 1)
                    break;
            }
            while (1);

            if (t == 2)
            {
                x265_log(param, X265_LOG_ERROR, "lambda file contains too many values\n");
                fclose(lfn);
                return true;
            }
            else
                x265_log(param, X265_LOG_DEBUG, "lambda%c[%d] = %lf\n", t ? '2' : ' ', i, value);
            table[i] = value;
        }
    }

    fclose(lfn);
    return false;
}

}
