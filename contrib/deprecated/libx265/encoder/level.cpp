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
#include "slice.h"
#include "level.h"

namespace X265_NS {
typedef struct
{
    uint32_t maxLumaSamples;
    uint32_t maxLumaSamplesPerSecond;
    uint32_t maxBitrateMain;
    uint32_t maxBitrateHigh;
    uint32_t maxCpbSizeMain;
    uint32_t maxCpbSizeHigh;
    uint32_t minCompressionRatio;
    Level::Name levelEnum;
    const char* name;
    int levelIdc;
} LevelSpec;

LevelSpec levels[] =
{
    { 36864,    552960,     128,      MAX_UINT, 350,    MAX_UINT, 2, Level::LEVEL1,   "1",   10 },
    { 122880,   3686400,    1500,     MAX_UINT, 1500,   MAX_UINT, 2, Level::LEVEL2,   "2",   20 },
    { 245760,   7372800,    3000,     MAX_UINT, 3000,   MAX_UINT, 2, Level::LEVEL2_1, "2.1", 21 },
    { 552960,   16588800,   6000,     MAX_UINT, 6000,   MAX_UINT, 2, Level::LEVEL3,   "3",   30 },
    { 983040,   33177600,   10000,    MAX_UINT, 10000,  MAX_UINT, 2, Level::LEVEL3_1, "3.1", 31 },
    { 2228224,  66846720,   12000,    30000,    12000,  30000,    4, Level::LEVEL4,   "4",   40 },
    { 2228224,  133693440,  20000,    50000,    20000,  50000,    4, Level::LEVEL4_1, "4.1", 41 },
    { 8912896,  267386880,  25000,    100000,   25000,  100000,   6, Level::LEVEL5,   "5",   50 },
    { 8912896,  534773760,  40000,    160000,   40000,  160000,   8, Level::LEVEL5_1, "5.1", 51 },
    { 8912896,  1069547520, 60000,    240000,   60000,  240000,   8, Level::LEVEL5_2, "5.2", 52 },
    { 35651584, 1069547520, 60000,    240000,   60000,  240000,   8, Level::LEVEL6,   "6",   60 },
    { 35651584, 2139095040, 120000,   480000,   120000, 480000,   8, Level::LEVEL6_1, "6.1", 61 },
    { 35651584, 4278190080U, 240000,  800000,   240000, 800000,   6, Level::LEVEL6_2, "6.2", 62 },
    { MAX_UINT, MAX_UINT, MAX_UINT, MAX_UINT, MAX_UINT, MAX_UINT, 1, Level::LEVEL8_5, "8.5", 85 },
};

/* determine minimum decoder level required to decode the described video */
void determineLevel(const x265_param &param, VPS& vps)
{
    vps.ptl.onePictureOnlyConstraintFlag = param.totalFrames == 1;
    vps.ptl.intraConstraintFlag = param.keyframeMax <= 1 || vps.ptl.onePictureOnlyConstraintFlag;
    vps.ptl.bitDepthConstraint = param.internalBitDepth;
    vps.ptl.chromaFormatConstraint = param.internalCsp;

    /* TODO: figure out HighThroughput signaling, aka: HbrFactor in section A.4.2, only available
     * for intra-only profiles (vps.ptl.intraConstraintFlag) */
    vps.ptl.lowerBitRateConstraintFlag = true;

    vps.maxTempSubLayers = param.bEnableTemporalSubLayers ? 2 : 1;
    
    if (param.internalCsp == X265_CSP_I420 && param.internalBitDepth <= 10)
    {
        /* Probably an HEVC v1 profile, but must check to be sure */
        if (param.internalBitDepth <= 8)
        {
            if (vps.ptl.onePictureOnlyConstraintFlag)
                vps.ptl.profileIdc = Profile::MAINSTILLPICTURE;
            else if (vps.ptl.intraConstraintFlag)
                vps.ptl.profileIdc = Profile::MAINREXT; /* Main Intra */
            else 
                vps.ptl.profileIdc = Profile::MAIN;
        }
        else if (param.internalBitDepth <= 10)
        {
            /* note there is no 10bit still picture profile */
            if (vps.ptl.intraConstraintFlag)
                vps.ptl.profileIdc = Profile::MAINREXT; /* Main10 Intra */
            else
                vps.ptl.profileIdc = Profile::MAIN10;
        }
    }
    else
        vps.ptl.profileIdc = Profile::MAINREXT;

    /* determine which profiles are compatible with this stream */

    memset(vps.ptl.profileCompatibilityFlag, 0, sizeof(vps.ptl.profileCompatibilityFlag));
    vps.ptl.profileCompatibilityFlag[vps.ptl.profileIdc] = true;
    if (vps.ptl.profileIdc == Profile::MAIN10 && param.internalBitDepth == 8)
        vps.ptl.profileCompatibilityFlag[Profile::MAIN] = true;
    else if (vps.ptl.profileIdc == Profile::MAIN)
        vps.ptl.profileCompatibilityFlag[Profile::MAIN10] = true;
    else if (vps.ptl.profileIdc == Profile::MAINSTILLPICTURE)
    {
        vps.ptl.profileCompatibilityFlag[Profile::MAIN] = true;
        vps.ptl.profileCompatibilityFlag[Profile::MAIN10] = true;
    }
    else if (vps.ptl.profileIdc == Profile::MAINREXT)
        vps.ptl.profileCompatibilityFlag[Profile::MAINREXT] = true;

    uint32_t lumaSamples = param.sourceWidth * param.sourceHeight;
    uint32_t samplesPerSec = (uint32_t)(lumaSamples * ((double)param.fpsNum / param.fpsDenom));
    uint32_t bitrate = param.rc.vbvMaxBitrate ? param.rc.vbvMaxBitrate : param.rc.bitrate;

    const uint32_t MaxDpbPicBuf = 6;
    vps.ptl.levelIdc = Level::NONE;
    vps.ptl.tierFlag = Level::MAIN;

    const size_t NumLevels = sizeof(levels) / sizeof(levels[0]);
    uint32_t i;
    if (param.bLossless)
    {
        i = 13;
        vps.ptl.minCrForLevel = 1;
        vps.ptl.maxLumaSrForLevel = MAX_UINT;
        vps.ptl.levelIdc = Level::LEVEL8_5;
        vps.ptl.tierFlag = Level::MAIN;
    }
    else if (param.uhdBluray)
    {
        i = 8;
        vps.ptl.levelIdc = levels[i].levelEnum;
        vps.ptl.tierFlag = Level::HIGH;
        vps.ptl.minCrForLevel = levels[i].minCompressionRatio;
        vps.ptl.maxLumaSrForLevel = levels[i].maxLumaSamplesPerSecond;
    }
    else for (i = 0; i < NumLevels; i++)
    {
        if (lumaSamples > levels[i].maxLumaSamples)
            continue;
        else if (samplesPerSec > levels[i].maxLumaSamplesPerSecond)
            continue;
        else if (bitrate > levels[i].maxBitrateMain && levels[i].maxBitrateHigh == MAX_UINT)
            continue;
        else if (bitrate > levels[i].maxBitrateHigh)
            continue;
        else if (param.sourceWidth > sqrt(levels[i].maxLumaSamples * 8.0f))
            continue;
        else if (param.sourceHeight > sqrt(levels[i].maxLumaSamples * 8.0f))
            continue;
        else if (param.levelIdc && param.levelIdc != levels[i].levelIdc)
            continue;
        uint32_t maxDpbSize = MaxDpbPicBuf;

        if (lumaSamples <= (levels[i].maxLumaSamples >> 2))
            maxDpbSize = X265_MIN(4 * MaxDpbPicBuf, 16);
        else if (lumaSamples <= (levels[i].maxLumaSamples >> 1))
            maxDpbSize = X265_MIN(2 * MaxDpbPicBuf, 16);
        else if (lumaSamples <= ((3 * levels[i].maxLumaSamples) >> 2))
            maxDpbSize = X265_MIN((4 * MaxDpbPicBuf) / 3, 16);

        /* The value of sps_max_dec_pic_buffering_minus1[ HighestTid ] + 1 shall be less than
         * or equal to MaxDpbSize */
        if (vps.maxDecPicBuffering > maxDpbSize)
            continue;

        /* For level 5 and higher levels, the value of CtbSizeY shall be equal to 32 or 64 */
        if (levels[i].levelEnum >= Level::LEVEL5 && param.maxCUSize < 32)
        {
            x265_log(&param, X265_LOG_WARNING, "level %s detected, but CTU size 16 is non-compliant\n", levels[i].name);
            vps.ptl.profileIdc = Profile::NONE;
            vps.ptl.levelIdc = Level::NONE;
            vps.ptl.tierFlag = Level::MAIN;
            x265_log(&param, X265_LOG_INFO, "NONE profile, Level-NONE (Main tier)\n");
            return;
        }

        /* The value of NumPocTotalCurr shall be less than or equal to 8 */
        int numPocTotalCurr = param.maxNumReferences + vps.numReorderPics;
        if (numPocTotalCurr > 8)
        {
            x265_log(&param, X265_LOG_WARNING, "level %s detected, but NumPocTotalCurr (total references) is non-compliant\n", levels[i].name);
            vps.ptl.profileIdc = Profile::NONE;
            vps.ptl.levelIdc = Level::NONE;
            vps.ptl.tierFlag = Level::MAIN;
            x265_log(&param, X265_LOG_INFO, "NONE profile, Level-NONE (Main tier)\n");
            return;
        }

#define CHECK_RANGE(value, main, high) (high != MAX_UINT && value > main && value <= high)

        if (CHECK_RANGE(bitrate, levels[i].maxBitrateMain, levels[i].maxBitrateHigh) ||
            CHECK_RANGE((uint32_t)param.rc.vbvBufferSize, levels[i].maxCpbSizeMain, levels[i].maxCpbSizeHigh))
        {
            /* The bitrate or buffer size are out of range for Main tier, but in
             * range for High tier. If the user allowed High tier then give
             * them High tier at this level.  Otherwise allow the loop to
             * progress to the Main tier of the next level */
            if (param.bHighTier)
                vps.ptl.tierFlag = Level::HIGH;
            else
                continue;
        }
        else
            vps.ptl.tierFlag = Level::MAIN;
#undef CHECK_RANGE

        vps.ptl.levelIdc = levels[i].levelEnum;
        vps.ptl.minCrForLevel = levels[i].minCompressionRatio;
        vps.ptl.maxLumaSrForLevel = levels[i].maxLumaSamplesPerSecond;
        break;
    }

    static const char *profiles[] = { "None", "Main", "Main 10", "Main Still Picture", "RExt" };
    static const char *tiers[]    = { "Main", "High" };

    char profbuf[64];
    strcpy(profbuf, profiles[vps.ptl.profileIdc]);

    bool bStillPicture = false;
    if (vps.ptl.profileIdc == Profile::MAINREXT)
    {
        if (vps.ptl.bitDepthConstraint > 12 && vps.ptl.intraConstraintFlag)
        {
            if (vps.ptl.onePictureOnlyConstraintFlag)
            {
                strcpy(profbuf, "Main 4:4:4 16 Still Picture");
                bStillPicture = true;
            }
            else
                strcpy(profbuf, "Main 4:4:4 16");
        }
        else if (param.internalCsp == X265_CSP_I420)
        {
            X265_CHECK(vps.ptl.intraConstraintFlag || vps.ptl.bitDepthConstraint > 10, "rext fail\n");
            if (vps.ptl.bitDepthConstraint <= 8)
                strcpy(profbuf, "Main");
            else if (vps.ptl.bitDepthConstraint <= 10)
                strcpy(profbuf, "Main 10");
            else if (vps.ptl.bitDepthConstraint <= 12)
                strcpy(profbuf, "Main 12");
        }
        else if (param.internalCsp == X265_CSP_I422)
        {
            /* there is no Main 4:2:2 profile, so it must be signaled as Main10 4:2:2 */
            if (param.internalBitDepth <= 10)
                strcpy(profbuf, "Main 4:2:2 10");
            else if (vps.ptl.bitDepthConstraint <= 12)
                strcpy(profbuf, "Main 4:2:2 12");
        }
        else if (param.internalCsp == X265_CSP_I444)
        {
            if (vps.ptl.bitDepthConstraint <= 8)
            {
                if (vps.ptl.onePictureOnlyConstraintFlag)
                {
                    strcpy(profbuf, "Main 4:4:4 Still Picture");
                    bStillPicture = true;
                }
                else
                    strcpy(profbuf, "Main 4:4:4");
            }
            else if (vps.ptl.bitDepthConstraint <= 10)
                strcpy(profbuf, "Main 4:4:4 10");
            else if (vps.ptl.bitDepthConstraint <= 12)
                strcpy(profbuf, "Main 4:4:4 12");
        }
        else
            strcpy(profbuf, "Unknown");

        if (vps.ptl.intraConstraintFlag && !bStillPicture)
            strcat(profbuf, " Intra");
    }
    x265_log(&param, X265_LOG_INFO, "%s profile, Level-%s (%s tier)\n",
             profbuf, levels[i].name, tiers[vps.ptl.tierFlag]);
}

/* enforce a maximum decoder level requirement, in other words assure that a
 * decoder of the specified level may decode the video about to be created.
 * Lower parameters where necessary to ensure the video will be decodable by a
 * decoder meeting this level of requirement.  Some parameters (resolution and
 * frame rate) are non-negotiable and thus this function may fail. In those
 * circumstances it will be quite noisy */
bool enforceLevel(x265_param& param, VPS& vps)
{
    vps.numReorderPics = (param.bBPyramid && param.bframes > 1) ? 2 : !!param.bframes;
    vps.maxDecPicBuffering = X265_MIN(MAX_NUM_REF, X265_MAX(vps.numReorderPics + 2, (uint32_t)param.maxNumReferences) + 1);

    /* no level specified by user, just auto-detect from the configuration */
    if (param.levelIdc <= 0)
        return true;

    uint32_t level = 0;
    while (levels[level].levelIdc != param.levelIdc && level + 1 < sizeof(levels) / sizeof(levels[0]))
        level++;
    if (levels[level].levelIdc != param.levelIdc)
    {
        x265_log(&param, X265_LOG_ERROR, "specified level %d does not exist\n", param.levelIdc);
        return false;
    }

    LevelSpec& l = levels[level];

    //highTier is allowed for this level and has not been explicitly disabled. This does not mean it is the final chosen tier
    bool allowHighTier = l.maxBitrateHigh < MAX_UINT && param.bHighTier;

    uint32_t lumaSamples = param.sourceWidth * param.sourceHeight;
    uint32_t samplesPerSec = (uint32_t)(lumaSamples * ((double)param.fpsNum / param.fpsDenom));
    bool ok = true;
    if (lumaSamples > l.maxLumaSamples)
        ok = false;
    else if (param.sourceWidth > sqrt(l.maxLumaSamples * 8.0f))
        ok = false;
    else if (param.sourceHeight > sqrt(l.maxLumaSamples * 8.0f))
        ok = false;
    if (!ok)
    {
        x265_log(&param, X265_LOG_ERROR, "picture dimensions are out of range for specified level\n");
        return false;
    }
    else if (samplesPerSec > l.maxLumaSamplesPerSecond)
    {
        x265_log(&param, X265_LOG_ERROR, "frame rate is out of range for specified level\n");
        return false;
    }

    /* Adjustments of Bitrate, VBV buffer size, refs will be triggered only if specified params do not fit 
     * within the max limits of that level (high tier if allowed, main otherwise)
     */

    if ((uint32_t)param.rc.vbvMaxBitrate > (allowHighTier ? l.maxBitrateHigh : l.maxBitrateMain))
    {
        param.rc.vbvMaxBitrate = allowHighTier ? l.maxBitrateHigh : l.maxBitrateMain;
        x265_log(&param, X265_LOG_WARNING, "lowering VBV max bitrate to %dKbps\n", param.rc.vbvMaxBitrate);
    }
    if ((uint32_t)param.rc.vbvBufferSize > (allowHighTier ? l.maxCpbSizeHigh : l.maxCpbSizeMain))
    {
        param.rc.vbvBufferSize = allowHighTier ? l.maxCpbSizeHigh : l.maxCpbSizeMain;
        x265_log(&param, X265_LOG_WARNING, "lowering VBV buffer size to %dKb\n", param.rc.vbvBufferSize);
    }

    switch (param.rc.rateControlMode)
    {
    case X265_RC_ABR:
        if ((uint32_t)param.rc.bitrate > (allowHighTier ? l.maxBitrateHigh : l.maxBitrateMain))
        {
            param.rc.bitrate =  allowHighTier ? l.maxBitrateHigh : l.maxBitrateMain;
            x265_log(&param, X265_LOG_WARNING, "lowering target bitrate to High tier limit of %dKbps\n", param.rc.bitrate);
        }
        break;

    case X265_RC_CQP:
        x265_log(&param, X265_LOG_ERROR, "Constant QP is inconsistent with specifying a decoder level, no bitrate guarantee is possible.\n");
        return false;

    case X265_RC_CRF:
        if (!param.rc.vbvBufferSize || !param.rc.vbvMaxBitrate)
        {
            if (!param.rc.vbvMaxBitrate)
                param.rc.vbvMaxBitrate = allowHighTier ? l.maxBitrateHigh : l.maxBitrateMain;
            if (!param.rc.vbvBufferSize)
                param.rc.vbvBufferSize = allowHighTier ? l.maxCpbSizeHigh : l.maxCpbSizeMain;
            x265_log(&param, X265_LOG_WARNING, "Specifying a decoder level with constant rate factor rate-control requires\n");
            x265_log(&param, X265_LOG_WARNING, "enabling VBV with vbv-bufsize=%dkb vbv-maxrate=%dkbps. VBV outputs are non-deterministic!\n",
                     param.rc.vbvBufferSize, param.rc.vbvMaxBitrate);
        }
        break;

    default:
        x265_log(&param, X265_LOG_ERROR, "Unknown rate control mode is inconsistent with specifying a decoder level\n");
        return false;
    }

    /* The value of sps_max_dec_pic_buffering_minus1[ HighestTid ] + 1 shall be less than or equal to MaxDpbSize */
    const uint32_t MaxDpbPicBuf = 6;
    uint32_t maxDpbSize = MaxDpbPicBuf;
    if (!param.uhdBluray) /* Do not change MaxDpbPicBuf for UHD-Bluray */
    {
        if (lumaSamples <= (l.maxLumaSamples >> 2))
            maxDpbSize = X265_MIN(4 * MaxDpbPicBuf, 16);
        else if (lumaSamples <= (l.maxLumaSamples >> 1))
            maxDpbSize = X265_MIN(2 * MaxDpbPicBuf, 16);
        else if (lumaSamples <= ((3 * l.maxLumaSamples) >> 2))
            maxDpbSize = X265_MIN((4 * MaxDpbPicBuf) / 3, 16);
    }

    int savedRefCount = param.maxNumReferences;
    while (vps.maxDecPicBuffering > maxDpbSize && param.maxNumReferences > 1)
    {
        param.maxNumReferences--;
        vps.maxDecPicBuffering = X265_MIN(MAX_NUM_REF, X265_MAX(vps.numReorderPics + 1, (uint32_t)param.maxNumReferences) + 1);
    }
    if (param.maxNumReferences != savedRefCount)
        x265_log(&param, X265_LOG_WARNING, "Lowering max references to %d to meet level requirement\n", param.maxNumReferences);

    /* For level 5 and higher levels, the value of CtbSizeY shall be equal to 32 or 64 */
    if (param.levelIdc >= 50 && param.maxCUSize < 32)
    {
        param.maxCUSize = 32;
        x265_log(&param, X265_LOG_WARNING, "Levels 5.0 and above require a maximum CTU size of at least 32, using --ctu 32\n");
    }

    /* The value of NumPocTotalCurr shall be less than or equal to 8 */
    int numPocTotalCurr = param.maxNumReferences + !!param.bframes;
    if (numPocTotalCurr > 8)
    {
        param.maxNumReferences = 8 - !!param.bframes;
        x265_log(&param, X265_LOG_WARNING, "Lowering max references to %d to meet numPocTotalCurr requirement\n", param.maxNumReferences);
    }

    return true;
}
}

#if EXPORT_C_API

/* these functions are exported as C functions (default) */
using namespace X265_NS;
extern "C" {

#else

/* these functions exist within private namespace (multilib) */
namespace X265_NS {

#endif

int x265_param_apply_profile(x265_param *param, const char *profile)
{
    if (!param || !profile)
        return 0;

    /* Check if profile bit-depth requirement is exceeded by internal bit depth */
    bool bInvalidDepth = false;
#if X265_DEPTH > 8
    if (!strcmp(profile, "main") || !strcmp(profile, "mainstillpicture") || !strcmp(profile, "msp") ||
        !strcmp(profile, "main444-8") || !strcmp(profile, "main-intra") ||
        !strcmp(profile, "main444-intra") || !strcmp(profile, "main444-stillpicture"))
        bInvalidDepth = true;
#endif
#if X265_DEPTH > 10
    if (!strcmp(profile, "main10") || !strcmp(profile, "main422-10") || !strcmp(profile, "main444-10") ||
        !strcmp(profile, "main10-intra") || !strcmp(profile, "main422-10-intra") || !strcmp(profile, "main444-10-intra"))
        bInvalidDepth = true;
#endif
#if X265_DEPTH > 12
    if (!strcmp(profile, "main12") || !strcmp(profile, "main422-12") || !strcmp(profile, "main444-12") ||
        !strcmp(profile, "main12-intra") || !strcmp(profile, "main422-12-intra") || !strcmp(profile, "main444-12-intra"))
        bInvalidDepth = true;
#endif

    if (bInvalidDepth)
    {
        x265_log(param, X265_LOG_ERROR, "%s profile not supported, internal bit depth %d.\n", profile, X265_DEPTH);
        return -1;
    }

    size_t l = strlen(profile);
    bool bBoolIntra = (l > 6 && !strcmp(profile + l - 6, "-intra")) ||
                      !strcmp(profile, "mainstillpicture") || !strcmp(profile, "msp");
    if (bBoolIntra)
    {
        /* The profile may be detected as still picture if param->totalFrames is 1 */
        param->keyframeMax = 1;
    }
    
    /* check that input color space is supported by profile */
    if (!strcmp(profile, "main") || !strcmp(profile, "main-intra") ||
        !strcmp(profile, "main10") || !strcmp(profile, "main10-intra") ||
        !strcmp(profile, "main12") || !strcmp(profile, "main12-intra") ||
        !strcmp(profile, "mainstillpicture") || !strcmp(profile, "msp"))
    {
        if (param->internalCsp != X265_CSP_I420)
        {
            x265_log(param, X265_LOG_ERROR, "%s profile not compatible with %s input chroma subsampling.\n",
                     profile, x265_source_csp_names[param->internalCsp]);
            return -1;
        }
    }
    else if (!strcmp(profile, "main422-10") || !strcmp(profile, "main422-10-intra") ||
             !strcmp(profile, "main422-12") || !strcmp(profile, "main422-12-intra"))
    {
        if (param->internalCsp != X265_CSP_I420 && param->internalCsp != X265_CSP_I422)
        {
            x265_log(param, X265_LOG_ERROR, "%s profile not compatible with %s input chroma subsampling.\n",
                     profile, x265_source_csp_names[param->internalCsp]);
            return -1;
        }
    }
    else if (!strcmp(profile, "main444-8") ||
             !strcmp(profile, "main444-intra") || !strcmp(profile, "main444-stillpicture") ||
             !strcmp(profile, "main444-10") || !strcmp(profile, "main444-10-intra") ||
             !strcmp(profile, "main444-12") || !strcmp(profile, "main444-12-intra") ||
             !strcmp(profile, "main444-16-intra") || !strcmp(profile, "main444-16-stillpicture"))
    {
        /* any color space allowed */
    }
    else
    {
        x265_log(param, X265_LOG_ERROR, "unknown profile <%s>\n", profile);
        return -1;
    }

    return 0;
}
}
