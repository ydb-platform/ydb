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

#ifndef X265_SEI_H
#define X265_SEI_H

#include "common.h"
#include "bitstream.h"
#include "slice.h"

namespace X265_NS {
// private namespace

class SEI : public SyntaxElementWriter
{
public:
    /* SEI users call write() to marshal an SEI to a bitstream.
     * The write() method calls writeSEI() which encodes the header */
    void write(Bitstream& bs, const SPS& sps);

    void setSize(uint32_t size);
    virtual ~SEI() {}
protected:
    SEIPayloadType  m_payloadType;
    uint32_t        m_payloadSize;
    virtual void writeSEI(const SPS&) = 0;
    void writeByteAlign();
};

class SEIuserDataUnregistered : public SEI
{
public:
    SEIuserDataUnregistered() : m_userData(NULL)
    {
        m_payloadType = USER_DATA_UNREGISTERED;
        m_payloadSize = 0;
    }
    static const uint8_t m_uuid_iso_iec_11578[16];
    uint8_t *m_userData;
    void writeSEI(const SPS&)
    {
        for (uint32_t i = 0; i < 16; i++)
            WRITE_CODE(m_uuid_iso_iec_11578[i], 8, "sei.uuid_iso_iec_11578[i]");
        for (uint32_t i = 0; i < m_payloadSize; i++)
            WRITE_CODE(m_userData[i], 8, "user_data");
    }
};

class SEIMasteringDisplayColorVolume : public SEI
{
public:
    SEIMasteringDisplayColorVolume()
    {
        m_payloadType = MASTERING_DISPLAY_INFO;
        m_payloadSize = (8 * 2 + 2 * 4);
    }
    uint16_t displayPrimaryX[3];
    uint16_t displayPrimaryY[3];
    uint16_t whitePointX, whitePointY;
    uint32_t maxDisplayMasteringLuminance;
    uint32_t minDisplayMasteringLuminance;
    bool parse(const char* value)
    {
        return sscanf(value, "G(%hu,%hu)B(%hu,%hu)R(%hu,%hu)WP(%hu,%hu)L(%u,%u)",
                      &displayPrimaryX[0], &displayPrimaryY[0],
                      &displayPrimaryX[1], &displayPrimaryY[1],
                      &displayPrimaryX[2], &displayPrimaryY[2],
                      &whitePointX, &whitePointY,
                      &maxDisplayMasteringLuminance, &minDisplayMasteringLuminance) == 10;
    }
    void writeSEI(const SPS&)
    {
        for (uint32_t i = 0; i < 3; i++)
        {
            WRITE_CODE(displayPrimaryX[i], 16, "display_primaries_x[ c ]");
            WRITE_CODE(displayPrimaryY[i], 16, "display_primaries_y[ c ]");
        }
        WRITE_CODE(whitePointX, 16, "white_point_x");
        WRITE_CODE(whitePointY, 16, "white_point_y");
        WRITE_CODE(maxDisplayMasteringLuminance, 32, "max_display_mastering_luminance");
        WRITE_CODE(minDisplayMasteringLuminance, 32, "min_display_mastering_luminance");
    }
};

class SEIContentLightLevel : public SEI
{
public:
    SEIContentLightLevel()
    {
        m_payloadType = CONTENT_LIGHT_LEVEL_INFO;
        m_payloadSize = 4;
    }
    uint16_t max_content_light_level;
    uint16_t max_pic_average_light_level;
    void writeSEI(const SPS&)
    {
        WRITE_CODE(max_content_light_level,     16, "max_content_light_level");
        WRITE_CODE(max_pic_average_light_level, 16, "max_pic_average_light_level");
    }
};

class SEIDecodedPictureHash : public SEI
{
public:
    SEIDecodedPictureHash()
    {
        m_payloadType = DECODED_PICTURE_HASH;
        m_payloadSize = 0;
    }
    enum Method
    {
        MD5,
        CRC,
        CHECKSUM,
    } m_method;
    uint8_t m_digest[3][16];
    void writeSEI(const SPS& sps)
    {
        int planes = (sps.chromaFormatIdc != X265_CSP_I400) ? 3 : 1;
        WRITE_CODE(m_method, 8, "hash_type");
        for (int yuvIdx = 0; yuvIdx < planes; yuvIdx++)
        {
            if (m_method == MD5)
            {
                for (uint32_t i = 0; i < 16; i++)
                    WRITE_CODE(m_digest[yuvIdx][i], 8, "picture_md5");
            }
            else if (m_method == CRC)
            {
                uint32_t val = (m_digest[yuvIdx][0] << 8) + m_digest[yuvIdx][1];
                WRITE_CODE(val, 16, "picture_crc");
            }
            else if (m_method == CHECKSUM)
            {
                uint32_t val = (m_digest[yuvIdx][0] << 24) + (m_digest[yuvIdx][1] << 16) + (m_digest[yuvIdx][2] << 8) + m_digest[yuvIdx][3];
                WRITE_CODE(val, 32, "picture_checksum");
            }
        }
    }
};

class SEIActiveParameterSets : public SEI
{
public:
    SEIActiveParameterSets()
    {
        m_payloadType = ACTIVE_PARAMETER_SETS;
        m_payloadSize = 0;
    }
    bool m_selfContainedCvsFlag;
    bool m_noParamSetUpdateFlag;

    void writeSEI(const SPS&)
    {
        WRITE_CODE(0, 4, "active_vps_id");
        WRITE_FLAG(m_selfContainedCvsFlag, "self_contained_cvs_flag");
        WRITE_FLAG(m_noParamSetUpdateFlag, "no_param_set_update_flag");
        WRITE_UVLC(0, "num_sps_ids_minus1");
        WRITE_UVLC(0, "active_seq_param_set_id");
        writeByteAlign();
    }
};

class SEIBufferingPeriod : public SEI
{
public:
    SEIBufferingPeriod()
        : m_cpbDelayOffset(0)
        , m_dpbDelayOffset(0)
        , m_auCpbRemovalDelayDelta(1)
    {
        m_payloadType = BUFFERING_PERIOD;
        m_payloadSize = 0;
    }
    bool     m_cpbDelayOffset;
    bool     m_dpbDelayOffset;
    uint32_t m_initialCpbRemovalDelay;
    uint32_t m_initialCpbRemovalDelayOffset;
    uint32_t m_auCpbRemovalDelayDelta;

    void writeSEI(const SPS& sps)
    {
        const HRDInfo& hrd = sps.vuiParameters.hrdParameters;

        WRITE_UVLC(0, "bp_seq_parameter_set_id");
        WRITE_FLAG(0, "rap_cpb_params_present_flag");
        WRITE_FLAG(0, "concatenation_flag");
        WRITE_CODE(m_auCpbRemovalDelayDelta - 1,   hrd.cpbRemovalDelayLength,       "au_cpb_removal_delay_delta_minus1");
        WRITE_CODE(m_initialCpbRemovalDelay,       hrd.initialCpbRemovalDelayLength,        "initial_cpb_removal_delay");
        WRITE_CODE(m_initialCpbRemovalDelayOffset, hrd.initialCpbRemovalDelayLength, "initial_cpb_removal_delay_offset");

        writeByteAlign();
    }
};

class SEIPictureTiming : public SEI
{
public:
    SEIPictureTiming()
    {
        m_payloadType = PICTURE_TIMING;
        m_payloadSize = 0;
    }
    uint32_t  m_picStruct;
    uint32_t  m_sourceScanType;
    bool      m_duplicateFlag;

    uint32_t  m_auCpbRemovalDelay;
    uint32_t  m_picDpbOutputDelay;

    void writeSEI(const SPS& sps)
    {
        const VUI *vui = &sps.vuiParameters;
        const HRDInfo *hrd = &vui->hrdParameters;

        if (vui->frameFieldInfoPresentFlag)
        {
            WRITE_CODE(m_picStruct, 4,          "pic_struct");
            WRITE_CODE(m_sourceScanType, 2,     "source_scan_type");
            WRITE_FLAG(m_duplicateFlag,         "duplicate_flag");
        }

        if (vui->hrdParametersPresentFlag)
        {
            WRITE_CODE(m_auCpbRemovalDelay - 1, hrd->cpbRemovalDelayLength, "au_cpb_removal_delay_minus1");
            WRITE_CODE(m_picDpbOutputDelay, hrd->dpbOutputDelayLength, "pic_dpb_output_delay");
            /* Removed sub-pic signaling June 2014 */
        }
        writeByteAlign();
    }
};

class SEIRecoveryPoint : public SEI
{
public:
    int  m_recoveryPocCnt;
    bool m_exactMatchingFlag;
    bool m_brokenLinkFlag;

    void writeSEI(const SPS&)
    {
        WRITE_SVLC(m_recoveryPocCnt,    "recovery_poc_cnt");
        WRITE_FLAG(m_exactMatchingFlag, "exact_matching_flag");
        WRITE_FLAG(m_brokenLinkFlag,    "broken_link_flag");
        writeByteAlign();
    }
};

//seongnam.oh@samsung.com :: for the Creative Intent Meta Data Encoding
class SEICreativeIntentMeta : public SEI
{
public:
    SEICreativeIntentMeta()
    {
        m_payloadType = USER_DATA_REGISTERED_ITU_T_T35;
        m_payloadSize = 0;
    }

    uint8_t *m_payload;

    // daniel.vt@samsung.com :: for the Creative Intent Meta Data Encoding ( seongnam.oh@samsung.com )
    void writeSEI(const SPS&)
    {
        if (!m_payload)
            return;

        uint32_t i = 0;
        for (; i < m_payloadSize; ++i)
            WRITE_CODE(m_payload[i], 8, "creative_intent_metadata");
    }
};
}
#endif // ifndef X265_SEI_H
