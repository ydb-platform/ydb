/*****************************************************************************
 * md5.h: Calculate MD5
 *****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Min Chen <chenm003@163.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;
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
 * For more information, contact us at chenm003@163.com.
 *****************************************************************************/

#ifndef X265_MD5_H
#define X265_MD5_H

#include "common.h"

namespace X265_NS {
//private x265 namespace

typedef struct MD5Context
{
    uint32_t buf[4];
    uint32_t bits[2];
    unsigned char in[64];
} MD5Context;

void MD5Init(MD5Context *context);
void MD5Update(MD5Context *context, unsigned char *buf, uint32_t len);
void MD5Final(MD5Context *ctx, uint8_t *digest);

class MD5
{
public:

    /**
     * initialize digest state
     */
    MD5()
    {
        MD5Init(&m_state);
    }

    /**
     * compute digest over buf of length len.
     * multiple calls may extend the digest over more data.
     */
    void update(unsigned char *buf, unsigned len)
    {
        MD5Update(&m_state, buf, len);
    }

    /**
     * flush any outstanding MD5 data, write the digest into digest.
     */
    void finalize(unsigned char digest[16])
    {
        MD5Final(&m_state, digest);
    }

private:

    MD5Context m_state;
};
}

#endif // ifndef X265_MD5_H
