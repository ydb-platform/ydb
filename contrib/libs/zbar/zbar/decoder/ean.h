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
#ifndef _EAN_H_
#define _EAN_H_

/* state of each parallel decode attempt */
typedef struct ean_pass_s {
    signed char state;          /* module position of w[idx] in symbol */
#define STATE_ADDON 0x40        /*   scanning add-on */
#define STATE_IDX   0x1f        /*   element offset into symbol */
    unsigned char raw[7];       /* decode in process */
} ean_pass_t;

/* EAN/UPC specific decode state */
typedef struct ean_decoder_s {
    ean_pass_t pass[4];         /* state of each parallel decode attempt */
    zbar_symbol_type_t left;   /* current holding buffer contents */
    zbar_symbol_type_t right;
    zbar_symbol_type_t addon;
    unsigned s4;                /* character width */
    signed char buf[18];        /* holding buffer */

    signed char enable;
    unsigned ean13_config;
    unsigned ean8_config;
    unsigned upca_config;
    unsigned upce_config;
    unsigned isbn10_config;
    unsigned isbn13_config;
} ean_decoder_t;

/* reset EAN/UPC pass specific state */
static inline void ean_new_scan (ean_decoder_t *ean)
{
    ean->pass[0].state = ean->pass[1].state = -1;
    ean->pass[2].state = ean->pass[3].state = -1;
    ean->s4 = 0;
}

/* reset all EAN/UPC state */
static inline void ean_reset (ean_decoder_t *ean)
{
    ean_new_scan(ean);
    ean->left = ean->right = ean->addon = ZBAR_NONE;
}

static inline unsigned ean_get_config (ean_decoder_t *ean,
                                       zbar_symbol_type_t sym)
{
    switch(sym & ZBAR_SYMBOL) {
    case ZBAR_EAN13:  return(ean->ean13_config);
    case ZBAR_EAN8:   return(ean->ean8_config);
    case ZBAR_UPCA:   return(ean->upca_config);
    case ZBAR_UPCE:   return(ean->upce_config);
    case ZBAR_ISBN10: return(ean->isbn10_config);
    case ZBAR_ISBN13: return(ean->isbn13_config);
    default:           return(0);
    }
}

/* decode EAN/UPC symbols */
zbar_symbol_type_t _zbar_decode_ean(zbar_decoder_t *dcode);

#endif
