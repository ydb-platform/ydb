/* lbase64.c - routines for dealing with base64 strings */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2022 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* Portions Copyright (c) 1992-1996 Regents of the University of Michigan.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that this notice is preserved and that due credit is given
 * to the University of Michigan at Ann Arbor.  The name of the
 * University may not be used to endorse or promote products derived
 * from this software without specific prior written permission.  This
 * software is provided ``as is'' without express or implied warranty.
 */
/* This work was originally developed by the University of Michigan
 * and distributed as part of U-MICH LDAP.
 */

#include "portable.h"

#include "ldap-int.h"

#define RIGHT2			0x03
#define RIGHT4			0x0f

static const unsigned char b642nib[0x80] = {
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0x3e, 0xff, 0xff, 0xff, 0x3f,
	0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b,
	0x3c, 0x3d, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
	0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
	0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
	0x17, 0x18, 0x19, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
	0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
	0x31, 0x32, 0x33, 0xff, 0xff, 0xff, 0xff, 0xff
};

int
ldap_int_decode_b64_inplace( struct berval *value )
{
	char	*p, *end, *byte;
	char	nib;

	byte = value->bv_val;
	end = value->bv_val + value->bv_len;

	for ( p = value->bv_val, value->bv_len = 0;
		p < end;
		p += 4, value->bv_len += 3 )
	{
		int i;
		for ( i = 0; i < 4; i++ ) {
			if ( p[i] != '=' && (p[i] & 0x80 ||
			    b642nib[ p[i] & 0x7f ] > 0x3f) ) {
				Debug2( LDAP_DEBUG_ANY,
					_("ldap_pvt_decode_b64_inplace: invalid base64 encoding"
					" char (%c) 0x%x\n"), p[i], p[i] );
				return( -1 );
			}
		}

		/* first digit */
		nib = b642nib[ p[0] & 0x7f ];
		byte[0] = nib << 2;
		/* second digit */
		nib = b642nib[ p[1] & 0x7f ];
		byte[0] |= nib >> 4;
		byte[1] = (nib & RIGHT4) << 4;
		/* third digit */
		if ( p[2] == '=' ) {
			value->bv_len += 1;
			break;
		}
		nib = b642nib[ p[2] & 0x7f ];
		byte[1] |= nib >> 2;
		byte[2] = (nib & RIGHT2) << 6;
		/* fourth digit */
		if ( p[3] == '=' ) {
			value->bv_len += 2;
			break;
		}
		nib = b642nib[ p[3] & 0x7f ];
		byte[2] |= nib;

		byte += 3;
	}
	value->bv_val[ value->bv_len ] = '\0';

    return LDAP_SUCCESS;
}
