/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
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

#include "portable.h"

#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/stdarg.h>
#include "lber-int.h"

char ber_pvt_opt_on;	/* used to get a non-NULL address for *_OPT_ON */

struct lber_options ber_int_options = {
	LBER_UNINITIALIZED, 0, 0 };

static BerMemoryFunctions	ber_int_memory_fns_datum;

int
ber_get_option(
	void	*item,
	int		option,
	void	*outvalue)
{
	const BerElement *ber;
	const Sockbuf *sb;

	if(outvalue == NULL) {
		/* no place to get to */
		ber_errno = LBER_ERROR_PARAM;
		return LBER_OPT_ERROR;
	}

	if(item == NULL) {
		switch ( option ) {
		case LBER_OPT_BER_DEBUG:
			* (int *) outvalue = ber_int_debug;
			return LBER_OPT_SUCCESS;

		case LBER_OPT_MEMORY_INUSE:
			/* The memory inuse is a global variable on kernel implementations.
			 * This means that memory debug is shared by all LDAP processes
			 * so for this variable to have much meaning, only one LDAP process
			 * should be running and memory inuse should be initialized to zero
			 * using the lber_set_option() function during startup.
			 * The counter is not accurate for multithreaded ldap applications.
			 */
#ifdef LDAP_MEMORY_DEBUG
			* (int *) outvalue = ber_int_meminuse;
			return LBER_OPT_SUCCESS;
#else
			return LBER_OPT_ERROR;
#endif

		case LBER_OPT_LOG_PRINT_FILE:
			*((FILE**)outvalue) = (FILE*)ber_pvt_err_file;
			return LBER_OPT_SUCCESS;

		case LBER_OPT_LOG_PRINT_FN:
			*(BER_LOG_PRINT_FN *)outvalue = ber_pvt_log_print;
			return LBER_OPT_SUCCESS;
		}

		ber_errno = LBER_ERROR_PARAM;
		return LBER_OPT_ERROR;
	}

	ber = item;
	sb = item;

	switch(option) {
	case LBER_OPT_BER_OPTIONS:
		assert( LBER_VALID( ber ) );
		* (int *) outvalue = ber->ber_options;
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_DEBUG:
		assert( LBER_VALID( ber ) );
		* (int *) outvalue = ber->ber_debug;
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_REMAINING_BYTES:
		assert( LBER_VALID( ber ) );
		*((ber_len_t *) outvalue) = ber_pvt_ber_remaining(ber);
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_TOTAL_BYTES:
		assert( LBER_VALID( ber ) );
		*((ber_len_t *) outvalue) = ber_pvt_ber_total(ber);
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_BYTES_TO_WRITE:
		assert( LBER_VALID( ber ) );
		*((ber_len_t *) outvalue) = ber_pvt_ber_write(ber);
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_MEMCTX:
		assert( LBER_VALID( ber ) );
		*((void **) outvalue) = ber->ber_memctx;
		return LBER_OPT_SUCCESS;
	
	default:
		/* bad param */
		ber_errno = LBER_ERROR_PARAM;
		break;
	}

	return LBER_OPT_ERROR;
}

int
ber_set_option(
	void	*item,
	int		option,
	LDAP_CONST void	*invalue)
{
	BerElement *ber;
	Sockbuf *sb;

	if(invalue == NULL) {
		/* no place to set from */
		ber_errno = LBER_ERROR_PARAM;
		return LBER_OPT_ERROR;
	}

	if(item == NULL) {
		switch ( option ) {
		case LBER_OPT_BER_DEBUG:
			ber_int_debug = * (const int *) invalue;
			return LBER_OPT_SUCCESS;

		case LBER_OPT_LOG_PRINT_FN:
			ber_pvt_log_print = (BER_LOG_PRINT_FN) invalue;
			return LBER_OPT_SUCCESS;

		case LBER_OPT_LOG_PRINT_FILE:
			ber_pvt_err_file = (void *) invalue;
			return LBER_OPT_SUCCESS;

		case LBER_OPT_MEMORY_INUSE:
			/* The memory inuse is a global variable on kernel implementations.
			 * This means that memory debug is shared by all LDAP processes
			 * so for this variable to have much meaning, only one LDAP process
			 * should be running and memory inuse should be initialized to zero
			 * using the lber_set_option() function during startup.
			 * The counter is not accurate for multithreaded applications.
			 */
#ifdef LDAP_MEMORY_DEBUG
			ber_int_meminuse = * (int *) invalue;
			return LBER_OPT_SUCCESS;
#else
			return LBER_OPT_ERROR;
#endif
		case LBER_OPT_MEMORY_FNS:
			if ( ber_int_memory_fns == NULL )
			{
				const BerMemoryFunctions *f =
					(const BerMemoryFunctions *) invalue;
				/* make sure all functions are provided */
				if(!( f->bmf_malloc && f->bmf_calloc
					&& f->bmf_realloc && f->bmf_free ))
				{
					ber_errno = LBER_ERROR_PARAM;
					return LBER_OPT_ERROR;
				}

				ber_int_memory_fns = &ber_int_memory_fns_datum;

				AC_MEMCPY(ber_int_memory_fns, f,
					 sizeof(BerMemoryFunctions));

				return LBER_OPT_SUCCESS;
			}
			break;

		case LBER_OPT_LOG_PROC:
			ber_int_log_proc = (BER_LOG_FN)invalue;
			return LBER_OPT_SUCCESS;
		}

		ber_errno = LBER_ERROR_PARAM;
		return LBER_OPT_ERROR;
	}

	ber = item;
	sb = item;

	switch(option) {
	case LBER_OPT_BER_OPTIONS:
		assert( LBER_VALID( ber ) );
		ber->ber_options = * (const int *) invalue;
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_DEBUG:
		assert( LBER_VALID( ber ) );
		ber->ber_debug = * (const int *) invalue;
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_REMAINING_BYTES:
		assert( LBER_VALID( ber ) );
		ber->ber_end = &ber->ber_ptr[* (const ber_len_t *) invalue];
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_TOTAL_BYTES:
		assert( LBER_VALID( ber ) );
		ber->ber_end = &ber->ber_buf[* (const ber_len_t *) invalue];
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_BYTES_TO_WRITE:
		assert( LBER_VALID( ber ) );
		ber->ber_ptr = &ber->ber_buf[* (const ber_len_t *) invalue];
		return LBER_OPT_SUCCESS;

	case LBER_OPT_BER_MEMCTX:
		assert( LBER_VALID( ber ) );
		ber->ber_memctx = *(void **)invalue;
		return LBER_OPT_SUCCESS;

	default:
		/* bad param */
		ber_errno = LBER_ERROR_PARAM;
		break;
	}

	return LBER_OPT_ERROR;
}
