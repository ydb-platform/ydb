/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*-------------------------------------------------------------------------
 *
 * Created:     H5trace.c
 *
 * Purpose:     Internal code for tracing API calls
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/
#include "H5module.h" /* This source code file is part of the H5 module */
#define H5I_FRIEND    /*suppress error about including H5Ipkg      */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FDprivate.h" /* File drivers                             */
#include "H5Rprivate.h"  /* References                               */
#include "H5Ipkg.h"      /* IDs                                      */
#include "H5Mpublic.h"   /* Maps                                     */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5RSprivate.h" /* Reference-counted strings                */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

#ifdef H5_HAVE_PARALLEL
/* datatypes of predefined drivers needed by H5_trace() */
#include "H5FDmpio.h"
#endif /* H5_HAVE_PARALLEL */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5_trace_args_bool(H5RS_str_t *rs, bool val);
static herr_t H5_trace_args_cset(H5RS_str_t *rs, H5T_cset_t cset);
static herr_t H5_trace_args_close_degree(H5RS_str_t *rs, H5F_close_degree_t degree);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5_trace_args_bool
 *
 * Purpose:     This routine formats an bool and adds the output to
 *		the refcounted string (RS) argument.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_trace_args_bool(H5RS_str_t *rs, bool val)
{
    /* FUNC_ENTER() should not be called */

    if (true == val)
        H5RS_acat(rs, "TRUE");
    else if (!val)
        H5RS_acat(rs, "FALSE");
    else
        H5RS_asprintf_cat(rs, "TRUE(%u)", (unsigned)val);

    return SUCCEED;
} /* end H5_trace_args_bool() */

/*-------------------------------------------------------------------------
 * Function:    H5_trace_args_cset
 *
 * Purpose:     This routine formats an H5T_cset_t and adds the output to
 *		the refcounted string (RS) argument.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_trace_args_cset(H5RS_str_t *rs, H5T_cset_t cset)
{
    /* FUNC_ENTER() should not be called */

    switch (cset) {
        case H5T_CSET_ERROR:
            H5RS_acat(rs, "H5T_CSET_ERROR");
            break;

        case H5T_CSET_ASCII:
            H5RS_acat(rs, "H5T_CSET_ASCII");
            break;

        case H5T_CSET_UTF8:
            H5RS_acat(rs, "H5T_CSET_UTF8");
            break;

        case H5T_CSET_RESERVED_2:
        case H5T_CSET_RESERVED_3:
        case H5T_CSET_RESERVED_4:
        case H5T_CSET_RESERVED_5:
        case H5T_CSET_RESERVED_6:
        case H5T_CSET_RESERVED_7:
        case H5T_CSET_RESERVED_8:
        case H5T_CSET_RESERVED_9:
        case H5T_CSET_RESERVED_10:
        case H5T_CSET_RESERVED_11:
        case H5T_CSET_RESERVED_12:
        case H5T_CSET_RESERVED_13:
        case H5T_CSET_RESERVED_14:
        case H5T_CSET_RESERVED_15:
            H5RS_asprintf_cat(rs, "H5T_CSET_RESERVED_%ld", (long)cset);
            break;

        default:
            H5RS_asprintf_cat(rs, "%ld", (long)cset);
            break;
    } /* end switch */

    return SUCCEED;
} /* end H5_trace_args_cset() */

/*-------------------------------------------------------------------------
 * Function:    H5_trace_args_close_degree
 *
 * Purpose:     This routine formats an H5F_close_degree_t and adds the output to
 *		the refcounted string (RS) argument.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5_trace_args_close_degree(H5RS_str_t *rs, H5F_close_degree_t degree)
{
    /* FUNC_ENTER() should not be called */

    switch (degree) {
        case H5F_CLOSE_DEFAULT:
            H5RS_acat(rs, "H5F_CLOSE_DEFAULT");
            break;

        case H5F_CLOSE_WEAK:
            H5RS_acat(rs, "H5F_CLOSE_WEAK");
            break;

        case H5F_CLOSE_SEMI:
            H5RS_acat(rs, "H5F_CLOSE_SEMI");
            break;

        case H5F_CLOSE_STRONG:
            H5RS_acat(rs, "H5F_CLOSE_STRONG");
            break;

        default:
            H5RS_asprintf_cat(rs, "%ld", (long)degree);
            break;
    } /* end switch */

    return SUCCEED;
} /* end H5_trace_args_cset() */

/*-------------------------------------------------------------------------
 * Function:    H5_trace_args
 *
 * Purpose:     This routine formats a set of function arguments, placing the
 *		resulting string in the refcounted string (RS) argument.
 *
 *		The TYPE argument is a string which gives the type of each of
 *              the following argument pairs.  Each type begins with zero or
 *		more asterisks (one for each level of indirection, although
 *		some types have one level of indirection already implied)
 *		followed by either one letter (lower case) or two letters
 *		(first one uppercase).
 *
 *              The variable argument list consists of pairs of values. Each
 *              pair is a string which is the formal argument name in the
 *              calling function, followed by the argument value.  The type
 *              of the argument value is given by the TYPE string.
 *
 * Note:        The TYPE string is meant to be terse and is generated by a
 *              separate perl script.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_trace_args(H5RS_str_t *rs, const char *type, va_list ap)
{
    const char *argname;
    int         argno = 0, ptr, asize_idx;
    hssize_t    asize[16];
    hssize_t    i;
    void       *vp = NULL;

    /* FUNC_ENTER() should not be called */

    /* Clear array sizes */
    for (i = 0; i < (hssize_t)NELMTS(asize); i++)
        asize[i] = -1;

    /* Parse the argument types */
    for (argno = 0; *type; argno++, type += (isupper(*type) ? 2 : 1)) {
        /* Count levels of indirection */
        for (ptr = 0; '*' == *type; type++)
            ptr++;

        /* Array parameter, possibly with another argument as the array size */
        if ('[' == *type) {
            char *rest;

            if ('a' == type[1]) {
                asize_idx = (int)strtol(type + 2, &rest, 10);
                assert(0 <= asize_idx && asize_idx < (int)NELMTS(asize));
                assert(']' == *rest);
                type = rest + 1;
            }
            else {
                rest = (char *)strchr(type, ']');
                assert(rest);
                type      = rest + 1;
                asize_idx = -1;
            }
        } /* end if */
        else
            asize_idx = -1;

        /*
         * The argument name.  If the argument name is the null pointer then
         * don't print the argument or the following `=' (this is used for
         * return values).
         */
        argname = va_arg(ap, char *);
        if (argname)
            H5RS_asprintf_cat(rs, "%s%s=", argno ? ", " : "", argname);

        /* A pointer/array */
        if (ptr) {
            vp = va_arg(ap, void *);
            if (vp) {
                switch (type[0]) {
                    case 'h': /* hsize_t */
                        H5RS_asprintf_cat(rs, "%p", vp);
                        if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                            hsize_t *p = (hsize_t *)vp;

                            H5RS_acat(rs, " {");
                            for (i = 0; i < asize[asize_idx]; i++) {
                                if (H5S_UNLIMITED == p[i])
                                    H5RS_asprintf_cat(rs, "%sH5S_UNLIMITED", (i ? ", " : ""));
                                else
                                    H5RS_asprintf_cat(rs, "%s%" PRIuHSIZE, (i ? ", " : ""), p[i]);
                            } /* end for */
                            H5RS_acat(rs, "}");
                        } /* end if */
                        break;

                    case 'H':
                        if ('s' == type[1]) { /* hssize_t */
                            H5RS_asprintf_cat(rs, "%p", vp);
                            if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                                hssize_t *p = (hssize_t *)vp;

                                H5RS_acat(rs, " {");
                                for (i = 0; i < asize[asize_idx]; i++)
                                    H5RS_asprintf_cat(rs, "%s%" PRIdHSIZE, (i ? ", " : ""), p[i]);
                                H5RS_acat(rs, "}");
                            } /* end if */
                        }     /* end if */
                        else
                            H5RS_asprintf_cat(rs, "%p", vp);
                        break;

                    case 'I':
                        if ('s' == type[1]) { /* int / int32_t */
                            H5RS_asprintf_cat(rs, "%p", vp);
                            if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                                int *p = (int *)vp;

                                H5RS_acat(rs, " {");
                                for (i = 0; i < asize[asize_idx]; i++)
                                    H5RS_asprintf_cat(rs, "%s%d", (i ? ", " : ""), p[i]);
                                H5RS_acat(rs, "}");
                            }                      /* end if */
                        }                          /* end if */
                        else if ('u' == type[1]) { /* unsigned / uint32_t */
                            H5RS_asprintf_cat(rs, "%p", vp);
                            if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                                unsigned *p = (unsigned *)vp;

                                H5RS_acat(rs, " {");
                                for (i = 0; i < asize[asize_idx]; i++)
                                    H5RS_asprintf_cat(rs, "%s%u", i ? ", " : "", p[i]);
                                H5RS_acat(rs, "}");
                            } /* end if */
                        }     /* end else-if */
                        else
                            H5RS_asprintf_cat(rs, "%p", vp);
                        break;

                    case 's': /* char* */
                        /* Strings have one level of indirection by default, pointers
                         *      to strings have 2 or more.
                         */
                        if (ptr > 1)
                            H5RS_asprintf_cat(rs, "%p", vp);
                        else
                            H5RS_asprintf_cat(rs, "\"%s\"", (const char *)vp);
                        break;

                    case 'U':
                        if ('l' == type[1]) { /* unsigned long */
                            H5RS_asprintf_cat(rs, "%p", vp);
                            if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                                unsigned long *p = (unsigned long *)vp;

                                H5RS_acat(rs, " {");
                                for (i = 0; i < asize[asize_idx]; i++)
                                    H5RS_asprintf_cat(rs, "%s%lu", i ? ", " : "", p[i]);
                                H5RS_acat(rs, "}");
                            }                      /* end if */
                        }                          /* end if */
                        else if ('L' == type[1]) { /* unsigned long long / uint64_t */
                            H5RS_asprintf_cat(rs, "%p", vp);
                            if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                                unsigned long long *p = (unsigned long long *)vp;

                                H5RS_acat(rs, " {");
                                for (i = 0; i < asize[asize_idx]; i++)
                                    H5RS_asprintf_cat(rs, "%s%llu", i ? ", " : "", p[i]);
                                H5RS_acat(rs, "}");
                            } /* end if */
                        }     /* end else-if */
                        else
                            H5RS_asprintf_cat(rs, "%p", vp);
                        break;

                    case 'x': /* void */
                        H5RS_asprintf_cat(rs, "%p", vp);
                        if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                            void **p = (void **)vp;

                            H5RS_acat(rs, " {");
                            for (i = 0; i < asize[asize_idx]; i++) {
                                if (p[i])
                                    H5RS_asprintf_cat(rs, "%s%p", (i ? ", " : ""), p[i]);
                                else
                                    H5RS_asprintf_cat(rs, "%sNULL", (i ? ", " : ""));
                            } /* end for */
                            H5RS_acat(rs, "}");
                        } /* end if */
                        break;

                    case 'z': /* size_t */
                        H5RS_asprintf_cat(rs, "%p", vp);
                        if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                            size_t *p = (size_t *)vp;

                            H5RS_acat(rs, " {");
                            for (i = 0; i < asize[asize_idx]; i++)
                                H5RS_asprintf_cat(rs, "%s%zu", (i ? ", " : ""), p[i]);
                            H5RS_acat(rs, "}");
                        } /* end if */
                        break;

                    case 'Z':
                        if ('s' == type[1]) { /* ssize_t */
                            H5RS_asprintf_cat(rs, "%p", vp);
                            if (asize_idx >= 0 && asize[asize_idx] >= 0) {
                                ssize_t *p = (ssize_t *)vp;

                                H5RS_acat(rs, " {");
                                for (i = 0; i < asize[asize_idx]; i++)
                                    H5RS_asprintf_cat(rs, "%s%zd", (i ? ", " : ""), p[i]);
                                H5RS_acat(rs, "}");
                            } /* end if */
                        }     /* end if */
                        else
                            H5RS_asprintf_cat(rs, "%p", vp);
                        break;

                    default:
                        H5RS_asprintf_cat(rs, "%p", vp);
                } /* end switch */
            }     /* end if */
            else
                H5RS_acat(rs, "NULL");
        } /* end if */
        /* A value */
        else {
            switch (type[0]) {
                case 'a': /* haddr_t */
                {
                    haddr_t addr = va_arg(ap, haddr_t);

                    if (H5_addr_defined(addr))
                        H5RS_asprintf_cat(rs, "%" PRIuHADDR, addr);
                    else
                        H5RS_acat(rs, "UNDEF");
                } /* end block */
                break;

                case 'A':
                    switch (type[1]) {
                        case 'i': /* H5A_info_t */
                        {
                            H5A_info_t ainfo = va_arg(ap, H5A_info_t);

                            H5RS_acat(rs, "{");
                            H5_trace_args_bool(rs, ainfo.corder_valid);
                            H5RS_asprintf_cat(rs, ", %u, ", ainfo.corder);
                            H5_trace_args_cset(rs, ainfo.cset);
                            H5RS_asprintf_cat(rs, "%" PRIuHSIZE "}", ainfo.data_size);
                        } /* end block */
                        break;

#ifndef H5_NO_DEPRECATED_SYMBOLS
                        case 'o': /* H5A_operator1_t */
                        {
                            H5A_operator1_t aop1 = (H5A_operator1_t)va_arg(ap, H5A_operator1_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)aop1);
                        } /* end block */
                        break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                        case 'O': /* H5A_operator2_t */
                        {
                            H5A_operator2_t aop2 = (H5A_operator2_t)va_arg(ap, H5A_operator2_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)aop2);
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(A%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'b': /* bool */
                {
                    /* Can't pass bool to va_arg() */
                    bool bool_var = (bool)va_arg(ap, int);

                    H5_trace_args_bool(rs, bool_var);
                } /* end block */
                break;

                case 'C':
                    switch (type[1]) {
                        case 'c': /* H5AC_cache_config_t */
                        {
                            H5AC_cache_config_t cc = va_arg(ap, H5AC_cache_config_t);

                            H5RS_asprintf_cat(rs, "{%d, ", cc.version);
                            H5_trace_args_bool(rs, cc.rpt_fcn_enabled);
                            H5RS_acat(rs, ", ");
                            H5_trace_args_bool(rs, cc.open_trace_file);
                            H5RS_acat(rs, ", ");
                            H5_trace_args_bool(rs, cc.close_trace_file);
                            H5RS_asprintf_cat(rs, ", '%s', ", cc.trace_file_name);
                            H5RS_acat(rs, ", ");
                            H5_trace_args_bool(rs, cc.evictions_enabled);
                            H5RS_acat(rs, ", ");
                            H5_trace_args_bool(rs, cc.set_initial_size);
                            H5RS_asprintf_cat(rs, ", %zu, ", cc.initial_size);
                            H5RS_asprintf_cat(rs, "%f, ", cc.min_clean_fraction);
                            H5RS_asprintf_cat(rs, "%zu, ", cc.max_size);
                            H5RS_asprintf_cat(rs, "%zu, ", cc.min_size);
                            H5RS_asprintf_cat(rs, "%ld, ", cc.epoch_length);
                            switch (cc.incr_mode) {
                                case H5C_incr__off:
                                    H5RS_acat(rs, "H5C_incr__off");
                                    break;

                                case H5C_incr__threshold:
                                    H5RS_acat(rs, "H5C_incr__threshold");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)cc.incr_mode);
                                    break;
                            } /* end switch */
                            H5RS_asprintf_cat(rs, ", %f, ", cc.lower_hr_threshold);
                            H5RS_asprintf_cat(rs, "%f, ", cc.increment);
                            H5_trace_args_bool(rs, cc.apply_max_increment);
                            H5RS_asprintf_cat(rs, ", %zu, ", cc.max_increment);
                            switch (cc.flash_incr_mode) {
                                case H5C_flash_incr__off:
                                    H5RS_acat(rs, "H5C_flash_incr__off");
                                    break;

                                case H5C_flash_incr__add_space:
                                    H5RS_acat(rs, "H5C_flash_incr__add_space");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)cc.flash_incr_mode);
                                    break;
                            } /* end switch */
                            H5RS_asprintf_cat(rs, ", %f, ", cc.flash_multiple);
                            H5RS_asprintf_cat(rs, "%f, ", cc.flash_threshold);
                            switch (cc.decr_mode) {
                                case H5C_decr__off:
                                    H5RS_acat(rs, "H5C_decr__off");
                                    break;

                                case H5C_decr__threshold:
                                    H5RS_acat(rs, "H5C_decr__threshold");
                                    break;

                                case H5C_decr__age_out:
                                    H5RS_acat(rs, "H5C_decr__age_out");
                                    break;

                                case H5C_decr__age_out_with_threshold:
                                    H5RS_acat(rs, "H5C_decr__age_out_with_threshold");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)cc.decr_mode);
                                    break;
                            } /* end switch */
                            H5RS_asprintf_cat(rs, ", %f, ", cc.upper_hr_threshold);
                            H5RS_asprintf_cat(rs, "%f, ", cc.decrement);
                            H5_trace_args_bool(rs, cc.apply_max_decrement);
                            H5RS_asprintf_cat(rs, ", %zu, ", cc.max_decrement);
                            H5RS_asprintf_cat(rs, "%d, ", cc.epochs_before_eviction);
                            H5_trace_args_bool(rs, cc.apply_empty_reserve);
                            H5RS_asprintf_cat(rs, ", %f, ", cc.empty_reserve);
                            H5RS_asprintf_cat(rs, "%zu, ", cc.dirty_bytes_threshold);
                            H5RS_asprintf_cat(rs, "%d}", cc.metadata_write_strategy);
                        } /* end block */
                        break;

                        case 'C': /* H5AC_cache_image_config_t */
                        {
                            H5AC_cache_image_config_t cic = va_arg(ap, H5AC_cache_image_config_t);

                            H5RS_asprintf_cat(rs, "{%d, ", cic.version);
                            H5_trace_args_bool(rs, cic.generate_image);
                            H5RS_acat(rs, ", ");
                            H5_trace_args_bool(rs, cic.save_resize_status);
                            H5RS_acat(rs, ", ");
                            H5RS_asprintf_cat(rs, "%d}", cic.entry_ageout);
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(C%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'd': /* double */
                {
                    double dbl = va_arg(ap, double);

                    H5RS_asprintf_cat(rs, "%g", dbl);
                } /* end block */
                break;

                case 'D':
                    switch (type[1]) {
                        case 'a': /* H5D_alloc_time_t */
                        {
                            H5D_alloc_time_t alloc_time = (H5D_alloc_time_t)va_arg(ap, int);

                            switch (alloc_time) {
                                case H5D_ALLOC_TIME_ERROR:
                                    H5RS_acat(rs, "H5D_ALLOC_TIME_ERROR");
                                    break;

                                case H5D_ALLOC_TIME_DEFAULT:
                                    H5RS_acat(rs, "H5D_ALLOC_TIME_DEFAULT");
                                    break;

                                case H5D_ALLOC_TIME_EARLY:
                                    H5RS_acat(rs, "H5D_ALLOC_TIME_EARLY");
                                    break;

                                case H5D_ALLOC_TIME_LATE:
                                    H5RS_acat(rs, "H5D_ALLOC_TIME_LATE");
                                    break;

                                case H5D_ALLOC_TIME_INCR:
                                    H5RS_acat(rs, "H5D_ALLOC_TIME_INCR");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)alloc_time);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'A': /* H5D_append_cb_t */
                        {
                            H5D_append_cb_t dapp = (H5D_append_cb_t)va_arg(ap, H5D_append_cb_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)dapp);
                        } /* end block */
                        break;

                        case 'c': /* H5FD_mpio_collective_opt_t */
                        {
                            H5FD_mpio_collective_opt_t opt = (H5FD_mpio_collective_opt_t)va_arg(ap, int);

                            switch (opt) {
                                case H5FD_MPIO_COLLECTIVE_IO:
                                    H5RS_acat(rs, "H5FD_MPIO_COLLECTIVE_IO");
                                    break;

                                case H5FD_MPIO_INDIVIDUAL_IO:
                                    H5RS_acat(rs, "H5FD_MPIO_INDIVIDUAL_IO");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)opt);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'C': /* H5D_selection_io_mode_t */
                        {
                            H5D_selection_io_mode_t selection_io_mode =
                                (H5D_selection_io_mode_t)va_arg(ap, int);

                            switch (selection_io_mode) {
                                case H5D_SELECTION_IO_MODE_DEFAULT:
                                    H5RS_acat(rs, "H5D_SELECTION_IO_MODE_DEFAULT");
                                    break;

                                case H5D_SELECTION_IO_MODE_OFF:
                                    H5RS_acat(rs, "H5D_SELECTION_IO_MODE_OFF");
                                    break;

                                case H5D_SELECTION_IO_MODE_ON:
                                    H5RS_acat(rs, "H5D_SELECTION_IO_MODE_ON");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)selection_io_mode);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'f': /* H5D_fill_time_t */
                        {
                            H5D_fill_time_t fill_time = (H5D_fill_time_t)va_arg(ap, int);

                            switch (fill_time) {
                                case H5D_FILL_TIME_ERROR:
                                    H5RS_acat(rs, "H5D_FILL_TIME_ERROR");
                                    break;

                                case H5D_FILL_TIME_ALLOC:
                                    H5RS_acat(rs, "H5D_FILL_TIME_ALLOC");
                                    break;

                                case H5D_FILL_TIME_NEVER:
                                    H5RS_acat(rs, "H5D_FILL_TIME_NEVER");
                                    break;

                                case H5D_FILL_TIME_IFSET:
                                    H5RS_acat(rs, "H5D_FILL_TIME_IFSET");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)fill_time);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'F': /* H5D_fill_value_t */
                        {
                            H5D_fill_value_t fill_value = (H5D_fill_value_t)va_arg(ap, int);

                            switch (fill_value) {
                                case H5D_FILL_VALUE_ERROR:
                                    H5RS_acat(rs, "H5D_FILL_VALUE_ERROR");
                                    break;

                                case H5D_FILL_VALUE_UNDEFINED:
                                    H5RS_acat(rs, "H5D_FILL_VALUE_UNDEFINED");
                                    break;

                                case H5D_FILL_VALUE_DEFAULT:
                                    H5RS_acat(rs, "H5D_FILL_VALUE_DEFAULT");
                                    break;

                                case H5D_FILL_VALUE_USER_DEFINED:
                                    H5RS_acat(rs, "H5D_FILL_VALUE_USER_DEFINED");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)fill_value);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'g': /* H5D_gather_func_t */
                        {
                            H5D_gather_func_t gop = (H5D_gather_func_t)va_arg(ap, H5D_gather_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)gop);
                        } /* end block */
                        break;

                        case 'h': /* H5FD_mpio_chunk_opt_t */
                        {
                            H5FD_mpio_chunk_opt_t opt = (H5FD_mpio_chunk_opt_t)va_arg(ap, int);

                            switch (opt) {
                                case H5FD_MPIO_CHUNK_DEFAULT:
                                    H5RS_acat(rs, "H5FD_MPIO_CHUNK_DEFAULT");
                                    break;

                                case H5FD_MPIO_CHUNK_ONE_IO:
                                    H5RS_acat(rs, "H5FD_MPIO_CHUNK_ONE_IO");
                                    break;

                                case H5FD_MPIO_CHUNK_MULTI_IO:
                                    H5RS_acat(rs, "H5FD_MPIO_CHUNK_MULTI_IO");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)opt);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'i': /* H5D_mpio_actual_io_mode_t */
                        {
                            H5D_mpio_actual_io_mode_t actual_io_mode =
                                (H5D_mpio_actual_io_mode_t)va_arg(ap, int);

                            switch (actual_io_mode) {
                                case H5D_MPIO_NO_COLLECTIVE:
                                    H5RS_acat(rs, "H5D_MPIO_NO_COLLECTIVE");
                                    break;

                                case H5D_MPIO_CHUNK_INDEPENDENT:
                                    H5RS_acat(rs, "H5D_MPIO_CHUNK_INDEPENDENT");
                                    break;

                                case H5D_MPIO_CHUNK_COLLECTIVE:
                                    H5RS_acat(rs, "H5D_MPIO_CHUNK_COLLECTIVE");
                                    break;

                                case H5D_MPIO_CHUNK_MIXED:
                                    H5RS_acat(rs, "H5D_MPIO_CHUNK_MIXED");
                                    break;

                                case H5D_MPIO_CONTIGUOUS_COLLECTIVE:
                                    H5RS_acat(rs, "H5D_MPIO_CONTIGUOUS_COLLECTIVE");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)actual_io_mode);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'I': /* H5FD_file_image_callbacks_t */
                        {
                            H5FD_file_image_callbacks_t ficb = va_arg(ap, H5FD_file_image_callbacks_t);

                            H5RS_asprintf_cat(rs, "{%p, ", (void *)(uintptr_t)ficb.image_malloc);
                            H5RS_asprintf_cat(rs, "%p, ", (void *)(uintptr_t)ficb.image_memcpy);
                            H5RS_asprintf_cat(rs, "%p, ", (void *)(uintptr_t)ficb.image_realloc);
                            H5RS_asprintf_cat(rs, "%p, ", (void *)(uintptr_t)ficb.image_free);
                            H5RS_asprintf_cat(rs, "%p, ", (void *)(uintptr_t)ficb.udata_copy);
                            H5RS_asprintf_cat(rs, "%p, ", (void *)(uintptr_t)ficb.udata_free);
                            H5RS_asprintf_cat(rs, "%p}", ficb.udata);
                        } /* end block */
                        break;

                        case 'k': /* H5D_chunk_index_t */
                        {
                            H5D_chunk_index_t idx = (H5D_chunk_index_t)va_arg(ap, int);

                            switch (idx) {
                                case H5D_CHUNK_IDX_BTREE:
                                    H5RS_acat(rs, "H5D_CHUNK_IDX_BTREE");
                                    break;

                                case H5D_CHUNK_IDX_NONE:
                                    H5RS_acat(rs, "H5D_CHUNK_IDX_NONE");
                                    break;

                                case H5D_CHUNK_IDX_FARRAY:
                                    H5RS_acat(rs, "H5D_CHUNK_IDX_FARRAY");
                                    break;

                                case H5D_CHUNK_IDX_EARRAY:
                                    H5RS_acat(rs, "H5D_CHUNK_IDX_EARRAY");
                                    break;

                                case H5D_CHUNK_IDX_BT2:
                                    H5RS_acat(rs, "H5D_CHUNK_IDX_BT2");
                                    break;

                                case H5D_CHUNK_IDX_SINGLE:
                                    H5RS_acat(rs, "H5D_CHUNK_IDX_SINGLE");
                                    break;

                                case H5D_CHUNK_IDX_NTYPES:
                                    H5RS_acat(rs, "ERROR: H5D_CHUNK_IDX_NTYPES (invalid value)");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "UNKNOWN VALUE: %ld", (long)idx);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'l': /* H5D_layout_t */
                        {
                            H5D_layout_t layout = (H5D_layout_t)va_arg(ap, int);

                            switch (layout) {
                                case H5D_LAYOUT_ERROR:
                                    H5RS_acat(rs, "H5D_LAYOUT_ERROR");
                                    break;

                                case H5D_COMPACT:
                                    H5RS_acat(rs, "H5D_COMPACT");
                                    break;

                                case H5D_CONTIGUOUS:
                                    H5RS_acat(rs, "H5D_CONTIGUOUS");
                                    break;

                                case H5D_CHUNKED:
                                    H5RS_acat(rs, "H5D_CHUNKED");
                                    break;

                                case H5D_VIRTUAL:
                                    H5RS_acat(rs, "H5D_VIRTUAL");
                                    break;

                                case H5D_NLAYOUTS:
                                    H5RS_acat(rs, "H5D_NLAYOUTS");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)layout);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'n': /* H5D_mpio_no_collective_cause_t */
                        {
                            H5D_mpio_no_collective_cause_t nocol_cause_mode =
                                (H5D_mpio_no_collective_cause_t)va_arg(ap, int);
                            bool flag_already_displayed = false;

                            /* Check for all bit-flags which might be set */
                            if (nocol_cause_mode & H5D_MPIO_COLLECTIVE) {
                                H5RS_acat(rs, "H5D_MPIO_COLLECTIVE");
                                flag_already_displayed = true;
                            } /* end if */
                            if (nocol_cause_mode & H5D_MPIO_SET_INDEPENDENT) {
                                H5RS_asprintf_cat(rs, "%sH5D_MPIO_SET_INDEPENDENT",
                                                  flag_already_displayed ? " | " : "");
                                flag_already_displayed = true;
                            } /* end if */
                            if (nocol_cause_mode & H5D_MPIO_DATATYPE_CONVERSION) {
                                H5RS_asprintf_cat(rs, "%sH5D_MPIO_DATATYPE_CONVERSION",
                                                  flag_already_displayed ? " | " : "");
                                flag_already_displayed = true;
                            } /* end if */
                            if (nocol_cause_mode & H5D_MPIO_DATA_TRANSFORMS) {
                                H5RS_asprintf_cat(rs, "%sH5D_MPIO_DATA_TRANSFORMS",
                                                  flag_already_displayed ? " | " : "");
                                flag_already_displayed = true;
                            } /* end if */
                            if (nocol_cause_mode & H5D_MPIO_MPI_OPT_TYPES_ENV_VAR_DISABLED) {
                                H5RS_asprintf_cat(rs, "%sH5D_MPIO_MPI_OPT_TYPES_ENV_VAR_DISABLED",
                                                  flag_already_displayed ? " | " : "");
                                flag_already_displayed = true;
                            } /* end if */
                            if (nocol_cause_mode & H5D_MPIO_NOT_SIMPLE_OR_SCALAR_DATASPACES) {
                                H5RS_asprintf_cat(rs, "%sH5D_MPIO_NOT_SIMPLE_OR_SCALAR_DATASPACES",
                                                  flag_already_displayed ? " | " : "");
                                flag_already_displayed = true;
                            } /* end if */
                            if (nocol_cause_mode & H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET) {
                                H5RS_asprintf_cat(rs, "%sH5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET",
                                                  flag_already_displayed ? " | " : "");
                                flag_already_displayed = true;
                            } /* end if */

                            /* Display '<none>' if there's no flags set */
                            if (!flag_already_displayed)
                                H5RS_acat(rs, "<none>");
                        } /* end block */
                        break;

                        case 'o': /* H5D_mpio_actual_chunk_opt_mode_t */
                        {
                            H5D_mpio_actual_chunk_opt_mode_t chunk_opt_mode =
                                (H5D_mpio_actual_chunk_opt_mode_t)va_arg(ap, int);

                            switch (chunk_opt_mode) {
                                case H5D_MPIO_NO_CHUNK_OPTIMIZATION:
                                    H5RS_acat(rs, "H5D_MPIO_NO_CHUNK_OPTIMIZATION");
                                    break;

                                case H5D_MPIO_LINK_CHUNK:
                                    H5RS_acat(rs, "H5D_MPIO_LINK_CHUNK");
                                    break;

                                case H5D_MPIO_MULTI_CHUNK:
                                    H5RS_acat(rs, "H5D_MPIO_MULTI_CHUNK");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)chunk_opt_mode);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'O': /* H5D_operator_t */
                        {
                            H5D_operator_t dop = (H5D_operator_t)va_arg(ap, H5D_operator_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)dop);
                        } /* end block */
                        break;

                        case 's': /* H5D_space_status_t */
                        {
                            H5D_space_status_t space_status = (H5D_space_status_t)va_arg(ap, int);

                            switch (space_status) {
                                case H5D_SPACE_STATUS_NOT_ALLOCATED:
                                    H5RS_acat(rs, "H5D_SPACE_STATUS_NOT_ALLOCATED");
                                    break;

                                case H5D_SPACE_STATUS_PART_ALLOCATED:
                                    H5RS_acat(rs, "H5D_SPACE_STATUS_PART_ALLOCATED");
                                    break;

                                case H5D_SPACE_STATUS_ALLOCATED:
                                    H5RS_acat(rs, "H5D_SPACE_STATUS_ALLOCATED");
                                    break;

                                case H5D_SPACE_STATUS_ERROR:
                                    H5RS_acat(rs, "H5D_SPACE_STATUS_ERROR");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)space_status);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'S': /* H5D_scatter_func_t */
                        {
                            H5D_scatter_func_t sop = (H5D_scatter_func_t)va_arg(ap, H5D_scatter_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)sop);
                        } /* end block */
                        break;

                        case 't': /* H5FD_mpio_xfer_t */
                        {
                            H5FD_mpio_xfer_t transfer = (H5FD_mpio_xfer_t)va_arg(ap, int);

                            switch (transfer) {
                                case H5FD_MPIO_INDEPENDENT:
                                    H5RS_acat(rs, "H5FD_MPIO_INDEPENDENT");
                                    break;

                                case H5FD_MPIO_COLLECTIVE:
                                    H5RS_acat(rs, "H5FD_MPIO_COLLECTIVE");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)transfer);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'v': /* H5D_vds_view_t */
                        {
                            H5D_vds_view_t view = (H5D_vds_view_t)va_arg(ap, int);

                            switch (view) {
                                case H5D_VDS_ERROR:
                                    H5RS_acat(rs, "H5D_VDS_ERROR");
                                    break;

                                case H5D_VDS_FIRST_MISSING:
                                    H5RS_acat(rs, "H5D_VDS_FIRST_MISSING");
                                    break;

                                case H5D_VDS_LAST_AVAILABLE:
                                    H5RS_acat(rs, "H5D_VDS_LAST_AVAILABLE");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)view);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'V': /* H5FD_class_value_t */
                        {
                            H5FD_class_value_t class_val = (H5FD_class_value_t)va_arg(ap, H5FD_class_value_t);

                            switch (class_val) {
                                case H5_VFD_INVALID:
                                    H5RS_acat(rs, "H5_VFD_INVALID");
                                    break;
                                case H5_VFD_SEC2:
                                    H5RS_acat(rs, "H5_VFD_SEC2");
                                    break;
                                case H5_VFD_CORE:
                                    H5RS_acat(rs, "H5_VFD_CORE");
                                    break;
                                case H5_VFD_LOG:
                                    H5RS_acat(rs, "H5_VFD_LOG");
                                    break;
                                case H5_VFD_FAMILY:
                                    H5RS_acat(rs, "H5_VFD_FAMILY");
                                    break;
                                case H5_VFD_MULTI:
                                    H5RS_acat(rs, "H5_VFD_MULTI");
                                    break;
                                case H5_VFD_STDIO:
                                    H5RS_acat(rs, "H5_VFD_STDIO");
                                    break;
#ifdef H5_HAVE_PARALLEL
                                case H5_VFD_MPIO:
                                    H5RS_acat(rs, "H5_VFD_MPIO");
                                    break;
#endif
#ifdef H5_HAVE_DIRECT
                                case H5_VFD_DIRECT:
                                    H5RS_acat(rs, "H5_VFD_DIRECT");
                                    break;
#endif
#ifdef H5_HAVE_MIRROR_VFD
                                case H5_VFD_MIRROR:
                                    H5RS_acat(rs, "H5_VFD_MIRROR");
                                    break;
#endif
#ifdef H5_HAVE_LIBHDFS
                                case H5_VFD_HDFS:
                                    H5RS_acat(rs, "H5_VFD_HDFS");
                                    break;
#endif
#ifdef H5_HAVE_ROS3_VFD
                                case H5_VFD_ROS3:
                                    H5RS_acat(rs, "H5_VFD_ROS3");
                                    break;
#endif
#ifdef H5_HAVE_SUBFILING_VFD
                                case H5_VFD_SUBFILING:
                                    H5RS_acat(rs, "H5_VFD_SUBFILING");
                                    break;
#endif
                                case H5_VFD_ONION:
                                    H5RS_acat(rs, "H5_VFD_ONION");
                                    break;
                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)class_val);
                                    break;
                            }
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(D%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'e': /* herr_t */
                {
                    herr_t status = va_arg(ap, herr_t);

                    if (status >= 0)
                        H5RS_acat(rs, "SUCCEED");
                    else
                        H5RS_acat(rs, "FAIL");
                } /* end block */
                break;

                case 'E':
                    switch (type[1]) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                        case 'a': /* H5E_auto1_t */
                        {
                            H5E_auto1_t eauto1 = (H5E_auto1_t)va_arg(ap, H5E_auto1_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)eauto1);
                        } /* end block */
                        break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                        case 'A': /* H5E_auto2_t */
                        {
                            H5E_auto2_t eauto2 = (H5E_auto2_t)va_arg(ap, H5E_auto2_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)eauto2);
                        } /* end block */
                        break;

                        case 'C': /* H5ES_event_complete_func_t */
                        {
                            H5ES_event_complete_func_t cfunc =
                                (H5ES_event_complete_func_t)va_arg(ap, H5ES_event_complete_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)cfunc);
                        } /* end block */
                        break;

                        case 'd': /* H5E_direction_t */
                        {
                            H5E_direction_t direction = (H5E_direction_t)va_arg(ap, int);

                            switch (direction) {
                                case H5E_WALK_UPWARD:
                                    H5RS_acat(rs, "H5E_WALK_UPWARD");
                                    break;

                                case H5E_WALK_DOWNWARD:
                                    H5RS_acat(rs, "H5E_WALK_DOWNWARD");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)direction);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'e': /* H5E_error_t */
                        {
                            H5E_error2_t *error = va_arg(ap, H5E_error2_t *);

                            H5RS_asprintf_cat(rs, "%p", (void *)error);
                        } /* end block */
                        break;

                        case 'I': /* H5ES_event_insert_func_t */
                        {
                            H5ES_event_insert_func_t ifunc =
                                (H5ES_event_insert_func_t)va_arg(ap, H5ES_event_insert_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)ifunc);
                        } /* end block */
                        break;

                        case 's': /* H5ES_status_t */
                        {
                            H5ES_status_t status = (H5ES_status_t)va_arg(ap, int);

                            switch (status) {
                                case H5ES_STATUS_IN_PROGRESS:
                                    H5RS_acat(rs, "H5ES_STATUS_IN_PROGRESS");
                                    break;

                                case H5ES_STATUS_SUCCEED:
                                    H5RS_acat(rs, "H5ES_STATUS_SUCCEED");
                                    break;

                                case H5ES_STATUS_CANCELED:
                                    H5RS_acat(rs, "H5ES_STATUS_CANCELED");
                                    break;

                                case H5ES_STATUS_FAIL:
                                    H5RS_acat(rs, "H5ES_STATUS_FAIL");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)status);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 't': /* H5E_type_t */
                        {
                            H5E_type_t etype = (H5E_type_t)va_arg(ap, int);

                            switch (etype) {
                                case H5E_MAJOR:
                                    H5RS_acat(rs, "H5E_MAJOR");
                                    break;

                                case H5E_MINOR:
                                    H5RS_acat(rs, "H5E_MINOR");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)etype);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(E%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'F':
                    switch (type[1]) {
                        case 'C': /* H5FD_class_t */
                        {
                            H5FD_class_t cls = va_arg(ap, H5FD_class_t);

                            H5RS_asprintf_cat(rs, "{'%s', %" PRIuHADDR ", ", cls.name, cls.maxaddr);
                            H5_trace_args_close_degree(rs, cls.fc_degree);
                            H5RS_acat(rs, ", ...}");
                        } /* end block */
                        break;

                        case 'd': /* H5F_close_degree_t */
                        {
                            H5F_close_degree_t degree = (H5F_close_degree_t)va_arg(ap, int);

                            H5_trace_args_close_degree(rs, degree);
                        } /* end block */
                        break;

                        case 'f': /* H5F_fspace_strategy_t */
                        {
                            H5F_fspace_strategy_t fs_strategy = (H5F_fspace_strategy_t)va_arg(ap, int);

                            switch (fs_strategy) {
                                case H5F_FSPACE_STRATEGY_FSM_AGGR:
                                    H5RS_acat(rs, "H5F_FSPACE_STRATEGY_FSM_AGGR");
                                    break;

                                case H5F_FSPACE_STRATEGY_PAGE:
                                    H5RS_acat(rs, "H5F_FSPACE_STRATEGY_PAGE");
                                    break;

                                case H5F_FSPACE_STRATEGY_AGGR:
                                    H5RS_acat(rs, "H5F_FSPACE_STRATEGY_AGGR");
                                    break;

                                case H5F_FSPACE_STRATEGY_NONE:
                                    H5RS_acat(rs, "H5F_FSPACE_STRATEGY_NONE");
                                    break;

                                case H5F_FSPACE_STRATEGY_NTYPES:
                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)fs_strategy);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'F': /* H5F_flush_cb_t */
                        {
                            H5F_flush_cb_t fflsh = (H5F_flush_cb_t)va_arg(ap, H5F_flush_cb_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)fflsh);
                        } /* end block */
                        break;

                        case 'I': /* H5F_info2_t */
                        {
                            H5F_info2_t fi2 = va_arg(ap, H5F_info2_t);

                            H5RS_asprintf_cat(rs, "{{%u, %" PRIuHSIZE ", %" PRIuHSIZE "}, ",
                                              fi2.super.version, fi2.super.super_size,
                                              fi2.super.super_ext_size);
                            H5RS_asprintf_cat(rs, "{%u, %" PRIuHSIZE ", %" PRIuHSIZE "}, ", fi2.free.version,
                                              fi2.free.meta_size, fi2.free.tot_space);
                            H5RS_asprintf_cat(rs, "{%u, %" PRIuHSIZE ", {%" PRIuHSIZE ", %" PRIuHSIZE "}}}",
                                              fi2.sohm.version, fi2.sohm.hdr_size,
                                              fi2.sohm.msgs_info.index_size, fi2.sohm.msgs_info.heap_size);
                        } /* end block */
                        break;

                        case 'm': /* H5F_mem_t */
                        {
                            H5F_mem_t mem_type = (H5F_mem_t)va_arg(ap, int);

                            switch (mem_type) {
                                case H5FD_MEM_NOLIST:
                                    H5RS_acat(rs, "H5FD_MEM_NOLIST");
                                    break;

                                case H5FD_MEM_DEFAULT:
                                    H5RS_acat(rs, "H5FD_MEM_DEFAULT");
                                    break;

                                case H5FD_MEM_SUPER:
                                    H5RS_acat(rs, "H5FD_MEM_SUPER");
                                    break;

                                case H5FD_MEM_BTREE:
                                    H5RS_acat(rs, "H5FD_MEM_BTREE");
                                    break;

                                case H5FD_MEM_DRAW:
                                    H5RS_acat(rs, "H5FD_MEM_DRAW");
                                    break;

                                case H5FD_MEM_GHEAP:
                                    H5RS_acat(rs, "H5FD_MEM_GHEAP");
                                    break;

                                case H5FD_MEM_LHEAP:
                                    H5RS_acat(rs, "H5FD_MEM_LHEAP");
                                    break;

                                case H5FD_MEM_OHDR:
                                    H5RS_acat(rs, "H5FD_MEM_OHDR");
                                    break;

                                case H5FD_MEM_NTYPES:
                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)mem_type);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 's': /* H5F_scope_t */
                        {
                            H5F_scope_t scope = (H5F_scope_t)va_arg(ap, int);

                            switch (scope) {
                                case H5F_SCOPE_LOCAL:
                                    H5RS_acat(rs, "H5F_SCOPE_LOCAL");
                                    break;

                                case H5F_SCOPE_GLOBAL:
                                    H5RS_acat(rs, "H5F_SCOPE_GLOBAL");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)scope);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 't': /* H5F_file_space_type_t */
                        {
                            H5F_file_space_type_t fspace_type = (H5F_file_space_type_t)va_arg(ap, int);

                            switch (fspace_type) {
                                case H5F_FILE_SPACE_DEFAULT:
                                    H5RS_acat(rs, "H5F_FILE_SPACE_DEFAULT");
                                    break;

                                case H5F_FILE_SPACE_ALL_PERSIST:
                                    H5RS_acat(rs, "H5F_FILE_SPACE_ALL_PERSIST");
                                    break;

                                case H5F_FILE_SPACE_ALL:
                                    H5RS_acat(rs, "H5F_FILE_SPACE_ALL");
                                    break;

                                case H5F_FILE_SPACE_AGGR_VFD:
                                    H5RS_acat(rs, "H5F_FILE_SPACE_AGGR_VFD");
                                    break;

                                case H5F_FILE_SPACE_VFD:
                                    H5RS_acat(rs, "H5F_FILE_SPACE_VFD");
                                    break;

                                case H5F_FILE_SPACE_NTYPES:
                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)fspace_type);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'v': /* H5F_libver_t */
                        {
                            H5F_libver_t libver_vers = (H5F_libver_t)va_arg(ap, int);

                            switch (libver_vers) {
                                case H5F_LIBVER_EARLIEST:
                                    H5RS_acat(rs, "H5F_LIBVER_EARLIEST");
                                    break;

                                case H5F_LIBVER_V18:
                                    H5RS_acat(rs, "H5F_LIBVER_V18");
                                    break;

                                case H5F_LIBVER_V110:
                                    H5RS_acat(rs, "H5F_LIBVER_V110");
                                    break;

                                case H5F_LIBVER_V112:
                                    H5RS_acat(rs, "H5F_LIBVER_V112");
                                    break;

                                case H5F_LIBVER_V114:
                                    HDcompile_assert(H5F_LIBVER_LATEST == H5F_LIBVER_V114);
                                    H5RS_acat(rs, "H5F_LIBVER_LATEST");
                                    break;

                                case H5F_LIBVER_ERROR:
                                case H5F_LIBVER_NBOUNDS:
                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)libver_vers);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(F%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'G':
                    switch (type[1]) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                        case 'i': /* H5G_iterate_t */
                        {
                            H5G_iterate_t git = (H5G_iterate_t)va_arg(ap, H5G_iterate_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)git);
                        } /* end block */
                        break;

                        case 'o': /* H5G_obj_t */
                        {
                            H5G_obj_t obj_type = (H5G_obj_t)va_arg(ap, int);

                            switch (obj_type) {
                                case H5G_UNKNOWN:
                                    H5RS_acat(rs, "H5G_UNKNOWN");
                                    break;

                                case H5G_GROUP:
                                    H5RS_acat(rs, "H5G_GROUP");
                                    break;

                                case H5G_DATASET:
                                    H5RS_acat(rs, "H5G_DATASET");
                                    break;

                                case H5G_TYPE:
                                    H5RS_acat(rs, "H5G_TYPE");
                                    break;

                                case H5G_LINK:
                                    H5RS_acat(rs, "H5G_LINK");
                                    break;

                                case H5G_UDLINK:
                                    H5RS_acat(rs, "H5G_UDLINK");
                                    break;

                                case H5G_RESERVED_5:
                                case H5G_RESERVED_6:
                                case H5G_RESERVED_7:
                                    H5RS_asprintf_cat(rs, "H5G_RESERVED(%ld)", (long)obj_type);
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)obj_type);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 's': /* H5G_stat_t */
                        {
                            H5G_stat_t *statbuf = va_arg(ap, H5G_stat_t *);

                            H5RS_asprintf_cat(rs, "%p", (void *)statbuf);
                        } /* end block */
                        break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(G%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'h': /* hsize_t */
                {
                    hsize_t hsize = va_arg(ap, hsize_t);

                    if (H5S_UNLIMITED == hsize)
                        H5RS_acat(rs, "H5S_UNLIMITED");
                    else {
                        H5RS_asprintf_cat(rs, "%" PRIuHSIZE, hsize);
                        asize[argno] = (hssize_t)hsize;
                    } /* end else */
                }     /* end block */
                break;

                case 'H':
                    switch (type[1]) {
                        case 'c': /* H5_atclose_func_t */
                        {
                            H5_atclose_func_t cfunc = (H5_atclose_func_t)va_arg(ap, H5_atclose_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)cfunc);
                        } /* end block */
                        break;

                        case 's': /* hssize_t */
                        {
                            hssize_t hssize = va_arg(ap, hssize_t);

                            H5RS_asprintf_cat(rs, "%" PRIdHSIZE, hssize);
                            asize[argno] = (hssize_t)hssize;
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(H%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'i': /* hid_t (and H5E_major_t / H5E_minor_t) */
                {
                    hid_t obj = va_arg(ap, hid_t);

                    if (H5P_DEFAULT == obj)
                        H5RS_acat(rs, "H5P_DEFAULT");
                    else if (obj < 0)
                        H5RS_acat(rs, "FAIL");
                    else {
                        switch (H5I_TYPE(obj)) { /* Use internal H5I macro instead of function call */
                            case H5I_UNINIT:
                                H5RS_asprintf_cat(rs, "0x%0llx (uninit - error)", (unsigned long long)obj);
                                break;

                            case H5I_BADID:
                                H5RS_asprintf_cat(rs, "0x%0llx (badid - error)", (unsigned long long)obj);
                                break;

                            case H5I_FILE:
                                H5RS_asprintf_cat(rs, "0x%0llx (file)", (unsigned long long)obj);
                                break;

                            case H5I_GROUP:
                                H5RS_asprintf_cat(rs, "0x%0llx (group)", (unsigned long long)obj);
                                break;

                            case H5I_DATATYPE:
                                if (obj == H5T_NATIVE_SCHAR_g)
                                    H5RS_acat(rs, "H5T_NATIVE_SCHAR");
                                else if (obj == H5T_NATIVE_UCHAR_g)
                                    H5RS_acat(rs, "H5T_NATIVE_UCHAR");
                                else if (obj == H5T_NATIVE_SHORT_g)
                                    H5RS_acat(rs, "H5T_NATIVE_SHORT");
                                else if (obj == H5T_NATIVE_USHORT_g)
                                    H5RS_acat(rs, "H5T_NATIVE_USHORT");
                                else if (obj == H5T_NATIVE_INT_g)
                                    H5RS_acat(rs, "H5T_NATIVE_INT");
                                else if (obj == H5T_NATIVE_UINT_g)
                                    H5RS_acat(rs, "H5T_NATIVE_UINT");
                                else if (obj == H5T_NATIVE_LONG_g)
                                    H5RS_acat(rs, "H5T_NATIVE_LONG");
                                else if (obj == H5T_NATIVE_ULONG_g)
                                    H5RS_acat(rs, "H5T_NATIVE_ULONG");
                                else if (obj == H5T_NATIVE_LLONG_g)
                                    H5RS_acat(rs, "H5T_NATIVE_LLONG");
                                else if (obj == H5T_NATIVE_ULLONG_g)
                                    H5RS_acat(rs, "H5T_NATIVE_ULLONG");
                                else if (obj == H5T_NATIVE_FLOAT_g)
                                    H5RS_acat(rs, "H5T_NATIVE_FLOAT");
                                else if (obj == H5T_NATIVE_DOUBLE_g)
                                    H5RS_acat(rs, "H5T_NATIVE_DOUBLE");
                                else if (obj == H5T_NATIVE_LDOUBLE_g)
                                    H5RS_acat(rs, "H5T_NATIVE_LDOUBLE");
                                else if (obj == H5T_IEEE_F32BE_g)
                                    H5RS_acat(rs, "H5T_IEEE_F32BE");
                                else if (obj == H5T_IEEE_F32LE_g)
                                    H5RS_acat(rs, "H5T_IEEE_F32LE");
                                else if (obj == H5T_IEEE_F64BE_g)
                                    H5RS_acat(rs, "H5T_IEEE_F64BE");
                                else if (obj == H5T_IEEE_F64LE_g)
                                    H5RS_acat(rs, "H5T_IEEE_F64LE");
                                else if (obj == H5T_STD_I8BE_g)
                                    H5RS_acat(rs, "H5T_STD_I8BE");
                                else if (obj == H5T_STD_I8LE_g)
                                    H5RS_acat(rs, "H5T_STD_I8LE");
                                else if (obj == H5T_STD_I16BE_g)
                                    H5RS_acat(rs, "H5T_STD_I16BE");
                                else if (obj == H5T_STD_I16LE_g)
                                    H5RS_acat(rs, "H5T_STD_I16LE");
                                else if (obj == H5T_STD_I32BE_g)
                                    H5RS_acat(rs, "H5T_STD_I32BE");
                                else if (obj == H5T_STD_I32LE_g)
                                    H5RS_acat(rs, "H5T_STD_I32LE");
                                else if (obj == H5T_STD_I64BE_g)
                                    H5RS_acat(rs, "H5T_STD_I64BE");
                                else if (obj == H5T_STD_I64LE_g)
                                    H5RS_acat(rs, "H5T_STD_I64LE");
                                else if (obj == H5T_STD_U8BE_g)
                                    H5RS_acat(rs, "H5T_STD_U8BE");
                                else if (obj == H5T_STD_U8LE_g)
                                    H5RS_acat(rs, "H5T_STD_U8LE");
                                else if (obj == H5T_STD_U16BE_g)
                                    H5RS_acat(rs, "H5T_STD_U16BE");
                                else if (obj == H5T_STD_U16LE_g)
                                    H5RS_acat(rs, "H5T_STD_U16LE");
                                else if (obj == H5T_STD_U32BE_g)
                                    H5RS_acat(rs, "H5T_STD_U32BE");
                                else if (obj == H5T_STD_U32LE_g)
                                    H5RS_acat(rs, "H5T_STD_U32LE");
                                else if (obj == H5T_STD_U64BE_g)
                                    H5RS_acat(rs, "H5T_STD_U64BE");
                                else if (obj == H5T_STD_U64LE_g)
                                    H5RS_acat(rs, "H5T_STD_U64LE");
                                else if (obj == H5T_STD_B8BE_g)
                                    H5RS_acat(rs, "H5T_STD_B8BE");
                                else if (obj == H5T_STD_B8LE_g)
                                    H5RS_acat(rs, "H5T_STD_B8LE");
                                else if (obj == H5T_STD_B16BE_g)
                                    H5RS_acat(rs, "H5T_STD_B16BE");
                                else if (obj == H5T_STD_B16LE_g)
                                    H5RS_acat(rs, "H5T_STD_B16LE");
                                else if (obj == H5T_STD_B32BE_g)
                                    H5RS_acat(rs, "H5T_STD_B32BE");
                                else if (obj == H5T_STD_B32LE_g)
                                    H5RS_acat(rs, "H5T_STD_B32LE");
                                else if (obj == H5T_STD_B64BE_g)
                                    H5RS_acat(rs, "H5T_STD_B64BE");
                                else if (obj == H5T_STD_B64LE_g)
                                    H5RS_acat(rs, "H5T_STD_B64LE");
                                else if (obj == H5T_C_S1_g)
                                    H5RS_acat(rs, "H5T_C_S1");
                                else if (obj == H5T_FORTRAN_S1_g)
                                    H5RS_acat(rs, "H5T_FORTRAN_S1");
                                else
                                    H5RS_asprintf_cat(rs, "0x%0llx (dtype)", (unsigned long long)obj);
                                break;

                            case H5I_DATASPACE:
                                H5RS_asprintf_cat(rs, "0x%0llx (dspace)", (unsigned long long)obj);
                                /* Save the rank of simple dataspaces for arrays */
                                /* This may generate recursive call to the library... -QAK */
                                {
                                    H5S_t *space;

                                    if (NULL != (space = (H5S_t *)H5I_object(obj)))
                                        if (H5S_SIMPLE == H5S_GET_EXTENT_TYPE(space))
                                            asize[argno] = H5S_GET_EXTENT_NDIMS(space);
                                }
                                break;

                            case H5I_DATASET:
                                H5RS_asprintf_cat(rs, "0x%0llx (dset)", (unsigned long long)obj);
                                break;

                            case H5I_ATTR:
                                H5RS_asprintf_cat(rs, "0x%0llx (attr)", (unsigned long long)obj);
                                break;

                            case H5I_MAP:
                                H5RS_asprintf_cat(rs, "0x%0llx (map)", (unsigned long long)obj);
                                break;

                            case H5I_VFL:
                                H5RS_asprintf_cat(rs, "0x%0llx (file driver)", (unsigned long long)obj);
                                break;

                            case H5I_VOL:
                                H5RS_asprintf_cat(rs, "0x%0llx (VOL plugin)", (unsigned long long)obj);
                                break;

                            case H5I_GENPROP_CLS:
                                H5RS_asprintf_cat(rs, "0x%0llx (genprop class)", (unsigned long long)obj);
                                break;

                            case H5I_GENPROP_LST:
                                H5RS_asprintf_cat(rs, "0x%0llx (genprop list)", (unsigned long long)obj);
                                break;

                            case H5I_ERROR_CLASS:
                                H5RS_asprintf_cat(rs, "0x%0llx (err class)", (unsigned long long)obj);
                                break;

                            case H5I_ERROR_MSG:
                                H5RS_asprintf_cat(rs, "0x%0llx (err msg)", (unsigned long long)obj);
                                break;

                            case H5I_ERROR_STACK:
                                H5RS_asprintf_cat(rs, "0x%0llx (err stack)", (unsigned long long)obj);
                                break;

                            case H5I_SPACE_SEL_ITER:
                                H5RS_asprintf_cat(rs, "0x%0llx (dataspace selection iterator)",
                                                  (unsigned long long)obj);
                                break;

                            case H5I_EVENTSET:
                                H5RS_asprintf_cat(rs, "0x%0llx (event set)", (unsigned long long)obj);
                                break;

                            case H5I_NTYPES:
                                H5RS_asprintf_cat(rs, "0x%0llx (ntypes - error)", (unsigned long long)obj);
                                break;

                            default:
                                H5RS_asprintf_cat(rs, "0x%0llx (unknown class)", (unsigned long long)obj);
                                break;
                        } /* end switch */
                    }     /* end else */
                }         /* end block */
                break;

                case 'I':
                    switch (type[1]) {
                        case 'D': /* H5I_future_discard_func_t */
                        {
                            H5I_future_discard_func_t ifdisc =
                                (H5I_future_discard_func_t)va_arg(ap, H5I_future_discard_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)ifdisc);
                        } /* end block */
                        break;

                        case 'f': /* H5I_free_t */
                        {
                            H5I_free_t ifree = (H5I_free_t)va_arg(ap, H5I_free_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)ifree);
                        } /* end block */
                        break;

                        case 'i': /* H5_index_t */
                        {
                            H5_index_t idx_type = (H5_index_t)va_arg(ap, int);

                            switch (idx_type) {
                                case H5_INDEX_UNKNOWN:
                                    H5RS_acat(rs, "H5_INDEX_UNKNOWN");
                                    break;

                                case H5_INDEX_NAME:
                                    H5RS_acat(rs, "H5_INDEX_NAME");
                                    break;

                                case H5_INDEX_CRT_ORDER:
                                    H5RS_acat(rs, "H5_INDEX_CRT_ORDER");
                                    break;

                                case H5_INDEX_N:
                                    H5RS_acat(rs, "H5_INDEX_N");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)idx_type);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'I': /* H5I_iterate_func_t */
                        {
                            H5I_iterate_func_t iiter = (H5I_iterate_func_t)va_arg(ap, H5I_iterate_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)iiter);
                        } /* end block */
                        break;

                        case 'o': /* H5_iter_order_t */
                        {
                            H5_iter_order_t order = (H5_iter_order_t)va_arg(ap, int);

                            switch (order) {
                                case H5_ITER_UNKNOWN:
                                    H5RS_acat(rs, "H5_ITER_UNKNOWN");
                                    break;

                                case H5_ITER_INC:
                                    H5RS_acat(rs, "H5_ITER_INC");
                                    break;

                                case H5_ITER_DEC:
                                    H5RS_acat(rs, "H5_ITER_DEC");
                                    break;

                                case H5_ITER_NATIVE:
                                    H5RS_acat(rs, "H5_ITER_NATIVE");
                                    break;

                                case H5_ITER_N:
                                    H5RS_acat(rs, "H5_ITER_N");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)order);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'R': /* H5I_future_realize_func_t */
                        {
                            H5I_future_realize_func_t ifreal =
                                (H5I_future_realize_func_t)va_arg(ap, H5I_future_realize_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)ifreal);
                        } /* end block */
                        break;

                        case 's': /* int / int32_t */
                        {
                            int is = va_arg(ap, int);

                            H5RS_asprintf_cat(rs, "%d", is);
                            asize[argno] = is;
                        } /* end block */
                        break;

                        case 'S': /* H5I_search_func_t */
                        {
                            H5I_search_func_t isearch = (H5I_search_func_t)va_arg(ap, H5I_search_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)isearch);
                        } /* end block */
                        break;

                        case 't': /* H5I_type_t */
                        {
                            H5I_type_t id_type = (H5I_type_t)va_arg(ap, int);

                            switch (id_type) {
                                case H5I_UNINIT:
                                    H5RS_acat(rs, "H5I_UNINIT");
                                    break;

                                case H5I_BADID:
                                    H5RS_acat(rs, "H5I_BADID");
                                    break;

                                case H5I_FILE:
                                    H5RS_acat(rs, "H5I_FILE");
                                    break;

                                case H5I_GROUP:
                                    H5RS_acat(rs, "H5I_GROUP");
                                    break;

                                case H5I_DATATYPE:
                                    H5RS_acat(rs, "H5I_DATATYPE");
                                    break;

                                case H5I_DATASPACE:
                                    H5RS_acat(rs, "H5I_DATASPACE");
                                    break;

                                case H5I_DATASET:
                                    H5RS_acat(rs, "H5I_DATASET");
                                    break;

                                case H5I_ATTR:
                                    H5RS_acat(rs, "H5I_ATTR");
                                    break;

                                case H5I_MAP:
                                    H5RS_acat(rs, "H5I_MAP");
                                    break;

                                case H5I_VFL:
                                    H5RS_acat(rs, "H5I_VFL");
                                    break;

                                case H5I_VOL:
                                    H5RS_acat(rs, "H5I_VOL");
                                    break;

                                case H5I_GENPROP_CLS:
                                    H5RS_acat(rs, "H5I_GENPROP_CLS");
                                    break;

                                case H5I_GENPROP_LST:
                                    H5RS_acat(rs, "H5I_GENPROP_LST");
                                    break;

                                case H5I_ERROR_CLASS:
                                    H5RS_acat(rs, "H5I_ERROR_CLASS");
                                    break;

                                case H5I_ERROR_MSG:
                                    H5RS_acat(rs, "H5I_ERROR_MSG");
                                    break;

                                case H5I_ERROR_STACK:
                                    H5RS_acat(rs, "H5I_ERROR_STACK");
                                    break;

                                case H5I_SPACE_SEL_ITER:
                                    H5RS_acat(rs, "H5I_SPACE_SEL_ITER");
                                    break;

                                case H5I_EVENTSET:
                                    H5RS_acat(rs, "H5I_EVENTSET");
                                    break;

                                case H5I_NTYPES:
                                    H5RS_acat(rs, "H5I_NTYPES");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)id_type);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'u': /* unsigned / uint32_t */
                        {
                            unsigned iu = va_arg(ap, unsigned);

                            H5RS_asprintf_cat(rs, "%u", iu);
                            asize[argno] = iu;
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(I%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'k': /* H5O_token_t */
                {
                    H5O_token_t token = va_arg(ap, H5O_token_t);
                    int         j;

                    for (j = 0; j < H5O_MAX_TOKEN_SIZE; j++)
                        H5RS_asprintf_cat(rs, "%02x", token.__data[j]);
                } /* end block */
                break;

                case 'L':
                    switch (type[1]) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                        case 'i': /* H5L_iterate1_t */
                        {
                            H5L_iterate1_t liter = (H5L_iterate1_t)va_arg(ap, H5L_iterate1_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)liter);
                        } /* end block */
                        break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                        case 'I': /* H5L_iterate2_t */
                        {
                            H5L_iterate2_t liter = (H5L_iterate2_t)va_arg(ap, H5L_iterate2_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)liter);
                        } /* end block */
                        break;

                        case 'l': /* H5L_type_t (or H5G_link_t) */
                        {
                            H5L_type_t link_type = (H5L_type_t)va_arg(ap, int);

                            switch (link_type) {
                                case H5L_TYPE_ERROR:
                                    H5RS_acat(rs, "H5L_TYPE_ERROR");
                                    break;

                                case H5L_TYPE_HARD:
                                    H5RS_acat(rs, "H5L_TYPE_HARD");
                                    break;

                                case H5L_TYPE_SOFT:
                                    H5RS_acat(rs, "H5L_TYPE_SOFT");
                                    break;

                                case H5L_TYPE_EXTERNAL:
                                    H5RS_acat(rs, "H5L_TYPE_EXTERNAL");
                                    break;

                                case H5L_TYPE_MAX:
                                    H5RS_acat(rs, "H5L_TYPE_MAX");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)link_type);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 't': /* H5L_elink_traverse_t */
                        {
                            H5L_elink_traverse_t elt = (H5L_elink_traverse_t)va_arg(ap, H5L_elink_traverse_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)elt);
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(G%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'M':
                    switch (type[1]) {
                        case 'a': /* H5MM_allocate_t */
                        {
                            H5MM_allocate_t afunc = (H5MM_allocate_t)va_arg(ap, H5MM_allocate_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)afunc);
                        } /* end block */
                        break;

#ifdef H5_HAVE_PARALLEL
                        case 'c': /* MPI_Comm */
                        {
                            MPI_Comm comm = va_arg(ap, MPI_Comm);

                            H5RS_asprintf_cat(rs, "%ld", (long)comm);
                        } /* end block */
                        break;
#endif /* H5_HAVE_PARALLEL */

                        case 'f': /* H5MM_free_t */
                        {
                            H5MM_free_t ffunc = (H5MM_free_t)va_arg(ap, H5MM_free_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)ffunc);
                        } /* end block */
                        break;

#ifdef H5_HAVE_PARALLEL
                        case 'i': /* MPI_Info */
                        {
                            MPI_Info info = va_arg(ap, MPI_Info);

                            H5RS_asprintf_cat(rs, "%ld", (long)info);
                        } /* end block */
                        break;
#endif /* H5_HAVE_PARALLEL */

#ifdef H5_HAVE_MAP_API
                        case 'I': /* H5M_iterate_t */
                        {
                            H5M_iterate_t miter = (H5M_iterate_t)va_arg(ap, H5M_iterate_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)miter);
                        } /* end block */
                        break;
#endif /* H5_HAVE_MAP_API */

                        case 't': /* H5FD_mem_t */
                        {
                            H5FD_mem_t mt = (H5FD_mem_t)va_arg(ap, int);

                            switch (mt) {
                                case H5FD_MEM_NOLIST:
                                    H5RS_acat(rs, "H5FD_MEM_NOLIST");
                                    break;

                                case H5FD_MEM_DEFAULT:
                                    H5RS_acat(rs, "H5FD_MEM_DEFAULT");
                                    break;

                                case H5FD_MEM_SUPER:
                                    H5RS_acat(rs, "H5FD_MEM_SUPER");
                                    break;

                                case H5FD_MEM_BTREE:
                                    H5RS_acat(rs, "H5FD_MEM_BTREE");
                                    break;

                                case H5FD_MEM_DRAW:
                                    H5RS_acat(rs, "H5FD_MEM_DRAW");
                                    break;

                                case H5FD_MEM_GHEAP:
                                    H5RS_acat(rs, "H5FD_MEM_GHEAP");
                                    break;

                                case H5FD_MEM_LHEAP:
                                    H5RS_acat(rs, "H5FD_MEM_LHEAP");
                                    break;

                                case H5FD_MEM_OHDR:
                                    H5RS_acat(rs, "H5FD_MEM_OHDR");
                                    break;

                                case H5FD_MEM_NTYPES:
                                    H5RS_acat(rs, "H5FD_MEM_NTYPES");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)mt);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        default:
                            goto error;
                    } /* end switch */
                    break;

                case 'o': /* off_t */
                {
                    off_t offset = va_arg(ap, off_t);

                    H5RS_asprintf_cat(rs, "%ld", (long)offset);
                } /* end block */
                break;

                case 'O':
                    switch (type[1]) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                        case 'i': /* H5O_iterate1_t */
                        {
                            H5O_iterate1_t oiter = (H5O_iterate1_t)va_arg(ap, H5O_iterate1_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)oiter);
                        } /* end block */
                        break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                        case 'I': /* H5O_iterate2_t */
                        {
                            H5O_iterate2_t oiter2 = (H5O_iterate2_t)va_arg(ap, H5O_iterate2_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)oiter2);
                        } /* end block */
                        break;

                        case 's': /* H5O_mcdt_search_cb_t */
                        {
                            H5O_mcdt_search_cb_t osrch =
                                (H5O_mcdt_search_cb_t)va_arg(ap, H5O_mcdt_search_cb_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)osrch);
                        } /* end block */
                        break;

                        case 't': /* H5O_type_t */
                        {
                            H5O_type_t objtype = (H5O_type_t)va_arg(ap, int);

                            switch (objtype) {
                                case H5O_TYPE_UNKNOWN:
                                    H5RS_acat(rs, "H5O_TYPE_UNKNOWN");
                                    break;

                                case H5O_TYPE_GROUP:
                                    H5RS_acat(rs, "H5O_TYPE_GROUP");
                                    break;

                                case H5O_TYPE_DATASET:
                                    H5RS_acat(rs, "H5O_TYPE_DATASET");
                                    break;

                                case H5O_TYPE_NAMED_DATATYPE:
                                    H5RS_acat(rs, "H5O_TYPE_NAMED_DATATYPE");
                                    break;

                                case H5O_TYPE_MAP:
                                    H5RS_acat(rs, "H5O_TYPE_MAP");
                                    break;

                                case H5O_TYPE_NTYPES:
                                    H5RS_acat(rs, "H5O_TYPE_NTYPES");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "BADTYPE(%ld)", (long)objtype);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(S%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'p': /* H5P_class_t */
                {
                    hid_t           pclass_id  = va_arg(ap, hid_t);
                    char           *class_name = NULL;
                    H5P_genclass_t *pclass;

                    /* Get the class name and print it */
                    /* (This may generate recursive call to the library... -QAK) */
                    if (NULL != (pclass = (H5P_genclass_t *)H5I_object(pclass_id)) &&
                        NULL != (class_name = H5P_get_class_name(pclass))) {
                        H5RS_asprintf_cat(rs, "%s", class_name);
                        H5MM_xfree(class_name);
                    } /* end if */
                    else
                        H5RS_asprintf_cat(rs, "%ld", (long)pclass_id);
                } /* end block */
                break;

                case 'P':
                    switch (type[1]) {
                        case 'c': /* H5P_cls_create_func_t */
                        {
                            H5P_cls_create_func_t pcls_crt =
                                (H5P_cls_create_func_t)va_arg(ap, H5P_cls_create_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)pcls_crt);
                        } /* end block */
                        break;

                        case 'C': /* H5P_prp_create_func_t */
                        {
                            H5P_prp_create_func_t prp_crt =
                                (H5P_prp_create_func_t)va_arg(ap, H5P_prp_create_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)prp_crt);
                        } /* end block */
                        break;

                        case 'D': /* H5P_prp_delete_func_t */
                        {
                            H5P_prp_delete_func_t prp_del =
                                (H5P_prp_delete_func_t)va_arg(ap, H5P_prp_delete_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)prp_del);
                        } /* end block */
                        break;

                        case 'G': /* H5P_prp_get_func_t */
                        {
                            H5P_prp_get_func_t prp_get = (H5P_prp_get_func_t)va_arg(ap, H5P_prp_get_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)prp_get);
                        } /* end block */
                        break;

                        case 'i': /* H5P_iterate_t */
                        {
                            H5P_iterate_t piter = (H5P_iterate_t)va_arg(ap, H5P_iterate_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)piter);
                        } /* end block */
                        break;

                        case 'l': /* H5P_cls_close_func_t */
                        {
                            H5P_cls_close_func_t pcls_cls =
                                (H5P_cls_close_func_t)va_arg(ap, H5P_cls_close_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)pcls_cls);
                        } /* end block */
                        break;

                        case 'L': /* H5P_prp_close_func_t */
                        {
                            H5P_prp_close_func_t prp_cls =
                                (H5P_prp_close_func_t)va_arg(ap, H5P_prp_close_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)prp_cls);
                        } /* end block */
                        break;

                        case 'M': /* H5P_prp_compare_func_t */
                        {
                            H5P_prp_compare_func_t prp_cmp =
                                (H5P_prp_compare_func_t)va_arg(ap, H5P_prp_compare_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)prp_cmp);
                        } /* end block */
                        break;

                        case 'o': /* H5P_cls_copy_func_t */
                        {
                            H5P_cls_copy_func_t pcls_cpy =
                                (H5P_cls_copy_func_t)va_arg(ap, H5P_cls_copy_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)pcls_cpy);
                        } /* end block */
                        break;

                        case 'O': /* H5P_prp_copy_func_t */
                        {
                            H5P_prp_copy_func_t prp_cpy =
                                (H5P_prp_copy_func_t)va_arg(ap, H5P_prp_copy_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)prp_cpy);
                        } /* end block */
                        break;

                        case 'S': /* H5P_prp_set_func_t */
                        {
                            H5P_prp_set_func_t prp_set = (H5P_prp_set_func_t)va_arg(ap, H5P_prp_set_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)prp_set);
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(P%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'R':
                    switch (type[1]) {
                        case 'd': /* hdset_reg_ref_t */
                        {
                            /* Note! region references are array types */
                            H5RS_acat(rs, "Reference Region");
                            goto error;
                        } /* end block */
                        break;

                        case 'o': /* hobj_ref_t */
                        {
                            hobj_ref_t ref = va_arg(ap, hobj_ref_t);

                            H5RS_asprintf_cat(rs, "Reference Object=%" PRIuHADDR, ref);
                        } /* end block */
                        break;

                        case 'r': /* H5R_ref_t */
                        {
                            /* Note! reference types are opaque types */
                            H5RS_acat(rs, "Reference Opaque");
                            goto error;
                        } /* end block */
                        break;

                        case 't': /* H5R_type_t */
                        {
                            H5R_type_t reftype = (H5R_type_t)va_arg(ap, int);

                            switch (reftype) {
                                case H5R_BADTYPE:
                                    H5RS_acat(rs, "H5R_BADTYPE");
                                    break;

                                case H5R_OBJECT1:
                                    H5RS_acat(rs, "H5R_OBJECT1");
                                    break;

                                case H5R_DATASET_REGION1:
                                    H5RS_acat(rs, "H5R_DATASET_REGION1");
                                    break;

                                case H5R_OBJECT2:
                                    H5RS_acat(rs, "H5R_OBJECT2");
                                    break;

                                case H5R_DATASET_REGION2:
                                    H5RS_acat(rs, "H5R_DATASET_REGION2");
                                    break;

                                case H5R_ATTR:
                                    H5RS_acat(rs, "H5R_ATTR");
                                    break;

                                case H5R_MAXTYPE:
                                    H5RS_acat(rs, "H5R_MAXTYPE");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "BADTYPE(%ld)", (long)reftype);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(S%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'S':
                    switch (type[1]) {
                        case 'c': /* H5S_class_t */
                        {
                            H5S_class_t cls = (H5S_class_t)va_arg(ap, int);

                            switch (cls) {
                                case H5S_NO_CLASS:
                                    H5RS_acat(rs, "H5S_NO_CLASS");
                                    break;

                                case H5S_SCALAR:
                                    H5RS_acat(rs, "H5S_SCALAR");
                                    break;

                                case H5S_SIMPLE:
                                    H5RS_acat(rs, "H5S_SIMPLE");
                                    break;

                                case H5S_NULL:
                                    H5RS_acat(rs, "H5S_NULL");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)cls);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 's': /* H5S_seloper_t */
                        {
                            H5S_seloper_t so = (H5S_seloper_t)va_arg(ap, int);

                            switch (so) {
                                case H5S_SELECT_NOOP:
                                    H5RS_acat(rs, "H5S_NOOP");
                                    break;

                                case H5S_SELECT_SET:
                                    H5RS_acat(rs, "H5S_SELECT_SET");
                                    break;

                                case H5S_SELECT_OR:
                                    H5RS_acat(rs, "H5S_SELECT_OR");
                                    break;

                                case H5S_SELECT_AND:
                                    H5RS_acat(rs, "H5S_SELECT_AND");
                                    break;

                                case H5S_SELECT_XOR:
                                    H5RS_acat(rs, "H5S_SELECT_XOR");
                                    break;

                                case H5S_SELECT_NOTB:
                                    H5RS_acat(rs, "H5S_SELECT_NOTB");
                                    break;

                                case H5S_SELECT_NOTA:
                                    H5RS_acat(rs, "H5S_SELECT_NOTA");
                                    break;

                                case H5S_SELECT_APPEND:
                                    H5RS_acat(rs, "H5S_SELECT_APPEND");
                                    break;

                                case H5S_SELECT_PREPEND:
                                    H5RS_acat(rs, "H5S_SELECT_PREPEND");
                                    break;

                                case H5S_SELECT_INVALID:
                                    H5RS_acat(rs, "H5S_SELECT_INVALID");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)so);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 't': /* H5S_sel_type */
                        {
                            H5S_sel_type st = (H5S_sel_type)va_arg(ap, int);

                            switch (st) {
                                case H5S_SEL_ERROR:
                                    H5RS_acat(rs, "H5S_SEL_ERROR");
                                    break;

                                case H5S_SEL_NONE:
                                    H5RS_acat(rs, "H5S_SEL_NONE");
                                    break;

                                case H5S_SEL_POINTS:
                                    H5RS_acat(rs, "H5S_SEL_POINTS");
                                    break;

                                case H5S_SEL_HYPERSLABS:
                                    H5RS_acat(rs, "H5S_SEL_HYPERSLABS");
                                    break;

                                case H5S_SEL_ALL:
                                    H5RS_acat(rs, "H5S_SEL_ALL");
                                    break;

                                case H5S_SEL_N:
                                    H5RS_acat(rs, "H5S_SEL_N");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)st);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(S%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 't': /* htri_t */
                {
                    htri_t tri_var = va_arg(ap, htri_t);

                    if (tri_var > 0)
                        H5RS_acat(rs, "TRUE");
                    else if (!tri_var)
                        H5RS_acat(rs, "FALSE");
                    else
                        H5RS_asprintf_cat(rs, "FAIL(%d)", (int)tri_var);
                } /* end block */
                break;

                case 'T':
                    switch (type[1]) {
                        case 'c': /* H5T_cset_t */
                        {
                            H5T_cset_t cset = (H5T_cset_t)va_arg(ap, int);

                            H5_trace_args_cset(rs, cset);
                        } /* end block */
                        break;

                        case 'C': /* H5T_conv_t */
                        {
                            H5T_conv_t tconv = (H5T_conv_t)va_arg(ap, H5T_conv_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)tconv);
                        } /* end block */
                        break;

                        case 'd': /* H5T_direction_t */
                        {
                            H5T_direction_t direct = (H5T_direction_t)va_arg(ap, int);

                            switch (direct) {
                                case H5T_DIR_DEFAULT:
                                    H5RS_acat(rs, "H5T_DIR_DEFAULT");
                                    break;

                                case H5T_DIR_ASCEND:
                                    H5RS_acat(rs, "H5T_DIR_ASCEND");
                                    break;

                                case H5T_DIR_DESCEND:
                                    H5RS_acat(rs, "H5T_DIR_DESCEND");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)direct);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'e': /* H5T_pers_t */
                        {
                            H5T_pers_t pers = (H5T_pers_t)va_arg(ap, int);

                            switch (pers) {
                                case H5T_PERS_DONTCARE:
                                    H5RS_acat(rs, "H5T_PERS_DONTCARE");
                                    break;

                                case H5T_PERS_SOFT:
                                    H5RS_acat(rs, "H5T_PERS_SOFT");
                                    break;

                                case H5T_PERS_HARD:
                                    H5RS_acat(rs, "H5T_PERS_HARD");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)pers);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'E': /* H5T_conv_except_func_t */
                        {
                            H5T_conv_except_func_t conv_ex =
                                (H5T_conv_except_func_t)va_arg(ap, H5T_conv_except_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)conv_ex);
                        } /* end block */
                        break;

                        case 'n': /* H5T_norm_t */
                        {
                            H5T_norm_t norm = (H5T_norm_t)va_arg(ap, int);

                            switch (norm) {
                                case H5T_NORM_ERROR:
                                    H5RS_acat(rs, "H5T_NORM_ERROR");
                                    break;

                                case H5T_NORM_IMPLIED:
                                    H5RS_acat(rs, "H5T_NORM_IMPLIED");
                                    break;

                                case H5T_NORM_MSBSET:
                                    H5RS_acat(rs, "H5T_NORM_MSBSET");
                                    break;

                                case H5T_NORM_NONE:
                                    H5RS_acat(rs, "H5T_NORM_NONE");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)norm);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'o': /* H5T_order_t */
                        {
                            H5T_order_t order = (H5T_order_t)va_arg(ap, int);

                            switch (order) {
                                case H5T_ORDER_ERROR:
                                    H5RS_acat(rs, "H5T_ORDER_ERROR");
                                    break;

                                case H5T_ORDER_LE:
                                    H5RS_acat(rs, "H5T_ORDER_LE");
                                    break;

                                case H5T_ORDER_BE:
                                    H5RS_acat(rs, "H5T_ORDER_BE");
                                    break;

                                case H5T_ORDER_VAX:
                                    H5RS_acat(rs, "H5T_ORDER_VAX");
                                    break;

                                case H5T_ORDER_MIXED:
                                    H5RS_acat(rs, "H5T_ORDER_MIXED");
                                    break;

                                case H5T_ORDER_NONE:
                                    H5RS_acat(rs, "H5T_ORDER_NONE");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)order);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'p': /* H5T_pad_t */
                        {
                            H5T_pad_t pad = (H5T_pad_t)va_arg(ap, int);

                            switch (pad) {
                                case H5T_PAD_ERROR:
                                    H5RS_acat(rs, "H5T_PAD_ERROR");
                                    break;

                                case H5T_PAD_ZERO:
                                    H5RS_acat(rs, "H5T_PAD_ZERO");
                                    break;

                                case H5T_PAD_ONE:
                                    H5RS_acat(rs, "H5T_PAD_ONE");
                                    break;

                                case H5T_PAD_BACKGROUND:
                                    H5RS_acat(rs, "H5T_PAD_BACKGROUND");
                                    break;

                                case H5T_NPAD:
                                    H5RS_acat(rs, "H5T_NPAD");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)pad);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 's': /* H5T_sign_t */
                        {
                            H5T_sign_t sign = (H5T_sign_t)va_arg(ap, int);

                            switch (sign) {
                                case H5T_SGN_ERROR:
                                    H5RS_acat(rs, "H5T_SGN_ERROR");
                                    break;

                                case H5T_SGN_NONE:
                                    H5RS_acat(rs, "H5T_SGN_NONE");
                                    break;

                                case H5T_SGN_2:
                                    H5RS_acat(rs, "H5T_SGN_2");
                                    break;

                                case H5T_NSGN:
                                    H5RS_acat(rs, "H5T_NSGN");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)sign);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 't': /* H5T_class_t */
                        {
                            H5T_class_t type_class = (H5T_class_t)va_arg(ap, int);

                            switch (type_class) {
                                case H5T_NO_CLASS:
                                    H5RS_acat(rs, "H5T_NO_CLASS");
                                    break;

                                case H5T_INTEGER:
                                    H5RS_acat(rs, "H5T_INTEGER");
                                    break;

                                case H5T_FLOAT:
                                    H5RS_acat(rs, "H5T_FLOAT");
                                    break;

                                case H5T_TIME:
                                    H5RS_acat(rs, "H5T_TIME");
                                    break;

                                case H5T_STRING:
                                    H5RS_acat(rs, "H5T_STRING");
                                    break;

                                case H5T_BITFIELD:
                                    H5RS_acat(rs, "H5T_BITFIELD");
                                    break;

                                case H5T_OPAQUE:
                                    H5RS_acat(rs, "H5T_OPAQUE");
                                    break;

                                case H5T_COMPOUND:
                                    H5RS_acat(rs, "H5T_COMPOUND");
                                    break;

                                case H5T_REFERENCE:
                                    H5RS_acat(rs, "H5T_REFERENCE");
                                    break;

                                case H5T_ENUM:
                                    H5RS_acat(rs, "H5T_ENUM");
                                    break;

                                case H5T_VLEN:
                                    H5RS_acat(rs, "H5T_VLEN");
                                    break;

                                case H5T_ARRAY:
                                    H5RS_acat(rs, "H5T_ARRAY");
                                    break;

                                case H5T_NCLASSES:
                                    H5RS_acat(rs, "H5T_NCLASSES");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)type_class);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'z': /* H5T_str_t */
                        {
                            H5T_str_t str = (H5T_str_t)va_arg(ap, int);

                            switch (str) {
                                case H5T_STR_ERROR:
                                    H5RS_acat(rs, "H5T_STR_ERROR");
                                    break;

                                case H5T_STR_NULLTERM:
                                    H5RS_acat(rs, "H5T_STR_NULLTERM");
                                    break;

                                case H5T_STR_NULLPAD:
                                    H5RS_acat(rs, "H5T_STR_NULLPAD");
                                    break;

                                case H5T_STR_SPACEPAD:
                                    H5RS_acat(rs, "H5T_STR_SPACEPAD");
                                    break;

                                case H5T_STR_RESERVED_3:
                                case H5T_STR_RESERVED_4:
                                case H5T_STR_RESERVED_5:
                                case H5T_STR_RESERVED_6:
                                case H5T_STR_RESERVED_7:
                                case H5T_STR_RESERVED_8:
                                case H5T_STR_RESERVED_9:
                                case H5T_STR_RESERVED_10:
                                case H5T_STR_RESERVED_11:
                                case H5T_STR_RESERVED_12:
                                case H5T_STR_RESERVED_13:
                                case H5T_STR_RESERVED_14:
                                case H5T_STR_RESERVED_15:
                                    H5RS_asprintf_cat(rs, "H5T_STR_RESERVED(%ld)", (long)str);
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)str);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(T%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'U':
                    switch (type[1]) {
                        case 'l': /* unsigned long */
                        {
                            unsigned long iul = va_arg(ap, unsigned long);

                            H5RS_asprintf_cat(rs, "%lu", iul);
                            asize[argno] = (hssize_t)iul;
                        } /* end block */
                        break;

                        case 'L': /* unsigned long long / uint64_t */
                        {
                            unsigned long long iull = va_arg(ap, unsigned long long);

                            H5RS_asprintf_cat(rs, "%llu", iull);
                            asize[argno] = (hssize_t)iull;
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(U%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'V':
                    switch (type[1]) {
                        case 'a': /* H5VL_attr_get_t */
                        {
                            H5VL_attr_get_t get = (H5VL_attr_get_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_ATTR_GET_SPACE:
                                    H5RS_acat(rs, "H5VL_ATTR_GET_SPACE");
                                    break;

                                case H5VL_ATTR_GET_TYPE:
                                    H5RS_acat(rs, "H5VL_ATTR_GET_TYPE");
                                    break;

                                case H5VL_ATTR_GET_ACPL:
                                    H5RS_acat(rs, "H5VL_ATTR_GET_ACPL");
                                    break;

                                case H5VL_ATTR_GET_NAME:
                                    H5RS_acat(rs, "H5VL_ATTR_GET_NAME");
                                    break;

                                case H5VL_ATTR_GET_STORAGE_SIZE:
                                    H5RS_acat(rs, "H5VL_ATTR_GET_STORAGE_SIZE");
                                    break;

                                case H5VL_ATTR_GET_INFO:
                                    H5RS_acat(rs, "H5VL_ATTR_GET_INFO");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'A': /* H5VL_blob_optional_t */
                        {
                            H5VL_blob_optional_t optional = (H5VL_blob_optional_t)va_arg(ap, int);

                            H5RS_asprintf_cat(rs, "%ld", (long)optional);
                        } /* end block */
                        break;

                        case 'b': /* H5VL_attr_specific_t */
                        {
                            H5VL_attr_specific_t specific = (H5VL_attr_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_ATTR_DELETE:
                                    H5RS_acat(rs, "H5VL_ATTR_DELETE");
                                    break;

                                case H5VL_ATTR_DELETE_BY_IDX:
                                    H5RS_acat(rs, "H5VL_ATTR_DELETE_BY_IDX");
                                    break;

                                case H5VL_ATTR_EXISTS:
                                    H5RS_acat(rs, "H5VL_ATTR_EXISTS");
                                    break;

                                case H5VL_ATTR_ITER:
                                    H5RS_acat(rs, "H5VL_ATTR_ITER");
                                    break;

                                case H5VL_ATTR_RENAME:
                                    H5RS_acat(rs, "H5VL_ATTR_RENAME");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'B': /* H5VL_blob_specific_t */
                        {
                            H5VL_blob_specific_t specific = (H5VL_blob_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_BLOB_DELETE:
                                    H5RS_acat(rs, "H5VL_BLOB_DELETE");
                                    break;

                                case H5VL_BLOB_ISNULL:
                                    H5RS_acat(rs, "H5VL_BLOB_ISNULL");
                                    break;

                                case H5VL_BLOB_SETNULL:
                                    H5RS_acat(rs, "H5VL_BLOB_SETNULL");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'c': /* H5VL_dataset_get_t */
                        {
                            H5VL_dataset_get_t get = (H5VL_dataset_get_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_DATASET_GET_SPACE:
                                    H5RS_acat(rs, "H5VL_DATASET_GET_SPACE");
                                    break;

                                case H5VL_DATASET_GET_SPACE_STATUS:
                                    H5RS_acat(rs, "H5VL_DATASET_GET_SPACE_STATUS");
                                    break;

                                case H5VL_DATASET_GET_TYPE:
                                    H5RS_acat(rs, "H5VL_DATASET_GET_TYPE");
                                    break;

                                case H5VL_DATASET_GET_DCPL:
                                    H5RS_acat(rs, "H5VL_DATASET_GET_DCPL");
                                    break;

                                case H5VL_DATASET_GET_DAPL:
                                    H5RS_acat(rs, "H5VL_DATASET_GET_DAPL");
                                    break;

                                case H5VL_DATASET_GET_STORAGE_SIZE:
                                    H5RS_acat(rs, "H5VL_DATASET_GET_STORAGE_SIZE");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'C': /* H5VL_class_value_t */
                        {
                            H5VL_class_value_t class_val = (H5VL_class_value_t)va_arg(ap, H5VL_class_value_t);

                            if (H5_VOL_NATIVE == class_val)
                                H5RS_acat(rs, "H5_VOL_NATIVE");
                            else
                                H5RS_asprintf_cat(rs, "%ld", (long)class_val);
                        } /* end block */
                        break;

                        case 'd': /* H5VL_dataset_specific_t */
                        {
                            H5VL_dataset_specific_t specific = (H5VL_dataset_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_DATASET_SET_EXTENT:
                                    H5RS_acat(rs, "H5VL_DATASET_SET_EXTENT");
                                    break;

                                case H5VL_DATASET_FLUSH:
                                    H5RS_acat(rs, "H5VL_DATASET_FLUSH");
                                    break;

                                case H5VL_DATASET_REFRESH:
                                    H5RS_acat(rs, "H5VL_DATASET_REFRESH");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'e': /* H5VL_datatype_get_t */
                        {
                            H5VL_datatype_get_t get = (H5VL_datatype_get_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_DATATYPE_GET_BINARY_SIZE:
                                    H5RS_acat(rs, "H5VL_DATATYPE_GET_BINARY_SIZE");
                                    break;

                                case H5VL_DATATYPE_GET_BINARY:
                                    H5RS_acat(rs, "H5VL_DATATYPE_GET_BINARY");
                                    break;

                                case H5VL_DATATYPE_GET_TCPL:
                                    H5RS_acat(rs, "H5VL_DATATYPE_GET_TCPL");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'f': /* H5VL_datatype_specific_t */
                        {
                            H5VL_datatype_specific_t specific = (H5VL_datatype_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_DATATYPE_FLUSH:
                                    H5RS_acat(rs, "H5VL_DATATYPE_FLUSH");
                                    break;

                                case H5VL_DATATYPE_REFRESH:
                                    H5RS_acat(rs, "H5VL_DATATYPE_REFRESH");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'g': /* H5VL_file_get_t */
                        {
                            H5VL_file_get_t get = (H5VL_file_get_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_FILE_GET_CONT_INFO:
                                    H5RS_acat(rs, "H5VL_FILE_GET_CONT_INFO");
                                    break;

                                case H5VL_FILE_GET_FAPL:
                                    H5RS_acat(rs, "H5VL_FILE_GET_FAPL");
                                    break;

                                case H5VL_FILE_GET_FCPL:
                                    H5RS_acat(rs, "H5VL_FILE_GET_FCPL");
                                    break;

                                case H5VL_FILE_GET_FILENO:
                                    H5RS_acat(rs, "H5VL_FILE_GET_FILENO");
                                    break;

                                case H5VL_FILE_GET_INTENT:
                                    H5RS_acat(rs, "H5VL_FILE_GET_INTENT");
                                    break;

                                case H5VL_FILE_GET_NAME:
                                    H5RS_acat(rs, "H5VL_FILE_GET_NAME");
                                    break;

                                case H5VL_FILE_GET_OBJ_COUNT:
                                    H5RS_acat(rs, "H5VL_FILE_GET_OBJ_COUNT");
                                    break;

                                case H5VL_FILE_GET_OBJ_IDS:
                                    H5RS_acat(rs, "H5VL_FILE_GET_OBJ_IDS");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'h': /* H5VL_file_specific_t */
                        {
                            H5VL_file_specific_t specific = (H5VL_file_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_FILE_FLUSH:
                                    H5RS_acat(rs, "H5VL_FILE_FLUSH");
                                    break;

                                case H5VL_FILE_REOPEN:
                                    H5RS_acat(rs, "H5VL_FILE_REOPEN");
                                    break;

                                case H5VL_FILE_IS_ACCESSIBLE:
                                    H5RS_acat(rs, "H5VL_FILE_IS_ACCESSIBLE");
                                    break;

                                case H5VL_FILE_DELETE:
                                    H5RS_acat(rs, "H5VL_FILE_DELETE");
                                    break;

                                case H5VL_FILE_IS_EQUAL:
                                    H5RS_acat(rs, "H5VL_FILE_IS_EQUAL");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'i': /* H5VL_group_get_t */
                        {
                            H5VL_group_get_t get = (H5VL_group_get_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_GROUP_GET_GCPL:
                                    H5RS_acat(rs, "H5VL_GROUP_GET_GCPL");
                                    break;

                                case H5VL_GROUP_GET_INFO:
                                    H5RS_acat(rs, "H5VL_GROUP_GET_INFO");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'j': /* H5VL_group_specific_t */
                        {
                            H5VL_group_specific_t specific = (H5VL_group_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_GROUP_MOUNT:
                                    H5RS_acat(rs, "H5VL_GROUP_MOUNT");
                                    break;

                                case H5VL_GROUP_UNMOUNT:
                                    H5RS_acat(rs, "H5VL_GROUP_UNMOUNT");
                                    break;

                                case H5VL_GROUP_FLUSH:
                                    H5RS_acat(rs, "H5VL_GROUP_FLUSH");
                                    break;

                                case H5VL_GROUP_REFRESH:
                                    H5RS_acat(rs, "H5VL_GROUP_REFRESH");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'k': /* H5VL_link_create_t */
                        {
                            H5VL_link_create_t create = (H5VL_link_create_t)va_arg(ap, int);

                            switch (create) {
                                case H5VL_LINK_CREATE_HARD:
                                    H5RS_acat(rs, "H5VL_LINK_CREATE_HARD");
                                    break;

                                case H5VL_LINK_CREATE_SOFT:
                                    H5RS_acat(rs, "H5VL_LINK_CREATE_SOFT");
                                    break;

                                case H5VL_LINK_CREATE_UD:
                                    H5RS_acat(rs, "H5VL_LINK_CREATE_UD");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)create);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'l': /* H5VL_link_get_t */
                        {
                            H5VL_link_get_t get = (H5VL_link_get_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_LINK_GET_INFO:
                                    H5RS_acat(rs, "H5VL_LINK_GET_INFO");
                                    break;

                                case H5VL_LINK_GET_NAME:
                                    H5RS_acat(rs, "H5VL_LINK_GET_NAME");
                                    break;

                                case H5VL_LINK_GET_VAL:
                                    H5RS_acat(rs, "H5VL_LINK_GET_VAL");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'L': /* H5VL_get_conn_lvl_t */
                        {
                            H5VL_get_conn_lvl_t get = (H5VL_get_conn_lvl_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_GET_CONN_LVL_CURR:
                                    H5RS_acat(rs, "H5VL_GET_CONN_LVL_CURR");
                                    break;

                                case H5VL_GET_CONN_LVL_TERM:
                                    H5RS_acat(rs, "H5VL_GET_CONN_LVL_TERM");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'm': /* H5VL_link_specific_t */
                        {
                            H5VL_link_specific_t specific = (H5VL_link_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_LINK_DELETE:
                                    H5RS_acat(rs, "H5VL_LINK_DELETE");
                                    break;

                                case H5VL_LINK_EXISTS:
                                    H5RS_acat(rs, "H5VL_LINK_EXISTS");
                                    break;

                                case H5VL_LINK_ITER:
                                    H5RS_acat(rs, "H5VL_LINK_ITER");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'n': /* H5VL_object_get_t */
                        {
                            H5VL_object_get_t get = (H5VL_object_get_t)va_arg(ap, int);

                            switch (get) {
                                case H5VL_OBJECT_GET_FILE:
                                    H5RS_acat(rs, "H5VL_OBJECT_GET_FILE");
                                    break;

                                case H5VL_OBJECT_GET_NAME:
                                    H5RS_acat(rs, "H5VL_OBJECT_GET_NAME");
                                    break;

                                case H5VL_OBJECT_GET_TYPE:
                                    H5RS_acat(rs, "H5VL_OBJECT_GET_TYPE");
                                    break;

                                case H5VL_OBJECT_GET_INFO:
                                    H5RS_acat(rs, "H5VL_OBJECT_GET_INFO");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)get);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'N': /* H5VL_request_notify_t */
                        {
                            H5VL_request_notify_t vlrnot =
                                (H5VL_request_notify_t)va_arg(ap, H5VL_request_notify_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)vlrnot);
                        } /* end block */
                        break;

                        case 'o': /* H5VL_object_specific_t */
                        {
                            H5VL_object_specific_t specific = (H5VL_object_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_OBJECT_CHANGE_REF_COUNT:
                                    H5RS_acat(rs, "H5VL_OBJECT_CHANGE_REF_COUNT");
                                    break;

                                case H5VL_OBJECT_EXISTS:
                                    H5RS_acat(rs, "H5VL_OBJECT_EXISTS");
                                    break;

                                case H5VL_OBJECT_LOOKUP:
                                    H5RS_acat(rs, "H5VL_OBJECT_LOOKUP");
                                    break;

                                case H5VL_OBJECT_VISIT:
                                    H5RS_acat(rs, "H5VL_OBJECT_VISIT");
                                    break;

                                case H5VL_OBJECT_FLUSH:
                                    H5RS_acat(rs, "H5VL_OBJECT_FLUSH");
                                    break;

                                case H5VL_OBJECT_REFRESH:
                                    H5RS_acat(rs, "H5VL_OBJECT_REFRESH");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'r': /* H5VL_request_specific_t */
                        {
                            H5VL_request_specific_t specific = (H5VL_request_specific_t)va_arg(ap, int);

                            switch (specific) {
                                case H5VL_REQUEST_GET_ERR_STACK:
                                    H5RS_acat(rs, "H5VL_REQUEST_GET_ERR_STACK");
                                    break;

                                case H5VL_REQUEST_GET_EXEC_TIME:
                                    H5RS_acat(rs, "H5VL_REQUEST_GET_EXEC_TIME");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)specific);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 's': /* H5VL_attr_optional_t */
                        {
                            H5VL_attr_optional_t optional = (H5VL_attr_optional_t)va_arg(ap, int);

                            switch (optional) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                                case H5VL_NATIVE_ATTR_ITERATE_OLD:
                                    H5RS_acat(rs, "H5VL_NATIVE_ATTR_ITERATE_OLD");
                                    break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)optional);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'S': /* H5VL_subclass_t */
                        {
                            H5VL_subclass_t subclass = (H5VL_subclass_t)va_arg(ap, int);

                            switch (subclass) {
                                case H5VL_SUBCLS_NONE:
                                    H5RS_acat(rs, "H5VL_SUBCLS_NONE");
                                    break;

                                case H5VL_SUBCLS_INFO:
                                    H5RS_acat(rs, "H5VL_SUBCLS_INFO");
                                    break;

                                case H5VL_SUBCLS_WRAP:
                                    H5RS_acat(rs, "H5VL_SUBCLS_WRAP");
                                    break;

                                case H5VL_SUBCLS_ATTR:
                                    H5RS_acat(rs, "H5VL_SUBCLS_ATTR");
                                    break;

                                case H5VL_SUBCLS_DATASET:
                                    H5RS_acat(rs, "H5VL_SUBCLS_DATASET");
                                    break;

                                case H5VL_SUBCLS_DATATYPE:
                                    H5RS_acat(rs, "H5VL_SUBCLS_DATATYPE");
                                    break;

                                case H5VL_SUBCLS_FILE:
                                    H5RS_acat(rs, "H5VL_SUBCLS_FILE");
                                    break;

                                case H5VL_SUBCLS_GROUP:
                                    H5RS_acat(rs, "H5VL_SUBCLS_GROUP");
                                    break;

                                case H5VL_SUBCLS_LINK:
                                    H5RS_acat(rs, "H5VL_SUBCLS_LINK");
                                    break;

                                case H5VL_SUBCLS_OBJECT:
                                    H5RS_acat(rs, "H5VL_SUBCLS_OBJECT");
                                    break;

                                case H5VL_SUBCLS_REQUEST:
                                    H5RS_acat(rs, "H5VL_SUBCLS_REQUEST");
                                    break;

                                case H5VL_SUBCLS_BLOB:
                                    H5RS_acat(rs, "H5VL_SUBCLS_BLOB");
                                    break;

                                case H5VL_SUBCLS_TOKEN:
                                    H5RS_acat(rs, "H5VL_SUBCLS_TOKEN");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)subclass);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 't': /* H5VL_dataset_optional_t */
                        {
                            H5VL_dataset_optional_t optional = (H5VL_dataset_optional_t)va_arg(ap, int);

                            switch (optional) {
                                case H5VL_NATIVE_DATASET_FORMAT_CONVERT:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_FORMAT_CONVERT");
                                    break;

                                case H5VL_NATIVE_DATASET_GET_CHUNK_INDEX_TYPE:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_GET_CHUNK_INDEX_TYPE");
                                    break;

                                case H5VL_NATIVE_DATASET_GET_CHUNK_STORAGE_SIZE:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_GET_CHUNK_STORAGE_SIZE");
                                    break;

                                case H5VL_NATIVE_DATASET_GET_NUM_CHUNKS:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_GET_NUM_CHUNKS");
                                    break;

                                case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_IDX:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_IDX");
                                    break;

                                case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_COORD:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_COORD");
                                    break;

                                case H5VL_NATIVE_DATASET_CHUNK_READ:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_CHUNK_READ");
                                    break;

                                case H5VL_NATIVE_DATASET_CHUNK_WRITE:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_CHUNK_WRITE");
                                    break;

                                case H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE");
                                    break;

                                case H5VL_NATIVE_DATASET_GET_OFFSET:
                                    H5RS_acat(rs, "H5VL_NATIVE_DATASET_GET_OFFSET");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)optional);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'u': /* H5VL_datatype_optional_t */
                        {
                            H5VL_datatype_optional_t optional = (H5VL_datatype_optional_t)va_arg(ap, int);

                            H5RS_asprintf_cat(rs, "%ld", (long)optional);
                        } /* end block */
                        break;

                        case 'v': /* H5VL_file_optional_t */
                        {
                            H5VL_file_optional_t optional = (H5VL_file_optional_t)va_arg(ap, int);

                            switch (optional) {
                                case H5VL_NATIVE_FILE_CLEAR_ELINK_CACHE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_CLEAR_ELINK_CACHE");
                                    break;

                                case H5VL_NATIVE_FILE_GET_FILE_IMAGE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_FILE_IMAGE");
                                    break;

                                case H5VL_NATIVE_FILE_GET_FREE_SECTIONS:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_FREE_SECTIONS");
                                    break;

                                case H5VL_NATIVE_FILE_GET_FREE_SPACE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_FREE_SPACE");
                                    break;

                                case H5VL_NATIVE_FILE_GET_INFO:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_INFO");
                                    break;

                                case H5VL_NATIVE_FILE_GET_MDC_CONF:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_MDC_CONF");
                                    break;

                                case H5VL_NATIVE_FILE_GET_MDC_HR:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_MDC_HR");
                                    break;

                                case H5VL_NATIVE_FILE_GET_MDC_SIZE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_MDC_SIZE");
                                    break;

                                case H5VL_NATIVE_FILE_GET_SIZE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_SIZE");
                                    break;

                                case H5VL_NATIVE_FILE_GET_VFD_HANDLE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_VFD_HANDLE");
                                    break;

                                case H5VL_NATIVE_FILE_RESET_MDC_HIT_RATE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_RESET_MDC_HIT_RATE");
                                    break;

                                case H5VL_NATIVE_FILE_SET_MDC_CONFIG:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_SET_MDC_CONFIG");
                                    break;

                                case H5VL_NATIVE_FILE_GET_METADATA_READ_RETRY_INFO:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_METADATA_READ_RETRY_INFO");
                                    break;

                                case H5VL_NATIVE_FILE_START_SWMR_WRITE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_START_SWMR_WRITE");
                                    break;

                                case H5VL_NATIVE_FILE_START_MDC_LOGGING:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_START_MDC_LOGGING");
                                    break;

                                case H5VL_NATIVE_FILE_STOP_MDC_LOGGING:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_STOP_MDC_LOGGING");
                                    break;

                                case H5VL_NATIVE_FILE_GET_MDC_LOGGING_STATUS:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_MDC_LOGGING_STATUS");
                                    break;

                                case H5VL_NATIVE_FILE_FORMAT_CONVERT:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_FORMAT_CONVERT");
                                    break;

                                case H5VL_NATIVE_FILE_RESET_PAGE_BUFFERING_STATS:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_RESET_PAGE_BUFFERING_STATS");
                                    break;

                                case H5VL_NATIVE_FILE_GET_PAGE_BUFFERING_STATS:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_PAGE_BUFFERING_STATS");
                                    break;

                                case H5VL_NATIVE_FILE_GET_MDC_IMAGE_INFO:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_MDC_IMAGE_INFO");
                                    break;

                                case H5VL_NATIVE_FILE_GET_EOA:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_EOA");
                                    break;

                                case H5VL_NATIVE_FILE_INCR_FILESIZE:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_INCR_FILESIZE");
                                    break;

                                case H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS");
                                    break;

                                case H5VL_NATIVE_FILE_GET_MIN_DSET_OHDR_FLAG:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_MIN_DSET_OHDR_FLAG");
                                    break;

                                case H5VL_NATIVE_FILE_SET_MIN_DSET_OHDR_FLAG:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_SET_MIN_DSET_OHDR_FLAG");
                                    break;

#ifdef H5_HAVE_PARALLEL
                                case H5VL_NATIVE_FILE_GET_MPI_ATOMICITY:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_GET_MPI_ATOMICITY");
                                    break;

                                case H5VL_NATIVE_FILE_SET_MPI_ATOMICITY:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_SET_MPI_ATOMICITY");
                                    break;
#endif /* H5_HAVE_PARALLEL */

                                case H5VL_NATIVE_FILE_POST_OPEN:
                                    H5RS_acat(rs, "H5VL_NATIVE_FILE_POST_OPEN");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)optional);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'w': /* H5VL_group_optional_t */
                        {
                            H5VL_group_optional_t optional = (H5VL_group_optional_t)va_arg(ap, int);

                            switch (optional) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                                case H5VL_NATIVE_GROUP_ITERATE_OLD:
                                    H5RS_acat(rs, "H5VL_NATIVE_GROUP_ITERATE_OLD");
                                    break;

                                case H5VL_NATIVE_GROUP_GET_OBJINFO:
                                    H5RS_acat(rs, "H5VL_NATIVE_GROUP_GET_OBJINFO");
                                    break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)optional);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'x': /* H5VL_link_optional_t */
                        {
                            H5VL_link_optional_t optional = (H5VL_link_optional_t)va_arg(ap, int);

                            H5RS_asprintf_cat(rs, "%ld", (long)optional);
                        } /* end block */
                        break;

                        case 'y': /* H5VL_object_optional_t */
                        {
                            H5VL_object_optional_t optional = (H5VL_object_optional_t)va_arg(ap, int);

                            switch (optional) {
                                case H5VL_NATIVE_OBJECT_GET_COMMENT:
                                    H5RS_acat(rs, "H5VL_NATIVE_OBJECT_GET_COMMENT");
                                    break;

                                case H5VL_NATIVE_OBJECT_SET_COMMENT:
                                    H5RS_acat(rs, "H5VL_NATIVE_OBJECT_SET_COMMENT");
                                    break;

                                case H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES:
                                    H5RS_acat(rs, "H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES");
                                    break;

                                case H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES:
                                    H5RS_acat(rs, "H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES");
                                    break;

                                case H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED:
                                    H5RS_acat(rs, "H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED");
                                    break;

                                case H5VL_NATIVE_OBJECT_GET_NATIVE_INFO:
                                    H5RS_acat(rs, "H5VL_NATIVE_OBJECT_GET_NATIVE_INFO");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)optional);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'z': /* H5VL_request_optional_t */
                        {
                            H5VL_request_optional_t optional = (H5VL_request_optional_t)va_arg(ap, int);

                            H5RS_asprintf_cat(rs, "%ld", (long)optional);
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(Z%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case 'x': { /* void / va_list */
                    vp = va_arg(ap, void *);

                    if (vp)
                        H5RS_asprintf_cat(rs, "%p", vp);
                    else
                        H5RS_acat(rs, "NULL");
                } /* end block */
                break;

                case 'z': {
                    size_t size = va_arg(ap, size_t);

                    H5RS_asprintf_cat(rs, "%zu", size);
                    asize[argno] = (hssize_t)size;
                } /* end block */
                break;

                case 'Z':
                    switch (type[1]) {
                        case 'a': /* H5Z_SO_scale_type_t */
                        {
                            H5Z_SO_scale_type_t scale_type = (H5Z_SO_scale_type_t)va_arg(ap, int);

                            switch (scale_type) {
                                case H5Z_SO_FLOAT_DSCALE:
                                    H5RS_acat(rs, "H5Z_SO_FLOAT_DSCALE");
                                    break;

                                case H5Z_SO_FLOAT_ESCALE:
                                    H5RS_acat(rs, "H5Z_SO_FLOAT_ESCALE");
                                    break;

                                case H5Z_SO_INT:
                                    H5RS_acat(rs, "H5Z_SO_INT");
                                    break;

                                default:
                                    H5RS_asprintf_cat(rs, "%ld", (long)scale_type);
                                    break;
                            } /* end switch */
                        }     /* end block */
                        break;

                        case 'c': /* H5Z_class2_t */
                        {
                            H5Z_class2_t *filter = va_arg(ap, H5Z_class2_t *);

                            H5RS_asprintf_cat(rs, "%p", (void *)filter);
                        } /* end block  */
                        break;

                        case 'e': /* H5Z_EDC_t */
                        {
                            H5Z_EDC_t edc = (H5Z_EDC_t)va_arg(ap, int);

                            if (H5Z_DISABLE_EDC == edc)
                                H5RS_acat(rs, "H5Z_DISABLE_EDC");
                            else if (H5Z_ENABLE_EDC == edc)
                                H5RS_acat(rs, "H5Z_ENABLE_EDC");
                            else
                                H5RS_asprintf_cat(rs, "%ld", (long)edc);
                        } /* end block */
                        break;

                        case 'f': /* H5Z_filter_t */
                        {
                            H5Z_filter_t id = va_arg(ap, H5Z_filter_t);

                            if (H5Z_FILTER_NONE == id)
                                H5RS_acat(rs, "H5Z_FILTER_NONE");
                            else if (H5Z_FILTER_DEFLATE == id)
                                H5RS_acat(rs, "H5Z_FILTER_DEFLATE");
                            else if (H5Z_FILTER_SHUFFLE == id)
                                H5RS_acat(rs, "H5Z_FILTER_SHUFFLE");
                            else if (H5Z_FILTER_FLETCHER32 == id)
                                H5RS_acat(rs, "H5Z_FILTER_FLETCHER32");
                            else if (H5Z_FILTER_SZIP == id)
                                H5RS_acat(rs, "H5Z_FILTER_SZIP");
                            else if (H5Z_FILTER_NBIT == id)
                                H5RS_acat(rs, "H5Z_FILTER_NBIT");
                            else if (H5Z_FILTER_SCALEOFFSET == id)
                                H5RS_acat(rs, "H5Z_FILTER_SCALEOFFSET");
                            else
                                H5RS_asprintf_cat(rs, "%ld", (long)id);
                        } /* end block */
                        break;

                        case 'F': /* H5Z_filter_func_t */
                        {
                            H5Z_filter_func_t ffunc = (H5Z_filter_func_t)va_arg(ap, H5Z_filter_func_t);

                            H5RS_asprintf_cat(rs, "%p", (void *)(uintptr_t)ffunc);
                        } /* end block */
                        break;

                        case 's': {
                            ssize_t ssize = va_arg(ap, ssize_t);

                            H5RS_asprintf_cat(rs, "%zd", ssize);
                            asize[argno] = (hssize_t)ssize;
                        } /* end block */
                        break;

                        default:
                            H5RS_asprintf_cat(rs, "BADTYPE(Z%c)", type[1]);
                            goto error;
                    } /* end switch */
                    break;

                case '#':
                    H5RS_acat(rs, "Unsupported type slipped through!");
                    break;

                case '!':
                    H5RS_acat(rs, "Unknown type slipped through!");
                    break;

                default:
                    if (isupper(type[0]))
                        H5RS_asprintf_cat(rs, "BADTYPE(%c%c)", type[0], type[1]);
                    else
                        H5RS_asprintf_cat(rs, "BADTYPE(%c)", type[0]);
                    goto error;
            } /* end switch */
        }     /* end else */
    }         /* end for */

    return SUCCEED;
error:
    return FAIL;
} /* end H5_trace_args() */

/*-------------------------------------------------------------------------
 * Function:    H5_trace
 *
 * Purpose:     This function is called whenever an API function is called
 *              and tracing is turned on.  If RETURNING is non-zero then
 *              the caller is about to return and RETURNING points to the
 *              time for the corresponding function call event.  Otherwise
 *              we print the function name and the arguments.
 *
 *              The TYPE argument is a string which gives the type of each of
 *              the following argument pairs.  Each type is zero or more
 *              asterisks (one for each level of indirection, although some
 *              types have one level of indirection already implied) followed
 *              by either one letter (lower case) or two letters (first one
 *              uppercase).
 *
 *              The variable argument list consists of pairs of values. Each
 *              pair is a string which is the formal argument name in the
 *              calling function, followed by the argument value.  The type
 *              of the argument value is given by the TYPE string.
 *
 * Note:        The TYPE string is meant to be terse and is generated by a
 *              separate perl script.
 *
 * WARNING:     DO NOT CALL ANY HDF5 FUNCTION THAT CALLS FUNC_ENTER(). DOING
 *              SO MAY CAUSE H5_trace() TO BE INVOKED RECURSIVELY OR MAY
 *              CAUSE LIBRARY INITIALIZATIONS THAT ARE NOT DESIRED.
 *
 * Return:      Execution time for an API call
 *
 *-------------------------------------------------------------------------
 */
double
H5_trace(const double *returning, const char *func, const char *type, ...)
{
    va_list           ap;
    H5RS_str_t       *rs = NULL;
    hssize_t          i;
    FILE             *out                 = H5_debug_g.trace;
    static bool       is_first_invocation = true;
    H5_timer_t        function_timer      = {{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}, false};
    H5_timevals_t     function_times      = {0.0, 0.0, 0.0};
    static H5_timer_t running_timer;
    H5_timevals_t     running_times;
    static int        current_depth   = 0;
    static int        last_call_depth = 0;

    /* FUNC_ENTER() should not be called */

    if (!out)
        return 0.0; /* Tracing is off */

    /* Initialize the timer for this function */
    if (H5_debug_g.ttimes)
        H5_timer_init(&function_timer);

    if (H5_debug_g.ttop) {
        if (returning) {
            if (current_depth > 1) {
                --current_depth;
                return 0.0;
            }
        }
        else {
            if (current_depth > 0) {
                /* Do not update last_call_depth */
                current_depth++;
                return 0.0;
            }
        }
    }

    /* Get time for event if the trace times flag is set */
    if (is_first_invocation && H5_debug_g.ttimes) {
        /* Start the library-wide timer */
        is_first_invocation = false;
        H5_timer_init(&running_timer);
        H5_timer_start(&running_timer);
    }

    /* Start the timer for this function */
    if (H5_debug_g.ttimes)
        H5_timer_start(&function_timer);

    /* Create the ref-counted string */
    rs = H5RS_create(NULL);

    /* Print the first part of the line.  This is the indication of the
     * nesting depth followed by the function name and either start of
     * argument list or start of return value.  If this call is for a
     * function return and no other calls have been made to H5_trace()
     * since the one for the function call, then we're continuing
     * the same line. */
    if (returning) {
        assert(current_depth > 0);
        --current_depth;
        if (current_depth < last_call_depth) {
            /* We are at the beginning of a line */
            if (H5_debug_g.ttimes) {
                char tmp[320];

                H5_timer_get_times(function_timer, &function_times);
                H5_timer_get_times(running_timer, &running_times);
                snprintf(tmp, sizeof(tmp), "%.6f", (function_times.elapsed - running_times.elapsed));
                H5RS_asprintf_cat(rs, " %*s ", (int)strlen(tmp), "");
            }
            for (i = 0; i < current_depth; i++)
                H5RS_aputc(rs, '+');
            H5RS_asprintf_cat(rs, "%*s%s = ", 2 * current_depth, "", func);
        }
        else
            /* Continue current line with return value */
            H5RS_acat(rs, " = ");
    }
    else {
        if (current_depth > last_call_depth)
            H5RS_acat(rs, " = <delayed>\n");
        if (H5_debug_g.ttimes) {
            H5_timer_get_times(function_timer, &function_times);
            H5_timer_get_times(running_timer, &running_times);
            H5RS_asprintf_cat(rs, "@%.6f ", (function_times.elapsed - running_times.elapsed));
        }
        for (i = 0; i < current_depth; i++)
            H5RS_aputc(rs, '+');
        H5RS_asprintf_cat(rs, "%*s%s(", 2 * current_depth, "", func);
    }

    /* Format arguments into the refcounted string */
    va_start(ap, type);
    H5_trace_args(rs, type, ap);
    va_end(ap);

    /* Display event time for return */
    if (returning && H5_debug_g.ttimes) {
        H5_timer_get_times(function_timer, &function_times);
        H5_timer_get_times(running_timer, &running_times);
        H5RS_asprintf_cat(rs, " @%.6f [dt=%.6f]", (function_times.elapsed - running_times.elapsed),
                          (function_times.elapsed - *returning));
    }

    /* Display generated string */
    if (returning)
        H5RS_acat(rs, ";\n");
    else {
        last_call_depth = current_depth++;
        H5RS_acat(rs, ")");
    }
    fputs(H5RS_get_str(rs), out);
    fflush(out);
    H5RS_decr(rs);

    if (H5_debug_g.ttimes)
        return function_times.elapsed;
    else
        return 0.0;
} /* end H5_trace() */
