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

/*
 * Module Info: This module contains command functionality for fields in
 *      enumerated & compound datatypes in the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"   /*generic functions			  */
#include "H5Eprivate.h"  /*error handling			  */
#include "H5Iprivate.h"  /*ID functions		   		  */
#include "H5MMprivate.h" /*memory management			  */
#include "H5Tpkg.h"      /*data-type functions			  */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_nmembers
 *
 * Purpose:	Determines how many members TYPE_ID has.  The type must be
 *		either a compound datatype or an enumeration datatype.
 *
 * Return:	Success:	Number of members defined in the datatype.
 *
 *		Failure:	Negative
 *
 * Errors:
 *
 *-------------------------------------------------------------------------
 */
int
H5Tget_nmembers(hid_t type_id)
{
    H5T_t *dt;        /* Datatype to query */
    int    ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("Is", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    if ((ret_value = H5T_get_nmembers(dt)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "cannot return member number");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_nmembers() */

/*-------------------------------------------------------------------------
 * Function:	H5T_get_nmembers
 *
 * Purpose:	Private function for H5Tget_nmembers.  Determines how many
 *              members DTYPE has.  The type must be either a compound data
 *              type or an enumeration datatype.
 *
 * Return:	Success:	Number of members defined in the datatype.
 *
 *		Failure:	Negative
 *
 * Errors:
 *
 *-------------------------------------------------------------------------
 */
int
H5T_get_nmembers(const H5T_t *dt)
{
    int ret_value = -1; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(dt);

    if (H5T_COMPOUND == dt->shared->type)
        ret_value = (int)dt->shared->u.compnd.nmembs;
    else if (H5T_ENUM == dt->shared->type)
        ret_value = (int)dt->shared->u.enumer.nmembs;
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "operation not supported for type class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_nmembers() */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_member_name
 *
 * Purpose:	Returns the name of a member of a compound or enumeration
 *		datatype. Members are stored in no particular order with
 *		numbers 0 through N-1 where N is the value returned by
 *		H5Tget_nmembers().
 *
 * Return:	Success:	Ptr to a string allocated with malloc().  The
 *				caller is responsible for freeing the string.
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
char *
H5Tget_member_name(hid_t type_id, unsigned membno)
{
    H5T_t *dt = NULL;
    char  *ret_value;

    FUNC_ENTER_API(NULL)
    H5TRACE2("*s", "iIu", type_id, membno);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a datatype");

    if (NULL == (ret_value = H5T__get_member_name(dt, membno)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "unable to get member name");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5T__get_member_name
 *
 * Purpose:	Private function for H5Tget_member_name.  Returns the name
 *              of a member of a compound or enumeration datatype. Members
 *              are stored in no particular order with numbers 0 through
 *              N-1 where N is the value returned by H5Tget_nmembers().
 *
 * Return:	Success:	Ptr to a string allocated with malloc().  The
 *				caller is responsible for freeing the string.
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
char *
H5T__get_member_name(H5T_t const *dt, unsigned membno)
{
    char *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dt);

    switch (dt->shared->type) {
        case H5T_COMPOUND:
            if (membno >= dt->shared->u.compnd.nmembs)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid member number");
            ret_value = H5MM_xstrdup(dt->shared->u.compnd.memb[membno].name);
            break;

        case H5T_ENUM:
            if (membno >= dt->shared->u.enumer.nmembs)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid member number");
            ret_value = H5MM_xstrdup(dt->shared->u.enumer.name[membno]);
            break;

        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_VLEN:
        case H5T_ARRAY:
        case H5T_NCLASSES:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "operation not supported for type class");
    } /*lint !e788 All appropriate cases are covered */

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Tget_member_index
 *
 * Purpose:     Returns the index of a member in a compound or enumeration
 *              datatype by given name.Members are stored in no particular
 *              order with numbers 0 through N-1 where N is the value
 *              returned by H5Tget_nmembers().
 *
 * Return:      Success:        index of the member if exists.
 *              Failure:        -1.
 *
 *-------------------------------------------------------------------------
 */
int
H5Tget_member_index(hid_t type_id, const char *name)
{
    H5T_t   *dt        = NULL;
    int      ret_value = FAIL;
    unsigned i;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("Is", "i*s", type_id, name);

    /* Check arguments */
    assert(name);
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Locate member by name */
    switch (dt->shared->type) {
        case H5T_COMPOUND:
            for (i = 0; i < dt->shared->u.compnd.nmembs; i++)
                if (!strcmp(dt->shared->u.compnd.memb[i].name, name))
                    HGOTO_DONE((int)i);
            break;
        case H5T_ENUM:
            for (i = 0; i < dt->shared->u.enumer.nmembs; i++)
                if (!strcmp(dt->shared->u.enumer.name[i], name))
                    HGOTO_DONE((int)i);
            break;

        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_VLEN:
        case H5T_ARRAY:
        case H5T_NCLASSES:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "operation not supported for this type");
    } /*lint !e788 All appropriate cases are covered */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_member_index() */

/*-------------------------------------------------------------------------
 * Function:	H5T__sort_value
 *
 * Purpose:	Sorts the members of a compound datatype by their offsets;
 *		sorts the members of an enum type by their values. This even
 *		works for locked datatypes since it doesn't change the value
 *		of the type.  MAP is an optional parallel integer array which
 *		is also swapped along with members of DT.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__sort_value(const H5T_t *dt, int *map)
{
    unsigned nmembs; /* Number of members for datatype */
    size_t   size;
    bool     swapped; /* Whether we've swapped fields */
    uint8_t  tbuf[32];
    unsigned i, j;                /* Local index variables */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(dt);
    assert(H5T_COMPOUND == dt->shared->type || H5T_ENUM == dt->shared->type);

    /* Use a bubble sort because we can short circuit */
    if (H5T_COMPOUND == dt->shared->type) {
        if (H5T_SORT_VALUE != dt->shared->u.compnd.sorted) {
            dt->shared->u.compnd.sorted = H5T_SORT_VALUE;
            nmembs                      = dt->shared->u.compnd.nmembs;
            for (i = nmembs - 1, swapped = true; i > 0 && swapped; --i) {
                for (j = 0, swapped = false; j < i; j++) {
                    if (dt->shared->u.compnd.memb[j].offset > dt->shared->u.compnd.memb[j + 1].offset) {
                        H5T_cmemb_t tmp                  = dt->shared->u.compnd.memb[j];
                        dt->shared->u.compnd.memb[j]     = dt->shared->u.compnd.memb[j + 1];
                        dt->shared->u.compnd.memb[j + 1] = tmp;
                        if (map) {
                            int x = map[j];

                            map[j]     = map[j + 1];
                            map[j + 1] = x;
                        } /* end if */
                        swapped = true;
                    } /* end if */
                }     /* end for */
            }         /* end for */
#ifndef NDEBUG
            /* I never trust a sort :-) -RPM */
            for (i = 0; i < (nmembs - 1); i++)
                assert(dt->shared->u.compnd.memb[i].offset < dt->shared->u.compnd.memb[i + 1].offset);
#endif
        } /* end if */
    }
    else if (H5T_ENUM == dt->shared->type) {
        if (H5T_SORT_VALUE != dt->shared->u.enumer.sorted) {
            dt->shared->u.enumer.sorted = H5T_SORT_VALUE;
            nmembs                      = dt->shared->u.enumer.nmembs;
            size                        = dt->shared->size;
            assert(size <= sizeof(tbuf));
            for (i = (nmembs - 1), swapped = true; i > 0 && swapped; --i) {
                for (j = 0, swapped = false; j < i; j++) {
                    if (memcmp((uint8_t *)dt->shared->u.enumer.value + (j * size),
                               (uint8_t *)dt->shared->u.enumer.value + ((j + 1) * size), size) > 0) {
                        /* Swap names */
                        char *tmp                        = dt->shared->u.enumer.name[j];
                        dt->shared->u.enumer.name[j]     = dt->shared->u.enumer.name[j + 1];
                        dt->shared->u.enumer.name[j + 1] = tmp;

                        /* Swap values */
                        H5MM_memcpy(tbuf, (uint8_t *)dt->shared->u.enumer.value + (j * size), size);
                        H5MM_memcpy((uint8_t *)dt->shared->u.enumer.value + (j * size),
                                    (uint8_t *)dt->shared->u.enumer.value + ((j + 1) * size), size);
                        H5MM_memcpy((uint8_t *)dt->shared->u.enumer.value + ((j + 1) * size), tbuf, size);

                        /* Swap map */
                        if (map) {
                            int x = map[j];

                            map[j]     = map[j + 1];
                            map[j + 1] = x;
                        } /* end if */

                        swapped = true;
                    } /* end if */
                }     /* end for */
            }         /* end for */
#ifndef NDEBUG
            /* I never trust a sort :-) -RPM */
            for (i = 0; i < (nmembs - 1); i++)
                assert(memcmp((uint8_t *)dt->shared->u.enumer.value + (i * size),
                              (uint8_t *)dt->shared->u.enumer.value + ((i + 1) * size), size) < 0);
#endif
        } /* end if */
    }     /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__sort_value() */

/*-------------------------------------------------------------------------
 * Function:	H5T__sort_name
 *
 * Purpose:	Sorts members of a compound or enumeration datatype by their
 *		names. This even works for locked datatypes since it doesn't
 *		change the value of the types.
 *
 * Return:	Success:	Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__sort_name(const H5T_t *dt, int *map)
{
    unsigned i, j, nmembs;
    size_t   size;
    bool     swapped;
    uint8_t  tbuf[32];

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(dt);
    assert(H5T_COMPOUND == dt->shared->type || H5T_ENUM == dt->shared->type);

    /* Use a bubble sort because we can short circuit */
    if (H5T_COMPOUND == dt->shared->type) {
        if (H5T_SORT_NAME != dt->shared->u.compnd.sorted) {
            dt->shared->u.compnd.sorted = H5T_SORT_NAME;
            nmembs                      = dt->shared->u.compnd.nmembs;
            for (i = nmembs - 1, swapped = true; i > 0 && swapped; --i) {
                for (j = 0, swapped = false; j < i; j++) {
                    if (strcmp(dt->shared->u.compnd.memb[j].name, dt->shared->u.compnd.memb[j + 1].name) >
                        0) {
                        H5T_cmemb_t tmp                  = dt->shared->u.compnd.memb[j];
                        dt->shared->u.compnd.memb[j]     = dt->shared->u.compnd.memb[j + 1];
                        dt->shared->u.compnd.memb[j + 1] = tmp;
                        swapped                          = true;
                        if (map) {
                            int x      = map[j];
                            map[j]     = map[j + 1];
                            map[j + 1] = x;
                        }
                    }
                }
            }
#ifndef NDEBUG
            /* I never trust a sort :-) -RPM */
            for (i = 0; i < nmembs - 1; i++) {
                assert(strcmp(dt->shared->u.compnd.memb[i].name, dt->shared->u.compnd.memb[i + 1].name) < 0);
            }
#endif
        }
    }
    else if (H5T_ENUM == dt->shared->type) {
        if (H5T_SORT_NAME != dt->shared->u.enumer.sorted) {
            dt->shared->u.enumer.sorted = H5T_SORT_NAME;
            nmembs                      = dt->shared->u.enumer.nmembs;
            size                        = dt->shared->size;
            assert(size <= sizeof(tbuf));
            for (i = nmembs - 1, swapped = true; i > 0 && swapped; --i) {
                for (j = 0, swapped = false; j < i; j++) {
                    if (strcmp(dt->shared->u.enumer.name[j], dt->shared->u.enumer.name[j + 1]) > 0) {
                        /* Swap names */
                        char *tmp                        = dt->shared->u.enumer.name[j];
                        dt->shared->u.enumer.name[j]     = dt->shared->u.enumer.name[j + 1];
                        dt->shared->u.enumer.name[j + 1] = tmp;

                        /* Swap values */
                        H5MM_memcpy(tbuf, (uint8_t *)dt->shared->u.enumer.value + (j * size), size);
                        H5MM_memcpy((uint8_t *)dt->shared->u.enumer.value + (j * size),
                                    (uint8_t *)dt->shared->u.enumer.value + ((j + 1) * size), size);
                        H5MM_memcpy((uint8_t *)dt->shared->u.enumer.value + ((j + 1) * size), tbuf, size);

                        /* Swap map */
                        if (map) {
                            int x      = map[j];
                            map[j]     = map[j + 1];
                            map[j + 1] = x;
                        }

                        swapped = true;
                    }
                }
            }
#ifndef NDEBUG
            /* I never trust a sort :-) -RPM */
            for (i = 0; i < nmembs - 1; i++)
                assert(strcmp(dt->shared->u.enumer.name[i], dt->shared->u.enumer.name[i + 1]) < 0);
#endif
        }
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
}
