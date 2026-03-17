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
 * Module Info: This module contains the functionality for enumerated datatypes
 *      in the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"   /*generic functions			  */
#include "H5Eprivate.h"  /*error handling			  */
#include "H5Iprivate.h"  /*ID functions		   		  */
#include "H5MMprivate.h" /*memory management			  */
#include "H5Tpkg.h"      /*data-type functions			  */

/* Static local functions */
static char  *H5T__enum_nameof(const H5T_t *dt, const void *value, char *name /*out*/, size_t size);
static herr_t H5T__enum_valueof(const H5T_t *dt, const char *name, void *value /*out*/);

/*-------------------------------------------------------------------------
 * Function:	H5Tenum_create
 *
 * Purpose:	Create a new enumeration data type based on the specified
 *		TYPE, which must be an integer type.
 *
 * Return:	Success:	ID of new enumeration data type
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tenum_create(hid_t parent_id)
{
    H5T_t *parent = NULL; /*base integer data type	*/
    H5T_t *dt     = NULL; /*new enumeration data type	*/
    hid_t  ret_value;     /*return value			*/

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", parent_id);

    /* Check args */
    if (NULL == (parent = (H5T_t *)H5I_object_verify(parent_id, H5I_DATATYPE)) ||
        H5T_INTEGER != parent->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not an integer data type");

    /* Build new type */
    if (NULL == (dt = H5T__enum_create(parent)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, H5I_INVALID_HID, "cannot create enum type");

    /* Register the type */
    if ((ret_value = H5I_register(H5I_DATATYPE, dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register data type ID");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tenum_create() */

/*-------------------------------------------------------------------------
 * Function:	H5T__enum_create
 *
 * Purpose:	Private function for H5Tenum_create.  Create a new
 *              enumeration data type based on the specified
 *		TYPE, which must be an integer type.
 *
 * Return:	Success:	new enumeration data type
 *
 *		Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T__enum_create(const H5T_t *parent)
{
    H5T_t *ret_value = NULL; /* New enumeration data type	*/

    FUNC_ENTER_PACKAGE

    assert(parent);

    /* Build new type */
    if (NULL == (ret_value = H5T__alloc()))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    ret_value->shared->type   = H5T_ENUM;
    ret_value->shared->parent = H5T_copy(parent, H5T_COPY_ALL);
    assert(ret_value->shared->parent);
    ret_value->shared->size = ret_value->shared->parent->shared->size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tenum_insert
 *
 * Purpose:	Insert a new enumeration data type member into an enumeration
 *		type. TYPE is the enumeration type, NAME is the name of the
 *		new member, and VALUE points to the value of the new member.
 *		The NAME and VALUE must both be unique within the TYPE. VALUE
 *		points to data which is of the data type defined when the
 *		enumeration type was created.
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tenum_insert(hid_t type, const char *name, const void *value)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*s*x", type, name, value);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5T_ENUM != dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an enumeration data type");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
    if (!value)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no value specified");

    /* Do work */
    if (H5T__enum_insert(dt, name, value) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to insert new enumeration member");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5T__enum_insert
 *
 * Purpose:	Insert a new member having a NAME and VALUE into an
 *		enumeration data TYPE.  The NAME and VALUE must both be
 *		unique. The VALUE points to data of the data type defined for
 *		the enumeration base type.
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__enum_insert(const H5T_t *dt, const char *name, const void *value)
{
    unsigned i;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dt);
    assert(name && *name);
    assert(value);

    /* The name and value had better not already exist */
    for (i = 0; i < dt->shared->u.enumer.nmembs; i++) {
        if (!strcmp(dt->shared->u.enumer.name[i], name))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "name redefinition");
        if (!memcmp((uint8_t *)dt->shared->u.enumer.value + (i * dt->shared->size), value, dt->shared->size))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "value redefinition");
    }

    /* Increase table sizes */
    if (dt->shared->u.enumer.nmembs >= dt->shared->u.enumer.nalloc) {
        char   **names;
        uint8_t *values;
        unsigned n = MAX(32, 2 * dt->shared->u.enumer.nalloc);

        if (NULL == (names = (char **)H5MM_realloc(dt->shared->u.enumer.name, n * sizeof(char *))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
        dt->shared->u.enumer.name = names;

        if (NULL == (values = (uint8_t *)H5MM_realloc(dt->shared->u.enumer.value, n * dt->shared->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
        dt->shared->u.enumer.value  = values;
        dt->shared->u.enumer.nalloc = n;
    }

    /* Insert new member at end of member arrays */
    dt->shared->u.enumer.sorted  = H5T_SORT_NONE;
    i                            = dt->shared->u.enumer.nmembs++;
    dt->shared->u.enumer.name[i] = H5MM_xstrdup(name);
    H5MM_memcpy((uint8_t *)dt->shared->u.enumer.value + (i * dt->shared->size), value, dt->shared->size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tget_member_value
 *
 * Purpose:	Return the value for an enumeration data type member.
 *
 * Return:	Success:	non-negative with the member value copied
 *				into the memory pointed to by VALUE.
 *
 *		Failure:	negative, VALUE memory is undefined.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tget_member_value(hid_t type, unsigned membno, void *value /*out*/)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iIux", type, membno, value);

    if (NULL == (dt = (H5T_t *)H5I_object_verify(type, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5T_ENUM != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not defined for data type class");
    if (membno >= dt->shared->u.enumer.nmembs)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid member number");
    if (!value)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "null value buffer");

    if (H5T__get_member_value(dt, membno, value) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get member value");
done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5T__get_member_value
 *
 * Purpose:	Private function for H5T__get_member_value.  Return the
 *              value for an enumeration data type member.
 *
 * Return:	Success:	non-negative with the member value copied
 *				into the memory pointed to by VALUE.
 *
 *		Failure:	negative, VALUE memory is undefined.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__get_member_value(const H5T_t *dt, unsigned membno, void *value /*out*/)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(dt);
    assert(value);

    H5MM_memcpy(value, (uint8_t *)dt->shared->u.enumer.value + (membno * dt->shared->size), dt->shared->size);

    FUNC_LEAVE_NOAPI(SUCCEED)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tenum_nameof
 *
 * Purpose:	Finds the symbol name that corresponds to the specified VALUE
 *		of an enumeration data type TYPE. At most SIZE characters of
 *		the symbol name are copied into the NAME buffer. If the
 *		entire symbol anem and null terminator do not fit in the NAME
 *		buffer then as many characters as possible are copied (not
 *		null terminated) and the function fails.
 *
 * Return:	Success:	Non-negative.
 *
 *		Failure:	Negative, first character of NAME is set to
 *				null if SIZE allows it.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tenum_nameof(hid_t type, const void *value, char *name /*out*/, size_t size)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*xxz", type, value, name, size);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5T_ENUM != dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an enumeration data type");
    if (!value)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no value supplied");
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name buffer supplied");

    if (NULL == H5T__enum_nameof(dt, value, name, size))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "nameof query failed");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5T__enum_nameof
 *
 * Purpose:	Finds the symbol name that corresponds to the specified
 *		VALUE of an enumeration data type DT. At most SIZE characters
 *		of the symbol name are copied into the NAME buffer. If the
 *		entire symbol name and null terminator do not fit in the NAME
 *		buffer then as many characters as possible are copied and the
 *		function returns failure.
 *
 *		If NAME is the null pointer and SIZE is zero then enough
 *		space is allocated to hold the result and a pointer to that
 *		memory is returned.
 *
 * Return:	Success:	Pointer to NAME
 *
 *		Failure:	NULL, name[0] is set to null.
 *
 *-------------------------------------------------------------------------
 */
static char *
H5T__enum_nameof(const H5T_t *dt, const void *value, char *name /*out*/, size_t size)
{
    H5T_t   *copied_dt = NULL;   /* Do sorting in copied datatype */
    unsigned lt, md = 0, rt;     /* Indices for binary search	*/
    int      cmp        = (-1);  /* Comparison result		*/
    bool     alloc_name = false; /* Whether name has been allocated */
    char    *ret_value  = NULL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dt && H5T_ENUM == dt->shared->type);
    assert(value);
    assert(name || 0 == size);

    if (name && size > 0)
        *name = '\0';

    /* Sanity check */
    if (dt->shared->u.enumer.nmembs == 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, NULL, "datatype has no members");

    /* Do a binary search over the values to find the correct one.  Do sorting
     * and search on the copied datatype to protect the original order. */
    if (NULL == (copied_dt = H5T_copy(dt, H5T_COPY_ALL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to copy data type");
    if (H5T__sort_value(copied_dt, NULL) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOMPARE, NULL, "value sort failed");

    lt = 0;
    rt = copied_dt->shared->u.enumer.nmembs;
    while (lt < rt) {
        md  = (lt + rt) / 2;
        cmp = memcmp(value, (uint8_t *)copied_dt->shared->u.enumer.value + (md * copied_dt->shared->size),
                     copied_dt->shared->size);
        if (cmp < 0)
            rt = md;
        else if (cmp > 0)
            lt = md + 1;
        else
            break;
    } /* end while */

    /* Value was not yet defined. This fixes bug # 774, 2002/06/05 EIP */
    if (cmp != 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, NULL, "value is currently not defined");

    /* Save result name */
    if (!name) {
        if (NULL == (name = (char *)H5MM_malloc(strlen(copied_dt->shared->u.enumer.name[md]) + 1)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
        alloc_name = true;
    } /* end if */
    strncpy(name, copied_dt->shared->u.enumer.name[md], size);
    if (strlen(copied_dt->shared->u.enumer.name[md]) >= size)
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOSPACE, NULL, "name has been truncated");

    /* Set return value */
    ret_value = name;

done:
    if (copied_dt)
        if (H5T_close_real(copied_dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, NULL, "unable to close data type");
    if (!ret_value && alloc_name)
        H5MM_free(name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__enum_nameof() */

/*-------------------------------------------------------------------------
 * Function:	H5Tenum_valueof
 *
 * Purpose:	Finds the value that corresponds to the specified NAME f an
 *		enumeration TYPE. The VALUE argument should be at least as
 *		large as the value of H5Tget_size(type) in order to hold the
 *		result.
 *
 * Return:	Success:	Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tenum_valueof(hid_t type, const char *name, void *value /*out*/)
{
    H5T_t *dt;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*sx", type, name, value);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5T_ENUM != dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an enumeration data type");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name");
    if (!value)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no value buffer");

    if (H5T__enum_valueof(dt, name, value) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "valueof query failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Tenum_valueof() */

/*-------------------------------------------------------------------------
 * Function:	H5T__enum_valueof
 *
 * Purpose:	Finds the value that corresponds to the specified symbol
 *		NAME of an enumeration data type DT and copy it to the VALUE
 *		result buffer. The VALUE should be allocated by the caller to
 *		be large enough for the result.
 *
 * Return:	Success:	Non-negative, value stored in VALUE.
 *
 *		Failure:	Negative, VALUE is undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__enum_valueof(const H5T_t *dt, const char *name, void *value /*out*/)
{
    unsigned lt, md = 0, rt;      /*indices for binary search	*/
    int      cmp       = (-1);    /*comparison result		*/
    H5T_t   *copied_dt = NULL;    /*do sorting in copied datatype */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dt && H5T_ENUM == dt->shared->type);
    assert(name && *name);
    assert(value);

    /* Sanity check */
    if (dt->shared->u.enumer.nmembs == 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, FAIL, "datatype has no members");

    /* Do a binary search over the names to find the correct one.  Do sorting
     * and search on the copied datatype to protect the original order. */
    if (NULL == (copied_dt = H5T_copy(dt, H5T_COPY_ALL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy data type");
    if (H5T__sort_name(copied_dt, NULL) < 0)
        HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOMPARE, FAIL, "value sort failed");

    lt = 0;
    rt = copied_dt->shared->u.enumer.nmembs;

    while (lt < rt) {
        md  = (lt + rt) / 2;
        cmp = strcmp(name, copied_dt->shared->u.enumer.name[md]);
        if (cmp < 0) {
            rt = md;
        }
        else if (cmp > 0) {
            lt = md + 1;
        }
        else {
            break;
        }
    }
    /* Value was not yet defined. This fixes bug # 774, 2002/06/05 EIP */
    if (cmp != 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, FAIL, "string doesn't exist in the enumeration type");

    H5MM_memcpy(value, (uint8_t *)copied_dt->shared->u.enumer.value + (md * copied_dt->shared->size),
                copied_dt->shared->size);

done:
    if (copied_dt)
        if (H5T_close_real(copied_dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "unable to close data type");

    FUNC_LEAVE_NOAPI(ret_value)
}
