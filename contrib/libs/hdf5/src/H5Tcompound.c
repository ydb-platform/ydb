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
 * Module Info: This module contains the functionality for compound datatypes
 *      in the H5T interface.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /*generic functions			  */
#include "H5CXprivate.h" /*API Contexts                            */
#include "H5Eprivate.h"  /*error handling			  */
#include "H5Iprivate.h"  /*ID functions		   		  */
#include "H5MMprivate.h" /*memory management			  */
#include "H5Tpkg.h"      /*data-type functions			  */

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
static herr_t H5T__pack(const H5T_t *dt);
static htri_t H5T__is_packed(const H5T_t *dt);
static H5T_t *H5T__reopen_member_type(const H5T_t *dt, unsigned membno);

/*********************/
/* Public Variables */
/*********************/

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
 * Function:	H5Tget_member_offset
 *
 * Purpose:	Returns the byte offset of the beginning of a member with
 *		respect to the beginning of the compound datatype datum.
 *
 * Return:	Success:	Byte offset.
 *
 *		Failure:	Zero. Zero is a valid offset, but this
 *				function will fail only if a call to
 *				H5Tget_member_dims() fails with the same
 *				arguments.
 *
 *-------------------------------------------------------------------------
 */
size_t
H5Tget_member_offset(hid_t type_id, unsigned membno)
{
    H5T_t *dt;        /* Datatype to query */
    size_t ret_value; /* Return value */

    FUNC_ENTER_API(0)
    H5TRACE2("z", "iIu", type_id, membno);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)) || H5T_COMPOUND != dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not a compound datatype");
    if (membno >= dt->shared->u.compnd.nmembs)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid member number");

    /* Value */
    ret_value = H5T_GET_MEMBER_OFFSET(dt->shared, membno);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_member_offset() */

/*-------------------------------------------------------------------------
 * Function:	H5T_get_member_offset
 *
 * Purpose:	Private function for H5Tget_member_offset.  Returns the byte
 *              offset of the beginning of a member with respect to the
 *              beginning of the compound datatype datum.
 *
 * Return:	Success:	Byte offset.
 *
 *		Failure:	Zero. Zero is a valid offset, but this
 *				function will fail only if a call to
 *				H5Tget_member_dims() fails with the same
 *				arguments.
 *
 *-------------------------------------------------------------------------
 */
size_t
H5T_get_member_offset(const H5T_t *dt, unsigned membno)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(dt);
    assert(membno < dt->shared->u.compnd.nmembs);

    FUNC_LEAVE_NOAPI(dt->shared->u.compnd.memb[membno].offset)
} /* end H5T_get_member_offset() */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_member_class
 *
 * Purpose:	Returns the datatype class of a member of a compound datatype.
 *
 * Return:	Success: Non-negative
 *
 *		Failure: H5T_NO_CLASS
 *
 *-------------------------------------------------------------------------
 */
H5T_class_t
H5Tget_member_class(hid_t type_id, unsigned membno)
{
    H5T_t      *dt;        /* Datatype to query */
    H5T_class_t ret_value; /* Return value */

    FUNC_ENTER_API(H5T_NO_CLASS)
    H5TRACE2("Tt", "iIu", type_id, membno);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)) || H5T_COMPOUND != dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_NO_CLASS, "not a compound datatype");
    if (membno >= dt->shared->u.compnd.nmembs)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5T_NO_CLASS, "invalid member number");

    /* Get the type's class.  We have to use this function to get type class
     *  because of the concern of variable-length string.
     */
    ret_value = H5T_GET_CLASS(dt->shared->u.compnd.memb[membno].type->shared, false);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_member_class() */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_member_type
 *
 * Purpose:	Returns the datatype of the specified member.	The caller
 *		should invoke H5Tclose() to release resources associated with
 *		the type.
 *
 * Return:	Success:	An OID of a copy of the member datatype;
 *				modifying the returned datatype does not
 *				modify the member type.
 *
 *		Failure:	H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tget_member_type(hid_t type_id, unsigned membno)
{
    H5T_t *dt;             /* Datatype to query */
    H5T_t *memb_dt = NULL; /* Member datatype */
    hid_t  ret_value;      /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "iIu", type_id, membno);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)) || H5T_COMPOUND != dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a compound datatype");
    if (membno >= dt->shared->u.compnd.nmembs)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid member number");

    /* Retrieve the datatype for the member */
    if (NULL == (memb_dt = H5T__reopen_member_type(dt, membno)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, H5I_INVALID_HID, "unable to retrieve member type");

    /* Get an ID for the datatype */
    if ((ret_value = H5I_register(H5I_DATATYPE, memb_dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable register datatype ID");

done:
    if (ret_value < 0)
        if (memb_dt && H5T_close(memb_dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, H5I_INVALID_HID, "can't close datatype");

    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_member_type() */

/*-------------------------------------------------------------------------
 * Function:	H5T_get_member_type
 *
 * Purpose:     Returns a copy of the data type of the specified member.
 *
 * Return:	Success:	A copy of the member datatype;
 *				modifying the returned datatype does not
 *				modify the member type.
 *
 *		Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_get_member_type(const H5T_t *dt, unsigned membno)
{
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Sanity checks */
    assert(dt);
    assert(membno < dt->shared->u.compnd.nmembs);

    /* Copy datatype */
    if (NULL == (ret_value = H5T_copy(dt->shared->u.compnd.memb[membno].type, H5T_COPY_TRANSIENT)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "unable to copy member datatype");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_member_type() */

/*-------------------------------------------------------------------------
 * Function:	H5T__reopen_member_type
 *
 * Purpose:	Private function for H5Tget_member_type.  Returns a re-opened
 *              copy of the data type of the specified member.
 *
 * Return:	Success:	A copy of the member datatype;
 *				modifying the returned datatype does not
 *				modify the member type.
 *
 *		Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__reopen_member_type(const H5T_t *dt, unsigned membno)
{
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dt);
    assert(membno < dt->shared->u.compnd.nmembs);

    /* Copy datatype, possibly re-opening it */
    if (NULL == (ret_value = H5T_copy_reopen(dt->shared->u.compnd.memb[membno].type)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "unable to reopen member datatype");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__reopen_member_type() */

/*-------------------------------------------------------------------------
 * Function:	H5T__get_member_size
 *
 * Purpose:	Returns the size of the specified member.
 *
 * Return:	Success:	The size in bytes of the member's datatype.
 *		Failure:        0
 *
 *-------------------------------------------------------------------------
 */
size_t
H5T__get_member_size(const H5T_t *dt, unsigned membno)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(dt);
    assert(membno < dt->shared->u.compnd.nmembs);

    FUNC_LEAVE_NOAPI(dt->shared->u.compnd.memb[membno].type->shared->size)
} /* end H5T__get_member_size() */

/*-------------------------------------------------------------------------
 * Function:	H5Tinsert
 *
 * Purpose:	Adds another member to the compound datatype PARENT_ID.  The
 *		new member has a NAME which must be unique within the
 *		compound datatype. The OFFSET argument defines the start of
 *		the member in an instance of the compound datatype, and
 *		MEMBER_ID is the type of the new member.
 *
 * Return:	Success:	Non-negative, the PARENT_ID compound data
 *				type is modified to include a copy of the
 *				member type MEMBER_ID.
 *
 *		Failure:	Negative
 *
 * Errors:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tinsert(hid_t parent_id, const char *name, size_t offset, hid_t member_id)
{
    H5T_t *parent;              /* The compound parent datatype */
    H5T_t *member;              /* The member datatype	*/
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*szi", parent_id, name, offset, member_id);

    /* Check args */
    if (parent_id == member_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't insert compound datatype within itself");
    if (NULL == (parent = (H5T_t *)H5I_object_verify(parent_id, H5I_DATATYPE)) ||
        H5T_COMPOUND != parent->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a compound datatype");
    if (H5T_STATE_TRANSIENT != parent->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "parent type read-only");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no member name");
    if (NULL == (member = (H5T_t *)H5I_object_verify(member_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Insert */
    if (H5T__insert(parent, name, offset, member) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, FAIL, "unable to insert member");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tinsert() */

/*-------------------------------------------------------------------------
 * Function:	H5Tpack
 *
 * Purpose:	Recursively removes padding from within a compound datatype
 *		to make it more efficient (space-wise) to store that data.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tpack(hid_t type_id)
{
    H5T_t *dt;                  /* Datatype to modify */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)) ||
        H5T_detect_class(dt, H5T_COMPOUND, true) <= 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a compound datatype");

    /* Pack */
    if (H5T__pack(dt) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to pack compound datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tpack() */

/*-------------------------------------------------------------------------
 * Function:	H5T__insert
 *
 * Purpose:	Adds a new MEMBER to the compound datatype PARENT.  The new
 *		member will have a NAME that is unique within PARENT and an
 *		instance of PARENT will have the member begin at byte offset
 *		OFFSET from the beginning.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__insert(H5T_t *parent, const char *name, size_t offset, const H5T_t *member)
{
    unsigned idx; /* Index of member to insert */
    size_t   total_size;
    unsigned i;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(parent && H5T_COMPOUND == parent->shared->type);
    assert(H5T_STATE_TRANSIENT == parent->shared->state);
    assert(member);
    assert(name && *name);

    /* Does NAME already exist in PARENT? */
    for (i = 0; i < parent->shared->u.compnd.nmembs; i++)
        if (!strcmp(parent->shared->u.compnd.memb[i].name, name))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, FAIL, "member name is not unique");

    /* Does the new member overlap any existing member ? */
    total_size = member->shared->size;
    for (i = 0; i < parent->shared->u.compnd.nmembs; i++)
        if ((offset <= parent->shared->u.compnd.memb[i].offset &&
             (offset + total_size) > parent->shared->u.compnd.memb[i].offset) ||
            (parent->shared->u.compnd.memb[i].offset <= offset &&
             (parent->shared->u.compnd.memb[i].offset + parent->shared->u.compnd.memb[i].size) > offset))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, FAIL, "member overlaps with another member");

    /* Does the new member overlap the end of the compound type? */
    if ((offset + total_size) > parent->shared->size)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, FAIL, "member extends past end of compound type");

    /* Increase member array if necessary */
    if (parent->shared->u.compnd.nmembs >= parent->shared->u.compnd.nalloc) {
        unsigned     na = MAX(1, parent->shared->u.compnd.nalloc * 2);
        H5T_cmemb_t *x = (H5T_cmemb_t *)H5MM_realloc(parent->shared->u.compnd.memb, na * sizeof(H5T_cmemb_t));

        if (!x)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "memory allocation failed");
        parent->shared->u.compnd.nalloc = na;
        parent->shared->u.compnd.memb   = x;
    } /* end if */

    /* Add member to end of member array */
    idx                                       = parent->shared->u.compnd.nmembs;
    parent->shared->u.compnd.memb[idx].offset = offset;
    parent->shared->u.compnd.memb[idx].size   = total_size;
    if (NULL == (parent->shared->u.compnd.memb[idx].name = H5MM_xstrdup(name)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "couldn't duplicate name string");
    if (NULL == (parent->shared->u.compnd.memb[idx].type = H5T_copy(member, H5T_COPY_ALL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "couldn't copy datatype");

    parent->shared->u.compnd.sorted = H5T_SORT_NONE;
    parent->shared->u.compnd.nmembs++;
    parent->shared->u.compnd.memb_size += total_size;

    /* It should not be possible to get this far if the type is already packed
     * - the new member would overlap something */
    assert(!(parent->shared->u.compnd.packed));

    /* Determine if the compound datatype becomes packed */
    H5T__update_packed(parent);

    /* Set the "force conversion" flag if the field's datatype indicates */
    if (member->shared->force_conv == true)
        parent->shared->force_conv = true;

    /* Check for member having a later version than the parent */
    if (parent->shared->version < member->shared->version)
        /* Upgrade parent datatype (and all other members also) */
        /* (can't use a partial datatype and later versions of the format are
         *  more efficient, so might as well upgrade all members also... -QAK)
         */
        if (H5T__upgrade_version(parent, member->shared->version) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't upgrade member encoding version");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__insert() */

/*-------------------------------------------------------------------------
 * Function:	H5T__pack
 *
 * Purpose:	Recursively packs a compound datatype by removing padding
 *		bytes. This is done in place (that is, destructively).
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__pack(const H5T_t *dt)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dt);

    if (H5T_detect_class(dt, H5T_COMPOUND, false) > 0) {
        /* If datatype has been packed, skip packing it and indicate success */
        if (true == H5T__is_packed(dt))
            HGOTO_DONE(SUCCEED);

        /* Check for packing unmodifiable datatype */
        if (H5T_STATE_TRANSIENT != dt->shared->state)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "datatype is read-only");

        if (dt->shared->parent) {
            if (H5T__pack(dt->shared->parent) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to pack parent of datatype");

            /* Adjust size of datatype appropriately */
            if (dt->shared->type == H5T_ARRAY)
                dt->shared->size = dt->shared->parent->shared->size * dt->shared->u.array.nelem;
            else if (dt->shared->type != H5T_VLEN)
                dt->shared->size = dt->shared->parent->shared->size;
        } /* end if */
        else if (dt->shared->type == H5T_COMPOUND) {
            size_t   offset; /* Offset of member */
            unsigned i;      /* Local index variable */

            /* Recursively pack the members */
            for (i = 0; i < dt->shared->u.compnd.nmembs; i++) {
                if (H5T__pack(dt->shared->u.compnd.memb[i].type) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL,
                                "unable to pack part of a compound datatype");

                /* Update the member size */
                dt->shared->u.compnd.memb[i].size = (dt->shared->u.compnd.memb[i].type)->shared->size;
            } /* end for */

            /* Remove padding between members */
            if (H5T__sort_value(dt, NULL) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOMPARE, FAIL, "value sort failed");
            for (i = 0, offset = 0; i < dt->shared->u.compnd.nmembs; i++) {
                dt->shared->u.compnd.memb[i].offset = offset;
                offset += dt->shared->u.compnd.memb[i].size;
            }

            /* Change total size */
            dt->shared->size = MAX(1, offset);

            /* Mark the type as packed now */
            dt->shared->u.compnd.packed = true;
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__pack() */

/*-------------------------------------------------------------------------
 * Function:	H5T__is_packed
 *
 * Purpose:	Checks whether a datatype which is compound (or has compound
 *              components) is packed.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5T__is_packed(const H5T_t *dt)
{
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(dt);

    /* Go up the chain as far as possible */
    while (dt->shared->parent)
        dt = dt->shared->parent;

    /* If this is a compound datatype, check if it is packed */
    if (dt->shared->type == H5T_COMPOUND)
        ret_value = (htri_t)(dt->shared->u.compnd.packed);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__is_packed() */

/*-------------------------------------------------------------------------
 * Function:	H5T__update_packed
 *
 * Purpose:	Checks whether a datatype which is compound became packed
 *              after recent changes.  This function does not assume that
 *              the status of the "packed" field is correct, and sets
 *              this field to the correct value.
 *
 * Return:	void
 *
 *-------------------------------------------------------------------------
 */
void
H5T__update_packed(const H5T_t *dt)
{
    unsigned i; /* Index */

    FUNC_ENTER_PACKAGE_NOERR

    assert(dt);
    assert(dt->shared->type == H5T_COMPOUND);

    /* First check if all space is used in the "top level" type */
    if (dt->shared->size == dt->shared->u.compnd.memb_size) {
        /* Set the packed flag to true */
        dt->shared->u.compnd.packed = true;

        /* Now check if all members are packed */
        for (i = 0; i < dt->shared->u.compnd.nmembs; i++)
            if (!H5T__is_packed(dt->shared->u.compnd.memb[i].type)) {
                dt->shared->u.compnd.packed = false;
                break;
            } /* end if */
    }         /* end if */
    else
        dt->shared->u.compnd.packed = false;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5T__update_packed() */
