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
 * Created:	H5Pdeprec.c
 *
 * Purpose:	Deprecated functions from the H5P interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* Files		  	        */
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5Ppkg.h"      /* Property lists		  	*/

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

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

#ifndef H5_NO_DEPRECATED_SYMBOLS

/*--------------------------------------------------------------------------
 NAME
    H5Pregister1
 PURPOSE
    Routine to register a new property in a property list class.
 USAGE
    herr_t H5Pregister1(class, name, size, default, prp_create, prp_set, prp_get, prp_close)
        hid_t class;            IN: Property list class to close
        const char *name;       IN: Name of property to register
        size_t size;            IN: Size of property in bytes
        void *def_value;        IN: Pointer to buffer containing default value
                                    for property in newly created property lists
        H5P_prp_create_func_t prp_create;   IN: Function pointer to property
                                    creation callback
        H5P_prp_set_func_t prp_set; IN: Function pointer to property set callback
        H5P_prp_get_func_t prp_get; IN: Function pointer to property get callback
        H5P_prp_delete_func_t prp_delete; IN: Function pointer to property delete callback
        H5P_prp_copy_func_t prp_copy; IN: Function pointer to property copy callback
        H5P_prp_close_func_t prp_close; IN: Function pointer to property close
                                    callback
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Registers a new property with a property list class.  The property will
    exist in all property list objects of that class after this routine is
    finished.  The name of the property must not already exist.  The default
    property value must be provided and all new property lists created with this
    property will have the property value set to the default provided.  Any of
    the callback routines may be set to NULL if they are not needed.

        Zero-sized properties are allowed and do not store any data in the
    property list.  These may be used as flags to indicate the presence or
    absence of a particular piece of information.  The 'default' pointer for a
    zero-sized property may be set to NULL.  The property 'create' & 'close'
    callbacks are called for zero-sized properties, but the 'set' and 'get'
    callbacks are never called.

        The 'create' callback is called when a new property list with this
    property is being created.  H5P_prp_create_func_t is defined as:
        typedef herr_t (*H5P_prp_create_func_t)(hid_t prop_id, const char *name,
                size_t size, void *initial_value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being created.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *initial_value; IN/OUT: The initial value for the property being created.
                                (The 'default' value passed to H5Pregister1)
    The 'create' routine may modify the value to be set and those changes will
    be stored as the initial value of the property.  If the 'create' routine
    returns a negative value, the new property value is not copied into the
    property and the property list creation routine returns an error value.

        The 'set' callback is called before a new value is copied into the
    property.  H5P_prp_set_func_t is defined as:
        typedef herr_t (*H5P_prp_set_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being modified.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *new_value;    IN/OUT: The value being set for the property.
    The 'set' routine may modify the value to be set and those changes will be
    stored as the value of the property.  If the 'set' routine returns a
    negative value, the new property value is not copied into the property and
    the property list set routine returns an error value.

        The 'get' callback is called before a value is retrieved from the
    property.  H5P_prp_get_func_t is defined as:
        typedef herr_t (*H5P_prp_get_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being queried.
        const char *name;   IN: The name of the property being queried.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value being retrieved for the property.
    The 'get' routine may modify the value to be retrieved and those changes
    will be returned to the calling function.  If the 'get' routine returns a
    negative value, the property value is returned and the property list get
    routine returns an error value.

        The 'delete' callback is called when a property is deleted from a
    property list.  H5P_prp_del_func_t is defined as:
        typedef herr_t (*H5P_prp_del_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list the property is deleted from.
        const char *name;   IN: The name of the property being deleted.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value of the property being deleted.
    The 'delete' routine may modify the value passed in, but the value is not
    used by the library when the 'delete' routine returns.  If the
    'delete' routine returns a negative value, the property list deletion
    routine returns an error value but the property is still deleted.

        The 'copy' callback is called when a property list with this
    property is copied.  H5P_prp_copy_func_t is defined as:
        typedef herr_t (*H5P_prp_copy_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being copied.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being copied.
    The 'copy' routine may modify the value to be copied and those changes will be
    stored as the value of the property.  If the 'copy' routine returns a
    negative value, the new property value is not copied into the property and
    the property list copy routine returns an error value.

        The 'close' callback is called when a property list with this
    property is being destroyed.  H5P_prp_close_func_t is defined as:
        typedef herr_t (*H5P_prp_close_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being closed.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being closed.
    The 'close' routine may modify the value passed in, but the value is not
    used by the library when the 'close' routine returns.  If the
    'close' routine returns a negative value, the property list close
    routine returns an error value but the property list is still closed.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The 'set' callback function may be useful to range check the value being
    set for the property or may perform some transformation/translation of the
    value set.  The 'get' callback would then [probably] reverse the
    transformation, etc.  A single 'get' or 'set' callback could handle
    multiple properties by performing different actions based on the property
    name or other properties in the property list.

        I would like to say "the property list is not closed" when a 'close'
    routine fails, but I don't think that's possible due to other properties in
    the list being successfully closed & removed from the property list.  I
    suppose that it would be possible to just remove the properties which have
    successful 'close' callbacks, but I'm not happy with the ramifications
    of a mangled, un-closable property list hanging around...  Any comments? -QAK

 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Pregister1(hid_t cls_id, const char *name, size_t size, void *def_value, H5P_prp_create_func_t prp_create,
             H5P_prp_set_func_t prp_set, H5P_prp_get_func_t prp_get, H5P_prp_delete_func_t prp_delete,
             H5P_prp_copy_func_t prp_copy, H5P_prp_close_func_t prp_close)
{
    H5P_genclass_t *pclass;      /* Property list class to modify */
    H5P_genclass_t *orig_pclass; /* Original property class */
    herr_t          ret_value;   /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "i*sz*xPCPSPGPDPOPL", cls_id, name, size, def_value, prp_create, prp_set, prp_get,
              prp_delete, prp_copy, prp_close);

    /* Check arguments. */
    if (NULL == (pclass = (H5P_genclass_t *)H5I_object_verify(cls_id, H5I_GENPROP_CLS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list class");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid class name");
    if (size > 0 && def_value == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "properties >0 size must have default");

    /* Create the new property list class */
    orig_pclass = pclass;
    if ((ret_value = H5P__register(&pclass, name, size, def_value, prp_create, prp_set, prp_get, NULL, NULL,
                                   prp_delete, prp_copy, NULL, prp_close)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "unable to register property in class");

    /* Check if the property class changed and needs to be substituted in the ID */
    if (pclass != orig_pclass) {
        H5P_genclass_t *old_pclass; /* Old property class */

        /* Substitute the new property class in the ID */
        if (NULL == (old_pclass = (H5P_genclass_t *)H5I_subst(cls_id, pclass)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to substitute property class in ID");
        assert(old_pclass == orig_pclass);

        /* Close the previous class */
        if (H5P__close_class(orig_pclass) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, FAIL,
                        "unable to close original property class after substitution");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pregister1() */

/*--------------------------------------------------------------------------
 NAME
    H5Pinsert1
 PURPOSE
    Routine to insert a new property in a property list.
 USAGE
    herr_t H5Pinsert1(plist, name, size, value, prp_set, prp_get, prp_close)
        hid_t plist;            IN: Property list to add property to
        const char *name;       IN: Name of property to add
        size_t size;            IN: Size of property in bytes
        void *value;            IN: Pointer to the value for the property
        H5P_prp_set_func_t prp_set; IN: Function pointer to property set callback
        H5P_prp_get_func_t prp_get; IN: Function pointer to property get callback
        H5P_prp_delete_func_t prp_delete; IN: Function pointer to property delete callback
        H5P_prp_copy_func_t prp_copy; IN: Function pointer to property copy callback
        H5P_prp_compare_func_t prp_cmp; IN: Function pointer to property compare callback
        H5P_prp_close_func_t prp_close; IN: Function pointer to property close
                                    callback
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Inserts a temporary property into a property list.  The property will
    exist only in this property list object.  The name of the property must not
    already exist.  The value must be provided unless the property is zero-
    sized.  Any of the callback routines may be set to NULL if they are not
    needed.

        Zero-sized properties are allowed and do not store any data in the
    property list.  These may be used as flags to indicate the presence or
    absence of a particular piece of information.  The 'value' pointer for a
    zero-sized property may be set to NULL.  The property 'close' callback is
    called for zero-sized properties, but the 'set' and 'get' callbacks are
    never called.

        The 'set' callback is called before a new value is copied into the
    property.  H5P_prp_set_func_t is defined as:
        typedef herr_t (*H5P_prp_set_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being modified.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *new_value;    IN/OUT: The value being set for the property.
    The 'set' routine may modify the value to be set and those changes will be
    stored as the value of the property.  If the 'set' routine returns a
    negative value, the new property value is not copied into the property and
    the property list set routine returns an error value.

        The 'get' callback is called before a value is retrieved from the
    property.  H5P_prp_get_func_t is defined as:
        typedef herr_t (*H5P_prp_get_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being queried.
        const char *name;   IN: The name of the property being queried.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value being retrieved for the property.
    The 'get' routine may modify the value to be retrieved and those changes
    will be returned to the calling function.  If the 'get' routine returns a
    negative value, the property value is returned and the property list get
    routine returns an error value.

        The 'delete' callback is called when a property is deleted from a
    property list.  H5P_prp_del_func_t is defined as:
        typedef herr_t (*H5P_prp_del_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list the property is deleted from.
        const char *name;   IN: The name of the property being deleted.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value of the property being deleted.
    The 'delete' routine may modify the value passed in, but the value is not
    used by the library when the 'delete' routine returns.  If the
    'delete' routine returns a negative value, the property list deletion
    routine returns an error value but the property is still deleted.

        The 'copy' callback is called when a property list with this
    property is copied.  H5P_prp_copy_func_t is defined as:
        typedef herr_t (*H5P_prp_copy_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being copied.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being copied.
    The 'copy' routine may modify the value to be copied and those changes will be
    stored as the value of the property.  If the 'copy' routine returns a
    negative value, the new property value is not copied into the property and
    the property list copy routine returns an error value.

        The 'compare' callback is called when a property list with this
    property is compared to another property list.  H5P_prp_compare_func_t is
    defined as:
        typedef int (*H5P_prp_compare_func_t)( void *value1, void *value2,
            size_t size);
    where the parameters to the callback function are:
        const void *value1; IN: The value of the first property being compared.
        const void *value2; IN: The value of the second property being compared.
        size_t size;        IN: The size of the property value
    The 'compare' routine may not modify the values to be compared.  The
    'compare' routine should return a positive value if VALUE1 is greater than
    VALUE2, a negative value if VALUE2 is greater than VALUE1 and zero if VALUE1
    and VALUE2 are equal.

        The 'close' callback is called when a property list with this
    property is being destroyed.  H5P_prp_close_func_t is defined as:
        typedef herr_t (*H5P_prp_close_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being closed.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being closed.
    The 'close' routine may modify the value passed in, but the value is not
    used by the library when the 'close' routine returns.  If the
    'close' routine returns a negative value, the property list close
    routine returns an error value but the property list is still closed.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The 'set' callback function may be useful to range check the value being
    set for the property or may perform some transformation/translation of the
    value set.  The 'get' callback would then [probably] reverse the
    transformation, etc.  A single 'get' or 'set' callback could handle
    multiple properties by performing different actions based on the property
    name or other properties in the property list.

        There is no 'create' callback routine for temporary property list
    objects, the initial value is assumed to have any necessary setup already
    performed on it.

        I would like to say "the property list is not closed" when a 'close'
    routine fails, but I don't think that's possible due to other properties in
    the list being successfully closed & removed from the property list.  I
    suppose that it would be possible to just remove the properties which have
    successful 'close' callbacks, but I'm not happy with the ramifications
    of a mangled, un-closable property list hanging around...  Any comments? -QAK

 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Pinsert1(hid_t plist_id, const char *name, size_t size, void *value, H5P_prp_set_func_t prp_set,
           H5P_prp_get_func_t prp_get, H5P_prp_delete_func_t prp_delete, H5P_prp_copy_func_t prp_copy,
           H5P_prp_close_func_t prp_close)
{
    H5P_genplist_t *plist;     /* Property list to modify */
    herr_t          ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE9("e", "i*sz*xPSPGPDPOPL", plist_id, name, size, value, prp_set, prp_get, prp_delete, prp_copy,
             prp_close);

    /* Check arguments. */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid property name");
    if (size > 0 && value == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "properties >0 size must have default");

    /* Create the new property list class */
    if ((ret_value = H5P_insert(plist, name, size, value, prp_set, prp_get, NULL, NULL, prp_delete, prp_copy,
                                NULL, prp_close)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "unable to register property in plist");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pinsert1() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_version
 *
 * Purpose:	Retrieves version information for various parts of a file.
 *
 *		SUPER:		The file super block.
 *		FREELIST:	The global free list.
 *		STAB:		The root symbol table entry.
 *		SHHDR:		Shared object headers.
 *
 *		Any (or even all) of the output arguments can be null
 *		pointers.
 *
 * Return:	Success:	Non-negative, version information is returned
 *				through the arguments.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_version(hid_t plist_id, unsigned *super /*out*/, unsigned *freelist /*out*/, unsigned *stab /*out*/,
               unsigned *shhdr /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ixxxx", plist_id, super, freelist, stab, shhdr);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (super)
        if (H5P_get(plist, H5F_CRT_SUPER_VERS_NAME, super) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get superblock version");
    if (freelist)
        *freelist = HDF5_FREESPACE_VERSION; /* (hard-wired) */
    if (stab)
        *stab = HDF5_OBJECTDIR_VERSION; /* (hard-wired) */
    if (shhdr)
        *shhdr = HDF5_SHAREDHEADER_VERSION; /* (hard-wired) */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_version() */

/*--------------------------------------------------------------------------
 NAME
    H5Pencode1
 PURPOSE
    Routine to convert the property values in a property list into a binary buffer
 USAGE
    herr_t H5Pencode1(plist_id, buf, nalloc)
        hid_t plist_id;         IN: Identifier to property list to encode
        void *buf:              OUT: buffer to gold the encoded plist
        size_t *nalloc;         IN/OUT: size of buffer needed to encode plist
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Encodes a property list into a binary buffer. If the buffer is NULL, then
    the call will set the size needed to encode the plist in nalloc. Otherwise
    the routine will encode the plist in buf.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Pencode1(hid_t plist_id, void *buf, size_t *nalloc)
{
    H5P_genplist_t *plist; /* Property list to query */
    hid_t           temp_fapl_id = H5P_DEFAULT;
    herr_t          ret_value    = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*x*z", plist_id, buf, nalloc);

    /* Check arguments. */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&temp_fapl_id, H5P_CLS_FACC, H5I_INVALID_HID, true) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* Call the internal encode routine */
    if ((ret_value = H5P__encode(plist, true, buf, nalloc)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, FAIL, "unable to encode property list");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pencode1() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_file_space
 *
 * Purpose:	    It is mapped to H5Pset_file_space_strategy().
 *
 * Return:	    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_file_space(hid_t plist_id, H5F_file_space_type_t strategy, hsize_t threshold)
{

    H5F_fspace_strategy_t new_strategy;                                 /* File space strategy type */
    bool                  new_persist   = H5F_FREE_SPACE_PERSIST_DEF;   /* Persisting free-space or not */
    hsize_t               new_threshold = H5F_FREE_SPACE_THRESHOLD_DEF; /* Free-space section threshold */
    H5F_file_space_type_t in_strategy   = strategy;                     /* Input strategy */
    hsize_t               in_threshold  = threshold;                    /* Input threshold */
    herr_t                ret_value     = SUCCEED;                      /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iFth", plist_id, strategy, threshold);

    if ((unsigned)in_strategy >= H5F_FILE_SPACE_NTYPES)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid strategy");
    /*
     *  For 1.10.0 H5Pset_file_space:
     *      If strategy is zero, the property is not changed;
     *      the existing strategy is retained.
     *      If threshold is zero, the property is not changed;
     *      the existing threshold is retained.
     */
    if (!in_strategy)
        H5Pget_file_space(plist_id, &in_strategy, NULL);
    if (!in_threshold)
        H5Pget_file_space(plist_id, NULL, &in_threshold);

    switch (in_strategy) {
        case H5F_FILE_SPACE_ALL_PERSIST:
            new_strategy  = H5F_FSPACE_STRATEGY_FSM_AGGR;
            new_persist   = true;
            new_threshold = in_threshold;
            break;

        case H5F_FILE_SPACE_ALL:
            new_strategy  = H5F_FSPACE_STRATEGY_FSM_AGGR;
            new_threshold = in_threshold;
            break;

        case H5F_FILE_SPACE_AGGR_VFD:
            new_strategy = H5F_FSPACE_STRATEGY_AGGR;
            break;

        case H5F_FILE_SPACE_VFD:
            new_strategy = H5F_FSPACE_STRATEGY_NONE;
            break;

        case H5F_FILE_SPACE_NTYPES:
        case H5F_FILE_SPACE_DEFAULT:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid file space strategy");
    }

    if (H5Pset_file_space_strategy(plist_id, new_strategy, new_persist, new_threshold) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set file space strategy");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_file_space() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_file_space
 *
 * Purpose:	    It is mapped to H5Pget_file_space_strategy().
 *
 * Return:	    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_file_space(hid_t plist_id, H5F_file_space_type_t *strategy /*out*/, hsize_t *threshold /*out*/)
{
    H5F_fspace_strategy_t new_strategy;        /* File space strategy type */
    bool                  new_persist;         /* Persisting free-space or not */
    hsize_t               new_threshold;       /* Free-space section threshold */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", plist_id, strategy, threshold);

    /* Get current file space info */
    if (H5Pget_file_space_strategy(plist_id, &new_strategy, &new_persist, &new_threshold) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get file space strategy");

    /* Get value(s) */
    if (strategy) {
        switch (new_strategy) {

            case H5F_FSPACE_STRATEGY_FSM_AGGR:
                if (new_persist)
                    *strategy = H5F_FILE_SPACE_ALL_PERSIST;
                else
                    *strategy = H5F_FILE_SPACE_ALL;
                break;

            case H5F_FSPACE_STRATEGY_AGGR:
                *strategy = H5F_FILE_SPACE_AGGR_VFD;
                break;

            case H5F_FSPACE_STRATEGY_NONE:
                *strategy = H5F_FILE_SPACE_VFD;
                break;

            case H5F_FSPACE_STRATEGY_PAGE:
            case H5F_FSPACE_STRATEGY_NTYPES:
            default:
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid file space strategy");
        }
    }

    if (threshold)
        *threshold = new_threshold;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_file_space() */
#endif /* H5_NO_DEPRECATED_SYMBOLS */
