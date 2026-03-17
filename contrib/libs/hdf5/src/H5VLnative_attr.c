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
 * Purpose:     Attribute callbacks for the native VOL connector
 *
 */

/****************/
/* Module Setup */
/****************/

#define H5A_FRIEND /* Suppress error about including H5Apkg    */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Apkg.h"      /* Attributes                               */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5Sprivate.h"  /* Dataspaces                               */
#include "H5Tprivate.h"  /* Datatypes                                */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

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

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_create
 *
 * Purpose:     Handles the attribute create callback
 *
 * Return:      Success:    attribute pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t type_id,
                         hid_t space_id, hid_t acpl_id, hid_t H5_ATTR_UNUSED aapl_id,
                         hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5P_genplist_t *plist;
    H5G_loc_t       loc;     /* Object location */
    H5G_loc_t       obj_loc; /* Location used to open group */
    bool            loc_found = false;
    H5T_t          *type, *dt; /* Datatype to use for attribute */
    H5S_t          *space;     /* Dataspace to use for attribute */
    H5A_t          *attr      = NULL;
    void           *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file or file object");
    if (0 == (H5F_INTENT(loc.oloc->file) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_ARGS, H5E_WRITEERROR, NULL, "no write intent on file");

    if (NULL == (plist = H5P_object_verify(aapl_id, H5P_ATTRIBUTE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "AAPL is not an attribute access property list");

    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a datatype");
    /* If this is a named datatype, get the connector's pointer to the datatype */
    type = H5T_get_actual_type(dt);

    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data space");

    if (loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* H5Acreate */
        /* Go do the real work for attaching the attribute to the dataset */
        if (NULL == (attr = H5A__create(&loc, attr_name, type, space, acpl_id)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "unable to create attribute");
    } /* end if */
    else if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        /* H5Acreate_by_name */
        if (NULL == (attr = H5A__create_by_name(&loc, loc_params->loc_data.loc_by_name.name, attr_name, type,
                                                space, acpl_id)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, NULL, "unable to create attribute");
    } /* end else-if */
    else
        HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, NULL, "unknown attribute create parameters");

    ret_value = (void *)attr;

done:
    /* Release resources */
    if (loc_found && H5G_loc_free(&obj_loc) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CANTRELEASE, NULL, "can't free location");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_open
 *
 * Purpose:     Handles the attribute open callback
 *
 * Return:      Success:    attribute pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t aapl_id,
                       hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5P_genplist_t *plist;
    H5G_loc_t       loc;         /* Object location */
    H5A_t          *attr = NULL; /* Attribute opened */
    void           *ret_value;

    FUNC_ENTER_PACKAGE

    /* check arguments */
    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file or file object");

    if (NULL == (plist = H5P_object_verify(aapl_id, H5P_ATTRIBUTE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "AAPL is not an attribute access property list");

    if (loc_params->type == H5VL_OBJECT_BY_SELF) {
        /* H5Aopen */
        /* Open the attribute */
        if (NULL == (attr = H5A__open(&loc, attr_name)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "unable to open attribute: '%s'", attr_name);
    } /* end if */
    else if (loc_params->type == H5VL_OBJECT_BY_NAME) {
        /* H5Aopen_by_name */
        /* Open the attribute on the object header */
        if (NULL == (attr = H5A__open_by_name(&loc, loc_params->loc_data.loc_by_name.name, attr_name)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open attribute");
    } /* end else-if */
    else if (loc_params->type == H5VL_OBJECT_BY_IDX) {
        /* H5Aopen_by_idx */
        /* Open the attribute in the object header */
        if (NULL == (attr = H5A__open_by_idx(
                         &loc, loc_params->loc_data.loc_by_idx.name, loc_params->loc_data.loc_by_idx.idx_type,
                         loc_params->loc_data.loc_by_idx.order, loc_params->loc_data.loc_by_idx.n)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "unable to open attribute");
    } /* end else-if */
    else
        HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, NULL, "unknown attribute open parameters");

    ret_value = (void *)attr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_read
 *
 * Purpose:     Handles the attribute read callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_attr_read(void *attr, hid_t dtype_id, void *buf, hid_t H5_ATTR_UNUSED dxpl_id,
                       void H5_ATTR_UNUSED **req)
{
    H5T_t *mem_type;  /* Memory datatype */
    herr_t ret_value; /* Return value */

    FUNC_ENTER_PACKAGE

    if (NULL == (mem_type = (H5T_t *)H5I_object_verify(dtype_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Go write the actual data to the attribute */
    if ((ret_value = H5A__read((H5A_t *)attr, mem_type, buf)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "unable to read attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_write
 *
 * Purpose:     Handles the attribute write callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_attr_write(void *attr, hid_t dtype_id, const void *buf, hid_t H5_ATTR_UNUSED dxpl_id,
                        void H5_ATTR_UNUSED **req)
{
    H5T_t *mem_type;  /* Memory datatype */
    herr_t ret_value; /* Return value */

    FUNC_ENTER_PACKAGE

    if (NULL == (mem_type = (H5T_t *)H5I_object_verify(dtype_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Go write the actual data to the attribute */
    if ((ret_value = H5A__write((H5A_t *)attr, mem_type, buf)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "unable to write attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_get
 *
 * Purpose:     Handles the attribute get callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                      void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* H5Aget_space */
        case H5VL_ATTR_GET_SPACE: {
            H5A_t *attr = (H5A_t *)obj;

            if ((args->args.get_space.space_id = H5A_get_space(attr)) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CANTGET, FAIL, "can't get space ID of attribute");
            break;
        }

        /* H5Aget_type */
        case H5VL_ATTR_GET_TYPE: {
            H5A_t *attr = (H5A_t *)obj;

            if ((args->args.get_type.type_id = H5A__get_type(attr)) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CANTGET, FAIL, "can't get datatype ID of attribute");
            break;
        }

        /* H5Aget_create_plist */
        case H5VL_ATTR_GET_ACPL: {
            H5A_t *attr = (H5A_t *)obj;

            if ((args->args.get_acpl.acpl_id = H5A__get_create_plist(attr)) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CANTGET, FAIL, "can't get creation property list for attr");

            break;
        }

        /* H5Aget_name */
        case H5VL_ATTR_GET_NAME: {
            H5VL_attr_get_name_args_t *get_name_args = &args->args.get_name;

            if (H5VL_OBJECT_BY_SELF == get_name_args->loc_params.type) {
                if (H5A__get_name((H5A_t *)obj, get_name_args->buf_size, get_name_args->buf,
                                  get_name_args->attr_name_len) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get attribute name");
            }
            else if (H5VL_OBJECT_BY_IDX == get_name_args->loc_params.type) {
                H5G_loc_t loc;
                H5A_t    *attr;

                /* check arguments */
                if (H5G_loc_real(obj, get_name_args->loc_params.obj_type, &loc) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

                /* Open the attribute on the object header */
                if (NULL == (attr = H5A__open_by_idx(&loc, get_name_args->loc_params.loc_data.loc_by_idx.name,
                                                     get_name_args->loc_params.loc_data.loc_by_idx.idx_type,
                                                     get_name_args->loc_params.loc_data.loc_by_idx.order,
                                                     get_name_args->loc_params.loc_data.loc_by_idx.n)))
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open attribute");

                /* Get the length of the name */
                *get_name_args->attr_name_len = strlen(attr->shared->name);

                /* Copy the name into the user's buffer, if given */
                if (get_name_args->buf) {
                    strncpy(get_name_args->buf, attr->shared->name,
                            MIN((*get_name_args->attr_name_len + 1), get_name_args->buf_size));
                    if (*get_name_args->attr_name_len >= get_name_args->buf_size)
                        get_name_args->buf[get_name_args->buf_size - 1] = '\0';
                } /* end if */

                /* Release resources */
                if (attr && H5A__close(attr) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "can't close attribute");
            }
            else
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get name of attr");

            break;
        }

        /* H5Aget_info */
        case H5VL_ATTR_GET_INFO: {
            H5VL_attr_get_info_args_t *get_info_args = &args->args.get_info;
            H5A_t                     *attr          = NULL;

            if (H5VL_OBJECT_BY_SELF == get_info_args->loc_params.type) {
                attr = (H5A_t *)obj;
                if (H5A__get_info(attr, get_info_args->ainfo) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_CANTGET, FAIL, "can't get attribute info");
            }
            else if (H5VL_OBJECT_BY_NAME == get_info_args->loc_params.type) {
                H5G_loc_t loc;

                /* check arguments */
                if (H5G_loc_real(obj, get_info_args->loc_params.obj_type, &loc) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

                /* Open the attribute on the object header */
                if (NULL ==
                    (attr = H5A__open_by_name(&loc, get_info_args->loc_params.loc_data.loc_by_name.name,
                                              get_info_args->attr_name)))
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open attribute");

                /* Get the attribute information */
                if (H5A__get_info(attr, get_info_args->ainfo) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to get attribute info");

                /* Release resources */
                if (attr && H5A__close(attr) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "can't close attribute");
            }
            else if (H5VL_OBJECT_BY_IDX == get_info_args->loc_params.type) {
                H5G_loc_t loc;

                /* check arguments */
                if (H5G_loc_real(obj, get_info_args->loc_params.obj_type, &loc) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

                /* Open the attribute on the object header */
                if (NULL == (attr = H5A__open_by_idx(&loc, get_info_args->loc_params.loc_data.loc_by_idx.name,
                                                     get_info_args->loc_params.loc_data.loc_by_idx.idx_type,
                                                     get_info_args->loc_params.loc_data.loc_by_idx.order,
                                                     get_info_args->loc_params.loc_data.loc_by_idx.n)))
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "can't open attribute");

                /* Get the attribute information */
                if (H5A__get_info(attr, get_info_args->ainfo) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to get attribute info");

                /* Release resources */
                if (attr && H5A__close(attr) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "can't close attribute");
            }
            else
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get name of attr");

            break;
        }

        case H5VL_ATTR_GET_STORAGE_SIZE: {
            H5A_t *attr = (H5A_t *)obj;

            /* Set storage size */
            *args->args.get_storage_size.data_size = attr->shared->data_size;
            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get this type of information from attr");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_specific
 *
 * Purpose:     Handles the attribute specific callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t *args,
                           hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5G_loc_t loc;
    herr_t    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get location for passed-in object */
    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    switch (args->op_type) {
        /* H5Adelete/delete_by_name */
        case H5VL_ATTR_DELETE: {
            if (H5VL_OBJECT_BY_SELF == loc_params->type) {
                /* Delete the attribute from the location */
                if (H5O__attr_remove(loc.oloc, args->args.del.name) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");
            } /* end if */
            else if (H5VL_OBJECT_BY_NAME == loc_params->type) {
                /* Delete the attribute */
                if (H5A__delete_by_name(&loc, loc_params->loc_data.loc_by_name.name, args->args.del.name) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");
            } /* end else-if */
            else
                HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unknown attribute delete location");
            break;
        }

        /* H5Adelete_by_idx */
        case H5VL_ATTR_DELETE_BY_IDX: {
            H5VL_attr_delete_by_idx_args_t *del_by_idx_args =
                &args->args.delete_by_idx; /* Arguments to delete_by_idx operation */

            if (H5VL_OBJECT_BY_NAME == loc_params->type) {
                /* Delete the attribute from the location */
                if (H5A__delete_by_idx(&loc, loc_params->loc_data.loc_by_name.name, del_by_idx_args->idx_type,
                                       del_by_idx_args->order, del_by_idx_args->n) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");
            } /* end if */
            else
                HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unknown attribute delete_by_idx location");
            break;
        }

        /* H5Aexists/exists_by_name */
        case H5VL_ATTR_EXISTS: {
            if (loc_params->type == H5VL_OBJECT_BY_SELF) {
                /* Check if the attribute exists */
                if (H5O__attr_exists(loc.oloc, args->args.exists.name, args->args.exists.exists) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to determine if attribute exists");
            } /* end if */
            else if (loc_params->type == H5VL_OBJECT_BY_NAME) {
                /* Check if the attribute exists */
                if (H5A__exists_by_name(loc, loc_params->loc_data.loc_by_name.name, args->args.exists.name,
                                        args->args.exists.exists) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to determine if attribute exists");
            } /* end else-if */
            else
                HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unknown parameters");
            break;
        }

        /* H5Aiterate/iterate_by_name */
        case H5VL_ATTR_ITER: {
            H5VL_attr_iterate_args_t *iter_args = &args->args.iterate; /* Arguments to iterate operation */
            static const char        *self_name = ".";                 /* Name for 'self' location */
            const char               *loc_name;                        /* Location name */

            /* Set correct name, for type of location */
            if (loc_params->type == H5VL_OBJECT_BY_SELF) /* H5Aiterate2 */
                loc_name = self_name;
            else if (loc_params->type == H5VL_OBJECT_BY_NAME) /* H5Aiterate_by_name */
                loc_name = loc_params->loc_data.loc_by_name.name;
            else
                HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unsupported location type");

            /* Iterate over attributes */
            if ((ret_value = H5A__iterate(&loc, loc_name, iter_args->idx_type, iter_args->order,
                                          iter_args->idx, iter_args->op, iter_args->op_data)) < 0)
                HERROR(H5E_ATTR, H5E_BADITER, "attribute iteration failed");
            break;
        }

        /* H5Arename/rename_by_name */
        case H5VL_ATTR_RENAME: {
            if (loc_params->type == H5VL_OBJECT_BY_SELF) { /* H5Arename */
                /* Call attribute rename routine */
                if (H5O__attr_rename(loc.oloc, args->args.rename.old_name, args->args.rename.new_name) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't rename attribute");
            }                                                   /* end if */
            else if (loc_params->type == H5VL_OBJECT_BY_NAME) { /* H5Arename_by_name */
                /* Call attribute rename routine */
                if (H5A__rename_by_name(loc, loc_params->loc_data.loc_by_name.name,
                                        args->args.rename.old_name, args->args.rename.new_name) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't rename attribute");
            } /* end else-if */
            else
                HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unknown attribute rename parameters");
            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid specific operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_optional
 *
 * Purpose:     Handles the attribute optional callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_attr_optional(void H5_ATTR_UNUSED *obj, H5VL_optional_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                           void H5_ATTR_UNUSED **req)
{
#ifndef H5_NO_DEPRECATED_SYMBOLS
    H5VL_native_attr_optional_args_t *opt_args = args->args; /* Pointer to native operation's arguments */
#endif                                                       /* H5_NO_DEPRECATED_SYMBOLS */
    herr_t ret_value = SUCCEED;                              /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
        case H5VL_NATIVE_ATTR_ITERATE_OLD: {
            H5VL_native_attr_iterate_old_t *iter_args = &opt_args->iterate_old;

            /* Call the actual iteration routine */
            if ((ret_value = H5A__iterate_old(iter_args->loc_id, iter_args->attr_num, iter_args->op,
                                              iter_args->op_data)) < 0)
                HERROR(H5E_VOL, H5E_BADITER, "error iterating over attributes");

            break;
        }
#endif /* H5_NO_DEPRECATED_SYMBOLS */

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid optional operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_attr_close
 *
 * Purpose:     Handles the attribute close callback
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL (attribute will not be closed)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_attr_close(void *attr, hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (H5A__close((H5A_t *)attr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "can't close attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_attr_close() */
