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
 * Module Info: This module contains the functionality for querying
 *      a "native" datatype for the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"   /* Generic Functions            */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling              */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5Pprivate.h"  /* Property lists            */
#include "H5MMprivate.h" /* Memory management            */
#include "H5Tpkg.h"      /* Datatypes                */

/* Static local functions */
static H5T_t *H5T__get_native_type(H5T_t *dt, H5T_direction_t direction, size_t *struct_align, size_t *offset,
                                   size_t *comp_size);
static H5T_t *H5T__get_native_integer(size_t prec, H5T_sign_t sign, H5T_direction_t direction,
                                      size_t *struct_align, size_t *offset, size_t *comp_size);
static H5T_t *H5T__get_native_float(size_t size, H5T_direction_t direction, size_t *struct_align,
                                    size_t *offset, size_t *comp_size);
static H5T_t *H5T__get_native_bitfield(size_t prec, H5T_direction_t direction, size_t *struct_align,
                                       size_t *offset, size_t *comp_size);
static herr_t H5T__cmp_offset(size_t *comp_size, size_t *offset, size_t elem_size, size_t nelems,
                              size_t align, size_t *struct_align);

/*-------------------------------------------------------------------------
 * Function:    H5Tget_native_type
 *
 * Purpose:     High-level API to return the native type of a datatype.
 *              The native type is chosen by matching the size and class of
 *              queried datatype from the following native primitive
 *              datatypes:
 *                      H5T_NATIVE_CHAR         H5T_NATIVE_UCHAR
 *                      H5T_NATIVE_SHORT        H5T_NATIVE_USHORT
 *                      H5T_NATIVE_INT          H5T_NATIVE_UINT
 *                      H5T_NATIVE_LONG         H5T_NATIVE_ULONG
 *                      H5T_NATIVE_LLONG        H5T_NATIVE_ULLONG
 *
 *                      H5T_NATIVE_FLOAT
 *                      H5T_NATIVE_DOUBLE
 *                      H5T_NATIVE_LDOUBLE
 *
 *              Compound, array, enum, and VL types all choose among these
 *              types for their members.  Time, Bitfield, Opaque, Reference
 *              types are only copy out.
 *
 * Return:      Success:        Returns the native data type if successful.
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tget_native_type(hid_t type_id, H5T_direction_t direction)
{
    H5T_t *dt;               /* Datatype to create native datatype from */
    H5T_t *new_dt    = NULL; /* Datatype for native datatype created */
    size_t comp_size = 0;    /* Compound datatype's size */
    hid_t  ret_value;        /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "iTd", type_id, direction);

    /* Check arguments */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a data type");
    if (direction != H5T_DIR_DEFAULT && direction != H5T_DIR_ASCEND && direction != H5T_DIR_DESCEND)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not valid direction value");

    /* Get the native type */
    if (NULL == (new_dt = H5T__get_native_type(dt, direction, NULL, NULL, &comp_size)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "cannot retrieve native type");

    /* Get an ID for the new type */
    if ((ret_value = H5I_register(H5I_DATATYPE, new_dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register data type");

done:
    /* Error cleanup */
    if (ret_value < 0)
        if (new_dt && H5T_close_real(new_dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release datatype");

    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_native_type() */

/*-------------------------------------------------------------------------
 * Function:    H5T__get_native_type
 *
 * Purpose:     Returns the native type of a datatype.
 *
 * Return:      Success:        Returns the native data type if successful.
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__get_native_type(H5T_t *dtype, H5T_direction_t direction, size_t *struct_align, size_t *offset,
                     size_t *comp_size)
{
    H5T_t  *super_type;       /* Super type of VL, array and enum datatypes */
    H5T_t  *nat_super_type;   /* Native form of VL, array & enum super datatype */
    H5T_t  *new_type  = NULL; /* New native datatype */
    H5T_t  *memb_type = NULL; /* Datatype of member */
    H5T_t **memb_list = NULL; /* List of compound member IDs */
    size_t *memb_offset =
        NULL; /* List of member offsets in compound type, including member size and alignment */
    char      **comp_mname     = NULL; /* List of member names in compound type */
    char       *memb_name      = NULL; /* Enum's member name */
    void       *memb_value     = NULL; /* Enum's member value */
    void       *tmp_memb_value = NULL; /* Enum's member value */
    hsize_t    *dims           = NULL; /* Dimension sizes for array */
    H5T_class_t h5_class;              /* Class of datatype to make native */
    size_t      size;                  /* Size of datatype to make native */
    size_t      prec;                  /* Precision of datatype to make native */
    int         snmemb;                /* Number of members in compound & enum types */
    unsigned    nmemb = 0;             /* Number of members in compound & enum types */
    unsigned    u;                     /* Local index variable */
    H5T_t      *ret_value = NULL;      /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dtype);

    if (H5T_NO_CLASS == (h5_class = H5T_get_class(dtype, false)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a valid class");

    if (0 == (size = H5T_get_size(dtype)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a valid size");

    switch (h5_class) {
        case H5T_INTEGER: {
            H5T_sign_t sign; /* Signedness of integer type */

            if (H5T_SGN_ERROR == (sign = H5T_get_sign(dtype)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a valid signess");

            prec = dtype->shared->u.atomic.prec;

            if (NULL ==
                (ret_value = H5T__get_native_integer(prec, sign, direction, struct_align, offset, comp_size)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot retrieve integer type");
        } /* end case */
        break;

        case H5T_FLOAT:
            if (NULL == (ret_value = H5T__get_native_float(size, direction, struct_align, offset, comp_size)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot retrieve float type");

            break;

        case H5T_STRING:
            if (NULL == (ret_value = H5T_copy(dtype, H5T_COPY_TRANSIENT)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot retrieve float type");

            if (H5T_IS_VL_STRING(dtype->shared)) {
                /* Update size, offset and compound alignment for parent. */
                if (H5T__cmp_offset(comp_size, offset, sizeof(char *), (size_t)1, H5T_POINTER_ALIGN_g,
                                    struct_align) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");
            } /* end if */
            else {
                /* Update size, offset and compound alignment for parent. */
                if (H5T__cmp_offset(comp_size, offset, sizeof(char), size, H5T_NATIVE_SCHAR_ALIGN_g,
                                    struct_align) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");
            } /* end else */
            break;

        /* The time type will be supported in the future.  Simply return "not supported"
         * message for now.*/
        case H5T_TIME:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "time type is not supported yet");

        case H5T_BITFIELD: {
            prec = dtype->shared->u.atomic.prec;

            if (NULL ==
                (ret_value = H5T__get_native_bitfield(prec, direction, struct_align, offset, comp_size)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot retrieve integer for bitfield type");
        } /* end case */
        break;

        case H5T_OPAQUE:
            if (NULL == (ret_value = H5T_copy(dtype, H5T_COPY_TRANSIENT)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot retrieve float type");

            /* Update size, offset and compound alignment for parent. */
            if (H5T__cmp_offset(comp_size, offset, sizeof(char), size, H5T_NATIVE_SCHAR_ALIGN_g,
                                struct_align) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");
            break;

        case H5T_REFERENCE: {
            H5T_t *dt; /* Datatype to make native */
            size_t align;
            size_t ref_size;

            if (NULL == (ret_value = H5T_copy(dtype, H5T_COPY_TRANSIENT)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot copy reference type");

            /* Decide if the data type is object reference. */
            if (NULL == (dt = (H5T_t *)H5I_object(H5T_STD_REF_OBJ_g)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data type");

            /* Update size, offset and compound alignment for parent. */
            if (0 == H5T_cmp(ret_value, dt, false)) {
                align    = H5T_HOBJREF_ALIGN_g;
                ref_size = sizeof(hobj_ref_t);
            } /* end if */
            else {
                /* Decide if the data type is dataset region reference. */
                if (NULL == (dt = (H5T_t *)H5I_object(H5T_STD_REF_DSETREG_g)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data type");

                if (0 == H5T_cmp(ret_value, dt, false)) {
                    align    = H5T_HDSETREGREF_ALIGN_g;
                    ref_size = sizeof(hdset_reg_ref_t);
                } /* end if */
                else {
                    /* Only pointers to underlying opaque reference types */
                    align    = H5T_REF_ALIGN_g;
                    ref_size = sizeof(H5R_ref_t);
                } /* end else */
            }     /* end else */

            if (H5T__cmp_offset(comp_size, offset, ref_size, (size_t)1, align, struct_align) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");
        } /* end case */
        break;

        case H5T_COMPOUND: {
            size_t children_size = 0; /* Total size of compound members */
            size_t children_st_align =
                0; /* The max alignment among compound members.  This'll be the compound alignment */

            if ((snmemb = H5T_get_nmembers(dtype)) <= 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "compound data type doesn't have any member");
            H5_CHECKED_ASSIGN(nmemb, unsigned, snmemb, int);

            if (NULL == (memb_list = (H5T_t **)H5MM_calloc(nmemb * sizeof(H5T_t *))))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot allocate memory");
            if (NULL == (memb_offset = (size_t *)H5MM_calloc(nmemb * sizeof(size_t))))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot allocate memory");
            if (NULL == (comp_mname = (char **)H5MM_calloc(nmemb * sizeof(char *))))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot allocate memory");

            /* Construct child compound type and retrieve a list of their IDs, offsets, total size, and
             * alignment for compound type. */
            for (u = 0; u < nmemb; u++) {
                if (NULL == (memb_type = H5T_get_member_type(dtype, u)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "member type retrieval failed");

                if (NULL == (comp_mname[u] = H5T__get_member_name(dtype, u)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "member type retrieval failed");

                if (NULL == (memb_list[u] = H5T__get_native_type(memb_type, direction, &children_st_align,
                                                                 &(memb_offset[u]), &children_size)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "member identifier retrieval failed");

                if (H5T_close_real(memb_type) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot close datatype");
            } /* end for */

            /* The alignment for whole compound type */
            if (children_st_align && children_size % children_st_align)
                children_size += children_st_align - (children_size % children_st_align);

            /* Construct new compound type based on native type */
            if (NULL == (new_type = H5T__create(H5T_COMPOUND, children_size)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot create a compound type");

            /* Insert members for the new compound type */
            for (u = 0; u < nmemb; u++)
                if (H5T__insert(new_type, comp_mname[u], memb_offset[u], memb_list[u]) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot insert member to compound datatype");

            /* Update size, offset and compound alignment for parent in the case of
             * nested compound type.  The alignment for a compound type as one field in
             * a compound type is the biggest compound alignment among all its members.
             * e.g. in the structure
             *    typedef struct s1 {
             *        char            c;
             *        int             i;
             *        s2              st;
             *        unsigned long long       l;
             *    } s1;
             *    typedef struct s2 {
             *        short           c2;
             *        long            l2;
             *        long long       ll2;
             *    } s2;
             * The alignment for ST in S1 is the biggest structure alignment of all the
             * members of S2, which is probably the LL2 of 'long long'. -SLU 2010/4/28
             */
            if (H5T__cmp_offset(comp_size, offset, children_size, (size_t)1, children_st_align,
                                struct_align) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");

            /* Close member data type */
            for (u = 0; u < nmemb; u++) {
                if (H5T_close_real(memb_list[u]) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot close datatype");

                /* Free member names in list */
                comp_mname[u] = (char *)H5MM_xfree(comp_mname[u]);
            } /* end for */

            /* Free lists for members */
            memb_list   = (H5T_t **)H5MM_xfree(memb_list);
            memb_offset = (size_t *)H5MM_xfree(memb_offset);
            comp_mname  = (char **)H5MM_xfree(comp_mname);

            ret_value = new_type;
        } /* end case */
        break;

        case H5T_ENUM: {
            H5T_path_t *tpath; /* Type conversion info    */
            hid_t       super_type_id, nat_super_type_id;

            /* Don't need to do anything special for alignment, offset since the ENUM type usually is integer.
             */

            /* Retrieve base type for enumerated type */
            if (NULL == (super_type = H5T_get_super(dtype)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "unable to get base type for enumerate type");
            if (NULL == (nat_super_type =
                             H5T__get_native_type(super_type, direction, struct_align, offset, comp_size)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "base native type retrieval failed");

            if ((super_type_id = H5I_register(H5I_DATATYPE, super_type, false)) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot register datatype");
            if ((nat_super_type_id = H5I_register(H5I_DATATYPE, nat_super_type, false)) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot register datatype");

            /* Allocate room for the enum values */
            if (NULL == (tmp_memb_value = H5MM_calloc(H5T_get_size(super_type))))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot allocate memory");
            if (NULL == (memb_value = H5MM_calloc(H5T_get_size(nat_super_type))))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot allocate memory");

            /* Construct new enum type based on native type */
            if (NULL == (new_type = H5T__enum_create(nat_super_type)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "unable to create enum type");

            /* Find the conversion function */
            if (NULL == (tpath = H5T_path_find(super_type, nat_super_type)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL,
                            "unable to convert between src and dst data types");

            /* Retrieve member info and insert members into new enum type */
            if ((snmemb = H5T_get_nmembers(dtype)) <= 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "enumerate data type doesn't have any member");
            H5_CHECKED_ASSIGN(nmemb, unsigned, snmemb, int);
            for (u = 0; u < nmemb; u++) {
                if (NULL == (memb_name = H5T__get_member_name(dtype, u)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot get member name");
                if (H5T__get_member_value(dtype, u, tmp_memb_value) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot get member value");
                H5MM_memcpy(memb_value, tmp_memb_value, H5T_get_size(super_type));

                if (H5T_convert(tpath, super_type_id, nat_super_type_id, (size_t)1, (size_t)0, (size_t)0,
                                memb_value, NULL) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot get member value");

                if (H5T__enum_insert(new_type, memb_name, memb_value) < 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot insert member");
                memb_name = (char *)H5MM_xfree(memb_name);
            }
            memb_value     = H5MM_xfree(memb_value);
            tmp_memb_value = H5MM_xfree(tmp_memb_value);

            /* Close base type */
            if (H5I_dec_app_ref(nat_super_type_id) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot close datatype");
            /* Close super type */
            if (H5I_dec_app_ref(super_type_id) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot close datatype");

            ret_value = new_type;
        } /* end case */
        break;

        case H5T_ARRAY: {
            int      sarray_rank; /* Array's rank */
            unsigned array_rank;  /* Array's rank */
            hsize_t  nelems       = 1;
            size_t   super_offset = 0;
            size_t   super_size   = 0;
            size_t   super_align  = 0;

            /* Retrieve dimension information for array data type */
            if ((sarray_rank = H5T__get_array_ndims(dtype)) <= 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot get dimension rank");
            H5_CHECKED_ASSIGN(array_rank, unsigned, sarray_rank, int);
            if (NULL == (dims = (hsize_t *)H5MM_malloc(array_rank * sizeof(hsize_t))))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot allocate memory");
            if (H5T__get_array_dims(dtype, dims) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot get dimension size");

            /* Retrieve base type for array type */
            if (NULL == (super_type = H5T_get_super(dtype)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "unable to get parent type for array type");
            if (NULL == (nat_super_type = H5T__get_native_type(super_type, direction, &super_align,
                                                               &super_offset, &super_size)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "parent native type retrieval failed");

            /* Close super type */
            if (H5T_close_real(super_type) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CLOSEERROR, NULL, "cannot close datatype");

            /* Create a new array type based on native type */
            if (NULL == (new_type = H5T__array_create(nat_super_type, array_rank, dims)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "unable to create array type");

            /* Close base type */
            if (H5T_close_real(nat_super_type) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CLOSEERROR, NULL, "cannot close datatype");

            for (u = 0; u < array_rank; u++)
                nelems *= dims[u];
            H5_CHECK_OVERFLOW(nelems, hsize_t, size_t);
            if (H5T__cmp_offset(comp_size, offset, super_size, (size_t)nelems, super_align, struct_align) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");

            dims = (hsize_t *)H5MM_xfree(dims);

            ret_value = new_type;
        } /* end case */
        break;

        case H5T_VLEN: {
            size_t vl_align   = 0;
            size_t vl_size    = 0;
            size_t super_size = 0;

            /* Retrieve base type for array type */
            if (NULL == (super_type = H5T_get_super(dtype)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "unable to get parent type for VL type");
            /* Don't need alignment, offset information if this VL isn't a field of compound type.  If it
             * is, go to a few steps below to compute the information directly. */
            if (NULL ==
                (nat_super_type = H5T__get_native_type(super_type, direction, NULL, NULL, &super_size)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "parent native type retrieval failed");

            /* Close super type */
            if (H5T_close_real(super_type) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CLOSEERROR, NULL, "cannot close datatype");

            /* Create a new array type based on native type */
            if (NULL == (new_type = H5T__vlen_create(nat_super_type)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "unable to create VL type");

            /* Close base type */
            if (H5T_close_real(nat_super_type) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CLOSEERROR, NULL, "cannot close datatype");

            /* Update size, offset and compound alignment for parent compound type directly. */
            vl_align = H5T_HVL_ALIGN_g;
            vl_size  = sizeof(hvl_t);

            if (H5T__cmp_offset(comp_size, offset, vl_size, (size_t)1, vl_align, struct_align) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");

            ret_value = new_type;
        } /* end case */
        break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "data type doesn't match any native type");
    } /* end switch */

done:
    /* Error cleanup */
    if (NULL == ret_value) {
        if (new_type)
            if (H5T_close_real(new_type) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, NULL, "unable to release datatype");

        /* Free lists for members */
        if (memb_list) {
            for (u = 0; u < nmemb; u++)
                if (memb_list[u] && H5T_close_real(memb_list[u]) < 0)
                    HDONE_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot close datatype");

            memb_list = (H5T_t **)H5MM_xfree(memb_list);
        } /* end if */
        memb_offset = (size_t *)H5MM_xfree(memb_offset);
        if (comp_mname) {
            for (u = 0; u < nmemb; u++)
                if (comp_mname[u])
                    H5MM_xfree(comp_mname[u]);
            comp_mname = (char **)H5MM_xfree(comp_mname);
        } /* end if */
        memb_name      = (char *)H5MM_xfree(memb_name);
        memb_value     = H5MM_xfree(memb_value);
        tmp_memb_value = H5MM_xfree(tmp_memb_value);
        dims           = (hsize_t *)H5MM_xfree(dims);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__get_native_type() */

/* Disable warning for intentional identical branches here -QAK */
/*
 *       This pragma only needs to surround the "duplicated branches" in
 *       the code below, but early (4.4.7, at least) gcc only allows
 *       diagnostic pragmas to be toggled outside of functions.
 */
H5_GCC_DIAG_OFF("duplicated-branches")

/*-------------------------------------------------------------------------
 * Function:    H5T__get_native_integer
 *
 * Purpose:     Returns the native integer type of a datatype.
 *
 * Return:      Success:        Returns the native data type if successful.
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__get_native_integer(size_t prec, H5T_sign_t sign, H5T_direction_t direction, size_t *struct_align,
                        size_t *offset, size_t *comp_size)
{
    H5T_t *dt;                 /* Appropriate native datatype to copy */
    hid_t  tid         = (-1); /* Datatype ID of appropriate native datatype */
    size_t align       = 0;    /* Alignment necessary for native datatype */
    size_t native_size = 0;    /* Datatype size of the native type */
    enum match_type {          /* The different kinds of integers we can match */
                      H5T_NATIVE_INT_MATCH_CHAR,
                      H5T_NATIVE_INT_MATCH_SHORT,
                      H5T_NATIVE_INT_MATCH_INT,
                      H5T_NATIVE_INT_MATCH_LONG,
                      H5T_NATIVE_INT_MATCH_LLONG,
                      H5T_NATIVE_INT_MATCH_UNKNOWN
    } match          = H5T_NATIVE_INT_MATCH_UNKNOWN;
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    if (direction == H5T_DIR_DEFAULT || direction == H5T_DIR_ASCEND) {
        if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_SCHAR_g))) {
            match       = H5T_NATIVE_INT_MATCH_CHAR;
            native_size = sizeof(char);
        }
        else if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_SHORT_g))) {
            match       = H5T_NATIVE_INT_MATCH_SHORT;
            native_size = sizeof(short);
        }
        else if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_INT_g))) {
            match       = H5T_NATIVE_INT_MATCH_INT;
            native_size = sizeof(int);
        }
        else if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_LONG_g))) {
            match       = H5T_NATIVE_INT_MATCH_LONG;
            native_size = sizeof(long);
        }
        else if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_LLONG_g))) {
            match       = H5T_NATIVE_INT_MATCH_LLONG;
            native_size = sizeof(long long);
        }
        else { /* If no native type matches the queried datatype, simply choose the type of biggest size. */
            match       = H5T_NATIVE_INT_MATCH_LLONG;
            native_size = sizeof(long long);
        }
    }
    else if (direction == H5T_DIR_DESCEND) {
        if (prec > H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_LONG_g))) {
            match       = H5T_NATIVE_INT_MATCH_LLONG;
            native_size = sizeof(long long);
        }
        else if (prec > H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_INT_g))) {
            match       = H5T_NATIVE_INT_MATCH_LONG;
            native_size = sizeof(long);
        }
        else if (prec > H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_SHORT_g))) {
            match       = H5T_NATIVE_INT_MATCH_INT;
            native_size = sizeof(int);
        }
        else if (prec > H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_SCHAR_g))) {
            match       = H5T_NATIVE_INT_MATCH_SHORT;
            native_size = sizeof(short);
        }
        else {
            match       = H5T_NATIVE_INT_MATCH_CHAR;
            native_size = sizeof(char);
        }
    }

    /* Set the appropriate native datatype information */
    switch (match) {
        case H5T_NATIVE_INT_MATCH_CHAR:
            if (sign == H5T_SGN_2)
                tid = H5T_NATIVE_SCHAR;
            else
                tid = H5T_NATIVE_UCHAR;

            align = H5T_NATIVE_SCHAR_ALIGN_g;
            break;

        case H5T_NATIVE_INT_MATCH_SHORT:
            if (sign == H5T_SGN_2)
                tid = H5T_NATIVE_SHORT;
            else
                tid = H5T_NATIVE_USHORT;
            align = H5T_NATIVE_SHORT_ALIGN_g;
            break;

        case H5T_NATIVE_INT_MATCH_INT:
            if (sign == H5T_SGN_2)
                tid = H5T_NATIVE_INT;
            else
                tid = H5T_NATIVE_UINT;

            align = H5T_NATIVE_INT_ALIGN_g;
            break;

        case H5T_NATIVE_INT_MATCH_LONG:
            if (sign == H5T_SGN_2)
                tid = H5T_NATIVE_LONG;
            else
                tid = H5T_NATIVE_ULONG;

            align = H5T_NATIVE_LONG_ALIGN_g;
            break;

        case H5T_NATIVE_INT_MATCH_LLONG:
            if (sign == H5T_SGN_2)
                tid = H5T_NATIVE_LLONG;
            else
                tid = H5T_NATIVE_ULLONG;

            align = H5T_NATIVE_LLONG_ALIGN_g;
            break;

        case H5T_NATIVE_INT_MATCH_UNKNOWN:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "Unknown native integer match");
    } /* end switch */

    /* Create new native type */
    assert(tid >= 0);
    if (NULL == (dt = (H5T_t *)H5I_object(tid)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data type");

    if (NULL == (ret_value = H5T_copy(dt, H5T_COPY_TRANSIENT)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot copy type");

    /* compute size and offset of compound type member. */
    if (H5T__cmp_offset(comp_size, offset, native_size, (size_t)1, align, struct_align) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__get_native_integer() */
H5_GCC_DIAG_ON("duplicated-branches")

/* Disable warning for intentional identical branches here -QAK */
/*
 *       This pragma only needs to surround the "duplicated branches" in
 *       the code below, but early (4.4.7, at least) gcc only allows
 *       diagnostic pragmas to be toggled outside of functions.
 */
H5_GCC_DIAG_OFF("duplicated-branches")

/*-------------------------------------------------------------------------
 * Function:    H5T__get_native_float
 *
 * Purpose:     Returns the native float type of a datatype.
 *
 * Return:      Success:        Returns the native data type if successful.
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__get_native_float(size_t size, H5T_direction_t direction, size_t *struct_align, size_t *offset,
                      size_t *comp_size)
{
    H5T_t *dt          = NULL; /* Appropriate native datatype to copy */
    hid_t  tid         = (-1); /* Datatype ID of appropriate native datatype */
    size_t align       = 0;    /* Alignment necessary for native datatype */
    size_t native_size = 0;    /* Datatype size of the native type */
    enum match_type {          /* The different kinds of floating point types we can match */
                      H5T_NATIVE_FLOAT_MATCH_FLOAT,
                      H5T_NATIVE_FLOAT_MATCH_DOUBLE,
                      H5T_NATIVE_FLOAT_MATCH_LDOUBLE,
                      H5T_NATIVE_FLOAT_MATCH_UNKNOWN
    } match          = H5T_NATIVE_FLOAT_MATCH_UNKNOWN;
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(size > 0);

    if (direction == H5T_DIR_DEFAULT || direction == H5T_DIR_ASCEND) {
        if (size <= sizeof(float)) {
            match       = H5T_NATIVE_FLOAT_MATCH_FLOAT;
            native_size = sizeof(float);
        }
        else if (size <= sizeof(double)) {
            match       = H5T_NATIVE_FLOAT_MATCH_DOUBLE;
            native_size = sizeof(double);
        }
        else if (size <= sizeof(long double)) {
            match       = H5T_NATIVE_FLOAT_MATCH_LDOUBLE;
            native_size = sizeof(long double);
        }
        else { /* If not match, return the biggest datatype */
            match       = H5T_NATIVE_FLOAT_MATCH_LDOUBLE;
            native_size = sizeof(long double);
        }
    }
    else {
        if (size > sizeof(double)) {
            match       = H5T_NATIVE_FLOAT_MATCH_LDOUBLE;
            native_size = sizeof(long double);
        }
        else if (size > sizeof(float)) {
            match       = H5T_NATIVE_FLOAT_MATCH_DOUBLE;
            native_size = sizeof(double);
        }
        else {
            match       = H5T_NATIVE_FLOAT_MATCH_FLOAT;
            native_size = sizeof(float);
        }
    }

    /* Set the appropriate native floating point information */
    switch (match) {
        case H5T_NATIVE_FLOAT_MATCH_FLOAT:
            tid   = H5T_NATIVE_FLOAT;
            align = H5T_NATIVE_FLOAT_ALIGN_g;
            break;

        case H5T_NATIVE_FLOAT_MATCH_DOUBLE:
            tid   = H5T_NATIVE_DOUBLE;
            align = H5T_NATIVE_DOUBLE_ALIGN_g;
            break;

        case H5T_NATIVE_FLOAT_MATCH_LDOUBLE:
            tid   = H5T_NATIVE_LDOUBLE;
            align = H5T_NATIVE_LDOUBLE_ALIGN_g;
            break;

        case H5T_NATIVE_FLOAT_MATCH_UNKNOWN:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "Unknown native floating-point match");
    } /* end switch */

    /* Create new native type */
    assert(tid >= 0);
    if (NULL == (dt = (H5T_t *)H5I_object(tid)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data type");
    if ((ret_value = H5T_copy(dt, H5T_COPY_TRANSIENT)) == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot retrieve float type");

    /* compute offset of compound type member. */
    if (H5T__cmp_offset(comp_size, offset, native_size, (size_t)1, align, struct_align) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__get_native_float() */
H5_GCC_DIAG_ON("duplicated-branches")

/* Disable warning for intentional identical branches here -QAK */
/*
 *       This pragma only needs to surround the "duplicated branches" in
 *       the code below, but early (4.4.7, at least) gcc only allows
 *       diagnostic pragmas to be toggled outside of functions.
 */
H5_GCC_DIAG_OFF("duplicated-branches")

/*-------------------------------------------------------------------------
 * Function:    H5T__get_native_bitfield
 *
 * Purpose:     Returns the native bitfield type of a datatype.  Bitfield
 *              is similar to unsigned integer.
 *
 * Return:      Success:        Returns the native data type if successful.
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__get_native_bitfield(size_t prec, H5T_direction_t direction, size_t *struct_align, size_t *offset,
                         size_t *comp_size)
{
    H5T_t *dt;                 /* Appropriate native datatype to copy */
    hid_t  tid         = (-1); /* Datatype ID of appropriate native datatype */
    size_t align       = 0;    /* Alignment necessary for native datatype */
    size_t native_size = 0;    /* Datatype size of the native type */
    H5T_t *ret_value   = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    if (direction == H5T_DIR_DEFAULT || direction == H5T_DIR_ASCEND) {
        if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_B8_g))) {
            tid         = H5T_NATIVE_B8;
            native_size = 1;
            align       = H5T_NATIVE_UINT8_ALIGN_g;
        }
        else if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_B16_g))) {
            tid         = H5T_NATIVE_B16;
            native_size = 2;
            align       = H5T_NATIVE_UINT16_ALIGN_g;
        }
        else if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_B32_g))) {
            tid         = H5T_NATIVE_B32;
            native_size = 4;
            align       = H5T_NATIVE_UINT32_ALIGN_g;
        }
        else if (prec <= H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_B64_g))) {
            tid         = H5T_NATIVE_B64;
            native_size = 8;
            align       = H5T_NATIVE_UINT64_ALIGN_g;
        }
        else { /* If no native type matches the queried datatype, simply choose the type of biggest size. */
            tid         = H5T_NATIVE_B64;
            native_size = 8;
            align       = H5T_NATIVE_UINT64_ALIGN_g;
        }
    }
    else if (direction == H5T_DIR_DESCEND) {
        if (prec > H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_B32_g))) {
            tid         = H5T_NATIVE_B64;
            native_size = 8;
            align       = H5T_NATIVE_UINT64_ALIGN_g;
        }
        else if (prec > H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_B16_g))) {
            tid         = H5T_NATIVE_B32;
            native_size = 4;
            align       = H5T_NATIVE_UINT32_ALIGN_g;
        }
        else if (prec > H5T_get_precision((H5T_t *)H5I_object(H5T_NATIVE_B8_g))) {
            tid         = H5T_NATIVE_B16;
            native_size = 2;
            align       = H5T_NATIVE_UINT16_ALIGN_g;
        }
        else {
            tid         = H5T_NATIVE_B8;
            native_size = 1;
            align       = H5T_NATIVE_UINT8_ALIGN_g;
        }
    }

    /* Create new native type */
    assert(tid >= 0);
    if (NULL == (dt = (H5T_t *)H5I_object(tid)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data type");

    if ((ret_value = H5T_copy(dt, H5T_COPY_TRANSIENT)) == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot copy type");

    /* compute size and offset of compound type member. */
    if (H5T__cmp_offset(comp_size, offset, native_size, (size_t)1, align, struct_align) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "cannot compute compound offset");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__get_native_bitfield() */
H5_GCC_DIAG_ON("duplicated-branches")

/*-------------------------------------------------------------------------
 * Function:    H5T__cmp_offset
 *
 * Purpose:    This function is only for convenience.  It computes the
 *              compound type size, offset of the member being considered
 *              and the alignment for the whole compound type.
 *
 * Return:    Success:        Non-negative value.
 *
 *            Failure:        Negative value.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__cmp_offset(size_t *comp_size, size_t *offset, size_t elem_size, size_t nelems, size_t align,
                size_t *struct_align)
{
    FUNC_ENTER_PACKAGE_NOERR

    if (offset && comp_size) {
        if (align > 1 && *comp_size % align) {
            /* Add alignment value */
            *offset = *comp_size + (align - *comp_size % align);
            *comp_size += (align - *comp_size % align);
        } /* end if */
        else
            *offset = *comp_size;

        /* compute size of compound type member. */
        *comp_size += nelems * elem_size;
    } /* end if */

    if (struct_align && *struct_align < align)
        *struct_align = align;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__cmp_offset() */

#define TAG_ALIGNMENT(tag) (offsetof(alignments_t, tag.x) - offsetof(alignments_t, tag))

/* clang-format off */
#define NATIVE_ENTRY_INITIALIZER(tag, type, precision, has_sign) {  \
  .alignmentp = &H5T_NATIVE_##tag##_ALIGN_g                         \
, .alignment = TAG_ALIGNMENT(tag)                                   \
, .hidp = &H5T_NATIVE_##tag##_g                                     \
, .size = sizeof(type)                                              \
, .atomic = {                                                       \
      .offset   = 0                                                 \
    , .prec     = (precision != 0) ? precision : (sizeof(type) * 8) \
    , .lsb_pad  = H5T_PAD_ZERO                                      \
    , .msb_pad  = H5T_PAD_ZERO                                      \
    , .u.i.sign = has_sign ? H5T_SGN_2 : H5T_SGN_NONE               \
    }                                                               \
}
/* clang-format on */

static H5T_order_t
get_host_byte_order(void)
{
    static const union {
        uint64_t u64;
        char     byte[8];
    } endian_exemplar = {.byte = {1}};

    return (endian_exemplar.u64 == 1) ? H5T_ORDER_LE : H5T_ORDER_BE;
}

/* Establish `H5T_t`s for C99 integer types including fixed- and
 * minimum-width types (uint16_t, uint_least16_t, uint_fast16_t, ...).
 *
 * Also establish alignment for some miscellaneous types: pointers,
 * HDF5 references, and so on.
 */
herr_t
H5T__init_native_internal(void)
{
    /* Here we construct a type that lets us find alignment constraints
     * without using the alignof operator, which is not available in C99.
     *
     * Between each sub-struct's `char` member `c` and member `x`, the
     * compiler must insert padding to ensure proper alignment of `x`.
     * We can find the alignment constraint of each `x` by looking at
     * its offset from the beginning of its sub-struct.
     */
    typedef struct {
        struct {
            char        c;
            signed char x;
        } SCHAR;
        struct {
            char          c;
            unsigned char x;
        } UCHAR;
        struct {
            char  c;
            short x;
        } SHORT;
        struct {
            char           c;
            unsigned short x;
        } USHORT;
        struct {
            char c;
            int  x;
        } INT;
        struct {
            char         c;
            unsigned int x;
        } UINT;
        struct {
            char c;
            long x;
        } LONG;
        struct {
            char          c;
            unsigned long x;
        } ULONG;
        struct {
            char      c;
            long long x;
        } LLONG;
        struct {
            char               c;
            unsigned long long x;
        } ULLONG;
        struct {
            char   c;
            int8_t x;
        } INT8;
        struct {
            char    c;
            uint8_t x;
        } UINT8;
        struct {
            char         c;
            int_least8_t x;
        } INT_LEAST8;
        struct {
            char          c;
            uint_least8_t x;
        } UINT_LEAST8;
        struct {
            char        c;
            int_fast8_t x;
        } INT_FAST8;
        struct {
            char         c;
            uint_fast8_t x;
        } UINT_FAST8;
        struct {
            char    c;
            int16_t x;
        } INT16;
        struct {
            char     c;
            uint16_t x;
        } UINT16;
        struct {
            char          c;
            int_least16_t x;
        } INT_LEAST16;
        struct {
            char           c;
            uint_least16_t x;
        } UINT_LEAST16;
        struct {
            char         c;
            int_fast16_t x;
        } INT_FAST16;
        struct {
            char          c;
            uint_fast16_t x;
        } UINT_FAST16;
        struct {
            char    c;
            int32_t x;
        } INT32;
        struct {
            char     c;
            uint32_t x;
        } UINT32;
        struct {
            char          c;
            int_least32_t x;
        } INT_LEAST32;
        struct {
            char           c;
            uint_least32_t x;
        } UINT_LEAST32;
        struct {
            char         c;
            int_fast32_t x;
        } INT_FAST32;
        struct {
            char          c;
            uint_fast32_t x;
        } UINT_FAST32;
        struct {
            char    c;
            int64_t x;
        } INT64;
        struct {
            char     c;
            uint64_t x;
        } UINT64;
        struct {
            char          c;
            int_least64_t x;
        } INT_LEAST64;
        struct {
            char           c;
            uint_least64_t x;
        } UINT_LEAST64;
        struct {
            char         c;
            int_fast64_t x;
        } INT_FAST64;
        struct {
            char          c;
            uint_fast64_t x;
        } UINT_FAST64;
        struct {
            char  c;
            void *x;
        } pointer;
        struct {
            char  c;
            hvl_t x;
        } hvl;
        struct {
            char       c;
            hobj_ref_t x;
        } hobjref;
        struct {
            char            c;
            hdset_reg_ref_t x;
        } hdsetregref;
        struct {
            char      c;
            H5R_ref_t x;
        } ref;
    } alignments_t;

    /* Describe a C99 type, `type`, and tell where to write its
     * H5T_t identifier and alignment.  Tables of these descriptions
     * drive the initialization of `H5T_t`s.
     */
    typedef struct {
        /* Pointer to the global variable that receives the
         * alignment of `type`:
         */
        size_t *alignmentp;
        size_t  alignment; // natural alignment of `type`
        /* Pointer to the global variable that receives the
         * identifier for `type`'s H5T_t:
         */
        hid_t       *hidp;
        size_t       size;   // sizeof(`type`)
        H5T_atomic_t atomic; // `type` facts such as signedness
    } native_int_t;

    typedef struct {
        const native_int_t *table;
        size_t              nelmts;
    } native_int_table_t;

    /* clang-format off */

    /* Version 19.10 of the PGI C compiler croaks on the following
     * tables if they are `static`, so make them `static` only if
     * some other compiler is used.
     */
#if defined(__PGIC__) && __PGIC__ == 19 && __PGIC_MINOR__ == 10
#   define static_unless_buggy_pgic
#else
#   define static_unless_buggy_pgic static
#endif

    /* The library compiles with a limit on `static` object size, so
     * I broke this table into three.
     */
    static_unless_buggy_pgic const native_int_t table1[] = {
      NATIVE_ENTRY_INITIALIZER(SCHAR, signed char, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UCHAR, unsigned char, 0, false)
    , NATIVE_ENTRY_INITIALIZER(SHORT, short, 0, true)
    , NATIVE_ENTRY_INITIALIZER(USHORT, unsigned short, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT, int, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT, unsigned int, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT, int, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT, unsigned int, 0, false)
    , NATIVE_ENTRY_INITIALIZER(LONG, long, 0, true)
    , NATIVE_ENTRY_INITIALIZER(ULONG, unsigned long, 0, false)
    , NATIVE_ENTRY_INITIALIZER(LLONG, long long, 0, true)
    , NATIVE_ENTRY_INITIALIZER(ULLONG, unsigned long long, 0, false)
    };
    static_unless_buggy_pgic const native_int_t table2[] = {
      NATIVE_ENTRY_INITIALIZER(INT8, int8_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT8, uint8_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_LEAST8, int_least8_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_LEAST8, uint_least8_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_FAST8, int_fast8_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_FAST8, uint_fast8_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT16, int16_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT16, uint16_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_LEAST16, int_least16_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_LEAST16, uint_least16_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_FAST16, int_fast16_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_FAST16, uint_fast16_t, 0, false)
    };
    static_unless_buggy_pgic const native_int_t table3[] = {
      NATIVE_ENTRY_INITIALIZER(INT32, int32_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT32, uint32_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_LEAST32, int_least32_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_LEAST32, uint_least32_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_FAST32, int_fast32_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_FAST32, uint_fast32_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT64, int64_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT64, uint64_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_LEAST64, int_least64_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_LEAST64, uint_least64_t, 0, false)
    , NATIVE_ENTRY_INITIALIZER(INT_FAST64, int_fast64_t, 0, true)
    , NATIVE_ENTRY_INITIALIZER(UINT_FAST64, uint_fast64_t, 0, false)
    };
    static_unless_buggy_pgic const native_int_table_t table_table[] = {
      {table1, NELMTS(table1)}
    , {table2, NELMTS(table2)}
    , {table3, NELMTS(table3)}
    };
#undef static_unless_buggy_pgic
    /* clang-format on */

    size_t      i, j;
    H5T_order_t byte_order = get_host_byte_order();

    for (i = 0; i < NELMTS(table_table); i++) {
        const native_int_t *table  = table_table[i].table;
        size_t              nelmts = table_table[i].nelmts;

        /* For each C99 type in `table`, create its H5T_t,
         * register a hid_t for the H5T_t, and record the type's
         * alignment and hid_t in the variables named by the
         * table.
         */
        for (j = 0; j < nelmts; j++) {
            H5T_t *dt;

            if (NULL == (dt = H5T__alloc()))
                return FAIL;

            dt->shared->state          = H5T_STATE_IMMUTABLE;
            dt->shared->type           = H5T_INTEGER;
            dt->shared->size           = table[j].size;
            dt->shared->u.atomic       = table[j].atomic;
            dt->shared->u.atomic.order = byte_order;
            *table[j].alignmentp       = table[j].alignment;

            if ((*table[j].hidp = H5I_register(H5I_DATATYPE, dt, false)) < 0)
                return FAIL;
        }
    }

    H5T_POINTER_ALIGN_g     = TAG_ALIGNMENT(pointer);
    H5T_HVL_ALIGN_g         = TAG_ALIGNMENT(hvl);
    H5T_HOBJREF_ALIGN_g     = TAG_ALIGNMENT(hobjref);
    H5T_HDSETREGREF_ALIGN_g = TAG_ALIGNMENT(hdsetregref);
    H5T_REF_ALIGN_g         = TAG_ALIGNMENT(ref);

    return SUCCEED;
}
