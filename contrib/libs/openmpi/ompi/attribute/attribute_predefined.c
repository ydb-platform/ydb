/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      University of Houston. All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Setup the predefined attributes in MPI.
 *
 * A number of pre-defined attributes are created here, most of which
 * are exactly what one would expect, but there are a few exceptions
 * -- so they're documented here.
 *
 * Predefined attributes are integer-valued or address-valued (per
 * MPI-2; see section 4.12.7, keeping in mind that Example 4.13 is
 * totally wrong -- see src/attribute/attribute.h for a lengthy
 * explanation of this).
 *
 * The only address-valued attribute is MPI_WIN_BASE.  We treat it as
 * if it were set from C.  All other attributes are integer-valued.
 * We treat them as if they were set from Fortran MPI-1 (i.e.,
 * MPI_ATTR_PUT) or Fortran MPI-2 (i.e., MPI_xxx_ATTR_SET).  Most
 * attributes are MPI-1 integer-valued, meaning that they are the size
 * of MPI_Fint (INTEGER).  But MPI_WIN_SIZE and MPI_WIN_DISP_UNIT are
 * MPI-2 integer-valued, meaning that they are the size of MPI_Aint
 * (INTEGER(KIND=MPI_ADDRESS_KIND)).
 *
 * MPI_TAG_UB is set to a fixed upper limit.
 *
 * MPI_HOST is set to MPI_PROC_NULL (per MPI-1, see 7.1.1, p192).
 *
 * MPI_IO is set to MPI_ANY_SOURCE.  We may need to revist this.
 *
 * MPI_WTIME_IS_GLOBAL is set to 0 (a conservative answer).
 *
 * MPI_APPNUM is set as the result of a GPR subscription.
 *
 * MPI_LASTUSEDCODE is set to an initial value and is reset every time
 * MPI_ADD_ERROR_CLASS or MPI_ADD_ERROR_CODE is invoked.
 * Its copy function is set to
 * MPI_COMM_NULL_COPY_FN, meaning that *only* MPI_COMM_WORLD will have
 * this attribute value.  As such, we only have to update
 * MPI_COMM_WORLD when this value changes (i.e., since this is an
 * integer-valued attribute, we have to update this attribute on every
 * communicator -- using NULL_COPY_FN ensures that only MPI_COMM_WORLD
 * has this attribute value set).
 *
 * MPI_UNIVERSE_SIZE is set as the result of a GPR subscription.
 *
 * MPI_WIN_BASE is an address-valued attribute, and is set directly
 * from MPI_WIN_CREATE.  MPI_WIN_SIZE and MPI_WIN_DISP_UNIT are both
 * integer-valued attributes, *BUT* at least the MPI_WIN_SIZE is an
 * MPI_Aint, so in terms of consistency, both should be the same --
 * hence, we treat them as MPI-2 Fortran integer-valued attributes.
 * All three of these atrributes have NULL_COPY_FN copy functions; it
 * doesn't make sense to copy them to new windows (because they're
 * values specific and unique to each window) -- especially when
 * WIN_CREATE will explicitly set them on new windows anyway.
 *
 * These are not supported yet, but are included here for consistency:
 *
 * MPI_IMPI_CLIENT_SIZE, MPI_IMPI_CLIENT_COLOR, MPI_IMPI_HOST_SIZE,
 * and MPI_IMPI_HOST_COLOR are integer-valued attributes.
 */

#include "ompi_config.h"

#include <stdlib.h>

#include "mpi.h"

#include "ompi/attribute/attribute.h"

#include "ompi/errhandler/errcode.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/pml/pml.h"

/*
 * Private functions
 */
static int create_comm(int target_keyval, bool want_inherit);
static int free_comm(int keyval);

static int create_win(int target_keyval);
static int free_win(int keyval);

static int set_f(int keyval, MPI_Fint value);


int ompi_attr_create_predefined(void)
{
    int ret;
    char *univ_size;
    int usize;

    /* Create all the keyvals */

    /* DO NOT CHANGE THE ORDER OF CREATING THESE KEYVALS!  This order
       strictly adheres to the order in mpi.h.  If you change the
       order here, you must change the order in mpi.h as well! */

    if (OMPI_SUCCESS != (ret = create_comm(MPI_TAG_UB, true)) ||
        OMPI_SUCCESS != (ret = create_comm(MPI_HOST, true)) ||
        OMPI_SUCCESS != (ret = create_comm(MPI_IO, true)) ||
        OMPI_SUCCESS != (ret = create_comm(MPI_WTIME_IS_GLOBAL, true)) ||
        OMPI_SUCCESS != (ret = create_comm(MPI_APPNUM, true)) ||
        OMPI_SUCCESS != (ret = create_comm(MPI_LASTUSEDCODE, false)) ||
        OMPI_SUCCESS != (ret = create_comm(MPI_UNIVERSE_SIZE, true)) ||
        OMPI_SUCCESS != (ret = create_win(MPI_WIN_BASE)) ||
        OMPI_SUCCESS != (ret = create_win(MPI_WIN_SIZE)) ||
        OMPI_SUCCESS != (ret = create_win(MPI_WIN_DISP_UNIT)) ||
        OMPI_SUCCESS != (ret = create_win(MPI_WIN_CREATE_FLAVOR)) ||
        OMPI_SUCCESS != (ret = create_win(MPI_WIN_MODEL)) ||
#if 0
        /* JMS For when we implement IMPI */
        OMPI_SUCCESS != (ret = create_comm(IMPI_CLIENT_SIZE, true)) ||
        OMPI_SUCCESS != (ret = create_comm(IMPI_CLIENT_COLOR, true)) ||
        OMPI_SUCCESS != (ret = create_comm(IMPI_HOST_SIZE, true)) ||
        OMPI_SUCCESS != (ret = create_comm(IMPI_HOST_COLOR, true)) ||
#endif
        0) {
        return ret;
    }

    /* Set default values for everything except MPI_UNIVERSE_SIZE */

    if (OMPI_SUCCESS != (ret = set_f(MPI_TAG_UB, mca_pml.pml_max_tag)) ||
        OMPI_SUCCESS != (ret = set_f(MPI_HOST, MPI_PROC_NULL)) ||
        OMPI_SUCCESS != (ret = set_f(MPI_IO, MPI_ANY_SOURCE)) ||
        OMPI_SUCCESS != (ret = set_f(MPI_WTIME_IS_GLOBAL, 0)) ||
        OMPI_SUCCESS != (ret = set_f(MPI_LASTUSEDCODE,
                                     ompi_mpi_errcode_lastused)) ||
#if 0
        /* JMS For when we implement IMPI */
        OMPI_SUCCESS != (ret = set(IMPI_CLIENT_SIZE,
                                   &attr_impi_client_size)) ||
        OMPI_SUCCESS != (ret = set(IMPI_CLIENT_COLOR,
                                   &attr_impi_client_color)) ||
        OMPI_SUCCESS != (ret = set(IMPI_HOST_SIZE,
                                   &attr_impi_host_size)) ||
        OMPI_SUCCESS != (ret = set(IMPI_HOST_COLOR,
                                   &attr_impi_host_color)) ||
#endif
        0) {
        return ret;
    }

    /* If the universe size is set, then use it. Otherwise default
     * to the size of MPI_COMM_WORLD */
    univ_size = getenv("OMPI_UNIVERSE_SIZE");
    if (NULL == univ_size || (usize = strtol(univ_size, NULL, 0)) <= 0) {
        ret = set_f(MPI_UNIVERSE_SIZE, ompi_comm_size(MPI_COMM_WORLD));
    } else {
        ret = set_f(MPI_UNIVERSE_SIZE, usize);
    }
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    ret = set_f(MPI_APPNUM, ompi_process_info.app_num);

    return ret;
}


int ompi_attr_free_predefined(void)
{
    int ret;

    if (OMPI_SUCCESS != (ret = free_comm(MPI_TAG_UB)) ||
        OMPI_SUCCESS != (ret = free_comm(MPI_HOST)) ||
        OMPI_SUCCESS != (ret = free_comm(MPI_IO)) ||
        OMPI_SUCCESS != (ret = free_comm(MPI_WTIME_IS_GLOBAL)) ||
        OMPI_SUCCESS != (ret = free_comm(MPI_APPNUM)) ||
        OMPI_SUCCESS != (ret = free_comm(MPI_LASTUSEDCODE)) ||
        OMPI_SUCCESS != (ret = free_comm(MPI_UNIVERSE_SIZE)) ||
        OMPI_SUCCESS != (ret = free_win(MPI_WIN_BASE)) ||
        OMPI_SUCCESS != (ret = free_win(MPI_WIN_SIZE)) ||
        OMPI_SUCCESS != (ret = free_win(MPI_WIN_DISP_UNIT)) ||
        OMPI_SUCCESS != (ret = free_win(MPI_WIN_CREATE_FLAVOR)) ||
        OMPI_SUCCESS != (ret = free_win(MPI_WIN_MODEL)) ||
#if 0
        /* JMS For when we implement IMPI */
        OMPI_SUCCESS != (ret = free_comm(IMPI_CLIENT_SIZE)) ||
        OMPI_SUCCESS != (ret = free_comm(IMPI_CLIENT_COLOR)) ||
        OMPI_SUCCESS != (ret = free_comm(IMPI_HOST_SIZE)) ||
        OMPI_SUCCESS != (ret = free_comm(IMPI_HOST_COLOR)) ||
#endif
        0) {
        return ret;
    }
    return OMPI_SUCCESS;
}


static int create_comm(int target_keyval, bool want_inherit)
{
    int err;
    int keyval;
    ompi_attribute_fn_ptr_union_t copy;
    ompi_attribute_fn_ptr_union_t del;

    keyval = -1;
    copy.attr_communicator_copy_fn = (MPI_Comm_internal_copy_attr_function*)
        (want_inherit ? MPI_COMM_DUP_FN : MPI_COMM_NULL_COPY_FN);
    del.attr_communicator_delete_fn = MPI_COMM_NULL_DELETE_FN;
    err = ompi_attr_create_keyval(COMM_ATTR, copy, del,
                                  &keyval, NULL, OMPI_KEYVAL_PREDEFINED, NULL);
    if (MPI_SUCCESS != err) {
        return err;
    }
    if (target_keyval != keyval) {
        return OMPI_ERR_BAD_PARAM;
    }
    return OMPI_SUCCESS;
}


static int free_comm(int keyval)
{
  int key = keyval;
  return ompi_attr_free_keyval (COMM_ATTR, &key, true);
}


static int create_win(int target_keyval)
{
    int err;
    int keyval;
    ompi_attribute_fn_ptr_union_t copy;
    ompi_attribute_fn_ptr_union_t del;

    keyval = -1;
    copy.attr_win_copy_fn = (MPI_Win_internal_copy_attr_function*)MPI_WIN_NULL_COPY_FN;
    del.attr_win_delete_fn = MPI_WIN_NULL_DELETE_FN;
    err = ompi_attr_create_keyval(WIN_ATTR, copy, del,
                                  &keyval, NULL, OMPI_KEYVAL_PREDEFINED, NULL);
    if (MPI_SUCCESS != err) {
        return err;
    }
    if (target_keyval != keyval) {
        return OMPI_ERR_BAD_PARAM;
    }
    return OMPI_SUCCESS;
}


static int free_win(int keyval)
{
  int key = keyval;
  return ompi_attr_free_keyval (WIN_ATTR, &key, true);
}


static int set_f(int keyval, MPI_Fint value)
{
    return ompi_attr_set_fint(COMM_ATTR, MPI_COMM_WORLD,
                              &MPI_COMM_WORLD->c_keyhash,
                              keyval, value,
                              true);
}
