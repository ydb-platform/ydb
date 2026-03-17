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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/dss/dss_internal.h"

int opal_dss_peek(opal_buffer_t *buffer, opal_data_type_t *type,
                  int32_t *num_vals)
{
    int ret;
    opal_buffer_t tmp;
    int32_t n=1;
    opal_data_type_t local_type;

    /* check for errors */
    if (buffer == NULL) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Double check and ensure that there is data left in the buffer. */

    if (buffer->unpack_ptr >= buffer->base_ptr + buffer->bytes_used) {
        *type = OPAL_NULL;
        *num_vals = 0;
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* if this is NOT a fully described buffer, then that is as much as
     * we can do - there is no way we can tell the caller what type is
     * in the buffer since that info wasn't stored.
     */
    if (OPAL_DSS_BUFFER_FULLY_DESC != buffer->type) {
        *type = OPAL_UNDEF;
        *num_vals = 0;
        return OPAL_ERR_UNKNOWN_DATA_TYPE;
    }

    /* cheat: unpack from a copy of the buffer -- leaving all the
       original pointers intact */
    tmp = *buffer;

    if (OPAL_SUCCESS != (ret = opal_dss_get_data_type(&tmp, &local_type))) {
        *type = OPAL_NULL;
        *num_vals = 0;
        return ret;
    }
    if (OPAL_INT32 != local_type) { /* if the length wasn't first, then error */
        *type = OPAL_NULL;
        *num_vals = 0;
        return OPAL_ERR_UNPACK_FAILURE;
    }
    if (OPAL_SUCCESS != (ret = opal_dss_unpack_int32(&tmp, num_vals, &n, OPAL_INT32))) {
        *type = OPAL_NULL;
        *num_vals = 0;
        return ret;
    }
    if (OPAL_SUCCESS != (ret = opal_dss_get_data_type(&tmp, type))) {
        *type = OPAL_NULL;
        *num_vals = 0;
    }

    return ret;
}

int opal_dss_peek_type(opal_buffer_t *buffer, opal_data_type_t *type)
{
    int ret;
    opal_buffer_t tmp;

    /* check for errors */
    if (buffer == NULL) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* if this is NOT a fully described buffer, then there isn't anything
     * we can do - there is no way we can tell the caller what type is
     * in the buffer since that info wasn't stored.
     */
    if (OPAL_DSS_BUFFER_FULLY_DESC != buffer->type) {
        *type = OPAL_UNDEF;
        return OPAL_ERR_UNKNOWN_DATA_TYPE;
    }
    /* Double check and ensure that there is data left in the buffer. */

    if (buffer->unpack_ptr >= buffer->base_ptr + buffer->bytes_used) {
        *type = OPAL_UNDEF;
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* cheat: unpack from a copy of the buffer -- leaving all the
    original pointers intact */
    tmp = *buffer;

    if (OPAL_SUCCESS != (ret = opal_dss_get_data_type(&tmp, type))) {
        *type = OPAL_UNDEF;
        return ret;
    }

    return OPAL_SUCCESS;
}
