/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stddef.h>

#include "opal/constants.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_datatype_internal.h"
#include "opal/datatype/opal_convertor.h"

int32_t opal_datatype_is_monotonic(opal_datatype_t* type )
{
    opal_convertor_t *pConv;
    uint32_t iov_count;
    struct iovec iov[5];
    size_t max_data = 0;
    long prev = -1;
    int rc;
    bool monotonic = true;

    pConv  = opal_convertor_create( opal_local_arch, 0 );
    if (OPAL_UNLIKELY(NULL == pConv)) {
        return 0;
    }
    rc = opal_convertor_prepare_for_send( pConv, type, 1, NULL );
    if( OPAL_UNLIKELY(OPAL_SUCCESS != rc)) {
        OBJ_RELEASE(pConv);
        return 0;
    }

    do {
        iov_count = 5;
        rc = opal_convertor_raw( pConv, iov, &iov_count, &max_data);
        for (uint32_t i=0; i<iov_count; i++) {
            if ((long)iov[i].iov_base < prev) {
                monotonic = false;
                goto cleanup;
            }
            prev = (long)iov[i].iov_base;
        }
    } while (rc != 1);

  cleanup:
    OBJ_RELEASE( pConv );

    return monotonic;
}
