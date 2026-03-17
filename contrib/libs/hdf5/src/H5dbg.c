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
 * Created:         H5dbg.c
 *
 * Purpose:         Generic debugging routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/
#include "H5module.h" /* This source code file is part of the H5 module */

/***********/
/* Headers */
/***********/
#include "H5private.h" /* Generic Functions            */

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
 * Function:    H5_buffer_dump
 *
 * Purpose:     Dumps a buffer of memory in an octal dump form
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_buffer_dump(FILE *stream, int indent, const uint8_t *buf, const uint8_t *marker, size_t buf_offset,
               size_t buf_size)
{
    size_t u, v; /* Local index variable */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(stream);
    assert(indent >= 0);
    assert(buf);
    assert(marker);
    assert(buf_size > 0);

    /*
     * Print the buffer in a VMS-style octal dump.
     */
    fprintf(stream, "%*sData follows (`__' indicates free region)...\n", indent, "");
    for (u = 0; u < buf_size; u += 16) {
        uint8_t c;

        fprintf(stream, "%*s %8zu: ", indent, "", u + buf_offset);

        /* Print the hex values */
        for (v = 0; v < 16; v++) {
            if (u + v < buf_size) {
                if (marker[u + v])
                    fprintf(stream, "__ ");
                else {
                    c = buf[buf_offset + u + v];
                    fprintf(stream, "%02x ", c);
                } /* end else */
            }     /* end if */
            else
                fprintf(stream, "   ");

            if (7 == v)
                fputc(' ', stream);
        } /* end for */
        fputc(' ', stream);

        /* Print the character values */
        for (v = 0; v < 16; v++) {
            if (u + v < buf_size) {
                if (marker[u + v])
                    fputc(' ', stream);
                else {
                    c = buf[buf_offset + u + v];

                    if (isprint(c))
                        fputc(c, stream);
                    else
                        fputc('.', stream);
                } /* end else */
            }     /* end if */

            if (7 == v)
                fputc(' ', stream);
        } /* end for */

        fputc('\n', stream);
    } /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5_buffer_dump() */
