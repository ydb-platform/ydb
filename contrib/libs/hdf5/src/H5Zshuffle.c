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

#include "H5Zmodule.h" /* This source code file is part of the H5Z module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Pprivate.h"  /* Property lists                       */
#include "H5Tprivate.h"  /* Datatypes         			*/
#include "H5Zpkg.h"      /* Data filters				*/

/* Local function prototypes */
static herr_t H5Z__set_local_shuffle(hid_t dcpl_id, hid_t type_id, hid_t space_id);
static size_t H5Z__filter_shuffle(unsigned flags, size_t cd_nelmts, const unsigned cd_values[], size_t nbytes,
                                  size_t *buf_size, void **buf);

/* This message derives from H5Z */
const H5Z_class2_t H5Z_SHUFFLE[1] = {{
    H5Z_CLASS_T_VERS,       /* H5Z_class_t version */
    H5Z_FILTER_SHUFFLE,     /* Filter id number		*/
    1,                      /* encoder_present flag (set to true) */
    1,                      /* decoder_present flag (set to true) */
    "shuffle",              /* Filter name for debugging	*/
    NULL,                   /* The "can apply" callback     */
    H5Z__set_local_shuffle, /* The "set local" callback     */
    H5Z__filter_shuffle,    /* The actual filter function	*/
}};

/* Local macros */
#define H5Z_SHUFFLE_PARM_SIZE 0 /* "Local" parameter for shuffling size */

/*-------------------------------------------------------------------------
 * Function:	H5Z__set_local_shuffle
 *
 * Purpose:	Set the "local" dataset parameter for data shuffling to be
 *              the size of the datatype.
 *
 * Return:	Success: Non-negative
 *		Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__set_local_shuffle(hid_t dcpl_id, hid_t type_id, hid_t H5_ATTR_UNUSED space_id)
{
    H5P_genplist_t *dcpl_plist;                          /* Property list pointer */
    const H5T_t    *type;                                /* Datatype */
    unsigned        flags;                               /* Filter flags */
    size_t          cd_nelmts = H5Z_SHUFFLE_USER_NPARMS; /* Number of filter parameters */
    unsigned        cd_values[H5Z_SHUFFLE_TOTAL_NPARMS]; /* Filter parameters */
    herr_t          ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the plist structure */
    if (NULL == (dcpl_plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get datatype */
    if (NULL == (type = (const H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Get the filter's current parameters */
    if (H5P_get_filter_by_id(dcpl_plist, H5Z_FILTER_SHUFFLE, &flags, &cd_nelmts, cd_values, (size_t)0, NULL,
                             NULL) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't get shuffle parameters");

    /* Set "local" parameter for this dataset */
    if ((cd_values[H5Z_SHUFFLE_PARM_SIZE] = (unsigned)H5T_get_size(type)) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

    /* Modify the filter's parameters for this dataset */
    if (H5P_modify_filter(dcpl_plist, H5Z_FILTER_SHUFFLE, flags, (size_t)H5Z_SHUFFLE_TOTAL_NPARMS,
                          cd_values) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTSET, FAIL, "can't set local shuffle parameters");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__set_local_shuffle() */

/*-------------------------------------------------------------------------
 * Function:	H5Z__filter_shuffle
 *
 * Purpose:	Implement an I/O filter which "de-interlaces" a block of data
 *              by putting all the bytes in a byte-position for each element
 *              together in the block.  For example, for 4-byte elements stored
 *              as: 012301230123, shuffling will store them as: 000111222333
 *              Usually, the bytes in each byte position are more related to
 *              each other and putting them together will increase compression.
 *
 * Return:	Success: Size of buffer filtered
 *		Failure: 0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5Z__filter_shuffle(unsigned flags, size_t cd_nelmts, const unsigned cd_values[], size_t nbytes,
                    size_t *buf_size, void **buf)
{
    void          *dest  = NULL;  /* Buffer to deposit [un]shuffled bytes into */
    unsigned char *_src  = NULL;  /* Alias for source buffer */
    unsigned char *_dest = NULL;  /* Alias for destination buffer */
    unsigned       bytesoftype;   /* Number of bytes per element */
    size_t         numofelements; /* Number of elements in buffer */
    size_t         i;             /* Local index variables */
#ifdef NO_DUFFS_DEVICE
    size_t j;             /* Local index variable */
#endif                    /* NO_DUFFS_DEVICE */
    size_t leftover;      /* Extra bytes at end of buffer */
    size_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (cd_nelmts != H5Z_SHUFFLE_TOTAL_NPARMS || cd_values[H5Z_SHUFFLE_PARM_SIZE] == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid shuffle parameters");

    /* Get the number of bytes per element from the parameter block */
    bytesoftype = cd_values[H5Z_SHUFFLE_PARM_SIZE];

    /* Compute the number of elements in buffer */
    numofelements = nbytes / bytesoftype;

    /* Don't do anything for 1-byte elements, or "fractional" elements */
    if (bytesoftype > 1 && numofelements > 1) {
        /* Compute the leftover bytes if there are any */
        leftover = nbytes % bytesoftype;

        /* Allocate the destination buffer */
        if (NULL == (dest = H5MM_malloc(nbytes)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0, "memory allocation failed for shuffle buffer");

        if (flags & H5Z_FLAG_REVERSE) {
            /* Get the pointer to the source buffer */
            _src = (unsigned char *)(*buf);

            /* Input; unshuffle */
            for (i = 0; i < bytesoftype; i++) {
                _dest = ((unsigned char *)dest) + i;
#define DUFF_GUTS                                                                                            \
    *_dest = *_src++;                                                                                        \
    _dest += bytesoftype;
#ifdef NO_DUFFS_DEVICE
                j = numofelements;
                while (j > 0) {
                    DUFF_GUTS;

                    j--;
                } /* end for */
#else             /* NO_DUFFS_DEVICE */
                {
                    size_t duffs_index; /* Counting index for Duff's device */

                    duffs_index = (numofelements + 7) / 8;
                    switch (numofelements % 8) {
                        default:
                            assert(0 && "This Should never be executed!");
                            break;
                        case 0:
                            do {
                                DUFF_GUTS
                                /* FALLTHROUGH */
                                H5_ATTR_FALLTHROUGH
                                case 7:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 6:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 5:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 4:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 3:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 2:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 1:
                                    DUFF_GUTS
                            } while (--duffs_index > 0);
                    } /* end switch */
                }
#endif            /* NO_DUFFS_DEVICE */
#undef DUFF_GUTS
            } /* end for */

            /* Add leftover to the end of data */
            if (leftover > 0) {
                /* Adjust back to end of shuffled bytes */
                _dest -= (bytesoftype - 1); /*lint !e794 _dest is initialized */
                H5MM_memcpy((void *)_dest, (void *)_src, leftover);
            }
        } /* end if */
        else {
            /* Get the pointer to the destination buffer */
            _dest = (unsigned char *)dest;

            /* Output; shuffle */
            for (i = 0; i < bytesoftype; i++) {
                _src = ((unsigned char *)(*buf)) + i;
#define DUFF_GUTS                                                                                            \
    *_dest++ = *_src;                                                                                        \
    _src += bytesoftype;
#ifdef NO_DUFFS_DEVICE
                j = numofelements;
                while (j > 0) {
                    DUFF_GUTS;

                    j--;
                } /* end for */
#else             /* NO_DUFFS_DEVICE */
                {
                    size_t duffs_index; /* Counting index for Duff's device */

                    duffs_index = (numofelements + 7) / 8;
                    switch (numofelements % 8) {
                        default:
                            assert(0 && "This Should never be executed!");
                            break;
                        case 0:
                            do {
                                DUFF_GUTS
                                /* FALLTHROUGH */
                                H5_ATTR_FALLTHROUGH
                                case 7:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 6:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 5:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 4:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 3:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 2:
                                    DUFF_GUTS
                                    /* FALLTHROUGH */
                                    H5_ATTR_FALLTHROUGH
                                case 1:
                                    DUFF_GUTS
                            } while (--duffs_index > 0);
                    } /* end switch */
                }
#endif            /* NO_DUFFS_DEVICE */
#undef DUFF_GUTS
            } /* end for */

            /* Add leftover to the end of data */
            if (leftover > 0) {
                /* Adjust back to end of shuffled bytes */
                _src -= (bytesoftype - 1); /*lint !e794 _src is initialized */
                H5MM_memcpy((void *)_dest, (void *)_src, leftover);
            }
        } /* end else */

        /* Free the input buffer */
        H5MM_xfree(*buf);

        /* Set the buffer information to return */
        *buf      = dest;
        *buf_size = nbytes;
    } /* end else */

    /* Set the return value */
    ret_value = nbytes;

done:
    FUNC_LEAVE_NOAPI(ret_value)
}
