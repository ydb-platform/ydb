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
 * Purpose:         The object modification time message
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/

static void  *H5O__mtime_new_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                    size_t p_size, const uint8_t *p);
static herr_t H5O__mtime_new_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static size_t H5O__mtime_new_size(const H5F_t *f, bool disable_shared, const void *_mesg);

static void  *H5O__mtime_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                size_t p_size, const uint8_t *p);
static herr_t H5O__mtime_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__mtime_copy(const void *_mesg, void *_dest);
static size_t H5O__mtime_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__mtime_free(void *_mesg);
static herr_t H5O__mtime_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_MTIME[1] = {{
    H5O_MTIME_ID,      /*message id number		*/
    "mtime",           /*message name for debugging	*/
    sizeof(time_t),    /*native message size		*/
    0,                 /* messages are shareable?       */
    H5O__mtime_decode, /*decode message		*/
    H5O__mtime_encode, /*encode message		*/
    H5O__mtime_copy,   /*copy the native value		*/
    H5O__mtime_size,   /*raw message size		*/
    NULL,              /* reset method			*/
    H5O__mtime_free,   /* free method			*/
    NULL,              /* file delete method		*/
    NULL,              /* link method			*/
    NULL,              /*set share method		*/
    NULL,              /*can share method		*/
    NULL,              /* pre copy native value to file */
    NULL,              /* copy native value to file    */
    NULL,              /* post copy native value to file    */
    NULL,              /* get creation index		*/
    NULL,              /* set creation index		*/
    H5O__mtime_debug   /*debug the message		*/
}};

/* This message derives from H5O message class */
/* (Only encode, decode & size routines are different from old mtime routines) */
const H5O_msg_class_t H5O_MSG_MTIME_NEW[1] = {{
    H5O_MTIME_NEW_ID,      /*message id number		*/
    "mtime_new",           /*message name for debugging	*/
    sizeof(time_t),        /*native message size		*/
    0,                     /* messages are shareable?       */
    H5O__mtime_new_decode, /*decode message		*/
    H5O__mtime_new_encode, /*encode message		*/
    H5O__mtime_copy,       /*copy the native value		*/
    H5O__mtime_new_size,   /*raw message size		*/
    NULL,                  /* reset method			*/
    H5O__mtime_free,       /* free method			*/
    NULL,                  /* file delete method		*/
    NULL,                  /* link method			*/
    NULL,                  /*set share method		*/
    NULL,                  /*can share method		*/
    NULL,                  /* pre copy native value to file */
    NULL,                  /* copy native value to file    */
    NULL,                  /* post copy native value to file    */
    NULL,                  /* get creation index		*/
    NULL,                  /* set creation index		*/
    H5O__mtime_debug       /*debug the message		*/
}};

/* Current version of new mtime information */
#define H5O_MTIME_VERSION 1

/* Declare a free list to manage the time_t struct */
H5FL_DEFINE(time_t);

/*-------------------------------------------------------------------------
 * Function:    H5O__mtime_new_decode
 *
 * Purpose:     Decode a new modification time message and return a pointer to
 *              a new time_t value.
 *
 *              This version of the modification time was used in HDF5
 *              1.6.1 and later.
 *
 *              The new modification time message format was added due to the
 *              performance overhead of the old format.
 *
 * Return:      Success:    Pointer to new message in native struct
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__mtime_new_decode(H5F_t H5_ATTR_NDEBUG_UNUSED *f, H5O_t H5_ATTR_UNUSED *open_oh,
                      unsigned H5_ATTR_UNUSED mesg_flags, unsigned H5_ATTR_UNUSED *ioflags, size_t p_size,
                      const uint8_t *p)
{
    const uint8_t *p_end = p + p_size - 1; /* End of input buffer */
    time_t        *mesg  = NULL;
    uint32_t       tmp_time;         /* Temporary copy of the time */
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    if (*p++ != H5O_MTIME_VERSION)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for mtime message");

    /* Skip reserved bytes */
    if (H5_IS_BUFFER_OVERFLOW(p, 3, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    p += 3;

    /* Get the time_t from the file */
    if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    UINT32DECODE(p, tmp_time);

    /* The return value */
    if (NULL == (mesg = H5FL_MALLOC(time_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    *mesg = (time_t)tmp_time;

    /* Set return value */
    ret_value = mesg;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__mtime_new_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_decode
 *
 * Purpose:     Decode a modification time message and return a pointer to a
 *              new time_t value.
 *
 *              This version of the modification time was used in HDF5
 *              1.6.0 and earlier.
 *
 *              The new modification time message format was added due to the
 *              performance overhead of the old format.
 *
 * Return:      Success:    Pointer to new message in native struct
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__mtime_decode(H5F_t H5_ATTR_NDEBUG_UNUSED *f, H5O_t H5_ATTR_UNUSED *open_oh,
                  unsigned H5_ATTR_UNUSED mesg_flags, unsigned H5_ATTR_UNUSED *ioflags, size_t p_size,
                  const uint8_t *p)
{
    const uint8_t *p_end = p + p_size - 1; /* End of input buffer */
    time_t        *mesg  = NULL;
    time_t         the_time;
    struct tm      tm;
    void          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    /* Buffer should have 14 message bytes and 2 reserved bytes */
    if (H5_IS_BUFFER_OVERFLOW(p, 16, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    for (int i = 0; i < 14; i++)
        if (!isdigit(p[i]))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "badly formatted modification time message");

    /* Convert YYYYMMDDhhmmss UTC to a time_t. */
    memset(&tm, 0, sizeof tm);
    tm.tm_year  = (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0') - 1900;
    tm.tm_mon   = (p[4] - '0') * 10 + (p[5] - '0') - 1;
    tm.tm_mday  = (p[6] - '0') * 10 + (p[7] - '0');
    tm.tm_hour  = (p[8] - '0') * 10 + (p[9] - '0');
    tm.tm_min   = (p[10] - '0') * 10 + (p[11] - '0');
    tm.tm_sec   = (p[12] - '0') * 10 + (p[13] - '0');
    tm.tm_isdst = -1; /* (figure it out) */
    if ((time_t)-1 == (the_time = H5_make_time(&tm)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "can't construct time info");

    /* The return value */
    if (NULL == (mesg = H5FL_MALLOC(time_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    *mesg = the_time;

    /* Set return value */
    ret_value = mesg;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__mtime_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_new_encode
 *
 * Purpose:	Encodes a new modification time message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mtime_new_encode(H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p,
                      const void *_mesg)
{
    const time_t *mesg = (const time_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(mesg);

    /* Version */
    *p++ = H5O_MTIME_VERSION;

    /* Reserved bytes */
    *p++ = 0;
    *p++ = 0;
    *p++ = 0;

    /* Encode time */
    UINT32ENCODE(p, *mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__mtime_new_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_encode
 *
 * Purpose:	Encodes a modification time message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mtime_encode(H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const time_t *mesg = (const time_t *)_mesg;
    struct tm    *tm;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(mesg);

    /* encode */
    tm = HDgmtime(mesg);
    sprintf((char *)p, "%04d%02d%02d%02d%02d%02d", 1900 + tm->tm_year, 1 + tm->tm_mon, tm->tm_mday,
            tm->tm_hour, tm->tm_min, tm->tm_sec);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__mtime_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_copy
 *
 * Purpose:	Copies a message from _MESG to _DEST, allocating _DEST if
 *		necessary.
 *
 * Return:	Success:	Ptr to _DEST
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__mtime_copy(const void *_mesg, void *_dest)
{
    const time_t *mesg      = (const time_t *)_mesg;
    time_t       *dest      = (time_t *)_dest;
    void         *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(mesg);
    if (!dest && NULL == (dest = H5FL_MALLOC(time_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* copy */
    *dest = *mesg;

    /* Set return value */
    ret_value = dest;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__mtime_copy() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_new_size
 *
 * Purpose:	Returns the size of the raw message in bytes not
 *		counting the message type or size fields, but only the data
 *		fields.	 This function doesn't take into account
 *		alignment.
 *
 * Return:	Success:	Message data size in bytes w/o alignment.
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__mtime_new_size(const H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared,
                    const void H5_ATTR_UNUSED *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);

    FUNC_LEAVE_NOAPI(8)
} /* end H5O__mtime_new_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_size
 *
 * Purpose:	Returns the size of the raw message in bytes not
 *		counting the message type or size fields, but only the data
 *		fields.	 This function doesn't take into account
 *		alignment.
 *
 * Return:	Success:	Message data size in bytes w/o alignment.
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__mtime_size(const H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared,
                const void H5_ATTR_UNUSED *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);

    FUNC_LEAVE_NOAPI(16)
} /* end H5O__mtime_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_free
 *
 * Purpose:	Frees the message
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mtime_free(void *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(mesg);

    mesg = H5FL_FREE(time_t, mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__mtime_free() */

/*-------------------------------------------------------------------------
 * Function:	H5O__mtime_debug
 *
 * Purpose:	Prints debugging info for the message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mtime_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const time_t *mesg = (const time_t *)_mesg;
    struct tm    *tm;
    char          buf[128];

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* debug */
    tm = HDlocaltime(mesg);

    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S %Z", tm);
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Time:", buf);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__mtime_debug() */
