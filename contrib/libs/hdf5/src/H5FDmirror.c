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
 * Purpose: Transmit write-only operations to a receiver/writer process on
 *          a remote host.
 */

#include "H5private.h" /* Generic Functions        */

#ifdef H5_HAVE_MIRROR_VFD

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5Eprivate.h"      /* Error handling           */
#include "H5Fprivate.h"      /* File access              */
#include "H5FDprivate.h"     /* File drivers             */
#include "H5FDmirror.h"      /* "Mirror" definitions     */
#error #include "H5FDmirror_priv.h" /* Private header for the mirror VFD */
#include "H5FLprivate.h"     /* Free Lists               */
#include "H5Iprivate.h"      /* IDs                      */
#include "H5MMprivate.h"     /* Memory management        */
#include "H5Pprivate.h"      /* Property lists           */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_MIRROR_g = 0;

/* Virtual file structure for a Mirror Driver */
typedef struct H5FD_mirror_t {
    H5FD_t             pub;     /* Public stuff, must be first            */
    H5FD_mirror_fapl_t fa;      /* Configuration structure                */
    haddr_t            eoa;     /* End of allocated region                */
    haddr_t            eof;     /* End of file; current file size         */
    int                sock_fd; /* Handle of socket to remote operator    */
    H5FD_mirror_xmit_t xmit;    /* Primary communication header           */
    uint32_t           xmit_i;  /* Counter of transmission sent and rec'd */
} H5FD_mirror_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely by the second
 *                  argument of the file seek function.
 */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(HDoff_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))

#ifndef BSWAP_64
#define BSWAP_64(X)                                                                                          \
    (uint64_t)((((X)&0x00000000000000FF) << 56) | (((X)&0x000000000000FF00) << 40) |                         \
               (((X)&0x0000000000FF0000) << 24) | (((X)&0x00000000FF000000) << 8) |                          \
               (((X)&0x000000FF00000000) >> 8) | (((X)&0x0000FF0000000000) >> 24) |                          \
               (((X)&0x00FF000000000000) >> 40) | (((X)&0xFF00000000000000) >> 56))
#endif /* BSWAP_64 */

/* Debugging flabs for verbose tracing -- nonzero to enable */
#define MIRROR_DEBUG_OP_CALLS   0
#define MIRROR_DEBUG_XMIT_BYTES 0

#if MIRROR_DEBUG_XMIT_BYTES
#define LOG_XMIT_BYTES(label, buf, len)                                                                      \
    do {                                                                                                     \
        ssize_t              bytes_written = 0;                                                              \
        const unsigned char *b             = NULL;                                                           \
                                                                                                             \
        fprintf(stdout, "%s bytes:\n```\n", (label));                                                        \
                                                                                                             \
        /* print whole lines */                                                                              \
        while ((len - bytes_written) >= 32) {                                                                \
            b = (const unsigned char *)(buf) + bytes_written;                                                \
            fprintf(stdout,                                                                                  \
                    "%04zX  %02X%02X%02X%02X %02X%02X%02X%02X"                                               \
                    " %02X%02X%02X%02X %02X%02X%02X%02X"                                                     \
                    " %02X%02X%02X%02X %02X%02X%02X%02X"                                                     \
                    " %02X%02X%02X%02X %02X%02X%02X%02X\n",                                                  \
                    bytes_written, b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], \
                    b[12], b[13], b[14], b[15], b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23],      \
                    b[24], b[25], b[26], b[27], b[28], b[29], b[30], b[31]);                                 \
            bytes_written += 32;                                                                             \
        }                                                                                                    \
                                                                                                             \
        /* start partial line */                                                                             \
        if (len > bytes_written) {                                                                           \
            fprintf(stdout, "%04zX ", bytes_written);                                                        \
        }                                                                                                    \
                                                                                                             \
        /* partial line blocks */                                                                            \
        while ((len - bytes_written) >= 4) {                                                                 \
            fprintf(stdout, " %02X%02X%02X%02X", (buf)[bytes_written], (buf)[bytes_written + 1],             \
                    (buf)[bytes_written + 2], (buf)[bytes_written + 3]);                                     \
            bytes_written += 4;                                                                              \
        }                                                                                                    \
                                                                                                             \
        /* block separator before partial block */                                                           \
        if (len > bytes_written) {                                                                           \
            fprintf(stdout, " ");                                                                            \
        }                                                                                                    \
                                                                                                             \
        /* partial block individual bytes */                                                                 \
        while (len > bytes_written) {                                                                        \
            fprintf(stdout, "%02X", (buf)[bytes_written++]);                                                 \
        }                                                                                                    \
                                                                                                             \
        /* end partial line */                                                                               \
        fprintf(stdout, "\n");                                                                               \
        fprintf(stdout, "```\n");                                                                            \
        fflush(stdout);                                                                                      \
    } while (0)
#else
#define LOG_XMIT_BYTES(label, buf, len) /* no-op */
#endif                                  /* MIRROR_DEBUG_XMIT_BYTE */

#if MIRROR_DEBUG_OP_CALLS
#define LOG_OP_CALL(name)                                                                                    \
    do {                                                                                                     \
        printf("called %s()\n", (name));                                                                     \
        fflush(stdout);                                                                                      \
    } while (0)
#else
#define LOG_OP_CALL(name) /* no-op */
#endif                    /* MIRROR_DEBUG_OP_CALLS */

/* Prototypes */
static herr_t  H5FD__mirror_term(void);
static void   *H5FD__mirror_fapl_get(H5FD_t *_file);
static void   *H5FD__mirror_fapl_copy(const void *_old_fa);
static herr_t  H5FD__mirror_fapl_free(void *_fa);
static haddr_t H5FD__mirror_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__mirror_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__mirror_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static H5FD_t *H5FD__mirror_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__mirror_close(H5FD_t *_file);
static herr_t  H5FD__mirror_query(const H5FD_t *_file, unsigned long *flags);
static herr_t  H5FD__mirror_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                  const void *buf);
static herr_t  H5FD__mirror_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                 void *buf);
static herr_t  H5FD__mirror_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__mirror_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD__mirror_unlock(H5FD_t *_file);

static herr_t H5FD__mirror_verify_reply(H5FD_mirror_t *file);

static const H5FD_class_t H5FD_mirror_g = {
    H5FD_CLASS_VERSION,     /* struct version       */
    H5FD_MIRROR_VALUE,      /* value                */
    "mirror",               /* name                 */
    MAXADDR,                /* maxaddr              */
    H5F_CLOSE_WEAK,         /* fc_degree            */
    H5FD__mirror_term,      /* terminate            */
    NULL,                   /* sb_size              */
    NULL,                   /* sb_encode            */
    NULL,                   /* sb_decode            */
    0,                      /* fapl_size            */
    H5FD__mirror_fapl_get,  /* fapl_get             */
    H5FD__mirror_fapl_copy, /* fapl_copy            */
    H5FD__mirror_fapl_free, /* fapl_free            */
    0,                      /* dxpl_size            */
    NULL,                   /* dxpl_copy            */
    NULL,                   /* dxpl_free            */
    H5FD__mirror_open,      /* open                 */
    H5FD__mirror_close,     /* close                */
    NULL,                   /* cmp                  */
    H5FD__mirror_query,     /* query                */
    NULL,                   /* get_type_map         */
    NULL,                   /* alloc                */
    NULL,                   /* free                 */
    H5FD__mirror_get_eoa,   /* get_eoa              */
    H5FD__mirror_set_eoa,   /* set_eoa              */
    H5FD__mirror_get_eof,   /* get_eof              */
    NULL,                   /* get_handle           */
    H5FD__mirror_read,      /* read                 */
    H5FD__mirror_write,     /* write                */
    NULL,                   /* read_vector          */
    NULL,                   /* write_vector         */
    NULL,                   /* read_selection       */
    NULL,                   /* write_selection      */
    NULL,                   /* flush                */
    H5FD__mirror_truncate,  /* truncate             */
    H5FD__mirror_lock,      /* lock                 */
    H5FD__mirror_unlock,    /* unlock               */
    NULL,                   /* del                  */
    NULL,                   /* ctl                  */
    H5FD_FLMAP_DICHOTOMY    /* fl_map               */
};

/* Declare a free list to manage the transmission buffers */
H5FL_BLK_DEFINE_STATIC(xmit);

/* Declare a free list to manage the H5FD_mirror_t struct */
H5FL_DEFINE_STATIC(H5FD_mirror_t);

/* Declare a free list to manage the H5FD_mirror_xmit_open_t struct */
H5FL_DEFINE_STATIC(H5FD_mirror_xmit_open_t);

/* -------------------------------------------------------------------------
 * Function:    H5FD_mirror_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the mirror driver.
 *              Failure:    Negative
 * -------------------------------------------------------------------------
 */
hid_t
H5FD_mirror_init(void)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    LOG_OP_CALL(__func__);

    if (H5I_VFL != H5I_get_type(H5FD_MIRROR_g)) {
        H5FD_MIRROR_g = H5FD_register(&H5FD_mirror_g, sizeof(H5FD_class_t), false);
        if (H5I_INVALID_HID == H5FD_MIRROR_g)
            HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register mirror");
    }
    ret_value = H5FD_MIRROR_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_mirror_init() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 * ---------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VFL ID */
    H5FD_MIRROR_g = 0;

    LOG_OP_CALL(__func__);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__mirror_term() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_decode_uint16
 *
 * Purpose:     Extract a 16-bit integer in "network" (Big-Endian) word order
 *              from the byte-buffer and return it with the local word order at
 *              the destination pointer.
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 * Return:      The number of bytes read from the buffer (2).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_decode_uint16(uint16_t *out, const unsigned char *_buf)
{
    uint16_t n = 0;

    LOG_OP_CALL(__func__);

    assert(_buf && out);

    H5MM_memcpy(&n, _buf, sizeof(n));
    *out = (uint16_t)ntohs(n);

    return 2; /* number of bytes eaten */
} /* end H5FD__mirror_xmit_decode_uint16() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_decode_uint32
 *
 * Purpose:     Extract a 32-bit integer in "network" (Big-Endian) word order
 *              from the byte-buffer and return it with the local word order at
 *              the destination pointer.
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 * Return:      The number of bytes read from the buffer (4).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_decode_uint32(uint32_t *out, const unsigned char *_buf)
{
    uint32_t n = 0;

    LOG_OP_CALL(__func__);

    assert(_buf && out);

    H5MM_memcpy(&n, _buf, sizeof(n));
    *out = (uint32_t)ntohl(n);

    return 4; /* number of bytes eaten */
} /* end H5FD__mirror_xmit_decode_uint32() */

/* ---------------------------------------------------------------------------
 * Function:    is_host_little_endian
 *
 * Purpose:     Determine whether the host machine is little-endian.
 *
 *              Store an integer with a known value, re-map the memory to a
 *              character array, and inspect the array's contents.
 *
 * Return:      The number of bytes written to the buffer (8).
 *
 * ---------------------------------------------------------------------------
 */
static bool
is_host_little_endian(void)
{
    union {
        uint32_t u32;
        uint8_t  u8[4];
    } echeck;
    echeck.u32 = 0xA1B2C3D4;

    if (echeck.u8[0] == 0xD4)
        return true;
    else
        return false;
} /* end is_host_little_endian() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_decode_uint64
 *
 * Purpose:     Extract a 64-bit integer in "network" (Big-Endian) word order
 *              from the byte-buffer and return it with the local word order.
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 *              WARNING: Does not accommodate other forms of endianness,
 *              e.g. "middle-endian".
 *
 * Return:      The number of bytes written to the buffer (8).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_decode_uint64(uint64_t *out, const unsigned char *_buf)
{
    uint64_t n = 0;

    LOG_OP_CALL(__func__);

    assert(_buf && out);

    H5MM_memcpy(&n, _buf, sizeof(n));
    if (true == is_host_little_endian())
        *out = BSWAP_64(n);
    else
        *out = n;

    return 8;
} /* end H5FD__mirror_xmit_decode_uint64() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_decode_uint8
 *
 * Purpose:     Extract a 8-bit integer in "network" (Big-Endian) word order
 *              from the byte-buffer and return it with the local word order at
 *              the destination pointer.
 *              (yes, it's one byte).
 *
 * Return:      The number of bytes read from the buffer (1).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_decode_uint8(uint8_t *out, const unsigned char *_buf)
{
    LOG_OP_CALL(__func__);

    assert(_buf && out);

    H5MM_memcpy(out, _buf, sizeof(uint8_t));

    return 1; /* number of bytes eaten */
} /* end H5FD__mirror_xmit_decode_uint8() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_encode_uint16
 *
 * Purpose:     Encode a 16-bit integer in "network" (Big-Endian) word order
 *              in place in the destination bytes-buffer.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes written to the buffer (2).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_encode_uint16(unsigned char *_dest, uint16_t v)
{
    uint16_t n = 0;

    LOG_OP_CALL(__func__);

    assert(_dest);

    n = (uint16_t)htons(v);
    H5MM_memcpy(_dest, &n, sizeof(n));

    return 2;
} /* end H5FD__mirror_xmit_encode_uint16() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_encode_uint32
 *
 * Purpose:     Encode a 32-bit integer in "network" (Big-Endian) word order
 *              in place in the destination bytes-buffer.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes written to the buffer (4).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_encode_uint32(unsigned char *_dest, uint32_t v)
{
    uint32_t n = 0;

    LOG_OP_CALL(__func__);

    assert(_dest);

    n = (uint32_t)htonl(v);
    H5MM_memcpy(_dest, &n, sizeof(n));

    return 4;
} /* end H5FD__mirror_xmit_encode_uint32() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_encode_uint64
 *
 * Purpose:     Encode a 64-bit integer in "network" (Big-Endian) word order
 *              in place in the destination bytes-buffer.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes written to the buffer (8).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_encode_uint64(unsigned char *_dest, uint64_t v)
{
    uint64_t n = v;

    LOG_OP_CALL(__func__);

    assert(_dest);

    if (true == is_host_little_endian())
        n = BSWAP_64(v);
    H5MM_memcpy(_dest, &n, sizeof(n));

    return 8;
} /* H5FD__mirror_xmit_encode_uint64() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD__mirror_xmit_encode_uint8
 *
 * Purpose:     Encode a 8-bit integer in "network" (Big-Endian) word order
 *              in place in the destination bytes-buffer.
 *              (yes, it's one byte).
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes read from the buffer (1).
 * ---------------------------------------------------------------------------
 */
size_t
H5FD__mirror_xmit_encode_uint8(unsigned char *dest, uint8_t v)
{
    LOG_OP_CALL(__func__);

    assert(dest);

    H5MM_memcpy(dest, &v, sizeof(v));

    return 1;
} /* end H5FD__mirror_xmit_encode_uint8() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_decode_header
 *
 * Purpose:     Extract a mirror_xmit_t "header" from the bytes-buffer.
 *
 *              Fields will be lifted from the buffer and stored in the
 *              target structure, using in the correct location (different
 *              systems may insert different padding between components) and
 *              word order (Big- vs Little-Endian).
 *
 *              The resulting structure should be sanity-checked with
 *              H5FD_mirror_xmit_is_xmit() before use.
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 * Return:      The number of bytes consumed from the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_decode_header(H5FD_mirror_xmit_t *out, const unsigned char *buf)
{
    size_t n_eaten = 0;

    LOG_OP_CALL(__func__);

    assert(out && buf);

    n_eaten += H5FD__mirror_xmit_decode_uint32(&(out->magic), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint8(&(out->version), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint32(&(out->session_token), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint32(&(out->xmit_count), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint8(&(out->op), &buf[n_eaten]);
    assert(n_eaten == H5FD_MIRROR_XMIT_HEADER_SIZE);

    return n_eaten;
} /* end H5FD_mirror_xmit_decode_header() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_decode_lock
 *
 * Purpose:     Extract a mirror_xmit_lock_t from the bytes-buffer.
 *
 *              Fields will be lifted from the buffer and stored in the
 *              target structure, using in the correct location (different
 *              systems may insert different padding between components) and
 *              word order (Big- vs Little-Endian).
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 *              The resulting structure should be sanity-checked with
 *              H5FD_mirror_xmit_is_lock() before use.
 *
 * Return:      The number of bytes consumed from the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_decode_lock(H5FD_mirror_xmit_lock_t *out, const unsigned char *buf)
{
    size_t n_eaten = 0;

    LOG_OP_CALL(__func__);

    assert(out && buf);

    n_eaten += H5FD_mirror_xmit_decode_header(&(out->pub), buf);
    n_eaten += H5FD__mirror_xmit_decode_uint64(&(out->rw), &buf[n_eaten]);
    assert(n_eaten == H5FD_MIRROR_XMIT_LOCK_SIZE);

    return n_eaten;
} /* end H5FD_mirror_xmit_decode_lock() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_decode_open
 *
 * Purpose:     Extract a mirror_xmit_open_t from the bytes-buffer.
 *
 *              Fields will be lifted from the buffer and stored in the
 *              target structure, using in the correct location (different
 *              systems may insert different padding between components) and
 *              word order (Big- vs Little-Endian).
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 *              The resulting structure should be sanity-checked with
 *              H5FD_mirror_xmit_is_open() before use.
 *
 * Return:      The maximum number of bytes that this decoding operation might
 *              have consumed from the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_decode_open(H5FD_mirror_xmit_open_t *out, const unsigned char *buf)
{
    size_t n_eaten = 0;

    LOG_OP_CALL(__func__);

    assert(out && buf);

    n_eaten += H5FD_mirror_xmit_decode_header(&(out->pub), buf);
    n_eaten += H5FD__mirror_xmit_decode_uint32(&(out->flags), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint64(&(out->maxaddr), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint64(&(out->size_t_blob), &buf[n_eaten]);
    assert((H5FD_MIRROR_XMIT_OPEN_SIZE - H5FD_MIRROR_XMIT_FILEPATH_MAX) == n_eaten);
    strncpy(out->filename, (const char *)&buf[n_eaten], H5FD_MIRROR_XMIT_FILEPATH_MAX - 1);
    out->filename[H5FD_MIRROR_XMIT_FILEPATH_MAX - 1] = 0; /* force final NULL */

    return H5FD_MIRROR_XMIT_OPEN_SIZE;
} /* end H5FD_mirror_xmit_decode_open() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_decode_reply
 *
 * Purpose:     Extract a mirror_xmit_reply_t from the bytes-buffer.
 *
 *              Fields will be lifted from the buffer and stored in the
 *              target structure, using in the correct location (different
 *              systems may insert different padding between components) and
 *              word order (Big- vs Little-Endian).
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 *              The resulting structure should be sanity-checked with
 *              H5FD_mirror_xmit_is_reply() before use.
 *
 * Return:      The maximum number of bytes that this decoding operation might
 *              have consumed from the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_decode_reply(H5FD_mirror_xmit_reply_t *out, const unsigned char *buf)
{
    size_t n_eaten = 0;

    LOG_OP_CALL(__func__);

    assert(out && buf);

    n_eaten += H5FD_mirror_xmit_decode_header(&(out->pub), buf);
    n_eaten += H5FD__mirror_xmit_decode_uint32(&(out->status), &buf[n_eaten]);
    assert((H5FD_MIRROR_XMIT_REPLY_SIZE - H5FD_MIRROR_STATUS_MESSAGE_MAX) == n_eaten);
    strncpy(out->message, (const char *)&buf[n_eaten], H5FD_MIRROR_STATUS_MESSAGE_MAX - 1);
    out->message[H5FD_MIRROR_STATUS_MESSAGE_MAX - 1] = 0; /* force NULL term */

    return H5FD_MIRROR_XMIT_REPLY_SIZE;
} /* end H5FD_mirror_xmit_decode_reply() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_decode_set_eoa
 *
 * Purpose:     Extract a mirror_xmit_eoa_t from the bytes-buffer.
 *
 *              Fields will be lifted from the buffer and stored in the
 *              target structure, using in the correct location (different
 *              systems may insert different padding between components) and
 *              word order (Big- vs Little-Endian).
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 *              The resulting structure should be sanity-checked with
 *              H5FD_mirror_xmit_is_set_eoa() before use.
 *
 * Return:      The number of bytes consumed from the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_decode_set_eoa(H5FD_mirror_xmit_eoa_t *out, const unsigned char *buf)
{
    size_t n_eaten = 0;

    LOG_OP_CALL(__func__);

    assert(out && buf);

    n_eaten += H5FD_mirror_xmit_decode_header(&(out->pub), buf);
    n_eaten += H5FD__mirror_xmit_decode_uint8(&(out->type), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint64(&(out->eoa_addr), &buf[n_eaten]);
    assert(n_eaten == H5FD_MIRROR_XMIT_EOA_SIZE);

    return n_eaten;
} /* end H5FD_mirror_xmit_decode_set_eoa() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_decode_write
 *
 * Purpose:     Extract a mirror_xmit_write_t from the bytes-buffer.
 *
 *              Fields will be lifted from the buffer and stored in the
 *              target structure, using in the correct location (different
 *              systems may insert different padding between components) and
 *              word order (Big- vs Little-Endian).
 *
 *              The programmer must ensure that the received buffer holds
 *              at least the expected size of data.
 *
 *              The resulting structure should be sanity-checked with
 *              H5FD_mirror_xmit_is_write() before use.
 *
 * Return:      The number of bytes consumed from the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_decode_write(H5FD_mirror_xmit_write_t *out, const unsigned char *buf)
{
    size_t n_eaten = 0;

    LOG_OP_CALL(__func__);

    assert(out && buf);

    n_eaten += H5FD_mirror_xmit_decode_header(&(out->pub), buf);
    n_eaten += H5FD__mirror_xmit_decode_uint8(&(out->type), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint64(&(out->offset), &buf[n_eaten]);
    n_eaten += H5FD__mirror_xmit_decode_uint64(&(out->size), &buf[n_eaten]);
    assert(n_eaten == H5FD_MIRROR_XMIT_WRITE_SIZE);

    return n_eaten;
} /* end H5FD_mirror_xmit_decode_write() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_encode_header
 *
 * Purpose:     Encode a mirror_xmit_t "header" to the bytes-buffer.
 *
 *              Fields will be packed into the buffer in a predictable manner,
 *              any numbers stored in "network" (Big-Endian) word order.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes written to the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_encode_header(unsigned char *dest, const H5FD_mirror_xmit_t *x)
{
    size_t n_writ = 0;

    LOG_OP_CALL(__func__);

    assert(dest && x);

    n_writ += H5FD__mirror_xmit_encode_uint32((dest + n_writ), x->magic);
    n_writ += H5FD__mirror_xmit_encode_uint8((dest + n_writ), x->version);
    n_writ += H5FD__mirror_xmit_encode_uint32((dest + n_writ), x->session_token);
    n_writ += H5FD__mirror_xmit_encode_uint32((dest + n_writ), x->xmit_count);
    n_writ += H5FD__mirror_xmit_encode_uint8((dest + n_writ), x->op);
    assert(n_writ == H5FD_MIRROR_XMIT_HEADER_SIZE);

    return n_writ;
} /* end H5FD_mirror_xmit_encode_header() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_encode_lock
 *
 * Purpose:     Encode a mirror_xmit_lock_t to the bytes-buffer.
 *              Fields will be packed into the buffer in a predictable manner,
 *              any numbers stored in "network" (Big-Endian) word order.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes written to the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_encode_lock(unsigned char *dest, const H5FD_mirror_xmit_lock_t *x)
{
    size_t n_writ = 0;

    LOG_OP_CALL(__func__);

    assert(dest && x);

    n_writ += H5FD_mirror_xmit_encode_header(dest, (const H5FD_mirror_xmit_t *)&(x->pub));
    n_writ += H5FD__mirror_xmit_encode_uint64(&dest[n_writ], x->rw);
    assert(n_writ == H5FD_MIRROR_XMIT_LOCK_SIZE);

    return n_writ;
} /* end H5FD_mirror_xmit_encode_lock() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_encode_open
 *
 * Purpose:     Encode a mirror_xmit_open_t to the bytes-buffer.
 *              Fields will be packed into the buffer in a predictable manner,
 *              any numbers stored in "network" (Big-Endian) word order.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The maximum number of bytes that this decoding operation might
 *              have written into the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_encode_open(unsigned char *dest, const H5FD_mirror_xmit_open_t *x)
{
    size_t n_writ = 0;

    LOG_OP_CALL(__func__);

    assert(dest && x);

    /* clear entire structure, but especially its filepath string area */
    memset(dest, 0, H5FD_MIRROR_XMIT_OPEN_SIZE);

    n_writ += H5FD_mirror_xmit_encode_header(dest, (const H5FD_mirror_xmit_t *)&(x->pub));
    n_writ += H5FD__mirror_xmit_encode_uint32(&dest[n_writ], x->flags);
    n_writ += H5FD__mirror_xmit_encode_uint64(&dest[n_writ], x->maxaddr);
    n_writ += H5FD__mirror_xmit_encode_uint64(&dest[n_writ], x->size_t_blob);
    assert((H5FD_MIRROR_XMIT_OPEN_SIZE - H5FD_MIRROR_XMIT_FILEPATH_MAX) == n_writ);
    strncpy((char *)&dest[n_writ], x->filename, H5FD_MIRROR_XMIT_FILEPATH_MAX);

    return H5FD_MIRROR_XMIT_OPEN_SIZE;
} /* end H5FD_mirror_xmit_encode_open() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_encode_reply
 *
 * Purpose:     Encode a mirror_xmit_reply_t to the bytes-buffer.
 *
 *              Fields will be packed into the buffer in a predictable manner,
 *              any numbers stored in "network" (Big-Endian) word order.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The maximum number of bytes that this decoding operation might
 *              have written into the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_encode_reply(unsigned char *dest, const H5FD_mirror_xmit_reply_t *x)
{
    size_t n_writ = 0;

    LOG_OP_CALL(__func__);

    assert(dest && x);

    /* clear entire structure, but especially its message string area */
    memset(dest, 0, H5FD_MIRROR_XMIT_REPLY_SIZE);

    n_writ += H5FD_mirror_xmit_encode_header(dest, (const H5FD_mirror_xmit_t *)&(x->pub));
    n_writ += H5FD__mirror_xmit_encode_uint32(&dest[n_writ], x->status);
    assert((H5FD_MIRROR_XMIT_REPLY_SIZE - H5FD_MIRROR_STATUS_MESSAGE_MAX) == n_writ);
    strncpy((char *)&dest[n_writ], x->message, H5FD_MIRROR_STATUS_MESSAGE_MAX);

    return H5FD_MIRROR_XMIT_REPLY_SIZE;
} /* end H5FD_mirror_xmit_encode_reply() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_encode_set_eoa
 *
 * Purpose:     Encode a mirror_xmit_eoa_t to the bytes-buffer.
 *
 *              Fields will be packed into the buffer in a predictable manner,
 *              any numbers stored in "network" (Big-Endian) word order.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes written to the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_encode_set_eoa(unsigned char *dest, const H5FD_mirror_xmit_eoa_t *x)
{
    size_t n_writ = 0;

    LOG_OP_CALL(__func__);

    assert(dest && x);

    n_writ += H5FD_mirror_xmit_encode_header(dest, (const H5FD_mirror_xmit_t *)&(x->pub));
    n_writ += H5FD__mirror_xmit_encode_uint8(&dest[n_writ], x->type);
    n_writ += H5FD__mirror_xmit_encode_uint64(&dest[n_writ], x->eoa_addr);
    assert(n_writ == H5FD_MIRROR_XMIT_EOA_SIZE);

    return n_writ;
} /* end H5FD_mirror_xmit_encode_set_eoa() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_encode_write
 *
 * Purpose:     Encode a mirror_xmit_write_t to the bytes-buffer.
 *
 *              Fields will be packed into the buffer in a predictable manner,
 *              any numbers stored in "network" (Big-Endian) word order.
 *
 *              The programmer must ensure that the destination buffer is
 *              large enough to hold the expected data.
 *
 * Return:      The number of bytes written to the buffer.
 * ---------------------------------------------------------------------------
 */
size_t
H5FD_mirror_xmit_encode_write(unsigned char *dest, const H5FD_mirror_xmit_write_t *x)
{
    size_t n_writ = 0;

    LOG_OP_CALL(__func__);

    assert(dest && x);

    n_writ += H5FD_mirror_xmit_encode_header(dest, (const H5FD_mirror_xmit_t *)&(x->pub));
    n_writ += H5FD__mirror_xmit_encode_uint8(&dest[n_writ], x->type);
    n_writ += H5FD__mirror_xmit_encode_uint64(&dest[n_writ], x->offset);
    n_writ += H5FD__mirror_xmit_encode_uint64(&dest[n_writ], x->size);
    assert(n_writ == H5FD_MIRROR_XMIT_WRITE_SIZE);

    return n_writ;
} /* end H5FD_mirror_xmit_encode_write() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_is_close
 *
 * Purpose:     Verify that a mirror_xmit_t is a valid CLOSE xmit.
 *
 *              Checks header validity and op code.
 *
 * Return:      true if valid; else false.
 * ---------------------------------------------------------------------------
 */
H5_ATTR_PURE bool
H5FD_mirror_xmit_is_close(const H5FD_mirror_xmit_t *xmit)
{
    LOG_OP_CALL(__func__);

    assert(xmit);

    if ((true == H5FD_mirror_xmit_is_xmit(xmit)) && (H5FD_MIRROR_OP_CLOSE == xmit->op))
        return true;

    return false;
} /* end H5FD_mirror_xmit_is_close() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_is_lock
 *
 * Purpose:     Verify that a mirror_xmit_lock_t is a valid LOCK xmit.
 *
 *              Checks header validity and op code.
 *
 * Return:      true if valid; else false.
 * ---------------------------------------------------------------------------
 */
H5_ATTR_PURE bool
H5FD_mirror_xmit_is_lock(const H5FD_mirror_xmit_lock_t *xmit)
{
    LOG_OP_CALL(__func__);

    assert(xmit);

    if ((true == H5FD_mirror_xmit_is_xmit(&(xmit->pub))) && (H5FD_MIRROR_OP_LOCK == xmit->pub.op))
        return true;

    return false;
} /* end H5FD_mirror_xmit_is_lock() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_is_open
 *
 * Purpose:     Verify that a mirror_xmit_open_t is a valid OPEN xmit.
 *
 *              Checks header validity and op code.
 *
 * Return:      true if valid; else false.
 * ---------------------------------------------------------------------------
 */
H5_ATTR_PURE bool
H5FD_mirror_xmit_is_open(const H5FD_mirror_xmit_open_t *xmit)
{
    LOG_OP_CALL(__func__);

    assert(xmit);

    if ((true == H5FD_mirror_xmit_is_xmit(&(xmit->pub))) && (H5FD_MIRROR_OP_OPEN == xmit->pub.op))

        return true;

    return false;
} /* end H5FD_mirror_xmit_is_open() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_is_eoa
 *
 * Purpose:     Verify that a mirror_xmit_eoa_t is a valid SET-EOA xmit.
 *
 *              Checks header validity and op code.
 *
 * Return:      true if valid; else false.
 * ---------------------------------------------------------------------------
 */
H5_ATTR_PURE bool
H5FD_mirror_xmit_is_set_eoa(const H5FD_mirror_xmit_eoa_t *xmit)
{
    LOG_OP_CALL(__func__);

    assert(xmit);

    if ((true == H5FD_mirror_xmit_is_xmit(&(xmit->pub))) && (H5FD_MIRROR_OP_SET_EOA == xmit->pub.op))
        return true;

    return false;
} /* end H5FD_mirror_xmit_is_eoa() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_is_reply
 *
 * Purpose:     Verify that a mirror_xmit_reply_t is a valid REPLY xmit.
 *
 *              Checks header validity and op code.
 *
 * Return:      true if valid; else false.
 * ---------------------------------------------------------------------------
 */
H5_ATTR_PURE bool
H5FD_mirror_xmit_is_reply(const H5FD_mirror_xmit_reply_t *xmit)
{
    LOG_OP_CALL(__func__);

    assert(xmit);

    if ((true == H5FD_mirror_xmit_is_xmit(&(xmit->pub))) && (H5FD_MIRROR_OP_REPLY == xmit->pub.op))
        return true;

    return false;
} /* end H5FD_mirror_xmit_is_reply() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_is_write
 *
 * Purpose:     Verify that a mirror_xmit_write_t is a valid WRITE xmit.
 *
 *              Checks header validity and op code.
 *
 * Return:      true if valid; else false.
 * ---------------------------------------------------------------------------
 */
H5_ATTR_PURE bool
H5FD_mirror_xmit_is_write(const H5FD_mirror_xmit_write_t *xmit)
{
    LOG_OP_CALL(__func__);

    assert(xmit);

    if ((true == H5FD_mirror_xmit_is_xmit(&(xmit->pub))) && (H5FD_MIRROR_OP_WRITE == xmit->pub.op))
        return true;

    return false;
} /* end H5FD_mirror_xmit_is_write() */

/* ---------------------------------------------------------------------------
 * Function:    H5FD_mirror_xmit_is_xmit
 *
 * Purpose:     Verify that a mirror_xmit_t is well-formed.
 *
 *              Checks magic number and structure version.
 *
 * Return:      true if valid; else false.
 * ---------------------------------------------------------------------------
 */
H5_ATTR_PURE bool
H5FD_mirror_xmit_is_xmit(const H5FD_mirror_xmit_t *xmit)
{
    LOG_OP_CALL(__func__);

    assert(xmit);

    if ((H5FD_MIRROR_XMIT_MAGIC != xmit->magic) || (H5FD_MIRROR_XMIT_CURR_VERSION != xmit->version))
        return false;

    return true;
} /* end H5FD_mirror_xmit_is_xmit() */

/* ----------------------------------------------------------------------------
 * Function:    H5FD__mirror_verify_reply
 *
 * Purpose:     Wait for and read reply data from remote processes.
 *              Sanity-check that a reply is well-formed and valid.
 *              If all checks pass, inspect the reply contents and handle
 *              reported error, if not an OK reply.
 *
 * Return:      SUCCEED if ok, else FAIL.
 * ----------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_verify_reply(H5FD_mirror_t *file)
{
    unsigned char                  *xmit_buf = NULL;
    struct H5FD_mirror_xmit_reply_t reply;
    ssize_t                         read_ret  = 0;
    herr_t                          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    assert(file && file->sock_fd);

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate xmit buffer");

    read_ret = HDread(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_REPLY_SIZE);
    if (read_ret < 0)
        HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "unable to read reply");
    if (read_ret != H5FD_MIRROR_XMIT_REPLY_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "unexpected read size");

    LOG_XMIT_BYTES("reply", xmit_buf, read_ret);

    if (H5FD_mirror_xmit_decode_reply(&reply, xmit_buf) != H5FD_MIRROR_XMIT_REPLY_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "unable to decode reply xmit");

    if (H5FD_mirror_xmit_is_reply(&reply) != true)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "xmit op code was not REPLY");

    if (reply.pub.session_token != file->xmit.session_token)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "wrong session");
    if (reply.pub.xmit_count != (file->xmit_i)++)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "xmit out of sync");
    if (reply.status != H5FD_MIRROR_STATUS_OK)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "%s", (const char *)(reply.message));

done:
    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_verify_reply() */

/* -------------------------------------------------------------------------
 * Function:    H5FD__mirror_fapl_get
 *
 * Purpose:     Get the file access property list which could be used to create
 *              an identical file.
 *
 * Return:      Success: pointer to the new file access property list value.
 *              Failure: NULL
 * -------------------------------------------------------------------------
 */
static void *
H5FD__mirror_fapl_get(H5FD_t *_file)
{
    H5FD_mirror_t      *file      = (H5FD_mirror_t *)_file;
    H5FD_mirror_fapl_t *fa        = NULL;
    void               *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    fa = (H5FD_mirror_fapl_t *)H5MM_calloc(sizeof(H5FD_mirror_fapl_t));
    if (NULL == fa)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "calloc failed");

    H5MM_memcpy(fa, &(file->fa), sizeof(H5FD_mirror_fapl_t));

    ret_value = fa;

done:
    if (ret_value == NULL)
        if (fa != NULL)
            H5MM_xfree(fa);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_fapl_get() */

/* -------------------------------------------------------------------------
 * Function:    H5FD__mirror_fapl_copy
 *
 * Purpose:     Copies the mirror vfd-specific file access properties.
 *
 * Return:      Success:        Pointer to a new property list
 *              Failure:        NULL
 * -------------------------------------------------------------------------
 */
static void *
H5FD__mirror_fapl_copy(const void *_old_fa)
{
    const H5FD_mirror_fapl_t *old_fa    = (const H5FD_mirror_fapl_t *)_old_fa;
    H5FD_mirror_fapl_t       *new_fa    = NULL;
    void                     *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    new_fa = (H5FD_mirror_fapl_t *)H5MM_malloc(sizeof(H5FD_mirror_fapl_t));
    if (new_fa == NULL)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "memory allocation failed");

    H5MM_memcpy(new_fa, old_fa, sizeof(H5FD_mirror_fapl_t));
    ret_value = new_fa;

done:
    if (ret_value == NULL)
        if (new_fa != NULL)
            H5MM_xfree(new_fa);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_fapl_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_fapl_free
 *
 * Purpose:     Frees the mirror VFD-specific file access properties.
 *
 * Return:      SUCCEED (cannot fail)
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_fapl_free(void *_fa)
{
    H5FD_mirror_fapl_t *fa = (H5FD_mirror_fapl_t *)_fa;

    FUNC_ENTER_PACKAGE_NOERR

    LOG_OP_CALL(__func__);

    /* sanity check */
    assert(fa != NULL);
    assert(fa->magic == H5FD_MIRROR_FAPL_MAGIC);

    fa->magic += 1; /* invalidate */
    H5MM_xfree(fa);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__mirror_fapl_free() */

/* -------------------------------------------------------------------------
 * Function:    H5Pget_fapl_mirror
 *
 * Purpose:     Get the configuration information for this fapl.
 *              Data is memcopied into the fa_dst pointer.
 *
 * Return:      SUCCEED/FAIL
 * -------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_mirror(hid_t fapl_id, H5FD_mirror_fapl_t *fa_dst /*out*/)
{
    const H5FD_mirror_fapl_t *fa_src    = NULL;
    H5P_genplist_t           *plist     = NULL;
    herr_t                    ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", fapl_id, fa_dst);

    LOG_OP_CALL(__func__);

    if (NULL == fa_dst)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "fa_dst is NULL");

    plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS);
    if (NULL == plist)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (H5P_peek_driver(plist) != H5FD_MIRROR)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");

    fa_src = (const H5FD_mirror_fapl_t *)H5P_peek_driver_info(plist);
    if (NULL == fa_src)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad VFL driver info");

    assert(fa_src->magic == H5FD_MIRROR_FAPL_MAGIC); /* sanity check */

    H5MM_memcpy(fa_dst, fa_src, sizeof(H5FD_mirror_fapl_t));

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fapl_mirror() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_mirror
 *
 * Purpose:     Modify the file access property list to use the mirror
 *              driver (H5FD_MIRROR) defined in this source file.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_mirror(hid_t fapl_id, H5FD_mirror_fapl_t *fa)
{
    H5P_genplist_t *plist     = NULL;
    herr_t          ret_value = FAIL;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*#", fapl_id, fa);

    LOG_OP_CALL(__func__);

    plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS);
    if (NULL == plist)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (NULL == fa)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "null fapl_t pointer");
    if (H5FD_MIRROR_FAPL_MAGIC != fa->magic)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fapl_t magic");
    if (H5FD_MIRROR_CURR_FAPL_T_VERSION != fa->version)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown fapl_t version");

    ret_value = H5P_set_driver(plist, H5FD_MIRROR, (const void *)fa, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_mirror() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_open
 *
 * Purpose:     Create and/or opens a file as an HDF5 file.
 *
 *              Initiate connection with remote Server/Writer.
 *              If successful, the remote file is open.
 *
 * Return:      Success:    A pointer to a new file data structure. The
 *                          public fields will be initialized by the
 *                          caller, which is always H5FD_open().
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__mirror_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    int                      live_socket = -1;
    struct sockaddr_in       target_addr;
    socklen_t                addr_size;
    unsigned char           *xmit_buf = NULL;
    H5FD_mirror_fapl_t       fa;
    H5FD_mirror_t           *file      = NULL;
    H5FD_mirror_xmit_open_t *open_xmit = NULL;
    H5FD_t                  *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    /* --------------- */
    /* Check arguments */
    /* --------------- */

    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
    if (strlen(name) >= H5FD_MIRROR_XMIT_FILEPATH_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "filename is too long");
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
    if (ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr");

    if (H5Pget_fapl_mirror(fapl_id, &fa) == FAIL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "can't get config info");
    if (H5FD_MIRROR_FAPL_MAGIC != fa.magic)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid fapl magic");
    if (H5FD_MIRROR_CURR_FAPL_T_VERSION != fa.version)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid fapl version");

    /* --------------------- */
    /* Handshake with remote */
    /* --------------------- */

    live_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (live_socket < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "can't create socket");

    target_addr.sin_family      = AF_INET;
    target_addr.sin_port        = htons((uint16_t)fa.handshake_port);
    target_addr.sin_addr.s_addr = inet_addr(fa.remote_ip);
    memset(target_addr.sin_zero, '\0', sizeof target_addr.sin_zero);

    addr_size = sizeof(target_addr);
    if (connect(live_socket, (struct sockaddr *)&target_addr, addr_size) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "can't connect to remote server");

    /* ------------- */
    /* Open the file */
    /* ------------- */

    file = (H5FD_mirror_t *)H5FL_CALLOC(H5FD_mirror_t);
    if (NULL == file)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate file struct");

    file->sock_fd = live_socket;
    file->xmit_i  = 0;

    file->xmit.magic         = H5FD_MIRROR_XMIT_MAGIC;
    file->xmit.version       = H5FD_MIRROR_XMIT_CURR_VERSION;
    file->xmit.xmit_count    = file->xmit_i++;
    file->xmit.session_token = (uint32_t)(0x01020304 ^ file->sock_fd); /* TODO: hashing? */
    /* int --> uint32_t may truncate on some systems... shouldn't matter? */

    open_xmit = (H5FD_mirror_xmit_open_t *)H5FL_CALLOC(H5FD_mirror_xmit_open_t);
    if (NULL == open_xmit)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate open_xmit struct");

    file->xmit.op          = H5FD_MIRROR_OP_OPEN;
    open_xmit->pub         = file->xmit;
    open_xmit->flags       = (uint32_t)flags;
    open_xmit->maxaddr     = (uint64_t)maxaddr;
    open_xmit->size_t_blob = (uint64_t)((size_t)(-1));
    snprintf(open_xmit->filename, H5FD_MIRROR_XMIT_FILEPATH_MAX - 1, "%s", name);

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate xmit buffer");

    if (H5FD_mirror_xmit_encode_open(xmit_buf, open_xmit) != H5FD_MIRROR_XMIT_OPEN_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, NULL, "unable to encode open");

    LOG_XMIT_BYTES("open", xmit_buf, H5FD_MIRROR_XMIT_OPEN_SIZE);

    if (HDwrite(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_OPEN_SIZE) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, NULL, "unable to transmit open");

    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "invalid reply");

    ret_value = (H5FD_t *)file;

done:
    if (NULL == ret_value) {
        if (file)
            file = H5FL_FREE(H5FD_mirror_t, file);
        if (live_socket >= 0 && HDclose(live_socket) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, NULL, "can't close socket");
    }

    if (open_xmit)
        open_xmit = H5FL_FREE(H5FD_mirror_xmit_open_t, open_xmit);
    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_close
 *
 * Purpose:     Closes the HDF5 file.
 *
 *              Tries to send a CLOSE op to the remote Writer and expects
 *              a valid reply, then closes the socket.
 *              In error, attempts to send a deliberately invalid xmit to the
 *              Writer to get it to close/abort, then attempts to close the
 *              socket.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file possibly not closed but resources freed.
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_close(H5FD_t *_file)
{
    H5FD_mirror_t *file         = (H5FD_mirror_t *)_file;
    unsigned char *xmit_buf     = NULL;
    int            xmit_encoded = 0; /* monitor point of failure */
    herr_t         ret_value    = SUCCEED;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    /* Sanity check */
    assert(file);
    assert(file->sock_fd >= 0);

    file->xmit.xmit_count = (file->xmit_i)++;
    file->xmit.op         = H5FD_MIRROR_OP_CLOSE;

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate xmit buffer");

    if (H5FD_mirror_xmit_encode_header(xmit_buf, &(file->xmit)) != H5FD_MIRROR_XMIT_HEADER_SIZE)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to encode close");
    xmit_encoded = 1;

    LOG_XMIT_BYTES("close", xmit_buf, H5FD_MIRROR_XMIT_HEADER_SIZE);

    if (HDwrite(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_HEADER_SIZE) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to transmit close");

    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid reply");

    if (HDclose(file->sock_fd) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, FAIL, "can't close socket");

done:
    if (ret_value == FAIL) {
        if (xmit_encoded == 0) {
            /* Encode failed; send GOODBYE to force writer halt.
             * We can ignore any response from the writer, if we receive
             * any reply at all.
             */
            if (HDwrite(file->sock_fd, "GOODBYE", strlen("GOODBYE")) < 0) {
                HDONE_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to transmit close");
                if (HDclose(file->sock_fd) < 0)
                    HDONE_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, FAIL, "can't close socket");
                file->sock_fd = -1; /* invalidate for later */
            }                       /* end if problem writing goodbye; go down hard */
            else if (HDshutdown(file->sock_fd, SHUT_WR) < 0)
                HDONE_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't shutdown socket write: %s", strerror(errno));
        } /* end if xmit encode failed */

        if (file->sock_fd >= 0)
            if (HDclose(file->sock_fd) < 0)
                HDONE_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, FAIL, "can't close socket");
    } /* end if error */

    file = H5FL_FREE(H5FD_mirror_t, file); /* always release resources */

    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_query
 *
 * Purpose:     Get the driver feature flags implemented by the driver.
 *
 * Return:      SUCCEED (non-negative) (can't fail)
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_query(const H5FD_t H5_ATTR_UNUSED *_file, unsigned long *flags)
{
    FUNC_ENTER_PACKAGE_NOERR

    LOG_OP_CALL(__func__);

    /* Notice: the Mirror VFD Writer currently uses only the Sec2 driver as
     * the underlying driver -- as such, the Mirror VFD implementation copies
     * the Sec2 feature flags as its own.
     *
     * File pointer is always NULL/unused -- the H5FD_FEAT_IGNORE_DRVRINFO flag
     * is never included.
     * -- JOS 2020-01-13
     */
    if (flags)
        *flags = H5FD_FEAT_AGGREGATE_METADATA | H5FD_FEAT_ACCUMULATE_METADATA | H5FD_FEAT_DATA_SIEVE |
                 H5FD_FEAT_AGGREGATE_SMALLDATA | H5FD_FEAT_POSIX_COMPAT_HANDLE | H5FD_FEAT_SUPPORTS_SWMR_IO |
                 H5FD_FEAT_DEFAULT_VFD_COMPATIBLE;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__mirror_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 *              Required to register the driver.
 *
 * Return:      The end-of-address marker.
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__mirror_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_mirror_t *file = (const H5FD_mirror_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    LOG_OP_CALL(__func__);

    assert(file);

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD__mirror_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED / FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr)
{
    H5FD_mirror_xmit_eoa_t xmit_eoa;
    unsigned char         *xmit_buf  = NULL;
    H5FD_mirror_t         *file      = (H5FD_mirror_t *)_file;
    herr_t                 ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    assert(file);

    file->eoa = addr; /* local copy */

    file->xmit.xmit_count = (file->xmit_i)++;
    file->xmit.op         = H5FD_MIRROR_OP_SET_EOA;

    xmit_eoa.pub      = file->xmit;
    xmit_eoa.type     = (uint8_t)type;
    xmit_eoa.eoa_addr = (uint64_t)addr;

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate xmit buffer");

    if (H5FD_mirror_xmit_encode_set_eoa(xmit_buf, &xmit_eoa) != H5FD_MIRROR_XMIT_EOA_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to encode set-eoa");

    LOG_XMIT_BYTES("set-eoa", xmit_buf, H5FD_MIRROR_XMIT_EOA_SIZE);

    if (HDwrite(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_EOA_SIZE) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to transmit set-eoa");

    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid reply");

done:
    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 *              Required to register the driver.
 *
 * Return:      End of file address, the first address past the end of the
 *              "file", either the filesystem file or the HDF5 file.
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__mirror_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_mirror_t *file = (const H5FD_mirror_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    LOG_OP_CALL(__func__);

    assert(file);

    FUNC_LEAVE_NOAPI(file->eof)
} /* end H5FD__mirror_get_eof() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_read
 *
 * Purpose:     Required to register the driver, but if called, MUST fail.
 *
 * Return:      FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_read(H5FD_t H5_ATTR_UNUSED *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED fapl_id,
                  haddr_t H5_ATTR_UNUSED addr, size_t H5_ATTR_UNUSED size, void H5_ATTR_UNUSED *buf)
{
    FUNC_ENTER_PACKAGE_NOERR

    LOG_OP_CALL(__func__);

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5FD__mirror_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_write
 *
 * Purpose:     Writes SIZE bytes of data to FILE beginning at address ADDR
 *              from buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 *              Send metadata regarding the write (location, size) to the
 *              remote Writer, then separately transmits the data.
 *              Both transmission expect an OK reply from the Writer.
 *              This two-exchange approach incurs significant overhead,
 *              but is a simple and modular approach.
 *              Start optimizations here.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_write(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr, size_t size,
                   const void *buf)
{
    H5FD_mirror_xmit_write_t xmit_write;
    unsigned char           *xmit_buf  = NULL;
    H5FD_mirror_t           *file      = (H5FD_mirror_t *)_file;
    herr_t                   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    assert(file);
    assert(buf);

    file->xmit.xmit_count = (file->xmit_i)++;
    file->xmit.op         = H5FD_MIRROR_OP_WRITE;

    xmit_write.pub    = file->xmit;
    xmit_write.size   = (uint64_t)size;
    xmit_write.offset = (uint64_t)addr;
    xmit_write.type   = (uint8_t)type;

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate xmit buffer");

    /* Notify Writer of incoming data to write. */
    if (H5FD_mirror_xmit_encode_write(xmit_buf, &xmit_write) != H5FD_MIRROR_XMIT_WRITE_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to encode write");

    LOG_XMIT_BYTES("write", xmit_buf, H5FD_MIRROR_XMIT_WRITE_SIZE);

    if (HDwrite(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_WRITE_SIZE) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to transmit write");

    /* Check that our write xmission was received */
    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid reply");

    /* Send the data to be written */
    if (HDwrite(file->sock_fd, buf, size) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to transmit data");

    /* Writer should reply that it got the data and is still okay/ready */
    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid reply");

done:
    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_truncate
 *
 * Purpose:     Makes sure that the true file size is the same (or larger)
 *              than the end-of-address.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool H5_ATTR_UNUSED closing)
{
    unsigned char *xmit_buf  = NULL;
    H5FD_mirror_t *file      = (H5FD_mirror_t *)_file;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    file->xmit.xmit_count = (file->xmit_i)++;
    file->xmit.op         = H5FD_MIRROR_OP_TRUNCATE;

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate xmit buffer");

    if (H5FD_mirror_xmit_encode_header(xmit_buf, &(file->xmit)) != H5FD_MIRROR_XMIT_HEADER_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to encode truncate");

    LOG_XMIT_BYTES("truncate", xmit_buf, H5FD_MIRROR_XMIT_HEADER_SIZE);

    if (HDwrite(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_HEADER_SIZE) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to transmit truncate");

    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid reply");

done:
    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_lock
 *
 * Purpose:     To place an advisory lock on a file.
 *              The lock type to apply depends on the parameter "rw":
 *                      true--opens for write: an exclusive lock
 *                      false--opens for read: a shared lock
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_lock(H5FD_t *_file, bool rw)
{
    H5FD_mirror_xmit_lock_t xmit_lock;
    unsigned char          *xmit_buf  = NULL;
    H5FD_mirror_t          *file      = (H5FD_mirror_t *)_file;
    herr_t                  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    file->xmit.xmit_count = (file->xmit_i)++;
    file->xmit.op         = H5FD_MIRROR_OP_LOCK;

    xmit_lock.pub = file->xmit;
    xmit_lock.rw  = (uint64_t)rw;

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate xmit buffer");

    if (H5FD_mirror_xmit_encode_lock(xmit_buf, &xmit_lock) != H5FD_MIRROR_XMIT_LOCK_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to encode lock");

    LOG_XMIT_BYTES("lock", xmit_buf, H5FD_MIRROR_XMIT_LOCK_SIZE);

    if (HDwrite(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_LOCK_SIZE) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to transmit lock");

    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid reply");

done:
    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_lock */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mirror_unlock
 *
 * Purpose:     Remove the existing lock on the file.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mirror_unlock(H5FD_t *_file)
{
    unsigned char *xmit_buf  = NULL;
    H5FD_mirror_t *file      = (H5FD_mirror_t *)_file;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    LOG_OP_CALL(__func__);

    file->xmit.xmit_count = (file->xmit_i)++;
    file->xmit.op         = H5FD_MIRROR_OP_UNLOCK;

    xmit_buf = H5FL_BLK_MALLOC(xmit, H5FD_MIRROR_XMIT_BUFFER_MAX);
    if (NULL == xmit_buf)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate xmit buffer");

    if (H5FD_mirror_xmit_encode_header(xmit_buf, &(file->xmit)) != H5FD_MIRROR_XMIT_HEADER_SIZE)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to encode unlock");

    LOG_XMIT_BYTES("unlock", xmit_buf, H5FD_MIRROR_XMIT_HEADER_SIZE);

    if (HDwrite(file->sock_fd, xmit_buf, H5FD_MIRROR_XMIT_HEADER_SIZE) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "unable to transmit unlock");

    if (H5FD__mirror_verify_reply(file) == FAIL)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid reply");

done:
    if (xmit_buf)
        xmit_buf = H5FL_BLK_FREE(xmit, xmit_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mirror_unlock */

#endif /* H5_HAVE_MIRROR_VFD */
