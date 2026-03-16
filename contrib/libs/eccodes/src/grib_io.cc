/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

#if GRIB_PTHREADS
static pthread_once_t once    = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;
static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex1, &attr);
    pthread_mutex_init(&mutex2, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex1;
static omp_nest_lock_t mutex2;
static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_io_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex1);
            omp_init_nest_lock(&mutex2);
            once = 1;
        }
    }
}
#endif


#define GRIB 0x47524942
#define BUDG 0x42554447
#define DIAG 0x44494147
#define TIDE 0x54494445
#define BUFR 0x42554652
#define HDF5 0x89484446
#define WRAP 0x57524150

#define ECCODES_READS_BUFR 1
#define ECCODES_READS_HDF5 1
#define ECCODES_READS_WRAP 1


typedef struct alloc_buffer
{
    size_t size;
    void* buffer;
} alloc_buffer;

typedef size_t (*readproc)(void*, void*, size_t, int*);
typedef int (*seekproc)(void*, off_t);
typedef off_t (*tellproc)(void*);
typedef void* (*allocproc)(void*, size_t*, int*);


typedef struct reader
{
    void* read_data;
    readproc read;

    void* alloc_data;
    allocproc alloc;
    int headers_only;

    seekproc seek;
    seekproc seek_from_start;
    tellproc tell;
    off_t offset;

    size_t message_size;

} reader;

// If no_alloc argument is 1, then the content of the message are not stored. We just seek to the final 7777
static int read_the_rest(reader* r, size_t message_length, unsigned char* tmp, int already_read, int check7777, int no_alloc)
{
    int err = GRIB_SUCCESS;
    size_t buffer_size;
    size_t rest;
    unsigned char* buffer = NULL;
    grib_context* c = grib_context_get_default();

    if (message_length == 0)
        return GRIB_BUFFER_TOO_SMALL;

    buffer_size     = message_length; // store the whole message in the buffer
    if (no_alloc)
        buffer_size = 5; // big enough to store the 7777
    rest            = message_length - already_read;
    r->message_size = message_length;
    buffer          = (unsigned char*)r->alloc(r->alloc_data, &buffer_size, &err);
    if (err)
        return err;

    if (no_alloc) {
        r->seek(r->read_data,  rest - 4); // jump to the end before the 7777
    } else {
        if (buffer == NULL || (buffer_size < message_length)) {
            return GRIB_BUFFER_TOO_SMALL;
        }
        memcpy(buffer, tmp, already_read);
    }

    bool read_failed = false;
    if (no_alloc) {
        read_failed = ((r->read(r->read_data, buffer, 4, &err) != 4) || err);
    }
    else {
        read_failed = ((r->read(r->read_data, buffer + already_read, rest, &err) != rest) || err);
    }
    if (read_failed) {
        if (c->debug)
            fprintf(stderr, "ECCODES DEBUG %s: Read failed (Coded length=%zu, Already read=%d)",
                    __func__, message_length, already_read);
        return err;
    }

    const size_t mlen = no_alloc ? 4 : message_length;
    if (check7777 && !r->headers_only &&
        (buffer[mlen - 4] != '7' ||
         buffer[mlen - 3] != '7' ||
         buffer[mlen - 2] != '7' ||
         buffer[mlen - 1] != '7'))
    {
        if (c->debug)
            fprintf(stderr, "ECCODES DEBUG %s: No final 7777 at expected location (Coded length=%zu)\n", __func__, message_length);
        return GRIB_WRONG_LENGTH;
    }

    return GRIB_SUCCESS;
}

#define CHECK_TMP_SIZE(a)                                                                                    \
    if (sizeof(tmp) < (a)) {                                                                                 \
        fprintf(stderr, "%s:%d sizeof(tmp)<%s %d<%d\n", __FILE__, __LINE__, #a, (int)sizeof(tmp), (int)(a)); \
        return GRIB_INTERNAL_ARRAY_TOO_SMALL;                                                                \
    }

#define GROW_BUF_IF_REQUIRED(desired_length)      \
    if (buf->length < (desired_length)) {         \
        grib_grow_buffer(c, buf, desired_length); \
        tmp = buf->data;                          \
    }

#define UINT3(a, b, c) (size_t)((a << 16) + (b << 8) + c);

static int read_GRIB(reader* r, int no_alloc)
{
    unsigned char* tmp  = NULL;
    size_t length       = 0;
    size_t total_length = 0;
    long edition        = 0;
    int err             = 0;
    int i               = 0, j;
    size_t sec1len      = 0;
    size_t sec2len      = 0;
    size_t sec3len      = 0;
    size_t sec4len      = 0;
    unsigned long flags;
    size_t buflen = 32768; /* See ECC-515: was 16368 */
    grib_context* c;
    grib_buffer* buf;

    /*TODO proper context*/
    c   = grib_context_get_default();
    tmp = (unsigned char*)malloc(buflen);
    if (!tmp)
        return GRIB_OUT_OF_MEMORY;
    buf           = grib_new_buffer(c, tmp, buflen);
    buf->property = CODES_MY_BUFFER;

    tmp[i++] = 'G';
    tmp[i++] = 'R';
    tmp[i++] = 'I';
    tmp[i++] = 'B';

    r->offset = r->tell(r->read_data) - 4;

    if (r->read(r->read_data, &tmp[i], 3, &err) != 3 || err)
        return err;

    length = UINT3(tmp[i], tmp[i + 1], tmp[i + 2]);
    i += 3;

    /* Edition number */
    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
        return err;

    edition = tmp[i++];

    switch (edition) {
        case 1:
            if (r->headers_only) {
                /* Read section 1 length */
                if (r->read(r->read_data, &tmp[i], 3, &err) != 3 || err)
                    return err;

                sec1len = UINT3(tmp[i], tmp[i + 1], tmp[i + 2]);
                i += 3;
                /* Read section 1. 3 = length */
                if ((r->read(r->read_data, tmp + i, sec1len - 3, &err) != sec1len - 3) || err)
                    return err;
                flags = tmp[15];

                i += sec1len - 3;

                GROW_BUF_IF_REQUIRED(i + 3);

                if (flags & (1 << 7)) {
                    /* Section 2 */
                    if (r->read(r->read_data, &tmp[i], 3, &err) != 3 || err)
                        return err;

                    sec2len = UINT3(tmp[i], tmp[i + 1], tmp[i + 2]);
                    GROW_BUF_IF_REQUIRED(i + sec2len);
                    i += 3;
                    /* Read section 2 */
                    if ((r->read(r->read_data, tmp + i, sec2len - 3, &err) != sec2len - 3) || err)
                        return err;
                    i += sec2len - 3;
                }


                if (flags & (1 << 6)) {
                    /* Section 3 */
                    GROW_BUF_IF_REQUIRED(i + 3);
                    for (j = 0; j < 3; j++) {
                        if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                            return err;

                        sec3len <<= 8;
                        sec3len |= tmp[i];
                        i++;
                    }

                    /* Read section 3 */
                    GROW_BUF_IF_REQUIRED(i + sec3len);
                    if ((r->read(r->read_data, tmp + i, sec3len - 3, &err) != sec3len - 3) || err)
                        return err;
                    i += sec3len - 3;
                }

                GROW_BUF_IF_REQUIRED(i + 11);

                /* Section 4 */
                for (j = 0; j < 3; j++) {
                    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                        return err;

                    sec4len <<= 8;
                    sec4len |= tmp[i];
                    i++;
                }

                /* we don't read the data, only headers */
                if ((r->read(r->read_data, tmp + i, 8, &err) != 8) || err)
                    return err;

                i += 8;

                total_length = length;
                /* length=8+sec1len + sec2len+sec3len+11; */
                length = i;
                r->seek(r->read_data, total_length - length - 1);
            }
            else if (length & 0x800000) {
                /* Large GRIBs */

                /* Read section 1 length */
                for (j = 0; j < 3; j++) {
                    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                        return err;

                    sec1len <<= 8;
                    sec1len |= tmp[i];
                    i++;
                }

                /* table version */
                if (r->read(r->read_data, &tmp[i++], 1, &err) != 1 || err)
                    return err;
                /* center */
                if (r->read(r->read_data, &tmp[i++], 1, &err) != 1 || err)
                    return err;
                /* process */
                if (r->read(r->read_data, &tmp[i++], 1, &err) != 1 || err)
                    return err;
                /* grid */
                if (r->read(r->read_data, &tmp[i++], 1, &err) != 1 || err)
                    return err;
                /* flags */
                if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                    return err;
                flags = tmp[i++];

                /* fprintf(stderr," sec1len=%d i=%d flags=%x\n",sec1len,i,flags); */

                GROW_BUF_IF_REQUIRED(8 + sec1len + 4 + 3);

                /* Read section 1. 3 = length, 5 = table,center,process,grid,flags */
                if ((r->read(r->read_data, tmp + i, sec1len - 3 - 5, &err) != sec1len - 3 - 5) || err)
                    return err;

                i += sec1len - 3 - 5;

                if (flags & (1 << 7)) {
                    /* Section 2 */
                    for (j = 0; j < 3; j++) {
                        if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                            return err;

                        sec2len <<= 8;
                        sec2len |= tmp[i];
                        i++;
                    }
                    /* Read section 2 */
                    GROW_BUF_IF_REQUIRED(i + sec2len);
                    if ((r->read(r->read_data, tmp + i, sec2len - 3, &err) != sec2len - 3) || err)
                        return err;
                    i += sec2len - 3;
                }

                GROW_BUF_IF_REQUIRED(sec1len + sec2len + 4 + 3);

                if (flags & (1 << 6)) {
                    /* Section 3 */
                    for (j = 0; j < 3; j++) {
                        if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                            return err;

                        sec3len <<= 8;
                        sec3len |= tmp[i];
                        i++;
                    }

                    /* Read section 3 */
                    GROW_BUF_IF_REQUIRED(sec1len + sec2len + sec3len + 4 + 3);
                    if ((r->read(r->read_data, tmp + i, sec3len - 3, &err) != sec3len - 3) || err)
                        return err;
                    i += sec3len - 3;
                }

                /* fprintf(stderr,"%s sec1len=%d i=%d\n",type,sec1len,i); */
                GROW_BUF_IF_REQUIRED(sec1len + sec2len + sec3len + 4 + 3);

                for (j = 0; j < 3; j++) {
                    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                        return err;

                    sec4len <<= 8;
                    sec4len |= tmp[i];
                    i++;
                }

                if (sec4len < 120) {
                    /* Special coding */
                    length &= 0x7fffff;
                    length *= 120;
                    length -= sec4len;
                    length += 4;
                }
                else {
                    /* length is already set to the right value */
                }
            }
            break;

        case 2:
        case 3:
            length = 0;

            if (sizeof(long) >= 8) {
                for (j = 0; j < 8; j++) {
                    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                        return err;

                    length <<= 8;
                    length |= tmp[i];
                    i++;
                }
            }
            else {
                /* Check if the length fits in a long */
                for (j = 0; j < 4; j++) {
                    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                        return err;

                    length <<= 8;
                    length |= tmp[i];
                    i++;
                }

                if (length)
                    return GRIB_MESSAGE_TOO_LARGE; /* Message too large */

                for (j = 0; j < 4; j++) {
                    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                        return err;

                    length <<= 8;
                    length |= tmp[i];
                    i++;
                }
            }
            break;

        default:
            r->seek_from_start(r->read_data, r->offset + 4);
            grib_buffer_delete(c, buf);
            return GRIB_UNSUPPORTED_EDITION;
    }

    /* ECCODES_ASSERT(i <= buf->length); */
    err = read_the_rest(r, length, tmp, i, /*check7777=*/1, no_alloc);
    if (err)
        r->seek_from_start(r->read_data, r->offset + 4);

    grib_buffer_delete(c, buf);

    return err;
}

static int read_PSEUDO(reader* r, const char* type, int no_alloc)
{
    unsigned char tmp[32]; /* Should be enough */
    size_t sec1len = 0;
    size_t sec4len = 0;
    int err        = 0;
    int i = 0, j = 0;

    ECCODES_ASSERT(strlen(type) == 4);
    for (j = 0; j < 4; j++) {
        tmp[i] = type[i];
        i++;
    }

    r->offset = r->tell(r->read_data) - 4;

    for (j = 0; j < 3; j++) {
        if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
            return err;

        sec1len <<= 8;
        sec1len |= tmp[i];
        i++;
    }

    /* fprintf(stderr,"%s sec1len=%d i=%d\n",type,sec1len,i); */
    CHECK_TMP_SIZE(sec1len + 4 + 3);

    /* Read sectoin1 */
    if ((r->read(r->read_data, tmp + i, sec1len - 3, &err) != sec1len - 3) || err)
        return err;

    i += sec1len - 3;

    for (j = 0; j < 3; j++) {
        if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
            return err;

        sec4len <<= 8;
        sec4len |= tmp[i];
        i++;
    }

    /* fprintf(stderr,"%s sec4len=%d i=%d l=%d\n",type,sec4len,i,4+sec1len+sec4len+4); */

    ECCODES_ASSERT(i <= sizeof(tmp));
    return read_the_rest(r, 4 + sec1len + sec4len + 4, tmp, i, /*check7777=*/1, no_alloc);
}

static int read_HDF5_offset(reader* r, int length, unsigned long* v, unsigned char* tmp, int* i)
{
    unsigned char buf[8];
    int j, k;
    int err = 0;


    if ((r->read(r->read_data, buf, length, &err) != length) || err) {
        return err;
    }

    k = *i;
    for (j = 0; j < length; j++) {
        tmp[k++] = buf[j];
    }
    *i = k;

    *v = 0;
    for (j = length - 1; j >= 0; j--) {
        *v <<= 8;
        *v |= buf[j];
    }

    return 0;
}

static int read_HDF5(reader* r)
{
    /*
     * See: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock
     */
    unsigned char tmp[49]; /* Should be enough */
    unsigned char buf[4];

    unsigned char version_of_superblock, size_of_offsets, size_of_lengths, consistency_flags;
    unsigned long base_address, superblock_extension_address, end_of_file_address;

    int i           = 0, j;
    int err         = 0;
    grib_context* c = grib_context_get_default();

    tmp[i++] = 137;
    tmp[i++] = 'H';
    tmp[i++] = 'D';
    tmp[i++] = 'F';

    if ((r->read(r->read_data, buf, 4, &err) != 4) || err) {
        return err;
    }

    if (!(buf[0] == '\r' && buf[1] == '\n' && buf[2] == 26 && buf[3] == '\n')) {
        /* Invalid magic, we should not use grib_context_log without a context */
        grib_context_log(c, GRIB_LOG_ERROR, "read_HDF5: invalid signature");
        return GRIB_INVALID_MESSAGE;
    }

    for (j = 0; j < 4; j++) {
        tmp[i++] = buf[j];
    }

    if ((r->read(r->read_data, &version_of_superblock, 1, &err) != 1) || err) {
        return err;
    }

    tmp[i++] = version_of_superblock;

    if (version_of_superblock == 2 || version_of_superblock == 3) {
        if ((r->read(r->read_data, &size_of_offsets, 1, &err) != 1) || err) {
            return err;
        }

        tmp[i++] = size_of_offsets;

        if (size_of_offsets > 8) {
            grib_context_log(c, GRIB_LOG_ERROR, "read_HDF5: invalid size_of_offsets: %ld, only <= 8 is supported", (long)size_of_offsets);
            return GRIB_NOT_IMPLEMENTED;
        }

        if ((r->read(r->read_data, &size_of_lengths, 1, &err) != 1) || err) {
            return err;
        }

        tmp[i++] = size_of_lengths;

        if ((r->read(r->read_data, &consistency_flags, 1, &err) != 1) || err) {
            return err;
        }

        tmp[i++] = consistency_flags;

        err = read_HDF5_offset(r, size_of_offsets, &base_address, tmp, &i);
        if (err) {
            return err;
        }

        err = read_HDF5_offset(r, size_of_offsets, &superblock_extension_address, tmp, &i);
        if (err) {
            return err;
        }

        err = read_HDF5_offset(r, size_of_offsets, &end_of_file_address, tmp, &i);
        if (err) {
            return err;
        }
    }
    else if (version_of_superblock == 0 || version_of_superblock == 1) {
        char skip[4];
        unsigned long file_free_space_info;
        unsigned char version_of_file_free_space, version_of_root_group_symbol_table, version_number_shared_header, ch;

        if ((r->read(r->read_data, &version_of_file_free_space, 1, &err) != 1) || err)
            return err;
        tmp[i++] = version_of_file_free_space;

        if ((r->read(r->read_data, &version_of_root_group_symbol_table, 1, &err) != 1) || err)
            return err;
        tmp[i++] = version_of_root_group_symbol_table;

        if ((r->read(r->read_data, &ch, 1, &err) != 1) || err)
            return err; /* reserved */
        tmp[i++] = ch;

        if ((r->read(r->read_data, &version_number_shared_header, 1, &err) != 1) || err)
            return err;
        tmp[i++] = version_number_shared_header;

        if ((r->read(r->read_data, &size_of_offsets, 1, &err) != 1) || err)
            return err;
        tmp[i++] = size_of_offsets;
        if (size_of_offsets > 8) {
            grib_context_log(c, GRIB_LOG_ERROR, "read_HDF5: invalid size_of_offsets: %ld, only <= 8 is supported", (long)size_of_offsets);
            return GRIB_NOT_IMPLEMENTED;
        }

        if ((r->read(r->read_data, &size_of_lengths, 1, &err) != 1) || err)
            return err;
        tmp[i++] = size_of_lengths;

        if ((r->read(r->read_data, &ch, 1, &err) != 1) || err)
            return err; /*reserved*/
        tmp[i++] = ch;

        if ((r->read(r->read_data, &skip, 4, &err) != 4) || err)
            return err; /* Group Leaf/Internal Node K: 4 bytes */
        tmp[i++] = skip[0];
        tmp[i++] = skip[1];
        tmp[i++] = skip[2];
        tmp[i++] = skip[3];

        if ((r->read(r->read_data, &skip, 4, &err) != 4) || err)
            return err; /* consistency_flags: 4 bytes */
        tmp[i++] = skip[0];
        tmp[i++] = skip[1];
        tmp[i++] = skip[2];
        tmp[i++] = skip[3];

        if (version_of_superblock == 1) {
            /* Indexed storage internal node K and reserved: only in version 1 of superblock */
            if ((r->read(r->read_data, &skip, 4, &err) != 4) || err)
                return err;
            tmp[i++] = skip[0];
            tmp[i++] = skip[1];
            tmp[i++] = skip[2];
            tmp[i++] = skip[3];
        }

        err = read_HDF5_offset(r, size_of_offsets, &base_address, tmp, &i);
        if (err)
            return err;

        err = read_HDF5_offset(r, size_of_offsets, &file_free_space_info, tmp, &i);
        if (err)
            return err;

        err = read_HDF5_offset(r, size_of_offsets, &end_of_file_address, tmp, &i);
        if (err)
            return err;
    }
    else {
        grib_context_log(c, GRIB_LOG_ERROR, "read_HDF5: invalid version of superblock: %ld", (long)version_of_superblock);
        return GRIB_NOT_IMPLEMENTED;
    }

    ECCODES_ASSERT(i <= sizeof(tmp));
    return read_the_rest(r, end_of_file_address, tmp, i, 0, 0);
}

static int read_WRAP(reader* r)
{
    /*
     * See: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock
     */
    unsigned char tmp[36]; /* Should be enough */
    unsigned char buf[8];

    unsigned long long length = 0;

    int i   = 0, j;
    int err = 0;

    tmp[i++] = 'W';
    tmp[i++] = 'R';
    tmp[i++] = 'A';
    tmp[i++] = 'P';

    if ((r->read(r->read_data, buf, 8, &err) != 8) || err) {
        return err;
    }

    for (j = 0; j < 8; j++) {
        length <<= 8;
        length |= buf[j];
        tmp[i++] = buf[j];
    }

    ECCODES_ASSERT(i <= sizeof(tmp));
    return read_the_rest(r, length, tmp, i, 1, 0);
}

static int read_BUFR(reader* r, int no_alloc)
{
    /* unsigned char tmp[65536];*/ /* Should be enough */
    size_t length      = 0;
    long edition       = 0;
    int err            = 0;
    int i              = 0, j;
    size_t buflen      = 2048;
    unsigned char* tmp = NULL;
    grib_context* c    = NULL;
    grib_buffer* buf   = NULL;

    /*TODO proper context*/
    c   = grib_context_get_default();
    tmp = (unsigned char*)malloc(buflen);
    if (!tmp)
        return GRIB_OUT_OF_MEMORY;
    buf           = grib_new_buffer(c, tmp, buflen);
    buf->property = CODES_MY_BUFFER;
    r->offset     = r->tell(r->read_data) - 4;

    tmp[i++] = 'B';
    tmp[i++] = 'U';
    tmp[i++] = 'F';
    tmp[i++] = 'R';

    for (j = 0; j < 3; j++) {
        if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
            return err;

        length <<= 8;
        length |= tmp[i];
        i++;
    }

    if (length == 0) {
        grib_buffer_delete(c, buf);
        return GRIB_INVALID_MESSAGE;
    }

    /* Edition number */
    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
        return err;

    edition = tmp[i++];

    /* ECCODES_ASSERT(edition != 1); */

    switch (edition) {
        case 0:
        case 1: {
            int n;
            size_t sec1len = 0;
            size_t sec2len = 0;
            size_t sec3len = 0;
            size_t sec4len = 0;
            unsigned long flags;

            sec1len = length;

            /* table version */
            if (r->read(r->read_data, &tmp[i++], 1, &err) != 1 || err)
                return err;
            /* center */
            if (r->read(r->read_data, &tmp[i++], 1, &err) != 1 || err)
                return err;
            /* update */
            if (r->read(r->read_data, &tmp[i++], 1, &err) != 1 || err)
                return err;
            /* flags */
            if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                return err;
            flags = tmp[i++];


            GROW_BUF_IF_REQUIRED(sec1len + 4 + 3);

            /* Read section 1. 3 = length, 5 = table,center,process,flags */

            n = sec1len - 8; /* Just a guess */
            if ((r->read(r->read_data, tmp + i, n, &err) != n) || err)
                return err;

            i += n;

            if (flags & (1 << 7)) {
                /* Section 2 */
                for (j = 0; j < 3; j++) {
                    if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                        return err;

                    sec2len <<= 8;
                    sec2len |= tmp[i];
                    i++;
                }

                GROW_BUF_IF_REQUIRED(sec1len + sec2len + 4 + 3);

                /* Read section 2 */
                if ((r->read(r->read_data, tmp + i, sec2len - 3, &err) != sec2len - 3) || err)
                    return err;
                i += sec2len - 3;
            }


            /* Section 3 */
            for (j = 0; j < 3; j++) {
                if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                    return err;

                sec3len <<= 8;
                sec3len |= tmp[i];
                i++;
            }

            GROW_BUF_IF_REQUIRED(sec1len + sec2len + sec3len + 4 + 3);

            /* Read section 3 */
            if (sec3len < 5) {
                return GRIB_INVALID_MESSAGE; // ECC-1778
            }
            if (sec3len > 10'000'000) {
                return GRIB_INVALID_MESSAGE; // ECC-1938
            }
            if ((r->read(r->read_data, tmp + i, sec3len - 3, &err) != sec3len - 3) || err)
                return err;
            i += sec3len - 3;

            for (j = 0; j < 3; j++) {
                if (r->read(r->read_data, &tmp[i], 1, &err) != 1 || err)
                    return err;

                sec4len <<= 8;
                sec4len |= tmp[i];
                i++;
            }

            /* fprintf(stderr," sec1len=%d sec2len=%d sec3len=%d sec4len=%d\n",sec1len, sec2len,sec3len,sec4len); */
            length = 4 + sec1len + sec2len + sec3len + sec4len + 4;
            /* fprintf(stderr,"length = %d i = %d\n",length,i); */
        } break;
        case 2:
        case 3:
        case 4:
            break;
        default:
            r->seek_from_start(r->read_data, r->offset + 4);
            grib_buffer_delete(c, buf);
            return GRIB_UNSUPPORTED_EDITION;
    }

    /* ECCODES_ASSERT(i <= sizeof(tmp)); */
    err = read_the_rest(r, length, tmp, i, /*check7777=*/1, no_alloc);
    if (err)
        r->seek_from_start(r->read_data, r->offset + 4);

    grib_buffer_delete(c, buf);

    return err;
}

static int ecc_read_any(reader* r, int no_alloc, int grib_ok, int bufr_ok, int hdf5_ok, int wrap_ok)
{
    unsigned char c;
    int err             = 0;
    unsigned long magic = 0;

    while (r->read(r->read_data, &c, 1, &err) == 1 && err == 0) {
        magic <<= 8;
        magic |= c;

        switch (magic & 0xffffffff) {
            case GRIB:
                if (grib_ok) {
                    err = read_GRIB(r, no_alloc);
                    return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */
                }
                break;

            case BUFR:
                if (bufr_ok) {
                    err = read_BUFR(r, no_alloc);
                    return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */
                }
                break;

            case HDF5:
                if (hdf5_ok) {
                    err = read_HDF5(r);
                    return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */
                }
                break;

            case WRAP:
                if (wrap_ok) {
                    err = read_WRAP(r);
                    return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */
                }
                break;

            case BUDG:
                if (grib_ok) {
                    err = read_PSEUDO(r, "BUDG", no_alloc);
                    return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */
                }
                break;
            case DIAG:
                if (grib_ok) {
                    err = read_PSEUDO(r, "DIAG", no_alloc);
                    return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */
                }
                break;
            case TIDE:
                if (grib_ok) {
                    err = read_PSEUDO(r, "TIDE", no_alloc);
                    return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */
                }
                break;
        }
    }

    return err;
}

static int read_any(reader* r, int no_alloc, int grib_ok, int bufr_ok, int hdf5_ok, int wrap_ok)
{
    int result = 0;

#ifndef ECCODES_EACH_THREAD_OWN_FILE
    /* If several threads can open the same file, then we need the locks
     * so each thread gets its own message. Otherwise if threads are passed
     * different files, then the lock is not needed
     */
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex1);
#endif

    result = ecc_read_any(r, no_alloc, grib_ok, bufr_ok, hdf5_ok, wrap_ok);

#ifndef ECCODES_EACH_THREAD_OWN_FILE
    GRIB_MUTEX_UNLOCK(&mutex1);
#endif
    return result;
}

static int read_any_gts(reader* r)
{
    unsigned char c;
    int err                 = 0;
    unsigned char* buffer   = NULL;
    unsigned long magic     = 0;
    unsigned long start     = 0x010d0d0a; /* SOH CR CR LF */
    unsigned long theEnd    = 0x0d0d0a03; /* CR CR LF ETX */
    unsigned char tmp[16384] = {0,}; /* See ECC-735 */
    size_t message_size = 0;
    size_t already_read = 0;
    int i               = 0;

    while (r->read(r->read_data, &c, 1, &err) == 1 && err == 0) {
        magic <<= 8;
        magic |= c;
        magic &= 0xffffffff;

        if (magic == start) {
            tmp[i++] = 0x01;
            tmp[i++] = 0x0d;
            tmp[i++] = 0x0d;
            tmp[i++] = 0x0a;

            r->offset = r->tell(r->read_data) - 4;

            if (r->read(r->read_data, &tmp[i], 6, &err) != 6 || err)
                return err == GRIB_END_OF_FILE ? GRIB_PREMATURE_END_OF_FILE : err; /* Premature EOF */

            if (tmp[7] != 0x0d || tmp[8] != 0x0d || tmp[9] != 0x0a) {
                r->seek(r->read_data, -6);
                continue;
            }
            magic        = 0;
            already_read = 10;
            message_size = already_read;
            while (r->read(r->read_data, &c, 1, &err) == 1 && err == 0) {
                message_size++;
                magic <<= 8;
                magic |= c;
                magic &= 0xffffffff;
                if (magic == theEnd) {
                    r->seek(r->read_data, already_read - message_size);
                    buffer = (unsigned char*)r->alloc(r->alloc_data, &message_size, &err);
                    if (!buffer)
                        return GRIB_OUT_OF_MEMORY;
                    if (err)
                        return err;
                    memcpy(buffer, tmp, already_read);
                    r->read(r->read_data, buffer + already_read, message_size - already_read, &err);
                    r->message_size = message_size;
                    return err;
                }
            }
        }
    }

    return err;
}

static int read_any_taf(reader* r)
{
    unsigned char c;
    int err                 = 0;
    unsigned char* buffer   = NULL;
    unsigned long magic     = 0;
    unsigned long start     = 0x54414620; // 4 chars: TAF plus a space
    unsigned char tmp[1000] = {0,}; /* Should be enough */
    size_t message_size = 0;
    size_t already_read = 0;
    int i               = 0;

    while (r->read(r->read_data, &c, 1, &err) == 1 && err == 0) {
        magic <<= 8;
        magic |= c;
        magic &= 0xffffffff;

        if (magic == start) {
            tmp[i++] = 0x54; //T
            tmp[i++] = 0x41; //A
            tmp[i++] = 0x46; //F
            tmp[i++] = 0x20; //space

            r->offset = r->tell(r->read_data) - 4;

            already_read = 4;
            message_size = already_read;
            while (r->read(r->read_data, &c, 1, &err) == 1 && err == 0) {
                message_size++;
                if (c == '=') {
                    r->seek(r->read_data, already_read - message_size);
                    buffer = (unsigned char*)r->alloc(r->alloc_data, &message_size, &err);
                    if (!buffer)
                        return GRIB_OUT_OF_MEMORY;
                    if (err)
                        return err;
                    memcpy(buffer, tmp, already_read);
                    r->read(r->read_data, buffer + already_read, message_size - already_read, &err);
                    r->message_size = message_size;
                    return err;
                }
            }
        }
    }

    return err;
}

static int read_any_metar(reader* r)
{
    unsigned char c;
    int err               = 0;
    unsigned char* buffer = NULL;
    unsigned long magic   = 0;
    unsigned long start   = 0x4d455441; // 4 chars: META
    unsigned char tmp[32] = {0,}; /* Should be enough */
    size_t message_size = 0;
    size_t already_read = 0;
    int i               = 0;

    while (r->read(r->read_data, &c, 1, &err) == 1 && err == 0) {
        magic <<= 8;
        magic |= c;
        magic &= 0xffffffff;

        if (magic == start) {
            if (r->read(r->read_data, &c, 1, &err) != 1 || err != 0)
                break;
            if (c == 'R') {
                tmp[i++] = 0x4d; // M
                tmp[i++] = 0x45; // E
                tmp[i++] = 0x54; // T
                tmp[i++] = 0x41; // A
                tmp[i++] = 'R';

                r->offset = r->tell(r->read_data) - 4;

                already_read = 5;
                message_size = already_read;
                while (r->read(r->read_data, &c, 1, &err) == 1 && err == 0) {
                    message_size++;
                    if (c == '=') {
                        r->seek(r->read_data, already_read - message_size);
                        buffer = (unsigned char*)r->alloc(r->alloc_data, &message_size, &err);
                        if (!buffer)
                            return GRIB_OUT_OF_MEMORY;
                        if (err)
                            return err;
                        memcpy(buffer, tmp, already_read);
                        r->read(r->read_data, buffer + already_read, message_size - already_read, &err);
                        r->message_size = message_size;
                        return err;
                    }
                }
            }
        }
    }

    return err;
}

off_t stdio_tell(void* data)
{
    FILE* f = (FILE*)data;
    return ftello(f);
}

int stdio_seek(void* data, off_t len)
{
    FILE* f = (FILE*)data;
    int err = 0;
    if (fseeko(f, len, SEEK_CUR))
        err = GRIB_IO_PROBLEM;
    return err;
}

int stdio_seek_from_start(void* data, off_t len)
{
    FILE* f = (FILE*)data;
    int err = 0;
    if (fseeko(f, len, SEEK_SET))
        err = GRIB_IO_PROBLEM;
    return err;
}

size_t stdio_read(void* data, void* buf, size_t len, int* err)
{
    FILE* f = (FILE*)data;
    size_t n;
    /* char iobuf[1024*1024]; */

    if (len == 0)
        return 0;

    /* setvbuf(f,iobuf,_IOFBF,sizeof(iobuf)); */
    n = fread(buf, 1, len, f);
    /* fprintf(stderr,"read %d = %x %c\n",1,(int)buf[0],buf[0]); */
    if (n != len) {
        /* fprintf(stderr,"Failed to read %d, only got %d\n",len,n); */
        *err = GRIB_IO_PROBLEM;
        if (feof(f))
            *err = GRIB_END_OF_FILE;
        if (ferror(f))
            *err = GRIB_IO_PROBLEM;
    }
    return n;
}

/*================== */
typedef struct user_buffer_t
{
    void* user_buffer;
    size_t buffer_size;
} user_buffer_t;

static void* user_provider_buffer(void* data, size_t* length, int* err)
{
    user_buffer_t* u = (user_buffer_t*)data;
    *length        = u->buffer_size;
    return u->user_buffer;
}

static int ecc_wmo_read_any_from_file(FILE* f, void* buffer, size_t* len, off_t* offset,
                                      int no_alloc, int grib_ok, int bufr_ok, int hdf5_ok, int wrap_ok)
{
    int err;
    user_buffer_t u;
    reader r;

    u.user_buffer = buffer;
    u.buffer_size = *len;

    r.read_data       = f;
    r.read            = &stdio_read;
    r.alloc_data      = &u;
    r.alloc           = &user_provider_buffer;
    r.headers_only    = 0;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.offset          = 0;
    r.message_size    = 0;

    err  = read_any(&r, no_alloc, grib_ok, bufr_ok, hdf5_ok, wrap_ok);
    *len = r.message_size;
    *offset = r.offset;

    return err;
}

int wmo_read_any_from_file(FILE* f, void* buffer, size_t* len)
{
    off_t offset = 0;
    return ecc_wmo_read_any_from_file(f, buffer, len, &offset, /*no_alloc=*/0, 1, 1, 1, 1);
}
int wmo_read_grib_from_file(FILE* f, void* buffer, size_t* len)
{
    off_t offset = 0;
    return ecc_wmo_read_any_from_file(f, buffer, len, &offset, /*no_alloc=*/0, 1, 0, 0, 0);
}
int wmo_read_bufr_from_file(FILE* f, void* buffer, size_t* len)
{
    off_t offset = 0;
    return ecc_wmo_read_any_from_file(f, buffer, len, &offset, /*no_alloc=*/0, 0, 1, 0, 0);
}

// Fast versions
int wmo_read_any_from_file_fast(FILE* f, size_t* msg_len, off_t* msg_offset)
{
    unsigned char buffer[64] = {0,};
    *msg_len = sizeof(buffer);
    return ecc_wmo_read_any_from_file(f, buffer, msg_len, msg_offset, /*no_alloc=*/1, 1, 1, 1, 1);
}
int wmo_read_grib_from_file_fast(FILE* f, size_t* msg_len, off_t* msg_offset)
{
    unsigned char buffer[64] = {0,};
    *msg_len = sizeof(buffer);
    return ecc_wmo_read_any_from_file(f, buffer, msg_len, msg_offset, /*no_alloc=*/1, 1, 0, 0, 0);
}
int wmo_read_bufr_from_file_fast(FILE* f, size_t* msg_len, off_t* msg_offset)
{
    unsigned char buffer[64] = {0,};
    *msg_len = sizeof(buffer);
    return ecc_wmo_read_any_from_file(f, buffer, msg_len, msg_offset, /*no_alloc=*/1, 0, 1, 0, 0);
}
int wmo_read_gts_from_file_fast(FILE* f, size_t* msg_len, off_t* msg_offset)
{
    //TODO(masn): Needs proper implementation; no malloc
    //unsigned char buffer[1024] = {0,};
    void* mesg   = NULL;
    int err      = GRIB_SUCCESS;
    grib_context* c = grib_context_get_default();

    *msg_len = 1024; // sizeof(buffer)
    mesg = wmo_read_gts_from_file_malloc(f, 0, msg_len, msg_offset, &err);
    grib_context_free(c, mesg);
    return err;
}

int wmo_read_gts_from_file(FILE* f, void* buffer, size_t* len)
{
    int err;
    user_buffer_t u;
    reader r;

    u.user_buffer = buffer;
    u.buffer_size = *len;

    r.read_data       = f;
    r.read            = &stdio_read;
    r.alloc_data      = &u;
    r.alloc           = &user_provider_buffer;
    r.headers_only    = 0;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.offset          = 0;
    r.message_size    = 0;

    err  = read_any_gts(&r);
    *len = r.message_size;

    return err;
}

// int wmo_read_taf_from_file(FILE* f, void* buffer, size_t* len)
// {
//     int err;
//     user_buffer_t u;
//     reader r;
//     u.user_buffer = buffer;
//     u.buffer_size = *len;
//     r.read_data       = f;
//     r.read            = &stdio_read;
//     r.alloc_data      = &u;
//     r.alloc           = &user_provider_buffer;
//     r.headers_only    = 0;
//     r.seek            = &stdio_seek;
//     r.seek_from_start = &stdio_seek_from_start;
//     r.tell            = &stdio_tell;
//     r.offset          = 0;
//     r.message_size    = 0;
//     err  = read_any_taf(&r);
//     *len = r.message_size;
//     return err;
// }

// int wmo_read_metar_from_file(FILE* f, void* buffer, size_t* len)
// {
//     int err;
//     user_buffer_t u;
//     reader r;
//     u.user_buffer = buffer;
//     u.buffer_size = *len;
//     r.read_data       = f;
//     r.read            = &stdio_read;
//     r.alloc_data      = &u;
//     r.alloc           = &user_provider_buffer;
//     r.headers_only    = 0;
//     r.seek            = &stdio_seek;
//     r.seek_from_start = &stdio_seek_from_start;
//     r.tell            = &stdio_tell;
//     r.offset          = 0;
//     r.message_size    = 0;
//     err  = read_any_metar(&r);
//     *len = r.message_size;
//     return err;
// }

/*================== */

typedef struct stream_struct
{
    void* stream_data;
    long (*stream_proc)(void*, void* buffer, long len);

} stream_struct;

static off_t stream_tell(void* data)
{
    return 0;
}

static int stream_seek(void* data, off_t len)
{
    return 0;
}
static size_t stream_read(void* data, void* buffer, size_t len, int* err)
{
    stream_struct* s = (stream_struct*)data;
    long n           = 0;

    if (len > LONG_MAX) {
        /* size_t cannot be coded into long */
        *err = GRIB_INTERNAL_ERROR;
        return 0;
    }

    n = s->stream_proc(s->stream_data, buffer, len);
    if (n != len) {
        *err = GRIB_IO_PROBLEM;
        if (n == -1)
            *err = GRIB_END_OF_FILE;
    }
    return n;
}

/*================== */


static void* allocate_buffer(void* data, size_t* length, int* err)
{
    alloc_buffer* u = (alloc_buffer*)data;
    u->buffer       = malloc(*length);
    u->size         = *length;
    if (u->buffer == NULL)
        *err = GRIB_OUT_OF_MEMORY; /* Cannot allocate buffer */
    return u->buffer;
}

int wmo_read_any_from_stream(void* stream_data, long (*stream_proc)(void*, void* buffer, long len), void* buffer, size_t* len)
{
    int err;
    stream_struct s;
    user_buffer_t u;
    reader r;

    s.stream_data = stream_data;
    s.stream_proc = stream_proc;

    u.user_buffer = buffer;
    u.buffer_size = *len;

    r.message_size    = 0;
    r.offset          = 0;
    r.read_data       = &s;
    r.read            = &stream_read;
    r.seek            = &stream_seek;
    r.seek_from_start = &stream_seek;
    r.tell            = &stream_tell;
    r.alloc_data      = &u;
    r.alloc           = &user_provider_buffer;
    r.headers_only    = 0;

    err  = read_any(&r, /*no_alloc=*/0, 1, 1, 1, 1);
    *len = r.message_size;

    return err;
}

/* This function allocates memory for the result so the user is responsible for freeing it */
void* wmo_read_any_from_stream_malloc(void* stream_data, long (*stream_proc)(void*, void* buffer, long len), size_t* size, int* err)
{
    alloc_buffer u;
    stream_struct s;
    reader r;

    u.buffer = NULL;

    s.stream_data = stream_data;
    s.stream_proc = stream_proc;

    r.message_size    = 0;
    r.offset          = 0;
    r.read_data       = &s;
    r.read            = &stream_read;
    r.seek            = &stream_seek;
    r.seek_from_start = &stream_seek;
    r.tell            = &stream_tell;
    r.alloc_data      = &u;
    r.alloc           = &allocate_buffer;
    r.headers_only    = 0;

    *err  = read_any(&r, /*no_alloc=*/0, 1, 1, 1, 1);
    *size = r.message_size;

    return u.buffer;
}

/*================== */

/* This function allocates memory for the result so the user is responsible for freeing it */
void* wmo_read_gts_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err)
{
    alloc_buffer u;
    reader r;

    u.buffer = NULL;
    r.offset = 0;

    r.message_size    = 0;
    r.read_data       = f;
    r.read            = &stdio_read;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.alloc_data      = &u;
    r.alloc           = &allocate_buffer;
    r.headers_only    = headers_only;

    *err    = read_any_gts(&r);
    *size   = r.message_size;
    *offset = r.offset;

    return u.buffer;
}

/* This function allocates memory for the result so the user is responsible for freeing it */
void* wmo_read_taf_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err)
{
    alloc_buffer u;
    reader r;

    u.buffer = NULL;

    r.read_data       = f;
    r.read            = &stdio_read;
    r.alloc_data      = &u;
    r.alloc           = &allocate_buffer;
    r.headers_only    = headers_only;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.offset          = 0;
    r.message_size    = 0;

    *err    = read_any_taf(&r);
    *size   = r.message_size;
    *offset = r.offset;

    return u.buffer;
}

/* This function allocates memory for the result so the user is responsible for freeing it */
void* wmo_read_metar_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err)
{
    alloc_buffer u;
    reader r;

    u.buffer = NULL;

    r.read_data       = f;
    r.read            = &stdio_read;
    r.alloc_data      = &u;
    r.alloc           = &allocate_buffer;
    r.headers_only    = headers_only;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.offset          = 0;
    r.message_size    = 0;

    *err    = read_any_metar(&r);
    *size   = r.message_size;
    *offset = r.offset;

    return u.buffer;
}

/* This function allocates memory for the result so the user is responsible for freeing it */
static void* ecc_wmo_read_any_from_file_malloc(FILE* f, int* err, size_t* size, off_t* offset,
                                            int grib_ok, int bufr_ok, int hdf5_ok, int wrap_ok, int headers_only)
{
    alloc_buffer u;
    reader r;

    u.buffer = NULL;
    u.size   = 0;

    r.message_size    = 0;
    r.read_data       = f;
    r.read            = &stdio_read;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.alloc_data      = &u;
    r.alloc           = &allocate_buffer;
    r.headers_only    = headers_only;
    r.offset          = 0;

    *err = read_any(&r, /*no_alloc=*/0, grib_ok, bufr_ok, hdf5_ok, wrap_ok);

    *size   = r.message_size;
    *offset = r.offset;

    return u.buffer;
}

/* This function allocates memory for the result so the user is responsible for freeing it */
void* wmo_read_any_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err)
{
    return ecc_wmo_read_any_from_file_malloc(f, err, size, offset, 1, 1, 1, 1, headers_only);
}
/* This function allocates memory for the result so the user is responsible for freeing it */
void* wmo_read_grib_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err)
{
    return ecc_wmo_read_any_from_file_malloc(f, err, size, offset, 1, 0, 0, 0, headers_only);
}
/* This function allocates memory for the result so the user is responsible for freeing it */
void* wmo_read_bufr_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err)
{
    return ecc_wmo_read_any_from_file_malloc(f, err, size, offset, 0, 1, 0, 0, headers_only);
}

/* ======================================= */

typedef struct context_alloc_buffer
{
    grib_context* ctx;
    void* buffer;
    size_t length;
} context_alloc_buffer;

static void* context_allocate_buffer(void* data, size_t* length, int* err)
{
    context_alloc_buffer* u = (context_alloc_buffer*)data;
    u->buffer               = grib_context_malloc(u->ctx, *length);
    u->length               = *length;

    if (u->buffer == NULL)
        *err = GRIB_OUT_OF_MEMORY; /* Cannot allocate buffer */
    return u->buffer;
}

int grib_read_any_headers_only_from_file(grib_context* ctx, FILE* f, void* buffer, size_t* len)
{
    int err;
    user_buffer_t u;
    reader r;

    u.user_buffer = buffer;
    u.buffer_size = *len;

    r.read_data       = f;
    r.read            = &stdio_read;
    r.alloc_data      = &u;
    r.alloc           = &user_provider_buffer;
    r.headers_only    = 1;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.offset          = 0;
    r.message_size    = 0;

    err = read_any(&r, /*no_alloc=*/0, 1, ECCODES_READS_BUFR, ECCODES_READS_HDF5, ECCODES_READS_WRAP);

    *len = r.message_size;

    return err;
}

int grib_read_any_from_file(grib_context* ctx, FILE* f, void* buffer, size_t* len)
{
    int err;
    user_buffer_t u;
    reader r;
    off_t offset;

    u.user_buffer = buffer;
    u.buffer_size = *len;

    r.read_data       = f;
    r.read            = &stdio_read;
    r.alloc_data      = &u;
    r.alloc           = &user_provider_buffer;
    r.headers_only    = 0;
    r.seek            = &stdio_seek;
    r.seek_from_start = &stdio_seek_from_start;
    r.tell            = &stdio_tell;
    r.offset          = 0;
    r.message_size    = 0;

    offset = ftello(f);

    err = read_any(&r, /*no_alloc=*/0, 1, ECCODES_READS_BUFR, ECCODES_READS_HDF5, ECCODES_READS_WRAP);

    if (err == GRIB_BUFFER_TOO_SMALL) {
        if (fseeko(f, offset, SEEK_SET))
            err = GRIB_IO_PROBLEM;
    }

    *len = r.message_size;

    return err;
}

/* ======================================= */
typedef struct memory_read_data
{
    unsigned char* data;
    size_t data_len;
} memory_read_data;

static off_t memory_tell(void* data)
{
    return 0;
}

static int memory_seek(void* data, off_t len)
{
    return 0;
}

static size_t memory_read(void* data, void* buf, size_t len, int* err)
{
    memory_read_data* m = (memory_read_data*)data;

    if (len == 0) {
        *err = GRIB_END_OF_FILE;
        return 0;
    }
    else {
        size_t l = len > m->data_len ? m->data_len : len;
        memcpy(buf, m->data, l);
        m->data_len -= l;
        m->data += l;
        return l;
    }
}

int grib_read_any_from_memory_alloc(grib_context* ctx, unsigned char** data, size_t* data_length, void** buffer, size_t* length)
{
    int err;
    memory_read_data m;
    context_alloc_buffer u;
    reader r;

    m.data     = *data;
    m.data_len = *data_length;

    u.buffer = NULL;
    u.length = 0;
    u.ctx    = ctx ? ctx : grib_context_get_default();

    r.read_data       = &m;
    r.read            = &memory_read;
    r.alloc_data      = &u;
    r.alloc           = &context_allocate_buffer;
    r.headers_only    = 0;
    r.seek            = &memory_seek;
    r.seek_from_start = &memory_seek;
    r.tell            = &memory_tell;
    r.offset          = 0;
    r.message_size    = 0;

    err     = read_any(&r, /*no_alloc=*/0, 1, ECCODES_READS_BUFR, ECCODES_READS_HDF5, ECCODES_READS_WRAP);
    *buffer = u.buffer;
    *length = u.length;

    *data_length = m.data_len;
    *data        = m.data;

    return err;
}

int grib_read_any_from_memory(grib_context* ctx, unsigned char** data, size_t* data_length, void* buffer, size_t* len)
{
    int err;
    memory_read_data m;
    user_buffer_t u;
    reader r;

    m.data     = *data;
    m.data_len = *data_length;

    u.user_buffer = buffer;
    u.buffer_size = *len;

    r.read_data       = &m;
    r.read            = &memory_read;
    r.alloc_data      = &u;
    r.alloc           = &user_provider_buffer;
    r.headers_only    = 0;
    r.seek            = &memory_seek;
    r.seek_from_start = &memory_seek;
    r.tell            = &memory_tell;
    r.offset          = 0;
    r.message_size    = 0;

    err  = read_any(&r, /*no_alloc=*/0, 1, ECCODES_READS_BUFR, ECCODES_READS_HDF5, ECCODES_READS_WRAP);
    *len = r.message_size;
    *data_length = m.data_len;
    *data        = m.data;

    return err;
}

int grib_count_in_file(grib_context* c, FILE* f, int* n)
{
    int err = 0;
    *n      = 0;
    if (!c)
        c = grib_context_get_default();

    if (c->multi_support_on) {
        /* GRIB-395 */
        grib_handle* h = NULL;
        while ((h = grib_handle_new_from_file(c, f, &err)) != NULL) {
            grib_handle_delete(h);
            (*n)++;
        }
    }
    else {
        size_t size  = 0;
        off_t offset = 0;
        while ((err = wmo_read_any_from_file_fast(f, &size, &offset)) == GRIB_SUCCESS) {
            (*n)++;
        }
    }

    rewind(f);

    return err == GRIB_END_OF_FILE ? 0 : err;
}

int grib_count_in_filename(grib_context* c, const char* filename, int* n)
{
    int err  = 0;
    FILE* fp = NULL;
    if (!c)
        c = grib_context_get_default();
    fp = fopen(filename, "rb");
    if (!fp) {
        grib_context_log(c, GRIB_LOG_ERROR, "grib_count_in_filename: Unable to read file \"%s\"", filename);
        perror(filename);
        return GRIB_IO_PROBLEM;
    }
    err = grib_count_in_file(c, fp, n);
    fclose(fp);
    return err;
}

typedef int (*decoder_proc)(FILE* f, size_t* size, off_t* offset);

static decoder_proc get_reader_for_product(ProductKind product)
{
    decoder_proc decoder = NULL;
    if      (product == PRODUCT_GRIB) decoder = &wmo_read_grib_from_file_fast;
    else if (product == PRODUCT_BUFR) decoder = &wmo_read_bufr_from_file_fast;
    else if (product == PRODUCT_GTS)  decoder = &wmo_read_gts_from_file_fast;
    else if (product == PRODUCT_ANY)  decoder = &wmo_read_any_from_file_fast;
    return decoder;
}

static int count_product_in_file(grib_context* c, FILE* f, ProductKind product, int* count)
{
    int err = 0;
    decoder_proc decoder = NULL;

    *count = 0;
    if (!c) c = grib_context_get_default();
    decoder = get_reader_for_product(product);

    if (!decoder) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Not supported for given product", __func__);
        return GRIB_INVALID_ARGUMENT;
    }

    if (c->multi_support_on && product == PRODUCT_GRIB) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Multi-field GRIBs not supported", __func__);
        err = GRIB_NOT_IMPLEMENTED;
    }
    else {
        size_t size  = 0;
        off_t offset = 0;
        while ((err = decoder(f, &size, &offset)) == GRIB_SUCCESS) {
            (*count)++;
        }
        rewind(f);
    }

    return err == GRIB_END_OF_FILE ? 0 : err;
}

static int codes_extract_offsets_malloc_internal(
    grib_context* c, const char* filename, ProductKind product,
    off_t** offsets, size_t** sizes,
    int* number_of_elements,
    int strict_mode)
{
    int err      = 0;
    size_t size  = 0;
    off_t offset = 0;
    int num_messages = 0, i = 0;
    decoder_proc decoder = NULL;
    FILE* f = NULL;

    decoder = get_reader_for_product(product);
    if (!decoder) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Not supported for given product", __func__);
        return GRIB_INVALID_ARGUMENT;
    }
    if (!c) c = grib_context_get_default();

    if (path_is_directory(filename)) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: \"%s\" is a directory", __func__, filename);
        return GRIB_IO_PROBLEM;
    }
    f = fopen(filename, "rb");
    if (!f) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to read file \"%s\"", __func__, filename);
        perror(filename);
        return GRIB_IO_PROBLEM;
    }

    err = count_product_in_file(c, f, product, &num_messages);
    if (err) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to count messages (%s)", __func__, grib_get_error_message(err));
        fclose(f);
        return err;
    }
    *number_of_elements = num_messages;
    if (num_messages == 0) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: No messages in file", __func__);
        fclose(f);
        return GRIB_INVALID_MESSAGE;
    }
    *offsets = (off_t*)calloc(num_messages, sizeof(off_t));
    if (!*offsets) {
        fclose(f);
        return GRIB_OUT_OF_MEMORY;
    }
    if (sizes) {
        *sizes = (size_t*)calloc(num_messages, sizeof(size_t));
        if (!*sizes) {
            fclose(f);
            return GRIB_OUT_OF_MEMORY;
        }
    }

    i = 0;
    while (err != GRIB_END_OF_FILE) {
        if (i >= num_messages)
            break;

        err = decoder(f, &size, &offset);
        if (!err) {
            (*offsets)[i] = offset;
            if (sizes) {
                (*sizes)[i] = size;
            }
        }
        else {
            if (strict_mode && (err != GRIB_END_OF_FILE && err != GRIB_PREMATURE_END_OF_FILE)) {
                fclose(f);
                return GRIB_DECODING_ERROR;
            }
        }
        ++i;
    }

    fclose(f);
    return err;
}

// The lagacy version only did the offsets
int codes_extract_offsets_malloc(
    grib_context* c, const char* filename, ProductKind product,
    off_t** offsets, int* number_of_elements, int strict_mode)
{
    // Call without doing the message sizes
    return codes_extract_offsets_malloc_internal(c, filename, product, offsets, NULL, number_of_elements, strict_mode);
}

// New function does both message offsets and sizes
int codes_extract_offsets_sizes_malloc(
    grib_context* c, const char* filename, ProductKind product,
    off_t** offsets, size_t** sizes, int* number_of_elements, int strict_mode)
{
    return codes_extract_offsets_malloc_internal(c, filename, product, offsets, sizes, number_of_elements, strict_mode);
}
