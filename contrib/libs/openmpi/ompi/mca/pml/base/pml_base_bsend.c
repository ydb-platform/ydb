/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "opal/threads/mutex.h"
#include "opal/threads/condition.h"
#include "ompi/datatype/ompi_datatype.h"
#include "opal/mca/allocator/base/base.h"
#include "opal/mca/allocator/allocator.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/pml_base_request.h"
#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "opal/mca/mpool/mpool.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */

static opal_mutex_t     mca_pml_bsend_mutex;      /* lock for thread safety */
static opal_condition_t mca_pml_bsend_condition;  /* condition variable to block on detach */
static mca_allocator_base_component_t* mca_pml_bsend_allocator_component;
static mca_allocator_base_module_t* mca_pml_bsend_allocator;  /* sub-allocator to manage users buffer */
static size_t           mca_pml_bsend_usersize;   /* user provided buffer size */
unsigned char          *mca_pml_bsend_userbase=NULL;/* user provided buffer base */
unsigned char          *mca_pml_bsend_base = NULL;/* adjusted base of user buffer */
unsigned char          *mca_pml_bsend_addr = NULL;/* current offset into user buffer */
static size_t           mca_pml_bsend_size;       /* adjusted size of user buffer */
static size_t           mca_pml_bsend_count;      /* number of outstanding requests */
static size_t           mca_pml_bsend_pagesz;     /* mmap page size */
static int              mca_pml_bsend_pagebits;   /* number of bits in pagesz */
static int32_t          mca_pml_bsend_init = 0;

/* defined in pml_base_open.c */
extern char *ompi_pml_base_bsend_allocator_name;

/*
 * Routine to return pages to sub-allocator as needed
 */
static void* mca_pml_bsend_alloc_segment(void *ctx, size_t *size_inout)
{
    void *addr;
    size_t size = *size_inout;
    if(mca_pml_bsend_addr + size > mca_pml_bsend_base + mca_pml_bsend_size) {
        return NULL;
    }
    /* allocate all that is left */
    size = mca_pml_bsend_size - (mca_pml_bsend_addr - mca_pml_bsend_base);
    addr = mca_pml_bsend_addr;
    mca_pml_bsend_addr += size;
    *size_inout = size;
    return addr;
}

/*
 * One time initialization at startup
 */
int mca_pml_base_bsend_init(bool thread_safe)
{
    size_t tmp;

    if(OPAL_THREAD_ADD_FETCH32(&mca_pml_bsend_init, 1) > 1)
        return OMPI_SUCCESS;

    /* initialize static objects */
    OBJ_CONSTRUCT(&mca_pml_bsend_mutex, opal_mutex_t);
    OBJ_CONSTRUCT(&mca_pml_bsend_condition, opal_condition_t);

    /* lookup name of the allocator to use for buffered sends */
    if(NULL == (mca_pml_bsend_allocator_component = mca_allocator_component_lookup(ompi_pml_base_bsend_allocator_name))) {
        return OMPI_ERR_BUFFER;
    }

    /* determine page size */
    tmp = mca_pml_bsend_pagesz = sysconf(_SC_PAGESIZE);
    mca_pml_bsend_pagebits = 0;
    while( tmp != 0 ) {
        tmp >>= 1;
        mca_pml_bsend_pagebits++;
    }
    return OMPI_SUCCESS;
}


/*
 * One-time cleanup at shutdown - release any resources.
 */
int mca_pml_base_bsend_fini(void)
{
    if(OPAL_THREAD_ADD_FETCH32(&mca_pml_bsend_init,-1) > 0)
        return OMPI_SUCCESS;

    if(NULL != mca_pml_bsend_allocator)
        mca_pml_bsend_allocator->alc_finalize(mca_pml_bsend_allocator);
    mca_pml_bsend_allocator = NULL;

    OBJ_DESTRUCT(&mca_pml_bsend_condition);
    OBJ_DESTRUCT(&mca_pml_bsend_mutex);
    return OMPI_SUCCESS;
}


/*
 * User-level call to attach buffer.
 */
int mca_pml_base_bsend_attach(void* addr, int size)
{
    int align;

    bool thread_safe = ompi_mpi_thread_multiple;
    if(NULL == addr || size <= 0) {
        return OMPI_ERR_BUFFER;
    }

    /* check for buffer already attached */
    OPAL_THREAD_LOCK(&mca_pml_bsend_mutex);
    if(NULL != mca_pml_bsend_allocator) {
        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
        return OMPI_ERR_BUFFER;
    }

    /* try to create an instance of the allocator - to determine thread safety level */
    mca_pml_bsend_allocator = mca_pml_bsend_allocator_component->allocator_init(thread_safe, mca_pml_bsend_alloc_segment, NULL, NULL);
    if(NULL == mca_pml_bsend_allocator) {
        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
        return OMPI_ERR_BUFFER;
    }

    /*
     * Save away what the user handed in.  This is done in case the
     * base and size are modified for alignment issues.
     */
    mca_pml_bsend_userbase = (unsigned char*)addr;
    mca_pml_bsend_usersize = size;
    /*
     * Align to pointer boundaries. The bsend overhead is large enough
     * to account for this.  Compute any alignment that needs to be done.
     */
    align = sizeof(void *) - ((size_t)addr & (sizeof(void *) - 1));

    /* setup local variables */
    mca_pml_bsend_base = (unsigned char *)addr + align;
    mca_pml_bsend_addr = (unsigned char *)addr + align;
    mca_pml_bsend_size = size - align;
    mca_pml_bsend_count = 0;
    OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
    return OMPI_SUCCESS;
}

/*
 * User-level call to detach buffer
 */
int mca_pml_base_bsend_detach(void* addr, int* size)
{
    OPAL_THREAD_LOCK(&mca_pml_bsend_mutex);

    /* is buffer attached */
    if(NULL == mca_pml_bsend_allocator) {
        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
        return OMPI_ERR_BUFFER;
    }

    /* wait on any pending requests */
    while(mca_pml_bsend_count != 0)
        opal_condition_wait(&mca_pml_bsend_condition, &mca_pml_bsend_mutex);

    /* free resources associated with the allocator */
    mca_pml_bsend_allocator->alc_finalize(mca_pml_bsend_allocator);
    mca_pml_bsend_allocator = NULL;

    /* return current settings */
    if(NULL != addr)
        *((void**)addr) = mca_pml_bsend_userbase;
    if(NULL != size)
        *size = (int)mca_pml_bsend_usersize;

    /* reset local variables */
    mca_pml_bsend_userbase = NULL;
    mca_pml_bsend_usersize = 0;
    mca_pml_bsend_base = NULL;
    mca_pml_bsend_addr = NULL;
    mca_pml_bsend_size = 0;
    mca_pml_bsend_count = 0;
    OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
    return OMPI_SUCCESS;
}


/*
 * pack send buffer into buffer
 */

int mca_pml_base_bsend_request_start(ompi_request_t* request)
{
    mca_pml_base_send_request_t* sendreq = (mca_pml_base_send_request_t*)request;
    struct iovec iov;
    unsigned int iov_count;
    size_t max_data;
    int rc;

    if(sendreq->req_bytes_packed > 0) {

        /* has a buffer been provided */
        OPAL_THREAD_LOCK(&mca_pml_bsend_mutex);
        if(NULL == mca_pml_bsend_addr) {
            sendreq->req_addr = NULL;
            OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
            return OMPI_ERR_BUFFER;
        }

        /* allocate a buffer to hold packed message */
        sendreq->req_addr = mca_pml_bsend_allocator->alc_alloc(
            mca_pml_bsend_allocator, sendreq->req_bytes_packed, 0);
        if(NULL == sendreq->req_addr) {
            /* release resources when request is freed */
            sendreq->req_base.req_pml_complete = true;
            OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
            return OMPI_ERR_BUFFER;
        }

        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);

        /* The convertor is already initialized in the begining so we just have to
         * pack the data in the newly allocated buffer.
         */
        iov.iov_base = (IOVBASE_TYPE*)sendreq->req_addr;
        iov.iov_len = sendreq->req_bytes_packed;
        iov_count = 1;
        max_data = iov.iov_len;
        if((rc = opal_convertor_pack( &sendreq->req_base.req_convertor,
                                      &iov,
                                      &iov_count,
                                      &max_data )) < 0) {
            return OMPI_ERROR;
        }

        /* setup convertor to point to packed buffer (at position zero) */
        opal_convertor_prepare_for_send( &sendreq->req_base.req_convertor, &(ompi_mpi_packed.dt.super),
                                         max_data, sendreq->req_addr );
        /* increment count of pending requests */
        mca_pml_bsend_count++;
    }

    return OMPI_SUCCESS;
}


/*
 * allocate buffer
 */

int mca_pml_base_bsend_request_alloc(ompi_request_t* request)
{
    mca_pml_base_send_request_t* sendreq = (mca_pml_base_send_request_t*)request;

    assert( sendreq->req_bytes_packed > 0 );

    /* has a buffer been provided */
    OPAL_THREAD_LOCK(&mca_pml_bsend_mutex);
    if(NULL == mca_pml_bsend_addr) {
        sendreq->req_addr = NULL;
        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
        return OMPI_ERR_BUFFER;
    }

    /* allocate a buffer to hold packed message */
    sendreq->req_addr = mca_pml_bsend_allocator->alc_alloc(
        mca_pml_bsend_allocator, sendreq->req_bytes_packed, 0);
    if(NULL == sendreq->req_addr) {
        /* release resources when request is freed */
        sendreq->req_base.req_pml_complete = true;
        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
        /* progress communications, with the hope that more resources
         *   will be freed */
        opal_progress();
        return OMPI_ERR_BUFFER;
    }

    /* increment count of pending requests */
    mca_pml_bsend_count++;
    OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);

    return OMPI_SUCCESS;
}

/*
 * allocate buffer
 */

void*  mca_pml_base_bsend_request_alloc_buf( size_t length )
{
    void* buf = NULL;
    /* has a buffer been provided */
    OPAL_THREAD_LOCK(&mca_pml_bsend_mutex);
    if(NULL == mca_pml_bsend_addr) {
        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
        return NULL;
    }

    /* allocate a buffer to hold packed message */
    buf = mca_pml_bsend_allocator->alc_alloc(
        mca_pml_bsend_allocator, length, 0);
    if(NULL == buf) {
        /* release resources when request is freed */
        OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
        /* progress communications, with the hope that more resources
         *   will be freed */
        opal_progress();
        return NULL;
    }

    /* increment count of pending requests */
    mca_pml_bsend_count++;
    OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);

    return buf;
}


/*
 *  Request completed - free buffer and decrement pending count
 */
int mca_pml_base_bsend_request_free(void* addr)
{
    /* remove from list of pending requests */
    OPAL_THREAD_LOCK(&mca_pml_bsend_mutex);

    /* free buffer */
    mca_pml_bsend_allocator->alc_free(mca_pml_bsend_allocator, addr);

    /* decrement count of buffered requests */
    if(--mca_pml_bsend_count == 0)
        opal_condition_signal(&mca_pml_bsend_condition);

    OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
    return OMPI_SUCCESS;
}



/*
 *  Request completed - free buffer and decrement pending count
 */
int mca_pml_base_bsend_request_fini(ompi_request_t* request)
{
    mca_pml_base_send_request_t* sendreq = (mca_pml_base_send_request_t*)request;
    if(sendreq->req_bytes_packed == 0 ||
       sendreq->req_addr == NULL ||
       sendreq->req_addr == sendreq->req_base.req_addr)
        return OMPI_SUCCESS;

    /* remove from list of pending requests */
    OPAL_THREAD_LOCK(&mca_pml_bsend_mutex);

    /* free buffer */
    mca_pml_bsend_allocator->alc_free(mca_pml_bsend_allocator, (void *)sendreq->req_addr);
    sendreq->req_addr = sendreq->req_base.req_addr;

    /* decrement count of buffered requests */
    if(--mca_pml_bsend_count == 0)
        opal_condition_signal(&mca_pml_bsend_condition);

    OPAL_THREAD_UNLOCK(&mca_pml_bsend_mutex);
    return OMPI_SUCCESS;
}


