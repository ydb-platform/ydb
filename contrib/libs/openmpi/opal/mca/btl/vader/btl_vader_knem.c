/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "btl_vader.h"

#if OPAL_BTL_VADER_HAVE_KNEM

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "opal/util/show_help.h"

OBJ_CLASS_INSTANCE(mca_btl_vader_registration_handle_t, mca_rcache_base_registration_t, NULL, NULL);

static int mca_btl_vader_knem_reg (void *reg_data, void *base, size_t size,
                                   mca_rcache_base_registration_t *reg)
{
    mca_btl_vader_registration_handle_t *knem_reg = (mca_btl_vader_registration_handle_t *) reg;
    struct knem_cmd_create_region knem_cr;
    struct knem_cmd_param_iovec knem_iov;

    knem_iov.base = (uintptr_t) base;
    knem_iov.len = size;

    knem_cr.iovec_array = (uintptr_t) &knem_iov;
    knem_cr.iovec_nr = 1;
    knem_cr.protection = 0;

    if (reg->access_flags & (MCA_RCACHE_ACCESS_LOCAL_WRITE | MCA_RCACHE_ACCESS_REMOTE_WRITE)) {
        knem_cr.protection |= PROT_WRITE;
    }

    if (reg->access_flags & MCA_RCACHE_ACCESS_REMOTE_READ) {
        knem_cr.protection |= PROT_READ;
    }

    /* Vader will explicitly destroy this cookie */
    knem_cr.flags = 0;
    if (OPAL_UNLIKELY(ioctl(mca_btl_vader.knem_fd, KNEM_CMD_CREATE_REGION, &knem_cr) < 0)) {
        return OPAL_ERROR;
    }

    knem_reg->btl_handle.cookie = knem_cr.cookie;
    knem_reg->btl_handle.base_addr = (intptr_t) base;

    return OPAL_SUCCESS;
}

static int mca_btl_vader_knem_dereg (void *reg_data, mca_rcache_base_registration_t *reg)
{
    mca_btl_vader_registration_handle_t *knem_reg = (mca_btl_vader_registration_handle_t *) reg;

    /* NTH: explicity ignore the return code. Don't care about this cookie anymore anyway. */
    (void) ioctl(mca_btl_vader.knem_fd, KNEM_CMD_DESTROY_REGION, &knem_reg->btl_handle.cookie);

    return OPAL_SUCCESS;
}

static mca_btl_base_registration_handle_t *
mca_btl_vader_register_mem_knem (struct mca_btl_base_module_t* btl,
                                 struct mca_btl_base_endpoint_t *endpoint,
                                 void *base, size_t size, uint32_t flags)
{
    mca_btl_vader_t *vader_module = (mca_btl_vader_t *) btl;
    mca_btl_vader_registration_handle_t *reg = NULL;
    int access_flags = flags & MCA_BTL_REG_FLAG_ACCESS_ANY;
    int rc;

    rc = vader_module->knem_rcache->rcache_register (vader_module->knem_rcache, base, size, 0,
                                                     access_flags,
                                                     (mca_rcache_base_registration_t **) &reg);
    if (OPAL_UNLIKELY(OPAL_SUCCESS != rc)) {
        return NULL;
    }

    return &reg->btl_handle;
}

static int
mca_btl_vader_deregister_mem_knem (struct mca_btl_base_module_t *btl, struct mca_btl_base_registration_handle_t *handle)
{
    mca_btl_vader_t *vader_module = (mca_btl_vader_t *) btl;
    mca_btl_vader_registration_handle_t *reg =
        (mca_btl_vader_registration_handle_t *)((intptr_t) handle - offsetof (mca_btl_vader_registration_handle_t, btl_handle));

    vader_module->knem_rcache->rcache_deregister (vader_module->knem_rcache, &reg->base);

    return OPAL_SUCCESS;
}

int mca_btl_vader_knem_init (void)
{
    mca_rcache_base_resources_t rcache_resources = {
        .cache_name = "vader", .reg_data = NULL,
        .sizeof_reg = sizeof (mca_btl_vader_registration_handle_t),
        .register_mem = mca_btl_vader_knem_reg,
        .deregister_mem = mca_btl_vader_knem_dereg
    };
    struct knem_cmd_info knem_info;
    int rc;

    /* Open the knem device.  Try to print a helpful message if we
       fail to open it. */
    mca_btl_vader.knem_fd = open("/dev/knem", O_RDWR);
    if (mca_btl_vader.knem_fd < 0) {
	if (EACCES == errno) {
	    struct stat sbuf;
	    if (0 != stat("/dev/knem", &sbuf)) {
		sbuf.st_mode = 0;
	    }
	    opal_show_help("help-btl-vader.txt", "knem permission denied",
			   true, opal_process_info.nodename, sbuf.st_mode);
	} else {
	    opal_show_help("help-btl-vader.txt", "knem fail open",
			   true, opal_process_info.nodename, errno,
			   strerror(errno));
	}

	return OPAL_ERR_NOT_AVAILABLE;
    }

    do {
	/* Check that the ABI if kernel module running is the same
	 * as what we were compiled against. */
        memset (&knem_info, 0, sizeof (knem_info));
	rc = ioctl(mca_btl_vader.knem_fd, KNEM_CMD_GET_INFO, &knem_info);
	if (rc < 0) {
	    opal_show_help("help-btl-vader.txt", "knem get ABI fail",
			   true, opal_process_info.nodename, errno,
			   strerror(errno));
	    break;
	}

	if (KNEM_ABI_VERSION != knem_info.abi) {
	    opal_show_help("help-btl-vader.txt", "knem ABI mismatch",
			   true, opal_process_info.nodename, KNEM_ABI_VERSION,
			   knem_info.abi);
	    break;
	}

	if (!(mca_btl_vader_component.knem_dma_min && (knem_info.features & KNEM_FEATURE_DMA))) {
	    /* disable DMA */
	    mca_btl_vader_component.knem_dma_min = UINT_MAX;
	}

	/* TODO: add async support */

	/* knem set up successfully */
	mca_btl_vader.super.btl_get = mca_btl_vader_get_knem;
	mca_btl_vader.super.btl_put = mca_btl_vader_put_knem;

        /* knem requires registration */
        mca_btl_vader.super.btl_register_mem = mca_btl_vader_register_mem_knem;
        mca_btl_vader.super.btl_deregister_mem = mca_btl_vader_deregister_mem_knem;
        mca_btl_vader.super.btl_registration_handle_size = sizeof (mca_btl_base_registration_handle_t);

        mca_btl_vader.knem_rcache = mca_rcache_base_module_create ("grdma", NULL,
                                                                   &rcache_resources);
        if (NULL == mca_btl_vader.knem_rcache) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

	return OPAL_SUCCESS;
    } while (0);

    mca_btl_vader_knem_fini ();

    return OPAL_ERR_NOT_AVAILABLE;;
}

int mca_btl_vader_knem_fini (void)
{
    if (-1 != mca_btl_vader.knem_fd) {
	close (mca_btl_vader.knem_fd);
	mca_btl_vader.knem_fd = -1;
    }

    if (mca_btl_vader.knem_rcache) {
        (void) mca_rcache_base_module_destroy (mca_btl_vader.knem_rcache);
        mca_btl_vader.knem_rcache = NULL;
    }

    return OPAL_SUCCESS;
}

int mca_btl_vader_knem_progress (void)
{
    /* NTH: does nothing until async support is added */
    return OPAL_SUCCESS;
}

#endif
