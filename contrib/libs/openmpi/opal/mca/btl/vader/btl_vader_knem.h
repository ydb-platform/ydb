/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(BTL_VADER_KNEM_H)
#define BTL_VADER_KNEM_H

#if OPAL_BTL_VADER_HAVE_KNEM

#error #include <knem_io.h>
#include <sys/mman.h>

/* At this time only knem requires a registration of "RDMA" buffers */
struct mca_btl_base_registration_handle_t {
    uint64_t cookie;
    intptr_t base_addr;
};

struct mca_btl_vader_registration_handle_t {
    mca_rcache_base_registration_t base;
    mca_btl_base_registration_handle_t btl_handle;
};
typedef struct mca_btl_vader_registration_handle_t mca_btl_vader_registration_handle_t;

int mca_btl_vader_knem_init (void);
int mca_btl_vader_knem_fini (void);
int mca_btl_vader_knem_progress (void);

#endif /* OPAL_BTL_VADER_HAVE_KNEM */

#endif /* defined(BTL_VADER_KNEM_H) */
