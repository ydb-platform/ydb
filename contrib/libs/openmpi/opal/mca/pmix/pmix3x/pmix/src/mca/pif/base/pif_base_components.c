/*
 * Copyright (c) 2010-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2016 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "pmix_config.h"

#include "pmix_common.h"
#include "src/util/output.h"
#include "src/mca/mca.h"
#include "src/mca/pif/pif.h"
#include "src/mca/pif/base/base.h"
#include "src/mca/pif/base/static-components.h"

/* instantiate the global list of interfaces */
pmix_list_t pmix_if_list = {{0}};
bool pmix_if_do_not_resolve = false;
bool pmix_if_retain_loopback = false;

static int pmix_pif_base_register (pmix_mca_base_register_flag_t flags);
static int pmix_pif_base_open (pmix_mca_base_open_flag_t flags);
static int pmix_pif_base_close(void);
static void pmix_pif_construct(pmix_pif_t *obj);

static bool frameopen = false;

/* instance the pmix_pif_t object */
PMIX_CLASS_INSTANCE(pmix_pif_t, pmix_list_item_t, pmix_pif_construct, NULL);

PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, pif, NULL, pmix_pif_base_register, pmix_pif_base_open, pmix_pif_base_close,
                                mca_pif_base_static_components, 0);

static int pmix_pif_base_register (pmix_mca_base_register_flag_t flags)
{
    pmix_if_do_not_resolve = false;
    (void) pmix_mca_base_framework_var_register (&pmix_pif_base_framework, "do_not_resolve",
                                                 "If nonzero, do not attempt to resolve interfaces",
                                                 PMIX_MCA_BASE_VAR_TYPE_BOOL, NULL, 0, PMIX_MCA_BASE_VAR_FLAG_SETTABLE,
                                                 PMIX_INFO_LVL_9, PMIX_MCA_BASE_VAR_SCOPE_ALL_EQ,
                                                 &pmix_if_do_not_resolve);

    pmix_if_retain_loopback = false;
    (void) pmix_mca_base_framework_var_register (&pmix_pif_base_framework, "retain_loopback",
                                                 "If nonzero, retain loopback interfaces",
                                                 PMIX_MCA_BASE_VAR_TYPE_BOOL, NULL, 0, PMIX_MCA_BASE_VAR_FLAG_SETTABLE,
                                                 PMIX_INFO_LVL_9, PMIX_MCA_BASE_VAR_SCOPE_ALL_EQ,
                                                 &pmix_if_retain_loopback);

    return PMIX_SUCCESS;
}


static int pmix_pif_base_open (pmix_mca_base_open_flag_t flags)
{
    if (frameopen) {
        return PMIX_SUCCESS;
    }
    frameopen = true;

    /* setup the global list */
    PMIX_CONSTRUCT(&pmix_if_list, pmix_list_t);

    return pmix_mca_base_framework_components_open(&pmix_pif_base_framework, flags);
}


static int pmix_pif_base_close(void)
{
    pmix_list_item_t *item;

    if (!frameopen) {
        return PMIX_SUCCESS;
    }
    frameopen = false;

    while (NULL != (item = pmix_list_remove_first(&pmix_if_list))) {
        PMIX_RELEASE(item);
    }
    PMIX_DESTRUCT(&pmix_if_list);

    return pmix_mca_base_framework_components_close(&pmix_pif_base_framework, NULL);
}

static void pmix_pif_construct(pmix_pif_t *obj)
{
    memset(obj->if_name, 0, sizeof(obj->if_name));
    obj->if_index = -1;
    obj->if_kernel_index = (uint16_t) -1;
    obj->af_family = PF_UNSPEC;
    obj->if_flags = 0;
    obj->if_speed = 0;
    memset(&obj->if_addr, 0, sizeof(obj->if_addr));
    obj->if_mask = 0;
    obj->if_bandwidth = 0;
    memset(obj->if_mac, 0, sizeof(obj->if_mac));
    obj->ifmtu = 0;
}
