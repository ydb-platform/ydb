/*
 * Copyright (c) 2010-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/if/if.h"
#include "opal/mca/if/base/base.h"
#include "opal/mca/if/base/static-components.h"

/* instantiate the global list of interfaces */
opal_list_t opal_if_list = {{0}};
bool opal_if_do_not_resolve = false;
bool opal_if_retain_loopback = false;

static int opal_if_base_register (mca_base_register_flag_t flags);
static int opal_if_base_open (mca_base_open_flag_t flags);
static int opal_if_base_close(void);
static void opal_if_construct(opal_if_t *obj);

static bool frameopen = false;

/* instance the opal_if_t object */
OBJ_CLASS_INSTANCE(opal_if_t, opal_list_item_t, opal_if_construct, NULL);

MCA_BASE_FRAMEWORK_DECLARE(opal, if, NULL, opal_if_base_register, opal_if_base_open, opal_if_base_close,
                           mca_if_base_static_components, 0);

static int opal_if_base_register (mca_base_register_flag_t flags)
{
    opal_if_do_not_resolve = false;
    (void) mca_base_framework_var_register (&opal_if_base_framework, "do_not_resolve",
                                            "If nonzero, do not attempt to resolve interfaces",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                            OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                            &opal_if_do_not_resolve);

    opal_if_retain_loopback = false;
    (void) mca_base_framework_var_register (&opal_if_base_framework, "retain_loopback",
                                            "If nonzero, retain loopback interfaces",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                            OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                            &opal_if_retain_loopback);

    return OPAL_SUCCESS;
}


static int opal_if_base_open (mca_base_open_flag_t flags)
{
    if (frameopen) {
        return OPAL_SUCCESS;
    }
    frameopen = true;

    /* setup the global list */
    OBJ_CONSTRUCT(&opal_if_list, opal_list_t);

    return mca_base_framework_components_open(&opal_if_base_framework, flags);
}


static int opal_if_base_close(void)
{
    opal_list_item_t *item;

    if (!frameopen) {
        return OPAL_SUCCESS;
    }
    frameopen = false;

    while (NULL != (item = opal_list_remove_first(&opal_if_list))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&opal_if_list);

    return mca_base_framework_components_close(&opal_if_base_framework, NULL);
}

static void opal_if_construct(opal_if_t *obj)
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
