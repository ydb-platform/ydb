/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 */
#include "opal_config.h"
#include "opal/constants.h"
#include "opal/util/output.h"

#include "opal/mca/event/base/base.h"
#include "external.h"

#include "opal/util/argv.h"

extern char *ompi_event_module_include;
static struct event_config *config = NULL;

opal_event_base_t* opal_event_base_create(void)
{
    opal_event_base_t *base;

    base = event_base_new_with_config(config);
    if (NULL == base) {
        /* there is no backend method that does what we want */
        opal_output(0, "No event method available");
    }
    return base;
}

int opal_event_init(void)
{
    const char **all_available_eventops = NULL;
    char **includes=NULL;
    bool dumpit=false;
    int i, j;

    if (opal_output_get_verbosity(opal_event_base_framework.framework_output) > 4) {
        event_enable_debug_mode();
    }

    all_available_eventops = event_get_supported_methods();

    if (NULL == ompi_event_module_include) {
        /* Shouldn't happen, but... */
        ompi_event_module_include = strdup("select");
    }
    includes = opal_argv_split(ompi_event_module_include,',');

    /* get a configuration object */
    config = event_config_new();
    /* cycle thru the available subsystems */
    for (i = 0 ; NULL != all_available_eventops[i] ; ++i) {
        /* if this module isn't included in the given ones,
         * then exclude it
         */
        dumpit = true;
        for (j=0; NULL != includes[j]; j++) {
            if (0 == strcmp("all", includes[j]) ||
                0 == strcmp(all_available_eventops[i], includes[j])) {
                dumpit = false;
                break;
            }
        }
        if (dumpit) {
            event_config_avoid_method(config, all_available_eventops[i]);
        }
    }
    opal_argv_free(includes);

    return OPAL_SUCCESS;
}

int opal_event_finalize(void)
{
    return OPAL_SUCCESS;
}

opal_event_t* opal_event_alloc(void)
{
    opal_event_t *ev;

    ev = (opal_event_t*)malloc(sizeof(opal_event_t));
    return ev;
}
