/*
 * Copyright (c) 2010-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"

#include "opal/constants.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/mca/event/event.h"
#include "opal/mca/event/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "opal/mca/event/base/static-components.h"

/**
 * Initialize the event MCA framework
 *
 * @retval OPAL_SUCCESS Upon success
 * @retval OPAL_ERROR Upon failure
 *
 * This must be the first function invoked in the event MCA
 * framework.  It initializes the event MCA framework, finds
 * and opens event components, etc.
 *
 * This function is invoked during opal_init().
 *
 * This function fills in the internal global variable
 * opal_event_base_components_opened, which is a list of all
 * event components that were successfully opened.  This
 * variable should \em only be used by other event base
 * functions -- it is not considered a public interface member --
 * and is only mentioned here for completeness.
 */
static int opal_event_base_open(mca_base_open_flag_t flags);
static int opal_event_base_close(void);

/* Use default register and close function */
MCA_BASE_FRAMEWORK_DECLARE(opal, event, NULL, NULL, opal_event_base_open,
			   opal_event_base_close, mca_event_base_static_components,
			   0);

/*
 * Globals
 */
opal_event_base_t *opal_sync_event_base=NULL;

static int opal_event_base_open(mca_base_open_flag_t flags)
{
    int rc = OPAL_SUCCESS;

    /* Silence compiler warning */
    (void) flags;

    /* no need to open the components since we only use one */

    /* init the lib */
    if (OPAL_SUCCESS != (rc = opal_event_init())) {
        return rc;
    }

    /* Declare our intent to use threads */
    opal_event_use_threads();

    /* get our event base */
    if (NULL == (opal_sync_event_base = opal_event_base_create())) {
        return OPAL_ERROR;
    }

    /* set the number of priorities */
    if (0 < OPAL_EVENT_NUM_PRI) {
        opal_event_base_priority_init(opal_sync_event_base, OPAL_EVENT_NUM_PRI);
    }

    return rc;
}

static int opal_event_base_close(void)
{
    int rc;
    
    /* cleanup components even though they are statically opened */
    if (OPAL_SUCCESS != (rc =mca_base_framework_components_close (&opal_event_base_framework,
						                  NULL))) {
        return rc;
    }

    opal_event_base_free(opal_sync_event_base);

    /* finalize the lib */
    return opal_event_finalize();

}

