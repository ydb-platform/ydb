/*
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Interface for manipulating how the RML receives contact information
 *
 * Interface for manipulating how the RML receives contact
 * information.  These functions are generally used during orte_init
 * and orte_finalize.
 */


#include "orte_config.h"
#include "orte/types.h"

#include "opal/dss/dss_types.h"

BEGIN_C_DECLS

/**
 * Parse a contact information string
 *
 * Parse a contact infromation string, such as that returned by
 * orte_rml.get_contact_info().  Generally used to extract the peer
 * name from a contact information string.  It can also be used to
 * extract the contact URI strings, although this is slightly less
 * useful as the URIs may be RML componenent specific and not have
 * general meaning.
 *
 * @param[in] contact_info  Contact information string for peer
 * @param[out] peer         Peer name in contact_info
 * @param[out] uris         URI strings for peer.  May be NULL if
 *                          information is not needed
 *
 * @retval ORTE_SUCCESS     Information successfully extraced
 * @retval ORTE_ERR_BAD_PARAM The contact_info was not a valid string
 * @retval ORTE_ERROR       An unspecified error occurred
 */
ORTE_DECLSPEC int orte_rml_base_parse_uris(const char* contact_inf,
                                           orte_process_name_t* peer,
                                           char*** uris);

END_C_DECLS
