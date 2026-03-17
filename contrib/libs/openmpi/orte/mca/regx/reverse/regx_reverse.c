/*
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2018      IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/types.h"
#include "opal/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <ctype.h>

#include "opal/util/argv.h"
#include "opal/util/basename.h"
#include "opal/util/opal_environ.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/regx/base/base.h"

#include "regx_reverse.h"

static int nidmap_create(opal_pointer_array_t *pool, char **regex);

orte_regx_base_module_t orte_regx_reverse_module = {
    .nidmap_create = nidmap_create,
    .nidmap_parse = orte_regx_base_nidmap_parse,
    .extract_node_names = orte_regx_base_extract_node_names,
    .encode_nodemap = orte_regx_base_encode_nodemap,
    .decode_daemon_nodemap = orte_regx_base_decode_daemon_nodemap,
    .generate_ppn = orte_regx_base_generate_ppn,
    .parse_ppn = orte_regx_base_parse_ppn
};

static int nidmap_create(opal_pointer_array_t *pool, char **regex)
{
    char *node;
    char prefix[ORTE_MAX_NODE_PREFIX];
    int i, j, n, len, startnum, nodenum, numdigits;
    bool found;
    char *suffix, *sfx, *nodenames;
    orte_regex_node_t *ndreg;
    orte_regex_range_t *range, *rng;
    opal_list_t nodenms, dvpids;
    opal_list_item_t *item, *itm2;
    char **regexargs = NULL, *tmp, *tmp2;
    orte_node_t *nptr;
    orte_vpid_t vpid;

    OBJ_CONSTRUCT(&nodenms, opal_list_t);
    OBJ_CONSTRUCT(&dvpids, opal_list_t);

    rng = NULL;
    for (n=0; n < pool->size; n++) {
        if (NULL == (nptr = (orte_node_t*)opal_pointer_array_get_item(pool, n))) {
            continue;
        }
        /* if no daemon has been assigned, then this node is not being used */
        if (NULL == nptr->daemon) {
            vpid = -1;  // indicates no daemon assigned
        } else {
            vpid = nptr->daemon->name.vpid;
        }
        /* deal with the daemon vpid - see if it is next in the
         * current range */
        if (NULL == rng) {
            /* just starting */
            rng = OBJ_NEW(orte_regex_range_t);
            rng->vpid = vpid;
            rng->cnt = 1;
            opal_list_append(&dvpids, &rng->super);
        } else if (UINT32_MAX == vpid) {
            if (-1 == rng->vpid) {
                rng->cnt++;
            } else {
                /* need to start another range */
                rng = OBJ_NEW(orte_regex_range_t);
                rng->vpid = vpid;
                rng->cnt = 1;
                opal_list_append(&dvpids, &rng->super);
            }
        } else if (-1 == rng->vpid) {
            /* need to start another range */
            rng = OBJ_NEW(orte_regex_range_t);
            rng->vpid = vpid;
            rng->cnt = 1;
            opal_list_append(&dvpids, &rng->super);
        } else {
            /* is this the next in line */
            if (vpid == (orte_vpid_t)(rng->vpid + rng->cnt)) {
                rng->cnt++;
            } else {
                /* need to start another range */
                rng = OBJ_NEW(orte_regex_range_t);
                rng->vpid = vpid;
                rng->cnt = 1;
                opal_list_append(&dvpids, &rng->super);
            }
        }
        node = nptr->name;
        opal_output_verbose(5, orte_regx_base_framework.framework_output,
                            "%s PROCESS NODE <%s>",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            node);
        /* determine this node's prefix by looking for first digit char */
        len = strlen(node);
        startnum = -1;
        memset(prefix, 0, ORTE_MAX_NODE_PREFIX);
        numdigits = 0;

        /* Valid hostname characters are:
         * - ascii letters, digits, and the '-' character.
         * Determine the prefix in reverse to better support hostnames like:
         * c712f6n01, c699c086 where there are sets of digits, and the lowest
         * set changes most frequently.
         */
        startnum = -1;
        memset(prefix, 0, ORTE_MAX_NODE_PREFIX);
        numdigits = 0;
        for (i=len-1; i >= 0; i--) {
            // Count all of the digits
            if( isdigit(node[i]) ) {
                numdigits++;
                continue;
            }
            else {
                // At this point everything at and above position 'i' is prefix.
                for( j = 0; j <= i; ++j) {
                    prefix[j] = node[j];
                }
                if (numdigits) {
                    startnum = j;
                }
                break;
            }
        }

        opal_output_verbose(5, orte_regx_base_framework.framework_output,
                            "%s PROCESS NODE <%s> : reverse / prefix \"%s\" / numdigits %d",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            node, prefix, numdigits);

        if (startnum < 0) {
            /* can't compress this name - just add it to the list */
            ndreg = OBJ_NEW(orte_regex_node_t);
            ndreg->prefix = strdup(node);
            opal_list_append(&nodenms, &ndreg->super);
            continue;
        }
        /* convert the digits and get any suffix */
        nodenum = strtol(&node[startnum], &sfx, 10);
        if (NULL != sfx) {
            suffix = strdup(sfx);
        } else {
            suffix = NULL;
        }
        /* is this node name already on our list? */
        found = false;
        if (0 != opal_list_get_size(&nodenms)) {
            ndreg = (orte_regex_node_t*)opal_list_get_last(&nodenms);

            if ((0 < strlen(prefix) && NULL == ndreg->prefix) ||
                (0 == strlen(prefix) && NULL != ndreg->prefix) ||
                (0 < strlen(prefix) && NULL != ndreg->prefix &&
                    0 != strcmp(prefix, ndreg->prefix)) ||
                (NULL == suffix && NULL != ndreg->suffix) ||
                (NULL != suffix && NULL == ndreg->suffix) ||
                (NULL != suffix && NULL != ndreg->suffix &&
                    0 != strcmp(suffix, ndreg->suffix)) ||
                (numdigits != ndreg->num_digits)) {
                found = false;
            } else {
                /* found a match - flag it */
                found = true;
            }
        }
        if (found) {
            /* get the last range on this nodeid - we do this
             * to preserve order
             */
            range = (orte_regex_range_t*)opal_list_get_last(&ndreg->ranges);
            if (NULL == range) {
                /* first range for this nodeid */
                range = OBJ_NEW(orte_regex_range_t);
                range->vpid = nodenum;
                range->cnt = 1;
                opal_list_append(&ndreg->ranges, &range->super);
            /* see if the node number is out of sequence */
            } else if (nodenum != (range->vpid + range->cnt)) {
                /* start a new range */
                range = OBJ_NEW(orte_regex_range_t);
                range->vpid = nodenum;
                range->cnt = 1;
                opal_list_append(&ndreg->ranges, &range->super);
            } else {
                /* everything matches - just increment the cnt */
                range->cnt++;
            }
        } else {
            /* need to add it */
            ndreg = OBJ_NEW(orte_regex_node_t);
            if (0 < strlen(prefix)) {
                ndreg->prefix = strdup(prefix);
            }
            if (NULL != suffix) {
                ndreg->suffix = strdup(suffix);
            }
            ndreg->num_digits = numdigits;
            opal_list_append(&nodenms, &ndreg->super);
            /* record the first range for this nodeid - we took
             * care of names we can't compress above
             */
            range = OBJ_NEW(orte_regex_range_t);
            range->vpid = nodenum;
            range->cnt = 1;
            opal_list_append(&ndreg->ranges, &range->super);
        }
        if (NULL != suffix) {
            free(suffix);
        }
    }
    /* begin constructing the regular expression */
    while (NULL != (item = opal_list_remove_first(&nodenms))) {
        ndreg = (orte_regex_node_t*)item;

        /* if no ranges, then just add the name */
        if (0 == opal_list_get_size(&ndreg->ranges)) {
            if (NULL != ndreg->prefix) {
                /* solitary node */
                asprintf(&tmp, "%s", ndreg->prefix);
                opal_argv_append_nosize(&regexargs, tmp);
                free(tmp);
            }
            OBJ_RELEASE(ndreg);
            continue;
        }
        /* start the regex for this nodeid with the prefix */
        if (NULL != ndreg->prefix) {
            asprintf(&tmp, "%s[%d:", ndreg->prefix, ndreg->num_digits);
        } else {
            asprintf(&tmp, "[%d:", ndreg->num_digits);
        }
        /* add the ranges */
        while (NULL != (itm2 = opal_list_remove_first(&ndreg->ranges))) {
            range = (orte_regex_range_t*)itm2;
            if (1 == range->cnt) {
                asprintf(&tmp2, "%s%u,", tmp, range->vpid);
            } else {
                asprintf(&tmp2, "%s%u-%u,", tmp, range->vpid, range->vpid + range->cnt - 1);
            }
            free(tmp);
            tmp = tmp2;
            OBJ_RELEASE(range);
        }
        /* replace the final comma */
        tmp[strlen(tmp)-1] = ']';
        if (NULL != ndreg->suffix) {
            /* add in the suffix, if provided */
            asprintf(&tmp2, "%s%s", tmp, ndreg->suffix);
            free(tmp);
            tmp = tmp2;
        }
        opal_argv_append_nosize(&regexargs, tmp);
        free(tmp);
        OBJ_RELEASE(ndreg);
    }

    /* assemble final result */
    nodenames = opal_argv_join(regexargs, ',');
    /* cleanup */
    opal_argv_free(regexargs);
    OBJ_DESTRUCT(&nodenms);

    /* do the same for the vpids */
    tmp = NULL;
    while (NULL != (item = opal_list_remove_first(&dvpids))) {
        rng = (orte_regex_range_t*)item;
        if (1 < rng->cnt) {
            if (NULL == tmp) {
                asprintf(&tmp, "%u(%u)", rng->vpid, rng->cnt);
            } else {
                asprintf(&tmp2, "%s,%u(%u)", tmp, rng->vpid, rng->cnt);
                free(tmp);
                tmp = tmp2;
            }
        } else {
            if (NULL == tmp) {
                asprintf(&tmp, "%u", rng->vpid);
            } else {
                asprintf(&tmp2, "%s,%u", tmp, rng->vpid);
                free(tmp);
                tmp = tmp2;
            }
        }
        OBJ_RELEASE(rng);
    }
    OPAL_LIST_DESTRUCT(&dvpids);

    /* now concatenate the results into one string */
    asprintf(&tmp2, "%s@%s", nodenames, tmp);
    free(nodenames);
    free(tmp);
    *regex = tmp2;
    return ORTE_SUCCESS;
}
