/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte_config.h"
#include "orte/constants.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

#include "opal/class/opal_list.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/mca/installdirs/installdirs.h"

#include "orte/util/show_help.h"
#include "orte/util/proc_info.h"
#include "orte/util/name_fns.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ras/base/base.h"
#include "orte/runtime/orte_globals.h"

#include "orte/util/hostfile/hostfile_lex.h"
#include "orte/util/hostfile/hostfile.h"


static const char *cur_hostfile_name = NULL;

static void hostfile_parse_error(int token)
{
    switch (token) {
    case ORTE_HOSTFILE_STRING:
        orte_show_help("help-hostfile.txt", "parse_error_string",
                       true,
                       cur_hostfile_name,
                       orte_util_hostfile_line,
                       token,
                       orte_util_hostfile_value.sval);
        break;
    case ORTE_HOSTFILE_IPV4:
    case ORTE_HOSTFILE_IPV6:
    case ORTE_HOSTFILE_INT:
        orte_show_help("help-hostfile.txt", "parse_error_int",
                       true,
                       cur_hostfile_name,
                       orte_util_hostfile_line,
                       token,
                       orte_util_hostfile_value.ival);
        break;
     default:
        orte_show_help("help-hostfile.txt", "parse_error",
                       true,
                       cur_hostfile_name,
                       orte_util_hostfile_line,
                       token );
        break;
    }
}

 /**
  * Return the integer following an = (actually may only return positive ints)
  */
static int hostfile_parse_int(void)
{
    if (ORTE_HOSTFILE_EQUAL != orte_util_hostfile_lex())
        return -1;
    if (ORTE_HOSTFILE_INT != orte_util_hostfile_lex())
        return -1;
    return orte_util_hostfile_value.ival;
}

/**
 * Return the string following an = (option to a keyword)
 */
static char *hostfile_parse_string(void)
{
    int rc;
    if (ORTE_HOSTFILE_EQUAL != orte_util_hostfile_lex()){
        return NULL;
    }
    rc = orte_util_hostfile_lex();
    if (ORTE_HOSTFILE_STRING != rc){
        return NULL;
    }
    return strdup(orte_util_hostfile_value.sval);
}

static orte_node_t* hostfile_lookup(opal_list_t* nodes, const char* name)
{
    opal_list_item_t* item;
    for(item =  opal_list_get_first(nodes);
        item != opal_list_get_end(nodes);
        item =  opal_list_get_next(item)) {
        orte_node_t* node = (orte_node_t*)item;
        if (strcmp(node->name, name) == 0) {
            return node;
        }
    }
    return NULL;
}

static int hostfile_parse_line(int token, opal_list_t* updates,
                               opal_list_t* exclude, bool keep_all)
{
    int rc;
    orte_node_t* node;
    bool got_max = false;
    char* value;
    char **argv;
    char* node_name = NULL;
    char* username = NULL;
    int cnt;
    int number_of_slots = 0;
    char buff[64];

    if (ORTE_HOSTFILE_STRING == token ||
        ORTE_HOSTFILE_HOSTNAME == token ||
        ORTE_HOSTFILE_INT == token ||
        ORTE_HOSTFILE_IPV4 == token ||
        ORTE_HOSTFILE_IPV6 == token) {

        if(ORTE_HOSTFILE_INT == token) {
            snprintf(buff, 64, "%d", orte_util_hostfile_value.ival);
            value = buff;
        } else {
            value = orte_util_hostfile_value.sval;
        }
        argv = opal_argv_split (value, '@');

        cnt = opal_argv_count (argv);
        if (1 == cnt) {
            node_name = strdup(argv[0]);
        } else if (2 == cnt) {
            username = strdup(argv[0]);
            node_name = strdup(argv[1]);
        } else {
            opal_output(0, "WARNING: Unhandled user@host-combination\n"); /* XXX */
        }
        opal_argv_free (argv);

        // Strip off the FQDN if present, ignore IP addresses
        if( !orte_keep_fqdn_hostnames && !opal_net_isaddr(node_name) ) {
            char *ptr;
            if (NULL != (ptr = strchr(node_name, '.'))) {
                *ptr = '\0';
            }
        }

        /* if the first letter of the name is '^', then this is a node
         * to be excluded. Remove the ^ character so the nodename is
         * usable, and put it on the exclude list
         */
        if ('^' == node_name[0]) {
            int i, len;
            len = strlen(node_name);
            for (i=1; i < len; i++) {
                node_name[i-1] = node_name[i];
            }
            node_name[len-1] = '\0';  /* truncate */

            OPAL_OUTPUT_VERBOSE((3, orte_ras_base_framework.framework_output,
                                 "%s hostfile: node %s is being excluded",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node_name));

            /* see if this is another name for us */
            if (orte_ifislocal(node_name)) {
                /* Nodename has been allocated, that is for sure */
                free (node_name);
                node_name = strdup(orte_process_info.nodename);
            }

            /* Do we need to make a new node object?  First check to see
               if it's already in the exclude list */
            if (NULL == (node = hostfile_lookup(exclude, node_name))) {
                node = OBJ_NEW(orte_node_t);
                node->name = node_name;
                if (NULL != username) {
                    orte_set_attribute(&node->attributes, ORTE_NODE_USERNAME, ORTE_ATTR_LOCAL, username, OPAL_STRING);
                }
                opal_list_append(exclude, &node->super);
            } else {
                free(node_name);
            }
            return ORTE_SUCCESS;
        }

        /* this is not a node to be excluded, so we need to process it and
         * add it to the "include" list. See if this host is actually us.
         */
        if (orte_ifislocal(node_name)) {
            /* Nodename has been allocated, that is for sure */
            free (node_name);
            node_name = strdup(orte_process_info.nodename);
        }

        OPAL_OUTPUT_VERBOSE((3, orte_ras_base_framework.framework_output,
                             "%s hostfile: node %s is being included - keep all is %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node_name,
                             keep_all ? "TRUE" : "FALSE"));

        /* Do we need to make a new node object? */
        if (keep_all || NULL == (node = hostfile_lookup(updates, node_name))) {
            node = OBJ_NEW(orte_node_t);
            node->name = node_name;
            node->slots = 1;
            if (NULL != username) {
                orte_set_attribute(&node->attributes, ORTE_NODE_USERNAME, ORTE_ATTR_LOCAL, username, OPAL_STRING);
            }
            opal_list_append(updates, &node->super);
        } else {
            /* this node was already found once - add a slot and mark slots as "given" */
            node->slots++;
            ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
            free(node_name);
        }
    } else if (ORTE_HOSTFILE_RELATIVE == token) {
        /* store this for later processing */
        node = OBJ_NEW(orte_node_t);
        node->name = strdup(orte_util_hostfile_value.sval);
        opal_list_append(updates, &node->super);
    } else if (ORTE_HOSTFILE_RANK == token) {
        /* we can ignore the rank, but we need to extract the node name. we
         * first need to shift over to the other side of the equal sign as
         * this is where the node name will be
         */
        while (!orte_util_hostfile_done &&
               ORTE_HOSTFILE_EQUAL != token) {
            token = orte_util_hostfile_lex();
        }
        if (orte_util_hostfile_done) {
            /* bad syntax somewhere */
            return ORTE_ERROR;
        }
        /* next position should be the node name */
        token = orte_util_hostfile_lex();
        if(ORTE_HOSTFILE_INT == token) {
            snprintf(buff, 64, "%d", orte_util_hostfile_value.ival);
            value = buff;
        } else {
            value = orte_util_hostfile_value.sval;
        }

        argv = opal_argv_split (value, '@');

        cnt = opal_argv_count (argv);
        if (1 == cnt) {
            node_name = strdup(argv[0]);
        } else if (2 == cnt) {
            username = strdup(argv[0]);
            node_name = strdup(argv[1]);
        } else {
            opal_output(0, "WARNING: Unhandled user@host-combination\n"); /* XXX */
        }
        opal_argv_free (argv);

        // Strip off the FQDN if present, ignore IP addresses
        if( !orte_keep_fqdn_hostnames && !opal_net_isaddr(node_name) ) {
            char *ptr;
            if (NULL != (ptr = strchr(node_name, '.'))) {
                *ptr = '\0';
            }
        }

        /* Do we need to make a new node object? */
        if (NULL == (node = hostfile_lookup(updates, node_name))) {
            node = OBJ_NEW(orte_node_t);
            node->name = node_name;
            node->slots = 1;
            if (NULL != username) {
                orte_set_attribute(&node->attributes, ORTE_NODE_USERNAME, ORTE_ATTR_LOCAL, username, OPAL_STRING);
            }
            opal_list_append(updates, &node->super);
        } else {
            /* add a slot */
            node->slots++;
            free(node_name);
        }
        OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                             "%s hostfile: node %s slots %d nodes-given %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name, node->slots,
                             ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN) ? "TRUE" : "FALSE"));
        /* mark the slots as "given" since we take them as being the
         * number specified via the rankfile
         */
        ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
        /* skip to end of line */
        while (!orte_util_hostfile_done &&
               ORTE_HOSTFILE_NEWLINE != token) {
            token = orte_util_hostfile_lex();
        }
        return ORTE_SUCCESS;
    } else {
        hostfile_parse_error(token);
        return ORTE_ERROR;
    }
    free(username);

    while (!orte_util_hostfile_done) {
        token = orte_util_hostfile_lex();

        switch (token) {
        case ORTE_HOSTFILE_DONE:
            goto done;

        case ORTE_HOSTFILE_NEWLINE:
            goto done;

        case ORTE_HOSTFILE_USERNAME:
            username = hostfile_parse_string();
            if (NULL != username) {
                orte_set_attribute(&node->attributes, ORTE_NODE_USERNAME, ORTE_ATTR_LOCAL, username, OPAL_STRING);
                free(username);
            }
            break;

        case ORTE_HOSTFILE_PORT:
            rc = hostfile_parse_int();
            if (rc < 0) {
                orte_show_help("help-hostfile.txt", "port",
                               true,
                               cur_hostfile_name, rc);
                return ORTE_ERROR;
            }
            orte_set_attribute(&node->attributes, ORTE_NODE_PORT, ORTE_ATTR_LOCAL, &rc, OPAL_INT);
            break;

        case ORTE_HOSTFILE_COUNT:
        case ORTE_HOSTFILE_CPU:
        case ORTE_HOSTFILE_SLOTS:
            rc = hostfile_parse_int();
            if (rc < 0) {
                orte_show_help("help-hostfile.txt", "slots",
                               true,
                               cur_hostfile_name, rc);
                opal_list_remove_item(updates, &node->super);
                OBJ_RELEASE(node);
                return ORTE_ERROR;
            }
            if (ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                /* multiple definitions were given for the
                 * slot count - this is not allowed
                 */
                orte_show_help("help-hostfile.txt", "slots-given",
                               true,
                               cur_hostfile_name, node->name);
                opal_list_remove_item(updates, &node->super);
                OBJ_RELEASE(node);
                return ORTE_ERROR;
            }
            node->slots = rc;
            ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);

            /* Ensure that slots_max >= slots */
            if (node->slots_max != 0 && node->slots_max < node->slots) {
                node->slots_max = node->slots;
            }
            break;

        case ORTE_HOSTFILE_SLOTS_MAX:
            rc = hostfile_parse_int();
            if (rc < 0) {
                orte_show_help("help-hostfile.txt", "max_slots",
                               true,
                               cur_hostfile_name, ((size_t) rc));
                opal_list_remove_item(updates, &node->super);
                OBJ_RELEASE(node);
                return ORTE_ERROR;
            }
            /* Only take this update if it puts us >= node_slots */
            if (rc >= node->slots) {
                if (node->slots_max != rc) {
                    node->slots_max = rc;
                    got_max = true;
                }
            } else {
                orte_show_help("help-hostfile.txt", "max_slots_lt",
                               true,
                               cur_hostfile_name, node->slots, rc);
                ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
                opal_list_remove_item(updates, &node->super);
                OBJ_RELEASE(node);
                return ORTE_ERROR;
            }
            break;

        case ORTE_HOSTFILE_STRING:
        case ORTE_HOSTFILE_INT:
            /* just ignore it */
            break;

        default:
            hostfile_parse_error(token);
            opal_list_remove_item(updates, &node->super);
            OBJ_RELEASE(node);
            return ORTE_ERROR;
        }
        if (number_of_slots > node->slots) {
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
            opal_list_remove_item(updates, &node->super);
            OBJ_RELEASE(node);
            return ORTE_ERROR;
        }
    }

 done:
    if (got_max && !ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
        node->slots = node->slots_max;
        ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
    }

    return ORTE_SUCCESS;
}


/**
 * Parse the specified file into a node list.
 */

static int hostfile_parse(const char *hostfile, opal_list_t* updates,
                          opal_list_t* exclude, bool keep_all)
{
    int token;
    int rc = ORTE_SUCCESS;


    cur_hostfile_name = hostfile;

    orte_util_hostfile_done = false;
    orte_util_hostfile_in = fopen(hostfile, "r");
    if (NULL == orte_util_hostfile_in) {
        if (NULL == orte_default_hostfile ||
            0 != strcmp(orte_default_hostfile, hostfile)) {
            /* not the default hostfile, so not finding it
             * is an error
             */
            orte_show_help("help-hostfile.txt", "no-hostfile", true, hostfile);
            rc = ORTE_ERR_SILENT;
            goto unlock;
        }
        /* if this is the default hostfile and it was given,
         * then it's an error
         */
        if (orte_default_hostfile_given) {
            orte_show_help("help-hostfile.txt", "no-hostfile", true, hostfile);
            rc = ORTE_ERR_NOT_FOUND;
            goto unlock;
        }
        /* otherwise, not finding it is okay */
        rc = ORTE_SUCCESS;
        goto unlock;
    }

    while (!orte_util_hostfile_done) {
        token = orte_util_hostfile_lex();

        switch (token) {
        case ORTE_HOSTFILE_DONE:
            orte_util_hostfile_done = true;
            break;

        case ORTE_HOSTFILE_NEWLINE:
            break;

        /*
         * This looks odd, since we have several forms of host-definitions:
         *   hostname              just plain as it is, being a ORTE_HOSTFILE_STRING
         *   IP4s and user@IPv4s
         *   hostname.domain and user@hostname.domain
         */
        case ORTE_HOSTFILE_STRING:
        case ORTE_HOSTFILE_INT:
        case ORTE_HOSTFILE_HOSTNAME:
        case ORTE_HOSTFILE_IPV4:
        case ORTE_HOSTFILE_IPV6:
        case ORTE_HOSTFILE_RELATIVE:
        case ORTE_HOSTFILE_RANK:
            rc = hostfile_parse_line(token, updates, exclude, keep_all);
            if (ORTE_SUCCESS != rc) {
                goto unlock;
            }
            break;

        default:
            hostfile_parse_error(token);
            goto unlock;
        }
    }
    fclose(orte_util_hostfile_in);
    orte_util_hostfile_in = NULL;
    orte_util_hostfile_lex_destroy();

unlock:
    cur_hostfile_name = NULL;

    return rc;
}


/**
 * Parse the provided hostfile and add the nodes to the list.
 */

int orte_util_add_hostfile_nodes(opal_list_t *nodes,
                                 char *hostfile)
{
    opal_list_t exclude, adds;
    opal_list_item_t *item, *itm;
    int rc;
    orte_node_t *nd, *node;
    bool found;

    OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                         "%s hostfile: checking hostfile %s for nodes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hostfile));

    OBJ_CONSTRUCT(&exclude, opal_list_t);
    OBJ_CONSTRUCT(&adds, opal_list_t);

    /* parse the hostfile and add any new contents to the list */
    if (ORTE_SUCCESS != (rc = hostfile_parse(hostfile, &adds, &exclude, false))) {
        goto cleanup;
    }

    /* check for any relative node directives */
    for (item = opal_list_get_first(&adds);
         item != opal_list_get_end(&adds);
         item = opal_list_get_next(item)) {
        node=(orte_node_t*)item;

        if ('+' == node->name[0]) {
            orte_show_help("help-hostfile.txt", "hostfile:relative-syntax",
                           true, node->name);
            rc = ORTE_ERR_SILENT;
            goto cleanup;
        }
    }

    /* remove from the list of nodes those that are in the exclude list */
    while (NULL != (item = opal_list_remove_first(&exclude))) {
        nd = (orte_node_t*)item;
        /* check for matches on nodes */
        for (itm = opal_list_get_first(&adds);
             itm != opal_list_get_end(&adds);
             itm = opal_list_get_next(itm)) {
            node = (orte_node_t*)itm;
            if (0 == strcmp(nd->name, node->name)) {
                /* match - remove it */
                opal_list_remove_item(&adds, itm);
                OBJ_RELEASE(itm);
                break;
            }
        }
        OBJ_RELEASE(item);
    }

    /* transfer across all unique nodes */
    while (NULL != (item = opal_list_remove_first(&adds))) {
        nd = (orte_node_t*)item;
        found = false;
        for (itm = opal_list_get_first(nodes);
             itm != opal_list_get_end(nodes);
             itm = opal_list_get_next(itm)) {
            node = (orte_node_t*)itm;
            if (0 == strcmp(nd->name, node->name)) {
                found = true;
                break;
            }
        }
        if (!found) {
            opal_list_append(nodes, &nd->super);
            OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                                 "%s hostfile: adding node %s slots %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), nd->name, nd->slots));
        } else {
            OBJ_RELEASE(item);
        }
    }

cleanup:
    OPAL_LIST_DESTRUCT(&exclude);
    OPAL_LIST_DESTRUCT(&adds);

    return rc;
}

/* Parse the provided hostfile and filter the nodes that are
 * on the input list, removing those that
 * are not found in the hostfile
 */
int orte_util_filter_hostfile_nodes(opal_list_t *nodes,
                                    char *hostfile,
                                    bool remove)
{
    opal_list_t newnodes, exclude;
    opal_list_item_t *item1, *item2, *next, *item3;
    orte_node_t *node_from_list, *node_from_file, *node_from_pool, *node3;
    int rc = ORTE_SUCCESS;
    char *cptr;
    int num_empty, nodeidx;
    bool want_all_empty = false;
    opal_list_t keep;
    bool found;

    OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                        "%s hostfile: filtering nodes through hostfile %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hostfile));

    /* parse the hostfile and create local list of findings */
    OBJ_CONSTRUCT(&newnodes, opal_list_t);
    OBJ_CONSTRUCT(&exclude, opal_list_t);
    if (ORTE_SUCCESS != (rc = hostfile_parse(hostfile, &newnodes, &exclude, false))) {
        OBJ_DESTRUCT(&newnodes);
        OBJ_DESTRUCT(&exclude);
        return rc;
    }

    /* if the hostfile was empty, then treat it as a no-op filter */
    if (0 == opal_list_get_size(&newnodes)) {
        OBJ_DESTRUCT(&newnodes);
        OBJ_DESTRUCT(&exclude);
        /* indicate that the hostfile was empty */
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }

    /* remove from the list of newnodes those that are in the exclude list
     * since we could have added duplicate names above due to the */
    while (NULL != (item1 = opal_list_remove_first(&exclude))) {
        node_from_file = (orte_node_t*)item1;
        /* check for matches on nodes */
        for (item2 = opal_list_get_first(&newnodes);
             item2 != opal_list_get_end(&newnodes);
             item2 = opal_list_get_next(item2)) {
            orte_node_t *node = (orte_node_t*)item2;
            if (0 == strcmp(node_from_file->name, node->name)) {
                /* match - remove it */
                opal_list_remove_item(&newnodes, item2);
                OBJ_RELEASE(item2);
                break;
            }
        }
        OBJ_RELEASE(item1);
    }

    /* now check our nodes and keep or mark those that match. We can
     * destruct our hostfile list as we go since this won't be needed
     */
    OBJ_CONSTRUCT(&keep, opal_list_t);
    while (NULL != (item2 = opal_list_remove_first(&newnodes))) {
        node_from_file = (orte_node_t*)item2;

        next = opal_list_get_next(item2);

        /* see if this is a relative node syntax */
        if ('+' == node_from_file->name[0]) {
            /* see if we specified empty nodes */
            if ('e' == node_from_file->name[1] ||
                'E' == node_from_file->name[1]) {
                /* request for empty nodes - do they want
                 * all of them?
                 */
                if (NULL != (cptr = strchr(node_from_file->name, ':'))) {
                    /* the colon indicates a specific # are requested */
                    cptr++; /* step past : */
                    num_empty = strtol(cptr, NULL, 10);
                } else {
                    /* want them all - set num_empty to max */
                    num_empty = INT_MAX;
                    want_all_empty = true;
                }
                /* search the list of nodes provided to us and find those
                 * that are empty
                 */
                item1 = opal_list_get_first(nodes);
                while (0 < num_empty && item1 != opal_list_get_end(nodes)) {
                    node_from_list = (orte_node_t*)item1;
                    next = opal_list_get_next(item1);  /* keep our place */
                    if (0 == node_from_list->slots_inuse) {
                        /* check to see if this node is explicitly called
                         * out later - if so, don't use it here
                         */
                        for (item3 = opal_list_get_first(&newnodes);
                             item3 != opal_list_get_end(&newnodes);
                             item3 = opal_list_get_next(item3)) {
                            node3 = (orte_node_t*)item3;
                            if (0 == strcmp(node3->name, node_from_list->name)) {
                                /* match - don't use it */
                                goto skipnode;
                            }
                        }
                        if (remove) {
                            /* remove item from list */
                            opal_list_remove_item(nodes, item1);
                            /* xfer to keep list */
                            opal_list_append(&keep, item1);
                        } else {
                            /* mark as included */
                            ORTE_FLAG_SET(node_from_list, ORTE_NODE_FLAG_MAPPED);
                        }
                        --num_empty;
                    }
                skipnode:
                    item1 = next;
                }
                /* did they get everything they wanted? */
                if (!want_all_empty && 0 < num_empty) {
                    orte_show_help("help-hostfile.txt", "hostfile:not-enough-empty",
                                   true, num_empty);
                    rc = ORTE_ERR_SILENT;
                    goto cleanup;
                }
            } else if ('n' == node_from_file->name[1] ||
                       'N' == node_from_file->name[1]) {
                /* they want a specific relative node #, so
                 * look it up on global pool
                 */
                nodeidx = strtol(&node_from_file->name[2], NULL, 10);
                if (NULL == (node_from_pool = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, nodeidx))) {
                    /* this is an error */
                    orte_show_help("help-hostfile.txt", "hostfile:relative-node-not-found",
                                   true, nodeidx, node_from_file->name);
                    rc = ORTE_ERR_SILENT;
                    goto cleanup;
                }
                /* search the list of nodes provided to us and find it */
                for (item1 = opal_list_get_first(nodes);
                     item1 != opal_list_get_end(nodes);
                     item1 = opal_list_get_next(nodes)) {
                    node_from_list = (orte_node_t*)item1;
                    if (0 == strcmp(node_from_list->name, node_from_pool->name)) {
                        if (remove) {
                            /* match - remove item from list */
                            opal_list_remove_item(nodes, item1);
                            /* xfer to keep list */
                            opal_list_append(&keep, item1);
                        } else {
                            /* mark as included */
                            ORTE_FLAG_SET(node_from_list, ORTE_NODE_FLAG_MAPPED);
                        }
                        break;
                    }
                }
            } else {
                /* invalid relative node syntax */
                orte_show_help("help-hostfile.txt", "hostfile:invalid-relative-node-syntax",
                               true, node_from_file->name);
                rc = ORTE_ERR_SILENT;
                goto cleanup;
            }
        } else {
            /* we are looking for a specific node on the list
             * search the provided list of nodes to see if this
             * one is found
             */
            found = false;
            for (item1 = opal_list_get_first(nodes);
                 item1 != opal_list_get_end(nodes);
                 item1 = opal_list_get_next(item1)) {
                node_from_list = (orte_node_t*)item1;
                /* we have converted all aliases for ourself
                 * to our own detected nodename, so no need
                 * to check for interfaces again - a simple
                 * strcmp will suffice */
                if (0 == strcmp(node_from_file->name, node_from_list->name)) {
                    /* if the slot count here is less than the
                     * total slots avail on this node, set it
                     * to the specified count - this allows people
                     * to subdivide an allocation
                     */
                    if (ORTE_FLAG_TEST(node_from_file, ORTE_NODE_FLAG_SLOTS_GIVEN) &&
                        node_from_file->slots < node_from_list->slots) {
                        node_from_list->slots = node_from_file->slots;
                    }
                    if (remove) {
                        /* remove the node from the list */
                        opal_list_remove_item(nodes, item1);
                        /* xfer it to keep list */
                        opal_list_append(&keep, item1);
                    } else {
                        /* mark as included */
                        ORTE_FLAG_SET(node_from_list, ORTE_NODE_FLAG_MAPPED);
                    }
                    found = true;
                    break;
                }
            }
            /* if the host in the newnode list wasn't found,
             * then that is an error we need to report to the
             * user and abort
             */
            if (!found) {
                orte_show_help("help-hostfile.txt", "hostfile:extra-node-not-found",
                               true, hostfile, node_from_file->name);
                rc = ORTE_ERR_SILENT;
                goto cleanup;
            }
        }
        /* cleanup the newnode list */
        OBJ_RELEASE(item2);
    }

    /* if we still have entries on our hostfile list, then
     * there were requested hosts that were not in our allocation.
     * This is an error - report it to the user and return an error
     */
    if (0 != opal_list_get_size(&newnodes)) {
        orte_show_help("help-hostfile.txt", "not-all-mapped-alloc",
                       true, hostfile);
        while (NULL != (item1 = opal_list_remove_first(&newnodes))) {
            OBJ_RELEASE(item1);
        }
        OBJ_DESTRUCT(&newnodes);
        return ORTE_ERR_SILENT;
    }

    if (!remove) {
        /* all done */
        OBJ_DESTRUCT(&newnodes);
        return ORTE_SUCCESS;
    }

    /* clear the rest of the nodes list */
    while (NULL != (item1 = opal_list_remove_first(nodes))) {
        OBJ_RELEASE(item1);
    }

    /* the nodes list has been cleared - rebuild it in order */
    while (NULL != (item1 = opal_list_remove_first(&keep))) {
        opal_list_append(nodes, item1);
    }

cleanup:
    OBJ_DESTRUCT(&newnodes);

    return rc;
}

int orte_util_get_ordered_host_list(opal_list_t *nodes,
                                    char *hostfile)
{
    opal_list_t exclude;
    opal_list_item_t *item, *itm, *item2, *item1;
    char *cptr;
    int num_empty, i, nodeidx, startempty=0;
    bool want_all_empty=false;
    orte_node_t *node_from_pool, *newnode;
    int rc;

    OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                         "%s hostfile: creating ordered list of hosts from hostfile %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hostfile));

    OBJ_CONSTRUCT(&exclude, opal_list_t);

    /* parse the hostfile and add the contents to the list, keeping duplicates */
    if (ORTE_SUCCESS != (rc = hostfile_parse(hostfile, nodes, &exclude, true))) {
        goto cleanup;
    }

    /* parse the nodes to process any relative node directives */
    item2 = opal_list_get_first(nodes);
    while (item2 != opal_list_get_end(nodes)) {
        orte_node_t *node=(orte_node_t*)item2;

        /* save the next location in case this one gets removed */
        item1 = opal_list_get_next(item2);

        if ('+' != node->name[0]) {
            item2 = item1;
            continue;
        }

        /* see if we specified empty nodes */
        if ('e' == node->name[1] ||
            'E' == node->name[1]) {
            /* request for empty nodes - do they want
             * all of them?
             */
            if (NULL != (cptr = strchr(node->name, ':'))) {
                /* the colon indicates a specific # are requested */
                cptr++; /* step past : */
                num_empty = strtol(cptr, NULL, 10);
            } else {
                /* want them all - set num_empty to max */
                num_empty = INT_MAX;
                want_all_empty = true;
            }
            /* insert empty nodes into newnodes list in place of the current item.
             * since item1 is the next item, we insert in front of it
             */
            if (!orte_hnp_is_allocated && 0 == startempty) {
               startempty = 1;
            }
            for (i=startempty; 0 < num_empty && i < orte_node_pool->size; i++) {
                if (NULL == (node_from_pool = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                    continue;
                }
                if (0 == node_from_pool->slots_inuse) {
                    newnode = OBJ_NEW(orte_node_t);
                    newnode->name = strdup(node_from_pool->name);
                    /* if the slot count here is less than the
                     * total slots avail on this node, set it
                     * to the specified count - this allows people
                     * to subdivide an allocation
                     */
                    if (node->slots < node_from_pool->slots) {
                        newnode->slots = node->slots;
                    } else {
                        newnode->slots = node_from_pool->slots;
                    }
                    opal_list_insert_pos(nodes, item1, &newnode->super);
                    /* track number added */
                    --num_empty;
                }
            }
            /* bookmark where we stopped in case they ask for more */
            startempty = i;
            /* did they get everything they wanted? */
            if (!want_all_empty && 0 < num_empty) {
                orte_show_help("help-hostfile.txt", "hostfile:not-enough-empty",
                               true, num_empty);
                rc = ORTE_ERR_SILENT;
                goto cleanup;
            }
            /* since we have expanded the provided node, remove
             * it from list
             */
            opal_list_remove_item(nodes, item2);
            OBJ_RELEASE(item2);
        } else if ('n' == node->name[1] ||
                   'N' == node->name[1]) {
            /* they want a specific relative node #, so
             * look it up on global pool
             */
            nodeidx = strtol(&node->name[2], NULL, 10);
            /* if the HNP is not allocated, then we need to
             * adjust the index as the node pool is offset
             * by one
             */
            if (!orte_hnp_is_allocated) {
                nodeidx++;
            }
            /* see if that location is filled */
            if (NULL == (node_from_pool = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, nodeidx))) {
                /* this is an error */
                orte_show_help("help-hostfile.txt", "hostfile:relative-node-not-found",
                               true, nodeidx, node->name);
                rc = ORTE_ERR_SILENT;
                goto cleanup;
            }
            /* create the node object */
            newnode = OBJ_NEW(orte_node_t);
            newnode->name = strdup(node_from_pool->name);
            /* if the slot count here is less than the
             * total slots avail on this node, set it
             * to the specified count - this allows people
             * to subdivide an allocation
             */
            if (node->slots < node_from_pool->slots) {
                newnode->slots = node->slots;
            } else {
                newnode->slots = node_from_pool->slots;
            }
            /* insert it before item1 */
            opal_list_insert_pos(nodes, item1, &newnode->super);
            /* since we have expanded the provided node, remove
             * it from list
             */
            opal_list_remove_item(nodes, item2);
            OBJ_RELEASE(item2);
        } else {
            /* invalid relative node syntax */
            orte_show_help("help-hostfile.txt", "hostfile:invalid-relative-node-syntax",
                           true, node->name);
            rc = ORTE_ERR_SILENT;
            goto cleanup;
        }

        /* move to next */
        item2 = item1;
    }

    /* remove from the list of nodes those that are in the exclude list */
    while(NULL != (item = opal_list_remove_first(&exclude))) {
        orte_node_t *exnode = (orte_node_t*)item;
        /* check for matches on nodes */
        for (itm = opal_list_get_first(nodes);
             itm != opal_list_get_end(nodes);
             itm = opal_list_get_next(itm)) {
            orte_node_t *node=(orte_node_t*)itm;
            if (0 == strcmp(exnode->name, node->name)) {
                /* match - remove it */
                opal_list_remove_item(nodes, itm);
                OBJ_RELEASE(itm);
                /* have to cycle through the entire list as we could
                 * have duplicates
                 */
            }
        }
        OBJ_RELEASE(item);
    }

cleanup:
    OBJ_DESTRUCT(&exclude);

    return rc;
}
