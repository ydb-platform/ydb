/*
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016-2019 IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <fcntl.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <ctype.h>


#include <pmix_common.h>
#include <pmix.h>

#include "src/include/pmix_socket_errno.h"
#include "src/include/pmix_globals.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/class/pmix_list.h"
#include "src/mca/gds/gds.h"
#include "src/client/pmix_client_ops.h"

#include "src/mca/preg/preg.h"
#include "preg_native.h"

static pmix_status_t generate_node_regex(const char *input,
                                         char **regex);
static pmix_status_t generate_ppn(const char *input,
                                  char **ppn);
static pmix_status_t parse_nodes(const char *regexp,
                                 char ***names);
static pmix_status_t parse_procs(const char *regexp,
                                 char ***procs);
static pmix_status_t resolve_peers(const char *nodename,
                                   const char *nspace,
                                   pmix_proc_t **procs, size_t *nprocs);
static pmix_status_t resolve_nodes(const char *nspace,
                                   char **nodelist);

pmix_preg_module_t pmix_preg_native_module = {
    .name = "pmix",
    .generate_node_regex = generate_node_regex,
    .generate_ppn = generate_ppn,
    .parse_nodes = parse_nodes,
    .parse_procs = parse_procs,
    .resolve_peers = resolve_peers,
    .resolve_nodes = resolve_nodes
};

static pmix_status_t regex_parse_value_ranges(char *base, char *ranges,
                                              int num_digits, char *suffix,
                                              char ***names);
static pmix_status_t regex_parse_value_range(char *base, char *range,
                                             int num_digits, char *suffix,
                                             char ***names);
static pmix_status_t pmix_regex_extract_nodes(char *regexp, char ***names);
static pmix_status_t pmix_regex_extract_ppn(char *regexp, char ***procs);


static pmix_status_t generate_node_regex(const char *input,
                                         char **regexp)
{
    char *vptr, *vsave;
    char prefix[PMIX_MAX_NODE_PREFIX];
    int i, j, len, startnum, vnum, numdigits;
    bool found, fullval;
    char *suffix, *sfx;
    pmix_regex_value_t *vreg;
    pmix_regex_range_t *range;
    pmix_list_t vids;
    char **regexargs = NULL, *tmp, *tmp2;
    char *cptr;

    /* define the default */
    *regexp = NULL;

    /* setup the list of results */
    PMIX_CONSTRUCT(&vids, pmix_list_t);

    /* cycle thru the array of input values - first copy
     * it so we don't overwrite what we were given*/
    vsave = strdup(input);
    vptr = vsave;
    while (NULL != (cptr = strchr(vptr, ',')) || 0 < strlen(vptr)) {
        if (NULL != cptr) {
            *cptr = '\0';
        }
        /* determine this node's prefix by looking for first non-alpha char */
        fullval = false;
        len = strlen(vptr);
        startnum = -1;
        memset(prefix, 0, PMIX_MAX_NODE_PREFIX);
        for (i=0, j=0; i < len; i++) {
            if (!isalpha(vptr[i])) {
                /* found a non-alpha char */
                if (!isdigit(vptr[i])) {
                    /* if it is anything but a digit, we just use
                     * the entire name
                     */
                    fullval = true;
                    break;
                }
                /* count the size of the numeric field - but don't
                 * add the digits to the prefix
                 */
                if (startnum < 0) {
                    /* okay, this defines end of the prefix */
                    startnum = i;
                }
                continue;
            }
            if (startnum < 0) {
                prefix[j++] = vptr[i];
            }
        }
        if (fullval || startnum < 0) {
            /* can't compress this name - just add it to the list */
            vreg = PMIX_NEW(pmix_regex_value_t);
            vreg->prefix = strdup(vptr);
            pmix_list_append(&vids, &vreg->super);
            /* move to the next posn */
            if (NULL == cptr) {
                break;
            }
            vptr = cptr + 1;
            continue;
        }
        /* convert the digits and get any suffix */
        vnum = strtol(&vptr[startnum], &sfx, 10);
        if (NULL != sfx) {
            suffix = strdup(sfx);
            numdigits = (int)(sfx - &vptr[startnum]);
        } else {
            suffix = NULL;
            numdigits = (int)strlen(&vptr[startnum]);
        }

        /* is this value already on our list? */
        found = false;
        PMIX_LIST_FOREACH(vreg, &vids, pmix_regex_value_t) {
            // The regex must preserve ordering of the values.
            // If we disqualified this entry in a previous check then exclude it
            // from future checks as well. This will prevent a later entry from
            // being 'pulled forward' accidentally. For example, given:
            // "a28n01,a99n02,a28n02"
            // Without this 'skip' the loop would have 'a28n02' combine with
            // 'a28n01' jumping over the 'a99n02' entry, and thus not preserving
            // the order of the list when the regex is unpacked.
            if( vreg->skip ) {
                continue;
            }

            if (0 < strlen(prefix) && NULL == vreg->prefix) {
                continue;
            }
            if (0 == strlen(prefix) && NULL != vreg->prefix) {
                continue;
            }
            if (0 < strlen(prefix) && NULL != vreg->prefix
                && 0 != strcmp(prefix, vreg->prefix)) {
                vreg->skip = true;
                continue;
            }
            if (NULL == suffix && NULL != vreg->suffix) {
                continue;
            }
            if (NULL != suffix && NULL == vreg->suffix) {
                continue;
            }
            if (NULL != suffix && NULL != vreg->suffix &&
                0 != strcmp(suffix, vreg->suffix)) {
                vreg->skip = true;
                continue;
            }
            if (numdigits != vreg->num_digits) {
                vreg->skip = true;
                continue;
            }
            /* found a match - flag it */
            found = true;
            /* get the last range on this nodeid - we do this
             * to preserve order
             */
            range = (pmix_regex_range_t*)pmix_list_get_last(&vreg->ranges);
            if (NULL == range) {
                /* first range for this value */
                range = PMIX_NEW(pmix_regex_range_t);
                range->start = vnum;
                range->cnt = 1;
                pmix_list_append(&vreg->ranges, &range->super);
                break;
            }
            /* see if the value is out of sequence */
            if (vnum != (range->start + range->cnt)) {
                /* start a new range */
                range = PMIX_NEW(pmix_regex_range_t);
                range->start = vnum;
                range->cnt = 1;
                pmix_list_append(&vreg->ranges, &range->super);
                break;
            }
            /* everything matches - just increment the cnt */
            range->cnt++;
            break;
        }
        if (!found) {
            /* need to add it */
            vreg = PMIX_NEW(pmix_regex_value_t);
            if (0 < strlen(prefix)) {
                vreg->prefix = strdup(prefix);
            }
            if (NULL != suffix) {
                vreg->suffix = strdup(suffix);
            }
            vreg->num_digits = numdigits;
            pmix_list_append(&vids, &vreg->super);
            /* record the first range for this value - we took
             * care of values we can't compress above
             */
            range = PMIX_NEW(pmix_regex_range_t);
            range->start = vnum;
            range->cnt = 1;
            pmix_list_append(&vreg->ranges, &range->super);
        }
        if (NULL != suffix) {
            free(suffix);
        }
        /* move to the next posn */
        if (NULL == cptr) {
            break;
        }
        vptr = cptr + 1;
    }
    free(vsave);

    /* begin constructing the regular expression */
    while (NULL != (vreg = (pmix_regex_value_t*)pmix_list_remove_first(&vids))) {
        /* if no ranges, then just add the name */
        if (0 == pmix_list_get_size(&vreg->ranges)) {
            if (NULL != vreg->prefix) {
                pmix_argv_append_nosize(&regexargs, vreg->prefix);
            }
            PMIX_RELEASE(vreg);
            continue;
        }
        /* start the regex for this value with the prefix */
        if (NULL != vreg->prefix) {
            if (0 > asprintf(&tmp, "%s[%d:", vreg->prefix, vreg->num_digits)) {
                return PMIX_ERR_NOMEM;
            }
        } else {
            if (0 > asprintf(&tmp, "[%d:", vreg->num_digits)) {
                return PMIX_ERR_NOMEM;
            }
        }
        /* add the ranges */
        while (NULL != (range = (pmix_regex_range_t*)pmix_list_remove_first(&vreg->ranges))) {
            if (1 == range->cnt) {
                if (0 > asprintf(&tmp2, "%s%d,", tmp, range->start)) {
                    return PMIX_ERR_NOMEM;
                }
            } else {
                if (0 > asprintf(&tmp2, "%s%d-%d,", tmp, range->start, range->start + range->cnt - 1)) {
                    return PMIX_ERR_NOMEM;
                }
            }
            free(tmp);
            tmp = tmp2;
            PMIX_RELEASE(range);
        }
        /* replace the final comma */
        tmp[strlen(tmp)-1] = ']';
        if (NULL != vreg->suffix) {
            /* add in the suffix, if provided */
            if (0 > asprintf(&tmp2, "%s%s", tmp, vreg->suffix)) {
                return PMIX_ERR_NOMEM;
            }
            free(tmp);
            tmp = tmp2;
        }
        pmix_argv_append_nosize(&regexargs, tmp);
        free(tmp);
        PMIX_RELEASE(vreg);
    }

    /* assemble final result */
    tmp = pmix_argv_join(regexargs, ',');
    if (0 > asprintf(regexp, "pmix[%s]", tmp)) {
        return PMIX_ERR_NOMEM;
    }
    free(tmp);

    /* cleanup */
    pmix_argv_free(regexargs);

    PMIX_DESTRUCT(&vids);
    return PMIX_SUCCESS;
}

static pmix_status_t generate_ppn(const char *input,
                                  char **regexp)
{
    char **ppn, **npn;
    int i, j, start, end;
    pmix_regex_value_t *vreg;
    pmix_regex_range_t *rng;
    pmix_list_t nodes;
    char *tmp, *tmp2;
    char *cptr;

    /* define the default */
    *regexp = NULL;

    /* setup the list of results */
    PMIX_CONSTRUCT(&nodes, pmix_list_t);

    /* split the input by node */
    ppn = pmix_argv_split(input, ';');

    /* for each node, split the input by comma */
    for (i=0; NULL != ppn[i]; i++) {
        rng = NULL;
        /* create a record for this node */
        vreg = PMIX_NEW(pmix_regex_value_t);
        pmix_list_append(&nodes, &vreg->super);
        /* split the input for this node */
        npn = pmix_argv_split(ppn[i], ',');
        /* look at each element */
        for (j=0; NULL != npn[j]; j++) {
            /* is this a range? */
            if (NULL != (cptr = strchr(npn[j], '-'))) {
                /* terminate the string */
                *cptr = '\0';
                ++cptr;
                start = strtol(npn[j], NULL, 10);
                end = strtol(cptr, NULL, 10);
                /* are we collecting a range? */
                if (NULL == rng) {
                    /* no - better start one */
                    rng = PMIX_NEW(pmix_regex_range_t);
                    rng->start = start;
                    rng->cnt = end - start + 1;
                    pmix_list_append(&vreg->ranges, &rng->super);
                } else {
                    /* is this a continuation of the current range? */
                    if (start == (rng->start + rng->cnt)) {
                        /* just add it to the end of this range */
                        rng->cnt++;
                    } else {
                        /* nope, there is a break - create new range */
                        rng = PMIX_NEW(pmix_regex_range_t);
                        rng->start = start;
                        rng->cnt = end - start + 1;
                        pmix_list_append(&vreg->ranges, &rng->super);
                    }
                }
            } else {
                /* single rank given */
                start = strtol(npn[j], NULL, 10);
                /* are we collecting a range? */
                if (NULL == rng) {
                    /* no - better start one */
                    rng = PMIX_NEW(pmix_regex_range_t);
                    rng->start = start;
                    rng->cnt = 1;
                    pmix_list_append(&vreg->ranges, &rng->super);
                } else {
                    /* is this a continuation of the current range? */
                    if (start == (rng->start + rng->cnt)) {
                        /* just add it to the end of this range */
                        rng->cnt++;
                    } else {
                        /* nope, there is a break - create new range */
                        rng = PMIX_NEW(pmix_regex_range_t);
                        rng->start = start;
                        rng->cnt = 1;
                        pmix_list_append(&vreg->ranges, &rng->super);
                    }
                }
            }
        }
        pmix_argv_free(npn);
    }
    pmix_argv_free(ppn);


    /* begin constructing the regular expression */
    tmp = strdup("pmix[");
    PMIX_LIST_FOREACH(vreg, &nodes, pmix_regex_value_t) {
        while (NULL != (rng = (pmix_regex_range_t*)pmix_list_remove_first(&vreg->ranges))) {
            if (1 == rng->cnt) {
                if (0 > asprintf(&tmp2, "%s%d,", tmp, rng->start)) {
                    return PMIX_ERR_NOMEM;
                }
            } else {
                if (0 > asprintf(&tmp2, "%s%d-%d,", tmp, rng->start, rng->start + rng->cnt - 1)) {
                    return PMIX_ERR_NOMEM;
                }
            }
            free(tmp);
            tmp = tmp2;
            PMIX_RELEASE(rng);
        }
        /* replace the final comma */
        tmp[strlen(tmp)-1] = ';';
    }

    /* replace the final semi-colon */
    tmp[strlen(tmp)-1] = ']';

    /* assemble final result */
    *regexp = tmp;

    PMIX_LIST_DESTRUCT(&nodes);
    return PMIX_SUCCESS;
}

static pmix_status_t parse_nodes(const char *regexp,
                                 char ***names)
{
    char *tmp, *ptr;
    pmix_status_t rc;

    /* set default */
    *names = NULL;

    /* protect against bozo */
    if (NULL == regexp) {
        return PMIX_SUCCESS;
    }

    /* protect the input string */
    tmp = strdup(regexp);
    /* strip the trailing bracket */
    tmp[strlen(tmp)-1] = '\0';

    /* the regex generator used to create this regex
     * is tagged at the beginning of the string */
    if (NULL == (ptr = strchr(tmp, '['))) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        free(tmp);
        return PMIX_ERR_BAD_PARAM;
    }
    *ptr = '\0';
    ++ptr;

    /* if it was done by PMIx, use that parser */
    if (0 == strcmp(tmp, "pmix")) {
        if (PMIX_SUCCESS != (rc = pmix_regex_extract_nodes(ptr, names))) {
            PMIX_ERROR_LOG(rc);
        }
    } else {
        /* this isn't an error - let someone else try */
        rc = PMIX_ERR_TAKE_NEXT_OPTION;
    }
    free(tmp);
    return rc;

}
static pmix_status_t parse_procs(const char *regexp,
                                 char ***procs)
{
    char *tmp, *ptr;
    pmix_status_t rc;

    /* set default */
    *procs = NULL;

    /* protect against bozo */
    if (NULL == regexp) {
        return PMIX_SUCCESS;
    }

    /* protect the input string */
    tmp = strdup(regexp);
    /* strip the trailing bracket */
    tmp[strlen(tmp)-1] = '\0';

    /* the regex generator used to create this regex
     * is tagged at the beginning of the string */
    if (NULL == (ptr = strchr(tmp, '['))) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        free(tmp);
        return PMIX_ERR_BAD_PARAM;
    }
    *ptr = '\0';
    ++ptr;

    /* if it was done by PMIx, use that parser */
    if (0 == strcmp(tmp, "pmix")) {
        if (PMIX_SUCCESS != (rc = pmix_regex_extract_ppn(ptr, procs))) {
            PMIX_ERROR_LOG(rc);
        }
    } else {
        /* this isn't an error - let someone else try */
        rc = PMIX_ERR_TAKE_NEXT_OPTION;
    }
    free(tmp);
    return rc;
}

static pmix_status_t resolve_peers(const char *nodename,
                                   const char *nspace,
                                   pmix_proc_t **procs, size_t *nprocs)
{
    pmix_cb_t cb;
    pmix_status_t rc;
    pmix_kval_t *kv;
    pmix_proc_t proc;
    char **ptr;
    pmix_info_t *info;
    pmix_proc_t *p=NULL;
    size_t ninfo, np=0, n, j;

    PMIX_CONSTRUCT(&cb, pmix_cb_t);

    cb.key = strdup(nodename);
    /* this data isn't going anywhere, so we don't require a copy */
    cb.copy = false;
    /* scope is irrelevant as the info we seek must be local */
    cb.scope = PMIX_SCOPE_UNDEF;
    /* let the proc point to the nspace */
    pmix_strncpy(proc.nspace, nspace, PMIX_MAX_NSLEN);
    proc.rank = PMIX_RANK_WILDCARD;
    cb.proc = &proc;

    PMIX_GDS_FETCH_KV(rc, pmix_client_globals.myserver, &cb);
    if (PMIX_SUCCESS != rc) {
        if (PMIX_ERR_INVALID_NAMESPACE != rc) {
            PMIX_ERROR_LOG(rc);
        }
        goto complete;
    }
    /* should just be the one value on the list */
    if (1 != pmix_list_get_size(&cb.kvs)) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        rc = PMIX_ERR_BAD_PARAM;
        goto complete;
    }
    kv = (pmix_kval_t*)pmix_list_get_first(&cb.kvs);
    /* the hostname used as a key with wildcard rank will return
     * a pmix_data_array_t of pmix_info_t structs */
    if (NULL == kv->value ||
        PMIX_DATA_ARRAY != kv->value->type ||
        NULL == kv->value->data.darray ||
        PMIX_INFO != kv->value->data.darray->type) {
        PMIX_ERROR_LOG(PMIX_ERR_DATA_VALUE_NOT_FOUND);
        rc = PMIX_ERR_DATA_VALUE_NOT_FOUND;
        goto complete;
    }
    info = (pmix_info_t*)kv->value->data.darray->array;
    ninfo = kv->value->data.darray->size;
    /* find the PMIX_LOCAL_PEERS key */
    for (n=0; n < ninfo; n++) {
        if (0 == strncmp(info[n].key, PMIX_LOCAL_PEERS, PMIX_MAX_KEYLEN)) {
            /* split the string */
            ptr = pmix_argv_split(info[n].value.data.string, ',');
            np = pmix_argv_count(ptr);
            PMIX_PROC_CREATE(p, np);
            if (NULL == p) {
                rc = PMIX_ERR_NOMEM;
                pmix_argv_free(ptr);
                goto complete;
            }
            for (j=0; j < np; j++) {
                pmix_strncpy(p[j].nspace, nspace, PMIX_MAX_NSLEN);
                p[j].rank = strtoul(ptr[j], NULL, 10);
            }
            rc = PMIX_SUCCESS;
            pmix_argv_free(ptr);
            break;
        }
    }

  complete:
    if (NULL != cb.info) {
        PMIX_INFO_FREE(cb.info, cb.ninfo);
    }
    if (NULL != cb.key) {
        free(cb.key);
        cb.key = NULL;
    }
    PMIX_DESTRUCT(&cb);
    *procs = p;
    *nprocs = np;

    return rc;
}

static pmix_status_t resolve_nodes(const char *nspace,
                                   char **nodelist)
{
    pmix_cb_t cb;
    pmix_status_t rc;
    pmix_kval_t *kv;
    pmix_proc_t proc;

    PMIX_CONSTRUCT(&cb, pmix_cb_t);

    /* setup default answer */
    *nodelist = NULL;

    /* create a pmix_info_t so we can pass the nspace
    * into the fetch as a qualifier */
    PMIX_INFO_CREATE(cb.info, 1);
    if (NULL == cb.info) {
        PMIX_DESTRUCT(&cb);
        return PMIX_ERR_NOMEM;
    }
    cb.ninfo = 1;
    PMIX_INFO_LOAD(&cb.info[0], PMIX_NSPACE, nspace, PMIX_STRING);

    /* tell the GDS what we want */
    cb.key = PMIX_NODE_MAP;
    /* this data isn't going anywhere, so we don't require a copy */
    cb.copy = false;
    /* scope is irrelevant as the info we seek must be local */
    cb.scope = PMIX_SCOPE_UNDEF;
    /* put the nspace in the proc field */
    pmix_strncpy(proc.nspace, nspace, PMIX_MAX_NSLEN);
    /* the info will be associated with PMIX_RANK_WILDCARD */
    proc.rank = PMIX_RANK_WILDCARD;
    cb.proc = &proc;

    PMIX_GDS_FETCH_KV(rc, pmix_client_globals.myserver, &cb);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }
    /* should just be the one value on the list */
    if (1 != pmix_list_get_size(&cb.kvs)) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        rc = PMIX_ERR_BAD_PARAM;
        goto complete;
    }
    kv = (pmix_kval_t*)pmix_list_get_first(&cb.kvs);
    /* the PMIX_NODE_MAP key is supposed to return
    * a regex string  - check that it did */
    if (NULL == kv->value ||
        PMIX_STRING != kv->value->type) {
        PMIX_ERROR_LOG(PMIX_ERR_DATA_VALUE_NOT_FOUND);
        rc = PMIX_ERR_DATA_VALUE_NOT_FOUND;
        goto complete;
    }
    /* return the string */
    if (NULL != kv->value->data.string) {
        *nodelist = strdup(kv->value->data.string);
    }

  complete:
    if (NULL != cb.info) {
      PMIX_INFO_FREE(cb.info, cb.ninfo);
    }
    return rc;
}

static pmix_status_t pmix_regex_extract_nodes(char *regexp, char ***names)
{
    int i, j, k, len;
    pmix_status_t ret;
    char *base;
    char *orig, *suffix;
    bool found_range = false;
    bool more_to_come = false;
    int num_digits;

    /* set the default */
    *names = NULL;

    if (NULL == regexp) {
        return PMIX_SUCCESS;
    }

    orig = base = strdup(regexp);
    if (NULL == base) {
        PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    PMIX_OUTPUT_VERBOSE((1, pmix_globals.debug_output,
                         "pmix:extract:nodes: checking list: %s", regexp));

    do {
        /* Find the base */
        len = strlen(base);
        for (i = 0; i <= len; ++i) {
            if (base[i] == '[') {
                /* we found a range. this gets dealt with below */
                base[i] = '\0';
                found_range = true;
                break;
            }
            if (base[i] == ',') {
                /* we found a singleton value, and there are more to come */
                base[i] = '\0';
                found_range = false;
                more_to_come = true;
                break;
            }
            if (base[i] == '\0') {
                /* we found a singleton value */
                found_range = false;
                more_to_come = false;
                break;
            }
        }
        if (i == 0 && !found_range) {
            /* we found a special character at the beginning of the string */
            free(orig);
            return PMIX_ERR_BAD_PARAM;
        }

        if (found_range) {
            /* If we found a range, get the number of digits in the numbers */
            i++;  /* step over the [ */
            for (j=i; j < len; j++) {
                if (base[j] == ':') {
                    base[j] = '\0';
                    break;
                }
            }
            if (j >= len) {
                /* we didn't find the number of digits */
                free(orig);
                return PMIX_ERR_BAD_PARAM;
            }
            num_digits = strtol(&base[i], NULL, 10);
            i = j + 1;  /* step over the : */
            /* now find the end of the range */
            for (j = i; j < len; ++j) {
                if (base[j] == ']') {
                    base[j] = '\0';
                    break;
                }
            }
            if (j >= len) {
                /* we didn't find the end of the range */
                free(orig);
                return PMIX_ERR_BAD_PARAM;
            }
            /* check for a suffix */
            if (j+1 < len && base[j+1] != ',') {
                /* find the next comma, if present */
                for (k=j+1; k < len && base[k] != ','; k++);
                if (k < len) {
                    base[k] = '\0';
                }
                suffix = strdup(&base[j+1]);
                if (k < len) {
                    base[k] = ',';
                }
                j = k-1;
            } else {
                suffix = NULL;
            }
            PMIX_OUTPUT_VERBOSE((1, pmix_globals.debug_output,
                                 "regex:extract:nodes: parsing range %s %s %s",
                                 base, base + i, suffix));

            ret = regex_parse_value_ranges(base, base + i, num_digits, suffix, names);
            if (NULL != suffix) {
                free(suffix);
            }
            if (PMIX_SUCCESS != ret) {
                free(orig);
                return ret;
            }
            if (j+1 < len && base[j + 1] == ',') {
                more_to_come = true;
                base = &base[j + 2];
            } else {
                more_to_come = false;
            }
        } else {
            /* If we didn't find a range, just add the value */
            if(PMIX_SUCCESS != (ret = pmix_argv_append_nosize(names, base))) {
                PMIX_ERROR_LOG(ret);
                free(orig);
                return ret;
            }
            /* step over the comma */
            i++;
            /* set base equal to the (possible) next base to look at */
            base = &base[i];
        }
    } while(more_to_come);

    free(orig);

    /* All done */
    return ret;
}


/*
 * Parse one or more ranges in a set
 *
 * @param base     The base text of the value name
 * @param *ranges  A pointer to a range. This can contain multiple ranges
 *                 (i.e. "1-3,10" or "5" or "9,0100-0130,250")
 * @param ***names An argv array to add the newly discovered values to
 */
static pmix_status_t regex_parse_value_ranges(char *base, char *ranges,
                                              int num_digits, char *suffix,
                                              char ***names)
{
    int i, len;
    pmix_status_t ret;
    char *start, *orig;

    /* Look for commas, the separator between ranges */

    len = strlen(ranges);
    for (orig = start = ranges, i = 0; i < len; ++i) {
        if (',' == ranges[i]) {
            ranges[i] = '\0';
            ret = regex_parse_value_range(base, start, num_digits, suffix, names);
            if (PMIX_SUCCESS != ret) {
                PMIX_ERROR_LOG(ret);
                return ret;
            }
            start = ranges + i + 1;
        }
    }

    /* Pick up the last range, if it exists */

    if (start < orig + len) {

        PMIX_OUTPUT_VERBOSE((1, pmix_globals.debug_output,
                             "regex:parse:ranges: parse range %s (2)", start));

        ret = regex_parse_value_range(base, start, num_digits, suffix, names);
        if (PMIX_SUCCESS != ret) {
            PMIX_ERROR_LOG(ret);
            return ret;
        }
    }

    /* All done */
    return PMIX_SUCCESS;
}


/*
 * Parse a single range in a set and add the full names of the values
 * found to the names argv
 *
 * @param base     The base text of the value name
 * @param *ranges  A pointer to a single range. (i.e. "1-3" or "5")
 * @param ***names An argv array to add the newly discovered values to
 */
static pmix_status_t regex_parse_value_range(char *base, char *range,
                                             int num_digits, char *suffix,
                                             char ***names)
{
    char *str, tmp[132];
    size_t i, k, start, end;
    size_t base_len, len;
    bool found;
    pmix_status_t ret;

    if (NULL == base || NULL == range) {
        return PMIX_ERROR;
    }

    len = strlen(range);
    base_len = strlen(base);
    /* Silence compiler warnings; start and end are always assigned
     properly, below */
    start = end = 0;

    /* Look for the beginning of the first number */

    for (found = false, i = 0; i < len; ++i) {
        if (isdigit((int) range[i])) {
            if (!found) {
                start = atoi(range + i);
                found = true;
                break;
            }
        }
    }
    if (!found) {
        PMIX_ERROR_LOG(PMIX_ERR_NOT_FOUND);
        return PMIX_ERR_NOT_FOUND;
    }

    /* Look for the end of the first number */

    for (found = false; i < len; ++i) {
        if (!isdigit(range[i])) {
            break;
        }
    }

    /* Was there no range, just a single number? */

    if (i >= len) {
        end = start;
        found = true;
    } else {
        /* Nope, there was a range.  Look for the beginning of the second
         * number
         */
        for (; i < len; ++i) {
            if (isdigit(range[i])) {
                end = strtol(range + i, NULL, 10);
                found = true;
                break;
            }
        }
    }
    if (!found) {
        PMIX_ERROR_LOG(PMIX_ERR_NOT_FOUND);
        return PMIX_ERR_NOT_FOUND;
    }

    /* Make strings for all values in the range */

    len = base_len + num_digits + 32;
    if (NULL != suffix) {
        len += strlen(suffix);
    }
    str = (char *) malloc(len);
    if (NULL == str) {
        PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    for (i = start; i <= end; ++i) {
        memset(str, 0, len);
        strcpy(str, base);
        /* we need to zero-pad the digits */
        for (k=0; k < (size_t)num_digits; k++) {
            str[k+base_len] = '0';
        }
        memset(tmp, 0, 132);
        snprintf(tmp, 132, "%lu", (unsigned long)i);
        for (k=0; k < strlen(tmp); k++) {
            str[base_len + num_digits - k - 1] = tmp[strlen(tmp)-k-1];
        }
        /* if there is a suffix, add it */
        if (NULL != suffix) {
            strcat(str, suffix);
        }
        ret = pmix_argv_append_nosize(names, str);
        if(PMIX_SUCCESS != ret) {
            PMIX_ERROR_LOG(ret);
            free(str);
            return ret;
        }
    }
    free(str);

    /* All done */
    return PMIX_SUCCESS;
}

static pmix_status_t pmix_regex_extract_ppn(char *regexp, char ***procs)
{
    char **rngs, **nds, *t, **ps=NULL;
    int i, j, k, start, end;

    /* split on semi-colons for nodes */
    nds = pmix_argv_split(regexp, ';');
    for (j=0; NULL != nds[j]; j++) {
        /* for each node, split it by comma */
        rngs = pmix_argv_split(nds[j], ',');
        /* parse each element */
        for (i=0; NULL != rngs[i]; i++) {
            /* look for a range */
            if (NULL == (t = strchr(rngs[i], '-'))) {
                /* just one value */
                pmix_argv_append_nosize(&ps, rngs[i]);
            } else {
                /* handle the range */
                *t = '\0';
                start = strtol(rngs[i], NULL, 10);
                ++t;
                end = strtol(t, NULL, 10);
                for (k=start; k <= end; k++) {
                    if (0 > asprintf(&t, "%d", k)) {
                        pmix_argv_free(nds);
                        pmix_argv_free(rngs);
                        return PMIX_ERR_NOMEM;
                    }
                    pmix_argv_append_nosize(&ps, t);
                    free(t);
                }
            }
        }
        pmix_argv_free(rngs);
        /* create the node entry */
        t = pmix_argv_join(ps, ',');
        pmix_argv_append_nosize(procs, t);
        free(t);
        pmix_argv_free(ps);
        ps = NULL;
    }

    pmix_argv_free(nds);
    return PMIX_SUCCESS;
}
