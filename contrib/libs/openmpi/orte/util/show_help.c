/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte_config.h"
#include "orte/types.h"
#include "orte/constants.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

#include "opal/util/show_help.h"
#include "opal/util/output.h"
#include "opal/dss/dss.h"
#include "opal/mca/event/event.h"
#include "opal/mca/pmix/pmix.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/routed/routed.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"

#include "orte/util/show_help.h"

bool orte_help_want_aggregate = false;
static int orte_help_output;

/*
 * Local variable to know whether aggregated show_help is available or
 * not
 */
static bool ready = false;

/*
 * Same for systems with or without full ORTE support
 */
bool orte_show_help_is_available(void)
{
    /* This is a function only to give us forward flexibility in case
       we need a more complicated check someday. */

    return ready;
}

/* List items for holding (filename, topic) tuples */
typedef struct {
    opal_list_item_t super;
    /* The filename */
    char *tli_filename;
    /* The topic */
    char *tli_topic;
    /* List of process names that have displayed this (filename, topic) */
    opal_list_t tli_processes;
    /* Time this message was displayed */
    time_t tli_time_displayed;
    /* Count of processes since last display (i.e., "new" processes
       that have showed this message that have not yet been output) */
    int tli_count_since_last_display;
    /* Do we want to display these? */
    bool tli_display;
} tuple_list_item_t;

static void tuple_list_item_constructor(tuple_list_item_t *obj);
static void tuple_list_item_destructor(tuple_list_item_t *obj);
static OBJ_CLASS_INSTANCE(tuple_list_item_t, opal_list_item_t,
                   tuple_list_item_constructor,
                   tuple_list_item_destructor);


/* List of (filename, topic) tuples that have already been displayed */
static opal_list_t abd_tuples;

/* How long to wait between displaying duplicate show_help notices */
static struct timeval show_help_interval = { 5, 0 };

/* Timer for displaying duplicate help message notices */
static time_t show_help_time_last_displayed = 0;
static bool show_help_timer_set = false;
static opal_event_t show_help_timer_event;

static opal_show_help_fn_t save_help = NULL;

static void tuple_list_item_constructor(tuple_list_item_t *obj)
{
    obj->tli_filename = NULL;
    obj->tli_topic = NULL;
    OBJ_CONSTRUCT(&(obj->tli_processes), opal_list_t);
    obj->tli_time_displayed = time(NULL);
    obj->tli_count_since_last_display = 0;
    obj->tli_display = true;
}

static void tuple_list_item_destructor(tuple_list_item_t *obj)
{
    opal_list_item_t *item, *next;

    if (NULL != obj->tli_filename) {
        free(obj->tli_filename);
    }
    if (NULL != obj->tli_topic) {
        free(obj->tli_topic);
    }
    for (item = opal_list_get_first(&(obj->tli_processes));
         opal_list_get_end(&(obj->tli_processes)) != item;
         item = next) {
        next = opal_list_get_next(item);
        opal_list_remove_item(&(obj->tli_processes), item);
        OBJ_RELEASE(item);
    }
}

/* dealing with special characters in xml output */
static char* xml_format(unsigned char *input)
{
    int i, j, k, len, outlen;
    char *output, qprint[10];
    char *endtag="</stderr>";
    char *starttag="<stderr>";
    int endtaglen, starttaglen;
    bool endtagged = false;

    len = strlen((char*)input);
    /* add some arbitrary size padding */
    output = (char*)malloc((len+1024)*sizeof(char));
    if (NULL == output) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return (char*)input; /* default to no xml formatting */
    }
    memset(output, 0, len+1024);
    outlen = len+1023;
    endtaglen = strlen(endtag);
    starttaglen = strlen(starttag);

    /* start at the beginning */
    k=0;

    /* start with the tag */
    for (j=0; j < starttaglen && k < outlen; j++) {
        output[k++] = starttag[j];
    }

    for (i=0; i < len; i++) {
        if ('&' == input[i]) {
            if (k+5 >= outlen) {
                ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                goto error;
            }
            snprintf(qprint, 10, "&amp;");
            for (j=0; j < (int)strlen(qprint) && k < outlen; j++) {
                output[k++] = qprint[j];
            }
        } else if ('<' == input[i]) {
            if (k+4 >= outlen) {
                ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                goto error;
            }
            snprintf(qprint, 10, "&lt;");
            for (j=0; j < (int)strlen(qprint) && k < outlen; j++) {
                output[k++] = qprint[j];
            }
        } else if ('>' == input[i]) {
            if (k+4 >= outlen) {
                ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                goto error;
            }
            snprintf(qprint, 10, "&gt;");
            for (j=0; j < (int)strlen(qprint) && k < outlen; j++) {
                output[k++] = qprint[j];
            }
        } else if (input[i] < 32 || input[i] > 127) {
            /* this is a non-printable character, so escape it too */
            if (k+7 >= outlen) {
                ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                goto error;
            }
            snprintf(qprint, 10, "&#%03d;", (int)input[i]);
            for (j=0; j < (int)strlen(qprint) && k < outlen; j++) {
                output[k++] = qprint[j];
            }
            /* if this was a \n, then we also need to break the line with the end tag */
            if ('\n' == input[i] && (k+endtaglen+1) < outlen) {
                /* we need to break the line with the end tag */
                for (j=0; j < endtaglen && k < outlen-1; j++) {
                    output[k++] = endtag[j];
                }
                /* move the <cr> over */
                output[k++] = '\n';
                /* if this isn't the end of the input buffer, add a new start tag */
                if (i < len-1 && (k+starttaglen) < outlen) {
                    for (j=0; j < starttaglen && k < outlen; j++) {
                        output[k++] = starttag[j];
                        endtagged = false;
                    }
                } else {
                    endtagged = true;
                }
            }
        } else {
            output[k++] = input[i];
        }
    }

    if (!endtagged) {
        /* need to add an endtag */
        for (j=0; j < endtaglen && k < outlen-1; j++) {
            output[k++] = endtag[j];
        }
        output[k++] = '\n';
    }

    return output;

error:
    /* if we couldn't complete the processing for
     * some reason, return the unprocessed input
     * so at least the message gets out!
     */
    free(output);
    return (char*)input;
}


/*
 * Returns ORTE_SUCCESS if the strings match; ORTE_ERROR otherwise.
 */
static int match(const char *a, const char *b)
{
    int rc = ORTE_ERROR;
    char *p1, *p2, *tmp1 = NULL, *tmp2 = NULL;
    size_t min;

    /* Check straight string match first */
    if (0 == strcmp(a, b)) return ORTE_SUCCESS;

    if (NULL != strchr(a, '*') || NULL != strchr(b, '*')) {
        tmp1 = strdup(a);
        if (NULL == tmp1) {
            return ORTE_ERR_OUT_OF_RESOURCE;
        }
        tmp2 = strdup(b);
        if (NULL == tmp2) {
            free(tmp1);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }
        p1 = strchr(tmp1, '*');
        p2 = strchr(tmp2, '*');

        if (NULL != p1) {
            *p1 = '\0';
        }
        if (NULL != p2) {
            *p2 = '\0';
        }
        min = strlen(tmp1);
        if (strlen(tmp2) < min) {
            min = strlen(tmp2);
        }
        if (0 == min || 0 == strncmp(tmp1, tmp2, min)) {
            rc = ORTE_SUCCESS;
        }
        free(tmp1);
        free(tmp2);
        return rc;
    }

    /* No match */
    return ORTE_ERROR;
}

/*
 * Check to see if a given (filename, topic) tuple has been displayed
 * already.  Return ORTE_SUCCESS if so, or ORTE_ERR_NOT_FOUND if not.
 *
 * Always return a tuple_list_item_t representing this (filename,
 * topic) entry in the list of "already been displayed tuples" (if it
 * wasn't in the list already, this function will create a new entry
 * in the list and return it).
 *
 * Note that a list is not an overly-efficient mechanism for this kind
 * of data.  The assupmtion is that there will only be a small numebr
 * of (filename, topic) tuples displayed so the storage required will
 * be fairly small, and linear searches will be fast enough.
 */
static int get_tli(const char *filename, const char *topic,
                   tuple_list_item_t **tli)
{
    opal_list_item_t *item;

    /* Search the list for a duplicate. */
    for (item = opal_list_get_first(&abd_tuples);
         opal_list_get_end(&abd_tuples) != item;
         item = opal_list_get_next(item)) {
        (*tli) = (tuple_list_item_t*) item;
        if (ORTE_SUCCESS == match((*tli)->tli_filename, filename) &&
            ORTE_SUCCESS == match((*tli)->tli_topic, topic)) {
            return ORTE_SUCCESS;
        }
    }

    /* Nope, we didn't find it -- make a new one */
    *tli = OBJ_NEW(tuple_list_item_t);
    if (NULL == *tli) {
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    (*tli)->tli_filename = strdup(filename);
    (*tli)->tli_topic = strdup(topic);
    opal_list_append(&abd_tuples, &((*tli)->super));
    return ORTE_ERR_NOT_FOUND;
}


static void show_accumulated_duplicates(int fd, short event, void *context)
{
    opal_list_item_t *item;
    time_t now = time(NULL);
    tuple_list_item_t *tli;
    char *tmp, *output;

    /* Loop through all the messages we've displayed and see if any
       processes have sent duplicates that have not yet been displayed
       yet */
    for (item = opal_list_get_first(&abd_tuples);
         opal_list_get_end(&abd_tuples) != item;
         item = opal_list_get_next(item)) {
        tli = (tuple_list_item_t*) item;
        if (tli->tli_display &&
            tli->tli_count_since_last_display > 0) {
            static bool first = true;
            if (orte_xml_output) {
                asprintf(&tmp, "%d more process%s sent help message %s / %s",
                         tli->tli_count_since_last_display,
                         (tli->tli_count_since_last_display > 1) ? "es have" : " has",
                         tli->tli_filename, tli->tli_topic);
                output = xml_format((unsigned char*)tmp);
                free(tmp);
                fprintf(orte_xml_fp, "%s", output);
                free(output);
            } else {
                opal_output(0, "%d more process%s sent help message %s / %s",
                            tli->tli_count_since_last_display,
                            (tli->tli_count_since_last_display > 1) ? "es have" : " has",
                            tli->tli_filename, tli->tli_topic);
            }
            tli->tli_count_since_last_display = 0;

            if (first) {
                if (orte_xml_output) {
                    fprintf(orte_xml_fp, "<stderr>Set MCA parameter \"orte_base_help_aggregate\" to 0 to see all help / error messages</stderr>\n");
                    fflush(orte_xml_fp);
                } else {
                    opal_output(0, "Set MCA parameter \"orte_base_help_aggregate\" to 0 to see all help / error messages");
                }
                first = false;
            }
        }
    }

    show_help_time_last_displayed = now;
    show_help_timer_set = false;
}

static int show_help(const char *filename, const char *topic,
                     const char *output, orte_process_name_t *sender)
{
    int rc;
    tuple_list_item_t *tli = NULL;
    orte_namelist_t *pnli;
    time_t now = time(NULL);

    /* If we're aggregating, check for duplicates.  Otherwise, don't
       track duplicates at all and always display the message. */
    if (orte_help_want_aggregate) {
        rc = get_tli(filename, topic, &tli);
    } else {
        rc = ORTE_ERR_NOT_FOUND;
    }

    /* If there's no output string (i.e., this is a control message
       asking us to suppress), then skip to the end. */
    if (NULL == output) {
        tli->tli_display = false;
        goto after_output;
    }

    /* Was it already displayed? */
    if (ORTE_SUCCESS == rc) {
        /* Yes.  But do we want to print anything?  That's complicated.

           We always show the first message of a given (filename,
           topic) tuple as soon as it arrives.  But we don't want to
           show duplicate notices often, because we could get overrun
           with them.  So we want to gather them up and say "We got N
           duplicates" every once in a while.

           And keep in mind that at termination, we'll unconditionally
           show all accumulated duplicate notices.

           A simple scheme is as follows:
           - when the first of a (filename, topic) tuple arrives
             - print the message
             - if a timer is not set, set T=now
           - when a duplicate (filename, topic) tuple arrives
             - if now>(T+5) and timer is not set (due to
               non-pre-emptiveness of our libevent, a timer *could* be
               set!)
               - print all accumulated duplicates
               - reset T=now
             - else if a timer was not set, set the timer for T+5
             - else if a timer was set, do nothing (just wait)
           - set T=now when the timer expires
        */
        ++tli->tli_count_since_last_display;
        if (now > show_help_time_last_displayed + 5 && !show_help_timer_set) {
            show_accumulated_duplicates(0, 0, NULL);
        } else if (!show_help_timer_set) {
            opal_event_evtimer_set(orte_event_base, &show_help_timer_event,
                                   show_accumulated_duplicates, NULL);
            opal_event_evtimer_add(&show_help_timer_event, &show_help_interval);
            show_help_timer_set = true;
        }
    }
    /* Not already displayed */
    else if (ORTE_ERR_NOT_FOUND == rc) {
        if (orte_xml_output) {
            char *tmp;
            tmp = xml_format((unsigned char*)output);
            fprintf(orte_xml_fp, "%s", tmp);
            fflush(orte_xml_fp);
            free(tmp);
        } else {
            opal_output(orte_help_output, "%s", output);
        }
        if (!show_help_timer_set) {
            show_help_time_last_displayed = now;
        }
    }
    /* Some other error occurred */
    else {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

 after_output:
    /* If we're aggregating, add this process name to the list */
    if (orte_help_want_aggregate) {
        pnli = OBJ_NEW(orte_namelist_t);
        if (NULL == pnli) {
            rc = ORTE_ERR_OUT_OF_RESOURCE;
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        pnli->name = *sender;
        opal_list_append(&(tli->tli_processes), &(pnli->super));
    }
    return ORTE_SUCCESS;
}


/* Note that this function is called from ess/hnp, so don't make it
   static */
void orte_show_help_recv(int status, orte_process_name_t* sender,
                         opal_buffer_t *buffer, orte_rml_tag_t tag,
                         void* cbdata)
{
    char *output=NULL;
    char *filename=NULL, *topic=NULL;
    int32_t n;
    int8_t have_output;
    int rc;

    OPAL_OUTPUT_VERBOSE((5, orte_debug_output,
                         "%s got show_help from %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(sender)));

    /* unpack the filename of the show_help text file */
    n = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &filename, &n, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }
    /* unpack the topic tag */
    n = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &topic, &n, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }
    /* unpack the flag */
    n = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &have_output, &n, OPAL_INT8))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }

    /* If we have an output string, unpack it */
    if (have_output) {
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &output, &n, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            goto cleanup;
        }
    }

    /* Send it to show_help */
    rc = show_help(filename, topic, output, sender);

cleanup:
    if (NULL != output) {
        free(output);
    }
    if (NULL != filename) {
        free(filename);
    }
    if (NULL != topic) {
        free(topic);
    }
 }

int orte_show_help_init(void)
{
    opal_output_stream_t lds;

    OPAL_OUTPUT_VERBOSE((5, orte_debug_output, "orte_show_help init"));

    /* Show help duplicate detection */
    if (ready) {
        return ORTE_SUCCESS;
    }

    OBJ_CONSTRUCT(&abd_tuples, opal_list_t);

    /* create an output stream for us */
    OBJ_CONSTRUCT(&lds, opal_output_stream_t);
    lds.lds_want_stderr = true;
    orte_help_output = opal_output_open(&lds);
    OBJ_DESTRUCT(&lds);

    save_help = opal_show_help;
    opal_show_help = orte_show_help;
    ready = true;

    return ORTE_SUCCESS;
}

void orte_show_help_finalize(void)
{
    if (!ready) {
        return;
    }
    ready = false;

    opal_output_close(orte_help_output);

    opal_show_help = save_help;
    save_help = NULL;

    /* Shutdown show_help, showing final messages */
    if (ORTE_PROC_IS_HNP) {
        show_accumulated_duplicates(0, 0, NULL);
        OBJ_DESTRUCT(&abd_tuples);
        if (show_help_timer_set) {
            opal_event_evtimer_del(&show_help_timer_event);
        }

        /* cancel the recv */
        orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_SHOW_HELP);
        return;
    }
}

int orte_show_help(const char *filename, const char *topic,
                   bool want_error_header, ...)
{
    int rc = ORTE_SUCCESS;
    va_list arglist;
    char *output;

    if (orte_execute_quiet) {
        return ORTE_SUCCESS;
    }

    va_start(arglist, want_error_header);
    output = opal_show_help_vstring(filename, topic, want_error_header,
                                    arglist);
    va_end(arglist);

    /* If nothing came back, there's nothing to do */
    if (NULL == output) {
        return ORTE_SUCCESS;
    }

    rc = orte_show_help_norender(filename, topic, want_error_header, output);
    free(output);
    return rc;
}

static void cbfunc(int status, void *cbdata)
{
    volatile bool *active = (volatile bool*)cbdata;
    *active = false;
}

int orte_show_help_norender(const char *filename, const char *topic,
                            bool want_error_header, const char *output)
{
    int rc = ORTE_SUCCESS;
    int8_t have_output = 1;
    opal_buffer_t *buf;
    bool am_inside = false;
    opal_list_t info;
    opal_value_t *kv;
    volatile bool active;
    struct timespec tp;

    if (!ready) {
        /* if we are finalizing, then we have no way to process
         * this through the orte_show_help system - just drop it to
         * stderr; that's at least better than not showing it.
         *
         * If we are not finalizing, then this is probably a show_help
         * stemming from either a cmd-line request to display the usage
         * message, or a show_help related to a user error. In either case,
         * we can't do anything but just print to stderr.
         */
        fprintf(stderr, "%s", output);
        goto CLEANUP;
    }

    /* if we are the HNP, or the RML has not yet been setup,
     * or ROUTED has not been setup,
     * or we weren't given an HNP, or we are running in standalone
     * mode, then all we can do is process this locally
     */
    if (ORTE_PROC_IS_HNP || ORTE_PROC_IS_TOOL ||
        orte_standalone_operation) {
        rc = show_help(filename, topic, output, ORTE_PROC_MY_NAME);
        goto CLEANUP;
    } else if (ORTE_PROC_IS_DAEMON) {
        if (NULL == orte_rml.send_buffer_nb ||
            NULL == orte_routed.get_route ||
            NULL == orte_process_info.my_hnp_uri) {
            rc = show_help(filename, topic, output, ORTE_PROC_MY_NAME);
            goto CLEANUP;
        }
    }

    /* otherwise, we relay the output message to
     * the HNP for processing
     */

    /* JMS Note that we *may* have a recursion situation here where
       the RML could call show_help.  Need to think about this
       properly, but put a safeguard in here for sure for the time
       being. */
    if (am_inside) {
        rc = show_help(filename, topic, output, ORTE_PROC_MY_NAME);
    } else {
        am_inside = true;

        /* build the message to the HNP */
        buf = OBJ_NEW(opal_buffer_t);
        /* pack the filename of the show_help text file */
        opal_dss.pack(buf, &filename, 1, OPAL_STRING);
        /* pack the topic tag */
        opal_dss.pack(buf, &topic, 1, OPAL_STRING);
        /* pack the flag that we have a string */
        opal_dss.pack(buf, &have_output, 1, OPAL_INT8);
        /* pack the resulting string */
        opal_dss.pack(buf, &output, 1, OPAL_STRING);

        /* if we are a daemon, then send it via RML to the HNP */
        if (ORTE_PROC_IS_DAEMON) {
            /* send it to the HNP */
            if (ORTE_SUCCESS != (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                              ORTE_PROC_MY_HNP, buf,
                                                              ORTE_RML_TAG_SHOW_HELP,
                                                              orte_rml_send_callback, NULL))) {
                OBJ_RELEASE(buf);
                /* okay, that didn't work, output locally  */
                opal_output(orte_help_output, "%s", output);
            } else {
                rc = ORTE_SUCCESS;
            }
        } else {
            /* if we are not a daemon (i.e., we are an app) and if PMIx
             * support for "log" is available, then use that channel */
            if (NULL != opal_pmix.log) {
                OBJ_CONSTRUCT(&info, opal_list_t);
                kv = OBJ_NEW(opal_value_t),
                kv->key = strdup(OPAL_PMIX_LOG_MSG);
                kv->type = OPAL_BYTE_OBJECT;
                opal_dss.unload(buf, (void**)&kv->data.bo.bytes, &kv->data.bo.size);
                opal_list_append(&info, &kv->super);
                active = true;
                tp.tv_sec = 0;
                tp.tv_nsec = 1000000;
                opal_pmix.log(&info, cbfunc, (void*)&active);
                while (active) {
                    nanosleep(&tp, NULL);
                }
                OBJ_RELEASE(buf);
                kv->data.bo.bytes = NULL;
                OPAL_LIST_DESTRUCT(&info);
                rc = ORTE_SUCCESS;
                goto CLEANUP;
            } else {
                rc = show_help(filename, topic, output, ORTE_PROC_MY_NAME);
            }
        }
        am_inside = false;
    }

CLEANUP:
    return rc;
}

int orte_show_help_suppress(const char *filename, const char *topic)
{
    int rc = ORTE_SUCCESS;
    int8_t have_output = 0;

    if (orte_execute_quiet) {
        return ORTE_SUCCESS;
    }

    if (!ready) {
        /* If we are finalizing, then we have no way to process this
           through the orte_show_help system - just drop it. */
        return ORTE_SUCCESS;
    }

    /* If we are the HNP, or the RML has not yet been setup, or ROUTED
       has not been setup, or we weren't given an HNP, then all we can
       do is process this locally. */
    if (ORTE_PROC_IS_HNP ||
        NULL == orte_rml.send_buffer_nb ||
        NULL == orte_routed.get_route ||
        NULL == orte_process_info.my_hnp_uri) {
        rc = show_help(filename, topic, NULL, ORTE_PROC_MY_NAME);
    }

    /* otherwise, we relay the output message to
     * the HNP for processing
     */
    else {
        opal_buffer_t *buf;
        static bool am_inside = false;

        /* JMS Note that we *may* have a recursion situation here where
           the RML could call show_help.  Need to think about this
           properly, but put a safeguard in here for sure for the time
           being. */
        if (am_inside) {
            rc = show_help(filename, topic, NULL, ORTE_PROC_MY_NAME);
        } else {
            am_inside = true;

            /* build the message to the HNP */
            buf = OBJ_NEW(opal_buffer_t);
            /* pack the filename of the show_help text file */
            opal_dss.pack(buf, &filename, 1, OPAL_STRING);
            /* pack the topic tag */
            opal_dss.pack(buf, &topic, 1, OPAL_STRING);
            /* pack the flag that we DO NOT have a string */
            opal_dss.pack(buf, &have_output, 1, OPAL_INT8);
            /* send it to the HNP */
            if (ORTE_SUCCESS != (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                              ORTE_PROC_MY_HNP, buf,
                                                              ORTE_RML_TAG_SHOW_HELP,
                                                              orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(buf);
                /* okay, that didn't work, just process locally error, just ignore return  */
                show_help(filename, topic, NULL, ORTE_PROC_MY_NAME);
            }
            am_inside = false;
        }
    }

    return ORTE_SUCCESS;
}
