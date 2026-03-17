/*
 * Copyright (C) 2014      Artem Polyakov <artpol84@gmail.com>
 * Copyright (c) 2014-2016 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <pmix_common.h>

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include <errno.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif


#if PMIX_ENABLE_TIMING

#include "src/class/pmix_pointer_array.h"
#include "src/class/pmix_list.h"
#include "src/util/output.h"
#include "src/util/basename.h"

#include "src/util/timings.h"

#define DELTAS_SANE_LIMIT (10*1024*1024)

struct interval_descr{
    pmix_timing_event_t *descr_ev, *begin_ev;
    double interval, overhead;
};

pmix_timing_event_t *pmix_timing_event_alloc(pmix_timing_t *t);
void pmix_timing_init(pmix_timing_t *t);
pmix_timing_prep_t pmix_timing_prep_ev(pmix_timing_t *t, const char *fmt, ...);

static PMIX_CLASS_INSTANCE(pmix_timing_event_t, pmix_list_item_t, NULL, NULL);


static char *nodename = NULL;
static char *jobid = "";
static double hnp_offs = 0;
static bool pmix_timing_overhead = false;

void pmix_init_id(char* nspace, int rank)
{
    asprintf(&jobid, "%s:%d", nspace, rank);
}

/* Get current timestamp. Derived from MPI_Wtime */

static double get_ts_gettimeofday(void)
{
    double ret;
    /* Use gettimeofday() if we pmix wasn't initialized */
    struct timeval tv;
    gettimeofday(&tv, NULL);
    ret = tv.tv_sec;
    ret += (double)tv.tv_usec / 1000000.0;
    return ret;
}

static get_ts_t _init_timestamping(void)
{
    return get_ts_gettimeofday;
}


pmix_timing_event_t *pmix_timing_event_alloc(pmix_timing_t *t)
{
    if( t->buffer_offset >= t->buffer_size ){
        // notch timings overhead
        double alloc_begin = t->get_ts();

        t->buffer = malloc(sizeof(pmix_timing_event_t)*t->buffer_size);
        if( t->buffer == NULL ){
            return NULL;
        }
        memset(t->buffer, 0, sizeof(pmix_timing_event_t)*t->buffer_size);

        double alloc_end = t->get_ts();

        t->buffer_offset = 0;
        t->buffer[0].fib = 1;
        t->buffer[0].ts_ovh = alloc_end - alloc_begin;
    }
    int tmp = t->buffer_offset;
    (t->buffer_offset)++;
    return t->buffer + tmp;
}

void pmix_timing_init(pmix_timing_t *t)
{
    memset(t,0,sizeof(*t));

    t->next_id_cntr = 0;
    t->current_id = -1;
    /* initialize events list */
    t->events = PMIX_NEW(pmix_list_t);
    /* Set buffer size */
    t->buffer_size = PMIX_TIMING_BUFSIZE;
    /* Set buffer_offset = buffer_size so new buffer
     * will be allocated at first event report */
    t->buffer_offset = t->buffer_size;
    /* initialize gettime function */
    t->get_ts = _init_timestamping();

}

pmix_timing_prep_t pmix_timing_prep_ev(pmix_timing_t *t, const char *fmt, ...)
{
    pmix_timing_event_t *ev = pmix_timing_event_alloc(t);
    if( ev == NULL ){
        pmix_timing_prep_t p = { t, NULL, PMIX_ERR_OUT_OF_RESOURCE };
        return p;
    }
    PMIX_CONSTRUCT(ev, pmix_timing_event_t);
    ev->ts = t->get_ts();
    va_list args;
    va_start( args, fmt );
    vsnprintf(ev->descr, PMIX_TIMING_DESCR_MAX - 1, fmt, args);
    ev->descr[PMIX_TIMING_DESCR_MAX-1] = '\0';
    va_end( args );
    pmix_timing_prep_t p = { t, ev, 0 };
    return p;
}

pmix_timing_prep_t pmix_timing_prep_ev_end(pmix_timing_t *t, const char *fmt, ...)
{
    pmix_timing_prep_t p = { t, NULL, 0 };

    if( t->current_id >= 0 ){
        pmix_timing_event_t *ev = pmix_timing_event_alloc(t);
        if( ev == NULL ){
            pmix_timing_prep_t p = { t, NULL, PMIX_ERR_OUT_OF_RESOURCE };
            return p;
        }
        PMIX_CONSTRUCT(ev, pmix_timing_event_t);
        ev->ts = t->get_ts();
        p.ev = ev;
    }
    return p;
}

void pmix_timing_add_step(pmix_timing_prep_t p,
                          const char *func, const char *file, int line)
{
    if( !p.errcode ) {
        p.ev->func = func;
        p.ev->file = file;
        p.ev->line = line;
        p.ev->type = PMIX_TIMING_TRACE;
        pmix_list_append(p.t->events, (pmix_list_item_t*)p.ev);
    }
}

/* Add description of the interval */
int pmix_timing_descr(pmix_timing_prep_t p,
                           const char *func, const char *file, int line)
{
    if( !p.errcode ){
        p.ev->func = func;
        p.ev->file = file;
        p.ev->line = line;
        p.ev->type = PMIX_TIMING_INTDESCR;
        p.ev->id = p.t->next_id_cntr;
        (p.t->next_id_cntr)++;
        pmix_list_append(p.t->events, (pmix_list_item_t*)p.ev);
        return p.ev->id;
    }
    return -1;
}

void pmix_timing_start_id(pmix_timing_t *t, int id, const char *func, const char *file, int line)
{
    /* No description is needed. If everything is OK
     * it'll be included in pmix_timing_start_init */
    pmix_timing_event_t *ev = pmix_timing_event_alloc(t);
    if( ev == NULL ){
        return;
    }
    PMIX_CONSTRUCT(ev, pmix_timing_event_t);

    t->current_id = id;
    ev->ts = t->get_ts();
    ev->func = func;
    ev->file = file;
    ev->line = line;
    ev->type = PMIX_TIMING_INTBEGIN;
    ev->id = id;
    pmix_list_append(t->events, (pmix_list_item_t*)ev);
}

void pmix_timing_end(pmix_timing_t *t, int id, const char *func, const char *file, int line )
{
    /* No description is needed. If everything is OK
     * it'll be included in pmix_timing_start_init */
    pmix_timing_event_t *ev = pmix_timing_event_alloc(t);
    if( ev == NULL ){
        return;
    }
    PMIX_CONSTRUCT(ev, pmix_timing_event_t);

    if( id < 0 ){
        ev->id = t->current_id;
        t->current_id = -1;
    } else {
        if( t->current_id == id ){
            t->current_id = -1;
        }
        ev->id = id;
    }
    ev->ts = t->get_ts();
    ev->func = func;
    ev->file = file;
    ev->line = line;
    ev->type = PMIX_TIMING_INTEND;
    pmix_list_append(t->events, (pmix_list_item_t*)ev);
}

void pmix_timing_end_prep(pmix_timing_prep_t p,
                                        const char *func, const char *file, int line)
{
    pmix_timing_event_t *ev = p.ev;

    if( !p.errcode && ( NULL != ev ) ){
        assert(  p.t->current_id >=0 );
        ev->id = p.t->current_id;
        p.t->current_id = -1;
        ev->func = func;
        ev->file = file;
        ev->line = line;
        ev->type = PMIX_TIMING_INTEND;
        pmix_list_append(p.t->events, (pmix_list_item_t*)ev);
    }
}

static int _prepare_descriptions(pmix_timing_t *t, struct interval_descr **__descr)
{
    struct interval_descr *descr;
    pmix_timing_event_t *ev, *next;

    if( t->next_id_cntr == 0 ){
        return 0;
    }

    *__descr = malloc(sizeof(struct interval_descr) * t->next_id_cntr);
    descr = *__descr;
    memset(descr, 0, sizeof(struct interval_descr) * t->next_id_cntr);

    PMIX_LIST_FOREACH_SAFE(ev, next, t->events, pmix_timing_event_t){

        /* pmix_output(0,"EVENT: type = %d, id=%d, ts = %.12le, ovh = %.12le %s",
                    ev->type, ev->id, ev->ts, ev->ts_ovh,
                    ev->descr );
        */
        switch(ev->type){
        case PMIX_TIMING_INTDESCR:{
            if( ev->id >= t->next_id_cntr){
                char *file = pmix_basename(ev->file);
                pmix_output(0,"pmix_timing: bad event id at %s:%d:%s, ignore and remove",
                            file, ev->line, ev->func);
                free(file);
                pmix_list_remove_item(t->events, (pmix_list_item_t *)ev);
                continue;
            }
            if( NULL != descr[ev->id].descr_ev ){
                pmix_timing_event_t *prev = descr[ev->id].descr_ev;
                char *file = pmix_basename(ev->file);
                char *file_prev = pmix_basename(prev->file);
                pmix_output(0,"pmix_timing: duplicated description at %s:%d:%s, "
                            "previous: %s:%d:%s, ignore and remove", file, ev->line, ev->func,
                            file_prev, prev->line, prev->func);
                free(file);
                free(file_prev);
                pmix_list_remove_item(t->events, (pmix_list_item_t *)ev);
                continue;
            }

            descr[ev->id].descr_ev = ev;
            descr[ev->id].begin_ev = NULL;
            descr[ev->id].interval = 0;
            descr[ev->id].overhead = 0;
            break;
        }
        case PMIX_TIMING_INTBEGIN:
        case PMIX_TIMING_INTEND:{
            if( ev->id >= t->next_id_cntr || (NULL == descr[ev->id].descr_ev ) ){
                char *file = pmix_basename(ev->file);
                pmix_output(0,"pmix_timing: bad event id at %s:%d:%s, ignore and remove",
                            file, ev->line, ev->func);
                free(file);
                pmix_list_remove_item(t->events, (pmix_list_item_t *)ev);
                continue;
            }
            break;
        }
        case PMIX_TIMING_TRACE:
            break;
        }
    }
    return t->next_id_cntr;
}

/* Output lines in portions that doesn't
 * exceed PMIX_TIMING_OUTBUF_SIZE for later automatic processing */
int pmix_timing_report(pmix_timing_t *t, char *fname)
{
    pmix_timing_event_t *ev;
    FILE *fp = NULL;
    char *buf = NULL;
    int buf_size = 0;
    struct interval_descr *descr = NULL;
    int rc = PMIX_SUCCESS;

    if( fname != NULL ){
        fp = fopen(fname,"a");
        if( fp == NULL ){
            pmix_output(0, "pmix_timing_report: Cannot open %s file"
                        " for writing timing information!",fname);
            rc = PMIX_ERROR;
            goto err_exit;
        }
    }

    _prepare_descriptions(t, &descr);

    buf = malloc(PMIX_TIMING_OUTBUF_SIZE+1);
    if( buf == NULL ){
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        goto err_exit;
    }
    buf[0] = '\0';

    double overhead = 0;
    PMIX_LIST_FOREACH(ev, t->events, pmix_timing_event_t){
        char *line, *file;
        if( ev->fib && pmix_timing_overhead ){
            overhead += ev->ts_ovh;
        }
        file = pmix_basename(ev->file);
        switch( ev->type ){
        case PMIX_TIMING_INTDESCR:
            // Service event, skip it.
            continue;
        case PMIX_TIMING_TRACE:
            rc = asprintf(&line,"[%s:%d] %s \"%s\" [PMIX_TRACE] %s:%d %.10lf\n",
                          nodename, getpid(), jobid, ev->descr, file, ev->line,
                          ev->ts + hnp_offs + overhead);
            break;
        case PMIX_TIMING_INTBEGIN:
            rc = asprintf(&line,"[%s:%d] %s \"%s [start]\" [PMIX_TRACE] %s:%d %.10lf\n",
                          nodename, getpid(), jobid, descr[ev->id].descr_ev->descr,
                          file, ev->line, ev->ts + hnp_offs + overhead);
            break;
        case PMIX_TIMING_INTEND:
            rc = asprintf(&line,"[%s:%d] %s \"%s [stop]\" [PMIX_TRACE] %s:%d %.10lf\n",
                          nodename, getpid(), jobid, descr[ev->id].descr_ev->descr,
                          file, ev->line, ev->ts + hnp_offs + overhead);
            break;
        }
        free(file);

        if( rc < 0 ){
            rc = PMIX_ERR_OUT_OF_RESOURCE;
            goto err_exit;
        }
        rc = 0;

        /* Sanity check: this shouldn't happen since description
             * is event only 1KB long and other fields should never
             * exceed 9KB */
        assert( strlen(line) <= PMIX_TIMING_OUTBUF_SIZE );


        if( buf_size + strlen(line) > PMIX_TIMING_OUTBUF_SIZE ){
            // flush buffer to the file
            if( fp != NULL ){
                fprintf(fp,"%s", buf);
                fprintf(fp,"\n");
            } else {
                pmix_output(0,"\n%s", buf);
            }
            buf[0] = '\0';
            buf_size = 0;
        }
        sprintf(buf,"%s%s", buf, line);
        buf_size += strlen(line);
        free(line);
    }

    if( buf_size > 0 ){
        // flush buffer to the file
        if( fp != NULL ){
            fprintf(fp,"%s", buf);
            fprintf(fp,"\n");
        } else {
            pmix_output(0,"\n%s", buf);
        }
        buf[0] = '\0';
        buf_size = 0;
    }

err_exit:
    if( NULL != descr ){
        free(descr);
    }
    if( buf != NULL ){
        free(buf);
    }
    if( fp != NULL ){
        fflush(fp);
        fclose(fp);
    }
    return rc;
}

/* Output events as one buffer so the data won't be mixed
 * with other output. This function is supposed to be human readable.
 * The output goes only to stdout. */
int pmix_timing_deltas(pmix_timing_t *t, char *fname)
{
    pmix_timing_event_t *ev;
    FILE *fp = NULL;
    char *buf = NULL;
    struct interval_descr *descr = NULL;
    int i, rc = PMIX_SUCCESS;
    size_t buf_size = 0, buf_used = 0;

    if( fname != NULL ){
        fp = fopen(fname,"a");
        if( fp == NULL ){
            pmix_output(0, "pmix_timing_report: Cannot open %s file"
                        " for writing timing information!",fname);
            rc = PMIX_ERROR;
            goto err_exit;
        }
    }

    _prepare_descriptions(t, &descr);

    PMIX_LIST_FOREACH(ev, t->events, pmix_timing_event_t){
        int id;
        if( ev->fib ){
            /* this event caused buffered memory allocation
             * for events. Account the overhead for all active
             * intervals. */
            int i;
            for( i = 0; i < t->next_id_cntr; i++){
                if( (NULL != descr[i].descr_ev) && (NULL != descr[i].begin_ev) ){
                    if( pmix_timing_overhead ){
                        descr[i].overhead += ev->ts_ovh;
                    }
                }
            }
        }

        /* we already process all PMIX_TIMING_DESCR events
         * and we ignore PMIX_TIMING_EVENT */
        if( ev->type == PMIX_TIMING_INTDESCR ||
                ev->type == PMIX_TIMING_TRACE){
            /* skip */
            continue;
        }

        id = ev->id;
        if( id < 0 || id >= t->next_id_cntr ){
            char *file = pmix_basename(ev->file);
            pmix_output(0,"pmix_timing_deltas: bad interval event id: %d at %s:%d:%s (maxid=%d)",
                        id, file, ev->line, ev->func, t->next_id_cntr - 1 );
            free(file);
            /* skip */
            continue;
        }

        /* id's assigned auomatically. Ther shouldn't be any gaps in descr[] */
        assert( NULL != descr[id].descr_ev);

        if( ev->type == PMIX_TIMING_INTBEGIN ){
            if( NULL != descr[id].begin_ev ){
                /* the measurement on this interval was already
                 * started! */
                pmix_timing_event_t *prev = descr[ev->id].begin_ev;
                char *file = pmix_basename(ev->file);
                char *file_prev = pmix_basename(prev->file);
                pmix_output(0,"pmix_timing_deltas: duplicated start statement at %s:%d:%s, "
                            "previous: %s:%d:%s", file, ev->line, ev->func,
                            file_prev, prev->line, prev->func);
                free(file);
                free(file_prev);
            } else {
                /* save pointer to the start of measurement event */
                descr[id].begin_ev = ev;
            }
            /* done, go to the next event */
            continue;
        }

        if( ev->type == PMIX_TIMING_INTEND ){
            if( NULL == descr[id].begin_ev ){
                /* the measurement on this interval wasn't started! */
                char *file = pmix_basename(ev->file);
                pmix_output(0,"pmix_timing_deltas: inteval end without start at %s:%d:%s",
                            file, ev->line, ev->func );
                free(file);
            } else {
                descr[id].interval += ev->ts - descr[id].begin_ev->ts;
                descr[id].begin_ev = NULL;
                if( ev->fib ){
                    descr[id].overhead += ev->ts_ovh;
                }
            }
            continue;
        }

        /* shouldn't ever get here: bad ev->type */
        pmix_output(0, "pmix_timing_deltas: bad event type %d", ev->type);
        assert(0);
    }

    buf = malloc(PMIX_TIMING_OUTBUF_SIZE + 1);
    if( buf == NULL ){
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        goto err_exit;
    }
    buf[0] = '\0';
    buf_size = PMIX_TIMING_OUTBUF_SIZE + 1;
    buf_used = 0;
    for(i = 0; i < t->next_id_cntr; i++){
        char *line = NULL;
        size_t line_size;
        rc = asprintf(&line,"[%s:%d] %s \"%s\" [PMIX_OVHD] %le\n",
                      nodename, getpid(), jobid, descr[i].descr_ev->descr,
                      descr[i].interval - descr[i].overhead);
        if( rc < 0 ){
            rc = PMIX_ERR_OUT_OF_RESOURCE;
            goto err_exit;
        }
        rc = 0;
        line_size = strlen(line);

        /* Sanity check: this shouldn't happen since description
         * is event only 1KB long and other fields should never
         * exceed 9KB */
        assert( line_size <= PMIX_TIMING_OUTBUF_SIZE );

        if( buf_used + strlen(line) > buf_size ){
            // Increase output buffer
            while( buf_used + line_size > buf_size && buf_size < DELTAS_SANE_LIMIT){
                buf_size += PMIX_TIMING_OUTBUF_SIZE + 1;
            }
            if( buf_size > DELTAS_SANE_LIMIT ){
                pmix_output(0, "pmix_timing_report: delta sane limit overflow (%u > %u)!\n",
                            (unsigned int)buf_size, DELTAS_SANE_LIMIT);
                free(line);
                rc = PMIX_ERR_OUT_OF_RESOURCE;
                goto err_exit;
            }
            buf = realloc(buf, buf_size);
            if( buf == NULL ){
                pmix_output(0, "pmix_timing_deltas: Out of memory!\n");
                rc = PMIX_ERR_OUT_OF_RESOURCE;
                goto err_exit;
            }
        }
        sprintf(buf,"%s%s", buf, line);
        buf_used += line_size;
        free(line);
    }


    if( buf_used > 0 ){
        // flush buffer to the file
        if( fp != NULL ){
            fprintf(fp,"%s", buf);
            fprintf(fp,"\n");
        } else {
            pmix_output(0,"\n%s", buf);
        }
        buf[0] = '\0';
        buf_size = 0;
    }

err_exit:
    if( NULL != descr ){
        free(descr);
    }
    if( NULL != buf ){
        free(buf);
    }
    if( fp != NULL ){
        fflush(fp);
        fclose(fp);
    }
    return rc;
}

void pmix_timing_release(pmix_timing_t *t)
{
    int cnt = pmix_list_get_size(t->events);

    if( cnt > 0 ){
        pmix_list_t *tmp = PMIX_NEW(pmix_list_t);
        int i;
        for(i=0; i<cnt; i++){
            pmix_timing_event_t *ev = (pmix_timing_event_t *)pmix_list_remove_first(t->events);
            if( ev->fib ){
                pmix_list_append(tmp,(pmix_list_item_t*)ev);
            }
        }

        cnt = pmix_list_get_size(tmp);
        for(i=0; i<cnt; i++){
            pmix_timing_event_t *ev = (pmix_timing_event_t *)pmix_list_remove_first(tmp);
            free(ev);
        }
        PMIX_RELEASE(tmp);
    } else {
        // Error case. At list one event was inserted at initialization.

    }

    PMIX_RELEASE(t->events);
    t->events = NULL;
}
#endif
