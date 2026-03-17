/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

#if ECCODES_TIMER

#ifndef ECCODES_ON_WINDOWS
#include <sys/time.h>
#include <time.h>
#include <sys/resource.h>
#endif

static grib_timer* timers = NULL;
int value_false           = 0;
int value_true            = 1;
#define long64 long

#define ROUND(a) ((long)((a) + 0.5))

double proc_cpu()
{
    struct rusage rup;

    if (getrusage(RUSAGE_SELF, &rup) != -1) {
        return (rup.ru_utime.tv_sec + rup.ru_utime.tv_usec / 1000000.0 +
                rup.ru_stime.tv_sec + rup.ru_stime.tv_usec / 1000000.0);
    }
    return clock() / (double)CLOCKS_PER_SEC;
}

static char* timetext(char* pfx, double ds, char* text)
{
    long s = ROUND(ds);
    long x = s;
    long n;
    char sec[20];
    char min[20];
    char hou[20];
    char day[20];

    *text = *sec = *min = *hou = *day = 0;
    if (s) {
        if ((n = x % 60) != 0)
            sprintf(sec, "%ld sec ", n);
        x /= 60;
        if ((n = x % 60) != 0)
            sprintf(min, "%ld min ", n);
        x /= 60;
        if ((n = x % 24) != 0)
            sprintf(hou, "%ld hour ", n);
        x /= 24;
        if ((n = x) != 0)
            sprintf(day, "%ld day ", n);

        sprintf(text, "%s%s%s%s%s", pfx, day, hou, min, sec);
    }

    return text;
}

char* timename(double t)
{
    static char buf[80];
    return timetext("", t, buf);
}

grib_timer* grib_get_timer(grib_context* c, const char* name, const char* statname, int elapsed)
{
    grib_timer* t = timers;
    if (!c)
        c = grib_context_get_default();

    while (t) {
        if (strcmp(name, t->name_) == 0)
            return t;
        t = t->next_;
    }

    t          = (grib_timer*)grib_context_malloc_clear(c, sizeof(grib_timer));
    t->name_   = (char*)name;
    t->context = c;
    t->active_ = value_false;
    t->count_  = 0;
    t->timer_  = 0;
    t->total_  = 0;

    t->elapsed_   = elapsed; /* Whether to print CPU usage */
    t->cpu_       = 0;
    t->total_cpu_ = 0;

    t->statname_ = 0;
    if (statname)
        t->statname_ = (char*)statname;

    t->next_ = timers;
    timers   = t;

    return t;
}

int grib_timer_start(grib_timer* t)
{
    int e = gettimeofday(&t->start_, NULL);
    if (e != 0)
        grib_context_log(t->context, GRIB_LOG_WARNING | GRIB_LOG_PERROR, "Error starting timer '%s'", t->name_ ? t->name_ : "unnamed");
    t->active_ = value_true;
    t->cpu_    = proc_cpu();
    return e;
}

int grib_timer_stop(grib_timer* t, long total)
{
    struct timeval stop, diff;
    int e    = gettimeofday(&stop, NULL);
    double c = proc_cpu();

    if (e != 0)
        grib_context_log(t->context, GRIB_LOG_WARNING | GRIB_LOG_PERROR, "Error stopping timer '%s'", t->name_ ? t->name_ : "unnamed");

    if (!t->active_) {
        grib_context_log(t->context, GRIB_LOG_WARNING, "Stopping non-started timer '%s'", t->name_ ? t->name_ : "unnamed");
        return 1;
    }

    diff.tv_sec  = stop.tv_sec - t->start_.tv_sec;
    diff.tv_usec = stop.tv_usec - t->start_.tv_usec;
    if (diff.tv_usec < 0) {
        diff.tv_sec--;
        diff.tv_usec += 1000000;
    }

    t->timer_ += (double)diff.tv_sec + ((double)diff.tv_usec / 1000000.);
    t->total_ += total;
    t->total_cpu_ += (c - t->cpu_);

    t->active_ = value_false;
    t->count_++;

    return e;
}

double grib_timer_value(grib_timer* t)
{
    return t->timer_;
}

const char* bytename(double bytes)
{
    static char* names[] = {
        "", "K", "M", "G", "T"
    };
    double x = bytes;
    int n    = 0;
    static char buf[20];

    while (x >= 1024.0) {
        x /= 1024.0;
        n++;
    }

    sprintf(buf, "%.2f %s", x, names[n]);

    return buf;
}

void grib_timer_print(grib_timer* t)
{
    char cpu[1024]   = "";
    const char* name = t->name_ ? t->name_ : "";
    if (t->timer_ >= 1) {
        if (!t->elapsed_ && t->total_cpu_ >= 1.0)
            sprintf(cpu, "cpu: %s", timename(t->total_cpu_));

        if (t->total_ != 0) {
            double rate = (double)t->total_ / t->timer_;
            char bytes[80];
            sprintf(bytes, "%sbyte(s)", bytename(t->total_));
            grib_context_print(t->context, stdout, "  %s: %s in %s [%sbyte/sec] %s\n",
                               name, bytes, timename(t->timer_), bytename(rate), cpu);
        }
        else {
            char* ctimename = timename(t->timer_);
            grib_context_print(t->context, stdout, "  %s: wall: %s%s\n", name, ctimename, cpu);
        }
        /*
    if(t->statname_)
      log_statistics(t->statname_,"%ld",(long)t->timer_);
         */
    }
}

void grib_timer_partial_rate(grib_timer* t, double start, long total)
{
    double ptime     = t->timer_ - start;
    long ptotal      = total;
    const char* name = t->name_ ? t->name_ : "";
    if (ptime >= 1) {
        double rate = (double)ptotal / ptime;
        char bytes[80];
        sprintf(bytes, "%sbyte(s)", bytename(ptotal));
        grib_context_log(t->context, GRIB_LOG_INFO, "  %s: %s in %s [%sbyte/sec]",
                         name, bytes, timename(ptime), bytename(rate));
    }
}

void grib_print_all_timers()
{
    grib_timer* t = timers;
    while (t) {
        grib_timer_print(t);
        t = t->next_;
    }
}

void grib_reset_all_timers()
{
    grib_timer* t = timers;
    while (t) {
        t->count_     = 0;
        t->timer_     = 0;
        t->active_    = 0;
        t->total_     = 0;
        t->total_cpu_ = 0;
        t             = t->next_;
    }
}


/*************************************************
 * Timed functions
 **************************************************/
/*
int timed_fread(char *buffer, int n, int length, FILE *f, grib_timer *t)
{
  int r = 0;
  long total = 0;
  timer_start(t);
  if((r = fread(buffer,n,length,f)) > 0)
    total = r*n;

  timer_stop(t,total);

  return r;
}


int timed_wind_next(wind* w, FILE *f, char *buffer, long *length, grib_timer *t)
{
  int r = 0;
  long64 total = 0;
  timer_start(t);
  if((r = wind_next(w,f,buffer,length)) == 0)
    total = *length;

  timer_stop(t,total);

  return r;
}

int timed_fwrite(char *buffer, int n, int length, FILE *f, grib_timer *t)
{
  int r = 0;
  long64 total = 0;
  timer_start(t);
  if((r = fwrite(buffer,n,length,f)) > 0)
    total = r*n;
  timer_stop(t,total);

  return r;
}

int timed_fclose(FILE *f, grib_timer *t)
{
  int r = 0;
  timer_start(t);
  r = fclose(f);
  timer_stop(t,0);

  return r;
}

int timed_writetcp(void *data, char *buffer, int n, grib_timer *t)
{
  int r = 0;
  long64 total = 0;
  timer_start(t);
  if((r = writetcp(data,buffer,n)) > 0)
    total = r;
  timer_stop(t,total);

  return r;
}

int timed_readtcp(void *data, char *buffer, int n, grib_timer *t)
{
  int r = 0;
  long64 total = 0;
  timer_start(t);
  if((r = readtcp(data,buffer,n)) > 0)
    total = r;
  timer_stop(t,total);
  return r;
}

int timed_readany(FILE *f, char *buffer, long *length, grib_timer *t)
{
  int e;
  long original = *length;
  long64 total = 0;
  timer_start(t);
  if(((e = _readany(f,buffer,length)) == NOERR) || (e == BUF_TO_SMALL))
    total = (e == BUF_TO_SMALL)?original:*length;
  timer_stop(t,total);

  if(e != NOERR && e != BUF_TO_SMALL && e != EOF)
  {
    if(e == NOT_FOUND_7777)
      marslog(LOG_WARN,"Group 7777 not found by readany",e);
    else
      marslog(LOG_WARN,"Error %d returned by readany",e);
  }

  return e;
}
 */

#else

grib_timer* grib_get_timer(grib_context* c, const char* name, const char* statname, int elapsed)
{
    if (!c)
        c = grib_context_get_default();
    grib_context_log(c, GRIB_LOG_ERROR, "%s function not available", __func__);
    return NULL;
}

int grib_timer_start(grib_timer* t)
{
    return 0;
}

int grib_timer_stop(grib_timer* t, long total)
{
    return 0;
}

double grib_timer_value(grib_timer* t)
{
    return 0;
}

void grib_timer_print(grib_timer* t)
{
}

void grib_timer_partial_rate(grib_timer* t, double start, long total)
{
}

void grib_print_all_timers()
{
}

void grib_reset_all_timers()
{
}

#endif
