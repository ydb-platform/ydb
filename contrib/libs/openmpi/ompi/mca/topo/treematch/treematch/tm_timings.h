#ifndef TIMINGS_H
#define TIMINGS_H
#include <stdio.h>

#ifndef _WIN32
#include <sys/time.h>
#else
#include <sys/timeb.h>
#endif
#include <stdlib.h>
#include <unistd.h>

#define MAX_CLOCK 1000

#ifndef _WIN32
typedef struct timeval CLOCK_T;


#define CLOCK(c) gettimeofday(&c,(struct timezone *)NULL)
#define CLOCK_DIFF(c1,c2)  \
((double)(c1.tv_sec-c2.tv_sec)+(double)(c1.tv_usec-c2.tv_usec)/1e+6)
#define CLOCK_DISPLAY(c) fprintf(stderr,"%d.%d",(int)c.tv_sec,(int)c.tv_usec)

#else    /* for windows */

#ifdef __CYGWIN__
typedef struct timeb CLOCK_T;
#else
typedef struct _timeb CLOCK_T;
#endif

#define CLOCK(c) _ftime(&c)
#define CLOCK_DIFF(c1,c2)  \
((double)(c1.time-c2.time)+(double)(c1.millitm-c2.millitm)/1e+3)
#define CLOCK_DISPLAY(c) fprintf(stderr,"%d.%d",(int)c.time,(int)c.millitm*1e+3)

#endif

double time_diff(void);
void get_time(void);

#define TIC get_time()
#define TOC time_diff()

#endif /*TIMINGS_H*/

