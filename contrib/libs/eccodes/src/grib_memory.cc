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

/* #define CHECK_FOR_LEAKS */

#if MANAGE_MEM

#if GRIB_PTHREADS
static pthread_once_t once   = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_memory_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex);
            once = 1;
        }
    }
}
#endif

union align
{
    double d;
    int n;
    void* p;
    long l;
};

#define WORD sizeof(union align)

static size_t page_size = 0;

typedef struct
{
    int pages; /* Number of pages to allocate */
    int clear; /* clear newly allocated memory */
    int first; /* Allocate in first only */
    void* priv;
} mempool;


typedef struct memblk
{
    struct memblk* next;
    long cnt;
    size_t left;
    size_t size;
    char buffer[WORD];
} memblk;

static memblk* reserve = NULL;

/*
   typedef struct memprocs {
   struct memprocs *next;
   memproc          proc;
   void            *data;
   } memprocs;

   static memprocs *mprocs = NULL;
 */

#define HEADER_SIZE (sizeof(memblk) - WORD)

static mempool _transient_mem = {
    10,
    0,
    1,
};

static mempool* transient_mem = &_transient_mem;

static mempool _permanent_mem = {
    10,
    0,
    1,
};

static mempool* permanent_mem = &_permanent_mem;

static void* fast_new(size_t s, mempool* pool)
{
    char* p;
    memblk* m;

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    m = (memblk*)pool->priv;

    /* align */

    s = ((s + WORD - 1) / WORD) * WORD;

    if (pool->first) {
        if (m && (m->left < s))
            m = NULL;
    }

    while (m && (m->left < s))
        m = m->next;

    if (m == NULL) {
        memblk* p;
        size_t size;
        if (!page_size)
            page_size = getpagesize();

        size = page_size * pool->pages;

        if (s > size - HEADER_SIZE) {
            /* marslog(LOG_WARN,"Object of %d bytes is too big for grib_fast_new",s); */
            /* marslog(LOG_WARN,"Block size if %d bytes", size - HEADER_SIZE); */
            size = ((s + HEADER_SIZE + (page_size - 1)) / page_size) * page_size;
        }

        p = (memblk*)(pool->clear ? calloc(size, 1) : malloc(size));
        if (!p) {
            GRIB_MUTEX_UNLOCK(&mutex);
            return NULL;
        }

        p->next = (memblk*)pool->priv;
        p->cnt  = 0;
        p->size = p->left = size - HEADER_SIZE;
        m                 = p;
        pool->priv        = (void*)p;
    }

    p = &m->buffer[m->size - m->left];
    m->left -= s;
    m->cnt++;

    GRIB_MUTEX_UNLOCK(&mutex);

    return p;
}


static void fast_delete(void* p, mempool* pool)
{
    memblk* m;
    memblk* n = NULL;

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    m = (memblk*)pool->priv;

    while (m) {
        if (((char*)p >= (char*)&m->buffer[0]) &&
            ((char*)p < (char*)&m->buffer[m->size])) {
            m->cnt--;
            if (m->cnt == 0) {
                if (n)
                    n->next = m->next;
                else
                    pool->priv = (void*)m->next;
                free((void*)m);
            }
            GRIB_MUTEX_UNLOCK(&mutex);
            return;
        }

        n = m;
        m = m->next;
    }
    ECCODES_ASSERT(1 == 0);
}

static void* fast_realloc(void* p, size_t s, mempool* pool)
{
    void* q;

    /* I'll come back here later... */

    if ((q = fast_new(s, pool)) == NULL)
        return NULL;
    memcpy(q, p, s);

    fast_delete(p, pool);

    return q;
}

/*
   void fast_memory_info(const char *title,mempool *pool) {
   memblk *m = (memblk*)pool->priv;
   int count = 0;
   int size = 0;
   while(m) {
    count++;
    size += m->size;
    m = m->next;
   }
   marslog(LOG_INFO,"%s : %sbytes %d blocks", title, bytename(size),count);
   }

   void memory_info() {
   memblk *r = reserve;
   long size = 0;
   while(r)
   {
   marslog(LOG_INFO,"Large buffer: %sbytes %s",
   bytename(r->size),r->cnt?"in use":"free");
   size += r->size;
   r = r->next;
   }
   marslog(LOG_INFO,"Total large : %sbytes",bytename(size));
   fast_memory_info("Transient memory",transient_mem);
   fast_memory_info("Permanent memory",permanent_mem);
   }
 */

void* grib_transient_malloc(const grib_context* c, size_t s)
{
    return fast_new(s, transient_mem);
}

void* grib_transient_realloc(const grib_context* c, void* p, size_t s)
{
    return fast_realloc(p, s, transient_mem);
}

void grib_transient_free(const grib_context* c, void* p)
{
    fast_delete(p, transient_mem);
}

void* grib_permanent_malloc(const grib_context* c, size_t s)
{
    return fast_new(s, permanent_mem);
}

void* grib_permanent_realloc(const grib_context* c, void* p, size_t s)
{
    return fast_realloc(p, s, permanent_mem);
}

void grib_permanent_free(const grib_context* c, void* p)
{
    fast_delete(p, permanent_mem);
}

void* grib_buffer_malloc(const grib_context* c, size_t s)
{
    memblk* r;

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    s = ((s + WORD - 1) / WORD) * WORD;
    r = reserve;
    while (r) {
        if (r->cnt == 0 && r->size == s)
            break;
        r = r->next;
    }

    if (r) {
        /* marslog(LOG_DBUG,"Reusing %ld bytes %d",s,r->size); */
    }
    else {
        size_t size = s + HEADER_SIZE;
        /* marslog(LOG_DBUG,"Allocating %d (%d)bytes",s,size); */
        r = (memblk*)malloc(size);
        if (!r)
            return NULL;
        r->next = reserve;
        reserve = r;
    }
    r->size = s;
    r->cnt  = 1;

    GRIB_MUTEX_UNLOCK(&mutex);

    return &r->buffer[0];
}

void grib_buffer_free(const grib_context* c, void* p)
{
    memblk* r;
    memblk* s;

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    r = (memblk*)(((char*)p) - HEADER_SIZE);
    s = reserve;
    while (s && (s != r))
        s = s->next;
    if (s == NULL)
        ; /* marslog(LOG_WARN,"release_mem: invalid pointer"); */
    else {
        s->cnt = 0;
    }

    GRIB_MUTEX_UNLOCK(&mutex);
}

void* grib_buffer_realloc(const grib_context* c, void* p, size_t s)
{
    memblk* r = (memblk*)(((char*)p) - HEADER_SIZE);
    void* n   = grib_buffer_malloc(c, s);
    memcpy(n, p, r->size < s ? r->size : s);
    grib_buffer_free(c, p);
    return n;
}

/*

   int purge_mem(void)
   {
   memblk *p = reserve;
   memblk *q = NULL;
   while(p)
   {
   if(p->cnt == 0)
   {
   if(q) q->next = p->next;
   else reserve = p->next;
   free(p);
   return 1;
   }
   q = p;
   p = p->next;
   }
   return 0;
   }

   void install_memory_proc(memproc proc,void *data)
   {
   memprocs *p = NEW_CLEAR(memprocs);
   p->proc = proc;
   p->data = data;
   p->next = mprocs;
   mprocs  = p;
   }

   void remove_memory_proc(memproc proc,void *data)
   {
   memprocs *p = mprocs;
   memprocs *q = NULL;

   while(p)
   {
   if(p->proc == proc && p->data == data)
   {
   if(q) q->next = p->next; else mprocs = p->next;
   FREE(p);
   return;
   }
   q = p;
   p = p->next;
   }
   marslog(LOG_WARN,"remove_memory_proc: cannot find proc");
   }
 */

#endif
